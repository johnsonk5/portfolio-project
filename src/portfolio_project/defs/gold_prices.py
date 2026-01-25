import os
from datetime import datetime, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_prices import silver_alpaca_prices

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)


@asset(
    name="gold_alpaca_prices",
    deps=[silver_alpaca_prices],
    partitions_def=GOLD_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def gold_alpaca_prices(context: AssetExecutionContext) -> None:
    """
    Build daily gold-layer prices and factor features from silver.prices.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    con = context.resources.duckdb

    try:
        con.execute("SELECT 1 FROM silver.prices LIMIT 1")
    except Exception as exc:
        context.log.warning("Silver prices table missing or unreadable: %s", exc)
        return

    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.prices (
            asset_id BIGINT,
            symbol VARCHAR,
            trade_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            trade_count BIGINT,
            vwap DOUBLE,
            dollar_volume DOUBLE,
            returns_1d DOUBLE,
            returns_5d DOUBLE,
            returns_21d DOUBLE,
            realized_vol_21d DOUBLE,
            momentum_12_1 DOUBLE,
            sma_50 DOUBLE,
            sma_200 DOUBLE,
            dist_sma_50 DOUBLE,
            dist_sma_200 DOUBLE
        )
        """
    )

    lookback_start = partition_date - timedelta(days=400)

    daily_sql = """
        WITH target_assets AS (
            SELECT DISTINCT asset_id, symbol
            FROM silver.prices
            WHERE CAST(timestamp AS DATE) = ?
        ),
        daily_prices AS (
            SELECT
                asset_id,
                symbol,
                CAST(timestamp AS DATE) AS trade_date,
                arg_min(open, timestamp) AS open,
                max(high) AS high,
                min(low) AS low,
                arg_max(close, timestamp) AS close,
                sum(volume) AS volume,
                sum(trade_count) AS trade_count,
                CASE
                    WHEN sum(CASE WHEN vwap IS NOT NULL AND volume IS NOT NULL THEN volume ELSE 0 END) > 0
                        THEN sum(vwap * volume) / sum(volume)
                    ELSE NULL
                END AS vwap
            FROM silver.prices
            WHERE CAST(timestamp AS DATE) BETWEEN ? AND ?
              AND asset_id IN (SELECT asset_id FROM target_assets)
            GROUP BY asset_id, symbol, CAST(timestamp AS DATE)
        ),
        daily_features AS (
            SELECT
                *,
                close / lag(close, 1) OVER (PARTITION BY asset_id ORDER BY trade_date) - 1
                    AS returns_1d,
                close / lag(close, 5) OVER (PARTITION BY asset_id ORDER BY trade_date) - 1
                    AS returns_5d,
                close / lag(close, 21) OVER (PARTITION BY asset_id ORDER BY trade_date) - 1
                    AS returns_21d,
                (
                    lag(close, 21) OVER (PARTITION BY asset_id ORDER BY trade_date)
                    / lag(close, 252) OVER (PARTITION BY asset_id ORDER BY trade_date)
                    - 1
                ) AS momentum_12_1,
                close * volume AS dollar_volume,
                avg(close) OVER (
                    PARTITION BY asset_id
                    ORDER BY trade_date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS sma_50,
                avg(close) OVER (
                    PARTITION BY asset_id
                    ORDER BY trade_date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS sma_200
            FROM daily_prices
        ),
        final AS (
            SELECT
                asset_id,
                symbol,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                trade_count,
                vwap,
                dollar_volume,
                returns_1d,
                returns_5d,
                returns_21d,
                stddev_samp(returns_1d) OVER (
                    PARTITION BY asset_id
                    ORDER BY trade_date
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                ) * sqrt(252) AS realized_vol_21d,
                momentum_12_1,
                sma_50,
                sma_200,
                CASE
                    WHEN sma_50 IS NULL OR sma_50 = 0 THEN NULL
                    ELSE close / sma_50 - 1
                END AS dist_sma_50,
                CASE
                    WHEN sma_200 IS NULL OR sma_200 = 0 THEN NULL
                    ELSE close / sma_200 - 1
                END AS dist_sma_200
            FROM daily_features
        )
        SELECT *
        FROM final
        WHERE trade_date = ?
    """

    con.execute(
        "DELETE FROM gold.prices WHERE trade_date = ?",
        [partition_date],
    )
    insert_sql = f"""
        INSERT INTO gold.prices
        {daily_sql}
    """
    con.execute(
        insert_sql,
        [partition_date, lookback_start, partition_date, partition_date],
    )

    row_count = con.execute(
        "SELECT count(*) AS count FROM gold.prices WHERE trade_date = ?",
        [partition_date],
    ).fetchone()[0]

    context.add_output_metadata(
        {
            "table": "gold.prices",
            "partition": context.partition_key,
            "row_count": row_count,
        }
    )
