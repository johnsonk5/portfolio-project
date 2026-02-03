import os
from datetime import datetime, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.silver_prices import silver_alpaca_prices

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)


@asset(
    name="gold_alpaca_prices",
    deps=[silver_alpaca_assets, silver_alpaca_prices],
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
    try:
        con.execute("SELECT 1 FROM silver.assets LIMIT 1")
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
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
            pct_below_52w_high DOUBLE,
            sma_50 DOUBLE,
            sma_200 DOUBLE,
            dist_sma_50 DOUBLE,
            dist_sma_200 DOUBLE
        )
        """
    )

    lookback_start = partition_date - timedelta(days=400)

    daily_sql = """
        WITH active_assets AS (
            SELECT asset_id, symbol
            FROM silver.assets
            WHERE is_active = TRUE
        ),
        target_assets AS (
            SELECT DISTINCT prices.asset_id, prices.symbol
            FROM silver.prices AS prices
            INNER JOIN active_assets AS assets
                ON prices.asset_id = assets.asset_id
            WHERE CAST(prices.timestamp AS DATE) = ?
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
        history_prices AS (
            SELECT asset_id, symbol, trade_date, close
            FROM gold.prices
            WHERE trade_date < ?
              AND asset_id IN (SELECT asset_id FROM target_assets)
        ),
        returns_base AS (
            SELECT * FROM history_prices
            UNION ALL
            SELECT asset_id, symbol, trade_date, close
            FROM daily_prices
        ),
        returns_features AS (
            SELECT
                asset_id,
                symbol,
                trade_date,
                close,
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
                max(close) OVER (
                    PARTITION BY asset_id
                    ORDER BY trade_date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS high_52w,
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
            FROM returns_base
        ),
        vol_features AS (
            SELECT
                *,
                stddev_samp(returns_1d) OVER (
                    PARTITION BY asset_id
                    ORDER BY trade_date
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                ) * sqrt(252) AS realized_vol_21d
            FROM returns_features
        ),
        final AS (
            SELECT
                d.asset_id,
                d.symbol,
                d.trade_date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.volume,
                d.trade_count,
                d.vwap,
                d.close * d.volume AS dollar_volume,
                v.returns_1d,
                v.returns_5d,
                v.returns_21d,
                v.realized_vol_21d,
                v.momentum_12_1,
                CASE
                    WHEN v.high_52w IS NULL OR v.high_52w = 0 THEN NULL
                    ELSE (v.high_52w - d.close) / v.high_52w
                END AS pct_below_52w_high,
                v.sma_50,
                v.sma_200,
                CASE
                    WHEN v.sma_50 IS NULL OR v.sma_50 = 0 THEN NULL
                    ELSE d.close / v.sma_50 - 1
                END AS dist_sma_50,
                CASE
                    WHEN v.sma_200 IS NULL OR v.sma_200 = 0 THEN NULL
                    ELSE d.close / v.sma_200 - 1
                END AS dist_sma_200
            FROM daily_prices d
            LEFT JOIN vol_features v
                ON v.asset_id = d.asset_id
               AND v.symbol = d.symbol
               AND v.trade_date = d.trade_date
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
        [
            partition_date,
            lookback_start,
            partition_date,
            partition_date,
            partition_date,
        ],
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
