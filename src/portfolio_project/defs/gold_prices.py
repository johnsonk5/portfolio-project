import os
from datetime import datetime, timedelta
from pathlib import Path

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.silver_prices import silver_alpaca_prices_parquet

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


def _table_exists(con, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        is not None
    )


@asset(
    name="gold_alpaca_prices",
    deps=[silver_alpaca_assets, silver_alpaca_prices_parquet],
    partitions_def=GOLD_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def gold_alpaca_prices(context: AssetExecutionContext) -> None:
    """
    Build daily gold-layer prices and factor features from silver parquet.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    con = context.resources.duckdb

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
            dist_sma_200 DOUBLE,
            sentiment_score DOUBLE
        )
        """
    )
    con.execute(
        "ALTER TABLE gold.prices ADD COLUMN IF NOT EXISTS sentiment_score DOUBLE"
    )

    sentiment_enabled = _table_exists(con, "gold", "headlines") and _table_exists(
        con, "silver", "ref_publishers"
    )
    sentiment_window_start = partition_date - timedelta(days=6)
    sentiment_window_end = partition_date + timedelta(days=1)

    lookback_start = partition_date - timedelta(days=400)
    parquet_paths = []
    cursor = lookback_start
    while cursor <= partition_date:
        parquet_path = (
            DATA_ROOT
            / "silver"
            / "prices"
            / f"date={cursor.strftime('%Y-%m-%d')}"
            / "prices.parquet"
        )
        if parquet_path.exists():
            parquet_paths.append(parquet_path.as_posix())
        cursor += timedelta(days=1)

    if not parquet_paths:
        context.log.warning(
            "No silver parquet partitions found between %s and %s.",
            lookback_start,
            partition_date,
        )
        return

    if sentiment_enabled:
        sentiment_cte = """
        , sentiment_scores AS (
            SELECT
                scored.asset_id,
                CAST(? AS DATE) AS trade_date,
                sum(scored.sentiment_value * scored.publisher_weight)
                    / nullif(sum(scored.publisher_weight), 0) AS sentiment_score
            FROM (
                SELECT
                    h.asset_id,
                    CASE
                        WHEN lower(h.sentiment) = 'positive' THEN 1
                        WHEN lower(h.sentiment) = 'neutral' THEN 0
                        WHEN lower(h.sentiment) = 'negative' THEN -1
                        ELSE NULL
                    END AS sentiment_value,
                    p.publisher_weight
                FROM gold.headlines AS h
                LEFT JOIN silver.ref_publishers AS p
                    ON h.publisher_id = p.publisher_id
                WHERE h.provider_publish_time >= ?
                  AND h.provider_publish_time < ?
                  AND h.asset_id IS NOT NULL
            ) AS scored
            WHERE scored.sentiment_value IS NOT NULL
              AND scored.publisher_weight IS NOT NULL
              AND scored.publisher_weight > 0
            GROUP BY scored.asset_id
        )
        """
        sentiment_select = """
        SELECT
            final.*,
            sentiment_scores.sentiment_score
        FROM final
        LEFT JOIN sentiment_scores
            ON sentiment_scores.asset_id = final.asset_id
           AND sentiment_scores.trade_date = final.trade_date
        """
        sentiment_params = [
            partition_date,
            sentiment_window_start,
            sentiment_window_end,
        ]
    else:
        sentiment_cte = ""
        sentiment_select = """
        SELECT
            final.*,
            CAST(NULL AS DOUBLE) AS sentiment_score
        FROM final
        """
        sentiment_params = []

    daily_sql = f"""
        WITH active_assets AS (
            SELECT asset_id, symbol
            FROM silver.assets
            WHERE is_active = TRUE
        ),
        target_assets AS (
            SELECT DISTINCT prices.asset_id, prices.symbol
            FROM prices AS prices
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
            FROM prices
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
        {sentiment_cte}
        {sentiment_select}
        WHERE final.trade_date = ?
    """

    con.execute(
        "DELETE FROM gold.prices WHERE trade_date = ?",
        [partition_date],
    )
    insert_sql = f"""
        WITH prices AS (
            SELECT *
            FROM read_parquet(?)
        )
        INSERT INTO gold.prices
        {daily_sql}
    """
    base_params = [
        partition_date,
        lookback_start,
        partition_date,
        partition_date,
    ]
    query_params = base_params + sentiment_params + [partition_date]
    con.execute(
        insert_sql,
        [
            parquet_paths,
            *query_params,
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
