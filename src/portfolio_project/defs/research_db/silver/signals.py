import os
from pathlib import Path

from dagster import AssetExecutionContext, asset

from portfolio_project.defs.research_db.silver.research_prices import (
    silver_research_daily_prices,
)

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
SIGNAL_VERSION = os.getenv("RESEARCH_SIGNALS_VERSION", "v1")
DUCKDB_SIGNALS_THREADS = int(os.getenv("RESEARCH_SIGNALS_DUCKDB_THREADS", "2"))
SIGNALS_SYMBOL_BUCKETS = int(os.getenv("RESEARCH_SIGNALS_SYMBOL_BUCKETS", "32"))


def _signals_select_sql() -> str:
    return """
        WITH prices AS (
            SELECT
                CAST(trade_date AS DATE) AS date,
                upper(trim(symbol)) AS symbol,
                CAST(close AS DOUBLE) AS close,
                CAST(adjusted_close AS DOUBLE) AS adjusted_close,
                CAST(volume AS BIGINT) AS volume,
                CAST(dollar_volume AS DOUBLE) AS dollar_volume,
                COALESCE(CAST(adjusted_close AS DOUBLE), CAST(close AS DOUBLE)) AS return_price
            FROM read_parquet(?)
            WHERE trade_date IS NOT NULL
              AND symbol IS NOT NULL
              AND trim(symbol) <> ''
              AND abs(hash(upper(trim(symbol)))) % ? = ?
        ),
        returns_base AS (
            SELECT
                date,
                symbol,
                close,
                adjusted_close,
                volume,
                dollar_volume,
                return_price,
                lag(return_price, 1) OVER w AS lag_price_1,
                lag(return_price, 5) OVER w AS lag_price_5,
                lag(return_price, 21) OVER w AS lag_price_21,
                lag(return_price, 63) OVER w AS lag_price_63,
                lag(return_price, 126) OVER w AS lag_price_126,
                lag(return_price, 252) OVER w AS lag_price_252,
                lag(return_price, 21) OVER w AS momentum_end_price,
                lag(return_price, 252) OVER w AS momentum_start_price,
                avg(close) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS sma_20,
                avg(close) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS sma_50,
                avg(close) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS sma_200,
                max(close) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS rolling_252d_high,
                min(close) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS rolling_252d_low,
                avg(dollar_volume) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                ) AS avg_dollar_volume_21d,
                avg(dollar_volume) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
                ) AS avg_dollar_volume_63d
            FROM prices
            WINDOW w AS (PARTITION BY symbol ORDER BY date)
        ),
        daily_returns AS (
            SELECT
                *,
                CASE
                    WHEN return_price IS NULL OR lag_price_1 IS NULL OR lag_price_1 = 0 THEN NULL
                    ELSE (return_price / lag_price_1) - 1
                END AS returns_1d
            FROM returns_base
        ),
        with_volatility AS (
            SELECT
                *,
                stddev_samp(returns_1d) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                ) * sqrt(252.0) AS realized_vol_21d,
                stddev_samp(returns_1d) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
                ) * sqrt(252.0) AS realized_vol_63d
            FROM daily_returns
        )
        SELECT
            date,
            symbol,
            close,
            adjusted_close,
            returns_1d,
            CASE
                WHEN return_price IS NULL OR lag_price_5 IS NULL OR lag_price_5 = 0 THEN NULL
                ELSE (return_price / lag_price_5) - 1
            END AS returns_5d,
            CASE
                WHEN return_price IS NULL OR lag_price_21 IS NULL OR lag_price_21 = 0 THEN NULL
                ELSE (return_price / lag_price_21) - 1
            END AS returns_21d,
            CASE
                WHEN return_price IS NULL OR lag_price_63 IS NULL OR lag_price_63 = 0 THEN NULL
                ELSE (return_price / lag_price_63) - 1
            END AS returns_63d,
            CASE
                WHEN return_price IS NULL OR lag_price_126 IS NULL OR lag_price_126 = 0 THEN NULL
                ELSE (return_price / lag_price_126) - 1
            END AS returns_126d,
            CASE
                WHEN return_price IS NULL OR lag_price_252 IS NULL OR lag_price_252 = 0 THEN NULL
                ELSE (return_price / lag_price_252) - 1
            END AS returns_252d,
            CASE
                WHEN momentum_end_price IS NULL
                     OR momentum_start_price IS NULL
                     OR momentum_start_price = 0 THEN NULL
                ELSE (momentum_end_price / momentum_start_price) - 1
            END AS momentum_12_1,
            sma_20,
            sma_50,
            sma_200,
            CASE
                WHEN close IS NULL OR sma_50 IS NULL OR sma_50 = 0 THEN NULL
                ELSE close / sma_50
            END AS price_to_sma_50,
            CASE
                WHEN close IS NULL OR sma_200 IS NULL OR sma_200 = 0 THEN NULL
                ELSE close / sma_200
            END AS price_to_sma_200,
            CASE
                WHEN sma_50 IS NULL OR sma_200 IS NULL OR sma_200 = 0 THEN NULL
                ELSE sma_50 / sma_200
            END AS sma_50_to_200,
            realized_vol_21d,
            realized_vol_63d,
            CASE
                WHEN close IS NULL OR rolling_252d_high IS NULL OR rolling_252d_high = 0 THEN NULL
                ELSE (close / rolling_252d_high) - 1
            END AS drawdown_from_252d_high,
            rolling_252d_high,
            rolling_252d_low,
            avg_dollar_volume_21d,
            avg_dollar_volume_63d,
            ? AS signal_version,
            current_timestamp AS load_timestamp
        FROM with_volatility
    """


@asset(
    name="signals_daily",
    key_prefix=["silver"],
    deps=[silver_research_daily_prices],
    required_resource_keys={"research_duckdb"},
)
def silver_signals_daily(context: AssetExecutionContext) -> None:
    """
    Build reusable daily research signals from silver.research_daily_prices.
    """
    con = context.resources.research_duckdb

    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET threads={DUCKDB_SIGNALS_THREADS}")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    prices_glob = (
        DATA_ROOT / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet"
    ).as_posix()
    select_sql = _signals_select_sql()

    con.execute("DROP TABLE IF EXISTS silver.signals_daily")
    con.execute(
        f"""
        CREATE TABLE silver.signals_daily AS
        SELECT *
        FROM ({select_sql}) AS seeded
        WHERE 1 = 0
        """,
        [prices_glob, SIGNALS_SYMBOL_BUCKETS, 0, SIGNAL_VERSION],
    )
    for bucket_index in range(SIGNALS_SYMBOL_BUCKETS):
        context.log.info(
            "Building silver.signals_daily bucket %s/%s",
            bucket_index + 1,
            SIGNALS_SYMBOL_BUCKETS,
        )
        con.execute(
            f"""
            INSERT INTO silver.signals_daily
            SELECT *
            FROM ({select_sql}) AS bucketed
            """,
            [prices_glob, SIGNALS_SYMBOL_BUCKETS, bucket_index, SIGNAL_VERSION],
        )

    row_count = con.execute("SELECT count(*) FROM silver.signals_daily").fetchone()[0]
    symbol_count = con.execute(
        "SELECT count(DISTINCT symbol) FROM silver.signals_daily"
    ).fetchone()[0]
    min_max_row = con.execute("SELECT min(date), max(date) FROM silver.signals_daily").fetchone()

    context.add_output_metadata(
        {
            "table": "silver.signals_daily",
            "row_count": int(row_count or 0),
            "symbol_count": int(symbol_count or 0),
            "min_date": str(min_max_row[0]) if min_max_row and min_max_row[0] else None,
            "max_date": str(min_max_row[1]) if min_max_row and min_max_row[1] else None,
            "signal_version": SIGNAL_VERSION,
        }
    )
