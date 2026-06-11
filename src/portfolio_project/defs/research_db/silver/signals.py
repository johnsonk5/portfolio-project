import os
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError

from portfolio_project.defs.research_db.dq_checks import log_required_field_null_check
from portfolio_project.defs.research_db.silver.research_prices import (
    silver_research_daily_prices,
)
from portfolio_project.defs.research_db.trading_calendar import create_valid_trading_dates_table

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
SIGNAL_VERSION = os.getenv("RESEARCH_SIGNALS_VERSION", "v1")
DUCKDB_SIGNALS_THREADS = int(os.getenv("RESEARCH_SIGNALS_DUCKDB_THREADS", "2"))
SIGNALS_SYMBOL_BUCKETS = int(os.getenv("RESEARCH_SIGNALS_SYMBOL_BUCKETS", "32"))


def _safe_partition_key(context: AssetExecutionContext) -> str | None:
    try:
        return context.partition_key
    except Exception:
        return None


def _parquet_has_column(con, parquet_glob: str, column: str) -> bool:
    try:
        rows = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name = true)",
            [parquet_glob],
        ).fetchall()
    except Exception:
        return False
    return column in {str(row[0]) for row in rows}


def _signals_select_sql(*, has_asset_id: bool) -> str:
    asset_id_sql = "CAST(p.asset_id AS BIGINT)" if has_asset_id else "NULL::BIGINT"
    return """
        WITH prices AS (
            SELECT
                {asset_id_sql} AS asset_id,
                CAST(p.trade_date AS DATE) AS date,
                upper(trim(p.symbol)) AS symbol,
                CAST(p.close AS DOUBLE) AS close,
                COALESCE(
                    CAST(p.adjusted_close AS DOUBLE),
                    CAST(p.close AS DOUBLE)
                ) AS adjusted_close,
                CAST(p.volume AS BIGINT) AS volume,
                CAST(p.dollar_volume AS DOUBLE) AS dollar_volume,
                COALESCE(CAST(p.adjusted_close AS DOUBLE), CAST(p.close AS DOUBLE)) AS return_price
            FROM read_parquet(?, union_by_name = true) AS p
            INNER JOIN valid_research_trading_dates AS trading_dates
                ON trading_dates.trade_date = CAST(p.trade_date AS DATE)
            WHERE p.trade_date IS NOT NULL
              AND p.symbol IS NOT NULL
              AND trim(p.symbol) <> ''
              AND abs(hash(upper(trim(p.symbol)))) % ? = ?
        ),
        returns_base AS (
            SELECT
                asset_id,
                date,
                symbol,
                close,
                adjusted_close,
                volume,
                dollar_volume,
                return_price,
                lag(return_price, 1) OVER w AS lag_price_1,
                lag(return_price, 5) OVER w AS lag_price_5,
                lag(return_price, 10) OVER w AS lag_price_10,
                lag(return_price, 21) OVER w AS lag_price_21,
                lag(return_price, 63) OVER w AS lag_price_63,
                lag(return_price, 126) OVER w AS lag_price_126,
                lag(return_price, 252) OVER w AS lag_price_252,
                lag(return_price, 21) OVER w AS momentum_end_price,
                lag(return_price, 252) OVER w AS momentum_start_price,
                avg(close) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS sma_20,
                avg(close) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS sma_50,
                avg(close) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS sma_200,
                max(close) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS rolling_252d_high,
                min(close) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS rolling_252d_low,
                avg(dollar_volume) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                ) AS avg_dollar_volume_21d,
                avg(dollar_volume) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
                ) AS avg_dollar_volume_63d
            FROM prices
            WINDOW w AS (PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol) ORDER BY date)
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
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                ) * sqrt(252.0) AS realized_vol_21d,
                stddev_samp(returns_1d) OVER (
                    PARTITION BY coalesce(CAST(asset_id AS VARCHAR), symbol)
                    ORDER BY date
                    ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
                ) * sqrt(252.0) AS realized_vol_63d
            FROM daily_returns
        )
        SELECT
            date,
            asset_id,
            symbol,
            close,
            adjusted_close,
            returns_1d,
            CASE
                WHEN return_price IS NULL OR lag_price_5 IS NULL OR lag_price_5 = 0 THEN NULL
                ELSE (return_price / lag_price_5) - 1
            END AS returns_5d,
            CASE
                WHEN return_price IS NULL OR lag_price_10 IS NULL OR lag_price_10 = 0 THEN NULL
                ELSE (return_price / lag_price_10) - 1
            END AS returns_10d,
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
            CASE
                WHEN close IS NULL OR rolling_252d_high IS NULL OR rolling_252d_high = 0 THEN NULL
                ELSE (rolling_252d_high - close) / rolling_252d_high
            END AS pct_below_52w_high,
            rolling_252d_high,
            rolling_252d_low,
            avg_dollar_volume_21d,
            avg_dollar_volume_63d,
            ? AS signal_version,
            current_timestamp AS load_timestamp
        FROM with_volatility
    """.format(asset_id_sql=asset_id_sql)


@asset(
    name="signals_daily",
    key_prefix=["silver"],
    deps=[silver_research_daily_prices],
    required_resource_keys={"research_duckdb", "duckdb"},
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
    select_sql = _signals_select_sql(
        has_asset_id=_parquet_has_column(con, prices_glob, "asset_id")
    )

    create_valid_trading_dates_table(con, prices_glob)
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

    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None
    partition_key = _safe_partition_key(context)

    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_signals_daily_required_fields_nulls",
        relation_sql="SELECT * FROM silver.signals_daily",
        relation_params=[],
        required_columns=[
            "date",
            "symbol",
            "close",
            "adjusted_close",
            "rolling_252d_high",
            "rolling_252d_low",
            "avg_dollar_volume_21d",
            "avg_dollar_volume_63d",
            "signal_version",
            "load_timestamp",
        ],
        details={"table": "silver.signals_daily"},
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
    )

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
