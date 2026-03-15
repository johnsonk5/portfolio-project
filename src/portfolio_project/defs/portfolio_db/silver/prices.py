import os
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    asset,
)

from portfolio_project.defs.portfolio_db.bronze.alpaca import (
    bronze_alpaca_bars,
)
from portfolio_project.defs.portfolio_db.silver.assets import silver_alpaca_assets


DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
SILVER_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)


def _silver_day_partition_path(trade_date: date) -> Path:
    return DATA_ROOT / "silver" / "prices" / f"date={trade_date.isoformat()}"


def _active_symbol_rows(con) -> list[tuple[int, str]]:
    rows = con.execute(
        """
        SELECT min(asset_id) AS asset_id, upper(trim(symbol)) AS symbol
        FROM silver.assets
        WHERE is_active = TRUE
          AND symbol IS NOT NULL
          AND trim(symbol) <> ''
        GROUP BY upper(trim(symbol))
        """
    ).fetchall()
    return [(int(row[0]), str(row[1])) for row in rows if row and row[0] is not None and row[1]]


def _silver_prices_sql() -> str:
    return """
        WITH bars AS (
            SELECT *
            FROM read_parquet(?)
            WHERE timestamp >= ? AND timestamp < ?
        ),
        active_assets AS (
            SELECT min(asset_id) AS asset_id, upper(trim(symbol)) AS symbol
            FROM silver.assets
            WHERE is_active = TRUE
              AND symbol IS NOT NULL
              AND trim(symbol) <> ''
            GROUP BY upper(trim(symbol))
        )
        SELECT
            assets.asset_id,
            bars.symbol,
            bars.timestamp,
            bars.open,
            bars.high,
            bars.low,
            bars.close,
            bars.volume,
            bars.trade_count,
            bars.vwap,
            bars.ingested_ts
        FROM bars
        INNER JOIN active_assets AS assets
            ON upper(trim(bars.symbol)) = assets.symbol
    """


def _query_silver_prices_for_day(
    con,
    parquet_paths: list[str],
    trade_date: date,
) -> pd.DataFrame:
    if not parquet_paths:
        return pd.DataFrame()
    start_dt = datetime.combine(trade_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    return con.execute(_silver_prices_sql(), [parquet_paths, start_dt, end_dt]).fetch_df()


def _clear_silver_day_partition(trade_date: date) -> None:
    day_dir = _silver_day_partition_path(trade_date)
    if day_dir.exists():
        shutil.rmtree(day_dir)


def _write_silver_day_symbol_files(
    con,
    parquet_paths: list[str],
    trade_date: date,
) -> tuple[int, int]:
    if not parquet_paths:
        return 0, 0

    start_dt = datetime.combine(trade_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    sql = _silver_prices_sql()
    row_count, files_written = con.execute(
        f"""
        SELECT
            count(*) AS row_count,
            count(DISTINCT symbol) AS files_written
        FROM ({sql}) AS silver_prices
        """,
        [parquet_paths, start_dt, end_dt],
    ).fetchone()
    if not row_count:
        return 0, 0

    out_dir = _silver_day_partition_path(trade_date)
    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            {sql}
        ) TO '{out_dir.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (symbol), FILENAME_PATTERN 'prices', OVERWRITE_OR_IGNORE TRUE)
        """,
        [parquet_paths, start_dt, end_dt],
    )
    return int(row_count), int(files_written)


def _bronze_day_symbol_paths(trade_date: date, symbols: list[str]) -> list[str]:
    base = DATA_ROOT / "bronze" / "alpaca_bars" / f"date={trade_date.isoformat()}"
    paths = [(base / f"symbol={symbol}" / "bars.parquet").as_posix() for symbol in symbols]
    return [path for path in paths if Path(path).exists()]


@asset(
    name="silver_alpaca_prices_parquet",
    deps=[bronze_alpaca_bars, silver_alpaca_assets],
    partitions_def=SILVER_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def silver_alpaca_prices_parquet(context: AssetExecutionContext) -> None:
    """
    Build silver prices partitioned by day and symbol from daily bronze input.
    """
    trade_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    con = context.resources.duckdb
    try:
        con.execute("SELECT 1 FROM silver.assets LIMIT 1")
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
        return

    active_symbol_rows = _active_symbol_rows(con)
    if not active_symbol_rows:
        context.log.warning("No active symbols found in silver.assets.")
        return

    active_symbols = [row[1] for row in active_symbol_rows]
    bronze_paths = _bronze_day_symbol_paths(trade_date, active_symbols)
    if not bronze_paths:
        context.log.warning("No bronze bars parquet files found for %s", context.partition_key)
        return

    _clear_silver_day_partition(trade_date)
    row_count, files_written = _write_silver_day_symbol_files(con, bronze_paths, trade_date)
    if row_count == 0:
        context.log.warning("No active bar data for partition %s.", context.partition_key)
        return

    context.add_output_metadata(
        {
            "row_count": row_count,
            "partition": context.partition_key,
            "files_written": files_written,
            "active_symbol_count": len(active_symbols),
        }
    )
