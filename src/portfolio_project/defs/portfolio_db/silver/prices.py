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


def _silver_day_file_path(trade_date: date, symbol: str) -> Path:
    return (
        DATA_ROOT
        / "silver"
        / "prices"
        / f"date={trade_date.isoformat()}"
        / f"symbol={symbol.upper()}"
        / "prices.parquet"
    )


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


def _normalize_silver_columns(prices_df: pd.DataFrame) -> pd.DataFrame:
    column_order = [
        "asset_id",
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "ingested_ts",
    ]
    existing_cols = [col for col in column_order if col in prices_df.columns]
    remaining_cols = [col for col in prices_df.columns if col not in existing_cols]
    return prices_df[existing_cols + remaining_cols]


def _query_silver_prices_for_day(
    con,
    parquet_paths: list[str],
    trade_date: date,
) -> pd.DataFrame:
    if not parquet_paths:
        return pd.DataFrame()
    start_dt = datetime.combine(trade_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    return con.execute(
        """
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
        """,
        [parquet_paths, start_dt, end_dt],
    ).fetch_df()


def _clear_silver_day_partition(trade_date: date) -> None:
    day_dir = DATA_ROOT / "silver" / "prices" / f"date={trade_date.isoformat()}"
    if day_dir.exists():
        shutil.rmtree(day_dir)


def _write_silver_day_symbol_files(prices_df: pd.DataFrame) -> tuple[int, int]:
    if prices_df is None or prices_df.empty:
        return 0, 0
    prices_df = _normalize_silver_columns(prices_df)
    if "timestamp" not in prices_df.columns or "symbol" not in prices_df.columns:
        return 0, 0

    timestamp_values = pd.to_datetime(prices_df["timestamp"], utc=True, errors="coerce")
    prices_df = prices_df[timestamp_values.notna()].copy()
    prices_df["trade_date"] = timestamp_values[timestamp_values.notna()].dt.date

    rows_written = 0
    files_written = 0
    for (symbol, trade_date), group in prices_df.groupby(["symbol", "trade_date"], sort=True):
        out_path = _silver_day_file_path(trade_date, str(symbol))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = group.drop(columns=["trade_date"])
        payload.to_parquet(out_path, index=False)
        rows_written += len(payload)
        files_written += 1
    return rows_written, files_written


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

    prices_df = _query_silver_prices_for_day(con, bronze_paths, trade_date)
    if prices_df is None or prices_df.empty:
        context.log.warning("No active bar data for partition %s.", context.partition_key)
        return

    _clear_silver_day_partition(trade_date)
    row_count, files_written = _write_silver_day_symbol_files(prices_df)
    context.add_output_metadata(
        {
            "row_count": row_count,
            "partition": context.partition_key,
            "files_written": files_written,
            "active_symbol_count": len(active_symbols),
        }
    )



