import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dagster import (
    Array,
    AssetExecutionContext,
    AssetKey,
    DailyPartitionsDefinition,
    Field,
    MonthlyPartitionsDefinition,
    String,
    asset,
)


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
BRONZE_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
BRONZE_MONTHLY_BACKFILL_PARTITIONS = MonthlyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
TICKERS_ENV = "ALPACA_TICKERS"
ALPACA_SYMBOL_BATCH_SIZE = int(os.getenv("ALPACA_SYMBOL_BATCH_SIZE", "200"))
ALPACA_REQUEST_SLEEP_SECONDS = float(os.getenv("ALPACA_REQUEST_SLEEP_SECONDS", "0.25"))
ALPACA_REQUEST_MAX_RETRIES = int(os.getenv("ALPACA_REQUEST_MAX_RETRIES", "4"))
ALPACA_REQUEST_RETRY_BASE_SECONDS = float(os.getenv("ALPACA_REQUEST_RETRY_BASE_SECONDS", "1.0"))


def _chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = len(values) or 1
    return [values[i : i + size] for i in range(0, len(values), size)]


def _normalize_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    normalized = df.copy()
    if "symbol" not in normalized.columns:
        normalized = normalized.reset_index()
    if "timestamp" not in normalized.columns and normalized.index.name == "timestamp":
        normalized = normalized.reset_index()
    if "symbol" not in normalized.columns:
        return pd.DataFrame()
    normalized["symbol"] = normalized["symbol"].astype(str).str.upper()
    normalized["ingested_ts"] = datetime.now(timezone.utc)
    return normalized


def _resolve_active_symbols(context: AssetExecutionContext) -> list[str]:
    env_symbols = [
        s.strip().upper() for s in os.getenv(TICKERS_ENV, "").split(",") if s.strip()
    ]
    config_symbols = context.op_config.get("symbols", None)
    active_symbols: list[str] = []
    try:
        active_symbols = [
            row[0]
            for row in context.resources.duckdb.execute(
                "SELECT symbol FROM silver.assets WHERE is_active = TRUE"
            ).fetchall()
            if row and row[0]
        ]
    except Exception as exc:
        context.log.warning("Unable to read silver.assets for active symbols: %s", exc)

    if active_symbols:
        return sorted({str(symbol).upper() for symbol in active_symbols})
    if env_symbols:
        if config_symbols:
            intersect = sorted({s.upper() for s in config_symbols} & set(env_symbols))
            if intersect:
                return intersect
        return sorted(set(env_symbols))
    if config_symbols:
        return sorted({str(symbol).upper() for symbol in config_symbols if symbol})
    return ["AAPL"]


def _fetch_bars_df_with_retry(
    context: AssetExecutionContext,
    symbols: list[str],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    for attempt in range(1, ALPACA_REQUEST_MAX_RETRIES + 2):
        try:
            return context.resources.alpaca.get_bars_df(
                symbol_or_symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            if attempt > ALPACA_REQUEST_MAX_RETRIES:
                raise
            sleep_seconds = ALPACA_REQUEST_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            context.log.warning(
                "Alpaca bars request failed for %s symbols (%s). retry=%s sleep=%.2fs",
                len(symbols),
                exc,
                attempt,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
    return pd.DataFrame()


def _write_bronze_day_symbol_files(
    context: AssetExecutionContext,
    partition_key: str,
    day_df: pd.DataFrame,
) -> tuple[int, int]:
    if day_df is None or day_df.empty:
        return 0, 0

    rows_written = 0
    files_written = 0
    for symbol, symbol_df in day_df.groupby("symbol", sort=True):
        out_path = (
            DATA_ROOT
            / "bronze"
            / "alpaca_bars"
            / f"date={partition_key}"
            / f"symbol={symbol}"
            / "bars.parquet"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        symbol_df.to_parquet(out_path, index=False)
        rows_written += len(symbol_df)
        files_written += 1
    return rows_written, files_written


def _ingest_bronze_day(
    context: AssetExecutionContext,
    partition_date: datetime.date,
    symbols: list[str],
) -> tuple[int, int]:
    partition_key = partition_date.strftime("%Y-%m-%d")
    start_date = datetime.combine(partition_date, datetime.min.time(), tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)

    frames: list[pd.DataFrame] = []
    for symbol_batch in _chunked(symbols, ALPACA_SYMBOL_BATCH_SIZE):
        batch_df = _fetch_bars_df_with_retry(context, symbol_batch, start_date, end_date)
        normalized = _normalize_bars_df(batch_df)
        if not normalized.empty:
            frames.append(normalized)
        if ALPACA_REQUEST_SLEEP_SECONDS > 0:
            time.sleep(ALPACA_REQUEST_SLEEP_SECONDS)

    if not frames:
        context.log.warning("No bar data returned for partition %s", partition_key)
        return 0, 0

    merged = pd.concat(frames, ignore_index=True)
    return _write_bronze_day_symbol_files(context, partition_key, merged)


@asset(
    name="bronze_alpaca_bars",
    partitions_def=BRONZE_PARTITIONS,
    required_resource_keys={"alpaca", "duckdb"},
    deps=[AssetKey("silver_alpaca_assets")],
    config_schema={
        "symbols": Field(Array(String), is_required=False),
    },
)
def bronze_alpaca_bars(context: AssetExecutionContext) -> None:
    """
    Write 5-minute Alpaca bar data to bronze partitioned by day and symbol.
    """
    symbols = _resolve_active_symbols(context)
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    row_count, files_written = _ingest_bronze_day(context, partition_date, symbols)
    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "symbol_count": len(symbols),
            "files_written": files_written,
            "row_count": row_count,
        }
    )


@asset(
    name="bronze_alpaca_bars_monthly_backfill",
    partitions_def=BRONZE_MONTHLY_BACKFILL_PARTITIONS,
    required_resource_keys={"alpaca", "duckdb"},
    deps=[AssetKey("silver_alpaca_assets")],
    config_schema={
        "symbols": Field(Array(String), is_required=False),
    },
)
def bronze_alpaca_bars_monthly_backfill(context: AssetExecutionContext) -> None:
    """
    Backfill all days in a month into bronze day+symbol partitions.
    """
    month_start = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    symbols = _resolve_active_symbols(context)

    total_rows = 0
    total_files = 0
    total_days = 0
    cursor = month_start
    while cursor < next_month:
        rows, files = _ingest_bronze_day(context, cursor, symbols)
        total_rows += rows
        total_files += files
        total_days += 1
        cursor += timedelta(days=1)

    context.add_output_metadata(
        {
            "partition_month_start": context.partition_key,
            "symbol_count": len(symbols),
            "calendar_days_processed": total_days,
            "files_written": total_files,
            "row_count": total_rows,
        }
    )


@asset(
    name="bronze_alpaca_assets",
    required_resource_keys={"alpaca"},
)
def bronze_alpaca_assets(context: AssetExecutionContext) -> None:
    """
    Write Alpaca asset (ticker universe) snapshot to a reference parquet file.
    """
    df = context.resources.alpaca.get_assets_df()
    if df is None or df.empty:
        context.log.warning("No asset data returned.")
        return

    # Coerce UUID objects to strings for parquet compatibility.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, uuid.UUID)).any():
            df[col] = df[col].astype(str)

    df["ingested_ts"] = datetime.now(timezone.utc)
    reference_dir = DATA_ROOT / "bronze" / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    out_path = reference_dir / "alpaca_assets.parquet"
    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df)}
    )
