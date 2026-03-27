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
    String,
    asset,
)

from portfolio_project.defs.portfolio_db.observability.observability_modules import write_dq_log

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
BRONZE_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
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
    normalized = normalized.reset_index()
    if "symbol" not in normalized.columns:
        return pd.DataFrame()
    normalized["symbol"] = normalized["symbol"].astype(str).str.upper()
    normalized["ingested_ts"] = datetime.now(timezone.utc)
    return normalized


def _resolve_active_symbols(context: AssetExecutionContext) -> list[str]:
    # Pull fallback symbols
    env_symbols = [s.strip().upper() for s in os.getenv(TICKERS_ENV, "").split(",") if s.strip()]
    config_symbols_raw = (getattr(context, "op_config", None) or {}).get("symbols")
    config_symbols_set: set[str] = set()
    for symbol in config_symbols_raw or []:
        if symbol is None:
            continue
        symbol_value = str(symbol).strip().upper()
        if symbol_value:
            config_symbols_set.add(symbol_value)
    config_symbols = sorted(config_symbols_set)

    # Pull active symbols from db
    active_symbols: list[str] = []
    silver_assets_issue_reason: str | None = None
    silver_assets_error: str | None = None
    try:
        active_symbols = [
            row[0]
            for row in context.resources.duckdb.execute(
                "SELECT symbol FROM silver.assets WHERE is_active = TRUE"
            ).fetchall()
            if row and row[0]
        ]
        if not active_symbols:
            silver_assets_issue_reason = "silver_assets_empty"
            context.log.warning(
                "silver.assets returned no active symbols; using fallback resolution."
            )
    except Exception as exc:
        silver_assets_issue_reason = "silver_assets_read_failed"
        silver_assets_error = str(exc)
        context.log.warning("Unable to read silver.assets for active symbols: %s", exc)

    # If there is one active symbol, return that
    if active_symbols:
        return sorted({str(symbol).upper() for symbol in active_symbols})

    # Set default symbols
    fallback_source = "default"
    resolved_symbols = ["AAPL"]

    # If there are either config or env symbols use those (config first)
    if config_symbols:
        resolved_symbols = config_symbols
        fallback_source = "config"
    elif env_symbols:
        resolved_symbols = sorted(set(env_symbols))
        fallback_source = "env"

    # Log warning if active symbols wasn't used
    if silver_assets_issue_reason is not None:
        write_dq_log(
            context=context,
            check_name="dq_active_symbols_source_silver_assets_readable",
            severity="YELLOW",
            status="WARN",
            measured_value=1.0,
            threshold_value=0.0,
            details={
                "reason": silver_assets_issue_reason,
                "fallback_source": fallback_source,
            },
            error_name=silver_assets_issue_reason,
            error_message=silver_assets_error,
        )
    return resolved_symbols


def _fetch_bars_df_with_retry(
    context: AssetExecutionContext,
    symbols: list[str],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    max_attempts = ALPACA_REQUEST_MAX_RETRIES + 1
    for attempt in range(1, max_attempts + 1):
        try:
            return context.resources.alpaca.get_bars_df(
                symbol_or_symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            if attempt == max_attempts:
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
    symbol_batches = _chunked(symbols, ALPACA_SYMBOL_BATCH_SIZE)
    for idx, symbol_batch in enumerate(symbol_batches):
        batch_df = _fetch_bars_df_with_retry(context, symbol_batch, start_date, end_date)
        normalized = _normalize_bars_df(batch_df)
        if not normalized.empty:
            frames.append(normalized)
        if ALPACA_REQUEST_SLEEP_SECONDS > 0 and idx < len(symbol_batches) - 1:
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

    context.add_output_metadata({"path": str(out_path), "row_count": len(df)})
