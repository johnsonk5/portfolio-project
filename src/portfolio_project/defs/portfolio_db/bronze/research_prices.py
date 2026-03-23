import os
import re
import shutil
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dagster import (
    Array,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Field,
    Float,
    Int,
    String,
    asset,
)

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
RESEARCH_EODHD_PRICES_PARTITIONS_START_DATE = os.getenv(
    "RESEARCH_EODHD_PRICES_PARTITIONS_START_DATE", "2000-01-01"
)
RESEARCH_ALPACA_PRICES_PARTITIONS_START_DATE = os.getenv(
    "RESEARCH_ALPACA_PRICES_PARTITIONS_START_DATE", "2016-01-01"
)
RESEARCH_PRICES_PARTITIONS_START_DATE = os.getenv(
    "RESEARCH_PRICES_PARTITIONS_START_DATE",
    RESEARCH_EODHD_PRICES_PARTITIONS_START_DATE,
)
EODHD_PRICES_PARTITIONS = DailyPartitionsDefinition(
    start_date=RESEARCH_PRICES_PARTITIONS_START_DATE
)
ALPACA_PRICES_PARTITIONS = DailyPartitionsDefinition(
    start_date=RESEARCH_PRICES_PARTITIONS_START_DATE
)
DEFAULT_EXCEPTION_SYMBOLS = ("SPY",)
ALPACA_BATCH_SIZE = int(os.getenv("RESEARCH_ALPACA_SYMBOL_BATCH_SIZE", "200"))
ALPACA_REQUEST_SLEEP_SECONDS = float(os.getenv("RESEARCH_ALPACA_REQUEST_SLEEP_SECONDS", "0.25"))
ALPACA_REQUEST_MAX_RETRIES = int(os.getenv("RESEARCH_ALPACA_REQUEST_MAX_RETRIES", "4"))
ALPACA_REQUEST_RETRY_BASE_SECONDS = float(
    os.getenv("RESEARCH_ALPACA_REQUEST_RETRY_BASE_SECONDS", "1.0")
)

_COMMON_STOCK_EXCLUSION_RE = re.compile(
    r"\b("
    r"ETF|ETN|ETP|FUND|TRUST|PREFERRED|PREF|DEPOSITARY|ADR|ADS|"
    r"WARRANT|RIGHT|UNIT|INCOME SHARES|BENEFICIAL INTEREST|SPAC"
    r")\b",
    flags=re.IGNORECASE,
)
_OTC_EXCHANGES = {"OTC", "OTCQB", "OTCQX", "PINK", "GREY"}


def _chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = len(values) or 1
    return [values[i : i + size] for i in range(0, len(values), size)]


def _normalize_symbols(symbols: list[str] | None) -> list[str]:
    if not symbols:
        return []
    return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})


def _force_include_exception_symbols(symbols: list[str]) -> list[str]:
    symbol_set = {symbol for symbol in symbols if symbol}
    for symbol in DEFAULT_EXCEPTION_SYMBOLS:
        symbol_set.add(symbol)
    return sorted(symbol_set)


def _apply_max_symbol_limit(symbols: list[str], max_symbols: int | None) -> list[str]:
    if max_symbols is None or max_symbols <= 0 or len(symbols) <= max_symbols:
        return symbols

    if "SPY" in symbols and max_symbols >= 1:
        limited_symbols = [symbol for symbol in symbols if symbol != "SPY"][: max_symbols - 1]
        limited_symbols.append("SPY")
        return sorted(limited_symbols)

    return symbols[:max_symbols]


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _is_probable_common_equity(asset_row: pd.Series) -> bool:
    asset_class = str(asset_row.get("asset_class", "")).strip().lower()
    if asset_class != "us_equity":
        return False

    if not _coerce_bool(asset_row.get("tradable")):
        return False

    exchange = str(asset_row.get("exchange", "")).strip().upper()
    if exchange in _OTC_EXCHANGES:
        return False

    status = str(asset_row.get("status", "")).strip().lower()
    if status and status != "active":
        return False

    searchable_fields = [
        str(asset_row.get("symbol", "")).strip(),
        str(asset_row.get("name", "")).strip(),
    ]
    attributes = asset_row.get("attributes")
    if isinstance(attributes, (list, tuple, set)):
        searchable_fields.extend(str(value) for value in attributes if value is not None)
    elif hasattr(attributes, "tolist"):
        attribute_values = attributes.tolist()
        if isinstance(attribute_values, list):
            searchable_fields.extend(str(value) for value in attribute_values if value is not None)
        elif attribute_values is not None and not pd.isna(attribute_values):
            searchable_fields.append(str(attribute_values))
    elif attributes is not None and not pd.isna(attributes):
        searchable_fields.append(str(attributes))

    searchable_text = " ".join(part for part in searchable_fields if part)
    if searchable_text and _COMMON_STOCK_EXCLUSION_RE.search(searchable_text):
        return False

    return True


def _resolve_alpaca_symbols(
    context: AssetExecutionContext,
    configured_symbols: list[str] | None = None,
    max_symbols: int | None = None,
) -> list[str]:
    configured = _normalize_symbols(configured_symbols)
    if configured:
        configured = _force_include_exception_symbols(configured)
        return _apply_max_symbol_limit(configured, max_symbols=max_symbols)

    assets_df = context.resources.alpaca.get_assets_df()
    if assets_df is None or assets_df.empty:
        context.log.warning("Alpaca asset universe returned no rows.")
        return _apply_max_symbol_limit(
            _force_include_exception_symbols([]),
            max_symbols=max_symbols,
        )

    symbols = [
        str(row["symbol"]).strip().upper()
        for _, row in assets_df.iterrows()
        if row.get("symbol") and _is_probable_common_equity(row)
    ]
    symbols = _force_include_exception_symbols(sorted(set(symbols)))
    return _apply_max_symbol_limit(symbols, max_symbols=max_symbols)


def _normalize_alpaca_daily_bars_df(
    df: pd.DataFrame,
    partition_date: date,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy().reset_index()
    if "symbol" not in normalized.columns:
        return pd.DataFrame()

    rename_map = {"level_0": "symbol", "level_1": "timestamp"}
    normalized = normalized.rename(columns=rename_map)
    if "timestamp" not in normalized.columns:
        return pd.DataFrame()

    normalized["symbol"] = normalized["symbol"].astype(str).str.upper()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True, errors="coerce")
    normalized = normalized[normalized["timestamp"].notna()].copy()
    normalized["trade_date"] = normalized["timestamp"].dt.tz_convert("America/New_York").dt.date
    normalized = normalized[normalized["trade_date"] == partition_date].copy()
    if normalized.empty:
        return pd.DataFrame()

    if "adjusted_close" not in normalized.columns:
        normalized["adjusted_close"] = pd.NA
    for optional_col in ["trade_count", "vwap"]:
        if optional_col not in normalized.columns:
            normalized[optional_col] = pd.NA

    normalized["source"] = "alpaca"
    normalized["ingested_ts"] = datetime.now(timezone.utc)
    ordered_columns = [
        "symbol",
        "timestamp",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "trade_count",
        "vwap",
        "source",
        "ingested_ts",
    ]
    existing_columns = [column for column in ordered_columns if column in normalized.columns]
    return normalized[existing_columns].sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _normalize_eodhd_daily_bars_df(
    df: pd.DataFrame,
    partition_date: date,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy()
    symbol_col = None
    for candidate in ["code", "symbol"]:
        if candidate in normalized.columns:
            symbol_col = candidate
            break
    if symbol_col is None:
        return pd.DataFrame()

    date_col = (
        "date"
        if "date" in normalized.columns
        else "trade_date" if "trade_date" in normalized.columns else None
    )
    if date_col is None:
        return pd.DataFrame()

    normalized["symbol"] = normalized[symbol_col].astype(str).str.strip().str.upper()
    normalized["trade_date"] = pd.to_datetime(normalized[date_col], errors="coerce").dt.date
    normalized = normalized[normalized["trade_date"] == partition_date].copy()
    normalized = normalized[normalized["symbol"].ne("")].copy()
    if normalized.empty:
        return pd.DataFrame()

    normalized["timestamp"] = pd.to_datetime(normalized["trade_date"], utc=True) + pd.Timedelta(
        hours=21
    )
    if "adjusted_close" not in normalized.columns:
        normalized["adjusted_close"] = pd.NA
    for optional_col in ["trade_count", "vwap"]:
        normalized[optional_col] = pd.NA

    normalized["source"] = "eodhd"
    normalized["ingested_ts"] = datetime.now(timezone.utc)
    ordered_columns = [
        "symbol",
        "timestamp",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "trade_count",
        "vwap",
        "source",
        "ingested_ts",
    ]
    existing_columns = [column for column in ordered_columns if column in normalized.columns]
    return normalized[existing_columns].sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _filter_day_df_to_symbols(
    day_df: pd.DataFrame,
    configured_symbols: list[str] | None = None,
    max_symbols: int | None = None,
) -> pd.DataFrame:
    if day_df is None or day_df.empty:
        return pd.DataFrame()

    configured = _normalize_symbols(configured_symbols)
    if configured:
        allowed_symbols = set(_force_include_exception_symbols(configured))
        filtered = day_df[day_df["symbol"].isin(allowed_symbols)].copy()
    else:
        filtered = day_df.copy()

    if filtered.empty:
        return filtered

    if max_symbols is None or max_symbols <= 0:
        return filtered.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    available_symbols = sorted(filtered["symbol"].dropna().astype(str).unique())
    limited_symbols = set(_apply_max_symbol_limit(available_symbols, max_symbols))
    filtered = filtered[filtered["symbol"].isin(limited_symbols)].copy()
    return filtered.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _fetch_alpaca_daily_bars_with_retry(
    context: AssetExecutionContext,
    symbols: list[str],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    max_attempts = ALPACA_REQUEST_MAX_RETRIES + 1
    for attempt in range(1, max_attempts + 1):
        try:
            return context.resources.alpaca.get_daily_bars_df(
                symbol_or_symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            if attempt == max_attempts:
                raise
            sleep_seconds = ALPACA_REQUEST_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            context.log.warning(
                "Alpaca research bars failed for %s symbols (%s). retry=%s sleep=%.2fs",
                len(symbols),
                exc,
                attempt,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
    return pd.DataFrame()


def _fetch_alpaca_daily_bars_for_day(
    context: AssetExecutionContext,
    partition_date: date,
    symbols: list[str],
    batch_size: int,
    request_sleep_seconds: float,
) -> pd.DataFrame:
    start_dt = datetime.combine(partition_date, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    frames: list[pd.DataFrame] = []
    symbol_batches = _chunked(symbols, batch_size)
    for index, symbol_batch in enumerate(symbol_batches):
        batch_df = _fetch_alpaca_daily_bars_with_retry(context, symbol_batch, start_dt, end_dt)
        normalized = _normalize_alpaca_daily_bars_df(batch_df, partition_date)
        if not normalized.empty:
            frames.append(normalized)
        if request_sleep_seconds > 0 and index < len(symbol_batches) - 1:
            time.sleep(request_sleep_seconds)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _clear_day_partition(dataset_name: str, partition_date: date) -> None:
    day_dir = DATA_ROOT / "bronze" / dataset_name / f"date={partition_date.isoformat()}"
    if day_dir.exists():
        shutil.rmtree(day_dir)


def _write_day_file(
    dataset_name: str,
    partition_date: date,
    day_df: pd.DataFrame,
) -> tuple[int, int]:
    if day_df is None or day_df.empty:
        return 0, 0

    out_path = (
        DATA_ROOT
        / "bronze"
        / dataset_name
        / f"date={partition_date.isoformat()}"
        / "prices.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    (
        day_df.sort_values(["symbol", "timestamp"])
        .reset_index(drop=True)
        .to_parquet(out_path, index=False)
    )
    return len(day_df), 1


@asset(
    name="eodhd_prices_daily",
    key_prefix=["bronze"],
    partitions_def=EODHD_PRICES_PARTITIONS,
    required_resource_keys={"eodhd"},
    config_schema={
        "symbols": Field(Array(String), is_required=False),
        "max_symbols": Field(Int, is_required=False),
    },
)
def bronze_eodhd_prices_daily(context: AssetExecutionContext) -> None:
    """
    Write EODHD daily end-of-day prices to bronze parquet with one file per trading day.

    This asset pulls the full configured exchange from EODHD in bulk, then optionally
    filters to a configured symbol subset for testing or targeted backfills.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    op_config = context.op_execution_context.op_config or {}

    raw_df = context.resources.eodhd.get_bulk_eod_prices_df(partition_date)
    day_df = _normalize_eodhd_daily_bars_df(raw_df, partition_date)
    day_df = _filter_day_df_to_symbols(
        day_df,
        configured_symbols=op_config.get("symbols"),
        max_symbols=op_config.get("max_symbols"),
    )

    dataset_name = "eodhd_prices_daily"
    _clear_day_partition(dataset_name, partition_date)
    row_count, files_written = _write_day_file(dataset_name, partition_date, day_df)
    if row_count == 0:
        context.log.warning("No EODHD price data returned for partition %s.", context.partition_key)
        return

    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "source": "eodhd",
            "symbol_count": int(day_df["symbol"].nunique()),
            "files_written": files_written,
            "row_count": row_count,
        }
    )


@asset(
    name="alpaca_prices_daily",
    key_prefix=["bronze"],
    partitions_def=ALPACA_PRICES_PARTITIONS,
    required_resource_keys={"alpaca"},
    config_schema={
        "symbols": Field(Array(String), is_required=False),
        "max_symbols": Field(Int, is_required=False),
        "alpaca_symbol_batch_size": Field(Int, is_required=False, default_value=ALPACA_BATCH_SIZE),
        "alpaca_request_sleep_seconds": Field(
            Float, is_required=False, default_value=ALPACA_REQUEST_SLEEP_SECONDS
        ),
    },
)
def bronze_alpaca_prices_daily(context: AssetExecutionContext) -> None:
    """
    Write Alpaca daily prices to bronze parquet with one file per trading day.

    This recent-window dataset is intentionally separate from EODHD so downstream logic
    can prefer Alpaca wherever it overlaps and fall back to EODHD elsewhere.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    op_config = context.op_execution_context.op_config or {}
    symbols = _resolve_alpaca_symbols(
        context,
        configured_symbols=op_config.get("symbols"),
        max_symbols=op_config.get("max_symbols"),
    )
    if not symbols:
        context.log.warning("No symbols resolved for Alpaca daily price ingestion.")
        return

    day_df = _fetch_alpaca_daily_bars_for_day(
        context,
        partition_date,
        symbols,
        batch_size=op_config.get("alpaca_symbol_batch_size", ALPACA_BATCH_SIZE),
        request_sleep_seconds=op_config.get(
            "alpaca_request_sleep_seconds", ALPACA_REQUEST_SLEEP_SECONDS
        ),
    )

    dataset_name = "alpaca_prices_daily"
    _clear_day_partition(dataset_name, partition_date)
    row_count, files_written = _write_day_file(dataset_name, partition_date, day_df)
    if row_count == 0:
        context.log.warning(
            "No Alpaca price data returned for partition %s.",
            context.partition_key,
        )
        return

    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "source": "alpaca",
            "symbol_count": len(symbols),
            "files_written": files_written,
            "row_count": row_count,
        }
    )
