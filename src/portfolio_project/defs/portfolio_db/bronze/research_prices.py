import os
import re
import shutil
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
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
RESEARCH_PRICES_PARTITIONS_START_DATE = os.getenv(
    "RESEARCH_PRICES_PARTITIONS_START_DATE", "2000-01-01"
)
RESEARCH_PRICES_PARTITIONS = DailyPartitionsDefinition(
    start_date=RESEARCH_PRICES_PARTITIONS_START_DATE
)
YAHOO_FINANCE_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
ALPACA_CUTOFF_DATE = date(2016, 1, 1)
DEFAULT_EXCEPTION_SYMBOLS = ("SPY",)
ALPACA_BATCH_SIZE = int(os.getenv("RESEARCH_ALPACA_SYMBOL_BATCH_SIZE", "200"))
ALPACA_REQUEST_SLEEP_SECONDS = float(os.getenv("RESEARCH_ALPACA_REQUEST_SLEEP_SECONDS", "0.25"))
ALPACA_REQUEST_MAX_RETRIES = int(os.getenv("RESEARCH_ALPACA_REQUEST_MAX_RETRIES", "4"))
ALPACA_REQUEST_RETRY_BASE_SECONDS = float(
    os.getenv("RESEARCH_ALPACA_REQUEST_RETRY_BASE_SECONDS", "1.0")
)
YAHOO_REQUEST_SLEEP_SECONDS = float(os.getenv("RESEARCH_YAHOO_REQUEST_SLEEP_SECONDS", "0.4"))
YAHOO_REQUEST_MAX_RETRIES = int(os.getenv("RESEARCH_YAHOO_REQUEST_MAX_RETRIES", "4"))
YAHOO_REQUEST_RETRY_BASE_SECONDS = float(
    os.getenv("RESEARCH_YAHOO_REQUEST_RETRY_BASE_SECONDS", "1.5")
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
    elif attributes is not None and not pd.isna(attributes):
        searchable_fields.append(str(attributes))

    searchable_text = " ".join(part for part in searchable_fields if part)
    if searchable_text and _COMMON_STOCK_EXCLUSION_RE.search(searchable_text):
        return False

    return True


def _resolve_research_symbols(
    context: AssetExecutionContext,
    configured_symbols: list[str] | None = None,
    max_symbols: int | None = None,
) -> list[str]:
    configured = _normalize_symbols(configured_symbols)
    if configured:
        return configured[:max_symbols] if max_symbols else configured

    assets_df = context.resources.alpaca.get_assets_df()
    if assets_df is None or assets_df.empty:
        context.log.warning("Alpaca asset universe returned no rows.")
        return list(DEFAULT_EXCEPTION_SYMBOLS)

    filtered_symbols = sorted(
        {
            str(row["symbol"]).strip().upper()
            for _, row in assets_df.iterrows()
            if row.get("symbol") and _is_probable_common_equity(row)
        }
    )

    for symbol in DEFAULT_EXCEPTION_SYMBOLS:
        if symbol not in filtered_symbols:
            filtered_symbols.append(symbol)

    filtered_symbols = sorted(set(filtered_symbols))
    if max_symbols is not None and max_symbols > 0:
        filtered_symbols = filtered_symbols[:max_symbols]
    return filtered_symbols


def _normalize_research_bars_df(
    df: pd.DataFrame,
    partition_date: date,
    source: str,
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

    for optional_col in ["trade_count", "vwap"]:
        if optional_col not in normalized.columns:
            normalized[optional_col] = pd.NA

    normalized["source"] = source
    normalized["ingested_ts"] = datetime.now(timezone.utc)
    ordered_columns = [
        "symbol",
        "timestamp",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "source",
        "ingested_ts",
    ]
    existing_columns = [column for column in ordered_columns if column in normalized.columns]
    return normalized[existing_columns].sort_values(["symbol", "timestamp"]).reset_index(drop=True)


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
        normalized = _normalize_research_bars_df(batch_df, partition_date, source="alpaca")
        if not normalized.empty:
            frames.append(normalized)
        if request_sleep_seconds > 0 and index < len(symbol_batches) - 1:
            time.sleep(request_sleep_seconds)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _make_yahoo_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        }
    )
    return session


def _fetch_yahoo_chart_json(
    session: requests.Session,
    symbol: str,
    partition_date: date,
) -> dict | None:
    start_dt = datetime.combine(
        partition_date - timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )
    end_dt = datetime.combine(
        partition_date + timedelta(days=2), datetime.min.time(), tzinfo=timezone.utc
    )
    params = {
        "period1": int(start_dt.timestamp()),
        "period2": int(end_dt.timestamp()),
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }

    max_attempts = YAHOO_REQUEST_MAX_RETRIES + 1
    url = YAHOO_FINANCE_CHART_URL.format(symbol=symbol)
    for attempt in range(1, max_attempts + 1):
        response = session.get(url, params=params, timeout=20)
        if response.status_code < 400:
            payload = response.json()
            result = ((payload.get("chart") or {}).get("result") or [None])[0]
            return result

        retryable = response.status_code in {429, 500, 502, 503, 504}
        if not retryable or attempt == max_attempts:
            response.raise_for_status()

        sleep_seconds = YAHOO_REQUEST_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
        time.sleep(sleep_seconds)
    return None


def _normalize_yahoo_result(
    symbol: str,
    partition_date: date,
    result: dict | None,
) -> pd.DataFrame:
    if not result:
        return pd.DataFrame()

    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
    if not timestamps or not quote:
        return pd.DataFrame()

    lengths = [len(timestamps)]
    lengths.extend(len(values) for values in quote.values() if isinstance(values, list))
    row_count = min(lengths)
    if row_count <= 0:
        return pd.DataFrame()

    df = pd.DataFrame(
        {
            "symbol": [symbol.upper()] * row_count,
            "timestamp": pd.to_datetime(timestamps[:row_count], unit="s", utc=True),
            "open": quote.get("open", [])[:row_count],
            "high": quote.get("high", [])[:row_count],
            "low": quote.get("low", [])[:row_count],
            "close": quote.get("close", [])[:row_count],
            "volume": quote.get("volume", [])[:row_count],
        }
    )
    return _normalize_research_bars_df(df, partition_date, source="yahoo_finance")


def _fetch_yahoo_daily_bars_for_day(
    context: AssetExecutionContext,
    partition_date: date,
    symbols: list[str],
    request_sleep_seconds: float,
) -> pd.DataFrame:
    session = _make_yahoo_session()
    frames: list[pd.DataFrame] = []
    for index, symbol in enumerate(symbols):
        try:
            result = _fetch_yahoo_chart_json(session, symbol, partition_date)
            normalized = _normalize_yahoo_result(symbol, partition_date, result)
            if not normalized.empty:
                frames.append(normalized)
        except Exception as exc:
            context.log.warning("Yahoo Finance research bars failed for %s: %s", symbol, exc)
        if request_sleep_seconds > 0 and index < len(symbols) - 1:
            time.sleep(request_sleep_seconds)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _clear_research_day_partition(partition_date: date) -> None:
    day_dir = DATA_ROOT / "bronze" / "research_prices_daily" / f"date={partition_date.isoformat()}"
    if day_dir.exists():
        shutil.rmtree(day_dir)


def _write_research_day_symbol_files(
    partition_date: date,
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
            / "research_prices_daily"
            / f"date={partition_date.isoformat()}"
            / f"symbol={symbol}"
            / "prices.parquet"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        symbol_df.to_parquet(out_path, index=False)
        rows_written += len(symbol_df)
        files_written += 1
    return rows_written, files_written


@asset(
    name="research_prices_daily",
    key_prefix=["bronze"],
    partitions_def=RESEARCH_PRICES_PARTITIONS,
    required_resource_keys={"alpaca"},
    config_schema={
        "symbols": Field(Array(String), is_required=False),
        "max_symbols": Field(Int, is_required=False),
        "alpaca_symbol_batch_size": Field(Int, is_required=False, default_value=ALPACA_BATCH_SIZE),
        "alpaca_request_sleep_seconds": Field(
            Float, is_required=False, default_value=ALPACA_REQUEST_SLEEP_SECONDS
        ),
        "yahoo_request_sleep_seconds": Field(
            Float, is_required=False, default_value=YAHOO_REQUEST_SLEEP_SECONDS
        ),
    },
)
def bronze_research_prices_daily(context: AssetExecutionContext) -> None:
    """
    Write daily research price bars to bronze parquet partitioned by day and symbol.

    The universe is current tradable U.S. common equities from Alpaca, excluding
    ETFs, OTC issues, preferreds, rights, warrants, and similar instruments.
    SPY is force-included as a benchmark even though it is an ETF.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    op_config = context.op_execution_context.op_config or {}
    symbols = _resolve_research_symbols(
        context,
        configured_symbols=op_config.get("symbols"),
        max_symbols=op_config.get("max_symbols"),
    )
    if not symbols:
        context.log.warning("No symbols resolved for research price ingestion.")
        return

    if partition_date >= ALPACA_CUTOFF_DATE:
        day_df = _fetch_alpaca_daily_bars_for_day(
            context,
            partition_date,
            symbols,
            batch_size=op_config.get("alpaca_symbol_batch_size", ALPACA_BATCH_SIZE),
            request_sleep_seconds=op_config.get(
                "alpaca_request_sleep_seconds", ALPACA_REQUEST_SLEEP_SECONDS
            ),
        )
        source_name = "alpaca"
    else:
        day_df = _fetch_yahoo_daily_bars_for_day(
            context,
            partition_date,
            symbols,
            request_sleep_seconds=op_config.get(
                "yahoo_request_sleep_seconds", YAHOO_REQUEST_SLEEP_SECONDS
            ),
        )
        source_name = "yahoo_finance"

    _clear_research_day_partition(partition_date)
    row_count, files_written = _write_research_day_symbol_files(partition_date, day_df)
    if row_count == 0:
        context.log.warning(
            "No research price data returned for partition %s.", context.partition_key
        )
        return

    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "source": source_name,
            "symbol_count": len(symbols),
            "files_written": files_written,
            "row_count": row_count,
        }
    )
