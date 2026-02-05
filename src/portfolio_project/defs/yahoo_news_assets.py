import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dagster import Array, AssetExecutionContext, DailyPartitionsDefinition, Field, Int, String, asset
from dagster import AssetKey


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
BRONZE_NEWS_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
TICKERS_ENV = "ALPACA_TICKERS"
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"


def _resolve_symbols(context: AssetExecutionContext) -> list[str]:
    env_symbols = [
        s.strip() for s in os.getenv(TICKERS_ENV, "").split(",") if s.strip()
    ]
    config_symbols = context.op_config.get("symbols", None)
    active_symbols = []
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
        return active_symbols
    if env_symbols:
        if config_symbols:
            symbols = sorted(set(config_symbols) & set(env_symbols))
            if not symbols:
                context.log.warning(
                    "No configured symbols are active; using symbols from %s.",
                    TICKERS_ENV,
                )
                return env_symbols
            return symbols
        return env_symbols
    return config_symbols or ["AAPL"]


def _fetch_news(
    session: requests.Session,
    symbol: str,
    news_count: int,
    max_retries: int,
    backoff_seconds: float,
) -> list[dict]:
    params = {
        "q": symbol,
        "newsCount": news_count,
        "quotesCount": 0,
    }
    attempt = 0
    while True:
        attempt += 1
        response = session.get(YAHOO_SEARCH_URL, params=params, timeout=20)
        if response.status_code != 429:
            response.raise_for_status()
            payload = response.json()
            return payload.get("news", []) or []
        if attempt > max_retries:
            response.raise_for_status()
        time.sleep(backoff_seconds * attempt)


@asset(
    name="bronze_yahoo_news",
    partitions_def=BRONZE_NEWS_PARTITIONS,
    required_resource_keys={"duckdb"},
    deps=[AssetKey("silver_alpaca_assets")],
    config_schema={
        "symbols": Field(Array(String), is_required=False),
        "news_count": Field(Int, is_required=False, default_value=10),
        "request_delay_seconds": Field(Int, is_required=False, default_value=1),
        "max_retries": Field(Int, is_required=False, default_value=3),
        "retry_backoff_seconds": Field(Int, is_required=False, default_value=2),
    },
)
def bronze_yahoo_news(context: AssetExecutionContext) -> None:
    """
    Fetch Yahoo Finance news headlines for the active ticker universe and store in bronze parquet.
    """
    symbols = _resolve_symbols(context)
    if not symbols:
        context.log.warning("No symbols resolved for Yahoo news ingestion.")
        return

    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    news_count = context.op_config.get("news_count", 10)
    request_delay_seconds = context.op_config.get("request_delay_seconds", 1)
    max_retries = context.op_config.get("max_retries", 3)
    retry_backoff_seconds = context.op_config.get("retry_backoff_seconds", 2)

    rows = []
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
    )
    for symbol in symbols:
        try:
            items = _fetch_news(
                session,
                symbol,
                news_count,
                max_retries=max_retries,
                backoff_seconds=retry_backoff_seconds,
            )
        except Exception as exc:
            context.log.warning("Yahoo news fetch failed for %s: %s", symbol, exc)
            continue

        for item in items:
            publish_ts = item.get("providerPublishTime")
            publish_dt = (
                datetime.fromtimestamp(publish_ts, tz=timezone.utc)
                if publish_ts is not None
                else None
            )
            rows.append(
                {
                    "symbol": str(symbol).upper(),
                    "uuid": item.get("uuid"),
                    "title": item.get("title"),
                    "publisher": item.get("publisher"),
                    "link": item.get("link"),
                    "provider_publish_time": publish_dt,
                    "type": item.get("type"),
                    "summary": item.get("summary"),
                    "query_date": partition_date,
                    "ingested_ts": datetime.now(timezone.utc),
                }
            )
        if request_delay_seconds:
            time.sleep(float(request_delay_seconds))

    if not rows:
        context.log.warning("No Yahoo news items returned for partition %s.", context.partition_key)
        return

    df = pd.DataFrame(rows)
    partition_dir = DATA_ROOT / "bronze" / "yahoo_news" / f"date={partition_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_path = partition_dir / "news.parquet"

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        df = pd.concat([existing, df], ignore_index=True)
        subset_cols = [c for c in ["symbol", "uuid", "link", "title", "provider_publish_time"] if c in df.columns]
        if subset_cols:
            df = df.drop_duplicates(subset=subset_cols, keep="last")

    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df), "symbols": len(set(df["symbol"]))}
    )
