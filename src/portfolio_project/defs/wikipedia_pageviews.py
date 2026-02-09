import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from dagster import Array, AssetExecutionContext, DailyPartitionsDefinition, Field, Float, Int, String, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
BRONZE_WIKIPEDIA_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
WIKIPEDIA_PROJECT = os.getenv("WIKIPEDIA_PROJECT", "en.wikipedia.org")
WIKIMEDIA_PAGEVIEWS_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
WIKIMEDIA_USER_AGENT_ENV = "WIKIMEDIA_USER_AGENT"
WIKIPEDIA_ARTICLE_OVERRIDES = {
    "Marsh": "Marsh_(company)",
}


def _article_from_name(name: str) -> str:
    cleaned = str(name).strip()
    if not cleaned:
        return ""
    override = WIKIPEDIA_ARTICLE_OVERRIDES.get(cleaned)
    if override:
        return override.replace(" ", "_")
    cleaned = re.sub(r"\s+Class\s+[A-Z]\s+Common\s+Stock$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+Common\s+Stock$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+Class\s+[A-Z]$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+Ordinary\s+Shares$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+ADS$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.replace(" ", "_")


def _fetch_pageviews(
    session: requests.Session,
    project: str,
    article: str,
    date_key: str,
    max_retries: int,
    backoff_seconds: float,
) -> list[dict]:
    url = (
        f"{WIKIMEDIA_PAGEVIEWS_API}/"
        f"{quote(project, safe='')}/"
        "all-access/user/"
        f"{quote(article, safe='')}/"
        "daily/"
        f"{date_key}/{date_key}"
    )
    attempt = 0
    while True:
        attempt += 1
        response = session.get(url, timeout=20)
        if response.status_code == 404:
            return []
        if response.status_code != 429:
            response.raise_for_status()
            payload = response.json()
            return payload.get("items", []) or []
        if attempt > max_retries:
            response.raise_for_status()
        time.sleep(backoff_seconds * attempt)


def _normalize_view_date(value, partition_date):
    if value is None or value == "":
        return partition_date
    text = str(value)
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return partition_date


@asset(
    name="bronze_wikipedia_pageviews",
    partitions_def=BRONZE_WIKIPEDIA_PARTITIONS,
    required_resource_keys={"duckdb"},
    deps=[silver_alpaca_assets],
    config_schema={
        "company_names": Field(Array(String), is_required=False),
        "request_delay_seconds": Field(Float, is_required=False, default_value=0.2),
        "max_retries": Field(Int, is_required=False, default_value=3),
        "retry_backoff_seconds": Field(Float, is_required=False, default_value=2.0),
    },
)
def bronze_wikipedia_pageviews(context: AssetExecutionContext) -> None:
    """
    Fetch daily Wikipedia pageviews for company names from silver.assets.
    """
    con = context.resources.duckdb
    try:
        assets_df = con.execute(
            """
            SELECT symbol, name, wikipedia_title
            FROM silver.assets
            WHERE is_active = TRUE AND name IS NOT NULL
            """
        ).fetch_df()
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
        return

    config_names = context.op_config.get("company_names") or []

    if config_names:
        names_df = pd.DataFrame({"name": config_names})
        names_df["symbol"] = None
    else:
        names_df = assets_df[["symbol", "name", "wikipedia_title"]].dropna(subset=["name"])

    if names_df.empty:
        context.log.warning("No company names available for Wikipedia pageviews.")
        return

    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    date_key = partition_date.strftime("%Y%m%d")
    request_delay_seconds = float(context.op_config.get("request_delay_seconds", 0.2))
    max_retries = int(context.op_config.get("max_retries", 3))
    retry_backoff_seconds = float(context.op_config.get("retry_backoff_seconds", 2.0))

    rows = []
    session = requests.Session()
    user_agent = os.getenv(
        WIKIMEDIA_USER_AGENT_ENV,
        "portfolio-project/0.1 (+https://example.com) contact: data@portfolio.local",
    )
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
    )

    for _, row in names_df.iterrows():
        name = str(row["name"]).strip()
        title = row.get("wikipedia_title")
        if isinstance(title, str) and title.strip():
            article = title.strip()
        else:
            article = _article_from_name(name)
        if not article:
            continue
        context.log.info("Wikipedia pageviews: name=%s article=%s", name, article)
        try:
            items = _fetch_pageviews(
                session,
                WIKIPEDIA_PROJECT,
                article,
                date_key,
                max_retries=max_retries,
                backoff_seconds=retry_backoff_seconds,
            )
        except Exception as exc:
            context.log.warning("Wikipedia pageviews fetch failed for %s: %s", name, exc)
            continue
        if not items:
            context.log.warning("No Wikipedia pageviews found for name=%s article=%s", name, article)
            continue

        for item in items:
            rows.append(
                {
                    "symbol": row.get("symbol"),
                    "company_name": name,
                    "article": item.get("article") or article,
                    "project": item.get("project") or WIKIPEDIA_PROJECT,
                    "access": item.get("access"),
                    "agent": item.get("agent"),
                    "granularity": item.get("granularity"),
                    "view_date": date_key,
                    "views": item.get("views"),
                    "ingested_ts": datetime.now(timezone.utc),
                }
            )
        if request_delay_seconds:
            time.sleep(request_delay_seconds)

    if not rows:
        context.log.warning("No Wikipedia pageviews returned for partition %s.", context.partition_key)
        return

    df = pd.DataFrame(rows)
    partition_dir = DATA_ROOT / "bronze" / "wikipedia_pageviews" / f"date={partition_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_path = partition_dir / "pageviews.parquet"

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        df = pd.concat([existing, df], ignore_index=True)
        subset_cols = [c for c in ["article", "view_date", "symbol"] if c in df.columns]
        if subset_cols:
            df = df.drop_duplicates(subset=subset_cols, keep="last")

    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df), "articles": len(set(df["article"]))}
    )


@asset(
    name="silver_wikipedia_pageviews",
    partitions_def=BRONZE_WIKIPEDIA_PARTITIONS,
    required_resource_keys={"duckdb"},
    deps=[bronze_wikipedia_pageviews, silver_alpaca_assets],
)
def silver_wikipedia_pageviews(context: AssetExecutionContext) -> None:
    """
    Normalize bronze Wikipedia pageviews into silver parquet partitions.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    bronze_path = (
        DATA_ROOT
        / "bronze"
        / "wikipedia_pageviews"
        / f"date={partition_date}"
        / "pageviews.parquet"
    )
    if not bronze_path.exists():
        context.log.warning("Bronze Wikipedia pageviews parquet not found at %s", bronze_path)
        return

    con = context.resources.duckdb
    try:
        con.execute("SELECT 1 FROM silver.assets LIMIT 1")
    except Exception as exc:
        context.log.warning("silver.assets is missing or unreadable: %s", exc)
        return

    bronze_path_sql = str(bronze_path).replace("'", "''")
    enriched_sql = f"""
        SELECT
            COALESCE(assets_symbol.asset_id, assets_name.asset_id) AS asset_id,
            bronze.granularity,
            bronze.view_date,
            bronze.views,
            bronze.ingested_ts
        FROM read_parquet('{bronze_path_sql}') AS bronze
        LEFT JOIN silver.assets AS assets_symbol
            ON upper(bronze.symbol) = upper(assets_symbol.symbol)
        LEFT JOIN silver.assets AS assets_name
            ON assets_symbol.asset_id IS NULL
            AND lower(trim(bronze.company_name)) = lower(trim(assets_name.name))
    """
    bronze_df = con.execute(enriched_sql).fetch_df()
    if bronze_df is None or bronze_df.empty:
        context.log.warning("No bronze Wikipedia pageviews rows available for %s.", partition_date)
        return

    bronze_df["view_date"] = bronze_df["view_date"].apply(
        lambda value: _normalize_view_date(value, partition_date)
    )

    out_dir = DATA_ROOT / "silver" / "wikipedia_pageviews" / f"view_date={partition_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data_0.parquet"

    df = bronze_df[["asset_id", "granularity", "view_date", "views", "ingested_ts"]].copy()
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        df = pd.concat([existing, df], ignore_index=True)
        subset_cols = [c for c in ["asset_id", "view_date", "granularity"] if c in df.columns]
        if subset_cols:
            df = df.drop_duplicates(subset=subset_cols, keep="last")

    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df)}
    )
