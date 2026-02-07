import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.yahoo_news_assets import bronze_yahoo_news


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
SILVER_NEWS_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
DEFAULT_PUBLISHER_WEIGHT = float(os.getenv("DEFAULT_PUBLISHER_WEIGHT", "0.05"))
TRANCO_LOOKUP_DAYS = int(os.getenv("TRANCO_LOOKUP_DAYS", "30"))
PUBLISHER_DOMAIN_MAP_PATH = os.getenv("PUBLISHER_DOMAIN_MAP_PATH", "").strip()

PUBLISHER_DOMAIN_OVERRIDES = {
    "associated press": "apnews.com",
    "axios": "axios.com",
    "bloomberg": "bloomberg.com",
    "cnbc": "cnbc.com",
    "financial times": "ft.com",
    "fortune": "fortune.com",
    "marketwatch": "marketwatch.com",
    "reuters": "reuters.com",
    "the wall street journal": "wsj.com",
    "wall street journal": "wsj.com",
    "the new york times": "nytimes.com",
    "new york times": "nytimes.com",
    "the washington post": "washingtonpost.com",
    "washington post": "washingtonpost.com",
    "yahoo finance": "yahoo.com",
}


def _normalize_publisher(name: str) -> str:
    if pd.isna(name):
        return ""
    name_str = str(name).strip()
    if not name_str:
        return ""
    return name_str.lower()


def _next_publisher_id(existing_df: pd.DataFrame) -> int:
    if existing_df.empty:
        return 1
    max_id = int(existing_df["publisher_id"].max())
    return max_id + 1


def _base_domain(host: str) -> str:
    host_str = str(host).strip().lower()
    if host_str.startswith("www."):
        host_str = host_str[4:]
    parts = [p for p in host_str.split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    public_suffixes = {
        "co.uk",
        "com.au",
        "com.br",
        "com.mx",
        "com.sg",
        "com.hk",
        "com.tw",
        "com.tr",
        "co.jp",
        "co.in",
        "co.kr",
        "co.nz",
        "co.za",
    }
    suffix = ".".join(parts[-2:])
    if suffix in public_suffixes and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _load_publisher_domain_overrides() -> dict[str, str]:
    overrides = {k: _base_domain(v) for k, v in PUBLISHER_DOMAIN_OVERRIDES.items()}
    if not PUBLISHER_DOMAIN_MAP_PATH:
        return overrides
    map_path = Path(PUBLISHER_DOMAIN_MAP_PATH)
    if not map_path.exists():
        return overrides
    df = pd.read_csv(map_path)
    if "publisher_name" not in df.columns or "publisher_domain" not in df.columns:
        return overrides
    for _, row in df.iterrows():
        norm_name = _normalize_publisher(row["publisher_name"])
        if not norm_name:
            continue
        domain = _base_domain(row["publisher_domain"])
        if domain:
            overrides[norm_name] = domain
    return overrides


def _publisher_domain_from_name(name: str, overrides: dict[str, str]) -> str:
    norm_name = _normalize_publisher(name)
    if not norm_name:
        return ""
    override = overrides.get(norm_name)
    if override:
        return override
    raw_name = str(name).strip()
    if "." in raw_name and " " not in raw_name:
        return _base_domain(raw_name)
    return ""


def _rank_to_weight(rank: int | None, default_weight: float) -> tuple[float, str]:
    if not rank or rank <= 0:
        return default_weight, "default"
    weight = 1.0 - (math.log10(rank) / 6.0)
    weight = min(1.0, max(default_weight, weight))
    return weight, "tranco"


def _find_latest_tranco_path(as_of_date: datetime.date) -> Path | None:
    tranco_root = DATA_ROOT / "bronze" / "tranco"
    if not tranco_root.exists():
        return None
    latest_date = None
    latest_path = None
    for entry in tranco_root.iterdir():
        if not entry.is_dir():
            continue
        if not entry.name.startswith("date="):
            continue
        date_str = entry.name.split("=", 1)[-1]
        try:
            entry_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if entry_date > as_of_date:
            continue
        candidate = entry / "tranco.csv"
        if not candidate.exists():
            continue
        if latest_date is None or entry_date > latest_date:
            latest_date = entry_date
            latest_path = candidate
    if latest_path is not None:
        return latest_path

    fallback = None
    fallback_date = None
    for entry in tranco_root.iterdir():
        if not entry.is_dir():
            continue
        if not entry.name.startswith("date="):
            continue
        date_str = entry.name.split("=", 1)[-1]
        try:
            entry_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        candidate = entry / "tranco.csv"
        if not candidate.exists():
            continue
        if fallback_date is None or entry_date > fallback_date:
            fallback_date = entry_date
            fallback = candidate
    return fallback


def _load_tranco_ranks(as_of_date: datetime.date) -> dict[str, int]:
    tranco_path = _find_latest_tranco_path(as_of_date)
    if tranco_path is None:
        return {}
    tranco_df = pd.read_csv(
        tranco_path,
        names=["rank", "domain"],
        usecols=[0, 1],
        dtype={"rank": "int64", "domain": "string"},
    )
    tranco_df["domain"] = tranco_df["domain"].map(_base_domain)
    return dict(zip(tranco_df["domain"], tranco_df["rank"]))


@asset(
    name="silver_ref_publishers",
    partitions_def=SILVER_NEWS_PARTITIONS,
    deps=[bronze_yahoo_news],
    required_resource_keys={"duckdb"},
)
def silver_ref_publishers(context: AssetExecutionContext) -> None:
    """
    Maintain a publisher reference table keyed by deterministic publisher_id.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    bronze_path = DATA_ROOT / "bronze" / "yahoo_news" / f"date={partition_date}" / "news.parquet"
    if not bronze_path.exists():
        context.log.warning("Bronze Yahoo news parquet not found at %s", bronze_path)
        return

    df = pd.read_parquet(bronze_path)
    if df.empty or "publisher" not in df.columns:
        context.log.warning("Bronze Yahoo news parquet is empty or missing publisher column.")
        return

    df["publisher_norm"] = df["publisher"].map(_normalize_publisher)
    df = df[df["publisher_norm"].astype(bool)]
    if df.empty:
        context.log.warning("No publisher values found in bronze Yahoo news partition.")
        return

    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.ref_publishers (
            publisher_id BIGINT,
            publisher_name VARCHAR,
            publisher_name_norm VARCHAR,
            ingested_ts TIMESTAMP,
            publisher_domain VARCHAR,
            publisher_weight DOUBLE,
            weight_source VARCHAR,
            weight_updated_ts TIMESTAMP
        )
        """
    )

    existing_df = con.execute(
        """
        SELECT publisher_id, publisher_name_norm, publisher_domain, publisher_weight, weight_source,
               weight_updated_ts
        FROM silver.ref_publishers
        """
    ).fetch_df()
    existing_norms = set(existing_df["publisher_name_norm"].astype(str)) if not existing_df.empty else set()

    domain_series = pd.Series(dtype="string")
    publisher_overrides = _load_publisher_domain_overrides()
    df["publisher_domain"] = df["publisher"].map(
        lambda value: _publisher_domain_from_name(value, publisher_overrides)
    )
    domain_series = (
        df[df["publisher_domain"].astype(bool)]
        .groupby("publisher_norm")["publisher_domain"]
        .agg(lambda values: values.value_counts().index[0])
    )

    unique_publishers = (
        df.dropna(subset=["publisher_norm"])
        .drop_duplicates(subset=["publisher_norm"])
        .loc[:, ["publisher", "publisher_norm"]]
    )
    unique_publishers["publisher_domain"] = unique_publishers["publisher_norm"].map(domain_series).fillna("")

    tranco_ranks = _load_tranco_ranks(partition_date)
    if not tranco_ranks:
        context.log.warning(
            "No Tranco ranks available; publisher weights will use default=%s.",
            DEFAULT_PUBLISHER_WEIGHT,
        )
    weight_now = datetime.now(timezone.utc)
    refresh_cutoff = weight_now - timedelta(days=TRANCO_LOOKUP_DAYS)
    new_publishers = unique_publishers[
        ~unique_publishers["publisher_norm"].isin(existing_norms)
    ]
    new_publishers = new_publishers.sort_values("publisher_norm", kind="stable")
    inserted_count = 0
    matched_count = 0
    refreshed_match_count = 0
    if not new_publishers.empty:
        new_rows = []
        next_id = _next_publisher_id(existing_df)
        for _, row in new_publishers.iterrows():
            norm_name = row["publisher_norm"]
            publisher_id = next_id
            next_id += 1
            publisher_domain = row.get("publisher_domain", "")
            rank = tranco_ranks.get(publisher_domain) if publisher_domain else None
            publisher_weight, weight_source = _rank_to_weight(rank, DEFAULT_PUBLISHER_WEIGHT)
            if weight_source == "tranco":
                matched_count += 1
            new_rows.append(
                {
                    "publisher_id": publisher_id,
                    "publisher_name": row["publisher"],
                    "publisher_name_norm": norm_name,
                    "ingested_ts": datetime.now(timezone.utc),
                    "publisher_domain": publisher_domain,
                    "publisher_weight": publisher_weight,
                    "weight_source": weight_source,
                    "weight_updated_ts": weight_now,
                }
            )

        new_df = pd.DataFrame(new_rows)
        con.register("new_publishers_df", new_df)
        con.execute(
            """
            INSERT INTO silver.ref_publishers
            SELECT
                publisher_id,
                publisher_name,
                publisher_name_norm,
                ingested_ts,
                publisher_domain,
                publisher_weight,
                weight_source,
                weight_updated_ts
            FROM new_publishers_df
            """
        )
        inserted_count = len(new_df)

    if not existing_df.empty:
        refresh_df = existing_df.copy()
        refresh_df["publisher_name_norm"] = refresh_df["publisher_name_norm"].astype(str)
        refresh_df["publisher_domain"] = refresh_df["publisher_domain"].fillna("")
        refresh_df["publisher_domain"] = refresh_df["publisher_domain"].astype(str)
        refresh_df["publisher_domain"] = refresh_df["publisher_domain"].replace("nan", "")
        refresh_df["publisher_domain"] = refresh_df["publisher_domain"].map(_base_domain)

        if not domain_series.empty:
            refresh_df = refresh_df.merge(
                domain_series.rename("observed_domain"),
                left_on="publisher_name_norm",
                right_index=True,
                how="left",
            )
            refresh_df["observed_domain"] = refresh_df["observed_domain"].fillna("")
            refresh_df["publisher_domain"] = refresh_df["publisher_domain"].where(
                refresh_df["publisher_domain"].astype(bool),
                refresh_df["observed_domain"],
            )

        refresh_df["weight_updated_ts"] = pd.to_datetime(
            refresh_df["weight_updated_ts"], utc=True, errors="coerce"
        )
        refresh_mask = refresh_df["weight_updated_ts"].isna() | (
            refresh_df["weight_updated_ts"] < refresh_cutoff
        )
        refresh_targets = refresh_df[refresh_mask].copy()
        if not refresh_targets.empty:
            refresh_targets["publisher_domain"] = refresh_targets["publisher_domain"].fillna("")
            refresh_targets["publisher_domain"] = refresh_targets["publisher_domain"].map(_base_domain)
            refresh_targets["tranco_rank"] = refresh_targets["publisher_domain"].map(tranco_ranks)
            refresh_targets[["publisher_weight", "weight_source"]] = refresh_targets["tranco_rank"].apply(
                lambda rank: pd.Series(_rank_to_weight(rank, DEFAULT_PUBLISHER_WEIGHT))
            )
            refresh_targets["weight_updated_ts"] = weight_now
            refreshed_match_count = int((refresh_targets["weight_source"] == "tranco").sum())
            refresh_targets = refresh_targets[
                ["publisher_id", "publisher_domain", "publisher_weight", "weight_source", "weight_updated_ts"]
            ]
            con.register("publisher_weight_updates_df", refresh_targets)
            con.execute(
                """
                UPDATE silver.ref_publishers AS p
                SET
                    publisher_domain = u.publisher_domain,
                    publisher_weight = u.publisher_weight,
                    weight_source = u.weight_source,
                    weight_updated_ts = u.weight_updated_ts
                FROM publisher_weight_updates_df AS u
                WHERE p.publisher_id = u.publisher_id
                """
            )

    total_count = con.execute(
        "SELECT count(*) FROM silver.ref_publishers"
    ).fetchone()[0]
    context.add_output_metadata(
        {
            "inserted_count": inserted_count,
            "total_count": total_count,
            "tranco_matches_new": matched_count,
            "tranco_matches_refresh": refreshed_match_count,
        }
    )


@asset(
    name="silver_news",
    partitions_def=SILVER_NEWS_PARTITIONS,
    deps=[bronze_yahoo_news, silver_ref_publishers, silver_alpaca_assets],
    required_resource_keys={"duckdb"},
)
def silver_news(context: AssetExecutionContext) -> None:
    """
    Normalize Yahoo news into silver parquet with asset_id and publisher_id.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    bronze_path = DATA_ROOT / "bronze" / "yahoo_news" / f"date={partition_date}" / "news.parquet"
    if not bronze_path.exists():
        context.log.warning("Bronze Yahoo news parquet not found at %s", bronze_path)
        return

    con = context.resources.duckdb
    try:
        con.execute("SELECT 1 FROM silver.assets LIMIT 1")
        con.execute("SELECT 1 FROM silver.ref_publishers LIMIT 1")
    except Exception as exc:
        context.log.warning("Silver reference tables missing or unreadable: %s", exc)
        return

    silver_root = DATA_ROOT / "silver" / "news" / f"date={context.partition_key}"
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "news.parquet"

    bronze_path_sql = bronze_path.as_posix().replace("'", "''")
    base_select = f"""
        SELECT
            assets.asset_id,
            upper(bronze.symbol) AS symbol,
            bronze.uuid,
            bronze.title,
            publishers.publisher_id,
            bronze.link,
            bronze.provider_publish_time,
            bronze.type,
            bronze.summary,
            bronze.query_date,
            bronze.ingested_ts
        FROM read_parquet('{bronze_path_sql}') AS bronze
        LEFT JOIN silver.assets AS assets
            ON upper(bronze.symbol) = upper(assets.symbol)
        LEFT JOIN silver.ref_publishers AS publishers
            ON lower(trim(bronze.publisher)) = publishers.publisher_name_norm
    """

    counts = con.execute(
        f"""
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS missing_asset_id_count,
            sum(CASE WHEN publisher_id IS NULL THEN 1 ELSE 0 END) AS missing_publisher_id_count
        FROM ({base_select}) AS enriched
        """
    ).fetchone()
    if counts is None or counts[0] == 0:
        context.log.warning("No Yahoo news rows found for partition %s.", context.partition_key)
        return

    out_path_sql = out_path.as_posix().replace("'", "''")
    con.execute(
        f"""
        COPY (
            {base_select}
        )
        TO '{out_path_sql}' (FORMAT PARQUET)
        """
    )

    context.add_output_metadata(
        {
            "parquet_path": str(out_path),
            "row_count": int(counts[0]),
            "missing_asset_id_count": int(counts[1]) if counts[1] is not None else 0,
            "missing_publisher_id_count": int(counts[2]) if counts[2] is not None else 0,
        }
    )
