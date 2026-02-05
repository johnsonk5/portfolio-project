import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.yahoo_news_assets import bronze_yahoo_news


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
SILVER_NEWS_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


def _normalize_publisher(name: str) -> str:
    return str(name).strip().lower()


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
            ingested_ts TIMESTAMP
        )
        """
    )

    existing_df = con.execute(
        "SELECT publisher_id, publisher_name_norm FROM silver.ref_publishers"
    ).fetch_df()
    existing_norms = set(existing_df["publisher_name_norm"].astype(str)) if not existing_df.empty else set()

    unique_publishers = (
        df.dropna(subset=["publisher_norm"])
        .drop_duplicates(subset=["publisher_norm"])
        .loc[:, ["publisher", "publisher_norm"]]
    )
    new_publishers = unique_publishers[
        ~unique_publishers["publisher_norm"].isin(existing_norms)
    ]
    new_publishers = new_publishers.sort_values("publisher_norm", kind="stable")
    if new_publishers.empty:
        total_count = con.execute(
            "SELECT count(*) FROM silver.ref_publishers"
        ).fetchone()[0]
        context.add_output_metadata(
            {"inserted_count": 0, "total_count": total_count}
        )
        return

    max_id = int(existing_df["publisher_id"].max()) if not existing_df.empty else 0
    new_rows = []
    next_id = max_id + 1
    for _, row in new_publishers.iterrows():
        norm_name = row["publisher_norm"]
        publisher_id = next_id
        next_id += 1
        new_rows.append(
            {
                "publisher_id": publisher_id,
                "publisher_name": row["publisher"],
                "publisher_name_norm": norm_name,
                "ingested_ts": datetime.now(timezone.utc),
            }
        )

    new_df = pd.DataFrame(new_rows)
    con.register("new_publishers_df", new_df)
    con.execute(
        """
        INSERT INTO silver.ref_publishers
        SELECT publisher_id, publisher_name, publisher_name_norm, ingested_ts
        FROM new_publishers_df
        """
    )

    total_count = con.execute(
        "SELECT count(*) FROM silver.ref_publishers"
    ).fetchone()[0]
    context.add_output_metadata(
        {"inserted_count": len(new_df), "total_count": total_count}
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
