import os
from datetime import datetime, timedelta
from pathlib import Path

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_news_assets import silver_news


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_NEWS_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


@asset(
    name="gold_headlines",
    partitions_def=GOLD_NEWS_PARTITIONS,
    deps=[silver_news],
    required_resource_keys={"duckdb"},
)
def gold_headlines(context: AssetExecutionContext) -> None:
    """
    Store the last month of news headlines in DuckDB, keyed by article date.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    silver_path = (
        DATA_ROOT / "silver" / "news" / f"date={context.partition_key}" / "news.parquet"
    )
    if not silver_path.exists():
        context.log.warning("Silver news parquet not found at %s", silver_path)
        return

    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.headlines (
            asset_id BIGINT,
            symbol VARCHAR,
            uuid VARCHAR,
            title VARCHAR,
            publisher_id BIGINT,
            link VARCHAR,
            provider_publish_time TIMESTAMP,
            type VARCHAR,
            summary VARCHAR,
            query_date DATE,
            ingested_ts TIMESTAMP,
            sentiment VARCHAR
        )
        """
    )

    cutoff_date = partition_date - timedelta(days=30)
    silver_path_sql = silver_path.as_posix().replace("'", "''")

    eligible_count = con.execute(
        """
        SELECT count(*)
        FROM read_parquet(?)
        WHERE provider_publish_time IS NOT NULL
          AND CAST(provider_publish_time AS DATE) >= ?
        """,
        [silver_path_sql, cutoff_date],
    ).fetchone()[0]

    con.execute(
        """
        DELETE FROM gold.headlines
        WHERE uuid IN (
            SELECT uuid FROM read_parquet(?)
        )
        """,
        [silver_path_sql],
    )
    con.execute(
        """
        DELETE FROM gold.headlines
        WHERE provider_publish_time IS NOT NULL
          AND CAST(provider_publish_time AS DATE) < ?
        """,
        [cutoff_date],
    )
    con.execute(
        """
        INSERT INTO gold.headlines (
            asset_id,
            symbol,
            uuid,
            title,
            publisher_id,
            link,
            provider_publish_time,
            type,
            summary,
            query_date,
            ingested_ts,
            sentiment
        )
        SELECT
            asset_id,
            symbol,
            uuid,
            title,
            publisher_id,
            link,
            provider_publish_time,
            type,
            summary,
            query_date,
            ingested_ts,
            NULL AS sentiment
        FROM read_parquet(?)
        WHERE provider_publish_time IS NOT NULL
          AND CAST(provider_publish_time AS DATE) >= ?
        """,
        [silver_path_sql, cutoff_date],
    )

    total_count = con.execute(
        "SELECT count(*) FROM gold.headlines"
    ).fetchone()[0]
    context.add_output_metadata(
        {
            "table": "gold.headlines",
            "partition": context.partition_key,
            "eligible_row_count": int(eligible_count),
            "total_row_count": int(total_count),
            "cutoff_date": str(cutoff_date),
        }
    )
