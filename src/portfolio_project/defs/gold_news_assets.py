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
    deps=[silver_news],
    partitions_def=GOLD_NEWS_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def gold_headlines(context: AssetExecutionContext) -> None:
    """
    Maintain a rolling 30-day headlines table in DuckDB.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    window_start = partition_date - timedelta(days=30)
    window_end = partition_date + timedelta(days=1)

    parquet_paths = []
    cursor = window_start
    while cursor <= partition_date:
        parquet_path = (
            DATA_ROOT
            / "silver"
            / "news"
            / f"date={cursor.strftime('%Y-%m-%d')}"
            / "news.parquet"
        )
        if parquet_path.exists():
            parquet_paths.append(parquet_path.as_posix())
        cursor += timedelta(days=1)

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

    con.execute(
        "DELETE FROM gold.headlines WHERE provider_publish_time < ?",
        [window_start],
    )

    if not parquet_paths:
        context.log.warning(
            "No silver news parquet partitions found between %s and %s.",
            window_start,
            partition_date,
        )
        return

    con.execute(
        """
        DELETE FROM gold.headlines
        WHERE provider_publish_time >= ? AND provider_publish_time < ?
        """,
        [window_start, window_end],
    )

    insert_sql = """
        WITH source_news AS (
            SELECT *
            FROM read_parquet(?)
        ),
        deduped AS (
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
                NULL AS sentiment,
                ROW_NUMBER() OVER (
                    PARTITION BY uuid
                    ORDER BY ingested_ts DESC NULLS LAST
                ) AS rn
            FROM source_news
            WHERE provider_publish_time >= ? AND provider_publish_time < ?
        )
        INSERT INTO gold.headlines
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
            sentiment
        FROM deduped
        WHERE rn = 1
    """
    con.execute(
        insert_sql,
        [parquet_paths, window_start, window_end],
    )

    row_count = con.execute(
        """
        SELECT count(*)
        FROM gold.headlines
        WHERE provider_publish_time >= ? AND provider_publish_time < ?
        """,
        [window_start, window_end],
    ).fetchone()[0]

    context.add_output_metadata(
        {
            "table": "gold.headlines",
            "partition": context.partition_key,
            "window_start": str(window_start),
            "window_end": str(window_end),
            "row_count": row_count,
        }
    )
