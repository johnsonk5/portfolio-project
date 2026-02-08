import os
from datetime import datetime, timedelta
from pathlib import Path

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.wikipedia_pageviews import silver_wikipedia_pageviews

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_ACTIVITY_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


@asset(
    name="gold_activity",
    deps=[silver_wikipedia_pageviews],
    partitions_def=GOLD_ACTIVITY_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def gold_activity(context: AssetExecutionContext) -> None:
    """
    Maintain a rolling 30-day activity table in DuckDB.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    window_start = partition_date - timedelta(days=29)
    window_end = partition_date + timedelta(days=1)
    calc_start = window_start - timedelta(days=29)

    parquet_paths = []
    cursor = calc_start
    while cursor <= partition_date:
        parquet_path = (
            DATA_ROOT
            / "silver"
            / "wikipedia_pageviews"
            / f"view_date={cursor.strftime('%Y-%m-%d')}"
            / "data_0.parquet"
        )
        if parquet_path.exists():
            parquet_paths.append(parquet_path.as_posix())
        cursor += timedelta(days=1)

    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.activity (
            asset_id BIGINT,
            activity_date DATE,
            source VARCHAR,
            event_type VARCHAR,
            views BIGINT,
            views_30d_avg DOUBLE,
            views_vs_30d_avg DOUBLE,
            ingested_ts TIMESTAMP
        )
        """
    )

    con.execute("DELETE FROM gold.activity WHERE activity_date < ?", [window_start])
    con.execute(
        """
        DELETE FROM gold.activity
        WHERE activity_date >= ? AND activity_date < ?
        """,
        [window_start, window_end],
    )

    if not parquet_paths:
        context.log.warning(
            "No silver Wikipedia pageviews parquet partitions found between %s and %s.",
            window_start,
            partition_date,
        )
        return

    insert_sql = """
        WITH source_data AS (
            SELECT
                asset_id,
                CAST(view_date AS DATE) AS activity_date,
                CAST(views AS BIGINT) AS views,
                ingested_ts
            FROM read_parquet(?)
            WHERE view_date >= ? AND view_date < ?
              AND asset_id IS NOT NULL
        ),
        features AS (
            SELECT
                asset_id,
                activity_date,
                'wikipedia' AS source,
                'pageview' AS event_type,
                views,
                avg(views) OVER (
                    PARTITION BY asset_id
                    ORDER BY activity_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS views_30d_avg,
                ingested_ts
            FROM source_data
        )
        INSERT INTO gold.activity
        SELECT
            asset_id,
            activity_date,
            source,
            event_type,
            views,
            views_30d_avg,
            CASE
                WHEN views_30d_avg IS NULL OR views_30d_avg = 0 THEN NULL
                ELSE views / views_30d_avg
            END AS views_vs_30d_avg,
            ingested_ts
        FROM features
        WHERE activity_date >= ? AND activity_date < ?
    """
    con.execute(
        insert_sql,
        [parquet_paths, calc_start, window_end, window_start, window_end],
    )

    row_count = con.execute(
        """
        SELECT count(*)
        FROM gold.activity
        WHERE activity_date >= ? AND activity_date < ?
        """,
        [window_start, window_end],
    ).fetchone()[0]

    context.add_output_metadata(
        {
            "table": "gold.activity",
            "partition": context.partition_key,
            "window_start": str(window_start),
            "window_end": str(window_end),
            "row_count": row_count,
        }
    )
