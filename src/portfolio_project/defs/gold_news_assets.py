import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_news_assets import silver_news

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_NEWS_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
SENTIMENT_BATCH_SIZE = int(os.getenv("NEWS_SENTIMENT_BATCH_SIZE", "32"))
_SENTIMENT_PIPELINE = None


def _get_sentiment_pipeline():
    global _SENTIMENT_PIPELINE
    if _SENTIMENT_PIPELINE is None:
        try:
            from transformers import pipeline
        except ImportError as exc:
            raise ImportError(
                "FinBERT sentiment requires transformers; install project dependencies."
            ) from exc
        _SENTIMENT_PIPELINE = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            truncation=True,
        )
    return _SENTIMENT_PIPELINE


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
    window_start = partition_date - timedelta(days=29)
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
    cutoff_date = partition_date - timedelta(days=30)

    eligible_count = con.execute(
        """
        SELECT count(*)
        FROM read_parquet(?)
        WHERE provider_publish_time IS NOT NULL
          AND CAST(provider_publish_time AS DATE) >= ?
        """,
        [parquet_paths, cutoff_date],
    ).fetchone()[0]

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
                concat(
                    upper(symbol),
                    '::',
                    coalesce(
                        nullif(uuid, ''),
                        nullif(link, ''),
                        nullif(title, '')
                    )
                ) AS dedupe_key,
                ROW_NUMBER() OVER (
                    PARTITION BY dedupe_key
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

    pending_df = con.execute(
        """
        SELECT
            symbol,
            uuid,
            link,
            title,
            summary,
            provider_publish_time,
            ingested_ts
        FROM gold.headlines
        WHERE provider_publish_time >= ? AND provider_publish_time < ?
          AND sentiment IS NULL
        """,
        [window_start, window_end],
    ).fetch_df()

    sentiment_updated = 0
    if not pending_df.empty:
        texts = []
        for _, row in pending_df.iterrows():
            title = row.get("title")
            summary = row.get("summary")
            text = title if isinstance(title, str) and title.strip() else summary
            if not isinstance(text, str) or not text.strip():
                text = ""
            texts.append(text)

        sentiment_pipe = _get_sentiment_pipeline()
        results = sentiment_pipe(
            texts,
            batch_size=SENTIMENT_BATCH_SIZE,
        )
        pending_df = pending_df.copy()
        pending_df["sentiment"] = [
            (result.get("label") or "").lower() for result in results
        ]
        updates_df = pending_df.loc[
            :,
            [
                "symbol",
                "uuid",
                "link",
                "title",
                "provider_publish_time",
                "ingested_ts",
                "sentiment",
            ],
        ]
        con.register("sentiment_updates_df", updates_df)
        con.execute(
            """
            UPDATE gold.headlines AS g
            SET sentiment = u.sentiment
            FROM sentiment_updates_df AS u
            WHERE g.sentiment IS NULL
              AND g.symbol = u.symbol
              AND coalesce(g.uuid, '') = coalesce(u.uuid, '')
              AND coalesce(g.link, '') = coalesce(u.link, '')
              AND coalesce(g.title, '') = coalesce(u.title, '')
              AND g.provider_publish_time = u.provider_publish_time
              AND g.ingested_ts = u.ingested_ts
            """
        )
        sentiment_updated = len(updates_df)

    context.add_output_metadata(
        {
            "table": "gold.headlines",
            "partition": context.partition_key,
            "window_start": str(window_start),
            "window_end": str(window_end),
            "row_count": row_count,
            "sentiment_updated": sentiment_updated,
        }
    )
