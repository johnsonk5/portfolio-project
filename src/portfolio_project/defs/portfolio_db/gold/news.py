import os
from datetime import datetime, timedelta
from pathlib import Path

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.portfolio_db.silver.news import silver_news

PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
GOLD_NEWS_PARTITIONS = DailyPartitionsDefinition(
    start_date=PARTITIONS_START_DATE,
    end_offset=1,
)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
SENTIMENT_BATCH_SIZE = int(os.getenv("NEWS_SENTIMENT_BATCH_SIZE", "32"))
_SENTIMENT_PIPELINE = None


def _headline_dedupe_expr(alias: str) -> str:
    return f"""
        concat(
            upper({alias}.symbol),
            '::',
            coalesce(
                nullif({alias}.uuid, ''),
                nullif({alias}.link, ''),
                nullif({alias}.title, '')
            )
        )
    """


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
            DATA_ROOT / "silver" / "news" / f"date={cursor.strftime('%Y-%m-%d')}" / "news.parquet"
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

    deleted_out_of_window_count = con.execute(
        """
        SELECT count(*)
        FROM gold.headlines
        WHERE provider_publish_time < ?
        """,
        [window_start],
    ).fetchone()[0]
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

    source_sql = """
        CREATE OR REPLACE TEMP TABLE current_gold_headlines AS
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
                {dedupe_expr} AS dedupe_key,
                ROW_NUMBER() OVER (
                    PARTITION BY {dedupe_expr}
                    ORDER BY ingested_ts DESC NULLS LAST
                ) AS rn
            FROM source_news
            WHERE provider_publish_time >= ? AND provider_publish_time < ?
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
            sentiment,
            dedupe_key
        FROM deduped
        WHERE rn = 1
    """.format(dedupe_expr=_headline_dedupe_expr("source_news"))
    con.execute(source_sql, [parquet_paths, window_start, window_end])

    deleted_window_refresh_count = con.execute(
        """
        SELECT count(*)
        FROM gold.headlines AS g
        WHERE g.provider_publish_time >= ?
          AND g.provider_publish_time < ?
          AND {target_dedupe} NOT IN (
              SELECT dedupe_key FROM current_gold_headlines
          )
        """.format(target_dedupe=_headline_dedupe_expr("g")),
        [window_start, window_end],
    ).fetchone()[0]

    merge_sql = """
        MERGE INTO gold.headlines AS target
        USING current_gold_headlines AS source
        ON {target_dedupe} = source.dedupe_key
        WHEN MATCHED THEN UPDATE SET
            asset_id = source.asset_id,
            symbol = source.symbol,
            uuid = source.uuid,
            title = source.title,
            publisher_id = source.publisher_id,
            link = source.link,
            provider_publish_time = source.provider_publish_time,
            type = source.type,
            summary = source.summary,
            query_date = source.query_date,
            ingested_ts = source.ingested_ts,
            sentiment = target.sentiment
        WHEN NOT MATCHED THEN INSERT (
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
        ) VALUES (
            source.asset_id,
            source.symbol,
            source.uuid,
            source.title,
            source.publisher_id,
            source.link,
            source.provider_publish_time,
            source.type,
            source.summary,
            source.query_date,
            source.ingested_ts,
            source.sentiment
        )
    """.format(target_dedupe=_headline_dedupe_expr("target"))

    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(
            """
            DELETE FROM gold.headlines AS g
            WHERE g.provider_publish_time >= ?
              AND g.provider_publish_time < ?
              AND {target_dedupe} NOT IN (
                  SELECT dedupe_key FROM current_gold_headlines
              )
            """.format(target_dedupe=_headline_dedupe_expr("g")),
            [window_start, window_end],
        )
        con.execute(merge_sql)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.execute("DROP TABLE IF EXISTS current_gold_headlines")

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
        pending_df["sentiment"] = [(result.get("label") or "").lower() for result in results]
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
              AND coalesce(g.symbol, '') = coalesce(u.symbol, '')
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
            "rows_inserted": int(row_count or 0),
            "rows_updated": 0,
            "rows_deleted": int(deleted_out_of_window_count or 0)
            + int(deleted_window_refresh_count or 0),
            "deleted_out_of_window_count": int(deleted_out_of_window_count or 0),
            "deleted_window_refresh_count": int(deleted_window_refresh_count or 0),
            "sentiment_updated": sentiment_updated,
            "eligible_count": int(eligible_count or 0),
        }
    )
