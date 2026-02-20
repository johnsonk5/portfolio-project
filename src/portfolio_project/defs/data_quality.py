import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from portfolio_project.defs.duckdb_resource import _acquire_duckdb_lock, _release_duckdb_lock


def _resolve_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "portfolio.duckdb"


def _with_duckdb_connection():
    db_path = _resolve_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = db_path.parent / ".duckdb_write.lock"
    lock_fd = _acquire_duckdb_lock(lock_path)
    con = duckdb.connect(str(db_path))
    try:
        yield con
    finally:
        con.close()
        _release_duckdb_lock(lock_path, lock_fd)


def _get_run_from_context(context):
    if hasattr(context, "dagster_run") and context.dagster_run is not None:
        return context.dagster_run
    run_id = getattr(context, "run_id", None)
    if run_id and hasattr(context, "instance"):
        try:
            return context.instance.get_run_by_id(run_id)
        except Exception:
            return None
    return None


def _table_exists(con, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        is not None
    )


def _dq_float_env(var_name: str, default_value: float) -> float:
    raw = os.getenv(var_name)
    if raw is None:
        return float(default_value)
    try:
        return float(raw)
    except ValueError:
        return float(default_value)


def _dq_row(
    *,
    run_id: str,
    job_name: str,
    partition_key: Optional[str],
    check_name: str,
    status: str,
    measured_value: Optional[float],
    threshold_value: Optional[float],
    details: dict,
    logged_ts: datetime,
) -> dict:
    return {
        "run_id": run_id,
        "job_name": job_name,
        "partition_key": partition_key,
        "check_name": check_name,
        "severity": "RED",
        "status": status,
        "measured_value": measured_value,
        "threshold_value": threshold_value,
        "details_json": json.dumps(details),
        "logged_ts": logged_ts,
    }


def _silver_price_partition_paths(partition_key: str) -> list[str]:
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    day_dir = data_root / "silver" / "prices" / f"date={partition_key}"
    if not day_dir.exists():
        return []
    paths: list[str] = []
    for symbol_dir in day_dir.glob("symbol=*"):
        candidate = symbol_dir / "prices.parquet"
        if candidate.exists():
            paths.append(candidate.as_posix())
    return sorted(paths)


def _write_data_quality_rows(con, rows: list[dict]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS observability")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS observability.data_quality_checks (
            run_id VARCHAR,
            job_name VARCHAR,
            partition_key VARCHAR,
            check_name VARCHAR,
            severity VARCHAR,
            status VARCHAR,
            measured_value DOUBLE,
            threshold_value DOUBLE,
            details_json VARCHAR,
            logged_ts TIMESTAMP
        )
        """
    )
    if not rows:
        return
    con.execute("DELETE FROM observability.data_quality_checks WHERE run_id = ?", [rows[0]["run_id"]])
    for row in rows:
        con.execute(
            """
            INSERT INTO observability.data_quality_checks (
                run_id,
                job_name,
                partition_key,
                check_name,
                severity,
                status,
                measured_value,
                threshold_value,
                details_json,
                logged_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["run_id"],
                row["job_name"],
                row["partition_key"],
                row["check_name"],
                row["severity"],
                row["status"],
                row["measured_value"],
                row["threshold_value"],
                row["details_json"],
                row["logged_ts"],
            ],
        )


def _check_daily_prices(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "daily_prices_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        return [
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_daily_prices_missing_partition",
                status="SKIPPED",
                measured_value=None,
                threshold_value=None,
                details={"reason": "missing_partition_key"},
                logged_ts=now,
            )
        ]

    rows = []
    silver_paths = _silver_price_partition_paths(partition_key)
    if not silver_paths:
        rows.append(
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_silver_prices_partition_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"silver_prices_partition_paths": silver_paths},
                logged_ts=now,
            )
        )
        return rows

    required_cols = {"asset_id", "symbol", "timestamp", "open", "high", "low", "close", "volume", "ingested_ts"}
    present_cols = {
        d[0]
        for d in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [silver_paths]).description
    }
    missing_cols = sorted(required_cols - present_cols)
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_prices_schema_required_columns",
            status="PASS" if not missing_cols else "FAIL",
            measured_value=float(len(missing_cols)),
            threshold_value=0.0,
            details={"missing_columns": missing_cols, "required_columns": sorted(required_cols)},
            logged_ts=now,
        )
    )

    duplicate_count = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT asset_id, timestamp, count(*) AS cnt
            FROM read_parquet(?)
            GROUP BY asset_id, timestamp
            HAVING count(*) > 1
        )
        """,
        [silver_paths],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_prices_uniqueness_asset_id_timestamp",
            status="PASS" if int(duplicate_count or 0) == 0 else "FAIL",
            measured_value=float(duplicate_count or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )

    null_threshold = _dq_float_env("DQ_PRICES_NULL_PCT_THRESHOLD", 0.0)
    nulls = con.execute(
        """
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS asset_id_nulls,
            sum(CASE WHEN symbol IS NULL OR trim(symbol) = '' THEN 1 ELSE 0 END) AS symbol_nulls,
            sum(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS timestamp_nulls,
            sum(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS open_nulls,
            sum(CASE WHEN high IS NULL THEN 1 ELSE 0 END) AS high_nulls,
            sum(CASE WHEN low IS NULL THEN 1 ELSE 0 END) AS low_nulls,
            sum(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS close_nulls,
            sum(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS volume_nulls
        FROM read_parquet(?)
        """,
        [silver_paths],
    ).fetchone()
    row_count = int(nulls[0] or 0)
    null_counts = {
        "asset_id": int(nulls[1] or 0),
        "symbol": int(nulls[2] or 0),
        "timestamp": int(nulls[3] or 0),
        "open": int(nulls[4] or 0),
        "high": int(nulls[5] or 0),
        "low": int(nulls[6] or 0),
        "close": int(nulls[7] or 0),
        "volume": int(nulls[8] or 0),
    }
    null_pcts = {
        col: (float(count) / float(row_count) if row_count > 0 else 0.0)
        for col, count in null_counts.items()
    }
    max_null_pct = max(null_pcts.values()) if null_pcts else 0.0
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_prices_null_thresholds",
            status="PASS" if max_null_pct <= null_threshold else "FAIL",
            measured_value=float(max_null_pct),
            threshold_value=float(null_threshold),
            details={"row_count": row_count, "null_counts": null_counts, "null_pcts": null_pcts},
            logged_ts=now,
        )
    )

    range_violations = con.execute(
        """
        SELECT sum(
            CASE
                WHEN (open IS NOT NULL AND open < 0)
                  OR (high IS NOT NULL AND high < 0)
                  OR (low IS NOT NULL AND low < 0)
                  OR (close IS NOT NULL AND close < 0)
                  OR (volume IS NOT NULL AND volume < 0)
                  OR (trade_count IS NOT NULL AND trade_count < 0)
                  OR (vwap IS NOT NULL AND vwap < 0)
                  OR (high IS NOT NULL AND low IS NOT NULL AND high < low)
                  OR (open IS NOT NULL AND high IS NOT NULL AND open > high)
                  OR (open IS NOT NULL AND low IS NOT NULL AND open < low)
                  OR (close IS NOT NULL AND high IS NOT NULL AND close > high)
                  OR (close IS NOT NULL AND low IS NOT NULL AND close < low)
                THEN 1 ELSE 0
            END
        )
        FROM read_parquet(?)
        """,
        [silver_paths],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_prices_ranges",
            status="PASS" if int(range_violations or 0) == 0 else "FAIL",
            measured_value=float(range_violations or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )

    if not _table_exists(con, "gold", "prices"):
        rows.append(
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_gold_prices_table_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"table": "gold.prices"},
                logged_ts=now,
            )
        )
        return rows

    gold_required = {"asset_id", "symbol", "trade_date", "open", "high", "low", "close", "volume", "dollar_volume"}
    gold_present = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'gold'
              AND table_name = 'prices'
            """
        ).fetchall()
    }
    gold_missing = sorted(gold_required - gold_present)
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_gold_prices_schema_required_columns",
            status="PASS" if not gold_missing else "FAIL",
            measured_value=float(len(gold_missing)),
            threshold_value=0.0,
            details={"missing_columns": gold_missing, "required_columns": sorted(gold_required)},
            logged_ts=now,
        )
    )

    gold_duplicates = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT asset_id, trade_date, count(*) AS cnt
            FROM gold.prices
            WHERE trade_date = ?
            GROUP BY asset_id, trade_date
            HAVING count(*) > 1
        )
        """,
        [partition_key],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_gold_prices_uniqueness_asset_id_trade_date",
            status="PASS" if int(gold_duplicates or 0) == 0 else "FAIL",
            measured_value=float(gold_duplicates or 0),
            threshold_value=0.0,
            details={"trade_date": partition_key},
            logged_ts=now,
        )
    )

    gold_null_threshold = _dq_float_env("DQ_GOLD_PRICES_CORE_NULL_PCT_THRESHOLD", 0.0)
    gold_nulls = con.execute(
        """
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS asset_id_nulls,
            sum(CASE WHEN symbol IS NULL OR trim(symbol) = '' THEN 1 ELSE 0 END) AS symbol_nulls,
            sum(CASE WHEN trade_date IS NULL THEN 1 ELSE 0 END) AS trade_date_nulls,
            sum(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS open_nulls,
            sum(CASE WHEN high IS NULL THEN 1 ELSE 0 END) AS high_nulls,
            sum(CASE WHEN low IS NULL THEN 1 ELSE 0 END) AS low_nulls,
            sum(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS close_nulls,
            sum(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS volume_nulls,
            sum(CASE WHEN dollar_volume IS NULL THEN 1 ELSE 0 END) AS dollar_volume_nulls
        FROM gold.prices
        WHERE trade_date = ?
        """,
        [partition_key],
    ).fetchone()
    gold_row_count = int(gold_nulls[0] or 0)
    core_counts = {
        "asset_id": int(gold_nulls[1] or 0),
        "symbol": int(gold_nulls[2] or 0),
        "trade_date": int(gold_nulls[3] or 0),
        "open": int(gold_nulls[4] or 0),
        "high": int(gold_nulls[5] or 0),
        "low": int(gold_nulls[6] or 0),
        "close": int(gold_nulls[7] or 0),
        "volume": int(gold_nulls[8] or 0),
        "dollar_volume": int(gold_nulls[9] or 0),
    }
    core_pcts = {
        k: (float(v) / float(gold_row_count) if gold_row_count > 0 else 0.0)
        for k, v in core_counts.items()
    }
    max_core_pct = max(core_pcts.values()) if core_pcts else 0.0
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_gold_prices_core_null_thresholds",
            status="PASS" if max_core_pct <= gold_null_threshold else "FAIL",
            measured_value=float(max_core_pct),
            threshold_value=float(gold_null_threshold),
            details={"row_count": gold_row_count, "null_counts": core_counts, "null_pcts": core_pcts},
            logged_ts=now,
        )
    )

    gold_range_violations = con.execute(
        """
        SELECT sum(
            CASE
                WHEN (open IS NOT NULL AND open < 0)
                  OR (high IS NOT NULL AND high < 0)
                  OR (low IS NOT NULL AND low < 0)
                  OR (close IS NOT NULL AND close < 0)
                  OR (volume IS NOT NULL AND volume < 0)
                  OR (trade_count IS NOT NULL AND trade_count < 0)
                  OR (vwap IS NOT NULL AND vwap < 0)
                  OR (dollar_volume IS NOT NULL AND dollar_volume < 0)
                  OR (realized_vol_21d IS NOT NULL AND realized_vol_21d < 0)
                  OR (high IS NOT NULL AND low IS NOT NULL AND high < low)
                  OR (open IS NOT NULL AND high IS NOT NULL AND open > high)
                  OR (open IS NOT NULL AND low IS NOT NULL AND open < low)
                  OR (close IS NOT NULL AND high IS NOT NULL AND close > high)
                  OR (close IS NOT NULL AND low IS NOT NULL AND close < low)
                THEN 1 ELSE 0
            END
        )
        FROM gold.prices
        WHERE trade_date = ?
        """,
        [partition_key],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_gold_prices_ranges",
            status="PASS" if int(gold_range_violations or 0) == 0 else "FAIL",
            measured_value=float(gold_range_violations or 0),
            threshold_value=0.0,
            details={"trade_date": partition_key},
            logged_ts=now,
        )
    )
    return rows


def _check_daily_news(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "daily_news_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        return [
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_daily_news_missing_partition",
                status="SKIPPED",
                measured_value=None,
                threshold_value=None,
                details={"reason": "missing_partition_key"},
                logged_ts=now,
            )
        ]

    rows = []
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    news_path = data_root / "silver" / "news" / f"date={partition_key}" / "news.parquet"
    if not news_path.exists():
        rows.append(
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_silver_news_partition_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"silver_news_partition_path": news_path.as_posix()},
                logged_ts=now,
            )
        )
        return rows

    required_cols = {
        "asset_id",
        "symbol",
        "uuid",
        "title",
        "publisher_id",
        "link",
        "provider_publish_time",
        "type",
        "summary",
        "query_date",
        "ingested_ts",
    }
    present_cols = {
        d[0]
        for d in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [news_path.as_posix()]).description
    }
    missing_cols = sorted(required_cols - present_cols)
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_news_schema_required_columns",
            status="PASS" if not missing_cols else "FAIL",
            measured_value=float(len(missing_cols)),
            threshold_value=0.0,
            details={"missing_columns": missing_cols, "required_columns": sorted(required_cols)},
            logged_ts=now,
        )
    )

    duplicate_count = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT
                upper(trim(symbol)) AS symbol_key,
                coalesce(nullif(uuid, ''), nullif(link, ''), nullif(title, '')) AS story_key,
                provider_publish_time,
                count(*) AS cnt
            FROM read_parquet(?)
            GROUP BY symbol_key, story_key, provider_publish_time
            HAVING count(*) > 1
        )
        """,
        [news_path.as_posix()],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_news_uniqueness_symbol_story_publish_time",
            status="PASS" if int(duplicate_count or 0) == 0 else "FAIL",
            measured_value=float(duplicate_count or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )

    both_missing_threshold = _dq_float_env("DQ_NEWS_BOTH_TITLE_LINK_NULL_PCT_THRESHOLD", 0.0)
    nulls = con.execute(
        """
        SELECT
            count(*) AS row_count,
            sum(
                CASE
                    WHEN (title IS NULL OR trim(title) = '')
                     AND (link IS NULL OR trim(link) = '')
                    THEN 1 ELSE 0
                END
            ) AS title_and_link_missing
        FROM read_parquet(?)
        """,
        [news_path.as_posix()],
    ).fetchone()
    row_count = int(nulls[0] or 0)
    both_missing = int(nulls[1] or 0)
    both_missing_pct = float(both_missing) / float(row_count) if row_count > 0 else 0.0
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_news_null_threshold_title_or_link",
            status="PASS" if both_missing_pct <= both_missing_threshold else "FAIL",
            measured_value=float(both_missing_pct),
            threshold_value=float(both_missing_threshold),
            details={"row_count": row_count, "title_and_link_missing": both_missing},
            logged_ts=now,
        )
    )
    return rows


def _check_wikipedia_activity(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "wikipedia_activity_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        return [
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_wikipedia_activity_missing_partition",
                status="SKIPPED",
                measured_value=None,
                threshold_value=None,
                details={"reason": "missing_partition_key"},
                logged_ts=now,
            )
        ]

    rows = []
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    wiki_path = data_root / "silver" / "wikipedia_pageviews" / f"view_date={partition_key}" / "data_0.parquet"
    if not wiki_path.exists():
        rows.append(
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_silver_wikipedia_partition_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"silver_wikipedia_partition_path": wiki_path.as_posix()},
                logged_ts=now,
            )
        )
        return rows

    required_cols = {"asset_id", "granularity", "view_date", "views", "ingested_ts"}
    present_cols = {
        d[0]
        for d in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [wiki_path.as_posix()]).description
    }
    missing_cols = sorted(required_cols - present_cols)
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_wikipedia_schema_required_columns",
            status="PASS" if not missing_cols else "FAIL",
            measured_value=float(len(missing_cols)),
            threshold_value=0.0,
            details={"missing_columns": missing_cols, "required_columns": sorted(required_cols)},
            logged_ts=now,
        )
    )

    duplicate_count = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT asset_id, view_date, granularity, count(*) AS cnt
            FROM read_parquet(?)
            GROUP BY asset_id, view_date, granularity
            HAVING count(*) > 1
        )
        """,
        [wiki_path.as_posix()],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_wikipedia_uniqueness_asset_id_view_date_granularity",
            status="PASS" if int(duplicate_count or 0) == 0 else "FAIL",
            measured_value=float(duplicate_count or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )

    asset_id_null_threshold = _dq_float_env("DQ_WIKIPEDIA_ASSET_ID_NULL_PCT_THRESHOLD", 0.25)
    nulls = con.execute(
        """
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS asset_id_nulls
        FROM read_parquet(?)
        """,
        [wiki_path.as_posix()],
    ).fetchone()
    row_count = int(nulls[0] or 0)
    asset_id_nulls = int(nulls[1] or 0)
    asset_id_null_pct = float(asset_id_nulls) / float(row_count) if row_count > 0 else 0.0
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_wikipedia_null_threshold_asset_id",
            status="PASS" if asset_id_null_pct <= asset_id_null_threshold else "FAIL",
            measured_value=float(asset_id_null_pct),
            threshold_value=float(asset_id_null_threshold),
            details={"row_count": row_count, "asset_id_nulls": asset_id_nulls},
            logged_ts=now,
        )
    )

    range_violations = con.execute(
        """
        SELECT sum(CASE WHEN views IS NOT NULL AND views < 0 THEN 1 ELSE 0 END)
        FROM read_parquet(?)
        """,
        [wiki_path.as_posix()],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_wikipedia_range_views_non_negative",
            status="PASS" if int(range_violations or 0) == 0 else "FAIL",
            measured_value=float(range_violations or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )
    return rows


def _check_sp500(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "sp500_update_job":
        return []

    now = datetime.now(timezone.utc)
    rows = []
    if not _table_exists(con, "silver", "ref_sp500"):
        rows.append(
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_silver_ref_sp500_table_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"table": "silver.ref_sp500"},
                logged_ts=now,
            )
        )
        return rows

    required_cols = {"symbol", "security", "asset_id"}
    present_cols = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'silver'
              AND table_name = 'ref_sp500'
            """
        ).fetchall()
    }
    missing_cols = sorted(required_cols - present_cols)
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_ref_sp500_schema_required_columns",
            status="PASS" if not missing_cols else "FAIL",
            measured_value=float(len(missing_cols)),
            threshold_value=0.0,
            details={"missing_columns": missing_cols, "required_columns": sorted(required_cols)},
            logged_ts=now,
        )
    )

    duplicate_symbols = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT upper(trim(symbol)) AS symbol_key, count(*) AS cnt
            FROM silver.ref_sp500
            GROUP BY symbol_key
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_ref_sp500_uniqueness_symbol",
            status="PASS" if int(duplicate_symbols or 0) == 0 else "FAIL",
            measured_value=float(duplicate_symbols or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )

    symbol_null_threshold = _dq_float_env("DQ_SP500_SYMBOL_NULL_PCT_THRESHOLD", 0.0)
    asset_id_null_threshold = _dq_float_env("DQ_SP500_ASSET_ID_NULL_PCT_THRESHOLD", 0.25)
    nulls = con.execute(
        """
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN symbol IS NULL OR trim(symbol) = '' THEN 1 ELSE 0 END) AS symbol_nulls,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS asset_id_nulls
        FROM silver.ref_sp500
        """
    ).fetchone()
    row_count = int(nulls[0] or 0)
    symbol_null_pct = float(int(nulls[1] or 0)) / float(row_count) if row_count > 0 else 0.0
    asset_id_null_pct = float(int(nulls[2] or 0)) / float(row_count) if row_count > 0 else 0.0
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_ref_sp500_null_thresholds",
            status=(
                "PASS"
                if symbol_null_pct <= symbol_null_threshold and asset_id_null_pct <= asset_id_null_threshold
                else "FAIL"
            ),
            measured_value=float(max(symbol_null_pct, asset_id_null_pct)),
            threshold_value=float(max(symbol_null_threshold, asset_id_null_threshold)),
            details={
                "symbol_null_pct": symbol_null_pct,
                "asset_id_null_pct": asset_id_null_pct,
                "symbol_null_threshold": symbol_null_threshold,
                "asset_id_null_threshold": asset_id_null_threshold,
            },
            logged_ts=now,
        )
    )
    return rows


def _latest_tranco_csv() -> Optional[Path]:
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    tranco_root = data_root / "bronze" / "tranco"
    if not tranco_root.exists():
        return None
    latest = None
    latest_date = None
    for entry in tranco_root.iterdir():
        if not entry.is_dir() or not entry.name.startswith("date="):
            continue
        date_str = entry.name.split("=", 1)[-1]
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        candidate = entry / "tranco.csv"
        if not candidate.exists():
            continue
        if latest_date is None or d > latest_date:
            latest_date = d
            latest = candidate
    return latest


def _check_tranco(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "tranco_update_job":
        return []
    now = datetime.now(timezone.utc)
    path = _latest_tranco_csv()
    if path is None:
        return [
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_bronze_tranco_snapshot_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"reason": "no_tranco_snapshot_found"},
                logged_ts=now,
            )
        ]

    path_sql = path.as_posix()
    rows = []
    sample = con.execute(
        """
        SELECT rank, domain
        FROM read_csv_auto(?, header = false, columns = {'rank': 'BIGINT', 'domain': 'VARCHAR'})
        LIMIT 1
        """,
        [path_sql],
    ).fetchone()
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_bronze_tranco_schema_rank_domain",
            status="PASS" if sample is not None else "FAIL",
            measured_value=0.0 if sample is not None else 1.0,
            threshold_value=0.0,
            details={"tranco_path": path_sql},
            logged_ts=now,
        )
    )

    dup_rank = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT rank, count(*) AS cnt
            FROM read_csv_auto(?, header = false, columns = {'rank': 'BIGINT', 'domain': 'VARCHAR'})
            GROUP BY rank
            HAVING count(*) > 1
        )
        """,
        [path_sql],
    ).fetchone()[0]
    dup_domain = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT lower(trim(domain)) AS domain_key, count(*) AS cnt
            FROM read_csv_auto(?, header = false, columns = {'rank': 'BIGINT', 'domain': 'VARCHAR'})
            GROUP BY domain_key
            HAVING count(*) > 1
        )
        """,
        [path_sql],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_bronze_tranco_uniqueness_rank_and_domain",
            status="PASS" if int(dup_rank or 0) == 0 and int(dup_domain or 0) == 0 else "FAIL",
            measured_value=float((dup_rank or 0) + (dup_domain or 0)),
            threshold_value=0.0,
            details={"duplicate_rank_count": int(dup_rank or 0), "duplicate_domain_count": int(dup_domain or 0)},
            logged_ts=now,
        )
    )

    range_violations = con.execute(
        """
        SELECT sum(
            CASE
                WHEN rank IS NULL OR rank < 1 OR rank > 1000000
                  OR domain IS NULL OR trim(domain) = ''
                THEN 1 ELSE 0
            END
        )
        FROM read_csv_auto(?, header = false, columns = {'rank': 'BIGINT', 'domain': 'VARCHAR'})
        """,
        [path_sql],
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_bronze_tranco_ranges",
            status="PASS" if int(range_violations or 0) == 0 else "FAIL",
            measured_value=float(range_violations or 0),
            threshold_value=0.0,
            details={},
            logged_ts=now,
        )
    )
    return rows


def _check_asset_status_updates(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "asset_status_updates_job":
        return []

    now = datetime.now(timezone.utc)
    rows = []
    if not _table_exists(con, "silver", "assets"):
        rows.append(
            _dq_row(
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
                check_name="dq_silver_assets_table_exists",
                status="FAIL",
                measured_value=0.0,
                threshold_value=1.0,
                details={"table": "silver.assets"},
                logged_ts=now,
            )
        )
        return rows

    required_cols = {"asset_id", "symbol", "is_active"}
    present_cols = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'silver'
              AND table_name = 'assets'
            """
        ).fetchall()
    }
    missing_cols = sorted(required_cols - present_cols)
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_assets_schema_required_columns",
            status="PASS" if not missing_cols else "FAIL",
            measured_value=float(len(missing_cols)),
            threshold_value=0.0,
            details={"missing_columns": missing_cols, "required_columns": sorted(required_cols)},
            logged_ts=now,
        )
    )

    dup_asset_id = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT asset_id, count(*) AS cnt
            FROM silver.assets
            GROUP BY asset_id
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    dup_symbol = con.execute(
        """
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT upper(trim(symbol)) AS symbol_key, count(*) AS cnt
            FROM silver.assets
            GROUP BY symbol_key
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_assets_uniqueness_asset_id_and_symbol",
            status="PASS" if int(dup_asset_id or 0) == 0 and int(dup_symbol or 0) == 0 else "FAIL",
            measured_value=float((dup_asset_id or 0) + (dup_symbol or 0)),
            threshold_value=0.0,
            details={"duplicate_asset_id_count": int(dup_asset_id or 0), "duplicate_symbol_count": int(dup_symbol or 0)},
            logged_ts=now,
        )
    )

    null_threshold = _dq_float_env("DQ_ASSETS_CORE_NULL_PCT_THRESHOLD", 0.0)
    nulls = con.execute(
        """
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS asset_id_nulls,
            sum(CASE WHEN symbol IS NULL OR trim(symbol) = '' THEN 1 ELSE 0 END) AS symbol_nulls,
            sum(CASE WHEN is_active IS NULL THEN 1 ELSE 0 END) AS is_active_nulls
        FROM silver.assets
        """
    ).fetchone()
    row_count = int(nulls[0] or 0)
    null_pcts = {
        "asset_id": (float(int(nulls[1] or 0)) / float(row_count) if row_count > 0 else 0.0),
        "symbol": (float(int(nulls[2] or 0)) / float(row_count) if row_count > 0 else 0.0),
        "is_active": (float(int(nulls[3] or 0)) / float(row_count) if row_count > 0 else 0.0),
    }
    max_null_pct = max(null_pcts.values()) if null_pcts else 0.0
    rows.append(
        _dq_row(
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
            check_name="dq_silver_assets_null_thresholds",
            status="PASS" if max_null_pct <= null_threshold else "FAIL",
            measured_value=float(max_null_pct),
            threshold_value=float(null_threshold),
            details={"row_count": row_count, "null_pcts": null_pcts},
            logged_ts=now,
        )
    )
    return rows


def write_data_quality_checks(context) -> None:
    run = _get_run_from_context(context)
    run_id = getattr(run, "run_id", None) or getattr(context, "run_id", None)
    job_name = getattr(run, "job_name", None) or getattr(context, "job_name", None)
    tags = run.tags or {} if run else {}
    partition_key = tags.get("dagster/partition")
    if not run_id or not job_name:
        return

    now = datetime.now(timezone.utc)

    def _run_checks(con) -> list[dict]:
        rows = []
        checks = [
            ("dq_daily_prices", _check_daily_prices),
            ("dq_daily_news", _check_daily_news),
            ("dq_wikipedia_activity", _check_wikipedia_activity),
            ("dq_sp500_update", _check_sp500),
            ("dq_tranco_update", _check_tranco),
            ("dq_asset_status_updates", _check_asset_status_updates),
        ]
        for check_name, check_fn in checks:
            try:
                rows.extend(check_fn(con, run_id, job_name, partition_key))
            except Exception as exc:
                rows.append(
                    _dq_row(
                        run_id=run_id,
                        job_name=job_name,
                        partition_key=partition_key,
                        check_name=check_name,
                        status="FAIL",
                        measured_value=None,
                        threshold_value=None,
                        details={"reason": "check_execution_error", "error_message": str(exc)},
                        logged_ts=now,
                    )
                )
        return rows

    resources = getattr(context, "resources", None)
    if resources is not None and hasattr(resources, "duckdb"):
        con = resources.duckdb
        rows = _run_checks(con)
        _write_data_quality_rows(con, rows)
        try:
            con.commit()
        except Exception:
            pass
        return

    for con in _with_duckdb_connection():
        rows = _run_checks(con)
        _write_data_quality_rows(con, rows)
