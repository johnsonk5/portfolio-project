import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    failure_hook,
    run_status_sensor,
    success_hook,
)

from portfolio_project.defs.data_quality import write_data_quality_checks
from portfolio_project.defs.duckdb_resource import _acquire_duckdb_lock, _release_duckdb_lock


def _to_utc_datetime(timestamp: Optional[float]) -> Optional[datetime]:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def _metadata_int(entry) -> Optional[int]:
    for attr in ("int_value", "value", "float_value"):
        if hasattr(entry, attr):
            value = getattr(entry, attr)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                if value.is_integer():
                    return int(value)
                return None
    return None


def _get_run_records(context, run_id: str):
    try:
        return context.instance.all_logs(run_id)
    except Exception as exc:
        try:
            context.log.warning("Run log all_logs failed for run_id=%s: %s", run_id, exc)
        except Exception:
            pass
        return []


def _collect_materialization_metrics(records) -> dict:
    assets_materialized_count = 0
    row_count_total = 0
    rows_inserted_total = 0
    rows_updated_total = 0
    rows_deleted_total = 0
    found_row_count = False
    found_mutations = False

    for record in records:
        entry = getattr(record, "event_log_entry", None) or record
        event = getattr(entry, "dagster_event", None)
        if event is None or event.event_type != DagsterEventType.ASSET_MATERIALIZATION:
            continue
        assets_materialized_count += 1
        if event.event_specific_data is None:
            continue
        materialization = event.event_specific_data.materialization
        if materialization is None or materialization.metadata is None:
            continue
        for key, value in materialization.metadata.items():
            metric = _metadata_int(value)
            if metric is None:
                continue
            if key == "row_count":
                row_count_total += metric
                found_row_count = True
            elif key == "rows_inserted":
                rows_inserted_total += metric
                found_mutations = True
            elif key == "rows_updated":
                rows_updated_total += metric
                found_mutations = True
            elif key == "rows_deleted":
                rows_deleted_total += metric
                found_mutations = True

    return {
        "assets_materialized_count": assets_materialized_count,
        "row_count": row_count_total if found_row_count else None,
        "rows_inserted": rows_inserted_total if found_mutations else None,
        "rows_updated": rows_updated_total if found_mutations else None,
        "rows_deleted": rows_deleted_total if found_mutations else None,
    }


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


def _get_duckdb_connection(context):
    resources = getattr(context, "resources", None)
    if resources is not None and hasattr(resources, "duckdb"):
        return resources.duckdb, False
    return None, True


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


def _get_run_times_from_instance(context, run_id: str):
    try:
        if hasattr(context.instance, "get_run_record_by_id"):
            rec = context.instance.get_run_record_by_id(run_id)
            if rec is not None:
                return (
                    _to_utc_datetime(getattr(rec, "start_time", None)),
                    _to_utc_datetime(getattr(rec, "end_time", None)),
                )
        if hasattr(context.instance, "get_run_stats"):
            stats = context.instance.get_run_stats(run_id)
            if stats is not None:
                return (
                    _to_utc_datetime(getattr(stats, "start_time", None)),
                    _to_utc_datetime(getattr(stats, "end_time", None)),
                )
    except Exception as exc:
        try:
            context.log.warning("Run log stats lookup failed for run_id=%s: %s", run_id, exc)
        except Exception:
            pass
    return None, None


def _run_timestamp(run, *fields: str) -> Optional[datetime]:
    if run is None:
        return None
    for field in fields:
        if hasattr(run, field):
            value = getattr(run, field)
            if value is not None:
                return _to_utc_datetime(value)
    return None


def _record_timestamp(record) -> Optional[datetime]:
    entry = getattr(record, "event_log_entry", None) or record
    for field in ("timestamp", "created_at", "create_timestamp", "event_timestamp"):
        if hasattr(entry, field):
            value = getattr(entry, field)
            if value is not None:
                return _to_utc_datetime(value)
    return None


def _write_freshness_check_rows(con, rows: list[dict]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS observability")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS observability.data_freshness_checks (
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
    con.execute("DELETE FROM observability.data_freshness_checks WHERE run_id = ?", [rows[0]["run_id"]])
    for row in rows:
        con.execute(
            """
            INSERT INTO observability.data_freshness_checks (
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


def _is_us_trading_day(partition_key: str) -> bool:
    try:
        day = datetime.strptime(partition_key, "%Y-%m-%d").date()
    except ValueError:
        return False

    # Fast fallback: weekends are never trading days.
    if day.weekday() >= 5:
        return False

    # Prefer NYSE calendar if available to account for market holidays.
    try:
        import pandas_market_calendars as mcal

        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(start_date=day, end_date=day)
        return not schedule.empty
    except Exception:
        return True


def _check_prices_freshness(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "daily_prices_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": "prices_active_symbol_coverage",
                "severity": "RED",
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            }
        ]

    if not _is_us_trading_day(partition_key):
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": "prices_active_symbol_coverage",
                "severity": "RED",
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": 0.0,
                "details_json": json.dumps(
                    {
                        "reason": "non_trading_day",
                        "market": "NYSE",
                    }
                ),
                "logged_ts": now,
            }
        ]

    active_symbol_count = con.execute(
        """
        SELECT count(*)
        FROM (
            SELECT DISTINCT upper(trim(symbol)) AS symbol
            FROM silver.assets
            WHERE is_active = TRUE
              AND symbol IS NOT NULL
              AND trim(symbol) <> ''
        )
        """
    ).fetchone()[0]

    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    silver_path = data_root / "silver" / "prices" / f"date={partition_key}" / "prices.parquet"
    present_symbol_count = 0
    missing_symbol_count = int(active_symbol_count)
    missing_symbol_samples = []
    if silver_path.exists():
        present_symbol_count, missing_symbol_count = con.execute(
            """
            WITH active_symbols AS (
                SELECT DISTINCT upper(trim(symbol)) AS symbol
                FROM silver.assets
                WHERE is_active = TRUE
                  AND symbol IS NOT NULL
                  AND trim(symbol) <> ''
            ),
            present_symbols AS (
                SELECT DISTINCT upper(trim(symbol)) AS symbol
                FROM read_parquet(?)
                WHERE symbol IS NOT NULL
                  AND trim(symbol) <> ''
            ),
            missing_symbols AS (
                SELECT a.symbol
                FROM active_symbols AS a
                LEFT JOIN present_symbols AS p
                    ON a.symbol = p.symbol
                WHERE p.symbol IS NULL
            )
            SELECT
                (SELECT count(*) FROM present_symbols) AS present_symbol_count,
                (SELECT count(*) FROM missing_symbols) AS missing_symbol_count
            """,
            [silver_path.as_posix()],
        ).fetchone()
        missing_symbol_samples = [
            row[0]
            for row in con.execute(
                """
                WITH active_symbols AS (
                    SELECT DISTINCT upper(trim(symbol)) AS symbol
                    FROM silver.assets
                    WHERE is_active = TRUE
                      AND symbol IS NOT NULL
                      AND trim(symbol) <> ''
                ),
                present_symbols AS (
                    SELECT DISTINCT upper(trim(symbol)) AS symbol
                    FROM read_parquet(?)
                    WHERE symbol IS NOT NULL
                      AND trim(symbol) <> ''
                )
                SELECT a.symbol
                FROM active_symbols AS a
                LEFT JOIN present_symbols AS p
                    ON a.symbol = p.symbol
                WHERE p.symbol IS NULL
                ORDER BY a.symbol
                LIMIT 25
                """,
                [silver_path.as_posix()],
            ).fetchall()
        ]

    missing_symbol_count = int(missing_symbol_count)
    status = "PASS" if missing_symbol_count == 0 else "FAIL"

    return [
        {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": "prices_active_symbol_coverage",
            "severity": "RED",
            "status": status,
            "measured_value": float(missing_symbol_count),
            "threshold_value": 0.0,
            "details_json": json.dumps(
                {
                    "active_symbol_count": int(active_symbol_count),
                    "symbols_with_prices": int(present_symbol_count),
                    "missing_symbol_count": int(missing_symbol_count),
                    "missing_symbol_samples": missing_symbol_samples,
                    "silver_partition_path": silver_path.as_posix(),
                }
            ),
            "logged_ts": now,
        }
    ]


def _check_wikipedia_freshness(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "wikipedia_activity_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": "wikipedia_assets_with_views_min_count",
                "severity": "RED",
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
        ]

    min_assets_with_views = int(os.getenv("WIKIPEDIA_MIN_ASSETS_WITH_VIEWS", "400"))

    eligible_asset_count = con.execute(
        """
        SELECT count(*)
        FROM silver.assets
        WHERE is_active = TRUE
          AND wikipedia_title IS NOT NULL
          AND trim(wikipedia_title) <> ''
        """
    ).fetchone()[0]

    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    wiki_path = data_root / "silver" / "wikipedia_pageviews" / f"view_date={partition_key}" / "data_0.parquet"
    assets_with_views = 0
    if wiki_path.exists():
        assets_with_views = con.execute(
            """
            SELECT count(DISTINCT asset_id)
            FROM read_parquet(?)
            WHERE asset_id IS NOT NULL
              AND views IS NOT NULL
            """,
            [wiki_path.as_posix()],
        ).fetchone()[0]

    missing_assets = max(int(eligible_asset_count) - int(assets_with_views), 0)
    min_count_status = "PASS" if int(assets_with_views) >= min_assets_with_views else "FAIL"

    details = {
        "eligible_asset_count": int(eligible_asset_count),
        "assets_with_views": int(assets_with_views),
        "missing_assets": int(missing_assets),
        "wikipedia_partition_path": wiki_path.as_posix(),
    }
    return [
        {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": "wikipedia_assets_with_views_min_count",
            "severity": "RED",
            "status": min_count_status,
            "measured_value": float(assets_with_views),
            "threshold_value": float(min_assets_with_views),
            "details_json": json.dumps(details),
            "logged_ts": now,
        },
    ]


def _check_news_freshness(con, run_id: str, job_name: str, partition_key: Optional[str]) -> list[dict]:
    if job_name != "daily_news_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": "daily_news_partition_row_count",
                "severity": "RED",
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            }
        ]

    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    news_path = data_root / "silver" / "news" / f"date={partition_key}" / "news.parquet"
    row_count = 0
    if news_path.exists():
        row_count = con.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [news_path.as_posix()],
        ).fetchone()[0]

    return [
        {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": "daily_news_partition_row_count",
            "severity": "RED",
            "status": "PASS" if int(row_count) > 0 else "FAIL",
            "measured_value": float(row_count),
            "threshold_value": 1.0,
            "details_json": json.dumps({"silver_news_partition_path": news_path.as_posix()}),
            "logged_ts": now,
        }
    ]


def _write_freshness_checks(context) -> None:
    run = _get_run_from_context(context)
    run_id = getattr(run, "run_id", None) or getattr(context, "run_id", None)
    job_name = getattr(run, "job_name", None) or getattr(context, "job_name", None)
    tags = run.tags or {} if run else {}
    partition_key = tags.get("dagster/partition")
    if not run_id or not job_name:
        return

    now = datetime.now(timezone.utc)

    def _run_checks_with_isolation(connection) -> list[dict]:
        check_rows = []
        checks = [
            ("prices_active_symbol_coverage", _check_prices_freshness),
            ("wikipedia_assets_with_views_min_count", _check_wikipedia_freshness),
            ("daily_news_partition_row_count", _check_news_freshness),
        ]
        for check_name, check_fn in checks:
            try:
                check_rows.extend(check_fn(connection, run_id, job_name, partition_key))
            except Exception as exc:
                check_rows.append(
                    {
                        "run_id": run_id,
                        "job_name": job_name,
                        "partition_key": partition_key,
                        "check_name": check_name,
                        "severity": "RED",
                        "status": "FAIL",
                        "measured_value": None,
                        "threshold_value": None,
                        "details_json": json.dumps(
                            {
                                "reason": "check_execution_error",
                                "error_message": str(exc),
                            }
                        ),
                        "logged_ts": now,
                    }
                )
        return check_rows

    con, _ = _get_duckdb_connection(context)
    if con is None:
        for owned_con in _with_duckdb_connection():
            check_rows = _run_checks_with_isolation(owned_con)
            _write_freshness_check_rows(owned_con, check_rows)
        return

    check_rows = _run_checks_with_isolation(con)
    _write_freshness_check_rows(con, check_rows)
    try:
        con.commit()
    except Exception:
        pass


def _write_run_log(context, status: str, error_message: Optional[str] = None) -> None:
    run = _get_run_from_context(context)
    run_id = getattr(run, "run_id", None) or getattr(context, "run_id", None)
    job_name = getattr(run, "job_name", None) or getattr(context, "job_name", None)
    if run_id:
        start_dt, end_dt = _get_run_times_from_instance(context, run_id)
    else:
        start_dt, end_dt = None, None
    if start_dt is None or end_dt is None:
        start_dt = start_dt or _run_timestamp(run, "start_time", "create_timestamp")
        end_dt = end_dt or _run_timestamp(run, "end_time", "update_timestamp")
    records = _get_run_records(context, run_id) if run_id else []
    context.log.info(
        "Run log timing debug run_id=%s start=%s end=%s records=%s",
        run_id,
        start_dt,
        end_dt,
        len(records),
    )
    if records and (start_dt is None or end_dt is None):
        timestamps = [ts for ts in (_record_timestamp(r) for r in records) if ts is not None]
        if timestamps:
            context.log.info(
                "Run log timing debug timestamps min=%s max=%s",
                min(timestamps),
                max(timestamps),
            )
        if timestamps:
            if start_dt is None:
                start_dt = min(timestamps)
            if end_dt is None:
                end_dt = max(timestamps)
    context.log.info(
        "Run log timing resolved run_id=%s start=%s end=%s",
        run_id,
        start_dt,
        end_dt,
    )
    end_dt = end_dt or datetime.now(timezone.utc)
    duration_seconds = (
        (end_dt - start_dt).total_seconds() if start_dt and end_dt else None
    )
    tags = run.tags or {} if run else {}
    partition_key = tags.get("dagster/partition")
    tags_json = json.dumps(tags) if tags else None

    metrics = _collect_materialization_metrics(records)

    db_path = _resolve_duckdb_path()
    context.log.info(
        "Run log hook fired for run_id=%s job=%s status=%s db=%s",
        run_id,
        job_name,
        status,
        db_path,
    )
    con, needs_owned = _get_duckdb_connection(context)
    if con is not None:
        con.execute("CREATE SCHEMA IF NOT EXISTS observability")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS observability.run_log (
                run_id VARCHAR,
                job_name VARCHAR,
                status VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds DOUBLE,
                partition_key VARCHAR,
                tags_json VARCHAR,
                assets_materialized_count BIGINT,
                row_count BIGINT,
                rows_inserted BIGINT,
                rows_updated BIGINT,
                rows_deleted BIGINT,
                error_message VARCHAR,
                logged_ts TIMESTAMP
            )
            """
        )
        con.execute("DELETE FROM observability.run_log WHERE run_id = ?", [run_id])
        con.execute(
            """
            INSERT INTO observability.run_log (
                run_id,
                job_name,
                status,
                start_time,
                end_time,
                duration_seconds,
                partition_key,
                tags_json,
                assets_materialized_count,
                row_count,
                rows_inserted,
                rows_updated,
                rows_deleted,
                error_message,
                logged_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                job_name,
                status,
                start_dt,
                end_dt,
                duration_seconds,
                partition_key,
                tags_json,
                metrics["assets_materialized_count"],
                metrics["row_count"],
                metrics["rows_inserted"],
                metrics["rows_updated"],
                metrics["rows_deleted"],
                error_message,
                datetime.now(timezone.utc),
            ],
        )
        try:
            con.commit()
        except Exception:
            pass
        try:
            row_count = con.execute(
                "SELECT count(1) FROM observability.run_log WHERE run_id = ?",
                [run_id],
            ).fetchone()[0]
            context.log.info("Run log row count for run_id=%s: %s", run_id, row_count)
        except Exception as exc:
            context.log.warning("Run log verify failed: %s", exc)
        return

    for con in _with_duckdb_connection():
        con.execute("CREATE SCHEMA IF NOT EXISTS observability")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS observability.run_log (
                run_id VARCHAR,
                job_name VARCHAR,
                status VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds DOUBLE,
                partition_key VARCHAR,
                tags_json VARCHAR,
                assets_materialized_count BIGINT,
                row_count BIGINT,
                rows_inserted BIGINT,
                rows_updated BIGINT,
                rows_deleted BIGINT,
                error_message VARCHAR,
                logged_ts TIMESTAMP
            )
            """
        )
        con.execute("DELETE FROM observability.run_log WHERE run_id = ?", [run_id])
        con.execute(
            """
            INSERT INTO observability.run_log (
                run_id,
                job_name,
                status,
                start_time,
                end_time,
                duration_seconds,
                partition_key,
                tags_json,
                assets_materialized_count,
                row_count,
                rows_inserted,
                rows_updated,
                rows_deleted,
                error_message,
                logged_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                job_name,
                status,
                start_dt,
                end_dt,
                duration_seconds,
                partition_key,
                tags_json,
                metrics["assets_materialized_count"],
                metrics["row_count"],
                metrics["rows_inserted"],
                metrics["rows_updated"],
                metrics["rows_deleted"],
                error_message,
                datetime.now(timezone.utc),
            ],
        )
        try:
            row_count = con.execute(
                "SELECT count(1) FROM observability.run_log WHERE run_id = ?",
                [run_id],
            ).fetchone()[0]
            context.log.info("Run log row count for run_id=%s: %s", run_id, row_count)
        except Exception as exc:
            context.log.warning("Run log verify failed: %s", exc)


@success_hook(required_resource_keys={"duckdb"})
def dagster_run_log_success(context) -> None:
    errors = []
    try:
        _write_run_log(context, status="SUCCESS")
    except Exception as exc:
        errors.append(f"run_log_write_failed: {exc}")
        context.log.warning("Run log write failed: %s", exc)
    try:
        _write_freshness_checks(context)
    except Exception as exc:
        errors.append(f"freshness_check_write_failed: {exc}")
        context.log.warning("Freshness check write failed: %s", exc)
    try:
        # DQ check failures are logged in observability.data_quality_checks.
        write_data_quality_checks(context)
    except Exception as exc:
        context.log.warning("Data quality check write failed: %s", exc)
    if errors:
        raise RuntimeError("Observability failure(s): " + " | ".join(errors))


@failure_hook(required_resource_keys={"duckdb"})
def dagster_run_log_failure(context) -> None:
    error_message = None
    if context.failure_event is not None:
        error_message = context.failure_event.message
    try:
        _write_run_log(context, status="FAILURE", error_message=error_message)
    except Exception as exc:
        context.log.warning("Run log write failed: %s", exc)
        raise RuntimeError(f"Observability failure: run_log_write_failed: {exc}") from exc


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def dagster_run_log_success_sensor(context) -> None:
    try:
        _write_run_log(context, status="SUCCESS")
    except Exception as exc:
        context.log.warning("Run log write failed: %s", exc)
        raise RuntimeError(f"Observability failure: run_log_write_failed: {exc}") from exc


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def dagster_run_log_failure_sensor(context) -> None:
    error_message = None
    if context.failure_event is not None:
        error_message = context.failure_event.message
    try:
        _write_run_log(context, status="FAILURE", error_message=error_message)
    except Exception as exc:
        context.log.warning("Run log write failed: %s", exc)
        raise RuntimeError(f"Observability failure: run_log_write_failed: {exc}") from exc
