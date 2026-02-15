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

from portfolio_project.defs.duckdb_resource import _acquire_duckdb_lock, _release_duckdb_lock


def _to_utc_datetime(timestamp: Optional[float]) -> Optional[datetime]:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def _metadata_number(entry) -> Optional[float]:
    for attr in ("value", "int_value", "float_value"):
        if hasattr(entry, attr):
            value = getattr(entry, attr)
            if isinstance(value, (int, float)):
                return float(value)
    return None


def _collect_materialization_metrics(context, run_id: str) -> dict:
    try:
        records = context.instance.all_logs(run_id)
    except Exception:
        records = []
    row_count_total = 0.0
    rows_inserted_total = 0.0
    rows_updated_total = 0.0
    rows_deleted_total = 0.0
    found_row_count = False
    found_mutations = False

    for record in records:
        entry = getattr(record, "event_log_entry", None) or record
        event = getattr(entry, "dagster_event", None)
        if event is None or event.event_type != DagsterEventType.ASSET_MATERIALIZATION:
            continue
        if event.event_specific_data is None:
            continue
        materialization = event.event_specific_data.materialization
        if materialization is None or materialization.metadata is None:
            continue
        for key, value in materialization.metadata.items():
            metric = _metadata_number(value)
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
        "assets_materialized_count": len(records),
        "row_count": int(row_count_total) if found_row_count else None,
        "rows_inserted": int(rows_inserted_total) if found_mutations else None,
        "rows_updated": int(rows_updated_total) if found_mutations else None,
        "rows_deleted": int(rows_deleted_total) if found_mutations else None,
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


def _write_run_log(context, status: str, error_message: Optional[str] = None) -> None:
    run = _get_run_from_context(context)
    run_id = getattr(run, "run_id", None) or getattr(context, "run_id", None)
    job_name = getattr(run, "job_name", None) or getattr(context, "job_name", None)
    start_dt = _to_utc_datetime(getattr(run, "start_time", None)) if run else None
    end_dt = _to_utc_datetime(getattr(run, "end_time", None)) if run else None
    end_dt = end_dt or datetime.now(timezone.utc)
    duration_seconds = (
        (end_dt - start_dt).total_seconds() if start_dt and end_dt else None
    )
    tags = run.tags or {} if run else {}
    partition_key = tags.get("dagster/partition")
    tags_json = json.dumps(tags) if tags else None

    metrics = _collect_materialization_metrics(context, run_id)

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


@success_hook(required_resource_keys={"duckdb"})
def dagster_run_log_success(context) -> None:
    try:
        _write_run_log(context, status="SUCCESS")
    except Exception as exc:
        context.log.warning("Run log write failed: %s", exc)


@failure_hook(required_resource_keys={"duckdb"})
def dagster_run_log_failure(context) -> None:
    error_message = None
    if context.failure_event is not None:
        error_message = context.failure_event.message
    try:
        _write_run_log(context, status="FAILURE", error_message=error_message)
    except Exception as exc:
        context.log.warning("Run log write failed: %s", exc)


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
