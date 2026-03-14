import json
from datetime import datetime, timezone
from typing import Any
import uuid


def ensure_data_quality_table(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS observability")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS observability.data_quality_checks (
            check_id VARCHAR,
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
    con.execute("ALTER TABLE observability.data_quality_checks ADD COLUMN IF NOT EXISTS check_id VARCHAR")


def write_dq_log(
    *,
    context=None,
    con=None,
    check_name: str,
    severity: str,
    status: str,
    measured_value: float | None = None,
    threshold_value: float | None = None,
    details: dict[str, Any] | None = None,
    error_name: str | None = None,
    error_message: str | None = None,
    run_id: str | None = None,
    job_name: str | None = None,
    partition_key: str | None = None,
    check_id: str | None = None,
    dedupe_by_run_check: bool = False,
) -> None:
    """
    Generic writer for observability.data_quality_checks rows.
    """
    if con is None:
        resources = getattr(context, "resources", None) if context is not None else None
        con = getattr(resources, "duckdb", None) if resources is not None else None
    if con is None:
        return

    resolved_run_id = run_id
    resolved_job_name = job_name
    resolved_partition_key = partition_key
    if context is not None:
        if resolved_run_id is None:
            raw_run_id = getattr(context, "run_id", None)
            resolved_run_id = str(raw_run_id) if raw_run_id else None
        if resolved_job_name is None:
            resolved_job_name = getattr(context, "job_name", None)
        if resolved_partition_key is None:
            resolved_partition_key = getattr(context, "partition_key", None)

    payload = dict(details or {})
    if error_name:
        payload["error_name"] = error_name
    if error_message:
        payload["error_message"] = error_message

    try:
        ensure_data_quality_table(con)
        if dedupe_by_run_check and resolved_run_id:
            con.execute(
                """
                DELETE FROM observability.data_quality_checks
                WHERE run_id = ?
                  AND check_name = ?
                """,
                [resolved_run_id, check_name],
            )
        con.execute(
            """
            INSERT INTO observability.data_quality_checks (
                check_id,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                check_id or str(uuid.uuid4()),
                resolved_run_id,
                resolved_job_name,
                resolved_partition_key,
                check_name,
                severity,
                status,
                measured_value,
                threshold_value,
                json.dumps(payload),
                datetime.now(timezone.utc),
            ],
        )
        try:
            con.commit()
        except Exception:
            pass
    except Exception as exc:
        if context is not None and hasattr(context, "log"):
            context.log.warning("Unable to write DQ log row (%s): %s", check_name, exc)
