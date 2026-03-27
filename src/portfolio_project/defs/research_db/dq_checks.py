from __future__ import annotations

from collections.abc import Sequence

from portfolio_project.defs.portfolio_db.observability.observability_modules import (
    write_dq_log,
)


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _null_count_expressions(columns: Sequence[str]) -> str:
    return ",\n                ".join(
        [
            (
                f"sum(CASE WHEN {_quote_identifier(column)} IS NULL THEN 1 ELSE 0 END) "
                f"AS {_quote_identifier(column)}"
            )
            for column in columns
        ]
    )


def log_duplicate_row_check(
    *,
    measured_con,
    observability_con,
    check_name: str,
    relation_sql: str,
    relation_params: Sequence[object] | None,
    key_columns: Sequence[str],
    details: dict | None = None,
    run_id: str | None = None,
    job_name: str | None = None,
    partition_key: str | None = None,
) -> None:
    quoted_key_columns = [_quote_identifier(column) for column in key_columns]
    group_by_sql = ", ".join(quoted_key_columns)
    select_keys_sql = ", ".join(quoted_key_columns)

    duplicate_count = measured_con.execute(
        f"""
        WITH scoped_rows AS (
            {relation_sql}
        )
        SELECT coalesce(sum(cnt - 1), 0)
        FROM (
            SELECT
                {select_keys_sql},
                count(*) AS cnt
            FROM scoped_rows
            GROUP BY {group_by_sql}
            HAVING count(*) > 1
        )
        """,
        list(relation_params or []),
    ).fetchone()[0]

    payload = {"key_columns": list(key_columns)}
    if details:
        payload.update(details)

    write_dq_log(
        con=observability_con,
        check_name=check_name,
        severity="RED",
        status="PASS" if int(duplicate_count or 0) == 0 else "FAIL",
        measured_value=float(duplicate_count or 0),
        threshold_value=0.0,
        details=payload,
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def log_required_field_null_check(
    *,
    measured_con,
    observability_con,
    check_name: str,
    relation_sql: str,
    relation_params: Sequence[object] | None,
    required_columns: Sequence[str],
    details: dict | None = None,
    run_id: str | None = None,
    job_name: str | None = None,
    partition_key: str | None = None,
) -> None:
    result = measured_con.execute(
        f"""
        WITH scoped_rows AS (
            {relation_sql}
        )
        SELECT
            count(*) AS row_count,
            {_null_count_expressions(required_columns)}
        FROM scoped_rows
        """,
        list(relation_params or []),
    ).fetchone()

    row_count = int(result[0] or 0)
    null_counts = {
        column: int(result[index + 1] or 0) for index, column in enumerate(required_columns)
    }
    measured_value = float(sum(null_counts.values()))
    payload = {
        "row_count": row_count,
        "required_columns": list(required_columns),
        "null_counts": null_counts,
    }
    if details:
        payload.update(details)

    write_dq_log(
        con=observability_con,
        check_name=check_name,
        severity="RED",
        status="PASS" if measured_value == 0 else "FAIL",
        measured_value=measured_value,
        threshold_value=0.0,
        details=payload,
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )
