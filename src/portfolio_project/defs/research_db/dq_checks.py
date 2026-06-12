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


def _fetch_records(con, sql: str, params: Sequence[object] | None = None) -> list[dict]:
    result = con.execute(sql, list(params or []))
    columns = [description[0] for description in result.description]
    records = []
    for row in result.fetchall():
        record = {}
        for column, value in zip(columns, row):
            if hasattr(value, "isoformat"):
                record[column] = value.isoformat()
            else:
                record[column] = value
        records.append(record)
    return records


def _status_for_zero_threshold(measured_value: float) -> str:
    return "PASS" if measured_value == 0 else "FAIL"


def log_security_identifier_mapping_checks(
    *,
    measured_con,
    observability_con,
    run_id: str | None = None,
    job_name: str | None = None,
    partition_key: str | None = None,
) -> None:
    duplicate_rows = _fetch_records(
        measured_con,
        """
        SELECT
            asset_id,
            source_symbol,
            identifier_type,
            identifier_value,
            identifier_source,
            valid_from_date,
            valid_to_date,
            count(*) AS row_count
        FROM silver.security_identifiers
        WHERE asset_id IS NOT NULL
        GROUP BY
            asset_id,
            source_symbol,
            identifier_type,
            identifier_value,
            identifier_source,
            valid_from_date,
            valid_to_date
        HAVING count(*) > 1
        ORDER BY row_count DESC, asset_id, source_symbol, identifier_type
        LIMIT 20
        """,
    )
    duplicate_count = float(sum(int(row["row_count"]) - 1 for row in duplicate_rows))
    write_dq_log(
        con=observability_con,
        check_name="dq_security_identifiers_duplicate_asset_id_mappings",
        severity="RED",
        status=_status_for_zero_threshold(duplicate_count),
        measured_value=duplicate_count,
        threshold_value=0.0,
        details={
            "table": "silver.security_identifiers",
            "key_columns": [
                "asset_id",
                "source_symbol",
                "identifier_type",
                "identifier_value",
                "identifier_source",
                "valid_from_date",
                "valid_to_date",
            ],
            "duplicate_groups_sample": duplicate_rows,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )

    cik_conflicts = _fetch_records(
        measured_con,
        """
        WITH cik_mappings AS (
            SELECT DISTINCT
                CAST(asset_id AS BIGINT) AS asset_id,
                upper(trim(source_symbol)) AS source_symbol,
                coalesce(
                    nullif(regexp_replace(trim(coalesce(cik, identifier_value)), '^0+', ''), ''),
                    '0'
                ) AS cik
            FROM silver.security_identifiers
            WHERE asset_id IS NOT NULL
              AND coalesce(is_current, TRUE) = TRUE
              AND (
                  (cik IS NOT NULL AND trim(cik) <> '')
                  OR (
                      lower(trim(identifier_type)) = 'cik'
                      AND identifier_value IS NOT NULL
                      AND trim(identifier_value) <> ''
                  )
              )
              AND source_symbol IS NOT NULL
              AND trim(source_symbol) <> ''
        )
        SELECT
            cik,
            source_symbol,
            count(DISTINCT asset_id) AS asset_count,
            string_agg(DISTINCT CAST(asset_id AS VARCHAR), ',' ORDER BY CAST(asset_id AS VARCHAR))
                AS asset_ids
        FROM cik_mappings
        GROUP BY cik, source_symbol
        HAVING count(DISTINCT asset_id) > 1
        ORDER BY asset_count DESC, cik, source_symbol
        LIMIT 20
        """,
    )
    cik_conflict_count = float(sum(int(row["asset_count"]) - 1 for row in cik_conflicts))
    write_dq_log(
        con=observability_con,
        check_name="dq_security_identifiers_cik_to_asset_conflicts",
        severity="RED",
        status=_status_for_zero_threshold(cik_conflict_count),
        measured_value=cik_conflict_count,
        threshold_value=0.0,
        details={
            "table": "silver.security_identifiers",
            "conflict_definition": (
                "current CIK and source_symbol pairs mapping to more than one asset_id"
            ),
            "conflicts_sample": cik_conflicts,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )

    symbol_conflicts = _fetch_records(
        measured_con,
        """
        WITH symbol_mappings AS (
            SELECT DISTINCT
                CAST(asset_id AS BIGINT) AS asset_id,
                upper(trim(source_symbol)) AS source_symbol
            FROM silver.security_identifiers
            WHERE asset_id IS NOT NULL
              AND coalesce(is_current, TRUE) = TRUE
              AND source_symbol IS NOT NULL
              AND trim(source_symbol) <> ''
        )
        SELECT
            source_symbol,
            count(DISTINCT asset_id) AS asset_count,
            string_agg(DISTINCT CAST(asset_id AS VARCHAR), ',' ORDER BY CAST(asset_id AS VARCHAR))
                AS asset_ids
        FROM symbol_mappings
        GROUP BY source_symbol
        HAVING count(DISTINCT asset_id) > 1
        ORDER BY asset_count DESC, source_symbol
        LIMIT 20
        """,
    )
    symbol_conflict_count = float(sum(int(row["asset_count"]) - 1 for row in symbol_conflicts))
    write_dq_log(
        con=observability_con,
        check_name="dq_security_identifiers_symbol_to_asset_conflicts",
        severity="RED",
        status=_status_for_zero_threshold(symbol_conflict_count),
        measured_value=symbol_conflict_count,
        threshold_value=0.0,
        details={
            "table": "silver.security_identifiers",
            "conflict_definition": "current source_symbol values mapping to more than one asset_id",
            "conflicts_sample": symbol_conflicts,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )

    missing_rates = _fetch_records(
        measured_con,
        """
        SELECT
            coalesce(nullif(trim(identifier_source), ''), '<missing>') AS identifier_source,
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS missing_asset_id_count,
            CASE
                WHEN count(*) = 0 THEN 0.0
                ELSE
                    CAST(sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS DOUBLE)
                    / CAST(count(*) AS DOUBLE)
            END AS missing_asset_id_rate
        FROM silver.security_identifiers
        GROUP BY coalesce(nullif(trim(identifier_source), ''), '<missing>')
        ORDER BY missing_asset_id_rate DESC, identifier_source
        """,
    )
    max_missing_rate = max(
        [float(row["missing_asset_id_rate"] or 0.0) for row in missing_rates],
        default=0.0,
    )
    write_dq_log(
        con=observability_con,
        check_name="dq_security_identifiers_missing_asset_id_rates_by_source",
        severity="RED",
        status=_status_for_zero_threshold(max_missing_rate),
        measured_value=max_missing_rate,
        threshold_value=0.0,
        details={
            "table": "silver.security_identifiers",
            "source_rates": missing_rates,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )
