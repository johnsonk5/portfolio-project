import json
import os
import statistics
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

from portfolio_project.defs.portfolio_db.observability.data_quality import write_data_quality_checks
from portfolio_project.defs.resources.duckdb import (
    _acquire_duckdb_lock,
    _release_duckdb_lock,
    duckdb_lock_path_for,
    resolve_duckdb_path,
)


def _freshness_severity(check_name: str) -> str:
    if check_name in {
        "daily_news_partition_row_count",
        "wikipedia_assets_with_views_min_count",
        "research_daily_prices_partition_row_count_vs_recent_median",
        "research_universe_membership_symbol_count_vs_recent_median",
        "research_universe_membership_day_over_day_drop_ratio",
        "research_universe_membership_avg_symbol_missing_data_rate",
    }:
        return "YELLOW"
    return "RED"


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
    asset_metrics = _collect_materialization_asset_metrics(records)
    assets_materialized_count = sum(int(row["assets_materialized_count"]) for row in asset_metrics)
    row_count_total = 0
    found_row_count = False
    for row in asset_metrics:
        if row["row_count"] is None:
            continue
        row_count_total += int(row["row_count"])
        found_row_count = True
    rows_inserted_total = sum(int(row["rows_inserted"]) for row in asset_metrics)
    rows_updated_total = sum(int(row["rows_updated"]) for row in asset_metrics)
    rows_deleted_total = sum(int(row["rows_deleted"]) for row in asset_metrics)
    found_mutations = any(
        int(row["rows_inserted"]) != 0
        or int(row["rows_updated"]) != 0
        or int(row["rows_deleted"]) != 0
        for row in asset_metrics
    )

    return {
        "assets_materialized_count": assets_materialized_count,
        "row_count": row_count_total if found_row_count else None,
        "rows_inserted": (
            rows_inserted_total if (found_mutations or assets_materialized_count > 0) else None
        ),
        "rows_updated": (
            rows_updated_total if (found_mutations or assets_materialized_count > 0) else None
        ),
        "rows_deleted": (
            rows_deleted_total if (found_mutations or assets_materialized_count > 0) else None
        ),
    }


def _materialization_metric_values(metadata: dict) -> dict:
    row_count_metric = _metadata_int(metadata.get("row_count")) if "row_count" in metadata else None

    inserted_metric = None
    for key in ("rows_inserted", "inserted_count"):
        if key in metadata:
            inserted_metric = _metadata_int(metadata[key])
            if inserted_metric is not None:
                break

    updated_metric = None
    for key in ("rows_updated", "updated_count"):
        if key in metadata:
            updated_metric = _metadata_int(metadata[key])
            if updated_metric is not None:
                break

    deleted_metric = None
    for key in ("rows_deleted", "deleted_count"):
        if key in metadata:
            deleted_metric = _metadata_int(metadata[key])
            if deleted_metric is not None:
                break

    return {
        "row_count": row_count_metric,
        "rows_inserted": int(inserted_metric or 0),
        "rows_updated": int(updated_metric or 0),
        "rows_deleted": int(deleted_metric or 0),
    }


def _asset_key_from_materialization(event, materialization) -> str:
    for candidate in (
        getattr(materialization, "asset_key", None),
        getattr(event, "asset_key", None),
    ):
        if candidate is None:
            continue
        path = getattr(candidate, "path", None)
        if path:
            return "/".join(str(part) for part in path)
        if isinstance(candidate, (list, tuple)):
            return "/".join(str(part) for part in candidate)
        return str(candidate)
    return "__unknown_asset__"


def _collect_materialization_asset_metrics(records) -> list[dict]:
    by_asset: dict[str, dict] = {}

    for record in records:
        entry = getattr(record, "event_log_entry", None) or record
        event = getattr(entry, "dagster_event", None)
        if event is None or event.event_type != DagsterEventType.ASSET_MATERIALIZATION:
            continue
        if event.event_specific_data is None:
            continue
        materialization = event.event_specific_data.materialization
        if materialization is None:
            continue
        asset_key = _asset_key_from_materialization(event, materialization)
        bucket = by_asset.setdefault(
            asset_key,
            {
                "asset_key": asset_key,
                "assets_materialized_count": 0,
                "row_count": 0,
                "has_row_count": False,
                "rows_inserted": 0,
                "rows_updated": 0,
                "rows_deleted": 0,
            },
        )
        bucket["assets_materialized_count"] += 1

        metadata = materialization.metadata or {}
        values = _materialization_metric_values(metadata)
        if values["row_count"] is not None:
            bucket["row_count"] += int(values["row_count"])
            bucket["has_row_count"] = True
        bucket["rows_inserted"] += int(values["rows_inserted"])
        bucket["rows_updated"] += int(values["rows_updated"])
        bucket["rows_deleted"] += int(values["rows_deleted"])

    rows = []
    for asset_key in sorted(by_asset):
        bucket = by_asset[asset_key]
        rows.append(
            {
                "asset_key": asset_key,
                "assets_materialized_count": int(bucket["assets_materialized_count"]),
                "row_count": int(bucket["row_count"]) if bucket["has_row_count"] else None,
                "rows_inserted": int(bucket["rows_inserted"]),
                "rows_updated": int(bucket["rows_updated"]),
                "rows_deleted": int(bucket["rows_deleted"]),
            }
        )
    return rows


def _ensure_observability_run_tables(con) -> None:
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
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS observability.run_asset_log (
            run_id VARCHAR,
            job_name VARCHAR,
            status VARCHAR,
            partition_key VARCHAR,
            asset_key VARCHAR,
            assets_materialized_count BIGINT,
            row_count BIGINT,
            rows_inserted BIGINT,
            rows_updated BIGINT,
            rows_deleted BIGINT,
            logged_ts TIMESTAMP
        )
        """
    )


def _write_run_asset_rows(
    con,
    run_id: str,
    job_name: Optional[str],
    status: str,
    partition_key: Optional[str],
    logged_ts: datetime,
    asset_metrics: list[dict],
) -> None:
    con.execute("DELETE FROM observability.run_asset_log WHERE run_id = ?", [run_id])
    for row in asset_metrics:
        con.execute(
            """
            INSERT INTO observability.run_asset_log (
                run_id,
                job_name,
                status,
                partition_key,
                asset_key,
                assets_materialized_count,
                row_count,
                rows_inserted,
                rows_updated,
                rows_deleted,
                logged_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                job_name,
                status,
                partition_key,
                row["asset_key"],
                row["assets_materialized_count"],
                row["row_count"],
                row["rows_inserted"],
                row["rows_updated"],
                row["rows_deleted"],
                logged_ts,
            ],
        )


def _with_duckdb_connection():
    db_path = resolve_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = duckdb_lock_path_for(db_path)
    lock_fd = _acquire_duckdb_lock(lock_path)
    con = None
    try:
        con = duckdb.connect(str(db_path))
        yield con
    finally:
        if con is not None:
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
    con.execute(
        "DELETE FROM observability.data_freshness_checks WHERE run_id = ?",
        [rows[0]["run_id"]],
    )
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


def _silver_price_partition_paths(partition_key: str) -> list[str]:
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    day_dir = data_root / "silver" / "prices" / f"date={partition_key}"
    if not day_dir.exists():
        return []
    paths: list[str] = []
    for symbol_dir in day_dir.glob("symbol=*"):
        for candidate in symbol_dir.glob("*.parquet"):
            paths.append(candidate.as_posix())
    return sorted(paths)


def _research_silver_price_partition_path(partition_key: str) -> Path:
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return (
        data_root
        / "silver"
        / "research_daily_prices"
        / f"month={partition_key[:7]}"
        / f"date={partition_key}.parquet"
    )


def _parse_research_partition_date(path: Path) -> Optional[str]:
    stem = path.stem
    if not stem.startswith("date="):
        return None
    partition_key = stem.removeprefix("date=")
    try:
        datetime.strptime(partition_key, "%Y-%m-%d")
    except ValueError:
        return None
    return partition_key


def _research_silver_price_partition_paths() -> list[Path]:
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    root = data_root / "silver" / "research_daily_prices"
    if not root.exists():
        return []
    paths = [
        path
        for path in root.glob("month=*/date=*.parquet")
        if _parse_research_partition_date(path) is not None
    ]
    return sorted(paths)


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


def _check_prices_freshness(
    con, run_id: str, job_name: str, partition_key: Optional[str]
) -> list[dict]:
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
                "severity": _freshness_severity("prices_active_symbol_coverage"),
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
                "severity": _freshness_severity("prices_active_symbol_coverage"),
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

    silver_paths = _silver_price_partition_paths(partition_key)
    present_symbol_count = 0
    missing_symbol_count = int(active_symbol_count)
    missing_symbol_samples = []
    if silver_paths:
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
            [silver_paths],
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
                [silver_paths],
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
            "severity": _freshness_severity("prices_active_symbol_coverage"),
            "status": status,
            "measured_value": float(missing_symbol_count),
            "threshold_value": 0.0,
            "details_json": json.dumps(
                {
                    "active_symbol_count": int(active_symbol_count),
                    "symbols_with_prices": int(present_symbol_count),
                    "missing_symbol_count": int(missing_symbol_count),
                    "missing_symbol_samples": missing_symbol_samples,
                    "silver_partition_paths": silver_paths,
                }
            ),
            "logged_ts": now,
        }
    ]


def _check_research_prices_freshness(
    con, run_id: str, job_name: str, partition_key: Optional[str]
) -> list[dict]:
    if job_name != "research_daily_prices_job":
        return []

    now = datetime.now(timezone.utc)
    if not partition_key:
        latest_check_name = "research_daily_prices_latest_trading_date_present"
        row_count_check_name = "research_daily_prices_partition_row_count_vs_recent_median"
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": latest_check_name,
                "severity": _freshness_severity(latest_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": row_count_check_name,
                "severity": _freshness_severity(row_count_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
        ]

    if not _is_us_trading_day(partition_key):
        latest_check_name = "research_daily_prices_latest_trading_date_present"
        row_count_check_name = "research_daily_prices_partition_row_count_vs_recent_median"
        details = json.dumps({"reason": "non_trading_day", "market": "NYSE"})
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": latest_check_name,
                "severity": _freshness_severity(latest_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": details,
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": row_count_check_name,
                "severity": _freshness_severity(row_count_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": details,
                "logged_ts": now,
            },
        ]

    expected_path = _research_silver_price_partition_path(partition_key)
    partition_paths = _research_silver_price_partition_paths()
    partition_dates = [
        parsed_date
        for parsed_date in (_parse_research_partition_date(path) for path in partition_paths)
        if parsed_date is not None
    ]
    latest_available_partition = max(partition_dates) if partition_dates else None

    expected_present = expected_path.exists()
    current_row_count = 0
    current_trade_date = None
    if expected_present:
        current_row_count, current_trade_date = con.execute(
            """
            SELECT count(*) AS row_count, max(trade_date) AS max_trade_date
            FROM read_parquet(?)
            """,
            [expected_path.as_posix()],
        ).fetchone()
        current_row_count = int(current_row_count or 0)
        if current_trade_date is not None:
            current_trade_date = (
                current_trade_date.isoformat()
                if hasattr(current_trade_date, "isoformat")
                else str(current_trade_date)
            )

    latest_present_status = (
        "PASS"
        if expected_present and current_row_count > 0 and current_trade_date == partition_key
        else "FAIL"
    )

    lookback_partitions = int(os.getenv("RESEARCH_FRESHNESS_ROW_COUNT_LOOKBACK_PARTITIONS", "20"))
    min_history = int(os.getenv("RESEARCH_FRESHNESS_ROW_COUNT_MIN_HISTORY", "3"))
    min_ratio = float(os.getenv("RESEARCH_FRESHNESS_MIN_ROW_COUNT_RATIO", "0.7"))

    prior_paths = [
        path
        for path in partition_paths
        if (parsed_date := _parse_research_partition_date(path)) is not None
        and parsed_date < partition_key
    ]
    prior_paths = sorted(
        prior_paths,
        key=lambda path: _parse_research_partition_date(path) or "",
        reverse=True,
    )[:lookback_partitions]

    prior_partition_counts: list[tuple[str, int]] = []
    for path in prior_paths:
        parsed_date = _parse_research_partition_date(path)
        if parsed_date is None:
            continue
        row_count = con.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [path.as_posix()],
        ).fetchone()[0]
        prior_partition_counts.append((parsed_date, int(row_count or 0)))

    latest_check = {
        "run_id": run_id,
        "job_name": job_name,
        "partition_key": partition_key,
        "check_name": "research_daily_prices_latest_trading_date_present",
        "severity": _freshness_severity("research_daily_prices_latest_trading_date_present"),
        "status": latest_present_status,
        "measured_value": 1.0 if latest_present_status == "PASS" else 0.0,
        "threshold_value": 1.0,
        "details_json": json.dumps(
            {
                "expected_partition_key": partition_key,
                "expected_partition_path": expected_path.as_posix(),
                "expected_partition_present": expected_present,
                "expected_partition_row_count": current_row_count,
                "expected_partition_trade_date": current_trade_date,
                "latest_available_partition_key": latest_available_partition,
            }
        ),
        "logged_ts": now,
    }

    if len(prior_partition_counts) < min_history:
        row_count_check = {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": "research_daily_prices_partition_row_count_vs_recent_median",
            "severity": _freshness_severity(
                "research_daily_prices_partition_row_count_vs_recent_median"
            ),
            "status": "SKIPPED",
            "measured_value": float(current_row_count) if expected_present else None,
            "threshold_value": None,
            "details_json": json.dumps(
                {
                    "reason": "insufficient_history",
                    "current_partition_path": expected_path.as_posix(),
                    "current_row_count": current_row_count,
                    "minimum_history_required": min_history,
                    "history_partitions_available": len(prior_partition_counts),
                    "recent_partition_counts": dict(prior_partition_counts),
                }
            ),
            "logged_ts": now,
        }
        return [latest_check, row_count_check]

    historical_counts = [count for _, count in prior_partition_counts]
    baseline_median = float(statistics.median(historical_counts))
    threshold_value = float(baseline_median * min_ratio)
    row_count_status = "PASS" if float(current_row_count) >= threshold_value else "FAIL"

    row_count_check = {
        "run_id": run_id,
        "job_name": job_name,
        "partition_key": partition_key,
        "check_name": "research_daily_prices_partition_row_count_vs_recent_median",
        "severity": _freshness_severity(
            "research_daily_prices_partition_row_count_vs_recent_median"
        ),
        "status": row_count_status,
        "measured_value": float(current_row_count),
        "threshold_value": threshold_value,
        "details_json": json.dumps(
            {
                "current_partition_path": expected_path.as_posix(),
                "current_row_count": current_row_count,
                "expected_partition_key": partition_key,
                "row_count_ratio_threshold": min_ratio,
                "baseline_median_row_count": baseline_median,
                "history_partitions_considered": len(prior_partition_counts),
                "recent_partition_counts": dict(prior_partition_counts),
            }
        ),
        "logged_ts": now,
    }
    return [latest_check, row_count_check]


def _check_research_universe_freshness(
    con, run_id: str, job_name: str, partition_key: Optional[str]
) -> list[dict]:
    if job_name != "research_daily_prices_job":
        return []

    now = datetime.now(timezone.utc)
    latest_check_name = "research_universe_membership_latest_trading_date_present"
    count_check_name = "research_universe_membership_symbol_count_vs_recent_median"
    drop_check_name = "research_universe_membership_day_over_day_drop_ratio"
    missing_rate_check_name = "research_universe_membership_avg_symbol_missing_data_rate"
    if not partition_key:
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": latest_check_name,
                "severity": _freshness_severity(latest_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": count_check_name,
                "severity": _freshness_severity(count_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": drop_check_name,
                "severity": _freshness_severity(drop_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": missing_rate_check_name,
                "severity": _freshness_severity(missing_rate_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps({"reason": "missing_partition_key"}),
                "logged_ts": now,
            },
        ]

    if not _is_us_trading_day(partition_key):
        details = json.dumps({"reason": "non_trading_day", "market": "NYSE"})
        return [
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": latest_check_name,
                "severity": _freshness_severity(latest_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": details,
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": count_check_name,
                "severity": _freshness_severity(count_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": details,
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": drop_check_name,
                "severity": _freshness_severity(drop_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": details,
                "logged_ts": now,
            },
            {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": missing_rate_check_name,
                "severity": _freshness_severity(missing_rate_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": details,
                "logged_ts": now,
            },
        ]

    universe_size = int(os.getenv("RESEARCH_UNIVERSE_SIZE", "500"))
    lookback_partitions = int(
        os.getenv("RESEARCH_UNIVERSE_FRESHNESS_LOOKBACK_PARTITIONS", "20")
    )
    min_history = int(os.getenv("RESEARCH_UNIVERSE_FRESHNESS_MIN_HISTORY", "3"))
    min_ratio = float(os.getenv("RESEARCH_UNIVERSE_FRESHNESS_MIN_SYMBOL_COUNT_RATIO", "0.95"))
    drop_lookback_partitions = int(
        os.getenv("RESEARCH_UNIVERSE_DROP_LOOKBACK_PARTITIONS", "20")
    )
    drop_min_history = int(os.getenv("RESEARCH_UNIVERSE_DROP_MIN_HISTORY", "3"))
    drop_ratio_multiplier = float(os.getenv("RESEARCH_UNIVERSE_DROP_RATIO_MULTIPLIER", "3.0"))
    drop_min_ratio = float(os.getenv("RESEARCH_UNIVERSE_DROP_MIN_RATIO", "0.03"))
    missing_data_lookback_partitions = int(
        os.getenv("RESEARCH_UNIVERSE_MISSING_DATA_LOOKBACK_PARTITIONS", "20")
    )
    missing_data_min_history = int(os.getenv("RESEARCH_UNIVERSE_MISSING_DATA_MIN_HISTORY", "3"))
    max_avg_missing_data_rate = float(
        os.getenv("RESEARCH_UNIVERSE_MISSING_DATA_MAX_AVG_RATE", "0.05")
    )

    universe_table_present = _table_exists(con, "silver", "universe_membership_daily")
    counts_by_date: list[tuple[str, int]] = []
    if universe_table_present:
        counts_by_date = [
            (
                member_date.isoformat() if hasattr(member_date, "isoformat") else str(member_date),
                int(symbol_count or 0),
            )
            for member_date, symbol_count in con.execute(
                """
                SELECT member_date, count(DISTINCT upper(trim(symbol))) AS symbol_count
                FROM silver.universe_membership_daily
                WHERE member_date IS NOT NULL
                  AND symbol IS NOT NULL
                  AND trim(symbol) <> ''
                GROUP BY member_date
                ORDER BY member_date
                """
            ).fetchall()
        ]

    counts_lookup = dict(counts_by_date)
    current_symbol_count = int(counts_lookup.get(partition_key, 0))
    latest_available_partition = max(counts_lookup) if counts_lookup else None
    latest_present_status = (
        "PASS"
        if current_symbol_count > 0 and latest_available_partition == partition_key
        else "FAIL"
    )

    latest_check = {
        "run_id": run_id,
        "job_name": job_name,
        "partition_key": partition_key,
        "check_name": latest_check_name,
        "severity": _freshness_severity(latest_check_name),
        "status": latest_present_status,
        "measured_value": 1.0 if latest_present_status == "PASS" else 0.0,
        "threshold_value": 1.0,
        "details_json": json.dumps(
            {
                "expected_member_date": partition_key,
                "universe_table_present": universe_table_present,
                "expected_member_date_present": partition_key in counts_lookup,
                "expected_member_date_symbol_count": current_symbol_count,
                "latest_available_member_date": latest_available_partition,
                "target_universe_size": universe_size,
            }
        ),
        "logged_ts": now,
    }

    drop_check: dict[str, object]
    if partition_key not in counts_lookup:
        drop_check = {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": drop_check_name,
            "severity": _freshness_severity(drop_check_name),
            "status": "SKIPPED",
            "measured_value": None,
            "threshold_value": None,
            "details_json": json.dumps(
                {
                    "reason": "expected_member_date_missing",
                    "expected_member_date": partition_key,
                    "latest_available_member_date": latest_available_partition,
                    "target_universe_size": universe_size,
                }
            ),
            "logged_ts": now,
        }
    else:
        recent_counts_for_drop = [
            (member_date, symbol_count)
            for member_date, symbol_count in counts_by_date
            if member_date <= partition_key
        ][-(drop_lookback_partitions + 1) :]
        prior_counts_for_drop = recent_counts_for_drop[:-1]
        if len(prior_counts_for_drop) < drop_min_history:
            drop_check = {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": drop_check_name,
                "severity": _freshness_severity(drop_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps(
                    {
                        "reason": "insufficient_history",
                        "expected_member_date": partition_key,
                        "minimum_history_required": drop_min_history,
                        "history_partitions_available": len(prior_counts_for_drop),
                        "recent_symbol_counts": dict(prior_counts_for_drop),
                    }
                ),
                "logged_ts": now,
            }
        else:
            previous_member_date, previous_symbol_count = prior_counts_for_drop[-1]
            current_drop_ratio = (
                max(float(previous_symbol_count) - float(current_symbol_count), 0.0)
                / float(previous_symbol_count)
                if previous_symbol_count > 0
                else 0.0
            )
            historical_drop_ratios = []
            for (_, earlier_count), (_, later_count) in zip(
                prior_counts_for_drop,
                prior_counts_for_drop[1:],
            ):
                if earlier_count <= 0:
                    continue
                historical_drop_ratios.append(
                    max(float(earlier_count) - float(later_count), 0.0) / float(earlier_count)
                )
            baseline_drop_ratio = (
                float(statistics.median(historical_drop_ratios))
                if historical_drop_ratios
                else 0.0
            )
            threshold_drop_ratio = max(
                baseline_drop_ratio * drop_ratio_multiplier,
                drop_min_ratio,
            )
            drop_check = {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": drop_check_name,
                "severity": _freshness_severity(drop_check_name),
                "status": "PASS" if current_drop_ratio <= threshold_drop_ratio else "FAIL",
                "measured_value": float(current_drop_ratio),
                "threshold_value": float(threshold_drop_ratio),
                "details_json": json.dumps(
                    {
                        "expected_member_date": partition_key,
                        "current_symbol_count": current_symbol_count,
                        "previous_member_date": previous_member_date,
                        "previous_symbol_count": previous_symbol_count,
                        "current_drop_count": max(
                            int(previous_symbol_count) - int(current_symbol_count),
                            0,
                        ),
                        "baseline_median_drop_ratio": baseline_drop_ratio,
                        "drop_ratio_multiplier": drop_ratio_multiplier,
                        "minimum_drop_ratio_threshold": drop_min_ratio,
                        "history_partitions_considered": len(prior_counts_for_drop),
                        "recent_symbol_counts": dict(prior_counts_for_drop),
                        "historical_drop_ratios": historical_drop_ratios,
                    }
                ),
                "logged_ts": now,
            }

    missing_rate_check: dict[str, object]
    if partition_key not in counts_lookup:
        missing_rate_check = {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": missing_rate_check_name,
            "severity": _freshness_severity(missing_rate_check_name),
            "status": "SKIPPED",
            "measured_value": None,
            "threshold_value": None,
            "details_json": json.dumps(
                {
                    "reason": "expected_member_date_missing",
                    "expected_member_date": partition_key,
                    "latest_available_member_date": latest_available_partition,
                    "target_universe_size": universe_size,
                }
            ),
            "logged_ts": now,
        }
    else:
        recent_member_dates = [
            member_date for member_date, _ in counts_by_date if member_date <= partition_key
        ][-missing_data_lookback_partitions:]
        if len(recent_member_dates) < missing_data_min_history:
            missing_rate_check = {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": missing_rate_check_name,
                "severity": _freshness_severity(missing_rate_check_name),
                "status": "SKIPPED",
                "measured_value": None,
                "threshold_value": None,
                "details_json": json.dumps(
                    {
                        "reason": "insufficient_history",
                        "expected_member_date": partition_key,
                        "minimum_history_required": missing_data_min_history,
                        "history_partitions_available": len(recent_member_dates),
                        "recent_member_dates": recent_member_dates,
                    }
                ),
                "logged_ts": now,
            }
        else:
            recent_window_start = recent_member_dates[0]
            recent_window_end = recent_member_dates[-1]
            existing_price_paths = []
            missing_price_partitions = []
            for member_date in recent_member_dates:
                partition_path = _research_silver_price_partition_path(member_date)
                if partition_path.exists():
                    existing_price_paths.append(partition_path.as_posix())
                else:
                    missing_price_partitions.append(member_date)

            if existing_price_paths:
                present_prices_cte = """
                present_prices AS (
                    SELECT DISTINCT
                        CAST(trade_date AS DATE) AS trade_date,
                        upper(trim(symbol)) AS symbol
                    FROM read_parquet(?)
                    WHERE trade_date IS NOT NULL
                      AND symbol IS NOT NULL
                      AND trim(symbol) <> ''
                )
                """
                present_prices_params = [existing_price_paths]
            else:
                present_prices_cte = """
                present_prices AS (
                    SELECT
                        CAST(NULL AS DATE) AS trade_date,
                        CAST(NULL AS VARCHAR) AS symbol
                    WHERE FALSE
                )
                """
                present_prices_params = []

            symbol_missing_rows = con.execute(
                f"""
                WITH latest_universe AS (
                    SELECT DISTINCT upper(trim(symbol)) AS symbol
                    FROM silver.universe_membership_daily
                    WHERE member_date = ?
                      AND symbol IS NOT NULL
                      AND trim(symbol) <> ''
                ),
                recent_membership AS (
                    SELECT DISTINCT
                        CAST(member_date AS DATE) AS member_date,
                        upper(trim(symbol)) AS symbol
                    FROM silver.universe_membership_daily
                    WHERE member_date >= ?
                      AND member_date <= ?
                      AND symbol IS NOT NULL
                      AND trim(symbol) <> ''
                      AND upper(trim(symbol)) IN (SELECT symbol FROM latest_universe)
                ),
                {present_prices_cte}
                SELECT
                    rm.symbol,
                    count(*) AS expected_dates,
                    count(pp.symbol) AS present_dates,
                    count(*) - count(pp.symbol) AS missing_dates
                FROM recent_membership AS rm
                LEFT JOIN present_prices AS pp
                    ON pp.trade_date = rm.member_date
                   AND pp.symbol = rm.symbol
                GROUP BY rm.symbol
                ORDER BY missing_dates DESC, expected_dates DESC, rm.symbol
                """,
                [partition_key, recent_window_start, recent_window_end, *present_prices_params],
            ).fetchall()

            symbol_missing_stats: list[dict[str, str | int | float]] = []
            for symbol, expected_dates, present_dates, missing_dates in symbol_missing_rows:
                expected_dates = int(expected_dates or 0)
                present_dates = int(present_dates or 0)
                missing_dates = int(missing_dates or 0)
                missing_rate = (
                    float(missing_dates) / float(expected_dates) if expected_dates > 0 else 0.0
                )
                symbol_missing_stats.append(
                    {
                        "symbol": str(symbol),
                        "expected_dates": expected_dates,
                        "present_dates": present_dates,
                        "missing_dates": missing_dates,
                        "missing_rate": missing_rate,
                    }
                )

            missing_rates = [
                float(row["missing_rate"]) for row in symbol_missing_stats if "missing_rate" in row
            ]
            average_missing_rate = (
                float(statistics.mean(missing_rates)) if missing_rates else 0.0
            )
            symbols_with_missing_data = [
                row
                for row in symbol_missing_stats
                if int(row["missing_dates"]) > 0
            ]
            max_missing_rate = max(missing_rates, default=0.0)
            missing_rate_check = {
                "run_id": run_id,
                "job_name": job_name,
                "partition_key": partition_key,
                "check_name": missing_rate_check_name,
                "severity": _freshness_severity(missing_rate_check_name),
                "status": (
                    "PASS"
                    if average_missing_rate <= max_avg_missing_data_rate
                    else "FAIL"
                ),
                "measured_value": average_missing_rate,
                "threshold_value": max_avg_missing_data_rate,
                "details_json": json.dumps(
                    {
                        "expected_member_date": partition_key,
                        "window_start_member_date": recent_window_start,
                        "window_end_member_date": recent_window_end,
                        "lookback_partitions_considered": len(recent_member_dates),
                        "latest_universe_symbol_count": current_symbol_count,
                        "symbols_evaluated": len(symbol_missing_stats),
                        "symbols_with_missing_data": len(symbols_with_missing_data),
                        "symbols_with_missing_data_rate": (
                            float(len(symbols_with_missing_data)) / float(len(symbol_missing_stats))
                            if symbol_missing_stats
                            else 0.0
                        ),
                        "max_symbol_missing_rate": float(max_missing_rate),
                        "missing_price_partitions": missing_price_partitions,
                        "top_missing_symbols": symbol_missing_stats[:10],
                    }
                ),
                "logged_ts": now,
            }

    prior_counts = [
        (member_date, symbol_count)
        for member_date, symbol_count in reversed(counts_by_date)
        if member_date < partition_key
    ][:lookback_partitions]

    if len(prior_counts) < min_history:
        count_check = {
            "run_id": run_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "check_name": count_check_name,
            "severity": _freshness_severity(count_check_name),
            "status": "SKIPPED",
            "measured_value": (
                float(current_symbol_count) if partition_key in counts_lookup else None
            ),
            "threshold_value": None,
            "details_json": json.dumps(
                {
                    "reason": "insufficient_history",
                    "current_symbol_count": current_symbol_count,
                    "expected_member_date": partition_key,
                    "target_universe_size": universe_size,
                    "minimum_history_required": min_history,
                    "history_partitions_available": len(prior_counts),
                    "recent_symbol_counts": dict(prior_counts),
                }
            ),
            "logged_ts": now,
        }
        return [latest_check, count_check, drop_check, missing_rate_check]

    historical_counts = [count for _, count in prior_counts]
    baseline_median = float(statistics.median(historical_counts))
    threshold_value = float(baseline_median * min_ratio)
    count_status = "PASS" if float(current_symbol_count) >= threshold_value else "FAIL"

    count_check = {
        "run_id": run_id,
        "job_name": job_name,
        "partition_key": partition_key,
        "check_name": count_check_name,
        "severity": _freshness_severity(count_check_name),
        "status": count_status,
        "measured_value": float(current_symbol_count),
        "threshold_value": threshold_value,
        "details_json": json.dumps(
            {
                "current_symbol_count": current_symbol_count,
                "expected_member_date": partition_key,
                "target_universe_size": universe_size,
                "symbol_count_ratio_threshold": min_ratio,
                "baseline_median_symbol_count": baseline_median,
                "history_partitions_considered": len(prior_counts),
                "recent_symbol_counts": dict(prior_counts),
            }
        ),
        "logged_ts": now,
    }
    return [latest_check, count_check, drop_check, missing_rate_check]


def _check_wikipedia_freshness(
    con, run_id: str, job_name: str, partition_key: Optional[str]
) -> list[dict]:
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
                "severity": _freshness_severity("wikipedia_assets_with_views_min_count"),
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
    wiki_path = (
        data_root
        / "silver"
        / "wikipedia_pageviews"
        / f"view_date={partition_key}"
        / "data_0.parquet"
    )
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
            "severity": _freshness_severity("wikipedia_assets_with_views_min_count"),
            "status": min_count_status,
            "measured_value": float(assets_with_views),
            "threshold_value": float(min_assets_with_views),
            "details_json": json.dumps(details),
            "logged_ts": now,
        },
    ]


def _check_news_freshness(
    con, run_id: str, job_name: str, partition_key: Optional[str]
) -> list[dict]:
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
                "severity": _freshness_severity("daily_news_partition_row_count"),
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
            "severity": _freshness_severity("daily_news_partition_row_count"),
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
            ("research_daily_prices_latest_trading_date_present", _check_research_prices_freshness),
            (
                "research_universe_membership_latest_trading_date_present",
                _check_research_universe_freshness,
            ),
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
                        "severity": _freshness_severity(check_name),
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
    if not run_id:
        context.log.warning(
            "Run log skipped because run_id is missing (status=%s, job=%s)",
            status,
            job_name,
        )
        return
    run_id = str(run_id)
    start_dt, end_dt = _get_run_times_from_instance(context, run_id)
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
    duration_seconds = (end_dt - start_dt).total_seconds() if start_dt and end_dt else None
    tags = run.tags or {} if run else {}
    partition_key = tags.get("dagster/partition")
    tags_json = json.dumps(tags) if tags else None

    metrics = _collect_materialization_metrics(records)
    asset_metrics = _collect_materialization_asset_metrics(records)
    logged_ts = datetime.now(timezone.utc)

    db_path = resolve_duckdb_path()
    context.log.info(
        "Run log hook fired for run_id=%s job=%s status=%s db=%s",
        run_id,
        job_name,
        status,
        db_path,
    )
    con, needs_owned = _get_duckdb_connection(context)
    if con is not None:
        _ensure_observability_run_tables(con)
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
                logged_ts,
            ],
        )
        _write_run_asset_rows(
            con=con,
            run_id=run_id,
            job_name=job_name,
            status=status,
            partition_key=partition_key,
            logged_ts=logged_ts,
            asset_metrics=asset_metrics,
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
        _ensure_observability_run_tables(con)
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
                logged_ts,
            ],
        )
        _write_run_asset_rows(
            con=con,
            run_id=run_id,
            job_name=job_name,
            status=status,
            partition_key=partition_key,
            logged_ts=logged_ts,
            asset_metrics=asset_metrics,
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
