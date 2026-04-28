import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class Notification:
    level: str
    title: str
    detail: str
    logged_ts: Any


def resolve_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "portfolio.duckdb"


def _table_exists(con, schema_name: str, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _load_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    try:
        if pd.isna(raw):
            return {}
    except (TypeError, ValueError):
        pass
    try:
        value = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _pipeline_name(job_name: Any) -> str:
    if not job_name:
        return "pipeline"
    return str(job_name).removesuffix("_job")


def _humanize_check_name(check_name: Any) -> str:
    if not check_name:
        return "concern"
    text = str(check_name)
    for prefix in ("dq_", "daily_", "silver_", "gold_", "bronze_", "research_"):
        text = text.removeprefix(prefix)
    return text.replace("_", " ")


def _format_count(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}"


def _format_detail(row: pd.Series) -> str:
    details = _load_json(row.get("details_json"))
    check_name = str(row.get("check_name") or "")
    measured_value = row.get("measured_value")
    threshold_value = row.get("threshold_value")

    if check_name == "prices_active_symbol_coverage":
        missing = details.get("missing_symbol_count", measured_value)
        active = details.get("active_symbol_count")
        missing_text = _format_count(missing) or "some"
        if active is not None:
            return f"Missing prices for {missing_text} of {_format_count(active)} active stocks."
        return f"Missing prices for {missing_text} active stocks."

    if check_name == "daily_news_partition_row_count":
        count = _format_count(measured_value) or "0"
        threshold = _format_count(threshold_value) or "1"
        return f"Only {count} news rows found; expected at least {threshold}."

    if "row_count_vs_recent_median" in check_name:
        count = _format_count(measured_value) or "0"
        baseline = _format_count(threshold_value) or "the recent baseline"
        return f"Row count is {count}, below expected level of {baseline}."

    if "missing_partition" in check_name or "partition_exists" in check_name:
        return "Expected data partition is missing."

    if "null_threshold" in check_name or "null_thresholds" in check_name:
        measured = _format_count(measured_value)
        threshold = _format_count(threshold_value)
        if measured and threshold:
            return f"Null rate reached {measured}%, above the {threshold}% threshold."
        return "Null values exceeded the configured threshold."

    if "schema" in check_name:
        missing_columns = details.get("missing_columns")
        if isinstance(missing_columns, list) and missing_columns:
            return f"Missing expected columns: {', '.join(map(str, missing_columns[:4]))}."
        return "Schema check found a mismatch."

    reason = details.get("reason")
    if reason:
        return str(reason).replace("_", " ").capitalize() + "."

    measured = _format_count(measured_value)
    threshold = _format_count(threshold_value)
    if measured and threshold:
        return f"Measured {measured}; expected {threshold}."
    return "Recent observability check needs attention."


def _notification_from_check_row(row: pd.Series) -> Notification:
    level = str(row.get("severity") or "YELLOW").upper()
    pipeline = _pipeline_name(row.get("job_name"))
    check_label = _humanize_check_name(row.get("check_name"))
    concern = "alert" if level == "RED" else "warning"
    return Notification(
        level=level,
        title=f"{pipeline} pipeline {concern}. {check_label.capitalize()}",
        detail=_format_detail(row),
        logged_ts=row.get("logged_ts"),
    )


def _notification_from_run_row(row: pd.Series) -> Notification:
    pipeline = _pipeline_name(row.get("job_name"))
    raw_error = row.get("details_json")
    error_message = "" if raw_error is None or pd.isna(raw_error) else str(raw_error).strip()
    return Notification(
        level="RED",
        title=f"{pipeline} pipeline alert. Run failed",
        detail=error_message or "Pipeline run failed without a recorded error message.",
        logged_ts=row.get("logged_ts"),
    )


@st.cache_data(ttl=30)
def load_notifications(limit: int = 12) -> list[Notification]:
    db_path = resolve_duckdb_path()
    if not db_path.exists():
        return []

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        frames: list[pd.DataFrame] = []
        if _table_exists(con, "observability", "data_quality_checks"):
            frames.append(
                con.execute(
                    """
                    SELECT
                        'data_quality' AS source,
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
                    FROM observability.data_quality_checks
                    WHERE upper(status) IN ('FAIL', 'FAILURE', 'SKIPPED', 'WARN')
                      AND upper(COALESCE(severity, 'YELLOW')) IN ('RED', 'YELLOW')
                    ORDER BY logged_ts DESC
                    LIMIT 50
                    """
                ).fetch_df()
            )
        if _table_exists(con, "observability", "data_freshness_checks"):
            frames.append(
                con.execute(
                    """
                    SELECT
                        'freshness' AS source,
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
                    FROM observability.data_freshness_checks
                    WHERE upper(status) IN ('FAIL', 'FAILURE', 'SKIPPED', 'WARN')
                      AND upper(COALESCE(severity, 'YELLOW')) IN ('RED', 'YELLOW')
                    ORDER BY logged_ts DESC
                    LIMIT 50
                    """
                ).fetch_df()
            )
        if _table_exists(con, "observability", "run_log"):
            frames.append(
                con.execute(
                    """
                    SELECT
                        'run' AS source,
                        run_id,
                        job_name,
                        partition_key,
                        NULL AS check_name,
                        'RED' AS severity,
                        status,
                        NULL AS measured_value,
                        NULL AS threshold_value,
                        error_message AS details_json,
                        COALESCE(end_time, logged_ts) AS logged_ts
                    FROM observability.run_log
                    WHERE upper(status) = 'FAILURE'
                       OR (error_message IS NOT NULL AND trim(error_message) <> '')
                    ORDER BY COALESCE(end_time, logged_ts) DESC
                    LIMIT 50
                    """
                ).fetch_df()
            )
    except Exception:
        return []
    finally:
        con.close()

    if not frames:
        return []

    non_empty_frames = [frame for frame in frames if not frame.empty]
    if not non_empty_frames:
        return []

    concerns = pd.concat(non_empty_frames, ignore_index=True)
    if concerns.empty:
        return []

    concerns = concerns.sort_values("logged_ts", ascending=False).head(limit)
    notifications: list[Notification] = []
    for _, row in concerns.iterrows():
        if row.get("source") == "run":
            notifications.append(_notification_from_run_row(row))
        else:
            notifications.append(_notification_from_check_row(row))
    return notifications


def _format_timestamp(value: Any) -> str:
    if value is None or pd.isna(value):
        return "Recently"
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.strftime("%b %d, %I:%M %p").replace(" 0", " ")
    return str(value)


def render_notification_center() -> None:
    notifications = load_notifications()
    red_count = sum(1 for item in notifications if item.level == "RED")
    count = len(notifications)
    label = f":bell: {count}" if count else ":bell:"

    _, notification_col = st.columns([0.88, 0.12])
    with notification_col:
        with st.popover(label, help="Recent red and yellow pipeline concerns"):
            st.markdown("**Recent Notifications**")
            st.caption(f"{red_count} red / {max(count - red_count, 0)} yellow")
            if not notifications:
                st.info("No recent red or yellow concerns.")
                return

            for index, item in enumerate(notifications):
                prefix = ":red[!] " if item.level == "RED" else ""
                st.markdown(f"**{prefix}{item.title}**")
                st.caption(item.detail)
                st.caption(_format_timestamp(item.logged_ts))
                if index < len(notifications) - 1:
                    st.divider()
