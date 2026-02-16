import os
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Observability",
    page_icon="OB",
    layout="wide",
)

with st.sidebar:
    st.page_link("streamlit_app.py", label="Market Vibecheck")
    st.page_link("pages/Deep_Dive.py", label="Deep Dive")
    st.page_link("pages/Observability.py", label="Observability")

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

:root {
  --bg-1: #0b1020;
  --bg-2: #0f1a36;
  --ink-1: #f4f7ff;
  --ink-2: #b6c2e2;
  --card: rgba(255, 255, 255, 0.05);
  --card-border: rgba(255, 255, 255, 0.08);
}

html, body, [class*="css"] {
  font-family: 'Space Grotesk', system-ui, -apple-system, Segoe UI, sans-serif;
}

.stApp {
  background: radial-gradient(1200px 600px at 10% -10%, #1a2453 0%, transparent 60%),
              radial-gradient(900px 500px at 90% 0%, #123d5c 0%, transparent 55%),
              linear-gradient(180deg, var(--bg-1), var(--bg-2));
  color: var(--ink-1);
}

.section-card {
  background: var(--card);
  border: 1px solid var(--card-border);
  padding: 18px 20px;
  border-radius: 16px;
  margin-bottom: 16px;
}

.section-title {
  font-size: 1.05rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--ink-2);
  margin-bottom: 8px;
}

[data-testid="stSidebarNav"] {
  display: none;
}
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)

KNOWN_JOBS = [
    "daily_prices_job",
    "daily_news_job",
    "wikipedia_activity_job",
    "asset_status_updates_job",
    "sp500_update_job",
    "tranco_update_job",
]


def _resolve_duckdb_path() -> Path:
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


@st.cache_data(ttl=30)
def _load_observability_data() -> dict:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return {
            "error": f"DuckDB not found at {db_path}",
            "recent_runs": pd.DataFrame(),
            "run_failures": pd.DataFrame(),
            "freshness_issues": pd.DataFrame(),
            "data_quality_issues": pd.DataFrame(),
            "data_quality_recent": pd.DataFrame(),
            "durations": pd.DataFrame(),
            "job_options": sorted(KNOWN_JOBS),
        }

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        has_run_log = _table_exists(con, "observability", "run_log")
        has_freshness = _table_exists(con, "observability", "data_freshness_checks")
        has_data_quality = _table_exists(con, "observability", "data_quality_checks")
        if not has_run_log:
            return {
                "error": "Table observability.run_log does not exist yet.",
                "recent_runs": pd.DataFrame(),
                "run_failures": pd.DataFrame(),
                "freshness_issues": pd.DataFrame(),
                "data_quality_issues": pd.DataFrame(),
                "data_quality_recent": pd.DataFrame(),
                "durations": pd.DataFrame(),
                "job_options": sorted(KNOWN_JOBS),
            }

        recent_runs = con.execute(
            """
            SELECT
                run_id,
                job_name,
                status,
                partition_key,
                start_time,
                end_time,
                duration_seconds,
                error_message,
                logged_ts
            FROM observability.run_log
            ORDER BY COALESCE(end_time, logged_ts) DESC
            LIMIT 50
            """
        ).fetch_df()

        run_failures = con.execute(
            """
            SELECT
                run_id,
                job_name,
                status,
                partition_key,
                error_message,
                end_time,
                logged_ts
            FROM observability.run_log
            WHERE status = 'FAILURE'
               OR (error_message IS NOT NULL AND trim(error_message) <> '')
            ORDER BY COALESCE(end_time, logged_ts) DESC
            LIMIT 50
            """
        ).fetch_df()

        durations = con.execute(
            """
            SELECT
                run_id,
                job_name,
                status,
                duration_seconds,
                end_time,
                logged_ts
            FROM observability.run_log
            WHERE duration_seconds IS NOT NULL
              AND COALESCE(end_time, logged_ts) >= (NOW() - INTERVAL 30 DAY)
            ORDER BY COALESCE(end_time, logged_ts) DESC
            """
        ).fetch_df()

        if has_freshness:
            freshness_issues = con.execute(
                """
                SELECT
                    run_id,
                    job_name,
                    partition_key,
                    check_name,
                    severity,
                    status,
                    details_json,
                    logged_ts
                FROM observability.data_freshness_checks
                WHERE status IN ('FAIL', 'SKIPPED')
                ORDER BY logged_ts DESC
                LIMIT 75
                """
            ).fetch_df()
        else:
            freshness_issues = pd.DataFrame()

        if has_data_quality:
            data_quality_issues = con.execute(
                """
                SELECT
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
                WHERE status IN ('FAIL', 'SKIPPED')
                ORDER BY logged_ts DESC
                LIMIT 100
                """
            ).fetch_df()
            data_quality_recent = con.execute(
                """
                SELECT
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
                ORDER BY logged_ts DESC
                LIMIT 200
                """
            ).fetch_df()
        else:
            data_quality_issues = pd.DataFrame()
            data_quality_recent = pd.DataFrame()

        observed_jobs = con.execute(
            "SELECT DISTINCT job_name FROM observability.run_log WHERE job_name IS NOT NULL ORDER BY job_name"
        ).fetch_df()
        observed_job_names = observed_jobs["job_name"].dropna().astype(str).tolist()
        job_options = sorted(set(KNOWN_JOBS) | set(observed_job_names))

        return {
            "error": None,
            "recent_runs": recent_runs,
            "run_failures": run_failures,
            "freshness_issues": freshness_issues,
            "data_quality_issues": data_quality_issues,
            "data_quality_recent": data_quality_recent,
            "durations": durations,
            "job_options": job_options,
        }
    except Exception as exc:
        return {
            "error": f"Failed to load observability data: {exc}",
            "recent_runs": pd.DataFrame(),
            "run_failures": pd.DataFrame(),
            "freshness_issues": pd.DataFrame(),
            "data_quality_issues": pd.DataFrame(),
            "data_quality_recent": pd.DataFrame(),
            "durations": pd.DataFrame(),
            "job_options": sorted(KNOWN_JOBS),
        }
    finally:
        con.close()


def _display_df(df: pd.DataFrame, empty_message: str) -> None:
    if df.empty:
        st.info(empty_message)
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


st.title("Observability")
st.caption("Monitor run health, freshness checks, data quality checks, and runtime trends.")

data = _load_observability_data()
if data["error"]:
    st.info(data["error"])

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Recent Failures and Errors</div>', unsafe_allow_html=True)

fail_col, fresh_col = st.columns([1, 1], gap="medium")
with fail_col:
    st.markdown("**Run Failures**")
    _display_df(
        data["run_failures"],
        "No failed runs with error messages in observability.run_log.",
    )

with fresh_col:
    st.markdown("**Freshness Issues (FAIL or SKIPPED)**")
    _display_df(
        data["freshness_issues"],
        "No freshness check failures or skipped checks found.",
    )

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Data Quality Checks</div>', unsafe_allow_html=True)

dq_issue_col, dq_recent_col = st.columns([1, 1], gap="medium")
with dq_issue_col:
    st.markdown("**DQ Issues (FAIL or SKIPPED)**")
    _display_df(
        data["data_quality_issues"],
        "No data quality failures or skipped checks found.",
    )

with dq_recent_col:
    st.markdown("**Recent DQ Results (All Statuses)**")
    _display_df(
        data["data_quality_recent"],
        "No data quality checks found yet.",
    )

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Recent Runs</div>', unsafe_allow_html=True)
_display_df(
    data["recent_runs"],
    "No recent run data found in observability.run_log.",
)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Job Duration Trend</div>', unsafe_allow_html=True)

job_options = data["job_options"]
selected_job = st.selectbox(
    "Select job",
    options=job_options,
    index=(job_options.index("daily_prices_job") if "daily_prices_job" in job_options else 0),
)

durations_df = data["durations"].copy()
if selected_job:
    durations_df = durations_df[durations_df["job_name"] == selected_job].copy()

if durations_df.empty:
    st.info(
        f"No duration data found for `{selected_job}`. This usually means the job has not run recently "
        "or observability rows are not available yet."
    )
else:
    durations_df["run_ts"] = pd.to_datetime(
        durations_df["end_time"].fillna(durations_df["logged_ts"]),
        errors="coerce",
    )
    durations_df = durations_df.dropna(subset=["run_ts"])
    durations_df = durations_df.sort_values("run_ts", ascending=True).tail(100)
    durations_df["duration_minutes"] = pd.to_numeric(
        durations_df["duration_seconds"], errors="coerce"
    ) / 60.0

    if durations_df.empty:
        st.info(f"No plottable duration timestamps found for `{selected_job}`.")
    else:
        line = (
            alt.Chart(durations_df)
            .mark_line(strokeWidth=2, color="#60a5fa")
            .encode(
                x=alt.X("run_ts:T", title="Run time"),
                y=alt.Y("duration_minutes:Q", title="Duration (minutes)"),
            )
        )
        points = (
            alt.Chart(durations_df)
            .mark_circle(size=85)
            .encode(
                x=alt.X("run_ts:T", title="Run time"),
                y=alt.Y("duration_minutes:Q", title="Duration (minutes)"),
                color=alt.Color(
                    "status:N",
                    scale=alt.Scale(
                        domain=["SUCCESS", "FAILURE"],
                        range=["#22c55e", "#ef4444"],
                    ),
                    legend=alt.Legend(title="Run status"),
                ),
                tooltip=[
                    alt.Tooltip("run_ts:T", title="Run time"),
                    alt.Tooltip("duration_minutes:Q", title="Duration (min)", format=".2f"),
                    alt.Tooltip("status:N", title="Status"),
                    alt.Tooltip("run_id:N", title="Run ID"),
                ],
            )
        )
        chart = alt.layer(line, points).properties(height=320).configure_axis(
            gridColor="rgba(148, 163, 184, 0.25)"
        )
        st.altair_chart(chart, use_container_width=True)

st.markdown("</div>", unsafe_allow_html=True)
