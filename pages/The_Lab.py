import os
from pathlib import Path

import altair as alt
import duckdb
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="The Lab",
    page_icon="TL",
    layout="wide",
)

with st.sidebar:
    st.page_link("streamlit_app.py", label="Market Vibecheck")
    st.page_link("pages/Deep_Dive.py", label="Deep Dive")
    st.page_link("pages/The_Lab.py", label="The Lab")
    st.page_link("pages/Observability.py", label="Observability")

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

:root {
  --bg-1: #0b1020;
  --bg-2: #0f1a36;
  --ink-1: #f4f7ff;
  --ink-2: #b6c2e2;
  --accent: #6ee7b7;
  --accent-2: #60a5fa;
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

.header-hero {
  background: linear-gradient(110deg, rgba(110, 231, 183, 0.12), rgba(96, 165, 250, 0.10));
  border: 1px solid var(--card-border);
  padding: 24px 28px;
  border-radius: 18px;
  margin-bottom: 16px;
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

.metric-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.08);
  font-size: 0.9rem;
  color: var(--ink-2);
}

[data-testid="stSidebarNav"] {
  display: none;
}
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)

COLORWAY = ["#6ee7b7", "#60a5fa", "#fbbf24", "#f97316", "#f472b6"]
FACTOR_LABELS = {
    "mkt_rf": "Market",
    "smb": "Size",
    "hml": "Value",
    "mom": "Momentum",
}
METRIC_LABELS = {
    "sharpe_ratio": "Sharpe",
    "annualized_volatility": "Volatility",
    "alpha": "Alpha",
    "max_drawdown": "Max Drawdown",
    "beta": "Beta",
}


def _resolve_research_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_RESEARCH_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "research.duckdb"


@st.cache_data(show_spinner=False)
def _load_strategy_catalog() -> tuple[pd.DataFrame, str | None]:
    db_path = _resolve_research_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), f"Research DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT
                strategy_id,
                strategy_name,
                strategy_version,
                description,
                ranking_method,
                benchmark_symbol,
                target_count
            FROM silver.strategy_definitions
            WHERE is_active = TRUE
            ORDER BY strategy_name, strategy_id
            """
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), f"Failed to load strategy catalog: {exc}"
    finally:
        con.close()

    return df, None


@st.cache_data(show_spinner=False)
def _load_strategy_comparison_payload(
    selected_strategy_ids: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
    if not selected_strategy_ids:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None

    db_path = _resolve_research_duckdb_path()
    if not db_path.exists():
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            f"Research DuckDB not found at {db_path}",
        )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        strategy_ids = list(selected_strategy_ids)
        performance_df = con.execute(
            """
            WITH selected_strategies AS (
                SELECT
                    strategy_id,
                    strategy_name,
                    benchmark_symbol,
                    ranking_method,
                    target_count
                FROM silver.strategy_definitions
                WHERE strategy_id = ANY(?)
            ),
            latest_performance AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY strategy_id
                        ORDER BY asof_ts DESC, run_id DESC
                    ) AS row_num
                FROM gold.strategy_performance
                WHERE strategy_id = ANY(?)
            )
            SELECT
                s.strategy_id,
                s.strategy_name,
                s.benchmark_symbol,
                s.ranking_method,
                s.target_count,
                p.run_id,
                p.cagr,
                p.sharpe_ratio,
                p.max_drawdown,
                p.annualized_volatility,
                p.alpha,
                p.asof_ts
            FROM selected_strategies AS s
            LEFT JOIN latest_performance AS p
                ON s.strategy_id = p.strategy_id
               AND p.row_num = 1
            ORDER BY s.strategy_name, s.strategy_id
            """,
            [strategy_ids, strategy_ids],
        ).fetch_df()

        run_ids = performance_df["run_id"].dropna().astype(str).unique().tolist()
        if not run_ids:
            return performance_df, pd.DataFrame(), pd.DataFrame(), None

        returns_df = con.execute(
            """
            SELECT
                strategy_id,
                date,
                portfolio_return,
                benchmark_return,
                cumulative_return,
                drawdown,
                holdings_count
            FROM gold.strategy_returns
            WHERE run_id = ANY(?)
            ORDER BY strategy_id, date
            """,
            [run_ids],
        ).fetch_df()

        factors_df = con.execute(
            """
            SELECT
                factor_date,
                mkt_rf,
                smb,
                hml,
                rf,
                mom
            FROM silver.vw_factors
            ORDER BY factor_date
            """
        ).fetch_df()
    except Exception as exc:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            f"Failed to load strategy comparison data: {exc}",
        )
    finally:
        con.close()

    return performance_df, returns_df, factors_df, None


def _compute_betas(returns_df: pd.DataFrame) -> pd.DataFrame:
    if returns_df.empty:
        return pd.DataFrame(columns=["strategy_id", "beta"])

    rows: list[dict[str, float | str | None]] = []
    grouped = returns_df.dropna(subset=["portfolio_return", "benchmark_return"]).groupby(
        "strategy_id", sort=False
    )
    for strategy_id, frame in grouped:
        benchmark = frame["benchmark_return"].astype(float)
        portfolio = frame["portfolio_return"].astype(float)
        variance = float(benchmark.var(ddof=1)) if len(frame) > 1 else float("nan")
        if not np.isfinite(variance) or variance <= 0:
            beta = None
        else:
            covariance = float(np.cov(portfolio, benchmark, ddof=1)[0, 1])
            beta = covariance / variance
        rows.append({"strategy_id": str(strategy_id), "beta": beta})
    return pd.DataFrame(rows)


def _compute_factor_exposures(
    returns_df: pd.DataFrame,
    factors_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = ["strategy_id", "factor", "exposure", "observations"]
    if returns_df.empty or factors_df.empty:
        return pd.DataFrame(columns=columns)

    merged = returns_df.merge(
        factors_df,
        left_on="date",
        right_on="factor_date",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(columns=columns)

    merged["portfolio_excess"] = merged["portfolio_return"].astype(float) - merged["rf"].astype(float)

    rows: list[dict[str, float | int | str]] = []
    regressors = ["mkt_rf", "smb", "hml", "mom"]
    for strategy_id, frame in merged.groupby("strategy_id", sort=False):
        regression_frame = frame.dropna(subset=["portfolio_excess", *regressors]).copy()
        if len(regression_frame) < 20:
            continue
        x = regression_frame[regressors].astype(float).to_numpy()
        y = regression_frame["portfolio_excess"].astype(float).to_numpy()
        coefficients, *_ = np.linalg.lstsq(x, y, rcond=None)
        for factor_name, coefficient in zip(regressors, coefficients, strict=True):
            rows.append(
                {
                    "strategy_id": str(strategy_id),
                    "factor": factor_name,
                    "exposure": float(coefficient),
                    "observations": int(len(regression_frame)),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _build_metric_table(
    performance_df: pd.DataFrame,
    beta_df: pd.DataFrame,
) -> pd.DataFrame:
    if performance_df.empty:
        return pd.DataFrame()

    frame = performance_df.merge(beta_df, on="strategy_id", how="left")
    frame = frame[
        [
            "strategy_name",
            "sharpe_ratio",
            "annualized_volatility",
            "alpha",
            "max_drawdown",
            "beta",
        ]
    ].rename(columns={"strategy_name": "Strategy"})
    frame = frame.set_index("Strategy").transpose()
    frame.index = [METRIC_LABELS.get(index, index) for index in frame.index]
    return frame


st.markdown(
    """
    <div class="header-hero">
      <div style="font-size: 2rem; font-weight: 700;">The Lab</div>
      <div style="color: var(--ink-2); margin-top: 6px;">
        Compare research strategies side by side across return paths, risk, and factor behavior.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

catalog_df, catalog_error = _load_strategy_catalog()
if catalog_error:
    st.info(catalog_error)
    st.stop()

if catalog_df.empty:
    st.info("No active strategies were found in the research catalog.")
    st.stop()

catalog_df = catalog_df.copy()
catalog_df["selector_label"] = catalog_df.apply(
    lambda row: f"{row['strategy_name']} ({row['strategy_id']})",
    axis=1,
)

default_labels = catalog_df["selector_label"].head(3).tolist()

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Strategy Comparison</div>', unsafe_allow_html=True)

selected_labels = st.multiselect(
    "Select up to 5 strategies",
    options=catalog_df["selector_label"].tolist(),
    default=default_labels,
    max_selections=5,
    help="Strategies use the catalog's human-readable names. Comparison pulls the latest materialized run for each strategy.",
)

if not selected_labels:
    st.info("Select at least one strategy to start the comparison.")
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

selected_rows = catalog_df[catalog_df["selector_label"].isin(selected_labels)].copy()
selected_rows = selected_rows.set_index("selector_label").loc[selected_labels].reset_index()
selected_strategy_ids = tuple(selected_rows["strategy_id"].tolist())

performance_df, returns_df, factors_df, load_error = _load_strategy_comparison_payload(
    selected_strategy_ids
)
if load_error:
    st.info(load_error)
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

if performance_df["run_id"].isna().all():
    st.info(
        "No materialized strategy performance runs were found for the selected strategies. "
        "Run the research strategy assets before using The Lab."
    )
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

name_map = (
    performance_df[["strategy_id", "strategy_name"]]
    .drop_duplicates()
    .set_index("strategy_id")["strategy_name"]
    .to_dict()
)

latest_asof = performance_df["asof_ts"].dropna().max()
if pd.notna(latest_asof):
    st.markdown(
        f'<div class="metric-pill">Latest strategy snapshot: {pd.to_datetime(latest_asof).strftime("%B %d, %Y %H:%M")}</div>',
        unsafe_allow_html=True,
    )

summary_cols = st.columns(min(len(selected_rows), 4))
for idx, (_, row) in enumerate(selected_rows.head(4).iterrows()):
    with summary_cols[idx]:
        st.metric(
            row["strategy_name"],
            f"{int(row['target_count'])} holdings",
            row["ranking_method"],
        )

st.markdown("</div>", unsafe_allow_html=True)

returns_df = returns_df.copy()
if not returns_df.empty:
    returns_df["date"] = pd.to_datetime(returns_df["date"])
    returns_df["strategy_name"] = returns_df["strategy_id"].map(name_map)

factors_df = factors_df.copy()
if not factors_df.empty:
    factors_df["factor_date"] = pd.to_datetime(factors_df["factor_date"])

beta_df = _compute_betas(returns_df)
factor_exposures_df = _compute_factor_exposures(returns_df, factors_df)
metric_table = _build_metric_table(performance_df, beta_df)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Cumulative Returns</div>', unsafe_allow_html=True)
if returns_df.empty:
    st.info("No strategy return paths were available for the selected runs.")
else:
    chart_df = returns_df.dropna(subset=["cumulative_return"]).copy()
    chart_df["cumulative_return_pct"] = chart_df["cumulative_return"] * 100.0
    cumulative_chart = (
        alt.Chart(chart_df)
        .mark_line(strokeWidth=2.5)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("cumulative_return_pct:Q", title="Cumulative return (%)"),
            color=alt.Color(
                "strategy_name:N",
                title="Strategy",
                scale=alt.Scale(range=COLORWAY),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("strategy_name:N", title="Strategy"),
                alt.Tooltip(
                    "cumulative_return_pct:Q",
                    title="Cumulative return (%)",
                    format=".2f",
                ),
            ],
        )
        .properties(height=420)
        .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
    )
    st.altair_chart(cumulative_chart, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

metrics_col, exposures_col = st.columns([1.15, 1], gap="large")

with metrics_col:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Headline Metrics</div>', unsafe_allow_html=True)
    if metric_table.empty:
        st.info("No headline metrics were available for the selected runs.")
    else:
        formatted_metric_table = metric_table.copy()
        for row_label in formatted_metric_table.index:
            if row_label in {"Volatility", "Alpha", "Max Drawdown"}:
                formatted_metric_table.loc[row_label] = formatted_metric_table.loc[row_label].map(
                    lambda value: (
                        f"{float(value) * 100.0:.2f}%"
                        if pd.notna(value)
                        else "n/a"
                    )
                )
            else:
                formatted_metric_table.loc[row_label] = formatted_metric_table.loc[row_label].map(
                    lambda value: f"{float(value):.2f}" if pd.notna(value) else "n/a"
                )
        st.dataframe(
            formatted_metric_table,
            use_container_width=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

with exposures_col:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Factor Exposures</div>', unsafe_allow_html=True)
    if factor_exposures_df.empty:
        st.info(
            "Factor exposure estimates were unavailable. This usually means the selected strategies "
            "do not overlap enough with `silver.vw_factors` yet."
        )
    else:
        exposure_chart_df = factor_exposures_df.copy()
        exposure_chart_df["strategy_name"] = exposure_chart_df["strategy_id"].map(name_map)
        exposure_chart_df["factor_label"] = exposure_chart_df["factor"].map(FACTOR_LABELS)
        exposure_chart = (
            alt.Chart(exposure_chart_df)
            .mark_bar()
            .encode(
                x=alt.X("factor_label:N", title="Factor"),
                y=alt.Y("exposure:Q", title="Exposure"),
                color=alt.Color(
                    "strategy_name:N",
                    title="Strategy",
                    scale=alt.Scale(range=COLORWAY),
                ),
                xOffset="strategy_name:N",
                tooltip=[
                    alt.Tooltip("strategy_name:N", title="Strategy"),
                    alt.Tooltip("factor_label:N", title="Factor"),
                    alt.Tooltip("exposure:Q", title="Exposure", format=".3f"),
                    alt.Tooltip("observations:Q", title="Regression obs", format=","),
                ],
            )
            .properties(height=390)
            .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
        )
        st.altair_chart(exposure_chart, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Selected Strategies</div>', unsafe_allow_html=True)
selected_display = selected_rows[
    [
        "strategy_name",
        "strategy_id",
        "ranking_method",
        "benchmark_symbol",
        "target_count",
        "description",
    ]
].rename(
    columns={
        "strategy_name": "Strategy",
        "strategy_id": "ID",
        "ranking_method": "Ranking Method",
        "benchmark_symbol": "Benchmark",
        "target_count": "Target Holdings",
        "description": "Description",
    }
)
st.dataframe(selected_display, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)
