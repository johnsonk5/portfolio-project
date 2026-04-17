import json
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
LAB_VIEWS = {
    "compare": "Compare",
    "detail": "Strategy Detail",
}


def _resolve_research_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_RESEARCH_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "research.duckdb"


def _resolve_research_prices_glob() -> str:
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return (
        data_root / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet"
    ).as_posix()


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
                rebalance_frequency,
                benchmark_symbol,
                target_count,
                weighting_method,
                long_short_flag,
                config_json
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
def _load_strategy_detail_payload(
    strategy_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
    if not strategy_id:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            None,
        )

    db_path = _resolve_research_duckdb_path()
    if not db_path.exists():
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            f"Research DuckDB not found at {db_path}",
        )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        run_id_df = con.execute(
            """
            WITH latest_success AS (
                SELECT
                    run_id,
                    row_number() OVER (
                        ORDER BY asof_ts DESC, completed_at DESC, started_at DESC, run_id DESC
                    ) AS row_num
                FROM silver.strategy_runs
                WHERE strategy_id = ?
                  AND run_status = 'success'
            )
            SELECT run_id
            FROM latest_success
            WHERE row_num = 1
            """,
            [strategy_id],
        ).fetch_df()
        run_ids = run_id_df["run_id"].dropna().astype(str).unique().tolist()

        definition_df = con.execute(
            """
            SELECT
                strategy_id,
                strategy_name,
                strategy_version,
                description,
                ranking_method,
                rebalance_frequency,
                target_count,
                weighting_method,
                benchmark_symbol,
                long_short_flag,
                start_date,
                end_date,
                config_json
            FROM silver.strategy_definitions
            WHERE strategy_id = ?
            """,
            [strategy_id],
        ).fetch_df()

        parameters_df = con.execute(
            """
            SELECT
                parameter_name,
                parameter_value,
                parameter_type,
                effective_start_date,
                effective_end_date,
                description
            FROM silver.strategy_parameters
            WHERE strategy_id = ?
              AND is_active = TRUE
            ORDER BY parameter_name, effective_start_date
            """,
            [strategy_id],
        ).fetch_df()

        if run_ids:
            performance_df = con.execute(
                """
                SELECT
                    run_id,
                    strategy_id,
                    cagr,
                    sharpe_ratio,
                    sortino_ratio,
                    max_drawdown,
                    annualized_volatility,
                    hit_rate,
                    turnover_avg,
                    benchmark_return,
                    alpha,
                    asof_ts
                FROM gold.strategy_performance
                WHERE strategy_id = ?
                  AND run_id = ANY(?)
                """,
                [strategy_id, run_ids],
            ).fetch_df()
        else:
            performance_df = pd.DataFrame()

        if run_ids:
            returns_df = con.execute(
                """
                SELECT
                    strategy_id,
                    date,
                    portfolio_return,
                    benchmark_return,
                    cumulative_return,
                    drawdown,
                    turnover,
                    holdings_count
                FROM gold.strategy_returns
                WHERE run_id = ANY(?)
                ORDER BY date
                """,
                [run_ids],
            ).fetch_df()
        else:
            returns_df = pd.DataFrame()

        if run_ids:
            holdings_df = con.execute(
                """
                SELECT
                    h.strategy_id,
                    h.rebalance_date,
                    h.symbol,
                    h.target_weight,
                    h.entry_rank,
                    h.signal_value
                FROM gold.strategy_holdings AS h
                WHERE h.strategy_id = ?
                  AND h.run_id = ANY(?)
                ORDER BY h.rebalance_date DESC, h.entry_rank, h.symbol
                """,
                [strategy_id, run_ids],
            ).fetch_df()
        else:
            holdings_df = pd.DataFrame()
    except Exception as exc:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            f"Failed to load strategy detail data: {exc}",
        )
    finally:
        con.close()

    return definition_df, parameters_df, performance_df, returns_df, holdings_df, None


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

    merged["portfolio_excess"] = merged["portfolio_return"].astype(float) - merged[
        "rf"
    ].astype(float)

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


def _compute_rolling_sharpe(
    returns: pd.Series,
    *,
    window: int = 63,
    min_periods: int = 21,
) -> pd.Series:
    values = pd.to_numeric(returns, errors="coerce").astype(float)
    rolling_mean = values.rolling(window=window, min_periods=min_periods).mean()
    rolling_std = values.rolling(window=window, min_periods=min_periods).std(ddof=1)
    sharpe = (rolling_mean / rolling_std) * np.sqrt(252.0)
    sharpe = sharpe.where(rolling_std > 0)
    return sharpe.replace([np.inf, -np.inf], np.nan)


def _build_benchmark_comparison_frame(
    returns_df: pd.DataFrame,
    *,
    strategy_name: str,
    benchmark_label: str,
) -> pd.DataFrame:
    columns = [
        "date",
        "series_name",
        "normalized_return",
        "drawdown",
        "rolling_sharpe",
        "daily_return",
    ]
    if returns_df.empty:
        return pd.DataFrame(columns=columns)

    frame = returns_df.sort_values("date", kind="stable").copy()
    frame["portfolio_return"] = pd.to_numeric(frame["portfolio_return"], errors="coerce")
    frame["benchmark_return"] = pd.to_numeric(frame["benchmark_return"], errors="coerce")
    frame["cumulative_return"] = pd.to_numeric(frame["cumulative_return"], errors="coerce")
    frame["drawdown"] = pd.to_numeric(frame["drawdown"], errors="coerce")

    strategy_frame = pd.DataFrame(
        {
            "date": frame["date"],
            "series_name": strategy_name,
            "normalized_return": frame["cumulative_return"],
            "drawdown": frame["drawdown"],
            "rolling_sharpe": _compute_rolling_sharpe(frame["portfolio_return"]),
            "daily_return": frame["portfolio_return"],
        }
    )

    comparison_frames = [strategy_frame]
    benchmark_frame = frame.loc[
        frame["benchmark_return"].notna(), ["date", "benchmark_return"]
    ].copy()
    if not benchmark_frame.empty:
        benchmark_wealth = (1.0 + benchmark_frame["benchmark_return"]).cumprod()
        benchmark_running_peak = benchmark_wealth.cummax()
        benchmark_frame = pd.DataFrame(
            {
                "date": benchmark_frame["date"],
                "series_name": benchmark_label,
                "normalized_return": benchmark_wealth - 1.0,
                "drawdown": (benchmark_wealth / benchmark_running_peak) - 1.0,
                "rolling_sharpe": _compute_rolling_sharpe(benchmark_frame["benchmark_return"]),
                "daily_return": benchmark_frame["benchmark_return"],
            }
        )
        comparison_frames.append(benchmark_frame)

    comparison_df = pd.concat(comparison_frames, ignore_index=True)
    return comparison_df.dropna(subset=["date"]).reset_index(drop=True)


def _benchmark_data_status(returns_df: pd.DataFrame) -> tuple[str, int, int]:
    if returns_df.empty or "benchmark_return" not in returns_df.columns:
        return "missing", 0, 0

    benchmark_returns = pd.to_numeric(returns_df["benchmark_return"], errors="coerce")
    observed_count = int(benchmark_returns.notna().sum())
    total_count = int(len(benchmark_returns))
    if observed_count == 0:
        return "missing", observed_count, total_count
    if observed_count < total_count:
        return "partial", observed_count, total_count
    return "full", observed_count, total_count


def _annualized_return_from_total_return(total_return: float | None, periods: int) -> float | None:
    if total_return is None or periods <= 0 or (1.0 + total_return) <= 0:
        return None
    return float((1.0 + total_return) ** (252.0 / periods) - 1.0)


def _compute_series_summary(
    returns: pd.Series,
    drawdowns: pd.Series,
    normalized_returns: pd.Series,
) -> dict[str, float | None]:
    daily_returns = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    drawdown_series = pd.to_numeric(drawdowns, errors="coerce").dropna().astype(float)
    normalized_series = pd.to_numeric(normalized_returns, errors="coerce").dropna().astype(float)
    periods = int(len(daily_returns))
    total_return = float(normalized_series.iloc[-1]) if not normalized_series.empty else None
    daily_std = float(daily_returns.std(ddof=1)) if periods > 1 else float("nan")
    annualized_volatility = daily_std * np.sqrt(252.0) if np.isfinite(daily_std) else None
    sharpe_ratio = (
        float(daily_returns.mean()) / daily_std * np.sqrt(252.0)
        if np.isfinite(daily_std) and daily_std > 0
        else None
    )
    return {
        "Total Return": total_return,
        "CAGR": _annualized_return_from_total_return(total_return, periods),
        "Sharpe": sharpe_ratio,
        "Volatility": annualized_volatility,
        "Max Drawdown": float(drawdown_series.min()) if not drawdown_series.empty else None,
    }


def _build_benchmark_summary_table(
    comparison_df: pd.DataFrame,
    *,
    strategy_name: str,
    benchmark_label: str,
) -> pd.DataFrame:
    if comparison_df.empty:
        return pd.DataFrame()

    summaries: dict[str, dict[str, float | None]] = {}
    for label in [strategy_name, benchmark_label]:
        series_frame = comparison_df[comparison_df["series_name"] == label].copy()
        summaries[label] = _compute_series_summary(
            series_frame["daily_return"],
            series_frame["drawdown"],
            series_frame["normalized_return"],
        )

    return pd.DataFrame(summaries)


def _format_comparison_metric(row_label: str, value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    numeric = float(value)
    if row_label in {"Total Return", "CAGR", "Volatility", "Max Drawdown"}:
        return f"{numeric * 100.0:.2f}%"
    return f"{numeric:.2f}"


def _render_strategy_benchmark_section(
    *,
    strategy_name: str,
    benchmark_symbol: str,
    detail_performance_df: pd.DataFrame,
    detail_returns_df: pd.DataFrame,
) -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-title">Benchmark Comparison</div>',
        unsafe_allow_html=True,
    )
    benchmark_label = f"Buy & Hold {benchmark_symbol}"

    if detail_performance_df.empty or detail_returns_df.empty:
        st.info(
            "No materialized strategy performance run was found for this strategy yet. "
            "Run the research strategy assets to unlock benchmark comparisons."
        )
        st.markdown("</div>", unsafe_allow_html=True)
        return

    detail_returns_df = detail_returns_df.copy()
    detail_returns_df["date"] = pd.to_datetime(detail_returns_df["date"])
    benchmark_status, benchmark_observations, total_observations = _benchmark_data_status(
        detail_returns_df
    )

    detail_performance_row = detail_performance_df.iloc[0]
    detail_snapshot_ts = detail_performance_row.get("asof_ts")
    if pd.notna(detail_snapshot_ts):
        snapshot_label = pd.to_datetime(detail_snapshot_ts).strftime("%B %d, %Y %H:%M")
        st.markdown(
            f'<div class="metric-pill" style="margin-bottom: 14px;">'
            f"Latest benchmark comparison snapshot: {snapshot_label}</div>",
            unsafe_allow_html=True,
        )

    if benchmark_status == "missing":
        st.info(
            "Benchmark comparison charts are unavailable for the latest run because the "
            "benchmark return series is missing."
        )
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if benchmark_status == "partial":
        st.info(
            "Benchmark return history is incomplete for the latest run. Comparison "
            f"charts below use {benchmark_observations:,} of {total_observations:,} "
            "available benchmark observations."
        )

    comparison_df = _build_benchmark_comparison_frame(
        detail_returns_df,
        strategy_name=strategy_name,
        benchmark_label=benchmark_label,
    )

    if comparison_df.empty:
        st.info("The latest run did not include enough return history to build the comparison.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    comparison_df["normalized_return_pct"] = comparison_df["normalized_return"] * 100.0
    comparison_df["drawdown_pct"] = comparison_df["drawdown"] * 100.0

    summary_table = _build_benchmark_summary_table(
        comparison_df,
        strategy_name=strategy_name,
        benchmark_label=benchmark_label,
    )

    strategy_summary = (
        summary_table[strategy_name] if not summary_table.empty else pd.Series(dtype=float)
    )
    benchmark_summary = (
        summary_table[benchmark_label] if not summary_table.empty else pd.Series(dtype=float)
    )
    total_return_delta = None
    if (
        not strategy_summary.empty
        and not benchmark_summary.empty
        and pd.notna(strategy_summary.get("Total Return"))
        and pd.notna(benchmark_summary.get("Total Return"))
    ):
        total_return_delta = float(strategy_summary["Total Return"]) - float(
            benchmark_summary["Total Return"]
        )
    sharpe_delta = None
    if (
        not strategy_summary.empty
        and not benchmark_summary.empty
        and pd.notna(strategy_summary.get("Sharpe"))
        and pd.notna(benchmark_summary.get("Sharpe"))
    ):
        sharpe_delta = float(strategy_summary["Sharpe"]) - float(
            benchmark_summary["Sharpe"]
        )

    st.markdown(
        '<div class="section-title" style="margin-top: 4px;">Quick Read</div>',
        unsafe_allow_html=True,
    )
    quick_read_cols = st.columns(3, gap="medium")
    with quick_read_cols[0]:
        st.metric(
            "Return Spread",
            _format_comparison_metric("Total Return", total_return_delta),
            f"vs {benchmark_symbol}",
        )
    with quick_read_cols[1]:
        st.metric(
            "Sharpe Spread",
            _format_comparison_metric("Sharpe", sharpe_delta),
            "rolling window: 63d chart below",
        )
    with quick_read_cols[2]:
        st.metric(
            "Observations",
            f"{len(detail_returns_df):,}",
            "daily return rows in latest run",
        )

    st.markdown(
        '<div class="section-title" style="margin-top: 18px;">Summary Metrics</div>',
        unsafe_allow_html=True,
    )
    if summary_table.empty:
        st.info("Summary metrics were unavailable for the latest run.")
    else:
        formatted_summary = summary_table.copy()
        for row_label in formatted_summary.index:
            formatted_summary.loc[row_label] = formatted_summary.loc[row_label].map(
                lambda value, row=row_label: _format_comparison_metric(row, value)
            )
        st.dataframe(formatted_summary, use_container_width=True)

    st.markdown(
        (
            '<div class="section-title" style="margin-top: 18px;">'
            "Normalized Cumulative Returns</div>"
        ),
        unsafe_allow_html=True,
    )
    series_domain = comparison_df["series_name"].dropna().astype(str).drop_duplicates().tolist()
    series_scale = alt.Scale(domain=series_domain, range=COLORWAY[:2])
    color_encoding = alt.Color(
        "series_name:N",
        title="Series",
        scale=series_scale,
        sort=series_domain,
    )
    shared_color_no_legend = alt.Color(
        "series_name:N",
        title="Series",
        scale=series_scale,
        sort=series_domain,
        legend=None,
    )
    stroke_dash_encoding = alt.StrokeDash(
        "series_name:N",
        sort=series_domain,
        scale=alt.Scale(domain=series_domain, range=[[1, 0], [6, 4]]),
        legend=None,
    )
    cumulative_chart_df = comparison_df.dropna(subset=["normalized_return_pct"])
    if cumulative_chart_df.empty:
        st.info("Normalized cumulative return data was unavailable for the latest run.")
    else:
        cumulative_chart = (
            alt.Chart(cumulative_chart_df)
            .mark_line(strokeWidth=2.5)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("normalized_return_pct:Q", title="Return from start (%)"),
                color=color_encoding,
                strokeDash=stroke_dash_encoding,
                detail="series_name:N",
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("series_name:N", title="Series"),
                    alt.Tooltip(
                        "normalized_return_pct:Q",
                        title="Return from start (%)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=340)
            .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
            .configure_legend(
                orient="top",
                direction="horizontal",
                titleColor="#b6c2e2",
                labelColor="#f4f7ff",
            )
        )
        st.altair_chart(cumulative_chart, use_container_width=True)

    lower_chart_left, lower_chart_right = st.columns(2, gap="large")
    drawdown_chart_df = comparison_df.dropna(subset=["drawdown_pct"])
    with lower_chart_left:
        if drawdown_chart_df.empty:
            st.info("Drawdown data was unavailable for the latest run.")
        else:
            drawdown_chart = (
                alt.Chart(drawdown_chart_df)
                .mark_line(strokeWidth=2.3)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("drawdown_pct:Q", title="Drawdown (%)"),
                    color=shared_color_no_legend,
                    strokeDash=stroke_dash_encoding,
                    detail="series_name:N",
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip("series_name:N", title="Series"),
                        alt.Tooltip("drawdown_pct:Q", title="Drawdown (%)", format=".2f"),
                    ],
                )
                .properties(height=280, title="Drawdown")
                .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
            )
            st.altair_chart(drawdown_chart, use_container_width=True)

    rolling_sharpe_df = comparison_df.dropna(subset=["rolling_sharpe"])
    if rolling_sharpe_df.empty:
        with lower_chart_right:
            st.info("Rolling Sharpe requires at least 21 observations in the latest run.")
    else:
        rolling_sharpe_chart = (
            alt.Chart(rolling_sharpe_df)
            .mark_line(strokeWidth=2.3)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("rolling_sharpe:Q", title="Sharpe ratio"),
                color=shared_color_no_legend,
                strokeDash=stroke_dash_encoding,
                detail="series_name:N",
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("series_name:N", title="Series"),
                    alt.Tooltip("rolling_sharpe:Q", title="Rolling Sharpe", format=".2f"),
                ],
            )
            .properties(height=280, title="Rolling Sharpe")
            .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
        )
        with lower_chart_right:
            st.altair_chart(rolling_sharpe_chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)


def _compute_holdings_rebalance_metrics(holdings_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "rebalance_date",
        "holdings_count",
        "turnover",
        "overlap_prev",
        "names_changed",
        "added_count",
        "removed_count",
    ]
    if holdings_df.empty:
        return pd.DataFrame(columns=columns)

    detail = holdings_df.copy()
    detail["rebalance_date"] = pd.to_datetime(detail["rebalance_date"])
    detail["target_weight"] = pd.to_numeric(detail["target_weight"], errors="coerce")
    detail["signal_value"] = pd.to_numeric(detail["signal_value"], errors="coerce")
    rebalances = sorted(detail["rebalance_date"].dropna().unique().tolist())

    rows: list[dict[str, object]] = []
    previous_frame = None
    for rebalance_date in rebalances:
        current_frame = detail.loc[detail["rebalance_date"] == rebalance_date].copy()
        current_weights = current_frame.set_index("symbol")["target_weight"].to_dict()
        current_symbols = set(current_weights)

        turnover = None
        overlap_prev = None
        added_count = None
        removed_count = None
        names_changed = None
        if previous_frame is not None:
            previous_weights = previous_frame.set_index("symbol")["target_weight"].to_dict()
            previous_symbols = set(previous_weights)
            union_symbols = current_symbols | previous_symbols
            turnover = 0.5 * sum(
                abs(
                    float(current_weights.get(symbol, 0.0))
                    - float(previous_weights.get(symbol, 0.0))
                )
                for symbol in union_symbols
            )
            intersection_count = len(current_symbols & previous_symbols)
            overlap_prev = (
                intersection_count / len(previous_symbols) if previous_symbols else None
            )
            added_count = len(current_symbols - previous_symbols)
            removed_count = len(previous_symbols - current_symbols)
            names_changed = (added_count or 0) + (removed_count or 0)

        rows.append(
            {
                "rebalance_date": rebalance_date,
                "holdings_count": int(len(current_frame)),
                "turnover": turnover,
                "overlap_prev": overlap_prev,
                "names_changed": names_changed,
                "added_count": added_count,
                "removed_count": removed_count,
            }
        )
        previous_frame = current_frame

    return pd.DataFrame(rows)


def _compute_average_holding_period_rebalances(holdings_df: pd.DataFrame) -> float | None:
    if holdings_df.empty:
        return None

    detail = holdings_df.copy()
    detail["rebalance_date"] = pd.to_datetime(detail["rebalance_date"])
    rebalances = sorted(detail["rebalance_date"].dropna().unique().tolist())
    rebalance_lookup = {rebalance_date: idx for idx, rebalance_date in enumerate(rebalances)}
    streak_lengths: list[int] = []

    for _, symbol_frame in detail.groupby("symbol", sort=False):
        indices = sorted(
            {
                rebalance_lookup[rebalance_date]
                for rebalance_date in pd.to_datetime(symbol_frame["rebalance_date"]).tolist()
                if rebalance_date in rebalance_lookup
            }
        )
        if not indices:
            continue
        streak_length = 1
        for current_idx, previous_idx in zip(indices[1:], indices[:-1], strict=False):
            if current_idx == previous_idx + 1:
                streak_length += 1
            else:
                streak_lengths.append(streak_length)
                streak_length = 1
        streak_lengths.append(streak_length)

    if not streak_lengths:
        return None
    return float(np.mean(streak_lengths))


def _compute_rebalance_changes(
    holdings_df: pd.DataFrame,
    rebalance_date: pd.Timestamp | None = None,
) -> tuple[pd.Timestamp | None, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if holdings_df.empty:
        empty = pd.DataFrame()
        return None, empty, empty, empty

    detail = holdings_df.copy()
    detail["rebalance_date"] = pd.to_datetime(detail["rebalance_date"])
    rebalances = sorted(detail["rebalance_date"].dropna().unique().tolist())
    target_rebalance = (
        pd.to_datetime(rebalance_date) if rebalance_date is not None else rebalances[-1]
    )
    if target_rebalance not in rebalances:
        empty = pd.DataFrame()
        return None, empty, empty, empty
    latest_rebalance = target_rebalance
    latest_frame = detail.loc[detail["rebalance_date"] == latest_rebalance].copy()
    if len(rebalances) < 2:
        empty = pd.DataFrame()
        return latest_rebalance, latest_frame, empty, empty

    target_index = rebalances.index(latest_rebalance)
    if target_index == 0:
        empty = pd.DataFrame()
        return latest_rebalance, latest_frame, empty, empty
    previous_rebalance = rebalances[target_index - 1]
    previous_frame = detail.loc[detail["rebalance_date"] == previous_rebalance].copy()

    latest_symbols = set(latest_frame["symbol"])
    previous_symbols = set(previous_frame["symbol"])

    added_df = latest_frame.loc[
        latest_frame["symbol"].isin(latest_symbols - previous_symbols)
    ].copy()
    removed_df = previous_frame.loc[
        previous_frame["symbol"].isin(previous_symbols - latest_symbols)
    ].copy()
    return latest_rebalance, latest_frame, added_df, removed_df


def _compute_symbol_frequency_table(holdings_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "symbol",
        "rebalances_held",
        "presence_pct",
        "average_weight",
        "average_rank",
        "average_signal_score",
        "first_seen",
        "last_seen",
    ]
    if holdings_df.empty:
        return pd.DataFrame(columns=columns)

    detail = holdings_df.copy()
    detail["rebalance_date"] = pd.to_datetime(detail["rebalance_date"])
    detail["target_weight"] = pd.to_numeric(detail["target_weight"], errors="coerce")
    detail["entry_rank"] = pd.to_numeric(detail["entry_rank"], errors="coerce")
    detail["signal_value"] = pd.to_numeric(detail["signal_value"], errors="coerce")
    total_rebalances = max(int(detail["rebalance_date"].nunique()), 1)

    frequency_df = (
        detail.groupby("symbol", sort=False)
        .agg(
            rebalances_held=("rebalance_date", "nunique"),
            average_weight=("target_weight", "mean"),
            average_rank=("entry_rank", "mean"),
            average_signal_score=("signal_value", "mean"),
            first_seen=("rebalance_date", "min"),
            last_seen=("rebalance_date", "max"),
        )
        .reset_index()
    )
    frequency_df["presence_pct"] = frequency_df["rebalances_held"] / total_rebalances
    return frequency_df.sort_values(
        ["rebalances_held", "average_signal_score", "symbol"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _load_research_price_history(symbols: tuple[str, ...]) -> tuple[pd.DataFrame, str | None]:
    columns = ["symbol", "trade_date", "close"]
    clean_symbols = tuple(
        sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    )
    if not clean_symbols:
        return pd.DataFrame(columns=columns), None

    price_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data")) / "silver" / "research_daily_prices"
    if not price_root.exists():
        return pd.DataFrame(columns=columns), "Research daily price history was not found on disk."

    con = duckdb.connect(":memory:")
    try:
        prices_df = con.execute(
            """
            SELECT
                upper(symbol) AS symbol,
                trade_date,
                close
            FROM read_parquet(?)
            WHERE upper(symbol) = ANY(?)
              AND close IS NOT NULL
            ORDER BY symbol, trade_date
            """,
            [_resolve_research_prices_glob(), list(clean_symbols)],
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(columns=columns), f"Failed to load research price history: {exc}"
    finally:
        con.close()

    if not prices_df.empty:
        prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"])
        prices_df["close"] = pd.to_numeric(prices_df["close"], errors="coerce")
    return prices_df, None


def _compute_symbol_cagr_table(
    holdings_df: pd.DataFrame,
    prices_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = ["symbol", "holding_spells", "average_cagr", "median_cagr", "latest_cagr"]
    if holdings_df.empty or prices_df.empty:
        return pd.DataFrame(columns=columns)

    detail = holdings_df.copy()
    detail["rebalance_date"] = pd.to_datetime(detail["rebalance_date"])
    rebalances = sorted(detail["rebalance_date"].dropna().unique().tolist())
    rebalance_lookup = {rebalance_date: idx for idx, rebalance_date in enumerate(rebalances)}

    price_frame = prices_df.copy()
    price_frame["trade_date"] = pd.to_datetime(price_frame["trade_date"])
    price_frame["close"] = pd.to_numeric(price_frame["close"], errors="coerce")
    price_map = {
        symbol: frame.sort_values("trade_date", kind="stable").reset_index(drop=True)
        for symbol, frame in price_frame.groupby("symbol", sort=False)
    }

    rows: list[dict[str, object]] = []
    for symbol, symbol_frame in detail.groupby("symbol", sort=False):
        indices = sorted(
            {
                rebalance_lookup[rebalance_date]
                for rebalance_date in pd.to_datetime(symbol_frame["rebalance_date"]).tolist()
                if rebalance_date in rebalance_lookup
            }
        )
        if not indices or symbol not in price_map:
            continue

        symbol_prices = price_map[symbol]
        trade_dates = symbol_prices["trade_date"]
        close_prices = symbol_prices["close"]
        symbol_cagrs: list[float] = []

        streak_start_idx = indices[0]
        streak_end_idx = indices[0]
        for current_idx in indices[1:] + [None]:
            if current_idx is not None and current_idx == streak_end_idx + 1:
                streak_end_idx = current_idx
                continue

            start_date = rebalances[streak_start_idx]
            if streak_end_idx + 1 < len(rebalances):
                end_date = rebalances[streak_end_idx + 1]
            else:
                end_date = trade_dates.max()

            start_position = int(trade_dates.searchsorted(start_date, side="left"))
            end_position = int(trade_dates.searchsorted(end_date, side="left"))
            if start_position >= len(symbol_prices) or end_position >= len(symbol_prices):
                streak_start_idx = current_idx if current_idx is not None else streak_start_idx
                streak_end_idx = current_idx if current_idx is not None else streak_end_idx
                continue

            start_trade_date = trade_dates.iloc[start_position]
            end_trade_date = trade_dates.iloc[end_position]
            start_price = float(close_prices.iloc[start_position])
            end_price = float(close_prices.iloc[end_position])
            elapsed_days = int((end_trade_date - start_trade_date).days)
            if start_price > 0 and end_price > 0 and elapsed_days > 0:
                symbol_cagrs.append(
                    float((end_price / start_price) ** (365.25 / elapsed_days) - 1.0)
                )

            if current_idx is not None:
                streak_start_idx = current_idx
                streak_end_idx = current_idx

        if symbol_cagrs:
            rows.append(
                {
                    "symbol": symbol,
                    "holding_spells": int(len(symbol_cagrs)),
                    "average_cagr": float(np.mean(symbol_cagrs)),
                    "median_cagr": float(np.median(symbol_cagrs)),
                    "latest_cagr": float(symbol_cagrs[-1]),
                }
            )

    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows).sort_values(
        ["average_cagr", "holding_spells", "symbol"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _read_query_param(name: str) -> str | None:
    value = st.query_params.get(name)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _set_lab_query_params(view: str, strategy_id: str | None = None) -> None:
    st.query_params.clear()
    st.query_params["lab_view"] = view
    if strategy_id:
        st.query_params["strategy_id"] = strategy_id


def _pretty_label(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "n/a"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip()
    if not text:
        return "n/a"
    return text.replace("_", " ").title()


def _parse_strategy_config(raw_config: object) -> dict[str, object]:
    if isinstance(raw_config, dict):
        return raw_config
    if isinstance(raw_config, str) and raw_config.strip():
        try:
            parsed = json.loads(raw_config)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _derive_signal_labels(
    definition_row: pd.Series,
    parameters_df: pd.DataFrame,
    config: dict[str, object],
) -> list[str]:
    signal_rows = parameters_df[
        parameters_df["parameter_name"].astype(str).str.contains("signal", case=False, na=False)
    ]
    signal_values = []
    for value in signal_rows["parameter_value"].tolist():
        label = _pretty_label(value)
        if label != "n/a":
            signal_values.append(label)
    if signal_values:
        return list(dict.fromkeys(signal_values))

    if str(config.get("selection_mode", "")).lower() == "fixed_symbol":
        symbol_rows = parameters_df[
            parameters_df["parameter_name"].astype(str).str.lower() == "symbol"
        ]
        if not symbol_rows.empty:
            symbol_value = symbol_rows["parameter_value"].iloc[0]
            return [f"Fixed Symbol: {symbol_value}"]

    ranking_method = definition_row.get("ranking_method")
    if pd.notna(ranking_method):
        return [_pretty_label(ranking_method)]
    return ["n/a"]


def _render_definition_metric(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div
          class="metric-pill"
          style="width: 100%; justify-content: space-between; margin-bottom: 10px;"
        >
          <span>{label}</span>
          <strong style="color: var(--ink-1);">{value}</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
query_view = _read_query_param("lab_view")
if query_view not in LAB_VIEWS:
    query_view = "compare"

query_strategy_id = _read_query_param("strategy_id")
valid_strategy_ids = set(catalog_df["strategy_id"].astype(str).tolist())

default_detail_strategy_id = (
    query_strategy_id
    if query_strategy_id in valid_strategy_ids
    else str(catalog_df["strategy_id"].iloc[0])
)

tab_keys = ["compare", "detail"]
if query_view == "detail":
    tab_keys = ["detail", "compare"]
tab_labels = [LAB_VIEWS[key] for key in tab_keys]
tabs = dict(zip(tab_keys, st.tabs(tab_labels), strict=True))

with tabs["compare"]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Strategy Comparison</div>', unsafe_allow_html=True)

    selected_labels = st.multiselect(
        "Select up to 5 strategies",
        options=catalog_df["selector_label"].tolist(),
        default=default_labels,
        max_selections=5,
        help=(
            "Strategies use the catalog's human-readable names. Comparison pulls "
            "the latest materialized run for each strategy."
        ),
    )

    if not selected_labels:
        st.info("Select at least one strategy to start the comparison.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    selected_rows = catalog_df[catalog_df["selector_label"].isin(selected_labels)].copy()
    selected_rows = selected_rows.set_index("selector_label").loc[selected_labels].reset_index()

    drilldown_options = []
    for _, row in selected_rows.iterrows():
        strategy_id = str(row["strategy_id"])
        strategy_name = str(row["strategy_name"])
        drilldown_options.append(
            f"[{strategy_name} Definition](?lab_view=detail&strategy_id={strategy_id})"
        )
    st.markdown(
        "Open a selected strategy in the detail tab: " + " | ".join(drilldown_options),
        unsafe_allow_html=False,
    )

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
        latest_snapshot = pd.to_datetime(latest_asof).strftime("%B %d, %Y %H:%M")
        st.markdown(
            f'<div class="metric-pill">Latest strategy snapshot: {latest_snapshot}</div>',
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
            st.markdown(
                f"[Open In Strategy Detail](?lab_view=detail&strategy_id={row['strategy_id']})"
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
        if chart_df.empty:
            st.info(
                "Cumulative return data was unavailable for the selected runs, so the chart "
                "could not be rendered."
            )
        else:
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
                    formatted_metric_table.loc[row_label] = formatted_metric_table.loc[
                        row_label
                    ].map(
                        lambda value: f"{float(value) * 100.0:.2f}%" if pd.notna(value) else "n/a"
                    )
                else:
                    formatted_metric_table.loc[row_label] = formatted_metric_table.loc[
                        row_label
                    ].map(lambda value: f"{float(value):.2f}" if pd.notna(value) else "n/a")
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
                "Factor exposure estimates were unavailable. This usually means the selected "
                "strategies "
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
            "rebalance_frequency",
            "weighting_method",
            "benchmark_symbol",
            "target_count",
            "description",
        ]
    ].rename(
        columns={
            "strategy_name": "Strategy",
            "strategy_id": "ID",
            "ranking_method": "Ranking Method",
            "rebalance_frequency": "Rebalance",
            "weighting_method": "Weighting",
            "benchmark_symbol": "Benchmark",
            "target_count": "Target Holdings",
            "description": "Description",
        }
    )
    st.dataframe(selected_display, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with tabs["detail"]:
    detail_options = catalog_df["selector_label"].tolist()
    detail_default_index = int(
        catalog_df.index[catalog_df["strategy_id"].astype(str) == default_detail_strategy_id][0]
    )

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Strategy Definition</div>', unsafe_allow_html=True)

    selected_detail_label = st.selectbox(
        "Select strategy",
        options=detail_options,
        index=detail_default_index,
        help="Choose any active research strategy to inspect its definition and metadata.",
    )
    selected_detail_row = catalog_df.loc[
        catalog_df["selector_label"] == selected_detail_label
    ].iloc[0]
    selected_detail_strategy_id = str(selected_detail_row["strategy_id"])

    if query_view == "detail" and selected_detail_strategy_id != query_strategy_id:
        _set_lab_query_params("detail", selected_detail_strategy_id)
        st.rerun()

    (
        definition_df,
        parameters_df,
        detail_performance_df,
        detail_returns_df,
        detail_holdings_df,
        detail_error,
    ) = _load_strategy_detail_payload(selected_detail_strategy_id)
    if detail_error:
        st.info(detail_error)
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    if definition_df.empty:
        st.info("The selected strategy was not found in `silver.strategy_definitions`.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    definition_row = definition_df.iloc[0]
    config = _parse_strategy_config(definition_row.get("config_json"))
    signal_labels = _derive_signal_labels(definition_row, parameters_df, config)

    description_text = (
        str(definition_row["description"])
        if pd.notna(definition_row["description"])
        else "No strategy description is available yet."
    )
    strategy_version_text = (
        f'{definition_row["strategy_id"]} | {definition_row["strategy_version"]}'
    )

    st.markdown(
        f"""
        <div style="font-size: 1.5rem; font-weight: 700; margin-bottom: 6px;">
          {definition_row["strategy_name"]}
        </div>
        <div style="color: var(--ink-2); margin-bottom: 16px;">
          {description_text}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f'<div class="metric-pill" style="margin-bottom: 12px;">{strategy_version_text}</div>',
        unsafe_allow_html=True,
    )

    metadata_col, config_col = st.columns([1.05, 1], gap="large")

    with metadata_col:
        _render_definition_metric("Universe", _pretty_label(config.get("universe")))
        _render_definition_metric(
            "Rebalance Frequency",
            _pretty_label(definition_row.get("rebalance_frequency")),
        )
        _render_definition_metric(
            "Weighting Method",
            _pretty_label(definition_row.get("weighting_method")),
        )
        _render_definition_metric(
            "Holding Count",
            str(int(definition_row["target_count"]))
            if pd.notna(definition_row.get("target_count"))
            else "n/a",
        )
        _render_definition_metric("Signals Used", ", ".join(signal_labels))
        _render_definition_metric(
            "Ranking Method",
            _pretty_label(definition_row.get("ranking_method")),
        )

    with config_col:
        _render_definition_metric(
            "Selection Mode",
            _pretty_label(config.get("selection_mode")),
        )
        _render_definition_metric(
            "Rebalance Anchor",
            _pretty_label(config.get("rebalance_anchor")),
        )
        _render_definition_metric(
            "Benchmark",
            _pretty_label(definition_row.get("benchmark_symbol")),
        )
        _render_definition_metric(
            "Long / Short",
            "Long / Short" if bool(definition_row.get("long_short_flag")) else "Long Only",
        )
        _render_definition_metric("Start Date", _pretty_label(definition_row.get("start_date")))
        _render_definition_metric("End Date", _pretty_label(definition_row.get("end_date")))

    st.markdown("</div>", unsafe_allow_html=True)

    _render_strategy_benchmark_section(
        strategy_name=str(definition_row["strategy_name"]),
        benchmark_symbol=str(definition_row.get("benchmark_symbol") or "SPY").strip().upper()
        or "SPY",
        detail_performance_df=detail_performance_df,
        detail_returns_df=detail_returns_df,
    )

    detail_tab, holdings_tab = st.tabs(["Details", "Holdings"])

    with holdings_tab:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Current Holdings</div>',
            unsafe_allow_html=True,
        )
        if detail_holdings_df.empty:
            st.info(
                "No materialized holdings snapshot was found for this strategy yet. "
                "Run the research strategy assets to unlock holdings detail."
            )
        else:
            detail_holdings_df = detail_holdings_df.copy()
            detail_holdings_df["rebalance_date"] = pd.to_datetime(
                detail_holdings_df["rebalance_date"]
            )
            detail_holdings_df["target_weight"] = pd.to_numeric(
                detail_holdings_df["target_weight"], errors="coerce"
            )
            detail_holdings_df["entry_rank"] = pd.to_numeric(
                detail_holdings_df["entry_rank"], errors="coerce"
            )
            detail_holdings_df["signal_value"] = pd.to_numeric(
                detail_holdings_df["signal_value"], errors="coerce"
            )

            rebalance_dates = sorted(
                detail_holdings_df["rebalance_date"].dropna().unique().tolist(),
                reverse=True,
            )
            selected_rebalance = st.selectbox(
                "Rebalance date",
                options=rebalance_dates,
                index=0,
                format_func=lambda value: pd.to_datetime(value).strftime("%B %d, %Y"),
                key=f"holdings_tab_rebalance_{selected_detail_strategy_id}",
                help="Inspect holdings at any rebalance date for this strategy.",
            )

            selected_holdings_df = detail_holdings_df.loc[
                detail_holdings_df["rebalance_date"] == selected_rebalance
            ].copy()
            selected_holdings_df["weight_pct"] = selected_holdings_df["target_weight"] * 100.0
            selected_date_label = pd.to_datetime(selected_rebalance).strftime("%B %d, %Y")
            st.markdown(
                f'<div class="metric-pill" style="margin-bottom: 14px;">'
                f"Showing {len(selected_holdings_df):,} holdings for {selected_date_label}</div>",
                unsafe_allow_html=True,
            )
            expected_target_count = (
                int(definition_row["target_count"])
                if pd.notna(definition_row.get("target_count"))
                else None
            )
            selected_symbols = selected_holdings_df["symbol"].astype(str).str.upper().tolist()
            if (
                expected_target_count is not None
                and expected_target_count > 1
                and len(selected_holdings_df) == 1
                and selected_symbols == ["SPY"]
            ):
                st.warning(
                    "This rebalance snapshot contains only `SPY` even though the strategy target "
                    f"holding count is {expected_target_count}. That points to an upstream "
                    "holdings materialization issue rather than a real rebalance."
                )
            holdings_display = selected_holdings_df.rename(
                columns={
                    "symbol": "Symbol",
                    "weight_pct": "Weight",
                    "entry_rank": "Rank",
                    "signal_value": "Signal Score",
                }
            )[["Symbol", "Weight", "Rank", "Signal Score"]]
            st.dataframe(
                holdings_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Weight": st.column_config.NumberColumn(format="%.2f%%"),
                    "Rank": st.column_config.NumberColumn(format="%d"),
                    "Signal Score": st.column_config.NumberColumn(format="%.4f"),
                },
            )

            st.markdown(
                (
                    '<div class="section-title" style="margin-top: 18px;">'
                    "What Changed At Selected Rebalance</div>"
                ),
                unsafe_allow_html=True,
            )
            selected_rebalance_ts, _, added_df, removed_df = _compute_rebalance_changes(
                detail_holdings_df,
                selected_rebalance,
            )
            if selected_rebalance_ts is None:
                st.info("The selected rebalance could not be resolved from holdings history.")
            elif added_df.empty and removed_df.empty:
                st.info(
                    "There is not enough rebalance history yet to compare the selected holdings "
                    "against the prior rebalance."
                )
            else:
                rebalance_metrics_df = _compute_holdings_rebalance_metrics(detail_holdings_df)
                latest_metrics = rebalance_metrics_df.loc[
                    rebalance_metrics_df["rebalance_date"] == selected_rebalance_ts
                ].copy()
                change_metric_cols = st.columns(4, gap="medium")
                with change_metric_cols[0]:
                    st.metric("Added", f"{len(added_df):,}")
                with change_metric_cols[1]:
                    st.metric("Removed", f"{len(removed_df):,}")
                with change_metric_cols[2]:
                    overlap_value = latest_metrics["overlap_prev"].iloc[0]
                    st.metric(
                        "Overlap",
                        f"{float(overlap_value) * 100.0:.1f}%"
                        if pd.notna(overlap_value)
                        else "n/a",
                    )
                with change_metric_cols[3]:
                    turnover_value = latest_metrics["turnover"].iloc[0]
                    st.metric(
                        "Turnover",
                        f"{float(turnover_value) * 100.0:.1f}%"
                        if pd.notna(turnover_value)
                        else "n/a",
                    )

                change_chart_df = pd.concat(
                    [
                        added_df.assign(change_type="Added", signed_score=added_df["signal_value"]),
                        removed_df.assign(
                            change_type="Removed",
                            signed_score=-removed_df["signal_value"].abs(),
                        ),
                    ],
                    ignore_index=True,
                )
                if not change_chart_df.empty:
                    change_chart = (
                        alt.Chart(change_chart_df)
                        .mark_bar(cornerRadius=6)
                        .encode(
                            x=alt.X("signed_score:Q", title="Signal-score view"),
                            y=alt.Y("symbol:N", sort="-x", title="Symbol"),
                            color=alt.Color(
                                "change_type:N",
                                title="Change",
                                scale=alt.Scale(
                                    domain=["Added", "Removed"],
                                    range=["#6ee7b7", "#f97316"],
                                ),
                            ),
                            tooltip=[
                                alt.Tooltip("symbol:N", title="Symbol"),
                                alt.Tooltip("change_type:N", title="Change"),
                                alt.Tooltip("signal_value:Q", title="Signal Score", format=".4f"),
                            ],
                        )
                        .properties(height=max(180, 24 * len(change_chart_df)))
                        .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
                    )
                    with st.container(height=360):
                        st.altair_chart(change_chart, use_container_width=True)

                added_col, removed_col = st.columns(2, gap="large")
                with added_col:
                    st.markdown(
                        '<div class="section-title" style="margin-top: 8px;">Added Names</div>',
                        unsafe_allow_html=True,
                    )
                    if added_df.empty:
                        st.info("No new names were added at the latest rebalance.")
                    else:
                        added_display = added_df.copy()
                        added_display["target_weight"] = added_display["target_weight"] * 100.0
                        st.dataframe(
                            added_display.rename(
                                columns={
                                    "symbol": "Symbol",
                                    "target_weight": "Weight (%)",
                                    "entry_rank": "Rank",
                                    "signal_value": "Signal Score",
                                }
                            )[["Symbol", "Weight (%)", "Rank", "Signal Score"]],
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Weight (%)": st.column_config.NumberColumn(format="%.2f"),
                                "Rank": st.column_config.NumberColumn(format="%d"),
                                "Signal Score": st.column_config.NumberColumn(format="%.4f"),
                            },
                        )
                with removed_col:
                    st.markdown(
                        '<div class="section-title" style="margin-top: 8px;">Removed Names</div>',
                        unsafe_allow_html=True,
                    )
                    if removed_df.empty:
                        st.info("No names were removed at the latest rebalance.")
                    else:
                        removed_display = removed_df.copy()
                        removed_display["target_weight"] = removed_display["target_weight"] * 100.0
                        st.dataframe(
                            removed_display.rename(
                                columns={
                                    "symbol": "Symbol",
                                    "target_weight": "Prior Weight (%)",
                                    "entry_rank": "Prior Rank",
                                    "signal_value": "Prior Signal Score",
                                }
                            )[["Symbol", "Prior Weight (%)", "Prior Rank", "Prior Signal Score"]],
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Prior Weight (%)": st.column_config.NumberColumn(format="%.2f"),
                                "Prior Rank": st.column_config.NumberColumn(format="%d"),
                                "Prior Signal Score": st.column_config.NumberColumn(format="%.4f"),
                            },
                        )

            st.markdown(
                (
                    '<div class="section-title" style="margin-top: 18px;">'
                    "Portfolio Stability Over Time</div>"
                ),
                unsafe_allow_html=True,
            )
            rebalance_metrics_df = _compute_holdings_rebalance_metrics(detail_holdings_df)
            stability_df = rebalance_metrics_df.dropna(subset=["turnover", "overlap_prev"]).copy()
            average_holding_period = _compute_average_holding_period_rebalances(detail_holdings_df)

            stability_cols = st.columns(4, gap="medium")
            avg_turnover = stability_df["turnover"].mean() if not stability_df.empty else np.nan
            avg_names_changed = (
                stability_df["names_changed"].mean() if not stability_df.empty else np.nan
            )
            avg_overlap = (
                stability_df["overlap_prev"].mean() if not stability_df.empty else np.nan
            )
            with stability_cols[0]:
                st.metric(
                    "Avg Turnover",
                    f"{float(avg_turnover) * 100.0:.1f}%" if pd.notna(avg_turnover) else "n/a",
                )
            with stability_cols[1]:
                st.metric(
                    "Names Changed",
                    f"{float(avg_names_changed):.1f}" if pd.notna(avg_names_changed) else "n/a",
                )
            with stability_cols[2]:
                st.metric(
                    "Avg Overlap",
                    f"{float(avg_overlap) * 100.0:.1f}%" if pd.notna(avg_overlap) else "n/a",
                )
            with stability_cols[3]:
                st.metric(
                    "Avg Holding Period",
                    f"{average_holding_period:.1f} rebalances"
                    if average_holding_period is not None
                    else "n/a",
                )

            if stability_df.empty:
                st.info(
                    "A stability time series needs at least two rebalances before turnover and "
                    "overlap can be calculated."
                )
            else:
                stability_df["turnover_pct"] = stability_df["turnover"] * 100.0
                turnover_chart = (
                    alt.Chart(stability_df)
                    .mark_line(point=True, strokeWidth=2.6)
                    .encode(
                        x=alt.X("rebalance_date:T", title="Rebalance Date"),
                        y=alt.Y("turnover_pct:Q", title="Turnover (%)"),
                        tooltip=[
                            alt.Tooltip("rebalance_date:T", title="Rebalance Date"),
                            alt.Tooltip("turnover_pct:Q", title="Turnover (%)", format=".2f"),
                            alt.Tooltip("overlap_prev:Q", title="Overlap", format=".3f"),
                            alt.Tooltip("names_changed:Q", title="Names Changed", format=","),
                        ],
                    )
                    .properties(height=280)
                    .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
                )
                st.altair_chart(turnover_chart, use_container_width=True)

                stability_table = stability_df.rename(
                    columns={
                        "rebalance_date": "Rebalance Date",
                        "turnover_pct": "Turnover (%)",
                        "overlap_prev": "Overlap",
                        "names_changed": "Names Changed",
                        "added_count": "Added",
                        "removed_count": "Removed",
                    }
                )[[
                    "Rebalance Date",
                    "Turnover (%)",
                    "Overlap",
                    "Names Changed",
                    "Added",
                    "Removed",
                ]]
                st.dataframe(
                    stability_table,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Rebalance Date": st.column_config.DateColumn(format="MMM DD, YYYY"),
                        "Turnover (%)": st.column_config.NumberColumn(format="%.2f"),
                        "Overlap": st.column_config.NumberColumn(format="%.3f"),
                        "Names Changed": st.column_config.NumberColumn(format="%d"),
                        "Added": st.column_config.NumberColumn(format="%d"),
                        "Removed": st.column_config.NumberColumn(format="%d"),
                    },
                )

            frequency_df = _compute_symbol_frequency_table(detail_holdings_df)
            recurring_col, cagr_col = st.columns(2, gap="large")
            with recurring_col:
                st.markdown(
                    '<div class="section-title" style="margin-top: 18px;">Recurring Names</div>',
                    unsafe_allow_html=True,
                )
                if frequency_df.empty:
                    st.info("No recurring-name statistics were available for this strategy yet.")
                else:
                    recurring_display = frequency_df.head(15).copy()
                    recurring_display["presence_pct"] = recurring_display["presence_pct"] * 100.0
                    recurring_display["average_weight"] = (
                        recurring_display["average_weight"] * 100.0
                    )
                    st.dataframe(
                        recurring_display.rename(
                            columns={
                                "symbol": "Symbol",
                                "rebalances_held": "Rebalances Held",
                                "presence_pct": "Presence (%)",
                                "average_weight": "Avg Weight (%)",
                                "average_rank": "Avg Rank",
                                "average_signal_score": "Avg Signal Score",
                                "first_seen": "First Seen",
                                "last_seen": "Last Seen",
                            }
                        ),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Rebalances Held": st.column_config.NumberColumn(format="%d"),
                            "Presence (%)": st.column_config.NumberColumn(format="%.1f"),
                            "Avg Weight (%)": st.column_config.NumberColumn(format="%.2f"),
                            "Avg Rank": st.column_config.NumberColumn(format="%.1f"),
                            "Avg Signal Score": st.column_config.NumberColumn(format="%.4f"),
                            "First Seen": st.column_config.DateColumn(format="MMM DD, YYYY"),
                            "Last Seen": st.column_config.DateColumn(format="MMM DD, YYYY"),
                        },
                    )

            with cagr_col:
                st.markdown(
                    (
                        '<div class="section-title" style="margin-top: 18px;">'
                        "Strongest Holding Outcomes</div>"
                    ),
                    unsafe_allow_html=True,
                )
                prices_df, prices_error = _load_research_price_history(
                    tuple(frequency_df["symbol"].tolist())
                )
                cagr_df = _compute_symbol_cagr_table(detail_holdings_df, prices_df)
                if prices_error:
                    st.info(prices_error)
                elif cagr_df.empty:
                    st.info(
                        "Symbol-level CAGR estimates were unavailable for this strategy's holdings "
                        "history."
                    )
                else:
                    cagr_display = cagr_df.head(12).copy()
                    for column in ["average_cagr", "median_cagr", "latest_cagr"]:
                        cagr_display[column] = cagr_display[column] * 100.0
                    st.dataframe(
                        cagr_display.rename(
                            columns={
                                "symbol": "Symbol",
                                "holding_spells": "Holding Spells",
                                "average_cagr": "Average CAGR (%)",
                                "median_cagr": "Median CAGR (%)",
                                "latest_cagr": "Latest Spell CAGR (%)",
                            }
                        ),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Holding Spells": st.column_config.NumberColumn(format="%d"),
                            "Average CAGR (%)": st.column_config.NumberColumn(format="%.2f"),
                            "Median CAGR (%)": st.column_config.NumberColumn(format="%.2f"),
                            "Latest Spell CAGR (%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
        st.markdown("</div>", unsafe_allow_html=True)

    with detail_tab:
        parameter_cols = [
            "parameter_name",
            "parameter_value",
            "parameter_type",
            "effective_start_date",
            "effective_end_date",
            "description",
        ]
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Active Signals And Parameters</div>',
            unsafe_allow_html=True,
        )
        if parameters_df.empty:
            st.info("No active parameter rows were found for this strategy.")
        else:
            parameter_display = parameters_df[parameter_cols].rename(
                columns={
                    "parameter_name": "Parameter",
                    "parameter_value": "Value",
                    "parameter_type": "Type",
                    "effective_start_date": "Effective Start",
                    "effective_end_date": "Effective End",
                    "description": "Description",
                }
            )
            st.dataframe(parameter_display, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

