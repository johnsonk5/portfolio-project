import os
import random
from pathlib import Path

import html
import duckdb
import pandas as pd
import streamlit as st
from streamlit_plotly_events import plotly_events
import plotly.graph_objects as go

from portfolio_project.components.hover_bar import hover_bar_chart, hover_grouped_bar_chart

st.set_page_config(
    page_title="Market Vibecheck",
    page_icon="MV",
    layout="wide",
)

with st.sidebar:
    st.page_link("streamlit_app.py", label="Market Vibecheck")
    st.page_link("pages/Deep_Dive.py", label="Deep Dive")


CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

:root {
  --bg-1: #0b1020;
  --bg-2: #0f1a36;
  --ink-1: #f4f7ff;
  --ink-2: #b6c2e2;
  --accent: #6ee7b7;
  --muted: #1d2746;
  --card: rgba(255, 255, 255, 0.05);
  --card-border: rgba(255, 255, 255, 0.08);
  --gain: #22c55e;
  --loss: #ef4444;
}

html, body, [class*="css"]  {
  font-family: 'Space Grotesk', system-ui, -apple-system, Segoe UI, sans-serif;
}

.stApp {
  background: radial-gradient(1200px 600px at 10% -10%, #1a2453 0%, transparent 60%),
              radial-gradient(900px 500px at 90% 0%, #123d5c 0%, transparent 55%),
              linear-gradient(180deg, var(--bg-1), var(--bg-2));
  color: var(--ink-1);
}

.header-hero {
  background: linear-gradient(110deg, rgba(110, 231, 183, 0.12), rgba(59, 130, 246, 0.08));
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
  height: 100%;
}

.section-title {
  font-size: 1.05rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
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

.tooltip {
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  cursor: help;
}

.tooltip::after {
  content: attr(data-tip);
  position: absolute;
  bottom: 120%;
  left: 0;
  z-index: 10;
  width: 220px;
  padding: 8px 10px;
  border-radius: 10px;
  background: rgba(15, 23, 42, 0.95);
  color: var(--ink-1);
  font-size: 0.82rem;
  line-height: 1.25;
  box-shadow: 0 10px 24px rgba(0, 0, 0, 0.35);
  opacity: 0;
  transform: translateY(6px);
  transition: opacity 0.2s ease, transform 0.2s ease;
  pointer-events: none;
}

.tooltip:hover::after {
  opacity: 1;
  transform: translateY(0);
}

[data-testid="stSidebarNav"] {
  display: none;
}

.ticker-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.92rem;
  background: rgba(6, 10, 20, 0.65);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  overflow: hidden;
}

.ticker-table thead th {
  text-align: left;
  color: var(--ink-2);
  font-size: 0.78rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 8px 10px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
  white-space: nowrap;
}

.ticker-table thead th.mom-tight {
  font-size: 0.7rem;
  letter-spacing: 0.06em;
}

.ticker-table tbody td {
  padding: 8px 10px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

.ticker-table .num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.ticker-table a {
  color: var(--ink-1);
  text-decoration: none;
  font-weight: 600;
}

.ticker-table a:hover {
  text-decoration: underline;
}

.stPlotlyChart,
.stPlotlyChart > div,
.js-plotly-plot,
.plotly {
  overflow: visible !important;
}

.section-card.no-clip {
  overflow: visible;
}
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)

QUOTES = [
    '"Well then buy it...well then sell it, I don\'t know... *vomits*" - Eric Andre',
    '"Don\'t Believe the Hype" - Flavor Flav',
    '"Mo\' money, Mo\' problems" - The Buddha',
    '"What\'s the most you ever lost on a coin toss?" - Anton Chigurh',
]

def _navigate_to_symbol(symbol: str) -> None:
    if not symbol:
        return
    symbol = str(symbol).strip().upper()
    st.session_state["selected_symbol"] = symbol

    # update query params robustly
    try:
        st.query_params.update({"symbol": symbol})
    except Exception:
        st.experimental_set_query_params(symbol=symbol)

    st.switch_page("pages/Deep_Dive.py")


def _render_symbol_table(
    df: pd.DataFrame,
    columns: list[str],
    labels: list[str],
    formats: list[str],
    min_rows: int | None = None,
) -> None:
    if df.empty:
        st.write("No data available.")
        return
    header_cells = []
    for label in labels:
        cls = "mom-tight" if label == "21D Ret (%)" else ""
        class_attr = f' class="{cls}"' if cls else ""
        header_cells.append(f"<th{class_attr}>{html.escape(label)}</th>")
    headers = "".join(header_cells)
    rows = []
    for _, row in df.iterrows():
        symbol = str(row[columns[0]])
        symbol_cell = (
            f'<td><a href="/Deep_Dive?symbol={html.escape(symbol)}" target="_self">'
            f"{html.escape(symbol)}</a></td>"
        )
        other_cells = []
        for col_idx, col_name in enumerate(columns[1:], start=1):
            value = row[col_name]
            fmt = formats[col_idx]
            if value is None or pd.isna(value):
                display = "n/a"
            else:
                display = fmt.format(value)
            other_cells.append(f'<td class="num">{html.escape(display)}</td>')
        rows.append(f"<tr>{symbol_cell}{''.join(other_cells)}</tr>")

    if min_rows is not None and len(rows) < min_rows:
        empty_cells = "<td>&nbsp;</td>" + "".join(
            '<td class="num">&nbsp;</td>' for _ in columns[1:]
        )
        for _ in range(min_rows - len(rows)):
            rows.append(f"<tr>{empty_cells}</tr>")
    table_html = f"""
    <table class="ticker-table">
      <thead><tr>{headers}</tr></thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def _resolve_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "portfolio.duckdb"


def _load_daily_returns() -> tuple[pd.DataFrame, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_date_row = con.execute(
            "SELECT MAX(trade_date) FROM gold.prices"
        ).fetchone()
        if not latest_date_row or latest_date_row[0] is None:
            return pd.DataFrame(), "No gold.prices data found."

        trade_date = latest_date_row[0]
        df = con.execute(
            """
            SELECT symbol, trade_date, returns_1d
            FROM gold.prices
            WHERE trade_date = ?
              AND returns_1d IS NOT NULL
            """,
            [trade_date],
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), f"Failed to load data: {exc}"
    finally:
        con.close()

    if df.empty:
        return df, "No return data available for the latest trade date."

    label = pd.to_datetime(trade_date).strftime("%B %d, %Y")
    df = df.copy()
    df["returns_pct"] = pd.to_numeric(df["returns_1d"], errors="coerce") * 100.0
    return df, label


def _load_risky_bets(
    hot_limit: int = 5,
    crash_limit: int = 5,
    sleepy_limit: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            None,
            f"DuckDB not found at {db_path}",
        )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_date_row = con.execute(
            "SELECT MAX(trade_date) FROM gold.prices"
        ).fetchone()
        if not latest_date_row or latest_date_row[0] is None:
            return (
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
                None,
                "No gold.prices data found.",
            )

        trade_date = latest_date_row[0]
        df = con.execute(
            """
            SELECT symbol, trade_date, realized_vol_21d, returns_21d
            FROM gold.prices
            WHERE trade_date = ?
              AND realized_vol_21d IS NOT NULL
            """,
            [trade_date],
        ).fetch_df()
    except Exception as exc:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            None,
            f"Failed to load data: {exc}",
        )
    finally:
        con.close()

    if df.empty:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            None,
            "No volatility data available for the latest trade date.",
        )

    df = df.copy()
    df["vol_pct"] = df["realized_vol_21d"] * 100.0
    df["mom_pct"] = df["returns_21d"] * 100.0

    hot = (
        df[df["returns_21d"] > 0]
        .sort_values("returns_21d", ascending=False)
        .head(hot_limit)
    )
    crash = (
        df[df["returns_21d"] < 0]
        .sort_values("returns_21d", ascending=True)
        .head(crash_limit)
    )
    sleepy = df.sort_values("realized_vol_21d", ascending=True).head(sleepy_limit)

    label = pd.to_datetime(trade_date).strftime("%B %d, %Y")
    return hot, crash, sleepy, label, None


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    return (series - series.mean()) / std


def _load_underrated_investments(
    limit: int = 5,
) -> tuple[pd.DataFrame, str | None, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), None, f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_date_row = con.execute(
            "SELECT MAX(trade_date) FROM gold.prices"
        ).fetchone()
        if not latest_date_row or latest_date_row[0] is None:
            return pd.DataFrame(), None, "No gold.prices data found."

        trade_date = latest_date_row[0]
        df = con.execute(
            """
            SELECT
                symbol,
                trade_date,
                momentum_12_1,
                pct_below_52w_high
            FROM gold.prices
            WHERE trade_date = ?
              AND momentum_12_1 IS NOT NULL
              AND pct_below_52w_high IS NOT NULL
            """,
            [trade_date],
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), None, f"Failed to load data: {exc}"
    finally:
        con.close()

    if df.empty:
        return df, None, "No underrated investment data available for the latest trade date."

    df = df.copy()
    df["score"] = _zscore(df["momentum_12_1"]) + _zscore(df["pct_below_52w_high"])
    df["momentum_12_1_pct"] = df["momentum_12_1"] * 100.0
    df["pct_below_52w_high_pct"] = df["pct_below_52w_high"] * 100.0
    df = df.sort_values("score", ascending=False).head(limit)

    label = pd.to_datetime(trade_date).strftime("%B %d, %Y")
    return df, label, None


def _load_sentiment_movers(
    limit: int = 5,
    ascending: bool = False,
) -> tuple[pd.DataFrame, str | None, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), None, f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_date_row = con.execute(
            "SELECT MAX(trade_date) FROM gold.prices"
        ).fetchone()
        if not latest_date_row or latest_date_row[0] is None:
            return pd.DataFrame(), None, "No gold.prices data found."

        trade_date = latest_date_row[0]
        df = con.execute(
            """
            SELECT
                symbol,
                trade_date,
                sentiment_score,
                returns_5d
            FROM gold.prices
            WHERE trade_date = ?
              AND sentiment_score IS NOT NULL
            """,
            [trade_date],
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), None, f"Failed to load data: {exc}"
    finally:
        con.close()

    if df.empty:
        return df, None, "No sentiment data available for the latest trade date."

    df = df.copy()
    df = df.sort_values("sentiment_score", ascending=ascending).head(limit)
    returns_abs_max = df["returns_5d"].abs().max()
    if pd.notna(returns_abs_max) and returns_abs_max <= 1.5:
        df["returns_5d_pct"] = df["returns_5d"] * 100.0
    else:
        df["returns_5d_pct"] = df["returns_5d"]

    label = pd.to_datetime(trade_date).strftime("%B %d, %Y")
    return df, label, None


def _build_sentiment_returns_chart(df: pd.DataFrame) -> go.Figure:
    symbols = df["symbol"].tolist()
    sentiment = df["sentiment_score"].tolist()
    returns = df["returns_5d_pct"].tolist()

    sentiment_color = "#3b82f6"
    returns_colors = ["#ef4444" if val < 0 else "#22c55e" for val in returns]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=symbols,
            y=sentiment,
            name="Sentiment",
            marker_color=sentiment_color,
            customdata=symbols,
            hovertemplate="Symbol: %{x}<br>Sentiment: %{y:.2f}<extra></extra>",
            offsetgroup="sentiment",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Bar(
            x=symbols,
            y=returns,
            name="5D Return (%)",
            marker_color=returns_colors,
            customdata=symbols,
            hovertemplate="Symbol: %{x}<br>5D return: %{y:.2f}%<extra></extra>",
            offsetgroup="returns",
            yaxis="y2",
        )
    )
    fig.update_layout(
        barmode="group",
        height=280,
        margin=dict(l=12, r=12, t=10, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f4f7ff"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(title="Sentiment", gridcolor="rgba(255,255,255,0.08)"),
        yaxis2=dict(
            title="5D Return (%)",
            overlaying="y",
            side="right",
            gridcolor="rgba(255,255,255,0.05)",
            showgrid=False,
        ),
        xaxis=dict(title="", tickfont=dict(color="#f4f7ff")),
    )
    return fig


def _load_big_picture(top_n: int = 7) -> tuple[dict, str | None, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return {}, None, f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_date_row = con.execute(
            "SELECT MAX(trade_date) FROM gold.prices"
        ).fetchone()
        if not latest_date_row or latest_date_row[0] is None:
            return {}, None, "No gold.prices data found."

        trade_date = latest_date_row[0]
        df = con.execute(
            """
            SELECT
                symbol,
                close,
                returns_1d,
                sma_50,
                sma_200,
                dollar_volume
            FROM gold.prices
            WHERE trade_date = ?
            """,
            [trade_date],
        ).fetch_df()
    except Exception as exc:
        return {}, None, f"Failed to load data: {exc}"
    finally:
        con.close()

    if df.empty:
        return {}, None, "No data available for the latest trade date."

    df = df.copy()
    valid_returns = df[df["returns_1d"].notna()]
    total_count = len(valid_returns)
    if total_count == 0:
        return {}, None, "No return data available for the latest trade date."

    pct_up = (valid_returns["returns_1d"] > 0).mean() * 100.0

    sma_50_mask = valid_returns["sma_50"].notna() & valid_returns["close"].notna()
    sma_200_mask = valid_returns["sma_200"].notna() & valid_returns["close"].notna()

    pct_above_50 = (
        (valid_returns.loc[sma_50_mask, "close"]
        > valid_returns.loc[sma_50_mask, "sma_50"]).mean()
        * 100.0
        if sma_50_mask.any()
        else None
    )
    pct_above_200 = (
        (valid_returns.loc[sma_200_mask, "close"]
        > valid_returns.loc[sma_200_mask, "sma_200"]).mean()
        * 100.0
        if sma_200_mask.any()
        else None
    )

    dv_mask = valid_returns["dollar_volume"].notna()
    contrib_pct = None
    if dv_mask.any():
        dv = valid_returns.loc[dv_mask, ["returns_1d", "dollar_volume"]].copy()
        dv = dv[dv["dollar_volume"] > 0]
        if not dv.empty:
            dv["weighted_return"] = dv["returns_1d"] * dv["dollar_volume"]
            total_weighted = dv["weighted_return"].sum()
            top = dv.sort_values("dollar_volume", ascending=False).head(top_n)
            top_weighted = top["weighted_return"].sum()
            if total_weighted != 0:
                contrib_pct = (top_weighted / total_weighted) * 100.0

    label = pd.to_datetime(trade_date).strftime("%B %d, %Y")
    metrics = {
        "pct_up": pct_up,
        "pct_above_50": pct_above_50,
        "pct_above_200": pct_above_200,
        "top_contrib": contrib_pct,
        "top_n": top_n,
        "universe": total_count,
    }
    return metrics, label, None


st.markdown(
    """
    <div class="header-hero">
      <div style="font-size: 2rem; font-weight: 700;">Market Vibecheck</div>
      <div style="color: var(--ink-2); margin-top: 6px;">
        Pulse on the market with daily winners, laggards, and emerging themes.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-card" style="margin-bottom: 16px;">
      <div style="font-size: 1rem; color: var(--ink-2); font-style: italic; text-align: center;">
        {quote}
      </div>
    </div>
    """.format(quote=random.choice(QUOTES)),
    unsafe_allow_html=True,
)

st.markdown('<div class="section-card" style="margin-bottom: 16px;">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Big Picture</div>', unsafe_allow_html=True)

big_picture, label, error = _load_big_picture(top_n=7)
if error:
    st.info(error)
else:
    st.markdown(
        f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
        unsafe_allow_html=True,
    )

    bp_cols = st.columns(4, gap="large")
    with bp_cols[0]:
        st.metric("% of stocks up", f"{big_picture['pct_up']:.1f}%")
    with bp_cols[1]:
        if big_picture["pct_above_50"] is None:
            st.metric("% above 50D SMA", "n/a")
        else:
            st.metric("% above 50D SMA", f"{big_picture['pct_above_50']:.1f}%")
    with bp_cols[2]:
        if big_picture["pct_above_200"] is None:
            st.metric("% above 200D SMA", "n/a")
        else:
            st.metric("% above 200D SMA", f"{big_picture['pct_above_200']:.1f}%")
    with bp_cols[3]:
        if big_picture["top_contrib"] is None:
            st.metric(
                f"Top {big_picture['top_n']} Contribution", "n/a"
            )
        else:
            st.metric(
                f"Top {big_picture['top_n']} Contribution",
                f"{big_picture['top_contrib']:.1f}%",
            )

st.markdown("</div>", unsafe_allow_html=True)

left, right = st.columns([2.6, 3.4], gap="large")

with left:
    st.markdown('<div class="section-card no-clip">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Top Gainers + Losers</div>', unsafe_allow_html=True)

    df, label = _load_daily_returns()
    if df.empty:
        st.info(label or "No data available yet.")
    else:
        st.markdown(
            f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
            unsafe_allow_html=True,
        )

        gainers = df[df["returns_pct"] > 0].sort_values("returns_pct", ascending=False).head(5)
        losers = df[df["returns_pct"] < 0].sort_values("returns_pct", ascending=True).head(5)
        ordered = pd.concat([gainers, losers], axis=0)
        if ordered.empty:
            st.info("No return data available for the latest trade date.")
            st.markdown("</div>", unsafe_allow_html=True)
            st.stop()
        row_height = 30
        chart_height = max(320, row_height * len(ordered))
        ordered = ordered.copy()
        ordered["returns_pct"] = pd.to_numeric(
            ordered["returns_pct"], errors="coerce"
        ).fillna(0.0)
        x_vals = ordered["returns_pct"].astype(float).tolist()
        y_vals = ordered["symbol"].astype(str).tolist()
        max_abs = max(abs(val) for val in x_vals) if x_vals else 1.0
        if max_abs == 0 or pd.isna(max_abs):
            max_abs = 1.0
        colors = ["#22c55e" if val >= 0 else "#ef4444" for val in x_vals]
        clicked_symbol = hover_bar_chart(
            x=x_vals,
            y=y_vals,
            colors=colors,
            height=chart_height,
            x_range=[-max_abs * 1.15, max_abs * 1.15],
            key="top-gainers-losers",
            open_in_new_tab=False,
        )
        if isinstance(clicked_symbol, dict):
            clicked_symbol = clicked_symbol.get("symbol")
        if clicked_symbol:
            _navigate_to_symbol(clicked_symbol)

    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="section-card" style="margin-top: 16px;">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Risky Bets</div>', unsafe_allow_html=True)

    hot, crash, sleepy, label, error = _load_risky_bets()
    if error:
        st.info(error)
    else:
        st.markdown(
            f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
            unsafe_allow_html=True,
        )

        hot_col, crash_col, sleepy_col = st.columns([2.0, 2.0, 1.6], gap="medium")
        with hot_col:
            st.markdown(
                '<span class="tooltip" data-tip="High-volatility names with positive 21-day returns."><strong>Hot Ones 🔥</strong></span>',
                unsafe_allow_html=True,
            )
            if hot.empty:
                st.write("No positive momentum names.")
            else:
                _render_symbol_table(
                    hot,
                    columns=["symbol", "vol_pct", "mom_pct"],
                    labels=["Symbol", "Vol", "21D Ret (%)"],
                    formats=["{}", "{:.2f}", "{:.2f}"],
                    min_rows=5,
                )

        with crash_col:
            st.markdown(
                '<span class="tooltip" data-tip="High-volatility names with negative 21-day returns."><strong>Crashing Out 😢</strong></span>',
                unsafe_allow_html=True,
            )
            if crash.empty:
                st.write("No negative momentum names.")
            else:
                _render_symbol_table(
                    crash,
                    columns=["symbol", "vol_pct", "mom_pct"],
                    labels=["Symbol", "Vol", "21D Ret (%)"],
                    formats=["{}", "{:.2f}", "{:.2f}"],
                    min_rows=5,
                )

        with sleepy_col:
            st.markdown(
                '<span class="tooltip" data-tip="Lowest-volatility names on the latest trade date."><strong>Sleepy 😴</strong></span>',
                unsafe_allow_html=True,
            )
            if sleepy.empty:
                st.write("No low-volatility names.")
            else:
                _render_symbol_table(
                    sleepy,
                    columns=["symbol", "vol_pct"],
                    labels=["Symbol", "Vol"],
                    formats=["{}", "{:.2f}"],
                    min_rows=5,
                )

    st.markdown("</div>", unsafe_allow_html=True)

underrated_col, good_news_col, bad_news_col = st.columns(
    [1.2, 0.9, 0.9], gap="medium"
)

with underrated_col:
    st.markdown('<div class="section-card" style="margin-top: 18px;">', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-title"><span class="tooltip" '
        'data-tip="Stocks with strong 12-1 momentum that are still well below their 52-week highs.">'
        '<strong>Underrated Investments</strong></span></div>',
        unsafe_allow_html=True,
    )

    underrated_df, label, error = _load_underrated_investments(limit=5)
    if error:
        st.info(error)
    else:
        st.markdown(
            f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
            unsafe_allow_html=True,
        )
        _render_symbol_table(
            underrated_df,
            columns=["symbol", "momentum_12_1_pct", "pct_below_52w_high_pct", "score"],
            labels=["Symbol", "12-1 Mom (%)", "Below 52W High (%)", "Score"],
            formats=["{}", "{:.2f}", "{:.2f}", "{:.2f}"],
        )

    st.markdown("</div>", unsafe_allow_html=True)

with good_news_col:
    st.markdown('<div class="section-card" style="margin-top: 18px;">', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-title"><span class="tooltip" '
        'data-tip="Highest headline sentiment scores with 5-day returns for context.">'
        '<strong>The Good News 🙏</strong></span></div>',
        unsafe_allow_html=True,
    )

    good_news_df, label, error = _load_sentiment_movers(limit=5, ascending=False)
    if error:
        st.info(error)
    else:
        st.markdown(
            f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
            unsafe_allow_html=True,
        )
        symbols = good_news_df["symbol"].tolist()
        sentiment = good_news_df["sentiment_score"].tolist()
        returns = good_news_df["returns_5d_pct"].tolist()
        returns_colors = ["#ef4444" if val < 0 else "#22c55e" for val in returns]
        sent_abs = abs(pd.Series(sentiment).dropna().abs().max() or 0)
        ret_abs = abs(pd.Series(returns).dropna().abs().max() or 0)
        y_range = [-sent_abs * 1.1, sent_abs * 1.1] if sent_abs else [-1, 1]
        y2_range = [-ret_abs * 1.1, ret_abs * 1.1] if ret_abs else [-1, 1]
        clicked_symbol = hover_grouped_bar_chart(
            categories=symbols,
            series=[
                {
                    "name": "Sentiment",
                    "values": sentiment,
                    "colors": "#3b82f6",
                    "yaxis": "y",
                    "suffix": "",
                },
                {
                    "name": "5D Return",
                    "values": returns,
                    "colors": returns_colors,
                    "yaxis": "y2",
                    "suffix": "%",
                },
            ],
            height=280,
            key="good-news-chart",
            open_in_new_tab=False,
            yaxis_title="Sentiment / 5D Return (%)",
            yaxis2_title="5D Return (%)",
            y_range=y_range,
            y2_range=y2_range,
            bar_gap=0.22,
            group_gap=0.14,
        )
        if isinstance(clicked_symbol, dict):
            clicked_symbol = clicked_symbol.get("symbol")
        if clicked_symbol:
            _navigate_to_symbol(clicked_symbol)

    st.markdown("</div>", unsafe_allow_html=True)

with bad_news_col:
    st.markdown('<div class="section-card" style="margin-top: 18px;">', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-title"><span class="tooltip" '
        'data-tip="Lowest headline sentiment scores with 5-day returns for context.">'
        '<strong>The Bad News 😈</strong></span></div>',
        unsafe_allow_html=True,
    )

    bad_news_df, label, error = _load_sentiment_movers(limit=5, ascending=True)
    if error:
        st.info(error)
    else:
        st.markdown(
            f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
            unsafe_allow_html=True,
        )
        symbols = bad_news_df["symbol"].tolist()
        sentiment = bad_news_df["sentiment_score"].tolist()
        returns = bad_news_df["returns_5d_pct"].tolist()
        returns_colors = ["#ef4444" if val < 0 else "#22c55e" for val in returns]
        sent_abs = abs(pd.Series(sentiment).dropna().abs().max() or 0)
        ret_abs = abs(pd.Series(returns).dropna().abs().max() or 0)
        y_range = [-sent_abs * 1.1, sent_abs * 1.1] if sent_abs else [-1, 1]
        y2_range = [-ret_abs * 1.1, ret_abs * 1.1] if ret_abs else [-1, 1]
        clicked_symbol = hover_grouped_bar_chart(
            categories=symbols,
            series=[
                {
                    "name": "Sentiment",
                    "values": sentiment,
                    "colors": "#3b82f6",
                    "yaxis": "y",
                    "suffix": "",
                },
                {
                    "name": "5D Return",
                    "values": returns,
                    "colors": returns_colors,
                    "yaxis": "y2",
                    "suffix": "%",
                },
            ],
            height=280,
            key="bad-news-chart",
            open_in_new_tab=False,
            yaxis_title="Sentiment / 5D Return (%)",
            yaxis2_title="5D Return (%)",
            y_range=y_range,
            y2_range=y2_range,
            bar_gap=0.22,
            group_gap=0.14,
        )
        if isinstance(clicked_symbol, dict):
            clicked_symbol = clicked_symbol.get("symbol")
        if clicked_symbol:
            _navigate_to_symbol(clicked_symbol)

    st.markdown("</div>", unsafe_allow_html=True)

