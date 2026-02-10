import os
import random
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st


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
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)

QUOTES = [
    '"Well then buy it...well then sell it, I don\'t know... *vomits*" - Eric Andre',
    '"Don\'t Believe the Hype" - Flavor Flav',
    '"Mo\' money, Mo\' problems" - The Buddha',
    '"What\'s the most you ever lost on a coin toss?" - Anton Chigurh',
]


def _resolve_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "portfolio.duckdb"


def _load_daily_returns(limit: int = 5) -> tuple[pd.DataFrame, str | None]:
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

    df = df.copy()
    df["returns_pct"] = df["returns_1d"] * 100.0
    df = df.sort_values("returns_pct", ascending=False)

    gainers = df.head(limit)
    losers = df.tail(limit).sort_values("returns_pct", ascending=True)

    combined = pd.concat([gainers, losers], axis=0)
    combined = combined.drop_duplicates(subset=["symbol"], keep="first")
    combined = combined.sort_values("returns_pct", ascending=True)

    label = pd.to_datetime(trade_date).strftime("%B %d, %Y")
    return combined, label


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


def _build_sentiment_returns_chart(df: pd.DataFrame) -> alt.Chart:
    sentiment_min = float(df["sentiment_score"].min())
    sentiment_max = float(df["sentiment_score"].max())
    sentiment_pad = max((sentiment_max - sentiment_min) * 0.08, 0.01)
    returns_min = float(df["returns_5d_pct"].min())
    returns_max = float(df["returns_5d_pct"].max())
    returns_pad = max((returns_max - returns_min) * 0.08, 0.05)

    sentiment_pos = max(0.0, sentiment_max) + sentiment_pad
    returns_pos = max(0.0, returns_max) + returns_pad
    ratio = 0.1
    if returns_pos > 0:
        ratio = max(ratio, abs(min(0.0, returns_min)) / returns_pos)
    if ratio > 0:
        sentiment_pos = max(sentiment_pos, abs(min(0.0, sentiment_min)) / ratio)

    sentiment_domain = [-sentiment_pos * ratio, sentiment_pos]
    returns_domain = [-returns_pos * ratio, returns_pos]

    sentiment_chart = (
        alt.Chart(df)
        .mark_bar(size=14, cornerRadiusEnd=4)
        .encode(
            x=alt.X("symbol:N", title="", sort=None),
            y=alt.Y(
                "sentiment_score:Q",
                title="",
                scale=alt.Scale(domain=sentiment_domain, zero=True),
            ),
            xOffset=alt.value(-8),
            color=alt.value("#3b82f6"),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("sentiment_score:Q", title="Sentiment", format=".2f"),
            ],
        )
    )
    returns_chart = (
        alt.Chart(df)
        .mark_bar(size=14, cornerRadiusEnd=4)
        .encode(
            x=alt.X("symbol:N", title="", sort=None),
            y=alt.Y(
                "returns_5d_pct:Q",
                title="",
                scale=alt.Scale(domain=returns_domain, zero=True),
            ),
            xOffset=alt.value(8),
            color=alt.condition(
                alt.datum.returns_5d_pct < 0,
                alt.value("#ef4444"),
                alt.value("#22c55e"),
            ),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("returns_5d_pct:Q", title="5D return (%)", format=".2f"),
            ],
        )
    )
    return (
        alt.layer(sentiment_chart, returns_chart)
        .resolve_scale(y="independent")
        .properties(height=280)
        .configure_axis(gridColor="rgba(255,255,255,0.05)")
    )


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

left, right = st.columns([2, 4], gap="large")

with left:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Top Gainers + Losers</div>', unsafe_allow_html=True)

    df, label = _load_daily_returns(limit=5)
    if df.empty:
        st.info(label or "No data available yet.")
    else:
        st.markdown(
            f"<div class=\"metric-pill\">Latest trade date: {label}</div>",
            unsafe_allow_html=True,
        )

        row_height = 26
        chart_height = max(320, row_height * len(df))

        chart = (
            alt.Chart(df)
            .mark_bar(size=16, cornerRadiusEnd=4)
            .encode(
                x=alt.X("returns_pct:Q", title="Daily return (%)"),
                y=alt.Y(
                    "symbol:N",
                    sort=alt.SortField("returns_pct", order="descending"),
                    title="",
                    axis=alt.Axis(labelLimit=0),
                ),
                color=alt.condition(
                    alt.datum.returns_pct > 0,
                    alt.value("#22c55e"),
                    alt.value("#ef4444"),
                ),
                tooltip=[
                    alt.Tooltip("symbol:N", title="Symbol"),
                    alt.Tooltip("returns_pct:Q", title="Return (%)", format=".2f"),
                ],
            )
            .properties(height=chart_height, width=380)
            .configure_axis(gridColor="rgba(255,255,255,0.05)")
        )

        st.altair_chart(chart, use_container_width=False)

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

        hot_col, crash_col, sleepy_col = st.columns([2, 2, 1.35], gap="small")
        with hot_col:
            st.markdown(
                '<span class="tooltip" data-tip="High-volatility names with positive 20-day momentum."><strong>Hot Ones 🔥</strong></span>',
                unsafe_allow_html=True,
            )
            if hot.empty:
                st.write("No positive momentum names.")
            else:
                st.dataframe(
                    hot[["symbol", "vol_pct", "mom_pct"]],
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "vol_pct": st.column_config.NumberColumn("Vol", format="%.2f"),
                        "mom_pct": st.column_config.NumberColumn("20D Mom (%)", format="%.2f"),
                    },
                )

        with crash_col:
            st.markdown(
                '<span class="tooltip" data-tip="High-volatility names with negative 20-day momentum."><strong>Crashing Out 😢</strong></span>',
                unsafe_allow_html=True,
            )
            if crash.empty:
                st.write("No negative momentum names.")
            else:
                st.dataframe(
                    crash[["symbol", "vol_pct", "mom_pct"]],
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "vol_pct": st.column_config.NumberColumn("Vol", format="%.2f"),
                        "mom_pct": st.column_config.NumberColumn("20D Mom (%)", format="%.2f"),
                    },
                )

        with sleepy_col:
            st.markdown(
                '<span class="tooltip" data-tip="Lowest-volatility names on the latest trade date."><strong>Sleepy 😴</strong></span>',
                unsafe_allow_html=True,
            )
            if sleepy.empty:
                st.write("No low-volatility names.")
            else:
                st.dataframe(
                    sleepy[["symbol", "vol_pct"]],
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "vol_pct": st.column_config.NumberColumn("Vol", format="%.2f"),
                    },
                )

    st.markdown("</div>", unsafe_allow_html=True)

underrated_col, good_news_col, bad_news_col = st.columns(
    [1.15, 0.925, 0.925], gap="large"
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
        st.dataframe(
            underrated_df[
                ["symbol", "momentum_12_1_pct", "pct_below_52w_high_pct", "score"]
            ],
            hide_index=True,
            use_container_width=True,
            column_config={
                "symbol": st.column_config.TextColumn("Symbol"),
                "momentum_12_1_pct": st.column_config.NumberColumn("12-1 Mom (%)", format="%.2f"),
                "pct_below_52w_high_pct": st.column_config.NumberColumn(
                    "Below 52W High (%)", format="%.2f"
                ),
                "score": st.column_config.NumberColumn("Score", format="%.2f"),
            },
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
        chart = _build_sentiment_returns_chart(good_news_df)
        st.altair_chart(chart, use_container_width=True)

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
        chart = _build_sentiment_returns_chart(bad_news_df)
        st.altair_chart(chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card" style="margin-top: 18px;">', unsafe_allow_html=True)
st.markdown('<div class="section-title">On Deck</div>', unsafe_allow_html=True)

on_deck = st.columns(3, gap="large")
with on_deck[0]:
    st.write("Sector pulse and leadership tables.")
with on_deck[1]:
    st.write("Factor spotlight and breadth metrics.")
with on_deck[2]:
    st.write("Portfolio impact and watchlist flags.")

st.markdown("</div>", unsafe_allow_html=True)
