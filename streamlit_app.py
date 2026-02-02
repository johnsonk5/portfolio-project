import os
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
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)


def _resolve_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "portfolio.duckdb"


def _load_daily_returns(limit: int = 10) -> tuple[pd.DataFrame, str | None]:
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

left, right = st.columns([2, 1], gap="large")

with left:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Top Gainers + Losers</div>', unsafe_allow_html=True)

    df, label = _load_daily_returns(limit=10)
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
            .properties(height=chart_height)
            .configure_axis(gridColor="rgba(255,255,255,0.05)")
        )

        st.altair_chart(chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Market Breadth</div>', unsafe_allow_html=True)
    st.write("Coming soon: advance/decline ratio, sector heat, volatility regime.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="section-card" style="margin-top: 16px;">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Risky Bets</div>', unsafe_allow_html=True)
    st.write("Coming soon: momentum clusters and speculative movers.")
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
