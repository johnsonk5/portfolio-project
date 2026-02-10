import os
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Deep Dive",
    page_icon="DD",
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
  --card: rgba(255, 255, 255, 0.05);
  --card-border: rgba(255, 255, 255, 0.08);
  --gain: #22c55e;
  --loss: #ef4444;
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

st.title("Deep Dive")
st.caption("Focus on one asset with price action and headline sentiment context.")


def _resolve_duckdb_path() -> Path:
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / "portfolio.duckdb"


def _load_symbols() -> tuple[pd.DataFrame, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT
                p.symbol,
                a.name
            FROM (
                SELECT DISTINCT symbol
                FROM gold.prices
                WHERE symbol IS NOT NULL
            ) AS p
            LEFT JOIN silver.assets AS a
                ON upper(p.symbol) = upper(a.symbol)
            ORDER BY p.symbol
            """
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), f"Failed to load symbols: {exc}"
    finally:
        con.close()

    return df, None


def _load_price_history(symbol: str) -> tuple[pd.DataFrame, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT
                trade_date,
                open,
                high,
                low,
                close,
                volume
            FROM gold.prices
            WHERE upper(symbol) = upper(?)
              AND trade_date IS NOT NULL
              AND open IS NOT NULL
              AND high IS NOT NULL
              AND low IS NOT NULL
              AND close IS NOT NULL
            ORDER BY trade_date
            """,
            [symbol],
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), f"Failed to load price history: {exc}"
    finally:
        con.close()

    if df.empty:
        return df, "No OHLC price history found for this symbol."

    return df, None


def _load_headlines(symbol: str, limit: int = 50) -> tuple[pd.DataFrame, str | None]:
    db_path = _resolve_duckdb_path()
    if not db_path.exists():
        return pd.DataFrame(), f"DuckDB not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT
                provider_publish_time,
                title,
                sentiment
            FROM gold.headlines
            WHERE upper(symbol) = upper(?)
              AND title IS NOT NULL
            ORDER BY provider_publish_time DESC NULLS LAST
            LIMIT ?
            """,
            [symbol, limit],
        ).fetch_df()
    except Exception as exc:
        return pd.DataFrame(), f"Failed to load headlines: {exc}"
    finally:
        con.close()

    if df.empty:
        return df, "No headlines found for this symbol."

    return df, None


def _filter_by_horizon(df: pd.DataFrame, horizon: str) -> pd.DataFrame:
    lookbacks = {
        "1M": 31,
        "3M": 92,
        "1Y": 366,
        "3Y": 1096,
        "5Y": 1826,
    }
    if df.empty:
        return df
    max_date = pd.to_datetime(df["trade_date"]).max()
    if pd.isna(max_date):
        return df
    cutoff = max_date - pd.Timedelta(days=lookbacks[horizon])
    return df[pd.to_datetime(df["trade_date"]) >= cutoff].copy()


def _normalize_sentiment(value: str) -> str:
    if not isinstance(value, str):
        return "neutral"
    mapping = {
        "positive": "positive",
        "neutral": "neutral",
        "negative": "negative",
    }
    return mapping.get(value.strip().lower(), "neutral")


def _headline_color(sentiment: str) -> str:
    if sentiment == "positive":
        return "#22c55e"
    if sentiment == "negative":
        return "#ef4444"
    return "#cbd5e1"


symbols_df, symbols_error = _load_symbols()
if symbols_error:
    st.info(symbols_error)
    st.stop()

if symbols_df.empty:
    st.info("No symbols are available yet.")
    st.stop()

symbols_df = symbols_df.copy()
symbols_df["name"] = symbols_df["name"].fillna("")
symbols_df["label"] = symbols_df.apply(
    lambda row: f"{row['symbol']} - {row['name']}" if row["name"] else row["symbol"],
    axis=1,
)

control_left, control_right = st.columns([2, 3], gap="large")
with control_left:
    selected_label = st.selectbox(
        "Select symbol",
        options=symbols_df["label"].tolist(),
        index=0,
    )
with control_right:
    horizon = st.radio(
        "Time horizon",
        options=["1M", "3M", "1Y", "3Y", "5Y"],
        index=0,
        horizontal=True,
    )

selected_symbol = symbols_df.loc[
    symbols_df["label"] == selected_label, "symbol"
].iloc[0]

prices_df, prices_error = _load_price_history(selected_symbol)
news_df, news_error = _load_headlines(selected_symbol, limit=50)
x_axis_format = "%b %Y" if horizon in {"3Y", "5Y"} else "%b"

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Historical Prices</div>', unsafe_allow_html=True)
if prices_error:
    st.info(prices_error)
else:
    prices_df = prices_df.copy()
    prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"])
    prices_df = _filter_by_horizon(prices_df, horizon)
    prices_df["body_low"] = prices_df[["open", "close"]].min(axis=1)
    prices_df["body_high"] = prices_df[["open", "close"]].max(axis=1)
    prices_df["direction"] = prices_df.apply(
        lambda row: "up" if row["close"] >= row["open"] else "down",
        axis=1,
    )
    price_min = float(prices_df["low"].min())
    price_max = float(prices_df["high"].max())
    price_pad = max((price_max - price_min) * 0.06, 0.01)
    y_domain = [price_min - price_pad, price_max + price_pad]

    latest_close = float(prices_df["close"].iloc[-1])
    latest_open = float(prices_df["open"].iloc[-1])
    latest_date = prices_df["trade_date"].iloc[-1].strftime("%B %d, %Y")
    delta_pct = ((latest_close / latest_open) - 1.0) * 100.0
    st.metric("Latest close", f"${latest_close:,.2f}", f"{delta_pct:.2f}%")
    st.caption(f"Viewing {horizon} through {latest_date}")

    wick = (
        alt.Chart(prices_df)
        .mark_rule(color="#93c5fd")
        .encode(
            x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
            y=alt.Y("low:Q", title="Price", scale=alt.Scale(domain=y_domain, zero=False)),
            y2="high:Q",
            tooltip=[
                alt.Tooltip("trade_date:T", title="Date"),
                alt.Tooltip("open:Q", title="Open", format=",.2f"),
                alt.Tooltip("high:Q", title="High", format=",.2f"),
                alt.Tooltip("low:Q", title="Low", format=",.2f"),
                alt.Tooltip("close:Q", title="Close", format=",.2f"),
                alt.Tooltip("volume:Q", title="Volume", format=","),
            ],
        )
    )
    body = (
        alt.Chart(prices_df)
        .mark_bar(size=7)
        .encode(
            x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
            y=alt.Y("body_low:Q", title="Price", scale=alt.Scale(domain=y_domain, zero=False)),
            y2="body_high:Q",
            color=alt.Color(
                "direction:N",
                scale=alt.Scale(domain=["up", "down"], range=["#22c55e", "#ef4444"]),
                legend=None,
            ),
        )
    )
    chart = (
        alt.layer(wick, body)
        .properties(height=540)
        .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
    )
    st.altair_chart(chart, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Recent Headlines</div>', unsafe_allow_html=True)
if news_error:
    st.info(news_error)
else:
    news_df = news_df.copy()
    news_df["Published"] = pd.to_datetime(news_df["provider_publish_time"], errors="coerce")
    news_df["Headline"] = news_df["title"].fillna("")
    news_df["Sentiment"] = news_df["sentiment"].apply(_normalize_sentiment)
    display_df = news_df[["Published", "Headline", "Sentiment"]].copy()

    def _style_row(row: pd.Series) -> list[str]:
        color = _headline_color(str(row["Sentiment"]))
        return [
            "",
            f"color: {color}; font-weight: 600;",
            f"color: {color}; font-weight: 700;",
        ]

    styled = display_df.style.apply(_style_row, axis=1).format(
        {
            "Published": lambda x: x.strftime("%Y-%m-%d %H:%M") if pd.notna(x) else "",
            "Sentiment": lambda x: str(x).capitalize(),
        }
    )
    st.dataframe(
        styled,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Published": st.column_config.TextColumn("Published"),
            "Headline": st.column_config.TextColumn("Headline", width="large"),
            "Sentiment": st.column_config.TextColumn("Sentiment"),
        },
    )
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Coming Next</div>', unsafe_allow_html=True)
placeholder_cols = st.columns(3, gap="large")
with placeholder_cols[0]:
    st.write("Headline volume trend chart is coming soon.")
with placeholder_cols[1]:
    st.write("Volatility context chart is coming soon.")
with placeholder_cols[2]:
    st.write("Wikipedia attention chart is coming soon.")
st.markdown("</div>", unsafe_allow_html=True)
