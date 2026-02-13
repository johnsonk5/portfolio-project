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
                volume,
                returns_1d,
            returns_5d,
            returns_21d,
            realized_vol_21d,
            sma_50,
            dist_sma_50
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

selected_label = st.selectbox(
    "Select symbol",
    options=symbols_df["label"].tolist(),
    index=0,
)

selected_symbol = symbols_df.loc[
    symbols_df["label"] == selected_label, "symbol"
].iloc[0]

prices_df, prices_error = _load_price_history(selected_symbol)
news_df, news_error = _load_headlines(selected_symbol, limit=50)
base_prices_df = prices_df.copy() if not prices_error else pd.DataFrame()

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Snapshot</div>', unsafe_allow_html=True)
if prices_error:
    st.info(prices_error)
else:
    prices_df = prices_df.copy()
    prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"])
    prices_df["body_low"] = prices_df[["open", "close"]].min(axis=1)
    prices_df["body_high"] = prices_df[["open", "close"]].max(axis=1)
    prices_df["direction"] = prices_df.apply(
        lambda row: "up" if row["close"] >= row["open"] else "down",
        axis=1,
    )
    latest_row = prices_df.iloc[-1]
    latest_close = float(latest_row["close"])
    latest_open = float(latest_row["open"])
    latest_date = latest_row["trade_date"].strftime("%B %d, %Y")
    delta_pct = None
    if len(prices_df) > 1 and pd.notna(prices_df.iloc[-2]["close"]):
        prior_close = float(prices_df.iloc[-2]["close"])
        if prior_close != 0:
            delta_pct = ((latest_close / prior_close) - 1.0) * 100.0
    if delta_pct is None and latest_open != 0:
        delta_pct = ((latest_close / latest_open) - 1.0) * 100.0
    latest_5d = latest_row.get("returns_5d")
    latest_21d = latest_row.get("returns_21d")
    latest_vol = None
    if "realized_vol_21d" in prices_df.columns and prices_df["realized_vol_21d"].notna().any():
        latest_vol = prices_df["realized_vol_21d"].dropna().iloc[-1]
    elif "returns_1d" in prices_df.columns:
        vol_series = prices_df["returns_1d"].rolling(21).std() * (252 ** 0.5)
        if vol_series.notna().any():
            latest_vol = vol_series.dropna().iloc[-1]

    kpi_cols = st.columns(4, gap="large")
    with kpi_cols[0]:
        delta_label = "n/a" if delta_pct is None else f"{delta_pct:.2f}%"
        st.metric("Latest close", f"${latest_close:,.2f}", delta_label)
    with kpi_cols[1]:
        if pd.isna(latest_5d):
            st.metric("5D return", "n/a")
        else:
            st.metric("5D return", f"{latest_5d * 100.0:.2f}%")
    with kpi_cols[2]:
        if pd.isna(latest_21d):
            st.metric("21D return", "n/a")
        else:
            st.metric("21D return", f"{latest_21d * 100.0:.2f}%")
    with kpi_cols[3]:
        if latest_vol is None:
            st.metric("21D volatility", "n/a")
        else:
            st.metric("21D volatility", f"{latest_vol * 100.0:.2f}%")

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Performance History</div>', unsafe_allow_html=True)
if prices_error:
    st.info(prices_error)
else:
    horizon = st.radio(
        "Time horizon",
        options=["1M", "3M", "1Y", "3Y", "5Y"],
        index=0,
        horizontal=True,
    )
    x_axis_format = "%b %Y" if horizon in {"3Y", "5Y"} else "%b"
    history_df = _filter_by_horizon(prices_df, horizon)
    price_min = float(history_df["low"].min())
    price_max = float(history_df["high"].max())
    price_pad = max((price_max - price_min) * 0.06, 0.01)
    y_domain = [price_min - price_pad, price_max + price_pad]
    latest_history_date = history_df["trade_date"].max()
    if pd.notna(latest_history_date):
        st.caption(f"Viewing {horizon} through {latest_history_date.strftime('%B %d, %Y')}")

    main_col, side_col = st.columns([2.2, 1], gap="large")
    chart_height = 540
    side_gap = 44
    side_chart_height = (chart_height - side_gap) / 2
    with main_col:
        st.markdown(
            '<div class="section-title" style="margin: 0 0 -8px 0; line-height: 1;">'
            "Prices</div>",
            unsafe_allow_html=True,
        )
        wick = (
            alt.Chart(history_df)
            .mark_rule(color="#93c5fd")
            .encode(
                x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                y=alt.Y(
                    "low:Q",
                    title="Price",
                    scale=alt.Scale(domain=y_domain, zero=False),
                ),
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
            alt.Chart(history_df)
            .mark_bar(size=7)
            .encode(
                x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                y=alt.Y(
                    "body_low:Q",
                    title="Price",
                    scale=alt.Scale(domain=y_domain, zero=False),
                ),
                y2="body_high:Q",
                color=alt.Color(
                    "direction:N",
                    scale=alt.Scale(domain=["up", "down"], range=["#22c55e", "#ef4444"]),
                    legend=None,
                ),
            )
        )
        overlay = None
        if history_df["sma_50"].notna().any():
            overlay = (
                alt.Chart(history_df.dropna(subset=["sma_50"]))
                .mark_line(color="#fbbf24", strokeWidth=2)
                .encode(
                    x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                    y=alt.Y(
                        "sma_50:Q",
                        title="Price",
                        scale=alt.Scale(domain=y_domain, zero=False),
                    ),
                    tooltip=[
                        alt.Tooltip("trade_date:T", title="Date"),
                        alt.Tooltip("sma_50:Q", title="SMA 50", format=",.2f"),
                    ],
                )
            )

        layers = [wick, body]
        if overlay is not None:
            layers.append(overlay)
        chart = (
            alt.layer(*layers)
            .properties(height=chart_height)
            .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
        )
        st.altair_chart(chart, use_container_width=True)

    with side_col:
        st.markdown(
            '<div class="section-title" style="margin: 0 0 -8px 0; line-height: 1;">'
            "Returns</div>",
            unsafe_allow_html=True,
        )
        returns_df = history_df[["trade_date", "returns_1d"]].copy()
        returns_df = returns_df.rename(columns={"returns_1d": "Daily"})
        valid_mask = returns_df["Daily"].notna()
        if valid_mask.any():
            cumulative = (1 + returns_df.loc[valid_mask, "Daily"]).cumprod() - 1
            returns_df.loc[valid_mask, "Daily"] = cumulative

        returns_df = returns_df.dropna(subset=["Daily"])
        returns_long = returns_df.rename(
            columns={"Daily": "ReturnPct"}
        )
        returns_long["Horizon"] = "1D"
        returns_long["ReturnPct"] = returns_long["ReturnPct"] * 100.0

        returns_chart = (
            alt.Chart(returns_long)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                y=alt.Y("ReturnPct:Q", title="Cumulative return (%)"),
                color=alt.Color(
                    "Horizon:N",
                    scale=alt.Scale(domain=["1D"], range=["#22c55e"]),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("trade_date:T", title="Date"),
                    alt.Tooltip("Horizon:N", title="Horizon"),
                    alt.Tooltip("ReturnPct:Q", title="Cumulative return (%)", format=".2f"),
                ],
            )
            .properties(height=side_chart_height)
            .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
        )
        st.altair_chart(returns_chart, use_container_width=True)

        st.markdown(
            '<div class="section-title" style="margin: 12px 0 -8px 0; line-height: 1;">'
            "Distance From SMA50</div>",
            unsafe_allow_html=True,
        )
        dist_df = history_df[["trade_date", "dist_sma_50"]].copy()
        dist_df = dist_df.dropna(subset=["dist_sma_50"])
        if dist_df.empty:
            st.caption("SMA50 distance unavailable for the selected horizon.")
        else:
            dist_df["dist_pct"] = dist_df["dist_sma_50"] * 100.0
            dist_min = float(dist_df["dist_pct"].min())
            dist_max = float(dist_df["dist_pct"].max())
            dist_pad = max((dist_max - dist_min) * 0.08, 0.5)
            dist_domain = [dist_min - dist_pad, dist_max + dist_pad]
            dist_chart = (
                alt.Chart(dist_df)
                .mark_line(strokeWidth=2, color="#fbbf24")
                .encode(
                    x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                    y=alt.Y(
                        "dist_pct:Q",
                        title="Distance from SMA50 (%)",
                        scale=alt.Scale(domain=dist_domain, zero=False),
                    ),
                    tooltip=[
                        alt.Tooltip("trade_date:T", title="Date"),
                        alt.Tooltip(
                            "dist_pct:Q",
                            title="Distance from SMA50 (%)",
                            format=".2f",
                        ),
                    ],
                )
                .properties(height=side_chart_height)
                .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
            )
            st.altair_chart(dist_chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Risk</div>', unsafe_allow_html=True)
if prices_error:
    st.info(prices_error)
else:
    vol_df = base_prices_df.copy()
    vol_df["trade_date"] = pd.to_datetime(vol_df["trade_date"])
    if "realized_vol_21d" in vol_df.columns and vol_df["realized_vol_21d"].notna().any():
        vol_df["vol"] = vol_df["realized_vol_21d"]
    else:
        vol_df["vol"] = (
            vol_df["returns_1d"].rolling(21).std() * (252 ** 0.5)
        )
    vol_df = _filter_by_horizon(vol_df, horizon)

    vol_chart = (
        alt.Chart(vol_df.dropna(subset=["vol"]))
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
            y=alt.Y("vol:Q", title="Annualized volatility"),
            tooltip=[
                alt.Tooltip("trade_date:T", title="Date"),
                alt.Tooltip("vol:Q", title="Volatility", format=".2%"),
            ],
        )
        .properties(height=240)
        .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
    )

    drawdown_df = history_df[["trade_date", "close"]].copy()
    drawdown_df = drawdown_df.dropna(subset=["close"])
    if not drawdown_df.empty:
        drawdown_df["running_peak"] = drawdown_df["close"].cummax()
        drawdown_df["drawdown_pct"] = (
            (drawdown_df["close"] / drawdown_df["running_peak"]) - 1.0
        ) * 100.0
    drawdown_df = drawdown_df.dropna(subset=["drawdown_pct"])

    volume_df = history_df[["trade_date", "volume"]].copy()
    volume_df = volume_df.dropna(subset=["volume"])

    vol_left, drawdown_col, volume_col = st.columns([1, 1, 1], gap="medium")
    with vol_left:
        st.markdown(
            '<div class="section-title" style="margin: 0 0 -8px 0; line-height: 1;">'
            "Volatility</div>",
            unsafe_allow_html=True,
        )
        st.altair_chart(vol_chart, use_container_width=True)

    with drawdown_col:
        st.markdown(
            '<div class="section-title" style="margin: 0 0 -8px 0; line-height: 1;">'
            "Drawdown</div>",
            unsafe_allow_html=True,
        )
        if drawdown_df.empty:
            st.caption("Drawdown unavailable for the selected horizon.")
        else:
            drawdown_chart = (
                alt.Chart(drawdown_df)
                .mark_area(opacity=0.35, color="#ef4444")
                .encode(
                    x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                    y=alt.Y(
                        "drawdown_pct:Q",
                        title="Drawdown (%)",
                        scale=alt.Scale(domain=[drawdown_df["drawdown_pct"].min(), 0]),
                    ),
                    tooltip=[
                        alt.Tooltip("trade_date:T", title="Date"),
                        alt.Tooltip(
                            "drawdown_pct:Q",
                            title="Drawdown (%)",
                            format=".2f",
                        ),
                    ],
                )
                .properties(height=240)
                .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
            )
            st.altair_chart(drawdown_chart, use_container_width=True)

    with volume_col:
        st.markdown(
            '<div class="section-title" style="margin: 0 0 -8px 0; line-height: 1;">'
            "Volume</div>",
            unsafe_allow_html=True,
        )
        if volume_df.empty:
            st.caption("Volume unavailable for the selected horizon.")
        else:
            volume_chart = (
                alt.Chart(volume_df)
                .mark_bar(color="#60a5fa", opacity=0.6)
                .encode(
                    x=alt.X("trade_date:T", title="Date", axis=alt.Axis(format=x_axis_format)),
                    y=alt.Y("volume:Q", title="Volume"),
                    tooltip=[
                        alt.Tooltip("trade_date:T", title="Date"),
                        alt.Tooltip("volume:Q", title="Volume", format=","),
                    ],
                )
                .properties(height=240)
                .configure_axis(gridColor="rgba(148, 163, 184, 0.25)")
            )
            st.altair_chart(volume_chart, use_container_width=True)
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
