# Dashboard

This document describes the current Streamlit pages and their behavior.

## Market Vibecheck (`streamlit_app.py`)

A daily overview page built off `gold.prices` and `gold.headlines`. Clicking on any symbol on this page will take you to the `Deep Dive` page for that symbol.

### Data Anchor
- All Market Vibecheck KPIs are calculated on `latest_trade_date = MAX(gold.prices.trade_date)`.

### Big Picture

<img width="768" height="87" alt="image" src="https://github.com/user-attachments/assets/7b3e56a8-c7aa-48cd-a6c2-c85333d40454" />

- `% of stocks up`:
  - Universe: rows where `returns_1d IS NOT NULL`.
  - Formula: `100 * mean(returns_1d > 0)`.
- `% above 50D SMA`:
  - Universe: rows with `close` and `sma_50` both non-null.
  - Formula: `100 * mean(close > sma_50)`.
- `% above 200D SMA`:
  - Universe: rows with `close` and `sma_200` both non-null.
  - Formula: `100 * mean(close > sma_200)`.
- `Mag 7 Return Impact`:
  - Mag 7 set: `AAPL, MSFT, NVDA, AMZN, GOOGL, GOOG, META, TSLA`.
  - Per-symbol impact: `weighted_return = returns_1d * dollar_volume`.
  - Formula:
    - numerator: `sum(abs(weighted_return))` over Mag 7.
    - denominator: `sum(abs(weighted_return))` over all symbols with non-null positive `dollar_volume`.
    - KPI: `100 * numerator / denominator`.
  - This metric is bounded in `[0, 100]` and represents share of total absolute daily return impact.

### Top Gainers + Losers
- Top 5 gainers and top 5 losers by latest `returns_1d`.
- Green for positive, red for negative.

### Risky Bets
- Uses latest `realized_vol_21d` and `returns_21d`.
- `Hot Ones`: positive 21D return names, ranked by highest 21D return.
- `Crashing Out`: negative 21D return names, ranked by lowest 21D return.
- `Sleepy`: lowest volatility names by `realized_vol_21d`.
- Each list displays up to 6 rows (with blank filler rows to keep card height stable).

Note: `Hot Ones` and `Crashing Out` are filtered by momentum sign and ranked by momentum, not ranked by volatility.

### Underrated Investments
- Uses `momentum_12_1` and `pct_below_52w_high`.
- Composite score = z-score(momentum_12_1) + z-score(pct_below_52w_high).
- Displays top 6 ranked names (with blank filler rows to keep card height stable).

### The Good News / The Bad News
- Based on latest `sentiment_score` and `returns_5d`.
- Good News: highest sentiment names.
- Bad News: lowest sentiment names.
- Grouped bars show sentiment and 5D return together.

## Deep Dive (`pages/Deep_Dive.py`)

Focused page for one symbol.

### Controls
- Symbol selector (with company name when available).
- Horizon selector: `1M`, `3M`, `1Y`, `3Y`, `5Y`.

### Sections

### Snapshot KPIs
- `Latest close`: latest `close`.
- Delta shown under `Latest close`:
  - Preferred: percent move vs previous close.
  - Fallback: percent move from latest open to latest close when previous close is unavailable.
- `5D return`: latest `returns_5d * 100`.
- `21D return`: latest `returns_21d * 100`.
- `21D volatility`:
  - Preferred: latest `realized_vol_21d * 100`.
  - Fallback: rolling 21-day stddev of `returns_1d` annualized by `sqrt(252)`, then `* 100`.

### Performance History
- Candlestick chart with dynamic y-axis.
- SMA50 overlay.
- Side chart of cumulative daily return over selected horizon.
- Side chart of distance from SMA50.

### Risk
- Volatility trend (`realized_vol_21d` or fallback rolling vol from `returns_1d`).
- Drawdown area chart from rolling peak.
- Volume bars.

### Recent Headlines
- Table showing recent headlines color coded by sentiment.
- `Published`, `Headline`, `Sentiment`.
- Sentiment color coding (positive, neutral, negative).

## Observability Page (`pages/Observability.py`)

Monitoring page for run health and checks.

### Recent Failures and Errors:
- Run failures from `observability.run_log`.
- Freshness issues from `observability.data_freshness_checks` where status in `FAIL`, `SKIPPED`, `WARN`.
### Data Quality Checks:
- DQ issues from `observability.data_quality_checks` where status in `FAIL`, `SKIPPED`, `WARN`.
- Recent DQ results (all statuses).
### Recent Runs 
- Table from `observability.run_log` showing all recent job runs.
### Run Asset Metrics 
- Asset focused table log from `observability.run_asset_log`.
### Job Duration Trend over last 30 days
- Line chart showing job duration, color coded for failure or success.
