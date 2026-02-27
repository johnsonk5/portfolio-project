# Dashboard

This document describes the current Streamlit pages and their behavior.

## Market Vibecheck (`streamlit_app.py`)

A daily overview page built off `gold.prices` and `gold.headlines`. Clicking on any symbol on this page will take you to the `Deep Dive` page or that symbol.

### Big Picture
- `% of stocks up` from latest `returns_1d` coverage.
- `% above 50D SMA` and `% above 200D SMA` from latest `close` vs `sma_50` and `sma_200`.
- `Top N Contribution` based on dollar-volume-weighted return contribution.

### Top Gainers + Losers
- Top 5 gainers and top 5 losers by latest `returns_1d`.
- Green for positive, red for negative.

### Risky Bets
- Uses latest `realized_vol_21d` and `returns_21d`.
- `Hot Ones`: positive 21D return names, ranked by highest 21D return.
- `Crashing Out`: negative 21D return names, ranked by lowest 21D return.
- `Sleepy`: lowest volatility names by `realized_vol_21d`.

Note: `Hot Ones` and `Crashing Out` are filtered by momentum sign and ranked by momentum, not ranked by volatility.

### Underrated Investments
- Uses `momentum_12_1` and `pct_below_52w_high`.
- Composite score = z-score(momentum_12_1) + z-score(pct_below_52w_high).
- Displays top ranked names.

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
- latest close 
- 5D return  
- 21D return  
- 21D volatility

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
