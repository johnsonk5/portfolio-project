# Metrics

This document explains gold-layer metrics and how they are calculated.

## Initial Canonical Fundamental Metrics

SEC fundamentals are mapped into a small initial canonical metric set before they are
pivoted into `gold.fundamentals_quarterly` and promoted into
`gold.fundamental_signals_daily`. These metrics are the first supported contract for
fundamental research features; additional SEC concepts should map into this set unless a
new research use case requires a separate canonical metric.

| Canonical metric | Statement family | Period type | Definition |
| --- | --- | --- | --- |
| `revenue` | Income statement | Duration | Total revenue or sales for the reporting period. |
| `net_income` | Income statement | Duration | Net income attributable to the registrant or parent when available, otherwise consolidated net income. Prefer income available to common shareholders only for per-share calculations. |
| `assets` | Balance sheet | Instant | Total assets at the reporting period end. |
| `equity` | Balance sheet | Instant | Stockholders' equity attributable to the registrant at the reporting period end, excluding noncontrolling interests when available; otherwise total equity. |
| `debt` | Balance sheet | Instant | Interest-bearing debt at the reporting period end, preferably short-term debt plus current maturities of long-term debt plus long-term debt. Do not use total liabilities as a fallback. |
| `cash` | Balance sheet | Instant | Cash and cash equivalents at the reporting period end. |
| `diluted_shares` | Shares | Duration | Diluted weighted-average shares outstanding for the reporting period, not point-in-time shares outstanding. |
| `diluted_eps` | Income statement | Duration | Diluted earnings per share for the reporting period. |
| `operating_cash_flow` | Cash flow statement | Duration | Net cash provided by operating activities for the reporting period; positive values mean cash provided and negative values mean cash used. |
| `capex` | Cash flow statement | Duration | Capital expenditures for property, plant, equipment, and similar long-lived assets, stored as a positive cash outflow. |

Canonical metric rules:

- Select canonical metrics from SEC source facts using versioned mapping rules with explicit concept priority, unit requirements, period-type requirements, and sign conventions.
- Use `diluted_shares` for the canonical shares metric and `diluted_eps` for EPS so downstream valuation features have a stable, conservative basis.
- Treat balance sheet metrics as point-in-time values from the selected filing's period end.
- Treat income statement and cash flow metrics as fiscal-quarter duration values. Year-to-date source facts must be converted to current-quarter values before promotion into `gold.fundamentals_quarterly`.
- For 10-K filings, derive Q4 duration values as annual values minus the sum of Q1 through Q3 when prior quarters are available.
- Daily research features may derive trailing-twelve-month values only from quarterly facts whose source filings were available as of the research date.
- Store monetary metrics in reported currency units, initially USD only. Store share metrics in shares and EPS metrics in currency per share. Retain non-matching units in bronze or silver unless a mapping version explicitly promotes them.
- Store `capex` as a positive cash outflow. If the selected source fact is reported as a negative cash flow, multiply by `-1` during canonicalization so free cash flow can be computed as `operating_cash_flow - capex`.
- Preserve SEC provenance on selected facts through source tag, taxonomy, accession number, form type, fiscal year, fiscal period, filing date, availability date, mapping version, mapping priority, and original SEC concept.
- Carry the mapping version on each selected canonical fact. Changes to concept priority, unit handling, period normalization, sign handling, or fallback logic require a new mapping version.
- Do not promote source facts into research features before `availability_date`, derived from `DATE(acceptance_datetime)` when available and otherwise `filing_date`.
- Point-in-time shares outstanding may be added later for market capitalization and enterprise-value calculations; it should not be conflated with diluted weighted-average shares.

## `gold.prices`

Daily per-asset metrics.

### Core Price Metrics
- `open`, `high`, `low`, `close`: daily OHLC built from intraday silver bars.
- `volume`: daily sum of volume.
- `trade_count`: daily sum of trade_count.
- `vwap`: volume-weighted aggregation of intraday `vwap` values.
- `dollar_volume`: `close * volume`.

### Return Metrics
- `returns_1d = adjusted_close_t / adjusted_close_t-1 - 1` when adjusted close is available, otherwise close.
- `returns_5d = adjusted_close_t / adjusted_close_t-5 - 1` when adjusted close is available, otherwise close.
- `returns_21d = adjusted_close_t / adjusted_close_t-21 - 1` when adjusted close is available, otherwise close.

### Trend and Risk Metrics
- `realized_vol_21d`: rolling 21-observation stddev of `returns_1d`, annualized by `sqrt(252)`.
- `momentum_12_1 = adjusted_close_t-21 / adjusted_close_t-252 - 1` when adjusted close is available, otherwise close.
- `pct_below_52w_high = (rolling_252d_high - close_t) / rolling_252d_high`
- `sma_50`: rolling 50-day average of close.
- `sma_200`: rolling 200-day average of close.
- `dist_sma_50 = close_t / sma_50 - 1`
- `dist_sma_200 = close_t / sma_200 - 1`

### Sentiment Metric
- `sentiment_score`: weighted average of headline sentiment over a 7-day window (`trade_date - 6` to `trade_date`).
- Label mapping:
  - positive = `1`
  - neutral = `0`
  - negative = `-1`
- Weighted by `silver.ref_publishers.publisher_weight`.

## `gold.headlines`

Rolling 30-day headlines table used by dashboard and sentiment logic.

### Key Fields
- `provider_publish_time`
- `title`
- `sentiment` (FinBERT label from headline text)

## `gold.activity`

Rolling activity table (currently Wikipedia pageviews).

### Metrics
- `views`: raw daily pageviews.
- `views_30d_avg`: rolling 30-day average views per asset.
- `views_vs_30d_avg = views / views_30d_avg` (NULL when denominator is zero or NULL).
