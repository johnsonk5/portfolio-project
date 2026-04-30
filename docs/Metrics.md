# Metrics

This document explains gold-layer metrics and how they are calculated.

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
