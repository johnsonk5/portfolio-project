# Sources

This document describes each source, what it powers, and important operational details.

## Alpaca API

### What We Use
- Assets metadata to build and maintain `silver.assets`.
- Market bars to build bronze and silver prices, then `gold.prices`.

### Why It Matters
- Defines the active trading universe (`is_active`) used by prices and news ingestion.
- Provides OHLCV inputs for core market metrics in the dashboard.

### Operational Notes
- The prices pipeline runs for the previous US trading day.
- Silver prices are written as partitioned parquet under `data/silver/prices/date=YYYY-MM-DD/symbol=.../prices*.parquet`.

## Yahoo Finance Search API

### What We Use
- News headlines per symbol (`title`, `link`, `publisher`, `provider_publish_time`, etc.).

### Why It Matters
- Feeds `silver.news` and `gold.headlines`.
- Supports sentiment analytics and dashboard "Good News / Bad News" panels.

### Operational Notes
- Ingestion runs daily for the current date partition.
- Publisher names are normalized and joined to `silver.ref_publishers`.

## Wikimedia REST API (Pageviews)

### What We Use
- Daily article pageviews for company-related pages.

### Why It Matters
- Feeds `silver.wikipedia_pageviews` and `gold.activity`.
- Adds an alternative attention signal alongside market and news data.

### Freshness and Lag
- Recent dates can lag. Data may not be complete for same-day or immediately prior-day runs.
- Reliability checks account for this using partition-level thresholds and explicit freshness checks.

### Notes on Edits Data
- Wikipedia edits were evaluated and intentionally not included in V1.
- Main reasons: weaker signal value and lower and less consistent timeliness for daily workflows.

## Tranco

### What We Use
- Top domains snapshot (`rank`, `domain`) from monthly CSV snapshots.

### Why It Matters
- Provides a ranking-based signal for `publisher_weight` in `silver.ref_publishers`.
- Publisher weights are used in `gold.prices.sentiment_score` aggregation.

### Operational Notes
- Monthly ingestion stores snapshots under `data/bronze/tranco/date=YYYY-MM-DD/tranco.csv`.
- If no rank match exists, default publisher weight fallback is used.

## Wikipedia S&P 500 Constituents

### What We Use
- Constituents and company metadata for `silver.ref_sp500`.

### Why It Matters
- Enriches asset metadata and supports index-related filtering and analysis.

### Operational Notes
- Refreshed weekly.
- Joined to `silver.assets` where symbol matching is possible.

## Kenneth R. French Data Library

- Daily Fama-French 3 factors plus daily momentum factor.
- Bronze ingestion stores snapshot parquet files under `data/bronze/fama_french_factors/date=YYYY-MM-DD/factors.parquet`.
- Silver normalization rebuilds a single parquet file at `data/silver/factors/factors.parquet`.
- Feeds `silver.factors`.
