# Pipelines

This document describes Dagster jobs, schedules, and primary assets.

## `daily_prices_job`

### Purpose
Build daily prices and factors for active symbols.

### Flow
- `bronze_alpaca_bars` -> `silver_alpaca_prices_parquet` -> `gold_alpaca_prices`

## Research Daily Prices Assets

### Purpose
Build merged research daily prices and a liquidity-based universe history.

### Flow
- `bronze_eodhd_prices_daily` + `bronze_alpaca_prices_daily` -> `silver.research_daily_prices`
- `silver.research_daily_prices` -> `silver.universe_membership_daily` -> `silver.universe_membership_events`

## `research_daily_prices_job`

### Purpose
Refresh the recent-window research price history from Alpaca and rebuild the downstream silver research prices plus liquidity-universe tables.

### Flow
- `bronze_alpaca_prices_daily` + `bronze_alpaca_corporate_actions_daily` -> `silver.alpaca_corporate_actions` -> `silver.research_daily_prices` -> `silver.signals_daily`
- `silver.research_daily_prices` -> `silver.universe_membership_daily` -> `silver.universe_membership_events`

### Data Quality
- Validates expected columns and DuckDB data types for each emitted `silver.research_daily_prices` partition and writes the result to `observability.data_quality_checks` in the portfolio DuckDB.
- Checks required fields plus logically invalid price/count values and price-range violations for each emitted `silver.research_daily_prices` partition.
- Checks each emitted `silver.research_daily_prices` partition for duplicate `symbol` + `trade_date` rows.

### Freshness
- Verifies the expected latest trading-date partition exists in `silver.research_daily_prices`.
- Compares the current partition row count against the recent median partition size and alerts on material drops.
- Verifies `silver.universe_membership_daily` includes the expected latest trading date.
- Tracks universe symbol counts by `member_date` and compares the latest count against the recent median.

### Schedule
- `research_daily_prices_schedule`: `35 9 * * *` America/New_York.
- Uses previous US trading day as partition key.

### Schedule
- `daily_prices_schedule`: `30 9 * * *` America/New_York.
- Uses previous US trading day as partition key.

## `prices_compaction_job`

### Purpose
Compact silver prices to partition by symbol and month. The silver layer can get big quickly and compacting it makes it easier to query by symbol.

### Flow
- `silver_alpaca_prices_compact`

### Schedule
- `prices_compaction_schedule`: `45 9 * * *` America/New_York.
- Partition key is first day of prior trading month selected by schedule function.

## `daily_news_job`

### Purpose
Ingest headlines and publish normalized plus gold news tables.

### Flow
- `bronze_yahoo_news` -> `silver_ref_publishers` -> `silver_news` -> `gold_headlines`

### Schedule
- `daily_news_schedule`: `0 9 * * *` America/New_York.
- Uses current local date as partition key.

## `wikipedia_activity_job`

### Purpose
Ingest Wikipedia pageviews and build activity analytics.

### Flow
- `bronze_wikipedia_pageviews` -> `silver_wikipedia_pageviews` -> `gold_activity`

### Schedule
- `wikipedia_daily_schedule`: `45 8 * * *` America/New_York.
- Uses previous US trading day as partition key.

## `sp500_update_job`

### Purpose
Refresh S&P 500 reference data.

### Flow
- `bronze_sp500_companies` -> `silver_sp500_companies`

### Schedule
- `sp500_weekly_schedule`: `0 17 * * 5` America/New_York.

## `tranco_update_job`

### Purpose
Ingest monthly Tranco snapshot for publisher weighting.

### Flow
- `bronze_tranco_snapshot`

### Schedule
- `tranco_monthly_schedule`: `0 18 1 * *` America/New_York.

## `asset_status_updates_job`

### Purpose
Update active status flags and history for assets.

### Trigger
- Manual; no schedule defined.

## `sample_demo_seed_job`

### Purpose
Seed demo data for local and demo flows.

### Trigger
- Manual; no schedule defined.

## Observability Hooks

These jobs use success and failure hooks:
- `dagster_run_log_success`
- `dagster_run_log_failure`

Success hook writes run log data and triggers freshness and DQ checks.

## `monthly_factors_job`

### Purpose
Ingest Kenneth French factor data and rebuild the silver factors parquet history.

### Flow
- `bronze_fama_french_factors` -> `silver_fama_french_factors_parquet`

### Schedule
- `monthly_factors_schedule`: `15 9 1 * *` America/New_York. Runs once a month on the 1st at 9:15 AM ET.
