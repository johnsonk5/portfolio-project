# Pipelines

This document describes Dagster jobs, schedules, and primary assets.

## `daily_prices_job`

### Purpose
Build daily prices and factors for active symbols.

### Flow
- `bronze_alpaca_bars` -> `silver_alpaca_prices_parquet` -> `gold_alpaca_prices`

`gold_alpaca_prices` reads `silver.alpaca_corporate_actions` when available to compute
split-adjusted close. Refresh corporate actions through `research_daily_prices_job` or
`scripts/backfill_portfolio_adjusted_close.py`; they are not materialized inside
`daily_prices_job` because the research corporate-action asset uses a different partition range.

### Schedule
- `daily_prices_schedule`: `30 9 * * *` America/New_York.
- Uses previous US trading day as partition key.

## Research Daily Prices Assets

### Purpose
Build merged research daily prices and a liquidity-based universe history.

### Flow
- Historical/backfill EODHD partitions in `bronze_eodhd_prices_daily` plus recent Alpaca partitions in `bronze_alpaca_prices_daily` -> `silver.research_daily_prices`
- `silver.research_daily_prices` -> `silver.universe_membership_daily` -> `silver.universe_membership_events`

EODHD ingestion is registered as a Dagster asset for historical backfill and gap filling, but it is
not part of the scheduled live refresh jobs. The scheduled research refresh currently uses Alpaca
daily prices and any already-seeded EODHD history.

## `research_daily_prices_job`

### Purpose
Refresh the recent-window research price history from Alpaca and rebuild the downstream silver research prices plus liquidity-universe tables.

### Flow
- `bronze_alpaca_prices_daily` + `bronze_alpaca_corporate_actions_daily` -> `silver.alpaca_corporate_actions` -> `silver.research_daily_prices` -> `silver.signals_daily`
- `silver.research_daily_prices` -> `silver.universe_membership_daily` -> `silver.universe_membership_events`

### Data Quality
- Validates expected columns and DuckDB data types for each emitted `silver.research_daily_prices` partition and writes the result to `observability.data_quality_checks` in the portfolio DuckDB.
- Checks required fields plus logically invalid price/count values and price-range violations for each emitted `silver.research_daily_prices` partition.
- Checks each emitted `silver.research_daily_prices` partition for duplicate asset-first price-key + `trade_date` rows.

### Freshness
- Verifies the expected latest trading-date partition exists in `silver.research_daily_prices`.
- Compares the current partition row count against the recent median partition size and alerts on material drops.
- Verifies `silver.universe_membership_daily` includes the expected latest trading date.
- Tracks universe symbol counts by `member_date` and compares the latest count against the recent median.
- Flags unusual day-over-day drops in research universe size.
- Tracks recent missing-data rates for symbols in the latest research universe.

### Schedule
- `research_daily_prices_schedule`: `35 9 * * *` America/New_York.
- Uses previous US trading day as partition key.

## SEC Fundamentals Assets

### Purpose
Ingest SEC company fundamentals and publish point-in-time research features for strategy research.

### Flow
- Raw SEC bulk archives under `data/bronze/sec/companyfacts/`, `data/bronze/sec/submissions/`, and `data/bronze/sec/company_tickers/` -> parsed bronze datasets under `data/bronze/sec_company_facts/`, `data/bronze/sec_submissions/`, and `data/bronze/sec_company_tickers/`
- Parsed bronze SEC datasets -> `silver.security_identifiers`, `silver.sec_submissions`, `silver.sec_facts_long`, and `silver.sec_statement_items`
- `silver.sec_statement_items` -> `gold.fundamentals_quarterly` -> `gold.fundamental_signals_daily`

### Universe And Coverage Assumptions
- SEC fundamentals are an enrichment layer for securities that can be mapped to SEC registrants; they are not the source of truth for the research universe.
- `silver.universe_membership_daily` remains driven by research prices and liquidity, so SEC-covered and non-SEC-covered symbols can both appear in the universe.
- Research price, signal, universe, and strategy tables carry `asset_id` as the primary join key, with `symbol` retained as a denormalized display field.
- Downstream SEC assets should carry `asset_id` when a CIK can be mapped to a project security and allow nullable `asset_id` for unmapped SEC issuers.
- Strategy features should keep explicit missing-fundamentals indicators so survivorship bias is not introduced by silently dropping symbols without SEC coverage.
- Historical joins must preserve source symbols and use effective-dated CIK/ticker mappings where available, because ticker reuse, symbol changes, issuer actions, and delistings can otherwise create survivorship-biased joins.
- Strategy definitions can opt into SEC-derived ranking fields by adding `signal_source = fundamental_signals_daily` to strategy parameters. Existing price-only strategies continue to rank from `silver.signals_daily`, and fundamental strategies produce no rankings when `gold.fundamental_signals_daily` has not been materialized.

### Lookahead Rules
- Fundamental values become research-usable only on or after their SEC availability date.
- `availability_date` is derived from `DATE(acceptance_datetime)` when `acceptance_datetime` is present; otherwise it falls back to `filing_date`.
- `gold.fundamental_signals_daily` must only expose a filing on rows where `date >= availability_date`.
- Reporting period dates such as `period_end_date` are economic-period labels, not availability timestamps, and must not be used to decide when a fundamental could have been known.
- Strategy rankings, backtests, and promoted signal columns must consume SEC fundamentals through point-in-time daily outputs or apply the same `date >= availability_date` filter.

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

## `weekly_digest_job`

### Purpose
Send a weekly plain-text email with run stats, market KPIs, stocks far below their 52-week highs, and five random recent news stories.

### Flow
- `weekly_digest_email`

### Schedule
- `weekly_digest_schedule`: `0 8 * * 1` America/New_York.

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
