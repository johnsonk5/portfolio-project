# Reliability

This document explains how the project manages data reliability and operational safety.

## Reliability Model

Reliability is handled with three layers:
- Run observability (`run_log`, `run_asset_log`).
- Freshness checks (`data_freshness_checks`).
- Data quality checks (`data_quality_checks`).

All observability tables are written to the portfolio DuckDB (`portfolio.duckdb`), even for research workflows that read and write data in `research.duckdb`.

## Run Observability

### Tables
- `observability.run_log`: one row per run id (upsert-like behavior via delete + insert per run id).
- `observability.run_asset_log`: per-asset metrics for a run (rows replaced per run id).

### Captured Metrics
- Run status, timing, partition, tags, error message.
- Materialization counts and mutation metrics: `row_count`, `rows_inserted`, `rows_updated`, `rows_deleted`.

## Freshness Checks

Freshness checks run on successful runs and write to `observability.data_freshness_checks`.

### Current Coverage
- `daily_prices_job`: `prices_active_symbol_coverage`.
- `research_daily_prices_job`:
  - `research_daily_prices_latest_trading_date_present`.
  - `research_daily_prices_partition_row_count_vs_recent_median`.
  - `research_universe_membership_latest_trading_date_present`.
  - `research_universe_membership_symbol_count_vs_recent_median`.
  - `research_universe_membership_day_over_day_drop_ratio`.
  - `research_universe_membership_avg_symbol_missing_data_rate`.
- `daily_news_job`: `daily_news_partition_row_count`.
- `wikipedia_activity_job`: `wikipedia_assets_with_views_min_count`.

### Status and Severity
- Status: `PASS`, `FAIL`, `SKIPPED`.
- Severity:
  - `RED`: `prices_active_symbol_coverage`.
  - `YELLOW`: news, Wikipedia, research row-count, and research-universe coverage threshold freshness checks.

## Data Quality Checks

DQ checks run on successful runs and write to `observability.data_quality_checks`.

### Current Coverage
- `daily_prices_job`:
  - `silver.assets` active-symbol uniqueness precondition.
  - Silver prices schema, uniqueness, null thresholds, and numeric/range checks.
  - Gold prices schema, uniqueness, null thresholds, and numeric/range checks.
- `research_daily_prices_job`:
  - `silver.research_daily_prices` expected columns and DuckDB data types.
  - `silver.research_daily_prices` uniqueness on `symbol` + `trade_date`.
  - `silver.research_daily_prices` null checks plus invalid numeric and price-range validation.
- `daily_news_job`:
  - Silver news schema, uniqueness, and null-threshold checks.
- `wikipedia_activity_job`:
  - Silver Wikipedia schema, uniqueness, null-threshold, and non-negative view checks.
- `sp500_update_job`:
  - `silver.ref_sp500` schema, uniqueness, null-threshold checks.
- `tranco_update_job`:
  - Latest Tranco snapshot schema, uniqueness, range checks.
- `asset_status_updates_job`:
  - `silver.assets` schema, uniqueness, null-threshold checks.

### Status and Severity
- Status: `PASS`, `FAIL`, `SKIPPED`, `WARN` (surfaced in dashboard issue views).
- Severity policy:
  - `RED` for critical hard failures.
  - `YELLOW` for less-essential datasets and softer warning-style checks.

### Important Behavior
- DQ rows are inserted per evaluation event; they are not globally replaced per `run_id` in the current writer path.
- A unique `check_id` is stored per DQ row.

## DuckDB Concurrency Safety

DuckDB access uses filesystem locking with one lock file per database, such as `data/duckdb/.portfolio.duckdb.write.lock` (or the configured DuckDB directory).

### Locking Behavior
- Lock acquisition is operation-scoped for DB calls (`execute`, `executemany`, `register`, `commit`).
- Long non-DB work does not hold the lock.
- Connection-open failure paths release the lock before re-raising.

### Tuning
- `lock_timeout_seconds` (default `120`).
- `stale_lock_seconds` (default `600`).

## Reliability Threshold Config

Environment variables for DQ thresholds:
- `DQ_PRICES_NULL_PCT_THRESHOLD`
- `DQ_GOLD_PRICES_CORE_NULL_PCT_THRESHOLD`
- `DQ_NEWS_BOTH_TITLE_LINK_NULL_PCT_THRESHOLD`
- `DQ_WIKIPEDIA_ASSET_ID_NULL_PCT_THRESHOLD`
- `DQ_SP500_SYMBOL_NULL_PCT_THRESHOLD`
- `DQ_SP500_ASSET_ID_NULL_PCT_THRESHOLD`
- `DQ_ASSETS_CORE_NULL_PCT_THRESHOLD`

Freshness threshold config:
- `WIKIPEDIA_MIN_ASSETS_WITH_VIEWS` (default `400`).
- `RESEARCH_FRESHNESS_MIN_ROW_COUNT_RATIO` (default `0.7`).
- `RESEARCH_FRESHNESS_ROW_COUNT_LOOKBACK_PARTITIONS` (default `20`).
- `RESEARCH_FRESHNESS_ROW_COUNT_MIN_HISTORY` (default `3`).
- `RESEARCH_UNIVERSE_FRESHNESS_MIN_SYMBOL_COUNT_RATIO` (default `0.95`).
- `RESEARCH_UNIVERSE_FRESHNESS_LOOKBACK_PARTITIONS` (default `20`).
- `RESEARCH_UNIVERSE_FRESHNESS_MIN_HISTORY` (default `3`).
- `RESEARCH_UNIVERSE_DROP_LOOKBACK_PARTITIONS` (default `20`).
- `RESEARCH_UNIVERSE_DROP_MIN_HISTORY` (default `3`).
- `RESEARCH_UNIVERSE_DROP_RATIO_MULTIPLIER` (default `3.0`).
- `RESEARCH_UNIVERSE_DROP_MIN_RATIO` (default `0.03`).
- `RESEARCH_UNIVERSE_MISSING_DATA_LOOKBACK_PARTITIONS` (default `20`).
- `RESEARCH_UNIVERSE_MISSING_DATA_MIN_HISTORY` (default `3`).
- `RESEARCH_UNIVERSE_MISSING_DATA_MAX_AVG_RATE` (default `0.05`).
