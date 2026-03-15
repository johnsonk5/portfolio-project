# Decisions

This document records important architecture, tech stack, and operating decisions that are already in effect or explicitly planned for Phase 2.

## Current Decisions

### 1. Dagster is the orchestration layer
- Status: Accepted
- Why: The project already defines assets, jobs, schedules, hooks, and resources in Dagster. This keeps ingestion, transformations, and operational checks in one workflow system.
- Evidence:
  - Asset jobs and schedules are defined in `src/portfolio_project/definitions.py`.
  - Success and failure hooks are attached to pipeline jobs for observability logging.

### 2. The project uses a medallion data architecture
- Status: Accepted
- Why: The repo and docs are organized around Bronze, Silver, Gold, and Observability layers. This separates raw ingestion, normalized datasets, curated analytics, and operational monitoring.
- Current shape:
  - Bronze: source-like raw outputs in partitioned parquet.
  - Silver: normalized, join-ready datasets, mostly in parquet with DuckDB references/views.
  - Gold: analytics and dashboard-serving tables in DuckDB.
  - Observability: run logs, freshness checks, and data quality checks in DuckDB.

### 3. Parquet is the default file format for Bronze and much of Silver
- Status: Accepted
- Why: Partitioned parquet supports reproducible ingestion, efficient storage, and downstream queryability without forcing every intermediate step into the database.
- Current shape:
  - Bronze data lives under `data/bronze`.
  - Silver price, news, and Wikipedia datasets are written as partitioned parquet datasets under `data/silver`.

### 4. DuckDB is the analytical database for app-serving and observability workloads
- Status: Accepted
- Why: DuckDB gives the project a lightweight local analytical store for curated tables, logging, and dashboard queries without introducing external infrastructure.
- Current defaults:
  - Default database paths: `data/duckdb/portfolio.duckdb` and `data/duckdb/research.duckdb`
  - Overrides supported through `PORTFOLIO_DUCKDB_PATH` and `PORTFOLIO_RESEARCH_DUCKDB_PATH`
  - Base data directory override supported through `PORTFOLIO_DATA_DIR`

### 5. Streamlit is the application layer
- Status: Accepted
- Why: The current user-facing product is a Streamlit app that reads the curated data and observability layers to expose market analytics and operational views.
- Current entrypoint:
  - `streamlit_app.py`

### 6. Alpaca is the primary market data provider for equities
- Status: Accepted
- Why: Alpaca is already the active source for equities reference data and price ingestion, and Phase 2 planning continues to use it for research prices where coverage is sufficient.
- Notes:
  - API credentials are provided via environment variables.
  - The current client supports historical market data plus trading/asset metadata access.

### 7. Reliability is treated as a first-class platform concern
- Status: Accepted
- Why: The project already persists run metadata, freshness results, and data quality results in an `observability` schema and surfaces them in the app.
- Current reliability model:
  - `observability.run_log`
  - `observability.run_asset_log`
  - `observability.data_freshness_checks`
  - `observability.data_quality_checks`

### 8. DuckDB access is serialized with a filesystem lock
- Status: Accepted
- Why: The current platform uses a lock file to avoid concurrent access issues against DuckDB from overlapping Dagster activity.
- Current behavior:
  - Lock path is colocated with each DuckDB file and includes the database name, for example `.portfolio.duckdb.write.lock`
  - Locking is operation-scoped rather than held for the full run
  - Timeout and stale-lock behavior are configurable

### 9. Schedules run in America/New_York and align with trading-day logic
- Status: Accepted
- Why: The pipelines are market-oriented and the current schedules explicitly use New York time, with price and Wikipedia jobs keyed off the prior US trading day where appropriate.
- Current examples:
  - `daily_prices_schedule`: `30 9 * * *`
  - `prices_compaction_schedule`: `45 9 * * *`
  - `daily_news_schedule`: `0 9 * * *`
  - `wikipedia_daily_schedule`: `45 8 * * *`

## Phase 2 Decisions

### 10. Research workloads will live in a separate DuckDB database
- Status: Accepted for implementation
- Why: Phase 2 introduces research ingestion, factor data, backtesting, and strategy outputs that should not share the same database lifecycle as the live/app-serving store.
- Intent:
  - Keep research assets distinct from the current live/app database
  - Add a separate observability schema for research workflows
  - Reduce coupling between experimental strategy work and the existing app data model

### 11. Research data will extend the historical window back to 2000
- Status: Accepted for implementation
- Why: The strategy framework needs a materially longer daily history for historical testing and factor analysis than the current app-focused daily pipelines provide.

### 12. The initial factor model is Carhart 4-factor
- Status: Accepted for implementation
- Why: Phase 2 strategy evaluation is planned around market, size, value, and momentum exposures, which supports alpha estimation and factor-based comparison.
- Included factors:
  - `MKT-RF`
  - `SMB`
  - `HML`
  - `MOM`

### 13. Fama-French factor data will come from the Kenneth R. French Data Library
- Status: Accepted for implementation
- Why: The Phase 2 strategy plan explicitly standardizes factor ingestion on this source for the research layer.

### 14. Strategy definitions and strategy runs are separate concerns
- Status: Accepted for implementation
- Why: Strategy configuration should remain independent from execution outputs so runs can be repeated, compared, and audited without overwriting definitions.
- Intended tables:
  - `silver.strategy_definitions`
  - `silver.strategy_runs`
  - `silver.strategy_parameters`
  - `gold.strategy_rankings`
  - `gold.strategy_holdings`
  - `gold.strategy_returns`
  - `gold.strategy_performance`

### 15. Strategy runs should be historical and reproducible
- Status: Accepted for implementation
- Why: Phase 2 requires backtests and reruns that can be compared over time.
- Expected implication:
  - Use run-level identifiers
  - Preserve prior strategy outputs instead of overwriting them in place

## Open Items

These are not decided yet and should remain out of scope for this file until explicitly chosen:

- Final layout for the `src/portfolio_project` reorganization
- Whether multiple DuckDB resources should be registered explicitly in `Definitions` or composed from environment-specific config
- Whether the initial research universe should remain S&P 500-only or expand after coverage/cost review
- Final alert severity policy once Phase 2 research pipelines are live
