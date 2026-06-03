# Decisions

This document records important architecture, tech stack, and operating decisions that are already in effect.

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
  - Silver: normalized, join-ready datasets. Storage varies between partitioned parquet and DuckDB tables based on data volume and access pattern.
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
- Current policy:
  - Observability tables are centralized in `portfolio.duckdb`.
  - `research.duckdb` is reserved for research data assets and derived research tables.

### 5. Streamlit is the application layer
- Status: Accepted
- Why: The current user-facing product is a Streamlit app that reads the curated data and observability layers to expose market analytics and operational views.
- Current entrypoint:
  - `streamlit_app.py`

### 6. Alpaca is the primary live and recent-window equities provider
- Status: Accepted
- Why: Alpaca remains the active source for equities reference data, intraday app-facing price ingestion, and the overlapping recent window of research daily prices.
- Notes:
  - API credentials are provided via environment variables.
  - The current client supports historical market data plus trading/asset metadata access.

### 7. EODHD is the historical research daily price provider
- Status: Accepted
- Why: The research layer needs broad daily US equity history back to 2000, and EODHD provides bulk historical end-of-day coverage without relying on Yahoo Finance scraping.
- Notes:
  - API credentials are provided via environment variables.
  - Bronze research daily prices are stored separately from the Alpaca recent-window dataset.
  - EODHD is treated as a historical/backfill source rather than a scheduled live ingestion source.

### 8. Reliability is treated as a first-class platform concern
- Status: Accepted
- Why: The project already persists run metadata, freshness results, and data quality results in an `observability` schema and surfaces them in the app.
- Current reliability model:
  - `observability.run_log`
  - `observability.run_asset_log`
  - `observability.data_freshness_checks`
  - `observability.data_quality_checks`

### 9. DuckDB access is serialized with a filesystem lock
- Status: Accepted
- Why: The current platform uses a lock file to avoid concurrent access issues against DuckDB from overlapping Dagster activity.
- Current behavior:
  - Lock path is colocated with each DuckDB file and includes the database name, for example `.portfolio.duckdb.write.lock`
  - Locking is operation-scoped rather than held for the full run
  - Timeout and stale-lock behavior are configurable

### 10. Schedules run in America/New_York and align with trading-day logic
- Status: Accepted
- Why: The pipelines are market-oriented and the current schedules explicitly use New York time, with price and Wikipedia jobs keyed off the prior US trading day where appropriate.
- Current examples:
  - `daily_prices_schedule`: `30 9 * * *`
  - `prices_compaction_schedule`: `45 9 * * *`
  - `daily_news_schedule`: `0 9 * * *`
  - `wikipedia_daily_schedule`: `45 8 * * *`

## Research Decisions

### 11. Research workloads will live in a separate DuckDB database
- Status: Accepted
- Why: Research ingestion, factor data, backtesting, and strategy outputs should not share the same database lifecycle as the live/app-serving store.
- Intent:
  - Keep research assets distinct from the current live/app database
  - Keep observability centralized in the portfolio/app database instead of adding a separate research observability schema
  - Reduce coupling between experimental strategy work and the existing app data model

### 12. Research data will extend the historical window back to 2000
- Status: Accepted
- Why: The strategy framework needs a materially longer daily history for historical testing and factor analysis than the current app-focused daily pipelines provide.

### 13. The initial factor model is Carhart 4-factor
- Status: Accepted
- Why: Strategy evaluation uses market, size, value, and momentum exposures, which supports alpha estimation and factor-based comparison.
- Included factors:
  - `MKT-RF`
  - `SMB`
  - `HML`
  - `MOM`

### 14. Fama-French factor data will come from the Kenneth R. French Data Library
- Status: Accepted
- Why: The research layer standardizes factor ingestion on this source.

### 15. Strategy definitions and strategy runs are separate concerns
- Status: Accepted
- Why: Strategy configuration should remain independent from execution outputs so runs can be repeated, compared, and audited without overwriting definitions.
- Tables:
  - `silver.strategy_definitions`
  - `silver.strategy_runs`
  - `silver.strategy_parameters`
  - `gold.strategy_rankings`
  - `gold.strategy_holdings`
  - `gold.strategy_returns`
  - `gold.strategy_performance`

### 16. Strategy runs should be historical and reproducible
- Status: Accepted
- Why: Backtests and reruns need to be compared over time.
- Implication:
  - Use run-level identifiers
  - Preserve prior strategy outputs instead of overwriting them in place

### 17. Research pricing will be sourced from separate bronze datasets by provider
- Status: Accepted
- Why: Historical coverage and recent overlap differ by provider, so the bronze layer keeps EODHD and Alpaca daily prices separate and leaves provider-priority logic to downstream consumers.
- Current implementation:
  - The shared research daily price partition start is controlled by `RESEARCH_PRICES_PARTITIONS_START_DATE`, defaulting to `RESEARCH_EODHD_PRICES_PARTITIONS_START_DATE` (`2000-01-01` by default).
  - `bronze.eodhd_prices_daily` is retained for historical/backfill partitions and gap filling.
  - `bronze.alpaca_prices_daily` is the scheduled recent/live research refresh source.
  - Where both providers overlap, downstream research logic should prefer Alpaca and fall back to EODHD elsewhere.
- Tradeoff:
  - Provider-specific bronze storage preserves auditability, while the silver layer owns precedence and liquidity-universe logic.

### 18. Research universe membership is derived from price liquidity, not a static constituent file
- Status: Accepted
- Why: The research workflows need a broad, reproducible investable universe that can evolve with market liquidity instead of inheriting a hand-maintained historical index file.
- Current implementation:
  - `silver.research_daily_prices` merges daily research prices and prefers Alpaca over EODHD on overlapping symbol-days.
  - `silver.universe_membership_daily` selects the top 500 symbols by trailing average dollar volume.
  - `silver.universe_membership_events` records adds and removals versus the prior trading day.

### 19. Reusable research signals are materialized in DuckDB
- Status: Accepted
- Why: Strategy selection and analysis need stable daily features without recomputing rolling metrics from parquet for every downstream asset.
- Current implementation:
  - `silver.signals_daily` is rebuilt from `silver.research_daily_prices`.
  - The table includes return horizons, moving averages, momentum, volatility, drawdown, 52-week high/low, and liquidity signals.

### 20. Simulation and run type references are catalog-driven
- Status: Accepted
- Why: Backtest and simulation semantics should be explicit, reusable, and validated before strategy outputs are built.
- Current implementation:
  - `ref.run_types` and `ref.simulation_types` are loaded from `src/portfolio_project/config/simulation_reference.yaml`.
  - `silver.strategy_runs` stores run type and simulation type references for downstream strategy outputs.

## Open Items

These are not decided yet and should remain out of scope for this file until explicitly chosen:

- Final layout for the `src/portfolio_project` reorganization
- Whether multiple DuckDB resources should be registered explicitly in `Definitions` or composed from environment-specific config
- Whether EODHD historical backfill should remain manual-only or get a dedicated unscheduled backfill job wrapper
- Final alert severity policy for research and strategy workflows
