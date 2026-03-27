# Architecture

This document describes the system architecture and storage layout.

## High-Level Flow

1. Pull source data from APIs and snapshot feeds.
2. Persist bronze outputs in partitioned parquet.
3. Normalize into silver assets/tables and silver parquet partitions.
4. Build gold analytics tables in DuckDB.
5. Serve dashboards from gold and observability schemas.

## Components

- Dagster: orchestration, jobs, schedules, hooks.
- DuckDB: analytics and observability store.
- Parquet files: partitioned bronze and silver storage.
- Streamlit: dashboard application.

## Data Layers

### Bronze
- Source-like raw outputs with `ingested_ts`.
- Stored as partitioned files under `data/bronze`.

### Silver
- Join-ready and normalized data.
- `silver.assets` and references in DuckDB.
- Prices, news, and Wikipedia are written as partitioned parquet datasets under `data/silver`.

### Gold
- Analytics layer in DuckDB (`gold.prices`, `gold.headlines`, `gold.activity`).

### Observability
- Reliability tables in DuckDB (`observability.*`).
- Includes run logs, per-asset run metrics, freshness checks, and DQ checks.
- Observability is centralized in the portfolio DuckDB, including checks emitted by research workflows.

## Storage Defaults

- Data root: `PORTFOLIO_DATA_DIR` (default `data`).
- DuckDB path defaults: `data/duckdb/portfolio.duckdb` for the live/app store and observability, and `data/duckdb/research.duckdb` for research data workflows.
- Optional overrides: `PORTFOLIO_DUCKDB_PATH` and `PORTFOLIO_RESEARCH_DUCKDB_PATH`.

## Concurrency and Safety

- A per-database filesystem lock controls DuckDB operations (for example `.portfolio.duckdb.write.lock`).
- Lock is operation-scoped to avoid blocking while waiting on external APIs.
