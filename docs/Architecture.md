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
- Prices, news, and Wikipedia are written as partitioned parquet datasets.

### Gold
- Analytics and factor layer in DuckDB (`gold.prices`, `gold.headlines`, `gold.activity`).

### Observability
- Reliability tables in DuckDB (`observability.*`).
- Includes run logs, per-asset run metrics, freshness checks, and DQ checks.

## Storage Defaults

- Data root: `PORTFOLIO_DATA_DIR` (default `data`).
- DuckDB path default: `data/duckdb/portfolio.duckdb`.
- Optional override: `PORTFOLIO_DUCKDB_PATH`.

## Concurrency and Safety

- Filesystem lock controls DuckDB operations (`.duckdb_write.lock`).
- Lock is operation-scoped to avoid blocking while waiting on external APIs.
