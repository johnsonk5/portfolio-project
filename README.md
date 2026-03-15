# Market Research Platform

## Overview

A reproducible market research platform that ingests equity price data and alternative media signals into a medallion data architecture.

The system automates ingestion, transformation, quality checks, and observability, and exposes curated gold-layer analytics through an interactive Streamlit application with several different dashboards views for insights into the market. This system brings data engineering rigor to the world of financial analytics resulting in a product for market analysis that is reliable, consistent and that provides users with powerful analytics for understanding the market.

## Documentation

- [Architecture](docs/Architecture.md)
- [Decisions](docs/Decisions.md)
- [Pipelines](docs/Pipelines.md)
- [Sources](docs/Sources.md)
- [Dashboard](docs/Dashboard.md)
- [Reliability](docs/Reliability.md)
- [Metrics](docs/Metrics.md)
- [Data Dictionaries](docs/data-dictionaries/README.md)

## Architecture

This project uses a medallion architecture with the following specs:

* Bronze - raw API data stored in partitioned parquet files
* Silver - Cleaned and normalized datasets stored in partitioned parquet files with views built on top in DuckDB.
* Gold - Aggregated factors and analytics stored in DuckDB

## Data Sources

* Alpaca API - For stock data and to build the assets table.
* Wikimedia REST API - Ingest page views data to track attention trends.
* Yahoo News API - Ingest headlines.
* Tranco API - Publisher rankings for sentiment scores.

## Dashboards

The streamlit app is built on top of the gold layer and allows users to analyze general market trends as well as the specific performance of individual stocks.

### Market Vibecheck

This page contains an overview of market trends for the day. This includes:

* Top gainers/losers
* High and low volatility assets
* Assets exhibiting strong momentum with low prices relative to the stock's yearly averages.

### Deep Dive

This page allows you to select any ticker in the dataset and see performance data for that stock. This page is accessible as a tab on the sidebar but it's also accessible by clicking on any of the assets on the Market Vibecheck page, allowing users to click through to any stocks that are of interest to the user.

Some of the things you can see here:

* Performance history with a toggleable time horizon
* A risk analysis for the stock including volatility, volume and drawdown charts
* Recent headlines with sentiment analysis

### Observability

The platform includes a built-in observability layer for monitoring pipeline health, data freshness, and data quality metrics.

Quality checks are written during ingestion and surfaced in the `observability` schema for dashboard consumption.

Here you can see:

* Recent runs, failures and any issues relating to data quality and freshness checks.
* A list of all recent runs from the logging table.
* Job duration trends for spotting latency and runtime issues.

## Getting Started

To get started, clone this repository and run this once from the project root:

```bash
uv run portfolio-bootstrap
```

What this does:

* Creates the local `data/` directories and DuckDB file.
* Runs a sample Dagster seed pipeline (`seed_demo_data`).
* Ensures `data/demo/` exists.
* Resolves demo zip source (GitHub release, local file, or direct URL).
* Copies or downloads `demo_data.zip` into `data/demo/demo_data.zip`.
* Writes `data/demo/demo_data.zip.sha256`.
* Writes a source marker file `data/demo/.demo_data_source.json`.
* Extracts the zip into `data/` so downstream pipelines can run normally on local data.

Once that's finished running, create a virtual environment

```bash
.venv/Scripts/activate.ps1
```

Then run the streamlit app and you'll see sample data.

```bash
streamlit run streamlit_app.py
```

For environment variables and defaults, use:
- [`docs/.env.sample`](docs/.env.sample)

Be sure to add your Alpaca API key and Secret Key to the .env file so that the pipelines can run.

For reliability behavior (including DuckDB locking, freshness, and data quality checks), see:
- [`docs/Reliability.md`](docs/Reliability.md)

