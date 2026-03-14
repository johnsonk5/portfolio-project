# AGENTS.md

## Project Summary

This repository is a market research platform built around:

- Dagster for orchestration, jobs, schedules, and asset materialization
- DuckDB for gold-layer analytics and observability tables
- Partitioned parquet datasets for bronze and silver storage
- Streamlit for the user-facing dashboard

The project follows a medallion architecture:

- `data/bronze`: raw source-like snapshots
- `data/silver`: normalized parquet datasets partitioned by date and often symbol
- `data/duckdb/portfolio.duckdb`: DuckDB database for gold and observability layers

Primary source code lives under `src/portfolio_project`.

## Repo Map

- `src/portfolio_project/definitions.py`: Dagster `Definitions`, jobs, and schedules
- `src/portfolio_project/defs/`: assets, resources, observability hooks, and data quality logic
- `streamlit_app.py`: main Market Vibecheck dashboard
- `pages/`: Streamlit subpages including Deep Dive and Observability
- `queries/`: SQL used by analytics and dashboard views
- `tests/`: pytest coverage for schedules, bootstrap, locking, and pipeline behavior
- `docs/`: architecture, pipelines, reliability, dashboard, and data dictionaries
- `demo/`: demo zip seed asset
- `data/`: local runtime data root created by bootstrap or tests

## Setup And Common Commands

Use `uv` and keep commands rooted at the repository root.

Bootstrap local demo data:

```powershell
uv run portfolio-bootstrap
```

Run the app:

```powershell
uv run streamlit run streamlit_app.py
```

Run all tests:

```powershell
uv run pytest
```

Run a targeted test file:

```powershell
uv run pytest tests/test_definitions_schedule.py
```

Run lint:

```powershell
uv run ruff check .
```

Run type checks for the currently configured scope:

```powershell
uv run mypy
```

## Environment And Data Assumptions

- Python version target is `>=3.10,<3.14`.
- Default data root is `data/`, controlled by `PORTFOLIO_DATA_DIR`.
- Default DuckDB path is `data/duckdb/portfolio.duckdb`, overridable with `PORTFOLIO_DUCKDB_PATH`.
- Environment variable examples live in `docs/.env.sample`.
- Alpaca credentials are required for live market ingestion.
- Demo and local development flows rely on `portfolio-bootstrap` and `seed_demo_data`.

Prefer environment-driven paths over hardcoded paths. Existing modules often resolve paths from `os.getenv(...)` at import time.

## Architecture And Coding Conventions

### Dagster

- Keep Dagster assets and jobs defined in `src/portfolio_project/defs/` and registered through `src/portfolio_project/definitions.py`.
- Preserve current schedule semantics. Trading-day-aware schedules use America/New_York and often target the previous US trading day.
- When adding or changing schedules, add or update tests in `tests/test_definitions_schedule.py`.
- Success and failure observability hooks are part of the current reliability model. Avoid bypassing them for production jobs.

### Data Layer

- Bronze and silver outputs are filesystem datasets; gold and observability data live in DuckDB.
- Silver prices are partitioned by both `date=YYYY-MM-DD` and `symbol=XYZ`; several tests depend on this structure.
- Prefer idempotent writes. Current patterns clear and rewrite the target day partition before emitting fresh parquet files.
- Preserve schema stability where possible because dashboard queries and tests assume current column sets.

### DuckDB

- Use the existing `duckdb_resource` instead of opening ad hoc write connections inside Dagster assets.
- The repository already enforces operation-scoped filesystem locking via `.duckdb_write.lock`. Preserve that behavior when touching resource code.
- If you change locking, connection lifecycle, or write semantics, run `tests/test_duckdb_resource_locking.py`.

### Streamlit

- The current UI uses a deliberate visual style built around `Space Grotesk`, layered gradients, and custom cards. Preserve that look unless the task is explicitly a redesign.
- Sidebar page links are hand-managed in each page.
- Prefer keeping data access logic small and explicit in the app layer; heavier transformation logic belongs in data assets or SQL.

## Testing Expectations

At minimum, run the narrowest relevant checks after changes:

- Schedule or partition logic: `uv run pytest tests/test_definitions_schedule.py`
- Bootstrap/demo changes: `uv run pytest tests/test_bootstrap.py tests/test_demo_seed_assets.py`
- DuckDB resource or locking changes: `uv run pytest tests/test_duckdb_resource_locking.py`
- Silver/gold price pipeline changes: `uv run pytest tests/test_smoke_pipeline.py`
- General code quality changes: `uv run ruff check .` and `uv run mypy` when touching typed files

If a change affects Dagster assets, also consider whether it impacts:

- observability rows and run metadata
- partition directory layout
- Streamlit queries against DuckDB

## Practical Guidance For Agents

- Read `README.md` and the relevant file in `docs/` before large changes.
- Prefer small, local changes that match existing patterns over introducing new frameworks or abstractions.
- Do not commit generated runtime data under `data/`, cache directories, or local lock files.
- Be careful with module-level globals such as `DATA_ROOT`; tests sometimes monkeypatch these directly.
- Use targeted pytest coverage to verify behavior before broadening to the full suite.
- When changing schemas, also update docs, tests, and any dashboard/query code that depends on them.

## Good First Places To Look

- New pipeline behavior: `src/portfolio_project/definitions.py` and `src/portfolio_project/defs/`
- Dashboard behavior: `streamlit_app.py`, `pages/Deep_Dive.py`, `pages/Observability.py`
- Reliability expectations: `docs/Reliability.md`
- Data flow and storage: `docs/Architecture.md` and `docs/Pipelines.md`
