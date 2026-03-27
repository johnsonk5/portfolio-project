import duckdb
from dagster import build_asset_context

from portfolio_project.defs.research_db.silver.strategy import (
    STRATEGY_DEFINITIONS_COLUMNS,
    STRATEGY_PARAMETERS_COLUMNS,
    STRATEGY_RUNS_COLUMNS,
    silver_strategy_definitions,
    silver_strategy_parameters,
    silver_strategy_runs,
)


def _describe_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE silver.{table_name}").fetchall()
    return [(str(row[0]), str(row[1]).upper()) for row in rows]


def test_strategy_tables_assets_create_contract_tables_and_preserve_rows() -> None:
    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    silver_strategy_definitions(context)
    silver_strategy_runs(context)
    silver_strategy_parameters(context)

    assert _describe_columns(con, "strategy_definitions") == STRATEGY_DEFINITIONS_COLUMNS
    assert _describe_columns(con, "strategy_runs") == STRATEGY_RUNS_COLUMNS
    assert _describe_columns(con, "strategy_parameters") == STRATEGY_PARAMETERS_COLUMNS

    con.execute(
        """
        INSERT INTO silver.strategy_definitions (
            strategy_id,
            strategy_name,
            strategy_version,
            ranking_method,
            rebalance_frequency,
            target_count,
            weighting_method,
            long_short_flag,
            is_active
        )
        VALUES (
            'momentum_top_10',
            'Momentum Top 10',
            'v1',
            'momentum_12_1',
            'Monthly',
            10,
            'Equal',
            false,
            true
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.strategy_runs (
            run_id,
            strategy_id,
            run_status,
            dataset_version,
            persist
        )
        VALUES ('run-001', 'momentum_top_10', 'success', 'research_snapshot_2026-03-27', true)
        """
    )
    con.execute(
        """
        INSERT INTO silver.strategy_parameters (
            strategy_id,
            parameter_name,
            parameter_value,
            parameter_type,
            effective_start_date,
            is_active
        )
        VALUES ('momentum_top_10', 'target_count', '10', 'int', DATE '2026-01-01', true)
        """
    )

    silver_strategy_definitions(context)
    silver_strategy_runs(context)
    silver_strategy_parameters(context)

    definitions_count = con.execute("SELECT count(*) FROM silver.strategy_definitions").fetchone()
    runs_count = con.execute("SELECT count(*) FROM silver.strategy_runs").fetchone()
    parameters_count = con.execute("SELECT count(*) FROM silver.strategy_parameters").fetchone()

    assert definitions_count is not None
    assert runs_count is not None
    assert parameters_count is not None
    assert definitions_count[0] == 1
    assert runs_count[0] == 1
    assert parameters_count[0] == 1
