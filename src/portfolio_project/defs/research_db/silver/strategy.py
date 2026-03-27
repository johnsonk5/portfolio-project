from dagster import AssetExecutionContext, asset


STRATEGY_DEFINITIONS_COLUMNS: list[tuple[str, str]] = [
    ("strategy_id", "VARCHAR"),
    ("strategy_name", "VARCHAR"),
    ("strategy_version", "VARCHAR"),
    ("description", "VARCHAR"),
    ("ranking_method", "VARCHAR"),
    ("rebalance_frequency", "VARCHAR"),
    ("target_count", "INTEGER"),
    ("weighting_method", "VARCHAR"),
    ("benchmark_symbol", "VARCHAR"),
    ("long_short_flag", "BOOLEAN"),
    ("start_date", "DATE"),
    ("end_date", "DATE"),
    ("is_active", "BOOLEAN"),
    ("config_json", "VARCHAR"),
    ("asof_ts", "TIMESTAMP"),
    ("run_id", "VARCHAR"),
]

STRATEGY_RUNS_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("run_status", "VARCHAR"),
    ("dataset_version", "VARCHAR"),
    ("code_version", "VARCHAR"),
    ("started_at", "TIMESTAMP"),
    ("completed_at", "TIMESTAMP"),
    ("error_message", "VARCHAR"),
    ("persist", "BOOLEAN"),
    ("asof_ts", "TIMESTAMP"),
]

STRATEGY_PARAMETERS_COLUMNS: list[tuple[str, str]] = [
    ("strategy_id", "VARCHAR"),
    ("parameter_name", "VARCHAR"),
    ("parameter_value", "VARCHAR"),
    ("parameter_type", "VARCHAR"),
    ("effective_start_date", "DATE"),
    ("effective_end_date", "DATE"),
    ("is_active", "BOOLEAN"),
    ("description", "VARCHAR"),
    ("ingest_ts", "TIMESTAMP"),
    ("asof_ts", "TIMESTAMP"),
    ("run_id", "VARCHAR"),
]


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _ensure_table_contract(
    con,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
) -> None:
    table_ref = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
    column_sql = ",\n            ".join(
        f"{_quote_identifier(name)} {type_sql}" for name, type_sql in columns
    )

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            {column_sql}
        )
        """
    )
    for column_name, type_sql in columns:
        con.execute(
            f"""
            ALTER TABLE {table_ref}
            ADD COLUMN IF NOT EXISTS {_quote_identifier(column_name)} {type_sql}
            """
        )


def _table_metadata(con, *, schema: str, table: str) -> dict[str, int]:
    row_count = con.execute(
        f"SELECT count(*) FROM {_quote_identifier(schema)}.{_quote_identifier(table)}"
    ).fetchone()[0]
    column_count = con.execute(
        """
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    return {
        "row_count": int(row_count or 0),
        "column_count": int(column_count or 0),
    }


@asset(
    name="strategy_definitions",
    key_prefix=["silver"],
    required_resource_keys={"research_duckdb"},
)
def silver_strategy_definitions(context: AssetExecutionContext) -> None:
    """
    Ensure the silver.strategy_definitions contract table exists in the research database.
    """
    con = context.resources.research_duckdb
    _ensure_table_contract(
        con,
        schema="silver",
        table="strategy_definitions",
        columns=STRATEGY_DEFINITIONS_COLUMNS,
    )
    table_metadata = _table_metadata(
        con,
        schema="silver",
        table="strategy_definitions",
    )
    context.add_output_metadata(
        {"table": "silver.strategy_definitions", **table_metadata}
    )


@asset(
    name="strategy_runs",
    key_prefix=["silver"],
    deps=[silver_strategy_definitions],
    required_resource_keys={"research_duckdb"},
)
def silver_strategy_runs(context: AssetExecutionContext) -> None:
    """
    Ensure the silver.strategy_runs contract table exists in the research database.
    """
    con = context.resources.research_duckdb
    _ensure_table_contract(
        con,
        schema="silver",
        table="strategy_runs",
        columns=STRATEGY_RUNS_COLUMNS,
    )
    table_metadata = _table_metadata(
        con,
        schema="silver",
        table="strategy_runs",
    )
    context.add_output_metadata(
        {"table": "silver.strategy_runs", **table_metadata}
    )


@asset(
    name="strategy_parameters",
    key_prefix=["silver"],
    deps=[silver_strategy_definitions],
    required_resource_keys={"research_duckdb"},
)
def silver_strategy_parameters(context: AssetExecutionContext) -> None:
    """
    Ensure the silver.strategy_parameters contract table exists in the research database.
    """
    con = context.resources.research_duckdb
    _ensure_table_contract(
        con,
        schema="silver",
        table="strategy_parameters",
        columns=STRATEGY_PARAMETERS_COLUMNS,
    )
    table_metadata = _table_metadata(
        con,
        schema="silver",
        table="strategy_parameters",
    )
    context.add_output_metadata(
        {"table": "silver.strategy_parameters", **table_metadata}
    )
