import os
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError

from portfolio_project.defs.research_db.bronze.research_prices import (
    bronze_alpaca_corporate_actions_daily,
)
from portfolio_project.defs.research_db.dq_checks import log_required_field_null_check

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


@asset(
    name="alpaca_corporate_actions",
    key_prefix=["silver"],
    deps=[bronze_alpaca_corporate_actions_daily],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def silver_alpaca_corporate_actions(context: AssetExecutionContext) -> None:
    """
    Build a research silver table of Alpaca split corporate actions keyed by effective date.
    """
    con = context.resources.research_duckdb
    actions_glob = (
        DATA_ROOT / "bronze" / "alpaca_corporate_actions" / "actions.parquet"
    ).as_posix()

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.alpaca_corporate_actions AS
        SELECT
            CAST(action_id AS VARCHAR) AS action_id,
            upper(trim(symbol)) AS symbol,
            CAST(action_type AS VARCHAR) AS action_type,
            CAST(effective_date AS DATE) AS effective_date,
            CAST(process_date AS DATE) AS process_date,
            CAST(old_rate AS DOUBLE) AS old_rate,
            CAST(new_rate AS DOUBLE) AS new_rate,
            CAST(cash_rate AS DOUBLE) AS cash_rate,
            CAST(split_ratio AS DOUBLE) AS split_ratio,
            CAST(source AS VARCHAR) AS source,
            CAST(ingested_ts AS TIMESTAMP) AS ingested_ts
        FROM read_parquet(?)
        WHERE symbol IS NOT NULL
          AND trim(symbol) <> ''
          AND effective_date IS NOT NULL
          AND (
                (
                    action_type IN ('forward_splits', 'reverse_splits')
                    AND old_rate IS NOT NULL
                    AND new_rate IS NOT NULL
                    AND old_rate > 0
                    AND new_rate > 0
                )
                OR (
                    action_type = 'cash_dividends'
                    AND cash_rate IS NOT NULL
                )
          )
        ORDER BY effective_date, symbol, action_type, process_date
        """,
        [actions_glob],
    )

    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_alpaca_corporate_actions_required_fields_nulls",
        relation_sql="SELECT * FROM silver.alpaca_corporate_actions",
        relation_params=[],
        required_columns=["symbol", "effective_date", "action_type", "source", "ingested_ts"],
        details={"table": "silver.alpaca_corporate_actions"},
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=getattr(context, "partition_key", None),
    )
    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_alpaca_corporate_actions_split_rate_nulls",
        relation_sql="""
            SELECT *
            FROM silver.alpaca_corporate_actions
            WHERE action_type IN ('forward_splits', 'reverse_splits')
        """,
        relation_params=[],
        required_columns=["old_rate", "new_rate"],
        details={
            "table": "silver.alpaca_corporate_actions",
            "rule": {
                "old_rate": "required when action_type in ('forward_splits', 'reverse_splits')",
                "new_rate": "required when action_type in ('forward_splits', 'reverse_splits')",
            },
        },
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=getattr(context, "partition_key", None),
    )
    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_alpaca_corporate_actions_cash_rate_nulls",
        relation_sql="""
            SELECT *
            FROM silver.alpaca_corporate_actions
            WHERE action_type = 'cash_dividends'
        """,
        relation_params=[],
        required_columns=["cash_rate"],
        details={
            "table": "silver.alpaca_corporate_actions",
            "rule": {"cash_rate": "required when action_type = 'cash_dividends'"},
        },
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=getattr(context, "partition_key", None),
    )

    row_count = con.execute("SELECT count(*) FROM silver.alpaca_corporate_actions").fetchone()[0]
    symbol_count = con.execute(
        "SELECT count(DISTINCT symbol) FROM silver.alpaca_corporate_actions"
    ).fetchone()[0]
    min_max_row = con.execute(
        "SELECT min(effective_date), max(effective_date) FROM silver.alpaca_corporate_actions"
    ).fetchone()
    context.add_output_metadata(
        {
            "table": "silver.alpaca_corporate_actions",
            "row_count": int(row_count or 0),
            "symbol_count": int(symbol_count or 0),
            "min_effective_date": str(min_max_row[0]) if min_max_row and min_max_row[0] else None,
            "max_effective_date": str(min_max_row[1]) if min_max_row and min_max_row[1] else None,
        }
    )
