import pandas as pd
from dagster import AssetExecutionContext, asset

from portfolio_project.defs.research_db.silver.strategy import (
    _replace_table_from_df,
    _table_metadata,
)
from portfolio_project.defs.research_db.trading_calendar import INVALID_TRADING_DAY_RECORDS

INVALID_TRADING_DAYS_COLUMNS: list[tuple[str, str]] = [
    ("invalid_date", "DATE"),
    ("reason_code", "VARCHAR"),
    ("description", "VARCHAR"),
]


@asset(
    name="invalid_trading_days",
    key_prefix=["ref"],
    required_resource_keys={"research_duckdb"},
)
def ref_invalid_trading_days(context: AssetExecutionContext) -> None:
    """
    Build ref.invalid_trading_days from repo-managed special closures and data exclusions.
    """
    con = context.resources.research_duckdb
    invalid_days_df = pd.DataFrame(
        INVALID_TRADING_DAY_RECORDS,
        columns=[column_name for column_name, _ in INVALID_TRADING_DAYS_COLUMNS],
    )
    invalid_days_df["invalid_date"] = pd.to_datetime(
        invalid_days_df["invalid_date"],
        errors="raise",
    ).dt.date

    _replace_table_from_df(
        con,
        schema="ref",
        table="invalid_trading_days",
        columns=INVALID_TRADING_DAYS_COLUMNS,
        df=invalid_days_df,
    )

    context.add_output_metadata(
        {
            "table": "ref.invalid_trading_days",
            "invalid_day_count": len(invalid_days_df),
            **_table_metadata(con, schema="ref", table="invalid_trading_days"),
        }
    )
