import os
from datetime import datetime
from pathlib import Path

from dagster import AssetExecutionContext, MonthlyPartitionsDefinition, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.silver_prices import PARTITIONS_START_DATE, silver_alpaca_prices_parquet


DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
SILVER_COMPACT_PARTITIONS = MonthlyPartitionsDefinition(start_date=PARTITIONS_START_DATE)


@asset(
    name="silver_alpaca_prices_compact",
    deps=[silver_alpaca_assets, silver_alpaca_prices_parquet],
    partitions_def=SILVER_COMPACT_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def silver_alpaca_prices_compact(context: AssetExecutionContext) -> None:
    """
    Compact daily silver prices into symbol/year/month parquet files.
    """
    month_start = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    year_key = month_start.strftime("%Y")
    month_key = month_start.strftime("%m")

    day_glob = (
        DATA_ROOT
        / "silver"
        / "prices"
        / "date=*"
        / "symbol=*"
        / "prices.parquet"
    ).as_posix()

    con = context.resources.duckdb
    prices_df = con.execute(
        """
        SELECT *
        FROM read_parquet(?, hive_partitioning = true)
        WHERE date_trunc('month', CAST(date AS DATE)) = ?
        """,
        [day_glob, month_start],
    ).fetch_df()

    if prices_df is None or prices_df.empty:
        context.log.warning("No silver prices rows found for month %s", context.partition_key)
        return

    files_written = 0
    rows_written = 0
    for symbol, symbol_df in prices_df.groupby("symbol", sort=True):
        symbol_key = str(symbol).upper()
        out_path = (
            DATA_ROOT
            / "silver"
            / "prices_compact"
            / f"symbol={symbol_key}"
            / f"year={year_key}"
            / f"month={month_key}"
            / "prices.parquet"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        symbol_df.to_parquet(out_path, index=False)
        files_written += 1
        rows_written += len(symbol_df)

    context.add_output_metadata(
        {
            "partition_month_start": context.partition_key,
            "files_written": files_written,
            "row_count": rows_written,
        }
    )
