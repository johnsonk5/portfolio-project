import os
from pathlib import Path

from datetime import datetime, timedelta

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.bronze_assets import bronze_alpaca_bars
from portfolio_project.defs.silver_assets import silver_alpaca_assets


DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
SILVER_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)


@asset(
    name="silver_alpaca_prices_parquet",
    deps=[bronze_alpaca_bars, silver_alpaca_assets],
    partitions_def=SILVER_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def silver_alpaca_prices_parquet(context: AssetExecutionContext) -> None:
    """
    Build a silver-layer parquet partition normalized on asset_id.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    con = context.resources.duckdb
    try:
        con.execute("SELECT 1 FROM silver.assets LIMIT 1")
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
        return

    active_symbols = [
        row[0]
        for row in con.execute(
            "SELECT symbol FROM silver.assets WHERE is_active = TRUE"
        ).fetchall()
        if row and row[0]
    ]
    if not active_symbols:
        context.log.warning("No active symbols found in silver.assets.")
        return

    month_key = partition_date.strftime("%Y-%m")
    bronze_root = DATA_ROOT / "bronze" / "alpaca_bars" / f"month={month_key}"
    parquet_paths = [
        (bronze_root / f"symbol={symbol.upper()}.parquet").as_posix()
        for symbol in active_symbols
    ]
    parquet_paths = [path for path in parquet_paths if Path(path).exists()]
    if not parquet_paths:
        context.log.warning("No bronze bars parquet files found at %s", bronze_root)
        return

    start_date = pd.Timestamp(partition_date, tz="UTC")
    end_date = pd.Timestamp(partition_date + timedelta(days=1), tz="UTC")
    start_dt = start_date.to_pydatetime()
    end_dt = end_date.to_pydatetime()

    # Filter and join in DuckDB so we do not load an entire month into pandas.
    prices_df = con.execute(
        """
        WITH bars AS (
            SELECT *
            FROM read_parquet(?)
            WHERE timestamp >= ? AND timestamp < ?
        ),
        active_assets AS (
            SELECT asset_id, symbol
            FROM silver.assets
            WHERE is_active = TRUE
        )
        SELECT
            assets.asset_id,
            bars.symbol,
            bars.timestamp,
            bars.open,
            bars.high,
            bars.low,
            bars.close,
            bars.volume,
            bars.trade_count,
            bars.vwap,
            bars.ingested_ts
        FROM bars
        INNER JOIN active_assets AS assets
            ON bars.symbol = assets.symbol
        """,
        [parquet_paths, start_dt, end_dt],
    ).fetch_df()
    if prices_df is None or prices_df.empty:
        context.log.warning("No active bar data for partition %s.", context.partition_key)
        return

    column_order = [
        "asset_id",
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "ingested_ts",
    ]
    existing_cols = [col for col in column_order if col in prices_df.columns]
    remaining_cols = [col for col in prices_df.columns if col not in existing_cols]
    prices_df = prices_df[existing_cols + remaining_cols]

    silver_root = DATA_ROOT / "silver" / "prices" / f"date={context.partition_key}"
    silver_root.mkdir(parents=True, exist_ok=True)
    parquet_path = silver_root / "prices.parquet"
    prices_df.to_parquet(parquet_path, index=False)

    context.add_output_metadata(
        {
            "row_count": len(prices_df),
            "partition": context.partition_key,
            "parquet_path": str(parquet_path),
        }
    )


@asset(
    name="silver_alpaca_prices",
    deps=[silver_alpaca_prices_parquet],
    partitions_def=SILVER_PARTITIONS,
    required_resource_keys={"duckdb"},
)
def silver_alpaca_prices(context: AssetExecutionContext) -> None:
    """
    Load the silver-layer parquet partition into DuckDB.
    """
    parquet_path = (
        DATA_ROOT
        / "silver"
        / "prices"
        / f"date={context.partition_key}"
        / "prices.parquet"
    )
    if not parquet_path.exists():
        context.log.warning("Silver parquet partition missing at %s", parquet_path)
        return

    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    start_date = pd.Timestamp(partition_date, tz="UTC")

    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    parquet_cols_df = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
    ).fetch_df()
    parquet_cols = parquet_cols_df["column_name"].tolist()

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS silver.prices AS
        SELECT *
        FROM read_parquet('{parquet_path.as_posix()}')
        LIMIT 0
        """
    )
    table_cols_df = con.execute(
        "PRAGMA table_info('silver.prices')"
    ).fetch_df()
    table_cols = table_cols_df["name"].tolist()
    con.execute(
        """
        DELETE FROM silver.prices
        WHERE CAST(timestamp AS DATE) = CAST(? AS DATE)
        """,
        [start_date],
    )
    select_exprs = [
        col if col in parquet_cols else f"NULL AS {col}" for col in table_cols
    ]
    select_list = ", ".join(select_exprs)
    con.execute(
        f"""
        INSERT INTO silver.prices ({", ".join(table_cols)})
        SELECT {select_list}
        FROM read_parquet('{parquet_path.as_posix()}')
        """
    )

    context.add_output_metadata(
        {
            "table": "silver.prices",
            "partition": context.partition_key,
            "parquet_path": str(parquet_path),
        }
    )
