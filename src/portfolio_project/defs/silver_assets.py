import os
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset

from portfolio_project.defs.bronze_assets import bronze_alpaca_assets

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
TICKERS_ENV = "ALPACA_TICKERS"


@asset(
    name="silver_alpaca_assets",
    deps=[bronze_alpaca_assets],
    required_resource_keys={"duckdb"},
)
def silver_alpaca_assets(context: AssetExecutionContext) -> None:
    """
    Build a silver-layer assets table with a surrogate identity key.
    """
    bronze_path = DATA_ROOT / "bronze" / "reference" / "alpaca_assets.parquet"
    if not bronze_path.exists():
        context.log.warning("Bronze assets parquet not found at %s", bronze_path)
        return

    df = pd.read_parquet(bronze_path)
    if df.empty:
        context.log.warning("Bronze assets parquet is empty at %s", bronze_path)
        return

    rename_map = {}
    if "id" in df.columns:
        rename_map["id"] = "alpaca_id"
    if "status" in df.columns:
        rename_map["status"] = "alpaca_status"
    if rename_map:
        df = df.rename(columns=rename_map)

    tickers = {
        s.strip() for s in os.getenv(TICKERS_ENV, "").split(",") if s.strip()
    }
    if "symbol" in df.columns:
        df["is_active"] = df["symbol"].isin(tickers)
    else:
        df["is_active"] = False
        context.log.warning("No symbol column found; is_active set to False.")

    order_cols = []
    if "symbol" in df.columns:
        order_cols.append('"symbol"')
    if "alpaca_id" in df.columns:
        order_cols.append('"alpaca_id"')
    order_sql = ", ".join(order_cols) if order_cols else "1"

    con = context.resources.duckdb
    con.register("silver_assets_df", df)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("DROP TABLE IF EXISTS silver.assets")
    con.execute(
        f"""
        CREATE TABLE silver.assets AS
        SELECT
            row_number() OVER (ORDER BY {order_sql}) AS asset_id,
            *
        FROM silver_assets_df
        """
    )

    context.add_output_metadata(
        {"table": "silver.assets", "row_count": len(df)}
    )