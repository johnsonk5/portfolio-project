import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dagster import AssetExecutionContext, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def _load_sp500_table() -> pd.DataFrame:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }
    response = requests.get(SP500_WIKIPEDIA_URL, headers=headers, timeout=30)
    response.raise_for_status()
    tables = pd.read_html(response.text)
    for table in tables:
        cols = {c.strip() for c in table.columns.astype(str)}
        if "Symbol" in cols and "Security" in cols:
            return table
    raise ValueError("Unable to locate S&P 500 table on Wikipedia.")


@asset(
    name="bronze_sp500_companies",
)
def bronze_sp500_companies(context: AssetExecutionContext) -> None:
    """
    Ingest S&P 500 constituents from Wikipedia into bronze parquet.
    """
    df = _load_sp500_table()
    df["ingested_ts"] = datetime.now(timezone.utc)
    df["source_url"] = SP500_WIKIPEDIA_URL

    reference_dir = DATA_ROOT / "bronze" / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    out_path = reference_dir / "sp500_companies.parquet"
    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df)}
    )


@asset(
    name="silver_sp500_companies",
    deps=[bronze_sp500_companies, silver_alpaca_assets],
    required_resource_keys={"duckdb"},
)
def silver_sp500_companies(context: AssetExecutionContext) -> None:
    """
    Normalize S&P 500 constituents into silver.ref_sp500 and update silver.assets.is_sp500.
    """
    bronze_path = DATA_ROOT / "bronze" / "reference" / "sp500_companies.parquet"
    if not bronze_path.exists():
        context.log.warning("Bronze S&P 500 parquet not found at %s", bronze_path)
        return

    df = pd.read_parquet(bronze_path)
    if df.empty:
        context.log.warning("Bronze S&P 500 parquet is empty at %s", bronze_path)
        return

    rename_map = {
        "Symbol": "symbol",
        "Security": "security",
        "GICS Sector": "gics_sector",
        "GICS Sub-Industry": "gics_sub_industry",
        "Headquarters Location": "headquarters_location",
        "Date first added": "date_first_added",
        "CIK": "cik",
        "Founded": "founded",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if "symbol" not in df.columns:
        context.log.warning("No symbol column found in S&P 500 data.")
        return

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()

    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    try:
        assets_df = con.execute(
            "SELECT asset_id, symbol FROM silver.assets"
        ).fetch_df()
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
        return

    assets_df["symbol_norm"] = assets_df["symbol"].astype(str).str.upper()
    df["symbol_norm"] = df["symbol"].astype(str).str.upper()
    df = df.merge(
        assets_df[["asset_id", "symbol_norm"]],
        on="symbol_norm",
        how="left",
    )
    df = df.drop(columns=["symbol_norm"])

    con.register("sp500_df", df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.ref_sp500 AS
        SELECT *
        FROM sp500_df
        """
    )

    con.execute("ALTER TABLE silver.assets ADD COLUMN IF NOT EXISTS is_sp500 BOOLEAN")
    con.execute("UPDATE silver.assets SET is_sp500 = FALSE WHERE is_sp500 IS NULL")
    con.execute(
        """
        UPDATE silver.assets AS a
        SET is_sp500 = TRUE
        FROM silver.ref_sp500 AS s
        WHERE a.asset_id = s.asset_id
        """
    )

    missing_assets = df["asset_id"].isna().sum()
    context.add_output_metadata(
        {
            "table": "silver.ref_sp500",
            "row_count": len(df),
            "missing_asset_id_count": int(missing_assets),
        }
    )
