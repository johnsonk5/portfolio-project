import os
from pathlib import Path

import pandas as pd
from dagster import Array, AssetExecutionContext, Field, String, asset

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

    tickers = _normalize_symbols(os.getenv(TICKERS_ENV, "").split(","))
    if "symbol" in df.columns:
        df["is_active"] = df["symbol"].isin(tickers)
    else:
        df["is_active"] = False
        context.log.warning("No symbol column found; is_active set to False.")

    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    existing_map_df = None
    try:
        existing_map_df = con.execute(
            "SELECT asset_id, alpaca_id FROM silver.assets"
        ).fetch_df()
    except Exception:
        existing_map_df = None

    if "alpaca_id" in df.columns:
        if existing_map_df is not None and not existing_map_df.empty:
            existing_map_df["alpaca_id"] = existing_map_df["alpaca_id"].astype(str)
            df["alpaca_id"] = df["alpaca_id"].astype(str)
            df = df.merge(existing_map_df, on="alpaca_id", how="left")
            max_id = int(existing_map_df["asset_id"].max())
        else:
            df["asset_id"] = pd.NA
            max_id = 0

        new_mask = df["asset_id"].isna()
        if new_mask.any():
            order_cols = [c for c in ["symbol", "alpaca_id"] if c in df.columns]
            if order_cols:
                df = df.sort_values(order_cols, kind="stable")
                new_mask = df["asset_id"].isna()
            new_ids = range(max_id + 1, max_id + 1 + int(new_mask.sum()))
            df.loc[new_mask, "asset_id"] = list(new_ids)

        df["asset_id"] = df["asset_id"].astype("int64")
    else:
        context.log.warning(
            "No alpaca_id column found; asset_id stability cannot be guaranteed."
        )
        df["asset_id"] = range(1, len(df) + 1)

    df = df[["asset_id"] + [c for c in df.columns if c != "asset_id"]]

    con.register("silver_assets_df", df)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.assets AS
        SELECT *
        FROM silver_assets_df
        """
    )

    context.add_output_metadata(
        {"table": "silver.assets", "row_count": len(df)}
    )


def _normalize_symbols(symbols) -> set[str]:
    return {s.strip().upper() for s in symbols or [] if s and s.strip()}


@asset(
    name="silver_alpaca_active_assets_history",
    deps=[silver_alpaca_assets],
    required_resource_keys={"duckdb"},
)
def silver_alpaca_active_assets_history(context: AssetExecutionContext) -> None:
    """
    Ensure the active assets history table exists.
    """
    con = context.resources.duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.active_assets_history (
            asset_id BIGINT,
            alpaca_id VARCHAR,
            symbol VARCHAR,
            change_type VARCHAR,
            change_date DATE,
            change_ts TIMESTAMP,
            previous_is_active BOOLEAN,
            new_is_active BOOLEAN
        )
        """
    )

    row_count = con.execute(
        "SELECT count(*) FROM silver.active_assets_history"
    ).fetchone()[0]
    context.add_output_metadata(
        {"table": "silver.active_assets_history", "row_count": row_count}
    )


@asset(
    name="silver_alpaca_assets_status_updates",
    deps=[silver_alpaca_assets],
    required_resource_keys={"duckdb"},
    config_schema={
        "symbols_activate": Field(Array(String), is_required=False),
        "symbols_deactivate": Field(Array(String), is_required=False),
    },
)
def silver_alpaca_assets_status_updates(context: AssetExecutionContext) -> None:
    """
    Update silver.assets.is_active for selected symbols and append change history.
    """
    symbols_activate = _normalize_symbols(context.op_config.get("symbols_activate"))
    symbols_deactivate = _normalize_symbols(context.op_config.get("symbols_deactivate"))

    overlap = symbols_activate & symbols_deactivate
    if overlap:
        context.log.warning(
            "Symbols present in both activate and deactivate lists: %s. Deactivate wins.",
            sorted(overlap),
        )
        symbols_activate -= overlap

    desired_updates = {symbol: True for symbol in symbols_activate}
    desired_updates.update({symbol: False for symbol in symbols_deactivate})
    if not desired_updates:
        context.log.info("No status updates requested.")
        context.add_output_metadata(
            {"updated_count": 0, "history_appended_count": 0}
        )
        return

    con = context.resources.duckdb
    try:
        assets_df = con.execute(
            """
            SELECT asset_id, alpaca_id, symbol, is_active
            FROM silver.assets
            """
        ).fetch_df()
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
        return

    if assets_df is None or assets_df.empty:
        context.log.warning("Silver assets table is empty; no updates applied.")
        return

    assets_df["symbol_norm"] = assets_df["symbol"].astype(str).str.upper()
    assets_df["desired_is_active"] = assets_df["symbol_norm"].map(desired_updates)
    updates_df = assets_df[assets_df["desired_is_active"].notna()].copy()
    missing_symbols = sorted(set(desired_updates) - set(updates_df["symbol_norm"]))
    if missing_symbols:
        context.log.warning("Requested symbols not found in silver.assets: %s", missing_symbols)
    if updates_df.empty:
        context.log.info("No matching symbols found in silver.assets.")
        context.add_output_metadata(
            {"updated_count": 0, "history_appended_count": 0, "missing_symbols": missing_symbols}
        )
        return

    updates_df["previous_is_active"] = updates_df["is_active"].astype(bool)
    updates_df["new_is_active"] = updates_df["desired_is_active"].astype(bool)
    updates_df = updates_df[updates_df["previous_is_active"] != updates_df["new_is_active"]]
    if updates_df.empty:
        context.log.info("All requested symbols already have the desired status.")
        context.add_output_metadata(
            {"updated_count": 0, "history_appended_count": 0, "missing_symbols": missing_symbols}
        )
        return

    history_updates_df = updates_df[
        ["asset_id", "alpaca_id", "symbol", "previous_is_active", "new_is_active"]
    ].copy()
    con.register("asset_status_updates_df", history_updates_df)

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.active_assets_history (
            asset_id BIGINT,
            alpaca_id VARCHAR,
            symbol VARCHAR,
            change_type VARCHAR,
            change_date DATE,
            change_ts TIMESTAMP,
            previous_is_active BOOLEAN,
            new_is_active BOOLEAN
        )
        """
    )

    con.execute(
        """
        UPDATE silver.assets AS a
        SET is_active = u.new_is_active
        FROM asset_status_updates_df AS u
        WHERE a.asset_id = u.asset_id
        """
    )
    con.execute(
        """
        INSERT INTO silver.active_assets_history (
            asset_id,
            alpaca_id,
            symbol,
            change_type,
            change_date,
            change_ts,
            previous_is_active,
            new_is_active
        )
        SELECT
            asset_id,
            alpaca_id,
            symbol,
            CASE WHEN new_is_active THEN 'activated' ELSE 'deactivated' END AS change_type,
            CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE) AS change_date,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS change_ts,
            previous_is_active,
            new_is_active
        FROM asset_status_updates_df
        """
    )
    con.execute(
        """
        INSERT INTO silver.active_assets_history (
            asset_id,
            alpaca_id,
            symbol,
            change_type,
            change_date,
            change_ts,
            previous_is_active,
            new_is_active
        )
        SELECT
            asset_id,
            alpaca_id,
            symbol,
            'snapshot' AS change_type,
            CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE) AS change_date,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS change_ts,
            NULL AS previous_is_active,
            TRUE AS new_is_active
        FROM silver.assets
        WHERE is_active = TRUE
        """
    )

    active_snapshot_count = con.execute(
        "SELECT count(*) FROM silver.assets WHERE is_active = TRUE"
    ).fetchone()[0]
    context.add_output_metadata(
        {
            "updated_count": len(history_updates_df),
            "history_appended_count": len(history_updates_df) + active_snapshot_count,
            "missing_symbols": missing_symbols,
        }
    )
