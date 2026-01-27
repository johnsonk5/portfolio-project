import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dagster import Array, AssetExecutionContext, DailyPartitionsDefinition, Field, String, asset
from dagster import AssetKey


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
BRONZE_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
TICKERS_ENV = "ALPACA_TICKERS"


@asset(
    name="bronze_alpaca_bars",
    partitions_def=BRONZE_PARTITIONS,
    required_resource_keys={"alpaca", "duckdb"},
    deps=[AssetKey("silver_alpaca_assets")],
    config_schema={
        "symbols": Field(Array(String), is_required=False),
    },
)
def bronze_alpaca_bars(context: AssetExecutionContext) -> None:
    """
    Write 5-minute Alpaca bar data to daily-partitioned parquet files.
    """
    env_symbols = [
        s.strip() for s in os.getenv(TICKERS_ENV, "").split(",") if s.strip()
    ]
    config_symbols = context.op_config.get("symbols", None)
    active_symbols = []
    try:
        active_symbols = [
            row[0]
            for row in context.resources.duckdb.execute(
                "SELECT symbol FROM silver.assets WHERE is_active = TRUE"
            ).fetchall()
            if row and row[0]
        ]
    except Exception as exc:
        context.log.warning("Unable to read silver.assets for active symbols: %s", exc)

    if active_symbols:
        symbols = active_symbols
    elif env_symbols:
        if config_symbols:
            symbols = sorted(set(config_symbols) & set(env_symbols))
            if not symbols:
                context.log.warning(
                    "No configured symbols are active; using active symbols from %s.",
                    TICKERS_ENV,
                )
                symbols = env_symbols
        else:
            symbols = env_symbols
    else:
        symbols = config_symbols or ["AAPL"]
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    start_date = partition_date
    end_date = partition_date + timedelta(days=1)

    frames = []
    for symbol in symbols:
        df = context.resources.alpaca.get_bars_df(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or df.empty:
            continue
        if "symbol" not in df.columns:
            df = df.reset_index()
            if "symbol" not in df.columns:
                df["symbol"] = symbol
        if "timestamp" not in df.columns and df.index.name == "timestamp":
            df = df.reset_index()
        df["ingested_ts"] = datetime.utcnow()
        frames.append(df)

    if not frames:
        context.log.warning("No bar data returned for partition %s", context.partition_key)
        return

    out_df = pd.concat(frames, ignore_index=True)
    month_key = partition_date.strftime("%Y-%m")
    partition_dir = DATA_ROOT / "bronze" / "alpaca_bars" / f"month={month_key}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_path = partition_dir / "bars.parquet"
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        out_df = pd.concat([existing, out_df], ignore_index=True)
    out_df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(out_df)}
    )


@asset(
    name="bronze_alpaca_assets",
    required_resource_keys={"alpaca"},
)
def bronze_alpaca_assets(context: AssetExecutionContext) -> None:
    """
    Write Alpaca asset (ticker universe) snapshot to a reference parquet file.
    """
    df = context.resources.alpaca.get_assets_df()
    if df is None or df.empty:
        context.log.warning("No asset data returned.")
        return

    # Coerce UUID objects to strings for parquet compatibility.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, uuid.UUID)).any():
            df[col] = df[col].astype(str)

    df["ingested_ts"] = datetime.utcnow()
    reference_dir = DATA_ROOT / "bronze" / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    out_path = reference_dir / "alpaca_assets.parquet"
    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df)}
    )
