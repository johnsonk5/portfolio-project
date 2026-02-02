import os
import uuid
from datetime import datetime, timedelta, timezone
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
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    start_date = datetime.combine(partition_date, datetime.min.time(), tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)

    df = context.resources.alpaca.get_bars_df(
        symbol_or_symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    if df is None or df.empty:
        context.log.warning("No bar data returned for partition %s", context.partition_key)
        return
    if "symbol" not in df.columns:
        df = df.reset_index()
    if "timestamp" not in df.columns and df.index.name == "timestamp":
        df = df.reset_index()
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper()
    else:
        context.log.warning("No symbol column found in bar data for partition %s", context.partition_key)
        return
    df["ingested_ts"] = datetime.now(timezone.utc)

    frames = [group.copy() for _, group in df.groupby("symbol")]

    if not frames:
        context.log.warning("No bar data returned for partition %s", context.partition_key)
        return

    month_key = partition_date.strftime("%Y-%m")
    partition_dir = DATA_ROOT / "bronze" / "alpaca_bars" / f"month={month_key}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    day_start = start_date
    day_end = end_date
    total_rows = 0
    written_files = []

    for df in frames:
        if df is None or df.empty:
            continue
        symbol = str(df["symbol"].iloc[0]).upper()
        out_path = partition_dir / f"symbol={symbol}.parquet"

        if out_path.exists():
            existing = pd.read_parquet(out_path)
            if "timestamp" in existing.columns:
                existing_ts = pd.to_datetime(existing["timestamp"], utc=True, errors="coerce")
                existing = existing[
                    ~((existing_ts >= day_start) & (existing_ts < day_end))
                ]
            else:
                existing = existing.iloc[0:0]
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(out_path, index=False)
        total_rows += len(df)
        written_files.append(str(out_path))

    context.add_output_metadata(
        {"partition": context.partition_key, "files_written": len(written_files), "row_count": total_rows}
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

    df["ingested_ts"] = datetime.now(timezone.utc)
    reference_dir = DATA_ROOT / "bronze" / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    out_path = reference_dir / "alpaca_assets.parquet"
    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {"path": str(out_path), "row_count": len(df)}
    )
