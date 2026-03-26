import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
RESEARCH_PRICES_PARTITIONS_START_DATE = os.getenv(
    "RESEARCH_EODHD_PRICES_PARTITIONS_START_DATE", "2000-01-01"
)
RESEARCH_PRICES_PARTITIONS = DailyPartitionsDefinition(
    start_date=RESEARCH_PRICES_PARTITIONS_START_DATE
)
RESEARCH_DAILY_PRICES_DATASET = "research_daily_prices"
SOURCE_PRIORITY = {"alpaca": 0, "eodhd": 1}
PRICE_COLUMNS = [
    "symbol",
    "timestamp",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "trade_count",
    "vwap",
    "dollar_volume",
    "source",
    "ingested_ts",
]


def _table_exists(con, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        is not None
    )


def _bronze_prices_path(dataset_name: str, trade_date: date) -> Path:
    return (
        DATA_ROOT
        / "bronze"
        / dataset_name
        / f"date={trade_date.isoformat()}"
        / "prices.parquet"
    )


def _silver_monthly_prices_path(trade_date: date) -> Path:
    return (
        DATA_ROOT
        / "silver"
        / RESEARCH_DAILY_PRICES_DATASET
        / f"month={trade_date.strftime('%Y-%m')}"
        / f"date={trade_date.isoformat()}.parquet"
    )


def _load_bronze_prices(dataset_name: str, trade_date: date) -> pd.DataFrame:
    path = _bronze_prices_path(dataset_name, trade_date)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _normalize_daily_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
    normalized = normalized[normalized["symbol"].ne("")].copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="coerce").dt.date
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True, errors="coerce")

    for column in [
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "trade_count",
        "vwap",
    ]:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized["source"] = normalized["source"].astype(str).str.strip().str.lower()
    normalized["ingested_ts"] = pd.to_datetime(
        normalized["ingested_ts"], utc=True, errors="coerce"
    )
    normalized["dollar_volume"] = (
        pd.to_numeric(normalized["close"], errors="coerce")
        * pd.to_numeric(normalized["volume"], errors="coerce")
    )
    normalized["source_priority"] = normalized["source"].map(SOURCE_PRIORITY).fillna(99).astype(int)
    normalized = normalized[
        normalized["trade_date"].notna()
        & normalized["timestamp"].notna()
        & normalized["source"].ne("")
    ].copy()

    ordered_columns = [*PRICE_COLUMNS, "source_priority"]
    return normalized[ordered_columns].sort_values(
        ["symbol", "trade_date", "source_priority", "timestamp", "ingested_ts"],
        ascending=[True, True, True, False, False],
        kind="stable",
    )


def combine_source_daily_prices(trade_date: date) -> pd.DataFrame:
    frames = [
        _normalize_daily_prices_df(_load_bronze_prices("alpaca_prices_daily", trade_date)),
        _normalize_daily_prices_df(_load_bronze_prices("eodhd_prices_daily", trade_date)),
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    for frame in frames:
        for column in [
            "adjusted_close",
            "trade_count",
            "vwap",
        ]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "trade_date"], keep="first").copy()
    combined = combined.drop(columns=["source_priority"])
    return combined[PRICE_COLUMNS].sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def write_research_daily_prices_partition(
    trade_date: date,
    day_df: pd.DataFrame,
    *,
    overwrite: bool,
) -> tuple[int, int]:
    out_path = _silver_monthly_prices_path(trade_date)
    if out_path.exists() and not overwrite:
        return 0, 0
    if day_df is None or day_df.empty:
        return 0, 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    day_df.to_parquet(out_path, index=False)
    return 1, len(day_df)


@asset(
    name="research_daily_prices",
    key_prefix=["silver"],
    partitions_def=RESEARCH_PRICES_PARTITIONS,
    required_resource_keys={"research_duckdb"},
)
def silver_research_daily_prices(context: AssetExecutionContext) -> None:
    """
    Build the research daily silver price partition by preferring Alpaca overlap and
    falling back to EODHD for the rest of the historical range.
    """
    trade_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    day_df = combine_source_daily_prices(trade_date)

    if day_df.empty:
        context.log.warning(
            "No research daily prices found for partition %s.",
            context.partition_key,
        )
        return

    alpaca_rows = day_df["source"].eq("alpaca")
    if alpaca_rows.any() and _table_exists(context.resources.research_duckdb, "silver", "alpaca_corporate_actions"):
        symbols = sorted(day_df.loc[alpaca_rows, "symbol"].dropna().astype(str).unique().tolist())
        if symbols:
            factor_df = context.resources.research_duckdb.execute(
                """
                SELECT
                    symbol,
                    exp(sum(ln(old_rate / new_rate))) AS split_adjustment_factor
                FROM silver.alpaca_corporate_actions
                WHERE effective_date > ?
                  AND action_type IN ('forward_splits', 'reverse_splits')
                  AND symbol = ANY(?)
                GROUP BY symbol
                """,
                [trade_date, symbols],
            ).fetch_df()
            if not factor_df.empty:
                factor_df["symbol"] = factor_df["symbol"].astype(str)
                factor_map = dict(
                    zip(
                        factor_df["symbol"].tolist(),
                        factor_df["split_adjustment_factor"].astype(float).tolist(),
                    )
                )
                split_factor = (
                    day_df.loc[alpaca_rows, "symbol"].map(factor_map).fillna(1.0).astype(float)
                )
            else:
                split_factor = pd.Series(1.0, index=day_df.loc[alpaca_rows].index, dtype="float64")

            day_df.loc[alpaca_rows, "adjusted_close"] = (
                pd.to_numeric(day_df.loc[alpaca_rows, "close"], errors="coerce") * split_factor.values
            )

    files_written, rows_written = write_research_daily_prices_partition(
        trade_date,
        day_df,
        overwrite=True,
    )
    source_counts = {
        str(source): int(count)
        for source, count in day_df["source"].value_counts().sort_index().items()
    }
    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "row_count": rows_written,
            "files_written": files_written,
            "symbol_count": int(day_df["symbol"].nunique()),
            "source_counts": source_counts,
            "table": "silver.research_daily_prices",
            "output_path": _silver_monthly_prices_path(trade_date).as_posix(),
        }
    )
