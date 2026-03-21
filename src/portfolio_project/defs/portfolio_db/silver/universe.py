import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
HISTORICAL_UNIVERSE_CSV_PATH = (
    DATA_ROOT / "csvs" / "S&P 500 Historical Components & Changes.csv"
)
HISTORICAL_UNIVERSE_START_DATE = date.fromisoformat(
    os.getenv("RESEARCH_HISTORICAL_UNIVERSE_START_DATE", "2000-01-01")
)
HISTORICAL_UNIVERSE_END_DATE_EXCLUSIVE = date.fromisoformat(
    os.getenv("RESEARCH_HISTORICAL_UNIVERSE_END_DATE_EXCLUSIVE", "2016-01-01")
)
UNIVERSE_SOURCE = "sp500_historical_csv"


def _normalize_symbol_token(token: object) -> dict[str, str] | None:
    raw_symbol = str(token or "").strip().upper()
    if not raw_symbol:
        return None

    parts = raw_symbol.rsplit("-", 1)
    if len(parts) == 2 and len(parts[1]) == 6 and parts[1].isdigit():
        symbol = parts[0]
    else:
        symbol = raw_symbol

    symbol = symbol.strip().upper()
    if not symbol:
        return None

    return {
        "raw_symbol": raw_symbol,
        "symbol": symbol,
        "query_symbol": symbol.replace(".", "-"),
    }


def _empty_snapshots_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["snapshot_date", "symbol", "raw_symbol", "query_symbol", "source"]
    )


def load_historical_universe_snapshots() -> pd.DataFrame:
    if not HISTORICAL_UNIVERSE_CSV_PATH.exists():
        raise FileNotFoundError(
            f"Historical universe CSV not found at {HISTORICAL_UNIVERSE_CSV_PATH}"
        )

    raw_df = pd.read_csv(HISTORICAL_UNIVERSE_CSV_PATH, usecols=["date", "tickers"])
    if raw_df.empty:
        return _empty_snapshots_df()

    raw_df["snapshot_date"] = pd.to_datetime(raw_df["date"], errors="coerce").dt.date
    raw_df = raw_df[
        raw_df["snapshot_date"].notna()
        & (raw_df["snapshot_date"] >= HISTORICAL_UNIVERSE_START_DATE)
        & (raw_df["snapshot_date"] < HISTORICAL_UNIVERSE_END_DATE_EXCLUSIVE)
    ].copy()
    if raw_df.empty:
        return _empty_snapshots_df()

    rows: list[dict[str, object]] = []
    for row in raw_df.itertuples(index=False):
        symbols_by_symbol: dict[str, dict[str, str]] = {}
        tickers_value = getattr(row, "tickers", None)
        if pd.isna(tickers_value):
            continue
        for token in str(tickers_value).split(","):
            parsed = _normalize_symbol_token(token)
            if parsed is None:
                continue
            symbols_by_symbol[parsed["symbol"]] = parsed
        for parsed in sorted(symbols_by_symbol.values(), key=lambda item: item["symbol"]):
            rows.append(
                {
                    "snapshot_date": row.snapshot_date,
                    "symbol": parsed["symbol"],
                    "raw_symbol": parsed["raw_symbol"],
                    "query_symbol": parsed["query_symbol"],
                    "source": UNIVERSE_SOURCE,
                }
            )

    if not rows:
        return _empty_snapshots_df()

    snapshots_df = pd.DataFrame(rows)
    return snapshots_df.sort_values(["snapshot_date", "symbol"]).reset_index(drop=True)


def historical_universe_symbol_records_for_date(
    target_date: date,
) -> list[tuple[str, str, str]]:
    if (
        target_date < HISTORICAL_UNIVERSE_START_DATE
        or target_date >= HISTORICAL_UNIVERSE_END_DATE_EXCLUSIVE
    ):
        return []

    snapshots_df = load_historical_universe_snapshots()
    if snapshots_df.empty:
        return []

    eligible = snapshots_df[snapshots_df["snapshot_date"] <= target_date]
    if eligible.empty:
        return []

    snapshot_date = eligible["snapshot_date"].max()
    members_df = eligible[eligible["snapshot_date"] == snapshot_date].copy()
    return list(
        members_df[["symbol", "query_symbol", "raw_symbol"]].itertuples(
            index=False, name=None
        )
    )


def _build_membership_events_df(snapshots_df: pd.DataFrame) -> pd.DataFrame:
    if snapshots_df.empty:
        return pd.DataFrame(
            columns=[
                "event_date",
                "symbol",
                "raw_symbol",
                "query_symbol",
                "event_type",
                "source",
                "ingested_ts",
            ]
        )

    events: list[dict[str, object]] = []
    previous_rows_by_symbol: dict[str, dict[str, object]] = {}
    ingested_ts = datetime.now(timezone.utc)

    for snapshot_date in sorted(snapshots_df["snapshot_date"].unique()):
        snapshot_slice = snapshots_df[snapshots_df["snapshot_date"] == snapshot_date].copy()
        current_rows_by_symbol = {
            str(row.symbol): row._asdict()
            for row in snapshot_slice[["symbol", "raw_symbol", "query_symbol", "source"]]
            .itertuples(index=False)
        }
        current_symbols = set(current_rows_by_symbol)
        previous_symbols = set(previous_rows_by_symbol)

        added_symbols = (
            current_symbols - previous_symbols if previous_rows_by_symbol else current_symbols
        )
        removed_symbols = previous_symbols - current_symbols

        for symbol in sorted(added_symbols):
            row = current_rows_by_symbol[symbol]
            events.append(
                {
                    "event_date": snapshot_date,
                    "symbol": row["symbol"],
                    "raw_symbol": row["raw_symbol"],
                    "query_symbol": row["query_symbol"],
                    "event_type": "added",
                    "source": row["source"],
                    "ingested_ts": ingested_ts,
                }
            )

        for symbol in sorted(removed_symbols):
            row = previous_rows_by_symbol[symbol]
            events.append(
                {
                    "event_date": snapshot_date,
                    "symbol": row["symbol"],
                    "raw_symbol": row["raw_symbol"],
                    "query_symbol": row["query_symbol"],
                    "event_type": "removed",
                    "source": row["source"],
                    "ingested_ts": ingested_ts,
                }
            )

        previous_rows_by_symbol = current_rows_by_symbol

    return pd.DataFrame(events)


def _build_membership_periods_df(snapshots_df: pd.DataFrame) -> pd.DataFrame:
    if snapshots_df.empty:
        return pd.DataFrame(
            columns=[
                "snapshot_date",
                "period_end",
                "symbol",
                "raw_symbol",
                "query_symbol",
                "source",
            ]
        )

    periods: list[pd.DataFrame] = []
    snapshot_dates = sorted(snapshots_df["snapshot_date"].unique())
    final_date = HISTORICAL_UNIVERSE_END_DATE_EXCLUSIVE - timedelta(days=1)

    for index, snapshot_date in enumerate(snapshot_dates):
        next_snapshot_date = snapshot_dates[index + 1] if index + 1 < len(snapshot_dates) else None
        period_end = (
            min(next_snapshot_date - timedelta(days=1), final_date)
            if next_snapshot_date is not None
            else final_date
        )
        if period_end < snapshot_date:
            continue

        period_df = snapshots_df[snapshots_df["snapshot_date"] == snapshot_date].copy()
        period_df["period_end"] = period_end
        periods.append(period_df)

    if not periods:
        return pd.DataFrame(
            columns=[
                "snapshot_date",
                "period_end",
                "symbol",
                "raw_symbol",
                "query_symbol",
                "source",
            ]
        )

    return pd.concat(periods, ignore_index=True)


@asset(
    name="universe_membership_events",
    key_prefix=["silver"],
    required_resource_keys={"research_duckdb"},
)
def silver_universe_membership_events(context: AssetExecutionContext) -> None:
    """
    Build change events for the pre-2016 research universe from the historical S&P 500 CSV.
    """
    snapshots_df = load_historical_universe_snapshots()
    events_df = _build_membership_events_df(snapshots_df)

    con = context.resources.research_duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.register("universe_membership_events_df", events_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.universe_membership_events AS
        SELECT *
        FROM universe_membership_events_df
        """
    )

    context.add_output_metadata(
        {
            "table": "silver.universe_membership_events",
            "row_count": len(events_df),
            "symbol_count": int(events_df["symbol"].nunique()) if not events_df.empty else 0,
        }
    )


@asset(
    name="universe_membership_daily",
    key_prefix=["silver"],
    deps=[silver_universe_membership_events],
    required_resource_keys={"research_duckdb"},
)
def silver_universe_membership_daily(context: AssetExecutionContext) -> None:
    """
    Expand historical universe snapshots into a daily membership table through 2015-12-31.
    """
    snapshots_df = load_historical_universe_snapshots()
    periods_df = _build_membership_periods_df(snapshots_df)
    periods_df["ingested_ts"] = datetime.now(timezone.utc)

    con = context.resources.research_duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.register("universe_membership_periods_df", periods_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.universe_membership_daily AS
        SELECT
            CAST(day AS DATE) AS member_date,
            symbol,
            raw_symbol,
            query_symbol,
            source,
            ingested_ts
        FROM universe_membership_periods_df,
        generate_series(snapshot_date, period_end, INTERVAL 1 DAY) AS days(day)
        ORDER BY member_date, symbol
        """
    )

    row_count = con.execute(
        "SELECT count(*) FROM silver.universe_membership_daily"
    ).fetchone()[0]
    symbol_count = con.execute(
        "SELECT count(DISTINCT symbol) FROM silver.universe_membership_daily"
    ).fetchone()[0]
    min_max_row = con.execute(
        "SELECT min(member_date), max(member_date) FROM silver.universe_membership_daily"
    ).fetchone()

    context.add_output_metadata(
        {
            "table": "silver.universe_membership_daily",
            "row_count": int(row_count),
            "symbol_count": int(symbol_count),
            "min_member_date": str(min_max_row[0]) if min_max_row and min_max_row[0] else None,
            "max_member_date": str(min_max_row[1]) if min_max_row and min_max_row[1] else None,
        }
    )


