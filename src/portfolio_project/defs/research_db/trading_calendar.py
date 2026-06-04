from datetime import date
from typing import Any

import pandas as pd

from portfolio_project.defs.portfolio_db.observability.run_log import _is_us_trading_day

INVALID_TRADING_DAY_RECORDS: list[dict[str, str]] = [
    {
        "invalid_date": "2001-09-11",
        "reason_code": "special_market_closure",
        "description": "NYSE closed after September 11 attacks.",
    },
    {
        "invalid_date": "2001-09-12",
        "reason_code": "special_market_closure",
        "description": "NYSE closed after September 11 attacks.",
    },
    {
        "invalid_date": "2001-09-13",
        "reason_code": "special_market_closure",
        "description": "NYSE closed after September 11 attacks.",
    },
    {
        "invalid_date": "2001-09-14",
        "reason_code": "special_market_closure",
        "description": "NYSE closed after September 11 attacks.",
    },
    {
        "invalid_date": "2004-06-11",
        "reason_code": "special_market_closure",
        "description": "National day of mourning for President Reagan.",
    },
    {
        "invalid_date": "2007-01-02",
        "reason_code": "special_market_closure",
        "description": "National day of mourning for President Ford.",
    },
    {
        "invalid_date": "2012-10-29",
        "reason_code": "special_market_closure",
        "description": "NYSE closed for Hurricane Sandy.",
    },
    {
        "invalid_date": "2012-10-30",
        "reason_code": "special_market_closure",
        "description": "NYSE closed for Hurricane Sandy.",
    },
    {
        "invalid_date": "2018-12-05",
        "reason_code": "special_market_closure",
        "description": "National day of mourning for President George H.W. Bush.",
    },
    {
        "invalid_date": "2025-01-09",
        "reason_code": "special_market_closure",
        "description": "National day of mourning for President Carter.",
    },
    {
        "invalid_date": "2005-06-15",
        "reason_code": "bad_source_partition",
        "description": "EODHD daily price partition is materially incomplete.",
    },
]


def is_us_trading_day(value: date | str) -> bool:
    partition_key = value.isoformat() if isinstance(value, date) else str(value)
    return _is_us_trading_day(partition_key)


def filter_us_trading_days(frame: pd.DataFrame, date_column: str) -> pd.DataFrame:
    if frame.empty or date_column not in frame.columns:
        return frame
    mask = pd.to_datetime(frame[date_column], errors="coerce").dt.date.map(is_us_trading_day)
    return frame.loc[mask.fillna(False)].copy()


def create_valid_trading_dates_table(
    con: Any,
    prices_glob: str,
    *,
    table_name: str = "valid_research_trading_dates",
) -> None:
    con.execute(
        "CREATE OR REPLACE TEMPORARY TABLE invalid_research_trading_dates (trade_date DATE)"
    )
    invalid_dates_df = pd.DataFrame(INVALID_TRADING_DAY_RECORDS)
    invalid_dates_df["invalid_date"] = pd.to_datetime(invalid_dates_df["invalid_date"]).dt.date
    con.register("invalid_research_trading_dates_df", invalid_dates_df)
    con.execute(
        """
        INSERT INTO invalid_research_trading_dates
        SELECT CAST(invalid_date AS DATE)
        FROM invalid_research_trading_dates_df
        """
    )
    if _table_exists(con, "ref", "invalid_trading_days"):
        con.execute(
            """
            INSERT INTO invalid_research_trading_dates
            SELECT CAST(invalid_date AS DATE)
            FROM ref.invalid_trading_days
            WHERE invalid_date IS NOT NULL
            EXCEPT
            SELECT trade_date
            FROM invalid_research_trading_dates
            """
        )

    rows = con.execute(
        """
        SELECT DISTINCT CAST(trade_date AS DATE) AS trade_date
        FROM read_parquet(?)
        WHERE trade_date IS NOT NULL
        ORDER BY trade_date
        """,
        [prices_glob],
    ).fetchall()
    invalid_dates = {
        row[0]
        for row in con.execute(
            """
            SELECT DISTINCT trade_date
            FROM invalid_research_trading_dates
            """
        ).fetchall()
        if row[0] is not None
    }
    trading_dates = [
        row[0]
        for row in rows
        if row[0] is not None and is_us_trading_day(row[0]) and row[0] not in invalid_dates
    ]
    con.execute(f"CREATE OR REPLACE TEMPORARY TABLE {table_name} (trade_date DATE)")
    if not trading_dates:
        return
    trading_dates_df = pd.DataFrame({"trade_date": trading_dates})
    con.register("valid_research_trading_dates_df", trading_dates_df)
    con.execute(
        f"""
        INSERT INTO {table_name}
        SELECT CAST(trade_date AS DATE)
        FROM valid_research_trading_dates_df
        """
    )


def _table_exists(con: Any, schema: str, table: str) -> bool:
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
