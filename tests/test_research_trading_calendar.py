from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from portfolio_project.defs.research_db.trading_calendar import create_valid_trading_dates_table


def test_create_valid_trading_dates_table_excludes_ref_invalid_days(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.parquet"
    pd.DataFrame(
        [
            {"trade_date": "2005-06-14", "symbol": "AAPL"},
            {"trade_date": "2005-06-15", "symbol": "AAPL"},
            {"trade_date": "2005-06-16", "symbol": "AAPL"},
        ]
    ).to_parquet(prices_path, index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS ref")
    con.execute(
        """
        CREATE TABLE ref.invalid_trading_days (
            invalid_date DATE,
            reason_code VARCHAR,
            description VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO ref.invalid_trading_days
        VALUES
            (DATE '2005-06-15', 'bad_source_partition', 'Incomplete source partition')
        """
    )

    create_valid_trading_dates_table(con, prices_path.as_posix())

    rows = con.execute(
        """
        SELECT trade_date
        FROM valid_research_trading_dates
        ORDER BY trade_date
        """
    ).fetchall()
    assert rows == [(date(2005, 6, 14),), (date(2005, 6, 16),)]
