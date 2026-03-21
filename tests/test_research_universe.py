from datetime import date
from pathlib import Path

import duckdb
from dagster import build_asset_context

import portfolio_project.defs.portfolio_db.silver.universe as universe_module


def test_universe_assets_build_events_and_daily(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    csv_dir = data_root / "csvs"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "S&P 500 Historical Components & Changes.csv"
    csv_path.write_text(
        'date,tickers\n2000-01-03,"AAPL,MSFT,BF.B"\n2000-01-10,"AAPL,BF.B,GOOG-201503"\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(universe_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(universe_module, "HISTORICAL_UNIVERSE_CSV_PATH", csv_path)
    monkeypatch.setattr(universe_module, "HISTORICAL_UNIVERSE_START_DATE", date(2000, 1, 1))
    monkeypatch.setattr(universe_module, "HISTORICAL_UNIVERSE_END_DATE_EXCLUSIVE", date(2016, 1, 1))

    con = duckdb.connect(":memory:")

    events_context = build_asset_context(resources={"research_duckdb": con})
    universe_module.silver_universe_membership_events(events_context)

    daily_context = build_asset_context(resources={"research_duckdb": con})
    universe_module.silver_universe_membership_daily(daily_context)

    events_rows = con.execute(
        """
        SELECT event_date, symbol, query_symbol, event_type
        FROM silver.universe_membership_events
        ORDER BY event_date, event_type, symbol
        """
    ).fetchall()
    assert events_rows == [
        (date(2000, 1, 3), "AAPL", "AAPL", "added"),
        (date(2000, 1, 3), "BF.B", "BF-B", "added"),
        (date(2000, 1, 3), "MSFT", "MSFT", "added"),
        (date(2000, 1, 10), "GOOG", "GOOG", "added"),
        (date(2000, 1, 10), "MSFT", "MSFT", "removed"),
    ]

    before_change = con.execute(
        """
        SELECT symbol
        FROM silver.universe_membership_daily
        WHERE member_date = '2000-01-05'
        ORDER BY symbol
        """
    ).fetchall()
    after_change = con.execute(
        """
        SELECT symbol
        FROM silver.universe_membership_daily
        WHERE member_date = '2000-01-10'
        ORDER BY symbol
        """
    ).fetchall()
    assert before_change == [("AAPL",), ("BF.B",), ("MSFT",)]
    assert after_change == [("AAPL",), ("BF.B",), ("GOOG",)]

    records = universe_module.historical_universe_symbol_records_for_date(date(2000, 1, 11))
    assert records == [
        ("AAPL", "AAPL", "AAPL"),
        ("BF.B", "BF-B", "BF.B"),
        ("GOOG", "GOOG", "GOOG-201503"),
    ]
