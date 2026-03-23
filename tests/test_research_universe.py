from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.portfolio_db.silver.universe as universe_module


def _write_silver_prices_daily(data_root: Path, partition_key: str, frame: pd.DataFrame) -> None:
    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / f"month={partition_key[:7]}"
        / f"date={partition_key}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_path, index=False)


def test_universe_assets_build_from_rolling_dollar_volume(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(universe_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(universe_module, "LIQUIDITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(universe_module, "UNIVERSE_SIZE", 2)

    _write_silver_prices_daily(
        data_root,
        "2026-02-12",
        pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT", "NVDA"],
                "timestamp": [
                    "2026-02-12T21:00:00Z",
                    "2026-02-12T21:00:00Z",
                    "2026-02-12T21:00:00Z",
                ],
                "trade_date": ["2026-02-12", "2026-02-12", "2026-02-12"],
                "close": [100.0, 50.0, 20.0],
                "volume": [1000, 1400, 3000],
                "dollar_volume": [100000.0, 70000.0, 60000.0],
                "source": ["alpaca", "alpaca", "eodhd"],
                "ingested_ts": [
                    "2026-02-12T22:00:00Z",
                    "2026-02-12T22:00:00Z",
                    "2026-02-12T22:00:00Z",
                ],
            }
        ),
    )
    _write_silver_prices_daily(
        data_root,
        "2026-02-13",
        pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT", "NVDA"],
                "timestamp": [
                    "2026-02-13T21:00:00Z",
                    "2026-02-13T21:00:00Z",
                    "2026-02-13T21:00:00Z",
                ],
                "trade_date": ["2026-02-13", "2026-02-13", "2026-02-13"],
                "close": [90.0, 120.0, 40.0],
                "volume": [1000, 1500, 8000],
                "dollar_volume": [90000.0, 180000.0, 320000.0],
                "source": ["alpaca", "alpaca", "eodhd"],
                "ingested_ts": [
                    "2026-02-13T22:00:00Z",
                    "2026-02-13T22:00:00Z",
                    "2026-02-13T22:00:00Z",
                ],
            }
        ),
    )
    _write_silver_prices_daily(
        data_root,
        "2026-02-17",
        pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT", "NVDA"],
                "timestamp": [
                    "2026-02-17T21:00:00Z",
                    "2026-02-17T21:00:00Z",
                    "2026-02-17T21:00:00Z",
                ],
                "trade_date": ["2026-02-17", "2026-02-17", "2026-02-17"],
                "close": [85.0, 160.0, 42.0],
                "volume": [900, 1800, 7000],
                "dollar_volume": [76500.0, 288000.0, 294000.0],
                "source": ["alpaca", "alpaca", "eodhd"],
                "ingested_ts": [
                    "2026-02-17T22:00:00Z",
                    "2026-02-17T22:00:00Z",
                    "2026-02-17T22:00:00Z",
                ],
            }
        ),
    )

    con = duckdb.connect(":memory:")

    daily_context = build_asset_context(resources={"research_duckdb": con})
    universe_module.silver_universe_membership_daily(daily_context)

    events_context = build_asset_context(resources={"research_duckdb": con})
    universe_module.silver_universe_membership_events(events_context)

    daily_rows = con.execute(
        """
        SELECT member_date, symbol, liquidity_rank, rolling_avg_dollar_volume
        FROM silver.universe_membership_daily
        ORDER BY member_date, liquidity_rank, symbol
        """
    ).fetchall()
    assert daily_rows == [
        (date(2026, 2, 12), "AAPL", 1, 100000.0),
        (date(2026, 2, 12), "MSFT", 2, 70000.0),
        (date(2026, 2, 13), "NVDA", 1, 190000.0),
        (date(2026, 2, 13), "MSFT", 2, 125000.0),
        (date(2026, 2, 17), "NVDA", 1, 307000.0),
        (date(2026, 2, 17), "MSFT", 2, 234000.0),
    ]

    event_rows = con.execute(
        """
        SELECT event_date, symbol, event_type, previous_liquidity_rank, new_liquidity_rank
        FROM silver.universe_membership_events
        ORDER BY event_date, event_type, symbol
        """
    ).fetchall()
    assert event_rows == [
        (date(2026, 2, 12), "AAPL", "added", None, 1),
        (date(2026, 2, 12), "MSFT", "added", None, 2),
        (date(2026, 2, 13), "NVDA", "added", None, 1),
        (date(2026, 2, 13), "AAPL", "removed", 1, None),
    ]

    records = universe_module.universe_membership_symbols_for_date(con, date(2026, 2, 17))
    assert records == [
        ("NVDA", 1, 307000.0),
        ("MSFT", 2, 234000.0),
    ]
