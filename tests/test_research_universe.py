from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.research_db.silver.universe as universe_module


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
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_CLOSE", 5.0)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_AVG_DOLLAR_VOLUME_63D", 1.0)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_CONTINUITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_TRADING_DAYS_252D", 1)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_POSITIVE_VOLUME_DAYS_252D", 1)

    _write_silver_prices_daily(
        data_root,
        "2026-02-12",
        pd.DataFrame(
            {
                "asset_id": [1, 2, 3],
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
                "asset_id": [1, 2, 3],
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
                "asset_id": [1, 2, 3],
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
    obs_con = duckdb.connect(":memory:")

    daily_context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    universe_module.silver_universe_membership_daily(daily_context)

    events_context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    universe_module.silver_universe_membership_events(events_context)

    daily_rows = con.execute(
        """
        SELECT member_date, asset_id, symbol, liquidity_rank, rolling_avg_dollar_volume
        FROM silver.universe_membership_daily
        ORDER BY member_date, liquidity_rank, symbol
        """
    ).fetchall()
    assert daily_rows == [
        (date(2026, 2, 12), 1, "AAPL", 1, 100000.0),
        (date(2026, 2, 12), 2, "MSFT", 2, 70000.0),
        (date(2026, 2, 13), 3, "NVDA", 1, 190000.0),
        (date(2026, 2, 13), 2, "MSFT", 2, 125000.0),
        (date(2026, 2, 17), 3, "NVDA", 1, 307000.0),
        (date(2026, 2, 17), 2, "MSFT", 2, 234000.0),
    ]

    event_rows = con.execute(
        """
        SELECT event_date, asset_id, symbol, event_type, previous_liquidity_rank, new_liquidity_rank
        FROM silver.universe_membership_events
        ORDER BY event_date, event_type, symbol
        """
    ).fetchall()
    assert event_rows == [
        (date(2026, 2, 12), 1, "AAPL", "added", None, 1),
        (date(2026, 2, 12), 2, "MSFT", "added", None, 2),
        (date(2026, 2, 13), 3, "NVDA", "added", None, 1),
        (date(2026, 2, 13), 1, "AAPL", "removed", 1, None),
    ]

    records = universe_module.universe_membership_symbols_for_date(con, date(2026, 2, 17))
    assert records == [
        ("NVDA", 1, 307000.0),
        ("MSFT", 2, 234000.0),
    ]

    dq_rows = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name IN (
            'dq_research_universe_membership_daily_required_fields_nulls',
            'dq_research_universe_membership_events_required_fields_nulls',
            'dq_research_universe_membership_events_added_fields_nulls',
            'dq_research_universe_membership_events_removed_fields_nulls'
        )
        ORDER BY check_name
        """
    ).fetchall()
    assert dq_rows == [
        ("dq_research_universe_membership_daily_required_fields_nulls", "PASS", 0.0),
        ("dq_research_universe_membership_events_added_fields_nulls", "PASS", 0.0),
        ("dq_research_universe_membership_events_removed_fields_nulls", "PASS", 0.0),
        ("dq_research_universe_membership_events_required_fields_nulls", "PASS", 0.0),
    ]


def test_universe_eligibility_filters_no_metadata_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(universe_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(universe_module, "LIQUIDITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(universe_module, "UNIVERSE_SIZE", 10)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_CLOSE", 5.0)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_AVG_DOLLAR_VOLUME_63D", 1_000_000.0)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_CONTINUITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_TRADING_DAYS_252D", 2)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_POSITIVE_VOLUME_DAYS_252D", 2)

    for partition_key in ["2026-02-12", "2026-02-13"]:
        _write_silver_prices_daily(
            data_root,
            partition_key,
            pd.DataFrame(
                {
                    "asset_id": [1, 2, 3, 4, 5, 6],
                    "symbol": ["AAPL", "0P00000M7O", "LOW", "THIN", "ABCQ", "XYZW"],
                    "timestamp": [f"{partition_key}T21:00:00Z"] * 6,
                    "trade_date": [partition_key] * 6,
                    "close": [100.0, 100.0, 3.0, 100.0, 10.0, 10.0],
                    "volume": [20_000, 20_000, 20_000, 10, 20_000, 20_000],
                    "dollar_volume": [
                        2_000_000.0,
                        2_000_000.0,
                        60_000.0,
                        1_000.0,
                        200_000.0,
                        200_000.0,
                    ],
                    "source": ["eodhd"] * 6,
                    "ingested_ts": [f"{partition_key}T22:00:00Z"] * 6,
                }
            ),
        )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    universe_module.silver_universe_membership_daily(context)

    eligibility_rows = con.execute(
        """
        SELECT
            symbol,
            passes_symbol_format,
            passes_min_price,
            passes_min_liquidity,
            passes_trading_continuity,
            passes_non_bankruptcy_suffix,
            passes_non_derivative_suffix,
            is_eligible_research_universe,
            exclusion_reasons
        FROM silver.universe_eligibility_daily
        WHERE date = DATE '2026-02-13'
        ORDER BY symbol
        """
    ).fetchall()
    by_symbol = {row[0]: row[1:] for row in eligibility_rows}

    assert by_symbol["AAPL"] == (True, True, True, True, True, True, True, "")
    assert by_symbol["0P00000M7O"][0] is False
    assert by_symbol["LOW"][1] is False
    assert by_symbol["THIN"][2] is False
    assert by_symbol["ABCQ"][4] is False
    assert by_symbol["XYZW"][5] is False

    members = con.execute(
        """
        SELECT symbol
        FROM silver.universe_membership_daily
        WHERE member_date = DATE '2026-02-13'
        ORDER BY symbol
        """
    ).fetchall()
    assert members == [("AAPL",)]


def test_universe_eligibility_excludes_market_holidays(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(universe_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(universe_module, "LIQUIDITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(universe_module, "UNIVERSE_SIZE", 10)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_CLOSE", 5.0)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_AVG_DOLLAR_VOLUME_63D", 1.0)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_CONTINUITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_TRADING_DAYS_252D", 1)
    monkeypatch.setattr(universe_module, "ELIGIBILITY_MIN_POSITIVE_VOLUME_DAYS_252D", 1)

    for partition_key in ["2026-02-13", "2026-02-16", "2026-02-17"]:
        _write_silver_prices_daily(
            data_root,
            partition_key,
            pd.DataFrame(
                {
                    "asset_id": [1],
                    "symbol": ["AAPL"],
                    "timestamp": [f"{partition_key}T21:00:00Z"],
                    "trade_date": [partition_key],
                    "close": [100.0],
                    "volume": [1000],
                    "dollar_volume": [100000.0],
                    "source": ["eodhd"],
                    "ingested_ts": [f"{partition_key}T22:00:00Z"],
                }
            ),
        )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    universe_module.silver_universe_membership_daily(context)

    dates = con.execute(
        """
        SELECT DISTINCT date
        FROM silver.universe_eligibility_daily
        ORDER BY date
        """
    ).fetchall()
    assert dates == [(date(2026, 2, 13),), (date(2026, 2, 17),)]

    members = con.execute(
        """
        SELECT DISTINCT member_date
        FROM silver.universe_membership_daily
        ORDER BY member_date
        """
    ).fetchall()
    assert members == [(date(2026, 2, 13),), (date(2026, 2, 17),)]
