from datetime import timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from dagster import build_asset_context

import portfolio_project.defs.research_db.silver.signals as signals_module


def _write_silver_prices_daily(data_root: Path, trade_date: str, frame: pd.DataFrame) -> None:
    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / f"month={trade_date[:7]}"
        / f"date={trade_date}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_path, index=False)


def test_signals_daily_builds_expected_metrics(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(signals_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(signals_module, "SIGNAL_VERSION", "test-v1")

    dates = pd.bdate_range("2025-01-01", periods=260)
    closes = pd.Series([100.0 + (idx * 0.5) for idx in range(len(dates))], dtype="float64")
    adjusted = closes * 1.02
    volumes = pd.Series([1_000_000 + (idx * 1000) for idx in range(len(dates))], dtype="int64")

    expected = pd.DataFrame(
        {
            "date": dates.date,
            "close": closes,
            "adjusted_close": adjusted,
            "dollar_volume": closes * volumes,
        }
    )
    expected["return_price"] = expected["adjusted_close"].fillna(expected["close"])
    expected["returns_1d"] = expected["return_price"].pct_change(1)
    expected["returns_5d"] = expected["return_price"].pct_change(5)
    expected["returns_21d"] = expected["return_price"].pct_change(21)
    expected["returns_63d"] = expected["return_price"].pct_change(63)
    expected["returns_126d"] = expected["return_price"].pct_change(126)
    expected["returns_252d"] = expected["return_price"].pct_change(252)
    expected["momentum_12_1"] = (
        expected["return_price"].shift(21) / expected["return_price"].shift(252)
    ) - 1
    expected["sma_20"] = expected["close"].rolling(20, min_periods=1).mean()
    expected["sma_50"] = expected["close"].rolling(50, min_periods=1).mean()
    expected["sma_200"] = expected["close"].rolling(200, min_periods=1).mean()
    expected["price_to_sma_50"] = expected["close"] / expected["sma_50"]
    expected["price_to_sma_200"] = expected["close"] / expected["sma_200"]
    expected["sma_50_to_200"] = expected["sma_50"] / expected["sma_200"]
    expected["realized_vol_21d"] = expected["returns_1d"].rolling(21, min_periods=2).std(ddof=1) * (
        252.0**0.5
    )
    expected["realized_vol_63d"] = expected["returns_1d"].rolling(63, min_periods=2).std(ddof=1) * (
        252.0**0.5
    )
    expected["rolling_252d_high"] = expected["close"].rolling(252, min_periods=1).max()
    expected["rolling_252d_low"] = expected["close"].rolling(252, min_periods=1).min()
    expected["drawdown_from_252d_high"] = (expected["close"] / expected["rolling_252d_high"]) - 1
    expected["avg_dollar_volume_21d"] = expected["dollar_volume"].rolling(21, min_periods=1).mean()
    expected["avg_dollar_volume_63d"] = expected["dollar_volume"].rolling(63, min_periods=1).mean()

    for idx, ts in enumerate(dates):
        trade_date = ts.date().isoformat()
        close = float(closes.iloc[idx])
        adj_close = float(adjusted.iloc[idx])
        volume = int(volumes.iloc[idx])
        _write_silver_prices_daily(
            data_root,
            trade_date,
            pd.DataFrame(
                {
                    "symbol": ["AAPL"],
                    "timestamp": [ts.tz_localize(timezone.utc)],
                    "trade_date": [trade_date],
                    "open": [close - 1.0],
                    "high": [close + 1.0],
                    "low": [close - 2.0],
                    "close": [close],
                    "adjusted_close": [adj_close],
                    "volume": [volume],
                    "trade_count": [1000],
                    "vwap": [close],
                    "dollar_volume": [close * volume],
                    "source": ["alpaca"],
                    "ingested_ts": [ts.tz_localize(timezone.utc)],
                }
            ),
        )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    signals_module.silver_signals_daily(context)

    actual = con.execute(
        """
        SELECT
            date,
            symbol,
            returns_1d,
            returns_5d,
            returns_21d,
            returns_63d,
            returns_126d,
            returns_252d,
            momentum_12_1,
            sma_20,
            sma_50,
            sma_200,
            price_to_sma_50,
            price_to_sma_200,
            sma_50_to_200,
            realized_vol_21d,
            realized_vol_63d,
            drawdown_from_252d_high,
            rolling_252d_high,
            rolling_252d_low,
            avg_dollar_volume_21d,
            avg_dollar_volume_63d,
            signal_version
        FROM silver.signals_daily
        ORDER BY date, symbol
        """
    ).df()

    assert len(actual) == len(expected)
    assert actual["symbol"].unique().tolist() == ["AAPL"]
    assert set(actual["signal_version"].unique()) == {"test-v1"}

    row_idx = len(expected) - 1
    actual_row = actual.iloc[row_idx]
    expected_row = expected.iloc[row_idx]

    for column in [
        "returns_1d",
        "returns_5d",
        "returns_21d",
        "returns_63d",
        "returns_126d",
        "returns_252d",
        "momentum_12_1",
        "sma_20",
        "sma_50",
        "sma_200",
        "price_to_sma_50",
        "price_to_sma_200",
        "sma_50_to_200",
        "realized_vol_21d",
        "realized_vol_63d",
        "drawdown_from_252d_high",
        "rolling_252d_high",
        "rolling_252d_low",
        "avg_dollar_volume_21d",
        "avg_dollar_volume_63d",
    ]:
        assert actual_row[column] == pytest.approx(expected_row[column], rel=1e-9)

    first_row = actual.iloc[0]
    assert pd.isna(first_row["returns_1d"])
    assert pd.isna(first_row["returns_5d"])
    assert pd.isna(first_row["returns_252d"])

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_research_signals_daily_required_fields_nulls'
        """
    ).fetchone()
    assert dq_row == ("dq_research_signals_daily_required_fields_nulls", "PASS", 0.0)
