from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.research_db.bronze.research_prices as research_prices_module


class _FakeAlpacaResource:
    def __init__(self, assets_df: pd.DataFrame | None = None) -> None:
        self._assets_df = assets_df if assets_df is not None else pd.DataFrame()
        self._corporate_actions_df = pd.DataFrame()

    def get_assets_df(self) -> pd.DataFrame:
        return self._assets_df.copy()

    def get_corporate_actions_df(
        self,
        symbols=None,
        start_date=None,
        end_date=None,
        types=None,
    ) -> pd.DataFrame:
        return self._corporate_actions_df.copy()


class _FakeEODHDResource:
    def __init__(self, daily_df: pd.DataFrame | None = None) -> None:
        self._daily_df = daily_df if daily_df is not None else pd.DataFrame()

    def get_bulk_eod_prices_df(self, trade_date: date) -> pd.DataFrame:
        return self._daily_df.copy()


def test_resolve_alpaca_symbols_filters_to_common_equities_and_keeps_spy() -> None:
    assets_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "asset_class": "us_equity",
                "tradable": True,
                "status": "active",
                "exchange": "NASDAQ",
            },
            {
                "symbol": "SPY",
                "name": "SPDR S&P 500 ETF Trust",
                "asset_class": "us_equity",
                "tradable": True,
                "status": "active",
                "exchange": "ARCA",
            },
            {
                "symbol": "QQQ",
                "name": "Invesco QQQ ETF",
                "asset_class": "us_equity",
                "tradable": True,
                "status": "active",
                "exchange": "NASDAQ",
            },
            {
                "symbol": "ABCPR",
                "name": "ABC Preferred Series A",
                "asset_class": "us_equity",
                "tradable": True,
                "status": "active",
                "exchange": "NYSE",
            },
            {
                "symbol": "OTCM",
                "name": "OTC Markets Group",
                "asset_class": "us_equity",
                "tradable": True,
                "status": "active",
                "exchange": "OTCQX",
            },
        ]
    )
    context = build_asset_context(resources={"alpaca": _FakeAlpacaResource(assets_df)})

    symbols = research_prices_module._resolve_alpaca_symbols(context)

    assert symbols == ["AAPL", "SPY"]


def test_bronze_alpaca_prices_daily_writes_partition(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    research_prices_module.DATA_ROOT = data_root

    def fake_resolve_symbols(context, configured_symbols=None, max_symbols=None):
        return ["AAPL", "MSFT"]

    def fake_fetch_alpaca(context, partition_date, symbols, batch_size, request_sleep_seconds):
        assert partition_date == date(2020, 1, 2)
        assert symbols == ["AAPL", "MSFT"]
        return pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "timestamp": [
                    datetime(2020, 1, 2, 21, 0, tzinfo=timezone.utc),
                    datetime(2020, 1, 2, 21, 0, tzinfo=timezone.utc),
                ],
                "trade_date": [date(2020, 1, 2), date(2020, 1, 2)],
                "open": [100.0, 200.0],
                "high": [101.0, 201.0],
                "low": [99.0, 199.0],
                "close": [100.5, 200.5],
                "adjusted_close": [pd.NA, pd.NA],
                "volume": [1000, 2000],
                "trade_count": [10, 20],
                "vwap": [100.2, 200.2],
                "source": ["alpaca", "alpaca"],
                "ingested_ts": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        )

    monkeypatch.setattr(research_prices_module, "_resolve_alpaca_symbols", fake_resolve_symbols)
    monkeypatch.setattr(
        research_prices_module, "_fetch_alpaca_daily_bars_for_day", fake_fetch_alpaca
    )

    context = build_asset_context(
        resources={"alpaca": _FakeAlpacaResource()},
        partition_key="2020-01-02",
    )
    research_prices_module.bronze_alpaca_prices_daily(context)

    out_path = data_root / "bronze" / "alpaca_prices_daily" / "date=2020-01-02" / "prices.parquet"
    assert out_path.exists()

    out_df = pd.read_parquet(out_path)
    assert len(out_df) == 2
    assert set(out_df["symbol"]) == {"AAPL", "MSFT"}
    assert set(out_df["source"]) == {"alpaca"}


def test_bronze_eodhd_prices_daily_writes_partition(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    research_prices_module.DATA_ROOT = data_root

    raw_df = pd.DataFrame(
        {
            "code": ["AAPL", "BF.B", "SPY"],
            "date": ["2005-06-15", "2005-06-15", "2005-06-15"],
            "open": [35.0, 50.0, 120.0],
            "high": [36.0, 51.0, 121.0],
            "low": [34.5, 49.0, 119.0],
            "close": [35.5, 50.5, 120.5],
            "adjusted_close": [35.4, 50.4, 120.4],
            "volume": [500000, 100000, 750000],
        }
    )

    context = build_asset_context(
        resources={"eodhd": _FakeEODHDResource(raw_df)},
        partition_key="2005-06-15",
    )
    research_prices_module.bronze_eodhd_prices_daily(context)

    out_path = data_root / "bronze" / "eodhd_prices_daily" / "date=2005-06-15" / "prices.parquet"
    assert out_path.exists()

    out_df = pd.read_parquet(out_path)
    assert len(out_df) == 3
    assert set(out_df["symbol"]) == {"AAPL", "BF.B", "SPY"}
    assert set(out_df["source"]) == {"eodhd"}
    assert float(out_df.loc[out_df["symbol"] == "AAPL", "adjusted_close"].iloc[0]) == 35.4


def test_bronze_alpaca_corporate_actions_daily_writes_split_partition(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    research_prices_module.DATA_ROOT = data_root

    def fake_resolve_symbols(context, configured_symbols=None, max_symbols=None):
        return ["AAPL", "MSFT"]

    def fake_fetch_actions(context, partition_date, symbols, batch_size, request_sleep_seconds):
        assert partition_date == date(2026, 2, 17)
        assert symbols == ["AAPL", "MSFT"]
        return pd.DataFrame(
            {
                "action_id": ["act-1", "act-2"],
                "symbol": ["AAPL", "MSFT"],
                "action_type": ["forward_splits", "cash_dividends"],
                "effective_date": [date(2026, 2, 17), date(2026, 2, 17)],
                "process_date": [date(2026, 2, 17), date(2026, 2, 17)],
                "old_rate": [1.0, pd.NA],
                "new_rate": [2.0, pd.NA],
                "cash_rate": [pd.NA, 0.25],
                "split_ratio": [2.0, pd.NA],
                "source": ["alpaca", "alpaca"],
                "ingested_ts": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            }
        )

    monkeypatch.setattr(research_prices_module, "_resolve_alpaca_symbols", fake_resolve_symbols)
    monkeypatch.setattr(
        research_prices_module,
        "_fetch_alpaca_corporate_actions_for_day",
        fake_fetch_actions,
    )

    context = build_asset_context(
        resources={"alpaca": _FakeAlpacaResource()},
        partition_key="2026-02-17",
    )
    research_prices_module.bronze_alpaca_corporate_actions_daily(context)

    out_path = (
        data_root
        / "bronze"
        / "alpaca_corporate_actions"
        / "actions.parquet"
    )
    assert out_path.exists()

    out_df = pd.read_parquet(out_path)
    assert len(out_df) == 2
    assert out_df["symbol"].tolist() == ["AAPL", "MSFT"]
    assert float(out_df["split_ratio"].iloc[0]) == 2.0
    assert float(out_df["cash_rate"].iloc[1]) == 0.25
