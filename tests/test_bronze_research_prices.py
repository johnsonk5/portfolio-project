from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.portfolio_db.bronze.research_prices as research_prices_module


class _FakeAlpacaResource:
    def __init__(self, assets_df: pd.DataFrame | None = None) -> None:
        self._assets_df = assets_df if assets_df is not None else pd.DataFrame()

    def get_assets_df(self) -> pd.DataFrame:
        return self._assets_df.copy()


def test_resolve_research_symbols_filters_to_common_equities_and_keeps_spy() -> None:
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

    symbols = research_prices_module._resolve_research_symbols(context)

    assert symbols == ["AAPL", "SPY"]


def test_bronze_research_prices_daily_writes_alpaca_partition(tmp_path: Path, monkeypatch) -> None:
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

    def fail_yahoo(*args, **kwargs):
        raise AssertionError("Yahoo path should not be used for 2020 partitions")

    monkeypatch.setattr(research_prices_module, "_resolve_research_symbols", fake_resolve_symbols)
    monkeypatch.setattr(
        research_prices_module, "_fetch_alpaca_daily_bars_for_day", fake_fetch_alpaca
    )
    monkeypatch.setattr(research_prices_module, "_fetch_yahoo_daily_bars_for_day", fail_yahoo)

    context = build_asset_context(
        resources={"alpaca": _FakeAlpacaResource()},
        partition_key="2020-01-02",
    )
    research_prices_module.bronze_research_prices_daily(context)

    aapl_path = (
        data_root
        / "bronze"
        / "research_prices_daily"
        / "date=2020-01-02"
        / "symbol=AAPL"
        / "prices.parquet"
    )
    msft_path = (
        data_root
        / "bronze"
        / "research_prices_daily"
        / "date=2020-01-02"
        / "symbol=MSFT"
        / "prices.parquet"
    )
    assert aapl_path.exists()
    assert msft_path.exists()

    aapl_df = pd.read_parquet(aapl_path)
    assert len(aapl_df) == 1
    assert aapl_df.loc[0, "source"] == "alpaca"


def test_bronze_research_prices_daily_writes_yahoo_partition(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    research_prices_module.DATA_ROOT = data_root

    def fake_resolve_symbols(context, configured_symbols=None, max_symbols=None):
        return ["AAPL"]

    def fail_alpaca(*args, **kwargs):
        raise AssertionError("Alpaca path should not be used for pre-2016 partitions")

    def fake_fetch_yahoo(context, partition_date, symbols, request_sleep_seconds):
        assert partition_date == date(2005, 6, 15)
        assert symbols == ["AAPL"]
        return pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "timestamp": [datetime(2005, 6, 15, 20, 0, tzinfo=timezone.utc)],
                "trade_date": [date(2005, 6, 15)],
                "open": [35.0],
                "high": [36.0],
                "low": [34.5],
                "close": [35.5],
                "volume": [500000],
                "trade_count": [pd.NA],
                "vwap": [pd.NA],
                "source": ["yahoo_finance"],
                "ingested_ts": [datetime.now(timezone.utc)],
            }
        )

    monkeypatch.setattr(research_prices_module, "_resolve_research_symbols", fake_resolve_symbols)
    monkeypatch.setattr(research_prices_module, "_fetch_alpaca_daily_bars_for_day", fail_alpaca)
    monkeypatch.setattr(research_prices_module, "_fetch_yahoo_daily_bars_for_day", fake_fetch_yahoo)

    context = build_asset_context(
        resources={"alpaca": _FakeAlpacaResource()},
        partition_key="2005-06-15",
    )
    research_prices_module.bronze_research_prices_daily(context)

    out_path = (
        data_root
        / "bronze"
        / "research_prices_daily"
        / "date=2005-06-15"
        / "symbol=AAPL"
        / "prices.parquet"
    )
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert len(df) == 1
    assert df.loc[0, "source"] == "yahoo_finance"
