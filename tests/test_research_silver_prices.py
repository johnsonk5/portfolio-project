from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.portfolio_db.silver.research_prices as research_silver_prices_module


def _write_bronze_prices(
    data_root: Path,
    dataset_name: str,
    partition_key: str,
    frame: pd.DataFrame,
) -> None:
    out_path = (
        data_root
        / "bronze"
        / dataset_name
        / f"date={partition_key}"
        / "prices.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_path, index=False)


def test_research_daily_prices_prefers_alpaca_on_overlap(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    _write_bronze_prices(
        data_root,
        "alpaca_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "timestamp": [
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                ],
                "trade_date": ["2026-02-13", "2026-02-13"],
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
        ),
    )
    _write_bronze_prices(
        data_root,
        "eodhd_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL", "NVDA"],
                "timestamp": [
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                ],
                "trade_date": ["2026-02-13", "2026-02-13"],
                "open": [98.0, 300.0],
                "high": [99.0, 301.0],
                "low": [97.0, 299.0],
                "close": [98.5, 300.5],
                "adjusted_close": [98.4, 300.4],
                "volume": [9999, 3000],
                "trade_count": [pd.NA, pd.NA],
                "vwap": [pd.NA, pd.NA],
                "source": ["eodhd", "eodhd"],
                "ingested_ts": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        ),
    )

    context = build_asset_context(partition_key=partition_key)
    research_silver_prices_module.silver_research_daily_prices(context)

    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / "month=2026-02"
        / f"date={partition_key}.parquet"
    )
    assert out_path.exists()

    out_df = pd.read_parquet(out_path).sort_values("symbol").reset_index(drop=True)
    assert out_df["symbol"].tolist() == ["AAPL", "MSFT", "NVDA"]
    assert out_df["source"].tolist() == ["alpaca", "alpaca", "eodhd"]
    assert float(out_df.loc[out_df["symbol"] == "AAPL", "close"].iloc[0]) == 100.5
    assert float(out_df.loc[out_df["symbol"] == "AAPL", "dollar_volume"].iloc[0]) == 100500.0


def test_research_daily_prices_rerun_replaces_stale_partition(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    first_frame = pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "timestamp": [
                datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
            ],
            "trade_date": ["2026-02-13", "2026-02-13"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "adjusted_close": [pd.NA, pd.NA],
            "volume": [1000, 2000],
            "trade_count": [10, 20],
            "vwap": [100.2, 200.2],
            "source": ["alpaca", "alpaca"],
            "ingested_ts": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
        }
    )
    second_frame = first_frame[first_frame["symbol"] == "AAPL"].copy()

    _write_bronze_prices(data_root, "alpaca_prices_daily", partition_key, first_frame)
    context = build_asset_context(partition_key=partition_key)
    research_silver_prices_module.silver_research_daily_prices(context)

    _write_bronze_prices(data_root, "alpaca_prices_daily", partition_key, second_frame)
    research_silver_prices_module.silver_research_daily_prices(context)

    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / "month=2026-02"
        / f"date={partition_key}.parquet"
    )
    out_df = pd.read_parquet(out_path)
    assert out_df["symbol"].tolist() == ["AAPL"]
