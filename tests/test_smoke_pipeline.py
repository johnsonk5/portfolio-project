from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from dagster import AssetKey, SourceAsset, materialize

import portfolio_project.defs.gold_prices as gold_prices_module
import portfolio_project.defs.silver_prices as silver_prices_module
from portfolio_project.defs.gold_prices import gold_alpaca_prices
from portfolio_project.defs.silver_prices import silver_alpaca_prices_parquet


def _write_fixture_bronze_bars(data_root: Path, partition_key: str, symbols: list[str]) -> None:
    for symbol in symbols:
        bronze_dir = (
            data_root / "bronze" / "alpaca_bars" / f"date={partition_key}" / f"symbol={symbol}"
        )
        bronze_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(
            {
                "symbol": [symbol, symbol],
                "timestamp": [
                    datetime(2026, 2, 13, 14, 35, tzinfo=timezone.utc),
                    datetime(2026, 2, 13, 14, 40, tzinfo=timezone.utc),
                ],
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.5, 100.5],
                "close": [100.8, 101.6],
                "volume": [1000, 1200],
                "trade_count": [12, 15],
                "vwap": [100.6, 101.4],
                "ingested_ts": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            }
        )
        df.to_parquet(bronze_dir / "bars.parquet", index=False)


@pytest.mark.smoke
def test_daily_prices_path_materializes_with_three_tickers(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    symbols = ["AAPL", "MSFT", "NVDA"]

    data_root = tmp_path / "data"
    _write_fixture_bronze_bars(data_root, partition_key, symbols)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    for idx, symbol in enumerate(symbols, start=1):
        con.execute(
            "INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (?, ?, TRUE)",
            [idx, symbol],
        )

    silver_prices_module.DATA_ROOT = data_root
    gold_prices_module.DATA_ROOT = data_root

    result = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            gold_alpaca_prices,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )

    assert result.success
    silver_glob = (
        data_root / "silver" / "prices" / f"date={partition_key}" / "symbol=*" / "prices.parquet"
    ).as_posix()
    silver_row = con.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [silver_glob],
    ).fetchone()
    gold_row = con.execute(
        "SELECT count(*) FROM gold.prices WHERE trade_date = ?",
        [partition_key],
    ).fetchone()
    assert silver_row is not None
    assert gold_row is not None
    silver_rows = silver_row[0]
    gold_rows = gold_row[0]
    assert silver_rows == len(symbols) * 2
    assert gold_rows == len(symbols)


@pytest.mark.smoke
def test_silver_prices_rerun_replaces_partition_and_removes_stale_symbol_files(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    first_symbols = ["AAPL", "MSFT", "NVDA"]
    second_symbols = ["AAPL", "MSFT"]

    data_root = tmp_path / "data"
    _write_fixture_bronze_bars(data_root, partition_key, first_symbols)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    for idx, symbol in enumerate(first_symbols, start=1):
        con.execute(
            "INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (?, ?, TRUE)",
            [idx, symbol],
        )

    silver_prices_module.DATA_ROOT = data_root

    result_first = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )
    assert result_first.success

    con.execute("UPDATE silver.assets SET is_active = FALSE WHERE symbol = 'NVDA'")
    _write_fixture_bronze_bars(data_root, partition_key, second_symbols)

    result_second = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )
    assert result_second.success

    day_dir = data_root / "silver" / "prices" / f"date={partition_key}"
    written_symbols = sorted(path.name.split("=", 1)[1] for path in day_dir.glob("symbol=*"))
    assert written_symbols == sorted(second_symbols)
    assert not (day_dir / "symbol=NVDA" / "prices.parquet").exists()


@pytest.mark.smoke
def test_silver_prices_dedupes_duplicate_active_symbols_in_assets(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    symbol = "AAPL"

    data_root = tmp_path / "data"
    _write_fixture_bronze_bars(data_root, partition_key, [symbol])

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    con.execute(
        "INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)"
    )
    con.execute(
        "INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (99, 'AAPL', TRUE)"
    )

    silver_prices_module.DATA_ROOT = data_root

    result = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )

    assert result.success
    out_path = data_root / "silver" / "prices" / f"date={partition_key}" / "symbol=AAPL" / "prices.parquet"
    df = pd.read_parquet(out_path)
    assert len(df) == 2
    assert set(df["asset_id"].tolist()) == {1}
