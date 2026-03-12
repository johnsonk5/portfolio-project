from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

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


def _write_fixture_silver_prices(
    data_root: Path,
    partition_key: str,
    symbol: str,
    frame: pd.DataFrame,
) -> None:
    out_dir = data_root / "silver" / "prices" / f"date={partition_key}" / f"symbol={symbol}"
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_dir / "prices.parquet", index=False)


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
def test_silver_prices_rerun_replaces_partition_and_removes_stale_symbol_files(
    tmp_path: Path,
) -> None:
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
def test_silver_prices_dedupes_duplicate_active_symbols_in_assets(
    tmp_path: Path,
) -> None:
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
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)")
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (99, 'AAPL', TRUE)")

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
    out_path = (
        data_root / "silver" / "prices" / f"date={partition_key}" / "symbol=AAPL" / "prices.parquet"
    )
    df = pd.read_parquet(out_path)
    assert len(df) == 2
    assert set(df["asset_id"].tolist()) == {1}


@pytest.mark.smoke
def test_gold_prices_computes_vwap_returns_and_sentiment(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    data_root = tmp_path / "data"
    gold_prices_module.DATA_ROOT = data_root

    silver_df = pd.DataFrame(
        {
            "asset_id": [1, 1, 1],
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "timestamp": [
                datetime(2026, 2, 13, 14, 31, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 14, 35, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 14, 40, tzinfo=timezone.utc),
            ],
            "open": [100.0, 101.0, 101.5],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [101.0, 101.5, 102.0],
            "volume": [10, 70, 20],
            "trade_count": [5, 20, 8],
            "vwap": [100.0, None, 102.0],
            "ingested_ts": [
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            ],
        }
    )
    _write_fixture_silver_prices(data_root, partition_key, "AAPL", silver_df)

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
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)")
    con.execute(
        """
        CREATE TABLE silver.ref_publishers (
            publisher_id BIGINT,
            publisher_weight DOUBLE
        )
        """
    )
    con.execute("INSERT INTO silver.ref_publishers VALUES (1, 2.0), (2, 1.0)")

    gold_prices_module._ensure_gold_table(con)
    con.execute(
        """
        INSERT INTO gold.prices (
            asset_id, symbol, trade_date, open, high, low, close, volume,
            trade_count, vwap, dollar_volume, returns_1d, returns_5d, returns_21d,
            realized_vol_21d, momentum_12_1, pct_below_52w_high, sma_50, sma_200,
            dist_sma_50, dist_sma_200, sentiment_score
        )
        VALUES (
            1, 'AAPL', '2026-02-12', 99, 101, 98, 100, 1000, 10, 100, 100000,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
        """
    )

    con.execute(
        "CREATE TABLE gold.headlines (asset_id BIGINT, publisher_id BIGINT, provider_publish_time TIMESTAMP, sentiment VARCHAR)"
    )
    con.execute(
        """
        INSERT INTO gold.headlines VALUES
            (1, 1, '2026-02-12 15:00:00', 'positive'),
            (1, 2, '2026-02-11 15:00:00', 'negative')
        """
    )

    result = materialize(
        assets=[
            gold_alpaca_prices,
            SourceAsset(AssetKey("silver_alpaca_assets")),
            SourceAsset(AssetKey("silver_alpaca_prices_parquet")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )
    assert result.success

    row = con.execute(
        """
        SELECT close, vwap, returns_1d, sentiment_score
        FROM gold.prices
        WHERE asset_id = 1 AND trade_date = ?
        """,
        [partition_key],
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(102.0)
    assert row[1] == pytest.approx((100.0 * 10 + 102.0 * 20) / 30.0)
    assert row[2] == pytest.approx(0.02)
    assert row[3] == pytest.approx(1.0 / 3.0)


@pytest.mark.smoke
def test_gold_prices_upsert_rolls_back_on_insert_error(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    partition_date = date(2026, 2, 13)
    data_root = tmp_path / "data"
    gold_prices_module.DATA_ROOT = data_root

    broken_silver_df = pd.DataFrame(
        {
            "asset_id": [1],
            "symbol": ["AAPL"],
            "timestamp": [datetime(2026, 2, 13, 14, 31, tzinfo=timezone.utc)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10],
            "ingested_ts": [datetime.now(timezone.utc)],
        }
    )
    _write_fixture_silver_prices(data_root, partition_key, "AAPL", broken_silver_df)

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
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)")

    gold_prices_module._ensure_gold_table(con)
    con.execute(
        """
        INSERT INTO gold.prices (
            asset_id, symbol, trade_date, open, high, low, close, volume,
            trade_count, vwap, dollar_volume, returns_1d, returns_5d, returns_21d,
            realized_vol_21d, momentum_12_1, pct_below_52w_high, sma_50, sma_200,
            dist_sma_50, dist_sma_200, sentiment_score
        )
        VALUES (
            1, 'AAPL', ?, 99, 101, 98, 200, 1000, 10, 100, 200000,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
        """,
        [partition_key],
    )

    context = SimpleNamespace(
        resources=SimpleNamespace(duckdb=con),
        log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
    )

    with pytest.raises(Exception):
        gold_prices_module._upsert_gold_for_day(context, partition_date)

    rows_after = con.execute(
        "SELECT count(*) FROM gold.prices WHERE trade_date = ?",
        [partition_key],
    ).fetchone()[0]
    close_after = con.execute(
        "SELECT close FROM gold.prices WHERE trade_date = ? AND asset_id = 1",
        [partition_key],
    ).fetchone()[0]
    assert rows_after == 1
    assert close_after == pytest.approx(200.0)
