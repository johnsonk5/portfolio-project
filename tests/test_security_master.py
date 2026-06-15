from pathlib import Path

import duckdb
import pandas as pd
from dagster import AssetKey, SourceAsset, materialize

import portfolio_project.defs.research_db.silver.security_master as security_master_module
from portfolio_project.defs.research_db.silver.security_master import (
    SECURITY_MASTER_COLUMNS,
    build_security_master_frame,
    silver_security_master,
)


def test_build_security_master_classifies_core_security_types() -> None:
    assets_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "asset_id": 1,
                "name": "Apple Inc. Common Stock",
                "asset_class": "us_equity",
                "exchange": "NASDAQ",
                "tradable": True,
            },
            {
                "symbol": "SPY",
                "name": "SPDR S&P 500 ETF Trust",
                "asset_class": "us_equity",
                "exchange": "ARCA",
                "tradable": True,
            },
            {
                "symbol": "ABCQ",
                "name": "ABC Holdings Chapter 11 Liquidation",
                "asset_class": "us_equity",
                "exchange": "OTCQX",
                "tradable": True,
            },
            {
                "symbol": "XYZW",
                "name": "XYZ Corp Warrant",
                "asset_class": "us_equity",
                "exchange": "NYSE",
                "tradable": True,
            },
            {
                "symbol": "TM",
                "name": "Toyota Motor Corporation American Depositary Shares",
                "asset_class": "us_equity",
                "exchange": "NYSE",
                "tradable": True,
            },
        ]
    )

    frame = build_security_master_frame(assets_df)
    by_symbol = frame.set_index("symbol").to_dict("index")

    assert list(frame.columns) == SECURITY_MASTER_COLUMNS
    assert by_symbol["AAPL"]["security_subtype"] == "common_stock"
    assert bool(by_symbol["AAPL"]["is_investable_common_equity"]) is True
    assert bool(by_symbol["SPY"]["is_etf"]) is True
    assert bool(by_symbol["SPY"]["is_fund_like"]) is True
    assert bool(by_symbol["SPY"]["is_investable_common_equity"]) is False
    assert bool(by_symbol["ABCQ"]["is_otc"]) is True
    assert bool(by_symbol["ABCQ"]["is_bankruptcy_related"]) is True
    assert bool(by_symbol["XYZW"]["is_derivative_security"]) is True
    assert bool(by_symbol["TM"]["is_adr"]) is True


def test_silver_security_master_materializes_from_research_prices_and_signals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(security_master_module, "DATA_ROOT", data_root)
    prices_path = (
        data_root / "silver" / "research_daily_prices" / "month=2026-02" / "date=2026-02-13.parquet"
    )
    prices_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["AAPL", "QQQ"],
            "trade_date": ["2026-02-13", "2026-02-13"],
            "close": [100.0, 500.0],
            "dollar_volume": [1_000_000.0, 2_000_000.0],
        }
    ).to_parquet(prices_path, index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.signals_daily (
            date DATE,
            symbol VARCHAR,
            close DOUBLE,
            signal_version VARCHAR,
            load_timestamp TIMESTAMP
        )
        """
    )
    con.executemany(
        """
        INSERT INTO silver.signals_daily (
            date,
            symbol,
            close,
            signal_version,
            load_timestamp
        )
        VALUES (?, ?, ?, ?, current_timestamp)
        """,
        [
            ("2026-02-13", "MSFT", 300.0, "test-v1"),
            ("2026-02-13", "QQQ", 500.0, "test-v1"),
        ],
    )

    result = materialize(
        assets=[
            silver_security_master,
            SourceAsset(AssetKey(["silver", "signals_daily"])),
            SourceAsset(AssetKey(["silver", "security_identifiers"])),
        ],
        resources={"research_duckdb": con},
    )

    assert result.success
    columns = [row[0] for row in con.execute("DESCRIBE silver.security_master").fetchall()]
    assert columns == SECURITY_MASTER_COLUMNS

    rows = con.execute(
        """
        SELECT asset_id, symbol, security_subtype, is_investable_common_equity
        FROM silver.security_master
        ORDER BY symbol
        """
    ).fetchall()
    assert rows == [
        (None, "AAPL", "common_stock", True),
        (None, "MSFT", "common_stock", True),
        (None, "QQQ", "etf", False),
    ]


def test_silver_security_master_enriches_from_high_confidence_sec_identifiers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(security_master_module, "DATA_ROOT", data_root)
    prices_path = (
        data_root / "silver" / "research_daily_prices" / "month=2026-02" / "date=2026-02-13.parquet"
    )
    prices_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "trade_date": ["2026-02-13"],
            "close": [100.0],
        }
    ).to_parquet(prices_path, index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.security_identifiers (
            asset_id BIGINT,
            source_symbol VARCHAR,
            security_name VARCHAR,
            identifier_type VARCHAR,
            identifier_value VARCHAR,
            cik VARCHAR,
            sec_ticker VARCHAR,
            alpaca_id VARCHAR,
            exchange VARCHAR,
            identifier_source VARCHAR,
            source_priority INTEGER,
            mapping_confidence DOUBLE,
            valid_from_date DATE,
            valid_to_date DATE,
            is_current BOOLEAN,
            source_snapshot_date DATE,
            ingestion_date DATE,
            ingested_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.security_identifiers VALUES
            (
                1,
                'AAPL',
                'Apple Inc.',
                'cik',
                '320193',
                '320193',
                'AAPL',
                NULL,
                'Nasdaq',
                'sec_company_tickers',
                15,
                0.9,
                DATE '1900-01-01',
                NULL,
                TRUE,
                DATE '2026-01-02',
                DATE '2026-01-02',
                TIMESTAMP '2026-01-02 00:00:00'
            )
        """
    )

    result = materialize(
        assets=[
            silver_security_master,
            SourceAsset(AssetKey(["silver", "signals_daily"])),
            SourceAsset(AssetKey(["silver", "security_identifiers"])),
        ],
        resources={"research_duckdb": con},
    )

    assert result.success
    row = con.execute(
        """
        SELECT
            asset_id,
            symbol,
            security_name,
            cik,
            sec_ticker,
            exchange,
            identifier_source,
            identifier_confidence,
            identifier_source_snapshot_date,
            classification_source
        FROM silver.security_master
        WHERE symbol = 'AAPL'
        """
    ).fetchone()
    assert row == (
        1,
        "AAPL",
        "Apple Inc.",
        "320193",
        "AAPL",
        "NASDAQ",
        "sec_company_tickers",
        0.9,
        pd.Timestamp("2026-01-02").date(),
        "sec_identifier_enriched_v1",
    )
