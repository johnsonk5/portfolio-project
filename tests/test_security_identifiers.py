import duckdb
import pandas as pd
from dagster import materialize

import portfolio_project.defs.research_db.silver.security_identifiers as identifiers_module
from portfolio_project.defs.research_db.dq_checks import (
    log_security_identifier_mapping_checks,
)
from portfolio_project.defs.research_db.silver.security_identifiers import (
    ASSET_IDENTITY_BRIDGE_COLUMNS,
    ASSET_SYMBOL_BRIDGE_COLUMNS,
    SECURITY_IDENTIFIERS_COLUMNS,
    build_asset_identity_bridge_frame,
    build_asset_symbol_bridge_frame,
    build_research_symbol_identifiers_frame,
    build_security_identifiers_from_assets_df,
    silver_security_identifiers,
)


def test_build_security_identifiers_from_assets_emits_symbol_and_alpaca_rows() -> None:
    frame = build_security_identifiers_from_assets_df(
        pd.DataFrame(
            {
                "asset_id": [1, 2],
                "symbol": ["aapl", "MSFT"],
                "name": ["Apple Inc.", "Microsoft Corp."],
                "alpaca_id": ["alpaca-aapl", None],
                "exchange": ["nasdaq", "NASDAQ"],
            }
        )
    )

    assert list(frame.columns) == SECURITY_IDENTIFIERS_COLUMNS
    rows = frame[
        ["asset_id", "source_symbol", "identifier_type", "identifier_value", "source_priority"]
    ].to_dict("records")
    assert rows == [
        {
            "asset_id": 1,
            "source_symbol": "AAPL",
            "identifier_type": "alpaca_id",
            "identifier_value": "alpaca-aapl",
            "source_priority": 5,
        },
        {
            "asset_id": 1,
            "source_symbol": "AAPL",
            "identifier_type": "symbol",
            "identifier_value": "AAPL",
            "source_priority": 10,
        },
        {
            "asset_id": 2,
            "source_symbol": "MSFT",
            "identifier_type": "symbol",
            "identifier_value": "MSFT",
            "source_priority": 10,
        },
    ]


def test_silver_security_identifiers_materializes_from_portfolio_assets(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(identifiers_module, "DATA_ROOT", tmp_path / "data")
    portfolio_con = duckdb.connect(":memory:")
    research_con = duckdb.connect(":memory:")
    portfolio_con.execute("CREATE SCHEMA silver")
    portfolio_con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            alpaca_id VARCHAR,
            symbol VARCHAR,
            name VARCHAR,
            exchange VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    portfolio_con.execute(
        """
        INSERT INTO silver.assets VALUES
            (1, 'alpaca-aapl', 'AAPL', 'Apple Inc.', 'NASDAQ', TRUE),
            (2, 'alpaca-msft', 'MSFT', 'Microsoft Corp.', 'NASDAQ', TRUE)
        """
    )
    portfolio_con.execute(
        """
        CREATE TABLE silver.ref_sp500 (
            asset_id BIGINT,
            symbol VARCHAR,
            security VARCHAR,
            cik VARCHAR
        )
        """
    )
    portfolio_con.execute(
        """
        INSERT INTO silver.ref_sp500 VALUES
            (1, 'AAPL', 'Apple Inc.', '0000320193')
        """
    )

    result = materialize(
        assets=[silver_security_identifiers],
        resources={"duckdb": portfolio_con, "research_duckdb": research_con},
    )

    assert result.success
    for con in [portfolio_con, research_con]:
        rows = con.execute(
            """
            SELECT asset_id, source_symbol, identifier_type, identifier_value, cik, is_current
            FROM silver.security_identifiers
            ORDER BY asset_id, identifier_type, identifier_value
            """
        ).fetchall()
        assert rows == [
            (1, "AAPL", "alpaca_id", "alpaca-aapl", None, True),
            (1, "AAPL", "cik", "320193", "320193", True),
            (1, "AAPL", "sec_ticker", "AAPL", "320193", True),
            (1, "AAPL", "symbol", "AAPL", None, True),
            (2, "MSFT", "alpaca_id", "alpaca-msft", None, True),
            (2, "MSFT", "symbol", "MSFT", None, True),
        ]

        identity_rows = con.execute(
            """
            SELECT asset_id, current_symbol, source_symbols, alpaca_id, cik
            FROM silver.asset_identity_bridge
            ORDER BY asset_id
            """
        ).fetchall()
        assert identity_rows == [
            (1, "AAPL", "AAPL", "alpaca-aapl", "320193"),
            (2, "MSFT", "MSFT", "alpaca-msft", None),
        ]

        source_rows = con.execute(
            """
            SELECT asset_id, current_symbol, source_symbol, symbol_role, alpaca_id, cik
            FROM silver.asset_symbol_bridge
            ORDER BY asset_id, source_symbol
            """
        ).fetchall()
        assert source_rows == [
            (1, "AAPL", "AAPL", "current", "alpaca-aapl", "320193"),
            (2, "MSFT", "MSFT", "current", "alpaca-msft", None),
        ]

    dq_rows = portfolio_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name LIKE 'dq_security_identifiers_%'
        ORDER BY check_name
        """
    ).fetchall()
    assert dq_rows == [
        ("dq_security_identifiers_cik_to_asset_conflicts", "PASS", 0.0),
        ("dq_security_identifiers_duplicate_asset_id_mappings", "PASS", 0.0),
        ("dq_security_identifiers_missing_asset_id_rates_by_source", "PASS", 0.0),
        ("dq_security_identifiers_symbol_to_asset_conflicts", "PASS", 0.0),
    ]


def test_build_research_symbol_identifiers_assigns_research_only_asset_ids() -> None:
    portfolio_identifiers = build_security_identifiers_from_assets_df(
        pd.DataFrame(
            {
                "asset_id": [1],
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "alpaca_id": ["alpaca-aapl"],
                "exchange": ["NASDAQ"],
            }
        )
    )
    research_symbols = pd.DataFrame(
        {
            "source_symbol": ["AAPL", "ZZZ", "AAA"],
            "first_trade_date": ["2020-01-02", "2021-03-04", "2019-05-06"],
        }
    )
    existing_research = pd.DataFrame({"source_symbol": ["ZZZ"], "asset_id": [42]})

    frame = build_research_symbol_identifiers_frame(
        research_symbols,
        portfolio_identifiers,
        existing_research,
    )

    rows = frame[
        ["asset_id", "source_symbol", "identifier_source", "source_priority", "valid_from_date"]
    ].to_dict("records")
    assert rows == [
        {
            "asset_id": 43,
            "source_symbol": "AAA",
            "identifier_source": "research_daily_prices",
            "source_priority": 50,
            "valid_from_date": pd.Timestamp("2019-05-06").date(),
        },
        {
            "asset_id": 42,
            "source_symbol": "ZZZ",
            "identifier_source": "research_daily_prices",
            "source_priority": 50,
            "valid_from_date": pd.Timestamp("2021-03-04").date(),
        },
    ]


def test_asset_bridge_frames_resolve_current_and_source_symbols() -> None:
    identifiers = pd.DataFrame(
        [
            {
                "asset_id": 1,
                "source_symbol": "AAPL",
                "security_name": "Apple Inc.",
                "identifier_type": "symbol",
                "identifier_value": "AAPL",
                "cik": pd.NA,
                "sec_ticker": pd.NA,
                "alpaca_id": "alpaca-aapl",
                "exchange": "NASDAQ",
                "identifier_source": "portfolio_silver_assets",
                "source_priority": 10,
                "mapping_confidence": 0.95,
                "valid_from_date": pd.Timestamp("1900-01-01").date(),
                "valid_to_date": pd.NaT,
                "is_current": True,
                "ingestion_date": pd.Timestamp("2026-01-01").date(),
                "ingested_ts": pd.Timestamp("2026-01-01"),
            },
            {
                "asset_id": 1,
                "source_symbol": "APPL",
                "security_name": "Apple Inc.",
                "identifier_type": "sec_ticker",
                "identifier_value": "APPL",
                "cik": "0000320193",
                "sec_ticker": "APPL",
                "alpaca_id": pd.NA,
                "exchange": pd.NA,
                "identifier_source": "sec_company_tickers",
                "source_priority": 20,
                "mapping_confidence": 0.8,
                "valid_from_date": pd.Timestamp("1900-01-01").date(),
                "valid_to_date": pd.Timestamp("1980-12-11").date(),
                "is_current": False,
                "ingestion_date": pd.Timestamp("2026-01-01").date(),
                "ingested_ts": pd.Timestamp("2026-01-01"),
            },
        ],
        columns=SECURITY_IDENTIFIERS_COLUMNS,
    )

    identity = build_asset_identity_bridge_frame(identifiers)
    symbol = build_asset_symbol_bridge_frame(identifiers)

    assert list(identity.columns) == ASSET_IDENTITY_BRIDGE_COLUMNS
    assert list(symbol.columns) == ASSET_SYMBOL_BRIDGE_COLUMNS
    assert identity.loc[0, "current_symbol"] == "AAPL"
    assert identity.loc[0, "source_symbols"] == "AAPL,APPL"
    assert identity.loc[0, "alpaca_id"] == "alpaca-aapl"
    assert identity.loc[0, "cik"] == "320193"

    roles = symbol.set_index("source_symbol")["symbol_role"].to_dict()
    assert roles == {"AAPL": "current", "APPL": "historical_or_source"}


def test_security_identifier_mapping_dq_checks_detect_conflicts_and_missing_asset_ids() -> None:
    measured_con = duckdb.connect(":memory:")
    observability_con = duckdb.connect(":memory:")
    measured_con.execute("CREATE SCHEMA silver")
    measured_con.execute(
        """
        CREATE TABLE silver.security_identifiers (
            asset_id BIGINT,
            source_symbol VARCHAR,
            identifier_type VARCHAR,
            identifier_value VARCHAR,
            cik VARCHAR,
            identifier_source VARCHAR,
            valid_from_date DATE,
            valid_to_date DATE,
            is_current BOOLEAN
        )
        """
    )
    measured_con.execute(
        """
        INSERT INTO silver.security_identifiers VALUES
            (1, 'AAPL', 'symbol', 'AAPL', NULL, 'portfolio_silver_assets',
                DATE '1900-01-01', NULL, TRUE),
            (1, 'AAPL', 'symbol', 'AAPL', NULL, 'portfolio_silver_assets',
                DATE '1900-01-01', NULL, TRUE),
            (2, 'AAPL', 'symbol', 'AAPL', NULL, 'bad_symbol_source',
                DATE '1900-01-01', NULL, TRUE),
            (1, 'AAPL', 'cik', '0000320193', '0000320193', 'sp500_wikipedia',
                DATE '1900-01-01', NULL, TRUE),
            (3, 'APPL', 'cik', '320193', '320193', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (NULL, 'MSFT', 'symbol', 'MSFT', NULL, 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE)
        """
    )

    log_security_identifier_mapping_checks(
        measured_con=measured_con,
        observability_con=observability_con,
        run_id="run-1",
        job_name="security_identifiers_job",
    )

    rows = observability_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        ORDER BY check_name
        """
    ).fetchall()
    assert rows == [
        ("dq_security_identifiers_cik_to_asset_conflicts", "FAIL", 1.0),
        ("dq_security_identifiers_duplicate_asset_id_mappings", "FAIL", 1.0),
        ("dq_security_identifiers_missing_asset_id_rates_by_source", "FAIL", 0.5),
        ("dq_security_identifiers_symbol_to_asset_conflicts", "FAIL", 1.0),
    ]
