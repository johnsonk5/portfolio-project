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
    build_manual_security_identifier_overrides_frame,
    build_research_symbol_identifiers_frame,
    build_sec_company_ticker_identifiers_frame,
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


def test_silver_security_identifiers_materializes_sec_company_tickers(
    tmp_path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(identifiers_module, "DATA_ROOT", data_root)
    tickers_path = (
        data_root
        / "bronze"
        / "sec_company_tickers"
        / "ingestion_date=2026-01-02"
        / "tickers.parquet"
    )
    tickers_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "cik": ["0000320193", "0001018724"],
            "name": ["Apple Inc.", "Amazon.com Inc."],
            "ticker": ["AAPL", "AMZN"],
            "exchange": ["Nasdaq", "Nasdaq"],
            "ingestion_date": [pd.Timestamp("2026-01-02").date()] * 2,
            "source_file": ["company_tickers_exchange.json"] * 2,
            "source_content_hash": ["hash"] * 2,
            "source_row_number": [1, 2],
            "ingested_ts": [pd.Timestamp("2026-01-02 00:00:00")] * 2,
        }
    ).to_parquet(tickers_path, index=False)

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

    result = materialize(
        assets=[silver_security_identifiers],
        resources={"duckdb": portfolio_con, "research_duckdb": research_con},
    )

    assert result.success
    rows = research_con.execute(
        """
        SELECT asset_id, source_symbol, identifier_type, identifier_value, cik, identifier_source
        FROM silver.security_identifiers
        WHERE identifier_source = 'sec_company_tickers'
        ORDER BY asset_id, identifier_type
        """
    ).fetchall()
    assert rows == [
        (1, "AAPL", "cik", "320193", "320193", "sec_company_tickers"),
        (1, "AAPL", "sec_ticker", "AAPL", "320193", "sec_company_tickers"),
    ]

    identity_row = research_con.execute(
        """
        SELECT asset_id, current_symbol, cik
        FROM silver.asset_identity_bridge
        WHERE asset_id = 1
        """
    ).fetchone()
    assert identity_row == (1, "AAPL", "320193")


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


def test_manual_security_identifier_overrides_map_research_only_symbols() -> None:
    research_identifiers = build_research_symbol_identifiers_frame(
        pd.DataFrame(
            {
                "source_symbol": ["HES"],
                "first_trade_date": ["2000-01-03"],
            }
        ),
        pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS),
        pd.DataFrame({"source_symbol": ["HES"], "asset_id": [43037]}),
    )
    overrides = pd.DataFrame(
        {
            "source_symbol": ["HES"],
            "canonical_symbol": ["HES"],
            "security_name": ["Hess Corp"],
            "cik": ["0000004447"],
            "sec_ticker": ["HES"],
            "exchange": ["NYSE"],
            "mapping_source": ["manual_review_eodhd_sec_symbol"],
            "confidence": [0.9],
            "notes": ["Reviewed exact historical ticker; not a company-name-only match."],
        }
    )

    frame = build_manual_security_identifier_overrides_frame(overrides, research_identifiers)

    assert frame[
        ["asset_id", "source_symbol", "identifier_type", "identifier_value", "cik"]
    ].to_dict("records") == [
        {
            "asset_id": 43037,
            "source_symbol": "HES",
            "identifier_type": "cik",
            "identifier_value": "4447",
            "cik": "4447",
        },
        {
            "asset_id": 43037,
            "source_symbol": "HES",
            "identifier_type": "sec_ticker",
            "identifier_value": "HES",
            "cik": "4447",
        },
    ]


def test_manual_security_identifier_overrides_skip_ambiguous_symbols() -> None:
    mapped_identifiers = pd.DataFrame(
        [
            {
                "asset_id": 1,
                "source_symbol": "ABC",
                "identifier_type": "symbol",
                "identifier_source": "research_daily_prices",
                "alpaca_id": pd.NA,
            },
            {
                "asset_id": 2,
                "source_symbol": "ABC",
                "identifier_type": "symbol",
                "identifier_source": "research_daily_prices",
                "alpaca_id": pd.NA,
            },
        ]
    )
    overrides = pd.DataFrame(
        {
            "source_symbol": ["ABC"],
            "canonical_symbol": ["ABC"],
            "cik": ["1234"],
            "sec_ticker": ["ABC"],
            "mapping_source": ["manual_review"],
            "confidence": [0.95],
        }
    )

    frame = build_manual_security_identifier_overrides_frame(overrides, mapped_identifiers)

    assert frame.empty


def test_manual_security_identifier_overrides_require_research_universe_symbol() -> None:
    mapped_identifiers = pd.DataFrame(
        [
            {
                "asset_id": 1,
                "source_symbol": "ABC",
                "identifier_type": "symbol",
                "identifier_source": "portfolio_silver_assets",
                "alpaca_id": "alpaca-abc",
            }
        ]
    )
    overrides = pd.DataFrame(
        {
            "source_symbol": ["ABC"],
            "canonical_symbol": ["ABC"],
            "cik": ["1234"],
            "sec_ticker": ["ABC"],
            "mapping_source": ["manual_review"],
            "confidence": [0.95],
        }
    )

    frame = build_manual_security_identifier_overrides_frame(overrides, mapped_identifiers)

    assert frame.empty


def test_build_sec_company_ticker_identifiers_maps_existing_project_symbols() -> None:
    portfolio_identifiers = build_security_identifiers_from_assets_df(
        pd.DataFrame(
            {
                "asset_id": [1, 2],
                "symbol": ["AAPL", "MSFT"],
                "name": ["Apple Inc.", "Microsoft Corp."],
                "alpaca_id": ["alpaca-aapl", "alpaca-msft"],
                "exchange": ["NASDAQ", "NASDAQ"],
            }
        )
    )
    sec_tickers = pd.DataFrame(
        {
            "cik": ["0000320193", "0000789019", "0001018724"],
            "name": ["Apple Inc.", "Microsoft Corp.", "Amazon.com Inc."],
            "ticker": ["aapl", "MSFT", "AMZN"],
            "exchange": ["Nasdaq", "Nasdaq", "Nasdaq"],
            "ingestion_date": ["2026-01-02", "2026-01-02", "2026-01-02"],
            "ingested_ts": pd.to_datetime(
                ["2026-01-02 00:00:00", "2026-01-02 00:00:00", "2026-01-02 00:00:00"]
            ),
        }
    )

    frame = build_sec_company_ticker_identifiers_frame(sec_tickers, portfolio_identifiers)

    assert list(frame.columns) == SECURITY_IDENTIFIERS_COLUMNS
    rows = frame[
        [
            "asset_id",
            "source_symbol",
            "identifier_type",
            "identifier_value",
            "cik",
            "identifier_source",
            "source_priority",
        ]
    ].to_dict("records")
    assert rows == [
        {
            "asset_id": 1,
            "source_symbol": "AAPL",
            "identifier_type": "cik",
            "identifier_value": "320193",
            "cik": "320193",
            "identifier_source": "sec_company_tickers",
            "source_priority": 15,
        },
        {
            "asset_id": 1,
            "source_symbol": "AAPL",
            "identifier_type": "sec_ticker",
            "identifier_value": "AAPL",
            "cik": "320193",
            "identifier_source": "sec_company_tickers",
            "source_priority": 15,
        },
        {
            "asset_id": 2,
            "source_symbol": "MSFT",
            "identifier_type": "cik",
            "identifier_value": "789019",
            "cik": "789019",
            "identifier_source": "sec_company_tickers",
            "source_priority": 15,
        },
        {
            "asset_id": 2,
            "source_symbol": "MSFT",
            "identifier_type": "sec_ticker",
            "identifier_value": "MSFT",
            "cik": "789019",
            "identifier_source": "sec_company_tickers",
            "source_priority": 15,
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
        ("dq_security_identifiers_cik_to_asset_conflicts", "PASS", 0.0),
        ("dq_security_identifiers_duplicate_asset_id_mappings", "FAIL", 1.0),
        ("dq_security_identifiers_missing_asset_id_rates_by_source", "FAIL", 0.5),
        ("dq_security_identifiers_symbol_to_asset_conflicts", "FAIL", 1.0),
    ]


def test_security_identifier_cik_dq_allows_shared_cik_share_classes() -> None:
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
            (10, 'GOOG', 'cik', '1652044', '1652044', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (11, 'GOOGL', 'cik', '1652044', '1652044', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (20, 'FOX', 'cik', '1754301', '1754301', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (21, 'FOXA', 'cik', '1754301', '1754301', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (30, 'NWS', 'cik', '1564708', '1564708', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (31, 'NWSA', 'cik', '1564708', '1564708', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE)
        """
    )

    log_security_identifier_mapping_checks(
        measured_con=measured_con,
        observability_con=observability_con,
        run_id="run-1",
        job_name="security_identifiers_job",
    )

    row = observability_con.execute(
        """
        SELECT status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_security_identifiers_cik_to_asset_conflicts'
        """
    ).fetchone()
    assert row == ("PASS", 0.0)


def test_security_identifier_cik_dq_fails_ambiguous_same_cik_and_source_symbol() -> None:
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
            (10, 'GOOG', 'cik', '1652044', '1652044', 'sec_company_tickers',
                DATE '1900-01-01', NULL, TRUE),
            (11, 'GOOG', 'cik', '1652044', '1652044', 'bad_identifier_source',
                DATE '1900-01-01', NULL, TRUE)
        """
    )

    log_security_identifier_mapping_checks(
        measured_con=measured_con,
        observability_con=observability_con,
        run_id="run-1",
        job_name="security_identifiers_job",
    )

    row = observability_con.execute(
        """
        SELECT status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_security_identifiers_cik_to_asset_conflicts'
        """
    ).fetchone()
    assert row == ("FAIL", 1.0)
