import duckdb
import pandas as pd
from dagster import materialize

import portfolio_project.defs.research_db.silver.security_identifiers as identifiers_module
from portfolio_project.defs.research_db.silver.security_identifiers import (
    SECURITY_IDENTIFIERS_COLUMNS,
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

    result = materialize(
        assets=[silver_security_identifiers],
        resources={"duckdb": portfolio_con, "research_duckdb": research_con},
    )

    assert result.success
    rows = research_con.execute(
        """
        SELECT asset_id, source_symbol, identifier_type, identifier_value, is_current
        FROM silver.security_identifiers
        ORDER BY asset_id, identifier_type
        """
    ).fetchall()
    assert rows == [
        (1, "AAPL", "alpaca_id", "alpaca-aapl", True),
        (1, "AAPL", "symbol", "AAPL", True),
        (2, "MSFT", "alpaca_id", "alpaca-msft", True),
        (2, "MSFT", "symbol", "MSFT", True),
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
