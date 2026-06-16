from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from dagster import build_asset_context

import portfolio_project.defs.research_db.gold.fundamentals as fundamentals_module


def _seed_statement_items(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    frame = pd.DataFrame(rows)
    con.register("statement_items_df", frame)
    con.execute(
        "CREATE OR REPLACE TABLE silver.sec_statement_items AS SELECT * FROM statement_items_df"
    )


def _statement_item(
    *,
    metric: str,
    value: float,
    accession_number: str = "0001",
    filing_date: str = "2024-02-16",
    acceptance_datetime: str = "2024-02-16 18:00:00",
    availability_date: str = "2024-02-16",
    source_snapshot_date: str = "2024-02-17",
) -> dict:
    return {
        "asset_id": 1,
        "cik": "123",
        "accession_number": accession_number,
        "canonical_metric": metric,
        "statement_type": "income_statement",
        "taxonomy": "us-gaap",
        "tag": metric,
        "unit": "USD",
        "reported_value": value,
        "value": value,
        "canonical_sign_rule": "preserve_reported_sign",
        "period_start_date": "2023-10-01",
        "period_end_date": "2023-12-31",
        "period_type": "duration",
        "fiscal_year": 2023,
        "fiscal_period": "Q4",
        "form": "10-K",
        "filing_date": filing_date,
        "acceptance_datetime": acceptance_datetime,
        "availability_date": availability_date,
        "mapping_version": "test",
        "mapping_priority": 10,
        "source_expression": metric,
        "source_accession_number": accession_number,
        "source_form": "10-K",
        "source_filed_date": filing_date,
        "source_acceptance_datetime": acceptance_datetime,
        "period_match_type": "exact_quarter",
        "is_component_sum": False,
        "is_fallback_concept": False,
        "is_restricted_cash_included": False,
        "is_lease_inclusive_debt": False,
        "is_ytd_derived_quarter": False,
        "source_snapshot_date": source_snapshot_date,
        "ingested_ts": "2024-02-17 00:00:00",
    }


def _write_price(data_root: Path, trade_date: str, close: float = 20.0) -> None:
    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / f"month={trade_date[:7]}"
        / f"date={trade_date}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "asset_id": [1],
            "symbol": ["AAA"],
            "timestamp": [pd.Timestamp(f"{trade_date}T21:00:00Z")],
            "trade_date": [trade_date],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "adjusted_close": [close],
            "volume": [1000],
            "trade_count": [10],
            "vwap": [close],
            "dollar_volume": [close * 1000],
            "source": ["test"],
            "ingested_ts": [pd.Timestamp(f"{trade_date}T21:01:00Z")],
        }
    ).to_parquet(out_path, index=False)


def test_fundamentals_quarterly_pivots_statement_items_and_prefers_latest_filing() -> None:
    con = duckdb.connect(":memory:")
    _seed_statement_items(
        con,
        [
            _statement_item(metric="revenue", value=100.0, accession_number="old"),
            _statement_item(metric="net_income", value=10.0, accession_number="old"),
            _statement_item(
                metric="revenue",
                value=120.0,
                accession_number="amended",
                filing_date="2024-03-01",
                acceptance_datetime="2024-03-01 18:00:00",
                availability_date="2024-03-01",
                source_snapshot_date="2024-03-02",
            ),
            _statement_item(
                metric="net_income",
                value=12.0,
                accession_number="amended",
                filing_date="2024-03-01",
                acceptance_datetime="2024-03-01 18:00:00",
                availability_date="2024-03-01",
                source_snapshot_date="2024-03-02",
            ),
        ],
    )

    context = build_asset_context(resources={"research_duckdb": con})
    fundamentals_module.gold_fundamentals_quarterly(context)

    rows = con.execute(
        """
        SELECT
            asset_id,
            cik,
            fiscal_year,
            fiscal_quarter,
            period_end_date,
            accession_number,
            revenue,
            net_income,
            statement_items_count
        FROM gold.fundamentals_quarterly
        """
    ).fetchall()
    assert rows == [(1, "123", 2023, "Q4", date(2023, 12, 31), "amended", 120.0, 12.0, 2)]


def test_fundamental_signals_daily_respects_availability_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(fundamentals_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(
        fundamentals_module,
        "PRICE_GLOB",
        (data_root / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet").as_posix(),
    )
    monkeypatch.setattr(fundamentals_module, "FUNDAMENTAL_SIGNAL_VERSION", "test-v1")

    _write_price(data_root, "2024-02-15", close=20.0)
    _write_price(data_root, "2024-02-16", close=21.0)
    _write_price(data_root, "2024-02-20", close=22.0)

    con = duckdb.connect(":memory:")
    _seed_statement_items(
        con,
        [
            _statement_item(metric="revenue", value=100.0),
            _statement_item(metric="net_income", value=10.0),
            _statement_item(metric="assets", value=500.0),
            _statement_item(metric="equity", value=250.0),
            _statement_item(metric="debt", value=100.0),
            _statement_item(metric="cash", value=50.0),
            _statement_item(metric="operating_cash_flow", value=15.0),
            _statement_item(metric="capex", value=5.0),
            _statement_item(metric="diluted_shares", value=10.0),
            _statement_item(metric="diluted_eps", value=1.0),
        ],
    )
    context = build_asset_context(resources={"research_duckdb": con})

    fundamentals_module.gold_fundamentals_quarterly(context)
    fundamentals_module.gold_fundamental_signals_daily(context)

    rows = con.execute(
        """
        SELECT
            date,
            has_fundamentals,
            cik,
            fiscal_quarter,
            days_since_filing,
            close,
            market_cap,
            revenue_ttm,
            free_cash_flow_ttm,
            price_to_sales,
            signal_version
        FROM gold.fundamental_signals_daily
        ORDER BY date
        """
    ).fetchall()

    assert rows == [
        (date(2024, 2, 16), True, "123", "Q4", 0, 21.0, 210.0, 100.0, 10.0, 2.1, "test-v1"),
        (date(2024, 2, 20), True, "123", "Q4", 4, 22.0, 220.0, 100.0, 10.0, 2.2, "test-v1"),
    ]
