import duckdb
import pandas as pd
from dagster import materialize

import portfolio_project.defs.research_db.silver.sec as sec_silver_module
from portfolio_project.defs.research_db.silver.sec import (
    SEC_FACTS_LONG_COLUMNS,
    SEC_SUBMISSIONS_COLUMNS,
    _bronze_sec_facts_files,
    _bronze_sec_submissions_files,
    build_cik_asset_id_map_frame,
    build_silver_sec_facts_long_frame,
    build_silver_sec_submissions_frame,
    resolve_sec_statement_item_asset_ids,
    silver_sec_facts_long,
    silver_sec_submissions,
)


def test_build_cik_asset_id_map_allows_shared_cik_asset_ids() -> None:
    identifiers = pd.DataFrame(
        {
            "asset_id": [10, 11, 20, None],
            "cik": ["0001652044", "1652044", "0000320193", "0000789019"],
            "identifier_type": ["cik", "sec_ticker", "cik", "cik"],
            "identifier_value": ["1652044", "GOOGL", "320193", "789019"],
            "is_current": [True, True, True, True],
        }
    )

    frame = build_cik_asset_id_map_frame(identifiers)

    assert frame.to_dict("records") == [
        {"cik": "1652044", "asset_id": 10},
        {"cik": "1652044", "asset_id": 11},
        {"cik": "320193", "asset_id": 20},
    ]


def test_build_silver_sec_submissions_resolves_asset_id_and_preserves_unmapped() -> None:
    bronze = pd.DataFrame(
        {
            "cik": ["0001652044", "0001652044", "0000789019"],
            "accession_number": ["0001652044-26-000001", "0001652044-26-000001", "acc-2"],
            "form": ["10-K", "10-K", "10-Q/A"],
            "filing_date": ["2026-02-01", "2026-02-01", "2026-03-01"],
            "report_date": ["2025-12-31", "2025-12-31", "2026-02-28"],
            "acceptance_datetime": ["20260201120000", "20260201120000", None],
            "primary_document": ["goog.htm", "goog.htm", "msft.htm"],
            "primary_doc_description": ["10-K", "10-K", "10-Q/A"],
            "file_number": ["001", "001", "002"],
            "film_number": ["1", "1", "2"],
            "act": ["34", "34", "34"],
            "items": [None, None, "2.02"],
            "size": [100, 100, 200],
            "source_archive_path": ["submissions.zip", "submissions.zip", "submissions.zip"],
            "source_member_name": [
                "CIK0001652044.json",
                "CIK0001652044.json",
                "CIK0000789019.json",
            ],
            "ingestion_date": ["2026-02-02", "2026-02-03", "2026-03-02"],
            "ingested_ts": [
                pd.Timestamp("2026-02-02"),
                pd.Timestamp("2026-02-03"),
                pd.Timestamp("2026-03-02"),
            ],
        }
    )
    cik_map = pd.DataFrame({"cik": ["1652044", "1652044"], "asset_id": [10, 11]})

    frame = build_silver_sec_submissions_frame(bronze, cik_map)

    assert list(frame.columns) == SEC_SUBMISSIONS_COLUMNS
    rows = frame[["asset_id", "cik", "accession_number", "is_amendment"]].to_dict("records")
    assert rows == [
        {
            "asset_id": 10,
            "cik": "1652044",
            "accession_number": "0001652044-26-000001",
            "is_amendment": False,
        },
        {
            "asset_id": 11,
            "cik": "1652044",
            "accession_number": "0001652044-26-000001",
            "is_amendment": False,
        },
        {"asset_id": None, "cik": "789019", "accession_number": "acc-2", "is_amendment": True},
    ]


def test_build_silver_sec_facts_long_resolves_asset_id() -> None:
    bronze = pd.DataFrame(
        {
            "cik": ["0000320193", "0000789019"],
            "accession_number": ["acc-1", "acc-2"],
            "taxonomy": ["us-gaap", "us-gaap"],
            "tag": ["Revenues", "Assets"],
            "label": ["Revenue", "Assets"],
            "description": ["Revenue", "Assets"],
            "unit": ["USD", "USD"],
            "value": ["123.45", "678.90"],
            "fiscal_year": [2025, 2025],
            "fiscal_period": ["FY", "Q1"],
            "form": ["10-K", "10-Q"],
            "filed_date": ["2026-02-01", "2026-03-01"],
            "period_start_date": ["2025-01-01", None],
            "period_end_date": ["2025-12-31", "2026-03-31"],
            "frame": ["CY2025", None],
            "ingestion_date": ["2026-02-02", "2026-03-02"],
            "ingested_ts": [pd.Timestamp("2026-02-02"), pd.Timestamp("2026-03-02")],
        }
    )
    cik_map = pd.DataFrame({"cik": ["320193"], "asset_id": [1]})

    frame = build_silver_sec_facts_long_frame(bronze, cik_map)

    assert list(frame.columns) == SEC_FACTS_LONG_COLUMNS
    rows = frame[["asset_id", "cik", "tag", "value", "period_type"]].to_dict("records")
    assert rows == [
        {
            "asset_id": 1,
            "cik": "320193",
            "tag": "Revenues",
            "value": 123.45,
            "period_type": "duration",
        },
        {
            "asset_id": None,
            "cik": "789019",
            "tag": "Assets",
            "value": 678.9,
            "period_type": "instant",
        },
    ]


def test_resolve_sec_statement_item_asset_ids_expands_shared_cik() -> None:
    statement_items = pd.DataFrame(
        {
            "cik": ["0001652044", "0000789019"],
            "accession_number": ["acc-1", "acc-2"],
            "canonical_metric": ["revenue", "revenue"],
        }
    )
    cik_map = pd.DataFrame({"cik": ["1652044", "1652044"], "asset_id": [10, 11]})

    frame = resolve_sec_statement_item_asset_ids(statement_items, cik_map)

    assert frame[["asset_id", "cik", "accession_number"]].to_dict("records") == [
        {"asset_id": 10, "cik": "1652044", "accession_number": "acc-1"},
        {"asset_id": 11, "cik": "1652044", "accession_number": "acc-1"},
        {"asset_id": None, "cik": "789019", "accession_number": "acc-2"},
    ]


def test_bronze_sec_file_selection_uses_latest_snapshots(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_silver_module, "DATA_ROOT", data_root)
    old_submissions = (
        data_root
        / "bronze"
        / "sec_submissions"
        / "ingestion_date=2026-01-01"
        / "submissions.parquet"
    )
    new_submissions = (
        data_root
        / "bronze"
        / "sec_submissions"
        / "ingestion_date=2026-01-02"
        / "submissions.parquet"
    )
    old_facts = (
        data_root
        / "bronze"
        / "sec_company_facts"
        / "ingestion_date=2026-01-01"
        / "taxonomy=us-gaap"
        / "facts.parquet"
    )
    new_facts = (
        data_root
        / "bronze"
        / "sec_company_facts"
        / "ingestion_date=2026-01-02"
        / "taxonomy=us-gaap"
        / "facts.parquet"
    )
    ifrs_facts = (
        data_root
        / "bronze"
        / "sec_company_facts"
        / "ingestion_date=2026-01-03"
        / "taxonomy=ifrs-full"
        / "facts.parquet"
    )
    for path in [old_submissions, new_submissions, old_facts, new_facts, ifrs_facts]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    assert _bronze_sec_submissions_files() == [new_submissions]
    assert _bronze_sec_facts_files() == [new_facts, ifrs_facts]


def test_silver_sec_assets_materialize_asset_ids_from_security_identifiers(
    tmp_path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_silver_module, "DATA_ROOT", data_root)
    submissions_path = (
        data_root
        / "bronze"
        / "sec_submissions"
        / "ingestion_date=2026-02-02"
        / "submissions.parquet"
    )
    facts_path = (
        data_root
        / "bronze"
        / "sec_company_facts"
        / "ingestion_date=2026-02-02"
        / "taxonomy=us-gaap"
        / "facts.parquet"
    )
    submissions_path.parent.mkdir(parents=True)
    facts_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "cik": ["0000320193"],
            "accession_number": ["acc-1"],
            "form": ["10-K"],
            "filing_date": ["2026-02-01"],
            "report_date": ["2025-12-31"],
            "acceptance_datetime": ["20260201120000"],
            "act": ["34"],
            "file_number": ["001"],
            "film_number": ["1"],
            "items": [None],
            "primary_document": ["aapl.htm"],
            "primary_doc_description": ["10-K"],
            "size": [100],
            "source_archive_path": ["submissions.zip"],
            "source_member_name": ["CIK0000320193.json"],
            "ingestion_date": ["2026-02-02"],
            "ingested_ts": [pd.Timestamp("2026-02-02")],
        }
    ).to_parquet(submissions_path, index=False)
    pd.DataFrame(
        {
            "cik": ["0000320193"],
            "accession_number": ["acc-1"],
            "taxonomy": ["us-gaap"],
            "tag": ["Revenues"],
            "label": ["Revenue"],
            "description": ["Revenue"],
            "unit": ["USD"],
            "value": [1000],
            "fiscal_year": [2025],
            "fiscal_period": ["FY"],
            "form": ["10-K"],
            "filed_date": ["2026-02-01"],
            "period_start_date": ["2025-01-01"],
            "period_end_date": ["2025-12-31"],
            "frame": ["CY2025"],
            "ingestion_date": ["2026-02-02"],
            "ingested_ts": [pd.Timestamp("2026-02-02")],
        }
    ).to_parquet(facts_path, index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.security_identifiers (
            asset_id BIGINT,
            cik VARCHAR,
            identifier_type VARCHAR,
            identifier_value VARCHAR,
            is_current BOOLEAN
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.security_identifiers VALUES
            (1, '320193', 'cik', '320193', TRUE)
        """
    )

    result = materialize(
        assets=[silver_sec_submissions, silver_sec_facts_long],
        resources={"research_duckdb": con},
    )

    assert result.success
    assert con.execute("SELECT asset_id, cik FROM silver.sec_submissions").fetchall() == [
        (1, "320193")
    ]
    assert con.execute("SELECT asset_id, cik FROM silver.sec_facts_long").fetchall() == [
        (1, "320193")
    ]
