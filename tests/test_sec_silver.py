import duckdb
import pandas as pd
from dagster import materialize

import portfolio_project.defs.research_db.silver.sec as sec_silver_module
from portfolio_project.defs.research_db.dq_checks import log_sec_fundamentals_quality_checks
from portfolio_project.defs.research_db.silver.sec import (
    SEC_FACTS_LONG_COLUMNS,
    SEC_STATEMENT_ITEMS_COLUMNS,
    SEC_STATEMENT_MAPPING_VERSION,
    SEC_SUBMISSIONS_COLUMNS,
    _bronze_sec_facts_files,
    _bronze_sec_submissions_files,
    _supported_sec_units_by_tag,
    build_cik_asset_id_map_frame,
    build_silver_sec_facts_long_frame,
    build_silver_sec_statement_items_frame,
    build_silver_sec_submissions_frame,
    resolve_sec_statement_item_asset_ids,
    silver_sec_facts_long,
    silver_sec_statement_items,
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


def test_build_cik_asset_id_map_ignores_sec_ticker_without_cik() -> None:
    identifiers = pd.DataFrame(
        {
            "asset_id": [10, 11],
            "cik": [None, "0001652044"],
            "identifier_type": ["sec_ticker", "sec_ticker"],
            "identifier_value": ["GOOG", "GOOGL"],
            "is_current": [True, True],
        }
    )

    frame = build_cik_asset_id_map_frame(identifiers)

    assert frame.to_dict("records") == [{"cik": "1652044", "asset_id": 11}]


def test_sec_statement_concept_mapping_is_stable() -> None:
    mappings = sec_silver_module._mapping_frame()

    selected = mappings[
        mappings["canonical_metric"].isin(["revenue", "debt", "capex", "diluted_eps"])
    ].sort_values(["canonical_metric", "mapping_priority", "tag"])

    assert SEC_STATEMENT_MAPPING_VERSION == "sec_us_gaap_v1"
    assert selected[["canonical_metric", "mapping_priority", "tag", "unit"]].to_dict("records") == [
        {
            "canonical_metric": "capex",
            "mapping_priority": 10,
            "tag": "PaymentsToAcquirePropertyPlantAndEquipment",
            "unit": "USD",
        },
        {
            "canonical_metric": "capex",
            "mapping_priority": 20,
            "tag": "PaymentsToAcquireProductiveAssets",
            "unit": "USD",
        },
        {
            "canonical_metric": "debt",
            "mapping_priority": 50,
            "tag": "LongTermDebt",
            "unit": "USD",
        },
        {
            "canonical_metric": "diluted_eps",
            "mapping_priority": 10,
            "tag": "EarningsPerShareDiluted",
            "unit": "USD/shares",
        },
        {
            "canonical_metric": "diluted_eps",
            "mapping_priority": 20,
            "tag": "EarningsPerShareBasicAndDiluted",
            "unit": "USD/shares",
        },
        {
            "canonical_metric": "revenue",
            "mapping_priority": 10,
            "tag": "RevenueFromContractWithCustomerExcludingAssessedTax",
            "unit": "USD",
        },
        {
            "canonical_metric": "revenue",
            "mapping_priority": 20,
            "tag": "RevenueFromContractWithCustomerIncludingAssessedTax",
            "unit": "USD",
        },
        {
            "canonical_metric": "revenue",
            "mapping_priority": 30,
            "tag": "Revenues",
            "unit": "USD",
        },
        {
            "canonical_metric": "revenue",
            "mapping_priority": 40,
            "tag": "SalesRevenueNet",
            "unit": "USD",
        },
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


def test_build_silver_sec_statement_items_maps_direct_and_component_facts() -> None:
    facts = pd.DataFrame(
        {
            "asset_id": [1, 1, 1, 1, 1, 1],
            "cik": ["320193"] * 6,
            "accession_number": ["acc-1"] * 6,
            "taxonomy": ["us-gaap"] * 6,
            "tag": [
                "Revenues",
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "LongTermDebtCurrent",
                "LongTermDebtNoncurrent",
                "ShortTermBorrowings",
                "PaymentsToAcquirePropertyPlantAndEquipment",
            ],
            "label": [""] * 6,
            "description": [""] * 6,
            "unit": ["USD"] * 6,
            "value": [900, 1000, 100, 400, 50, -25],
            "value_raw": ["900", "1000", "100", "400", "50", "-25"],
            "decimals": [None] * 6,
            "period_start_date": [
                "2025-01-01",
                "2025-01-01",
                None,
                None,
                None,
                "2025-01-01",
            ],
            "period_end_date": ["2025-12-31"] * 6,
            "period_type": [
                "duration",
                "duration",
                "instant",
                "instant",
                "instant",
                "duration",
            ],
            "fiscal_year": [2025] * 6,
            "fiscal_period": ["FY"] * 6,
            "form": ["10-K"] * 6,
            "filed_date": ["2026-02-01"] * 6,
            "frame": ["CY2025"] * 6,
            "source_snapshot_date": ["2026-02-02"] * 6,
            "ingestion_date": ["2026-02-02"] * 6,
            "ingested_ts": [pd.Timestamp("2026-02-02")] * 6,
        }
    )
    submissions = pd.DataFrame(
        {
            "accession_number": ["acc-1"],
            "filing_date": ["2026-02-01"],
            "acceptance_datetime": [pd.Timestamp("2026-02-01 12:00:00", tz="UTC")],
        }
    )

    frame = build_silver_sec_statement_items_frame(facts, submissions)

    assert list(frame.columns) == SEC_STATEMENT_ITEMS_COLUMNS
    selected = frame.set_index("canonical_metric")
    assert selected.loc["revenue", "tag"] == "RevenueFromContractWithCustomerExcludingAssessedTax"
    assert selected.loc["revenue", "mapping_priority"] == 10
    assert selected.loc["debt", "value"] == 550
    assert selected.loc["debt", "is_component_sum"]
    assert selected.loc["capex", "value"] == 25
    assert selected.loc["capex", "canonical_sign_rule"] == "positive_cash_outflow"
    assert selected.loc["revenue", "availability_date"].isoformat() == "2026-02-01"


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
            "cik": ["0000320193"] * 4,
            "accession_number": ["acc-1"] * 4,
            "taxonomy": ["us-gaap"] * 4,
            "tag": [
                "Revenues",
                "LongTermDebtCurrent",
                "LongTermDebtNoncurrent",
                "ShortTermBorrowings",
            ],
            "label": ["Revenue", "Debt current", "Debt noncurrent", "Borrowings"],
            "description": ["Revenue", "Debt current", "Debt noncurrent", "Borrowings"],
            "unit": ["USD"] * 4,
            "value": [1000, 100, 400, 50],
            "fiscal_year": [2025] * 4,
            "fiscal_period": ["FY"] * 4,
            "form": ["10-K"] * 4,
            "filed_date": ["2026-02-01"] * 4,
            "period_start_date": ["2025-01-01", None, None, None],
            "period_end_date": ["2025-12-31"] * 4,
            "frame": ["CY2025", None, None, None],
            "ingestion_date": ["2026-02-02"] * 4,
            "ingested_ts": [pd.Timestamp("2026-02-02")] * 4,
        }
    ).to_parquet(facts_path, index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.security_identifiers (
            asset_id BIGINT,
            source_symbol VARCHAR,
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
            (1, 'AAPL', '320193', 'cik', '320193', TRUE)
        """
    )

    observability_con = duckdb.connect(":memory:")
    result = materialize(
        assets=[silver_sec_submissions, silver_sec_facts_long, silver_sec_statement_items],
        resources={"duckdb": observability_con, "research_duckdb": con},
    )

    assert result.success
    assert con.execute("SELECT asset_id, cik FROM silver.sec_submissions").fetchall() == [
        (1, "320193")
    ]
    assert con.execute("SELECT count(*) FROM silver.sec_facts_long").fetchone() == (4,)
    assert con.execute(
        """
        SELECT asset_id, cik, canonical_metric, value
        FROM silver.sec_statement_items
        ORDER BY canonical_metric
        """
    ).fetchall() == [
        (1, "320193", "debt", 550.0),
        (1, "320193", "revenue", 1000.0),
    ]


def test_sec_fundamentals_dq_checks_detect_reliability_violations() -> None:
    measured_con = duckdb.connect(":memory:")
    observability_con = duckdb.connect(":memory:")
    measured_con.execute("CREATE SCHEMA silver")
    measured_con.execute(
        """
        CREATE TABLE silver.sec_submissions (
            asset_id BIGINT,
            cik VARCHAR,
            accession_number VARCHAR,
            accession_number_nodash VARCHAR,
            form VARCHAR,
            filing_date DATE,
            source_url VARCHAR,
            ingestion_date DATE,
            ingested_ts TIMESTAMP
        )
        """
    )
    measured_con.execute(
        """
        INSERT INTO silver.sec_submissions VALUES
            (1, '320193', 'acc-1', 'acc1', '10-K', DATE '2026-02-01',
                'submissions.zip#CIK0000320193.json', DATE '2026-02-02',
                TIMESTAMP '2026-02-02 00:00:00'),
            (1, '320193', 'acc-1', 'acc1', '10-K', DATE '2026-02-01',
                'submissions.zip#CIK0000320193.json', DATE '2026-02-02',
                TIMESTAMP '2026-02-02 00:00:00'),
            (2, '789019', '', '', NULL, NULL, NULL, DATE '2026-02-02',
                TIMESTAMP '2026-02-02 00:00:00')
        """
    )
    measured_con.execute(
        """
        CREATE TABLE silver.sec_facts_long (
            asset_id BIGINT,
            cik VARCHAR,
            accession_number VARCHAR,
            taxonomy VARCHAR,
            tag VARCHAR,
            unit VARCHAR,
            value DOUBLE,
            period_start_date DATE,
            period_end_date DATE,
            frame VARCHAR,
            period_type VARCHAR,
            source_snapshot_date DATE,
            ingestion_date DATE,
            ingested_ts TIMESTAMP
        )
        """
    )
    measured_con.execute(
        """
        INSERT INTO silver.sec_facts_long VALUES
            (1, '320193', 'acc-1', 'us-gaap', 'Revenues', 'USD', 100.0,
                DATE '2025-01-01', DATE '2025-12-31', 'CY2025', 'duration',
                DATE '2026-02-02', DATE '2026-02-02', TIMESTAMP '2026-02-02'),
            (1, '320193', 'acc-1', 'us-gaap', 'Revenues', 'USD', 100.0,
                DATE '2025-01-01', DATE '2025-12-31', 'CY2025', 'duration',
                DATE '2026-02-02', DATE '2026-02-02', TIMESTAMP '2026-02-02'),
            (1, '320193', 'acc-2', 'us-gaap', 'Revenues', 'shares', 10.0,
                DATE '2025-01-01', DATE '2025-12-31', 'CY2025', 'duration',
                DATE '2026-02-02', DATE '2026-02-02', TIMESTAMP '2026-02-02')
        """
    )
    measured_con.execute(
        """
        CREATE TABLE silver.sec_statement_items (
            cik VARCHAR,
            accession_number VARCHAR,
            canonical_metric VARCHAR,
            statement_type VARCHAR,
            taxonomy VARCHAR,
            tag VARCHAR,
            unit VARCHAR,
            value DOUBLE,
            period_end_date DATE,
            filing_date DATE,
            availability_date DATE,
            mapping_version VARCHAR,
            mapping_priority BIGINT
        )
        """
    )
    measured_con.execute(
        """
        INSERT INTO silver.sec_statement_items VALUES
            ('320193', 'acc-1', 'revenue', 'income_statement', 'us-gaap',
                'Revenues', 'USD', 100.0, DATE '2025-12-31', DATE '2026-02-01',
                DATE '2026-02-01', 'sec_us_gaap_v1', 30)
        """
    )
    measured_con.execute(
        """
        CREATE TABLE silver.security_identifiers (
            asset_id BIGINT,
            source_symbol VARCHAR,
            cik VARCHAR,
            is_current BOOLEAN
        )
        """
    )
    measured_con.execute(
        """
        INSERT INTO silver.security_identifiers VALUES
            (1, 'AAPL', '320193', TRUE),
            (2, 'AAPL', '789019', TRUE),
            (3, 'MSFT', '789019', TRUE),
            (4, 'MSFT', '789019', TRUE)
        """
    )

    log_sec_fundamentals_quality_checks(
        measured_con=measured_con,
        observability_con=observability_con,
        supported_units_by_tag=_supported_sec_units_by_tag(),
        run_id="run-1",
        job_name="sec_fundamentals_job",
    )

    rows = observability_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        ORDER BY check_name
        """
    ).fetchall()
    assert rows == [
        ("dq_sec_cik_ticker_mapping_conflicts", "FAIL", 2.0),
        ("dq_sec_facts_long_duplicate_facts", "FAIL", 1.0),
        ("dq_sec_facts_long_required_fields", "PASS", 0.0),
        ("dq_sec_facts_long_unsupported_units", "FAIL", 1.0),
        ("dq_sec_statement_items_required_fields", "PASS", 0.0),
        ("dq_sec_submissions_accession_uniqueness", "FAIL", 1.0),
        ("dq_sec_submissions_required_fields", "FAIL", 5.0),
    ]
