import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from dagster import materialize

import portfolio_project.defs.research_db.bronze.sec as sec_bronze_module
from portfolio_project.defs.research_db.bronze.sec import (
    SEC_BRONZE_DATASETS,
    bronze_sec_bulk_archives,
    materialize_bronze_sec_company_facts,
    materialize_bronze_sec_company_tickers,
    materialize_bronze_sec_submissions,
    parse_company_facts_json_document,
    parse_company_facts_zip_to_parquet,
    parse_company_tickers_exchange_json,
    parse_submission_json_document,
    parse_submissions_zip_to_parquet,
)


class _FakeResponse:
    def __init__(self, content: bytes, headers: dict[str, str] | None = None) -> None:
        self.content = content
        self.headers = headers or {}


class _FakeSecClient:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.calls: list[str] = []

    def _resolve_url(self, url_or_path: str) -> str:
        return f"https://www.sec.gov/{url_or_path.lstrip('/')}"

    def get(self, url_or_path: str) -> _FakeResponse:
        self.calls.append(url_or_path)
        return _FakeResponse(
            self.payloads[url_or_path],
            headers={
                "ETag": f"etag-{Path(url_or_path).name}",
                "Last-Modified": "Sun, 15 Mar 2026 08:00:00 GMT",
            },
        )


def _company_tickers_payload() -> bytes:
    return json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, " Apple Inc. ", "aapl", "Nasdaq"],
                [789019, "MICROSOFT CORP", "MSFT", "Nasdaq"],
            ],
        }
    ).encode()


def _submission_document() -> dict:
    return {
        "cik": "0000320193",
        "entityType": "operating",
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "name": "Apple Inc.",
        "tickers": ["AAPL"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-26-000001", "0000320193-25-000002"],
                "filingDate": ["2026-01-31", "2025-01-31"],
                "reportDate": ["2025-12-27", "2024-12-28"],
                "acceptanceDateTime": [
                    "2026-01-31T18:01:02.000Z",
                    "2025-01-31T18:01:02.000Z",
                ],
                "act": ["34", "34"],
                "form": ["10-K", "10-K"],
                "fileNumber": ["001-36743", "001-36743"],
                "filmNumber": ["26500001", "25500002"],
                "items": ["", ""],
                "core_type": ["10-K", "10-K"],
                "size": [123456, 234567],
                "isXBRL": [1, 1],
                "isInlineXBRL": [1, 1],
                "isXBRLNumeric": [1, 1],
                "primaryDocument": ["aapl-20251227.htm", "aapl-20241228.htm"],
                "primaryDocDescription": ["10-K", "10-K"],
            }
        },
    }


def _submission_chunk_document() -> dict:
    return {
        "accessionNumber": ["0001181431-10-016632"],
        "filingDate": ["2010-03-16"],
        "reportDate": ["2010-03-12"],
        "acceptanceDateTime": ["2010-03-16T18:43:23.000Z"],
        "act": ["34"],
        "form": ["4"],
        "fileNumber": ["001-00001"],
        "filmNumber": ["10600001"],
        "items": [""],
        "core_type": ["4"],
        "size": [9876],
        "isXBRL": [0],
        "isInlineXBRL": [0],
        "isXBRLNumeric": [0],
        "primaryDocument": ["xslF345X03/rrd270114.xml"],
        "primaryDocDescription": ["FORM 4"],
    }


def _write_submissions_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("CIK0000320193.json", json.dumps(_submission_document()))
        archive.writestr(
            "CIK0000005981-submissions-001.json",
            json.dumps(_submission_chunk_document()),
        )


def _company_facts_document() -> dict:
    return {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenue",
                    "description": "Total revenue.",
                    "units": {
                        "USD": [
                            {
                                "start": "2025-09-28",
                                "end": "2025-12-27",
                                "val": 123456000000,
                                "accn": "0000320193-26-000001",
                                "fy": 2026,
                                "fp": "Q1",
                                "form": "10-Q",
                                "filed": "2026-01-31",
                                "frame": "CY2025Q4",
                            }
                        ]
                    },
                },
                "Assets": {
                    "label": "Assets",
                    "description": "Total assets.",
                    "units": {
                        "USD": [
                            {
                                "end": "2025-12-27",
                                "val": 500000000000,
                                "accn": "0000320193-26-000001",
                                "fy": 2026,
                                "fp": "Q1",
                                "form": "10-Q",
                                "filed": "2026-01-31",
                                "frame": "CY2025Q4I",
                            }
                        ]
                    },
                },
                "GrossProfit": {
                    "label": "Gross profit",
                    "description": "Unsupported test concept.",
                    "units": {
                        "USD": [
                            {
                                "start": "2025-09-28",
                                "end": "2025-12-27",
                                "val": 1,
                                "accn": "0000320193-26-000001",
                                "fy": 2026,
                                "fp": "Q1",
                                "form": "10-Q",
                                "filed": "2026-01-31",
                            }
                        ]
                    },
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "label": "Common shares outstanding",
                    "description": "Unsupported taxonomy test concept.",
                    "units": {
                        "shares": [
                            {
                                "end": "2025-12-27",
                                "val": 15000000000,
                                "accn": "0000320193-26-000001",
                                "fy": 2026,
                                "fp": "Q1",
                                "form": "10-Q",
                                "filed": "2026-01-31",
                            }
                        ]
                    },
                }
            },
        },
    }


def _write_company_facts_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("CIK0000320193.json", json.dumps(_company_facts_document()))


def test_bronze_sec_bulk_archives_writes_raw_files_and_ingestion_log(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_bronze_module, "DATA_ROOT", data_root)

    fixed_now = datetime(2026, 3, 15, 9, 30, tzinfo=timezone.utc)

    class _FixedDateTime:
        @staticmethod
        def now(tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(sec_bronze_module, "datetime", _FixedDateTime)

    payloads = {
        dataset.url_path: f"{dataset.dataset}-payload".encode() for dataset in SEC_BRONZE_DATASETS
    }
    sec_client = _FakeSecClient(payloads)

    result = materialize([bronze_sec_bulk_archives], resources={"sec": sec_client})

    assert result.success
    assert sec_client.calls == [dataset.url_path for dataset in SEC_BRONZE_DATASETS]

    ingestion_log_path = data_root / "bronze" / "sec" / "ingestion_log.parquet"
    ingestion_log = pd.read_parquet(ingestion_log_path)

    assert list(ingestion_log.columns) == sec_bronze_module.SEC_INGESTION_LOG_COLUMNS
    assert ingestion_log["dataset"].tolist() == [
        "company_tickers",
        "companyfacts",
        "submissions",
    ]
    assert ingestion_log["ingestion_date"].tolist() == ["2026-03-15"] * 3
    assert ingestion_log["changed_flag"].tolist() == [True, True, True]
    assert ingestion_log["file_size"].tolist() == [
        len(payloads["/files/company_tickers_exchange.json"]),
        len(payloads["/Archives/edgar/daily-index/xbrl/companyfacts.zip"]),
        len(payloads["/Archives/edgar/daily-index/bulkdata/submissions.zip"]),
    ]

    for dataset in SEC_BRONZE_DATASETS:
        out_path = (
            data_root
            / "bronze"
            / "sec"
            / dataset.raw_subdir
            / "ingestion_date=2026-03-15"
            / dataset.filename
        )
        assert out_path.read_bytes() == payloads[dataset.url_path]


def test_bronze_sec_bulk_archives_reuses_existing_archive_for_unchanged_hash(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_bronze_module, "DATA_ROOT", data_root)

    now_values = iter(
        [
            datetime(2026, 3, 15, 9, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 16, 9, 30, tzinfo=timezone.utc),
        ]
    )

    class _QueuedDateTime:
        @staticmethod
        def now(tz=None):
            value = next(now_values)
            if tz is None:
                return value.replace(tzinfo=None)
            return value.astimezone(tz)

    monkeypatch.setattr(sec_bronze_module, "datetime", _QueuedDateTime)

    payloads = {
        dataset.url_path: f"{dataset.dataset}-payload".encode() for dataset in SEC_BRONZE_DATASETS
    }

    assert materialize(
        [bronze_sec_bulk_archives],
        resources={"sec": _FakeSecClient(payloads)},
    ).success
    assert materialize(
        [bronze_sec_bulk_archives],
        resources={"sec": _FakeSecClient(payloads)},
    ).success

    ingestion_log = pd.read_parquet(data_root / "bronze" / "sec" / "ingestion_log.parquet")
    assert len(ingestion_log) == 6

    first_rows = ingestion_log[ingestion_log["ingestion_date"] == "2026-03-15"].sort_values(
        "dataset"
    )
    second_rows = ingestion_log[ingestion_log["ingestion_date"] == "2026-03-16"].sort_values(
        "dataset"
    )

    assert first_rows["changed_flag"].tolist() == [True, True, True]
    assert second_rows["changed_flag"].tolist() == [False, False, False]
    assert second_rows["local_path"].tolist() == first_rows["local_path"].tolist()

    for dataset in SEC_BRONZE_DATASETS:
        duplicate_path = (
            data_root
            / "bronze"
            / "sec"
            / dataset.raw_subdir
            / "ingestion_date=2026-03-16"
            / dataset.filename
        )
        assert not duplicate_path.exists()


def test_parse_company_tickers_exchange_json_normalizes_rows() -> None:
    frame = parse_company_tickers_exchange_json(
        _company_tickers_payload(),
        ingestion_date="2026-03-15",
        source_file="company_tickers_exchange.json",
        source_content_hash="abc123",
        ingested_ts=pd.Timestamp("2026-03-15T09:30:00Z"),
    )

    assert list(frame.columns) == sec_bronze_module.SEC_COMPANY_TICKERS_COLUMNS
    assert frame[["cik", "name", "ticker", "exchange"]].to_dict("records") == [
        {
            "cik": "320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "exchange": "Nasdaq",
        },
        {
            "cik": "789019",
            "name": "MICROSOFT CORP",
            "ticker": "MSFT",
            "exchange": "Nasdaq",
        },
    ]
    assert frame["source_row_number"].tolist() == [1, 2]
    assert frame["ingestion_date"].tolist() == ["2026-03-15", "2026-03-15"]
    assert frame["source_content_hash"].tolist() == ["abc123", "abc123"]


def test_parse_company_tickers_exchange_json_rejects_missing_required_fields() -> None:
    payload = json.dumps({"fields": ["cik", "ticker"], "data": [[320193, "AAPL"]]}).encode()

    with pytest.raises(ValueError, match="missing required fields"):
        parse_company_tickers_exchange_json(
            payload,
            ingestion_date="2026-03-15",
            source_file="company_tickers_exchange.json",
            source_content_hash="abc123",
        )


def test_materialize_bronze_sec_company_tickers_writes_parquet_from_ingestion_log(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_bronze_module, "DATA_ROOT", data_root)
    raw_path = (
        data_root
        / "bronze"
        / "sec"
        / "company_tickers"
        / "ingestion_date=2026-03-15"
        / "company_tickers_exchange.json"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(_company_tickers_payload())

    ingestion_log_path = data_root / "bronze" / "sec" / "ingestion_log.parquet"
    ingestion_log_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "dataset": "company_tickers",
                "source_url": "https://www.sec.gov/files/company_tickers_exchange.json",
                "retrieved_at": pd.Timestamp("2026-03-15T09:30:00Z"),
                "ingestion_date": "2026-03-15",
                "etag": "etag",
                "last_modified": "Sun, 15 Mar 2026 08:00:00 GMT",
                "content_hash": "abc123",
                "file_size": raw_path.stat().st_size,
                "local_path": str(raw_path),
                "changed_flag": True,
            }
        ],
        columns=sec_bronze_module.SEC_INGESTION_LOG_COLUMNS,
    ).to_parquet(ingestion_log_path, index=False)

    metrics = materialize_bronze_sec_company_tickers()

    out_path = (
        data_root
        / "bronze"
        / "sec_company_tickers"
        / "ingestion_date=2026-03-15"
        / "tickers.parquet"
    )
    parsed = pd.read_parquet(out_path)
    assert metrics == {
        "source_snapshot_count": 1,
        "written_snapshot_count": 1,
        "skipped_snapshot_count": 0,
        "row_count": 2,
        "latest_ingestion_date": "2026-03-15",
    }
    assert parsed["ticker"].tolist() == ["AAPL", "MSFT"]
    assert parsed["source_file"].tolist() == [str(raw_path), str(raw_path)]


def test_parse_submission_json_document_emits_one_row_per_accession() -> None:
    frame = parse_submission_json_document(
        _submission_document(),
        ingestion_date="2026-03-15",
        source_archive_path="submissions.zip",
        source_archive_content_hash="abc123",
        source_member_name="CIK0000320193.json",
        ingested_ts=pd.Timestamp("2026-03-15T09:30:00Z"),
    )

    assert list(frame.columns) == sec_bronze_module.SEC_SUBMISSIONS_COLUMNS
    assert frame[["cik", "entity_name", "tickers", "exchanges"]].to_dict("records") == [
        {
            "cik": "320193",
            "entity_name": "Apple Inc.",
            "tickers": "AAPL",
            "exchanges": "Nasdaq",
        },
        {
            "cik": "320193",
            "entity_name": "Apple Inc.",
            "tickers": "AAPL",
            "exchanges": "Nasdaq",
        },
    ]
    assert frame["accession_number"].tolist() == [
        "0000320193-26-000001",
        "0000320193-25-000002",
    ]
    assert frame["form"].tolist() == ["10-K", "10-K"]
    assert frame["source_row_number"].tolist() == [1, 2]


def test_parse_submission_json_document_derives_cik_for_chunk_members() -> None:
    frame = parse_submission_json_document(
        _submission_chunk_document(),
        ingestion_date="2026-03-15",
        source_archive_path="submissions.zip",
        source_archive_content_hash="abc123",
        source_member_name="CIK0000005981-submissions-001.json",
    )

    assert frame["cik"].tolist() == ["5981"]
    assert frame["entity_name"].isna().all()
    assert frame["accession_number"].tolist() == ["0001181431-10-016632"]
    assert frame["form"].tolist() == ["4"]


def test_parse_submissions_zip_to_parquet_writes_fixture_archive(tmp_path: Path) -> None:
    zip_path = tmp_path / "submissions.zip"
    out_path = tmp_path / "submissions.parquet"
    _write_submissions_zip(zip_path)

    row_count = parse_submissions_zip_to_parquet(
        zip_path,
        out_path,
        ingestion_date="2026-03-15",
        source_archive_content_hash="abc123",
        chunk_size=1,
    )

    parsed = pd.read_parquet(out_path)
    assert row_count == 3
    assert len(parsed) == 3
    assert parsed["accession_number"].tolist() == [
        "0000320193-26-000001",
        "0000320193-25-000002",
        "0001181431-10-016632",
    ]
    assert parsed["source_member_name"].tolist() == [
        "CIK0000320193.json",
        "CIK0000320193.json",
        "CIK0000005981-submissions-001.json",
    ]


def test_parse_submissions_zip_to_parquet_rejects_malformed_json(tmp_path: Path) -> None:
    zip_path = tmp_path / "submissions.zip"
    out_path = tmp_path / "submissions.parquet"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("CIK0000320193.json", "{not json")

    with pytest.raises(ValueError, match="Invalid SEC submissions JSON member"):
        parse_submissions_zip_to_parquet(
            zip_path,
            out_path,
            ingestion_date="2026-03-15",
            source_archive_content_hash="abc123",
        )


def test_materialize_bronze_sec_submissions_writes_parquet_from_ingestion_log(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_bronze_module, "DATA_ROOT", data_root)
    raw_path = (
        data_root
        / "bronze"
        / "sec"
        / "submissions"
        / "ingestion_date=2026-03-15"
        / "submissions.zip"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    _write_submissions_zip(raw_path)

    ingestion_log_path = data_root / "bronze" / "sec" / "ingestion_log.parquet"
    ingestion_log_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "dataset": "submissions",
                "source_url": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
                "retrieved_at": pd.Timestamp("2026-03-15T09:30:00Z"),
                "ingestion_date": "2026-03-15",
                "etag": "etag",
                "last_modified": "Sun, 15 Mar 2026 08:00:00 GMT",
                "content_hash": "abc123",
                "file_size": raw_path.stat().st_size,
                "local_path": str(raw_path),
                "changed_flag": True,
            }
        ],
        columns=sec_bronze_module.SEC_INGESTION_LOG_COLUMNS,
    ).to_parquet(ingestion_log_path, index=False)

    metrics = materialize_bronze_sec_submissions()

    out_path = (
        data_root
        / "bronze"
        / "sec_submissions"
        / "ingestion_date=2026-03-15"
        / "submissions.parquet"
    )
    parsed = pd.read_parquet(out_path)
    assert metrics == {
        "source_snapshot_count": 1,
        "written_snapshot_count": 1,
        "skipped_snapshot_count": 0,
        "row_count": 3,
        "latest_ingestion_date": "2026-03-15",
    }
    assert parsed["accession_number"].tolist() == [
        "0000320193-26-000001",
        "0000320193-25-000002",
        "0001181431-10-016632",
    ]


def test_parse_company_facts_json_document_filters_to_supported_us_gaap_tags() -> None:
    frame = parse_company_facts_json_document(
        _company_facts_document(),
        ingestion_date="2026-03-15",
        source_archive_path="companyfacts.zip",
        source_archive_content_hash="abc123",
        source_member_name="CIK0000320193.json",
        ingested_ts=pd.Timestamp("2026-03-15T09:30:00Z"),
    )

    assert list(frame.columns) == sec_bronze_module.SEC_COMPANY_FACTS_COLUMNS
    assert frame["tag"].tolist() == ["Revenues", "Assets"]
    assert frame["taxonomy"].tolist() == ["us-gaap", "us-gaap"]
    assert frame["cik"].tolist() == ["320193", "320193"]
    assert frame["entity_name"].tolist() == ["Apple Inc.", "Apple Inc."]
    assert frame["accession_number"].tolist() == [
        "0000320193-26-000001",
        "0000320193-26-000001",
    ]
    assert frame["period_start_date"].tolist() == ["2025-09-28", pd.NA]
    assert frame["period_end_date"].tolist() == ["2025-12-27", "2025-12-27"]


def test_parse_company_facts_zip_to_parquet_writes_fixture_archive(tmp_path: Path) -> None:
    zip_path = tmp_path / "companyfacts.zip"
    out_path = tmp_path / "facts.parquet"
    _write_company_facts_zip(zip_path)

    row_count = parse_company_facts_zip_to_parquet(
        zip_path,
        out_path,
        ingestion_date="2026-03-15",
        source_archive_content_hash="abc123",
        chunk_size=1,
    )

    parsed = pd.read_parquet(out_path)
    assert row_count == 2
    assert parsed["tag"].tolist() == ["Revenues", "Assets"]
    assert parsed["unit"].tolist() == ["USD", "USD"]
    assert parsed["value"].tolist() == [123456000000, 500000000000]
    assert parsed["source_member_name"].tolist() == [
        "CIK0000320193.json",
        "CIK0000320193.json",
    ]


def test_parse_company_facts_zip_to_parquet_rejects_malformed_json(tmp_path: Path) -> None:
    zip_path = tmp_path / "companyfacts.zip"
    out_path = tmp_path / "facts.parquet"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("CIK0000320193.json", "{not json")

    with pytest.raises(ValueError, match="Invalid SEC company facts JSON member"):
        parse_company_facts_zip_to_parquet(
            zip_path,
            out_path,
            ingestion_date="2026-03-15",
            source_archive_content_hash="abc123",
        )


def test_materialize_bronze_sec_company_facts_writes_parquet_from_ingestion_log(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(sec_bronze_module, "DATA_ROOT", data_root)
    raw_path = (
        data_root
        / "bronze"
        / "sec"
        / "companyfacts"
        / "ingestion_date=2026-03-15"
        / "companyfacts.zip"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    _write_company_facts_zip(raw_path)

    ingestion_log_path = data_root / "bronze" / "sec" / "ingestion_log.parquet"
    ingestion_log_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "dataset": "companyfacts",
                "source_url": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
                "retrieved_at": pd.Timestamp("2026-03-15T09:30:00Z"),
                "ingestion_date": "2026-03-15",
                "etag": "etag",
                "last_modified": "Sun, 15 Mar 2026 08:00:00 GMT",
                "content_hash": "abc123",
                "file_size": raw_path.stat().st_size,
                "local_path": str(raw_path),
                "changed_flag": True,
            }
        ],
        columns=sec_bronze_module.SEC_INGESTION_LOG_COLUMNS,
    ).to_parquet(ingestion_log_path, index=False)

    metrics = materialize_bronze_sec_company_facts()

    out_path = (
        data_root
        / "bronze"
        / "sec_company_facts"
        / "ingestion_date=2026-03-15"
        / "taxonomy=us-gaap"
        / "facts.parquet"
    )
    parsed = pd.read_parquet(out_path)
    assert metrics == {
        "source_snapshot_count": 1,
        "written_snapshot_count": 1,
        "skipped_snapshot_count": 0,
        "row_count": 2,
        "latest_ingestion_date": "2026-03-15",
    }
    assert parsed["tag"].tolist() == ["Revenues", "Assets"]
