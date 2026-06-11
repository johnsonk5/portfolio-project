from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import materialize

import portfolio_project.defs.research_db.bronze.sec as sec_bronze_module
from portfolio_project.defs.research_db.bronze.sec import (
    SEC_BRONZE_DATASETS,
    bronze_sec_bulk_archives,
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


def test_bronze_sec_bulk_archives_writes_raw_files_and_manifest(
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

    manifest_path = data_root / "bronze" / "sec" / "manifest.parquet"
    manifest = pd.read_parquet(manifest_path)

    assert list(manifest.columns) == sec_bronze_module.SEC_MANIFEST_COLUMNS
    assert manifest["dataset"].tolist() == [
        "company_tickers",
        "companyfacts",
        "submissions",
    ]
    assert manifest["ingestion_date"].tolist() == ["2026-03-15"] * 3
    assert manifest["changed_flag"].tolist() == [True, True, True]
    assert manifest["file_size"].tolist() == [
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

    manifest = pd.read_parquet(data_root / "bronze" / "sec" / "manifest.parquet")
    assert len(manifest) == 6

    first_rows = manifest[manifest["ingestion_date"] == "2026-03-15"].sort_values("dataset")
    second_rows = manifest[manifest["ingestion_date"] == "2026-03-16"].sort_values("dataset")

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
