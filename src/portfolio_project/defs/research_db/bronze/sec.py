import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))

SEC_MANIFEST_COLUMNS = [
    "dataset",
    "source_url",
    "retrieved_at",
    "ingestion_date",
    "etag",
    "last_modified",
    "content_hash",
    "file_size",
    "local_path",
    "changed_flag",
]


@dataclass(frozen=True)
class SecBronzeDataset:
    dataset: str
    url_path: str
    raw_subdir: str
    filename: str


SEC_BRONZE_DATASETS = [
    SecBronzeDataset(
        dataset="companyfacts",
        url_path="/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        raw_subdir="companyfacts",
        filename="companyfacts.zip",
    ),
    SecBronzeDataset(
        dataset="submissions",
        url_path="/Archives/edgar/daily-index/bulkdata/submissions.zip",
        raw_subdir="submissions",
        filename="submissions.zip",
    ),
    SecBronzeDataset(
        dataset="company_tickers",
        url_path="/files/company_tickers_exchange.json",
        raw_subdir="company_tickers",
        filename="company_tickers_exchange.json",
    ),
]


def _manifest_path() -> Path:
    return DATA_ROOT / "bronze" / "sec" / "manifest.parquet"


def _empty_manifest() -> pd.DataFrame:
    return pd.DataFrame(columns=SEC_MANIFEST_COLUMNS)


def _read_manifest(path: Path) -> pd.DataFrame:
    if not path.exists():
        return _empty_manifest()
    manifest = pd.read_parquet(path)
    for column in SEC_MANIFEST_COLUMNS:
        if column not in manifest.columns:
            manifest[column] = pd.NA
    return manifest[SEC_MANIFEST_COLUMNS].copy()


def _content_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _existing_manifest_row(
    manifest: pd.DataFrame,
    *,
    dataset: str,
    content_hash: str,
) -> pd.Series | None:
    if manifest.empty:
        return None
    matches = manifest[
        (manifest["dataset"] == dataset)
        & (manifest["content_hash"] == content_hash)
        & (manifest["changed_flag"] == True)  # noqa: E712
    ]
    if matches.empty:
        return None
    return matches.sort_values("retrieved_at", kind="stable").iloc[0]


def _raw_path(dataset: SecBronzeDataset, ingestion_date: str) -> Path:
    return (
        DATA_ROOT
        / "bronze"
        / "sec"
        / dataset.raw_subdir
        / f"ingestion_date={ingestion_date}"
        / dataset.filename
    )


def _append_manifest_rows(manifest_path: Path, rows: list[dict]) -> pd.DataFrame:
    existing = _read_manifest(manifest_path)
    new_rows = pd.DataFrame(rows, columns=SEC_MANIFEST_COLUMNS)
    if existing.empty:
        updated = new_rows
    else:
        updated = pd.concat([existing, new_rows], ignore_index=True)
    updated = updated[SEC_MANIFEST_COLUMNS].sort_values(
        ["retrieved_at", "dataset"], kind="stable"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    updated.to_parquet(manifest_path, index=False)
    return updated


@asset(name="bronze_sec_bulk_archives", required_resource_keys={"sec"})
def bronze_sec_bulk_archives(context: AssetExecutionContext) -> None:
    """
    Fetch SEC bulk raw sources into bronze and append retrieval rows to the
    manifest. Identical archive bytes reuse the first stored local file path.
    """
    manifest_path = _manifest_path()
    manifest = _read_manifest(manifest_path)
    retrieved_at = datetime.now(timezone.utc)
    ingestion_date = retrieved_at.date().isoformat()
    manifest_rows = []
    changed_count = 0

    sec_client = context.resources.sec
    for dataset in SEC_BRONZE_DATASETS:
        response = sec_client.get(dataset.url_path)
        payload = response.content
        digest = _content_hash(payload)
        prior_row = _existing_manifest_row(
            manifest,
            dataset=dataset.dataset,
            content_hash=digest,
        )
        changed_flag = prior_row is None

        if changed_flag:
            local_path = _raw_path(dataset, ingestion_date)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(payload)
            changed_count += 1
        else:
            local_path = Path(str(prior_row["local_path"]))

        manifest_rows.append(
            {
                "dataset": dataset.dataset,
                "source_url": sec_client._resolve_url(dataset.url_path),
                "retrieved_at": retrieved_at,
                "ingestion_date": ingestion_date,
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_hash": digest,
                "file_size": len(payload),
                "local_path": str(local_path),
                "changed_flag": changed_flag,
            }
        )

    updated_manifest = _append_manifest_rows(manifest_path, manifest_rows)

    context.add_output_metadata(
        {
            "manifest_path": str(manifest_path),
            "retrieval_count": len(manifest_rows),
            "changed_count": changed_count,
            "manifest_row_count": len(updated_manifest),
            "ingestion_date": ingestion_date,
        }
    )
