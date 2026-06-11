import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))

SEC_INGESTION_LOG_COLUMNS = [
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


def _ingestion_log_path() -> Path:
    return DATA_ROOT / "bronze" / "sec" / "ingestion_log.parquet"


def _empty_ingestion_log() -> pd.DataFrame:
    return pd.DataFrame(columns=SEC_INGESTION_LOG_COLUMNS)


def _read_ingestion_log(path: Path) -> pd.DataFrame:
    if not path.exists():
        return _empty_ingestion_log()
    ingestion_log = pd.read_parquet(path)
    for column in SEC_INGESTION_LOG_COLUMNS:
        if column not in ingestion_log.columns:
            ingestion_log[column] = pd.NA
    return ingestion_log[SEC_INGESTION_LOG_COLUMNS].copy()


def _content_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _existing_ingestion_log_row(
    ingestion_log: pd.DataFrame,
    *,
    dataset: str,
    content_hash: str,
) -> pd.Series | None:
    if ingestion_log.empty:
        return None
    matches = ingestion_log[
        (ingestion_log["dataset"] == dataset)
        & (ingestion_log["content_hash"] == content_hash)
        & (ingestion_log["changed_flag"] == True)  # noqa: E712
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


def _append_ingestion_log_rows(ingestion_log_path: Path, rows: list[dict]) -> pd.DataFrame:
    existing = _read_ingestion_log(ingestion_log_path)
    new_rows = pd.DataFrame(rows, columns=SEC_INGESTION_LOG_COLUMNS)
    if existing.empty:
        updated = new_rows
    else:
        updated = pd.concat([existing, new_rows], ignore_index=True)
    updated = updated[SEC_INGESTION_LOG_COLUMNS].sort_values(
        ["retrieved_at", "dataset"], kind="stable"
    )
    ingestion_log_path.parent.mkdir(parents=True, exist_ok=True)
    updated.to_parquet(ingestion_log_path, index=False)
    return updated


@asset(name="bronze_sec_bulk_archives", required_resource_keys={"sec"})
def bronze_sec_bulk_archives(context: AssetExecutionContext) -> None:
    """
    Fetch SEC bulk raw sources into bronze and append retrieval rows to the
    ingestion log. Identical archive bytes reuse the first stored local file path.
    """
    ingestion_log_path = _ingestion_log_path()
    ingestion_log = _read_ingestion_log(ingestion_log_path)
    retrieved_at = datetime.now(timezone.utc)
    ingestion_date = retrieved_at.date().isoformat()
    ingestion_log_rows = []
    changed_count = 0

    sec_client = context.resources.sec
    for dataset in SEC_BRONZE_DATASETS:
        response = sec_client.get(dataset.url_path)
        payload = response.content
        digest = _content_hash(payload)
        prior_row = _existing_ingestion_log_row(
            ingestion_log,
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

        ingestion_log_rows.append(
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

    updated_ingestion_log = _append_ingestion_log_rows(ingestion_log_path, ingestion_log_rows)

    context.add_output_metadata(
        {
            "ingestion_log_path": str(ingestion_log_path),
            "retrieval_count": len(ingestion_log_rows),
            "changed_count": changed_count,
            "ingestion_log_row_count": len(updated_ingestion_log),
            "ingestion_date": ingestion_date,
        }
    )
