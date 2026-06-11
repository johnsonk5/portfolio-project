import hashlib
import json
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

SEC_COMPANY_TICKERS_COLUMNS = [
    "cik",
    "name",
    "ticker",
    "exchange",
    "ingestion_date",
    "source_file",
    "source_content_hash",
    "source_row_number",
    "ingested_ts",
]

SEC_COMPANY_TICKERS_REQUIRED_FIELDS = ["cik", "name", "ticker", "exchange"]


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


def _parsed_company_tickers_path(ingestion_date: str) -> Path:
    return (
        DATA_ROOT
        / "bronze"
        / "sec_company_tickers"
        / f"ingestion_date={ingestion_date}"
        / "tickers.parquet"
    )


def _normalize_ticker(value: object) -> object:
    if value is None:
        return pd.NA
    try:
        if pd.isna(value):
            return pd.NA
    except (TypeError, ValueError):
        pass
    text = str(value).strip().upper()
    return text if text else pd.NA


def _normalize_text(value: object) -> object:
    if value is None:
        return pd.NA
    try:
        if pd.isna(value):
            return pd.NA
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text if text else pd.NA


def _normalize_cik(value: object) -> object:
    if value is None:
        return pd.NA
    try:
        if pd.isna(value):
            return pd.NA
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return pd.NA
    if text.endswith(".0"):
        text = text[:-2]
    normalized = text.lstrip("0")
    return normalized or "0"


def parse_company_tickers_exchange_json(
    payload: bytes,
    *,
    ingestion_date: str,
    source_file: str,
    source_content_hash: str,
    ingested_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Parse SEC company_tickers_exchange.json into the bronze parquet schema.
    """
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid SEC company_tickers_exchange.json payload") from exc

    if not isinstance(document, dict):
        raise ValueError("SEC company tickers payload must be a JSON object")

    fields = document.get("fields")
    rows = document.get("data")
    if not isinstance(fields, list) or not isinstance(rows, list):
        raise ValueError("SEC company tickers payload must include fields and data arrays")

    missing_fields = [
        field for field in SEC_COMPANY_TICKERS_REQUIRED_FIELDS if field not in fields
    ]
    if missing_fields:
        raise ValueError(
            "SEC company tickers payload is missing required fields: "
            + ", ".join(missing_fields)
        )

    records = []
    field_count = len(fields)
    for row_number, row in enumerate(rows, start=1):
        if not isinstance(row, list) or len(row) != field_count:
            raise ValueError(
                f"SEC company tickers row {row_number} does not match fields length"
            )
        record = dict(zip(fields, row))
        records.append(
            {
                "cik": _normalize_cik(record.get("cik")),
                "name": _normalize_text(record.get("name")),
                "ticker": _normalize_ticker(record.get("ticker")),
                "exchange": _normalize_text(record.get("exchange")),
                "ingestion_date": ingestion_date,
                "source_file": source_file,
                "source_content_hash": source_content_hash,
                "source_row_number": row_number,
                "ingested_ts": ingested_ts or pd.Timestamp.utcnow(),
            }
        )

    if not records:
        return pd.DataFrame(columns=SEC_COMPANY_TICKERS_COLUMNS)

    tickers = pd.DataFrame(records, columns=SEC_COMPANY_TICKERS_COLUMNS)
    tickers = tickers.dropna(subset=["cik", "ticker"]).copy()
    tickers["source_row_number"] = pd.to_numeric(
        tickers["source_row_number"], errors="coerce"
    ).astype("Int64")
    tickers = tickers.sort_values(["ticker", "cik", "source_row_number"], kind="stable")
    return tickers.reset_index(drop=True)


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


def _company_ticker_ingestion_rows() -> pd.DataFrame:
    ingestion_log = _read_ingestion_log(_ingestion_log_path())
    if ingestion_log.empty:
        return pd.DataFrame(columns=SEC_INGESTION_LOG_COLUMNS)
    rows = ingestion_log[
        (ingestion_log["dataset"] == "company_tickers")
        & ingestion_log["ingestion_date"].notna()
        & ingestion_log["local_path"].notna()
    ].copy()
    if rows.empty:
        return pd.DataFrame(columns=SEC_INGESTION_LOG_COLUMNS)
    return rows.sort_values(["ingestion_date", "retrieved_at"], kind="stable")


def materialize_bronze_sec_company_tickers(*, force: bool = False) -> dict[str, int]:
    ticker_rows = _company_ticker_ingestion_rows()
    written_count = 0
    skipped_count = 0
    row_count = 0
    latest_ingestion_date = None

    for row in ticker_rows.to_dict("records"):
        ingestion_date = str(row["ingestion_date"])
        source_path = Path(str(row["local_path"]))
        if not source_path.exists():
            raise FileNotFoundError(f"SEC company tickers raw file not found: {source_path}")

        output_path = _parsed_company_tickers_path(ingestion_date)
        if output_path.exists() and not force:
            skipped_count += 1
            continue

        frame = parse_company_tickers_exchange_json(
            source_path.read_bytes(),
            ingestion_date=ingestion_date,
            source_file=str(source_path),
            source_content_hash=str(row["content_hash"]),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()
        frame.to_parquet(output_path, index=False)

        written_count += 1
        row_count += len(frame)
        latest_ingestion_date = ingestion_date

    return {
        "source_snapshot_count": int(len(ticker_rows)),
        "written_snapshot_count": written_count,
        "skipped_snapshot_count": skipped_count,
        "row_count": row_count,
        "latest_ingestion_date": latest_ingestion_date or "",
    }


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


@asset(name="bronze_sec_company_tickers", deps=[bronze_sec_bulk_archives])
def bronze_sec_company_tickers(context: AssetExecutionContext) -> None:
    """
    Parse SEC company_tickers_exchange.json raw snapshots into bronze parquet.
    """
    metrics = materialize_bronze_sec_company_tickers()
    context.add_output_metadata(
        {
            "dataset": "sec_company_tickers",
            "output_root": str(DATA_ROOT / "bronze" / "sec_company_tickers"),
            **metrics,
        }
    )
