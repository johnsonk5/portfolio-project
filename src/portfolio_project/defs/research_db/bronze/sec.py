import hashlib
import json
import os
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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

SEC_SUBMISSIONS_COLUMNS = [
    "cik",
    "entity_name",
    "entity_type",
    "sic",
    "sic_description",
    "tickers",
    "exchanges",
    "accession_number",
    "filing_date",
    "report_date",
    "acceptance_datetime",
    "act",
    "form",
    "file_number",
    "film_number",
    "items",
    "core_type",
    "size",
    "is_xbrl",
    "is_inline_xbrl",
    "is_xbrl_numeric",
    "primary_document",
    "primary_doc_description",
    "ingestion_date",
    "source_archive_path",
    "source_archive_content_hash",
    "source_member_name",
    "source_row_number",
    "ingested_ts",
]

SEC_SUBMISSION_SOURCE_FIELDS = [
    "accessionNumber",
    "filingDate",
    "reportDate",
    "acceptanceDateTime",
    "act",
    "form",
    "fileNumber",
    "filmNumber",
    "items",
    "core_type",
    "size",
    "isXBRL",
    "isInlineXBRL",
    "isXBRLNumeric",
    "primaryDocument",
    "primaryDocDescription",
]

SEC_SUBMISSIONS_CHUNK_SIZE = int(os.getenv("SEC_SUBMISSIONS_PARSE_CHUNK_SIZE", "100000"))


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


def _parsed_submissions_path(ingestion_date: str) -> Path:
    return (
        DATA_ROOT
        / "bronze"
        / "sec_submissions"
        / f"ingestion_date={ingestion_date}"
        / "submissions.parquet"
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


def _join_list_values(values: object) -> object:
    if not isinstance(values, list):
        return pd.NA
    normalized = [str(value).strip() for value in values if str(value).strip()]
    return ",".join(normalized) if normalized else pd.NA


def _value_at(values: object, index: int) -> object:
    if not isinstance(values, list) or index >= len(values):
        return pd.NA
    value = values[index]
    if value is None:
        return pd.NA
    return value


def _submission_recent_filings(document: dict) -> dict:
    filings = document.get("filings")
    if isinstance(filings, dict) and isinstance(filings.get("recent"), dict):
        return filings["recent"]
    if "accessionNumber" in document:
        return document
    return {}


def _cik_from_submission_member(member_name: str) -> object:
    stem = Path(member_name).stem
    if stem.startswith("CIK"):
        stem = stem[3:]
    if "-submissions-" in stem:
        stem = stem.split("-submissions-", 1)[0]
    return _normalize_cik(stem)


def _submission_records(
    document: dict,
    *,
    ingestion_date: str,
    source_archive_path: str,
    source_archive_content_hash: str,
    source_member_name: str,
    ingested_ts: pd.Timestamp | None = None,
) -> list[dict]:
    recent = _submission_recent_filings(document)
    accession_numbers = recent.get("accessionNumber")
    if not isinstance(accession_numbers, list):
        return []

    cik = _normalize_cik(document.get("cik", _cik_from_submission_member(source_member_name)))
    now = ingested_ts or pd.Timestamp.utcnow()
    records = []
    for index, accession_number in enumerate(accession_numbers):
        accession_text = _normalize_text(accession_number)
        if pd.isna(accession_text):
            continue
        records.append(
            {
                "cik": cik,
                "entity_name": _normalize_text(document.get("name")),
                "entity_type": _normalize_text(document.get("entityType")),
                "sic": _normalize_text(document.get("sic")),
                "sic_description": _normalize_text(document.get("sicDescription")),
                "tickers": _join_list_values(document.get("tickers")),
                "exchanges": _join_list_values(document.get("exchanges")),
                "accession_number": accession_text,
                "filing_date": _normalize_text(_value_at(recent.get("filingDate"), index)),
                "report_date": _normalize_text(_value_at(recent.get("reportDate"), index)),
                "acceptance_datetime": _normalize_text(
                    _value_at(recent.get("acceptanceDateTime"), index)
                ),
                "act": _normalize_text(_value_at(recent.get("act"), index)),
                "form": _normalize_text(_value_at(recent.get("form"), index)),
                "file_number": _normalize_text(_value_at(recent.get("fileNumber"), index)),
                "film_number": _normalize_text(_value_at(recent.get("filmNumber"), index)),
                "items": _normalize_text(_value_at(recent.get("items"), index)),
                "core_type": _normalize_text(_value_at(recent.get("core_type"), index)),
                "size": _value_at(recent.get("size"), index),
                "is_xbrl": _value_at(recent.get("isXBRL"), index),
                "is_inline_xbrl": _value_at(recent.get("isInlineXBRL"), index),
                "is_xbrl_numeric": _value_at(recent.get("isXBRLNumeric"), index),
                "primary_document": _normalize_text(
                    _value_at(recent.get("primaryDocument"), index)
                ),
                "primary_doc_description": _normalize_text(
                    _value_at(recent.get("primaryDocDescription"), index)
                ),
                "ingestion_date": ingestion_date,
                "source_archive_path": source_archive_path,
                "source_archive_content_hash": source_archive_content_hash,
                "source_member_name": source_member_name,
                "source_row_number": index + 1,
                "ingested_ts": now,
            }
        )
    return records


def parse_submission_json_document(
    document: dict,
    *,
    ingestion_date: str,
    source_archive_path: str,
    source_archive_content_hash: str,
    source_member_name: str,
    ingested_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Parse one SEC submissions JSON member into one row per accession.
    """
    records = _submission_records(
        document,
        ingestion_date=ingestion_date,
        source_archive_path=source_archive_path,
        source_archive_content_hash=source_archive_content_hash,
        source_member_name=source_member_name,
        ingested_ts=ingested_ts,
    )
    if not records:
        return pd.DataFrame(columns=SEC_SUBMISSIONS_COLUMNS)
    return pd.DataFrame(records, columns=SEC_SUBMISSIONS_COLUMNS)


def _prepare_submissions_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    for column in SEC_SUBMISSIONS_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[SEC_SUBMISSIONS_COLUMNS].copy()
    string_columns = [
        column
        for column in SEC_SUBMISSIONS_COLUMNS
        if column
        not in {
            "size",
            "is_xbrl",
            "is_inline_xbrl",
            "is_xbrl_numeric",
            "source_row_number",
            "ingested_ts",
        }
    ]
    for column in string_columns:
        prepared[column] = prepared[column].astype("string")
    prepared["source_row_number"] = pd.to_numeric(
        prepared["source_row_number"], errors="coerce"
    ).astype("Int64")
    prepared["size"] = pd.to_numeric(prepared["size"], errors="coerce").astype("Int64")
    for column in ["is_xbrl", "is_inline_xbrl", "is_xbrl_numeric"]:
        prepared[column] = prepared[column].astype("boolean")
    prepared["ingested_ts"] = pd.to_datetime(prepared["ingested_ts"], errors="coerce")
    return prepared


def _write_submissions_chunk(
    writer: pq.ParquetWriter | None,
    frame: pd.DataFrame,
    output_path: Path,
) -> pq.ParquetWriter:
    table = pa.Table.from_pandas(_prepare_submissions_frame(frame), preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter(output_path, table.schema)
    writer.write_table(table)
    return writer


def parse_submissions_zip_to_parquet(
    source_path: Path,
    output_path: Path,
    *,
    ingestion_date: str,
    source_archive_content_hash: str,
    chunk_size: int = SEC_SUBMISSIONS_CHUNK_SIZE,
) -> int:
    """
    Parse a SEC submissions.zip archive into one bronze parquet file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(".tmp.parquet")
    if temp_path.exists():
        temp_path.unlink()

    writer = None
    row_count = 0
    pending_records = []
    pending_rows = 0
    source_archive_path = str(source_path)
    ingested_ts = pd.Timestamp.utcnow()

    try:
        with zipfile.ZipFile(source_path) as archive:
            for member_name in archive.namelist():
                if not member_name.endswith(".json"):
                    continue
                with archive.open(member_name) as member_file:
                    try:
                        document = json.load(member_file)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"Invalid SEC submissions JSON member: {member_name}"
                        ) from exc
                if not isinstance(document, dict):
                    raise ValueError(f"SEC submissions member is not a JSON object: {member_name}")

                records = _submission_records(
                    document,
                    ingestion_date=ingestion_date,
                    source_archive_path=source_archive_path,
                    source_archive_content_hash=source_archive_content_hash,
                    source_member_name=member_name,
                    ingested_ts=ingested_ts,
                )
                if not records:
                    continue

                pending_records.extend(records)
                pending_rows += len(records)
                row_count += len(records)
                if pending_rows >= chunk_size:
                    chunk = pd.DataFrame(pending_records, columns=SEC_SUBMISSIONS_COLUMNS)
                    writer = _write_submissions_chunk(writer, chunk, temp_path)
                    pending_records = []
                    pending_rows = 0

        if pending_records:
            chunk = pd.DataFrame(pending_records, columns=SEC_SUBMISSIONS_COLUMNS)
            writer = _write_submissions_chunk(writer, chunk, temp_path)

        if writer is None:
            empty = pd.DataFrame(columns=SEC_SUBMISSIONS_COLUMNS)
            empty.to_parquet(temp_path, index=False)
        else:
            writer.close()
            writer = None

        if output_path.exists():
            output_path.unlink()
        shutil.move(str(temp_path), output_path)
        return row_count
    finally:
        if writer is not None:
            writer.close()
        if temp_path.exists():
            temp_path.unlink()


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


def _dataset_ingestion_rows(dataset_name: str) -> pd.DataFrame:
    ingestion_log = _read_ingestion_log(_ingestion_log_path())
    if ingestion_log.empty:
        return pd.DataFrame(columns=SEC_INGESTION_LOG_COLUMNS)
    rows = ingestion_log[
        (ingestion_log["dataset"] == dataset_name)
        & ingestion_log["ingestion_date"].notna()
        & ingestion_log["local_path"].notna()
    ].copy()
    if rows.empty:
        return pd.DataFrame(columns=SEC_INGESTION_LOG_COLUMNS)
    return rows.sort_values(["ingestion_date", "retrieved_at"], kind="stable")


def materialize_bronze_sec_company_tickers(*, force: bool = False) -> dict[str, int]:
    ticker_rows = _dataset_ingestion_rows("company_tickers")
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


def materialize_bronze_sec_submissions(*, force: bool = False) -> dict[str, int]:
    submission_rows = _dataset_ingestion_rows("submissions")
    written_count = 0
    skipped_count = 0
    row_count = 0
    latest_ingestion_date = None

    for row in submission_rows.to_dict("records"):
        ingestion_date = str(row["ingestion_date"])
        source_path = Path(str(row["local_path"]))
        if not source_path.exists():
            raise FileNotFoundError(f"SEC submissions raw archive not found: {source_path}")

        output_path = _parsed_submissions_path(ingestion_date)
        if output_path.exists() and not force:
            skipped_count += 1
            continue

        parsed_rows = parse_submissions_zip_to_parquet(
            source_path,
            output_path,
            ingestion_date=ingestion_date,
            source_archive_content_hash=str(row["content_hash"]),
        )

        written_count += 1
        row_count += parsed_rows
        latest_ingestion_date = ingestion_date

    return {
        "source_snapshot_count": int(len(submission_rows)),
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


@asset(name="bronze_sec_submissions", deps=[bronze_sec_bulk_archives])
def bronze_sec_submissions(context: AssetExecutionContext) -> None:
    """
    Parse SEC submissions.zip raw snapshots into one-row-per-accession bronze parquet.
    """
    metrics = materialize_bronze_sec_submissions()
    context.add_output_metadata(
        {
            "dataset": "sec_submissions",
            "output_root": str(DATA_ROOT / "bronze" / "sec_submissions"),
            **metrics,
        }
    )
