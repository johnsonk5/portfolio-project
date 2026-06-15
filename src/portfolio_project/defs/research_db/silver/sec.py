import os
from pathlib import Path

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, AssetKey, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))

SEC_SUBMISSIONS_COLUMNS = [
    "asset_id",
    "cik",
    "accession_number",
    "accession_number_nodash",
    "form",
    "filing_date",
    "report_date",
    "acceptance_datetime",
    "primary_document",
    "primary_doc_description",
    "file_number",
    "film_number",
    "act",
    "is_amendment",
    "amended_accession_number",
    "items",
    "size_bytes",
    "source_url",
    "ingestion_date",
    "ingested_ts",
]

SEC_FACTS_LONG_COLUMNS = [
    "asset_id",
    "cik",
    "accession_number",
    "taxonomy",
    "tag",
    "label",
    "description",
    "unit",
    "value",
    "value_raw",
    "decimals",
    "period_start_date",
    "period_end_date",
    "period_type",
    "fiscal_year",
    "fiscal_period",
    "form",
    "filed_date",
    "frame",
    "source_snapshot_date",
    "ingestion_date",
    "ingested_ts",
]


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


def _empty_cik_asset_id_map() -> pd.DataFrame:
    return pd.DataFrame(columns=["cik", "asset_id"])


def build_cik_asset_id_map_frame(identifiers_df: pd.DataFrame) -> pd.DataFrame:
    if identifiers_df is None or identifiers_df.empty:
        return _empty_cik_asset_id_map()

    df = identifiers_df.copy()
    has_identifier_type = "identifier_type" in df.columns
    for column in ["asset_id", "cik", "identifier_type", "identifier_value", "is_current"]:
        if column not in df.columns:
            df[column] = pd.NA
    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce").astype("Int64")
    df["cik"] = df["cik"].map(_normalize_cik)
    missing_cik = df["cik"].isna()
    df.loc[missing_cik, "cik"] = df.loc[missing_cik, "identifier_value"].map(_normalize_cik)
    df["identifier_type"] = df["identifier_type"].astype("string").str.strip().str.lower()
    current_mask = df["is_current"].map(lambda value: True if pd.isna(value) else bool(value))
    if has_identifier_type:
        identifier_mask = df["identifier_type"].isin(["cik", "sec_ticker"])
    else:
        identifier_mask = True
    df = df[df["asset_id"].notna() & df["cik"].notna() & identifier_mask & current_mask].copy()
    if df.empty:
        return _empty_cik_asset_id_map()
    return (
        df[["cik", "asset_id"]]
        .drop_duplicates()
        .sort_values(["cik", "asset_id"], kind="stable")
        .reset_index(drop=True)
    )


def resolve_sec_asset_ids_by_cik(
    frame: pd.DataFrame,
    cik_asset_id_map_df: pd.DataFrame,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        resolved = pd.DataFrame() if frame is None else frame.copy()
        if "asset_id" not in resolved.columns:
            resolved.insert(0, "asset_id", pd.Series(dtype="Int64"))
        return resolved

    source = frame.copy()
    if "asset_id" in source.columns:
        source = source.drop(columns=["asset_id"])
    if "cik" not in source.columns:
        source["cik"] = pd.NA
    source["_sec_source_row_id"] = range(len(source))
    source["cik"] = source["cik"].map(_normalize_cik)

    mapping = build_cik_asset_id_map_frame(cik_asset_id_map_df)
    resolved = source.merge(mapping, on="cik", how="left")
    asset_id = pd.to_numeric(resolved.pop("asset_id"), errors="coerce").astype("Int64")
    resolved.insert(0, "asset_id", asset_id)
    return resolved.drop(columns=["_sec_source_row_id"])


def resolve_sec_statement_item_asset_ids(
    statement_items_df: pd.DataFrame,
    cik_asset_id_map_df: pd.DataFrame,
) -> pd.DataFrame:
    return resolve_sec_asset_ids_by_cik(statement_items_df, cik_asset_id_map_df)


def _table_exists(con, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        is not None
    )


def _load_cik_asset_id_map(con) -> pd.DataFrame:
    if not _table_exists(con, "silver", "security_identifiers"):
        return _empty_cik_asset_id_map()
    identifiers_df = con.execute(
        """
        SELECT asset_id, cik, identifier_type, identifier_value, is_current
        FROM silver.security_identifiers
        WHERE asset_id IS NOT NULL
          AND (
              (cik IS NOT NULL AND trim(cik) <> '')
              OR (
                  lower(trim(identifier_type)) = 'cik'
                  AND identifier_value IS NOT NULL
                  AND trim(identifier_value) <> ''
              )
          )
        """
    ).fetch_df()
    return build_cik_asset_id_map_frame(identifiers_df)


def _parquet_files(root: Path, pattern: str) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.glob(pattern))


def _ingestion_date_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("ingestion_date="):
            return part.split("=", 1)[1]
    return ""


def _latest_ingestion_date_files(files: list[Path]) -> list[Path]:
    dated_files = [(path, _ingestion_date_from_path(path)) for path in files]
    dated_files = [(path, date) for path, date in dated_files if date]
    if not dated_files:
        return files
    latest_date = max(date for _, date in dated_files)
    return [path for path, date in dated_files if date == latest_date]


def _bronze_sec_submissions_files(data_root: Path | None = None) -> list[Path]:
    root = data_root or DATA_ROOT
    return _latest_ingestion_date_files(
        _parquet_files(
            root / "bronze" / "sec_submissions",
            "ingestion_date=*/submissions.parquet",
        )
    )


def _taxonomy_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("taxonomy="):
            return part.split("=", 1)[1]
    return ""


def _latest_ingestion_date_files_by_taxonomy(files: list[Path]) -> list[Path]:
    latest_by_taxonomy: dict[str, tuple[str, Path]] = {}
    for path in files:
        taxonomy = _taxonomy_from_path(path)
        ingestion_date = _ingestion_date_from_path(path)
        if not taxonomy or not ingestion_date:
            continue
        current = latest_by_taxonomy.get(taxonomy)
        if current is None or ingestion_date > current[0]:
            latest_by_taxonomy[taxonomy] = (ingestion_date, path)
    if not latest_by_taxonomy:
        return files
    return sorted(path for _, path in latest_by_taxonomy.values())


def _bronze_sec_facts_files(data_root: Path | None = None) -> list[Path]:
    root = data_root or DATA_ROOT
    return _latest_ingestion_date_files_by_taxonomy(
        _parquet_files(
            root / "bronze" / "sec_company_facts",
            "ingestion_date=*/taxonomy=*/facts.parquet",
        )
    )


def _configure_sec_duckdb_workload(con) -> None:
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET threads = 1")


def _coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def build_silver_sec_submissions_frame(
    bronze_submissions_df: pd.DataFrame,
    cik_asset_id_map_df: pd.DataFrame,
) -> pd.DataFrame:
    if bronze_submissions_df is None or bronze_submissions_df.empty:
        return pd.DataFrame(columns=SEC_SUBMISSIONS_COLUMNS)

    df = bronze_submissions_df.copy()
    for column in [
        "cik",
        "accession_number",
        "form",
        "filing_date",
        "report_date",
        "acceptance_datetime",
        "primary_document",
        "primary_doc_description",
        "file_number",
        "film_number",
        "act",
        "items",
        "size",
        "source_archive_path",
        "source_member_name",
        "ingestion_date",
        "ingested_ts",
    ]:
        if column not in df.columns:
            df[column] = pd.NA
    df["cik"] = df["cik"].map(_normalize_cik)
    df["accession_number"] = df["accession_number"].astype("string").str.strip()
    df = df[df["cik"].notna() & df["accession_number"].notna() & df["accession_number"].ne("")]
    if df.empty:
        return pd.DataFrame(columns=SEC_SUBMISSIONS_COLUMNS)

    resolved = resolve_sec_asset_ids_by_cik(df, cik_asset_id_map_df)
    resolved["accession_number_nodash"] = (
        resolved["accession_number"].astype("string").str.replace("-", "", regex=False)
    )
    resolved["filing_date"] = _coerce_date(resolved["filing_date"])
    resolved["report_date"] = _coerce_date(resolved["report_date"])
    resolved["acceptance_datetime"] = pd.to_datetime(
        resolved["acceptance_datetime"], errors="coerce", utc=True
    )
    resolved["is_amendment"] = (
        resolved["form"].astype("string").str.upper().str.endswith("/A").fillna(False)
    )
    resolved["amended_accession_number"] = pd.NA
    resolved["size_bytes"] = pd.to_numeric(resolved["size"], errors="coerce").astype("Int64")
    resolved["source_url"] = resolved["source_archive_path"].astype("string")
    member_name = resolved["source_member_name"].astype("string")
    resolved.loc[member_name.notna() & member_name.ne(""), "source_url"] = (
        resolved["source_url"].astype("string") + "#" + member_name
    )
    resolved["ingestion_date"] = _coerce_date(resolved["ingestion_date"])
    resolved["ingested_ts"] = pd.to_datetime(resolved["ingested_ts"], errors="coerce", utc=True)
    resolved = resolved.sort_values(
        ["accession_number", "asset_id", "ingestion_date", "ingested_ts"],
        ascending=[True, True, False, False],
        kind="stable",
    ).drop_duplicates(subset=["accession_number", "asset_id"], keep="first")
    return resolved[SEC_SUBMISSIONS_COLUMNS].reset_index(drop=True)


def build_silver_sec_facts_long_frame(
    bronze_facts_df: pd.DataFrame,
    cik_asset_id_map_df: pd.DataFrame,
) -> pd.DataFrame:
    if bronze_facts_df is None or bronze_facts_df.empty:
        return pd.DataFrame(columns=SEC_FACTS_LONG_COLUMNS)

    df = bronze_facts_df.copy()
    for column in [
        "cik",
        "accession_number",
        "taxonomy",
        "tag",
        "label",
        "description",
        "unit",
        "value",
        "fiscal_year",
        "fiscal_period",
        "form",
        "filed_date",
        "period_start_date",
        "period_end_date",
        "frame",
        "ingestion_date",
        "ingested_ts",
    ]:
        if column not in df.columns:
            df[column] = pd.NA
    df["cik"] = df["cik"].map(_normalize_cik)
    df["accession_number"] = df["accession_number"].astype("string").str.strip()
    df = df[df["cik"].notna() & df["accession_number"].notna() & df["accession_number"].ne("")]
    if df.empty:
        return pd.DataFrame(columns=SEC_FACTS_LONG_COLUMNS)

    resolved = resolve_sec_asset_ids_by_cik(df, cik_asset_id_map_df)
    resolved["value_raw"] = resolved["value"].astype("string")
    resolved["value"] = pd.to_numeric(resolved["value"], errors="coerce")
    resolved["decimals"] = pd.NA
    resolved["period_start_date"] = _coerce_date(resolved["period_start_date"])
    resolved["period_end_date"] = _coerce_date(resolved["period_end_date"])
    resolved["period_type"] = resolved["period_start_date"].map(
        lambda value: "instant" if pd.isna(value) else "duration"
    )
    resolved["fiscal_year"] = pd.to_numeric(resolved["fiscal_year"], errors="coerce").astype(
        "Int64"
    )
    resolved["filed_date"] = _coerce_date(resolved["filed_date"])
    resolved["source_snapshot_date"] = _coerce_date(resolved["ingestion_date"])
    resolved["ingestion_date"] = _coerce_date(resolved["ingestion_date"])
    resolved["ingested_ts"] = pd.to_datetime(resolved["ingested_ts"], errors="coerce", utc=True)
    resolved = resolved.sort_values(
        [
            "cik",
            "asset_id",
            "accession_number",
            "taxonomy",
            "tag",
            "unit",
            "period_start_date",
            "period_end_date",
            "frame",
            "ingestion_date",
            "ingested_ts",
        ],
        ascending=[True, True, True, True, True, True, True, True, True, False, False],
        kind="stable",
    ).drop_duplicates(
        subset=[
            "cik",
            "asset_id",
            "accession_number",
            "taxonomy",
            "tag",
            "unit",
            "period_start_date",
            "period_end_date",
            "frame",
        ],
        keep="first",
    )
    return resolved[SEC_FACTS_LONG_COLUMNS].reset_index(drop=True)


def _write_table(con, table_name: str, frame: pd.DataFrame, columns: list[str]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    prepared = frame.copy()
    for column in columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[columns].copy()
    con.register("sec_silver_df", prepared)
    con.execute(f"CREATE OR REPLACE TABLE silver.{table_name} AS SELECT * FROM sec_silver_df")


def _parquet_path_params(files: list[Path]) -> list[str]:
    return [path.as_posix() for path in files]


def _register_cik_asset_id_map(con, cik_asset_id_map_df: pd.DataFrame) -> None:
    try:
        con.unregister("cik_asset_id_map_df")
    except duckdb.CatalogException:
        pass
    con.register("cik_asset_id_map_df", build_cik_asset_id_map_frame(cik_asset_id_map_df))


def _write_silver_sec_submissions_from_parquet(
    con,
    files: list[Path],
    cik_asset_id_map_df: pd.DataFrame,
) -> None:
    if not files:
        _write_table(
            con,
            "sec_submissions",
            pd.DataFrame(columns=SEC_SUBMISSIONS_COLUMNS),
            SEC_SUBMISSIONS_COLUMNS,
        )
        return

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    _configure_sec_duckdb_workload(con)
    _register_cik_asset_id_map(con, cik_asset_id_map_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.sec_submissions AS
        WITH bronze AS (
            SELECT
                coalesce(
                    nullif(regexp_replace(trim(CAST(cik AS VARCHAR)), '^0+', ''), ''),
                    '0'
                ) AS cik,
                trim(CAST(accession_number AS VARCHAR)) AS accession_number,
                CAST(form AS VARCHAR) AS form,
                try_cast(filing_date AS DATE) AS filing_date,
                try_cast(report_date AS DATE) AS report_date,
                coalesce(
                    try_strptime(CAST(acceptance_datetime AS VARCHAR), '%Y%m%d%H%M%S'),
                    try_cast(acceptance_datetime AS TIMESTAMP)
                ) AS acceptance_datetime,
                CAST(primary_document AS VARCHAR) AS primary_document,
                CAST(primary_doc_description AS VARCHAR) AS primary_doc_description,
                CAST(file_number AS VARCHAR) AS file_number,
                CAST(film_number AS VARCHAR) AS film_number,
                CAST(act AS VARCHAR) AS act,
                CAST(items AS VARCHAR) AS items,
                try_cast(size AS BIGINT) AS size_bytes,
                CAST(source_archive_path AS VARCHAR) AS source_archive_path,
                CAST(source_member_name AS VARCHAR) AS source_member_name,
                try_cast(ingestion_date AS DATE) AS ingestion_date,
                try_cast(ingested_ts AS TIMESTAMP) AS ingested_ts
            FROM read_parquet(?, union_by_name = true)
            WHERE cik IS NOT NULL
              AND trim(CAST(cik AS VARCHAR)) <> ''
              AND accession_number IS NOT NULL
              AND trim(CAST(accession_number AS VARCHAR)) <> ''
        ),
        expanded AS (
            SELECT
                CAST(map.asset_id AS BIGINT) AS asset_id,
                bronze.cik,
                bronze.accession_number,
                replace(bronze.accession_number, '-', '') AS accession_number_nodash,
                bronze.form,
                bronze.filing_date,
                bronze.report_date,
                bronze.acceptance_datetime,
                bronze.primary_document,
                bronze.primary_doc_description,
                bronze.file_number,
                bronze.film_number,
                bronze.act,
                coalesce(upper(trim(bronze.form)) LIKE '%/A', false) AS is_amendment,
                CAST(NULL AS VARCHAR) AS amended_accession_number,
                bronze.items,
                bronze.size_bytes,
                CASE
                    WHEN bronze.source_member_name IS NOT NULL
                         AND trim(bronze.source_member_name) <> ''
                    THEN bronze.source_archive_path || '#' || bronze.source_member_name
                    ELSE bronze.source_archive_path
                END AS source_url,
                bronze.ingestion_date,
                bronze.ingested_ts
            FROM bronze
            LEFT JOIN cik_asset_id_map_df AS map
              ON bronze.cik = map.cik
        )
        SELECT
            asset_id,
            cik,
            accession_number,
            accession_number_nodash,
            form,
            filing_date,
            report_date,
            acceptance_datetime,
            primary_document,
            primary_doc_description,
            file_number,
            film_number,
            act,
            is_amendment,
            amended_accession_number,
            items,
            size_bytes,
            source_url,
            ingestion_date,
            ingested_ts
        FROM expanded
        """,
        [_parquet_path_params(files)],
    )


def _write_silver_sec_facts_long_from_parquet(
    con,
    files: list[Path],
    cik_asset_id_map_df: pd.DataFrame,
) -> None:
    if not files:
        _write_table(
            con,
            "sec_facts_long",
            pd.DataFrame(columns=SEC_FACTS_LONG_COLUMNS),
            SEC_FACTS_LONG_COLUMNS,
        )
        return

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    _configure_sec_duckdb_workload(con)
    _register_cik_asset_id_map(con, cik_asset_id_map_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.sec_facts_long AS
        WITH bronze AS (
            SELECT
                coalesce(
                    nullif(regexp_replace(trim(CAST(cik AS VARCHAR)), '^0+', ''), ''),
                    '0'
                ) AS cik,
                trim(CAST(accession_number AS VARCHAR)) AS accession_number,
                CAST(taxonomy AS VARCHAR) AS taxonomy,
                CAST(tag AS VARCHAR) AS tag,
                CAST(label AS VARCHAR) AS label,
                CAST(description AS VARCHAR) AS description,
                CAST(unit AS VARCHAR) AS unit,
                try_cast(value AS DOUBLE) AS value,
                CAST(value AS VARCHAR) AS value_raw,
                CAST(NULL AS VARCHAR) AS decimals,
                try_cast(period_start_date AS DATE) AS period_start_date,
                try_cast(period_end_date AS DATE) AS period_end_date,
                CASE
                    WHEN try_cast(period_start_date AS DATE) IS NULL THEN 'instant'
                    ELSE 'duration'
                END AS period_type,
                try_cast(fiscal_year AS BIGINT) AS fiscal_year,
                CAST(fiscal_period AS VARCHAR) AS fiscal_period,
                CAST(form AS VARCHAR) AS form,
                try_cast(filed_date AS DATE) AS filed_date,
                CAST(frame AS VARCHAR) AS frame,
                try_cast(ingestion_date AS DATE) AS source_snapshot_date,
                try_cast(ingestion_date AS DATE) AS ingestion_date,
                try_cast(ingested_ts AS TIMESTAMP) AS ingested_ts
            FROM read_parquet(?, union_by_name = true)
            WHERE cik IS NOT NULL
              AND trim(CAST(cik AS VARCHAR)) <> ''
              AND accession_number IS NOT NULL
              AND trim(CAST(accession_number AS VARCHAR)) <> ''
        ),
        expanded AS (
            SELECT
                CAST(map.asset_id AS BIGINT) AS asset_id,
                bronze.*
            FROM bronze
            LEFT JOIN cik_asset_id_map_df AS map
              ON bronze.cik = map.cik
        )
        SELECT
            asset_id,
            cik,
            accession_number,
            taxonomy,
            tag,
            label,
            description,
            unit,
            value,
            value_raw,
            decimals,
            period_start_date,
            period_end_date,
            period_type,
            fiscal_year,
            fiscal_period,
            form,
            filed_date,
            frame,
            source_snapshot_date,
            ingestion_date,
            ingested_ts
        FROM expanded
        """,
        [_parquet_path_params(files)],
    )


def _silver_table_metrics(
    con,
    table_name: str,
    cik_asset_id_map_df: pd.DataFrame,
) -> dict[str, int]:
    row_count = con.execute(f"SELECT count(*) FROM silver.{table_name}").fetchone()[0]
    mapped_asset_id_rows = con.execute(
        f"SELECT count(*) FROM silver.{table_name} WHERE asset_id IS NOT NULL"
    ).fetchone()[0]
    return {
        "row_count": int(row_count or 0),
        "mapped_asset_id_rows": int(mapped_asset_id_rows or 0),
        "mapped_cik_count": int(cik_asset_id_map_df["cik"].nunique()),
    }


def materialize_silver_sec_submissions(
    context: AssetExecutionContext,
    *,
    data_root: Path | None = None,
) -> dict[str, int]:
    con = context.resources.research_duckdb
    cik_asset_id_map_df = _load_cik_asset_id_map(con)
    _write_silver_sec_submissions_from_parquet(
        con,
        _bronze_sec_submissions_files(data_root),
        cik_asset_id_map_df,
    )
    return _silver_table_metrics(con, "sec_submissions", cik_asset_id_map_df)


def materialize_silver_sec_facts_long(
    context: AssetExecutionContext,
    *,
    data_root: Path | None = None,
) -> dict[str, int]:
    con = context.resources.research_duckdb
    cik_asset_id_map_df = _load_cik_asset_id_map(con)
    _write_silver_sec_facts_long_from_parquet(
        con,
        _bronze_sec_facts_files(data_root),
        cik_asset_id_map_df,
    )
    return _silver_table_metrics(con, "sec_facts_long", cik_asset_id_map_df)


@asset(
    name="sec_submissions",
    key_prefix=["silver"],
    deps=[AssetKey("bronze_sec_submissions"), AssetKey(["silver", "security_identifiers"])],
    required_resource_keys={"research_duckdb"},
)
def silver_sec_submissions(context: AssetExecutionContext) -> None:
    metrics = materialize_silver_sec_submissions(context)
    context.add_output_metadata({"table": "silver.sec_submissions", **metrics})


@asset(
    name="sec_facts_long",
    key_prefix=["silver"],
    deps=[AssetKey("bronze_sec_company_facts"), AssetKey(["silver", "security_identifiers"])],
    required_resource_keys={"research_duckdb"},
)
def silver_sec_facts_long(context: AssetExecutionContext) -> None:
    metrics = materialize_silver_sec_facts_long(context)
    context.add_output_metadata({"table": "silver.sec_facts_long", **metrics})
