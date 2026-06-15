import os
from pathlib import Path

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, AssetKey, asset
from dagster._core.errors import DagsterInvalidPropertyError, DagsterInvariantViolationError

from portfolio_project.defs.research_db.dq_checks import log_sec_fundamentals_quality_checks

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

SEC_STATEMENT_ITEMS_COLUMNS = [
    "asset_id",
    "cik",
    "accession_number",
    "canonical_metric",
    "statement_type",
    "taxonomy",
    "tag",
    "unit",
    "reported_value",
    "value",
    "canonical_sign_rule",
    "period_start_date",
    "period_end_date",
    "period_type",
    "fiscal_year",
    "fiscal_period",
    "form",
    "filing_date",
    "acceptance_datetime",
    "availability_date",
    "mapping_version",
    "mapping_priority",
    "source_expression",
    "source_accession_number",
    "source_form",
    "source_filed_date",
    "source_acceptance_datetime",
    "period_match_type",
    "is_component_sum",
    "is_fallback_concept",
    "is_restricted_cash_included",
    "is_lease_inclusive_debt",
    "is_ytd_derived_quarter",
    "source_snapshot_date",
    "ingested_ts",
]

SEC_STATEMENT_MAPPING_VERSION = "sec_us_gaap_v1"

SEC_DIRECT_CONCEPT_MAPPINGS = [
    {
        "canonical_metric": "revenue",
        "statement_type": "income_statement",
        "mapping_priority": 10,
        "tag": "RevenueFromContractWithCustomerExcludingAssessedTax",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "revenue",
        "statement_type": "income_statement",
        "mapping_priority": 20,
        "tag": "RevenueFromContractWithCustomerIncludingAssessedTax",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "revenue",
        "statement_type": "income_statement",
        "mapping_priority": 30,
        "tag": "Revenues",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "revenue",
        "statement_type": "income_statement",
        "mapping_priority": 40,
        "tag": "SalesRevenueNet",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "net_income",
        "statement_type": "income_statement",
        "mapping_priority": 10,
        "tag": "NetIncomeLoss",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "net_income",
        "statement_type": "income_statement",
        "mapping_priority": 20,
        "tag": "ProfitLoss",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "net_income",
        "statement_type": "income_statement",
        "mapping_priority": 30,
        "tag": "NetIncomeLossAvailableToCommonStockholdersBasic",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "assets",
        "statement_type": "balance_sheet",
        "mapping_priority": 10,
        "tag": "Assets",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "equity",
        "statement_type": "balance_sheet",
        "mapping_priority": 10,
        "tag": "StockholdersEquity",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "equity",
        "statement_type": "balance_sheet",
        "mapping_priority": 20,
        "tag": "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "debt",
        "statement_type": "balance_sheet",
        "mapping_priority": 50,
        "tag": "LongTermDebt",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "cash",
        "statement_type": "balance_sheet",
        "mapping_priority": 10,
        "tag": "CashAndCashEquivalentsAtCarryingValue",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "cash",
        "statement_type": "balance_sheet",
        "mapping_priority": 20,
        "tag": "Cash",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "cash",
        "statement_type": "balance_sheet",
        "mapping_priority": 30,
        "tag": "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "unit": "USD",
        "period_type": "instant",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "diluted_shares",
        "statement_type": "shares",
        "mapping_priority": 10,
        "tag": "WeightedAverageNumberOfDilutedSharesOutstanding",
        "unit": "shares",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "diluted_eps",
        "statement_type": "income_statement",
        "mapping_priority": 10,
        "tag": "EarningsPerShareDiluted",
        "unit": "USD/shares",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "diluted_eps",
        "statement_type": "income_statement",
        "mapping_priority": 20,
        "tag": "EarningsPerShareBasicAndDiluted",
        "unit": "USD/shares",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "operating_cash_flow",
        "statement_type": "cash_flow",
        "mapping_priority": 10,
        "tag": "NetCashProvidedByUsedInOperatingActivities",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "operating_cash_flow",
        "statement_type": "cash_flow",
        "mapping_priority": 20,
        "tag": "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "preserve_reported_sign",
    },
    {
        "canonical_metric": "capex",
        "statement_type": "cash_flow",
        "mapping_priority": 10,
        "tag": "PaymentsToAcquirePropertyPlantAndEquipment",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "positive_cash_outflow",
    },
    {
        "canonical_metric": "capex",
        "statement_type": "cash_flow",
        "mapping_priority": 20,
        "tag": "PaymentsToAcquireProductiveAssets",
        "unit": "USD",
        "period_type": "duration",
        "canonical_sign_rule": "positive_cash_outflow",
    },
]

SEC_DEBT_COMPONENT_MAPPINGS = [
    {
        "mapping_priority": 10,
        "required_tags": ["LongTermDebtCurrent", "LongTermDebtNoncurrent"],
        "optional_tags": ["ShortTermBorrowings"],
        "source_expression": (
            "ShortTermBorrowings + LongTermDebtCurrent + LongTermDebtNoncurrent"
        ),
        "is_lease_inclusive_debt": False,
    },
    {
        "mapping_priority": 20,
        "required_tags": [
            "LongTermDebtAndCapitalLeaseObligationsCurrent",
            "LongTermDebtAndCapitalLeaseObligations",
        ],
        "optional_tags": ["ShortTermBorrowings"],
        "source_expression": (
            "ShortTermBorrowings + "
            "LongTermDebtAndCapitalLeaseObligationsCurrent + "
            "LongTermDebtAndCapitalLeaseObligations"
        ),
        "is_lease_inclusive_debt": True,
    },
    {
        "mapping_priority": 30,
        "required_tags": ["LongTermDebtCurrent", "LongTermDebtNoncurrent"],
        "optional_tags": [],
        "source_expression": "LongTermDebtCurrent + LongTermDebtNoncurrent",
        "is_lease_inclusive_debt": False,
    },
    {
        "mapping_priority": 40,
        "required_tags": [
            "LongTermDebtAndCapitalLeaseObligationsCurrent",
            "LongTermDebtAndCapitalLeaseObligations",
        ],
        "optional_tags": [],
        "source_expression": (
            "LongTermDebtAndCapitalLeaseObligationsCurrent + "
            "LongTermDebtAndCapitalLeaseObligations"
        ),
        "is_lease_inclusive_debt": True,
    },
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
    df["identifier_type"] = df["identifier_type"].astype("string").str.strip().str.lower()
    missing_cik = df["cik"].isna()
    if has_identifier_type:
        identifier_value_cik_mask = missing_cik & df["identifier_type"].eq("cik")
    else:
        identifier_value_cik_mask = missing_cik
    df.loc[identifier_value_cik_mask, "cik"] = df.loc[
        identifier_value_cik_mask, "identifier_value"
    ].map(_normalize_cik)
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
    temp_directory = DATA_ROOT / "duckdb" / "tmp"
    temp_directory.mkdir(parents=True, exist_ok=True)
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET threads = 1")
    con.execute(f"SET temp_directory = '{temp_directory.as_posix()}'")


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


def _mapping_frame() -> pd.DataFrame:
    return pd.DataFrame(SEC_DIRECT_CONCEPT_MAPPINGS)


def _supported_sec_units_by_tag() -> dict[str, list[str]]:
    supported: dict[str, set[str]] = {}
    for mapping in SEC_DIRECT_CONCEPT_MAPPINGS:
        supported.setdefault(str(mapping["tag"]), set()).add(str(mapping["unit"]))
    for mapping in SEC_DEBT_COMPONENT_MAPPINGS:
        for tag in [*mapping["required_tags"], *mapping["optional_tags"]]:
            supported.setdefault(str(tag), set()).add("USD")
    return {tag: sorted(units) for tag, units in sorted(supported.items())}


def _context_dq_metadata(
    context: AssetExecutionContext,
) -> tuple[str | None, str | None, str | None]:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None
    try:
        partition_key = getattr(context, "partition_key", None)
    except (DagsterInvalidPropertyError, DagsterInvariantViolationError):
        partition_key = None
    return str(run_id) if run_id else None, job_name, partition_key


def _log_sec_dq_checks(context: AssetExecutionContext) -> None:
    resources = getattr(context, "resources", None)
    if resources is None or not hasattr(resources, "duckdb"):
        return
    run_id, job_name, partition_key = _context_dq_metadata(context)
    log_sec_fundamentals_quality_checks(
        measured_con=context.resources.research_duckdb,
        observability_con=context.resources.duckdb,
        supported_units_by_tag=_supported_sec_units_by_tag(),
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )


def _period_match_type(row: pd.Series) -> str:
    fiscal_period = str(row.get("fiscal_period") or "").upper()
    period_type = str(row.get("period_type") or "")
    if period_type == "instant":
        return "period_end"
    if fiscal_period == "FY":
        return "exact_annual"
    if fiscal_period.startswith("Q"):
        return "exact_quarter"
    return "reported_period"


def _availability_date(
    acceptance_datetime: pd.Series,
    filing_date: pd.Series,
) -> pd.Series:
    acceptance_dates = pd.to_datetime(acceptance_datetime, errors="coerce", utc=True).dt.date
    filing_dates = _coerce_date(filing_date)
    return acceptance_dates.combine_first(filing_dates)


def _enrich_statement_candidates_with_submissions(
    candidates: pd.DataFrame,
    submissions_df: pd.DataFrame,
) -> pd.DataFrame:
    enriched = candidates.copy()
    if submissions_df is None or submissions_df.empty:
        enriched["filing_date"] = _coerce_date(enriched.get("filed_date", pd.Series(dtype=object)))
        enriched["acceptance_datetime"] = pd.NaT
        return enriched

    submissions = submissions_df.copy()
    for column in ["accession_number", "filing_date", "acceptance_datetime"]:
        if column not in submissions.columns:
            submissions[column] = pd.NA
    submissions["accession_number"] = submissions["accession_number"].astype("string").str.strip()
    submissions["filing_date"] = _coerce_date(submissions["filing_date"])
    submissions["acceptance_datetime"] = pd.to_datetime(
        submissions["acceptance_datetime"], errors="coerce", utc=True
    )
    submissions = submissions.sort_values(
        ["accession_number", "filing_date", "acceptance_datetime"],
        ascending=[True, False, False],
        kind="stable",
    ).drop_duplicates(subset=["accession_number"], keep="first")
    enriched = enriched.merge(
        submissions[["accession_number", "filing_date", "acceptance_datetime"]],
        on="accession_number",
        how="left",
    )
    enriched["filing_date"] = enriched["filing_date"].combine_first(
        _coerce_date(enriched.get("filed_date", pd.Series(dtype=object)))
    )
    return enriched


def _finalize_statement_items(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(columns=SEC_STATEMENT_ITEMS_COLUMNS)

    result = candidates.copy()
    result["availability_date"] = _availability_date(
        result["acceptance_datetime"], result["filing_date"]
    )
    result["source_accession_number"] = result["accession_number"]
    result["source_form"] = result["form"]
    result["source_filed_date"] = result["filed_date"]
    result["source_acceptance_datetime"] = result["acceptance_datetime"]
    result["period_match_type"] = result.apply(_period_match_type, axis=1)
    result["is_ytd_derived_quarter"] = False
    result["mapping_version"] = SEC_STATEMENT_MAPPING_VERSION
    result["is_fallback_concept"] = (
        result["mapping_priority"]
        > result.groupby("canonical_metric")["mapping_priority"].transform("min")
    )
    result = result.sort_values(
        [
            "asset_id",
            "cik",
            "accession_number",
            "canonical_metric",
            "period_end_date",
            "fiscal_year",
            "fiscal_period",
            "mapping_priority",
            "is_component_sum",
            "source_acceptance_datetime",
            "source_filed_date",
            "source_snapshot_date",
            "ingested_ts",
        ],
        ascending=[
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            False,
            False,
            False,
            False,
        ],
        kind="stable",
    ).drop_duplicates(
        subset=[
            "asset_id",
            "cik",
            "accession_number",
            "canonical_metric",
            "period_end_date",
            "fiscal_year",
            "fiscal_period",
        ],
        keep="first",
    )
    for column in SEC_STATEMENT_ITEMS_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    return result[SEC_STATEMENT_ITEMS_COLUMNS].reset_index(drop=True)


def _build_direct_statement_candidates(facts_df: pd.DataFrame) -> pd.DataFrame:
    mappings = _mapping_frame()
    candidates = facts_df.merge(
        mappings,
        on=["tag", "unit", "period_type"],
        how="inner",
        suffixes=("", "_mapping"),
    )
    if candidates.empty:
        return candidates
    candidates = candidates[candidates["taxonomy"].eq("us-gaap")].copy()
    candidates["reported_value"] = pd.to_numeric(candidates["value"], errors="coerce")
    candidates["value"] = candidates["reported_value"]
    capex_mask = candidates["canonical_sign_rule"].eq("positive_cash_outflow")
    candidates.loc[capex_mask, "value"] = candidates.loc[capex_mask, "value"].abs()
    candidates["source_expression"] = candidates["tag"]
    candidates["is_component_sum"] = False
    candidates["is_restricted_cash_included"] = candidates["tag"].eq(
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"
    )
    candidates["is_lease_inclusive_debt"] = candidates["tag"].str.contains(
        "CapitalLease", na=False
    )
    return candidates


def _build_debt_component_candidates(facts_df: pd.DataFrame) -> pd.DataFrame:
    debt_facts = facts_df[
        facts_df["taxonomy"].eq("us-gaap")
        & facts_df["unit"].eq("USD")
        & facts_df["period_type"].eq("instant")
    ].copy()
    if debt_facts.empty:
        return pd.DataFrame()

    key_columns = [
        "asset_id",
        "cik",
        "accession_number",
        "period_end_date",
        "fiscal_year",
        "fiscal_period",
    ]
    rows = []
    for _, group in debt_facts.groupby(key_columns, dropna=False, sort=False):
        tag_rows = {
            str(row["tag"]): row
            for _, row in group.sort_values(
                ["source_snapshot_date", "ingested_ts"], ascending=[False, False], kind="stable"
            ).iterrows()
        }
        for mapping in SEC_DEBT_COMPONENT_MAPPINGS:
            required_tags = mapping["required_tags"]
            if not all(tag in tag_rows for tag in required_tags):
                continue
            component_tags = [
                tag
                for tag in [*mapping["optional_tags"], *required_tags]
                if tag in tag_rows
            ]
            components = [tag_rows[tag] for tag in component_tags]
            primary = components[0]
            value = sum(
                pd.to_numeric(component["value"], errors="coerce")
                for component in components
            )
            rows.append(
                {
                    **{column: primary.get(column) for column in facts_df.columns},
                    "canonical_metric": "debt",
                    "statement_type": "balance_sheet",
                    "tag": "+".join(component_tags),
                    "reported_value": value,
                    "value": value,
                    "canonical_sign_rule": "preserve_reported_sign",
                    "mapping_priority": mapping["mapping_priority"],
                    "source_expression": mapping["source_expression"],
                    "is_component_sum": True,
                    "is_restricted_cash_included": False,
                    "is_lease_inclusive_debt": mapping["is_lease_inclusive_debt"],
                }
            )
    return pd.DataFrame(rows)


def build_silver_sec_statement_items_frame(
    facts_long_df: pd.DataFrame,
    submissions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if facts_long_df is None or facts_long_df.empty:
        return pd.DataFrame(columns=SEC_STATEMENT_ITEMS_COLUMNS)

    facts = facts_long_df.copy()
    for column in SEC_FACTS_LONG_COLUMNS:
        if column not in facts.columns:
            facts[column] = pd.NA
    facts["asset_id"] = pd.to_numeric(facts["asset_id"], errors="coerce").astype("Int64")
    facts["cik"] = facts["cik"].map(_normalize_cik)
    facts["accession_number"] = facts["accession_number"].astype("string").str.strip()
    facts["value"] = pd.to_numeric(facts["value"], errors="coerce")
    facts["period_start_date"] = _coerce_date(facts["period_start_date"])
    facts["period_end_date"] = _coerce_date(facts["period_end_date"])
    facts["fiscal_year"] = pd.to_numeric(facts["fiscal_year"], errors="coerce").astype("Int64")
    facts["filed_date"] = _coerce_date(facts["filed_date"])
    facts["source_snapshot_date"] = _coerce_date(facts["source_snapshot_date"])
    facts["ingested_ts"] = pd.to_datetime(facts["ingested_ts"], errors="coerce", utc=True)
    facts = facts[
        facts["cik"].notna()
        & facts["accession_number"].notna()
        & facts["accession_number"].ne("")
        & facts["period_end_date"].notna()
        & facts["value"].notna()
        & ~facts["form"].astype("string").str.upper().eq("8-K")
    ].copy()
    if facts.empty:
        return pd.DataFrame(columns=SEC_STATEMENT_ITEMS_COLUMNS)

    direct = _build_direct_statement_candidates(facts)
    debt_components = _build_debt_component_candidates(facts)
    candidates = pd.concat([direct, debt_components], ignore_index=True, sort=False)
    if candidates.empty:
        return pd.DataFrame(columns=SEC_STATEMENT_ITEMS_COLUMNS)
    candidates = _enrich_statement_candidates_with_submissions(candidates, submissions_df)
    return _finalize_statement_items(candidates)


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
        QUALIFY row_number() OVER (
            PARTITION BY accession_number, asset_id
            ORDER BY ingestion_date DESC NULLS LAST, ingested_ts DESC NULLS LAST
        ) = 1
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
        QUALIFY row_number() OVER (
            PARTITION BY
                cik,
                asset_id,
                accession_number,
                taxonomy,
                tag,
                unit,
                period_start_date,
                period_end_date,
                frame
            ORDER BY
                source_snapshot_date DESC NULLS LAST,
                ingestion_date DESC NULLS LAST,
                ingested_ts DESC NULLS LAST
        ) = 1
        """,
        [_parquet_path_params(files)],
    )


def _write_silver_sec_statement_items(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    _configure_sec_duckdb_workload(con)
    if not _table_exists(con, "silver", "sec_facts_long"):
        _write_table(
            con,
            "sec_statement_items",
            pd.DataFrame(columns=SEC_STATEMENT_ITEMS_COLUMNS),
            SEC_STATEMENT_ITEMS_COLUMNS,
        )
        return

    direct_mappings = _mapping_frame()
    try:
        con.unregister("sec_direct_concept_mappings_df")
    except duckdb.CatalogException:
        pass
    con.register("sec_direct_concept_mappings_df", direct_mappings)

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE sec_statement_submissions AS
        SELECT
            accession_number,
            filing_date,
            acceptance_datetime
        FROM silver.sec_submissions
        QUALIFY row_number() OVER (
            PARTITION BY accession_number
            ORDER BY
                acceptance_datetime DESC NULLS LAST,
                filing_date DESC NULLS LAST,
                ingested_ts DESC NULLS LAST
        ) = 1
        """
        if _table_exists(con, "silver", "sec_submissions")
        else """
        CREATE OR REPLACE TEMP TABLE sec_statement_submissions AS
        SELECT
            CAST(NULL AS VARCHAR) AS accession_number,
            CAST(NULL AS DATE) AS filing_date,
            CAST(NULL AS TIMESTAMP) AS acceptance_datetime
        WHERE false
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE sec_statement_candidates (
            asset_id BIGINT,
            cik VARCHAR,
            accession_number VARCHAR,
            canonical_metric VARCHAR,
            statement_type VARCHAR,
            taxonomy VARCHAR,
            tag VARCHAR,
            unit VARCHAR,
            reported_value DOUBLE,
            value DOUBLE,
            canonical_sign_rule VARCHAR,
            period_start_date DATE,
            period_end_date DATE,
            period_type VARCHAR,
            fiscal_year BIGINT,
            fiscal_period VARCHAR,
            form VARCHAR,
            filing_date DATE,
            acceptance_datetime TIMESTAMP,
            availability_date DATE,
            mapping_version VARCHAR,
            mapping_priority BIGINT,
            source_expression VARCHAR,
            source_accession_number VARCHAR,
            source_form VARCHAR,
            source_filed_date DATE,
            source_acceptance_datetime TIMESTAMP,
            period_match_type VARCHAR,
            is_component_sum BOOLEAN,
            is_fallback_concept BOOLEAN,
            is_restricted_cash_included BOOLEAN,
            is_lease_inclusive_debt BOOLEAN,
            is_ytd_derived_quarter BOOLEAN,
            source_snapshot_date DATE,
            ingested_ts TIMESTAMP
        )
        """
    )
    con.execute(
        f"""
        INSERT INTO sec_statement_candidates
        SELECT
            CAST(f.asset_id AS BIGINT) AS asset_id,
            f.cik,
            f.accession_number,
            m.canonical_metric,
            m.statement_type,
            f.taxonomy,
            f.tag,
            f.unit,
            CAST(f.value AS DOUBLE) AS reported_value,
            CASE
                WHEN m.canonical_sign_rule = 'positive_cash_outflow'
                THEN abs(CAST(f.value AS DOUBLE))
                ELSE CAST(f.value AS DOUBLE)
            END AS value,
            m.canonical_sign_rule,
            f.period_start_date,
            f.period_end_date,
            f.period_type,
            CAST(f.fiscal_year AS BIGINT) AS fiscal_year,
            f.fiscal_period,
            f.form,
            coalesce(s.filing_date, f.filed_date) AS filing_date,
            s.acceptance_datetime,
            coalesce(CAST(s.acceptance_datetime AS DATE), s.filing_date, f.filed_date)
                AS availability_date,
            '{SEC_STATEMENT_MAPPING_VERSION}' AS mapping_version,
            CAST(m.mapping_priority AS BIGINT) AS mapping_priority,
            f.tag AS source_expression,
            f.accession_number AS source_accession_number,
            f.form AS source_form,
            f.filed_date AS source_filed_date,
            s.acceptance_datetime AS source_acceptance_datetime,
            CASE
                WHEN f.period_type = 'instant' THEN 'period_end'
                WHEN upper(coalesce(f.fiscal_period, '')) = 'FY' THEN 'exact_annual'
                WHEN upper(coalesce(f.fiscal_period, '')) LIKE 'Q%' THEN 'exact_quarter'
                ELSE 'reported_period'
            END AS period_match_type,
            false AS is_component_sum,
            CAST(m.mapping_priority AS BIGINT) > 10 AS is_fallback_concept,
            f.tag = 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents'
                AS is_restricted_cash_included,
            contains(f.tag, 'CapitalLease') AS is_lease_inclusive_debt,
            false AS is_ytd_derived_quarter,
            f.source_snapshot_date,
            f.ingested_ts
        FROM silver.sec_facts_long AS f
        JOIN sec_direct_concept_mappings_df AS m
          ON f.tag = m.tag
         AND f.unit = m.unit
         AND f.period_type = m.period_type
        LEFT JOIN sec_statement_submissions AS s
          ON f.accession_number = s.accession_number
        WHERE f.cik IS NOT NULL
          AND trim(CAST(f.cik AS VARCHAR)) <> ''
          AND f.accession_number IS NOT NULL
          AND trim(CAST(f.accession_number AS VARCHAR)) <> ''
          AND f.period_end_date IS NOT NULL
          AND f.value IS NOT NULL
          AND f.taxonomy = 'us-gaap'
          AND coalesce(upper(trim(f.form)), '') <> '8-K'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE sec_statement_debt_facts AS
            SELECT *
            FROM silver.sec_facts_long
            WHERE cik IS NOT NULL
              AND trim(CAST(cik AS VARCHAR)) <> ''
              AND accession_number IS NOT NULL
              AND trim(CAST(accession_number AS VARCHAR)) <> ''
              AND period_end_date IS NOT NULL
              AND value IS NOT NULL
              AND taxonomy = 'us-gaap'
              AND coalesce(upper(trim(form)), '') <> '8-K'
              AND unit = 'USD'
              AND period_type = 'instant'
              AND tag IN (
                  'ShortTermBorrowings',
                  'LongTermDebtCurrent',
                  'LongTermDebtNoncurrent',
                  'LongTermDebtAndCapitalLeaseObligationsCurrent',
                  'LongTermDebtAndCapitalLeaseObligations'
              )
        """
    )
    con.execute(
        f"""
        INSERT INTO sec_statement_candidates
        SELECT
            CAST(c.asset_id AS BIGINT) AS asset_id,
            c.cik,
            c.accession_number,
            'debt' AS canonical_metric,
            'balance_sheet' AS statement_type,
            c.taxonomy,
            CASE
                WHEN st.tag IS NULL THEN 'LongTermDebtCurrent+LongTermDebtNoncurrent'
                ELSE 'ShortTermBorrowings+LongTermDebtCurrent+LongTermDebtNoncurrent'
            END AS tag,
            c.unit,
            coalesce(CAST(st.value AS DOUBLE), 0)
                + CAST(c.value AS DOUBLE)
                + CAST(n.value AS DOUBLE) AS reported_value,
            coalesce(CAST(st.value AS DOUBLE), 0)
                + CAST(c.value AS DOUBLE)
                + CAST(n.value AS DOUBLE) AS value,
            'preserve_reported_sign' AS canonical_sign_rule,
            c.period_start_date,
            c.period_end_date,
            c.period_type,
            CAST(c.fiscal_year AS BIGINT) AS fiscal_year,
            c.fiscal_period,
            c.form,
            coalesce(s.filing_date, c.filed_date) AS filing_date,
            s.acceptance_datetime,
            coalesce(CAST(s.acceptance_datetime AS DATE), s.filing_date, c.filed_date)
                AS availability_date,
            '{SEC_STATEMENT_MAPPING_VERSION}' AS mapping_version,
            CASE WHEN st.tag IS NULL THEN 30 ELSE 10 END AS mapping_priority,
            CASE
                WHEN st.tag IS NULL THEN 'LongTermDebtCurrent + LongTermDebtNoncurrent'
                ELSE 'ShortTermBorrowings + LongTermDebtCurrent + LongTermDebtNoncurrent'
            END AS source_expression,
            c.accession_number AS source_accession_number,
            c.form AS source_form,
            c.filed_date AS source_filed_date,
            s.acceptance_datetime AS source_acceptance_datetime,
            'period_end' AS period_match_type,
            true AS is_component_sum,
            st.tag IS NULL AS is_fallback_concept,
            false AS is_restricted_cash_included,
            false AS is_lease_inclusive_debt,
            false AS is_ytd_derived_quarter,
            greatest(c.source_snapshot_date, n.source_snapshot_date, st.source_snapshot_date)
                AS source_snapshot_date,
            greatest(c.ingested_ts, n.ingested_ts, st.ingested_ts) AS ingested_ts
        FROM sec_statement_debt_facts AS c
        JOIN sec_statement_debt_facts AS n
          ON c.asset_id IS NOT DISTINCT FROM n.asset_id
         AND c.cik = n.cik
         AND c.accession_number = n.accession_number
         AND c.period_end_date = n.period_end_date
         AND c.fiscal_year IS NOT DISTINCT FROM n.fiscal_year
         AND c.fiscal_period IS NOT DISTINCT FROM n.fiscal_period
         AND n.tag = 'LongTermDebtNoncurrent'
        LEFT JOIN sec_statement_debt_facts AS st
          ON c.asset_id IS NOT DISTINCT FROM st.asset_id
         AND c.cik = st.cik
         AND c.accession_number = st.accession_number
         AND c.period_end_date = st.period_end_date
         AND c.fiscal_year IS NOT DISTINCT FROM st.fiscal_year
         AND c.fiscal_period IS NOT DISTINCT FROM st.fiscal_period
         AND st.tag = 'ShortTermBorrowings'
        LEFT JOIN sec_statement_submissions AS s
          ON c.accession_number = s.accession_number
        WHERE c.tag = 'LongTermDebtCurrent'
        """
    )
    con.execute(
        f"""
        INSERT INTO sec_statement_candidates
        SELECT
            CAST(c.asset_id AS BIGINT) AS asset_id,
            c.cik,
            c.accession_number,
            'debt' AS canonical_metric,
            'balance_sheet' AS statement_type,
            c.taxonomy,
            CASE
                WHEN st.tag IS NULL THEN
                    'LongTermDebtAndCapitalLeaseObligationsCurrent+'
                    || 'LongTermDebtAndCapitalLeaseObligations'
                ELSE
                    'ShortTermBorrowings+'
                    || 'LongTermDebtAndCapitalLeaseObligationsCurrent+'
                    || 'LongTermDebtAndCapitalLeaseObligations'
            END AS tag,
            c.unit,
            coalesce(CAST(st.value AS DOUBLE), 0)
                + CAST(c.value AS DOUBLE)
                + CAST(n.value AS DOUBLE) AS reported_value,
            coalesce(CAST(st.value AS DOUBLE), 0)
                + CAST(c.value AS DOUBLE)
                + CAST(n.value AS DOUBLE) AS value,
            'preserve_reported_sign' AS canonical_sign_rule,
            c.period_start_date,
            c.period_end_date,
            c.period_type,
            CAST(c.fiscal_year AS BIGINT) AS fiscal_year,
            c.fiscal_period,
            c.form,
            coalesce(s.filing_date, c.filed_date) AS filing_date,
            s.acceptance_datetime,
            coalesce(CAST(s.acceptance_datetime AS DATE), s.filing_date, c.filed_date)
                AS availability_date,
            '{SEC_STATEMENT_MAPPING_VERSION}' AS mapping_version,
            CASE WHEN st.tag IS NULL THEN 40 ELSE 20 END AS mapping_priority,
            CASE
                WHEN st.tag IS NULL THEN
                    'LongTermDebtAndCapitalLeaseObligationsCurrent + '
                    || 'LongTermDebtAndCapitalLeaseObligations'
                ELSE
                    'ShortTermBorrowings + '
                    || 'LongTermDebtAndCapitalLeaseObligationsCurrent + '
                    || 'LongTermDebtAndCapitalLeaseObligations'
            END AS source_expression,
            c.accession_number AS source_accession_number,
            c.form AS source_form,
            c.filed_date AS source_filed_date,
            s.acceptance_datetime AS source_acceptance_datetime,
            'period_end' AS period_match_type,
            true AS is_component_sum,
            true AS is_fallback_concept,
            false AS is_restricted_cash_included,
            true AS is_lease_inclusive_debt,
            false AS is_ytd_derived_quarter,
            greatest(c.source_snapshot_date, n.source_snapshot_date, st.source_snapshot_date)
                AS source_snapshot_date,
            greatest(c.ingested_ts, n.ingested_ts, st.ingested_ts) AS ingested_ts
        FROM sec_statement_debt_facts AS c
        JOIN sec_statement_debt_facts AS n
          ON c.asset_id IS NOT DISTINCT FROM n.asset_id
         AND c.cik = n.cik
         AND c.accession_number = n.accession_number
         AND c.period_end_date = n.period_end_date
         AND c.fiscal_year IS NOT DISTINCT FROM n.fiscal_year
         AND c.fiscal_period IS NOT DISTINCT FROM n.fiscal_period
         AND n.tag = 'LongTermDebtAndCapitalLeaseObligations'
        LEFT JOIN sec_statement_debt_facts AS st
          ON c.asset_id IS NOT DISTINCT FROM st.asset_id
         AND c.cik = st.cik
         AND c.accession_number = st.accession_number
         AND c.period_end_date = st.period_end_date
         AND c.fiscal_year IS NOT DISTINCT FROM st.fiscal_year
         AND c.fiscal_period IS NOT DISTINCT FROM st.fiscal_period
         AND st.tag = 'ShortTermBorrowings'
        LEFT JOIN sec_statement_submissions AS s
          ON c.accession_number = s.accession_number
        WHERE c.tag = 'LongTermDebtAndCapitalLeaseObligationsCurrent'
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.sec_statement_items AS
        SELECT {", ".join(SEC_STATEMENT_ITEMS_COLUMNS)}
        FROM sec_statement_candidates
        QUALIFY row_number() OVER (
            PARTITION BY
                asset_id,
                cik,
                accession_number,
                canonical_metric,
                period_end_date,
                fiscal_year,
                fiscal_period
            ORDER BY
                mapping_priority ASC,
                is_component_sum ASC,
                source_acceptance_datetime DESC NULLS LAST,
                source_filed_date DESC NULLS LAST,
                source_snapshot_date DESC NULLS LAST,
                ingested_ts DESC NULLS LAST
        ) = 1
        """
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
    _log_sec_dq_checks(context)
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
    _log_sec_dq_checks(context)
    return _silver_table_metrics(con, "sec_facts_long", cik_asset_id_map_df)


def materialize_silver_sec_statement_items(context: AssetExecutionContext) -> dict[str, int]:
    con = context.resources.research_duckdb
    _write_silver_sec_statement_items(con)
    _log_sec_dq_checks(context)
    row_count = con.execute("SELECT count(*) FROM silver.sec_statement_items").fetchone()[0]
    mapped_asset_id_rows = con.execute(
        "SELECT count(*) FROM silver.sec_statement_items WHERE asset_id IS NOT NULL"
    ).fetchone()[0]
    canonical_metric_count = con.execute(
        "SELECT count(DISTINCT canonical_metric) FROM silver.sec_statement_items"
    ).fetchone()[0]
    return {
        "row_count": int(row_count or 0),
        "mapped_asset_id_rows": int(mapped_asset_id_rows or 0),
        "canonical_metric_count": int(canonical_metric_count or 0),
    }


@asset(
    name="sec_submissions",
    key_prefix=["silver"],
    deps=[AssetKey("bronze_sec_submissions"), AssetKey(["silver", "security_identifiers"])],
    required_resource_keys={"duckdb", "research_duckdb"},
)
def silver_sec_submissions(context: AssetExecutionContext) -> None:
    metrics = materialize_silver_sec_submissions(context)
    context.add_output_metadata({"table": "silver.sec_submissions", **metrics})


@asset(
    name="sec_facts_long",
    key_prefix=["silver"],
    deps=[AssetKey("bronze_sec_company_facts"), AssetKey(["silver", "security_identifiers"])],
    required_resource_keys={"duckdb", "research_duckdb"},
)
def silver_sec_facts_long(context: AssetExecutionContext) -> None:
    metrics = materialize_silver_sec_facts_long(context)
    context.add_output_metadata({"table": "silver.sec_facts_long", **metrics})


@asset(
    name="sec_statement_items",
    key_prefix=["silver"],
    deps=[AssetKey(["silver", "sec_facts_long"]), AssetKey(["silver", "sec_submissions"])],
    required_resource_keys={"duckdb", "research_duckdb"},
)
def silver_sec_statement_items(context: AssetExecutionContext) -> None:
    metrics = materialize_silver_sec_statement_items(context)
    context.add_output_metadata({"table": "silver.sec_statement_items", **metrics})
