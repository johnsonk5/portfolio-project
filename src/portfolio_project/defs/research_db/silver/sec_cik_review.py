import os
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, AssetKey, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))

UNMAPPED_SEC_CIK_REVIEW_COLUMNS = [
    "impact_rank",
    "cik",
    "sec_registrant_name",
    "sec_fact_count",
    "sec_submission_count",
    "earliest_filing_date",
    "latest_filing_date",
    "earliest_report_date",
    "latest_report_date",
    "sec_tickers",
    "sec_ticker_exchanges",
    "exact_ticker_project_symbols",
    "exact_ticker_asset_ids",
    "exact_ticker_research_universe",
    "normalized_name_project_symbols",
    "normalized_name_asset_ids",
    "normalized_name_research_universe",
    "candidate_asset_id",
    "candidate_source_symbol",
    "reason_unmapped",
]


def _reports_root(data_root: Path | None = None) -> Path:
    root = data_root or DATA_ROOT
    return root / "reports"


def _sec_company_tickers_glob(data_root: Path | None = None) -> str:
    root = data_root or DATA_ROOT
    return (
        root / "bronze" / "sec_company_tickers" / "ingestion_date=*" / "tickers.parquet"
    ).as_posix()


def _sec_submissions_glob(data_root: Path | None = None) -> str:
    root = data_root or DATA_ROOT
    return (
        root / "bronze" / "sec_submissions" / "ingestion_date=*" / "submissions.parquet"
    ).as_posix()


def _sec_company_tickers_files_exist(data_root: Path | None = None) -> bool:
    root = data_root or DATA_ROOT
    return any((root / "bronze" / "sec_company_tickers").glob("ingestion_date=*/tickers.parquet"))


def _sec_submissions_files_exist(data_root: Path | None = None) -> bool:
    root = data_root or DATA_ROOT
    return any((root / "bronze" / "sec_submissions").glob("ingestion_date=*/submissions.parquet"))


def _empty_report_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=UNMAPPED_SEC_CIK_REVIEW_COLUMNS)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read_parquet_expr(path: str) -> str:
    return f"read_parquet({_sql_string(path)}, union_by_name = true)"


def build_unmapped_sec_cik_review_frame(con, *, data_root: Path | None = None) -> pd.DataFrame:
    con.execute("CREATE SCHEMA IF NOT EXISTS observability")
    if not _sec_company_tickers_files_exist(data_root):
        return _empty_report_frame()

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW _sec_current_tickers AS
        SELECT
            coalesce(nullif(regexp_replace(trim(CAST(cik AS VARCHAR)), '^0+', ''), ''), '0')
                AS cik,
            upper(trim(CAST(ticker AS VARCHAR))) AS ticker,
            CAST(name AS VARCHAR) AS sec_ticker_name,
            upper(trim(CAST(exchange AS VARCHAR))) AS exchange,
            try_cast(ingestion_date AS DATE) AS ingestion_date
        FROM read_parquet(?, union_by_name = true)
        WHERE cik IS NOT NULL
          AND ticker IS NOT NULL
          AND trim(CAST(ticker AS VARCHAR)) <> ''
        QUALIFY row_number() OVER (
            PARTITION BY
                coalesce(nullif(regexp_replace(trim(CAST(cik AS VARCHAR)), '^0+', ''), ''), '0'),
                upper(trim(CAST(ticker AS VARCHAR)))
            ORDER BY try_cast(ingestion_date AS DATE) DESC NULLS LAST
        ) = 1
        """.replace(
            "read_parquet(?, union_by_name = true)",
            _read_parquet_expr(_sec_company_tickers_glob(data_root)),
        )
    )
    if _sec_submissions_files_exist(data_root):
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW _sec_bronze_submission_names AS
            SELECT
                coalesce(nullif(regexp_replace(trim(CAST(cik AS VARCHAR)), '^0+', ''), ''), '0')
                    AS cik,
                any_value(CAST(entity_name AS VARCHAR)) AS entity_name,
                any_value(CAST(tickers AS VARCHAR)) AS submission_tickers,
                any_value(CAST(exchanges AS VARCHAR)) AS submission_exchanges
            FROM read_parquet(?, union_by_name = true)
            WHERE cik IS NOT NULL
            GROUP BY
                coalesce(nullif(regexp_replace(trim(CAST(cik AS VARCHAR)), '^0+', ''), ''), '0')
            """.replace(
                "read_parquet(?, union_by_name = true)",
                _read_parquet_expr(_sec_submissions_glob(data_root)),
            )
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW _sec_bronze_submission_names AS
            SELECT
                NULL::VARCHAR AS cik,
                NULL::VARCHAR AS entity_name,
                NULL::VARCHAR AS submission_tickers,
                NULL::VARCHAR AS submission_exchanges
            WHERE false
            """
        )

    report = con.execute(
        """
        WITH mapped_ciks AS (
            SELECT DISTINCT
                coalesce(
                    nullif(regexp_replace(trim(coalesce(cik, identifier_value)), '^0+', ''), ''),
                    '0'
                ) AS cik
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
        ),
        unmapped_facts AS (
            SELECT
                cik,
                count(*) AS sec_fact_count,
                min(filed_date) AS earliest_fact_filing_date,
                max(filed_date) AS latest_fact_filing_date,
                min(period_end_date) AS earliest_report_date,
                max(period_end_date) AS latest_report_date
            FROM silver.sec_facts_long
            WHERE asset_id IS NULL
              AND cik IS NOT NULL
            GROUP BY cik
        ),
        submissions AS (
            SELECT
                cik,
                count(*) AS sec_submission_count,
                min(filing_date) AS earliest_submission_filing_date,
                max(filing_date) AS latest_submission_filing_date,
                min(report_date) AS earliest_submission_report_date,
                max(report_date) AS latest_submission_report_date
            FROM silver.sec_submissions
            WHERE cik IS NOT NULL
            GROUP BY cik
        ),
        ticker_names AS (
            SELECT
                cik,
                any_value(sec_ticker_name) AS sec_ticker_name
            FROM _sec_current_tickers
            GROUP BY cik
        ),
        unmapped AS (
            SELECT
                f.cik,
                coalesce(n.entity_name, t.sec_ticker_name, f.cik) AS sec_registrant_name,
                f.sec_fact_count,
                coalesce(s.sec_submission_count, 0) AS sec_submission_count,
                least(
                    f.earliest_fact_filing_date,
                    s.earliest_submission_filing_date
                ) AS earliest_filing_date,
                greatest(
                    f.latest_fact_filing_date,
                    s.latest_submission_filing_date
                ) AS latest_filing_date,
                least(f.earliest_report_date, s.earliest_submission_report_date)
                    AS earliest_report_date,
                greatest(f.latest_report_date, s.latest_submission_report_date)
                    AS latest_report_date
            FROM unmapped_facts f
            LEFT JOIN mapped_ciks m ON f.cik = m.cik
            LEFT JOIN submissions s ON f.cik = s.cik
            LEFT JOIN _sec_bronze_submission_names n ON f.cik = n.cik
            LEFT JOIN ticker_names t ON f.cik = t.cik
            WHERE m.cik IS NULL
        ),
        ticker_rollup AS (
            SELECT
                cik,
                string_agg(DISTINCT ticker, ',' ORDER BY ticker) AS sec_tickers,
                string_agg(DISTINCT exchange, ',' ORDER BY exchange) AS sec_ticker_exchanges
            FROM _sec_current_tickers
            GROUP BY cik
        ),
        project_symbols AS (
            SELECT
                si.asset_id,
                upper(trim(si.source_symbol)) AS source_symbol,
                aib.security_name,
                regexp_replace(lower(coalesce(aib.security_name, '')), '[^a-z0-9]+', '', 'g')
                    AS normalized_security_name,
                bool_or(si.identifier_source = 'research_daily_prices') AS in_research_universe
            FROM silver.security_identifiers si
            LEFT JOIN silver.asset_identity_bridge aib ON si.asset_id = aib.asset_id
            WHERE si.identifier_type = 'symbol'
              AND si.asset_id IS NOT NULL
              AND si.source_symbol IS NOT NULL
            GROUP BY si.asset_id, upper(trim(si.source_symbol)), aib.security_name
        ),
        exact_ticker_matches AS (
            SELECT
                t.cik,
                string_agg(DISTINCT p.source_symbol, ',' ORDER BY p.source_symbol)
                    AS exact_ticker_project_symbols,
                string_agg(
                    DISTINCT CAST(p.asset_id AS VARCHAR),
                    ',' ORDER BY CAST(p.asset_id AS VARCHAR)
                )
                    AS exact_ticker_asset_ids,
                bool_or(p.in_research_universe) AS exact_ticker_research_universe,
                count(DISTINCT p.asset_id) AS exact_ticker_asset_count
            FROM _sec_current_tickers t
            JOIN project_symbols p ON t.ticker = p.source_symbol
            GROUP BY t.cik
        ),
        normalized_name_matches AS (
            SELECT
                u.cik,
                string_agg(DISTINCT p.source_symbol, ',' ORDER BY p.source_symbol)
                    AS normalized_name_project_symbols,
                string_agg(
                    DISTINCT CAST(p.asset_id AS VARCHAR),
                    ',' ORDER BY CAST(p.asset_id AS VARCHAR)
                )
                    AS normalized_name_asset_ids,
                bool_or(p.in_research_universe) AS normalized_name_research_universe,
                count(DISTINCT p.asset_id) AS normalized_name_asset_count
            FROM unmapped u
            JOIN project_symbols p
              ON regexp_replace(lower(coalesce(u.sec_registrant_name, '')), '[^a-z0-9]+', '', 'g')
                 = p.normalized_security_name
             AND p.normalized_security_name <> ''
            GROUP BY u.cik
        ),
        candidates AS (
            SELECT
                u.*,
                tr.sec_tickers,
                tr.sec_ticker_exchanges,
                etm.exact_ticker_project_symbols,
                etm.exact_ticker_asset_ids,
                coalesce(etm.exact_ticker_research_universe, false)
                    AS exact_ticker_research_universe,
                nmm.normalized_name_project_symbols,
                nmm.normalized_name_asset_ids,
                coalesce(nmm.normalized_name_research_universe, false)
                    AS normalized_name_research_universe,
                CASE
                    WHEN etm.exact_ticker_asset_count = 1
                         AND coalesce(etm.exact_ticker_research_universe, false)
                    THEN etm.exact_ticker_asset_ids
                    WHEN nmm.normalized_name_asset_count = 1
                         AND coalesce(nmm.normalized_name_research_universe, false)
                         AND etm.exact_ticker_asset_count IS NULL
                    THEN nmm.normalized_name_asset_ids
                    ELSE NULL
                END AS candidate_asset_id,
                CASE
                    WHEN etm.exact_ticker_asset_count = 1
                         AND coalesce(etm.exact_ticker_research_universe, false)
                    THEN etm.exact_ticker_project_symbols
                    WHEN nmm.normalized_name_asset_count = 1
                         AND coalesce(nmm.normalized_name_research_universe, false)
                         AND etm.exact_ticker_asset_count IS NULL
                    THEN nmm.normalized_name_project_symbols
                    ELSE NULL
                END AS candidate_source_symbol,
                CASE
                    WHEN etm.exact_ticker_asset_count > 1
                    THEN 'ambiguous_exact_ticker_multiple_assets'
                    WHEN nmm.normalized_name_asset_count > 1
                    THEN 'ambiguous_normalized_name_multiple_assets'
                    WHEN tr.sec_tickers IS NULL
                         AND nmm.normalized_name_asset_count IS NULL
                    THEN 'no_sec_ticker_or_exact_name_project_match'
                    WHEN tr.sec_tickers IS NOT NULL
                         AND etm.exact_ticker_asset_count IS NULL
                    THEN 'sec_ticker_not_in_project_symbols'
                    WHEN etm.exact_ticker_asset_count = 1
                         AND NOT coalesce(etm.exact_ticker_research_universe, false)
                    THEN 'exact_ticker_not_in_research_universe'
                    WHEN nmm.normalized_name_asset_count = 1
                         AND NOT coalesce(nmm.normalized_name_research_universe, false)
                    THEN 'normalized_name_match_not_in_research_universe'
                    WHEN nmm.normalized_name_asset_count = 1
                         AND etm.exact_ticker_asset_count IS NULL
                    THEN 'normalized_name_only_requires_manual_review'
                    ELSE 'requires_manual_review'
                END AS reason_unmapped
            FROM unmapped u
            LEFT JOIN ticker_rollup tr ON u.cik = tr.cik
            LEFT JOIN exact_ticker_matches etm ON u.cik = etm.cik
            LEFT JOIN normalized_name_matches nmm ON u.cik = nmm.cik
        )
        SELECT
            row_number() OVER (
                ORDER BY
                    sec_fact_count DESC,
                    exact_ticker_research_universe DESC,
                    normalized_name_research_universe DESC,
                    latest_filing_date DESC NULLS LAST,
                    CAST(cik AS BIGINT)
            ) AS impact_rank,
            cik,
            sec_registrant_name,
            sec_fact_count,
            sec_submission_count,
            earliest_filing_date,
            latest_filing_date,
            earliest_report_date,
            latest_report_date,
            sec_tickers,
            sec_ticker_exchanges,
            exact_ticker_project_symbols,
            exact_ticker_asset_ids,
            exact_ticker_research_universe,
            normalized_name_project_symbols,
            normalized_name_asset_ids,
            normalized_name_research_universe,
            candidate_asset_id,
            candidate_source_symbol,
            reason_unmapped
        FROM candidates
        ORDER BY impact_rank
        """
    ).fetch_df()
    if report.empty:
        return _empty_report_frame()
    return report[UNMAPPED_SEC_CIK_REVIEW_COLUMNS]


def materialize_unmapped_sec_cik_review(
    context: AssetExecutionContext,
    *,
    data_root: Path | None = None,
) -> dict[str, object]:
    report_df = build_unmapped_sec_cik_review_frame(
        context.resources.research_duckdb,
        data_root=data_root,
    )
    output_path = _reports_root(data_root) / "unmapped_sec_cik_review.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(output_path, index=False)
    return {
        "row_count": len(report_df),
        "output_path": str(output_path),
        "candidate_asset_id_rows": int(report_df["candidate_asset_id"].notna().sum())
        if not report_df.empty
        else 0,
    }


@asset(
    name="unmapped_sec_cik_review",
    key_prefix=["observability"],
    deps=[
        AssetKey(["silver", "security_identifiers"]),
        AssetKey(["silver", "sec_submissions"]),
        AssetKey(["silver", "sec_facts_long"]),
    ],
    required_resource_keys={"research_duckdb"},
)
def unmapped_sec_cik_review(context: AssetExecutionContext) -> None:
    metrics = materialize_unmapped_sec_cik_review(context)
    context.add_output_metadata(metrics)
