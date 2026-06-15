import duckdb
import pandas as pd

import portfolio_project.defs.research_db.silver.sec_cik_review as review_module
from portfolio_project.defs.research_db.silver.sec_cik_review import (
    UNMAPPED_SEC_CIK_REVIEW_COLUMNS,
    build_unmapped_sec_cik_review_frame,
)


def test_build_unmapped_sec_cik_review_ranks_exact_research_symbol_candidates(
    tmp_path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(review_module, "DATA_ROOT", data_root)
    tickers_path = (
        data_root
        / "bronze"
        / "sec_company_tickers"
        / "ingestion_date=2026-01-02"
        / "tickers.parquet"
    )
    submissions_path = (
        data_root
        / "bronze"
        / "sec_submissions"
        / "ingestion_date=2026-01-02"
        / "submissions.parquet"
    )
    tickers_path.parent.mkdir(parents=True)
    submissions_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "cik": ["0000004447"],
            "name": ["HESS CORP"],
            "ticker": ["HES"],
            "exchange": ["NYSE"],
            "ingestion_date": [pd.Timestamp("2026-01-02").date()],
        }
    ).to_parquet(tickers_path, index=False)
    pd.DataFrame(
        {
            "cik": ["0000004447"],
            "entity_name": ["HESS CORP"],
            "tickers": ["HES"],
            "exchanges": ["NYSE"],
        }
    ).to_parquet(submissions_path, index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.security_identifiers (
            asset_id BIGINT,
            source_symbol VARCHAR,
            security_name VARCHAR,
            identifier_type VARCHAR,
            identifier_value VARCHAR,
            cik VARCHAR,
            identifier_source VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.security_identifiers VALUES
            (43037, 'HES', NULL, 'symbol', 'HES', NULL, 'research_daily_prices'),
            (1, 'AAPL', NULL, 'cik', '320193', '320193', 'sec_company_tickers')
        """
    )
    con.execute(
        """
        CREATE TABLE silver.asset_identity_bridge (
            asset_id BIGINT,
            current_symbol VARCHAR,
            source_symbols VARCHAR,
            alpaca_id VARCHAR,
            cik VARCHAR,
            security_name VARCHAR,
            exchange VARCHAR,
            is_current BOOLEAN,
            asof_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.asset_identity_bridge VALUES
            (43037, 'HES', 'HES', NULL, NULL, 'Hess Corp', NULL, TRUE, now())
        """
    )
    con.execute(
        """
        CREATE TABLE silver.sec_facts_long (
            asset_id BIGINT,
            cik VARCHAR,
            filed_date DATE,
            period_end_date DATE
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.sec_facts_long VALUES
            (NULL, '4447', DATE '2025-05-08', DATE '2025-03-31'),
            (NULL, '4447', DATE '2024-05-08', DATE '2024-03-31'),
            (1, '320193', DATE '2025-01-31', DATE '2024-12-31')
        """
    )
    con.execute(
        """
        CREATE TABLE silver.sec_submissions (
            asset_id BIGINT,
            cik VARCHAR,
            filing_date DATE,
            report_date DATE
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.sec_submissions VALUES
            (NULL, '4447', DATE '2025-05-08', DATE '2025-03-31')
        """
    )

    frame = build_unmapped_sec_cik_review_frame(con)

    assert list(frame.columns) == UNMAPPED_SEC_CIK_REVIEW_COLUMNS
    assert frame[["cik", "sec_registrant_name", "candidate_asset_id"]].to_dict("records") == [
        {
            "cik": "4447",
            "sec_registrant_name": "HESS CORP",
            "candidate_asset_id": "43037",
        }
    ]
    assert frame.loc[0, "reason_unmapped"] == "requires_manual_review"
