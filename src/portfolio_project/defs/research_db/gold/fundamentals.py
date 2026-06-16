import os
from pathlib import Path

from dagster import AssetExecutionContext, asset

from portfolio_project.defs.research_db.silver.research_prices import (
    RESEARCH_DAILY_PRICES_DATASET,
    silver_research_daily_prices,
)
from portfolio_project.defs.research_db.silver.strategy import (
    _ensure_table_contract,
    _table_metadata,
)
from portfolio_project.defs.research_db.trading_calendar import create_valid_trading_dates_table

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
PRICE_GLOB = (
    DATA_ROOT / "silver" / RESEARCH_DAILY_PRICES_DATASET / "month=*" / "date=*.parquet"
).as_posix()
FUNDAMENTAL_SIGNAL_VERSION = os.getenv("FUNDAMENTAL_SIGNALS_VERSION", "v1")
FUNDAMENTAL_STALE_AFTER_DAYS = int(os.getenv("FUNDAMENTAL_STALE_AFTER_DAYS", "540"))

FUNDAMENTALS_QUARTERLY_COLUMNS: list[tuple[str, str]] = [
    ("asset_id", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("cik", "VARCHAR"),
    ("fiscal_year", "INTEGER"),
    ("fiscal_quarter", "VARCHAR"),
    ("period_start_date", "DATE"),
    ("period_end_date", "DATE"),
    ("filing_date", "DATE"),
    ("acceptance_datetime", "TIMESTAMP"),
    ("availability_date", "DATE"),
    ("accession_number", "VARCHAR"),
    ("form", "VARCHAR"),
    ("revenue", "DOUBLE"),
    ("net_income", "DOUBLE"),
    ("assets", "DOUBLE"),
    ("liabilities", "DOUBLE"),
    ("equity", "DOUBLE"),
    ("debt", "DOUBLE"),
    ("cash", "DOUBLE"),
    ("operating_cash_flow", "DOUBLE"),
    ("capex", "DOUBLE"),
    ("diluted_shares", "DOUBLE"),
    ("diluted_eps", "DOUBLE"),
    ("source_snapshot_date", "DATE"),
    ("statement_items_count", "INTEGER"),
    ("load_timestamp", "TIMESTAMP"),
]

FUNDAMENTAL_SIGNALS_DAILY_COLUMNS: list[tuple[str, str]] = [
    ("date", "DATE"),
    ("asset_id", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("cik", "VARCHAR"),
    ("fiscal_year", "INTEGER"),
    ("fiscal_quarter", "VARCHAR"),
    ("period_end_date", "DATE"),
    ("filing_date", "DATE"),
    ("acceptance_datetime", "TIMESTAMP"),
    ("availability_date", "DATE"),
    ("days_since_filing", "INTEGER"),
    ("has_fundamentals", "BOOLEAN"),
    ("is_stale_fundamentals", "BOOLEAN"),
    ("close", "DOUBLE"),
    ("market_cap", "DOUBLE"),
    ("revenue_ttm", "DOUBLE"),
    ("net_income_ttm", "DOUBLE"),
    ("operating_cash_flow_ttm", "DOUBLE"),
    ("capex_ttm", "DOUBLE"),
    ("free_cash_flow_ttm", "DOUBLE"),
    ("diluted_eps_ttm", "DOUBLE"),
    ("revenue_growth_yoy", "DOUBLE"),
    ("net_income_growth_yoy", "DOUBLE"),
    ("eps_growth_yoy", "DOUBLE"),
    ("net_margin_ttm", "DOUBLE"),
    ("return_on_equity", "DOUBLE"),
    ("debt_to_equity", "DOUBLE"),
    ("cash_to_assets", "DOUBLE"),
    ("price_to_sales", "DOUBLE"),
    ("price_to_earnings", "DOUBLE"),
    ("price_to_book", "DOUBLE"),
    ("free_cash_flow_yield", "DOUBLE"),
    ("signal_version", "VARCHAR"),
    ("load_timestamp", "TIMESTAMP"),
]


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


def _prices_files_exist() -> bool:
    prices_root = DATA_ROOT / "silver" / RESEARCH_DAILY_PRICES_DATASET
    return prices_root.exists() and any(prices_root.glob("month=*/date=*.parquet"))


def _empty_table(con, *, table: str, columns: list[tuple[str, str]]) -> None:
    _ensure_table_contract(con, schema="gold", table=table, columns=columns)
    con.execute(f"DELETE FROM gold.{table}")


def materialize_fundamentals_quarterly(con) -> int:
    if not _table_exists(con, "silver", "sec_statement_items"):
        raise ValueError(
            "silver.sec_statement_items is required to build gold.fundamentals_quarterly."
        )

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.asset_identity_bridge (
            asset_id BIGINT,
            current_symbol VARCHAR,
            source_symbols VARCHAR
        )
        """
    )
    _empty_table(con, table="fundamentals_quarterly", columns=FUNDAMENTALS_QUARTERLY_COLUMNS)
    con.execute(
        """
        INSERT INTO gold.fundamentals_quarterly (
            asset_id,
            symbol,
            cik,
            fiscal_year,
            fiscal_quarter,
            period_start_date,
            period_end_date,
            filing_date,
            acceptance_datetime,
            availability_date,
            accession_number,
            form,
            revenue,
            net_income,
            assets,
            liabilities,
            equity,
            debt,
            cash,
            operating_cash_flow,
            capex,
            diluted_shares,
            diluted_eps,
            source_snapshot_date,
            statement_items_count,
            load_timestamp
        )
        WITH statement_items AS (
            SELECT
                CAST(i.asset_id AS BIGINT) AS asset_id,
                COALESCE(b.current_symbol, b.source_symbols) AS symbol,
                CAST(i.cik AS VARCHAR) AS cik,
                CAST(i.fiscal_year AS INTEGER) AS fiscal_year,
                CASE
                    WHEN upper(trim(CAST(i.fiscal_period AS VARCHAR))) = 'FY' THEN 'Q4'
                    ELSE upper(trim(CAST(i.fiscal_period AS VARCHAR)))
                END AS fiscal_quarter,
                CAST(i.period_start_date AS DATE) AS period_start_date,
                CAST(i.period_end_date AS DATE) AS period_end_date,
                CAST(i.filing_date AS DATE) AS filing_date,
                CAST(i.acceptance_datetime AS TIMESTAMP) AS acceptance_datetime,
                COALESCE(
                    CAST(i.availability_date AS DATE),
                    CAST(i.acceptance_datetime AS DATE),
                    CAST(i.filing_date AS DATE)
                ) AS availability_date,
                CAST(i.accession_number AS VARCHAR) AS accession_number,
                CAST(i.form AS VARCHAR) AS form,
                lower(trim(CAST(i.canonical_metric AS VARCHAR))) AS canonical_metric,
                CAST(i.value AS DOUBLE) AS value,
                CAST(i.source_snapshot_date AS DATE) AS source_snapshot_date,
                CAST(i.ingested_ts AS TIMESTAMP) AS ingested_ts
            FROM silver.sec_statement_items AS i
            LEFT JOIN silver.asset_identity_bridge AS b
                ON CAST(b.asset_id AS BIGINT) = CAST(i.asset_id AS BIGINT)
            WHERE i.cik IS NOT NULL
              AND i.period_end_date IS NOT NULL
              AND i.fiscal_year IS NOT NULL
              AND i.fiscal_period IS NOT NULL
              AND i.canonical_metric IS NOT NULL
              AND lower(trim(CAST(i.canonical_metric AS VARCHAR))) IN (
                    'revenue',
                    'net_income',
                    'assets',
                    'liabilities',
                    'equity',
                    'debt',
                    'cash',
                    'operating_cash_flow',
                    'capex',
                    'diluted_shares',
                    'diluted_eps'
              )
        ),
        filing_pivots AS (
            SELECT
                asset_id,
                any_value(symbol) AS symbol,
                cik,
                fiscal_year,
                fiscal_quarter,
                min(period_start_date) AS period_start_date,
                period_end_date,
                filing_date,
                acceptance_datetime,
                availability_date,
                accession_number,
                form,
                max(CASE WHEN canonical_metric = 'revenue' THEN value END) AS revenue,
                max(CASE WHEN canonical_metric = 'net_income' THEN value END) AS net_income,
                max(CASE WHEN canonical_metric = 'assets' THEN value END) AS assets,
                max(CASE WHEN canonical_metric = 'liabilities' THEN value END) AS liabilities,
                max(CASE WHEN canonical_metric = 'equity' THEN value END) AS equity,
                max(CASE WHEN canonical_metric = 'debt' THEN value END) AS debt,
                max(CASE WHEN canonical_metric = 'cash' THEN value END) AS cash,
                max(CASE WHEN canonical_metric = 'operating_cash_flow' THEN value END)
                    AS operating_cash_flow,
                max(CASE WHEN canonical_metric = 'capex' THEN value END) AS capex,
                max(CASE WHEN canonical_metric = 'diluted_shares' THEN value END)
                    AS diluted_shares,
                max(CASE WHEN canonical_metric = 'diluted_eps' THEN value END) AS diluted_eps,
                max(source_snapshot_date) AS source_snapshot_date,
                count(DISTINCT canonical_metric)::INTEGER AS statement_items_count,
                max(ingested_ts) AS max_ingested_ts
            FROM statement_items
            WHERE fiscal_quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
            GROUP BY
                asset_id,
                cik,
                fiscal_year,
                fiscal_quarter,
                period_end_date,
                filing_date,
                acceptance_datetime,
                availability_date,
                accession_number,
                form
        ),
        selected AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY
                        coalesce(CAST(asset_id AS VARCHAR), cik),
                        fiscal_year,
                        fiscal_quarter,
                        period_end_date
                    ORDER BY
                        acceptance_datetime DESC NULLS LAST,
                        filing_date DESC NULLS LAST,
                        source_snapshot_date DESC NULLS LAST,
                        statement_items_count DESC,
                        max_ingested_ts DESC NULLS LAST,
                        accession_number DESC NULLS LAST
                ) AS row_num
            FROM filing_pivots
        )
        SELECT
            asset_id,
            symbol,
            cik,
            fiscal_year,
            fiscal_quarter,
            period_start_date,
            period_end_date,
            filing_date,
            acceptance_datetime,
            availability_date,
            accession_number,
            form,
            revenue,
            net_income,
            assets,
            liabilities,
            equity,
            debt,
            cash,
            operating_cash_flow,
            capex,
            diluted_shares,
            diluted_eps,
            source_snapshot_date,
            statement_items_count,
            current_timestamp AS load_timestamp
        FROM selected
        WHERE row_num = 1
        ORDER BY asset_id, cik, fiscal_year, fiscal_quarter, period_end_date
        """
    )
    return int(con.execute("SELECT count(*) FROM gold.fundamentals_quarterly").fetchone()[0] or 0)


def materialize_fundamental_signals_daily(con) -> int:
    if not _table_exists(con, "gold", "fundamentals_quarterly"):
        raise ValueError(
            "gold.fundamentals_quarterly is required to build gold.fundamental_signals_daily."
        )
    if not _prices_files_exist():
        _empty_table(
            con, table="fundamental_signals_daily", columns=FUNDAMENTAL_SIGNALS_DAILY_COLUMNS
        )
        return 0

    create_valid_trading_dates_table(con, PRICE_GLOB)
    _empty_table(con, table="fundamental_signals_daily", columns=FUNDAMENTAL_SIGNALS_DAILY_COLUMNS)
    con.execute(
        """
        INSERT INTO gold.fundamental_signals_daily (
            date,
            asset_id,
            symbol,
            cik,
            fiscal_year,
            fiscal_quarter,
            period_end_date,
            filing_date,
            acceptance_datetime,
            availability_date,
            days_since_filing,
            has_fundamentals,
            is_stale_fundamentals,
            close,
            market_cap,
            revenue_ttm,
            net_income_ttm,
            operating_cash_flow_ttm,
            capex_ttm,
            free_cash_flow_ttm,
            diluted_eps_ttm,
            revenue_growth_yoy,
            net_income_growth_yoy,
            eps_growth_yoy,
            net_margin_ttm,
            return_on_equity,
            debt_to_equity,
            cash_to_assets,
            price_to_sales,
            price_to_earnings,
            price_to_book,
            free_cash_flow_yield,
            signal_version,
            load_timestamp
        )
        WITH fundamental_assets AS (
            SELECT
                asset_id,
                min(availability_date) AS first_availability_date
            FROM gold.fundamentals_quarterly
            WHERE asset_id IS NOT NULL
              AND availability_date IS NOT NULL
            GROUP BY asset_id
        ),
        prices AS (
            SELECT
                CAST(p.trade_date AS DATE) AS date,
                CAST(p.asset_id AS BIGINT) AS asset_id,
                upper(trim(CAST(p.symbol AS VARCHAR))) AS symbol,
                CAST(p.close AS DOUBLE) AS close
            FROM read_parquet(?, union_by_name = true) AS p
            INNER JOIN valid_research_trading_dates AS d
                ON d.trade_date = CAST(p.trade_date AS DATE)
            INNER JOIN fundamental_assets AS f
                ON f.asset_id = CAST(p.asset_id AS BIGINT)
               AND CAST(p.trade_date AS DATE) >= f.first_availability_date
            WHERE p.asset_id IS NOT NULL
              AND p.trade_date IS NOT NULL
              AND p.symbol IS NOT NULL
              AND trim(CAST(p.symbol AS VARCHAR)) <> ''
        ),
        fundamentals AS (
            SELECT
                *,
                CASE fiscal_quarter
                    WHEN 'Q1' THEN 1
                    WHEN 'Q2' THEN 2
                    WHEN 'Q3' THEN 3
                    WHEN 'Q4' THEN 4
                    ELSE NULL
                END AS fiscal_quarter_number
            FROM gold.fundamentals_quarterly
            WHERE asset_id IS NOT NULL
              AND availability_date IS NOT NULL
        ),
        derived_base AS (
            SELECT
                *,
                sum(revenue) OVER w AS revenue_ttm,
                sum(net_income) OVER w AS net_income_ttm,
                sum(operating_cash_flow) OVER w AS operating_cash_flow_ttm,
                sum(capex) OVER w AS capex_ttm,
                sum(diluted_eps) OVER w AS diluted_eps_ttm,
                lag(revenue, 4) OVER q AS revenue_prior_year_quarter,
                lag(net_income, 4) OVER q AS net_income_prior_year_quarter,
                lag(diluted_eps, 4) OVER q AS diluted_eps_prior_year_quarter
            FROM fundamentals
            WINDOW
                q AS (
                    PARTITION BY asset_id
                    ORDER BY fiscal_year, fiscal_quarter_number, period_end_date
                ),
                w AS (
                    PARTITION BY asset_id
                    ORDER BY fiscal_year, fiscal_quarter_number, period_end_date
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                )
        ),
        derived_effective AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY asset_id, availability_date
                    ORDER BY
                        period_end_date DESC NULLS LAST,
                        acceptance_datetime DESC NULLS LAST,
                        filing_date DESC NULLS LAST,
                        source_snapshot_date DESC NULLS LAST
                ) AS effective_row_num
            FROM derived_base
        ),
        point_in_time AS (
            SELECT
                p.date,
                p.asset_id,
                p.symbol,
                d.cik,
                d.fiscal_year,
                d.fiscal_quarter,
                d.period_end_date,
                d.filing_date,
                d.acceptance_datetime,
                d.availability_date,
                date_diff('day', d.availability_date, p.date)::INTEGER AS days_since_filing,
                d.asset_id IS NOT NULL AS has_fundamentals,
                CASE
                    WHEN d.asset_id IS NULL THEN FALSE
                    ELSE date_diff('day', d.availability_date, p.date) > ?
                END AS is_stale_fundamentals,
                p.close,
                CASE
                    WHEN p.close IS NULL OR d.diluted_shares IS NULL OR d.diluted_shares <= 0
                    THEN NULL
                    ELSE p.close * d.diluted_shares
                END AS market_cap,
                d.revenue_ttm,
                d.net_income_ttm,
                d.operating_cash_flow_ttm,
                d.capex_ttm,
                CASE
                    WHEN d.operating_cash_flow_ttm IS NULL OR d.capex_ttm IS NULL THEN NULL
                    ELSE d.operating_cash_flow_ttm - d.capex_ttm
                END AS free_cash_flow_ttm,
                d.diluted_eps_ttm,
                CASE
                    WHEN d.revenue IS NULL
                         OR d.revenue_prior_year_quarter IS NULL
                         OR d.revenue_prior_year_quarter = 0 THEN NULL
                    ELSE (d.revenue / d.revenue_prior_year_quarter) - 1
                END AS revenue_growth_yoy,
                CASE
                    WHEN d.net_income IS NULL
                         OR d.net_income_prior_year_quarter IS NULL
                         OR d.net_income_prior_year_quarter = 0 THEN NULL
                    ELSE (d.net_income / d.net_income_prior_year_quarter) - 1
                END AS net_income_growth_yoy,
                CASE
                    WHEN d.diluted_eps IS NULL
                         OR d.diluted_eps_prior_year_quarter IS NULL
                         OR d.diluted_eps_prior_year_quarter = 0 THEN NULL
                    ELSE (d.diluted_eps / d.diluted_eps_prior_year_quarter) - 1
                END AS eps_growth_yoy,
                CASE
                    WHEN d.net_income_ttm IS NULL OR d.revenue_ttm IS NULL OR d.revenue_ttm = 0
                    THEN NULL
                    ELSE d.net_income_ttm / d.revenue_ttm
                END AS net_margin_ttm,
                CASE
                    WHEN d.net_income_ttm IS NULL OR d.equity IS NULL OR d.equity = 0 THEN NULL
                    ELSE d.net_income_ttm / d.equity
                END AS return_on_equity,
                CASE
                    WHEN d.debt IS NULL OR d.equity IS NULL OR d.equity = 0 THEN NULL
                    ELSE d.debt / d.equity
                END AS debt_to_equity,
                CASE
                    WHEN d.cash IS NULL OR d.assets IS NULL OR d.assets = 0 THEN NULL
                    ELSE d.cash / d.assets
                END AS cash_to_assets,
                d.equity
            FROM prices AS p
            ASOF LEFT JOIN (
                SELECT *
                FROM derived_effective
                WHERE effective_row_num = 1
            ) AS d
                ON d.asset_id = p.asset_id
               AND p.date >= d.availability_date
        )
        SELECT
            date,
            asset_id,
            symbol,
            cik,
            fiscal_year,
            fiscal_quarter,
            period_end_date,
            filing_date,
            acceptance_datetime,
            availability_date,
            days_since_filing,
            has_fundamentals,
            is_stale_fundamentals,
            close,
            market_cap,
            revenue_ttm,
            net_income_ttm,
            operating_cash_flow_ttm,
            capex_ttm,
            free_cash_flow_ttm,
            diluted_eps_ttm,
            revenue_growth_yoy,
            net_income_growth_yoy,
            eps_growth_yoy,
            net_margin_ttm,
            return_on_equity,
            debt_to_equity,
            cash_to_assets,
            CASE
                WHEN market_cap IS NULL OR revenue_ttm IS NULL OR revenue_ttm = 0 THEN NULL
                ELSE market_cap / revenue_ttm
            END AS price_to_sales,
            CASE
                WHEN market_cap IS NULL OR net_income_ttm IS NULL OR net_income_ttm = 0 THEN NULL
                ELSE market_cap / net_income_ttm
            END AS price_to_earnings,
            CASE
                WHEN market_cap IS NULL OR equity IS NULL OR equity = 0 THEN NULL
                ELSE market_cap / equity
            END AS price_to_book,
            CASE
                WHEN market_cap IS NULL
                     OR market_cap = 0
                     OR operating_cash_flow_ttm IS NULL
                     OR capex_ttm IS NULL THEN NULL
                ELSE (operating_cash_flow_ttm - capex_ttm) / market_cap
            END AS free_cash_flow_yield,
            ? AS signal_version,
            current_timestamp AS load_timestamp
        FROM point_in_time
        ORDER BY date, asset_id
        """,
        [PRICE_GLOB, FUNDAMENTAL_STALE_AFTER_DAYS, FUNDAMENTAL_SIGNAL_VERSION],
    )
    return int(
        con.execute("SELECT count(*) FROM gold.fundamental_signals_daily").fetchone()[0] or 0
    )


@asset(
    name="fundamentals_quarterly",
    key_prefix=["gold"],
    required_resource_keys={"research_duckdb"},
)
def gold_fundamentals_quarterly(context: AssetExecutionContext) -> None:
    """
    Pivot curated SEC statement items into one gold row per asset and fiscal quarter.
    """
    con = context.resources.research_duckdb
    row_count = materialize_fundamentals_quarterly(con)
    context.add_output_metadata(
        {
            "table": "gold.fundamentals_quarterly",
            **_table_metadata(con, schema="gold", table="fundamentals_quarterly"),
            "row_count": row_count,
        }
    )


@asset(
    name="fundamental_signals_daily",
    key_prefix=["gold"],
    deps=[gold_fundamentals_quarterly, silver_research_daily_prices],
    required_resource_keys={"research_duckdb"},
)
def gold_fundamental_signals_daily(context: AssetExecutionContext) -> None:
    """
    Build point-in-time daily fundamental features from quarterly filings and prices.
    """
    con = context.resources.research_duckdb
    row_count = materialize_fundamental_signals_daily(con)
    context.add_output_metadata(
        {
            "table": "gold.fundamental_signals_daily",
            **_table_metadata(con, schema="gold", table="fundamental_signals_daily"),
            "row_count": row_count,
            "signal_version": FUNDAMENTAL_SIGNAL_VERSION,
        }
    )
