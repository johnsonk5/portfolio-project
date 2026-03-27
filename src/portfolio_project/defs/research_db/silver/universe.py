import os
from datetime import date
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError

from portfolio_project.defs.research_db.dq_checks import log_required_field_null_check
from portfolio_project.defs.research_db.silver.research_prices import (
    RESEARCH_DAILY_PRICES_DATASET,
    silver_research_daily_prices,
)

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
LIQUIDITY_LOOKBACK_DAYS = int(os.getenv("RESEARCH_UNIVERSE_LIQUIDITY_LOOKBACK_DAYS", "20"))
UNIVERSE_SIZE = int(os.getenv("RESEARCH_UNIVERSE_SIZE", "500"))
UNIVERSE_SOURCE = "rolling_avg_dollar_volume_top_500"


def _safe_partition_key(context: AssetExecutionContext) -> str | None:
    try:
        return context.partition_key
    except Exception:
        return None


def _silver_prices_glob() -> str:
    return (
        DATA_ROOT / "silver" / RESEARCH_DAILY_PRICES_DATASET / "month=*" / "date=*.parquet"
    ).as_posix()


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


def universe_membership_symbols_for_date(con, target_date: date) -> list[tuple[str, int, float]]:
    if not _table_exists(con, "silver", "universe_membership_daily"):
        return []
    rows = con.execute(
        """
        SELECT symbol, liquidity_rank, rolling_avg_dollar_volume
        FROM silver.universe_membership_daily
        WHERE member_date = ?
        ORDER BY liquidity_rank, symbol
        """,
        [target_date],
    ).fetchall()
    return [(str(symbol), int(rank), float(liquidity)) for symbol, rank, liquidity in rows]


@asset(
    name="universe_membership_daily",
    key_prefix=["silver"],
    deps=[silver_research_daily_prices],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def silver_universe_membership_daily(context: AssetExecutionContext) -> None:
    """
    Build the daily research universe from the top names by trailing average dollar volume.
    """
    con = context.resources.research_duckdb
    prices_glob = _silver_prices_glob()

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE silver.universe_membership_daily AS
            WITH prices AS (
                SELECT
                    CAST(trade_date AS DATE) AS trade_date,
                    upper(trim(symbol)) AS symbol,
                    CAST(dollar_volume AS DOUBLE) AS dollar_volume
                FROM read_parquet(?)
                WHERE trade_date IS NOT NULL
                  AND symbol IS NOT NULL
                  AND trim(symbol) <> ''
                  AND dollar_volume IS NOT NULL
                  AND dollar_volume > 0
            ),
            liquidity AS (
                SELECT
                    trade_date AS member_date,
                    symbol,
                    avg(dollar_volume) OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date
                        ROWS BETWEEN ? PRECEDING AND CURRENT ROW
                    ) AS rolling_avg_dollar_volume
                FROM prices
            ),
            ranked AS (
                SELECT
                    member_date,
                    symbol,
                    rolling_avg_dollar_volume,
                    row_number() OVER (
                        PARTITION BY member_date
                        ORDER BY rolling_avg_dollar_volume DESC, symbol ASC
                    ) AS liquidity_rank
                FROM liquidity
            )
            SELECT
                member_date,
                symbol,
                liquidity_rank,
                rolling_avg_dollar_volume,
                ? AS source,
                current_timestamp AS ingested_ts
            FROM ranked
            WHERE liquidity_rank <= ?
            ORDER BY member_date, liquidity_rank, symbol
            """,
            [prices_glob, LIQUIDITY_LOOKBACK_DAYS - 1, UNIVERSE_SOURCE, UNIVERSE_SIZE],
        )
    except Exception as exc:
        context.log.warning(
            "Unable to build silver.universe_membership_daily from %s: %s",
            prices_glob,
            exc,
        )
        return

    row_count = con.execute("SELECT count(*) FROM silver.universe_membership_daily").fetchone()[0]
    symbol_count = con.execute(
        "SELECT count(DISTINCT symbol) FROM silver.universe_membership_daily"
    ).fetchone()[0]
    min_max_row = con.execute(
        "SELECT min(member_date), max(member_date) FROM silver.universe_membership_daily"
    ).fetchone()

    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None
    partition_key = _safe_partition_key(context)

    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_universe_membership_daily_required_fields_nulls",
        relation_sql="SELECT * FROM silver.universe_membership_daily",
        relation_params=[],
        required_columns=[
            "member_date",
            "symbol",
            "liquidity_rank",
            "rolling_avg_dollar_volume",
            "source",
            "ingested_ts",
        ],
        details={"table": "silver.universe_membership_daily"},
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
    )

    context.add_output_metadata(
        {
            "table": "silver.universe_membership_daily",
            "row_count": int(row_count or 0),
            "symbol_count": int(symbol_count or 0),
            "min_member_date": str(min_max_row[0]) if min_max_row and min_max_row[0] else None,
            "max_member_date": str(min_max_row[1]) if min_max_row and min_max_row[1] else None,
            "universe_size": UNIVERSE_SIZE,
            "liquidity_lookback_days": LIQUIDITY_LOOKBACK_DAYS,
        }
    )


@asset(
    name="universe_membership_events",
    key_prefix=["silver"],
    deps=[silver_universe_membership_daily],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def silver_universe_membership_events(context: AssetExecutionContext) -> None:
    """
    Build add/remove events between consecutive liquidity-universe trading days.
    """
    con = context.resources.research_duckdb
    if not _table_exists(con, "silver", "universe_membership_daily"):
        context.log.warning("silver.universe_membership_daily is missing; skipping event build.")
        return

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.universe_membership_events AS
        WITH distinct_membership AS (
            SELECT DISTINCT
                member_date,
                symbol,
                source,
                liquidity_rank,
                rolling_avg_dollar_volume
            FROM silver.universe_membership_daily
        ),
        days AS (
            SELECT
                member_date,
                lag(member_date) OVER (ORDER BY member_date) AS previous_member_date
            FROM (
                SELECT DISTINCT member_date
                FROM distinct_membership
            )
        ),
        current_members AS (
            SELECT
                d.member_date,
                d.previous_member_date,
                m.symbol,
                m.source
            FROM days AS d
            INNER JOIN distinct_membership AS m
                ON m.member_date = d.member_date
        ),
        previous_members AS (
            SELECT
                d.member_date,
                d.previous_member_date,
                m.symbol,
                m.source
            FROM days AS d
            INNER JOIN distinct_membership AS m
                ON m.member_date = d.previous_member_date
        ),
        added AS (
            SELECT
                curr.member_date AS event_date,
                curr.symbol,
                curr.source,
                'added' AS event_type
            FROM current_members AS curr
            LEFT JOIN previous_members AS prev
                ON curr.member_date = prev.member_date
               AND curr.symbol = prev.symbol
               AND curr.source = prev.source
            WHERE prev.symbol IS NULL
        ),
        removed AS (
            SELECT
                prev.member_date AS event_date,
                prev.symbol,
                prev.source,
                'removed' AS event_type
            FROM previous_members AS prev
            LEFT JOIN current_members AS curr
                ON prev.member_date = curr.member_date
               AND prev.symbol = curr.symbol
               AND prev.source = curr.source
            WHERE curr.symbol IS NULL
        ),
        changes AS (
            SELECT * FROM added
            UNION ALL
            SELECT * FROM removed
        )
        SELECT
            changes.event_date,
            changes.symbol,
            changes.event_type,
            prev.liquidity_rank AS previous_liquidity_rank,
            curr.liquidity_rank AS new_liquidity_rank,
            prev.rolling_avg_dollar_volume AS previous_rolling_avg_dollar_volume,
            curr.rolling_avg_dollar_volume AS new_rolling_avg_dollar_volume,
            changes.source,
            current_timestamp AS ingested_ts
        FROM changes
        LEFT JOIN distinct_membership AS prev
            ON prev.member_date = (
                SELECT previous_member_date
                FROM days
                WHERE member_date = changes.event_date
            )
           AND prev.symbol = changes.symbol
           AND prev.source = changes.source
        LEFT JOIN distinct_membership AS curr
            ON curr.member_date = changes.event_date
           AND curr.symbol = changes.symbol
           AND curr.source = changes.source
        ORDER BY changes.event_date, changes.event_type, changes.symbol
        """
    )

    row_count = con.execute("SELECT count(*) FROM silver.universe_membership_events").fetchone()[0]
    symbol_count = con.execute(
        "SELECT count(DISTINCT symbol) FROM silver.universe_membership_events"
    ).fetchone()[0]

    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None
    partition_key = _safe_partition_key(context)

    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_universe_membership_events_required_fields_nulls",
        relation_sql="SELECT * FROM silver.universe_membership_events",
        relation_params=[],
        required_columns=["event_date", "symbol", "event_type", "source", "ingested_ts"],
        details={"table": "silver.universe_membership_events"},
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
    )
    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_universe_membership_events_added_fields_nulls",
        relation_sql="""
            SELECT *
            FROM silver.universe_membership_events
            WHERE event_type = 'added'
        """,
        relation_params=[],
        required_columns=["new_liquidity_rank", "new_rolling_avg_dollar_volume"],
        details={
            "table": "silver.universe_membership_events",
            "rule": "new rank and liquidity are required for added rows",
        },
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
    )
    log_required_field_null_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        check_name="dq_research_universe_membership_events_removed_fields_nulls",
        relation_sql="""
            SELECT *
            FROM silver.universe_membership_events
            WHERE event_type = 'removed'
        """,
        relation_params=[],
        required_columns=["previous_liquidity_rank", "previous_rolling_avg_dollar_volume"],
        details={
            "table": "silver.universe_membership_events",
            "rule": "previous rank and liquidity are required for removed rows",
        },
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
    )

    context.add_output_metadata(
        {
            "table": "silver.universe_membership_events",
            "row_count": int(row_count or 0),
            "symbol_count": int(symbol_count or 0),
        }
    )
