import os
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset
from dagster._core.errors import DagsterInvalidPropertyError

from portfolio_project.defs.portfolio_db.observability.observability_modules import (
    write_dq_log,
)
from portfolio_project.defs.research_db.dq_checks import (
    log_duplicate_row_check,
    log_required_field_null_check,
)

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
RESEARCH_PRICES_PARTITIONS_START_DATE = os.getenv(
    "RESEARCH_EODHD_PRICES_PARTITIONS_START_DATE", "2000-01-01"
)
RESEARCH_PRICES_PARTITIONS = DailyPartitionsDefinition(
    start_date=RESEARCH_PRICES_PARTITIONS_START_DATE
)
RESEARCH_DAILY_PRICES_DATASET = "research_daily_prices"
SOURCE_PRIORITY = {"alpaca": 0, "eodhd": 1}
RESEARCH_DAILY_PRICES_MIN_SYMBOL_COUNT = int(
    os.getenv("RESEARCH_DAILY_PRICES_MIN_SYMBOL_COUNT", "2")
)
RESEARCH_DAILY_PRICES_EXCEPTION_SYMBOLS = {"SPY"}
PRICE_COLUMNS = [
    "symbol",
    "timestamp",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "trade_count",
    "vwap",
    "dollar_volume",
    "source",
    "ingested_ts",
]
EXPECTED_DUCKDB_TYPES = {
    "symbol": {"VARCHAR"},
    "timestamp": {"TIMESTAMP WITH TIME ZONE"},
    "trade_date": {"DATE"},
    "open": {"DOUBLE"},
    "high": {"DOUBLE"},
    "low": {"DOUBLE"},
    "close": {"DOUBLE"},
    "adjusted_close": {"DOUBLE"},
    "volume": {"BIGINT"},
    "trade_count": {"BIGINT"},
    "vwap": {"DOUBLE"},
    "dollar_volume": {"DOUBLE"},
    "source": {"VARCHAR"},
    "ingested_ts": {"TIMESTAMP WITH TIME ZONE"},
}
NON_NEGATIVE_PRICE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "vwap",
    "dollar_volume",
]
NON_NEGATIVE_COUNT_COLUMNS = [
    "volume",
    "trade_count",
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


def _table_has_column(con, schema: str, table: str, column: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
            LIMIT 1
            """,
            [schema, table, column],
        ).fetchone()
        is not None
    )


def _bronze_prices_path(dataset_name: str, trade_date: date) -> Path:
    return DATA_ROOT / "bronze" / dataset_name / f"date={trade_date.isoformat()}" / "prices.parquet"


def _silver_monthly_prices_path(trade_date: date) -> Path:
    return (
        DATA_ROOT
        / "silver"
        / RESEARCH_DAILY_PRICES_DATASET
        / f"month={trade_date.strftime('%Y-%m')}"
        / f"date={trade_date.isoformat()}.parquet"
    )


def _load_bronze_prices(dataset_name: str, trade_date: date) -> pd.DataFrame:
    path = _bronze_prices_path(dataset_name, trade_date)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _validate_research_daily_prices_schema(con, parquet_path: Path) -> dict:
    del con
    schema_con = duckdb.connect(":memory:")
    try:
        schema_rows = schema_con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
            [parquet_path.as_posix()],
        ).fetchall()
    finally:
        schema_con.close()
    actual_types = {str(row[0]): str(row[1]).upper() for row in schema_rows}
    expected_columns = list(EXPECTED_DUCKDB_TYPES)
    actual_columns = list(actual_types)
    missing_columns = [column for column in expected_columns if column not in actual_types]
    unexpected_columns = [
        column for column in actual_columns if column not in EXPECTED_DUCKDB_TYPES
    ]

    type_mismatches = {}
    for column, expected_types in EXPECTED_DUCKDB_TYPES.items():
        actual_type = actual_types.get(column)
        if actual_type is None:
            continue
        normalized_expected = {expected_type.upper() for expected_type in expected_types}
        if actual_type not in normalized_expected:
            type_mismatches[column] = {
                "expected": sorted(normalized_expected),
                "actual": actual_type,
            }

    status = "PASS"
    if missing_columns or unexpected_columns or type_mismatches:
        status = "FAIL"

    return {
        "status": status,
        "measured_value": float(
            len(missing_columns) + len(unexpected_columns) + len(type_mismatches)
        ),
        "threshold_value": 0.0,
        "details": {
            "path": parquet_path.as_posix(),
            "expected_columns": expected_columns,
            "actual_columns": actual_columns,
            "missing_columns": missing_columns,
            "unexpected_columns": unexpected_columns,
            "type_mismatches": type_mismatches,
        },
    }


def _log_research_daily_prices_schema_check(
    context: AssetExecutionContext,
    con,
    parquet_path: Path,
) -> None:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    partition_key = getattr(context, "partition_key", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    result = _validate_research_daily_prices_schema(con, parquet_path)
    write_dq_log(
        con=con,
        check_name="dq_research_daily_prices_schema_columns_and_types",
        severity="RED",
        status=result["status"],
        measured_value=result["measured_value"],
        threshold_value=result["threshold_value"],
        details=result["details"],
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def _log_research_daily_prices_required_field_check(
    context: AssetExecutionContext,
    parquet_path: Path,
) -> None:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    partition_key = getattr(context, "partition_key", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    measured_con = duckdb.connect(":memory:")
    try:
        log_required_field_null_check(
            measured_con=measured_con,
            observability_con=context.resources.duckdb,
            check_name="dq_research_daily_prices_required_fields_nulls",
            relation_sql="SELECT * FROM read_parquet(?, hive_partitioning = false)",
            relation_params=[parquet_path.as_posix()],
            required_columns=[
                "symbol",
                "timestamp",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source",
                "ingested_ts",
            ],
            details={"path": parquet_path.as_posix(), "table": "silver.research_daily_prices"},
            run_id=str(run_id) if run_id else None,
            job_name=job_name,
            partition_key=partition_key,
        )
    finally:
        measured_con.close()


def _log_research_daily_prices_duplicate_symbol_date_check(
    context: AssetExecutionContext,
    parquet_path: Path,
) -> None:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    partition_key = getattr(context, "partition_key", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    measured_con = duckdb.connect(":memory:")
    try:
        log_duplicate_row_check(
            measured_con=measured_con,
            observability_con=context.resources.duckdb,
            check_name="dq_research_daily_prices_uniqueness_symbol_trade_date",
            relation_sql="SELECT * FROM read_parquet(?, hive_partitioning = false)",
            relation_params=[parquet_path.as_posix()],
            key_columns=["symbol", "trade_date"],
            details={"path": parquet_path.as_posix(), "table": "silver.research_daily_prices"},
            run_id=str(run_id) if run_id else None,
            job_name=job_name,
            partition_key=partition_key,
        )
    finally:
        measured_con.close()


def _log_research_daily_prices_invalid_values_check(
    context: AssetExecutionContext,
    parquet_path: Path,
) -> None:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    partition_key = getattr(context, "partition_key", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    measured_con = duckdb.connect(":memory:")
    try:
        row = measured_con.execute(
            """
            WITH scoped_rows AS (
                SELECT * FROM read_parquet(?, hive_partitioning = false)
            )
            SELECT
                count(*) FILTER (
                    WHERE open < 0
                       OR high < 0
                       OR low < 0
                       OR close < 0
                       OR adjusted_close < 0
                       OR volume < 0
                       OR trade_count < 0
                       OR vwap < 0
                       OR dollar_volume < 0
                ) AS negative_value_rows,
                count(*) FILTER (
                    WHERE high < low
                       OR open > high
                       OR open < low
                       OR close > high
                       OR close < low
                ) AS ohlc_range_violation_rows,
                count(*) FILTER (
                    WHERE vwap IS NOT NULL
                      AND low IS NOT NULL
                      AND high IS NOT NULL
                      AND (vwap < low OR vwap > high)
                ) AS vwap_outside_range_rows,
                count(*) FILTER (
                    WHERE adjusted_close IS NOT NULL
                      AND adjusted_close <= 0
                ) AS adjusted_close_non_positive_rows
            FROM scoped_rows
            """,
            [parquet_path.as_posix()],
        ).fetchone()

        check_counts = {
            "negative_value_rows": int(row[0] or 0),
            "ohlc_range_violation_rows": int(row[1] or 0),
            "vwap_outside_range_rows": int(row[2] or 0),
            "adjusted_close_non_positive_rows": int(row[3] or 0),
        }
        measured_value = float(sum(check_counts.values()))
        details = {
            "path": parquet_path.as_posix(),
            "table": "silver.research_daily_prices",
            "non_negative_columns": [
                *NON_NEGATIVE_PRICE_COLUMNS,
                *NON_NEGATIVE_COUNT_COLUMNS,
            ],
            "checks": {
                "negative_values": "All numeric market value/count fields must be non-negative.",
                "ohlc_ranges": "high >= low and both open/close must stay within [low, high].",
                "vwap_range": "vwap must stay within [low, high] when present.",
                "adjusted_close_positive": "adjusted_close must be > 0 when present.",
            },
            "violation_counts": check_counts,
        }
        write_dq_log(
            con=context.resources.duckdb,
            check_name="dq_research_daily_prices_invalid_values",
            severity="RED",
            status="PASS" if measured_value == 0 else "FAIL",
            measured_value=measured_value,
            threshold_value=0.0,
            details=details,
            run_id=str(run_id) if run_id else None,
            job_name=job_name,
            partition_key=partition_key,
            dedupe_by_run_check=True,
        )
    finally:
        measured_con.close()


def _log_research_daily_prices_symbol_coverage_check(
    context: AssetExecutionContext,
    day_df: pd.DataFrame,
) -> dict[str, object]:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    partition_key = getattr(context, "partition_key", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    symbols = sorted(
        day_df["symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist()
    )
    symbol_count = len(symbols)
    exception_only_universe = bool(symbols) and set(symbols).issubset(
        RESEARCH_DAILY_PRICES_EXCEPTION_SYMBOLS
    )
    source_counts = {
        str(source): int(count)
        for source, count in day_df["source"].astype(str).value_counts().sort_index().items()
    }
    status = (
        "FAIL"
        if symbol_count < RESEARCH_DAILY_PRICES_MIN_SYMBOL_COUNT or exception_only_universe
        else "PASS"
    )
    details = {
        "table": "silver.research_daily_prices",
        "expected_partition_key": partition_key,
        "symbol_count": symbol_count,
        "minimum_symbol_count": RESEARCH_DAILY_PRICES_MIN_SYMBOL_COUNT,
        "exception_only_universe": exception_only_universe,
        "exception_symbols": sorted(RESEARCH_DAILY_PRICES_EXCEPTION_SYMBOLS),
        "source_counts": source_counts,
        "symbols_preview": symbols[:20],
    }
    write_dq_log(
        con=context.resources.duckdb,
        check_name="dq_research_daily_prices_symbol_coverage",
        severity="RED",
        status=status,
        measured_value=float(symbol_count),
        threshold_value=float(RESEARCH_DAILY_PRICES_MIN_SYMBOL_COUNT),
        details=details,
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )
    return {"status": status, "details": details}


def _normalize_daily_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    normalized = df.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
    normalized = normalized[normalized["symbol"].ne("")].copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="coerce").dt.date
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True, errors="coerce")

    for column in [
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "trade_count",
        "vwap",
    ]:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized["source"] = normalized["source"].astype(str).str.strip().str.lower()
    normalized["ingested_ts"] = pd.to_datetime(normalized["ingested_ts"], utc=True, errors="coerce")
    normalized["dollar_volume"] = pd.to_numeric(
        normalized["close"], errors="coerce"
    ) * pd.to_numeric(normalized["volume"], errors="coerce")
    normalized["source_priority"] = normalized["source"].map(SOURCE_PRIORITY).fillna(99).astype(int)
    normalized = normalized[
        normalized["trade_date"].notna()
        & normalized["timestamp"].notna()
        & normalized["source"].ne("")
    ].copy()

    ordered_columns = [*PRICE_COLUMNS, "source_priority"]
    return normalized[ordered_columns].sort_values(
        ["symbol", "trade_date", "source_priority", "timestamp", "ingested_ts"],
        ascending=[True, True, True, False, False],
        kind="stable",
    )


def combine_source_daily_prices(trade_date: date) -> pd.DataFrame:
    frames = [
        _normalize_daily_prices_df(_load_bronze_prices("alpaca_prices_daily", trade_date)),
        _normalize_daily_prices_df(_load_bronze_prices("eodhd_prices_daily", trade_date)),
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    for frame in frames:
        for column in [
            "adjusted_close",
            "trade_count",
            "vwap",
        ]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "trade_date"], keep="first").copy()
    combined = combined.drop(columns=["source_priority"])
    return combined[PRICE_COLUMNS].sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def write_research_daily_prices_partition(
    trade_date: date,
    day_df: pd.DataFrame,
    *,
    overwrite: bool,
) -> tuple[int, int]:
    out_path = _silver_monthly_prices_path(trade_date)
    if out_path.exists() and not overwrite:
        return 0, 0
    if day_df is None or day_df.empty:
        return 0, 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    day_df.to_parquet(temp_path, index=False)
    temp_path.replace(out_path)
    return 1, len(day_df)


@asset(
    name="research_daily_prices",
    key_prefix=["silver"],
    partitions_def=RESEARCH_PRICES_PARTITIONS,
    required_resource_keys={"research_duckdb", "duckdb"},
)
def silver_research_daily_prices(context: AssetExecutionContext) -> None:
    """
    Build the research daily silver price partition by preferring Alpaca overlap and
    falling back to EODHD for the rest of the historical range.
    """
    trade_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    day_df = combine_source_daily_prices(trade_date)

    if day_df.empty:
        context.log.warning(
            "No research daily prices found for partition %s.",
            context.partition_key,
        )
        return

    symbol_coverage_result = _log_research_daily_prices_symbol_coverage_check(context, day_df)
    if symbol_coverage_result["status"] != "PASS":
        raise ValueError(
            "Research daily prices symbol coverage check failed for "
            f"{context.partition_key}: {symbol_coverage_result['details']}"
        )

    alpaca_rows = day_df["source"].eq("alpaca")
    has_corporate_actions = _table_exists(
        context.resources.research_duckdb,
        "silver",
        "alpaca_corporate_actions",
    )
    if alpaca_rows.any() and has_corporate_actions:
        symbols = sorted(day_df.loc[alpaca_rows, "symbol"].dropna().astype(str).unique().tolist())
        if symbols:
            has_action_type = _table_has_column(
                context.resources.research_duckdb,
                "silver",
                "alpaca_corporate_actions",
                "action_type",
            )
            action_type_filter = ""
            if has_action_type:
                action_type_filter = "AND action_type IN ('forward_splits', 'reverse_splits')"

            factor_df = context.resources.research_duckdb.execute(
                f"""
                SELECT
                    symbol,
                    exp(sum(ln(old_rate / new_rate))) AS split_adjustment_factor
                FROM silver.alpaca_corporate_actions
                WHERE effective_date > ?
                  {action_type_filter}
                  AND symbol = ANY(?)
                GROUP BY symbol
                """,
                [trade_date, symbols],
            ).fetch_df()
            if not factor_df.empty:
                factor_df["symbol"] = factor_df["symbol"].astype(str)
                factor_map = dict(
                    zip(
                        factor_df["symbol"].tolist(),
                        factor_df["split_adjustment_factor"].astype(float).tolist(),
                    )
                )
                split_factor = (
                    day_df.loc[alpaca_rows, "symbol"].map(factor_map).fillna(1.0).astype(float)
                )
            else:
                split_factor = pd.Series(1.0, index=day_df.loc[alpaca_rows].index, dtype="float64")

            day_df.loc[alpaca_rows, "adjusted_close"] = (
                pd.to_numeric(day_df.loc[alpaca_rows, "close"], errors="coerce")
                * split_factor.values
            )

    files_written, rows_written = write_research_daily_prices_partition(
        trade_date,
        day_df,
        overwrite=True,
    )
    output_path = _silver_monthly_prices_path(trade_date)
    _log_research_daily_prices_schema_check(
        context,
        context.resources.duckdb,
        output_path,
    )
    _log_research_daily_prices_required_field_check(context, output_path)
    _log_research_daily_prices_duplicate_symbol_date_check(context, output_path)
    _log_research_daily_prices_invalid_values_check(context, output_path)
    source_counts = {
        str(source): int(count)
        for source, count in day_df["source"].value_counts().sort_index().items()
    }
    context.add_output_metadata(
        {
            "partition": context.partition_key,
            "row_count": rows_written,
            "files_written": files_written,
            "symbol_count": int(day_df["symbol"].nunique()),
            "source_counts": source_counts,
            "table": "silver.research_daily_prices",
            "output_path": output_path.as_posix(),
        }
    )
