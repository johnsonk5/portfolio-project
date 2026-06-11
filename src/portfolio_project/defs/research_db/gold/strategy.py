import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError

from portfolio_project.defs.portfolio_db.observability.observability_modules import (
    write_dq_log,
)
from portfolio_project.defs.research_db.dq_checks import log_duplicate_row_check
from portfolio_project.defs.research_db.ref.trading_days import ref_invalid_trading_days
from portfolio_project.defs.research_db.silver.research_prices import (
    RESEARCH_DAILY_PRICES_DATASET,
    silver_research_daily_prices,
)
from portfolio_project.defs.research_db.silver.security_master import silver_security_master
from portfolio_project.defs.research_db.silver.signals import silver_signals_daily
from portfolio_project.defs.research_db.silver.strategy import (
    STRATEGY_RUNS_COLUMNS,
    _ensure_table_contract,
    _quote_identifier,
    _safe_run_id,
    silver_strategy_definitions,
    silver_strategy_parameters,
    silver_strategy_runs,
)
from portfolio_project.defs.research_db.silver.universe import silver_universe_membership_daily
from portfolio_project.defs.research_db.trading_calendar import (
    INVALID_TRADING_DAY_RECORDS,
    create_valid_trading_dates_table,
    filter_us_trading_days,
    is_us_trading_day,
)

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
PRICE_GLOB = (
    DATA_ROOT / "silver" / RESEARCH_DAILY_PRICES_DATASET / "month=*" / "date=*.parquet"
).as_posix()
MAX_ABS_DAILY_SECURITY_RETURN = float(
    os.getenv("RESEARCH_STRATEGY_MAX_ABS_DAILY_SECURITY_RETURN", "5.0")
)
STRATEGY_HOLDINGS_WEIGHT_SUM_TOLERANCE = float(
    os.getenv("RESEARCH_STRATEGY_HOLDINGS_WEIGHT_SUM_TOLERANCE", "1e-6")
)
DUCKDB_STRATEGY_THREADS = int(os.getenv("RESEARCH_STRATEGY_DUCKDB_THREADS", "2"))
MISSING_STRATEGIES_JOB_NAME = "strategy_missing_backfill_job"

STRATEGY_RANKINGS_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("rebalance_date", "DATE"),
    ("asset_id", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("score", "DOUBLE"),
    ("rank", "INTEGER"),
    ("selected_flag", "BOOLEAN"),
    ("asof_ts", "TIMESTAMP"),
]

STRATEGY_HOLDINGS_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("rebalance_date", "DATE"),
    ("asset_id", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("target_weight", "DOUBLE"),
    ("side", "VARCHAR"),
    ("entry_rank", "INTEGER"),
    ("signal_value", "DOUBLE"),
    ("asof_ts", "TIMESTAMP"),
]

STRATEGY_RETURNS_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("date", "DATE"),
    ("portfolio_return", "DOUBLE"),
    ("benchmark_return", "DOUBLE"),
    ("excess_return", "DOUBLE"),
    ("cumulative_return", "DOUBLE"),
    ("drawdown", "DOUBLE"),
    ("turnover", "DOUBLE"),
    ("holdings_count", "INTEGER"),
    ("held_symbols_expected", "INTEGER"),
    ("held_symbols_with_returns", "INTEGER"),
    ("missing_symbols", "VARCHAR"),
    ("asof_ts", "TIMESTAMP"),
]

STRATEGY_PERFORMANCE_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("cagr", "DOUBLE"),
    ("sharpe_ratio", "DOUBLE"),
    ("sortino_ratio", "DOUBLE"),
    ("max_drawdown", "DOUBLE"),
    ("annualized_volatility", "DOUBLE"),
    ("hit_rate", "DOUBLE"),
    ("turnover_avg", "DOUBLE"),
    ("benchmark_return", "DOUBLE"),
    ("alpha", "DOUBLE"),
    ("asof_ts", "TIMESTAMP"),
]


@dataclass(frozen=True)
class StrategyConfig:
    strategy_id: str
    rebalance_frequency: str
    benchmark_symbol: str
    target_count: int
    weighting_method: str
    long_short_flag: bool
    start_date: date | None
    end_date: date | None
    config: dict[str, Any]
    run_id: str


@dataclass(frozen=True)
class SimulationTypeConfig:
    simulation_type_id: int | None
    simulation_type_code: str
    fill_price_basis: str
    slippage_model: str
    slippage_bps: float
    slippage_params: dict[str, Any]


DEFAULT_SIMULATION_TYPE = SimulationTypeConfig(
    simulation_type_id=None,
    simulation_type_code="close_no_cost",
    fill_price_basis="close",
    slippage_model="none",
    slippage_bps=0.0,
    slippage_params={},
)


def _now_utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _safe_job_name(context: AssetExecutionContext) -> str | None:
    try:
        return getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        return None


def _safe_partition_key(context: AssetExecutionContext) -> str | None:
    try:
        return getattr(context, "partition_key", None)
    except Exception:
        return None


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


def _table_columns(con, schema: str, table: str) -> set[str]:
    if not _table_exists(con, schema, table):
        return set()
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {str(row[0]).lower() for row in rows if row[0] is not None}


def _invalid_research_trading_dates(con) -> set[date]:
    invalid_dates = {
        pd.Timestamp(record["invalid_date"]).date()
        for record in INVALID_TRADING_DAY_RECORDS
        if record.get("invalid_date")
    }
    if _table_exists(con, "ref", "invalid_trading_days"):
        invalid_dates.update(
            pd.Timestamp(row[0]).date()
            for row in con.execute(
                """
                SELECT DISTINCT invalid_date
                FROM ref.invalid_trading_days
                WHERE invalid_date IS NOT NULL
                """
            ).fetchall()
            if row[0] is not None
        )
    return invalid_dates


def _filter_valid_research_trading_days(con, values: list[date]) -> list[date]:
    invalid_dates = _invalid_research_trading_dates(con)
    return [
        value
        for value in values
        if value is not None and is_us_trading_day(value) and value not in invalid_dates
    ]


def _filter_valid_research_trading_frame(
    con,
    frame: pd.DataFrame,
    date_column: str,
) -> pd.DataFrame:
    frame = filter_us_trading_days(frame, date_column)
    if frame.empty or date_column not in frame.columns:
        return frame
    invalid_dates = _invalid_research_trading_dates(con)
    if not invalid_dates:
        return frame
    dates = pd.to_datetime(frame[date_column], errors="coerce").dt.date
    return frame.loc[~dates.isin(invalid_dates)].copy()


def _candidate_eligibility_join_sql(con) -> str:
    joins: list[str] = []
    if _table_exists(con, "silver", "universe_eligibility_daily"):
        joins.append(
            """
                INNER JOIN silver.universe_eligibility_daily AS ue
                    ON CAST(ue.date AS DATE) = CAST(s.date AS DATE)
                   AND CAST(ue.asset_id AS BIGINT) = CAST(s.asset_id AS BIGINT)
                   AND coalesce(ue.is_eligible_research_universe, FALSE) = TRUE
            """
        )
    if _table_exists(con, "silver", "security_master"):
        joins.append(
            """
                INNER JOIN silver.security_master AS sm
                    ON upper(trim(coalesce(sm.canonical_symbol, sm.symbol))) = upper(trim(s.symbol))
                   AND coalesce(sm.is_investable_common_equity, FALSE) = TRUE
            """
        )
    return "\n".join(joins)


def _safe_json_loads(raw_value: Any) -> dict[str, Any]:
    if raw_value in (None, ""):
        return {}
    parsed = json.loads(str(raw_value))
    return parsed if isinstance(parsed, dict) else {}


def _simulation_type_for_run(con, run_id: str) -> SimulationTypeConfig:
    if not _table_exists(con, "silver", "strategy_runs"):
        return DEFAULT_SIMULATION_TYPE

    run_row = con.execute(
        """
        SELECT run_type_id, simulation_type_id
        FROM silver.strategy_runs
        WHERE run_id = ?
        LIMIT 1
        """,
        [run_id],
    ).fetchone()
    if run_row is None:
        return DEFAULT_SIMULATION_TYPE

    run_type_id = str(run_row[0] or "").strip().lower()
    simulation_type_id = run_row[1]
    if run_type_id != "simulation" or simulation_type_id is None:
        return DEFAULT_SIMULATION_TYPE
    if not _table_exists(con, "ref", "simulation_types"):
        raise ValueError(
            f"Strategy run {run_id} has simulation_type_id={simulation_type_id}, "
            "but ref.simulation_types is not materialized."
        )

    type_row = con.execute(
        """
        SELECT
            simulation_type_id,
            simulation_type_code,
            fill_price_basis,
            slippage_model,
            slippage_bps,
            slippage_params_json
        FROM ref.simulation_types
        WHERE simulation_type_id = ?
          AND is_active = TRUE
        LIMIT 1
        """,
        [simulation_type_id],
    ).fetchone()
    if type_row is None:
        raise ValueError(
            f"Strategy run {run_id} references inactive or unknown "
            f"simulation_type_id={simulation_type_id}."
        )

    return SimulationTypeConfig(
        simulation_type_id=int(type_row[0]),
        simulation_type_code=str(type_row[1]).strip().lower(),
        fill_price_basis=str(type_row[2]).strip().lower(),
        slippage_model=str(type_row[3]).strip().lower(),
        slippage_bps=float(type_row[4] or 0.0),
        slippage_params=_safe_json_loads(type_row[5]),
    )


def _coerce_parameter_value(raw_value: Any, parameter_type: str) -> Any:
    if raw_value is None:
        return None
    normalized_type = str(parameter_type or "").strip().lower()
    raw_str = str(raw_value).strip()
    if normalized_type in {"int", "integer"}:
        return int(float(raw_str))
    if normalized_type in {"double", "float", "number"}:
        return float(raw_str)
    if normalized_type in {"bool", "boolean"}:
        return raw_str.lower() in {"1", "true", "t", "yes", "y"}
    return raw_str


def _strategy_run_id(context: AssetExecutionContext, strategy_id: str) -> str:
    run_id = _safe_run_id(context)
    if run_id:
        return f"{run_id}:{strategy_id}"
    return f"manual:{strategy_id}"


def _pending_strategy_run_ids(con, strategy_id: str) -> list[str]:
    if not _table_exists(con, "silver", "strategy_runs"):
        return []
    rows = con.execute(
        """
        SELECT run_id
        FROM silver.strategy_runs
        WHERE strategy_id = ?
          AND run_type_id = 'simulation'
          AND run_status IN ('pending', 'running')
        ORDER BY asof_ts DESC NULLS LAST,
                 started_at DESC NULLS LAST,
                 run_id DESC
        """,
        [strategy_id],
    ).fetchall()
    return [str(row[0]) for row in rows if row[0] not in (None, "")]


def _simulation_strategy_run_exists(con, run_id: str) -> bool:
    if not _table_exists(con, "silver", "strategy_runs"):
        return False
    return (
        con.execute(
            """
            SELECT 1
            FROM silver.strategy_runs
            WHERE run_id = ?
              AND run_type_id = 'simulation'
            LIMIT 1
            """,
            [run_id],
        ).fetchone()
        is not None
    )


def _active_strategies(con, context: AssetExecutionContext) -> list[StrategyConfig]:
    if not _table_exists(con, "silver", "strategy_definitions"):
        return []

    rows = con.execute(
        """
        SELECT
            strategy_id,
            rebalance_frequency,
            benchmark_symbol,
            target_count,
            weighting_method,
            long_short_flag,
            start_date,
            end_date,
            config_json
        FROM silver.strategy_definitions
        WHERE is_active = TRUE
        ORDER BY strategy_id
        """
    ).fetchall()
    strategies: list[StrategyConfig] = []
    for row in rows:
        strategy_id = str(row[0])
        run_ids = _pending_strategy_run_ids(con, strategy_id) or [
            _strategy_run_id(context, strategy_id)
        ]
        strategy_configs = [
            StrategyConfig(
                strategy_id=strategy_id,
                rebalance_frequency=str(row[1]),
                benchmark_symbol=str(row[2]),
                target_count=int(row[3]),
                weighting_method=str(row[4]),
                long_short_flag=bool(row[5]),
                start_date=row[6],
                end_date=row[7],
                config=_safe_json_loads(row[8]),
                run_id=run_id,
            )
            for run_id in run_ids
        ]
        strategies.extend(
            sorted(
                strategy_configs,
                key=lambda strategy_config: strategy_config.run_id,
            )
        )
    return strategies


def _ranking_method_for_strategy(con, strategy_id: str) -> str:
    if not _table_exists(con, "silver", "strategy_definitions"):
        return ""
    row = con.execute(
        """
        SELECT ranking_method
        FROM silver.strategy_definitions
        WHERE strategy_id = ?
        LIMIT 1
        """,
        [strategy_id],
    ).fetchone()
    if row is None or row[0] is None:
        return ""
    return str(row[0]).strip().lower()


def _strategies_with_latest_run_ids(
    con,
    context: AssetExecutionContext,
    *,
    source_table: str | None,
) -> list[StrategyConfig]:
    strategies = _active_strategies(con, context)
    if not source_table or not _table_exists(con, "gold", source_table):
        return strategies

    resolved: list[StrategyConfig] = []
    for strategy in strategies:
        row = con.execute(
            f"""
            SELECT run_id
            FROM gold.{_quote_identifier(source_table)}
            WHERE strategy_id = ?
              AND run_id = ?
            ORDER BY asof_ts DESC
            LIMIT 1
            """,
            [strategy.strategy_id, strategy.run_id],
        ).fetchone()
        if row is None or row[0] in (None, ""):
            if _simulation_strategy_run_exists(con, strategy.run_id):
                resolved.append(strategy)
                continue
            row = con.execute(
                f"""
                SELECT run_id
                FROM gold.{_quote_identifier(source_table)}
                WHERE strategy_id = ?
                ORDER BY asof_ts DESC, run_id DESC
                LIMIT 1
                """,
                [strategy.strategy_id],
            ).fetchone()
        if row is None or row[0] in (None, ""):
            resolved.append(strategy)
            continue
        resolved.append(
            StrategyConfig(
                strategy_id=strategy.strategy_id,
                rebalance_frequency=strategy.rebalance_frequency,
                benchmark_symbol=strategy.benchmark_symbol,
                target_count=strategy.target_count,
                weighting_method=strategy.weighting_method,
                long_short_flag=strategy.long_short_flag,
                start_date=strategy.start_date,
                end_date=strategy.end_date,
                config=dict(strategy.config),
                run_id=str(row[0]),
            )
        )
    return resolved


def _filter_missing_strategies(
    con,
    strategies: list[StrategyConfig],
) -> list[StrategyConfig]:
    if not strategies or not _table_exists(con, "gold", "strategy_performance"):
        return strategies

    pending_simulation_run_ids = {
        str(row[0])
        for row in con.execute(
            """
            SELECT DISTINCT run_id
            FROM silver.strategy_runs
            WHERE run_id IS NOT NULL
              AND lower(trim(coalesce(run_type_id, ''))) = 'simulation'
              AND lower(trim(coalesce(run_status, ''))) IN ('pending', 'running')
            """
        ).fetchall()
    }
    completed_run_ids = {
        str(row[0])
        for row in con.execute(
            """
            SELECT DISTINCT run_id
            FROM gold.strategy_performance
            WHERE run_id IS NOT NULL
            """
        ).fetchall()
    }
    return [
        strategy
        for strategy in strategies
        if strategy.run_id in pending_simulation_run_ids or strategy.run_id not in completed_run_ids
    ]


def _strategies_for_context(
    con,
    context: AssetExecutionContext,
    *,
    source_table: str | None,
) -> list[StrategyConfig]:
    strategies = (
        _strategies_with_latest_run_ids(con, context, source_table=source_table)
        if source_table
        else _active_strategies(con, context)
    )
    if _safe_job_name(context) == MISSING_STRATEGIES_JOB_NAME:
        return _filter_missing_strategies(con, strategies)
    return strategies


def _active_parameters_by_date(con, strategy_id: str, rebalance_date: date) -> dict[str, Any]:
    if not _table_exists(con, "silver", "strategy_parameters"):
        return {}

    rows = con.execute(
        """
        SELECT parameter_name, parameter_value, parameter_type
        FROM silver.strategy_parameters
        WHERE strategy_id = ?
          AND is_active = TRUE
          AND CAST(effective_start_date AS DATE) <= ?
          AND (
                effective_end_date IS NULL
                OR CAST(effective_end_date AS DATE) >= ?
          )
        ORDER BY parameter_name
        """,
        [strategy_id, rebalance_date, rebalance_date],
    ).fetchall()
    return {
        str(parameter_name): _coerce_parameter_value(parameter_value, parameter_type)
        for parameter_name, parameter_value, parameter_type in rows
    }


def _rebalance_dates_for_strategy(con, strategy: StrategyConfig) -> list[date]:
    if not _table_exists(con, "silver", "signals_daily"):
        return []

    start_date = strategy.start_date or date(1900, 1, 1)
    end_date = strategy.end_date or date(2999, 12, 31)
    rebalance_frequency = strategy.rebalance_frequency.strip().lower()
    if rebalance_frequency == "daily":
        rows = con.execute(
            """
            SELECT DISTINCT CAST(date AS DATE) AS rebalance_date
            FROM silver.signals_daily
            WHERE CAST(date AS DATE) >= ?
              AND CAST(date AS DATE) <= ?
            ORDER BY rebalance_date
            """,
            [start_date, end_date],
        ).fetchall()
        return _filter_valid_research_trading_days(con, [row[0] for row in rows])

    if rebalance_frequency == "weekly":
        rows = con.execute(
            """
            WITH filtered AS (
                SELECT CAST(date AS DATE) AS signal_date
                FROM silver.signals_daily
                WHERE CAST(date AS DATE) >= ?
                  AND CAST(date AS DATE) <= ?
            )
            SELECT max(signal_date) AS rebalance_date
            FROM filtered
            GROUP BY strftime(signal_date, '%G-%V')
            ORDER BY rebalance_date
            """,
            [start_date, end_date],
        ).fetchall()
        return _filter_valid_research_trading_days(con, [row[0] for row in rows])

    rows = con.execute(
        """
        WITH filtered AS (
            SELECT CAST(date AS DATE) AS signal_date
            FROM silver.signals_daily
            WHERE CAST(date AS DATE) >= ?
              AND CAST(date AS DATE) <= ?
        )
        SELECT max(signal_date) AS rebalance_date
        FROM filtered
        GROUP BY year(signal_date), month(signal_date)
        ORDER BY rebalance_date
        """,
        [start_date, end_date],
    ).fetchall()
    return _filter_valid_research_trading_days(con, [row[0] for row in rows])


def _build_rankings_for_strategy(
    con,
    strategy: StrategyConfig,
    asof_ts: datetime,
) -> list[dict[str, Any]]:
    rankings: list[dict[str, Any]] = []
    rebalance_dates = _rebalance_dates_for_strategy(con, strategy)

    for rebalance_date in rebalance_dates:
        parameters = _active_parameters_by_date(con, strategy.strategy_id, rebalance_date)
        ranking_method = _ranking_method_for_strategy(con, strategy.strategy_id)
        signal_column = str(parameters.get("signal_column") or "momentum_12_1").strip()
        secondary_signal_column = str(parameters.get("secondary_signal_column") or "").strip()
        ranking_direction = str(parameters.get("ranking_direction") or "desc").strip().lower()
        score_method = str(parameters.get("score_method") or "").strip().lower()
        universe_name = str(strategy.config.get("universe") or "").strip().lower()
        selection_mode = str(strategy.config.get("selection_mode") or "").strip().lower()
        fixed_symbol = str(parameters.get("symbol") or strategy.benchmark_symbol).strip().upper()
        max_pct_below_52w_high = parameters.get("max_pct_below_52w_high")

        if selection_mode == "fixed_symbol" or universe_name == "benchmark_only":
            candidate_rows = con.execute(
                """
                SELECT
                    upper(trim(symbol)) AS symbol,
                    CAST(asset_id AS BIGINT) AS asset_id,
                    1.0 AS score
                FROM silver.signals_daily
                WHERE CAST(date AS DATE) = ?
                  AND asset_id IS NOT NULL
                  AND upper(trim(symbol)) = ?
                LIMIT 1
                """,
                [rebalance_date, fixed_symbol],
            ).fetchall()
        elif ranking_method == "random_selection":
            eligibility_join_sql = _candidate_eligibility_join_sql(con)
            sql = """
                SELECT upper(trim(s.symbol)) AS symbol, CAST(s.asset_id AS BIGINT) AS asset_id
                FROM silver.signals_daily AS s
                INNER JOIN silver.universe_membership_daily AS u
                    ON CAST(u.member_date AS DATE) = CAST(s.date AS DATE)
                   AND CAST(u.asset_id AS BIGINT) = CAST(s.asset_id AS BIGINT)
                {eligibility_join_sql}
                WHERE CAST(s.date AS DATE) = ?
                  AND s.asset_id IS NOT NULL
            """.format(eligibility_join_sql=eligibility_join_sql)
            random_params: list[Any] = [rebalance_date]
            min_avg_dollar_volume_21d = parameters.get("min_avg_dollar_volume_21d")
            if min_avg_dollar_volume_21d is not None:
                sql += " AND CAST(s.avg_dollar_volume_21d AS DOUBLE) >= ?"
                random_params.append(float(min_avg_dollar_volume_21d))
            sql += " ORDER BY symbol"
            candidate_rows = con.execute(sql, random_params).fetchall()
        else:
            eligibility_join_sql = _candidate_eligibility_join_sql(con)
            secondary_select_sql = ""
            secondary_not_null_sql = ""
            if secondary_signal_column:
                secondary_identifier = _quote_identifier(secondary_signal_column)
                secondary_select_sql = (
                    f", CAST(s.{secondary_identifier} AS DOUBLE) AS secondary_score"
                )
                secondary_not_null_sql = (
                    f" AND CAST(s.{secondary_identifier} AS DOUBLE) IS NOT NULL"
                )
            sql = f"""
                SELECT
                    upper(trim(s.symbol)) AS symbol,
                    CAST(s.asset_id AS BIGINT) AS asset_id,
                    CAST(s.{_quote_identifier(signal_column)} AS DOUBLE) AS primary_score
                    {secondary_select_sql}
                FROM silver.signals_daily AS s
                INNER JOIN silver.universe_membership_daily AS u
                    ON CAST(u.member_date AS DATE) = CAST(s.date AS DATE)
                   AND CAST(u.asset_id AS BIGINT) = CAST(s.asset_id AS BIGINT)
                {eligibility_join_sql}
                WHERE CAST(s.date AS DATE) = ?
                  AND s.asset_id IS NOT NULL
                  AND CAST(s.{_quote_identifier(signal_column)} AS DOUBLE) IS NOT NULL
                  {secondary_not_null_sql}
            """
            params: list[Any] = [rebalance_date]
            min_avg_dollar_volume_21d = parameters.get("min_avg_dollar_volume_21d")
            if min_avg_dollar_volume_21d is not None:
                sql += " AND CAST(s.avg_dollar_volume_21d AS DOUBLE) >= ?"
                params.append(float(min_avg_dollar_volume_21d))
            min_price_to_sma_200 = parameters.get("min_price_to_sma_200")
            if min_price_to_sma_200 is not None:
                sql += " AND CAST(s.price_to_sma_200 AS DOUBLE) >= ?"
                params.append(float(min_price_to_sma_200))
            min_momentum_12_1 = parameters.get("min_momentum_12_1")
            if min_momentum_12_1 is not None:
                sql += " AND CAST(s.momentum_12_1 AS DOUBLE) > ?"
                params.append(float(min_momentum_12_1))
            if max_pct_below_52w_high is not None:
                sql += " AND CAST(s.pct_below_52w_high AS DOUBLE) <= ?"
                params.append(float(max_pct_below_52w_high))
            candidate_rows = con.execute(sql, params).fetchall()

        if ranking_method == "random_selection":
            candidate_df = pd.DataFrame(candidate_rows, columns=["symbol", "asset_id"])
            if candidate_df.empty:
                continue
            random_seed = int(parameters.get("random_seed") or 0)
            candidate_df["score"] = candidate_df["symbol"].map(
                lambda symbol: (
                    int.from_bytes(
                        hashlib.sha256(
                            f"{strategy.strategy_id}|{rebalance_date.isoformat()}|{random_seed}|{symbol}".encode(
                                "utf-8"
                            )
                        ).digest()[:8],
                        byteorder="big",
                        signed=False,
                    )
                    / float(2**64 - 1)
                )
            )
        else:
            candidate_columns = (
                ["symbol", "primary_score", "secondary_score"]
                if secondary_signal_column
                else ["symbol", "asset_id", "primary_score"]
            )
            if secondary_signal_column:
                candidate_columns = ["symbol", "asset_id", "primary_score", "secondary_score"]
            candidate_df = pd.DataFrame(candidate_rows, columns=candidate_columns)
            if candidate_df.empty:
                continue

            if secondary_signal_column and score_method == "zscore_sum":

                def _zscore(series: pd.Series) -> pd.Series:
                    std = series.std(ddof=0)
                    if pd.isna(std) or std == 0:
                        return pd.Series(0.0, index=series.index)
                    return (series - series.mean()) / std

                candidate_df["score"] = _zscore(candidate_df["primary_score"]) + _zscore(
                    candidate_df["secondary_score"]
                )
            elif secondary_signal_column and score_method == "ratio":
                candidate_df = candidate_df[
                    candidate_df["secondary_score"].notna() & candidate_df["secondary_score"].gt(0)
                ].copy()
                if candidate_df.empty:
                    continue
                candidate_df["score"] = (
                    candidate_df["primary_score"] / candidate_df["secondary_score"]
                )
            else:
                candidate_df["score"] = candidate_df["primary_score"]

        ascending = ranking_direction == "asc"
        candidate_df = candidate_df.sort_values(
            ["score", "symbol"],
            ascending=[ascending, True],
            kind="stable",
        ).reset_index(drop=True)
        candidate_df["rank"] = range(1, len(candidate_df) + 1)
        candidate_df["selected_flag"] = candidate_df["rank"] <= strategy.target_count

        rankings.extend(
            {
                "run_id": strategy.run_id,
                "strategy_id": strategy.strategy_id,
                "rebalance_date": rebalance_date,
                "asset_id": int(row.asset_id),
                "symbol": str(row.symbol),
                "score": float(row.score),
                "rank": int(row.rank),
                "selected_flag": bool(row.selected_flag),
                "asof_ts": asof_ts,
            }
            for row in candidate_df.itertuples(index=False)
        )

    return rankings


def _current_run_ids(strategies: list[StrategyConfig]) -> list[str]:
    return [strategy.run_id for strategy in strategies]


def _delete_rows_for_run_ids(con, table_name: str, run_ids: list[str]) -> None:
    if not run_ids:
        return
    con.execute(
        f"DELETE FROM gold.{_quote_identifier(table_name)} WHERE run_id = ANY(?)",
        [run_ids],
    )


def _register_temp_df(con, name: str, df: pd.DataFrame) -> None:
    try:
        con.unregister(name)
    except Exception:
        pass
    con.register(name, df)


def _temp_table_exists(con, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {_quote_identifier(table_name)} LIMIT 0")
    except Exception:
        return False
    return True


def _strategy_rebalance_plan_records(
    con,
    strategies: list[StrategyConfig],
) -> list[dict[str, Any]]:
    ranking_methods = {
        strategy.strategy_id: _ranking_method_for_strategy(con, strategy.strategy_id)
        for strategy in strategies
    }
    records: list[dict[str, Any]] = []
    for strategy in strategies:
        ranking_method = ranking_methods.get(strategy.strategy_id, "")
        for rebalance_date in _rebalance_dates_for_strategy(con, strategy):
            parameters = _active_parameters_by_date(
                con,
                strategy.strategy_id,
                rebalance_date,
            )
            records.append(
                {
                    "run_id": strategy.run_id,
                    "strategy_id": strategy.strategy_id,
                    "rebalance_date": rebalance_date,
                    "ranking_method": ranking_method,
                    "benchmark_symbol": strategy.benchmark_symbol.strip().upper(),
                    "target_count": strategy.target_count,
                    "weighting_method": strategy.weighting_method.strip().lower(),
                    "long_short_flag": strategy.long_short_flag,
                    "signal_column": str(parameters.get("signal_column") or "momentum_12_1")
                    .strip()
                    .lower(),
                    "secondary_signal_column": str(parameters.get("secondary_signal_column") or "")
                    .strip()
                    .lower(),
                    "ranking_direction": str(parameters.get("ranking_direction") or "desc")
                    .strip()
                    .lower(),
                    "score_method": str(parameters.get("score_method") or "").strip().lower(),
                    "universe_name": str(strategy.config.get("universe") or "").strip().lower(),
                    "selection_mode": str(strategy.config.get("selection_mode") or "")
                    .strip()
                    .lower(),
                    "fixed_symbol": str(parameters.get("symbol") or strategy.benchmark_symbol)
                    .strip()
                    .upper(),
                    "min_avg_dollar_volume_21d": parameters.get("min_avg_dollar_volume_21d"),
                    "min_price_to_sma_200": parameters.get("min_price_to_sma_200"),
                    "min_momentum_12_1": parameters.get("min_momentum_12_1"),
                    "max_pct_below_52w_high": parameters.get("max_pct_below_52w_high"),
                    "random_seed": int(parameters.get("random_seed") or 0),
                }
            )
    return records


def _ensure_strategy_rebalance_plan(
    con,
    strategies: list[StrategyConfig],
) -> int:
    columns = [
        "run_id",
        "strategy_id",
        "rebalance_date",
        "ranking_method",
        "benchmark_symbol",
        "target_count",
        "weighting_method",
        "long_short_flag",
        "signal_column",
        "secondary_signal_column",
        "ranking_direction",
        "score_method",
        "universe_name",
        "selection_mode",
        "fixed_symbol",
        "min_avg_dollar_volume_21d",
        "min_price_to_sma_200",
        "min_momentum_12_1",
        "max_pct_below_52w_high",
        "random_seed",
    ]
    plan_df = pd.DataFrame(_strategy_rebalance_plan_records(con, strategies), columns=columns)
    _register_temp_df(con, "strategy_rebalance_plan_df", plan_df)
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_rebalance_plan AS
        SELECT
            CAST(run_id AS VARCHAR) AS run_id,
            CAST(strategy_id AS VARCHAR) AS strategy_id,
            CAST(rebalance_date AS DATE) AS rebalance_date,
            CAST(ranking_method AS VARCHAR) AS ranking_method,
            CAST(benchmark_symbol AS VARCHAR) AS benchmark_symbol,
            CAST(target_count AS INTEGER) AS target_count,
            CAST(weighting_method AS VARCHAR) AS weighting_method,
            CAST(long_short_flag AS BOOLEAN) AS long_short_flag,
            CAST(signal_column AS VARCHAR) AS signal_column,
            CAST(secondary_signal_column AS VARCHAR) AS secondary_signal_column,
            CAST(ranking_direction AS VARCHAR) AS ranking_direction,
            CAST(score_method AS VARCHAR) AS score_method,
            CAST(universe_name AS VARCHAR) AS universe_name,
            CAST(selection_mode AS VARCHAR) AS selection_mode,
            CAST(fixed_symbol AS VARCHAR) AS fixed_symbol,
            CAST(min_avg_dollar_volume_21d AS DOUBLE) AS min_avg_dollar_volume_21d,
            CAST(min_price_to_sma_200 AS DOUBLE) AS min_price_to_sma_200,
            CAST(min_momentum_12_1 AS DOUBLE) AS min_momentum_12_1,
            CAST(max_pct_below_52w_high AS DOUBLE) AS max_pct_below_52w_high,
            CAST(random_seed AS INTEGER) AS random_seed
        FROM strategy_rebalance_plan_df
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_expected_rebalance_dates AS
        SELECT DISTINCT run_id, strategy_id, rebalance_date
        FROM temp_strategy_rebalance_plan
        """
    )
    return int(len(plan_df))


def _expected_rebalance_dates_for_strategy(
    con,
    strategy: StrategyConfig,
) -> list[date]:
    if _temp_table_exists(con, "temp_strategy_expected_rebalance_dates"):
        rows = con.execute(
            """
            SELECT rebalance_date
            FROM temp_strategy_expected_rebalance_dates
            WHERE run_id = ?
              AND strategy_id = ?
            ORDER BY rebalance_date
            """,
            [strategy.run_id, strategy.strategy_id],
        ).fetchall()
        return [row[0] for row in rows if row[0] is not None]
    return _rebalance_dates_for_strategy(con, strategy)


def _purge_non_persistent_run_rows(con) -> None:
    if not _table_exists(con, "silver", "strategy_runs"):
        return

    purge_tables = [
        "strategy_rankings",
        "strategy_holdings",
        "strategy_returns",
        "strategy_performance",
    ]
    for table_name in purge_tables:
        if not _table_exists(con, "gold", table_name):
            continue
        con.execute(
            f"""
            DELETE FROM gold.{_quote_identifier(table_name)}
            WHERE run_id IN (
                SELECT run_id
                FROM silver.strategy_runs
                WHERE persist = FALSE
            )
            """
        )


def _ensure_strategy_run_rows(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
    run_status: str,
) -> None:
    if not strategies:
        return

    dataset_version = con.execute(
        """
        SELECT cast(max(date) AS VARCHAR)
        FROM silver.signals_daily
        """
    ).fetchone()[0]
    run_rows = pd.DataFrame(
        [
            {
                "run_id": strategy.run_id,
                "strategy_id": strategy.strategy_id,
                "run_type_id": "backtest",
                "simulation_type_id": None,
                "run_status": run_status,
                "dataset_version": dataset_version,
                "code_version": None,
                "started_at": asof_ts,
                "completed_at": None,
                "error_message": None,
                "rankings_row_count": None,
                "holdings_row_count": None,
                "returns_row_count": None,
                "performance_row_count": None,
                "persist": True,
                "asof_ts": asof_ts,
            }
            for strategy in strategies
        ]
    )
    _register_temp_df(con, "strategy_run_seed_df", run_rows)
    target_columns = [column_name for column_name, _ in STRATEGY_RUNS_COLUMNS]
    quoted_target_columns = ", ".join(_quote_identifier(column) for column in target_columns)
    projected_seed_columns = ", ".join(
        f"seed.{_quote_identifier(column)}" for column in target_columns
    )
    con.execute(
        f"""
        INSERT INTO silver.strategy_runs
        ({quoted_target_columns})
        SELECT {projected_seed_columns}
        FROM strategy_run_seed_df
        AS seed
        WHERE NOT EXISTS (
            SELECT 1
            FROM silver.strategy_runs AS existing
            WHERE existing.run_id = seed.run_id
        )
        """
    )
    con.execute(
        """
        UPDATE silver.strategy_runs AS target
        SET
            run_status = seed.run_status,
            run_type_id = coalesce(target.run_type_id, seed.run_type_id),
            dataset_version = seed.dataset_version,
            started_at = coalesce(target.started_at, seed.started_at),
            asof_ts = seed.asof_ts,
            persist = coalesce(target.persist, TRUE)
        FROM strategy_run_seed_df AS seed
        WHERE target.run_id = seed.run_id
        """
    )


def _update_strategy_run_row_counts(
    con,
    strategies: list[StrategyConfig],
    *,
    source_table: str,
    target_column: str,
    asof_ts: datetime,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return

    rows = con.execute(
        f"""
        SELECT run_id, count(*) AS row_count
        FROM gold.{_quote_identifier(source_table)}
        WHERE run_id = ANY(?)
        GROUP BY run_id
        """,
        [run_ids],
    ).fetchall()
    row_counts_by_run_id = {str(run_id): int(row_count or 0) for run_id, row_count in rows}
    count_rows = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "row_count": row_counts_by_run_id.get(run_id, 0),
                "asof_ts": asof_ts,
            }
            for run_id in run_ids
        ]
    )
    _register_temp_df(con, "strategy_run_count_df", count_rows)
    con.execute(
        f"""
        UPDATE silver.strategy_runs AS target
        SET
            {_quote_identifier(target_column)} = seed.row_count,
            asof_ts = seed.asof_ts
        FROM strategy_run_count_df AS seed
        WHERE target.run_id = seed.run_id
        """
    )


def _update_strategy_runs_success(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return
    con.execute(
        """
        UPDATE silver.strategy_runs
        SET
            run_status = 'success',
            completed_at = ?,
            error_message = NULL,
            asof_ts = ?
        WHERE run_id = ANY(?)
        """,
        [asof_ts, asof_ts, run_ids],
    )


def _update_strategy_runs_failure(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
    error_message: str,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return
    con.execute(
        """
        UPDATE silver.strategy_runs
        SET
            run_status = 'failed',
            completed_at = ?,
            error_message = ?,
            asof_ts = ?
        WHERE run_id = ANY(?)
        """,
        [asof_ts, error_message, asof_ts, run_ids],
    )


def _log_dq_count_check(
    *,
    observability_con,
    check_name: str,
    measured_value: float,
    details: dict[str, Any],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
    severity: str = "RED",
) -> None:
    write_dq_log(
        con=observability_con,
        check_name=check_name,
        severity=severity,
        status="PASS" if measured_value == 0.0 else "FAIL",
        measured_value=measured_value,
        threshold_value=0.0,
        details=details,
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def _log_skipped_dq_check(
    *,
    observability_con,
    check_name: str,
    details: dict[str, Any],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    write_dq_log(
        con=observability_con,
        check_name=check_name,
        severity="YELLOW",
        status="SKIPPED",
        measured_value=None,
        threshold_value=None,
        details=details,
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def _log_strategy_run_contract_checks(
    *,
    measured_con,
    observability_con,
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    if not _table_exists(measured_con, "silver", "strategy_runs"):
        return

    duplicate_rows = measured_con.execute(
        """
        SELECT run_id, count(*) AS row_count
        FROM silver.strategy_runs
        WHERE run_id IS NOT NULL
        GROUP BY run_id
        HAVING count(*) > 1
        ORDER BY run_id
        """
    ).fetchall()
    duplicate_run_ids = [
        {"run_id": str(row[0]), "row_count": int(row[1] or 0)} for row in duplicate_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_unique_run_id",
        measured_value=float(sum(row["row_count"] - 1 for row in duplicate_run_ids)),
        details={
            "table": "silver.strategy_runs",
            "duplicate_run_ids": duplicate_run_ids,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    if not _table_exists(measured_con, "ref", "run_types"):
        _log_skipped_dq_check(
            observability_con=observability_con,
            check_name="dq_silver_strategy_runs_valid_run_type",
            details={
                "table": "silver.strategy_runs",
                "required_reference_table": "ref.run_types",
            },
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
        )
    else:
        invalid_run_type_rows = measured_con.execute(
            """
            SELECT sr.run_id, sr.run_type_id
            FROM silver.strategy_runs AS sr
            LEFT JOIN ref.run_types AS rt
                ON lower(trim(sr.run_type_id)) = lower(trim(rt.run_type_code))
               AND rt.is_active = TRUE
            WHERE sr.run_type_id IS NULL
               OR trim(sr.run_type_id) = ''
               OR rt.run_type_code IS NULL
            ORDER BY sr.run_id
            """
        ).fetchall()
        invalid_run_types = [
            {"run_id": str(row[0]), "run_type_id": None if row[1] is None else str(row[1])}
            for row in invalid_run_type_rows
        ]
        _log_dq_count_check(
            observability_con=observability_con,
            check_name="dq_silver_strategy_runs_valid_run_type",
            measured_value=float(len(invalid_run_types)),
            details={
                "table": "silver.strategy_runs",
                "reference_table": "ref.run_types",
                "invalid_run_types": invalid_run_types,
            },
            run_id=run_id,
            job_name=job_name,
            partition_key=partition_key,
        )

    requirement_rows = measured_con.execute(
        """
        SELECT run_id, run_type_id, simulation_type_id
        FROM silver.strategy_runs
        WHERE (
                lower(trim(coalesce(run_type_id, ''))) = 'simulation'
                AND simulation_type_id IS NULL
              )
           OR (
                lower(trim(coalesce(run_type_id, ''))) <> 'simulation'
                AND simulation_type_id IS NOT NULL
              )
        ORDER BY run_id
        """
    ).fetchall()
    requirement_failures = [
        {
            "run_id": str(row[0]),
            "run_type_id": None if row[1] is None else str(row[1]),
            "simulation_type_id": None if row[2] is None else int(row[2]),
        }
        for row in requirement_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_simulation_type_required_scope",
        measured_value=float(len(requirement_failures)),
        details={
            "table": "silver.strategy_runs",
            "rule": "simulation_type_id is required for simulation runs and null otherwise",
            "failing_runs": requirement_failures,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    active_status_completed_rows = measured_con.execute(
        """
        SELECT run_id, run_type_id, run_status, completed_at
        FROM silver.strategy_runs
        WHERE lower(trim(coalesce(run_status, ''))) IN ('pending', 'running')
          AND completed_at IS NOT NULL
        ORDER BY run_id
        """
    ).fetchall()
    active_status_completed_runs = [
        {
            "run_id": str(row[0]),
            "run_type_id": None if row[1] is None else str(row[1]),
            "run_status": None if row[2] is None else str(row[2]),
            "completed_at": None if row[3] is None else str(row[3]),
        }
        for row in active_status_completed_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_active_status_has_completed_at",
        measured_value=float(len(active_status_completed_runs)),
        details={
            "table": "silver.strategy_runs",
            "rule": "pending/running runs must not have completed_at populated",
            "failing_runs": active_status_completed_runs,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    active_status_error_rows = measured_con.execute(
        """
        SELECT run_id, run_type_id, run_status, error_message
        FROM silver.strategy_runs
        WHERE lower(trim(coalesce(run_status, ''))) IN ('pending', 'running')
          AND nullif(trim(coalesce(error_message, '')), '') IS NOT NULL
        ORDER BY run_id
        """
    ).fetchall()
    active_status_error_runs = [
        {
            "run_id": str(row[0]),
            "run_type_id": None if row[1] is None else str(row[1]),
            "run_status": None if row[2] is None else str(row[2]),
            "error_message": None if row[3] is None else str(row[3]),
        }
        for row in active_status_error_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_active_status_has_error",
        measured_value=float(len(active_status_error_runs)),
        details={
            "table": "silver.strategy_runs",
            "rule": "pending/running runs must not have error_message populated",
            "failing_runs": active_status_error_runs,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    pending_backtest_performance_runs: list[dict[str, Any]] = []
    if _table_exists(measured_con, "gold", "strategy_performance"):
        pending_backtest_performance_rows = measured_con.execute(
            """
            SELECT sr.run_id, sr.run_status, count(*) AS performance_row_count
            FROM silver.strategy_runs AS sr
            INNER JOIN gold.strategy_performance AS gp
                ON gp.run_id = sr.run_id
            WHERE lower(trim(coalesce(sr.run_type_id, ''))) = 'backtest'
              AND lower(trim(coalesce(sr.run_status, ''))) IN ('pending', 'running')
            GROUP BY sr.run_id, sr.run_status
            ORDER BY sr.run_id
            """
        ).fetchall()
        pending_backtest_performance_runs = [
            {
                "run_id": str(row[0]),
                "run_status": None if row[1] is None else str(row[1]),
                "performance_row_count": int(row[2] or 0),
            }
            for row in pending_backtest_performance_rows
        ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_pending_backtests_have_performance_outputs",
        measured_value=float(len(pending_backtest_performance_runs)),
        details={
            "table": "silver.strategy_runs",
            "result_table": "gold.strategy_performance",
            "rule": "pending/running backtest runs must not already have performance rows",
            "failing_runs": pending_backtest_performance_runs,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    success_missing_performance_runs: list[dict[str, Any]]
    if _table_exists(measured_con, "gold", "strategy_performance"):
        success_missing_performance_rows = measured_con.execute(
            """
            SELECT sr.run_id, sr.run_type_id, sr.run_status, coalesce(sr.persist, TRUE) AS persist
            FROM silver.strategy_runs AS sr
            LEFT JOIN gold.strategy_performance AS gp
                ON gp.run_id = sr.run_id
            WHERE lower(trim(coalesce(sr.run_status, ''))) = 'success'
              AND lower(trim(coalesce(sr.run_type_id, ''))) IN ('backtest', 'simulation')
              AND coalesce(sr.persist, TRUE) = TRUE
              AND gp.run_id IS NULL
            ORDER BY sr.run_id
            """
        ).fetchall()
    else:
        success_missing_performance_rows = measured_con.execute(
            """
            SELECT run_id, run_type_id, run_status, coalesce(persist, TRUE) AS persist
            FROM silver.strategy_runs
            WHERE lower(trim(coalesce(run_status, ''))) = 'success'
              AND lower(trim(coalesce(run_type_id, ''))) IN ('backtest', 'simulation')
              AND coalesce(persist, TRUE) = TRUE
            ORDER BY run_id
            """
        ).fetchall()
    success_missing_performance_runs = [
        {
            "run_id": str(row[0]),
            "run_type_id": None if row[1] is None else str(row[1]),
            "run_status": None if row[2] is None else str(row[2]),
            "persist": bool(row[3]),
        }
        for row in success_missing_performance_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_success_runs_have_performance",
        measured_value=float(len(success_missing_performance_runs)),
        details={
            "table": "silver.strategy_runs",
            "result_table": "gold.strategy_performance",
            "rule": "persisted successful backtest/simulation runs must have performance rows",
            "failing_runs": success_missing_performance_runs,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    if not _table_exists(measured_con, "ref", "simulation_types"):
        for check_name in (
            "dq_silver_strategy_runs_simulation_type_exists",
            "dq_silver_strategy_runs_simulation_type_active",
            "dq_silver_strategy_runs_reportable_simulations_lookahead_safe",
        ):
            _log_skipped_dq_check(
                observability_con=observability_con,
                check_name=check_name,
                details={
                    "table": "silver.strategy_runs",
                    "required_reference_table": "ref.simulation_types",
                },
                run_id=run_id,
                job_name=job_name,
                partition_key=partition_key,
            )
        return

    missing_type_rows = measured_con.execute(
        """
        SELECT sr.run_id, sr.simulation_type_id
        FROM silver.strategy_runs AS sr
        LEFT JOIN ref.simulation_types AS st
            ON sr.simulation_type_id = st.simulation_type_id
        WHERE lower(trim(coalesce(sr.run_type_id, ''))) = 'simulation'
          AND sr.simulation_type_id IS NOT NULL
          AND st.simulation_type_id IS NULL
        ORDER BY sr.run_id
        """
    ).fetchall()
    missing_types = [
        {"run_id": str(row[0]), "simulation_type_id": int(row[1])} for row in missing_type_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_simulation_type_exists",
        measured_value=float(len(missing_types)),
        details={
            "table": "silver.strategy_runs",
            "reference_table": "ref.simulation_types",
            "failing_runs": missing_types,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    inactive_type_rows = measured_con.execute(
        """
        SELECT
            sr.run_id,
            sr.simulation_type_id,
            st.simulation_type_code,
            st.is_active
        FROM silver.strategy_runs AS sr
        INNER JOIN ref.simulation_types AS st
            ON sr.simulation_type_id = st.simulation_type_id
        WHERE lower(trim(coalesce(sr.run_type_id, ''))) = 'simulation'
          AND coalesce(st.is_active, FALSE) = FALSE
        ORDER BY sr.run_id
        """
    ).fetchall()
    inactive_types = [
        {
            "run_id": str(row[0]),
            "simulation_type_id": int(row[1]),
            "simulation_type_code": None if row[2] is None else str(row[2]),
            "is_active": bool(row[3]),
        }
        for row in inactive_type_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_simulation_type_active",
        measured_value=float(len(inactive_types)),
        details={
            "table": "silver.strategy_runs",
            "reference_table": "ref.simulation_types",
            "failing_runs": inactive_types,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    unsafe_rows = measured_con.execute(
        """
        SELECT
            sr.run_id,
            sr.simulation_type_id,
            st.simulation_type_code,
            st.lookahead_safe_flag
        FROM silver.strategy_runs AS sr
        INNER JOIN ref.simulation_types AS st
            ON sr.simulation_type_id = st.simulation_type_id
        WHERE lower(trim(coalesce(sr.run_type_id, ''))) = 'simulation'
          AND coalesce(sr.persist, TRUE) = TRUE
          AND coalesce(st.lookahead_safe_flag, FALSE) = FALSE
        ORDER BY sr.run_id
        """
    ).fetchall()
    unsafe_reportable_runs = [
        {
            "run_id": str(row[0]),
            "simulation_type_id": int(row[1]),
            "simulation_type_code": None if row[2] is None else str(row[2]),
            "lookahead_safe_flag": bool(row[3]),
        }
        for row in unsafe_rows
    ]
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_silver_strategy_runs_reportable_simulations_lookahead_safe",
        measured_value=float(len(unsafe_reportable_runs)),
        details={
            "table": "silver.strategy_runs",
            "reference_table": "ref.simulation_types",
            "rule": "persisted simulation runs must use lookahead-safe simulation types",
            "failing_runs": unsafe_reportable_runs,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )


def _log_simulation_result_checks(
    *,
    measured_con,
    observability_con,
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    if not _table_exists(measured_con, "silver", "strategy_runs"):
        return

    result_tables = [
        ("gold.strategy_rankings", "strategy_rankings", "rebalance_date"),
        ("gold.strategy_holdings", "strategy_holdings", "rebalance_date"),
        ("gold.strategy_returns", "strategy_returns", "date"),
    ]

    future_results: list[dict[str, Any]] = []
    for table_label, table_name, date_column in result_tables:
        if not _table_exists(measured_con, "gold", table_name):
            continue
        rows = measured_con.execute(
            f"""
            SELECT
                '{table_label}' AS table_name,
                sr.run_id,
                r.strategy_id,
                CAST(r.{_quote_identifier(date_column)} AS DATE) AS result_date
            FROM gold.{_quote_identifier(table_name)} AS r
            INNER JOIN silver.strategy_runs AS sr
                ON r.run_id = sr.run_id
            WHERE lower(trim(coalesce(sr.run_type_id, ''))) = 'simulation'
              AND CAST(r.{_quote_identifier(date_column)} AS DATE) > current_date
            ORDER BY table_name, sr.run_id, result_date
            """
        ).fetchall()
        future_results.extend(
            {
                "table": str(row[0]),
                "run_id": str(row[1]),
                "strategy_id": str(row[2]),
                "result_date": str(row[3]),
            }
            for row in rows
        )
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_gold_strategy_simulation_results_no_future_dates",
        measured_value=float(len(future_results)),
        details={
            "tables": [table_label for table_label, _, _ in result_tables],
            "failing_rows": future_results,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    failed_run_outputs: list[dict[str, Any]] = []
    for table_label, table_name in (
        ("gold.strategy_holdings", "strategy_holdings"),
        ("gold.strategy_returns", "strategy_returns"),
    ):
        if not _table_exists(measured_con, "gold", table_name):
            continue
        rows = measured_con.execute(
            f"""
            SELECT
                '{table_label}' AS table_name,
                sr.run_id,
                r.strategy_id,
                count(*) AS row_count
            FROM gold.{_quote_identifier(table_name)} AS r
            INNER JOIN silver.strategy_runs AS sr
                ON r.run_id = sr.run_id
            WHERE lower(trim(coalesce(sr.run_status, ''))) = 'failed'
            GROUP BY sr.run_id, r.strategy_id
            ORDER BY table_name, sr.run_id, r.strategy_id
            """
        ).fetchall()
        failed_run_outputs.extend(
            {
                "table": str(row[0]),
                "run_id": str(row[1]),
                "strategy_id": str(row[2]),
                "row_count": int(row[3] or 0),
            }
            for row in rows
        )
    _log_dq_count_check(
        observability_con=observability_con,
        check_name="dq_gold_strategy_failed_runs_no_holdings_or_returns",
        measured_value=float(sum(row["row_count"] for row in failed_run_outputs)),
        details={
            "tables": ["gold.strategy_holdings", "gold.strategy_returns"],
            "failing_groups": failed_run_outputs,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )


def _materialize_rankings(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
) -> int:
    _ensure_table_contract(
        con,
        schema="gold",
        table="strategy_rankings",
        columns=STRATEGY_RANKINGS_COLUMNS,
    )
    _purge_non_persistent_run_rows(con)
    run_ids = _current_run_ids(strategies)
    _delete_rows_for_run_ids(con, "strategy_rankings", run_ids)
    if not strategies:
        return 0

    _ensure_strategy_rebalance_plan(con, strategies)
    if (con.execute("SELECT count(*) FROM temp_strategy_rebalance_plan").fetchone()[0] or 0) == 0:
        return 0

    con.execute(
        """
        INSERT INTO gold.strategy_rankings
        SELECT
            p.run_id,
            p.strategy_id,
            p.rebalance_date,
            CAST(s.asset_id AS BIGINT) AS asset_id,
            upper(trim(s.symbol)) AS symbol,
            1.0 AS score,
            1 AS rank,
            TRUE AS selected_flag,
            ? AS asof_ts
        FROM temp_strategy_rebalance_plan AS p
        INNER JOIN silver.signals_daily AS s
            ON CAST(s.date AS DATE) = p.rebalance_date
           AND upper(trim(s.symbol)) = p.fixed_symbol
           AND s.asset_id IS NOT NULL
        WHERE p.selection_mode = 'fixed_symbol'
           OR p.universe_name = 'benchmark_only'
        """,
        [asof_ts],
    )

    ranking_groups = con.execute(
        """
        SELECT DISTINCT
            signal_column,
            secondary_signal_column,
            score_method,
            ranking_direction
        FROM temp_strategy_rebalance_plan
        WHERE ranking_method <> 'random_selection'
          AND selection_mode <> 'fixed_symbol'
          AND universe_name <> 'benchmark_only'
        ORDER BY signal_column, secondary_signal_column, score_method, ranking_direction
        """
    ).fetchall()
    eligibility_join_sql = _candidate_eligibility_join_sql(con)
    signal_table_columns = _table_columns(con, "silver", "signals_daily")
    filter_sql_by_column = {
        "avg_dollar_volume_21d": (
            """
                  AND (
                        p.min_avg_dollar_volume_21d IS NULL
                        OR CAST(s.avg_dollar_volume_21d AS DOUBLE)
                           >= p.min_avg_dollar_volume_21d
                  )
            """
            if "avg_dollar_volume_21d" in signal_table_columns
            else " AND p.min_avg_dollar_volume_21d IS NULL"
        ),
        "price_to_sma_200": (
            """
                  AND (
                        p.min_price_to_sma_200 IS NULL
                        OR CAST(s.price_to_sma_200 AS DOUBLE) >= p.min_price_to_sma_200
                  )
            """
            if "price_to_sma_200" in signal_table_columns
            else " AND p.min_price_to_sma_200 IS NULL"
        ),
        "momentum_12_1": (
            """
                  AND (
                        p.min_momentum_12_1 IS NULL
                        OR CAST(s.momentum_12_1 AS DOUBLE) > p.min_momentum_12_1
                  )
            """
            if "momentum_12_1" in signal_table_columns
            else " AND p.min_momentum_12_1 IS NULL"
        ),
        "pct_below_52w_high": (
            """
                  AND (
                        p.max_pct_below_52w_high IS NULL
                        OR CAST(s.pct_below_52w_high AS DOUBLE)
                           <= p.max_pct_below_52w_high
                  )
            """
            if "pct_below_52w_high" in signal_table_columns
            else " AND p.max_pct_below_52w_high IS NULL"
        ),
    }
    for signal_column, secondary_signal_column, score_method, ranking_direction in ranking_groups:
        signal_identifier = _quote_identifier(str(signal_column))
        secondary_signal = str(secondary_signal_column or "")
        secondary_identifier = _quote_identifier(secondary_signal) if secondary_signal else ""
        secondary_select_sql = (
            f", CAST(s.{secondary_identifier} AS DOUBLE) AS secondary_score"
            if secondary_signal
            else ", NULL::DOUBLE AS secondary_score"
        )
        secondary_not_null_sql = (
            f"AND CAST(s.{secondary_identifier} AS DOUBLE) IS NOT NULL" if secondary_signal else ""
        )
        ratio_filter_sql = (
            "WHERE secondary_score IS NOT NULL AND secondary_score > 0"
            if secondary_signal and str(score_method) == "ratio"
            else ""
        )
        if secondary_signal and str(score_method) == "zscore_sum":
            score_sql = """
                (
                    CASE
                        WHEN stddev_pop(primary_score) OVER score_window IS NULL
                          OR stddev_pop(primary_score) OVER score_window = 0
                        THEN 0.0
                        ELSE (
                            primary_score - avg(primary_score) OVER score_window
                        ) / stddev_pop(primary_score) OVER score_window
                    END
                    +
                    CASE
                        WHEN stddev_pop(secondary_score) OVER score_window IS NULL
                          OR stddev_pop(secondary_score) OVER score_window = 0
                        THEN 0.0
                        ELSE (
                            secondary_score - avg(secondary_score) OVER score_window
                        ) / stddev_pop(secondary_score) OVER score_window
                    END
                )
            """
        elif secondary_signal and str(score_method) == "ratio":
            score_sql = "primary_score / secondary_score"
        else:
            score_sql = "primary_score"

        rank_direction_sql = "ASC" if str(ranking_direction).lower() == "asc" else "DESC"
        con.execute(
            f"""
            INSERT INTO gold.strategy_rankings
            WITH candidates AS (
                SELECT
                    p.run_id,
                    p.strategy_id,
                    p.rebalance_date,
                    p.target_count,
                    CAST(s.asset_id AS BIGINT) AS asset_id,
                    upper(trim(s.symbol)) AS symbol,
                    CAST(s.{signal_identifier} AS DOUBLE) AS primary_score
                    {secondary_select_sql}
                FROM temp_strategy_rebalance_plan AS p
                INNER JOIN silver.signals_daily AS s
                    ON CAST(s.date AS DATE) = p.rebalance_date
                INNER JOIN silver.universe_membership_daily AS u
                    ON CAST(u.member_date AS DATE) = CAST(s.date AS DATE)
                   AND CAST(u.asset_id AS BIGINT) = CAST(s.asset_id AS BIGINT)
                {eligibility_join_sql}
                WHERE p.signal_column = ?
                  AND p.secondary_signal_column = ?
                  AND p.score_method = ?
                  AND p.ranking_direction = ?
                  AND p.ranking_method <> 'random_selection'
                  AND p.selection_mode <> 'fixed_symbol'
                  AND p.universe_name <> 'benchmark_only'
                  AND s.asset_id IS NOT NULL
                  AND CAST(s.{signal_identifier} AS DOUBLE) IS NOT NULL
                  {secondary_not_null_sql}
                  {filter_sql_by_column["avg_dollar_volume_21d"]}
                  {filter_sql_by_column["price_to_sma_200"]}
                  {filter_sql_by_column["momentum_12_1"]}
                  {filter_sql_by_column["pct_below_52w_high"]}
            ),
            filtered AS (
                SELECT *
                FROM candidates
                {ratio_filter_sql}
            ),
            scored AS (
                SELECT
                    run_id,
                    strategy_id,
                    rebalance_date,
                    target_count,
                    asset_id,
                    symbol,
                    {score_sql} AS score
                FROM filtered
                WINDOW score_window AS (PARTITION BY run_id, rebalance_date)
            ),
            ranked AS (
                SELECT
                    run_id,
                    strategy_id,
                    rebalance_date,
                    asset_id,
                    symbol,
                    score,
                    row_number() OVER (
                        PARTITION BY run_id, rebalance_date
                        ORDER BY score {rank_direction_sql}, asset_id ASC, symbol ASC
                    ) AS rank,
                    target_count
                FROM scored
            )
            SELECT
                run_id,
                strategy_id,
                rebalance_date,
                asset_id,
                symbol,
                score,
                CAST(rank AS INTEGER) AS rank,
                rank <= target_count AS selected_flag,
                ? AS asof_ts
            FROM ranked
            """,
            [
                signal_column,
                secondary_signal_column,
                score_method,
                ranking_direction,
                asof_ts,
            ],
        )

    random_candidate_rows = con.execute(
        f"""
        SELECT
            p.run_id,
            p.strategy_id,
            p.rebalance_date,
            p.target_count,
            p.random_seed,
            CAST(s.asset_id AS BIGINT) AS asset_id,
            upper(trim(s.symbol)) AS symbol
        FROM temp_strategy_rebalance_plan AS p
        INNER JOIN silver.signals_daily AS s
            ON CAST(s.date AS DATE) = p.rebalance_date
        INNER JOIN silver.universe_membership_daily AS u
            ON CAST(u.member_date AS DATE) = CAST(s.date AS DATE)
           AND CAST(u.asset_id AS BIGINT) = CAST(s.asset_id AS BIGINT)
        {eligibility_join_sql}
        WHERE p.ranking_method = 'random_selection'
          AND s.asset_id IS NOT NULL
          {filter_sql_by_column["avg_dollar_volume_21d"]}
        ORDER BY p.run_id, p.rebalance_date, symbol
        """
    ).fetchall()
    if random_candidate_rows:
        random_df = pd.DataFrame(
            random_candidate_rows,
            columns=[
                "run_id",
                "strategy_id",
                "rebalance_date",
                "target_count",
                "random_seed",
                "asset_id",
                "symbol",
            ],
        )
        random_df["score"] = random_df.apply(
            lambda row: (
                int.from_bytes(
                    hashlib.sha256(
                        (
                            f"{row['strategy_id']}|"
                            f"{pd.Timestamp(row['rebalance_date']).date().isoformat()}|"
                            f"{int(row['random_seed'])}|{row['symbol']}"
                        ).encode("utf-8")
                    ).digest()[:8],
                    byteorder="big",
                    signed=False,
                )
                / float(2**64 - 1)
            ),
            axis=1,
        )
        random_df = random_df.sort_values(
            ["run_id", "rebalance_date", "score", "symbol"],
            ascending=[True, True, False, True],
            kind="stable",
        )
        random_df["rank"] = (
            random_df.groupby(["run_id", "rebalance_date"], sort=False).cumcount() + 1
        )
        random_df["selected_flag"] = random_df["rank"] <= random_df["target_count"]
        random_df["asof_ts"] = asof_ts
        random_df = random_df[
            [
                "run_id",
                "strategy_id",
                "rebalance_date",
                "asset_id",
                "symbol",
                "score",
                "rank",
                "selected_flag",
                "asof_ts",
            ]
        ]
        _register_temp_df(con, "strategy_random_rankings_df", random_df)
        con.execute(
            """
            INSERT INTO gold.strategy_rankings
            SELECT
                CAST(run_id AS VARCHAR),
                CAST(strategy_id AS VARCHAR),
                CAST(rebalance_date AS DATE),
                CAST(asset_id AS BIGINT),
                CAST(symbol AS VARCHAR),
                CAST(score AS DOUBLE),
                CAST(rank AS INTEGER),
                CAST(selected_flag AS BOOLEAN),
                CAST(asof_ts AS TIMESTAMP)
            FROM strategy_random_rankings_df
            """
        )

    row = con.execute(
        """
        SELECT count(*)
        FROM gold.strategy_rankings
        WHERE run_id = ANY(?)
        """,
        [run_ids],
    )
    return int(row.fetchone()[0] or 0)


def _materialize_holdings(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
) -> int:
    _ensure_table_contract(
        con,
        schema="gold",
        table="strategy_holdings",
        columns=STRATEGY_HOLDINGS_COLUMNS,
    )
    run_ids = _current_run_ids(strategies)
    _delete_rows_for_run_ids(con, "strategy_holdings", run_ids)
    if not strategies:
        return 0

    _ensure_strategy_rebalance_plan(con, strategies)
    unsupported_weighting = con.execute(
        """
        SELECT strategy_id, weighting_method
        FROM temp_strategy_rebalance_plan
        WHERE weighting_method <> 'equal'
        LIMIT 1
        """
    ).fetchone()
    if unsupported_weighting is not None:
        raise ValueError(
            "Unsupported weighting_method for "
            f"{unsupported_weighting[0]}: {unsupported_weighting[1]}"
        )

    con.execute(
        """
        INSERT INTO gold.strategy_holdings (
            "run_id",
            "strategy_id",
            "rebalance_date",
            "asset_id",
            "symbol",
            "target_weight",
            "side",
            "entry_rank",
            "signal_value",
            "asof_ts"
        )
        WITH selected_rankings AS (
            SELECT
                r.run_id,
                r.strategy_id,
                r.rebalance_date,
                r.asset_id,
                r.symbol,
                r.rank,
                r.score,
                count(*) OVER (
                    PARTITION BY r.run_id, r.rebalance_date
                ) AS holding_count
            FROM gold.strategy_rankings AS r
            WHERE r.run_id = ANY(?)
              AND r.selected_flag = TRUE
        )
        SELECT
            r.run_id,
            r.strategy_id,
            r.rebalance_date,
            r.asset_id,
            r.symbol,
            1.0 / r.holding_count AS target_weight,
            CASE WHEN p.long_short_flag THEN 'SHORT' ELSE 'LONG' END AS side,
            r.rank AS entry_rank,
            r.score AS signal_value,
            ? AS asof_ts
        FROM selected_rankings AS r
        INNER JOIN temp_strategy_rebalance_plan AS p
            ON p.run_id = r.run_id
           AND p.strategy_id = r.strategy_id
           AND p.rebalance_date = r.rebalance_date
        ORDER BY r.run_id, r.rebalance_date, r.rank, r.asset_id, r.symbol
        """,
        [run_ids, asof_ts],
    )
    row = con.execute(
        """
        SELECT count(*)
        FROM gold.strategy_holdings
        WHERE run_id = ANY(?)
        """,
        [run_ids],
    )
    return int(row.fetchone()[0] or 0)


def _log_holdings_weight_sum_check(
    *,
    measured_con,
    observability_con,
    strategies: list[StrategyConfig],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return

    failing_rows = measured_con.execute(
        """
        WITH weight_sums AS (
            SELECT
                run_id,
                strategy_id,
                rebalance_date,
                sum(coalesce(target_weight, 0.0)) AS weight_sum
            FROM gold.strategy_holdings
            WHERE run_id = ANY(?)
            GROUP BY run_id, strategy_id, rebalance_date
        )
        SELECT
            run_id,
            strategy_id,
            rebalance_date,
            weight_sum,
            abs(weight_sum - 1.0) AS abs_deviation
        FROM weight_sums
        WHERE abs(weight_sum - 1.0) > ?
        ORDER BY strategy_id, rebalance_date, run_id
        """,
        [run_ids, STRATEGY_HOLDINGS_WEIGHT_SUM_TOLERANCE],
    ).fetchall()

    failing_groups = [
        {
            "run_id": str(row[0]),
            "strategy_id": str(row[1]),
            "rebalance_date": str(row[2]),
            "weight_sum": float(row[3]),
            "abs_deviation": float(row[4]),
        }
        for row in failing_rows
    ]
    abs_deviations = [float(row[4]) for row in failing_rows]
    max_abs_deviation = max(abs_deviations, default=0.0)

    write_dq_log(
        con=observability_con,
        check_name="dq_gold_strategy_holdings_weight_sum_by_rebalance",
        severity="RED",
        status="PASS" if not failing_groups else "FAIL",
        measured_value=float(len(failing_groups)),
        threshold_value=0.0,
        details={
            "table": "gold.strategy_holdings",
            "run_ids": run_ids,
            "expected_weight_sum": 1.0,
            "tolerance": STRATEGY_HOLDINGS_WEIGHT_SUM_TOLERANCE,
            "failing_groups": failing_groups,
            "max_abs_deviation": max_abs_deviation,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def _log_holdings_duplicate_symbol_check(
    *,
    measured_con,
    observability_con,
    strategies: list[StrategyConfig],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return

    log_duplicate_row_check(
        measured_con=measured_con,
        observability_con=observability_con,
        check_name="dq_gold_strategy_holdings_unique_asset_id_per_rebalance",
        relation_sql="""
            SELECT
                run_id,
                strategy_id,
                rebalance_date,
                asset_id,
                symbol
            FROM gold.strategy_holdings
            WHERE run_id = ANY(?)
        """,
        relation_params=[run_ids],
        key_columns=["run_id", "strategy_id", "rebalance_date", "asset_id"],
        details={
            "table": "gold.strategy_holdings",
            "run_ids": run_ids,
            "uniqueness_scope": ["strategy_id", "rebalance_date", "asset_id"],
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )


def _log_rebalance_dates_present_check(
    *,
    measured_con,
    observability_con,
    strategies: list[StrategyConfig],
    table_name: str,
    check_name: str,
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return

    actual_rows = measured_con.execute(
        f"""
        SELECT DISTINCT strategy_id, rebalance_date
        FROM gold.{_quote_identifier(table_name)}
        WHERE run_id = ANY(?)
        ORDER BY strategy_id, rebalance_date
        """,
        [run_ids],
    ).fetchall()
    actual_dates_by_strategy: dict[str, set[date]] = {}
    for strategy_id, rebalance_date in actual_rows:
        if rebalance_date is None:
            continue
        actual_dates_by_strategy.setdefault(str(strategy_id), set()).add(rebalance_date)

    failing_strategies: list[dict[str, Any]] = []
    measured_value = 0.0
    for strategy in strategies:
        expected_dates = set(_expected_rebalance_dates_for_strategy(measured_con, strategy))
        actual_dates = actual_dates_by_strategy.get(strategy.strategy_id, set())
        missing_dates = sorted(expected_dates - actual_dates)
        unexpected_dates = sorted(actual_dates - expected_dates)
        measured_value += float(len(missing_dates) + len(unexpected_dates))
        if not missing_dates and not unexpected_dates:
            continue
        failing_strategies.append(
            {
                "run_id": strategy.run_id,
                "strategy_id": strategy.strategy_id,
                "rebalance_frequency": strategy.rebalance_frequency,
                "expected_rebalance_dates": [str(value) for value in sorted(expected_dates)],
                "actual_rebalance_dates": [str(value) for value in sorted(actual_dates)],
                "missing_rebalance_dates": [str(value) for value in missing_dates],
                "unexpected_rebalance_dates": [str(value) for value in unexpected_dates],
            }
        )

    write_dq_log(
        con=observability_con,
        check_name=check_name,
        severity="RED",
        status="PASS" if measured_value == 0.0 else "FAIL",
        measured_value=measured_value,
        threshold_value=0.0,
        details={
            "table": f"gold.{table_name}",
            "run_ids": run_ids,
            "evaluated_strategy_count": len(strategies),
            "failing_strategies": failing_strategies,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def _load_price_history(
    con,
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(
            columns=["trade_date", "symbol", "open", "close", "close_fill", "vwap", "price"]
        )
    schema_rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name = true)",
        [PRICE_GLOB],
    ).fetchall()
    available_columns = {str(row[0]).lower() for row in schema_rows}
    adjustment_ratio_expr = """
        CASE
            WHEN adjusted_close IS NOT NULL
             AND close IS NOT NULL
             AND CAST(close AS DOUBLE) > 0
            THEN CAST(adjusted_close AS DOUBLE) / CAST(close AS DOUBLE)
            ELSE 1.0
        END
    """
    open_expr = (
        f"CAST(open AS DOUBLE) * ({adjustment_ratio_expr})"
        if "open" in available_columns
        else "NULL::DOUBLE"
    )
    vwap_expr = (
        f"CAST(vwap AS DOUBLE) * ({adjustment_ratio_expr})"
        if "vwap" in available_columns
        else "NULL::DOUBLE"
    )
    price_df = con.execute(
        f"""
        SELECT
            CAST(trade_date AS DATE) AS trade_date,
            upper(trim(symbol)) AS symbol,
            {open_expr} AS open,
            CAST(close AS DOUBLE) AS close,
            CAST(coalesce(adjusted_close, close) AS DOUBLE) AS close_fill,
            {vwap_expr} AS vwap,
            CAST(coalesce(adjusted_close, close) AS DOUBLE) AS price
        FROM read_parquet(?, union_by_name = true)
        WHERE upper(trim(symbol)) = ANY(?)
          AND CAST(trade_date AS DATE) >= ?
          AND CAST(trade_date AS DATE) <= ?
          AND coalesce(adjusted_close, close) IS NOT NULL
        ORDER BY trade_date, symbol
        """,
        [PRICE_GLOB, symbols, start_date, end_date],
    ).fetch_df()
    return _filter_valid_research_trading_frame(con, price_df, "trade_date")


def _load_distinct_trading_dates(
    con,
    *,
    start_date: date,
    end_date: date,
    symbols: list[str] | None = None,
) -> list[date]:
    params: list[Any] = [PRICE_GLOB, start_date, end_date]
    sql = """
        SELECT DISTINCT CAST(trade_date AS DATE) AS trade_date
        FROM read_parquet(?)
        WHERE CAST(trade_date AS DATE) >= ?
          AND CAST(trade_date AS DATE) <= ?
    """
    if symbols:
        sql += "\n  AND upper(trim(symbol)) = ANY(?)"
        params.append(symbols)
    sql += "\nORDER BY trade_date"
    rows = con.execute(sql, params).fetchall()
    return _filter_valid_research_trading_days(con, [row[0] for row in rows])


def _daily_symbol_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame(columns=["trade_date", "symbol", "asset_return"])
    frame = price_df.sort_values(["symbol", "trade_date"], kind="stable").copy()
    frame["prev_price"] = frame.groupby("symbol")["price"].shift(1)
    valid_prices = (
        frame["price"].notna()
        & frame["prev_price"].notna()
        & frame["price"].gt(0)
        & frame["prev_price"].gt(0)
    )
    frame["asset_return"] = pd.NA
    frame.loc[valid_prices, "asset_return"] = (
        frame.loc[valid_prices, "price"] / frame.loc[valid_prices, "prev_price"]
    ) - 1.0
    extreme_mask = frame["asset_return"].notna() & (
        frame["asset_return"].abs() > MAX_ABS_DAILY_SECURITY_RETURN
    )
    frame.loc[extreme_mask, "asset_return"] = pd.NA
    return frame[["trade_date", "symbol", "asset_return"]]


def _fill_price_column(fill_price_basis: str) -> str:
    normalized_basis = fill_price_basis.strip().lower()
    if normalized_basis in {"open", "next_open"}:
        return "open"
    if normalized_basis == "vwap":
        return "vwap"
    return "close_fill"


def _simulation_trade_price_returns(
    price_df: pd.DataFrame,
    *,
    simulation_type: SimulationTypeConfig,
) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame(columns=["trade_date", "symbol", "asset_return"])

    frame = price_df.sort_values(["symbol", "trade_date"], kind="stable").copy()
    fill_column = _fill_price_column(simulation_type.fill_price_basis)
    frame["entry_price"] = pd.to_numeric(frame[fill_column], errors="coerce")
    frame["close_fill"] = pd.to_numeric(frame["close_fill"], errors="coerce")
    frame["prev_close"] = frame.groupby("symbol")["close_fill"].shift(1)

    same_session_fill = simulation_type.fill_price_basis == "close"
    entry_basis = frame["prev_close"] if same_session_fill else frame["entry_price"]
    valid_prices = (
        frame["close_fill"].notna()
        & entry_basis.notna()
        & frame["close_fill"].gt(0)
        & entry_basis.gt(0)
    )
    frame["asset_return"] = pd.NA
    frame.loc[valid_prices, "asset_return"] = (
        frame.loc[valid_prices, "close_fill"] / entry_basis.loc[valid_prices]
    ) - 1.0
    extreme_mask = frame["asset_return"].notna() & (
        frame["asset_return"].abs() > MAX_ABS_DAILY_SECURITY_RETURN
    )
    frame.loc[extreme_mask, "asset_return"] = pd.NA
    return frame[["trade_date", "symbol", "asset_return"]]


def _volatility_slippage_bps(
    returns_df: pd.DataFrame,
    *,
    trade_day: date,
    symbols: list[str],
    simulation_type: SimulationTypeConfig,
) -> float:
    params = simulation_type.slippage_params
    window_days = int(params.get("volatility_window_days") or 21)
    base_bps = float(params.get("base_bps") or 0.0)
    volatility_multiplier = float(params.get("volatility_multiplier") or 0.0)
    history = returns_df[
        (pd.to_datetime(returns_df["trade_date"]).dt.date < trade_day)
        & (returns_df["symbol"].isin(symbols))
    ].copy()
    if history.empty:
        return base_bps
    recent = history.sort_values("trade_date", kind="stable").groupby("symbol").tail(window_days)
    realized_vol = pd.to_numeric(recent["asset_return"], errors="coerce").dropna().std(ddof=0)
    if pd.isna(realized_vol):
        return base_bps
    return base_bps + (float(realized_vol) * 10_000.0 * volatility_multiplier)


def _slippage_bps_for_trade(
    returns_df: pd.DataFrame,
    *,
    trade_day: date,
    symbols: list[str],
    simulation_type: SimulationTypeConfig,
) -> float:
    if simulation_type.slippage_model == "none":
        return 0.0
    if simulation_type.slippage_model == "fixed_bps":
        return simulation_type.slippage_bps
    if simulation_type.slippage_model == "volatility_based":
        return _volatility_slippage_bps(
            returns_df,
            trade_day=trade_day,
            symbols=symbols,
            simulation_type=simulation_type,
        )
    raise ValueError(
        "Unsupported slippage_model for "
        f"{simulation_type.simulation_type_code}: {simulation_type.slippage_model}"
    )


def _next_trading_date(dates: list[date], current_date: date) -> date | None:
    for candidate in dates:
        if candidate > current_date:
            return candidate
    return None


def _expected_return_dates_for_strategy(
    con,
    strategy: StrategyConfig,
) -> list[date]:
    if _temp_table_exists(con, "temp_strategy_expected_return_dates"):
        rows = con.execute(
            """
            SELECT date
            FROM temp_strategy_expected_return_dates
            WHERE run_id = ?
              AND strategy_id = ?
            ORDER BY date
            """,
            [strategy.run_id, strategy.strategy_id],
        ).fetchall()
        return [row[0] for row in rows if row[0] is not None]

    holdings_rows = con.execute(
        """
        SELECT DISTINCT rebalance_date, symbol
        FROM gold.strategy_holdings
        WHERE run_id = ?
        ORDER BY rebalance_date, symbol
        """,
        [strategy.run_id],
    ).fetchall()
    rebalance_dates = sorted(
        {pd.Timestamp(row[0]).date() for row in holdings_rows if row[0] is not None}
    )
    if not rebalance_dates:
        return []

    symbols = sorted(
        {
            strategy.benchmark_symbol.strip().upper(),
            *[str(row[1]).strip().upper() for row in holdings_rows if row[1] not in (None, "")],
        }
    )
    trading_dates = _load_distinct_trading_dates(
        con,
        start_date=rebalance_dates[0],
        end_date=strategy.end_date or date(2999, 12, 31),
        symbols=symbols,
    )
    if not trading_dates:
        return []

    expected_dates: list[date] = []
    for index, rebalance_date in enumerate(rebalance_dates):
        effective_start = _next_trading_date(trading_dates, rebalance_date)
        if effective_start is None:
            continue
        next_rebalance = rebalance_dates[index + 1] if index + 1 < len(rebalance_dates) else None
        period_dates = [
            trade_date
            for trade_date in trading_dates
            if trade_date >= effective_start
            and (next_rebalance is None or trade_date < next_rebalance)
        ]
        expected_dates.extend(period_dates)
    return expected_dates


def _build_returns_for_strategy(
    con,
    strategy: StrategyConfig,
    asof_ts: datetime,
) -> list[dict[str, Any]]:
    simulation_type = _simulation_type_for_run(con, strategy.run_id)
    holdings_rows = con.execute(
        """
        SELECT rebalance_date, symbol, target_weight
        FROM gold.strategy_holdings
        WHERE run_id = ?
        ORDER BY rebalance_date, symbol
        """,
        [strategy.run_id],
    ).fetchall()
    if not holdings_rows:
        return []

    holdings_df = pd.DataFrame(
        holdings_rows,
        columns=["rebalance_date", "symbol", "target_weight"],
    )
    benchmark_symbol = strategy.benchmark_symbol.strip().upper()
    symbols = sorted(
        {benchmark_symbol, *holdings_df["symbol"].astype(str).str.upper().unique().tolist()}
    )
    start_date = holdings_df["rebalance_date"].min()
    price_df = _load_price_history(
        con,
        symbols=symbols,
        start_date=start_date,
        end_date=strategy.end_date or date(2999, 12, 31),
    )
    if price_df.empty:
        return []

    close_returns_df = _daily_symbol_returns(price_df)
    trade_returns_df = _simulation_trade_price_returns(
        price_df,
        simulation_type=simulation_type,
    )
    trading_dates = sorted(pd.to_datetime(close_returns_df["trade_date"]).dt.date.unique().tolist())
    holdings_df["rebalance_date"] = pd.to_datetime(holdings_df["rebalance_date"]).dt.date

    rebalance_dates = sorted(holdings_df["rebalance_date"].unique().tolist())
    periods: list[dict[str, Any]] = []
    previous_weights: dict[str, float] | None = None

    for index, rebalance_date in enumerate(rebalance_dates):
        effective_start = _next_trading_date(trading_dates, rebalance_date)
        if effective_start is None:
            continue
        next_rebalance = rebalance_dates[index + 1] if index + 1 < len(rebalance_dates) else None
        current_weights_df = holdings_df.loc[
            holdings_df["rebalance_date"] == rebalance_date, ["symbol", "target_weight"]
        ].copy()
        current_weights = {
            str(symbol): float(target_weight)
            for symbol, target_weight in current_weights_df.itertuples(index=False)
        }
        if previous_weights is None:
            turnover = 0.0
            trade_notional = sum(abs(weight) for weight in current_weights.values())
        else:
            all_symbols = set(previous_weights) | set(current_weights)
            trade_notional = sum(
                abs(current_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0))
                for symbol in all_symbols
            )
            turnover = 0.5 * trade_notional
        previous_weights = current_weights
        periods.append(
            {
                "effective_start": effective_start,
                "effective_end": next_rebalance,
                "weights": current_weights,
                "turnover": turnover,
                "trade_notional": trade_notional,
            }
        )

    if not periods:
        return []

    trade_return_wide = trade_returns_df.pivot(
        index="trade_date",
        columns="symbol",
        values="asset_return",
    )
    close_return_wide = close_returns_df.pivot(
        index="trade_date",
        columns="symbol",
        values="asset_return",
    )
    benchmark_returns = (
        close_return_wide[benchmark_symbol]
        if benchmark_symbol in close_return_wide.columns
        else pd.Series(dtype="float64")
    )
    daily_rows: list[dict[str, Any]] = []
    cumulative_wealth = 1.0
    peak_wealth = 1.0

    for period in periods:
        for trade_day in trading_dates:
            if trade_day < period["effective_start"]:
                continue
            if period["effective_end"] is not None and trade_day >= period["effective_end"]:
                continue
            trade_date = pd.Timestamp(trade_day)
            weights = period["weights"]
            weighted_returns = []
            held_symbols_expected = len(weights)
            held_symbols_with_returns = 0
            missing_symbols: list[str] = []
            period_return_wide = (
                trade_return_wide if trade_day == period["effective_start"] else close_return_wide
            )
            for symbol, weight in weights.items():
                missing_return_row = (
                    symbol not in period_return_wide.columns
                    or trade_date not in period_return_wide.index
                )
                if missing_return_row:
                    symbol_return = pd.NA
                else:
                    symbol_return = period_return_wide.at[trade_date, symbol]
                if pd.isna(symbol_return):
                    missing_symbols.append(symbol)
                    continue
                held_symbols_with_returns += 1
                weighted_returns.append(weight * float(symbol_return))
            portfolio_return = None if missing_symbols else float(sum(weighted_returns))
            if (
                portfolio_return is not None
                and trade_day == period["effective_start"]
                and period["trade_notional"]
            ):
                traded_symbols = sorted(weights)
                slippage_bps = _slippage_bps_for_trade(
                    close_returns_df,
                    trade_day=trade_day,
                    symbols=traded_symbols,
                    simulation_type=simulation_type,
                )
                portfolio_return -= float(period["trade_notional"]) * (slippage_bps / 10_000.0)
            benchmark_return = None
            if not benchmark_returns.empty and trade_date in benchmark_returns.index:
                bench_value = benchmark_returns.loc[trade_date]
                benchmark_return = None if pd.isna(bench_value) else float(bench_value)
            if portfolio_return is None:
                cumulative_return = None
                drawdown = None
            else:
                cumulative_wealth *= 1.0 + portfolio_return
                peak_wealth = max(peak_wealth, cumulative_wealth)
                cumulative_return = cumulative_wealth - 1.0
                drawdown = (cumulative_wealth / peak_wealth) - 1.0 if peak_wealth else None
            daily_rows.append(
                {
                    "run_id": strategy.run_id,
                    "strategy_id": strategy.strategy_id,
                    "date": trade_day,
                    "portfolio_return": portfolio_return,
                    "benchmark_return": benchmark_return,
                    "excess_return": (
                        None
                        if benchmark_return is None or portfolio_return is None
                        else portfolio_return - benchmark_return
                    ),
                    "cumulative_return": cumulative_return,
                    "drawdown": drawdown,
                    "turnover": (
                        period["turnover"] if trade_day == period["effective_start"] else 0.0
                    ),
                    "holdings_count": len(weights),
                    "held_symbols_expected": held_symbols_expected,
                    "held_symbols_with_returns": held_symbols_with_returns,
                    "missing_symbols": ",".join(sorted(missing_symbols)) or None,
                    "asof_ts": asof_ts,
                }
            )

    return daily_rows


def _ensure_strategy_run_config_table(
    con,
    strategies: list[StrategyConfig],
) -> None:
    rows = []
    for strategy in strategies:
        simulation_type = _simulation_type_for_run(con, strategy.run_id)
        rows.append(
            {
                "run_id": strategy.run_id,
                "strategy_id": strategy.strategy_id,
                "benchmark_symbol": strategy.benchmark_symbol.strip().upper(),
                "end_date": strategy.end_date,
                "fill_price_basis": simulation_type.fill_price_basis,
                "slippage_model": simulation_type.slippage_model,
                "slippage_bps": simulation_type.slippage_bps,
                "volatility_window_days": int(
                    simulation_type.slippage_params.get("volatility_window_days") or 21
                ),
                "volatility_base_bps": float(
                    simulation_type.slippage_params.get("base_bps") or 0.0
                ),
                "volatility_multiplier": float(
                    simulation_type.slippage_params.get("volatility_multiplier") or 0.0
                ),
            }
        )
    config_df = pd.DataFrame(
        rows,
        columns=[
            "run_id",
            "strategy_id",
            "benchmark_symbol",
            "end_date",
            "fill_price_basis",
            "slippage_model",
            "slippage_bps",
            "volatility_window_days",
            "volatility_base_bps",
            "volatility_multiplier",
        ],
    )
    _register_temp_df(con, "strategy_run_config_df", config_df)
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_run_config AS
        SELECT
            CAST(run_id AS VARCHAR) AS run_id,
            CAST(strategy_id AS VARCHAR) AS strategy_id,
            CAST(benchmark_symbol AS VARCHAR) AS benchmark_symbol,
            CAST(end_date AS DATE) AS end_date,
            CAST(fill_price_basis AS VARCHAR) AS fill_price_basis,
            CAST(slippage_model AS VARCHAR) AS slippage_model,
            CAST(slippage_bps AS DOUBLE) AS slippage_bps,
            CAST(volatility_window_days AS INTEGER) AS volatility_window_days,
            CAST(volatility_base_bps AS DOUBLE) AS volatility_base_bps,
            CAST(volatility_multiplier AS DOUBLE) AS volatility_multiplier
        FROM strategy_run_config_df
        """
    )


def _price_history_select_sql(con) -> str:
    schema_rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name = true)",
        [PRICE_GLOB],
    ).fetchall()
    available_columns = {str(row[0]).lower() for row in schema_rows}
    adjustment_ratio_expr = """
        CASE
            WHEN adjusted_close IS NOT NULL
             AND close IS NOT NULL
             AND CAST(close AS DOUBLE) > 0
            THEN CAST(adjusted_close AS DOUBLE) / CAST(close AS DOUBLE)
            ELSE 1.0
        END
    """
    open_expr = (
        f"CAST(open AS DOUBLE) * ({adjustment_ratio_expr})"
        if "open" in available_columns
        else "NULL::DOUBLE"
    )
    vwap_expr = (
        f"CAST(vwap AS DOUBLE) * ({adjustment_ratio_expr})"
        if "vwap" in available_columns
        else "NULL::DOUBLE"
    )
    asset_id_expr = (
        "CAST(p.asset_id AS BIGINT)" if "asset_id" in available_columns else "NULL::BIGINT"
    )
    return f"""
        SELECT DISTINCT
            CAST(p.trade_date AS DATE) AS trade_date,
            {asset_id_expr} AS asset_id,
            upper(trim(p.symbol)) AS symbol,
            {open_expr} AS open,
            CAST(p.close AS DOUBLE) AS close,
            CAST(coalesce(p.adjusted_close, p.close) AS DOUBLE) AS close_fill,
            {vwap_expr} AS vwap,
            CAST(coalesce(p.adjusted_close, p.close) AS DOUBLE) AS price
        FROM read_parquet(?, union_by_name = true) AS p
        INNER JOIN temp_strategy_symbols AS selected_symbols
            ON (
                selected_symbols.asset_id IS NOT NULL
                AND selected_symbols.asset_id = {asset_id_expr}
            )
            OR (
                selected_symbols.asset_id IS NULL
                AND selected_symbols.symbol = upper(trim(p.symbol))
            )
        INNER JOIN valid_strategy_trading_dates AS trading_dates
            ON trading_dates.trade_date = CAST(p.trade_date AS DATE)
        WHERE CAST(p.trade_date AS DATE) >= ?
          AND CAST(p.trade_date AS DATE) <= ?
          AND coalesce(p.adjusted_close, p.close) IS NOT NULL
    """


def _ensure_strategy_price_return_tables(
    con,
    run_ids: list[str],
) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_run_symbols AS
        SELECT DISTINCT run_id, CAST(asset_id AS BIGINT) AS asset_id, upper(trim(symbol)) AS symbol
        FROM gold.strategy_holdings
        WHERE run_id = ANY(?)
          AND asset_id IS NOT NULL
        UNION
        SELECT DISTINCT run_id, NULL::BIGINT AS asset_id, benchmark_symbol AS symbol
        FROM temp_strategy_run_config
        WHERE run_id = ANY(?)
          AND benchmark_symbol IS NOT NULL
          AND trim(benchmark_symbol) <> ''
        """,
        [run_ids, run_ids],
    )
    symbol_rows = con.execute(
        """
        SELECT DISTINCT asset_id, symbol
        FROM temp_strategy_run_symbols
        ORDER BY asset_id, symbol
        """,
    ).fetchall()
    symbol_df = pd.DataFrame(
        [
            {"asset_id": row[0], "symbol": str(row[1])}
            for row in symbol_rows
            if row[0] is not None or row[1] not in (None, "")
        ],
        columns=["asset_id", "symbol"],
    )
    _register_temp_df(con, "strategy_symbols_df", symbol_df)
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_symbols AS
        SELECT CAST(asset_id AS BIGINT) AS asset_id, CAST(symbol AS VARCHAR) AS symbol
        FROM strategy_symbols_df
        """
    )

    bounds = con.execute(
        """
        SELECT min(CAST(rebalance_date AS DATE)), max(coalesce(end_date, DATE '2999-12-31'))
        FROM gold.strategy_holdings AS h
        INNER JOIN temp_strategy_run_config AS c
            ON c.run_id = h.run_id
        WHERE h.run_id = ANY(?)
        """,
        [run_ids],
    ).fetchone()
    start_date = bounds[0] if bounds and bounds[0] is not None else date(1900, 1, 1)
    end_date = bounds[1] if bounds and bounds[1] is not None else date(2999, 12, 31)

    create_valid_trading_dates_table(
        con,
        PRICE_GLOB,
        table_name="valid_strategy_trading_dates",
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_price_history AS
        {_price_history_select_sql(con)}
        ORDER BY trade_date, symbol
        """,
        [PRICE_GLOB, start_date, end_date],
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_symbol_returns AS
        WITH base AS (
            SELECT
                trade_date,
                asset_id,
                symbol,
                open,
                close,
                close_fill,
                vwap,
                price,
                lag(price) OVER asset_window AS prev_price,
                lag(close_fill) OVER asset_window AS prev_close
            FROM temp_strategy_price_history
            WINDOW asset_window AS (PARTITION BY asset_id ORDER BY trade_date)
        )
        SELECT
            trade_date,
            asset_id,
            symbol,
            CASE
                WHEN price IS NOT NULL
                 AND prev_price IS NOT NULL
                 AND price > 0
                 AND prev_price > 0
                 AND abs((price / prev_price) - 1.0) <= {MAX_ABS_DAILY_SECURITY_RETURN}
                THEN (price / prev_price) - 1.0
                ELSE NULL
            END AS close_return,
            CASE
                WHEN close_fill IS NOT NULL
                 AND prev_close IS NOT NULL
                 AND close_fill > 0
                 AND prev_close > 0
                 AND abs((close_fill / prev_close) - 1.0) <= {MAX_ABS_DAILY_SECURITY_RETURN}
                THEN (close_fill / prev_close) - 1.0
                ELSE NULL
            END AS close_fill_return,
            CASE
                WHEN close_fill IS NOT NULL
                 AND open IS NOT NULL
                 AND close_fill > 0
                 AND open > 0
                 AND abs((close_fill / open) - 1.0) <= {MAX_ABS_DAILY_SECURITY_RETURN}
                THEN (close_fill / open) - 1.0
                ELSE NULL
            END AS open_return,
            CASE
                WHEN close_fill IS NOT NULL
                 AND vwap IS NOT NULL
                 AND close_fill > 0
                 AND vwap > 0
                 AND abs((close_fill / vwap) - 1.0) <= {MAX_ABS_DAILY_SECURITY_RETURN}
                THEN (close_fill / vwap) - 1.0
                ELSE NULL
            END AS vwap_return
        FROM base
        """
    )


def _create_strategy_return_period_tables(con, run_ids: list[str]) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_trading_dates AS
        SELECT DISTINCT
            rs.run_id,
            r.trade_date
        FROM temp_strategy_run_symbols AS rs
        INNER JOIN temp_strategy_symbol_returns AS r
            ON (
                rs.asset_id IS NOT NULL
                AND r.asset_id = rs.asset_id
            )
            OR (
                rs.asset_id IS NULL
                AND r.symbol = rs.symbol
            )
        ORDER BY run_id, trade_date
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_period_base AS
        WITH rebalances AS (
            SELECT DISTINCT
                h.run_id,
                h.strategy_id,
                CAST(h.rebalance_date AS DATE) AS rebalance_date,
                c.end_date,
                c.benchmark_symbol,
                c.fill_price_basis,
                c.slippage_model,
                c.slippage_bps,
                c.volatility_window_days,
                c.volatility_base_bps,
                c.volatility_multiplier,
                lag(CAST(h.rebalance_date AS DATE)) OVER (
                    PARTITION BY h.run_id
                    ORDER BY CAST(h.rebalance_date AS DATE)
                ) AS previous_rebalance_date,
                lead(CAST(h.rebalance_date AS DATE)) OVER (
                    PARTITION BY h.run_id
                    ORDER BY CAST(h.rebalance_date AS DATE)
                ) AS next_rebalance_date
            FROM gold.strategy_holdings AS h
            INNER JOIN temp_strategy_run_config AS c
                ON c.run_id = h.run_id
            WHERE h.run_id = ANY(?)
        )
        SELECT
            r.*,
            (
                SELECT min(trade_date)
                FROM temp_strategy_trading_dates AS d
                WHERE d.run_id = r.run_id
                  AND d.trade_date > r.rebalance_date
                  AND d.trade_date <= coalesce(r.end_date, DATE '2999-12-31')
            ) AS effective_start
        FROM rebalances AS r
        """,
        [run_ids],
    )
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_period_turnover AS
        WITH period_symbols AS (
            SELECT p.run_id, p.rebalance_date, h.asset_id, h.symbol
            FROM temp_strategy_period_base AS p
            INNER JOIN gold.strategy_holdings AS h
                ON h.run_id = p.run_id
               AND CAST(h.rebalance_date AS DATE) = p.rebalance_date
            UNION
            SELECT p.run_id, p.rebalance_date, h.asset_id, h.symbol
            FROM temp_strategy_period_base AS p
            INNER JOIN gold.strategy_holdings AS h
                ON h.run_id = p.run_id
               AND CAST(h.rebalance_date AS DATE) = p.previous_rebalance_date
        ),
        weighted AS (
            SELECT
                ps.run_id,
                ps.rebalance_date,
                ps.asset_id,
                ps.symbol,
                coalesce(current_h.target_weight, 0.0) AS current_weight,
                coalesce(previous_h.target_weight, 0.0) AS previous_weight
            FROM period_symbols AS ps
            LEFT JOIN gold.strategy_holdings AS current_h
                ON current_h.run_id = ps.run_id
               AND CAST(current_h.rebalance_date AS DATE) = ps.rebalance_date
               AND current_h.asset_id = ps.asset_id
            LEFT JOIN temp_strategy_period_base AS p
                ON p.run_id = ps.run_id
               AND p.rebalance_date = ps.rebalance_date
            LEFT JOIN gold.strategy_holdings AS previous_h
                ON previous_h.run_id = ps.run_id
               AND CAST(previous_h.rebalance_date AS DATE) = p.previous_rebalance_date
               AND previous_h.asset_id = ps.asset_id
        )
        SELECT
            p.run_id,
            p.strategy_id,
            p.rebalance_date,
            p.previous_rebalance_date,
            p.next_rebalance_date,
            p.effective_start,
            p.end_date,
            p.benchmark_symbol,
            p.fill_price_basis,
            p.slippage_model,
            p.slippage_bps,
            p.volatility_window_days,
            p.volatility_base_bps,
            p.volatility_multiplier,
            sum(abs(w.current_weight - w.previous_weight)) AS trade_notional,
            CASE
                WHEN p.previous_rebalance_date IS NULL THEN 0.0
                ELSE 0.5 * sum(abs(w.current_weight - w.previous_weight))
            END AS turnover
        FROM temp_strategy_period_base AS p
        LEFT JOIN weighted AS w
            ON w.run_id = p.run_id
           AND w.rebalance_date = p.rebalance_date
        WHERE p.effective_start IS NOT NULL
        GROUP BY
            p.run_id,
            p.strategy_id,
            p.rebalance_date,
            p.previous_rebalance_date,
            p.next_rebalance_date,
            p.effective_start,
            p.end_date,
            p.benchmark_symbol,
            p.fill_price_basis,
            p.slippage_model,
            p.slippage_bps,
            p.volatility_window_days,
            p.volatility_base_bps,
            p.volatility_multiplier
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_period_slippage AS
        WITH traded_symbols AS (
            SELECT DISTINCT
                p.run_id,
                p.rebalance_date,
                p.effective_start,
                p.volatility_window_days,
                p.volatility_base_bps,
                p.volatility_multiplier,
                h.asset_id,
                h.symbol
            FROM temp_strategy_period_turnover AS p
            INNER JOIN gold.strategy_holdings AS h
                ON h.run_id = p.run_id
               AND CAST(h.rebalance_date AS DATE) = p.rebalance_date
            WHERE p.slippage_model = 'volatility_based'
        ),
        recent_returns AS (
            SELECT
                ts.run_id,
                ts.rebalance_date,
                r.close_return,
                row_number() OVER (
                    PARTITION BY ts.run_id, ts.rebalance_date, ts.asset_id
                    ORDER BY r.trade_date DESC
                ) AS row_num,
                ts.volatility_window_days,
                ts.volatility_base_bps,
                ts.volatility_multiplier
            FROM traded_symbols AS ts
            INNER JOIN temp_strategy_symbol_returns AS r
                ON r.asset_id = ts.asset_id
               AND r.trade_date < ts.effective_start
               AND r.close_return IS NOT NULL
        )
        SELECT
            run_id,
            rebalance_date,
            volatility_base_bps
              + (
                    coalesce(stddev_pop(close_return), 0.0)
                    * 10000.0
                    * volatility_multiplier
                ) AS slippage_bps
        FROM recent_returns
        WHERE row_num <= volatility_window_days
        GROUP BY run_id, rebalance_date, volatility_base_bps, volatility_multiplier
        """
    )


def _materialize_returns(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
) -> int:
    _ensure_table_contract(
        con,
        schema="gold",
        table="strategy_returns",
        columns=STRATEGY_RETURNS_COLUMNS,
    )
    run_ids = _current_run_ids(strategies)
    _delete_rows_for_run_ids(con, "strategy_returns", run_ids)
    if not strategies:
        return 0

    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET threads={DUCKDB_STRATEGY_THREADS}")
    _ensure_strategy_run_config_table(con, strategies)
    holdings_count = con.execute(
        """
        SELECT count(*)
        FROM gold.strategy_holdings
        WHERE run_id = ANY(?)
        """,
        [run_ids],
    ).fetchone()[0]
    if not holdings_count:
        return 0

    _ensure_strategy_price_return_tables(con, run_ids)
    _create_strategy_return_period_tables(con, run_ids)
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_strategy_expected_return_dates AS
        SELECT DISTINCT
            p.run_id,
            p.strategy_id,
            d.trade_date AS date
        FROM temp_strategy_period_turnover AS p
        INNER JOIN temp_strategy_trading_dates AS d
            ON d.run_id = p.run_id
           AND d.trade_date >= p.effective_start
           AND (
                p.next_rebalance_date IS NULL
                OR d.trade_date < p.next_rebalance_date
           )
           AND d.trade_date <= coalesce(p.end_date, DATE '2999-12-31')
        """
    )
    for run_id in run_ids:
        con.execute(
            """
            INSERT INTO gold.strategy_returns (
                "run_id",
                "strategy_id",
                "date",
                "portfolio_return",
                "benchmark_return",
                "excess_return",
                "cumulative_return",
                "drawdown",
                "turnover",
                "holdings_count",
                "held_symbols_expected",
                "held_symbols_with_returns",
                "missing_symbols",
                "asof_ts"
            )
            WITH daily_holdings AS (
                SELECT
                    p.run_id,
                    p.strategy_id,
                    p.rebalance_date,
                    d.trade_date,
                    p.effective_start,
                    p.benchmark_symbol,
                    p.fill_price_basis,
                    p.slippage_model,
                    p.slippage_bps,
                    p.volatility_base_bps,
                    p.trade_notional,
                    p.turnover,
                    h.asset_id,
                    h.symbol,
                    h.target_weight,
                    CASE
                        WHEN d.trade_date = p.effective_start
                         AND p.fill_price_basis IN ('open', 'next_open')
                        THEN r.open_return
                        WHEN d.trade_date = p.effective_start
                         AND p.fill_price_basis = 'vwap'
                        THEN r.vwap_return
                        ELSE r.close_return
                    END AS asset_return
                FROM temp_strategy_period_turnover AS p
                INNER JOIN temp_strategy_trading_dates AS d
                    ON d.run_id = p.run_id
                   AND d.trade_date >= p.effective_start
                   AND (
                        p.next_rebalance_date IS NULL
                        OR d.trade_date < p.next_rebalance_date
                   )
                   AND d.trade_date <= coalesce(p.end_date, DATE '2999-12-31')
                INNER JOIN gold.strategy_holdings AS h
                    ON h.run_id = p.run_id
                   AND CAST(h.rebalance_date AS DATE) = p.rebalance_date
                LEFT JOIN temp_strategy_symbol_returns AS r
                    ON r.trade_date = d.trade_date
                   AND r.asset_id = h.asset_id
                WHERE p.run_id = ?
            ),
            aggregated AS (
                SELECT
                    run_id,
                    strategy_id,
                    rebalance_date,
                    trade_date,
                    effective_start,
                    benchmark_symbol,
                    fill_price_basis,
                    slippage_model,
                    slippage_bps,
                    volatility_base_bps,
                    trade_notional,
                    turnover,
                    count(*) AS held_symbols_expected,
                    count(asset_return) AS held_symbols_with_returns,
                    string_agg(
                        CASE WHEN asset_return IS NULL THEN symbol ELSE NULL END,
                        ','
                        ORDER BY symbol
                    ) AS missing_symbols,
                    sum(target_weight * asset_return) AS weighted_return
                FROM daily_holdings
                GROUP BY ALL
            ),
            with_slippage AS (
                SELECT
                    a.*,
                    CASE
                        WHEN a.held_symbols_with_returns < a.held_symbols_expected THEN NULL
                        ELSE a.weighted_return
                          - CASE
                                WHEN a.trade_date = a.effective_start
                                 AND coalesce(a.trade_notional, 0.0) <> 0.0
                                THEN coalesce(a.trade_notional, 0.0)
                                  * (
                                        CASE
                                            WHEN a.slippage_model = 'none' THEN 0.0
                                            WHEN a.slippage_model = 'fixed_bps'
                                            THEN coalesce(a.slippage_bps, 0.0)
                                            WHEN a.slippage_model = 'volatility_based'
                                            THEN coalesce(
                                                ps.slippage_bps,
                                                a.volatility_base_bps,
                                                0.0
                                            )
                                            ELSE 0.0
                                        END
                                    ) / 10000.0
                                ELSE 0.0
                            END
                    END AS portfolio_return
                FROM aggregated AS a
                LEFT JOIN temp_strategy_period_slippage AS ps
                    ON ps.run_id = a.run_id
                   AND ps.rebalance_date = a.rebalance_date
            ),
            with_benchmark AS (
                SELECT
                    s.*,
                    b.close_return AS benchmark_return
                FROM with_slippage AS s
                LEFT JOIN temp_strategy_symbol_returns AS b
                    ON b.trade_date = s.trade_date
                   AND b.symbol = s.benchmark_symbol
            ),
            with_wealth AS (
                SELECT
                    *,
                    CASE
                        WHEN portfolio_return IS NULL THEN NULL
                        ELSE exp(
                            sum(
                                CASE
                                    WHEN portfolio_return IS NULL THEN NULL
                                    ELSE ln(1.0 + portfolio_return)
                                END
                            ) OVER (
                                PARTITION BY run_id
                                ORDER BY trade_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        )
                    END AS cumulative_wealth
                FROM with_benchmark
            ),
            final_rows AS (
                SELECT
                    run_id,
                    strategy_id,
                    trade_date AS date,
                    portfolio_return,
                    benchmark_return,
                    CASE
                        WHEN benchmark_return IS NULL OR portfolio_return IS NULL THEN NULL
                        ELSE portfolio_return - benchmark_return
                    END AS excess_return,
                    CASE
                        WHEN cumulative_wealth IS NULL THEN NULL
                        ELSE cumulative_wealth - 1.0
                    END AS cumulative_return,
                    CASE
                        WHEN cumulative_wealth IS NULL THEN NULL
                        ELSE (
                            cumulative_wealth
                            / max(cumulative_wealth) OVER (
                                PARTITION BY run_id
                                ORDER BY trade_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        ) - 1.0
                    END AS drawdown,
                    CASE WHEN trade_date = effective_start THEN turnover ELSE 0.0 END AS turnover,
                    held_symbols_expected AS holdings_count,
                    held_symbols_expected,
                    held_symbols_with_returns,
                    nullif(missing_symbols, '') AS missing_symbols,
                    ? AS asof_ts
                FROM with_wealth
            )
            SELECT *
            FROM final_rows
            ORDER BY date
            """,
            [run_id, asof_ts],
        )
    row = con.execute(
        """
        SELECT count(*)
        FROM gold.strategy_returns
        WHERE run_id = ANY(?)
        """,
        [run_ids],
    )
    return int(row.fetchone()[0] or 0)


def _log_strategy_return_continuity_check(
    *,
    measured_con,
    observability_con,
    strategies: list[StrategyConfig],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> None:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return

    actual_rows = measured_con.execute(
        """
        SELECT strategy_id, date
        FROM gold.strategy_returns
        WHERE run_id = ANY(?)
        ORDER BY strategy_id, date
        """,
        [run_ids],
    ).fetchall()
    actual_dates_by_strategy: dict[str, set[date]] = {}
    for strategy_id, return_date in actual_rows:
        if return_date is None:
            continue
        actual_dates_by_strategy.setdefault(str(strategy_id), set()).add(
            pd.Timestamp(return_date).date()
        )

    failing_strategies: list[dict[str, Any]] = []
    measured_value = 0.0
    for strategy in strategies:
        expected_dates = set(_expected_return_dates_for_strategy(measured_con, strategy))
        actual_dates = actual_dates_by_strategy.get(strategy.strategy_id, set())
        missing_dates = sorted(expected_dates - actual_dates)
        unexpected_dates = sorted(actual_dates - expected_dates)
        measured_value += float(len(missing_dates) + len(unexpected_dates))
        if not missing_dates and not unexpected_dates:
            continue
        failing_strategies.append(
            {
                "run_id": strategy.run_id,
                "strategy_id": strategy.strategy_id,
                "expected_return_dates": [str(value) for value in sorted(expected_dates)],
                "actual_return_dates": [str(value) for value in sorted(actual_dates)],
                "missing_return_dates": [str(value) for value in missing_dates],
                "unexpected_return_dates": [str(value) for value in unexpected_dates],
            }
        )

    write_dq_log(
        con=observability_con,
        check_name="dq_gold_strategy_returns_expected_return_dates",
        severity="RED",
        status="PASS" if measured_value == 0.0 else "FAIL",
        measured_value=measured_value,
        threshold_value=0.0,
        details={
            "table": "gold.strategy_returns",
            "run_ids": run_ids,
            "evaluated_strategy_count": len(strategies),
            "failing_strategies": failing_strategies,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )


def _log_strategy_benchmark_series_check(
    *,
    measured_con,
    observability_con,
    strategies: list[StrategyConfig],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> bool:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return True

    benchmark_rows = measured_con.execute(
        """
        SELECT strategy_id, date, benchmark_return
        FROM gold.strategy_returns
        WHERE run_id = ANY(?)
        ORDER BY strategy_id, date
        """,
        [run_ids],
    ).fetchall()
    benchmark_dates_by_strategy: dict[str, set[date]] = {}
    null_benchmark_dates_by_strategy: dict[str, set[date]] = {}
    for strategy_id, return_date, benchmark_return in benchmark_rows:
        if return_date is None:
            continue
        parsed_date = pd.Timestamp(return_date).date()
        strategy_key = str(strategy_id)
        benchmark_dates_by_strategy.setdefault(strategy_key, set()).add(parsed_date)
        if benchmark_return is None:
            null_benchmark_dates_by_strategy.setdefault(strategy_key, set()).add(parsed_date)

    failing_strategies: list[dict[str, Any]] = []
    measured_value = 0.0
    for strategy in strategies:
        expected_dates = set(_expected_return_dates_for_strategy(measured_con, strategy))
        actual_dates = benchmark_dates_by_strategy.get(strategy.strategy_id, set())
        null_dates = null_benchmark_dates_by_strategy.get(strategy.strategy_id, set())
        missing_dates = sorted(expected_dates - actual_dates)
        missing_value_dates = sorted(expected_dates & null_dates)
        measured_value += float(len(missing_dates) + len(missing_value_dates))
        if not missing_dates and not missing_value_dates:
            continue
        failing_strategies.append(
            {
                "run_id": strategy.run_id,
                "strategy_id": strategy.strategy_id,
                "benchmark_symbol": strategy.benchmark_symbol,
                "expected_benchmark_dates": [str(value) for value in sorted(expected_dates)],
                "available_benchmark_dates": [str(value) for value in sorted(actual_dates)],
                "missing_benchmark_dates": [str(value) for value in missing_dates],
                "null_benchmark_value_dates": [str(value) for value in missing_value_dates],
            }
        )

    write_dq_log(
        con=observability_con,
        check_name="dq_gold_strategy_returns_benchmark_series_present",
        severity="RED",
        status="PASS" if measured_value == 0.0 else "FAIL",
        measured_value=measured_value,
        threshold_value=0.0,
        details={
            "table": "gold.strategy_returns",
            "run_ids": run_ids,
            "evaluated_strategy_count": len(strategies),
            "failing_strategies": failing_strategies,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )
    return measured_value == 0.0


def _log_strategy_held_return_coverage_check(
    *,
    measured_con,
    observability_con,
    strategies: list[StrategyConfig],
    run_id: str | None,
    job_name: str | None,
    partition_key: str | None,
) -> bool:
    run_ids = _current_run_ids(strategies)
    if not run_ids:
        return True

    missing_rows = measured_con.execute(
        """
        SELECT
            run_id,
            strategy_id,
            date,
            held_symbols_expected,
            held_symbols_with_returns,
            missing_symbols
        FROM gold.strategy_returns
        WHERE run_id = ANY(?)
          AND coalesce(held_symbols_with_returns, 0) < coalesce(held_symbols_expected, 0)
        ORDER BY run_id, date
        """,
        [run_ids],
    ).fetchall()
    failing_dates = [
        {
            "run_id": str(row[0]),
            "strategy_id": str(row[1]),
            "date": str(row[2]),
            "held_symbols_expected": int(row[3] or 0),
            "held_symbols_with_returns": int(row[4] or 0),
            "missing_symbols": [] if row[5] in (None, "") else str(row[5]).split(","),
        }
        for row in missing_rows
    ]
    measured_value = float(len(failing_dates))
    write_dq_log(
        con=observability_con,
        check_name="dq_gold_strategy_returns_held_symbol_return_coverage",
        severity="RED",
        status="PASS" if measured_value == 0.0 else "FAIL",
        measured_value=measured_value,
        threshold_value=0.0,
        details={
            "table": "gold.strategy_returns",
            "run_ids": run_ids,
            "failing_dates": failing_dates,
        },
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
        dedupe_by_run_check=True,
    )
    return measured_value == 0.0


def _annualized_return(total_return: float, periods: int) -> float | None:
    if periods <= 0 or (1.0 + total_return) <= 0:
        return None
    return (1.0 + total_return) ** (252.0 / periods) - 1.0


def _performance_row(
    run_id: str,
    strategy_id: str,
    returns_df: pd.DataFrame,
    asof_ts: datetime,
) -> dict[str, Any]:
    frame = returns_df.sort_values("date", kind="stable").copy()
    frame = frame.dropna(subset=["portfolio_return", "cumulative_return"])
    portfolio = frame["portfolio_return"].astype(float)
    benchmark = frame["benchmark_return"].astype(float)
    excess = frame["excess_return"].astype(float)
    periods = int(len(frame))
    total_return = float(frame["cumulative_return"].iloc[-1]) if periods else 0.0
    benchmark_total_return = float((1.0 + benchmark).prod() - 1.0) if periods else 0.0
    cagr = _annualized_return(total_return, periods)
    benchmark_cagr = _annualized_return(benchmark_total_return, periods)
    daily_std = float(portfolio.std(ddof=1)) if periods > 1 else math.nan
    downside = portfolio[portfolio < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else math.nan
    annualized_volatility = daily_std * math.sqrt(252.0) if math.isfinite(daily_std) else None
    sharpe_ratio = (
        float(portfolio.mean()) / daily_std * math.sqrt(252.0)
        if math.isfinite(daily_std) and daily_std > 0
        else None
    )
    sortino_ratio = (
        float(portfolio.mean()) / downside_std * math.sqrt(252.0)
        if math.isfinite(downside_std) and downside_std > 0
        else None
    )
    alpha = (
        float(excess.mean()) * 252.0
        if periods and math.isfinite(float(excess.mean()))
        else ((cagr - benchmark_cagr) if cagr is not None and benchmark_cagr is not None else None)
    )
    return {
        "run_id": run_id,
        "strategy_id": strategy_id,
        "cagr": cagr,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "max_drawdown": float(frame["drawdown"].min()) if periods else None,
        "annualized_volatility": annualized_volatility,
        "hit_rate": float((portfolio > 0).mean()) if periods else None,
        "turnover_avg": float(frame["turnover"].mean()) if periods else None,
        "benchmark_return": benchmark_total_return,
        "alpha": alpha,
        "asof_ts": asof_ts,
    }


def _materialize_performance(
    con,
    strategies: list[StrategyConfig],
    *,
    asof_ts: datetime,
) -> int:
    _ensure_table_contract(
        con,
        schema="gold",
        table="strategy_performance",
        columns=STRATEGY_PERFORMANCE_COLUMNS,
    )
    run_ids = _current_run_ids(strategies)
    _delete_rows_for_run_ids(con, "strategy_performance", run_ids)
    if not run_ids:
        return 0

    con.execute(
        """
        INSERT INTO gold.strategy_performance (
            "run_id",
            "strategy_id",
            "cagr",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "annualized_volatility",
            "hit_rate",
            "turnover_avg",
            "benchmark_return",
            "alpha",
            "asof_ts"
        )
        WITH clean AS (
            SELECT *
            FROM gold.strategy_returns
            WHERE run_id = ANY(?)
              AND portfolio_return IS NOT NULL
              AND cumulative_return IS NOT NULL
        ),
        last_returns AS (
            SELECT run_id, cumulative_return
            FROM (
                SELECT
                    run_id,
                    cumulative_return,
                    row_number() OVER (
                        PARTITION BY run_id
                        ORDER BY date DESC
                    ) AS row_num
                FROM clean
            )
            WHERE row_num = 1
        ),
        aggregates AS (
            SELECT
                run_id,
                any_value(strategy_id) AS strategy_id,
                count(*) AS periods,
                avg(portfolio_return) AS portfolio_mean,
                stddev_samp(portfolio_return) AS portfolio_std,
                stddev_samp(
                    CASE WHEN portfolio_return < 0 THEN portfolio_return ELSE NULL END
                ) AS downside_std,
                min(drawdown) AS max_drawdown,
                avg(CASE WHEN portfolio_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
                avg(turnover) AS turnover_avg,
                avg(excess_return) AS mean_excess_return,
                exp(
                    coalesce(
                        sum(
                            CASE
                                WHEN benchmark_return IS NULL THEN NULL
                                ELSE ln(1.0 + benchmark_return)
                            END
                        ),
                        0.0
                    )
                ) - 1.0 AS benchmark_total_return
            FROM clean
            GROUP BY run_id
        ),
        metrics AS (
            SELECT
                a.run_id,
                a.strategy_id,
                CASE
                    WHEN a.periods <= 0 OR 1.0 + l.cumulative_return <= 0 THEN NULL
                    ELSE pow(1.0 + l.cumulative_return, 252.0 / a.periods) - 1.0
                END AS cagr,
                CASE
                    WHEN a.portfolio_std IS NOT NULL AND a.portfolio_std > 0
                    THEN a.portfolio_mean / a.portfolio_std * sqrt(252.0)
                    ELSE NULL
                END AS sharpe_ratio,
                CASE
                    WHEN a.downside_std IS NOT NULL AND a.downside_std > 0
                    THEN a.portfolio_mean / a.downside_std * sqrt(252.0)
                    ELSE NULL
                END AS sortino_ratio,
                a.max_drawdown,
                CASE
                    WHEN a.portfolio_std IS NOT NULL
                    THEN a.portfolio_std * sqrt(252.0)
                    ELSE NULL
                END AS annualized_volatility,
                a.hit_rate,
                a.turnover_avg,
                a.benchmark_total_return AS benchmark_return,
                CASE
                    WHEN a.mean_excess_return IS NOT NULL
                    THEN a.mean_excess_return * 252.0
                    WHEN (
                        CASE
                            WHEN a.periods <= 0 OR 1.0 + l.cumulative_return <= 0 THEN NULL
                            ELSE pow(1.0 + l.cumulative_return, 252.0 / a.periods) - 1.0
                        END
                    ) IS NOT NULL
                     AND (
                        CASE
                            WHEN a.periods <= 0 OR 1.0 + a.benchmark_total_return <= 0 THEN NULL
                            ELSE pow(1.0 + a.benchmark_total_return, 252.0 / a.periods) - 1.0
                        END
                    ) IS NOT NULL
                    THEN (
                        CASE
                            WHEN a.periods <= 0 OR 1.0 + l.cumulative_return <= 0 THEN NULL
                            ELSE pow(1.0 + l.cumulative_return, 252.0 / a.periods) - 1.0
                        END
                    ) - (
                        CASE
                            WHEN a.periods <= 0 OR 1.0 + a.benchmark_total_return <= 0 THEN NULL
                            ELSE pow(1.0 + a.benchmark_total_return, 252.0 / a.periods) - 1.0
                        END
                    )
                    ELSE NULL
                END AS alpha
            FROM aggregates AS a
            INNER JOIN last_returns AS l
                ON l.run_id = a.run_id
        )
        SELECT
            run_id,
            strategy_id,
            cagr,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown,
            annualized_volatility,
            hit_rate,
            turnover_avg,
            benchmark_return,
            alpha,
            ? AS asof_ts
        FROM metrics
        ORDER BY strategy_id, run_id
        """,
        [run_ids, asof_ts],
    )
    row = con.execute(
        """
        SELECT count(*)
        FROM gold.strategy_performance
        WHERE run_id = ANY(?)
        """,
        [run_ids],
    )
    return int(row.fetchone()[0] or 0)


@asset(
    name="strategy_rankings",
    key_prefix=["gold"],
    deps=[
        ref_invalid_trading_days,
        silver_strategy_definitions,
        silver_strategy_parameters,
        silver_strategy_runs,
        silver_signals_daily,
        silver_universe_membership_daily,
        silver_security_master,
        silver_research_daily_prices,
    ],
    required_resource_keys={"research_duckdb"},
)
def gold_strategy_rankings(context: AssetExecutionContext) -> None:
    """
    Materialize monthly strategy rankings for all active research strategies.
    """
    con = context.resources.research_duckdb
    strategies = _strategies_for_context(con, context, source_table=None)
    asof_ts = _now_utc_naive()
    _ensure_strategy_run_rows(con, strategies, asof_ts=asof_ts, run_status="running")
    try:
        row_count = _materialize_rankings(con, strategies, asof_ts=asof_ts)
        _update_strategy_run_row_counts(
            con,
            strategies,
            source_table="strategy_rankings",
            target_column="rankings_row_count",
            asof_ts=asof_ts,
        )
    except Exception as exc:
        _update_strategy_runs_failure(con, strategies, asof_ts=asof_ts, error_message=str(exc))
        raise
    context.add_output_metadata(
        {
            "table": "gold.strategy_rankings",
            "strategy_count": len(strategies),
            "row_count": row_count,
        }
    )


@asset(
    name="strategy_holdings",
    key_prefix=["gold"],
    deps=[gold_strategy_rankings],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def gold_strategy_holdings(context: AssetExecutionContext) -> None:
    """
    Materialize rebalance holdings from strategy rankings.
    """
    con = context.resources.research_duckdb
    strategies = _strategies_for_context(
        con,
        context,
        source_table="strategy_rankings",
    )
    asof_ts = _now_utc_naive()
    try:
        _log_strategy_run_contract_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        row_count = _materialize_holdings(con, strategies, asof_ts=asof_ts)
        _update_strategy_run_row_counts(
            con,
            strategies,
            source_table="strategy_holdings",
            target_column="holdings_row_count",
            asof_ts=asof_ts,
        )
        _log_holdings_duplicate_symbol_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        _log_holdings_weight_sum_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        _log_rebalance_dates_present_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            table_name="strategy_rankings",
            check_name="dq_gold_strategy_rankings_expected_rebalance_dates",
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        _log_rebalance_dates_present_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            table_name="strategy_holdings",
            check_name="dq_gold_strategy_holdings_expected_rebalance_dates",
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        _log_simulation_result_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
    except Exception as exc:
        _update_strategy_runs_failure(con, strategies, asof_ts=asof_ts, error_message=str(exc))
        _log_simulation_result_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        raise
    context.add_output_metadata(
        {
            "table": "gold.strategy_holdings",
            "strategy_count": len(strategies),
            "row_count": row_count,
        }
    )


@asset(
    name="strategy_returns",
    key_prefix=["gold"],
    deps=[gold_strategy_holdings],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def gold_strategy_returns(context: AssetExecutionContext) -> None:
    """
    Materialize daily return paths from rebalance holdings.
    """
    con = context.resources.research_duckdb
    strategies = _strategies_for_context(
        con,
        context,
        source_table="strategy_holdings",
    )
    asof_ts = _now_utc_naive()
    try:
        _log_strategy_run_contract_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        row_count = _materialize_returns(con, strategies, asof_ts=asof_ts)
        _update_strategy_run_row_counts(
            con,
            strategies,
            source_table="strategy_returns",
            target_column="returns_row_count",
            asof_ts=asof_ts,
        )
        _log_strategy_return_continuity_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        _log_strategy_held_return_coverage_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        _log_simulation_result_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
    except Exception as exc:
        _update_strategy_runs_failure(con, strategies, asof_ts=asof_ts, error_message=str(exc))
        _log_simulation_result_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        raise
    context.add_output_metadata(
        {
            "table": "gold.strategy_returns",
            "strategy_count": len(strategies),
            "row_count": row_count,
        }
    )


@asset(
    name="strategy_performance",
    key_prefix=["gold"],
    deps=[gold_strategy_returns],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def gold_strategy_performance(context: AssetExecutionContext) -> None:
    """
    Materialize one-row strategy performance summaries for the current strategy runs.
    """
    con = context.resources.research_duckdb
    strategies = _strategies_for_context(
        con,
        context,
        source_table="strategy_returns",
    )
    asof_ts = _now_utc_naive()
    try:
        _log_strategy_run_contract_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        benchmark_series_ready = _log_strategy_benchmark_series_check(
            measured_con=con,
            observability_con=context.resources.duckdb,
            strategies=strategies,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        if not benchmark_series_ready:
            raise ValueError(
                "Missing benchmark series values for one or more strategy return dates; "
                "strategy comparison outputs were not materialized."
            )
        row_count = _materialize_performance(con, strategies, asof_ts=asof_ts)
        _update_strategy_run_row_counts(
            con,
            strategies,
            source_table="strategy_performance",
            target_column="performance_row_count",
            asof_ts=asof_ts,
        )
        _update_strategy_runs_success(con, strategies, asof_ts=asof_ts)
        _log_simulation_result_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
    except Exception as exc:
        _update_strategy_runs_failure(con, strategies, asof_ts=asof_ts, error_message=str(exc))
        _log_simulation_result_checks(
            measured_con=con,
            observability_con=context.resources.duckdb,
            run_id=_safe_run_id(context),
            job_name=_safe_job_name(context),
            partition_key=_safe_partition_key(context),
        )
        raise
    context.add_output_metadata(
        {
            "table": "gold.strategy_performance",
            "strategy_count": len(strategies),
            "row_count": row_count,
        }
    )
