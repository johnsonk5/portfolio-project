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
from portfolio_project.defs.research_db.silver.research_prices import (
    RESEARCH_DAILY_PRICES_DATASET,
    silver_research_daily_prices,
)
from portfolio_project.defs.research_db.silver.signals import silver_signals_daily
from portfolio_project.defs.research_db.silver.strategy import (
    _ensure_table_contract,
    _quote_identifier,
    _safe_run_id,
    silver_strategy_definitions,
    silver_strategy_parameters,
    silver_strategy_runs,
)
from portfolio_project.defs.research_db.silver.universe import silver_universe_membership_daily

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
MISSING_STRATEGIES_JOB_NAME = "strategy_missing_backfill_job"

STRATEGY_RANKINGS_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("rebalance_date", "DATE"),
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


def _safe_json_loads(raw_value: Any) -> dict[str, Any]:
    if raw_value in (None, ""):
        return {}
    parsed = json.loads(str(raw_value))
    return parsed if isinstance(parsed, dict) else {}


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
        strategies.append(
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
                run_id=_strategy_run_id(context, strategy_id),
            )
        )
    return strategies


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

    completed_strategy_ids = {
        str(row[0])
        for row in con.execute(
            """
            SELECT DISTINCT strategy_id
            FROM gold.strategy_performance
            WHERE strategy_id IS NOT NULL
            """
        ).fetchall()
    }
    return [
        strategy for strategy in strategies if strategy.strategy_id not in completed_strategy_ids
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
        return [row[0] for row in rows if row[0] is not None]

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
        return [row[0] for row in rows if row[0] is not None]

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
    return [row[0] for row in rows if row[0] is not None]


def _build_rankings_for_strategy(
    con,
    strategy: StrategyConfig,
    asof_ts: datetime,
) -> list[dict[str, Any]]:
    rankings: list[dict[str, Any]] = []
    rebalance_dates = _rebalance_dates_for_strategy(con, strategy)

    for rebalance_date in rebalance_dates:
        parameters = _active_parameters_by_date(con, strategy.strategy_id, rebalance_date)
        signal_column = str(parameters.get("signal_column") or "momentum_12_1").strip()
        secondary_signal_column = str(parameters.get("secondary_signal_column") or "").strip()
        ranking_direction = str(parameters.get("ranking_direction") or "desc").strip().lower()
        score_method = str(parameters.get("score_method") or "").strip().lower()
        universe_name = str(strategy.config.get("universe") or "").strip().lower()
        selection_mode = str(strategy.config.get("selection_mode") or "").strip().lower()
        fixed_symbol = str(parameters.get("symbol") or strategy.benchmark_symbol).strip().upper()

        if selection_mode == "fixed_symbol" or universe_name == "benchmark_only":
            candidate_rows = con.execute(
                """
                SELECT
                    upper(trim(symbol)) AS symbol,
                    1.0 AS score
                FROM silver.signals_daily
                WHERE CAST(date AS DATE) = ?
                  AND upper(trim(symbol)) = ?
                LIMIT 1
                """,
                [rebalance_date, fixed_symbol],
            ).fetchall()
        else:
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
                    CAST(s.{_quote_identifier(signal_column)} AS DOUBLE) AS primary_score
                    {secondary_select_sql}
                FROM silver.signals_daily AS s
                INNER JOIN silver.universe_membership_daily AS u
                    ON CAST(u.member_date AS DATE) = CAST(s.date AS DATE)
                   AND upper(trim(u.symbol)) = upper(trim(s.symbol))
                WHERE CAST(s.date AS DATE) = ?
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
            candidate_rows = con.execute(sql, params).fetchall()

        candidate_columns = (
            ["symbol", "primary_score", "secondary_score"]
            if secondary_signal_column
            else ["symbol", "primary_score"]
        )
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
                "run_status": run_status,
                "dataset_version": dataset_version,
                "code_version": None,
                "started_at": asof_ts,
                "completed_at": None,
                "error_message": None,
                "persist": True,
                "asof_ts": asof_ts,
            }
            for strategy in strategies
        ]
    )
    _register_temp_df(con, "strategy_run_seed_df", run_rows)
    con.execute(
        """
        INSERT INTO silver.strategy_runs
        SELECT seed.*
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
            dataset_version = seed.dataset_version,
            started_at = coalesce(target.started_at, seed.started_at),
            asof_ts = seed.asof_ts,
            persist = coalesce(target.persist, TRUE)
        FROM strategy_run_seed_df AS seed
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

    ranking_rows: list[dict[str, Any]] = []
    for strategy in strategies:
        ranking_rows.extend(_build_rankings_for_strategy(con, strategy, asof_ts))

    if not ranking_rows:
        return 0

    ranking_df = pd.DataFrame(ranking_rows)
    _register_temp_df(con, "strategy_rankings_df", ranking_df)
    con.execute(
        """
        INSERT INTO gold.strategy_rankings
        SELECT
            run_id,
            strategy_id,
            rebalance_date,
            symbol,
            score,
            rank,
            selected_flag,
            asof_ts
        FROM strategy_rankings_df
        """
    )
    return int(len(ranking_df))


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

    holding_rows: list[dict[str, Any]] = []
    for strategy in strategies:
        selected_rows = con.execute(
            """
            SELECT rebalance_date, symbol, rank, score
            FROM gold.strategy_rankings
            WHERE run_id = ?
              AND selected_flag = TRUE
            ORDER BY rebalance_date, rank, symbol
            """,
            [strategy.run_id],
        ).fetchall()
        if not selected_rows:
            continue
        selected_df = pd.DataFrame(
            selected_rows,
            columns=["rebalance_date", "symbol", "entry_rank", "signal_value"],
        )
        counts = selected_df.groupby("rebalance_date")["symbol"].transform("count")
        if strategy.weighting_method.strip().lower() != "equal":
            raise ValueError(
                "Unsupported weighting_method for "
                f"{strategy.strategy_id}: {strategy.weighting_method}"
            )
        selected_df["target_weight"] = 1.0 / counts
        selected_df["run_id"] = strategy.run_id
        selected_df["strategy_id"] = strategy.strategy_id
        selected_df["side"] = "SHORT" if strategy.long_short_flag else "LONG"
        selected_df["asof_ts"] = asof_ts
        holding_rows.extend(selected_df.to_dict(orient="records"))

    if not holding_rows:
        return 0

    holding_df = pd.DataFrame(holding_rows)
    holding_df = holding_df[
        [
            "run_id",
            "strategy_id",
            "rebalance_date",
            "symbol",
            "target_weight",
            "side",
            "entry_rank",
            "signal_value",
            "asof_ts",
        ]
    ]
    _register_temp_df(con, "strategy_holdings_df", holding_df)
    con.execute(
        """
        INSERT INTO gold.strategy_holdings
        SELECT *
        FROM strategy_holdings_df
        """
    )
    return int(len(holding_df))


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
    max_abs_deviation = max(
        (group["abs_deviation"] for group in failing_groups),
        default=0.0,
    )

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
        check_name="dq_gold_strategy_holdings_unique_symbol_per_rebalance",
        relation_sql="""
            SELECT
                run_id,
                strategy_id,
                rebalance_date,
                symbol
            FROM gold.strategy_holdings
            WHERE run_id = ANY(?)
        """,
        relation_params=[run_ids],
        key_columns=["run_id", "strategy_id", "rebalance_date", "symbol"],
        details={
            "table": "gold.strategy_holdings",
            "run_ids": run_ids,
            "uniqueness_scope": ["strategy_id", "rebalance_date", "symbol"],
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
        expected_dates = set(_rebalance_dates_for_strategy(measured_con, strategy))
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
        return pd.DataFrame(columns=["trade_date", "symbol", "price"])
    return con.execute(
        """
        SELECT
            CAST(trade_date AS DATE) AS trade_date,
            upper(trim(symbol)) AS symbol,
            CAST(coalesce(adjusted_close, close) AS DOUBLE) AS price
        FROM read_parquet(?)
        WHERE upper(trim(symbol)) = ANY(?)
          AND CAST(trade_date AS DATE) >= ?
          AND CAST(trade_date AS DATE) <= ?
          AND coalesce(adjusted_close, close) IS NOT NULL
        ORDER BY trade_date, symbol
        """,
        [PRICE_GLOB, symbols, start_date, end_date],
    ).fetch_df()


def _load_distinct_trading_dates(
    con,
    *,
    start_date: date,
    end_date: date,
) -> list[date]:
    rows = con.execute(
        """
        SELECT DISTINCT CAST(trade_date AS DATE) AS trade_date
        FROM read_parquet(?)
        WHERE CAST(trade_date AS DATE) >= ?
          AND CAST(trade_date AS DATE) <= ?
        ORDER BY trade_date
        """,
        [PRICE_GLOB, start_date, end_date],
    ).fetchall()
    return [row[0] for row in rows if row[0] is not None]


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
    extreme_mask = (
        frame["asset_return"].notna()
        & (frame["asset_return"].abs() > MAX_ABS_DAILY_SECURITY_RETURN)
    )
    frame.loc[extreme_mask, "asset_return"] = pd.NA
    return frame[["trade_date", "symbol", "asset_return"]]


def _next_trading_date(dates: list[date], current_date: date) -> date | None:
    for candidate in dates:
        if candidate > current_date:
            return candidate
    return None


def _expected_return_dates_for_strategy(
    con,
    strategy: StrategyConfig,
) -> list[date]:
    holdings_rows = con.execute(
        """
        SELECT DISTINCT rebalance_date
        FROM gold.strategy_holdings
        WHERE run_id = ?
        ORDER BY rebalance_date
        """,
        [strategy.run_id],
    ).fetchall()
    rebalance_dates = [
        pd.Timestamp(row[0]).date() for row in holdings_rows if row[0] is not None
    ]
    if not rebalance_dates:
        return []

    trading_dates = _load_distinct_trading_dates(
        con,
        start_date=rebalance_dates[0],
        end_date=strategy.end_date or date(2999, 12, 31),
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

    returns_df = _daily_symbol_returns(price_df)
    trading_dates = sorted(pd.to_datetime(returns_df["trade_date"]).dt.date.unique().tolist())
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
        else:
            all_symbols = set(previous_weights) | set(current_weights)
            turnover = 0.5 * sum(
                abs(current_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0))
                for symbol in all_symbols
            )
        previous_weights = current_weights
        periods.append(
            {
                "effective_start": effective_start,
                "effective_end": next_rebalance,
                "weights": current_weights,
                "turnover": turnover,
            }
        )

    if not periods:
        return []

    asset_return_wide = returns_df.pivot(
        index="trade_date",
        columns="symbol",
        values="asset_return",
    )
    benchmark_returns = (
        asset_return_wide[benchmark_symbol]
        if benchmark_symbol in asset_return_wide.columns
        else pd.Series(dtype="float64")
    )
    daily_rows: list[dict[str, Any]] = []
    cumulative_wealth = 1.0
    peak_wealth = 1.0

    for period in periods:
        for trade_date in asset_return_wide.index:
            trade_day = pd.Timestamp(trade_date).date()
            if trade_day < period["effective_start"]:
                continue
            if period["effective_end"] is not None and trade_day >= period["effective_end"]:
                continue
            weights = period["weights"]
            weighted_returns = []
            for symbol, weight in weights.items():
                symbol_return = asset_return_wide.at[trade_date, symbol]
                weighted_returns.append(
                    weight * (0.0 if pd.isna(symbol_return) else float(symbol_return))
                )
            portfolio_return = float(sum(weighted_returns))
            benchmark_return = 0.0
            if not benchmark_returns.empty and trade_date in benchmark_returns.index:
                bench_value = benchmark_returns.loc[trade_date]
                benchmark_return = 0.0 if pd.isna(bench_value) else float(bench_value)
            cumulative_wealth *= 1.0 + portfolio_return
            peak_wealth = max(peak_wealth, cumulative_wealth)
            drawdown = (cumulative_wealth / peak_wealth) - 1.0 if peak_wealth else None
            daily_rows.append(
                {
                    "run_id": strategy.run_id,
                    "strategy_id": strategy.strategy_id,
                    "date": trade_day,
                    "portfolio_return": portfolio_return,
                    "benchmark_return": benchmark_return,
                    "excess_return": portfolio_return - benchmark_return,
                    "cumulative_return": cumulative_wealth - 1.0,
                    "drawdown": drawdown,
                    "turnover": (
                        period["turnover"] if trade_day == period["effective_start"] else 0.0
                    ),
                    "holdings_count": len(weights),
                    "asof_ts": asof_ts,
                }
            )

    return daily_rows


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

    return_rows: list[dict[str, Any]] = []
    for strategy in strategies:
        return_rows.extend(_build_returns_for_strategy(con, strategy, asof_ts))

    if not return_rows:
        return 0

    returns_df = pd.DataFrame(return_rows)
    returns_df = returns_df[
        [
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
            "asof_ts",
        ]
    ]
    _register_temp_df(con, "strategy_returns_df", returns_df)
    con.execute(
        """
        INSERT INTO gold.strategy_returns
        SELECT *
        FROM strategy_returns_df
        """
    )
    return int(len(returns_df))


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
        else (
            (cagr - benchmark_cagr)
            if cagr is not None and benchmark_cagr is not None
            else None
        )
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

    performance_rows: list[dict[str, Any]] = []
    for strategy in strategies:
        returns_df = con.execute(
            """
            SELECT *
            FROM gold.strategy_returns
            WHERE run_id = ?
            ORDER BY date
            """,
            [strategy.run_id],
        ).fetch_df()
        if returns_df.empty:
            continue
        performance_rows.append(
            _performance_row(strategy.run_id, strategy.strategy_id, returns_df, asof_ts)
        )

    if not performance_rows:
        return 0

    performance_df = pd.DataFrame(performance_rows)
    performance_df = performance_df[
        [
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
            "asof_ts",
        ]
    ]
    _register_temp_df(con, "strategy_performance_df", performance_df)
    con.execute(
        """
        INSERT INTO gold.strategy_performance
        SELECT *
        FROM strategy_performance_df
        """
    )
    return int(len(performance_df))


@asset(
    name="strategy_rankings",
    key_prefix=["gold"],
    deps=[
        silver_strategy_definitions,
        silver_strategy_parameters,
        silver_strategy_runs,
        silver_signals_daily,
        silver_universe_membership_daily,
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
    row_count = _materialize_rankings(con, strategies, asof_ts=asof_ts)
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
    row_count = _materialize_holdings(con, strategies, asof_ts=asof_ts)
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
    row_count = _materialize_returns(con, strategies, asof_ts=asof_ts)
    _log_strategy_return_continuity_check(
        measured_con=con,
        observability_con=context.resources.duckdb,
        strategies=strategies,
        run_id=_safe_run_id(context),
        job_name=_safe_job_name(context),
        partition_key=_safe_partition_key(context),
    )
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
    required_resource_keys={"research_duckdb"},
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
    row_count = _materialize_performance(con, strategies, asof_ts=asof_ts)
    _update_strategy_runs_success(con, strategies, asof_ts=asof_ts)
    context.add_output_metadata(
        {
            "table": "gold.strategy_performance",
            "strategy_count": len(strategies),
            "row_count": row_count,
        }
    )
