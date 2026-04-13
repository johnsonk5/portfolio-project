from datetime import date
from pathlib import Path
from typing import cast

import duckdb
import pandas as pd
import pytest
from dagster import AssetExecutionContext, build_asset_context

import portfolio_project.defs.research_db.gold.strategy as gold_strategy_module
import portfolio_project.defs.research_db.silver.strategy as silver_strategy_module

TEST_GOLD_STRATEGY_YAML = """
strategies:
  - strategy_id: benchmark_spy_buy_and_hold
    strategy_name: Buy And Hold SPY
    strategy_version: v1
    description: Benchmark strategy.
    ranking_method: single_asset_hold
    rebalance_frequency: Monthly
    target_count: 1
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2024-01-01
    end_date:
    is_active: true
    config:
      universe: benchmark_only
      selection_mode: fixed_symbol
    parameters:
      - parameter_name: symbol
        parameter_value: SPY
        parameter_type: string
        effective_start_date: 2024-01-01
        effective_end_date:
        is_active: true
        description: Benchmark symbol.
  - strategy_id: momentum_top_1
    strategy_name: Momentum Top 1
    strategy_version: v1
    description: Momentum strategy.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 1
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2024-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
      selection_mode: top_n
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2024-01-01
        effective_end_date:
        is_active: true
        description: Signal column.
      - parameter_name: ranking_direction
        parameter_value: desc
        parameter_type: string
        effective_start_date: 2024-01-01
        effective_end_date:
        is_active: true
        description: Ranking direction.
"""


def _describe_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE gold.{table_name}").fetchall()
    return [(str(row[0]), str(row[1]).upper()) for row in rows]


def _seed_research_inputs(con: duckdb.DuckDBPyConnection, data_root: Path) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    signals_df = pd.DataFrame(
        [
            {
                "date": "2024-01-31",
                "symbol": "SPY",
                "momentum_12_1": 0.0,
                "returns_5d": 0.0,
                "realized_vol_21d": 0.1,
                "avg_dollar_volume_21d": 10_000_000.0,
                "price_to_sma_200": 1.0,
            },
            {
                "date": "2024-01-31",
                "symbol": "AAA",
                "momentum_12_1": 0.9,
                "returns_5d": 0.05,
                "realized_vol_21d": 0.2,
                "avg_dollar_volume_21d": 5_000_000.0,
                "price_to_sma_200": 1.1,
            },
            {
                "date": "2024-01-31",
                "symbol": "BBB",
                "momentum_12_1": 0.2,
                "returns_5d": 0.01,
                "realized_vol_21d": 0.2,
                "avg_dollar_volume_21d": 5_000_000.0,
                "price_to_sma_200": 1.0,
            },
            {
                "date": "2024-02-29",
                "symbol": "SPY",
                "momentum_12_1": 0.0,
                "returns_5d": 0.0,
                "realized_vol_21d": 0.1,
                "avg_dollar_volume_21d": 10_000_000.0,
                "price_to_sma_200": 1.0,
            },
            {
                "date": "2024-02-29",
                "symbol": "AAA",
                "momentum_12_1": 0.1,
                "returns_5d": -0.03,
                "realized_vol_21d": 0.2,
                "avg_dollar_volume_21d": 5_000_000.0,
                "price_to_sma_200": 1.0,
            },
            {
                "date": "2024-02-29",
                "symbol": "BBB",
                "momentum_12_1": 0.8,
                "returns_5d": 0.04,
                "realized_vol_21d": 0.2,
                "avg_dollar_volume_21d": 5_000_000.0,
                "price_to_sma_200": 1.1,
            },
        ]
    )
    con.register("signals_df", signals_df)
    con.execute("CREATE TABLE silver.signals_daily AS SELECT * FROM signals_df")

    universe_df = pd.DataFrame(
        [
            {
                "member_date": "2024-01-31",
                "symbol": "AAA",
                "liquidity_rank": 1,
                "rolling_avg_dollar_volume": 5_000_000.0,
                "source": "test",
                "ingested_ts": "2024-01-31 00:00:00",
            },
            {
                "member_date": "2024-01-31",
                "symbol": "BBB",
                "liquidity_rank": 2,
                "rolling_avg_dollar_volume": 4_000_000.0,
                "source": "test",
                "ingested_ts": "2024-01-31 00:00:00",
            },
            {
                "member_date": "2024-02-29",
                "symbol": "AAA",
                "liquidity_rank": 2,
                "rolling_avg_dollar_volume": 4_000_000.0,
                "source": "test",
                "ingested_ts": "2024-02-29 00:00:00",
            },
            {
                "member_date": "2024-02-29",
                "symbol": "BBB",
                "liquidity_rank": 1,
                "rolling_avg_dollar_volume": 5_000_000.0,
                "source": "test",
                "ingested_ts": "2024-02-29 00:00:00",
            },
        ]
    )
    con.register("universe_df", universe_df)
    con.execute("CREATE TABLE silver.universe_membership_daily AS SELECT * FROM universe_df")

    prices_df = pd.DataFrame(
        [
            {"trade_date": "2024-01-31", "symbol": "SPY", "close": 100.0, "adjusted_close": 100.0},
            {"trade_date": "2024-02-01", "symbol": "SPY", "close": 102.0, "adjusted_close": 102.0},
            {"trade_date": "2024-02-29", "symbol": "SPY", "close": 102.0, "adjusted_close": 102.0},
            {
                "trade_date": "2024-03-01",
                "symbol": "SPY",
                "close": 100.98,
                "adjusted_close": 100.98,
            },
            {"trade_date": "2024-01-31", "symbol": "AAA", "close": 100.0, "adjusted_close": 100.0},
            {"trade_date": "2024-02-01", "symbol": "AAA", "close": 110.0, "adjusted_close": 110.0},
            {"trade_date": "2024-02-29", "symbol": "AAA", "close": 110.0, "adjusted_close": 110.0},
            {"trade_date": "2024-03-01", "symbol": "AAA", "close": 112.2, "adjusted_close": 112.2},
            {"trade_date": "2024-01-31", "symbol": "BBB", "close": 100.0, "adjusted_close": 100.0},
            {"trade_date": "2024-02-01", "symbol": "BBB", "close": 100.0, "adjusted_close": 100.0},
            {"trade_date": "2024-02-29", "symbol": "BBB", "close": 100.0, "adjusted_close": 100.0},
            {"trade_date": "2024-03-01", "symbol": "BBB", "close": 110.0, "adjusted_close": 110.0},
        ]
    )
    month_dir = data_root / "silver" / "research_daily_prices" / "month=2024-01"
    month_dir.mkdir(parents=True, exist_ok=True)
    prices_df.loc[prices_df["trade_date"] == "2024-01-31"].to_parquet(
        month_dir / "date=2024-01-31.parquet", index=False
    )
    month_dir_feb = data_root / "silver" / "research_daily_prices" / "month=2024-02"
    month_dir_feb.mkdir(parents=True, exist_ok=True)
    prices_df.loc[prices_df["trade_date"] == "2024-02-01"].to_parquet(
        month_dir_feb / "date=2024-02-01.parquet", index=False
    )
    prices_df.loc[prices_df["trade_date"] == "2024-02-29"].to_parquet(
        month_dir_feb / "date=2024-02-29.parquet", index=False
    )
    month_dir_mar = data_root / "silver" / "research_daily_prices" / "month=2024-03"
    month_dir_mar.mkdir(parents=True, exist_ok=True)
    prices_df.loc[prices_df["trade_date"] == "2024-03-01"].to_parquet(
        month_dir_mar / "date=2024-03-01.parquet", index=False
    )


def test_strategy_gold_assets_build_rankings_holdings_returns_and_performance(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_GOLD_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(silver_strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)
    monkeypatch.setattr(gold_strategy_module, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        gold_strategy_module,
        "PRICE_GLOB",
        (tmp_path / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet").as_posix(),
    )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})

    silver_strategy_module.silver_strategy_definitions(context)
    silver_strategy_module.silver_strategy_parameters(context)
    silver_strategy_module.silver_strategy_runs(context)
    _seed_research_inputs(con, tmp_path)

    gold_strategy_module.gold_strategy_rankings(context)
    gold_strategy_module.gold_strategy_holdings(context)
    gold_strategy_module.gold_strategy_returns(context)
    gold_strategy_module.gold_strategy_performance(context)

    assert _describe_columns(con, "strategy_rankings") == (
        gold_strategy_module.STRATEGY_RANKINGS_COLUMNS
    )
    assert _describe_columns(con, "strategy_holdings") == (
        gold_strategy_module.STRATEGY_HOLDINGS_COLUMNS
    )
    assert _describe_columns(con, "strategy_returns") == (
        gold_strategy_module.STRATEGY_RETURNS_COLUMNS
    )
    assert _describe_columns(con, "strategy_performance") == (
        gold_strategy_module.STRATEGY_PERFORMANCE_COLUMNS
    )

    ranking_rows = con.execute(
        """
        SELECT strategy_id, rebalance_date, symbol, rank, selected_flag
        FROM gold.strategy_rankings
        ORDER BY strategy_id, rebalance_date, rank
        """
    ).fetchall()
    assert ranking_rows == [
        ("benchmark_spy_buy_and_hold", date(2024, 1, 31), "SPY", 1, True),
        ("benchmark_spy_buy_and_hold", date(2024, 2, 29), "SPY", 1, True),
        ("momentum_top_1", date(2024, 1, 31), "AAA", 1, True),
        ("momentum_top_1", date(2024, 1, 31), "BBB", 2, False),
        ("momentum_top_1", date(2024, 2, 29), "BBB", 1, True),
        ("momentum_top_1", date(2024, 2, 29), "AAA", 2, False),
    ]

    holding_rows = con.execute(
        """
        SELECT strategy_id, rebalance_date, symbol, target_weight, entry_rank
        FROM gold.strategy_holdings
        ORDER BY strategy_id, rebalance_date
        """
    ).fetchall()
    assert holding_rows == [
        ("benchmark_spy_buy_and_hold", date(2024, 1, 31), "SPY", 1.0, 1),
        ("benchmark_spy_buy_and_hold", date(2024, 2, 29), "SPY", 1.0, 1),
        ("momentum_top_1", date(2024, 1, 31), "AAA", 1.0, 1),
        ("momentum_top_1", date(2024, 2, 29), "BBB", 1.0, 1),
    ]

    return_rows = con.execute(
        """
        SELECT
            strategy_id,
            date,
            round(portfolio_return, 4),
            round(benchmark_return, 4),
            holdings_count
        FROM gold.strategy_returns
        ORDER BY strategy_id, date
        """
    ).fetchall()
    assert return_rows == [
        ("benchmark_spy_buy_and_hold", date(2024, 2, 1), 0.02, 0.02, 1),
        ("benchmark_spy_buy_and_hold", date(2024, 3, 1), -0.01, -0.01, 1),
        ("momentum_top_1", date(2024, 2, 1), 0.1, 0.02, 1),
        ("momentum_top_1", date(2024, 3, 1), 0.1, -0.01, 1),
    ]

    performance_rows = con.execute(
        """
        SELECT strategy_id, round(benchmark_return, 4), round(alpha, 4)
        FROM gold.strategy_performance
        ORDER BY strategy_id
        """
    ).fetchall()
    assert performance_rows == [
        ("benchmark_spy_buy_and_hold", 0.0098, 0.0),
        ("momentum_top_1", 0.0098, 23.94),
    ]

    run_rows = con.execute(
        """
        SELECT
            strategy_id,
            run_status,
            rankings_row_count,
            holdings_row_count,
            returns_row_count,
            performance_row_count,
            completed_at IS NOT NULL,
            error_message
        FROM silver.strategy_runs
        WHERE strategy_id IN ('benchmark_spy_buy_and_hold', 'momentum_top_1')
        ORDER BY strategy_id
        """
    ).fetchall()
    assert run_rows == [
        ("benchmark_spy_buy_and_hold", "success", 2, 2, 2, 1, True, None),
        ("momentum_top_1", "success", 4, 2, 2, 1, True, None),
    ]

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_unique_symbol_per_rebalance'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_holdings_unique_symbol_per_rebalance", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_weight_sum_by_rebalance'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_holdings_weight_sum_by_rebalance", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_rankings_expected_rebalance_dates'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_rankings_expected_rebalance_dates", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_expected_rebalance_dates'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_holdings_expected_rebalance_dates", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_benchmark_series_present'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_returns_benchmark_series_present", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_expected_return_dates'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_returns_expected_return_dates", "PASS", 0.0)


def test_strategy_rankings_failure_updates_strategy_run_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_GOLD_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(silver_strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    silver_strategy_module.silver_strategy_definitions(context)
    silver_strategy_module.silver_strategy_runs(context)
    _seed_research_inputs(con, tmp_path)

    def _raise_materialization_error(*args, **kwargs):
        raise RuntimeError("ranking pipeline exploded")

    monkeypatch.setattr(
        gold_strategy_module,
        "_materialize_rankings",
        _raise_materialization_error,
    )

    with pytest.raises(RuntimeError, match="ranking pipeline exploded"):
        gold_strategy_module.gold_strategy_rankings(context)

    run_rows = con.execute(
        """
        SELECT
            strategy_id,
            run_status,
            completed_at IS NOT NULL,
            error_message,
            rankings_row_count,
            holdings_row_count,
            returns_row_count,
            performance_row_count
        FROM silver.strategy_runs
        ORDER BY strategy_id
        """
    ).fetchall()
    assert run_rows == [
        (
            "benchmark_spy_buy_and_hold",
            "failed",
            True,
            "ranking pipeline exploded",
            None,
            None,
            None,
            None,
        ),
        (
            "momentum_top_1",
            "failed",
            True,
            "ranking pipeline exploded",
            None,
            None,
            None,
            None,
        ),
    ]


def test_strategy_gold_assets_use_existing_upstream_run_ids_across_separate_runs(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_GOLD_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(silver_strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)
    monkeypatch.setattr(gold_strategy_module, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        gold_strategy_module,
        "PRICE_GLOB",
        (tmp_path / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet").as_posix(),
    )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    ranking_context = build_asset_context(resources={"research_duckdb": con})

    silver_strategy_module.silver_strategy_definitions(ranking_context)
    silver_strategy_module.silver_strategy_parameters(ranking_context)
    silver_strategy_module.silver_strategy_runs(ranking_context)
    _seed_research_inputs(con, tmp_path)

    monkeypatch.setattr(gold_strategy_module, "_safe_run_id", lambda _context: "run-rankings")
    gold_strategy_module.gold_strategy_rankings(ranking_context)

    downstream_context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    monkeypatch.setattr(gold_strategy_module, "_safe_run_id", lambda _context: "run-downstream")
    gold_strategy_module.gold_strategy_holdings(downstream_context)
    gold_strategy_module.gold_strategy_returns(downstream_context)
    gold_strategy_module.gold_strategy_performance(downstream_context)

    counts = con.execute(
        """
        SELECT
            (SELECT count(*) FROM gold.strategy_rankings),
            (SELECT count(*) FROM gold.strategy_holdings),
            (SELECT count(*) FROM gold.strategy_returns),
            (SELECT count(*) FROM gold.strategy_performance)
        """
    ).fetchone()
    assert counts == (6, 4, 4, 2)

    run_ids = con.execute(
        """
        SELECT DISTINCT run_id
        FROM gold.strategy_holdings
        ORDER BY run_id
        """
    ).fetchall()
    assert run_ids == [
        ("run-rankings:benchmark_spy_buy_and_hold",),
        ("run-rankings:momentum_top_1",),
    ]

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_unique_symbol_per_rebalance'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_holdings_unique_symbol_per_rebalance", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_weight_sum_by_rebalance'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_holdings_weight_sum_by_rebalance", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_rankings_expected_rebalance_dates'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_rankings_expected_rebalance_dates", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_expected_rebalance_dates'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_holdings_expected_rebalance_dates", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_benchmark_series_present'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_returns_benchmark_series_present", "PASS", 0.0)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_expected_return_dates'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_returns_expected_return_dates", "PASS", 0.0)


def test_strategy_performance_blocks_when_benchmark_series_is_missing_for_expected_dates(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_GOLD_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(silver_strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)
    monkeypatch.setattr(gold_strategy_module, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        gold_strategy_module,
        "PRICE_GLOB",
        (tmp_path / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet").as_posix(),
    )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})

    silver_strategy_module.silver_strategy_definitions(context)
    silver_strategy_module.silver_strategy_parameters(context)
    silver_strategy_module.silver_strategy_runs(context)
    _seed_research_inputs(con, tmp_path)

    month_dir_mar = tmp_path / "silver" / "research_daily_prices" / "month=2024-03"
    month_dir_mar.mkdir(parents=True, exist_ok=True)
    prices_df = pd.DataFrame(
        [
            {"trade_date": "2024-03-01", "symbol": "AAA", "close": 112.2, "adjusted_close": 112.2},
            {"trade_date": "2024-03-01", "symbol": "BBB", "close": 110.0, "adjusted_close": 110.0},
        ]
    )
    prices_df.to_parquet(month_dir_mar / "date=2024-03-01.parquet", index=False)

    gold_strategy_module.gold_strategy_rankings(context)
    gold_strategy_module.gold_strategy_holdings(context)
    gold_strategy_module.gold_strategy_returns(context)

    with pytest.raises(
        ValueError,
        match="Missing benchmark series values for one or more strategy return dates",
    ):
        gold_strategy_module.gold_strategy_performance(context)

    dq_row = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_benchmark_series_present'
        """
    ).fetchone()
    assert dq_row == ("dq_gold_strategy_returns_benchmark_series_present", "FAIL", 2.0)

    details_row = obs_con.execute(
        """
        SELECT details_json
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_benchmark_series_present'
        """
    ).fetchone()
    assert details_row is not None
    details_json = str(details_row[0])
    assert '"benchmark_symbol": "SPY"' in details_json
    assert '"null_benchmark_value_dates": ["2024-03-01"]' in details_json

    assert gold_strategy_module._table_exists(con, "gold", "strategy_performance") is False

    run_rows = con.execute(
        """
        SELECT strategy_id, run_status, error_message
        FROM silver.strategy_runs
        WHERE strategy_id IN ('benchmark_spy_buy_and_hold', 'momentum_top_1')
        ORDER BY strategy_id
        """
    ).fetchall()
    assert run_rows == [
        (
            "benchmark_spy_buy_and_hold",
            "failed",
            "Missing benchmark series values for one or more strategy return dates; "
            "strategy comparison outputs were not materialized.",
        ),
        (
            "momentum_top_1",
            "failed",
            "Missing benchmark series values for one or more strategy return dates; "
            "strategy comparison outputs were not materialized.",
        ),
    ]


def test_strategy_holdings_weight_sum_dq_check_fails_when_rebalance_weights_do_not_sum_to_one() -> (
    None
):
    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE gold.strategy_holdings (
            run_id VARCHAR,
            strategy_id VARCHAR,
            rebalance_date DATE,
            symbol VARCHAR,
            target_weight DOUBLE,
            side VARCHAR,
            entry_rank INTEGER,
            signal_value DOUBLE,
            asof_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.strategy_holdings (
            run_id,
            strategy_id,
            rebalance_date,
            symbol,
            target_weight,
            side,
            entry_rank,
            signal_value,
            asof_ts
        )
        VALUES
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-01-31',
                'AAA',
                0.60,
                'LONG',
                1,
                1.0,
                current_timestamp
            ),
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-01-31',
                'BBB',
                0.35,
                'LONG',
                2,
                0.9,
                current_timestamp
            ),
            (
                'run-2:strategy_b',
                'strategy_b',
                '2024-01-31',
                'CCC',
                1.00,
                'LONG',
                1,
                0.8,
                current_timestamp
            )
        """
    )

    strategies = [
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_a",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=2,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-1:strategy_a",
        ),
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_b",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=1,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-2:strategy_b",
        ),
    ]

    gold_strategy_module._log_holdings_weight_sum_check(
        measured_con=con,
        observability_con=obs_con,
        strategies=strategies,
        run_id="strategy-holdings-run",
        job_name="strategy_holdings_job",
        partition_key=None,
    )

    dq_row = obs_con.execute(
        """
        SELECT status, measured_value, threshold_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_weight_sum_by_rebalance'
        """
    ).fetchone()
    assert dq_row == ("FAIL", 1.0, 0.0)

    details_row = obs_con.execute(
        """
        SELECT details_json
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_weight_sum_by_rebalance'
        """
    ).fetchone()
    assert details_row is not None
    details_json = str(details_row[0])
    assert '"strategy_id": "strategy_a"' in details_json
    assert '"weight_sum": 0.95' in details_json


def test_strategy_holdings_duplicate_symbol_dq_check_fails_for_same_rebalance_symbol() -> None:
    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE gold.strategy_holdings (
            run_id VARCHAR,
            strategy_id VARCHAR,
            rebalance_date DATE,
            symbol VARCHAR,
            target_weight DOUBLE,
            side VARCHAR,
            entry_rank INTEGER,
            signal_value DOUBLE,
            asof_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.strategy_holdings (
            run_id,
            strategy_id,
            rebalance_date,
            symbol,
            target_weight,
            side,
            entry_rank,
            signal_value,
            asof_ts
        )
        VALUES
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-01-31',
                'AAA',
                0.50,
                'LONG',
                1,
                1.0,
                current_timestamp
            ),
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-01-31',
                'AAA',
                0.50,
                'LONG',
                2,
                0.9,
                current_timestamp
            ),
            (
                'run-2:strategy_b',
                'strategy_b',
                '2024-01-31',
                'BBB',
                1.00,
                'LONG',
                1,
                0.8,
                current_timestamp
            )
        """
    )

    strategies = [
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_a",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=2,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-1:strategy_a",
        ),
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_b",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=1,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-2:strategy_b",
        ),
    ]

    gold_strategy_module._log_holdings_duplicate_symbol_check(
        measured_con=con,
        observability_con=obs_con,
        strategies=strategies,
        run_id="strategy-holdings-run",
        job_name="strategy_holdings_job",
        partition_key=None,
    )

    dq_row = obs_con.execute(
        """
        SELECT status, measured_value, threshold_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_unique_symbol_per_rebalance'
        """
    ).fetchone()
    assert dq_row == ("FAIL", 1.0, 0.0)

    details_row = obs_con.execute(
        """
        SELECT details_json
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_unique_symbol_per_rebalance'
        """
    ).fetchone()
    assert details_row is not None
    details_json = str(details_row[0])
    assert '"uniqueness_scope": ["strategy_id", "rebalance_date", "symbol"]' in details_json
    assert '"key_columns": ["run_id", "strategy_id", "rebalance_date", "symbol"]' in details_json


def test_strategy_rankings_expected_rebalance_dates_dq_check_fails_for_missing_month() -> None:
    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE silver.signals_daily AS
        SELECT *
        FROM (
            VALUES
                ('2024-01-31', 'AAA'),
                ('2024-02-29', 'AAA')
        ) AS t(date, symbol)
        """
    )
    con.execute(
        """
        CREATE TABLE gold.strategy_rankings (
            run_id VARCHAR,
            strategy_id VARCHAR,
            rebalance_date DATE,
            symbol VARCHAR,
            score DOUBLE,
            rank INTEGER,
            selected_flag BOOLEAN,
            asof_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.strategy_rankings (
            run_id,
            strategy_id,
            rebalance_date,
            symbol,
            score,
            rank,
            selected_flag,
            asof_ts
        )
        VALUES (
            'run-1:strategy_a',
            'strategy_a',
            '2024-01-31',
            'AAA',
            1.0,
            1,
            TRUE,
            current_timestamp
        )
        """
    )

    strategies = [
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_a",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=1,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-1:strategy_a",
        )
    ]

    gold_strategy_module._log_rebalance_dates_present_check(
        measured_con=con,
        observability_con=obs_con,
        strategies=strategies,
        table_name="strategy_rankings",
        check_name="dq_gold_strategy_rankings_expected_rebalance_dates",
        run_id="strategy-holdings-run",
        job_name="strategy_holdings_job",
        partition_key=None,
    )

    dq_row = obs_con.execute(
        """
        SELECT status, measured_value, threshold_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_rankings_expected_rebalance_dates'
        """
    ).fetchone()
    assert dq_row == ("FAIL", 1.0, 0.0)

    details_row = obs_con.execute(
        """
        SELECT details_json
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_rankings_expected_rebalance_dates'
        """
    ).fetchone()
    assert details_row is not None
    details_json = str(details_row[0])
    assert '"missing_rebalance_dates": ["2024-02-29"]' in details_json
    assert '"table": "gold.strategy_rankings"' in details_json


def test_strategy_holdings_expected_rebalance_dates_dq_check_fails_for_unexpected_month() -> None:
    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE silver.signals_daily AS
        SELECT *
        FROM (
            VALUES
                ('2024-01-31', 'AAA'),
                ('2024-02-29', 'AAA')
        ) AS t(date, symbol)
        """
    )
    con.execute(
        """
        CREATE TABLE gold.strategy_holdings (
            run_id VARCHAR,
            strategy_id VARCHAR,
            rebalance_date DATE,
            symbol VARCHAR,
            target_weight DOUBLE,
            side VARCHAR,
            entry_rank INTEGER,
            signal_value DOUBLE,
            asof_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.strategy_holdings (
            run_id,
            strategy_id,
            rebalance_date,
            symbol,
            target_weight,
            side,
            entry_rank,
            signal_value,
            asof_ts
        )
        VALUES
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-01-31',
                'AAA',
                1.0,
                'LONG',
                1,
                1.0,
                current_timestamp
            ),
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-02-29',
                'AAA',
                1.0,
                'LONG',
                1,
                1.0,
                current_timestamp
            ),
            (
                'run-1:strategy_a',
                'strategy_a',
                '2024-03-31',
                'AAA',
                1.0,
                'LONG',
                1,
                1.0,
                current_timestamp
            )
        """
    )

    strategies = [
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_a",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=1,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-1:strategy_a",
        )
    ]

    gold_strategy_module._log_rebalance_dates_present_check(
        measured_con=con,
        observability_con=obs_con,
        strategies=strategies,
        table_name="strategy_holdings",
        check_name="dq_gold_strategy_holdings_expected_rebalance_dates",
        run_id="strategy-holdings-run",
        job_name="strategy_holdings_job",
        partition_key=None,
    )

    dq_row = obs_con.execute(
        """
        SELECT status, measured_value, threshold_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_expected_rebalance_dates'
        """
    ).fetchone()
    assert dq_row == ("FAIL", 1.0, 0.0)

    details_row = obs_con.execute(
        """
        SELECT details_json
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_holdings_expected_rebalance_dates'
        """
    ).fetchone()
    assert details_row is not None
    details_json = str(details_row[0])
    assert '"unexpected_rebalance_dates": ["2024-03-31"]' in details_json
    assert '"table": "gold.strategy_holdings"' in details_json


def test_strategy_returns_expected_dates_dq_check_fails_for_missing_trading_day(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    holdings_df = pd.DataFrame(
        [
            {
                "run_id": "run-1:strategy_a",
                "strategy_id": "strategy_a",
                "rebalance_date": "2024-01-31",
                "symbol": "AAA",
                "target_weight": 1.0,
                "side": "LONG",
                "entry_rank": 1,
                "signal_value": 1.0,
                "asof_ts": pd.Timestamp("2024-01-31"),
            },
            {
                "run_id": "run-1:strategy_a",
                "strategy_id": "strategy_a",
                "rebalance_date": "2024-02-29",
                "symbol": "AAA",
                "target_weight": 1.0,
                "side": "LONG",
                "entry_rank": 1,
                "signal_value": 1.0,
                "asof_ts": pd.Timestamp("2024-02-29"),
            },
        ]
    )
    con.register("holdings_df", holdings_df)
    con.execute("CREATE TABLE gold.strategy_holdings AS SELECT * FROM holdings_df")

    returns_df = pd.DataFrame(
        [
            {
                "run_id": "run-1:strategy_a",
                "strategy_id": "strategy_a",
                "date": "2024-02-01",
                "portfolio_return": 0.01,
                "benchmark_return": 0.0,
                "excess_return": 0.01,
                "cumulative_return": 0.01,
                "drawdown": 0.0,
                "turnover": 1.0,
                "holdings_count": 1,
                "asof_ts": pd.Timestamp("2024-02-01"),
            }
        ]
    )
    con.register("returns_df", returns_df)
    con.execute("CREATE TABLE gold.strategy_returns AS SELECT * FROM returns_df")

    strategies = [
        gold_strategy_module.StrategyConfig(
            strategy_id="strategy_a",
            rebalance_frequency="Monthly",
            benchmark_symbol="SPY",
            target_count=1,
            weighting_method="equal",
            long_short_flag=False,
            start_date=date(2024, 1, 1),
            end_date=None,
            config={},
            run_id="run-1:strategy_a",
        )
    ]

    with pytest.MonkeyPatch.context() as monkeypatch:
        trading_dates_df = pd.DataFrame(
            [
                {
                    "trade_date": "2024-02-01",
                    "symbol": "AAA",
                    "close": 100.0,
                    "adjusted_close": 100.0,
                },
                {
                    "trade_date": "2024-02-02",
                    "symbol": "AAA",
                    "close": 101.0,
                    "adjusted_close": 101.0,
                },
                {
                    "trade_date": "2024-02-29",
                    "symbol": "AAA",
                    "close": 102.0,
                    "adjusted_close": 102.0,
                },
                {
                    "trade_date": "2024-03-01",
                    "symbol": "AAA",
                    "close": 103.0,
                    "adjusted_close": 103.0,
                },
            ]
        )
        temp_dir = tmp_path
        month_dir_feb = temp_dir / "silver" / "research_daily_prices" / "month=2024-02"
        month_dir_feb.mkdir(parents=True, exist_ok=True)
        trading_dates_df.loc[
            trading_dates_df["trade_date"].isin(["2024-02-01", "2024-02-02"])
        ].to_parquet(month_dir_feb / "date=2024-02-01.parquet", index=False)
        trading_dates_df.loc[trading_dates_df["trade_date"] == "2024-02-29"].to_parquet(
            month_dir_feb / "date=2024-02-29.parquet", index=False
        )
        month_dir_mar = temp_dir / "silver" / "research_daily_prices" / "month=2024-03"
        month_dir_mar.mkdir(parents=True, exist_ok=True)
        trading_dates_df.loc[trading_dates_df["trade_date"] == "2024-03-01"].to_parquet(
            month_dir_mar / "date=2024-03-01.parquet", index=False
        )
        monkeypatch.setattr(
            gold_strategy_module,
            "PRICE_GLOB",
            (
                temp_dir / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet"
            ).as_posix(),
        )

        gold_strategy_module._log_strategy_return_continuity_check(
            measured_con=con,
            observability_con=obs_con,
            strategies=strategies,
            run_id="strategy-returns-run",
            job_name="strategy_returns_job",
            partition_key=None,
        )

    dq_row = obs_con.execute(
        """
        SELECT status, measured_value, threshold_value
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_expected_return_dates'
        """
    ).fetchone()
    assert dq_row == ("FAIL", 2.0, 0.0)

    details_row = obs_con.execute(
        """
        SELECT details_json
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_gold_strategy_returns_expected_return_dates'
        """
    ).fetchone()
    assert details_row is not None
    details_json = str(details_row[0])
    assert '"missing_return_dates": ["2024-02-02", "2024-03-01"]' in details_json
    assert '"table": "gold.strategy_returns"' in details_json


def test_daily_symbol_returns_drops_non_positive_and_extreme_price_jumps() -> None:
    price_df = pd.DataFrame(
        [
            {"trade_date": "2011-04-06", "symbol": "IVL", "price": 0.0},
            {"trade_date": "2011-04-07", "symbol": "IVL", "price": 55.0},
            {"trade_date": "2003-08-04", "symbol": "ISA", "price": 0.02},
            {"trade_date": "2003-08-05", "symbol": "ISA", "price": 9630.0},
            {"trade_date": "2024-01-02", "symbol": "AAA", "price": 100.0},
            {"trade_date": "2024-01-03", "symbol": "AAA", "price": 110.0},
        ]
    )

    returns_df = gold_strategy_module._daily_symbol_returns(price_df)
    rows = {
        (str(row.symbol), str(row.trade_date)): row.asset_return
        for row in returns_df.itertuples(index=False)
    }

    assert pd.isna(rows[("IVL", "2011-04-07")])
    assert pd.isna(rows[("ISA", "2003-08-05")])
    assert rows[("AAA", "2024-01-03")] == pytest.approx(0.1)


def test_strategies_for_missing_backfill_job_filters_completed_strategies(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_GOLD_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(silver_strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    silver_context = build_asset_context(resources={"research_duckdb": con})
    silver_strategy_module.silver_strategy_definitions(silver_context)

    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(
        """
        CREATE TABLE gold.strategy_performance (
            run_id VARCHAR,
            strategy_id VARCHAR,
            cagr DOUBLE,
            sharpe_ratio DOUBLE,
            sortino_ratio DOUBLE,
            max_drawdown DOUBLE,
            annualized_volatility DOUBLE,
            hit_rate DOUBLE,
            turnover_avg DOUBLE,
            benchmark_return DOUBLE,
            alpha DOUBLE,
            asof_ts TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.strategy_performance (run_id, strategy_id, asof_ts)
        VALUES ('existing-run', 'benchmark_spy_buy_and_hold', current_timestamp)
        """
    )

    class DummyContext:
        job_name = gold_strategy_module.MISSING_STRATEGIES_JOB_NAME

    strategies = gold_strategy_module._strategies_for_context(
        con,
        cast(AssetExecutionContext, DummyContext()),
        source_table=None,
    )

    assert [strategy.strategy_id for strategy in strategies] == ["momentum_top_1"]


def test_composite_underrated_momentum_ranking_requires_positive_momentum() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE silver.signals_daily AS
        SELECT *
        FROM (
            VALUES
                ('2024-01-31', 'AAA', 0.9, 0.40, 5000000.0),
                ('2024-01-31', 'BBB', 0.4, 0.10, 5000000.0),
                ('2024-01-31', 'CCC', -0.2, 0.80, 5000000.0)
        ) AS t(date, symbol, momentum_12_1, pct_below_52w_high, avg_dollar_volume_21d)
        """
    )
    con.execute(
        """
        CREATE TABLE silver.universe_membership_daily AS
        SELECT *
        FROM (
            VALUES
                ('2024-01-31', 'AAA', 1, 5000000.0, 'test', current_timestamp),
                ('2024-01-31', 'BBB', 2, 5000000.0, 'test', current_timestamp),
                ('2024-01-31', 'CCC', 3, 5000000.0, 'test', current_timestamp)
        ) AS t(member_date, symbol, liquidity_rank, rolling_avg_dollar_volume, source, ingested_ts)
        """
    )
    strategy_parameters_df = pd.DataFrame(
        [
            {
                "strategy_id": "underrated_momentum_top_10",
                "parameter_name": "signal_column",
                "parameter_value": "momentum_12_1",
                "parameter_type": "string",
                "effective_start_date": "2000-01-01",
                "effective_end_date": None,
                "is_active": True,
                "description": "",
                "ingest_ts": pd.Timestamp("2024-01-01"),
                "asof_ts": pd.Timestamp("2024-01-01"),
                "run_id": "run-001",
            },
            {
                "strategy_id": "underrated_momentum_top_10",
                "parameter_name": "secondary_signal_column",
                "parameter_value": "pct_below_52w_high",
                "parameter_type": "string",
                "effective_start_date": "2000-01-01",
                "effective_end_date": None,
                "is_active": True,
                "description": "",
                "ingest_ts": pd.Timestamp("2024-01-01"),
                "asof_ts": pd.Timestamp("2024-01-01"),
                "run_id": "run-001",
            },
            {
                "strategy_id": "underrated_momentum_top_10",
                "parameter_name": "score_method",
                "parameter_value": "zscore_sum",
                "parameter_type": "string",
                "effective_start_date": "2000-01-01",
                "effective_end_date": None,
                "is_active": True,
                "description": "",
                "ingest_ts": pd.Timestamp("2024-01-01"),
                "asof_ts": pd.Timestamp("2024-01-01"),
                "run_id": "run-001",
            },
            {
                "strategy_id": "underrated_momentum_top_10",
                "parameter_name": "ranking_direction",
                "parameter_value": "desc",
                "parameter_type": "string",
                "effective_start_date": "2000-01-01",
                "effective_end_date": None,
                "is_active": True,
                "description": "",
                "ingest_ts": pd.Timestamp("2024-01-01"),
                "asof_ts": pd.Timestamp("2024-01-01"),
                "run_id": "run-001",
            },
            {
                "strategy_id": "underrated_momentum_top_10",
                "parameter_name": "min_momentum_12_1",
                "parameter_value": "0",
                "parameter_type": "double",
                "effective_start_date": "2000-01-01",
                "effective_end_date": None,
                "is_active": True,
                "description": "",
                "ingest_ts": pd.Timestamp("2024-01-01"),
                "asof_ts": pd.Timestamp("2024-01-01"),
                "run_id": "run-001",
            },
            {
                "strategy_id": "underrated_momentum_top_10",
                "parameter_name": "min_avg_dollar_volume_21d",
                "parameter_value": "1000000",
                "parameter_type": "int",
                "effective_start_date": "2000-01-01",
                "effective_end_date": None,
                "is_active": True,
                "description": "",
                "ingest_ts": pd.Timestamp("2024-01-01"),
                "asof_ts": pd.Timestamp("2024-01-01"),
                "run_id": "run-001",
            },
        ]
    )
    con.register("strategy_parameters_df", strategy_parameters_df)
    con.execute("CREATE TABLE silver.strategy_parameters AS SELECT * FROM strategy_parameters_df")

    strategy = gold_strategy_module.StrategyConfig(
        strategy_id="underrated_momentum_top_10",
        rebalance_frequency="Monthly",
        benchmark_symbol="SPY",
        target_count=10,
        weighting_method="equal",
        long_short_flag=False,
        start_date=date(2024, 1, 1),
        end_date=None,
        config={"universe": "universe_membership_daily", "selection_mode": "top_n"},
        run_id="run-001",
    )

    ranking_rows = gold_strategy_module._build_rankings_for_strategy(
        con,
        strategy,
        gold_strategy_module._now_utc_naive(),
    )

    ranked_symbols = [row["symbol"] for row in ranking_rows]
    assert ranked_symbols == ["AAA", "BBB"]
    assert all(row["score"] is not None for row in ranking_rows)
