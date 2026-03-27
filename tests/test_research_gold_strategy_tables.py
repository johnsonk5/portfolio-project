from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from dagster import build_asset_context

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
    context = build_asset_context(resources={"research_duckdb": con})

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
        SELECT strategy_id, run_status, completed_at IS NOT NULL
        FROM silver.strategy_runs
        WHERE strategy_id IN ('benchmark_spy_buy_and_hold', 'momentum_top_1')
        ORDER BY strategy_id
        """
    ).fetchall()
    assert run_rows == [
        ("benchmark_spy_buy_and_hold", "success", True),
        ("momentum_top_1", "success", True),
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
    ranking_context = build_asset_context(resources={"research_duckdb": con})

    silver_strategy_module.silver_strategy_definitions(ranking_context)
    silver_strategy_module.silver_strategy_parameters(ranking_context)
    silver_strategy_module.silver_strategy_runs(ranking_context)
    _seed_research_inputs(con, tmp_path)

    monkeypatch.setattr(gold_strategy_module, "_safe_run_id", lambda _context: "run-rankings")
    gold_strategy_module.gold_strategy_rankings(ranking_context)

    downstream_context = build_asset_context(resources={"research_duckdb": con})
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
