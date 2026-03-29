from pathlib import Path

import duckdb
import pytest
from dagster import build_asset_context

import portfolio_project.defs.research_db.silver.strategy as strategy_module

TEST_STRATEGY_YAML = """
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
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: benchmark_only
    parameters:
      - parameter_name: symbol
        parameter_value: SPY
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
        description: Held benchmark symbol.
  - strategy_id: momentum_top_10
    strategy_name: Momentum Top 10
    strategy_version: v1
    description: Momentum strategy.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 10
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
        description: Ranking field.
      - parameter_name: ranking_direction
        parameter_value: desc
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
        description: Sort order.
"""


def _describe_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE silver.{table_name}").fetchall()
    return [(str(row[0]), str(row[1]).upper()) for row in rows]


def test_strategy_assets_seed_definitions_and_parameters_from_yaml(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    strategy_module.silver_strategy_definitions(context)
    strategy_module.silver_strategy_parameters(context)
    strategy_module.silver_strategy_runs(context)

    assert _describe_columns(con, "strategy_definitions") == (
        strategy_module.STRATEGY_DEFINITIONS_COLUMNS
    )
    assert _describe_columns(con, "strategy_parameters") == (
        strategy_module.STRATEGY_PARAMETERS_COLUMNS
    )
    assert _describe_columns(con, "strategy_runs") == strategy_module.STRATEGY_RUNS_COLUMNS

    definition_rows = con.execute(
        """
        SELECT strategy_id, strategy_name, ranking_method, target_count, benchmark_symbol
        FROM silver.strategy_definitions
        ORDER BY strategy_id
        """
    ).fetchall()
    assert definition_rows == [
        (
            "benchmark_spy_buy_and_hold",
            "Buy And Hold SPY",
            "single_asset_hold",
            1,
            "SPY",
        ),
        ("momentum_top_10", "Momentum Top 10", "momentum_12_1_desc", 10, "SPY"),
    ]

    parameter_rows = con.execute(
        """
        SELECT strategy_id, parameter_name, parameter_value, parameter_type
        FROM silver.strategy_parameters
        ORDER BY strategy_id, parameter_name
        """
    ).fetchall()
    assert parameter_rows == [
        ("benchmark_spy_buy_and_hold", "symbol", "SPY", "string"),
        ("momentum_top_10", "ranking_direction", "desc", "string"),
        ("momentum_top_10", "signal_column", "momentum_12_1", "string"),
    ]


def test_strategy_runs_asset_preserves_existing_run_rows(tmp_path: Path, monkeypatch) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(TEST_STRATEGY_YAML, encoding="utf-8")
    monkeypatch.setattr(strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    strategy_module.silver_strategy_definitions(context)
    strategy_module.silver_strategy_runs(context)
    con.execute(
        """
        INSERT INTO silver.strategy_runs (
            run_id,
            strategy_id,
            run_status,
            dataset_version,
            persist
        )
        VALUES ('run-001', 'benchmark_spy_buy_and_hold', 'success', 'snapshot-001', true)
        """
    )

    strategy_module.silver_strategy_runs(context)

    run_rows = con.execute(
        """
        SELECT run_id, strategy_id, run_status, dataset_version, persist
        FROM silver.strategy_runs
        """
    ).fetchall()
    assert run_rows == [
        (
            "run-001",
            "benchmark_spy_buy_and_hold",
            "success",
            "snapshot-001",
            True,
        )
    ]


@pytest.mark.parametrize(
    ("catalog_yaml", "error_message"),
    [
        (
            """
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
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: benchmark_only
    parameters:
      - parameter_name: rebalance_buffer_bps
        parameter_value: "0"
        parameter_type: int
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
""",
            "missing required parameters for single_asset_hold: symbol",
        ),
        (
            """
strategies:
  - strategy_id: momentum_top_10
    strategy_name: Momentum Top 10
    strategy_version: v1
    description: Momentum strategy.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 10
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
""",
            "missing required parameters for momentum_12_1_desc: ranking_direction",
        ),
    ],
)
def test_strategy_definitions_require_expected_parameter_names(
    tmp_path: Path, monkeypatch, catalog_yaml: str, error_message: str
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(catalog_yaml, encoding="utf-8")
    monkeypatch.setattr(strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    with pytest.raises(ValueError, match=error_message):
        strategy_module.silver_strategy_definitions(context)


@pytest.mark.parametrize(
    ("catalog_yaml", "error_message"),
    [
        (
            """
strategies:
  - strategy_id: momentum_top_10
    strategy_name: Momentum Top 10
    strategy_version: v1
    description: Momentum strategy.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 0
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
      - parameter_name: ranking_direction
        parameter_value: desc
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
""",
            "target_count must be greater than 0",
        ),
        (
            """
strategies:
  - strategy_id: low_volatility_top_10
    strategy_name: Low Volatility Top 10
    strategy_version: v1
    description: Low volatility strategy.
    ranking_method: realized_vol_21d_asc
    rebalance_frequency: Monthly
    target_count: 10
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
    parameters:
      - parameter_name: signal_column
        parameter_value: realized_vol_21d
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
      - parameter_name: ranking_direction
        parameter_value: sideways
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
""",
            "parameter ranking_direction must be one of: asc, desc",
        ),
    ],
)
def test_strategy_definitions_validate_value_ranges(
    tmp_path: Path, monkeypatch, catalog_yaml: str, error_message: str
) -> None:
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(catalog_yaml, encoding="utf-8")
    monkeypatch.setattr(strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    with pytest.raises(ValueError, match=error_message):
        strategy_module.silver_strategy_definitions(context)


def test_strategy_definitions_reject_conflicting_name_version_pairs(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_yaml = """
strategies:
  - strategy_id: momentum_top_10
    strategy_name: Momentum Top 10
    strategy_version: v1
    description: Momentum strategy.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 10
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
      - parameter_name: ranking_direction
        parameter_value: desc
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
  - strategy_id: momentum_top_10_clone
    strategy_name: Momentum Top 10
    strategy_version: v1
    description: Duplicate strategy identity.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 20
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2000-01-01
    end_date:
    is_active: true
    config:
      universe: universe_membership_daily
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
      - parameter_name: ranking_direction
        parameter_value: desc
        parameter_type: string
        effective_start_date: 2000-01-01
        effective_end_date:
        is_active: true
"""
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(catalog_yaml, encoding="utf-8")
    monkeypatch.setattr(strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    with pytest.raises(
        ValueError,
        match="Conflicting strategy definitions in strategy catalog",
    ):
        strategy_module.silver_strategy_definitions(context)
