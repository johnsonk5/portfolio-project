from pathlib import Path

import duckdb
import pytest
from dagster import build_asset_context

import portfolio_project.defs.research_db.ref.simulation as simulation_ref_module

TEST_SIMULATION_REFERENCE_YAML = """
simulation_types:
  - simulation_type_id: 1
    simulation_type_code: close_no_cost
    description: Same-day close-fill backtest.
    fill_price_basis: close
    slippage_model: none
    slippage_bps: 0
    commission_model: none
    lookahead_safe_flag: false
    is_active: true
  - simulation_type_id: 2
    simulation_type_code: next_open_fixed_bps
    description: Next-open fill with fixed slippage.
    fill_price_basis: next_open
    slippage_model: fixed_bps
    slippage_bps: 5
    commission_model: none
    lookahead_safe_flag: true
    is_active: true
run_types:
  - run_type_code: backtest
    description: Historical replay.
    is_active: true
  - run_type_code: simulation
    description: Dry run.
    is_active: true
  - run_type_code: paper
    description: Paper trading.
    is_active: true
  - run_type_code: live
    description: Live trading.
    is_active: true
"""


def _describe_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE ref.{table_name}").fetchall()
    return [(str(row[0]), str(row[1]).upper()) for row in rows]


def test_simulation_reference_assets_seed_ref_tables_from_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog_path = tmp_path / "simulation_reference.yaml"
    catalog_path.write_text(TEST_SIMULATION_REFERENCE_YAML, encoding="utf-8")
    monkeypatch.setattr(simulation_ref_module, "SIMULATION_REFERENCE_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})

    simulation_ref_module.ref_simulation_types(context)
    simulation_ref_module.ref_run_types(context)

    assert _describe_columns(con, "simulation_types") == (
        simulation_ref_module.SIMULATION_TYPES_COLUMNS
    )
    assert _describe_columns(con, "run_types") == simulation_ref_module.RUN_TYPES_COLUMNS

    simulation_rows = con.execute(
        """
        SELECT
            simulation_type_id,
            simulation_type_code,
            fill_price_basis,
            slippage_model,
            slippage_bps,
            lookahead_safe_flag,
            is_active
        FROM ref.simulation_types
        ORDER BY simulation_type_id
        """
    ).fetchall()
    assert simulation_rows == [
        (1, "close_no_cost", "close", "none", 0.0, False, True),
        (2, "next_open_fixed_bps", "next_open", "fixed_bps", 5.0, True, True),
    ]

    run_type_rows = con.execute(
        """
        SELECT run_type_code, is_active
        FROM ref.run_types
        ORDER BY run_type_code
        """
    ).fetchall()
    assert run_type_rows == [
        ("backtest", True),
        ("live", True),
        ("paper", True),
        ("simulation", True),
    ]


@pytest.mark.parametrize(
    ("catalog_yaml", "asset_name", "error_message"),
    [
        (
            """
simulation_types:
  - simulation_type_id: 1
    simulation_type_code: bad_fill
    description: Bad fill basis.
    fill_price_basis: midpoint
    slippage_model: none
    slippage_bps: 0
    commission_model: none
    lookahead_safe_flag: true
    is_active: true
run_types:
  - run_type_code: backtest
    description: Historical replay.
    is_active: true
  - run_type_code: simulation
    description: Dry run.
    is_active: true
  - run_type_code: paper
    description: Paper trading.
    is_active: true
  - run_type_code: live
    description: Live trading.
    is_active: true
""",
            "simulation_types",
            "fill_price_basis must be one of",
        ),
        (
            """
simulation_types:
  - simulation_type_id: 1
    simulation_type_code: close_no_cost
    description: Same-day close-fill backtest.
    fill_price_basis: close
    slippage_model: none
    slippage_bps: 0
    commission_model: none
    lookahead_safe_flag: false
    is_active: true
run_types:
  - run_type_code: backtest
    description: Historical replay.
    is_active: true
""",
            "run_types",
            "missing required run_type_code values",
        ),
    ],
)
def test_simulation_reference_assets_validate_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    catalog_yaml: str,
    asset_name: str,
    error_message: str,
) -> None:
    catalog_path = tmp_path / "simulation_reference.yaml"
    catalog_path.write_text(catalog_yaml, encoding="utf-8")
    monkeypatch.setattr(simulation_ref_module, "SIMULATION_REFERENCE_PATH", catalog_path)

    con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con})
    asset_fn = getattr(simulation_ref_module, f"ref_{asset_name}")

    with pytest.raises(ValueError, match=error_message):
        asset_fn(context)
