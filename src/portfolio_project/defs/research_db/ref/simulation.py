from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dagster import AssetExecutionContext, asset

from portfolio_project.defs.research_db.silver.strategy import (
    _replace_table_from_df,
    _table_metadata,
)

SIMULATION_REFERENCE_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "simulation_reference.yaml"
)

SIMULATION_TYPES_COLUMNS: list[tuple[str, str]] = [
    ("simulation_type_id", "INTEGER"),
    ("simulation_type_code", "VARCHAR"),
    ("description", "VARCHAR"),
    ("fill_price_basis", "VARCHAR"),
    ("slippage_model", "VARCHAR"),
    ("slippage_bps", "DOUBLE"),
    ("commission_model", "VARCHAR"),
    ("lookahead_safe_flag", "BOOLEAN"),
    ("is_active", "BOOLEAN"),
]

RUN_TYPES_COLUMNS: list[tuple[str, str]] = [
    ("run_type_code", "VARCHAR"),
    ("description", "VARCHAR"),
    ("is_active", "BOOLEAN"),
]

REQUIRED_SIMULATION_TYPE_FIELDS = [
    "simulation_type_id",
    "simulation_type_code",
    "description",
    "fill_price_basis",
    "slippage_model",
    "slippage_bps",
    "commission_model",
    "lookahead_safe_flag",
    "is_active",
]

REQUIRED_RUN_TYPE_FIELDS = [
    "run_type_code",
    "description",
    "is_active",
]

SUPPORTED_FILL_PRICE_BASES = {"close", "next_open", "open", "vwap"}
SUPPORTED_SLIPPAGE_MODELS = {"none", "fixed_bps", "volatility_based"}
SUPPORTED_RUN_TYPE_CODES = {"backtest", "simulation", "paper", "live"}


def _load_reference_catalog(catalog_path: Path) -> dict[str, Any]:
    if not catalog_path.exists():
        raise FileNotFoundError(f"Simulation reference catalog file not found: {catalog_path}")

    raw = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("simulation_reference.yaml must define a top-level mapping.")
    return raw


def _require_fields(record: dict[str, Any], required_fields: list[str], record_label: str) -> None:
    missing_fields = [field for field in required_fields if record.get(field) in (None, "")]
    if missing_fields:
        raise ValueError(f"{record_label} is missing required fields: {', '.join(missing_fields)}")


def _simulation_type_records(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    simulation_types = catalog.get("simulation_types", [])
    if not isinstance(simulation_types, list):
        raise ValueError(
            "simulation_reference.yaml must define a top-level 'simulation_types' list."
        )

    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    seen_codes: set[str] = set()

    for raw_record in simulation_types:
        if not isinstance(raw_record, dict):
            raise ValueError("Simulation type entries must be mappings.")
        _require_fields(
            raw_record,
            REQUIRED_SIMULATION_TYPE_FIELDS,
            "Simulation type",
        )

        simulation_type_id = int(raw_record["simulation_type_id"])
        simulation_type_code = str(raw_record["simulation_type_code"]).strip().lower()
        fill_price_basis = str(raw_record["fill_price_basis"]).strip().lower()
        slippage_model = str(raw_record["slippage_model"]).strip().lower()
        slippage_bps = float(raw_record["slippage_bps"])

        if simulation_type_id in seen_ids:
            raise ValueError(
                f"Duplicate simulation_type_id in simulation reference catalog: "
                f"{simulation_type_id}"
            )
        if simulation_type_code in seen_codes:
            raise ValueError(
                f"Duplicate simulation_type_code in simulation reference catalog: "
                f"{simulation_type_code}"
            )
        if fill_price_basis not in SUPPORTED_FILL_PRICE_BASES:
            raise ValueError(
                f"Simulation type {simulation_type_code} fill_price_basis must be one of: "
                f"{', '.join(sorted(SUPPORTED_FILL_PRICE_BASES))}"
            )
        if slippage_model not in SUPPORTED_SLIPPAGE_MODELS:
            raise ValueError(
                f"Simulation type {simulation_type_code} slippage_model must be one of: "
                f"{', '.join(sorted(SUPPORTED_SLIPPAGE_MODELS))}"
            )
        if slippage_bps < 0:
            raise ValueError(
                f"Simulation type {simulation_type_code} slippage_bps must be greater than "
                "or equal to 0."
            )

        seen_ids.add(simulation_type_id)
        seen_codes.add(simulation_type_code)
        records.append(
            {
                "simulation_type_id": simulation_type_id,
                "simulation_type_code": simulation_type_code,
                "description": str(raw_record["description"]).strip(),
                "fill_price_basis": fill_price_basis,
                "slippage_model": slippage_model,
                "slippage_bps": slippage_bps,
                "commission_model": str(raw_record["commission_model"]).strip().lower(),
                "lookahead_safe_flag": bool(raw_record["lookahead_safe_flag"]),
                "is_active": bool(raw_record["is_active"]),
            }
        )

    return records


def _run_type_records(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    run_types = catalog.get("run_types", [])
    if not isinstance(run_types, list):
        raise ValueError("simulation_reference.yaml must define a top-level 'run_types' list.")

    records: list[dict[str, Any]] = []
    seen_codes: set[str] = set()

    for raw_record in run_types:
        if not isinstance(raw_record, dict):
            raise ValueError("Run type entries must be mappings.")
        _require_fields(raw_record, REQUIRED_RUN_TYPE_FIELDS, "Run type")

        run_type_code = str(raw_record["run_type_code"]).strip().lower()
        if run_type_code in seen_codes:
            raise ValueError(
                f"Duplicate run_type_code in simulation reference catalog: {run_type_code}"
            )
        if run_type_code not in SUPPORTED_RUN_TYPE_CODES:
            raise ValueError(
                f"Run type {run_type_code} must be one of: "
                f"{', '.join(sorted(SUPPORTED_RUN_TYPE_CODES))}"
            )

        seen_codes.add(run_type_code)
        records.append(
            {
                "run_type_code": run_type_code,
                "description": str(raw_record["description"]).strip(),
                "is_active": bool(raw_record["is_active"]),
            }
        )

    missing_run_types = sorted(SUPPORTED_RUN_TYPE_CODES - seen_codes)
    if missing_run_types:
        raise ValueError(
            "simulation_reference.yaml is missing required run_type_code values: "
            f"{', '.join(missing_run_types)}"
        )

    return records


@asset(
    name="simulation_types",
    key_prefix=["ref"],
    required_resource_keys={"research_duckdb"},
)
def ref_simulation_types(context: AssetExecutionContext) -> None:
    """
    Build ref.simulation_types from the repo-managed simulation reference catalog.
    """
    con = context.resources.research_duckdb
    catalog = _load_reference_catalog(SIMULATION_REFERENCE_PATH)
    simulation_type_df = pd.DataFrame(
        _simulation_type_records(catalog),
        columns=[column_name for column_name, _ in SIMULATION_TYPES_COLUMNS],
    )

    _replace_table_from_df(
        con,
        schema="ref",
        table="simulation_types",
        columns=SIMULATION_TYPES_COLUMNS,
        df=simulation_type_df,
    )

    context.add_output_metadata(
        {
            "table": "ref.simulation_types",
            "catalog_path": str(SIMULATION_REFERENCE_PATH),
            "simulation_type_count": len(simulation_type_df),
            **_table_metadata(con, schema="ref", table="simulation_types"),
        }
    )


@asset(
    name="run_types",
    key_prefix=["ref"],
    required_resource_keys={"research_duckdb"},
)
def ref_run_types(context: AssetExecutionContext) -> None:
    """
    Build ref.run_types from the repo-managed simulation reference catalog.
    """
    con = context.resources.research_duckdb
    catalog = _load_reference_catalog(SIMULATION_REFERENCE_PATH)
    run_type_df = pd.DataFrame(
        _run_type_records(catalog),
        columns=[column_name for column_name, _ in RUN_TYPES_COLUMNS],
    )

    _replace_table_from_df(
        con,
        schema="ref",
        table="run_types",
        columns=RUN_TYPES_COLUMNS,
        df=run_type_df,
    )

    context.add_output_metadata(
        {
            "table": "ref.run_types",
            "catalog_path": str(SIMULATION_REFERENCE_PATH),
            "run_type_count": len(run_type_df),
            **_table_metadata(con, schema="ref", table="run_types"),
        }
    )
