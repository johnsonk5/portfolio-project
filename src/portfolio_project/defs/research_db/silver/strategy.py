import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError

STRATEGY_CATALOG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "investment_strategies.yaml"
)

STRATEGY_DEFINITIONS_COLUMNS: list[tuple[str, str]] = [
    ("strategy_id", "VARCHAR"),
    ("strategy_name", "VARCHAR"),
    ("strategy_version", "VARCHAR"),
    ("description", "VARCHAR"),
    ("ranking_method", "VARCHAR"),
    ("rebalance_frequency", "VARCHAR"),
    ("target_count", "INTEGER"),
    ("weighting_method", "VARCHAR"),
    ("benchmark_symbol", "VARCHAR"),
    ("long_short_flag", "BOOLEAN"),
    ("start_date", "DATE"),
    ("end_date", "DATE"),
    ("is_active", "BOOLEAN"),
    ("config_json", "VARCHAR"),
    ("asof_ts", "TIMESTAMP"),
    ("run_id", "VARCHAR"),
]

STRATEGY_RUNS_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "VARCHAR"),
    ("strategy_id", "VARCHAR"),
    ("run_status", "VARCHAR"),
    ("dataset_version", "VARCHAR"),
    ("code_version", "VARCHAR"),
    ("started_at", "TIMESTAMP"),
    ("completed_at", "TIMESTAMP"),
    ("error_message", "VARCHAR"),
    ("persist", "BOOLEAN"),
    ("asof_ts", "TIMESTAMP"),
]

STRATEGY_PARAMETERS_COLUMNS: list[tuple[str, str]] = [
    ("strategy_id", "VARCHAR"),
    ("parameter_name", "VARCHAR"),
    ("parameter_value", "VARCHAR"),
    ("parameter_type", "VARCHAR"),
    ("effective_start_date", "DATE"),
    ("effective_end_date", "DATE"),
    ("is_active", "BOOLEAN"),
    ("description", "VARCHAR"),
    ("ingest_ts", "TIMESTAMP"),
    ("asof_ts", "TIMESTAMP"),
    ("run_id", "VARCHAR"),
]

REQUIRED_DEFINITION_FIELDS = [
    "strategy_id",
    "strategy_name",
    "strategy_version",
    "ranking_method",
    "rebalance_frequency",
    "target_count",
    "weighting_method",
    "benchmark_symbol",
    "long_short_flag",
    "start_date",
    "is_active",
]

REQUIRED_PARAMETER_FIELDS = [
    "parameter_name",
    "parameter_value",
    "parameter_type",
    "effective_start_date",
    "is_active",
]

REQUIRED_PARAMETERS_BY_RANKING_METHOD: dict[str, set[str]] = {
    "single_asset_hold": {"symbol"},
    "momentum_12_1_desc": {"signal_column", "ranking_direction"},
    "realized_vol_21d_asc": {"signal_column", "ranking_direction"},
    "returns_5d_asc": {"signal_column", "ranking_direction"},
    "composite_momentum_below_52w_high_desc": {
        "signal_column",
        "secondary_signal_column",
        "score_method",
        "ranking_direction",
        "min_momentum_12_1",
    },
}


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _safe_run_id(context: AssetExecutionContext) -> str | None:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    return str(run_id) if run_id else None


def _load_catalog(catalog_path: Path) -> list[dict[str, Any]]:
    if not catalog_path.exists():
        raise FileNotFoundError(f"Strategy catalog file not found: {catalog_path}")

    raw = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    strategies = raw.get("strategies", [])
    if not isinstance(strategies, list):
        raise ValueError("investment_strategies.yaml must define a top-level 'strategies' list.")
    return strategies


def _require_fields(record: dict[str, Any], required_fields: list[str], record_label: str) -> None:
    missing_fields = [field for field in required_fields if record.get(field) in (None, "")]
    if missing_fields:
        raise ValueError(f"{record_label} is missing required fields: {', '.join(missing_fields)}")


def _parse_optional_date(raw_value: Any) -> date | None:
    if raw_value in (None, ""):
        return None
    return datetime.strptime(str(raw_value), "%Y-%m-%d").date()


def _now_utc_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _validate_required_strategy_parameters(strategy: dict[str, Any]) -> None:
    strategy_id = str(strategy["strategy_id"]).strip()
    ranking_method = str(strategy["ranking_method"]).strip()
    required_parameters = REQUIRED_PARAMETERS_BY_RANKING_METHOD.get(ranking_method)
    if not required_parameters:
        return

    parameters = strategy.get("parameters") or []
    if not isinstance(parameters, list):
        raise ValueError(f"Strategy {strategy_id} parameters must be a list.")

    parameter_names = {
        str(parameter.get("parameter_name") or "").strip()
        for parameter in parameters
        if isinstance(parameter, dict)
    }
    missing_parameters = sorted(required_parameters - parameter_names)
    if missing_parameters:
        raise ValueError(
            f"Strategy {strategy_id} is missing required parameters for "
            f"{ranking_method}: {', '.join(missing_parameters)}"
        )


def _definition_records(
    strategies: list[dict[str, Any]],
    *,
    asof_ts: datetime,
    run_id: str | None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen_strategy_ids: set[str] = set()

    for strategy in strategies:
        _require_fields(strategy, REQUIRED_DEFINITION_FIELDS, "Strategy definition")
        strategy_id = str(strategy["strategy_id"]).strip()
        if strategy_id in seen_strategy_ids:
            raise ValueError(f"Duplicate strategy_id in strategy catalog: {strategy_id}")
        seen_strategy_ids.add(strategy_id)
        _validate_required_strategy_parameters(strategy)

        config = strategy.get("config") or {}
        if not isinstance(config, dict):
            raise ValueError(f"Strategy {strategy_id} has a non-dict config payload.")

        records.append(
            {
                "strategy_id": strategy_id,
                "strategy_name": str(strategy["strategy_name"]).strip(),
                "strategy_version": str(strategy["strategy_version"]).strip(),
                "description": strategy.get("description"),
                "ranking_method": str(strategy["ranking_method"]).strip(),
                "rebalance_frequency": str(strategy["rebalance_frequency"]).strip(),
                "target_count": int(strategy["target_count"]),
                "weighting_method": str(strategy["weighting_method"]).strip(),
                "benchmark_symbol": str(strategy["benchmark_symbol"]).strip(),
                "long_short_flag": bool(strategy["long_short_flag"]),
                "start_date": _parse_optional_date(strategy.get("start_date")),
                "end_date": _parse_optional_date(strategy.get("end_date")),
                "is_active": bool(strategy["is_active"]),
                "config_json": json.dumps(config, sort_keys=True),
                "asof_ts": asof_ts,
                "run_id": run_id,
            }
        )

    return records


def _parameter_records(
    strategies: list[dict[str, Any]],
    *,
    ingest_ts: datetime,
    asof_ts: datetime,
    run_id: str | None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen_parameter_keys: set[tuple[str, str, date]] = set()

    for strategy in strategies:
        strategy_id = str(strategy["strategy_id"]).strip()
        _validate_required_strategy_parameters(strategy)
        parameters = strategy.get("parameters") or []
        if not isinstance(parameters, list):
            raise ValueError(f"Strategy {strategy_id} parameters must be a list.")

        for parameter in parameters:
            if not isinstance(parameter, dict):
                raise ValueError(f"Strategy {strategy_id} contains a non-dict parameter entry.")
            _require_fields(
                parameter,
                REQUIRED_PARAMETER_FIELDS,
                f"Strategy parameter for {strategy_id}",
            )

            effective_start_date = _parse_optional_date(parameter.get("effective_start_date"))
            if effective_start_date is None:
                raise ValueError(
                    f"Strategy parameter for {strategy_id} is missing effective_start_date."
                )

            parameter_name = str(parameter["parameter_name"]).strip()
            parameter_key = (strategy_id, parameter_name, effective_start_date)
            if parameter_key in seen_parameter_keys:
                raise ValueError(
                    "Duplicate strategy parameter in strategy catalog: "
                    f"{strategy_id}/{parameter_name}/{effective_start_date.isoformat()}"
                )
            seen_parameter_keys.add(parameter_key)

            records.append(
                {
                    "strategy_id": strategy_id,
                    "parameter_name": parameter_name,
                    "parameter_value": str(parameter["parameter_value"]),
                    "parameter_type": str(parameter["parameter_type"]).strip(),
                    "effective_start_date": effective_start_date,
                    "effective_end_date": _parse_optional_date(parameter.get("effective_end_date")),
                    "is_active": bool(parameter["is_active"]),
                    "description": parameter.get("description"),
                    "ingest_ts": ingest_ts,
                    "asof_ts": asof_ts,
                    "run_id": run_id,
                }
            )

    return records


def _ensure_table_contract(
    con,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
) -> None:
    table_ref = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
    column_sql = ",\n            ".join(
        f"{_quote_identifier(name)} {type_sql}" for name, type_sql in columns
    )

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            {column_sql}
        )
        """
    )
    for column_name, type_sql in columns:
        con.execute(
            f"""
            ALTER TABLE {table_ref}
            ADD COLUMN IF NOT EXISTS {_quote_identifier(column_name)} {type_sql}
            """
        )


def _replace_table_from_df(
    con,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    df: pd.DataFrame,
) -> None:
    _ensure_table_contract(con, schema=schema, table=table, columns=columns)
    table_ref = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
    selected_columns = [column_name for column_name, _ in columns]
    df = df.reindex(columns=selected_columns)

    temp_view_name = f"{table}_seed_df"
    try:
        con.unregister(temp_view_name)
    except Exception:
        pass
    con.register(temp_view_name, df)
    con.execute(f"DELETE FROM {table_ref}")
    con.execute(
        f"""
        INSERT INTO {table_ref} (
            {", ".join(_quote_identifier(column) for column in selected_columns)}
        )
        SELECT {", ".join(_quote_identifier(column) for column in selected_columns)}
        FROM {temp_view_name}
        """
    )


def _table_metadata(con, *, schema: str, table: str) -> dict[str, int]:
    row_count = con.execute(
        f"SELECT count(*) FROM {_quote_identifier(schema)}.{_quote_identifier(table)}"
    ).fetchone()[0]
    column_count = con.execute(
        """
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    return {
        "row_count": int(row_count or 0),
        "column_count": int(column_count or 0),
    }


@asset(
    name="strategy_definitions",
    key_prefix=["silver"],
    required_resource_keys={"research_duckdb"},
)
def silver_strategy_definitions(context: AssetExecutionContext) -> None:
    """
    Build silver.strategy_definitions from the repo-managed strategy catalog.
    """
    con = context.resources.research_duckdb
    catalog = _load_catalog(STRATEGY_CATALOG_PATH)
    asof_ts = _now_utc_naive()
    run_id = _safe_run_id(context)
    definition_df = pd.DataFrame(
        _definition_records(catalog, asof_ts=asof_ts, run_id=run_id),
        columns=[column_name for column_name, _ in STRATEGY_DEFINITIONS_COLUMNS],
    )

    _replace_table_from_df(
        con,
        schema="silver",
        table="strategy_definitions",
        columns=STRATEGY_DEFINITIONS_COLUMNS,
        df=definition_df,
    )

    context.add_output_metadata(
        {
            "table": "silver.strategy_definitions",
            "catalog_path": str(STRATEGY_CATALOG_PATH),
            "strategy_count": len(definition_df),
            **_table_metadata(con, schema="silver", table="strategy_definitions"),
        }
    )


@asset(
    name="strategy_runs",
    key_prefix=["silver"],
    deps=[silver_strategy_definitions],
    required_resource_keys={"research_duckdb"},
)
def silver_strategy_runs(context: AssetExecutionContext) -> None:
    """
    Ensure the append-oriented silver.strategy_runs table exists in the research database.
    """
    con = context.resources.research_duckdb
    _ensure_table_contract(
        con,
        schema="silver",
        table="strategy_runs",
        columns=STRATEGY_RUNS_COLUMNS,
    )
    table_metadata = _table_metadata(con, schema="silver", table="strategy_runs")
    context.add_output_metadata({"table": "silver.strategy_runs", **table_metadata})


@asset(
    name="strategy_parameters",
    key_prefix=["silver"],
    deps=[silver_strategy_definitions],
    required_resource_keys={"research_duckdb"},
)
def silver_strategy_parameters(context: AssetExecutionContext) -> None:
    """
    Build silver.strategy_parameters from the repo-managed strategy catalog.
    """
    con = context.resources.research_duckdb
    catalog = _load_catalog(STRATEGY_CATALOG_PATH)
    ingest_ts = _now_utc_naive()
    asof_ts = ingest_ts
    run_id = _safe_run_id(context)
    parameter_df = pd.DataFrame(
        _parameter_records(
            catalog,
            ingest_ts=ingest_ts,
            asof_ts=asof_ts,
            run_id=run_id,
        ),
        columns=[column_name for column_name, _ in STRATEGY_PARAMETERS_COLUMNS],
    )

    _replace_table_from_df(
        con,
        schema="silver",
        table="strategy_parameters",
        columns=STRATEGY_PARAMETERS_COLUMNS,
        df=parameter_df,
    )

    context.add_output_metadata(
        {
            "table": "silver.strategy_parameters",
            "catalog_path": str(STRATEGY_CATALOG_PATH),
            "parameter_count": len(parameter_df),
            **_table_metadata(con, schema="silver", table="strategy_parameters"),
        }
    )
