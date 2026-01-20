import os
from pathlib import Path

import duckdb
from dagster import Field, String, resource


@resource(
    config_schema={
        "db_path": Field(String, is_required=False),
    }
)
def duckdb_resource(context):
    """
    Dagster resource for a DuckDB connection.

    Configuration:
    - db_path: Optional explicit path to the DuckDB file.
    - PORTFOLIO_DUCKDB_PATH: Environment variable fallback.
    - PORTFOLIO_DATA_DIR: Base data directory fallback (defaults to "data").
    """
    configured_path = context.resource_config.get("db_path")
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if configured_path:
        db_path = Path(configured_path)
    elif env_path:
        db_path = Path(env_path)
    else:
        data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
        db_path = data_root / "duckdb" / "portfolio.duckdb"

    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()
