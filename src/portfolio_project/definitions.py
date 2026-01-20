from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder

from portfolio_project.defs.bronze_assets import bronze_alpaca_assets, bronze_alpaca_bars

from portfolio_project.defs.alpaca_resource import alpaca_resource
from portfolio_project.defs.duckdb_resource import duckdb_resource


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)

defs = Definitions(
    assets=[bronze_alpaca_bars, bronze_alpaca_assets],
    resources={
        "alpaca": alpaca_resource,
        "duckdb": duckdb_resource,
    },
)
