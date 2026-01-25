from pathlib import Path

from zoneinfo import ZoneInfo

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    definitions,
    load_from_defs_folder,
)

from portfolio_project.defs.bronze_assets import (
    BRONZE_PARTITIONS,
    bronze_alpaca_assets,
    bronze_alpaca_bars,
)
from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.silver_prices import (
    silver_alpaca_prices,
    silver_alpaca_prices_parquet,
)
from portfolio_project.defs.gold_prices import gold_alpaca_prices

from portfolio_project.defs.alpaca_resource import alpaca_resource
from portfolio_project.defs.duckdb_resource import duckdb_resource


prices_selection = AssetSelection.assets(
    bronze_alpaca_bars,
    silver_alpaca_prices_parquet,
    silver_alpaca_prices,
    gold_alpaca_prices,
)

daily_prices_job = define_asset_job(
    name="daily_prices_job",
    selection=prices_selection,
    partitions_def=BRONZE_PARTITIONS,
)

def _daily_prices_schedule_fn(context):
    scheduled_time = context.scheduled_execution_time
    if scheduled_time is None:
        return []
    local_time = scheduled_time.astimezone(ZoneInfo("America/New_York"))
    partition_key = local_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=partition_key,
        partition_key=partition_key,
    )


daily_prices_schedule = ScheduleDefinition(
    name="daily_prices_schedule",
    cron_schedule="45 17 * * *",
    execution_timezone="America/New_York",
    job=daily_prices_job,
    execution_fn=_daily_prices_schedule_fn,
)


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)

defs = Definitions(
    assets=[
        bronze_alpaca_bars,
        bronze_alpaca_assets,
        silver_alpaca_assets,
        silver_alpaca_prices_parquet,
        silver_alpaca_prices,
        gold_alpaca_prices,
    ],
    jobs=[daily_prices_job],
    schedules=[daily_prices_schedule],
    resources={
        "alpaca": alpaca_resource,
        "duckdb": duckdb_resource,
    },
)
