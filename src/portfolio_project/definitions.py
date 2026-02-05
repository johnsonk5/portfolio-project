from pathlib import Path

from zoneinfo import ZoneInfo
from datetime import timedelta

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
    definitions,
    load_from_defs_folder,
)

from portfolio_project.defs.bronze_assets import (
    BRONZE_PARTITIONS,
    bronze_alpaca_assets,
    bronze_alpaca_bars,
)
from portfolio_project.defs.silver_assets import (
    silver_alpaca_active_assets_history,
    silver_alpaca_assets,
    silver_alpaca_assets_status_updates,
)
from portfolio_project.defs.silver_prices import (
    silver_alpaca_prices_parquet,
)
from portfolio_project.defs.gold_prices import gold_alpaca_prices
from portfolio_project.defs.sp500_assets import (
    bronze_sp500_companies,
    silver_sp500_companies,
)
from portfolio_project.defs.yahoo_news_assets import bronze_yahoo_news

from portfolio_project.defs.alpaca_resource import alpaca_resource
from portfolio_project.defs.duckdb_resource import duckdb_resource


prices_selection = AssetSelection.assets(
    bronze_alpaca_bars,
    silver_alpaca_prices_parquet,
    gold_alpaca_prices,
)

daily_prices_job = define_asset_job(
    name="daily_prices_job",
    selection=prices_selection,
    partitions_def=BRONZE_PARTITIONS,
    executor_def=in_process_executor,
)

asset_status_updates_selection = AssetSelection.assets(
    silver_alpaca_assets_status_updates,
)

asset_status_updates_job = define_asset_job(
    name="asset_status_updates_job",
    selection=asset_status_updates_selection,
)

sp500_selection = AssetSelection.assets(
    bronze_sp500_companies,
    silver_sp500_companies,
)

sp500_update_job = define_asset_job(
    name="sp500_update_job",
    selection=sp500_selection,
)

sp500_weekly_schedule = ScheduleDefinition(
    name="sp500_weekly_schedule",
    cron_schedule="0 17 * * 5",
    execution_timezone="America/New_York",
    job=sp500_update_job,
)

def _daily_prices_schedule_fn(context):
    scheduled_time = context.scheduled_execution_time
    if scheduled_time is None:
        return []
    scheduled_utc = scheduled_time.astimezone(ZoneInfo("UTC"))
    partition_date = scheduled_utc.date() - timedelta(days=1)
    partition_key = partition_date.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=partition_key,
        partition_key=partition_key,
    )


daily_prices_schedule = ScheduleDefinition(
    name="daily_prices_schedule",
    cron_schedule="0 9 * * *",
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
        bronze_yahoo_news,
        silver_alpaca_assets,
        silver_alpaca_active_assets_history,
        silver_alpaca_assets_status_updates,
        silver_alpaca_prices_parquet,
        gold_alpaca_prices,
        bronze_sp500_companies,
        silver_sp500_companies,
    ],
    jobs=[daily_prices_job, asset_status_updates_job, sp500_update_job],
    schedules=[daily_prices_schedule, sp500_weekly_schedule],
    resources={
        "alpaca": alpaca_resource,
        "duckdb": duckdb_resource,
    },
)
