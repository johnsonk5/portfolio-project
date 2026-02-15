from zoneinfo import ZoneInfo
from datetime import timedelta

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
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
from portfolio_project.defs.gold_activity import gold_activity
from portfolio_project.defs.sp500_assets import (
    bronze_sp500_companies,
    silver_sp500_companies,
)
from portfolio_project.defs.yahoo_news_assets import (
    BRONZE_NEWS_PARTITIONS,
    bronze_yahoo_news,
)
from portfolio_project.defs.silver_news_assets import (
    silver_ref_publishers,
    silver_news,
)
from portfolio_project.defs.tranco_assets import bronze_tranco_snapshot
from portfolio_project.defs.gold_news_assets import gold_headlines

from portfolio_project.defs.wikipedia_pageviews import (
    BRONZE_WIKIPEDIA_PARTITIONS,
    bronze_wikipedia_pageviews,
    silver_wikipedia_pageviews,
)
from portfolio_project.defs.run_log import (
    dagster_run_log_failure,
    dagster_run_log_failure_sensor,
    dagster_run_log_success,
    dagster_run_log_success_sensor,
)

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
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

asset_status_updates_selection = AssetSelection.assets(
    silver_alpaca_assets_status_updates,
)

asset_status_updates_job = define_asset_job(
    name="asset_status_updates_job",
    selection=asset_status_updates_selection,
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

sp500_selection = AssetSelection.assets(
    bronze_sp500_companies,
    silver_sp500_companies,
)

sp500_update_job = define_asset_job(
    name="sp500_update_job",
    selection=sp500_selection,
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

news_selection = AssetSelection.assets(
    bronze_yahoo_news,
    silver_ref_publishers,
    silver_news,
    gold_headlines,
)

daily_news_job = define_asset_job(
    name="daily_news_job",
    selection=news_selection,
    partitions_def=BRONZE_NEWS_PARTITIONS,
    executor_def=in_process_executor,
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

wikipedia_activity_selection = AssetSelection.assets(
    bronze_wikipedia_pageviews,
    silver_wikipedia_pageviews,
    gold_activity,
)

wikipedia_activity_job = define_asset_job(
    name="wikipedia_activity_job",
    selection=wikipedia_activity_selection,
    partitions_def=BRONZE_WIKIPEDIA_PARTITIONS,
    executor_def=in_process_executor,
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

tranco_update_job = define_asset_job(
    name="tranco_update_job",
    selection=AssetSelection.assets(bronze_tranco_snapshot),
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

sp500_weekly_schedule = ScheduleDefinition(
    name="sp500_weekly_schedule",
    cron_schedule="0 17 * * 5",
    execution_timezone="America/New_York",
    job=sp500_update_job,
)

def _previous_trading_day(local_date):
    weekday = local_date.weekday()
    if weekday == 0:
        return local_date - timedelta(days=3)
    if weekday == 6:
        return local_date - timedelta(days=2)
    if weekday == 5:
        return local_date - timedelta(days=1)
    return local_date - timedelta(days=1)


def _daily_prices_schedule_fn(context):
    scheduled_time = context.scheduled_execution_time
    if scheduled_time is None:
        return []
    scheduled_local = scheduled_time.astimezone(ZoneInfo("America/New_York"))
    partition_date = _previous_trading_day(scheduled_local.date())
    partition_key = partition_date.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=partition_key,
        partition_key=partition_key,
    )


daily_prices_schedule = ScheduleDefinition(
    name="daily_prices_schedule",
    cron_schedule="30 9 * * *",
    execution_timezone="America/New_York",
    job=daily_prices_job,
    execution_fn=_daily_prices_schedule_fn,
)

def _daily_news_schedule_fn(context):
    scheduled_time = context.scheduled_execution_time
    if scheduled_time is None:
        return []
    scheduled_local = scheduled_time.astimezone(ZoneInfo("America/New_York"))
    partition_key = scheduled_local.date().strftime("%Y-%m-%d")
    return RunRequest(
        run_key=partition_key,
        partition_key=partition_key,
    )


daily_news_schedule = ScheduleDefinition(
    name="daily_news_schedule",
    cron_schedule="0 9 * * *",
    execution_timezone="America/New_York",
    job=daily_news_job,
    execution_fn=_daily_news_schedule_fn,
)

def _daily_wikipedia_schedule_fn(context):
    scheduled_time = context.scheduled_execution_time
    if scheduled_time is None:
        return []
    scheduled_local = scheduled_time.astimezone(ZoneInfo("America/New_York"))
    partition_date = _previous_trading_day(scheduled_local.date())
    partition_key = partition_date.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=partition_key,
        partition_key=partition_key,
    )


wikipedia_daily_schedule = ScheduleDefinition(
    name="wikipedia_daily_schedule",
    cron_schedule="45 8 * * *",
    execution_timezone="America/New_York",
    job=wikipedia_activity_job,
    execution_fn=_daily_wikipedia_schedule_fn,
)

tranco_monthly_schedule = ScheduleDefinition(
    name="tranco_monthly_schedule",
    cron_schedule="0 18 1 * *",
    execution_timezone="America/New_York",
    job=tranco_update_job,
)

defs = Definitions(
    assets=[
        bronze_alpaca_bars,
        bronze_alpaca_assets,
        bronze_yahoo_news,
        bronze_tranco_snapshot,
        bronze_wikipedia_pageviews,
        silver_wikipedia_pageviews,
        silver_ref_publishers,
        silver_news,
        gold_headlines,
        silver_alpaca_assets,
        silver_alpaca_active_assets_history,
        silver_alpaca_assets_status_updates,
        silver_alpaca_prices_parquet,
        gold_alpaca_prices,
        gold_activity,
        bronze_sp500_companies,
        silver_sp500_companies,
    ],
    jobs=[
        daily_prices_job,
        daily_news_job,
        wikipedia_activity_job,
        asset_status_updates_job,
        sp500_update_job,
        tranco_update_job,
    ],
    schedules=[
        daily_prices_schedule,
        daily_news_schedule,
        wikipedia_daily_schedule,
        sp500_weekly_schedule,
        tranco_monthly_schedule,
    ],
    sensors=[
        dagster_run_log_success_sensor,
        dagster_run_log_failure_sensor,
    ],
    resources={
        "alpaca": alpaca_resource,
        "duckdb": duckdb_resource,
    },
)
