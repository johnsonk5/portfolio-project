from datetime import timedelta
from zoneinfo import ZoneInfo

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)

from portfolio_project.defs.portfolio_db.bronze.alpaca import (
    BRONZE_PARTITIONS,
    bronze_alpaca_assets,
    bronze_alpaca_bars,
)
from portfolio_project.defs.portfolio_db.bronze.fama_french import (
    bronze_fama_french_factors,
)
from portfolio_project.defs.portfolio_db.bronze.news import (
    BRONZE_NEWS_PARTITIONS,
    bronze_yahoo_news,
)
from portfolio_project.defs.portfolio_db.bronze.research_prices import (
    bronze_alpaca_prices_daily,
    bronze_eodhd_prices_daily,
)
from portfolio_project.defs.portfolio_db.bronze.tranco import bronze_tranco_snapshot
from portfolio_project.defs.portfolio_db.demo.seed_data import seed_demo_data
from portfolio_project.defs.portfolio_db.gold.activity import gold_activity
from portfolio_project.defs.portfolio_db.gold.news import gold_headlines
from portfolio_project.defs.portfolio_db.gold.prices import gold_alpaca_prices
from portfolio_project.defs.portfolio_db.observability.run_log import (
    _is_us_trading_day,
    dagster_run_log_failure,
    dagster_run_log_success,
)
from portfolio_project.defs.portfolio_db.reference.sp500 import (
    bronze_sp500_companies,
    silver_sp500_companies,
)
from portfolio_project.defs.portfolio_db.reference.wikipedia import (
    BRONZE_WIKIPEDIA_PARTITIONS,
    bronze_wikipedia_pageviews,
    silver_wikipedia_pageviews,
)
from portfolio_project.defs.portfolio_db.resources.alpaca import alpaca_resource
from portfolio_project.defs.portfolio_db.resources.duckdb import duckdb_resource
from portfolio_project.defs.portfolio_db.resources.eodhd import eodhd_resource
from portfolio_project.defs.portfolio_db.silver.assets import (
    silver_alpaca_active_assets_history,
    silver_alpaca_assets,
    silver_alpaca_assets_status_updates,
)
from portfolio_project.defs.portfolio_db.silver.factors import (
    silver_fama_french_factors_parquet,
)
from portfolio_project.defs.portfolio_db.silver.news import (
    silver_news,
    silver_ref_publishers,
)
from portfolio_project.defs.portfolio_db.silver.research_prices import (
    silver_research_daily_prices,
)
from portfolio_project.defs.portfolio_db.silver.prices import (
    silver_alpaca_prices_parquet,
)
from portfolio_project.defs.portfolio_db.silver.prices_compact import (
    SILVER_COMPACT_PARTITIONS,
    silver_alpaca_prices_compact,
)
from portfolio_project.defs.portfolio_db.silver.universe import (
    silver_universe_membership_daily,
    silver_universe_membership_events,
)

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

prices_compaction_job = define_asset_job(
    name="prices_compaction_job",
    selection=AssetSelection.assets(
        silver_alpaca_prices_compact,
    ),
    partitions_def=SILVER_COMPACT_PARTITIONS,
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

factors_selection = AssetSelection.assets(
    bronze_fama_french_factors,
    silver_fama_french_factors_parquet,
)

monthly_factors_job = define_asset_job(
    name="monthly_factors_job",
    selection=factors_selection,
    executor_def=in_process_executor,
    hooks={dagster_run_log_success, dagster_run_log_failure},
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

sample_demo_seed_job = define_asset_job(
    name="sample_demo_seed_job",
    selection=AssetSelection.assets(
        seed_demo_data,
    ),
    hooks={dagster_run_log_success, dagster_run_log_failure},
)

sp500_weekly_schedule = ScheduleDefinition(
    name="sp500_weekly_schedule",
    cron_schedule="0 17 * * 5",
    execution_timezone="America/New_York",
    job=sp500_update_job,
)


def _previous_trading_day(local_date):
    candidate = local_date - timedelta(days=1)
    while not _is_us_trading_day(candidate.isoformat()):
        candidate -= timedelta(days=1)
    return candidate


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


def _prices_compaction_schedule_fn(context):
    scheduled_time = context.scheduled_execution_time
    if scheduled_time is None:
        return []
    scheduled_local = scheduled_time.astimezone(ZoneInfo("America/New_York"))
    partition_date = _previous_trading_day(scheduled_local.date())
    partition_key = partition_date.replace(day=1).strftime("%Y-%m-%d")
    run_key = f"{partition_key}|{scheduled_local.date().isoformat()}"
    return RunRequest(
        run_key=run_key,
        partition_key=partition_key,
    )


prices_compaction_schedule = ScheduleDefinition(
    name="prices_compaction_schedule",
    cron_schedule="45 9 * * *",
    execution_timezone="America/New_York",
    job=prices_compaction_job,
    execution_fn=_prices_compaction_schedule_fn,
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

monthly_factors_schedule = ScheduleDefinition(
    name="monthly_factors_schedule",
    cron_schedule="15 9 1 * *",
    execution_timezone="America/New_York",
    job=monthly_factors_job,
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
        bronze_eodhd_prices_daily,
        bronze_alpaca_prices_daily,
        bronze_yahoo_news,
        bronze_fama_french_factors,
        bronze_tranco_snapshot,
        bronze_wikipedia_pageviews,
        silver_wikipedia_pageviews,
        seed_demo_data,
        silver_ref_publishers,
        silver_news,
        silver_fama_french_factors_parquet,
        gold_headlines,
        silver_alpaca_assets,
        silver_alpaca_active_assets_history,
        silver_alpaca_assets_status_updates,
        silver_alpaca_prices_parquet,
        silver_alpaca_prices_compact,
        silver_research_daily_prices,
        silver_universe_membership_events,
        silver_universe_membership_daily,
        gold_alpaca_prices,
        gold_activity,
        bronze_sp500_companies,
        silver_sp500_companies,
    ],
    jobs=[
        daily_prices_job,
        prices_compaction_job,
        daily_news_job,
        monthly_factors_job,
        wikipedia_activity_job,
        asset_status_updates_job,
        sp500_update_job,
        tranco_update_job,
        sample_demo_seed_job,
    ],
    schedules=[
        daily_prices_schedule,
        prices_compaction_schedule,
        daily_news_schedule,
        monthly_factors_schedule,
        wikipedia_daily_schedule,
        sp500_weekly_schedule,
        tranco_monthly_schedule,
    ],
    resources={
        "alpaca": alpaca_resource,
        "eodhd": eodhd_resource,
        "duckdb": duckdb_resource,
        "research_duckdb": duckdb_resource.configured(
            {
                "env_var": "PORTFOLIO_RESEARCH_DUCKDB_PATH",
                "default_db_name": "research.duckdb",
            }
        ),
    },
)
