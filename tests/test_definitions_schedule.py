from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from portfolio_project.definitions import (
    _daily_news_schedule_fn,
    _daily_prices_schedule_fn,
    _previous_trading_day,
    _prices_compaction_schedule_fn,
    _research_daily_prices_schedule_fn,
    defs,
    monthly_factors_schedule,
    research_daily_prices_schedule,
)


def test_previous_trading_day_weekday_and_weekend_logic() -> None:
    assert _previous_trading_day(datetime(2026, 2, 18).date()).isoformat() == "2026-02-17"
    assert _previous_trading_day(datetime(2026, 2, 16).date()).isoformat() == "2026-02-13"
    assert _previous_trading_day(datetime(2026, 2, 15).date()).isoformat() == "2026-02-13"
    assert _previous_trading_day(datetime(2026, 2, 14).date()).isoformat() == "2026-02-13"


def test_daily_prices_schedule_uses_previous_trading_day_partition() -> None:
    scheduled_utc = datetime(2026, 2, 16, 14, 30, tzinfo=timezone.utc)
    context = SimpleNamespace(scheduled_execution_time=scheduled_utc)

    request = _daily_prices_schedule_fn(context)
    assert request.partition_key == "2026-02-13"
    assert request.run_key == "2026-02-13"


def test_daily_news_schedule_uses_same_day_partition() -> None:
    scheduled_utc = datetime(2026, 2, 17, 14, 0, tzinfo=timezone.utc)
    context = SimpleNamespace(scheduled_execution_time=scheduled_utc)

    request = _daily_news_schedule_fn(context)
    assert request.partition_key == "2026-02-17"
    assert request.run_key == "2026-02-17"


def test_research_daily_prices_schedule_uses_previous_trading_day_partition() -> None:
    scheduled_utc = datetime(2026, 2, 16, 14, 35, tzinfo=timezone.utc)
    context = SimpleNamespace(scheduled_execution_time=scheduled_utc)

    request = _research_daily_prices_schedule_fn(context)
    assert request.partition_key == "2026-02-13"
    assert request.run_key == "2026-02-13"


def test_prices_compaction_schedule_keeps_month_partition_and_unique_daily_run_key() -> None:
    context_day_1 = SimpleNamespace(
        scheduled_execution_time=datetime(2026, 2, 17, 14, 45, tzinfo=timezone.utc)
    )
    context_day_2 = SimpleNamespace(
        scheduled_execution_time=datetime(2026, 2, 18, 14, 45, tzinfo=timezone.utc)
    )

    request_day_1 = _prices_compaction_schedule_fn(context_day_1)
    request_day_2 = _prices_compaction_schedule_fn(context_day_2)

    assert request_day_1.partition_key == "2026-02-01"
    assert request_day_2.partition_key == "2026-02-01"
    assert request_day_1.run_key == "2026-02-01|2026-02-17"
    assert request_day_2.run_key == "2026-02-01|2026-02-18"


def test_previous_trading_day_skips_configured_market_holiday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_is_us_trading_day(partition_key: str) -> bool:
        if partition_key == "2026-02-16":
            return False
        day = datetime.strptime(partition_key, "%Y-%m-%d").date()
        return day.weekday() < 5

    monkeypatch.setattr("portfolio_project.definitions._is_us_trading_day", _fake_is_us_trading_day)
    assert _previous_trading_day(datetime(2026, 2, 17).date()).isoformat() == "2026-02-13"


def test_monthly_factors_schedule_runs_on_first_of_month() -> None:
    assert monthly_factors_schedule.cron_schedule == "15 9 1 * *"
    assert monthly_factors_schedule.execution_timezone == "America/New_York"
    assert monthly_factors_schedule.job.name == "monthly_factors_job"


def test_research_daily_prices_schedule_metadata() -> None:
    assert research_daily_prices_schedule.cron_schedule == "35 9 * * *"
    assert research_daily_prices_schedule.execution_timezone == "America/New_York"
    assert research_daily_prices_schedule.job.name == "research_daily_prices_job"


def test_research_daily_prices_job_resolves_from_definitions() -> None:
    job_def = defs.get_job_def("research_daily_prices_job")
    assert job_def.name == "research_daily_prices_job"
