from datetime import datetime, timezone
from types import SimpleNamespace

from portfolio_project.definitions import (
    _daily_news_schedule_fn,
    _daily_prices_schedule_fn,
    _previous_trading_day,
)


def test_previous_trading_day_weekday_and_weekend_logic() -> None:
    assert _previous_trading_day(datetime(2026, 2, 17).date()).isoformat() == "2026-02-16"
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
