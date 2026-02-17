from types import SimpleNamespace

from dagster import DagsterEventType

from portfolio_project.defs.run_log import (
    _collect_materialization_metrics,
    _is_us_trading_day,
    _metadata_int,
)


def _meta_value(*, int_value=None, value=None, float_value=None):
    return SimpleNamespace(int_value=int_value, value=value, float_value=float_value)


def _materialization_record(metadata: dict):
    materialization = SimpleNamespace(metadata=metadata)
    event_data = SimpleNamespace(materialization=materialization)
    dagster_event = SimpleNamespace(
        event_type=DagsterEventType.ASSET_MATERIALIZATION,
        event_specific_data=event_data,
    )
    entry = SimpleNamespace(dagster_event=dagster_event)
    return SimpleNamespace(event_log_entry=entry)


def test_metadata_int_reads_supported_numeric_shapes() -> None:
    assert _metadata_int(_meta_value(int_value=3)) == 3
    assert _metadata_int(_meta_value(value=7)) == 7
    assert _metadata_int(_meta_value(float_value=5.0)) == 5
    assert _metadata_int(_meta_value(float_value=5.5)) is None
    assert _metadata_int(_meta_value(value=True)) is None


def test_collect_materialization_metrics_aggregates_expected_totals() -> None:
    records = [
        _materialization_record(
            {
                "row_count": _meta_value(int_value=10),
                "rows_inserted": _meta_value(value=4),
                "rows_updated": _meta_value(float_value=2.0),
            }
        ),
        _materialization_record(
            {
                "row_count": _meta_value(value=8),
                "rows_deleted": _meta_value(int_value=1),
            }
        ),
    ]

    metrics = _collect_materialization_metrics(records)
    assert metrics["assets_materialized_count"] == 2
    assert metrics["row_count"] == 18
    assert metrics["rows_inserted"] == 4
    assert metrics["rows_updated"] == 2
    assert metrics["rows_deleted"] == 1


def test_is_us_trading_day_rejects_weekends_and_bad_inputs() -> None:
    assert _is_us_trading_day("2026-02-14") is False
    assert _is_us_trading_day("not-a-date") is False
