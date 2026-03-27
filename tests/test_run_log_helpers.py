import json
from types import SimpleNamespace

import duckdb
import pandas as pd
from dagster import DagsterEventType

from portfolio_project.defs.portfolio_db.observability.run_log import (
    _check_research_prices_freshness,
    _check_research_universe_freshness,
    _collect_materialization_asset_metrics,
    _collect_materialization_metrics,
    _is_us_trading_day,
    _metadata_int,
)


def _meta_value(*, int_value=None, value=None, float_value=None):
    return SimpleNamespace(int_value=int_value, value=value, float_value=float_value)


def _materialization_record(metadata: dict, asset_key_path: list[str] | None = None):
    asset_key = SimpleNamespace(path=asset_key_path) if asset_key_path else None
    materialization = SimpleNamespace(metadata=metadata, asset_key=asset_key)
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


def test_collect_materialization_metrics_accepts_legacy_mutation_keys() -> None:
    records = [
        _materialization_record(
            {
                "row_count": _meta_value(int_value=5),
                "inserted_count": _meta_value(int_value=3),
                "updated_count": _meta_value(int_value=2),
                "deleted_count": _meta_value(int_value=1),
            }
        )
    ]

    metrics = _collect_materialization_metrics(records)
    assert metrics["assets_materialized_count"] == 1
    assert metrics["row_count"] == 5
    assert metrics["rows_inserted"] == 3
    assert metrics["rows_updated"] == 2
    assert metrics["rows_deleted"] == 1


def test_collect_materialization_metrics_dedupes_alias_and_canonical_keys() -> None:
    records = [
        _materialization_record(
            {
                "rows_inserted": _meta_value(int_value=4),
                "inserted_count": _meta_value(int_value=4),
                "rows_updated": _meta_value(int_value=1),
                "updated_count": _meta_value(int_value=1),
                "rows_deleted": _meta_value(int_value=0),
                "deleted_count": _meta_value(int_value=0),
            }
        )
    ]

    metrics = _collect_materialization_metrics(records)
    assert metrics["rows_inserted"] == 4
    assert metrics["rows_updated"] == 1
    assert metrics["rows_deleted"] == 0


def test_collect_materialization_metrics_defaults_mutations_to_zero_when_missing() -> None:
    records = [
        _materialization_record(
            {
                "row_count": _meta_value(int_value=12),
            }
        )
    ]
    metrics = _collect_materialization_metrics(records)
    assert metrics["assets_materialized_count"] == 1
    assert metrics["row_count"] == 12
    assert metrics["rows_inserted"] == 0
    assert metrics["rows_updated"] == 0
    assert metrics["rows_deleted"] == 0


def test_collect_materialization_asset_metrics_rolls_up_by_asset_key() -> None:
    records = [
        _materialization_record(
            {"row_count": _meta_value(int_value=10), "rows_inserted": _meta_value(int_value=6)},
            asset_key_path=["silver", "news"],
        ),
        _materialization_record(
            {"row_count": _meta_value(int_value=4), "inserted_count": _meta_value(int_value=6)},
            asset_key_path=["silver", "news"],
        ),
        _materialization_record(
            {"rows_updated": _meta_value(int_value=2), "deleted_count": _meta_value(int_value=1)},
            asset_key_path=["gold", "headlines"],
        ),
    ]

    metrics = _collect_materialization_asset_metrics(records)
    assert metrics == [
        {
            "asset_key": "gold/headlines",
            "assets_materialized_count": 1,
            "row_count": None,
            "rows_inserted": 0,
            "rows_updated": 2,
            "rows_deleted": 1,
        },
        {
            "asset_key": "silver/news",
            "assets_materialized_count": 2,
            "row_count": 14,
            "rows_inserted": 12,
            "rows_updated": 0,
            "rows_deleted": 0,
        },
    ]


def test_is_us_trading_day_rejects_weekends_and_bad_inputs() -> None:
    assert _is_us_trading_day("2026-02-14") is False
    assert _is_us_trading_day("not-a-date") is False


def _write_research_partition(tmp_path, partition_key: str, row_count: int) -> None:
    out_path = (
        tmp_path
        / "silver"
        / "research_daily_prices"
        / f"month={partition_key[:7]}"
        / f"date={partition_key}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": [f"SYM{i:04d}" for i in range(row_count)],
            "trade_date": [partition_key] * row_count,
        }
    ).to_parquet(out_path, index=False)


def _write_research_partition_symbols(tmp_path, partition_key: str, symbols: list[str]) -> None:
    out_path = (
        tmp_path
        / "silver"
        / "research_daily_prices"
        / f"month={partition_key[:7]}"
        / f"date={partition_key}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": symbols,
            "trade_date": [partition_key] * len(symbols),
        }
    ).to_parquet(out_path, index=False)


def _write_universe_counts(con, counts_by_date: dict[str, int]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.universe_membership_daily (
            member_date DATE,
            symbol VARCHAR,
            liquidity_rank BIGINT,
            rolling_avg_dollar_volume DOUBLE,
            source VARCHAR,
            ingested_ts TIMESTAMP
        )
        """
    )
    for member_date, symbol_count in counts_by_date.items():
        for rank in range(1, symbol_count + 1):
            con.execute(
                """
                INSERT INTO silver.universe_membership_daily (
                    member_date,
                    symbol,
                    liquidity_rank,
                    rolling_avg_dollar_volume,
                    source,
                    ingested_ts
                )
                VALUES (?, ?, ?, ?, ?, current_timestamp)
                """,
                [member_date, f"SYM{rank:04d}", rank, float(symbol_count - rank + 1), "test"],
            )


def test_research_price_freshness_fails_when_expected_latest_partition_is_missing(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("PORTFOLIO_DATA_DIR", str(tmp_path))
    _write_research_partition(tmp_path, "2026-02-12", 100)

    rows = _check_research_prices_freshness(
        duckdb.connect(":memory:"),
        "run-1",
        "research_daily_prices_job",
        "2026-02-13",
    )

    by_name = {row["check_name"]: row for row in rows}
    latest_row = by_name["research_daily_prices_latest_trading_date_present"]
    assert latest_row["status"] == "FAIL"
    assert latest_row["measured_value"] == 0.0
    assert latest_row["threshold_value"] == 1.0
    assert json.loads(latest_row["details_json"]) == {
        "expected_partition_key": "2026-02-13",
        "expected_partition_path": str(
            tmp_path
            / "silver"
            / "research_daily_prices"
            / "month=2026-02"
            / "date=2026-02-13.parquet"
        ).replace("\\", "/"),
        "expected_partition_present": False,
        "expected_partition_row_count": 0,
        "expected_partition_trade_date": None,
        "latest_available_partition_key": "2026-02-12",
    }


def test_research_price_freshness_fails_when_row_count_drops_below_recent_baseline(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("PORTFOLIO_DATA_DIR", str(tmp_path))
    _write_research_partition(tmp_path, "2026-02-10", 100)
    _write_research_partition(tmp_path, "2026-02-11", 110)
    _write_research_partition(tmp_path, "2026-02-12", 90)
    _write_research_partition(tmp_path, "2026-02-13", 20)

    rows = _check_research_prices_freshness(
        duckdb.connect(":memory:"),
        "run-2",
        "research_daily_prices_job",
        "2026-02-13",
    )

    by_name = {row["check_name"]: row for row in rows}
    latest_row = by_name["research_daily_prices_latest_trading_date_present"]
    count_row = by_name["research_daily_prices_partition_row_count_vs_recent_median"]

    assert latest_row["status"] == "PASS"
    assert latest_row["measured_value"] == 1.0
    assert count_row["status"] == "FAIL"
    assert count_row["measured_value"] == 20.0
    assert count_row["threshold_value"] == 70.0

    count_details = json.loads(count_row["details_json"])
    assert count_details["baseline_median_row_count"] == 100.0
    assert count_details["row_count_ratio_threshold"] == 0.7
    assert count_details["recent_partition_counts"] == {
        "2026-02-12": 90,
        "2026-02-11": 110,
        "2026-02-10": 100,
    }


def test_research_universe_freshness_fails_when_expected_latest_member_date_is_missing() -> None:
    con = duckdb.connect(":memory:")
    _write_universe_counts(
        con,
        {
            "2026-02-10": 500,
            "2026-02-11": 500,
            "2026-02-12": 500,
        },
    )

    rows = _check_research_universe_freshness(
        con,
        "run-3",
        "research_daily_prices_job",
        "2026-02-13",
    )

    by_name = {row["check_name"]: row for row in rows}
    latest_row = by_name["research_universe_membership_latest_trading_date_present"]
    assert latest_row["status"] == "FAIL"
    assert latest_row["measured_value"] == 0.0
    assert latest_row["threshold_value"] == 1.0
    assert json.loads(latest_row["details_json"]) == {
        "expected_member_date": "2026-02-13",
        "universe_table_present": True,
        "expected_member_date_present": False,
        "expected_member_date_symbol_count": 0,
        "latest_available_member_date": "2026-02-12",
        "target_universe_size": 500,
    }


def test_research_universe_freshness_fails_when_symbol_count_drops_below_recent_baseline() -> None:
    con = duckdb.connect(":memory:")
    _write_universe_counts(
        con,
        {
            "2026-02-10": 500,
            "2026-02-11": 500,
            "2026-02-12": 480,
            "2026-02-13": 300,
        },
    )

    rows = _check_research_universe_freshness(
        con,
        "run-4",
        "research_daily_prices_job",
        "2026-02-13",
    )

    by_name = {row["check_name"]: row for row in rows}
    latest_row = by_name["research_universe_membership_latest_trading_date_present"]
    count_row = by_name["research_universe_membership_symbol_count_vs_recent_median"]
    drop_row = by_name["research_universe_membership_day_over_day_drop_ratio"]

    assert latest_row["status"] == "PASS"
    assert latest_row["measured_value"] == 1.0
    assert count_row["status"] == "FAIL"
    assert count_row["measured_value"] == 300.0
    assert count_row["threshold_value"] == 475.0
    assert drop_row["status"] == "FAIL"
    assert drop_row["measured_value"] == 0.375
    assert drop_row["threshold_value"] == 0.06

    count_details = json.loads(count_row["details_json"])
    assert count_details["baseline_median_symbol_count"] == 500.0
    assert count_details["symbol_count_ratio_threshold"] == 0.95
    assert count_details["recent_symbol_counts"] == {
        "2026-02-12": 480,
        "2026-02-11": 500,
        "2026-02-10": 500,
    }

    drop_details = json.loads(drop_row["details_json"])
    assert drop_details["previous_member_date"] == "2026-02-12"
    assert drop_details["previous_symbol_count"] == 480
    assert drop_details["current_drop_count"] == 180
    assert drop_details["baseline_median_drop_ratio"] == 0.02
    assert drop_details["drop_ratio_multiplier"] == 3.0
    assert drop_details["minimum_drop_ratio_threshold"] == 0.03
    assert drop_details["historical_drop_ratios"] == [0.0, 0.04]


def test_research_universe_freshness_flags_unusual_day_over_day_drop() -> None:
    con = duckdb.connect(":memory:")
    _write_universe_counts(
        con,
        {
            "2026-02-10": 500,
            "2026-02-11": 500,
            "2026-02-12": 500,
            "2026-02-13": 480,
        },
    )

    rows = _check_research_universe_freshness(
        con,
        "run-4b",
        "research_daily_prices_job",
        "2026-02-13",
    )

    by_name = {row["check_name"]: row for row in rows}
    count_row = by_name["research_universe_membership_symbol_count_vs_recent_median"]
    drop_row = by_name["research_universe_membership_day_over_day_drop_ratio"]

    assert count_row["status"] == "PASS"
    assert count_row["measured_value"] == 480.0
    assert count_row["threshold_value"] == 475.0

    assert drop_row["status"] == "FAIL"
    assert drop_row["measured_value"] == 0.04
    assert drop_row["threshold_value"] == 0.03

    drop_details = json.loads(drop_row["details_json"])
    assert drop_details["previous_member_date"] == "2026-02-12"
    assert drop_details["previous_symbol_count"] == 500
    assert drop_details["current_drop_count"] == 20
    assert drop_details["baseline_median_drop_ratio"] == 0.0
    assert drop_details["historical_drop_ratios"] == [0.0, 0.0]


def test_research_universe_missing_data_rate_fails_when_recent_member_symbols_have_gaps(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("PORTFOLIO_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("RESEARCH_UNIVERSE_MISSING_DATA_LOOKBACK_PARTITIONS", "4")
    monkeypatch.setenv("RESEARCH_UNIVERSE_MISSING_DATA_MIN_HISTORY", "3")
    monkeypatch.setenv("RESEARCH_UNIVERSE_MISSING_DATA_MAX_AVG_RATE", "0.10")

    _write_research_partition_symbols(tmp_path, "2026-02-10", ["AAA", "BBB", "CCC"])
    _write_research_partition_symbols(tmp_path, "2026-02-11", ["AAA", "BBB", "CCC"])
    _write_research_partition_symbols(tmp_path, "2026-02-12", ["AAA", "BBB"])
    _write_research_partition_symbols(tmp_path, "2026-02-13", ["AAA", "CCC"])

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.universe_membership_daily (
            member_date DATE,
            symbol VARCHAR,
            liquidity_rank BIGINT,
            rolling_avg_dollar_volume DOUBLE,
            source VARCHAR,
            ingested_ts TIMESTAMP
        )
        """
    )
    for member_date in ("2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13"):
        for rank, symbol in enumerate(("AAA", "BBB", "CCC"), start=1):
            con.execute(
                """
                INSERT INTO silver.universe_membership_daily (
                    member_date,
                    symbol,
                    liquidity_rank,
                    rolling_avg_dollar_volume,
                    source,
                    ingested_ts
                )
                VALUES (?, ?, ?, ?, ?, current_timestamp)
                """,
                [member_date, symbol, rank, float(100 - rank), "test"],
            )

    rows = _check_research_universe_freshness(
        con,
        "run-5",
        "research_daily_prices_job",
        "2026-02-13",
    )

    by_name = {row["check_name"]: row for row in rows}
    missing_rate_row = by_name["research_universe_membership_avg_symbol_missing_data_rate"]

    assert missing_rate_row["status"] == "FAIL"
    assert missing_rate_row["measured_value"] == 0.16666666666666666
    assert missing_rate_row["threshold_value"] == 0.10

    details = json.loads(missing_rate_row["details_json"])
    assert details["window_start_member_date"] == "2026-02-10"
    assert details["window_end_member_date"] == "2026-02-13"
    assert details["lookback_partitions_considered"] == 4
    assert details["latest_universe_symbol_count"] == 3
    assert details["symbols_evaluated"] == 3
    assert details["symbols_with_missing_data"] == 2
    assert details["symbols_with_missing_data_rate"] == 0.6666666666666666
    assert details["max_symbol_missing_rate"] == 0.25
    assert details["missing_price_partitions"] == []
    assert details["top_missing_symbols"][:2] == [
        {
            "symbol": "BBB",
            "expected_dates": 4,
            "present_dates": 3,
            "missing_dates": 1,
            "missing_rate": 0.25,
        },
        {
            "symbol": "CCC",
            "expected_dates": 4,
            "present_dates": 3,
            "missing_dates": 1,
            "missing_rate": 0.25,
        },
    ]
