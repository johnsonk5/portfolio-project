from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.research_db.silver.research_prices as research_silver_prices_module


def _write_bronze_prices(
    data_root: Path,
    dataset_name: str,
    partition_key: str,
    frame: pd.DataFrame,
) -> None:
    out_path = data_root / "bronze" / dataset_name / f"date={partition_key}" / "prices.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_path, index=False)


def test_research_daily_prices_prefers_alpaca_on_overlap(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    _write_bronze_prices(
        data_root,
        "alpaca_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "timestamp": [
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                ],
                "trade_date": ["2026-02-13", "2026-02-13"],
                "open": [100.0, 200.0],
                "high": [101.0, 201.0],
                "low": [99.0, 199.0],
                "close": [100.5, 200.5],
                "adjusted_close": [pd.NA, pd.NA],
                "volume": [1000, 2000],
                "trade_count": [10, 20],
                "vwap": [100.2, 200.2],
                "source": ["alpaca", "alpaca"],
                "ingested_ts": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        ),
    )
    _write_bronze_prices(
        data_root,
        "eodhd_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL", "NVDA"],
                "timestamp": [
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                ],
                "trade_date": ["2026-02-13", "2026-02-13"],
                "open": [98.0, 300.0],
                "high": [99.0, 301.0],
                "low": [97.0, 299.0],
                "close": [98.5, 300.5],
                "adjusted_close": [98.4, 300.4],
                "volume": [9999, 3000],
                "trade_count": [pd.NA, pd.NA],
                "vwap": [pd.NA, pd.NA],
                "source": ["eodhd", "eodhd"],
                "ingested_ts": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        ),
    )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(
        partition_key=partition_key,
        resources={"research_duckdb": con, "duckdb": obs_con},
    )
    research_silver_prices_module.silver_research_daily_prices(context)

    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / "month=2026-02"
        / f"date={partition_key}.parquet"
    )
    assert out_path.exists()

    out_df = pd.read_parquet(out_path).sort_values("symbol").reset_index(drop=True)
    assert out_df["symbol"].tolist() == ["AAPL", "MSFT", "NVDA"]
    assert out_df["source"].tolist() == ["alpaca", "alpaca", "eodhd"]
    assert float(out_df.loc[out_df["symbol"] == "AAPL", "close"].iloc[0]) == 100.5
    assert float(out_df.loc[out_df["symbol"] == "AAPL", "dollar_volume"].iloc[0]) == 100500.0


def test_research_daily_prices_rerun_replaces_stale_partition(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    first_frame = pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "timestamp": [
                datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc),
            ],
            "trade_date": ["2026-02-13", "2026-02-13"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "adjusted_close": [pd.NA, pd.NA],
            "volume": [1000, 2000],
            "trade_count": [10, 20],
            "vwap": [100.2, 200.2],
            "source": ["alpaca", "alpaca"],
            "ingested_ts": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
        }
    )
    second_frame = first_frame[first_frame["symbol"] == "AAPL"].copy()

    _write_bronze_prices(data_root, "alpaca_prices_daily", partition_key, first_frame)
    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(
        partition_key=partition_key,
        resources={"research_duckdb": con, "duckdb": obs_con},
    )
    research_silver_prices_module.silver_research_daily_prices(context)

    _write_bronze_prices(data_root, "alpaca_prices_daily", partition_key, second_frame)
    research_silver_prices_module.silver_research_daily_prices(context)

    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / "month=2026-02"
        / f"date={partition_key}.parquet"
    )
    out_df = pd.read_parquet(out_path)
    assert out_df["symbol"].tolist() == ["AAPL"]


def test_research_daily_prices_applies_split_adjustments_for_alpaca_rows(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    _write_bronze_prices(
        data_root,
        "alpaca_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "timestamp": [datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc)],
                "trade_date": ["2026-02-13"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "adjusted_close": [pd.NA],
                "volume": [1000],
                "trade_count": [10],
                "vwap": [100.2],
                "source": ["alpaca"],
                "ingested_ts": [datetime.now(timezone.utc)],
            }
        ),
    )

    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE SCHEMA silver;
        CREATE TABLE silver.alpaca_corporate_actions (
            symbol VARCHAR,
            effective_date DATE,
            old_rate DOUBLE,
            new_rate DOUBLE
        );
        INSERT INTO silver.alpaca_corporate_actions VALUES
            ('AAPL', DATE '2026-02-17', 1.0, 2.0)
        """
    )
    context = build_asset_context(
        partition_key=partition_key,
        resources={"research_duckdb": con, "duckdb": duckdb.connect(":memory:")},
    )
    research_silver_prices_module.silver_research_daily_prices(context)

    out_path = (
        data_root
        / "silver"
        / "research_daily_prices"
        / "month=2026-02"
        / f"date={partition_key}.parquet"
    )
    out_df = pd.read_parquet(out_path)
    row = out_df.iloc[0]
    assert float(row["adjusted_close"]) == 50.25


def test_validate_research_daily_prices_schema_detects_missing_columns_and_type_mismatches(
    tmp_path: Path,
) -> None:
    bad_path = tmp_path / "bad_research_daily_prices.parquet"
    pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "timestamp": [datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc)],
            "trade_date": ["2026-02-13"],
            "open": ["100.0"],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "adjusted_close": [100.5],
            "volume": [1000],
            "trade_count": [10],
            "vwap": [100.2],
            "dollar_volume": [100500.0],
            "source": ["alpaca"],
        }
    ).to_parquet(bad_path, index=False)

    con = duckdb.connect(":memory:")
    result = research_silver_prices_module._validate_research_daily_prices_schema(con, bad_path)

    assert result["status"] == "FAIL"
    assert result["details"]["missing_columns"] == ["ingested_ts"]
    assert result["details"]["type_mismatches"]["trade_date"]["actual"] == "VARCHAR"
    assert result["details"]["type_mismatches"]["open"]["actual"] == "VARCHAR"


def test_research_daily_prices_writes_schema_dq_check_to_portfolio_observability(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    _write_bronze_prices(
        data_root,
        "alpaca_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "timestamp": [datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc)],
                "trade_date": ["2026-02-13"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "adjusted_close": [pd.NA],
                "volume": [1000],
                "trade_count": [10],
                "vwap": [100.2],
                "source": ["alpaca"],
                "ingested_ts": [datetime.now(timezone.utc)],
            }
        ),
    )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(
        partition_key=partition_key,
        resources={"research_duckdb": con, "duckdb": obs_con},
    )
    research_silver_prices_module.silver_research_daily_prices(context)

    row = obs_con.execute(
        """
        SELECT check_name, status, partition_key
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_research_daily_prices_schema_columns_and_types'
        """
    ).fetchone()

    assert row == (
        "dq_research_daily_prices_schema_columns_and_types",
        "PASS",
        partition_key,
    )


def test_research_daily_prices_writes_required_fields_dq_check_to_portfolio_observability(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    research_silver_prices_module.DATA_ROOT = data_root
    partition_key = "2026-02-13"

    _write_bronze_prices(
        data_root,
        "alpaca_prices_daily",
        partition_key,
        pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "timestamp": [datetime(2026, 2, 13, 21, 0, tzinfo=timezone.utc)],
                "trade_date": ["2026-02-13"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "adjusted_close": [pd.NA],
                "volume": [1000],
                "trade_count": [10],
                "vwap": [100.2],
                "source": ["alpaca"],
                "ingested_ts": [datetime.now(timezone.utc)],
            }
        ),
    )

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(
        partition_key=partition_key,
        resources={"research_duckdb": con, "duckdb": obs_con},
    )
    research_silver_prices_module.silver_research_daily_prices(context)

    row = obs_con.execute(
        """
        SELECT check_name, status, measured_value, partition_key
        FROM observability.data_quality_checks
        WHERE check_name = 'dq_research_daily_prices_required_fields_nulls'
        """
    ).fetchone()

    assert row == (
        "dq_research_daily_prices_required_fields_nulls",
        "PASS",
        0.0,
        partition_key,
    )
