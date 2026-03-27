from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from dagster import build_asset_context

import portfolio_project.defs.research_db.silver.corporate_actions as corporate_actions_module


def test_silver_alpaca_corporate_actions_writes_required_field_dq_checks(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(corporate_actions_module, "DATA_ROOT", data_root)

    actions_path = data_root / "bronze" / "alpaca_corporate_actions" / "actions.parquet"
    actions_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "action_id": ["act-1", "act-2"],
            "symbol": ["AAPL", "MSFT"],
            "action_type": ["forward_splits", "cash_dividends"],
            "effective_date": [date(2026, 2, 17), date(2026, 2, 17)],
            "process_date": [date(2026, 2, 17), date(2026, 2, 17)],
            "old_rate": [1.0, pd.NA],
            "new_rate": [2.0, pd.NA],
            "cash_rate": [pd.NA, 0.25],
            "split_ratio": [2.0, pd.NA],
            "source": ["alpaca", "alpaca"],
            "ingested_ts": pd.to_datetime(["2026-02-17T22:00:00Z", "2026-02-17T22:00:00Z"]),
        }
    ).to_parquet(actions_path, index=False)

    con = duckdb.connect(":memory:")
    obs_con = duckdb.connect(":memory:")
    context = build_asset_context(resources={"research_duckdb": con, "duckdb": obs_con})
    corporate_actions_module.silver_alpaca_corporate_actions(context)

    dq_rows = obs_con.execute(
        """
        SELECT check_name, status, measured_value
        FROM observability.data_quality_checks
        WHERE check_name IN (
            'dq_research_alpaca_corporate_actions_required_fields_nulls',
            'dq_research_alpaca_corporate_actions_split_rate_nulls',
            'dq_research_alpaca_corporate_actions_cash_rate_nulls'
        )
        ORDER BY check_name
        """
    ).fetchall()

    assert dq_rows == [
        ("dq_research_alpaca_corporate_actions_cash_rate_nulls", "PASS", 0.0),
        ("dq_research_alpaca_corporate_actions_required_fields_nulls", "PASS", 0.0),
        ("dq_research_alpaca_corporate_actions_split_rate_nulls", "PASS", 0.0),
    ]
