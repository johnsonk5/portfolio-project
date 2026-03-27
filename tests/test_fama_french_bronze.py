from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dagster import materialize

import portfolio_project.defs.research_db.bronze.fama_french as ff_module
from portfolio_project.defs.research_db.bronze.fama_french import (
    _extract_daily_rows,
    bronze_fama_french_factors,
)


def test_extract_daily_rows_parses_kenneth_french_daily_payload() -> None:
    raw_text = """
This file was created for testing.
,Mkt-RF,SMB,HML,RF
20260102,1.23,-0.45,0.67,0.01
20260105,-0.50,0.10,0.20,0.01

Copyright 2026
"""
    df = _extract_daily_rows(raw_text, ["mkt_rf", "smb", "hml", "rf"])

    assert list(df.columns) == ["factor_date", "mkt_rf", "smb", "hml", "rf"]
    assert df["factor_date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-02", "2026-01-05"]
    assert df["mkt_rf"].tolist() == [1.23, -0.5]
    assert df["rf"].tolist() == [0.01, 0.01]


def test_bronze_fama_french_factors_materializes_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    ff_module.DATA_ROOT = data_root

    core_df = pd.DataFrame(
        {
            "factor_date": pd.to_datetime(["2026-01-02", "2026-01-05"]),
            "mkt_rf": [1.23, -0.5],
            "smb": [-0.45, 0.1],
            "hml": [0.67, 0.2],
            "rf": [0.01, 0.01],
        }
    )
    momentum_df = pd.DataFrame(
        {
            "factor_date": pd.to_datetime(["2026-01-02", "2026-01-05"]),
            "mom": [0.75, -0.2],
        }
    )

    def _fake_fetch_factor_frame(_session, url: str, columns: list[str]) -> pd.DataFrame:
        if columns == ["mkt_rf", "smb", "hml", "rf"]:
            return core_df.copy()
        if columns == ["mom"]:
            return momentum_df.copy()
        raise AssertionError(f"Unexpected column request: {columns} for {url}")

    fixed_now = datetime(2026, 3, 15, 16, 30, tzinfo=timezone.utc)

    class _FixedDateTime:
        @staticmethod
        def now(tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(ff_module, "_fetch_factor_frame", _fake_fetch_factor_frame)
    monkeypatch.setattr(ff_module, "datetime", _FixedDateTime)

    result = materialize([bronze_fama_french_factors])

    assert result.success
    out_path = data_root / "bronze" / "fama_french_factors" / "date=2026-03-15" / "factors.parquet"
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert list(df.columns) == [
        "factor_date",
        "mkt_rf",
        "smb",
        "hml",
        "rf",
        "mom",
        "source",
        "frequency",
        "ingested_ts",
    ]
    assert df["factor_date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-02", "2026-01-05"]
    assert df["mom"].tolist() == [0.75, -0.2]
    assert df["source"].nunique() == 1
    assert df["frequency"].tolist() == ["daily", "daily"]
