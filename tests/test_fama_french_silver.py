from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from dagster import materialize

import portfolio_project.defs.portfolio_db.silver.factors as factors_module
from portfolio_project.defs.portfolio_db.silver.factors import (
    _latest_bronze_factors_snapshot,
    silver_fama_french_factors_parquet,
)


def test_latest_bronze_factors_snapshot_picks_most_recent_date(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    factors_module.DATA_ROOT = data_root

    first_dir = data_root / "bronze" / "fama_french_factors" / "date=2026-03-14"
    second_dir = data_root / "bronze" / "fama_french_factors" / "date=2026-03-15"
    first_dir.mkdir(parents=True, exist_ok=True)
    second_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"factor_date": pd.to_datetime(["2026-03-14"])}).to_parquet(
        first_dir / "factors.parquet", index=False
    )
    pd.DataFrame({"factor_date": pd.to_datetime(["2026-03-15"])}).to_parquet(
        second_dir / "factors.parquet", index=False
    )

    snapshot = _latest_bronze_factors_snapshot()

    assert snapshot is not None
    snapshot_date, snapshot_path = snapshot
    assert snapshot_date == date(2026, 3, 15)
    assert snapshot_path == second_dir / "factors.parquet"


def test_silver_fama_french_factors_materializes_single_parquet_file(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    factors_module.DATA_ROOT = data_root

    bronze_dir = data_root / "bronze" / "fama_french_factors" / "date=2026-03-15"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    bronze_df = pd.DataFrame(
        {
            "factor_date": pd.to_datetime(["2026-01-30", "2026-02-03"]),
            "mkt_rf": [0.5, -0.25],
            "smb": [0.1, 0.2],
            "hml": [-0.3, 0.4],
            "rf": [0.01, 0.01],
            "mom": [0.6, -0.1],
            "source": ["kenneth_r_french_data_library", "kenneth_r_french_data_library"],
            "frequency": ["daily", "daily"],
            "ingested_ts": pd.to_datetime(["2026-03-15T16:30:00Z", "2026-03-15T16:30:00Z"]),
        }
    )
    bronze_df.to_parquet(bronze_dir / "factors.parquet", index=False)

    con = duckdb.connect(":memory:")
    try:
        result = materialize(
            [silver_fama_french_factors_parquet],
            resources={"research_duckdb": con},
        )
        assert result.success

        out_path = data_root / "silver" / "factors" / "factors.parquet"
        assert out_path.exists()

        df = pd.read_parquet(out_path)
        assert df["factor_date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-30", "2026-02-03"]
        assert df["bronze_snapshot_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2026-03-15",
            "2026-03-15",
        ]
        assert df["mom"].tolist() == [0.6, -0.1]
    finally:
        con.close()
