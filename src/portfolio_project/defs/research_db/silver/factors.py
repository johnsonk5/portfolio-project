import os
import shutil
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError

from portfolio_project.defs.research_db.bronze.fama_french import bronze_fama_french_factors
from portfolio_project.defs.research_db.dq_checks import log_required_field_null_check

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


def _silver_factors_root() -> Path:
    return DATA_ROOT / "silver" / "factors"


def _silver_factors_path() -> Path:
    return _silver_factors_root() / "factors.parquet"


def _parse_snapshot_date(snapshot_path: Path) -> date:
    snapshot_token = snapshot_path.parent.name
    if not snapshot_token.startswith("date="):
        raise ValueError(f"Unexpected bronze factors partition path: {snapshot_path}")
    return datetime.strptime(snapshot_token.split("=", 1)[1], "%Y-%m-%d").date()


def _latest_bronze_factors_snapshot() -> tuple[date, Path] | None:
    snapshots = sorted(
        (DATA_ROOT / "bronze" / "fama_french_factors").glob("date=*/factors.parquet"),
        key=lambda path: path.parent.name,
    )
    if not snapshots:
        return None
    latest_path = snapshots[-1]
    return _parse_snapshot_date(latest_path), latest_path


def _clear_silver_factors_root() -> None:
    silver_root = _silver_factors_root()
    if silver_root.exists():
        shutil.rmtree(silver_root)


def _normalized_factors_df(con, bronze_path: Path, snapshot_date: date) -> pd.DataFrame:
    bronze_df = pd.read_parquet(bronze_path)
    if bronze_df is None or bronze_df.empty:
        return pd.DataFrame()

    con.register("bronze_factors_df", bronze_df)
    normalized_df = con.execute(
        """
        SELECT
            CAST(factor_date AS DATE) AS factor_date,
            CAST(mkt_rf AS DOUBLE) AS mkt_rf,
            CAST(smb AS DOUBLE) AS smb,
            CAST(hml AS DOUBLE) AS hml,
            CAST(rf AS DOUBLE) AS rf,
            CAST(mom AS DOUBLE) AS mom,
            source,
            frequency,
            CAST(ingested_ts AS TIMESTAMP) AS ingested_ts,
            CAST(? AS DATE) AS bronze_snapshot_date
        FROM bronze_factors_df
        WHERE factor_date IS NOT NULL
        ORDER BY factor_date
        """,
        [snapshot_date],
    ).fetch_df()
    return normalized_df


def _write_silver_factors(df: pd.DataFrame) -> tuple[int, int]:
    if df is None or df.empty:
        return 0, 0

    out_path = _silver_factors_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return len(df), 1


@asset(
    name="silver_fama_french_factors_parquet",
    deps=[bronze_fama_french_factors],
    required_resource_keys={"research_duckdb", "duckdb"},
)
def silver_fama_french_factors_parquet(context: AssetExecutionContext) -> None:
    """
    Build normalized factor history parquet from the latest bronze Fama-French snapshot.
    """
    latest_snapshot = _latest_bronze_factors_snapshot()
    if latest_snapshot is None:
        context.log.warning("No bronze Fama-French snapshot was found.")
        return

    snapshot_date, bronze_path = latest_snapshot
    con = context.resources.research_duckdb
    normalized_df = _normalized_factors_df(con, bronze_path, snapshot_date)
    if normalized_df is None or normalized_df.empty:
        context.log.warning("Bronze Fama-French snapshot at %s is empty.", bronze_path)
        return

    _clear_silver_factors_root()
    rows_written, files_written = _write_silver_factors(normalized_df)
    if rows_written == 0:
        context.log.warning("No silver factor rows were written from %s.", bronze_path)
        return

    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None

    parquet_path = _silver_factors_path().as_posix()
    log_required_field_null_check(
        measured_con=context.resources.research_duckdb,
        observability_con=context.resources.duckdb,
        check_name="dq_research_factors_required_fields_nulls",
        relation_sql="SELECT * FROM read_parquet(?)",
        relation_params=[parquet_path],
        required_columns=[
            "factor_date",
            "mkt_rf",
            "smb",
            "hml",
            "rf",
            "source",
            "frequency",
            "ingested_ts",
            "bronze_snapshot_date",
        ],
        details={"path": parquet_path, "table": "silver.factors"},
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=getattr(context, "partition_key", None),
    )
    log_required_field_null_check(
        measured_con=context.resources.research_duckdb,
        observability_con=context.resources.duckdb,
        check_name="dq_research_factors_mom_required_after_start_nulls",
        relation_sql="""
            SELECT *
            FROM read_parquet(?)
            WHERE factor_date >= COALESCE(
                (
                    SELECT min(factor_date)
                    FROM read_parquet(?)
                    WHERE mom IS NOT NULL
                ),
                DATE '0001-01-01'
            )
        """,
        relation_params=[parquet_path, parquet_path],
        required_columns=["mom"],
        details={
            "path": parquet_path,
            "table": "silver.factors",
            "rule": "mom is required from the first non-null momentum row onward",
        },
        run_id=str(run_id) if run_id else None,
        job_name=job_name,
        partition_key=getattr(context, "partition_key", None),
    )

    context.add_output_metadata(
        {
            "bronze_snapshot_path": str(bronze_path),
            "bronze_snapshot_date": snapshot_date.isoformat(),
            "parquet_path": str(_silver_factors_path()),
            "row_count": rows_written,
            "files_written": files_written,
            "min_factor_date": normalized_df["factor_date"].min().isoformat(),
            "max_factor_date": normalized_df["factor_date"].max().isoformat(),
        }
    )
