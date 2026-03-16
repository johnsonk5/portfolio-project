import csv
import io
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dagster import AssetExecutionContext, asset


DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
FAMA_FRENCH_3_FACTORS_DAILY_URL = os.getenv(
    "FAMA_FRENCH_3_FACTORS_DAILY_URL",
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip",
)
FAMA_FRENCH_MOMENTUM_DAILY_URL = os.getenv(
    "FAMA_FRENCH_MOMENTUM_DAILY_URL",
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("FAMA_FRENCH_REQUEST_TIMEOUT_SECONDS", "60"))
USER_AGENT = os.getenv(
    "FAMA_FRENCH_USER_AGENT",
    "portfolio-project/0.1 (research data ingestion)",
)

_DATE_PATTERN = re.compile(r"^\d{8}$")


def _read_zipped_text(zip_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member_names = [name for name in zf.namelist() if not name.endswith("/")]
        if not member_names:
            raise ValueError("Zip archive did not contain a data file.")
        with zf.open(member_names[0]) as handle:
            return handle.read().decode("utf-8-sig", errors="replace")


def _extract_daily_rows(raw_text: str, expected_column_names: list[str]) -> pd.DataFrame:
    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(raw_text))
    expected_width = len(expected_column_names) + 1
    for row in reader:
        if len(row) < expected_width:
            continue
        date_value = row[0].strip()
        if not _DATE_PATTERN.match(date_value):
            continue
        rows.append([date_value, *[value.strip() for value in row[1:expected_width]]])

    if not rows:
        raise ValueError("No daily factor rows were found in the Kenneth French payload.")

    df = pd.DataFrame(rows, columns=["factor_date", *expected_column_names])
    df["factor_date"] = pd.to_datetime(df["factor_date"], format="%Y%m%d")
    for column in expected_column_names:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _fetch_factor_frame(
    session: requests.Session,
    url: str,
    columns: list[str],
) -> pd.DataFrame:
    response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    raw_text = _read_zipped_text(response.content)
    return _extract_daily_rows(raw_text, columns)


@asset(name="bronze_fama_french_factors")
def bronze_fama_french_factors(context: AssetExecutionContext) -> None:
    """
    Download the daily Fama-French 3 factors and daily momentum factor,
    then write a source-like bronze snapshot parquet file.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    core_df = _fetch_factor_frame(
        session,
        FAMA_FRENCH_3_FACTORS_DAILY_URL,
        ["mkt_rf", "smb", "hml", "rf"],
    )
    momentum_df = _fetch_factor_frame(
        session,
        FAMA_FRENCH_MOMENTUM_DAILY_URL,
        ["mom"],
    )

    df = core_df.merge(momentum_df, on="factor_date", how="outer", validate="one_to_one")
    df = df.sort_values("factor_date", kind="stable").reset_index(drop=True)
    df["source"] = "kenneth_r_french_data_library"
    df["frequency"] = "daily"
    df["ingested_ts"] = datetime.now(timezone.utc)

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    out_dir = DATA_ROOT / "bronze" / "fama_french_factors" / f"date={snapshot_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "factors.parquet"
    df.to_parquet(out_path, index=False)

    context.add_output_metadata(
        {
            "path": str(out_path),
            "row_count": len(df),
            "min_factor_date": df["factor_date"].min().date().isoformat(),
            "max_factor_date": df["factor_date"].max().date().isoformat(),
            "column_count": len(df.columns),
        }
    )
