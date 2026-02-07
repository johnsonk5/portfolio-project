import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dagster import AssetExecutionContext, asset


DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
TRANCO_LIST_ID = os.getenv("TRANCO_LIST_ID", "GVL2K").strip()
TRANCO_URL = os.getenv("TRANCO_URL", "https://tranco-list.eu/top-1m.csv")


@asset(name="bronze_tranco_snapshot")
def bronze_tranco_snapshot(context: AssetExecutionContext) -> None:
    """
    Download the latest Tranco top-1m list for weighting publisher influence.
    """
    snapshot_date = datetime.now(timezone.utc).date()
    out_dir = DATA_ROOT / "bronze" / "tranco" / f"date={snapshot_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "tranco.csv"

    if TRANCO_LIST_ID:
        url = f"https://tranco-list.eu/download/{TRANCO_LIST_ID}/1000000"
    else:
        url = TRANCO_URL

    response = requests.get(url, timeout=60, stream=True)
    if response.status_code == 404:
        context.log.warning(
            "Tranco URL returned 404 (%s). Set TRANCO_LIST_ID or TRANCO_URL.",
            url,
        )
        return
    response.raise_for_status()

    row_count = 0
    with out_path.open("wb") as handle:
        for line in response.iter_lines():
            if not line:
                continue
            handle.write(line + b"\n")
            row_count += 1

    context.add_output_metadata({"path": str(out_path), "row_count": row_count})
