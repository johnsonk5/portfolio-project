import os
import sys
from pathlib import Path

import duckdb
from dagster import materialize

from portfolio_project.defs.demo_seed_assets import (
    seed_demo_data,
)


def _resolve_data_root() -> Path:
    return Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


def _resolve_db_path(data_root: Path) -> Path:
    configured_path = os.getenv("PORTFOLIO_DUCKDB_PATH", "").strip()
    if configured_path:
        return Path(configured_path)
    return data_root / "duckdb" / "portfolio.duckdb"


def main() -> int:
    data_root = _resolve_data_root()
    db_path = _resolve_db_path(data_root)

    for directory in [
        data_root / "bronze",
        data_root / "silver",
        data_root / "gold",
        db_path.parent,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.close()

    result = materialize(
        assets=[seed_demo_data],
    )

    if not result.success:
        print("Bootstrap failed while seeding demo data.", file=sys.stderr)
        return 1

    print(f"Bootstrap complete. Data root: {data_root}")
    print(f"DuckDB path: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
