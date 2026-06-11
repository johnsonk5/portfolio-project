from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import pandas as pd

DOWNSTREAM_TABLES = [
    "signals_daily",
    "universe_eligibility_daily",
    "universe_membership_daily",
    "universe_membership_events",
]


def _resolve_data_root(configured: str | None) -> Path:
    if configured:
        return Path(configured)
    return Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


def _resolve_research_db_path(configured: str | None, data_root: Path) -> Path:
    if configured:
        return Path(configured)
    env_path = os.getenv("PORTFOLIO_RESEARCH_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    return data_root / "duckdb" / "research.duckdb"


def _load_asset_id_map(con) -> pd.DataFrame:
    return con.execute(
        """
        WITH ranked AS (
            SELECT
                upper(trim(source_symbol)) AS symbol,
                CAST(asset_id AS BIGINT) AS asset_id,
                row_number() OVER (
                    PARTITION BY upper(trim(source_symbol))
                    ORDER BY source_priority ASC, mapping_confidence DESC, asset_id ASC
                ) AS rn
            FROM silver.security_identifiers
            WHERE is_current = TRUE
              AND asset_id IS NOT NULL
              AND source_symbol IS NOT NULL
              AND trim(source_symbol) <> ''
        )
        SELECT symbol, asset_id
        FROM ranked
        WHERE rn = 1
        ORDER BY symbol
        """
    ).fetch_df()


def _research_price_files(data_root: Path) -> list[Path]:
    prices_root = data_root / "silver" / "research_daily_prices"
    if not prices_root.exists():
        return []
    return sorted(prices_root.glob("month=*/date=*.parquet"))


def enrich_research_price_parquet(
    *,
    data_root: Path,
    asset_id_map_df: pd.DataFrame,
    dry_run: bool,
    limit_files: int | None = None,
) -> dict[str, int]:
    asset_map = dict(zip(asset_id_map_df["symbol"], asset_id_map_df["asset_id"]))
    files = _research_price_files(data_root)
    if limit_files is not None:
        files = files[:limit_files]

    stats = {
        "files_scanned": 0,
        "files_rewritten": 0,
        "rows_scanned": 0,
        "rows_filled": 0,
        "unmapped_rows": 0,
    }
    for path in files:
        stats["files_scanned"] += 1
        df = pd.read_parquet(path)
        if df.empty or "symbol" not in df.columns:
            continue
        stats["rows_scanned"] += len(df)
        if "asset_id" not in df.columns:
            df.insert(0, "asset_id", pd.NA)

        missing_mask = df["asset_id"].isna()
        if not missing_mask.any():
            continue

        resolved = df.loc[missing_mask, "symbol"].astype(str).str.strip().str.upper().map(asset_map)
        fill_mask = missing_mask.copy()
        fill_mask.loc[missing_mask] = resolved.notna().to_numpy()
        filled_count = int(fill_mask.sum())
        stats["rows_filled"] += filled_count
        stats["unmapped_rows"] += int(missing_mask.sum() - filled_count)
        if filled_count == 0:
            continue

        df.loc[fill_mask, "asset_id"] = resolved[resolved.notna()].astype("int64").to_numpy()
        df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce").astype("Int64")
        if not dry_run:
            temp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
            df.to_parquet(temp_path, index=False)
            temp_path.replace(path)
        stats["files_rewritten"] += 1

    return stats


def _table_exists(con, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'silver'
              AND table_name = ?
            LIMIT 1
            """,
            [table],
        ).fetchone()
        is not None
    )


def update_downstream_duckdb_tables(
    *,
    con,
    asset_id_map_df: pd.DataFrame,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {}
    con.register("asset_id_map_df", asset_id_map_df)
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE temp_asset_id_map AS
        SELECT
            upper(trim(symbol)) AS symbol,
            CAST(asset_id AS BIGINT) AS asset_id
        FROM asset_id_map_df
        WHERE symbol IS NOT NULL
          AND trim(symbol) <> ''
          AND asset_id IS NOT NULL
        """
    )
    for table in DOWNSTREAM_TABLES:
        if not _table_exists(con, table):
            continue
        if dry_run:
            has_asset_id = (
                con.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'silver'
                      AND table_name = ?
                      AND column_name = 'asset_id'
                    LIMIT 1
                    """,
                    [table],
                ).fetchone()
                is not None
            )
            if not has_asset_id:
                stats[table] = 0
                continue
        else:
            con.execute(f"ALTER TABLE silver.{table} ADD COLUMN IF NOT EXISTS asset_id BIGINT")
        fillable_count = con.execute(
            f"""
            SELECT count(*)
            FROM silver.{table} AS target
            INNER JOIN temp_asset_id_map AS m
                ON upper(trim(target.symbol)) = m.symbol
            WHERE target.asset_id IS NULL
            """
        ).fetchone()[0]
        stats[table] = int(fillable_count or 0)
        if dry_run or not fillable_count:
            continue
        con.execute(
            f"""
            UPDATE silver.{table} AS target
            SET asset_id = m.asset_id
            FROM temp_asset_id_map AS m
            WHERE target.asset_id IS NULL
              AND upper(trim(target.symbol)) = m.symbol
            """
        )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill research asset_id values from silver.security_identifiers."
    )
    parser.add_argument("--data-root", default=None)
    parser.add_argument("--research-db-path", default=None)
    parser.add_argument("--parquet-only", action="store_true")
    parser.add_argument("--duckdb-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-files", type=int, default=None)
    args = parser.parse_args()

    data_root = _resolve_data_root(args.data_root)
    research_db_path = _resolve_research_db_path(args.research_db_path, data_root)
    if not research_db_path.exists():
        raise FileNotFoundError(f"Research DuckDB not found at {research_db_path}")

    con = duckdb.connect(str(research_db_path), read_only=args.parquet_only or args.dry_run)
    try:
        asset_id_map_df = _load_asset_id_map(con)
        if asset_id_map_df.empty:
            raise ValueError("silver.security_identifiers has no usable current mappings.")
        print(f"Loaded {len(asset_id_map_df):,} symbol-to-asset_id mappings.")

        if not args.duckdb_only:
            parquet_stats = enrich_research_price_parquet(
                data_root=data_root,
                asset_id_map_df=asset_id_map_df,
                dry_run=args.dry_run,
                limit_files=args.limit_files,
            )
            print(f"Parquet enrichment: {parquet_stats}")

        if not args.parquet_only:
            table_stats = update_downstream_duckdb_tables(
                con=con,
                asset_id_map_df=asset_id_map_df,
                dry_run=args.dry_run,
            )
            print(f"DuckDB table updates: {table_stats}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
