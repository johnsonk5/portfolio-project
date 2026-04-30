import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portfolio_project.defs.portfolio_db.gold import prices as gold_prices_module  # noqa: E402
from portfolio_project.defs.research_db.bronze import research_prices as bronze_module  # noqa: E402
from portfolio_project.defs.resources.alpaca import AlpacaClient  # noqa: E402
from portfolio_project.defs.resources.duckdb import (  # noqa: E402
    LockedDuckDBConnection,
    _acquire_duckdb_lock,
    _release_duckdb_lock,
    duckdb_lock_path_for,
    resolve_duckdb_path,
)
from portfolio_project.defs.resources.env import load_local_env  # noqa: E402

load_local_env()


class _Logger:
    def warning(self, message: str, *args) -> None:
        print("WARNING: " + (message % args if args else message))


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iter_calendar_dates(start_date: date, end_date: date, skip_weekends: bool):
    cursor = start_date
    while cursor <= end_date:
        if not skip_weekends or cursor.weekday() < 5:
            yield cursor
        cursor += timedelta(days=1)


def _chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = len(values) or 1
    return [values[index : index + size] for index in range(0, len(values), size)]


def _open_locked_duckdb(
    configured_path: str | None,
    *,
    env_var: str,
    default_db_name: str,
) -> LockedDuckDBConnection:
    db_path = resolve_duckdb_path(
        configured_path,
        env_var=env_var,
        default_db_name=default_db_name,
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = duckdb_lock_path_for(db_path)
    lock_fd = _acquire_duckdb_lock(lock_path)
    try:
        raw_connection = duckdb.connect(str(db_path))
    finally:
        _release_duckdb_lock(lock_path, lock_fd)

    return LockedDuckDBConnection(
        connection=raw_connection,
        lock_path=lock_path,
        lock_timeout_seconds=120,
        stale_lock_seconds=600,
    )


def _collect_symbols_from_portfolio_prices(
    data_root: Path,
    start_date: date,
    end_date: date,
) -> list[str]:
    symbols: set[str] = set()
    prices_root = data_root / "silver" / "prices"
    for trade_date in _iter_calendar_dates(start_date, end_date, skip_weekends=False):
        day_dir = prices_root / f"date={trade_date.isoformat()}"
        if not day_dir.exists():
            continue
        for symbol_dir in day_dir.glob("symbol=*"):
            symbol = symbol_dir.name.split("=", 1)[1].strip().upper()
            if symbol:
                symbols.add(symbol)
    return sorted(symbols)


def _refresh_corporate_actions(
    data_root: Path,
    start_date: date,
    end_date: date,
    symbols: list[str],
    batch_size: int,
    request_sleep_seconds: float,
) -> tuple[int, int]:
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError("ALPACA_API_KEY and ALPACA_SECRET_KEY must be set to refresh actions.")

    env_paper = os.getenv("ALPACA_PAPER")
    paper = (
        True
        if env_paper is None
        else env_paper.strip().lower()
        in {
            "1",
            "true",
            "t",
            "yes",
            "y",
            "on",
        }
    )
    client = AlpacaClient(api_key, secret_key, paper=paper)
    bronze_module.DATA_ROOT = data_root

    frames: list[pd.DataFrame] = []
    symbol_batches = _chunked(symbols, batch_size)
    for index, symbol_batch in enumerate(symbol_batches, start=1):
        raw_df = client.get_corporate_actions_df(
            symbols=symbol_batch,
            start_date=start_date,
            end_date=end_date,
            types=bronze_module.ALPACA_CORPORATE_ACTION_TYPES,
        )
        normalized = bronze_module._normalize_alpaca_corporate_actions_df(
            raw_df,
            start_date=start_date,
            end_date=end_date,
        )
        if not normalized.empty:
            frames.append(normalized)
        if request_sleep_seconds > 0 and index < len(symbol_batches):
            time.sleep(request_sleep_seconds)

    actions_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    overwrite_dates = list(_iter_calendar_dates(start_date, end_date, skip_weekends=False))
    return bronze_module._upsert_corporate_actions_file(
        actions_df,
        overwrite_effective_dates=overwrite_dates,
    )


def _rebuild_silver_corporate_actions_table(research_con) -> int:
    actions_path = bronze_module._corporate_actions_path()
    if not actions_path.exists():
        research_con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        research_con.execute(
            """
            CREATE OR REPLACE TABLE silver.alpaca_corporate_actions (
                action_id VARCHAR,
                symbol VARCHAR,
                action_type VARCHAR,
                effective_date DATE,
                process_date DATE,
                old_rate DOUBLE,
                new_rate DOUBLE,
                cash_rate DOUBLE,
                split_ratio DOUBLE,
                source VARCHAR,
                ingested_ts TIMESTAMP
            )
            """
        )
        return 0

    research_con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    research_con.execute(
        """
        CREATE OR REPLACE TABLE silver.alpaca_corporate_actions AS
        SELECT
            CAST(action_id AS VARCHAR) AS action_id,
            upper(trim(symbol)) AS symbol,
            CAST(action_type AS VARCHAR) AS action_type,
            CAST(effective_date AS DATE) AS effective_date,
            CAST(process_date AS DATE) AS process_date,
            CAST(old_rate AS DOUBLE) AS old_rate,
            CAST(new_rate AS DOUBLE) AS new_rate,
            CAST(cash_rate AS DOUBLE) AS cash_rate,
            CAST(split_ratio AS DOUBLE) AS split_ratio,
            CAST(source AS VARCHAR) AS source,
            CAST(ingested_ts AS TIMESTAMP) AS ingested_ts
        FROM read_parquet(?)
        WHERE symbol IS NOT NULL
          AND trim(symbol) <> ''
          AND effective_date IS NOT NULL
          AND (
                (
                    action_type IN ('forward_splits', 'reverse_splits')
                    AND old_rate IS NOT NULL
                    AND new_rate IS NOT NULL
                    AND old_rate > 0
                    AND new_rate > 0
                )
                OR (
                    action_type = 'cash_dividends'
                    AND cash_rate IS NOT NULL
                )
          )
        ORDER BY effective_date, symbol, action_type, process_date
        """,
        [actions_path.as_posix()],
    )
    return int(
        research_con.execute("SELECT count(*) FROM silver.alpaca_corporate_actions").fetchone()[0]
        or 0
    )


def _gold_partition_has_silver_prices(data_root: Path, trade_date: date) -> bool:
    return bool(gold_prices_module._silver_paths_for_day(trade_date))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill portfolio gold adjusted_close directly from existing silver price parquet "
            "and Alpaca corporate actions, without launching Dagster runs."
        )
    )
    parser.add_argument("--start-date", required=True, help="First gold price date, YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Last gold price date, YYYY-MM-DD.")
    parser.add_argument("--data-root", default=os.getenv("PORTFOLIO_DATA_DIR", "data"))
    parser.add_argument("--portfolio-duckdb-path", default=None)
    parser.add_argument("--research-duckdb-path", default=None)
    parser.add_argument(
        "--skip-weekends",
        action="store_true",
        help="Skip Saturday/Sunday when scanning gold price partitions.",
    )
    parser.add_argument(
        "--refresh-corporate-actions",
        action="store_true",
        help=(
            "Fetch Alpaca corporate actions before rebuilding gold. Use this when the bronze "
            "actions file may not include all future splits needed by the selected dates."
        ),
    )
    parser.add_argument(
        "--corporate-actions-start-date",
        default=None,
        help="First corporate-action effective date to refresh. Defaults to --start-date.",
    )
    parser.add_argument(
        "--corporate-actions-end-date",
        default=None,
        help="Last corporate-action effective date to refresh. Defaults to today.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols for action refresh.",
    )
    parser.add_argument("--alpaca-batch-size", type=int, default=200)
    parser.add_argument("--alpaca-request-sleep-seconds", type=float, default=0.25)
    parser.add_argument("--log-every", type=int, default=25)
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if end_date < start_date:
        raise ValueError("--end-date must be on or after --start-date")

    data_root = Path(args.data_root)
    os.environ["PORTFOLIO_DATA_DIR"] = str(data_root)
    bronze_module.DATA_ROOT = data_root
    gold_prices_module.DATA_ROOT = data_root

    portfolio_con = _open_locked_duckdb(
        args.portfolio_duckdb_path,
        env_var="PORTFOLIO_DUCKDB_PATH",
        default_db_name="portfolio.duckdb",
    )
    research_con = _open_locked_duckdb(
        args.research_duckdb_path,
        env_var="PORTFOLIO_RESEARCH_DUCKDB_PATH",
        default_db_name="research.duckdb",
    )

    try:
        action_rows_written = 0
        action_files_written = 0
        if args.refresh_corporate_actions:
            action_start = (
                _parse_date(args.corporate_actions_start_date)
                if args.corporate_actions_start_date
                else start_date
            )
            action_end = (
                _parse_date(args.corporate_actions_end_date)
                if args.corporate_actions_end_date
                else date.today()
            )
            if action_end < action_start:
                raise ValueError("--corporate-actions-end-date must be on or after start date")

            if args.symbols:
                symbols = sorted(
                    {symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()}
                )
            else:
                symbols = _collect_symbols_from_portfolio_prices(data_root, start_date, end_date)
            if not symbols:
                raise RuntimeError("No symbols found for corporate-action refresh.")

            print(
                f"Refreshing corporate actions symbols={len(symbols)} "
                f"effective_dates={action_start}..{action_end}"
            )
            action_rows_written, action_files_written = _refresh_corporate_actions(
                data_root=data_root,
                start_date=action_start,
                end_date=action_end,
                symbols=symbols,
                batch_size=args.alpaca_batch_size,
                request_sleep_seconds=args.alpaca_request_sleep_seconds,
            )

        silver_action_rows = _rebuild_silver_corporate_actions_table(research_con)
        context = SimpleNamespace(
            resources=SimpleNamespace(duckdb=portfolio_con, research_duckdb=research_con),
            log=_Logger(),
        )

        processed = 0
        inserted = 0
        deleted = 0
        skipped_missing_silver = 0
        for trade_date in _iter_calendar_dates(start_date, end_date, args.skip_weekends):
            if not _gold_partition_has_silver_prices(data_root, trade_date):
                skipped_missing_silver += 1
                continue
            row_count, deleted_count = gold_prices_module._upsert_gold_for_day(context, trade_date)
            processed += 1
            inserted += row_count
            deleted += deleted_count
            if args.log_every > 0 and processed % args.log_every == 0:
                print(
                    f"Gold progress processed={processed} inserted={inserted} "
                    f"deleted={deleted} skipped_missing_silver={skipped_missing_silver}"
                )

        print(
            "Done "
            f"gold_dates_processed={processed} gold_rows_inserted={inserted} "
            f"gold_rows_deleted={deleted} skipped_missing_silver={skipped_missing_silver} "
            f"bronze_action_rows_written={action_rows_written} "
            f"bronze_action_files_written={action_files_written} "
            f"silver_action_rows={silver_action_rows}"
        )
    finally:
        portfolio_con.close()
        research_con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
