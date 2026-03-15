import os
import sys
from datetime import date, timedelta
from math import sin, sqrt
from pathlib import Path

import duckdb
from dagster import materialize

from portfolio_project.defs.portfolio_db.demo.seed_data import (
    seed_demo_data,
)
from portfolio_project.defs.portfolio_db.gold.prices import _ensure_gold_table


DEMO_ASSETS = [
    (1, "AAPL", "Apple Inc."),
    (2, "MSFT", "Microsoft Corp."),
    (3, "NVDA", "NVIDIA Corp."),
    (4, "AMZN", "Amazon.com Inc."),
    (5, "GOOGL", "Alphabet Inc. Class A"),
]


def _resolve_data_root() -> Path:
    return Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


def _resolve_db_path(data_root: Path) -> Path:
    configured_path = os.getenv("PORTFOLIO_DUCKDB_PATH", "").strip()
    if configured_path:
        return Path(configured_path)
    return data_root / "duckdb" / "portfolio.duckdb"


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return row is not None


def _ensure_silver_assets_table(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            name VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    existing_count = con.execute("SELECT count(*) FROM silver.assets").fetchone()[0]
    if existing_count and int(existing_count) > 0:
        return 0
    con.executemany(
        "INSERT INTO silver.assets (asset_id, symbol, name, is_active) VALUES (?, ?, ?, TRUE)",
        DEMO_ASSETS,
    )
    return len(DEMO_ASSETS)


def _recent_trading_days(num_days: int, end_date: date) -> list[date]:
    trading_days: list[date] = []
    cursor = end_date
    while len(trading_days) < num_days:
        if cursor.weekday() < 5:
            trading_days.append(cursor)
        cursor -= timedelta(days=1)
    trading_days.reverse()
    return trading_days


def _seed_fallback_gold_prices(con: duckdb.DuckDBPyConnection) -> int:
    _ensure_gold_table(con)

    if _table_exists(con, "gold", "prices"):
        existing_count = con.execute("SELECT count(*) FROM gold.prices").fetchone()[0]
        if existing_count and int(existing_count) > 0:
            return 0

    trade_days = _recent_trading_days(num_days=320, end_date=date.today())
    rows: list[tuple] = []

    for asset_id, symbol, _name in DEMO_ASSETS:
        phase = asset_id * 0.9
        base_price = 80.0 + (asset_id * 35.0)
        trend = 0.12 + (asset_id * 0.015)
        closes: list[float] = []
        returns_1d: list[float | None] = []

        for idx, trade_day in enumerate(trade_days):
            close = base_price + (idx * trend) + 6.0 * sin((idx / 12.0) + phase)
            open_price = close * (1.0 + 0.003 * sin((idx / 7.0) + phase))
            high = max(open_price, close) * 1.006
            low = min(open_price, close) * 0.994
            volume = int(900_000 + 240_000 * (asset_id + 0.8 * sin(idx / 9.0 + phase)))
            trade_count = max(1, int(volume / 120))
            vwap = (open_price + high + low + close) / 4.0
            dollar_volume = close * volume

            prev_close = closes[idx - 1] if idx >= 1 else None
            ret_1d = (close / prev_close - 1.0) if prev_close else None
            ret_5d = (close / closes[idx - 5] - 1.0) if idx >= 5 else None
            ret_21d = (close / closes[idx - 21] - 1.0) if idx >= 21 else None
            returns_1d.append(ret_1d)

            vol_21d = None
            if idx >= 21:
                window = [v for v in returns_1d[idx - 20 : idx + 1] if v is not None]
                if len(window) >= 2:
                    mean = sum(window) / len(window)
                    variance = sum((v - mean) ** 2 for v in window) / (len(window) - 1)
                    vol_21d = sqrt(variance) * sqrt(252.0)

            momentum_12_1 = (closes[idx - 21] / closes[idx - 252] - 1.0) if idx >= 252 else None

            high_52w = max(closes[max(0, idx - 251) : idx + 1] + [close])
            pct_below_52w_high = (
                (high_52w - close) / high_52w if high_52w and high_52w > 0 else None
            )

            sma_50 = sum(closes[idx - 49 : idx + 1] + [close]) / 50.0 if idx >= 49 else None
            sma_200 = sum(closes[idx - 199 : idx + 1] + [close]) / 200.0 if idx >= 199 else None
            dist_sma_50 = (close / sma_50 - 1.0) if sma_50 else None
            dist_sma_200 = (close / sma_200 - 1.0) if sma_200 else None
            sentiment_score = 0.6 * sin((idx / 18.0) + phase)

            rows.append(
                (
                    asset_id,
                    symbol,
                    trade_day,
                    open_price,
                    high,
                    low,
                    close,
                    volume,
                    trade_count,
                    vwap,
                    dollar_volume,
                    ret_1d,
                    ret_5d,
                    ret_21d,
                    vol_21d,
                    momentum_12_1,
                    pct_below_52w_high,
                    sma_50,
                    sma_200,
                    dist_sma_50,
                    dist_sma_200,
                    sentiment_score,
                )
            )

            closes.append(close)

    con.executemany(
        """
        INSERT INTO gold.prices (
            asset_id, symbol, trade_date, open, high, low, close, volume, trade_count, vwap,
            dollar_volume, returns_1d, returns_5d, returns_21d, realized_vol_21d, momentum_12_1,
            pct_below_52w_high, sma_50, sma_200, dist_sma_50, dist_sma_200, sentiment_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


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

    inserted_assets = 0
    inserted_prices = 0
    con = duckdb.connect(str(db_path))
    try:
        inserted_assets = _ensure_silver_assets_table(con)
        inserted_prices = _seed_fallback_gold_prices(con)
    finally:
        con.close()

    print(f"Bootstrap complete. Data root: {data_root}")
    print(f"DuckDB path: {db_path}")
    if inserted_assets > 0 or inserted_prices > 0:
        print(
            "Demo fallback data was generated in DuckDB "
            f"(silver.assets inserted: {inserted_assets}, gold.prices inserted: {inserted_prices})."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

