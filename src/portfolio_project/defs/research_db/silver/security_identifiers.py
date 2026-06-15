import os
from pathlib import Path

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster._core.errors import DagsterInvalidPropertyError, DagsterInvariantViolationError

from portfolio_project.defs.research_db.dq_checks import (
    log_security_identifier_mapping_checks,
)

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
DEFAULT_SECURITY_IDENTIFIER_OVERRIDES_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "security_identifier_overrides.csv"
)

SECURITY_IDENTIFIERS_COLUMNS = [
    "asset_id",
    "source_symbol",
    "security_name",
    "identifier_type",
    "identifier_value",
    "cik",
    "sec_ticker",
    "alpaca_id",
    "exchange",
    "identifier_source",
    "source_priority",
    "mapping_confidence",
    "valid_from_date",
    "valid_to_date",
    "is_current",
    "source_snapshot_date",
    "ingestion_date",
    "ingested_ts",
]

ASSET_IDENTITY_BRIDGE_COLUMNS = [
    "asset_id",
    "current_symbol",
    "source_symbols",
    "alpaca_id",
    "cik",
    "security_name",
    "exchange",
    "is_current",
    "asof_ts",
]

ASSET_SYMBOL_BRIDGE_COLUMNS = [
    "asset_id",
    "current_symbol",
    "source_symbol",
    "symbol_role",
    "alpaca_id",
    "cik",
    "identifier_source",
    "source_priority",
    "mapping_confidence",
    "valid_from_date",
    "valid_to_date",
    "is_current",
    "source_snapshot_date",
    "asof_ts",
]


def _table_exists(con, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        is not None
    )


def _create_empty_security_identifiers(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.security_identifiers (
            asset_id BIGINT,
            source_symbol VARCHAR,
            security_name VARCHAR,
            identifier_type VARCHAR,
            identifier_value VARCHAR,
            cik VARCHAR,
            sec_ticker VARCHAR,
            alpaca_id VARCHAR,
            exchange VARCHAR,
            identifier_source VARCHAR,
            source_priority INTEGER,
            mapping_confidence DOUBLE,
            valid_from_date DATE,
            valid_to_date DATE,
            is_current BOOLEAN,
            source_snapshot_date DATE,
            ingestion_date DATE,
            ingested_ts TIMESTAMP
        )
        """
    )


def _max_int_value(series: pd.Series) -> int:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return 0
    return int(numeric.max())


def build_security_identifiers_from_assets_df(assets_df: pd.DataFrame) -> pd.DataFrame:
    if assets_df is None or assets_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    df = assets_df.copy()
    for column in ["asset_id", "symbol", "name", "alpaca_id", "exchange"]:
        if column not in df.columns:
            df[column] = pd.NA

    df = df[df["asset_id"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce").astype("Int64")
    df["source_symbol"] = df["symbol"].astype("string").str.strip().str.upper()
    df["security_name"] = df["name"].astype("string").str.strip()
    df["alpaca_id"] = df["alpaca_id"].astype("string").str.strip()
    df["exchange"] = df["exchange"].astype("string").str.strip().str.upper()
    df = df[df["source_symbol"].notna() & df["source_symbol"].ne("")].copy()

    rows = []
    base_columns = [
        "asset_id",
        "source_symbol",
        "security_name",
        "alpaca_id",
        "exchange",
    ]
    for row in df[base_columns].to_dict("records"):
        rows.append(
            {
                **row,
                "identifier_type": "symbol",
                "identifier_value": row["source_symbol"],
                "cik": pd.NA,
                "sec_ticker": pd.NA,
                "identifier_source": "portfolio_silver_assets",
                "source_priority": 10,
                "mapping_confidence": 0.95,
                "valid_from_date": pd.Timestamp("1900-01-01").date(),
                "valid_to_date": pd.NaT,
                "is_current": True,
                "source_snapshot_date": pd.NaT,
                "ingestion_date": pd.Timestamp.utcnow().date(),
                "ingested_ts": pd.Timestamp.utcnow(),
            }
        )
        alpaca_id = row.get("alpaca_id")
        if not pd.isna(alpaca_id) and str(alpaca_id).strip():
            rows.append(
                {
                    **row,
                    "identifier_type": "alpaca_id",
                    "identifier_value": str(alpaca_id).strip(),
                    "cik": pd.NA,
                    "sec_ticker": pd.NA,
                    "identifier_source": "portfolio_silver_assets",
                    "source_priority": 5,
                    "mapping_confidence": 1.0,
                    "valid_from_date": pd.Timestamp("1900-01-01").date(),
                    "valid_to_date": pd.NaT,
                    "is_current": True,
                    "source_snapshot_date": pd.NaT,
                    "ingestion_date": pd.Timestamp.utcnow().date(),
                    "ingested_ts": pd.Timestamp.utcnow(),
                }
            )

    if not rows:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    identifiers_df = pd.DataFrame(rows, columns=SECURITY_IDENTIFIERS_COLUMNS)
    identifiers_df = identifiers_df.sort_values(
        ["asset_id", "identifier_type", "identifier_value"],
        kind="stable",
    ).reset_index(drop=True)
    return identifiers_df


def _normalize_cik(value: object) -> object:
    if value is None:
        return pd.NA
    try:
        if pd.isna(value):
            return pd.NA
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return pd.NA
    if text.endswith(".0"):
        text = text[:-2]
    normalized = text.lstrip("0")
    return normalized or "0"


def _load_sp500_identifiers(
    con,
    portfolio_identifiers_df: pd.DataFrame,
) -> pd.DataFrame:
    if not _table_exists(con, "silver", "ref_sp500"):
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    sp500_df = con.execute(
        """
        SELECT *
        FROM silver.ref_sp500
        WHERE cik IS NOT NULL
          AND symbol IS NOT NULL
          AND trim(symbol) <> ''
        """
    ).fetch_df()
    if sp500_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    for column in ["asset_id", "symbol", "security", "cik"]:
        if column not in sp500_df.columns:
            sp500_df[column] = pd.NA
    sp500_df["source_symbol"] = sp500_df["symbol"].astype("string").str.strip().str.upper()
    sp500_df["cik"] = sp500_df["cik"].map(_normalize_cik)

    symbol_to_asset_id = {}
    if portfolio_identifiers_df is not None and not portfolio_identifiers_df.empty:
        current_symbols_df = portfolio_identifiers_df[
            portfolio_identifiers_df["identifier_type"].eq("symbol")
            & portfolio_identifiers_df["asset_id"].notna()
        ][["source_symbol", "asset_id"]].copy()
        current_symbols_df["source_symbol"] = (
            current_symbols_df["source_symbol"].astype("string").str.strip().str.upper()
        )
        symbol_to_asset_id = dict(
            zip(current_symbols_df["source_symbol"], current_symbols_df["asset_id"])
        )

    sp500_df["asset_id"] = pd.to_numeric(sp500_df["asset_id"], errors="coerce").astype("Int64")
    missing_asset_id = sp500_df["asset_id"].isna()
    sp500_df.loc[missing_asset_id, "asset_id"] = sp500_df.loc[
        missing_asset_id, "source_symbol"
    ].map(symbol_to_asset_id)
    sp500_df = sp500_df.dropna(subset=["asset_id", "source_symbol", "cik"]).copy()
    if sp500_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    now = pd.Timestamp.utcnow()
    rows = []
    for row in sp500_df.to_dict("records"):
        base = {
            "asset_id": int(row["asset_id"]),
            "source_symbol": row["source_symbol"],
            "security_name": row.get("security", pd.NA),
            "cik": row["cik"],
            "sec_ticker": row["source_symbol"],
            "alpaca_id": pd.NA,
            "exchange": pd.NA,
            "identifier_source": "sp500_wikipedia",
            "source_priority": 20,
            "mapping_confidence": 0.85,
            "valid_from_date": pd.Timestamp("1900-01-01").date(),
            "valid_to_date": pd.NaT,
            "is_current": True,
            "source_snapshot_date": pd.NaT,
            "ingestion_date": now.date(),
            "ingested_ts": now,
        }
        rows.append(
            {
                **base,
                "identifier_type": "cik",
                "identifier_value": row["cik"],
            }
        )
        rows.append(
            {
                **base,
                "identifier_type": "sec_ticker",
                "identifier_value": row["source_symbol"],
            }
        )

    return pd.DataFrame(rows, columns=SECURITY_IDENTIFIERS_COLUMNS)


def _sec_company_tickers_root(data_root: Path | None = None) -> Path:
    root = data_root or DATA_ROOT
    return root / "bronze" / "sec_company_tickers"


def _sec_company_tickers_files(data_root: Path | None = None) -> list[Path]:
    tickers_root = _sec_company_tickers_root(data_root)
    if not tickers_root.exists():
        return []
    return sorted(tickers_root.glob("ingestion_date=*/tickers.parquet"))


def _load_sec_company_tickers(data_root: Path | None = None) -> pd.DataFrame:
    files = _sec_company_tickers_files(data_root)
    if not files:
        return pd.DataFrame(
            columns=[
                "cik",
                "name",
                "ticker",
                "exchange",
                "ingestion_date",
                "ingested_ts",
            ]
        )
    reader_con = duckdb.connect(":memory:")
    try:
        return reader_con.execute(
            """
            SELECT
                cik,
                name,
                ticker,
                exchange,
                CAST(ingestion_date AS DATE) AS ingestion_date,
                CAST(ingested_ts AS TIMESTAMP) AS ingested_ts
            FROM read_parquet(?, union_by_name = true)
            WHERE cik IS NOT NULL
              AND trim(cik) <> ''
              AND ticker IS NOT NULL
              AND trim(ticker) <> ''
            ORDER BY
                upper(trim(ticker)),
                coalesce(nullif(trim(cik), ''), '0'),
                CAST(ingestion_date AS DATE),
                CAST(ingested_ts AS TIMESTAMP)
            """,
            [[path.as_posix() for path in files]],
        ).fetch_df()
    finally:
        reader_con.close()


def build_sec_company_ticker_identifiers_frame(
    sec_company_tickers_df: pd.DataFrame,
    portfolio_identifiers_df: pd.DataFrame,
) -> pd.DataFrame:
    if sec_company_tickers_df is None or sec_company_tickers_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    sec_df = sec_company_tickers_df.copy()
    for column in ["cik", "name", "ticker", "exchange", "ingestion_date", "ingested_ts"]:
        if column not in sec_df.columns:
            sec_df[column] = pd.NA
    sec_df["source_symbol"] = sec_df["ticker"].astype("string").str.strip().str.upper()
    sec_df["cik"] = sec_df["cik"].map(_normalize_cik)
    sec_df["security_name"] = sec_df["name"].astype("string").str.strip()
    sec_df["exchange"] = sec_df["exchange"].astype("string").str.strip().str.upper()
    sec_df["ingestion_date"] = pd.to_datetime(sec_df["ingestion_date"], errors="coerce")
    sec_df["ingested_ts"] = pd.to_datetime(sec_df["ingested_ts"], errors="coerce", utc=True)
    sec_df = sec_df.dropna(subset=["source_symbol", "cik"]).copy()
    sec_df = sec_df[sec_df["source_symbol"].ne("")]
    if sec_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    if portfolio_identifiers_df is None or portfolio_identifiers_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    project_symbols_df = portfolio_identifiers_df[
        portfolio_identifiers_df["identifier_type"].eq("symbol")
        & portfolio_identifiers_df["asset_id"].notna()
    ][["source_symbol", "asset_id", "alpaca_id"]].copy()
    project_symbols_df["source_symbol"] = (
        project_symbols_df["source_symbol"].astype("string").str.strip().str.upper()
    )
    project_symbols_df = project_symbols_df.dropna(subset=["source_symbol", "asset_id"])
    project_symbols_df = project_symbols_df.drop_duplicates(subset=["source_symbol"], keep="first")
    if project_symbols_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    sec_df = sec_df.merge(
        project_symbols_df,
        on="source_symbol",
        how="inner",
        suffixes=("", "_project"),
    )
    if sec_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    sec_df["asset_id"] = pd.to_numeric(sec_df["asset_id"], errors="coerce").astype("Int64")
    sec_df = sec_df.dropna(subset=["asset_id"])
    sec_df["source_snapshot_date"] = sec_df["ingestion_date"].dt.date
    sec_df = sec_df.sort_values(
        ["source_symbol", "cik", "ingestion_date", "ingested_ts"],
        ascending=[True, True, True, True],
        kind="stable",
    ).drop_duplicates(
        subset=["asset_id", "source_symbol", "cik", "source_snapshot_date"],
        keep="last",
    )
    sec_df["latest_snapshot_date"] = sec_df.groupby(
        ["asset_id", "source_symbol", "cik"], dropna=False
    )["source_snapshot_date"].transform("max")

    rows = []
    now = pd.Timestamp.utcnow()
    for row in sec_df.to_dict("records"):
        ingestion_date = pd.to_datetime(row.get("ingestion_date"), errors="coerce")
        ingested_ts = pd.to_datetime(row.get("ingested_ts"), errors="coerce")
        base = {
            "asset_id": int(row["asset_id"]),
            "source_symbol": row["source_symbol"],
            "security_name": row.get("security_name", pd.NA),
            "cik": row["cik"],
            "sec_ticker": row["source_symbol"],
            "alpaca_id": row.get("alpaca_id", pd.NA),
            "exchange": row.get("exchange", pd.NA),
            "identifier_source": "sec_company_tickers",
            "source_priority": 15,
            "mapping_confidence": 0.9,
            "valid_from_date": pd.Timestamp("1900-01-01").date(),
            "valid_to_date": pd.NaT,
            "is_current": row.get("source_snapshot_date") == row.get("latest_snapshot_date"),
            "source_snapshot_date": row.get("source_snapshot_date", pd.NaT),
            "ingestion_date": (
                ingestion_date.date() if not pd.isna(ingestion_date) else now.date()
            ),
            "ingested_ts": ingested_ts if not pd.isna(ingested_ts) else now,
        }
        rows.append(
            {
                **base,
                "identifier_type": "cik",
                "identifier_value": row["cik"],
            }
        )
        rows.append(
            {
                **base,
                "identifier_type": "sec_ticker",
                "identifier_value": row["source_symbol"],
            }
        )

    if not rows:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    return pd.DataFrame(rows, columns=SECURITY_IDENTIFIERS_COLUMNS)


def _research_prices_glob(data_root: Path | None = None) -> str:
    root = data_root or DATA_ROOT
    return (root / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet").as_posix()


def _research_prices_files_exist(data_root: Path | None = None) -> bool:
    root = data_root or DATA_ROOT
    prices_root = root / "silver" / "research_daily_prices"
    return prices_root.exists() and any(prices_root.glob("month=*/date=*.parquet"))


def _load_research_price_symbols(con, data_root: Path | None = None) -> pd.DataFrame:
    del con
    if not _research_prices_files_exist(data_root):
        return pd.DataFrame(columns=["source_symbol", "first_trade_date", "last_trade_date"])
    reader_con = duckdb.connect(":memory:")
    try:
        return reader_con.execute(
            """
            SELECT
                upper(trim(symbol)) AS source_symbol,
                min(CAST(trade_date AS DATE)) AS first_trade_date,
                max(CAST(trade_date AS DATE)) AS last_trade_date
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol IS NOT NULL
              AND trim(symbol) <> ''
            GROUP BY upper(trim(symbol))
            ORDER BY source_symbol
            """,
            [_research_prices_glob(data_root)],
        ).fetch_df()
    finally:
        reader_con.close()


def _load_existing_research_identifier_map(con) -> pd.DataFrame:
    if not _table_exists(con, "silver", "security_identifiers"):
        return pd.DataFrame(columns=["source_symbol", "asset_id"])
    return con.execute(
        """
        SELECT
            upper(trim(source_symbol)) AS source_symbol,
            min(CAST(asset_id AS BIGINT)) AS asset_id
        FROM silver.security_identifiers
        WHERE identifier_source = 'research_daily_prices'
          AND identifier_type = 'symbol'
          AND asset_id IS NOT NULL
          AND source_symbol IS NOT NULL
          AND trim(source_symbol) <> ''
        GROUP BY upper(trim(source_symbol))
        ORDER BY source_symbol
        """
    ).fetch_df()


def build_research_symbol_identifiers_frame(
    research_symbols_df: pd.DataFrame,
    portfolio_identifiers_df: pd.DataFrame,
    existing_research_identifiers_df: pd.DataFrame,
) -> pd.DataFrame:
    if research_symbols_df is None or research_symbols_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    symbols_df = research_symbols_df.copy()
    for column in ["source_symbol", "first_trade_date"]:
        if column not in symbols_df.columns:
            symbols_df[column] = pd.NA
    symbols_df["source_symbol"] = (
        symbols_df["source_symbol"].astype("string").str.strip().str.upper()
    )
    symbols_df = symbols_df[
        symbols_df["source_symbol"].notna() & symbols_df["source_symbol"].ne("")
    ]
    symbols_df = symbols_df.drop_duplicates(subset=["source_symbol"], keep="first")
    if symbols_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    mapped_symbols = set()
    max_asset_id = 0
    if portfolio_identifiers_df is not None and not portfolio_identifiers_df.empty:
        mapped_symbols = set(
            portfolio_identifiers_df["source_symbol"].dropna().astype(str).str.upper()
        )
        max_asset_id = max(
            max_asset_id,
            _max_int_value(portfolio_identifiers_df["asset_id"]),
        )

    if existing_research_identifiers_df is None or existing_research_identifiers_df.empty:
        existing_df = pd.DataFrame(columns=["source_symbol", "asset_id"])
    else:
        existing_df = existing_research_identifiers_df.copy()
    existing_df["source_symbol"] = (
        existing_df["source_symbol"].astype("string").str.strip().str.upper()
    )
    existing_df["asset_id"] = pd.to_numeric(existing_df["asset_id"], errors="coerce").astype(
        "Int64"
    )
    existing_df = existing_df.dropna(subset=["source_symbol", "asset_id"])
    existing_by_symbol = dict(zip(existing_df["source_symbol"], existing_df["asset_id"]))
    if not existing_df.empty:
        max_asset_id = max(max_asset_id, int(existing_df["asset_id"].max()))

    research_only = symbols_df[~symbols_df["source_symbol"].isin(mapped_symbols)].copy()
    if research_only.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    rows = []
    next_asset_id = max_asset_id + 1
    for row in research_only.sort_values("source_symbol", kind="stable").to_dict("records"):
        source_symbol = str(row["source_symbol"])
        asset_id = existing_by_symbol.get(source_symbol)
        if asset_id is None or pd.isna(asset_id):
            asset_id = next_asset_id
            next_asset_id += 1
        first_trade_date = pd.to_datetime(row.get("first_trade_date"), errors="coerce")
        valid_from_date = (
            first_trade_date.date()
            if not pd.isna(first_trade_date)
            else pd.Timestamp("1900-01-01").date()
        )
        rows.append(
            {
                "asset_id": int(asset_id),
                "source_symbol": source_symbol,
                "security_name": pd.NA,
                "identifier_type": "symbol",
                "identifier_value": source_symbol,
                "cik": pd.NA,
                "sec_ticker": pd.NA,
                "alpaca_id": pd.NA,
                "exchange": pd.NA,
                "identifier_source": "research_daily_prices",
                "source_priority": 50,
                "mapping_confidence": 0.6,
                "valid_from_date": valid_from_date,
                "valid_to_date": pd.NaT,
                "is_current": True,
                "source_snapshot_date": pd.NaT,
                "ingestion_date": pd.Timestamp.utcnow().date(),
                "ingested_ts": pd.Timestamp.utcnow(),
            }
        )

    if not rows:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    return pd.DataFrame(rows, columns=SECURITY_IDENTIFIERS_COLUMNS)


def _load_security_identifier_overrides() -> pd.DataFrame:
    configured_path = os.getenv("PORTFOLIO_SECURITY_IDENTIFIER_OVERRIDES_PATH")
    path = Path(configured_path) if configured_path else DEFAULT_SECURITY_IDENTIFIER_OVERRIDES_PATH
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def build_manual_security_identifier_overrides_frame(
    overrides_df: pd.DataFrame,
    mapped_identifiers_df: pd.DataFrame,
) -> pd.DataFrame:
    if overrides_df is None or overrides_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    if mapped_identifiers_df is None or mapped_identifiers_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    symbol_map_df = mapped_identifiers_df[
        mapped_identifiers_df["identifier_type"].eq("symbol")
        & mapped_identifiers_df["asset_id"].notna()
        & mapped_identifiers_df["source_symbol"].notna()
    ][["source_symbol", "asset_id", "alpaca_id", "identifier_source"]].copy()
    symbol_map_df["source_symbol"] = (
        symbol_map_df["source_symbol"].astype("string").str.strip().str.upper()
    )
    symbol_map_df = symbol_map_df.dropna(subset=["source_symbol", "asset_id"])
    symbol_counts = symbol_map_df.groupby("source_symbol")["asset_id"].nunique().reset_index()
    unambiguous_symbols = set(
        symbol_counts.loc[symbol_counts["asset_id"].eq(1), "source_symbol"].astype(str)
    )
    symbol_map_df = symbol_map_df[symbol_map_df["source_symbol"].isin(unambiguous_symbols)]
    research_symbols = set(
        symbol_map_df.loc[
            symbol_map_df["identifier_source"].eq("research_daily_prices"),
            "source_symbol",
        ].astype(str)
    )
    symbol_map_df = symbol_map_df.drop_duplicates(subset=["source_symbol"], keep="first")
    if symbol_map_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    overrides = overrides_df.copy()
    for column in [
        "source_symbol",
        "canonical_symbol",
        "asset_id",
        "security_name",
        "cik",
        "sec_ticker",
        "exchange",
        "valid_from_date",
        "valid_to_date",
        "is_current",
        "mapping_source",
        "confidence",
        "notes",
    ]:
        if column not in overrides.columns:
            overrides[column] = pd.NA
    overrides["source_symbol"] = overrides["source_symbol"].astype("string").str.strip().str.upper()
    overrides["canonical_symbol"] = (
        overrides["canonical_symbol"].astype("string").str.strip().str.upper()
    )
    missing_canonical = overrides["canonical_symbol"].isna() | overrides["canonical_symbol"].eq("")
    overrides.loc[missing_canonical, "canonical_symbol"] = overrides.loc[
        missing_canonical, "source_symbol"
    ]
    overrides["cik"] = overrides["cik"].map(_normalize_cik)
    overrides["sec_ticker"] = overrides["sec_ticker"].astype("string").str.strip().str.upper()
    missing_sec_ticker = overrides["sec_ticker"].isna() | overrides["sec_ticker"].eq("")
    overrides.loc[missing_sec_ticker, "sec_ticker"] = overrides.loc[
        missing_sec_ticker, "source_symbol"
    ]
    overrides["exchange"] = overrides["exchange"].astype("string").str.strip().str.upper()
    overrides["mapping_source"] = overrides["mapping_source"].astype("string").str.strip()
    overrides["confidence"] = pd.to_numeric(overrides["confidence"], errors="coerce").fillna(0.0)
    overrides["valid_from_date"] = (
        pd.to_datetime(
            overrides["valid_from_date"],
            errors="coerce",
        )
        .fillna(pd.Timestamp("1900-01-01"))
        .dt.date
    )
    overrides["valid_to_date"] = pd.to_datetime(overrides["valid_to_date"], errors="coerce").dt.date
    overrides["is_current"] = overrides["is_current"].map(
        lambda value: str(value).strip().lower() not in {"0", "false", "f", "no", "n"}
    )
    overrides["asset_id"] = pd.to_numeric(overrides["asset_id"], errors="coerce").astype("Int64")
    overrides = overrides.dropna(subset=["source_symbol", "canonical_symbol", "cik"])
    overrides = overrides[overrides["source_symbol"].ne("") & overrides["canonical_symbol"].ne("")]
    if overrides.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    overrides = overrides[overrides["canonical_symbol"].isin(research_symbols)]
    if overrides.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    overrides = overrides.merge(
        symbol_map_df.rename(columns={"source_symbol": "canonical_symbol"}),
        on="canonical_symbol",
        how="inner",
        suffixes=("", "_derived"),
    )
    if overrides.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    missing_asset_id = overrides["asset_id"].isna()
    overrides.loc[missing_asset_id, "asset_id"] = overrides.loc[
        missing_asset_id, "asset_id_derived"
    ]
    overrides = overrides[
        overrides["asset_id"].astype("Int64").eq(overrides["asset_id_derived"].astype("Int64"))
    ].copy()
    overrides["asset_id"] = pd.to_numeric(overrides["asset_id"], errors="coerce").astype("Int64")
    overrides = overrides.dropna(subset=["asset_id"])

    now = pd.Timestamp.utcnow()
    rows = []
    for row in overrides.to_dict("records"):
        base = {
            "asset_id": int(row["asset_id"]),
            "source_symbol": row["source_symbol"],
            "security_name": row.get("security_name", pd.NA),
            "cik": row["cik"],
            "sec_ticker": row["sec_ticker"],
            "alpaca_id": row.get("alpaca_id", pd.NA),
            "exchange": row.get("exchange", pd.NA),
            "identifier_source": "manual_security_identifier_overrides",
            "source_priority": 18,
            "mapping_confidence": float(row["confidence"]),
            "valid_from_date": row["valid_from_date"],
            "valid_to_date": row["valid_to_date"],
            "is_current": bool(row["is_current"]),
            "source_snapshot_date": pd.NaT,
            "ingestion_date": now.date(),
            "ingested_ts": now,
        }
        rows.append({**base, "identifier_type": "cik", "identifier_value": row["cik"]})
        rows.append(
            {
                **base,
                "identifier_type": "sec_ticker",
                "identifier_value": row["sec_ticker"],
            }
        )

    if not rows:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    return pd.DataFrame(rows, columns=SECURITY_IDENTIFIERS_COLUMNS)


def _first_non_empty(values: pd.Series) -> object:
    for value in values:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if text:
            return text
    return pd.NA


def _bool_or_false(value: object) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return bool(value)


def _prepare_identifier_frame(identifiers_df: pd.DataFrame) -> pd.DataFrame:
    if identifiers_df is None or identifiers_df.empty:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    prepared = identifiers_df.copy()
    for column in SECURITY_IDENTIFIERS_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[SECURITY_IDENTIFIERS_COLUMNS].copy()
    prepared["asset_id"] = pd.to_numeric(prepared["asset_id"], errors="coerce").astype("Int64")
    prepared["source_symbol"] = prepared["source_symbol"].astype("string").str.strip().str.upper()
    prepared["identifier_type"] = (
        prepared["identifier_type"].astype("string").str.strip().str.lower()
    )
    prepared["identifier_source"] = (
        prepared["identifier_source"].astype("string").str.strip().str.lower()
    )
    prepared["source_priority"] = (
        pd.to_numeric(prepared["source_priority"], errors="coerce").fillna(999).astype("int64")
    )
    prepared["mapping_confidence"] = pd.to_numeric(
        prepared["mapping_confidence"], errors="coerce"
    ).fillna(0.0)
    prepared["cik"] = prepared["cik"].map(_normalize_cik)
    prepared["source_snapshot_date"] = pd.to_datetime(
        prepared["source_snapshot_date"], errors="coerce"
    ).dt.date
    prepared = prepared.dropna(subset=["asset_id"])
    return prepared


def _build_current_symbol_map(identifiers_df: pd.DataFrame) -> dict[int, str]:
    symbols_df = identifiers_df[
        identifiers_df["identifier_type"].eq("symbol")
        & identifiers_df["is_current"].fillna(False).astype(bool)
        & identifiers_df["source_symbol"].notna()
        & identifiers_df["source_symbol"].ne("")
    ].copy()
    if symbols_df.empty:
        return {}
    symbols_df = symbols_df.sort_values(
        ["asset_id", "source_priority", "mapping_confidence", "source_symbol"],
        ascending=[True, True, False, True],
        kind="stable",
    )
    current_df = symbols_df.drop_duplicates(subset=["asset_id"], keep="first")
    return {
        int(row["asset_id"]): str(row["source_symbol"])
        for row in current_df[["asset_id", "source_symbol"]].to_dict("records")
    }


def build_asset_identity_bridge_frame(identifiers_df: pd.DataFrame) -> pd.DataFrame:
    prepared = _prepare_identifier_frame(identifiers_df)
    if prepared.empty:
        return pd.DataFrame(columns=ASSET_IDENTITY_BRIDGE_COLUMNS)

    current_symbol_map = _build_current_symbol_map(prepared)
    now = pd.Timestamp.utcnow()
    rows = []
    for asset_id, group in prepared.groupby("asset_id", sort=True):
        symbol_values = sorted(
            {
                str(value)
                for value in group["source_symbol"].dropna().astype(str)
                if str(value).strip()
            }
        )
        rows.append(
            {
                "asset_id": int(asset_id),
                "current_symbol": current_symbol_map.get(int(asset_id), pd.NA),
                "source_symbols": ",".join(symbol_values) if symbol_values else pd.NA,
                "alpaca_id": _first_non_empty(
                    pd.concat(
                        [
                            group.loc[
                                group["identifier_type"].eq("alpaca_id"),
                                "identifier_value",
                            ],
                            group["alpaca_id"],
                        ],
                        ignore_index=True,
                    )
                ),
                "cik": _first_non_empty(group["cik"]),
                "security_name": _first_non_empty(group["security_name"]),
                "exchange": _first_non_empty(group["exchange"]),
                "is_current": bool(group["is_current"].fillna(False).astype(bool).any()),
                "asof_ts": now,
            }
        )

    return pd.DataFrame(rows, columns=ASSET_IDENTITY_BRIDGE_COLUMNS)


def build_asset_symbol_bridge_frame(identifiers_df: pd.DataFrame) -> pd.DataFrame:
    prepared = _prepare_identifier_frame(identifiers_df)
    if prepared.empty:
        return pd.DataFrame(columns=ASSET_SYMBOL_BRIDGE_COLUMNS)

    current_symbol_map = _build_current_symbol_map(prepared)
    identity_df = build_asset_identity_bridge_frame(prepared)
    alpaca_by_asset = dict(zip(identity_df["asset_id"], identity_df["alpaca_id"]))
    cik_by_asset = dict(zip(identity_df["asset_id"], identity_df["cik"]))
    now = pd.Timestamp.utcnow()
    symbol_df = prepared[
        prepared["source_symbol"].notna() & prepared["source_symbol"].ne("")
    ].copy()
    symbol_df = symbol_df.sort_values(
        [
            "asset_id",
            "source_symbol",
            "is_current",
            "source_priority",
            "mapping_confidence",
        ],
        ascending=[True, True, False, True, False],
        kind="stable",
    )
    symbol_df = symbol_df.drop_duplicates(
        subset=[
            "asset_id",
            "source_symbol",
            "valid_from_date",
            "valid_to_date",
        ],
        keep="first",
    )

    rows = []
    for row in symbol_df.to_dict("records"):
        asset_id = int(row["asset_id"])
        source_symbol = str(row["source_symbol"])
        current_symbol = current_symbol_map.get(asset_id, pd.NA)
        rows.append(
            {
                "asset_id": asset_id,
                "current_symbol": current_symbol,
                "source_symbol": source_symbol,
                "symbol_role": (
                    "current"
                    if isinstance(current_symbol, str) and source_symbol == current_symbol
                    else "historical_or_source"
                ),
                "alpaca_id": alpaca_by_asset.get(asset_id, pd.NA),
                "cik": cik_by_asset.get(asset_id, pd.NA),
                "identifier_source": row["identifier_source"],
                "source_priority": int(row["source_priority"]),
                "mapping_confidence": float(row["mapping_confidence"]),
                "valid_from_date": row["valid_from_date"],
                "valid_to_date": row["valid_to_date"],
                "is_current": _bool_or_false(row["is_current"]),
                "source_snapshot_date": row["source_snapshot_date"],
                "asof_ts": now,
            }
        )

    return pd.DataFrame(rows, columns=ASSET_SYMBOL_BRIDGE_COLUMNS)


def _write_identifier_tables(con, identifiers_df: pd.DataFrame) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.register("security_identifiers_df", identifiers_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.security_identifiers AS
        SELECT
            asset_id::BIGINT AS asset_id,
            source_symbol::VARCHAR AS source_symbol,
            security_name::VARCHAR AS security_name,
            identifier_type::VARCHAR AS identifier_type,
            identifier_value::VARCHAR AS identifier_value,
            cik::VARCHAR AS cik,
            sec_ticker::VARCHAR AS sec_ticker,
            alpaca_id::VARCHAR AS alpaca_id,
            exchange::VARCHAR AS exchange,
            identifier_source::VARCHAR AS identifier_source,
            source_priority::INTEGER AS source_priority,
            mapping_confidence::DOUBLE AS mapping_confidence,
            valid_from_date::DATE AS valid_from_date,
            valid_to_date::DATE AS valid_to_date,
            is_current::BOOLEAN AS is_current,
            source_snapshot_date::DATE AS source_snapshot_date,
            ingestion_date::DATE AS ingestion_date,
            ingested_ts::TIMESTAMP AS ingested_ts
        FROM security_identifiers_df
        """
    )

    identity_bridge_df = build_asset_identity_bridge_frame(identifiers_df)
    symbol_bridge_df = build_asset_symbol_bridge_frame(identifiers_df)
    con.register("asset_identity_bridge_df", identity_bridge_df)
    con.register("asset_symbol_bridge_df", symbol_bridge_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.asset_identity_bridge AS
        SELECT
            asset_id::BIGINT AS asset_id,
            current_symbol::VARCHAR AS current_symbol,
            source_symbols::VARCHAR AS source_symbols,
            alpaca_id::VARCHAR AS alpaca_id,
            cik::VARCHAR AS cik,
            security_name::VARCHAR AS security_name,
            exchange::VARCHAR AS exchange,
            is_current::BOOLEAN AS is_current,
            asof_ts::TIMESTAMP AS asof_ts
        FROM asset_identity_bridge_df
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.asset_symbol_bridge AS
        SELECT
            asset_id::BIGINT AS asset_id,
            current_symbol::VARCHAR AS current_symbol,
            source_symbol::VARCHAR AS source_symbol,
            symbol_role::VARCHAR AS symbol_role,
            alpaca_id::VARCHAR AS alpaca_id,
            cik::VARCHAR AS cik,
            identifier_source::VARCHAR AS identifier_source,
            source_priority::INTEGER AS source_priority,
            mapping_confidence::DOUBLE AS mapping_confidence,
            valid_from_date::DATE AS valid_from_date,
            valid_to_date::DATE AS valid_to_date,
            is_current::BOOLEAN AS is_current,
            source_snapshot_date::DATE AS source_snapshot_date,
            asof_ts::TIMESTAMP AS asof_ts
        FROM asset_symbol_bridge_df
        """
    )


def _context_dq_metadata(
    context: AssetExecutionContext,
) -> tuple[str | None, str | None, str | None]:
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    run_id = getattr(run, "run_id", None) or getattr(context, "run_id", None)
    try:
        job_name = getattr(context, "job_name", None)
    except DagsterInvalidPropertyError:
        job_name = None
    try:
        partition_key = getattr(context, "partition_key", None)
    except (DagsterInvalidPropertyError, DagsterInvariantViolationError):
        partition_key = None
    return str(run_id) if run_id else None, job_name, partition_key


def materialize_security_identifier_tables(
    context: AssetExecutionContext,
    *,
    data_root: Path | None = None,
) -> dict[str, int]:
    """
    Mirror durable project identifiers into both DuckDB databases.
    """
    portfolio_con = context.resources.duckdb
    research_con = context.resources.research_duckdb
    portfolio_con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    research_con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    if _table_exists(portfolio_con, "silver", "assets"):
        assets_df = portfolio_con.execute(
            """
            SELECT *
            FROM silver.assets
            WHERE asset_id IS NOT NULL
              AND symbol IS NOT NULL
              AND trim(symbol) <> ''
            """
        ).fetch_df()
    else:
        context.log.warning(
            "portfolio silver.assets is missing; building research-only identifiers."
        )
        assets_df = pd.DataFrame()
    portfolio_identifiers_df = build_security_identifiers_from_assets_df(assets_df)
    sp500_identifiers_df = _load_sp500_identifiers(
        portfolio_con,
        portfolio_identifiers_df,
    )
    sec_company_tickers_df = _load_sec_company_tickers(data_root)
    sec_company_ticker_identifiers_df = build_sec_company_ticker_identifiers_frame(
        sec_company_tickers_df,
        portfolio_identifiers_df,
    )
    research_symbols_df = _load_research_price_symbols(research_con, data_root)
    existing_research_identifiers_df = _load_existing_research_identifier_map(research_con)
    research_identifiers_df = build_research_symbol_identifiers_frame(
        research_symbols_df,
        portfolio_identifiers_df,
        existing_research_identifiers_df,
    )
    mapped_identifier_frames = [
        frame
        for frame in [portfolio_identifiers_df, research_identifiers_df]
        if frame is not None and not frame.empty
    ]
    mapped_identifiers_df = (
        pd.concat(mapped_identifier_frames, ignore_index=True)
        if mapped_identifier_frames
        else pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    )
    manual_identifier_overrides_df = build_manual_security_identifier_overrides_frame(
        _load_security_identifier_overrides(),
        mapped_identifiers_df,
    )
    identifier_frames = [
        frame
        for frame in [
            portfolio_identifiers_df,
            sec_company_ticker_identifiers_df,
            sp500_identifiers_df,
            research_identifiers_df,
            manual_identifier_overrides_df,
        ]
        if frame is not None and not frame.empty
    ]
    if identifier_frames:
        identifiers_df = pd.concat(identifier_frames, ignore_index=True)
    else:
        identifiers_df = pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    _write_identifier_tables(portfolio_con, identifiers_df)
    _write_identifier_tables(research_con, identifiers_df)
    run_id, job_name, partition_key = _context_dq_metadata(context)
    log_security_identifier_mapping_checks(
        measured_con=research_con,
        observability_con=portfolio_con,
        run_id=run_id,
        job_name=job_name,
        partition_key=partition_key,
    )

    row_count = research_con.execute("SELECT count(*) FROM silver.security_identifiers").fetchone()[
        0
    ]
    asset_count = research_con.execute(
        "SELECT count(DISTINCT asset_id) FROM silver.security_identifiers"
    ).fetchone()[0]
    identity_bridge_count = research_con.execute(
        "SELECT count(*) FROM silver.asset_identity_bridge"
    ).fetchone()[0]
    symbol_bridge_count = research_con.execute(
        "SELECT count(*) FROM silver.asset_symbol_bridge"
    ).fetchone()[0]
    return {
        "row_count": int(row_count or 0),
        "asset_count": int(asset_count or 0),
        "portfolio_identifier_rows": len(portfolio_identifiers_df),
        "sec_company_ticker_identifier_rows": len(sec_company_ticker_identifiers_df),
        "sp500_identifier_rows": len(sp500_identifiers_df),
        "research_identifier_rows": len(research_identifiers_df),
        "manual_identifier_override_rows": len(manual_identifier_overrides_df),
        "identity_bridge_rows": int(identity_bridge_count or 0),
        "symbol_bridge_rows": int(symbol_bridge_count or 0),
    }


@asset(
    name="security_identifiers",
    key_prefix=["silver"],
    required_resource_keys={"duckdb", "research_duckdb"},
)
def silver_security_identifiers(context: AssetExecutionContext) -> None:
    metrics = materialize_security_identifier_tables(context)
    context.add_output_metadata(
        {
            "table": "silver.security_identifiers",
            **metrics,
        }
    )
