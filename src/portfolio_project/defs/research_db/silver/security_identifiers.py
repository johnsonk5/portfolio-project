import os
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset

DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))

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
    "ingestion_date",
    "ingested_ts",
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


def _research_prices_glob() -> str:
    return (
        DATA_ROOT / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet"
    ).as_posix()


def _research_prices_files_exist() -> bool:
    prices_root = DATA_ROOT / "silver" / "research_daily_prices"
    return prices_root.exists() and any(prices_root.glob("month=*/date=*.parquet"))


def _load_research_price_symbols(con) -> pd.DataFrame:
    if not _research_prices_files_exist():
        return pd.DataFrame(columns=["source_symbol", "first_trade_date", "last_trade_date"])
    return con.execute(
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
        [_research_prices_glob()],
    ).fetch_df()


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
    existing_df["asset_id"] = pd.to_numeric(
        existing_df["asset_id"], errors="coerce"
    ).astype("Int64")
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
                "ingestion_date": pd.Timestamp.utcnow().date(),
                "ingested_ts": pd.Timestamp.utcnow(),
            }
        )

    if not rows:
        return pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)
    return pd.DataFrame(rows, columns=SECURITY_IDENTIFIERS_COLUMNS)


@asset(
    name="security_identifiers",
    key_prefix=["silver"],
    required_resource_keys={"duckdb", "research_duckdb"},
)
def silver_security_identifiers(context: AssetExecutionContext) -> None:
    """
    Mirror durable project identifiers from portfolio silver assets into research DuckDB.
    """
    portfolio_con = context.resources.duckdb
    research_con = context.resources.research_duckdb
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
    research_symbols_df = _load_research_price_symbols(research_con)
    existing_research_identifiers_df = _load_existing_research_identifier_map(research_con)
    research_identifiers_df = build_research_symbol_identifiers_frame(
        research_symbols_df,
        portfolio_identifiers_df,
        existing_research_identifiers_df,
    )
    identifier_frames = [
        frame
        for frame in [portfolio_identifiers_df, research_identifiers_df]
        if frame is not None and not frame.empty
    ]
    if identifier_frames:
        identifiers_df = pd.concat(identifier_frames, ignore_index=True)
    else:
        identifiers_df = pd.DataFrame(columns=SECURITY_IDENTIFIERS_COLUMNS)

    research_con.register("security_identifiers_df", identifiers_df)
    research_con.execute(
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
            ingestion_date::DATE AS ingestion_date,
            ingested_ts::TIMESTAMP AS ingested_ts
        FROM security_identifiers_df
        """
    )

    row_count = research_con.execute(
        "SELECT count(*) FROM silver.security_identifiers"
    ).fetchone()[0]
    asset_count = research_con.execute(
        "SELECT count(DISTINCT asset_id) FROM silver.security_identifiers"
    ).fetchone()[0]
    context.add_output_metadata(
        {
            "table": "silver.security_identifiers",
            "row_count": int(row_count or 0),
            "asset_count": int(asset_count or 0),
            "portfolio_identifier_rows": len(portfolio_identifiers_df),
            "research_identifier_rows": len(research_identifiers_df),
        }
    )
