import os
import re
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, AssetKey, asset

from portfolio_project.defs.research_db.silver.signals import silver_signals_daily

CLASSIFICATION_SOURCE = "research_dataset_heuristic_v1"
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))

SECURITY_MASTER_COLUMNS = [
    "asset_id",
    "symbol",
    "canonical_symbol",
    "security_name",
    "cik",
    "sec_ticker",
    "security_type",
    "security_subtype",
    "exchange",
    "identifier_source",
    "identifier_confidence",
    "identifier_source_snapshot_date",
    "classification_confidence",
    "classification_reason",
    "classification_source",
    "is_common_stock",
    "is_etf",
    "is_adr",
    "is_otc",
    "is_bankruptcy_related",
    "is_derivative_security",
    "is_fund_like",
    "is_investable_common_equity",
]

COMMON_STOCK_TERMS = (
    " common stock",
    " ordinary shares",
    " voting shares",
    " class a",
    " class b",
    " class c",
)
ETF_TERMS = (
    " etf",
    " exchange traded fund",
    " spdr ",
    " ishares ",
    " vanguard ",
    " invesco ",
    " proshares ",
    " direxion ",
    " wisdomtree ",
    " global x ",
    " first trust ",
    " ark ",
)
FUND_TERMS = (
    " fund",
    " trust",
    " etn",
    " closed-end",
    " closed end",
    " mutual fund",
    " index",
)
ADR_TERMS = (" adr", " american depositary", " american depository", " sponsored adr")
BANKRUPTCY_TERMS = (
    " bankrupt",
    " bankruptcy",
    " chapter 11",
    " chapter 7",
    " liquidation",
    " liquidating trust",
)
DERIVATIVE_TERMS = (
    " warrant",
    " right",
    " unit",
    " preferred",
    " preference",
    " note",
    " bond",
    " debenture",
    " option",
    " call",
    " put",
)
OTC_EXCHANGES = {"OTC", "OTCQB", "OTCQX", "OTCM", "OTCBB", "PINK", "GREY", "EXPERT"}
KNOWN_ETF_SYMBOLS = {
    "DIA",
    "EFA",
    "EEM",
    "GLD",
    "IWM",
    "QQQ",
    "SLV",
    "SPY",
    "TLT",
    "USO",
    "VEA",
    "VTI",
    "VWO",
    "XLE",
    "XLF",
    "XLK",
}


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _normalize_enum_like(value: object) -> str:
    text = _clean_text(value)
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.upper()


def _normalize_bool(value: object) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _canonical_symbol(symbol: object) -> str:
    text = _clean_text(symbol).upper()
    return re.sub(r"\s+", "", text).replace("/", ".")


def _first_text_value(*values: object) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _contains_any(haystack: str, needles: tuple[str, ...]) -> bool:
    padded = f" {haystack.lower()} "
    return any(needle in padded for needle in needles)


def _classify_security(row: pd.Series) -> dict[str, object]:
    symbol = _clean_text(row.get("symbol")).upper()
    canonical_symbol = _canonical_symbol(symbol)
    security_name = _first_text_value(row.get("name"), row.get("security_name"))
    exchange = _normalize_enum_like(row.get("exchange"))
    asset_class = _normalize_enum_like(row.get("asset_class"))
    attributes = _clean_text(row.get("attributes")).lower()
    source = _clean_text(row.get("source")).lower()
    tradable = _normalize_bool(row.get("tradable"))
    if "tradable" not in row.index and source in {
        "alpaca",
        "eodhd",
        "research",
        "research_daily_prices",
        "signals_daily",
    }:
        tradable = True

    name_lower = security_name.lower()
    symbol_suffix = canonical_symbol.rsplit(".", 1)[-1] if "." in canonical_symbol else ""

    is_otc = exchange in OTC_EXCHANGES or exchange.startswith("OTC")
    is_etf = (
        canonical_symbol in KNOWN_ETF_SYMBOLS
        or _contains_any(name_lower, ETF_TERMS)
        or "etf" in attributes
        or (exchange == "ARCA" and _contains_any(name_lower, FUND_TERMS))
    )
    is_adr = _contains_any(name_lower, ADR_TERMS) or (
        symbol_suffix in {"Y", "F"} and " adr" in name_lower
    )
    is_bankruptcy_related = _contains_any(name_lower, BANKRUPTCY_TERMS) or (
        canonical_symbol.endswith("Q") and not is_etf
    )
    is_derivative_security = (
        _contains_any(name_lower, DERIVATIVE_TERMS)
        or symbol_suffix in {"W", "WS", "WT", "R", "U", "P", "PR"}
        or canonical_symbol.endswith("W")
    )
    is_fund_like = is_etf or _contains_any(name_lower, FUND_TERMS)

    is_equity_asset = asset_class in {"US_EQUITY", "EQUITY", "COMMON_STOCK", ""}
    is_common_stock = (
        is_equity_asset
        and not is_etf
        and not is_derivative_security
        and not is_fund_like
        and bool(symbol)
    )
    if _contains_any(name_lower, COMMON_STOCK_TERMS):
        is_common_stock = not (is_etf or is_derivative_security or is_fund_like)

    if is_etf:
        security_type = "fund"
        security_subtype = "etf"
    elif is_derivative_security:
        security_type = "derivative"
        security_subtype = "warrant_right_unit_or_preferred"
    elif is_adr:
        security_type = "equity"
        security_subtype = "adr"
    elif is_common_stock:
        security_type = "equity"
        security_subtype = "common_stock"
    else:
        security_type = "unknown"
        security_subtype = "unknown"

    exclusion_reasons = []
    if not tradable:
        exclusion_reasons.append("not tradable in Alpaca metadata")
    if is_otc:
        exclusion_reasons.append("OTC exchange")
    if is_bankruptcy_related:
        exclusion_reasons.append("bankruptcy-related symbol/name")
    if is_derivative_security:
        exclusion_reasons.append("derivative-like symbol/name")
    if is_fund_like:
        exclusion_reasons.append("fund-like symbol/name")
    if is_adr:
        exclusion_reasons.append("ADR")

    is_investable_common_equity = bool(
        is_common_stock
        and tradable
        and not is_etf
        and not is_adr
        and not is_otc
        and not is_bankruptcy_related
        and not is_derivative_security
        and not is_fund_like
    )

    signals = [
        bool(asset_class),
        bool(exchange),
        bool(security_name),
        bool(security_type != "unknown"),
        bool(tradable),
    ]
    confidence = 0.45 + 0.1 * sum(signals)
    if source and not (asset_class or exchange or security_name):
        confidence = 0.6 if security_type != "unknown" else 0.35
    if security_type == "unknown":
        confidence -= 0.2
    if is_bankruptcy_related or is_derivative_security or is_etf:
        confidence += 0.05
    confidence = max(0.05, min(0.99, confidence))
    identifier_confidence = pd.to_numeric(
        pd.Series([row.get("identifier_confidence", pd.NA)]), errors="coerce"
    ).iloc[0]
    if not pd.isna(identifier_confidence) and float(identifier_confidence) > confidence:
        confidence = min(0.99, float(identifier_confidence))

    if is_investable_common_equity:
        reason = "Classified as tradable common equity from research dataset metadata."
    elif exclusion_reasons:
        reason = "Excluded from investable common equity: " + "; ".join(exclusion_reasons) + "."
    else:
        reason = "Insufficient metadata to classify as investable common equity."
    if _clean_text(row.get("identifier_source")):
        reason = f"{reason} Enriched with current SEC identifier mapping."

    return {
        "asset_id": row.get("asset_id", pd.NA),
        "symbol": symbol,
        "canonical_symbol": canonical_symbol,
        "security_name": security_name,
        "cik": row.get("cik", pd.NA),
        "sec_ticker": row.get("sec_ticker", pd.NA),
        "security_type": security_type,
        "security_subtype": security_subtype,
        "exchange": exchange,
        "identifier_source": row.get("identifier_source", pd.NA),
        "identifier_confidence": row.get("identifier_confidence", pd.NA),
        "identifier_source_snapshot_date": row.get("identifier_source_snapshot_date", pd.NaT),
        "classification_confidence": round(confidence, 2),
        "classification_reason": reason,
        "classification_source": row.get("classification_source_override", CLASSIFICATION_SOURCE),
        "is_common_stock": bool(is_common_stock),
        "is_etf": bool(is_etf),
        "is_adr": bool(is_adr),
        "is_otc": bool(is_otc),
        "is_bankruptcy_related": bool(is_bankruptcy_related),
        "is_derivative_security": bool(is_derivative_security),
        "is_fund_like": bool(is_fund_like),
        "is_investable_common_equity": is_investable_common_equity,
    }


def build_security_master_frame(assets_df: pd.DataFrame) -> pd.DataFrame:
    if assets_df is None or assets_df.empty:
        return pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)

    rows = [_classify_security(row) for _, row in assets_df.iterrows()]
    frame = pd.DataFrame(rows, columns=SECURITY_MASTER_COLUMNS)
    frame = frame.drop_duplicates(subset=["canonical_symbol"], keep="first")
    return frame.sort_values("canonical_symbol", kind="stable").reset_index(drop=True)


def _load_security_identifier_enrichment(con) -> pd.DataFrame:
    if not _table_exists(con, "silver", "security_identifiers"):
        return pd.DataFrame()

    columns = {
        str(row[0]) for row in con.execute("DESCRIBE silver.security_identifiers").fetchall()
    }
    source_snapshot_expr = (
        "source_snapshot_date" if "source_snapshot_date" in columns else "CAST(NULL AS DATE)"
    )
    return con.execute(
        f"""
        WITH sec_identifiers AS (
            SELECT
                asset_id,
                upper(trim(source_symbol)) AS symbol,
                security_name,
                cik,
                sec_ticker,
                exchange,
                identifier_source,
                mapping_confidence,
                {source_snapshot_expr} AS source_snapshot_date,
                source_priority
            FROM silver.security_identifiers
            WHERE asset_id IS NOT NULL
              AND source_symbol IS NOT NULL
              AND trim(source_symbol) <> ''
              AND is_current = true
              AND lower(trim(identifier_type)) IN ('cik', 'sec_ticker')
              AND lower(trim(identifier_source)) IN (
                  'sec_company_tickers',
                  'manual_security_identifier_overrides',
                  'sp500_wikipedia'
              )
              AND mapping_confidence >= 0.85
        )
        SELECT
            asset_id,
            symbol,
            security_name,
            cik,
            sec_ticker,
            exchange,
            identifier_source,
            mapping_confidence AS identifier_confidence,
            source_snapshot_date AS identifier_source_snapshot_date
        FROM sec_identifiers
        QUALIFY row_number() OVER (
            PARTITION BY symbol
            ORDER BY
                mapping_confidence DESC,
                source_priority ASC,
                source_snapshot_date DESC NULLS LAST,
                asset_id ASC
        ) = 1
        ORDER BY symbol
        """
    ).fetch_df()


def _enrich_candidates_with_sec_identifiers(
    candidates_df: pd.DataFrame,
    identifiers_df: pd.DataFrame,
) -> pd.DataFrame:
    if candidates_df is None or candidates_df.empty:
        return pd.DataFrame() if candidates_df is None else candidates_df
    if identifiers_df is None or identifiers_df.empty:
        return candidates_df

    candidates = candidates_df.copy()
    candidates["symbol"] = candidates["symbol"].astype("string").str.strip().str.upper()
    identifiers = identifiers_df.copy()
    identifiers["symbol"] = identifiers["symbol"].astype("string").str.strip().str.upper()
    enriched = candidates.merge(
        identifiers,
        on="symbol",
        how="left",
        suffixes=("", "_identifier"),
    )
    has_identifier = enriched["identifier_source"].notna()
    for target, source in [
        ("security_name", "security_name_identifier"),
        ("exchange", "exchange_identifier"),
    ]:
        if source in enriched.columns:
            source_has_value = enriched[source].notna() & enriched[source].astype("string").ne("")
            enriched.loc[has_identifier & source_has_value, target] = enriched.loc[
                has_identifier & source_has_value, source
            ]
            enriched = enriched.drop(columns=[source])
    enriched.loc[has_identifier, "classification_source_override"] = "sec_identifier_enriched_v1"
    return enriched


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


def _research_prices_glob() -> str:
    return (
        DATA_ROOT / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet"
    ).as_posix()


def _research_prices_files_exist() -> bool:
    prices_root = DATA_ROOT / "silver" / "research_daily_prices"
    return prices_root.exists() and any(prices_root.glob("month=*/date=*.parquet"))


def _load_research_security_candidates(con, context: AssetExecutionContext) -> pd.DataFrame:
    frames = []
    if _table_exists(con, "silver", "signals_daily"):
        frames.append(
            con.execute(
                """
                SELECT DISTINCT
                    upper(trim(symbol)) AS symbol,
                    'signals_daily' AS source
                FROM silver.signals_daily
                WHERE symbol IS NOT NULL
                  AND trim(symbol) <> ''
                """
            ).fetch_df()
        )

    if _research_prices_files_exist():
        frames.append(
            con.execute(
                """
                SELECT DISTINCT
                    upper(trim(symbol)) AS symbol,
                    'research_daily_prices' AS source
                FROM read_parquet(?)
                WHERE symbol IS NOT NULL
                  AND trim(symbol) <> ''
                """,
                [_research_prices_glob()],
            ).fetch_df()
        )

    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if frames:
        coverage_df = pd.concat(frames, ignore_index=True)
        coverage_df = coverage_df.dropna(subset=["symbol"])
        coverage_df["symbol"] = coverage_df["symbol"].astype(str).str.strip().str.upper()
        coverage_df["source"] = coverage_df["source"].astype(str)
        source_df = (
            coverage_df.groupby("symbol", as_index=False)["source"]
            .agg(lambda values: "+".join(sorted(set(values))))
            .sort_values("symbol", kind="stable")
        )
        source_df["canonical_symbol"] = source_df["symbol"]
        source_df["security_name"] = pd.NA
        source_df["asset_class"] = pd.NA
        source_df["exchange"] = pd.NA
        source_df["tradable"] = True
        return source_df[
            [
                "symbol",
                "canonical_symbol",
                "security_name",
                "asset_class",
                "exchange",
                "tradable",
                "source",
            ]
        ]

    context.log.warning(
        "No research symbols found; expected silver.signals_daily or "
        "data/silver/research_daily_prices parquet files."
    )
    return pd.DataFrame()


@asset(
    name="silver_security_master",
    deps=[silver_signals_daily, AssetKey(["silver", "security_identifiers"])],
    required_resource_keys={"research_duckdb"},
)
def silver_security_master(context: AssetExecutionContext) -> None:
    """
    Build a classified security master from symbols with research price or signal data.
    """
    con = context.resources.research_duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    assets_df = _load_research_security_candidates(con, context)
    identifier_enrichment_df = _load_security_identifier_enrichment(con)
    assets_df = _enrich_candidates_with_sec_identifiers(assets_df, identifier_enrichment_df)
    security_master_df = build_security_master_frame(assets_df)
    con.register("security_master_df", security_master_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.security_master AS
        SELECT
            asset_id::BIGINT AS asset_id,
            symbol::VARCHAR AS symbol,
            canonical_symbol::VARCHAR AS canonical_symbol,
            security_name::VARCHAR AS security_name,
            cik::VARCHAR AS cik,
            sec_ticker::VARCHAR AS sec_ticker,
            security_type::VARCHAR AS security_type,
            security_subtype::VARCHAR AS security_subtype,
            exchange::VARCHAR AS exchange,
            identifier_source::VARCHAR AS identifier_source,
            identifier_confidence::DOUBLE AS identifier_confidence,
            identifier_source_snapshot_date::DATE AS identifier_source_snapshot_date,
            classification_confidence::DOUBLE AS classification_confidence,
            classification_reason::VARCHAR AS classification_reason,
            classification_source::VARCHAR AS classification_source,
            is_common_stock::BOOLEAN AS is_common_stock,
            is_etf::BOOLEAN AS is_etf,
            is_adr::BOOLEAN AS is_adr,
            is_otc::BOOLEAN AS is_otc,
            is_bankruptcy_related::BOOLEAN AS is_bankruptcy_related,
            is_derivative_security::BOOLEAN AS is_derivative_security,
            is_fund_like::BOOLEAN AS is_fund_like,
            is_investable_common_equity::BOOLEAN AS is_investable_common_equity
        FROM security_master_df
        """
    )

    context.add_output_metadata(
        {
            "table": "silver.security_master",
            "row_count": len(security_master_df),
            "classification_source": CLASSIFICATION_SOURCE,
        }
    )
