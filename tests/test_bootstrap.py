import duckdb

from portfolio_project.bootstrap import (
    _ensure_silver_assets_table,
    _seed_fallback_gold_prices,
)


def test_bootstrap_fallback_creates_assets_and_gold_prices() -> None:
    con = duckdb.connect(":memory:")
    try:
        inserted_assets = _ensure_silver_assets_table(con)
        inserted_prices = _seed_fallback_gold_prices(con)

        assert inserted_assets > 0
        assert inserted_prices > 0

        assets_count = con.execute("SELECT count(*) FROM silver.assets").fetchone()[0]
        prices_count = con.execute("SELECT count(*) FROM gold.prices").fetchone()[0]
        latest_row = con.execute(
            """
            SELECT returns_1d, momentum_12_1, sma_50, sma_200
            FROM gold.prices
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()

        assert int(assets_count) >= inserted_assets
        assert int(prices_count) == inserted_prices
        assert latest_row is not None
        assert latest_row[0] is not None
        assert latest_row[1] is not None
        assert latest_row[2] is not None
        assert latest_row[3] is not None
    finally:
        con.close()


def test_bootstrap_fallback_is_idempotent_when_gold_prices_exist() -> None:
    con = duckdb.connect(":memory:")
    try:
        _ensure_silver_assets_table(con)
        first_insert_count = _seed_fallback_gold_prices(con)
        second_insert_count = _seed_fallback_gold_prices(con)
        prices_count = con.execute("SELECT count(*) FROM gold.prices").fetchone()[0]

        assert first_insert_count > 0
        assert second_insert_count == 0
        assert int(prices_count) == first_insert_count
    finally:
        con.close()
