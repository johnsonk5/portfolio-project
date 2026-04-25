from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from dagster import AssetKey, SourceAsset, build_asset_context, materialize

import portfolio_project.defs.portfolio_db.gold.news as gold_news_module
import portfolio_project.defs.portfolio_db.gold.prices as gold_prices_module
import portfolio_project.defs.portfolio_db.silver.prices as silver_prices_module
import portfolio_project.defs.research_db.gold.strategy as research_gold_strategy_module
import portfolio_project.defs.research_db.silver.signals as research_signals_module
import portfolio_project.defs.research_db.silver.strategy as research_strategy_module
import portfolio_project.defs.research_db.silver.universe as research_universe_module
from portfolio_project.defs.portfolio_db.gold.news import gold_headlines
from portfolio_project.defs.portfolio_db.gold.prices import gold_alpaca_prices
from portfolio_project.defs.portfolio_db.silver.prices import silver_alpaca_prices_parquet
from portfolio_project.defs.research_db.gold.strategy import (
    gold_strategy_holdings,
    gold_strategy_performance,
    gold_strategy_rankings,
    gold_strategy_returns,
)
from portfolio_project.defs.research_db.silver.signals import silver_signals_daily
from portfolio_project.defs.research_db.silver.strategy import (
    silver_strategy_definitions,
    silver_strategy_parameters,
    silver_strategy_runs,
)
from portfolio_project.defs.research_db.silver.universe import (
    silver_universe_membership_daily,
    silver_universe_membership_events,
)

RESEARCH_SMOKE_STRATEGY_YAML = """
strategies:
  - strategy_id: benchmark_spy_buy_and_hold
    strategy_name: Buy And Hold SPY
    strategy_version: smoke
    description: Benchmark strategy.
    ranking_method: single_asset_hold
    rebalance_frequency: Monthly
    target_count: 1
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2024-01-01
    end_date: 2024-02-29
    is_active: true
    config:
      universe: benchmark_only
      selection_mode: fixed_symbol
    parameters:
      - parameter_name: symbol
        parameter_value: SPY
        parameter_type: string
        effective_start_date: 2024-01-01
        effective_end_date:
        is_active: true
        description: Benchmark symbol.
  - strategy_id: momentum_top_1
    strategy_name: Momentum Top 1
    strategy_version: smoke
    description: Momentum strategy.
    ranking_method: momentum_12_1_desc
    rebalance_frequency: Monthly
    target_count: 1
    weighting_method: equal
    benchmark_symbol: SPY
    long_short_flag: false
    start_date: 2024-01-01
    end_date: 2024-02-29
    is_active: true
    config:
      universe: universe_membership_daily
      selection_mode: top_n
    parameters:
      - parameter_name: signal_column
        parameter_value: momentum_12_1
        parameter_type: string
        effective_start_date: 2024-01-01
        effective_end_date:
        is_active: true
        description: Signal column.
      - parameter_name: ranking_direction
        parameter_value: desc
        parameter_type: string
        effective_start_date: 2024-01-01
        effective_end_date:
        is_active: true
        description: Ranking direction.
"""


def _write_fixture_bronze_bars(data_root: Path, partition_key: str, symbols: list[str]) -> None:
    for symbol in symbols:
        bronze_dir = (
            data_root / "bronze" / "alpaca_bars" / f"date={partition_key}" / f"symbol={symbol}"
        )
        bronze_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(
            {
                "symbol": [symbol, symbol],
                "timestamp": [
                    datetime(2026, 2, 13, 14, 35, tzinfo=timezone.utc),
                    datetime(2026, 2, 13, 14, 40, tzinfo=timezone.utc),
                ],
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.5, 100.5],
                "close": [100.8, 101.6],
                "volume": [1000, 1200],
                "trade_count": [12, 15],
                "vwap": [100.6, 101.4],
                "ingested_ts": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            }
        )
        df.to_parquet(bronze_dir / "bars.parquet", index=False)


def _write_fixture_silver_prices(
    data_root: Path,
    partition_key: str,
    symbol: str,
    frame: pd.DataFrame,
) -> None:
    out_dir = data_root / "silver" / "prices" / f"date={partition_key}" / f"symbol={symbol}"
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_dir / "prices0.parquet", index=False)


def _write_research_price_partitions(data_root: Path) -> None:
    dates = pd.bdate_range("2023-01-03", "2024-03-01")
    rows = []
    for idx, ts in enumerate(dates):
        trade_date = ts.date().isoformat()
        prices = {
            "SPY": 100.0 + (idx * 0.04),
            "AAA": 45.0 + (idx * 0.18),
            "BBB": 90.0 + (idx * 0.02),
        }
        volumes = {
            "SPY": 1_000_000,
            "AAA": 800_000,
            "BBB": 700_000,
        }
        for symbol, close in prices.items():
            volume = volumes[symbol]
            rows.append(
                {
                    "symbol": symbol,
                    "timestamp": ts.tz_localize(timezone.utc),
                    "trade_date": trade_date,
                    "open": close - 0.25,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "adjusted_close": close,
                    "volume": volume,
                    "trade_count": 100,
                    "vwap": close,
                    "dollar_volume": close * volume,
                    "source": "smoke",
                    "ingested_ts": datetime.now(timezone.utc),
                }
            )

    prices_df = pd.DataFrame(rows)
    for trade_date, frame in prices_df.groupby("trade_date", sort=True):
        out_path = (
            data_root
            / "silver"
            / "research_daily_prices"
            / f"month={str(trade_date)[:7]}"
            / f"date={trade_date}.parquet"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(out_path, index=False)


@pytest.mark.smoke
def test_daily_prices_path_materializes_with_three_tickers(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    symbols = ["AAPL", "MSFT", "NVDA"]

    data_root = tmp_path / "data"
    _write_fixture_bronze_bars(data_root, partition_key, symbols)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    for idx, symbol in enumerate(symbols, start=1):
        con.execute(
            "INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (?, ?, TRUE)",
            [idx, symbol],
        )

    silver_prices_module.DATA_ROOT = data_root
    gold_prices_module.DATA_ROOT = data_root

    result = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            gold_alpaca_prices,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )

    assert result.success
    silver_glob = (
        data_root / "silver" / "prices" / f"date={partition_key}" / "symbol=*" / "prices0.parquet"
    ).as_posix()
    silver_row = con.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [silver_glob],
    ).fetchone()
    gold_row = con.execute(
        "SELECT count(*) FROM gold.prices WHERE trade_date = ?",
        [partition_key],
    ).fetchone()
    assert silver_row is not None
    assert gold_row is not None
    silver_rows = silver_row[0]
    gold_rows = gold_row[0]
    assert silver_rows == len(symbols) * 2
    assert gold_rows == len(symbols)


@pytest.mark.smoke
def test_silver_prices_rerun_replaces_partition_and_removes_stale_symbol_files(
    tmp_path: Path,
) -> None:
    partition_key = "2026-02-13"
    first_symbols = ["AAPL", "MSFT", "NVDA"]
    second_symbols = ["AAPL", "MSFT"]

    data_root = tmp_path / "data"
    _write_fixture_bronze_bars(data_root, partition_key, first_symbols)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    for idx, symbol in enumerate(first_symbols, start=1):
        con.execute(
            "INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (?, ?, TRUE)",
            [idx, symbol],
        )

    silver_prices_module.DATA_ROOT = data_root

    result_first = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )
    assert result_first.success

    con.execute("UPDATE silver.assets SET is_active = FALSE WHERE symbol = 'NVDA'")
    _write_fixture_bronze_bars(data_root, partition_key, second_symbols)

    result_second = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )
    assert result_second.success

    day_dir = data_root / "silver" / "prices" / f"date={partition_key}"
    written_symbols = sorted(path.name.split("=", 1)[1] for path in day_dir.glob("symbol=*"))
    assert written_symbols == sorted(second_symbols)
    assert not (day_dir / "symbol=NVDA" / "prices0.parquet").exists()


@pytest.mark.smoke
def test_silver_prices_dedupes_duplicate_active_symbols_in_assets(
    tmp_path: Path,
) -> None:
    partition_key = "2026-02-13"
    symbol = "AAPL"

    data_root = tmp_path / "data"
    _write_fixture_bronze_bars(data_root, partition_key, [symbol])

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)")
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (99, 'AAPL', TRUE)")

    silver_prices_module.DATA_ROOT = data_root

    result = materialize(
        assets=[
            silver_alpaca_prices_parquet,
            SourceAsset(AssetKey("bronze_alpaca_bars")),
            SourceAsset(AssetKey("silver_alpaca_assets")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )

    assert result.success
    out_path = (
        data_root
        / "silver"
        / "prices"
        / f"date={partition_key}"
        / "symbol=AAPL"
        / "prices0.parquet"
    )
    df = pd.read_parquet(out_path)
    assert len(df) == 2
    assert set(df["asset_id"].tolist()) == {1}


@pytest.mark.smoke
def test_gold_prices_computes_vwap_returns_and_sentiment(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    data_root = tmp_path / "data"
    gold_prices_module.DATA_ROOT = data_root

    silver_df = pd.DataFrame(
        {
            "asset_id": [1, 1, 1],
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "timestamp": [
                datetime(2026, 2, 13, 14, 31, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 14, 35, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 14, 40, tzinfo=timezone.utc),
            ],
            "open": [100.0, 101.0, 101.5],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [101.0, 101.5, 102.0],
            "volume": [10, 70, 20],
            "trade_count": [5, 20, 8],
            "vwap": [100.0, None, 102.0],
            "ingested_ts": [
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            ],
        }
    )
    _write_fixture_silver_prices(data_root, partition_key, "AAPL", silver_df)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)")
    con.execute(
        """
        CREATE TABLE silver.ref_publishers (
            publisher_id BIGINT,
            publisher_weight DOUBLE
        )
        """
    )
    con.execute("INSERT INTO silver.ref_publishers VALUES (1, 2.0), (2, 1.0)")

    gold_prices_module._ensure_gold_table(con)
    con.execute(
        """
        INSERT INTO gold.prices (
            asset_id, symbol, trade_date, open, high, low, close, volume,
            trade_count, vwap, dollar_volume, returns_1d, returns_5d, returns_21d,
            realized_vol_21d, momentum_12_1, pct_below_52w_high, sma_50, sma_200,
            dist_sma_50, dist_sma_200, sentiment_score
        )
        VALUES (
            1, 'AAPL', '2026-02-12', 99, 101, 98, 100, 1000, 10, 100, 100000,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
        """
    )

    con.execute(
        """
        CREATE TABLE gold.headlines (
            asset_id BIGINT,
            publisher_id BIGINT,
            provider_publish_time TIMESTAMP,
            sentiment VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.headlines VALUES
            (1, 1, '2026-02-12 15:00:00', 'positive'),
            (1, 2, '2026-02-11 15:00:00', 'negative')
        """
    )

    result = materialize(
        assets=[
            gold_alpaca_prices,
            SourceAsset(AssetKey("silver_alpaca_assets")),
            SourceAsset(AssetKey("silver_alpaca_prices_parquet")),
        ],
        resources={"duckdb": con},
        partition_key=partition_key,
    )
    assert result.success

    row = con.execute(
        """
        SELECT close, vwap, returns_1d, sentiment_score
        FROM gold.prices
        WHERE asset_id = 1 AND trade_date = ?
        """,
        [partition_key],
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(102.0)
    assert row[1] == pytest.approx((100.0 * 10 + 102.0 * 20) / 30.0)
    assert row[2] == pytest.approx(0.02)
    assert row[3] == pytest.approx(1.0 / 3.0)


@pytest.mark.smoke
def test_gold_headlines_rerun_upserts_window_without_rebuilding_sentiment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    partition_key = "2026-02-13"
    data_root = tmp_path / "data"
    gold_news_module.DATA_ROOT = data_root

    news_dir = data_root / "silver" / "news" / f"date={partition_key}"
    news_dir.mkdir(parents=True, exist_ok=True)
    news_df = pd.DataFrame(
        {
            "asset_id": [1, 2],
            "symbol": ["AAPL", "MSFT"],
            "uuid": ["u1", "u2"],
            "title": ["AAPL title", "MSFT title"],
            "publisher_id": [10, 20],
            "link": ["https://example.com/aapl", "https://example.com/msft"],
            "provider_publish_time": [
                datetime(2026, 2, 13, 15, 0, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 16, 0, tzinfo=timezone.utc),
            ],
            "type": ["STORY", "STORY"],
            "summary": ["AAPL summary", "MSFT summary"],
            "query_date": [date(2026, 2, 13), date(2026, 2, 13)],
            "ingested_ts": [
                datetime(2026, 2, 13, 18, 0, tzinfo=timezone.utc),
                datetime(2026, 2, 13, 18, 5, tzinfo=timezone.utc),
            ],
        }
    )
    news_df.to_parquet(news_dir / "news.parquet", index=False)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA gold")
    con.execute(
        """
        CREATE TABLE gold.headlines (
            asset_id BIGINT,
            symbol VARCHAR,
            uuid VARCHAR,
            title VARCHAR,
            publisher_id BIGINT,
            link VARCHAR,
            provider_publish_time TIMESTAMP,
            type VARCHAR,
            summary VARCHAR,
            query_date DATE,
            ingested_ts TIMESTAMP,
            sentiment VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO gold.headlines VALUES
            (
                1,
                'AAPL',
                'u1',
                'Old AAPL title',
                10,
                'https://example.com/aapl',
                '2026-02-13 15:00:00+00:00',
                'STORY',
                'Old summary',
                '2026-02-13',
                '2026-02-13 17:00:00+00:00',
                'positive'
            ),
            (
                3,
                'NVDA',
                'u3',
                'NVDA stale',
                30,
                'https://example.com/nvda',
                '2026-02-12 15:00:00+00:00',
                'STORY',
                'NVDA summary',
                '2026-02-12',
                '2026-02-12 17:00:00+00:00',
                'negative'
            )
        """
    )

    def fake_sentiment_pipeline(texts, batch_size):
        return [{"label": "neutral"} for _ in texts]

    monkeypatch.setattr(
        gold_news_module, "_get_sentiment_pipeline", lambda: fake_sentiment_pipeline
    )

    context = build_asset_context(resources={"duckdb": con}, partition_key=partition_key)
    gold_headlines(context)

    rows = con.execute(
        "SELECT symbol, uuid, title, sentiment FROM gold.headlines ORDER BY symbol"
    ).fetchall()
    assert rows == [
        ("AAPL", "u1", "AAPL title", "positive"),
        ("MSFT", "u2", "MSFT title", "neutral"),
    ]


@pytest.mark.smoke
def test_gold_prices_upsert_rolls_back_on_insert_error(tmp_path: Path) -> None:
    partition_key = "2026-02-13"
    partition_date = date(2026, 2, 13)
    data_root = tmp_path / "data"
    gold_prices_module.DATA_ROOT = data_root

    broken_silver_df = pd.DataFrame(
        {
            "asset_id": [1],
            "symbol": ["AAPL"],
            "timestamp": [datetime(2026, 2, 13, 14, 31, tzinfo=timezone.utc)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10],
            "ingested_ts": [datetime.now(timezone.utc)],
        }
    )
    _write_fixture_silver_prices(data_root, partition_key, "AAPL", broken_silver_df)

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.assets (
            asset_id BIGINT,
            symbol VARCHAR,
            is_active BOOLEAN
        )
        """
    )
    con.execute("INSERT INTO silver.assets (asset_id, symbol, is_active) VALUES (1, 'AAPL', TRUE)")

    gold_prices_module._ensure_gold_table(con)
    con.execute(
        """
        INSERT INTO gold.prices (
            asset_id, symbol, trade_date, open, high, low, close, volume,
            trade_count, vwap, dollar_volume, returns_1d, returns_5d, returns_21d,
            realized_vol_21d, momentum_12_1, pct_below_52w_high, sma_50, sma_200,
            dist_sma_50, dist_sma_200, sentiment_score
        )
        VALUES (
            1, 'AAPL', ?, 99, 101, 98, 200, 1000, 10, 100, 200000,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
        """,
        [partition_key],
    )

    context = build_asset_context(resources={"duckdb": con})

    with pytest.raises(Exception):
        gold_prices_module._upsert_gold_for_day(context, partition_date)

    rows_after_row = con.execute(
        "SELECT count(*) FROM gold.prices WHERE trade_date = ?",
        [partition_key],
    ).fetchone()
    close_after_row = con.execute(
        "SELECT close FROM gold.prices WHERE trade_date = ? AND asset_id = 1",
        [partition_key],
    ).fetchone()
    assert rows_after_row is not None
    assert close_after_row is not None
    rows_after = rows_after_row[0]
    close_after = close_after_row[0]
    assert rows_after == 1
    assert close_after == pytest.approx(200.0)


@pytest.mark.smoke
def test_research_workflow_materializes_strategy_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_root = tmp_path / "data"
    catalog_path = tmp_path / "investment_strategies.yaml"
    catalog_path.write_text(RESEARCH_SMOKE_STRATEGY_YAML, encoding="utf-8")
    _write_research_price_partitions(data_root)

    monkeypatch.setattr(research_signals_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(research_signals_module, "SIGNALS_SYMBOL_BUCKETS", 1)
    monkeypatch.setattr(research_universe_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(research_universe_module, "LIQUIDITY_LOOKBACK_DAYS", 2)
    monkeypatch.setattr(research_universe_module, "UNIVERSE_SIZE", 3)
    monkeypatch.setattr(research_strategy_module, "STRATEGY_CATALOG_PATH", catalog_path)
    monkeypatch.setattr(research_gold_strategy_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(
        research_gold_strategy_module,
        "PRICE_GLOB",
        (data_root / "silver" / "research_daily_prices" / "month=*" / "date=*.parquet").as_posix(),
    )

    research_con = duckdb.connect(":memory:")
    observability_con = duckdb.connect(":memory:")

    result = materialize(
        assets=[
            SourceAsset(AssetKey(["silver", "research_daily_prices"])),
            silver_signals_daily,
            silver_universe_membership_daily,
            silver_universe_membership_events,
            silver_strategy_definitions,
            silver_strategy_parameters,
            silver_strategy_runs,
            gold_strategy_rankings,
            gold_strategy_holdings,
            gold_strategy_returns,
            gold_strategy_performance,
        ],
        resources={
            "research_duckdb": research_con,
            "duckdb": observability_con,
        },
    )

    assert result.success

    output_counts = research_con.execute(
        """
        SELECT
            (SELECT count(*) FROM silver.signals_daily),
            (SELECT count(*) FROM silver.universe_membership_daily),
            (SELECT count(*) FROM silver.universe_membership_events),
            (SELECT count(*) FROM gold.strategy_rankings),
            (SELECT count(*) FROM gold.strategy_holdings),
            (SELECT count(*) FROM gold.strategy_returns),
            (SELECT count(*) FROM gold.strategy_performance)
        """
    ).fetchone()
    assert output_counts is not None
    assert all(count > 0 for count in output_counts)

    strategy_rows = research_con.execute(
        """
        SELECT strategy_id, run_status, rankings_row_count, holdings_row_count,
               returns_row_count, performance_row_count
        FROM silver.strategy_runs
        WHERE strategy_id IN ('benchmark_spy_buy_and_hold', 'momentum_top_1')
        ORDER BY strategy_id
        """
    ).fetchall()
    assert len(strategy_rows) == 2
    by_strategy = {row[0]: row[1:] for row in strategy_rows}
    assert by_strategy["benchmark_spy_buy_and_hold"][:3] == ("success", 2, 2)
    assert by_strategy["benchmark_spy_buy_and_hold"][3] > 0
    assert by_strategy["benchmark_spy_buy_and_hold"][4] == 1
    assert by_strategy["momentum_top_1"][:3] == ("success", 6, 2)
    assert by_strategy["momentum_top_1"][3] > 0
    assert by_strategy["momentum_top_1"][4] == 1

    selected_holdings = research_con.execute(
        """
        SELECT strategy_id, rebalance_date, symbol, target_weight
        FROM gold.strategy_holdings
        ORDER BY strategy_id, rebalance_date, symbol
        """
    ).fetchall()
    assert selected_holdings == [
        ("benchmark_spy_buy_and_hold", date(2024, 1, 31), "SPY", 1.0),
        ("benchmark_spy_buy_and_hold", date(2024, 2, 29), "SPY", 1.0),
        ("momentum_top_1", date(2024, 1, 31), "AAA", 1.0),
        ("momentum_top_1", date(2024, 2, 29), "AAA", 1.0),
    ]

    dq_rows = observability_con.execute(
        """
        SELECT check_name, status
        FROM observability.data_quality_checks
        WHERE check_name IN (
            'dq_research_signals_daily_required_fields_nulls',
            'dq_research_universe_membership_daily_required_fields_nulls',
            'dq_research_universe_membership_events_required_fields_nulls',
            'dq_gold_strategy_holdings_weight_sum_by_rebalance',
            'dq_gold_strategy_returns_benchmark_series_present',
            'dq_gold_strategy_returns_expected_return_dates'
        )
        ORDER BY check_name
        """
    ).fetchall()
    assert dq_rows == [
        ("dq_gold_strategy_holdings_weight_sum_by_rebalance", "PASS"),
        ("dq_gold_strategy_returns_benchmark_series_present", "PASS"),
        ("dq_gold_strategy_returns_expected_return_dates", "PASS"),
        ("dq_research_signals_daily_required_fields_nulls", "PASS"),
        ("dq_research_universe_membership_daily_required_fields_nulls", "PASS"),
        ("dq_research_universe_membership_events_required_fields_nulls", "PASS"),
    ]
