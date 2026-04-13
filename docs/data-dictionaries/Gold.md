# Gold Data Dictionary

## `gold.prices`

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id`. |
| `symbol` | `object` | Ticker symbol. |
| `trade_date` | `date` | Trading date (UTC). |
| `open` | `float` | Daily open price. |
| `high` | `float` | Daily high price. |
| `low` | `float` | Daily low price. |
| `close` | `float` | Daily close price. |
| `volume` | `int` | Daily volume. |
| `trade_count` | `int` | Daily trade count (if provided). |
| `vwap` | `float` | Daily volume-weighted average price (if provided). |
| `dollar_volume` | `float` | `close * volume`. |
| `returns_1d` | `float` | 1-day return. |
| `returns_5d` | `float` | 5-day return. |
| `returns_21d` | `float` | 21-day return. |
| `realized_vol_21d` | `float` | 21-day annualized realized volatility. |
| `momentum_12_1` | `float` | 12-1 momentum (`t-21` over `t-252`). |
| `pct_below_52w_high` | `float` | Percent below 52-week high. |
| `sma_50` | `float` | 50-day simple moving average of close. |
| `sma_200` | `float` | 200-day simple moving average of close. |
| `dist_sma_50` | `float` | `(close / sma_50) - 1`. |
| `dist_sma_200` | `float` | `(close / sma_200) - 1`. |
| `sentiment_score` | `float` | Weighted 7-day headline sentiment score. |

## `gold.headlines`

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id` (nullable). |
| `symbol` | `object` | Ticker symbol. |
| `uuid` | `object` | News item identifier. |
| `title` | `object` | Headline text. |
| `publisher_id` | `int` | Foreign key to `silver.ref_publishers.publisher_id` (nullable). |
| `link` | `object` | Link to news item. |
| `provider_publish_time` | `datetime64[us]` | Publish timestamp (UTC). |
| `type` | `object` | News item type. |
| `summary` | `object` | Summary text. |
| `query_date` | `date` | Query partition date. |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |
| `sentiment` | `object` | FinBERT sentiment label (`positive`, `negative`, `neutral`). |

## `gold.activity`

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id`. |
| `activity_date` | `date` | Activity date (UTC). |
| `source` | `object` | Activity source (for example, `wikipedia`). |
| `event_type` | `object` | Activity event type (for example, `pageview`). |
| `views` | `int` | Raw pageview count. |
| `views_30d_avg` | `float` | 30-day rolling average views. |
| `views_vs_30d_avg` | `float` | `views / views_30d_avg`. |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |

## `gold.strategy_rankings`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `object` | Strategy run identifier. |
| `strategy_id` | `object` | Strategy identifier. |
| `rebalance_date` | `date` | Rebalance date. |
| `symbol` | `object` | Ticker symbol. |
| `score` | `float` | Strategy ranking score at rebalance. |
| `rank` | `int` | Rank position within each rebalance date. |
| `selected_flag` | `bool` | Whether the symbol was selected into holdings. |
| `asof_ts` | `datetime64[us]` | Load timestamp. |

## `gold.strategy_holdings`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `object` | Strategy run identifier. |
| `strategy_id` | `object` | Strategy identifier. |
| `rebalance_date` | `date` | Rebalance date. |
| `symbol` | `object` | Ticker symbol. |
| `target_weight` | `float` | Assigned portfolio weight for the rebalance period. |
| `side` | `object` | Portfolio side. |
| `entry_rank` | `int` | Rank used for inclusion. |
| `signal_value` | `float` | Ranking signal value used for the holding. |
| `asof_ts` | `datetime64[us]` | Load timestamp. |

## `gold.strategy_returns`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `object` | Strategy run identifier. |
| `strategy_id` | `object` | Strategy identifier. |
| `date` | `date` | Trading date. |
| `portfolio_return` | `float` | Strategy daily return. |
| `benchmark_return` | `float` | Benchmark daily return. |
| `excess_return` | `float` | Active return versus benchmark. |
| `cumulative_return` | `float` | Compounded cumulative return path. |
| `drawdown` | `float` | Running drawdown from the cumulative peak. |
| `turnover` | `float` | Daily turnover, non-zero on rebalance effective dates. |
| `holdings_count` | `int` | Active holdings count for the date. |
| `asof_ts` | `datetime64[us]` | Load timestamp. |

## `gold.strategy_performance`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `object` | Strategy run identifier. |
| `strategy_id` | `object` | Strategy identifier. |
| `cagr` | `float` | Annualized return. |
| `sharpe_ratio` | `float` | Annualized Sharpe ratio. |
| `sortino_ratio` | `float` | Annualized Sortino ratio. |
| `max_drawdown` | `float` | Maximum drawdown over the run. |
| `annualized_volatility` | `float` | Annualized volatility. |
| `hit_rate` | `float` | Share of positive daily returns. |
| `turnover_avg` | `float` | Average daily turnover. |
| `benchmark_return` | `float` | Benchmark total return over the run. |
| `alpha` | `float` | Annualized excess return estimate versus benchmark. |
| `asof_ts` | `datetime64[us]` | Load timestamp. |
