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
| `adjusted_close` | `float` | Split-adjusted daily close using known Alpaca split corporate actions. |
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

## `gold.fundamentals_quarterly`

*DuckDB Table in the research DuckDB gold schema*

One row per `asset_id`, `cik`, and quarterly reporting period after canonical SEC statement items have been pivoted into research-ready fields. The natural key is `asset_id`, `cik`, `fiscal_year`, `fiscal_quarter`, and `period_end_date`.

Lookahead rule: a quarterly row describes a historical reporting period, but downstream research must treat it as unavailable until its `availability_date`. Research may use the row only when the research date is on or after `availability_date`, which is derived from `DATE(acceptance_datetime)` when available and otherwise from `filing_date`.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key used for joins across portfolio and research DuckDB databases. |
| `symbol` | `object` | Canonical project ticker symbol. |
| `cik` | `object` | SEC Central Index Key without left-padding. |
| `fiscal_year` | `int` | Fiscal year reported by SEC. |
| `fiscal_quarter` | `object` | Fiscal quarter label, normally `Q1`, `Q2`, `Q3`, or `Q4`. |
| `period_start_date` | `date` | Reporting period start date when available. |
| `period_end_date` | `date` | Reporting period end date. |
| `filing_date` | `date` | SEC filing date for the source filing. |
| `acceptance_datetime` | `timestamp` | SEC acceptance datetime when available, stored in UTC. |
| `availability_date` | `date` | First date fundamentals may be used in research; derived from `acceptance_datetime` date when available, otherwise `filing_date`. |
| `accession_number` | `object` | Source SEC filing accession number. |
| `form` | `object` | Source filing form, typically `10-Q`, `10-K`, or an amendment. |
| `revenue` | `float` | Quarterly revenue. |
| `net_income` | `float` | Quarterly net income attributable to the registrant or parent when available, otherwise consolidated net income. |
| `assets` | `float` | Total assets at period end. |
| `liabilities` | `float` | Total liabilities at period end. |
| `equity` | `float` | Stockholders' equity attributable to the registrant at period end, excluding noncontrolling interests when available. |
| `debt` | `float` | Interest-bearing debt at period end; total liabilities must not be used as a fallback. |
| `cash` | `float` | Cash and cash equivalents at period end. |
| `operating_cash_flow` | `float` | Quarterly operating cash flow, positive for cash provided and negative for cash used. |
| `capex` | `float` | Quarterly capital expenditures stored as a positive cash outflow. |
| `diluted_shares` | `float` | Diluted weighted-average shares outstanding for the period, not point-in-time shares outstanding. |
| `diluted_eps` | `float` | Diluted earnings per share for the period. |
| `source_snapshot_date` | `date` | SEC bulk snapshot date represented by the source silver rows. |
| `statement_items_count` | `int` | Number of curated statement items used to populate the row. |
| `load_timestamp` | `timestamp` | Gold table load timestamp. |

## `gold.fundamental_signals_daily`

*DuckDB Table in the research DuckDB gold schema*

Point-in-time daily fundamental features joined to research trading dates and prices. A row must not expose a quarterly fundamental before `date >= availability_date`; rows before the first available filing for an asset should either be absent or have `has_fundamentals = false`.

Lookahead rule: every selected filing or derived fundamental feature must satisfy `date >= availability_date`. `period_end_date` is the reporting period being described and must not be treated as the date when the market could have known the value.

| Column | Type | Description |
| --- | --- | --- |
| `date` | `date` | Trading date for the signal row. |
| `asset_id` | `int` | Durable project asset key used for joins across portfolio and research DuckDB databases. |
| `symbol` | `object` | Canonical project ticker symbol. |
| `cik` | `object` | SEC Central Index Key without left-padding. |
| `fiscal_year` | `int` | Fiscal year of the latest available quarterly fundamentals as of `date`. |
| `fiscal_quarter` | `object` | Fiscal quarter of the latest available fundamentals as of `date`. |
| `period_end_date` | `date` | Reporting period end date of the latest available fundamentals. |
| `filing_date` | `date` | SEC filing date of the latest available source filing. |
| `acceptance_datetime` | `timestamp` | SEC acceptance datetime of the latest available source filing when available. |
| `availability_date` | `date` | First date the selected fundamentals were allowed into the signal set. |
| `days_since_filing` | `int` | Calendar days between `date` and `availability_date`. |
| `has_fundamentals` | `bool` | Whether point-in-time fundamentals are available for the asset on `date`. |
| `is_stale_fundamentals` | `bool` | Whether the latest available filing is older than the configured freshness threshold. |
| `close` | `float` | Daily close used for price-linked valuation features. |
| `market_cap` | `float` | `close * diluted_shares` when diluted shares are trustworthy. |
| `revenue_ttm` | `float` | Trailing four-quarter revenue as of `date`. |
| `net_income_ttm` | `float` | Trailing four-quarter net income as of `date`. |
| `operating_cash_flow_ttm` | `float` | Trailing four-quarter operating cash flow as of `date`. |
| `capex_ttm` | `float` | Trailing four-quarter capital expenditures as of `date`. |
| `free_cash_flow_ttm` | `float` | `operating_cash_flow_ttm - capex_ttm`. |
| `diluted_eps_ttm` | `float` | Trailing four-quarter diluted EPS as of `date`. |
| `revenue_growth_yoy` | `float` | Year-over-year quarterly revenue growth. |
| `net_income_growth_yoy` | `float` | Year-over-year quarterly net income growth. |
| `eps_growth_yoy` | `float` | Year-over-year quarterly diluted EPS growth. |
| `net_margin_ttm` | `float` | `net_income_ttm / revenue_ttm`. |
| `return_on_equity` | `float` | `net_income_ttm / equity` using latest available equity. |
| `debt_to_equity` | `float` | `debt / equity` using latest available balance sheet values. |
| `cash_to_assets` | `float` | `cash / assets` using latest available balance sheet values. |
| `price_to_sales` | `float` | `market_cap / revenue_ttm`. |
| `price_to_earnings` | `float` | `market_cap / net_income_ttm`. |
| `price_to_book` | `float` | `market_cap / equity`. |
| `free_cash_flow_yield` | `float` | `free_cash_flow_ttm / market_cap`. |
| `signal_version` | `object` | Fundamental signal definition version. |
| `load_timestamp` | `timestamp` | Gold table load timestamp. |

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
| `held_symbols_expected` | `int` | Number of held symbols expected to contribute returns for the date. |
| `held_symbols_with_returns` | `int` | Number of held symbols with available return observations for the date. |
| `missing_symbols` | `object` | Comma-separated held symbols missing return observations; null when coverage is complete. |
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
