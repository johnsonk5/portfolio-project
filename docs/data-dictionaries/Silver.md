# Silver Data Dictionary

In order to best manage pipeline speed and query runtime, some of these tables remained in partitioned parquet files and others were brought into DuckDB.

## `silver.assets`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Surrogate identity key. |
| `alpaca_id` | `object` | Original Alpaca asset id. |
| `asset_class` | `object` | Asset class category. |
| `exchange` | `object` | Exchange where listed. |
| `symbol` | `object` | Ticker symbol. |
| `name` | `object` | Full name. |
| `alpaca_status` | `object` | Source asset status. |
| `tradable` | `bool` | Tradable flag. |
| `marginable` | `bool` | Marginable flag. |
| `shortable` | `bool` | Shortable flag. |
| `easy_to_borrow` | `bool` | Easy-to-borrow flag. |
| `fractionable` | `bool` | Fractional-trading support flag. |
| `min_order_size` | `object` | Minimum order size. |
| `min_trade_increment` | `object` | Minimum trade increment. |
| `price_increment` | `object` | Minimum price increment. |
| `maintenance_margin_requirement` | `float64` | Maintenance margin requirement. |
| `attributes` | `object` | Additional source attributes. |
| `ingested_ts` | `datetime64` | Ingest timestamp. |
| `is_active` | `bool` | Active universe flag. |
| `is_sp500` | `bool` | S&P 500 membership flag. |
| `wikipedia_title` | `object` | Resolved English Wikipedia title (nullable). |

## `silver.prices`

*Parquet file*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id`. |
| `symbol` | `object` | Ticker symbol. |
| `timestamp` | `timestamp` | Bar timestamp (UTC). |
| `open` | `float` | Open price. |
| `high` | `float` | High price. |
| `low` | `float` | Low price. |
| `close` | `float` | Close price. |
| `volume` | `int` | Volume traded. |
| `trade_count` | `int` | Number of trades (if provided). |
| `vwap` | `float` | Volume-weighted average price (if provided). |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.active_assets_history`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id`. |
| `alpaca_id` | `object` | Original Alpaca asset id. |
| `symbol` | `object` | Ticker symbol. |
| `change_type` | `object` | Change classification (`activated`, `deactivated`, `snapshot`). |
| `change_date` | `date` | Date of change (UTC). |
| `change_ts` | `timestamp` | Timestamp of change (UTC). |
| `previous_is_active` | `bool` | Previous active flag. |
| `new_is_active` | `bool` | New active flag. |

## `silver.research_daily_prices`

*Partitioned Parquet file*

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | `object` | Canonical ticker symbol. |
| `timestamp` | `timestamp` | Daily bar timestamp (UTC). |
| `trade_date` | `date` | Trading date represented by the bar. |
| `open` | `float` | Open price for the day. |
| `high` | `float` | High price for the day. |
| `low` | `float` | Low price for the day. |
| `close` | `float` | Close price for the day. |
| `adjusted_close` | `float` | Split-adjusted close for Alpaca-backed rows when split corporate actions are available, or source-provided adjusted close when available. |
| `volume` | `int` | Daily share volume. |
| `trade_count` | `int` | Daily trade count when provided. |
| `vwap` | `float` | Daily VWAP when provided. |
| `dollar_volume` | `float` | `close * volume`. |
| `source` | `object` | Winning upstream provider for the day (`alpaca` preferred over `eodhd`). |
| `ingested_ts` | `timestamp` | Upstream ingest timestamp carried into silver. |

## `silver.alpaca_corporate_actions`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `action_id` | `object` | Alpaca corporate action identifier when provided. |
| `symbol` | `object` | Canonical ticker symbol. |
| `action_type` | `object` | Corporate action type (`forward_splits`, `reverse_splits`). |
| `effective_date` | `date` | Effective or ex-date used for split adjustment. |
| `process_date` | `date` | Process date reported by Alpaca. |
| `old_rate` | `float` | Old share rate in the split action. |
| `new_rate` | `float` | New share rate in the split action. |
| `cash_rate` | `float` | Cash dividend rate when the action type is `cash_dividends`. |
| `split_ratio` | `float` | `new_rate / old_rate`. |
| `source` | `object` | Source label (`alpaca`). |
| `ingested_ts` | `timestamp` | Ingest timestamp. |

## `silver.universe_membership_events`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `event_date` | `date` | Trading date where membership changed versus the prior trading day. |
| `symbol` | `object` | Canonical ticker symbol. |
| `event_type` | `object` | Membership change classification (`added` or `removed`). |
| `previous_liquidity_rank` | `int` | Prior-day liquidity rank when the symbol was already in the universe. |
| `new_liquidity_rank` | `int` | Current-day liquidity rank when the symbol is in the new universe. |
| `previous_rolling_avg_dollar_volume` | `float` | Prior-day trailing average dollar volume when available. |
| `new_rolling_avg_dollar_volume` | `float` | Current-day trailing average dollar volume when available. |
| `source` | `object` | Source label for the liquidity rule. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.universe_membership_daily`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `member_date` | `date` | Trading date for the liquidity-ranked daily universe. |
| `symbol` | `object` | Canonical ticker symbol. |
| `liquidity_rank` | `int` | Rank by trailing average dollar volume for that trading day. |
| `rolling_avg_dollar_volume` | `float` | Trailing average dollar volume used for membership selection. |
| `source` | `object` | Source label for the liquidity rule. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.ref_sp500`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id` (nullable). |
| `symbol` | `object` | Ticker symbol. |
| `security` | `object` | Company name. |
| `gics_sector` | `object` | GICS sector. |
| `gics_sub_industry` | `object` | GICS sub-industry. |
| `headquarters_location` | `object` | Headquarters location. |
| `date_first_added` | `object` | Date first added (source format). |
| `cik` | `object` | SEC CIK. |
| `founded` | `object` | Founded value (source format). |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |
| `source_url` | `object` | Source URL. |

## `silver.ref_publishers`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `publisher_id` | `int` | Deterministic publisher id. |
| `publisher_name` | `object` | Publisher name from source. |
| `publisher_name_norm` | `object` | Normalized publisher name (lowercase, trimmed). |
| `ingested_ts` | `timestamp` | First insert timestamp. |
| `publisher_domain` | `object` | Mapped base domain. |
| `publisher_weight` | `float` | Weight derived from Tranco rank or default. |
| `weight_source` | `object` | `tranco` or `default`. |
| `weight_updated_ts` | `timestamp` | Last weight refresh timestamp. |

## `silver.news`

*Partitioned Parquet file*

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

## `silver.wikipedia_pageviews`

*Partitioned Parquet file*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Foreign key to `silver.assets.asset_id` (nullable). |
| `granularity` | `object` | Time granularity. |
| `view_date` | `date` | Normalized view date. |
| `views` | `int` | Pageview count. |
| `ingested_ts` | `timestamp` | Ingest timestamp. |

## `silver.factors`

*Parquet file*

| Column | Type | Description |
| --- | --- | --- |
| `factor_date` | `date` | Trading date for the factor observation. |
| `mkt_rf` | `float` | Market excess return (`MKT-RF`). |
| `smb` | `float` | Size factor (`SMB`). |
| `hml` | `float` | Value factor (`HML`). |
| `rf` | `float` | Daily risk-free rate (`RF`). |
| `mom` | `float` | Daily momentum factor (`MOM`). |
| `source` | `object` | Source label for the Kenneth R. French Data Library. |
| `frequency` | `object` | Frequency label (`daily`). |
| `ingested_ts` | `timestamp` | Bronze ingest timestamp carried forward. |
| `bronze_snapshot_date` | `date` | Bronze snapshot date used to build the silver parquet files. |

## `silver.strategy_definitions`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `strategy_id` | `object` | Unique strategy identifier. |
| `strategy_name` | `object` | Human-readable strategy name. |
| `strategy_version` | `object` | Version label. |
| `description` | `object` | Optional description. |
| `ranking_method` | `object` | Ranking metric or score logic. |
| `rebalance_frequency` | `object` | Rebalance cadence such as monthly, weekly, or daily. |
| `target_count` | `int` | Number of target holdings. |
| `weighting_method` | `object` | Portfolio weighting rule. |
| `benchmark_symbol` | `object` | Benchmark reference symbol. |
| `long_short_flag` | `bool` | Whether the strategy is long-short. |
| `start_date` | `date` | Effective start date. |
| `end_date` | `date` | Effective end date. |
| `is_active` | `bool` | Active strategy flag. |
| `config_json` | `object` | Additional serialized configuration payload. |
| `asof_ts` | `timestamp` | Load timestamp. |
| `run_id` | `object` | Pipeline run identifier that loaded the row. |


## `silver.strategy_runs`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `object` | Unique strategy run identifier. |
| `strategy_id` | `object` | Parent strategy identifier. |
| `run_type_id` | `object` | Run type code such as `backtest`, `simulation`, `paper`, or `live`. |
| `simulation_type_id` | `int` | Simulation type identifier for simulation runs; null for non-simulation run types. |
| `run_status` | `object` | Run status such as `pending`, `running`, `success`, or `failed`. |
| `dataset_version` | `object` | Research dataset snapshot identifier used for the run. |
| `code_version` | `object` | Code version, commit, or release tag used for the run. |
| `started_at` | `timestamp` | Execution start timestamp. |
| `completed_at` | `timestamp` | Execution end timestamp. |
| `error_message` | `object` | Failure details when the run does not succeed. |
| `rankings_row_count` | `int64` | Number of rows written for this run into `gold.strategy_rankings`. |
| `holdings_row_count` | `int64` | Number of rows written for this run into `gold.strategy_holdings`. |
| `returns_row_count` | `int64` | Number of rows written for this run into `gold.strategy_returns`. |
| `performance_row_count` | `int64` | Number of rows written for this run into `gold.strategy_performance`. |
| `persist` | `bool` | Whether downstream strategy outputs should be retained. |
| `asof_ts` | `timestamp` | Load timestamp. |

## `silver.strategy_parameters`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `strategy_id` | `object` | Parent strategy identifier. |
| `parameter_name` | `object` | Strategy parameter name. |
| `parameter_value` | `object` | Parameter value stored as text. |
| `parameter_type` | `object` | Declared parameter type such as `int`, `double`, `string`, or `bool`. |
| `effective_start_date` | `date` | Date the parameter becomes active. |
| `effective_end_date` | `date` | Date the parameter stops being active, if applicable. |
| `is_active` | `bool` | Whether the parameter row is currently active. |
| `description` | `object` | Optional parameter description. |
| `ingest_ts` | `timestamp` | Ingestion timestamp. |
| `asof_ts` | `timestamp` | Load timestamp. |
| `run_id` | `object` | Pipeline run identifier that loaded the row. |
