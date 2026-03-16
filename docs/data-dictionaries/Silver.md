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
