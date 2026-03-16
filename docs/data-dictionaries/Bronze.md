# Bronze Data Dictionary

All of the tables in this layer are partitioned parquet files unless otherwise stated.

## `bronze.alpaca_bars` 

| Column | Type | Description |
| --- | --- | --- |
| `open` | `float` | Open price for interval. |
| `high` | `float` | High price for interval. |
| `low` | `float` | Low price for interval. |
| `close` | `float` | Close price for interval. |
| `volume` | `int` | Volume traded. |
| `symbol` | `object` | Ticker symbol. |
| `timestamp` | `timestamp` | Bar timestamp (UTC). |
| `trade_count` | `int` | Number of trades in interval (optional). |
| `vwap` | `float` | Volume-weighted average price (optional). |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `bronze.alpaca_assets`

| Column | Type | Description |
| --- | --- | --- |
| `id` | `object` | Unique identifier for the asset. |
| `asset_class` | `object` | Asset class category. |
| `exchange` | `object` | Exchange where listed. |
| `symbol` | `object` | Ticker symbol. |
| `name` | `object` | Full asset name. |
| `status` | `object` | Source asset status. |
| `tradable` | `bool` | Whether tradable. |
| `marginable` | `bool` | Whether marginable. |
| `shortable` | `bool` | Whether shortable. |
| `easy_to_borrow` | `bool` | Easy-to-borrow flag. |
| `fractionable` | `bool` | Fractional-trading support flag. |
| `min_order_size` | `object` | Minimum order size. |
| `min_trade_increment` | `object` | Minimum trade increment. |
| `price_increment` | `object` | Minimum price increment. |
| `maintenance_margin_requirement` | `float64` | Maintenance margin requirement. |
| `attributes` | `object` | Additional source attributes. |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |

## `bronze.research_prices_daily`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | `object` | Ticker symbol. |
| `timestamp` | `timestamp` | Daily bar timestamp (UTC). |
| `trade_date` | `date` | New York trading date represented by the bar. |
| `open` | `float` | Open price for the day. |
| `high` | `float` | High price for the day. |
| `low` | `float` | Low price for the day. |
| `close` | `float` | Close price for the day. |
| `volume` | `int` | Daily share volume. |
| `trade_count` | `int` | Daily trade count when the source provides it. |
| `vwap` | `float` | Daily VWAP when the source provides it. |
| `source` | `object` | Upstream source (`alpaca` or `yahoo_finance`). |
| `ingested_ts` | `timestamp` | Ingest timestamp. |

## `bronze.sp500_companies`

| Column | Type | Description |
| --- | --- | --- |
| `Symbol` | `object` | Ticker symbol from source. |
| `Security` | `object` | Company name. |
| `GICS Sector` | `object` | GICS sector. |
| `GICS Sub-Industry` | `object` | GICS sub-industry. |
| `Headquarters Location` | `object` | Headquarters location. |
| `Date first added` | `object` | Date first added (source format). |
| `CIK` | `object` | SEC CIK. |
| `Founded` | `object` | Founded value (source format). |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |
| `source_url` | `object` | Source URL. |

## `bronze.yahoo_news`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | `object` | Ticker symbol queried. |
| `uuid` | `object` | News item identifier. |
| `title` | `object` | Headline text. |
| `publisher` | `object` | Publisher name. |
| `link` | `object` | News link. |
| `provider_publish_time` | `datetime64[us]` | Publish timestamp (UTC). |
| `type` | `object` | News item type. |
| `summary` | `object` | Summary text. |
| `query_date` | `date` | Query partition date. |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |

## `bronze.tranco`

*This table is a csv*

| Column | Type | Description |
| --- | --- | --- |
| `rank` | `int` | Global domain rank (1 = most popular). |
| `domain` | `object` | Base domain name. |

## `bronze.wikipedia_pageviews`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | `object` | Ticker symbol (nullable). |
| `company_name` | `object` | Company name used for article resolution. |
| `article` | `object` | Wikipedia article title used in API request. |
| `project` | `object` | Wikimedia project (for example, `en.wikipedia.org`). |
| `access` | `object` | Access mode. |
| `agent` | `object` | Agent type. |
| `granularity` | `object` | Time granularity (`daily`). |
| `view_date` | `object` | Date key in `YYYYMMDD` format. |
| `views` | `int` | Pageview count. |
| `ingested_ts` | `datetime64[us]` | Ingest timestamp. |
