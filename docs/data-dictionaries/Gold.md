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
