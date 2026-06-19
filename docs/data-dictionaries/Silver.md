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

## `silver.security_master`

*DuckDB Table in the research DuckDB silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key when the symbol has a high-confidence identifier mapping. |
| `symbol` | `object` | Source ticker symbol. |
| `canonical_symbol` | `object` | Normalized ticker symbol used for joins and de-duplication. |
| `security_name` | `object` | Security name from source metadata when available. |
| `cik` | `object` | SEC Central Index Key without left-padding when available from a high-confidence identifier mapping. |
| `sec_ticker` | `object` | Ticker reported by SEC company ticker data when available. |
| `security_type` | `object` | Broad classification such as `equity`, `fund`, `derivative`, or `unknown`. |
| `security_subtype` | `object` | More specific classification such as `common_stock`, `etf`, or `adr`. |
| `exchange` | `object` | Normalized exchange code from source metadata when available. |
| `identifier_source` | `object` | Identifier source used to enrich the security master when the mapping confidence exceeds research-symbol-only metadata. |
| `identifier_confidence` | `float` | Confidence score for the selected identifier mapping. |
| `identifier_source_snapshot_date` | `date` | Source snapshot date for the identifier mapping used to enrich the row. |
| `classification_confidence` | `float` | Heuristic confidence score between 0 and 1. |
| `classification_reason` | `object` | Human-readable reason for the classification and investability decision. |
| `classification_source` | `object` | Classification method identifier, currently based on symbols present in research prices and signals with metadata fallback where available. |
| `is_common_stock` | `bool` | Whether the row appears to be common equity. |
| `is_etf` | `bool` | Whether the row appears to be an ETF. |
| `is_adr` | `bool` | Whether the row appears to be an ADR. |
| `is_otc` | `bool` | Whether the row is listed on an OTC venue. |
| `is_bankruptcy_related` | `bool` | Whether the symbol or name suggests bankruptcy or liquidation. |
| `is_derivative_security` | `bool` | Whether the row appears to be a warrant, right, unit, preferred, or similar derivative-like security. |
| `is_fund_like` | `bool` | Whether the row appears to be fund-like, including ETFs and trusts. |
| `is_investable_common_equity` | `bool` | Convenience flag for tradable common equity excluding ETFs, ADRs, OTC names, bankruptcy-related names, derivatives, and fund-like securities. |

## `silver.security_identifiers`

*DuckDB Table in both portfolio and research DuckDB silver schemas*

Stores effective-dated external identifier mappings for project securities. The natural key is `asset_id`, `identifier_source`, `identifier_type`, `identifier_value`, and `valid_from_date`. Current rows have `is_current = true` and `valid_to_date` null. Alpaca-backed portfolio securities retain their `silver.assets.asset_id`; research-only symbols from `silver.research_daily_prices` receive the next available durable `asset_id` until a stronger identifier mapping is available.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key used for joins across portfolio and research DuckDB databases. |
| `source_symbol` | `object` | Ticker symbol as reported by the source system. |
| `security_name` | `object` | Company or security name from the identifier source. |
| `identifier_type` | `object` | Identifier namespace such as `cik`, `sec_ticker`, `alpaca_id`, `cusip`, or `isin`. |
| `identifier_value` | `object` | Identifier value in normalized source form; CIK identifiers are stored without left-padding. |
| `cik` | `object` | SEC Central Index Key without left-padding when available. |
| `sec_ticker` | `object` | Ticker reported by SEC company ticker data. |
| `alpaca_id` | `object` | Alpaca asset identifier when available. |
| `exchange` | `object` | Exchange reported by the identifier source when available. |
| `identifier_source` | `object` | Source label such as `sec_company_tickers`, `alpaca_assets`, or `manual_override`. |
| `source_priority` | `int` | Deterministic priority used to resolve conflicting mappings, where lower values win. |
| `mapping_confidence` | `float` | Confidence score between 0 and 1 for the symbol-to-identifier mapping. |
| `valid_from_date` | `date` | First date the mapping is considered valid. |
| `valid_to_date` | `date` | Last date the mapping is considered valid, null for current mappings. |
| `is_current` | `bool` | Whether this row is the active mapping for the identifier source and symbol. |
| `source_snapshot_date` | `date` | Source snapshot date for the raw identifier mapping, usually the SEC company ticker ingestion date when no source-provided effective date exists. |
| `ingestion_date` | `date` | Bronze ingestion date of the source snapshot that produced the mapping. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.asset_identity_bridge`

*DuckDB Table in both portfolio and research DuckDB silver schemas*

One row per durable `asset_id` with the preferred current symbol plus compact external identifiers used for cross-database joins.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key used for joins across portfolio and research DuckDB databases. |
| `current_symbol` | `object` | Preferred current ticker symbol selected from current symbol identifiers by source priority. |
| `source_symbols` | `object` | Comma-separated set of current, historical, and source-reported symbols known for the asset. |
| `alpaca_id` | `object` | Alpaca asset identifier when available. |
| `cik` | `object` | SEC Central Index Key without left-padding when available. |
| `security_name` | `object` | Company or security name from the highest available identifier metadata. |
| `exchange` | `object` | Exchange from identifier metadata when available. |
| `is_current` | `bool` | Whether any identifier row for the asset is current. |
| `asof_ts` | `timestamp` | Bridge table build timestamp. |

## `silver.asset_symbol_bridge`

*DuckDB Table in both portfolio and research DuckDB silver schemas*

One row per durable `asset_id` and known source symbol. Use this table to resolve current or historical/source ticker symbols to `asset_id`, Alpaca ID, and CIK.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key used for joins across portfolio and research DuckDB databases. |
| `current_symbol` | `object` | Preferred current ticker symbol selected from current symbol identifiers by source priority. |
| `source_symbol` | `object` | Current, historical, or source-reported ticker symbol for the asset. |
| `symbol_role` | `object` | `current` when `source_symbol` matches `current_symbol`; otherwise `historical_or_source`. |
| `alpaca_id` | `object` | Alpaca asset identifier when available. |
| `cik` | `object` | SEC Central Index Key without left-padding when available. |
| `identifier_source` | `object` | Source that contributed the selected source-symbol mapping. |
| `source_priority` | `int` | Deterministic priority used to resolve duplicate source-symbol mappings. |
| `mapping_confidence` | `float` | Confidence score for the selected source-symbol mapping. |
| `valid_from_date` | `date` | First date the mapping is considered valid. |
| `valid_to_date` | `date` | Last date the mapping is considered valid, null for open-ended mappings. |
| `is_current` | `bool` | Whether the selected source-symbol mapping is current. |
| `source_snapshot_date` | `date` | Source snapshot date for the selected source-symbol mapping. |
| `asof_ts` | `timestamp` | Bridge table build timestamp. |

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
| `asset_id` | `int` | Durable project asset key resolved from `silver.security_identifiers` or portfolio `silver.assets`, nullable while a source symbol is unmapped. |
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

## `silver.signals_daily`

*DuckDB Table in the research DuckDB silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `date` | `date` | Trading date for the signal row. |
| `asset_id` | `int` | Durable project asset key resolved from research daily prices, nullable for unmapped symbols or legacy partitions. |
| `symbol` | `object` | Canonical ticker symbol. |
| `close` | `float` | Daily close used for price-level signals. |
| `adjusted_close` | `float` | Adjusted close when available, otherwise close. |
| `returns_1d` | `float` | 1-day return. |
| `returns_5d` | `float` | 5-day return. |
| `returns_10d` | `float` | 10-day return. |
| `returns_21d` | `float` | 21-day return. |
| `returns_63d` | `float` | 63-day return. |
| `returns_126d` | `float` | 126-day return. |
| `returns_252d` | `float` | 252-day return. |
| `momentum_12_1` | `float` | 12-1 momentum using the t-21 and t-252 return prices. |
| `sma_20` | `float` | 20-day simple moving average of close. |
| `sma_50` | `float` | 50-day simple moving average of close. |
| `sma_200` | `float` | 200-day simple moving average of close. |
| `price_to_sma_50` | `float` | `close / sma_50`. |
| `price_to_sma_200` | `float` | `close / sma_200`. |
| `sma_50_to_200` | `float` | `sma_50 / sma_200`. |
| `realized_vol_21d` | `float` | 21-day annualized realized volatility. |
| `realized_vol_63d` | `float` | 63-day annualized realized volatility. |
| `drawdown_from_252d_high` | `float` | `close / rolling_252d_high - 1`. |
| `pct_below_52w_high` | `float` | `(rolling_252d_high - close) / rolling_252d_high`. |
| `rolling_252d_high` | `float` | Rolling 252-trading-day high of close. |
| `rolling_252d_low` | `float` | Rolling 252-trading-day low of close. |
| `avg_dollar_volume_21d` | `float` | Rolling 21-day average dollar volume. |
| `avg_dollar_volume_63d` | `float` | Rolling 63-day average dollar volume. |
| `signal_version` | `object` | Signal definition version. |
| `load_timestamp` | `timestamp` | Signal table load timestamp. |

## `silver.universe_membership_events`

*DuckDB Table in silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `event_date` | `date` | Trading date where membership changed versus the prior trading day. |
| `asset_id` | `int` | Durable project asset key carried from universe membership, nullable for unmapped symbols. |
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
| `asset_id` | `int` | Durable project asset key carried from research daily prices, nullable for unmapped symbols. |
| `symbol` | `object` | Canonical ticker symbol. |
| `liquidity_rank` | `int` | Rank by trailing average dollar volume for that trading day. |
| `rolling_avg_dollar_volume` | `float` | Trailing average dollar volume used for membership selection. |
| `source` | `object` | Source label for the liquidity rule. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.universe_eligibility_daily`

*DuckDB Table in the research DuckDB silver schema*

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key carried from research daily prices, nullable for unmapped symbols. |
| `symbol` | `object` | Canonical ticker symbol. |
| `date` | `date` | Trading date for the eligibility decision. |
| `passes_symbol_format` | `bool` | Whether the symbol matches the no-metadata sanity pattern. |
| `passes_min_price` | `bool` | Whether close is at or above the configured minimum price. |
| `passes_min_liquidity` | `bool` | Whether trailing average dollar volume meets the configured threshold. |
| `passes_trading_continuity` | `bool` | Whether enough recent price and positive-volume observations exist. |
| `passes_non_bankruptcy_suffix` | `bool` | Whether the symbol avoids the bankruptcy-like `Q` suffix rule. |
| `passes_non_derivative_suffix` | `bool` | Whether the symbol avoids warrant, right, unit, and preferred-like suffix rules. |
| `is_eligible_research_universe` | `bool` | Combined eligibility flag used before liquidity ranking. |
| `exclusion_reasons` | `object` | Semicolon-separated failed eligibility rules, blank when eligible. |
| `close` | `float` | Close used for the minimum price rule. |
| `avg_dollar_volume_63d` | `float` | Rolling average dollar volume used for liquidity filtering and ranking. |
| `trading_days_seen_252d` | `int` | Count of recent rows with close observations. |
| `volume_positive_days_252d` | `int` | Count of recent rows with positive volume. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.sec_submissions`

*DuckDB Table in the research DuckDB silver schema*

One row per SEC filing accession. The natural key is `accession_number`; `cik` and `accession_number` are required. SEC silver assets must carry `asset_id` alongside CIK by resolving CIK through `silver.asset_identity_bridge` or `silver.security_identifiers`. `asset_id` may be null only when the SEC issuer's CIK is not mapped to a project asset.

Deduplication rule: repeated rows for the same accession across source snapshots collapse to one row when filing metadata is equivalent, retaining the latest source snapshot metadata. If repeated accession rows disagree on core filing metadata such as CIK, form, filing date, report date, acceptance datetime, or primary document, the silver asset keeps the row from the latest SEC source snapshot and records the conflict through a DQ check. Duplicate checks must preserve unmapped issuers by coalescing null `asset_id` with CIK where an asset-scoped key is needed. Amendments are separate filings keyed by their own accession number; `amended_accession_number` is lineage metadata and does not replace the amendment accession as the row key.

DQ checks:
- `dq_sec_submissions_required_fields`: required SEC filing lineage fields must be present.
- `dq_sec_submissions_accession_uniqueness`: each `asset_id`, CIK, and accession combination should appear once after silver deduplication.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key resolved from CIK bridge mappings; nullable only when the SEC issuer CIK is unmapped. |
| `cik` | `object` | SEC Central Index Key without left-padding. |
| `accession_number` | `object` | SEC accession number with dashes. |
| `accession_number_nodash` | `object` | SEC accession number without dashes for URL construction and joins. |
| `form` | `object` | SEC form type such as `10-K`, `10-Q`, `8-K`, `10-K/A`, or `10-Q/A`. |
| `filing_date` | `date` | SEC filing date. |
| `report_date` | `date` | Fiscal period report date reported by SEC. |
| `acceptance_datetime` | `timestamp` | SEC acceptance datetime when available, stored in UTC. |
| `primary_document` | `object` | Primary filing document filename. |
| `primary_doc_description` | `object` | SEC primary document description. |
| `file_number` | `object` | SEC file number when available. |
| `film_number` | `object` | SEC film number when available. |
| `act` | `object` | SEC act code when available. |
| `is_amendment` | `bool` | Whether the filing form is an amendment. |
| `amended_accession_number` | `object` | Prior accession amended by this filing when inferable, otherwise null. |
| `items` | `object` | SEC item list for applicable forms, stored as source text. |
| `size_bytes` | `int` | Filing size in bytes when provided by SEC. |
| `source_url` | `object` | SEC source URL or archive member path for lineage. |
| `ingestion_date` | `date` | Bronze ingestion date of the submissions snapshot. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.sec_facts_long`

*DuckDB Table in the research DuckDB silver schema*

Normalized long-form XBRL facts from SEC company facts. The deduplication key is `cik`, `accession_number`, `taxonomy`, `tag`, `unit`, `period_start_date`, `period_end_date`, and `frame`, with latest source snapshot metadata retained when duplicate source rows are equivalent. SEC silver assets must carry `asset_id` alongside CIK by resolving CIK through `silver.asset_identity_bridge` or `silver.security_identifiers`. `asset_id` may be null only when the SEC issuer's CIK is not mapped to a project asset.

Deduplication rule: exact duplicate source facts collapse to one row while retaining the latest `source_snapshot_date`, `ingestion_date`, and `ingested_ts`. Duplicate detection must normalize null key components, such as null `period_start_date` for instant facts and null `frame`, to sentinel values in comparison queries. If duplicate-key rows disagree on value, decimals, fiscal metadata, form, or filed date, the silver asset keeps the row from the latest SEC source snapshot and records the conflict through a DQ check.

DQ checks:
- `dq_sec_facts_long_required_fields`: required SEC fact lineage and period fields must be present.
- `dq_sec_facts_long_duplicate_facts`: the SEC fact key should remain unique after silver deduplication.
- `dq_sec_facts_long_unsupported_units`: supported concepts must use the unit expected by the SEC statement mapping rules.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key resolved from CIK bridge mappings; nullable only when the SEC issuer CIK is unmapped. |
| `cik` | `object` | SEC Central Index Key without left-padding. |
| `accession_number` | `object` | Filing accession number associated with the fact. |
| `taxonomy` | `object` | XBRL taxonomy namespace such as `us-gaap`, `dei`, or `ifrs-full`. |
| `tag` | `object` | XBRL concept tag from the taxonomy. |
| `label` | `object` | SEC concept label when available. |
| `description` | `object` | SEC concept description when available. |
| `unit` | `object` | Unit reported by SEC, such as `USD`, `shares`, or `USD/shares`. |
| `value` | `float` | Numeric fact value after parser normalization. |
| `value_raw` | `object` | Raw source value as text for audit and parser troubleshooting. |
| `decimals` | `object` | SEC decimals field as reported. |
| `period_start_date` | `date` | Fact period start date for duration facts, null for instant facts. |
| `period_end_date` | `date` | Fact period end date. |
| `period_type` | `object` | `instant` or `duration`. |
| `fiscal_year` | `int` | Fiscal year reported by SEC. |
| `fiscal_period` | `object` | Fiscal period reported by SEC, such as `FY`, `Q1`, `Q2`, `Q3`, or `Q4`. |
| `form` | `object` | Filing form that provided the fact. |
| `filed_date` | `date` | SEC filed date for the fact. |
| `frame` | `object` | SEC frame identifier when present. |
| `source_snapshot_date` | `date` | SEC bulk snapshot date represented by the bronze archive. |
| `ingestion_date` | `date` | Bronze ingestion date of the company facts snapshot. |
| `ingested_ts` | `timestamp` | ETL ingest timestamp. |

## `silver.sec_statement_items`

*DuckDB Table in the research DuckDB silver schema*

Curated, concept-mapped facts used to build gold fundamentals. The natural key is `asset_id`, `cik`, `accession_number`, `canonical_metric`, `period_end_date`, `fiscal_year`, and `fiscal_period`.

Deduplication rule: `asset_id` is required for mapped project securities and must be carried from upstream SEC silver rows alongside CIK. Rows for unmapped SEC issuers may be retained with null `asset_id`, but duplicate checks must coalesce null `asset_id` with CIK so unmapped issuers are still checked. When multiple source facts can populate the same canonical metric, the selection order is lowest `mapping_priority`, exact period matches before derived period matches, direct facts before component-sum expressions, latest `source_acceptance_datetime` or `source_filed_date`, then latest `source_snapshot_date`. Mapping metadata, source accession metadata, period match fields, and derivation flags are retained so the selected canonical value is auditable.

DQ checks:
- `dq_sec_statement_items_required_fields`: required canonical metric, source concept, period, filing availability, and mapping metadata fields must be present.
- `dq_sec_cik_ticker_mapping_conflicts`: current ticker-to-CIK and ticker/CIK-to-asset mappings in `silver.security_identifiers` should be unambiguous before SEC facts are joined to project securities.

| Column | Type | Description |
| --- | --- | --- |
| `asset_id` | `int` | Durable project asset key resolved from CIK bridge mappings; nullable only when the SEC issuer CIK is unmapped. |
| `cik` | `object` | SEC Central Index Key without left-padding. |
| `accession_number` | `object` | Filing accession number associated with the source fact. |
| `canonical_metric` | `object` | Stable metric name such as `revenue`, `net_income`, `assets`, `liabilities`, `equity`, `debt`, `cash`, `operating_cash_flow`, `capex`, `diluted_shares`, or `diluted_eps`. |
| `statement_type` | `object` | Statement family such as `income_statement`, `balance_sheet`, `cash_flow`, or `shares`. |
| `taxonomy` | `object` | XBRL taxonomy namespace of the selected source concept. |
| `tag` | `object` | Source XBRL concept tag selected for the canonical metric. |
| `unit` | `object` | Unit of the selected source fact. |
| `reported_value` | `float` | Numeric source fact value before canonical sign normalization or component-sum derivation. |
| `value` | `float` | Curated canonical numeric value for the metric after period normalization, sign handling, and component-sum derivation. |
| `canonical_sign_rule` | `object` | Sign rule applied during canonicalization, such as `preserve_reported_sign` or `positive_cash_outflow`. |
| `period_start_date` | `date` | Fact period start date for duration metrics, null for instant metrics. |
| `period_end_date` | `date` | Fact period end date. |
| `period_type` | `object` | `instant` or `duration`. |
| `fiscal_year` | `int` | Fiscal year reported by SEC. |
| `fiscal_period` | `object` | Fiscal period reported by SEC, such as `FY`, `Q1`, `Q2`, `Q3`, or `Q4`. |
| `form` | `object` | Filing form that provided the selected fact. |
| `filing_date` | `date` | SEC filing date. |
| `acceptance_datetime` | `timestamp` | SEC acceptance datetime when available, stored in UTC. |
| `availability_date` | `date` | First trading-date candidate on which the metric may be used; derived from `acceptance_datetime` date when available, otherwise `filing_date`. |
| `mapping_version` | `object` | Version identifier for the canonical concept mapping rules. |
| `mapping_priority` | `int` | Priority of the selected source concept within the canonical metric mapping. |
| `source_expression` | `object` | Direct source concept or component-sum expression used to produce the canonical value. |
| `source_accession_number` | `object` | Accession number of the selected source fact or primary accession for a component-sum expression. |
| `source_form` | `object` | Form type of the selected source fact or primary filing for a component-sum expression. |
| `source_filed_date` | `date` | Filing date of the selected source fact or primary filing for a component-sum expression. |
| `source_acceptance_datetime` | `timestamp` | Acceptance datetime of the selected source fact or primary filing for a component-sum expression. |
| `period_match_type` | `object` | Match classification such as `exact_quarter`, `exact_annual`, `ytd_derived_quarter`, or `q4_derived_from_annual`. |
| `is_component_sum` | `bool` | Whether the value was built from multiple source facts. |
| `is_fallback_concept` | `bool` | Whether the selected mapping priority was not the preferred concept for the canonical metric. |
| `is_restricted_cash_included` | `bool` | Whether the cash value includes restricted cash through the selected source concept. |
| `is_lease_inclusive_debt` | `bool` | Whether the debt value includes capital or finance lease obligations through the selected source concept or expression. |
| `is_ytd_derived_quarter` | `bool` | Whether the value was derived by subtracting prior YTD values to produce a fiscal-quarter value. |
| `source_snapshot_date` | `date` | SEC bulk snapshot date represented by the bronze archive. |
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
| `mkt_rf` | `float` | Market excess return (`MKT-RF`) stored as a decimal return, not percent. |
| `smb` | `float` | Size factor (`SMB`) stored as a decimal return, not percent. |
| `hml` | `float` | Value factor (`HML`) stored as a decimal return, not percent. |
| `rf` | `float` | Daily risk-free rate (`RF`) stored as a decimal return, not percent. |
| `mom` | `float` | Daily momentum factor (`MOM`) stored as a decimal return, not percent. |
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

Strategy ranking parameters may include `signal_source`. Supported values are `silver_signals_daily` and `fundamental_signals_daily`; omitted values default to `silver_signals_daily`.
