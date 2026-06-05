# Metrics

This document explains gold-layer metrics and how they are calculated.

## Initial Canonical Fundamental Metrics

SEC fundamentals are mapped into a small initial canonical metric set before they are
pivoted into `gold.fundamentals_quarterly` and promoted into
`gold.fundamental_signals_daily`. These metrics are the first supported contract for
fundamental research features; additional SEC concepts should map into this set unless a
new research use case requires a separate canonical metric.

| Canonical metric | Statement family | Period type | Definition |
| --- | --- | --- | --- |
| `revenue` | Income statement | Duration | Total revenue or sales for the reporting period. |
| `net_income` | Income statement | Duration | Broad net income or loss for the reporting entity; common-stockholder income is only used as a fallback when broad net income concepts are unavailable. |
| `assets` | Balance sheet | Instant | Total assets at the reporting period end. |
| `equity` | Balance sheet | Instant | Stockholders' equity attributable to the registrant at the reporting period end, excluding noncontrolling interests when available; otherwise total equity. |
| `debt` | Balance sheet | Instant | Interest-bearing debt at the reporting period end, preferably short-term debt plus current maturities of long-term debt plus long-term debt. Do not use total liabilities as a fallback. |
| `cash` | Balance sheet | Instant | Cash and cash equivalents at the reporting period end. |
| `diluted_shares` | Shares | Duration | Diluted weighted-average shares outstanding for the reporting period, not point-in-time shares outstanding. |
| `diluted_eps` | Income statement | Duration | Diluted earnings per share for the reporting period. |
| `operating_cash_flow` | Cash flow statement | Duration | Net cash provided by operating activities for the reporting period; positive values mean cash provided and negative values mean cash used. |
| `capex` | Cash flow statement | Duration | Capital expenditures for property, plant, equipment, and similar long-lived assets, stored as a positive cash outflow. |

Canonical metric rules:

- Select canonical metrics from SEC source facts using versioned mapping rules with explicit concept priority, unit requirements, period-type requirements, and sign conventions.
- Use `diluted_shares` for the canonical shares metric and `diluted_eps` for EPS so downstream valuation features have a stable, conservative basis.
- Treat balance sheet metrics as point-in-time values from the selected filing's period end.
- Treat income statement and cash flow metrics as fiscal-quarter duration values. Year-to-date source facts must be converted to current-quarter values before promotion into `gold.fundamentals_quarterly`.
- For 10-K filings, derive Q4 duration values as annual values minus the sum of Q1 through Q3 when prior quarters are available.
- Daily research features may derive trailing-twelve-month values only from quarterly facts whose source filings were available as of the research date.
- Store monetary metrics in reported currency units, initially USD only. Store share metrics in shares and EPS metrics in currency per share. Retain non-matching units in bronze or silver unless a mapping version explicitly promotes them.
- Store `capex` as a positive cash outflow. If the selected source fact is reported as a negative cash flow, multiply by `-1` during canonicalization so free cash flow can be computed as `operating_cash_flow - capex`.
- Preserve SEC provenance on selected facts through source tag, taxonomy, accession number, form type, fiscal year, fiscal period, filing date, availability date, mapping version, mapping priority, and original SEC concept.
- Carry the mapping version on each selected canonical fact. Changes to concept priority, unit handling, period normalization, sign handling, or fallback logic require a new mapping version.
- Do not promote source facts into research features before `availability_date`, derived from `DATE(acceptance_datetime)` when available and otherwise `filing_date`.
- Point-in-time shares outstanding may be added later for market capitalization and enterprise-value calculations; it should not be conflated with diluted weighted-average shares.

### US GAAP Concept Fallback Mappings

The initial SEC mapping version should support `us-gaap` facts only. Candidate facts are
evaluated in priority order within each canonical metric after unit, period type, fiscal
period, taxonomy namespace, form type, and availability filters are applied. If multiple
facts survive for the same canonical metric and period, choose the lowest numeric mapping
priority, then the most exact period match, then the latest accepted filing for
amendments or restatements. The selected canonical value must preserve source concept,
source accession, reported unit, period boundaries, and whether the value came from a
direct fact, fallback concept, component sum, or derived quarter calculation.

| Canonical metric | Priority | `us-gaap` concept or expression | Required unit | Selection notes |
| --- | --- | --- | --- | --- |
| `revenue` | 10 | `RevenueFromContractWithCustomerExcludingAssessedTax` | `USD` | Preferred ASC 606 revenue concept. |
| `revenue` | 20 | `RevenueFromContractWithCustomerIncludingAssessedTax` | `USD` | Use when assessed-tax-exclusive revenue is not available. |
| `revenue` | 30 | `Revenues` | `USD` | Broad fallback for total revenue. |
| `revenue` | 40 | `SalesRevenueNet` | `USD` | Legacy net sales fallback. |
| `net_income` | 10 | `NetIncomeLoss` | `USD` | Preferred broad net income concept. |
| `net_income` | 20 | `ProfitLoss` | `USD` | Fallback when the filer uses the generic profit/loss concept. |
| `net_income` | 30 | `NetIncomeLossAvailableToCommonStockholdersBasic` | `USD` | Use only when a registrant/parent net income concept is unavailable; do not mix this with per-share numerator logic without preserving the source concept. |
| `assets` | 10 | `Assets` | `USD` | Total assets at period end. |
| `equity` | 10 | `StockholdersEquity` | `USD` | Preferred equity attributable to the registrant/parent. |
| `equity` | 20 | `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | `USD` | Use only when parent-attributable equity is unavailable. |
| `debt` | 10 | `ShortTermBorrowings` + `LongTermDebtCurrent` + `LongTermDebtNoncurrent` | `USD` | Preferred component sum when all available components can be matched for the same period. Missing optional components may be treated as zero only for explicitly optional debt components, such as short-term borrowings, and only when at least one primary debt component exists for the same period. |
| `debt` | 20 | `ShortTermBorrowings` + `LongTermDebtAndCapitalLeaseObligationsCurrent` + `LongTermDebtAndCapitalLeaseObligations` | `USD` | Lease-inclusive component sum for filers that use legacy capital-lease debt concepts. |
| `debt` | 30 | `LongTermDebtCurrent` + `LongTermDebtNoncurrent` | `USD` | Fallback when short-term borrowings are absent. |
| `debt` | 40 | `LongTermDebtAndCapitalLeaseObligationsCurrent` + `LongTermDebtAndCapitalLeaseObligations` | `USD` | Lease-inclusive fallback when short-term borrowings are absent. |
| `debt` | 50 | `LongTermDebt` | `USD` | Last-resort long-term debt fallback; preserve the source concept because current borrowings may be missing. |
| `cash` | 10 | `CashAndCashEquivalentsAtCarryingValue` | `USD` | Preferred cash and equivalents concept. |
| `cash` | 20 | `Cash` | `USD` | Cash-only fallback when cash equivalents are not reported. |
| `cash` | 30 | `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents` | `USD` | Last-resort fallback; preserve the source concept because restricted cash may reduce comparability. |
| `diluted_shares` | 10 | `WeightedAverageNumberOfDilutedSharesOutstanding` | `shares` | Preferred diluted weighted-average share count. |
| `diluted_eps` | 10 | `EarningsPerShareDiluted` | `USD/shares` | Preferred diluted EPS. |
| `diluted_eps` | 20 | `EarningsPerShareBasicAndDiluted` | `USD/shares` | Use only when separate diluted EPS is unavailable. |
| `operating_cash_flow` | 10 | `NetCashProvidedByUsedInOperatingActivities` | `USD` | Preferred operating cash flow; preserve sign as reported. |
| `operating_cash_flow` | 20 | `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations` | `USD` | Fallback when total operating cash flow is unavailable. |
| `capex` | 10 | `PaymentsToAcquirePropertyPlantAndEquipment` | `USD` | Preferred capital expenditure concept; store as a positive cash outflow. Multiply by `-1` only if the source fact is reported as a negative cash-flow value. |
| `capex` | 20 | `PaymentsToAcquireProductiveAssets` | `USD` | Broader productive-assets fallback; store as a positive cash outflow and preserve the source concept. Multiply by `-1` only if the source fact is reported as a negative cash-flow value. |

Period and form selection rules:

- For duration metrics, prefer facts whose start and end dates exactly match the fiscal quarter or fiscal year period being built.
- For quarterly rows, prefer quarter-duration facts over year-to-date facts.
- Derive a quarterly duration value from year-to-date facts only when the prior YTD fact is available from the same fiscal year, same source concept, compatible unit, and compatible filing lineage.
- For 10-K filings, derive Q4 duration values from annual values only when Q1 through Q3 values are available from compatible source concepts and units.
- Do not annualize quarterly income statement or cash flow facts.
- Prefer 10-Q and 10-K facts for standard quarterly and annual periods. Allow 10-Q/A and 10-K/A to supersede original filings for the same fiscal period only on or after the amendment `availability_date`.

Debt mapping rules:

- Do not infer zero for missing current or noncurrent long-term debt components unless a broader reported total or a documented taxonomy calculation relationship supports the inference.
- Do not include operating lease liabilities in canonical `debt` for the initial mapping version.
- Preserve whether a debt value is lease-inclusive so downstream research can separate strict debt from debt plus capital or finance lease obligations.

Audit fields:

Selected canonical facts should retain enough metadata to explain unusual values,
including `reported_value`, `canonical_value`, `canonical_sign_rule`,
`source_concept`, `source_expression`, `source_accession_number`, `source_form`,
`source_filed_date`, `source_acceptance_datetime`, `period_match_type`, `unit`,
`is_component_sum`, `is_fallback_concept`, `is_restricted_cash_included`,
`is_lease_inclusive_debt`, and `is_ytd_derived_quarter`.

Mapping exclusions:

- Do not map `CostOfRevenue`, `GrossProfit`, `OperatingIncomeLoss`, or `InterestIncomeExpenseNonOperatingNet` into `revenue`.
- Do not map `Liabilities`, `LiabilitiesCurrent`, or other total-liability concepts into `debt`.
- Do not map `OperatingLeaseLiabilityCurrent` or `OperatingLeaseLiabilityNoncurrent` into `debt` in the initial version.
- Do not map `WeightedAverageNumberOfSharesOutstandingBasic` into `diluted_shares` in the initial version.
- Do not map `EarningsPerShareBasic` into `diluted_eps` in the initial version.
- Do not map acquisition, business-combination, investment-purchase, or lease-payment concepts into `capex`.
- Do not promote non-USD monetary facts, non-`shares` share facts, or non-`USD/shares` EPS facts without a new mapping version.
- Do not use 8-K earnings release facts to supersede 10-Q or 10-K facts in the initial version.

Financial-sector note:

Revenue comparability is weaker for banks, insurers, REITs, and other financial-sector
issuers. The initial mapping can still select supported `revenue` facts for these
issuers, but sector-specific revenue mappings should be introduced under a later mapping
version before using revenue-heavy factors across mixed sectors.

## `gold.prices`

Daily per-asset metrics.

### Core Price Metrics
- `open`, `high`, `low`, `close`: daily OHLC built from intraday silver bars.
- `volume`: daily sum of volume.
- `trade_count`: daily sum of trade_count.
- `vwap`: volume-weighted aggregation of intraday `vwap` values.
- `dollar_volume`: `close * volume`.

### Return Metrics
- `returns_1d = adjusted_close_t / adjusted_close_t-1 - 1` when adjusted close is available, otherwise close.
- `returns_5d = adjusted_close_t / adjusted_close_t-5 - 1` when adjusted close is available, otherwise close.
- `returns_21d = adjusted_close_t / adjusted_close_t-21 - 1` when adjusted close is available, otherwise close.

### Trend and Risk Metrics
- `realized_vol_21d`: rolling 21-observation stddev of `returns_1d`, annualized by `sqrt(252)`.
- `momentum_12_1 = adjusted_close_t-21 / adjusted_close_t-252 - 1` when adjusted close is available, otherwise close.
- `pct_below_52w_high = (rolling_252d_high - close_t) / rolling_252d_high`
- `sma_50`: rolling 50-day average of close.
- `sma_200`: rolling 200-day average of close.
- `dist_sma_50 = close_t / sma_50 - 1`
- `dist_sma_200 = close_t / sma_200 - 1`

### Sentiment Metric
- `sentiment_score`: weighted average of headline sentiment over a 7-day window (`trade_date - 6` to `trade_date`).
- Label mapping:
  - positive = `1`
  - neutral = `0`
  - negative = `-1`
- Weighted by `silver.ref_publishers.publisher_weight`.

## `gold.headlines`

Rolling 30-day headlines table used by dashboard and sentiment logic.

### Key Fields
- `provider_publish_time`
- `title`
- `sentiment` (FinBERT label from headline text)

## `gold.activity`

Rolling activity table (currently Wikipedia pageviews).

### Metrics
- `views`: raw daily pageviews.
- `views_30d_avg`: rolling 30-day average views per asset.
- `views_vs_30d_avg = views / views_30d_avg` (NULL when denominator is zero or NULL).
