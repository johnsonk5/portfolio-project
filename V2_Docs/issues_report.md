# Issues Report

Generated: 2026-05-07

Scope reviewed: metric documentation, price/signal/universe assets, research strategy ranking/holding/return code, Fama-French factor ingestion, Streamlit dashboard views, and related tests. I did not make production code changes. The working tree already had unstaged changes in `src/portfolio_project/defs/research_db/gold/strategy.py` and `tests/test_research_gold_strategy_tables.py`; findings below describe the current working tree.

## Executive Summary

The highest-risk issues are metric-scale and simulation-modeling problems rather than ordinary coding errors. The most important confirmed issue is that Fama-French factors are stored in percent units while strategy returns are decimal returns, which makes The Lab's factor excess-return and exposure calculations mathematically inconsistent. The strategy return engine also treats missing constituent returns as 0%, which can materially bias backtests, especially around delistings, symbol gaps, suspended trading, or sparse historical data.

## Findings

### 1. Fama-French factors are in percent units but strategy returns are decimals

Priority: High

References:
- `src/portfolio_project/defs/research_db/bronze/fama_french.py:58`
- `src/portfolio_project/defs/research_db/silver/factors.py:61`
- `pages/The_Lab.py:545`
- `pages/The_Lab.py:550`
- `tests/test_fama_french_bronze.py:27`
- `tests/test_fama_french_silver.py:47`

The Kenneth French daily files publish factor values in percent units. The ingestion path parses values directly with `pd.to_numeric(...)` and the silver layer casts them through unchanged. Tests also assert raw values like `1.23`, `0.5`, and `0.01`, which means a 1.23% market factor is stored as `1.23` rather than `0.0123`.

The Lab then computes:

```python
portfolio_excess = portfolio_return - rf
```

and regresses decimal portfolio returns against `mkt_rf`, `smb`, `hml`, and `mom`. Since `portfolio_return` is decimal and `rf`/factor columns are percent units, this can make factor exposures, portfolio excess returns, and any interpretation of factor behavior wrong by roughly 100x. Example: a daily RF of `0.01` means 0.01%, but the dashboard subtracts it as 1%.

Suggested remediation:
- Normalize all factor return fields to decimal form at bronze or silver ingestion by dividing `mkt_rf`, `smb`, `hml`, `rf`, and `mom` by `100.0`.
- Update tests to expect decimal values.
- Update `docs/data-dictionaries/Silver.md` to explicitly state decimal return units.
- Consider a one-time migration or rebuild of `data/silver/factors/factors.parquet`.

### 2. Factor exposure regression omits an intercept

Priority: Medium

References:
- `pages/The_Lab.py:550`
- `pages/The_Lab.py:556`
- `pages/The_Lab.py:557`
- `docs/Dashboard.md:163`

The Lab estimates factor exposures with `np.linalg.lstsq(x, y, ...)` where `x` contains only factor columns. There is no intercept term. For factor models, the intercept is usually the residual alpha, and omitting it forces the regression line through zero. This can distort beta/exposure estimates when the strategy has positive or negative average excess return not explained by the factors.

Suggested remediation:
- Add a constant column to the regression design matrix.
- Optionally surface the intercept as factor-model alpha separately from the simple annualized active-return alpha in `gold.strategy_performance`.
- Once factor units are fixed, add a targeted test with known coefficients and non-zero intercept.

### 3. Missing constituent returns are treated as 0% returns in strategy simulations

Priority: High

References:
- `src/portfolio_project/defs/research_db/gold/strategy.py:1962`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1967`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1979`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1991`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1995`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1996`

Strategy returns are generated only over benchmark trading dates. For each held symbol, if the symbol is missing from the return matrix or its return is null, the implementation contributes `0.0` for that symbol:

```python
weight * (0.0 if pd.isna(symbol_return) else float(symbol_return))
```

That can materially bias backtests. Missing prices are not neutral information. For a held name, missing return data can mean a bad data gap, halted trading, delisting, symbol change, or incomplete vendor coverage. Treating the missing return as exactly flat preserves capital and can inflate results or reduce measured volatility/drawdown.

Suggested remediation:
- Track missing held-symbol return counts per day and fail or flag the run when above a very small threshold.
- Prefer one explicit policy: carry forward last price with a data-quality flag, drop and renormalize weights with a cash assumption, or mark the strategy return null/invalid for the day.
- Add DQ checks against `gold.strategy_returns` to report missing held return coverage.

### 4. Simulation date axis is benchmark-driven, not holdings-data-driven

Priority: Medium

References:
- `src/portfolio_project/defs/research_db/gold/strategy.py:1962`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1967`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1979`
- `src/portfolio_project/defs/research_db/gold/strategy.py:2008`

The return loop iterates over `benchmark_trade_dates`, derived from non-null benchmark returns. This guarantees benchmark comparability, but it also means the strategy path silently excludes dates where the benchmark is missing even if held securities have data. Conversely, if the benchmark exists and held securities are missing, the strategy records a return using 0% placeholders for missing holdings.

Suggested remediation:
- Build a canonical trading calendar from the research price universe or benchmark, but separately validate full held-symbol coverage for each return date.
- Record coverage diagnostics such as `held_symbols_expected`, `held_symbols_with_returns`, and `missing_symbols`.

### 5. Strategy catalog accepts weighting methods the gold implementation rejects

Priority: Medium

References:
- `src/portfolio_project/defs/research_db/silver/strategy.py:124`
- `src/portfolio_project/defs/research_db/silver/strategy.py:234`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1405`

The silver strategy validator advertises `equal`, `rank`, and `volatility` as supported weighting methods, but gold holdings materialization only implements `equal` and raises for anything else. This is not currently a live bug if all active strategies are equal-weighted, but it is a contract mismatch. A future catalog entry can validate cleanly and then fail downstream during gold materialization.

Suggested remediation:
- Either remove `rank` and `volatility` from `SUPPORTED_WEIGHTING_METHODS` until implemented, or implement them in `_materialize_holdings`.
- Add a test proving every accepted weighting method can materialize holdings or is rejected at catalog validation time.

### 6. `long_short_flag` only changes a label, not return direction or exposure

Priority: Medium

References:
- `src/portfolio_project/defs/research_db/gold/strategy.py:1413`
- `pages/The_Lab.py:1828`
- `docs/data-dictionaries/Gold.md:83`

When `long_short_flag` is true, holdings are labeled `SHORT`, but target weights remain positive and the return engine later loads only `rebalance_date`, `symbol`, and `target_weight`. It does not read or apply `side`. A short portfolio would therefore earn long-side returns while being displayed as short/long-short in downstream views.

Suggested remediation:
- If short strategies are not supported yet, reject `long_short_flag: true` at catalog validation.
- If they are intended, carry `side` into return generation and invert returns or signed weights for short legs.
- Add tests where a positive asset return produces a negative strategy contribution for a short holding.

### 7. Same-day close simulation is explicitly lookahead-unsafe but can still appear beside safer simulations

Priority: Medium

References:
- `src/portfolio_project/config/simulation_reference.yaml:1`
- `src/portfolio_project/config/simulation_reference.yaml:8`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1744`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1760`
- `src/portfolio_project/defs/research_db/gold/strategy.py:1987`

The `close_no_cost` simulation is marked `lookahead_safe_flag: false`, which is accurate: rankings are based on close-derived signals at the rebalance date, and the close-fill return model effectively enters at the rebalance close for the next session's close-to-close return. This is acceptable as a labeled diagnostic backtest, but it can be misleading if compared directly with next-open simulations without prominent handling.

Suggested remediation:
- In The Lab, visually separate or filter out `lookahead_safe_flag = false` runs by default.
- Consider excluding lookahead-unsafe runs from headline comparison tables unless the user opts in.

### 8. Dashboard and strategy SMA/52-week metrics mix adjusted returns with unadjusted trend prices

Priority: Medium

References:
- `docs/Metrics.md:17`
- `docs/Metrics.md:23`
- `src/portfolio_project/defs/portfolio_db/gold/prices.py:316`
- `src/portfolio_project/defs/portfolio_db/gold/prices.py:345`
- `src/portfolio_project/defs/portfolio_db/gold/prices.py:354`
- `src/portfolio_project/defs/research_db/silver/signals.py:75`
- `src/portfolio_project/defs/research_db/silver/signals.py:159`

Returns and 12-1 momentum use adjusted close when available, but SMA, 52-week high, and `pct_below_52w_high` are based on raw `close`. A split inside the lookback window can create discontinuities in trend and drawdown metrics even when returns are adjusted. This is especially risky because the dashboard combines adjusted momentum with raw 52-week-distance scoring for "Underrated Investments."

Suggested remediation:
- Decide whether trend metrics should be calculated on adjusted close consistently.
- If raw close is intentional, document that SMA and 52-week metrics are raw-price technical levels and may not be comparable to adjusted-return metrics.
- Add a split scenario test for `pct_below_52w_high`, SMA, and `dist_sma_*`.

### 9. `dollar_volume` remains raw-close based while adjusted prices are used elsewhere

Priority: Low

References:
- `src/portfolio_project/defs/research_db/silver/research_prices.py:558`
- `src/portfolio_project/defs/research_db/silver/signals.py:83`
- `src/portfolio_project/defs/research_db/silver/universe.py:75`

Research `dollar_volume` is computed as raw `close * volume`; liquidity universe and liquidity filters then use rolling averages of that field. This is usually fine for same-day notional liquidity, but around splits it creates mechanical jumps because volume and raw close shift on split dates. Adjusted-close-derived dollar volume would reduce split artifacts for historical comparability, while raw-close dollar volume may better represent actual daily traded notional.

Suggested remediation:
- Keep raw dollar volume if the goal is actual traded notional, but document it explicitly.
- If the goal is stable historical liquidity ranking, consider an adjusted notional liquidity field and use that for universe construction.

## Test Coverage Gaps To Add

- Fama-French unit normalization: verify `1.23` source input becomes `0.0123` before dashboard/regression use.
- Factor exposure regression: known synthetic data with non-zero intercept and known betas.
- Strategy missing-price policy: a held symbol missing on a benchmark trading date should fail, flag, or follow an explicit cash/renormalization rule.
- Long/short behavior: positive asset returns should reduce a short-only portfolio.
- Catalog contract test: every accepted `weighting_method` either materializes or is rejected before silver table creation.
- Split-sensitive trend metrics: verify SMA and 52-week calculations around a split with adjusted and raw prices.

## Notes

- I did not run the full test suite because this was an audit/report task and no production code was changed.
- The current tests around Fama-French ingestion appear to lock in percent-unit values, so fixing that issue will require updating tests, docs, and any existing local factor parquet.
