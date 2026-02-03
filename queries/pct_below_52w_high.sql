-- Update pct_below_52w_high using a 252 trading-day rolling high of close
ALTER TABLE gold.prices ADD COLUMN IF NOT EXISTS pct_below_52w_high DOUBLE;

CREATE OR REPLACE TEMP VIEW v_pct_below_52w_high AS
SELECT
  asset_id,
  symbol,
  trade_date,
  CASE
    WHEN high_52w IS NULL OR high_52w = 0 THEN NULL
    ELSE (high_52w - close) / high_52w
  END AS pct_below_52w_high
FROM (
  SELECT
    asset_id,
    symbol,
    trade_date,
    close,
    max(close) OVER (
      PARTITION BY asset_id
      ORDER BY trade_date
      ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS high_52w
  FROM gold.prices
) t;

UPDATE gold.prices AS p
SET pct_below_52w_high = (
  SELECT v.pct_below_52w_high
  FROM v_pct_below_52w_high v
  WHERE v.asset_id = p.asset_id
    AND v.symbol = p.symbol
    AND v.trade_date = p.trade_date
);
