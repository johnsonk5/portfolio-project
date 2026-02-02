-- Query for populating the momentum_12_1 field in gold.prices
UPDATE gold.prices AS p
SET momentum_12_1 = s.mom_12_1
FROM (
  SELECT
    asset_id,
    symbol,
    trade_date,
    CASE
      WHEN lag(close, 252) OVER w IS NULL THEN NULL
      WHEN lag(close, 252) OVER w = 0 THEN NULL
      WHEN lag(close, 21)  OVER w IS NULL THEN NULL
      ELSE (lag(close, 21) OVER w / lag(close, 252) OVER w) - 1
    END AS mom_12_1
  FROM gold.prices
  WINDOW w AS (PARTITION BY asset_id ORDER BY trade_date)
) AS s
WHERE p.asset_id = s.asset_id
  AND p.symbol   = s.symbol
  AND p.trade_date = s.trade_date
  AND p.momentum_12_1 IS NULL;
