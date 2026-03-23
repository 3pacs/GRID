-- GRID Taxonomy Fix: Break down generic "core" and "other" subfamilies
-- Run: psql -U grid -d griddb -f scripts/taxonomy_fix.sql

BEGIN;

-- =====================================================================
-- 1. EQUITY/OTHER (35) -> proper subfamilies
-- =====================================================================

-- Single stock prices (tickers without fundamentals suffix)
UPDATE feature_registry
SET subfamily = 'stock_price'
WHERE family = 'equity' AND subfamily = 'other'
  AND name ~ '^[a-z]{2,5}$'
  AND name NOT IN ('sp500','nasdaq','gold','btc','eth','sol','vix');

-- Full price series (e.g., dvn_full, rxt_full)
UPDATE feature_registry
SET subfamily = 'stock_price'
WHERE family = 'equity' AND subfamily = 'other'
  AND name LIKE '%_full';

-- Moving averages and range metrics -> technical
UPDATE feature_registry
SET subfamily = 'technical'
WHERE family = 'equity' AND subfamily = 'other'
  AND (name LIKE '%_fifty_day%' OR name LIKE '%_two_hundred%'
       OR name LIKE '%_fifty_two_%' OR name LIKE '%_avg%');

-- Beta -> risk
UPDATE feature_registry
SET subfamily = 'risk'
WHERE family = 'equity' AND subfamily = 'other'
  AND name LIKE '%_beta';

-- Volume metrics
UPDATE feature_registry
SET subfamily = 'volume'
WHERE family = 'equity' AND subfamily = 'other'
  AND (name LIKE '%_volume%' OR name LIKE '%_vol_avg%');

-- Market cap
UPDATE feature_registry
SET subfamily = 'market_cap'
WHERE family = 'equity' AND subfamily = 'other'
  AND name LIKE '%_market_cap%';

-- Catch remaining equity/other -> stock_price (most likely individual stocks)
UPDATE feature_registry
SET subfamily = 'stock_price'
WHERE family = 'equity' AND subfamily = 'other';

-- =====================================================================
-- 2. SENTIMENT — check if features exist, fix any that got lost
-- =====================================================================

-- Wiki pageviews -> sentiment/attention
UPDATE feature_registry
SET family = 'sentiment', subfamily = 'attention'
WHERE name LIKE 'wiki_%' AND family != 'sentiment';

-- Reddit -> sentiment/social
UPDATE feature_registry
SET family = 'sentiment', subfamily = 'social'
WHERE name LIKE 'reddit_%' AND family != 'sentiment';

-- News -> sentiment/news
UPDATE feature_registry
SET family = 'sentiment', subfamily = 'news'
WHERE name LIKE 'news_%' AND family != 'sentiment';

-- Polymarket -> sentiment/prediction
UPDATE feature_registry
SET family = 'sentiment', subfamily = 'prediction'
WHERE name LIKE 'polymarket_%' AND family != 'sentiment';

-- GDELT -> sentiment/geopolitical
UPDATE feature_registry
SET family = 'sentiment', subfamily = 'geopolitical'
WHERE name LIKE 'gdelt_%' AND family != 'sentiment';

-- Consumer sentiment stays sentiment
UPDATE feature_registry
SET subfamily = 'survey'
WHERE name = 'consumer_sentiment' AND subfamily IS NULL OR subfamily = 'core';

-- Crypto fear & greed is sentiment, not crypto
UPDATE feature_registry
SET family = 'sentiment', subfamily = 'crypto_sentiment'
WHERE name = 'crypto_fear_greed' AND family = 'crypto';

-- Ensure all sentiment features have subfamilies
UPDATE feature_registry
SET subfamily = 'attention'
WHERE family = 'sentiment' AND name LIKE 'wiki_%' AND (subfamily IS NULL OR subfamily = 'core');

UPDATE feature_registry
SET subfamily = 'social'
WHERE family = 'sentiment' AND name LIKE 'reddit_%' AND (subfamily IS NULL OR subfamily = 'core');

UPDATE feature_registry
SET subfamily = 'news'
WHERE family = 'sentiment' AND name LIKE 'news_%' AND (subfamily IS NULL OR subfamily = 'core');

UPDATE feature_registry
SET subfamily = 'prediction'
WHERE family = 'sentiment' AND name LIKE 'polymarket_%' AND (subfamily IS NULL OR subfamily = 'core');

UPDATE feature_registry
SET subfamily = 'geopolitical'
WHERE family = 'sentiment' AND name LIKE 'gdelt_%' AND (subfamily IS NULL OR subfamily = 'core');

-- =====================================================================
-- 3. MACRO/CORE (30) -> granular subfamilies
-- =====================================================================

-- Inflation
UPDATE feature_registry
SET subfamily = 'inflation'
WHERE family = 'macro' AND subfamily = 'core'
  AND name IN ('cpi', 'core_pce', 'pce_deflator', 'breakeven_10y');

-- Employment
UPDATE feature_registry
SET subfamily = 'employment'
WHERE family = 'macro' AND subfamily = 'core'
  AND name IN ('unemployment', 'nonfarm_payrolls', 'initial_claims',
               'continued_claims', 'jolts_openings');

-- Output & activity
UPDATE feature_registry
SET subfamily = 'output'
WHERE family = 'macro' AND subfamily = 'core'
  AND name IN ('real_gdp', 'industrial_production', 'capacity_util',
               'chicago_fed', 'kansas_fed', 'retail_sales');

-- Money supply & Fed
UPDATE feature_registry
SET subfamily = 'monetary'
WHERE family = 'macro' AND subfamily = 'core'
  AND name IN ('m2_money_supply', 'fed_balance_sheet', 'reverse_repo',
               'sofr', 'loan_growth');

-- Housing
UPDATE feature_registry
SET subfamily = 'housing'
WHERE family = 'macro' AND subfamily = 'core'
  AND name IN ('housing_starts', 'building_permits', 'mortgage_30y',
               'case_shiller');

-- Consumer
UPDATE feature_registry
SET subfamily = 'consumer'
WHERE family = 'macro' AND subfamily = 'core'
  AND name IN ('consumer_sentiment', 'retail_sales');

-- =====================================================================
-- 4. RATES/CORE (18) -> granular subfamilies
-- =====================================================================

-- SOFR and fed funds
UPDATE feature_registry
SET subfamily = 'short_rate'
WHERE family = 'rates' AND subfamily = 'core'
  AND name IN ('sofr', 'fed_funds', 'ny_fed_sofr');

-- Bond ETFs
UPDATE feature_registry
SET subfamily = 'bond_etf'
WHERE family = 'rates' AND subfamily = 'core'
  AND name IN ('tlt', 'ief', 'shy', 'tip', 'bnd', 'hyg', 'lqd');

-- Spreads
UPDATE feature_registry
SET subfamily = 'spread'
WHERE family = 'rates' AND subfamily = 'core'
  AND name IN ('hy_spread', 'breakeven_10y');

-- MOVE index
UPDATE feature_registry
SET subfamily = 'rate_vol'
WHERE family = 'rates' AND subfamily = 'core'
  AND name IN ('move_index', 'ice_bofa_move');

-- Treasury auctions
UPDATE feature_registry
SET subfamily = 'auction'
WHERE family = 'rates' AND subfamily = 'core'
  AND name IN ('treasury_bid_to_cover', 'treasury_auction_yield');

-- =====================================================================
-- 5. CREDIT/CORE (27) -> granular subfamilies
-- =====================================================================

-- HY spread
UPDATE feature_registry
SET subfamily = 'spread'
WHERE family = 'credit' AND subfamily = 'core'
  AND name LIKE '%spread%';

-- CDS
UPDATE feature_registry
SET subfamily = 'cds'
WHERE family = 'credit' AND subfamily = 'core'
  AND name LIKE '%cds%';

-- ETFs
UPDATE feature_registry
SET subfamily = 'etf'
WHERE family = 'credit' AND subfamily = 'core'
  AND name IN ('hyg', 'lqd', 'jnk', 'bkln');

-- =====================================================================
-- 6. COMMODITY/CORE (11) -> granular subfamilies
-- =====================================================================

-- Precious metals
UPDATE feature_registry
SET subfamily = 'precious_metals'
WHERE family = 'commodity' AND subfamily = 'core'
  AND name IN ('gold', 'silver', 'platinum');

-- Industrial metals
UPDATE feature_registry
SET subfamily = 'industrial_metals'
WHERE family = 'commodity' AND subfamily = 'core'
  AND name IN ('copper');

-- Energy
UPDATE feature_registry
SET subfamily = 'energy'
WHERE family = 'commodity' AND subfamily = 'core'
  AND name IN ('crude_oil', 'nat_gas', 'uranium_etf', 'eog', 'dvn', 'xle');

-- Agriculture
UPDATE feature_registry
SET subfamily = 'agriculture'
WHERE family = 'commodity' AND subfamily = 'core'
  AND name IN ('wheat', 'corn', 'soybeans');

-- =====================================================================
-- 7. FX/CORE (8) -> granular subfamilies
-- =====================================================================

UPDATE feature_registry
SET subfamily = 'major_pair'
WHERE family = 'fx' AND subfamily = 'core'
  AND name IN ('eurusd', 'usdjpy', 'gbpusd');

UPDATE feature_registry
SET subfamily = 'em_pair'
WHERE family = 'fx' AND subfamily = 'core'
  AND name IN ('usdcnh');

UPDATE feature_registry
SET subfamily = 'index'
WHERE family = 'fx' AND subfamily = 'core'
  AND name IN ('dollar_index', 'dxy_etf');

-- =====================================================================
-- 8. CRYPTO/CORE (11) -> granular subfamilies
-- =====================================================================

UPDATE feature_registry
SET subfamily = 'stablecoin'
WHERE family = 'crypto' AND subfamily = 'core'
  AND name IN ('usdt_supply', 'usdc_supply');

UPDATE feature_registry
SET subfamily = 'market_metric'
WHERE family = 'crypto' AND subfamily = 'core'
  AND (name LIKE '%_market_cap' OR name LIKE '%_total_volume'
       OR name LIKE '%_dominance' OR name = 'active_cryptos');

UPDATE feature_registry
SET subfamily = 'dex'
WHERE family = 'crypto' AND subfamily = 'core'
  AND name LIKE 'dex_%';

UPDATE feature_registry
SET subfamily = 'memecoin'
WHERE family = 'crypto' AND subfamily = 'core'
  AND name LIKE 'pump_%';

-- =====================================================================
-- 9. Rename remaining "core" to something more descriptive
-- =====================================================================

-- Any remaining core in vol
UPDATE feature_registry
SET subfamily = 'implied'
WHERE family = 'vol' AND subfamily = 'core'
  AND name IN ('vix', 'vxn');

UPDATE feature_registry
SET subfamily = 'realized'
WHERE family = 'vol' AND subfamily = 'core'
  AND name LIKE '%_realized_vol%';

UPDATE feature_registry
SET subfamily = 'term_structure'
WHERE family = 'vol' AND subfamily = 'core'
  AND name LIKE '%_term%';

-- =====================================================================
-- VERIFICATION
-- =====================================================================

-- Report remaining "core" or "other" that need attention
DO $$
DECLARE
    core_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO core_count
    FROM feature_registry
    WHERE subfamily IN ('core', 'other');
    IF core_count > 0 THEN
        RAISE NOTICE '% features still have generic subfamilies (core/other)', core_count;
    ELSE
        RAISE NOTICE 'All features have specific subfamilies';
    END IF;
END $$;

-- Final summary
SELECT family, subfamily, COUNT(*) as cnt
FROM feature_registry
GROUP BY family, subfamily
ORDER BY family, subfamily;

COMMIT;
