-- GRID Feature Registry Taxonomy Migration
-- Fixes mislabeled families and establishes canonical 13-family taxonomy
-- Run: psql -U grid -d griddb -f scripts/taxonomy_migration.sql

BEGIN;

-- Step 1: Add subfamily column for finer granularity
ALTER TABLE feature_registry ADD COLUMN IF NOT EXISTS subfamily TEXT;

-- Step 2: Drop old CHECK constraint
ALTER TABLE feature_registry DROP CONSTRAINT IF EXISTS feature_registry_family_check;

-- Step 3: Remap families BEFORE adding new constraint

-- A: breadth -> equity (equity indices, ETFs, sector ETFs)
-- Keep actual breadth measures (adline, pct_above) as equity/breadth
UPDATE feature_registry
SET subfamily = 'index', family = 'equity'
WHERE family = 'breadth'
  AND name IN ('sp500','nasdaq','russell2000','nikkei','dax','ftse',
               'hang_seng','shanghai','kospi','bovespa');

UPDATE feature_registry
SET subfamily = 'sector_etf', family = 'equity'
WHERE family = 'breadth'
  AND name IN ('eem','xlf','xlk','xlv','xlu','xlre','xli','xlp',
               'xly','xlb','xlc','vnq','ita','xle');

UPDATE feature_registry
SET subfamily = 'breadth', family = 'equity'
WHERE family = 'breadth'
  AND (name LIKE '%adline%' OR name LIKE '%pct_above%');

UPDATE feature_registry
SET subfamily = 'momentum', family = 'equity'
WHERE family = 'breadth'
  AND (name LIKE '%mom_%' OR name LIKE '%_full');

-- Catch remaining breadth -> equity
UPDATE feature_registry
SET subfamily = COALESCE(subfamily, 'other'), family = 'equity'
WHERE family = 'breadth';

-- B: earnings -> equity (stock prices) or equity/fundamentals
UPDATE feature_registry
SET subfamily = 'fundamentals', family = 'equity'
WHERE family = 'earnings'
  AND (name LIKE '%_pe_ratio' OR name LIKE '%_fcf' OR name LIKE '%_revenue_growth'
       OR name LIKE '%_profit_margin' OR name LIKE '%_roe' OR name LIKE '%_dividend_yield'
       OR name LIKE '%_ev_ebitda' OR name LIKE '%_pb_ratio' OR name LIKE '%_earnings_surprise');

UPDATE feature_registry
SET subfamily = 'stock_price', family = 'equity'
WHERE family = 'earnings';

-- C: Crypto assets mislabeled as sentiment
UPDATE feature_registry
SET family = 'crypto', subfamily = 'asset_price'
WHERE name IN ('btc','eth','sol','tao','ada','link','avax')
  AND family = 'sentiment';

UPDATE feature_registry
SET family = 'crypto', subfamily = 'market_metric'
WHERE name IN ('crypto_total_mcap','crypto_total_volume','btc_dominance',
               'eth_dominance','active_cryptos')
  AND family = 'sentiment';

UPDATE feature_registry
SET family = 'crypto', subfamily = 'defi'
WHERE name IN ('defi_total_tvl','tvl_ethereum','tvl_solana','tvl_arbitrum',
               'tvl_base','tvl_bsc')
  AND family = 'sentiment';

UPDATE feature_registry
SET family = 'crypto', subfamily = 'on_chain'
WHERE (name LIKE 'mempool_%' OR name LIKE 'lightning_%')
  AND family = 'sentiment';

-- D: Weather mislabeled as macro
UPDATE feature_registry
SET family = 'alternative', subfamily = 'weather'
WHERE (name LIKE 'weather_%' OR name LIKE 'noaa_%')
  AND family = 'macro';

-- E: FDA mislabeled as macro
UPDATE feature_registry
SET family = 'alternative', subfamily = 'regulatory'
WHERE name LIKE 'fda_%' AND family = 'macro';

-- F: Patents mislabeled as macro
UPDATE feature_registry
SET family = 'alternative', subfamily = 'patents'
WHERE name LIKE 'patent_%' AND family = 'macro';

-- G: Other macro mislabelings
UPDATE feature_registry
SET family = 'alternative', subfamily = 'corporate'
WHERE name = 'wikidata_company_dissolutions' AND family = 'macro';

UPDATE feature_registry
SET family = 'trade', subfamily = 'disruption'
WHERE name = 'imf_portwatch_status' AND family = 'macro';

-- H: RSI/MACD mislabeled as vol
UPDATE feature_registry
SET family = 'equity', subfamily = 'technical'
WHERE name IN ('spy_rsi','qqq_rsi','spy_macd') AND family = 'vol';

UPDATE feature_registry
SET family = 'crypto', subfamily = 'technical'
WHERE name = 'btc_rsi_av' AND family = 'vol';

-- I: SEC filings mislabeled as sentiment
UPDATE feature_registry
SET family = 'alternative', subfamily = 'sec_filings'
WHERE name = 'sec_form4_activity' AND family = 'sentiment';

-- J: Congressional trading
UPDATE feature_registry
SET family = 'alternative', subfamily = 'political'
WHERE name = 'congress_trade_volume' AND family = 'sentiment';

-- K: Wiki pageviews -> sentiment/attention (keep as sentiment)
UPDATE feature_registry
SET subfamily = 'attention'
WHERE name LIKE 'wiki_%' AND family = 'sentiment' AND subfamily IS NULL;

-- L: Reddit -> sentiment/social
UPDATE feature_registry
SET subfamily = 'social'
WHERE name LIKE 'reddit_%' AND family = 'sentiment' AND subfamily IS NULL;

-- M: News -> sentiment/news
UPDATE feature_registry
SET subfamily = 'news'
WHERE name LIKE 'news_%' AND family = 'sentiment' AND subfamily IS NULL;

-- N: Polymarket -> sentiment/prediction
UPDATE feature_registry
SET subfamily = 'prediction'
WHERE name LIKE 'polymarket_%' AND family = 'sentiment' AND subfamily IS NULL;

-- O: GDELT -> sentiment/geopolitical
UPDATE feature_registry
SET subfamily = 'geopolitical'
WHERE name LIKE 'gdelt_%' AND family = 'sentiment' AND subfamily IS NULL;

-- P: Seed_v2 unofficial families -> canonical + subfamily
UPDATE feature_registry SET subfamily = family, family = 'macro'
WHERE family IN ('euro_macro','china_macro','em_macro');

UPDATE feature_registry SET subfamily = family, family = 'rates'
WHERE family = 'em_rates';

UPDATE feature_registry SET subfamily = family, family = 'alternative'
WHERE family IN ('physical','altdata','sec_velocity','innovation','productivity');

UPDATE feature_registry SET subfamily = family, family = 'commodity'
WHERE family = 'agriculture';

UPDATE feature_registry SET subfamily = family, family = 'alternative'
WHERE family = 'tsfresh';

UPDATE feature_registry SET subfamily = family, family = 'trade'
WHERE family IN ('complexity','trade');

UPDATE feature_registry SET subfamily = family, family = 'flows'
WHERE family = 'flows';

UPDATE feature_registry SET subfamily = family, family = 'systemic'
WHERE family = 'systemic';

-- Q: EIA energy data
UPDATE feature_registry
SET subfamily = 'energy'
WHERE name LIKE 'eia_%' AND family = 'commodity' AND subfamily IS NULL;

-- R: Rates subfamilies
UPDATE feature_registry SET subfamily = 'treasury'
WHERE family = 'rates' AND name LIKE 'treasury_%' AND subfamily IS NULL;

UPDATE feature_registry SET subfamily = 'yield_curve'
WHERE family = 'rates' AND name LIKE 'yield_curve%' AND subfamily IS NULL;

UPDATE feature_registry SET subfamily = 'spread'
WHERE family = 'rates' AND name LIKE '%spread%' AND subfamily IS NULL;

-- S: Set subfamily for features that still don't have one
UPDATE feature_registry SET subfamily = 'core'
WHERE subfamily IS NULL;

-- Step 4: Add new CHECK constraint with canonical 13 families
ALTER TABLE feature_registry ADD CONSTRAINT feature_registry_family_check
CHECK (family IN (
    'rates', 'credit', 'equity', 'vol', 'fx',
    'commodity', 'sentiment', 'macro', 'crypto',
    'alternative', 'flows', 'systemic', 'trade'
));

-- Verify migration
DO $$
DECLARE
    bad_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO bad_count
    FROM feature_registry
    WHERE family NOT IN ('rates','credit','equity','vol','fx','commodity',
                         'sentiment','macro','crypto','alternative',
                         'flows','systemic','trade');
    IF bad_count > 0 THEN
        RAISE EXCEPTION 'Migration incomplete: % features have invalid families', bad_count;
    END IF;
    RAISE NOTICE 'Taxonomy migration complete. All families valid.';
END $$;

-- Summary report
SELECT family, subfamily, COUNT(*) as cnt
FROM feature_registry
GROUP BY family, subfamily
ORDER BY family, subfamily;

COMMIT;
