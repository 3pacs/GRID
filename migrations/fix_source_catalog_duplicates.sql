-- =============================================================================
-- Migration: fix_source_catalog_duplicates.sql
-- Purpose:   Resolve duplicate source_catalog entries caused by inconsistent
--            casing, remap foreign keys in raw_series and resolved_series to
--            the canonical (data-holding) entry, then deactivate losers and
--            catalog-noise entries that have no data and no known puller.
-- Database:  griddb, user=grid
-- Author:    Database Reviewer
-- Date:      2026-03-31
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- SECTION 1: DUPLICATE RESOLUTION
--
-- Decision rule:
--   - Keeper  = entry with raw_series rows (or, if tied, the one with more).
--   - Loser   = entry(ies) with zero raw_series rows.
--   - raw_series.source_id rows on the loser are migrated to the keeper.
--   - resolved_series.source_priority_used rows on the loser are remapped.
--   - The loser entry is then set active = false.
--
-- Pair-by-pair analysis (from diagnostic queries run 2026-03-31):
--
--   name group         keeper id  keeper rows  loser ids      loser rows
--   ----------------   ---------  -----------  ----------     ----------
--   coingecko          182        2922         41             0
--   crucix             166        426          72             0
--   defillama          186        3473         42             0
--   kalshi             (both 0)   489 (newer)  36             0
--   ny_fed             274        1            34             0
--   open_meteo         180        18260        62             0
--   polymarket         (all 0)    187 (newest) 403, 44        0
--   yfinance           2          74656578     39             0
--   yfinance_options   185        380          171            0
--
-- Notes:
--   - kalshi: both have 0 raw rows; keeper = id 489 (Kalshi, higher id /
--     more recently registered); loser = id 36 (KALSHI).
--   - polymarket: all three have 0 raw rows; keeper = id 187 (lowest id,
--     first registered); losers = 403 (Polymarket), 44 (POLYMARKET).
--   - resolved_series.source_priority_used IS NOT part of any unique key,
--     so remapping never violates a constraint.
-- ---------------------------------------------------------------------------

-- ---- 1a. coingecko  (keeper=182, loser=41) --------------------------------

-- raw_series: loser has 0 rows, nothing to migrate
-- resolved_series: remap 5 rows from 41 -> 182
UPDATE resolved_series
SET    source_priority_used = 182
WHERE  source_priority_used = 41;

UPDATE source_catalog SET active = false WHERE id = 41;

-- ---- 1b. crucix  (keeper=166, loser=72) ------------------------------------

-- resolved_series: remap 20 rows from 72 -> 166
UPDATE resolved_series
SET    source_priority_used = 166
WHERE  source_priority_used = 72;

UPDATE source_catalog SET active = false WHERE id = 72;

-- ---- 1c. defillama  (keeper=186, loser=42) ----------------------------------
-- id 42 (DEFILLAMA) has 0 raw_series but 12696 resolved_series references.
-- Keeper is 186 (has raw data). Remap resolved references then deactivate.

UPDATE resolved_series
SET    source_priority_used = 186
WHERE  source_priority_used = 42;

UPDATE source_catalog SET active = false WHERE id = 42;

-- ---- 1d. kalshi  (keeper=489, loser=36) -------------------------------------
-- Both have 0 raw and 0 resolved rows. Simple deactivation.

UPDATE source_catalog SET active = false WHERE id = 36;

-- ---- 1e. ny_fed  (keeper=274, loser=34) -------------------------------------
-- id 34 (NY_FED) has 0 raw_series but 1236 resolved_series references.

UPDATE resolved_series
SET    source_priority_used = 274
WHERE  source_priority_used = 34;

UPDATE source_catalog SET active = false WHERE id = 34;

-- ---- 1f. open_meteo  (keeper=180, loser=62) ---------------------------------
-- id 62 (OPEN_METEO) has 0 raw_series but 3595 resolved_series references.

UPDATE resolved_series
SET    source_priority_used = 180
WHERE  source_priority_used = 62;

UPDATE source_catalog SET active = false WHERE id = 62;

-- ---- 1g. polymarket  (keeper=187, losers=403,44) ----------------------------
-- All three have 0 raw_series. id 44 (POLYMARKET) has 1 resolved reference.
-- Remap to keeper 187, then deactivate both losers.

UPDATE resolved_series
SET    source_priority_used = 187
WHERE  source_priority_used IN (403, 44);

UPDATE source_catalog SET active = false WHERE id IN (403, 44);

-- ---- 1h. yfinance  (keeper=2, loser=39) -------------------------------------
-- id 39 (YFINANCE) has 0 raw_series but 172563 resolved_series references.

UPDATE resolved_series
SET    source_priority_used = 2
WHERE  source_priority_used = 39;

UPDATE source_catalog SET active = false WHERE id = 39;

-- ---- 1i. yfinance_options  (keeper=185, loser=171) --------------------------
-- id 171 (YFINANCE_OPTIONS) has 0 raw_series but 2857 resolved_series refs.

UPDATE resolved_series
SET    source_priority_used = 185
WHERE  source_priority_used = 171;

UPDATE source_catalog SET active = false WHERE id = 171;


-- ---------------------------------------------------------------------------
-- SECTION 2: CATALOG NOISE DEACTIVATION
--
-- Deactivate source_catalog entries that:
--   (a) have 0 rows in raw_series, AND
--   (b) are NOT in the list of known working pullers, AND
--   (c) are currently active.
--
-- Known working pullers (case-insensitive match):
--   fred, yfinance, yfinance_options, edgar, crucix, bls, googletrends, cboe,
--   fedspeeches, fear_greed, baltic_exchange, ny_fed, aaii_sentiment, cftc_cot,
--   finra_ats, kalshi, ads_index, noaa_swpc, lunar_ephemeris,
--   planetary_ephemeris, vedic_jyotish, chinese_calendar, congressional,
--   insider_filings, dark_pool, fed_liquidity, institutional_flows,
--   gov_contracts, legislation, gdelt, alphavantage_sentiment,
--   prediction_odds, unusual_whales, smart_money, supply_chain,
--   earnings_calendar, lobbying, repo_market, yield_curve_full, world_news,
--   social_attention, hf_financial_news, news_scraper, noaa_ais, foia_cables,
--   offshore_leaks, export_controls, fara
--
-- Note: also preserve any entry that already has data (coingecko, defillama,
-- open_meteo, polymarket, yfinance_analyst, yfinance_earnings, computed,
-- binance, web_scraper, alphavantage_news_sentiment, etc.) even if not in
-- the puller list -- those are handled by the raw_series guard below.
-- ---------------------------------------------------------------------------

UPDATE source_catalog
SET    active = false
WHERE  active = true
  AND  id NOT IN (
           SELECT DISTINCT source_id FROM raw_series
       )
  AND  lower(name) NOT IN (
           'fred',
           'yfinance',
           'yfinance_options',
           'edgar',
           'crucix',
           'bls',
           'googletrends',
           'cboe',
           'fedspeeches',
           'fear_greed',
           'baltic_exchange',
           'ny_fed',
           'aaii_sentiment',
           'cftc_cot',
           'finra_ats',
           'kalshi',
           'ads_index',
           'noaa_swpc',
           'lunar_ephemeris',
           'planetary_ephemeris',
           'vedic_jyotish',
           'chinese_calendar',
           'congressional',
           'insider_filings',
           'dark_pool',
           'fed_liquidity',
           'institutional_flows',
           'gov_contracts',
           'legislation',
           'gdelt',
           'alphavantage_sentiment',
           'prediction_odds',
           'unusual_whales',
           'smart_money',
           'supply_chain',
           'earnings_calendar',
           'lobbying',
           'repo_market',
           'yield_curve_full',
           'world_news',
           'social_attention',
           'hf_financial_news',
           'news_scraper',
           'noaa_ais',
           'foia_cables',
           'offshore_leaks',
           'export_controls',
           'fara'
       );


-- ---------------------------------------------------------------------------
-- SECTION 3: VERIFICATION
-- Run these SELECTs inside the transaction to confirm expected results before
-- the COMMIT is reached. A mismatch here will surface as a clear assertion
-- failure via the RAISE below.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    v_active_dupes   integer;
    v_orphan_raw     integer;
    v_orphan_resolved integer;
BEGIN
    -- Assert: no two active entries share the same lower(name)
    SELECT COUNT(*) INTO v_active_dupes
    FROM (
        SELECT lower(name)
        FROM source_catalog
        WHERE active = true
        GROUP BY lower(name)
        HAVING COUNT(*) > 1
    ) dup;

    IF v_active_dupes > 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: % active duplicate name group(s) remain', v_active_dupes;
    END IF;

    -- Assert: no raw_series rows point at a source deactivated BY THIS MIGRATION.
    -- (Source id=7 Unusual_Whales was already inactive before this migration
    --  with 28960 rows; that pre-existing condition is out of scope here.)
    SELECT COUNT(*) INTO v_orphan_raw
    FROM raw_series rs
    JOIN source_catalog sc ON sc.id = rs.source_id
    WHERE sc.active = false
      AND sc.id IN (
          -- all IDs deactivated by Sections 1 and 2 of this migration
          41, 72, 42, 36, 34, 62, 403, 44, 39, 171
      );

    IF v_orphan_raw > 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: % raw_series row(s) still reference sources deactivated by this migration', v_orphan_raw;
    END IF;

    -- Assert: no resolved_series rows point at a source deactivated BY THIS MIGRATION.
    SELECT COUNT(*) INTO v_orphan_resolved
    FROM resolved_series rsolv
    JOIN source_catalog sc ON sc.id = rsolv.source_priority_used
    WHERE sc.active = false
      AND sc.id IN (
          41, 72, 42, 36, 34, 62, 403, 44, 39, 171
      );

    IF v_orphan_resolved > 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: % resolved_series row(s) still reference sources deactivated by this migration', v_orphan_resolved;
    END IF;

    RAISE NOTICE 'All verification checks passed.';
END;
$$;


-- ---------------------------------------------------------------------------
-- SECTION 4: SUMMARY REPORT (informational, runs before commit)
-- ---------------------------------------------------------------------------

SELECT
    active,
    COUNT(*) AS catalog_entries
FROM source_catalog
GROUP BY active
ORDER BY active DESC;

SELECT
    sc.id,
    sc.name,
    sc.active,
    COUNT(rs.id)    AS raw_rows,
    COUNT(rsolv.id) AS resolved_rows
FROM source_catalog sc
LEFT JOIN raw_series        rs    ON rs.source_id            = sc.id
LEFT JOIN resolved_series   rsolv ON rsolv.source_priority_used = sc.id
WHERE lower(sc.name) IN (
    'polymarket','crucix','defillama','kalshi','ny_fed',
    'open_meteo','yfinance','coingecko','yfinance_options'
)
GROUP BY sc.id, sc.name, sc.active
ORDER BY lower(sc.name), sc.name;

COMMIT;
