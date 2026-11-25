-- backfill_resolved_series_from_ticker_metrics.sql
--
-- Background
-- ----------
-- `resolved_series` is the PIT-correct price surface that
-- `walk_forward_validate.py` and other downstream consumers query.
-- For equity tickers (feature_registry.name ending in `_full`) the
-- canonical pipeline used to be Tiingo -> raw_series -> resolved_series
-- via the normalizer. Tiingo was paused mid-cycle (bandwidth exhausted,
-- see Inbox/Agent-TODO around 2026-05-15), so the equity tail of
-- resolved_series froze at 2026-03-31 while every walk-forward audit
-- relied on it.
--
-- Meanwhile codex's `td_backfill_gem_tickers.py` (2026-05-17) populated
-- `ticker_metrics_daily` with Twelve Data close prices through the
-- current trading day. This script bridges that fresh table back into
-- `resolved_series` so the PIT path produces real returns again.
--
-- After running, walk_forward_validate's PIT hit-rate goes from 0/200
-- to ~180/200 on the same 200-row slice (verified 2026-05-17 19:49 UTC).
--
-- Usage
-- -----
--   PGPASSWORD=... psql -h <host> -U grid -d griddb \
--     -f scripts/backfill_resolved_series_from_ticker_metrics.sql
--
-- Idempotent: ON CONFLICT DO NOTHING. Re-run daily.
--
-- Point-in-time stamps (E1-V7c, fixed 2026-11-21)
-- ------------------------------------------------
-- A bridged row becomes knowable when GRID fetched it into
-- ticker_metrics_daily (`tmd.as_of`, set by every writer of that table),
-- not on its observation day. Both `release_date` and `vintage_date` are
-- that fetch day (UTC, the day convention of store/pit.py), never earlier
-- than `obs_date`; a row with no `as_of` is stamped with the day this script
-- runs, the first day GRID can show it held the value. Until 2026-11-21
-- this script wrote `release_date = obs_date`: those rows claim release up
-- to ~60 days before GRID held them. store/pit.py and the direct readers
-- fence `vintage_date <= as_of` as well, so they no longer leak; existing
-- rows are not rewritten here (counts:
-- scripts/count_release_before_vintage.py, read-only).
--
-- Source filter and provenance (Codex hold on #1372, fixed 2026-11-25)
-- --------------------------------------------------------------------
-- ticker_metrics_daily has several writers, and its `source` column says
-- which one wrote each close. Until 2026-11-25 this script read every
-- non-null close and stamped all of them TWELVEDATA. It now reads only the
-- labels in `bridge_source` below, matched exactly (case-sensitive, no
-- pattern), and stamps each row with the source_catalog id that its label
-- maps to. A label that is not listed, NULL included, is never bridged.
--
-- Labels on griddb (read-only count, 2026-11-25 09:45Z):
--   twelvedata_universe_backfill   325,189 rows   BRIDGED -> TWELVEDATA
--     (scripts/td_backfill_universe.py)
--   twelvedata_backfill              1,853 rows   BRIDGED -> TWELVEDATA
--     (scripts/td_backfill_gem_tickers.py, scripts/backfill_smallcap_coverage.py)
--   sec_xbrl + yfinance            177,986 rows   EXCLUDED
--     ingestion/altdata/sec_xbrl_shares.py copies the raw_series close
--     `YF:<T>:close` here to compute market cap. That close is the
--     as-traded (unadjusted) basis, but equity `<t>_full` panels are on the
--     adjusted basis (docs/reference/PRICE_PANEL_BASIS.md), and the
--     resolver already owns that raw_series row. Bridging it would mix bases
--     in one panel and give the resolver's data a second, wrong provenance.
--   NULL                                 0 rows   EXCLUDED (no provenance)
-- Also excluded: `stored_close_backfill:<sources>` from
-- scripts/close_backfill_universe.py (no rows on griddb yet). Those are
-- copies of closes that other sources already store, so they map to no
-- single source_catalog id. Add a label here only with its own catalog id.
--
-- Pinned raw-close panels are never bridged (Codex hold on #1459, 2026-11-25)
-- ---------------------------------------------------------------------------
-- normalization/entity_map.py RAW_CLOSE_FULL_FEATURES lists the `_full`
-- panels that are not equity adjusted closes: `spy_full` is the receipted
-- unadjusted SPY close that only Resolver._resolve_spy_close_receipt writes
-- (price_close_contract.SPY_CLOSE_FEATURE); `sp500_full`, `gold_full` and the
-- crypto panels are index, futures and coin closes. A TwelveData equity close
-- joins them by ticker name alone (SPY -> spy_full; GOLD, Barrick's stock,
-- -> gold_full, the gold future), so the bridge skips every one of them.
-- tests/test_ticker_metrics_bridge_sql_pg.py pins this list to the frozenset.
-- Rows already written there are not touched by this script (read-only
-- counts 2026-11-25: spy_full 54, gold_full 67 TWELVEDATA rows).

SET statement_timeout = 300000;

-- 1) Ensure TWELVEDATA source exists in source_catalog.
INSERT INTO source_catalog
  (name, base_url, cost_tier, latency_class, pit_available, revision_behavior,
   trust_score, priority_rank, active)
SELECT 'TWELVEDATA', 'https://api.twelvedata.com', 'PAID', 'EOD', false, 'RARE',
       'HIGH', 60, true
WHERE NOT EXISTS (SELECT 1 FROM source_catalog WHERE name = 'TWELVEDATA');

-- 2) Backfill resolved_series from the TwelveData rows of
--    ticker_metrics_daily (see "Source filter" above) for every
--    feature whose name matches `<ticker>_full`. Only rows newer than
--    the existing per-feature max get a fresh resolved row; ON CONFLICT
--    catches the rare race where the normalizer wrote the same key.
INSERT INTO resolved_series
  (feature_id, obs_date, release_date, vintage_date, value,
   source_priority_used, conflict_flag, resolution_version)
SELECT
  fr.id AS feature_id,
  tmd.obs_date,
  held.held_on AS release_date,
  held.held_on AS vintage_date,
  tmd.close_price::double precision AS value,
  sc.id AS source_priority_used,
  false AS conflict_flag,
  1 AS resolution_version
FROM ticker_metrics_daily tmd
-- Allowlist of ticker_metrics_daily.source labels -> source_catalog.name.
-- The inner joins drop every other label, NULL included.
JOIN (VALUES
        ('twelvedata_universe_backfill', 'TWELVEDATA'),
        ('twelvedata_backfill',          'TWELVEDATA')
     ) AS bridge_source (tmd_source, catalog_name)
  ON bridge_source.tmd_source = tmd.source
JOIN source_catalog sc
  ON sc.name = bridge_source.catalog_name
JOIN feature_registry fr
  ON fr.name = lower(tmd.ticker) || '_full'
CROSS JOIN LATERAL (
  SELECT GREATEST(
           tmd.obs_date,
           COALESCE((tmd.as_of AT TIME ZONE 'UTC')::date, (now() AT TIME ZONE 'UTC')::date)
         ) AS held_on
) held
LEFT JOIN LATERAL (
  SELECT max(obs_date) AS max_obs FROM resolved_series WHERE feature_id = fr.id
) rs ON true
WHERE tmd.close_price IS NOT NULL
  AND tmd.obs_date > COALESCE(rs.max_obs, '1900-01-01'::date)
  -- RAW_CLOSE_FULL_FEATURES (see "Pinned raw-close panels" above)
  AND fr.name NOT IN ('spy_full', 'sp500_full', 'gold_full',
                      'btc_usd_full', 'eth_usd_full', 'sol_usd_full')
ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING;

-- 3) Report what landed.
SELECT
  'TWELVEDATA-backfilled rows total' AS metric,
  count(*)::text AS value
FROM resolved_series
WHERE source_priority_used = (SELECT id FROM source_catalog WHERE name = 'TWELVEDATA');

SELECT
  'max obs_date by sample feature' AS metric,
  feature_id::text || ' (' || fr.name || ') = ' || max(obs_date)::text AS value
FROM resolved_series rs
JOIN feature_registry fr ON fr.id = rs.feature_id
WHERE rs.feature_id IN (1696, 1769, 1688, 1767, 1737, 666)  -- AAPL/ABBV/AMZN/AVGO/BAC/CI
GROUP BY feature_id, fr.name
ORDER BY 2;
