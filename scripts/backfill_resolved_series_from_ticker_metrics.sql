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

SET statement_timeout = 300000;

-- 1) Ensure TWELVEDATA source exists in source_catalog.
INSERT INTO source_catalog
  (name, base_url, cost_tier, latency_class, pit_available, revision_behavior,
   trust_score, priority_rank, active)
SELECT 'TWELVEDATA', 'https://api.twelvedata.com', 'PAID', 'EOD', false, 'RARE',
       'HIGH', 60, true
WHERE NOT EXISTS (SELECT 1 FROM source_catalog WHERE name = 'TWELVEDATA');

-- 2) Backfill resolved_series from ticker_metrics_daily for every
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
  (SELECT id FROM source_catalog WHERE name = 'TWELVEDATA') AS source_priority_used,
  false AS conflict_flag,
  1 AS resolution_version
FROM ticker_metrics_daily tmd
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
