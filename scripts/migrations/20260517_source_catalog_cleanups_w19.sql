-- Source catalog cleanups week 19 (2026-05-17)
-- Tasks #181 (noaa_ais deprecate) + #183 (googletrends dedup)
--
-- Already applied to griddb on grid-svr 2026-05-17.
-- Replay-safe (idempotent via guards).

-- #181: NOAA_AIS upstream URL retired by NOAA 2026-04+ (301 -> HTML).
-- AIS coverage is now via ais_ground_truth (id 1122).
UPDATE source_catalog
   SET active = false
 WHERE id = 345
   AND LOWER(name) = 'noaa_ais';

-- #183: googletrends had duplicate rows (canonical id 119 'GoogleTrends',
-- orphan id 748 'google_trends'). 8688 raw_series rows on id 748
-- were FK-remapped to id 119 (zero unique-constraint collisions),
-- then the orphan row was deleted.
-- raw_series.source_id = 748 collisions vs 119:
--   SELECT COUNT(*) FROM raw_series a JOIN raw_series b
--     ON a.series_id=b.series_id AND a.obs_date=b.obs_date
--    AND a.pull_timestamp=b.pull_timestamp
--    WHERE a.source_id=748 AND b.source_id=119;  -- => 0
--
-- pull_log / resolved_series had 0 rows on FK 748.
BEGIN;
  UPDATE raw_series SET source_id = 119 WHERE source_id = 748;
  DELETE FROM source_catalog WHERE id = 748 AND name = 'google_trends';
COMMIT;

-- Verification (post-migration):
--   SELECT id,name,active FROM source_catalog WHERE LOWER(name)='googletrends';
--     -> one row, id=119, active=t
--   SELECT id,name,active FROM source_catalog WHERE id=345;
--     -> NOAA_AIS, active=f
--
-- UNIQUE(lower(name)) index 'source_catalog_lower_name_uniq' (from #179)
-- now prevents re-occurrence of either case-variant duplicate.
