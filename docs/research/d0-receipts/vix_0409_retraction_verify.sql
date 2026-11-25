-- Read-only check after the 2026-11-13 retraction of the 2026-04-09 yfinance vix_spot vintage.
WITH f AS (SELECT id FROM feature_registry WHERE name = 'vix_spot'),
tagged AS (
  SELECT x.* FROM resolved_series_retractions x WHERE x.run_tag = 'vix-spot-yf-20260409'
),
live AS (
  SELECT DISTINCT ON (rs.obs_date) rs.obs_date, rs.vintage_date
  FROM resolved_series rs
  WHERE rs.feature_id = (SELECT id FROM f)
    AND NOT EXISTS (SELECT 1 FROM resolved_series_retractions x
                    WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date
                      AND x.vintage_date = rs.vintage_date)
  ORDER BY rs.obs_date, rs.vintage_date DESC
)
SELECT
  (SELECT count(*) FROM tagged) AS tagged_rows,
  (SELECT count(*) FROM tagged WHERE feature_id = (SELECT id FROM f) AND vintage_date = DATE '2026-04-09') AS tagged_in_target,
  (SELECT count(DISTINCT obs_date) FROM tagged) AS tagged_distinct_obs,
  (SELECT min(obs_date)::text FROM tagged) AS tagged_min_obs,
  (SELECT max(obs_date)::text FROM tagged) AS tagged_max_obs,
  (SELECT min(retracted_at)::text FROM tagged) AS retracted_at_min,
  (SELECT max(retracted_at)::text FROM tagged) AS retracted_at_max,
  (SELECT count(DISTINCT reason) FROM tagged) AS distinct_reasons,
  (SELECT count(*) FROM resolved_series_retractions WHERE feature_id = (SELECT id FROM f)) AS vix_spot_retractions_total,
  (SELECT count(*) FROM resolved_series rs WHERE rs.feature_id = (SELECT id FROM f) AND rs.vintage_date = DATE '2026-04-09') AS vintage_rows,
  (SELECT count(*) FROM resolved_series rs WHERE rs.feature_id = (SELECT id FROM f) AND rs.vintage_date = DATE '2026-04-09'
     AND NOT EXISTS (SELECT 1 FROM resolved_series_retractions x WHERE x.feature_id = rs.feature_id
                     AND x.obs_date = rs.obs_date AND x.vintage_date = rs.vintage_date)) AS vintage_rows_unretracted,
  (SELECT count(*) FROM live WHERE vintage_date = DATE '2026-04-09') AS served_from_vintage,
  (SELECT count(*) FROM live) AS live_obs_dates;
