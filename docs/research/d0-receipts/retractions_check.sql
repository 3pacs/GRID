-- Retractions on vix_spot / skew_index / spy_full, and spy_full live rows on 2025-01-09 and 2025-07-04. Read-only.
WITH f AS (SELECT id, name FROM feature_registry WHERE name IN ('spy_full', 'vix_spot', 'skew_index'))
SELECT 'retractions' AS part, f.name AS a, count(x.*)::text AS b, string_agg(DISTINCT x.run_tag, ',') AS c
FROM f LEFT JOIN resolved_series_retractions x ON x.feature_id = f.id GROUP BY f.name
UNION ALL
SELECT 'spy_live_closed_day', rs.obs_date::text, count(*)::text, NULL
FROM resolved_series rs JOIN f ON f.id = rs.feature_id AND f.name = 'spy_full'
WHERE rs.obs_date IN (DATE '2025-01-09', DATE '2025-07-04', DATE '2004-06-11')
  AND NOT EXISTS (SELECT 1 FROM resolved_series_retractions x
                   WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date AND x.vintage_date = rs.vintage_date)
GROUP BY rs.obs_date
UNION ALL
SELECT 'vix_bad_0409_window', 'next vintage after 2026-04-09', min(nv)::text, max(nv)::text
FROM (SELECT (SELECT min(o.vintage_date) FROM resolved_series o
               WHERE o.feature_id = rs.feature_id AND o.obs_date = rs.obs_date AND o.vintage_date > rs.vintage_date) AS nv
      FROM resolved_series rs JOIN f ON f.id = rs.feature_id AND f.name = 'vix_spot'
      WHERE rs.vintage_date = DATE '2026-04-09') s
ORDER BY 1, 2;
