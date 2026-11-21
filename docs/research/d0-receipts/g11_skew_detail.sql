-- G11 SKEW detail receipt (#1142). Read-only. First-vintage lag and VIX dates lacking SKEW since 2026-03-26.
WITH f AS (
  SELECT id, name FROM feature_registry WHERE name IN ('skew_index', 'vix_spot')
), first_v AS (
  SELECT rs.obs_date, min(rs.vintage_date) AS first_vintage
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id AND f.name = 'skew_index'
  WHERE rs.obs_date >= DATE '2026-03-26'
  GROUP BY rs.obs_date
), vixd AS (
  SELECT DISTINCT rs.obs_date
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id AND f.name = 'vix_spot'
  WHERE rs.obs_date >= DATE '2026-03-26'
)
SELECT 'lag_hist' AS part, (first_vintage - obs_date)::text AS k, count(*)::text AS n
FROM first_v GROUP BY first_vintage - obs_date
UNION ALL
SELECT 'vix_date_missing_skew', v.obs_date::text, NULL
FROM vixd v WHERE v.obs_date <= (SELECT max(obs_date) FROM first_v)
  AND NOT EXISTS (SELECT 1 FROM first_v s WHERE s.obs_date = v.obs_date)
UNION ALL
SELECT 'skew_date_not_vix', s.obs_date::text, NULL
FROM first_v s WHERE NOT EXISTS (SELECT 1 FROM vixd v WHERE v.obs_date = s.obs_date)
ORDER BY 1, 2;
