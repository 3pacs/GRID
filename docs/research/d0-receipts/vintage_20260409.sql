-- The 2026-04-09 yfinance vintage in spy_full and vix_spot: size, deviation from other sources, retraction state. Read-only.
WITH f AS (SELECT id, name FROM feature_registry WHERE name IN ('spy_full', 'vix_spot')),
yf AS (SELECT id FROM source_catalog WHERE name = 'yfinance'),
v AS (
  SELECT f.name, rs.feature_id, rs.obs_date, rs.vintage_date, rs.value,
         (SELECT o.value FROM resolved_series o
           WHERE o.feature_id = rs.feature_id AND o.obs_date = rs.obs_date AND o.vintage_date <> rs.vintage_date
           ORDER BY o.vintage_date LIMIT 1) AS ref_value,
         EXISTS (SELECT 1 FROM resolved_series_retractions x
                  WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date AND x.vintage_date = rs.vintage_date) AS retracted,
         (SELECT string_agg(DISTINCT x.run_tag, ',') FROM resolved_series_retractions x
                  WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date AND x.vintage_date = rs.vintage_date) AS tag
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id
  WHERE rs.vintage_date = DATE '2026-04-09' AND rs.source_priority_used = (SELECT id FROM yf)
)
SELECT name AS part,
       count(*)::text AS a,
       count(*) FILTER (WHERE ref_value IS NOT NULL AND abs(value / NULLIF(ref_value, 0) - 1) > 0.05)::text AS b,
       count(*) FILTER (WHERE retracted)::text AS c,
       count(*) FILTER (WHERE ref_value IS NOT NULL AND abs(value / NULLIF(ref_value, 0) - 1) > 0.05 AND NOT retracted)::text AS d,
       min(obs_date)::text || ' .. ' || max(obs_date)::text AS e,
       string_agg(DISTINCT tag, ',') AS g,
       count(*) FILTER (WHERE ref_value IS NULL)::text AS h
FROM v GROUP BY name
UNION ALL
SELECT name || ' bad_unretracted_sample', obs_date::text, round(value::numeric, 2)::text, round(ref_value::numeric, 2)::text, NULL, NULL, NULL, NULL
FROM (SELECT * FROM v WHERE NOT retracted AND (ref_value IS NULL OR abs(value / NULLIF(ref_value, 0) - 1) > 0.05)
      ORDER BY name, obs_date DESC LIMIT 12) s
ORDER BY 1, 2;
