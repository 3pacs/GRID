-- vix_spot dates versus the SPY session calendar (resolved spy_full dates). Read-only.
WITH fv AS (SELECT id FROM feature_registry WHERE name = 'vix_spot'),
fs AS (SELECT id FROM feature_registry WHERE name = 'spy_full'),
fk AS (SELECT id FROM feature_registry WHERE name = 'skew_index'),
vd AS (SELECT DISTINCT obs_date FROM resolved_series rs JOIN fv ON fv.id = rs.feature_id),
sd AS (SELECT DISTINCT obs_date FROM resolved_series rs JOIN fs ON fs.id = rs.feature_id),
kd AS (SELECT DISTINCT obs_date FROM resolved_series rs JOIN fk ON fk.id = rs.feature_id),
lim AS (SELECT min(obs_date) AS s0, max(obs_date) AS s1 FROM sd)
SELECT 'A_vix_not_spy_by_year' AS part, extract(year FROM v.obs_date)::text AS a, count(*)::text AS b,
       string_agg(v.obs_date::text, ',' ORDER BY v.obs_date) FILTER (WHERE v.obs_date >= DATE '2020-01-01') AS c
FROM vd v, lim WHERE v.obs_date BETWEEN lim.s0 AND lim.s1 AND NOT EXISTS (SELECT 1 FROM sd WHERE sd.obs_date = v.obs_date)
GROUP BY extract(year FROM v.obs_date)
UNION ALL
SELECT 'B_spy_not_vix_by_year', extract(year FROM s.obs_date)::text, count(*)::text,
       string_agg(s.obs_date::text, ',' ORDER BY s.obs_date) FILTER (WHERE s.obs_date >= DATE '2020-01-01')
FROM sd s WHERE NOT EXISTS (SELECT 1 FROM vd WHERE vd.obs_date = s.obs_date)
GROUP BY extract(year FROM s.obs_date)
UNION ALL
SELECT 'C_vix_no_skew_class', CASE WHEN EXISTS (SELECT 1 FROM sd WHERE sd.obs_date = v.obs_date) THEN 'spy_session' ELSE 'no_spy_session' END,
       count(*)::text, string_agg(v.obs_date::text, ',' ORDER BY v.obs_date)
FROM vd v WHERE v.obs_date BETWEEN DATE '1990-01-02' AND DATE '2026-11-11'
  AND NOT EXISTS (SELECT 1 FROM kd WHERE kd.obs_date = v.obs_date)
GROUP BY 2
UNION ALL
SELECT 'D_spy_range', (SELECT s0::text FROM lim), (SELECT s1::text FROM lim), NULL
ORDER BY 1, 2;
