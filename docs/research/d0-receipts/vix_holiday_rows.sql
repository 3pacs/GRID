-- vix_spot holiday rows (D0 side finding). Read-only.
WITH f AS (SELECT id FROM feature_registry WHERE name = 'vix_spot'),
hol(d) AS (VALUES (DATE '2026-05-25'), (DATE '2026-06-19'), (DATE '2026-07-03'), (DATE '2026-09-07')),
skewf AS (SELECT id FROM feature_registry WHERE name = 'skew_index'),
rv AS (
  SELECT rs.obs_date, rs.vintage_date, rs.value, sc.name AS src, rs.conflict_flag
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id JOIN source_catalog sc ON sc.id = rs.source_priority_used
),
skew_dates AS (SELECT DISTINCT obs_date FROM resolved_series rs JOIN skewf ON skewf.id = rs.feature_id)
SELECT 'A_resolved_holiday' AS part, rv.obs_date::text AS a, rv.vintage_date::text AS b, rv.value::text AS c, rv.src AS d,
       (SELECT p.value::text || ' @' || p.obs_date::text FROM rv p WHERE p.obs_date < rv.obs_date ORDER BY p.obs_date DESC, p.vintage_date DESC LIMIT 1) AS e
FROM rv JOIN hol ON hol.d = rv.obs_date
UNION ALL
SELECT 'B_raw_holiday', r.obs_date::text, r.series_id || ' / ' || sc.name, r.pull_status,
       count(*)::text || ' rows; values ' || string_agg(DISTINCT r.value::text, ','),
       min(r.pull_timestamp)::text || ' .. ' || max(r.pull_timestamp)::text
FROM raw_series r JOIN source_catalog sc ON sc.id = r.source_id JOIN hol ON hol.d = r.obs_date
WHERE r.series_id IN ('VIXCLS', 'YF:^VIX:close', 'CBOE:VIX')
GROUP BY r.obs_date, r.series_id, sc.name, r.pull_status
UNION ALL
SELECT 'C_raw_prevday', r.obs_date::text, r.series_id, r.pull_status, string_agg(DISTINCT r.value::text, ','), NULL
FROM raw_series r JOIN hol ON r.obs_date = hol.d - (CASE WHEN extract(isodow FROM hol.d) = 1 THEN 3 ELSE 1 END)
WHERE r.series_id IN ('VIXCLS', 'YF:^VIX:close', 'CBOE:VIX') AND r.pull_status = 'SUCCESS'
GROUP BY r.obs_date, r.series_id, r.pull_status
UNION ALL
SELECT 'D_history_no_skew_by_src_dow', rv.src, extract(isodow FROM rv.obs_date)::text, count(DISTINCT rv.obs_date)::text,
       min(rv.obs_date)::text, max(rv.obs_date)::text
FROM rv
WHERE rv.obs_date BETWEEN DATE '1990-01-02' AND DATE '2026-11-11'
  AND NOT EXISTS (SELECT 1 FROM skew_dates s WHERE s.obs_date = rv.obs_date)
GROUP BY rv.src, extract(isodow FROM rv.obs_date)
UNION ALL
SELECT 'E_all_vix_by_src_weekend', rv.src, (extract(isodow FROM rv.obs_date) >= 6)::text, count(DISTINCT rv.obs_date)::text, min(rv.obs_date)::text, max(rv.obs_date)::text
FROM rv GROUP BY rv.src, (extract(isodow FROM rv.obs_date) >= 6)
ORDER BY 1, 2, 3;
