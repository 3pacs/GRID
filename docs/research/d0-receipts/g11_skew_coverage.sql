-- G11 SKEW coverage receipt (#1142). Read-only. Calendar reference = resolved vix_spot obs_dates.
WITH f AS (
  SELECT id, name FROM feature_registry WHERE name IN ('skew_index', 'vix_spot')
), skew AS (
  SELECT rs.obs_date, rs.vintage_date, rs.value
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id AND f.name = 'skew_index'
), vixd AS (
  SELECT DISTINCT rs.obs_date
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id AND f.name = 'vix_spot'
), sd AS (SELECT DISTINCT obs_date FROM skew)
SELECT 'resolved' AS part,
       (SELECT min(obs_date) FROM skew)::text AS start_date,
       (SELECT max(obs_date) FROM skew)::text AS end_date,
       (SELECT count(*) FROM sd) AS distinct_obs_dates,
       (SELECT count(*) FROM skew) AS vintage_rows,
       (SELECT count(*) FROM skew WHERE vintage_date < obs_date) AS vintage_before_obs,
       (SELECT count(*) FROM (SELECT obs_date FROM skew GROUP BY obs_date HAVING count(DISTINCT value) > 1) c) AS dates_with_conflicting_values,
       (SELECT count(*) FROM vixd v WHERE v.obs_date BETWEEN (SELECT min(obs_date) FROM skew) AND (SELECT max(obs_date) FROM skew)
                                  AND NOT EXISTS (SELECT 1 FROM sd WHERE sd.obs_date = v.obs_date)) AS vix_dates_missing_skew,
       (SELECT count(*) FROM vixd v WHERE v.obs_date >= DATE '2026-03-26' AND v.obs_date <= (SELECT max(obs_date) FROM skew)
                                  AND NOT EXISTS (SELECT 1 FROM sd WHERE sd.obs_date = v.obs_date)) AS vix_dates_missing_skew_since_20260326,
       (SELECT max(vintage_date - obs_date) FROM skew WHERE obs_date >= DATE '2026-03-26') AS max_vintage_lag_days_since_20260326
UNION ALL
SELECT 'raw:' || rs.series_id, min(rs.obs_date)::text, max(rs.obs_date)::text,
       count(DISTINCT rs.obs_date), count(*), NULL, NULL, NULL, NULL, NULL
FROM raw_series rs
WHERE rs.series_id IN ('CBOE:SKEW', 'skew_index') AND rs.pull_status = 'SUCCESS'
GROUP BY rs.series_id
ORDER BY 1;
