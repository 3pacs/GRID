-- Rows of the 2026-04-09 yfinance vix_spot vintage that are the first release of their obs_date. Read-only.
WITH f AS (SELECT id FROM feature_registry WHERE name = 'vix_spot'),
v AS (
  SELECT rs.obs_date, rs.value,
         (SELECT min(o.vintage_date) FROM resolved_series o WHERE o.feature_id = rs.feature_id AND o.obs_date = rs.obs_date) AS first_vintage,
         (SELECT o.value FROM resolved_series o WHERE o.feature_id = rs.feature_id AND o.obs_date = rs.obs_date
           AND o.vintage_date < rs.vintage_date ORDER BY o.vintage_date DESC LIMIT 1) AS prior_value
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id
  WHERE rs.vintage_date = DATE '2026-04-09'
)
SELECT count(*)::text AS total,
       count(*) FILTER (WHERE first_vintage = DATE '2026-04-09')::text AS first_release,
       string_agg(obs_date::text || '=' || round(value::numeric, 2)::text, ',') FILTER (WHERE first_vintage = DATE '2026-04-09') AS first_release_rows,
       count(*) FILTER (WHERE prior_value IS NOT NULL AND abs(value / NULLIF(prior_value, 0) - 1) > 0.05)::text AS off_vs_prior_vintage
FROM v;
