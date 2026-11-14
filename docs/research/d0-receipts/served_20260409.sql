-- Where the latest live (non-retracted) vintage of spy_full / vix_spot is the 2026-04-09 yfinance row. Read-only.
WITH f AS (SELECT id, name FROM feature_registry WHERE name IN ('spy_full', 'vix_spot')),
live AS (
  SELECT DISTINCT ON (rs.feature_id, rs.obs_date) f.name, rs.feature_id, rs.obs_date, rs.vintage_date, rs.value, rs.source_priority_used
  FROM resolved_series rs JOIN f ON f.id = rs.feature_id
  WHERE NOT EXISTS (SELECT 1 FROM resolved_series_retractions x
                     WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date AND x.vintage_date = rs.vintage_date)
  ORDER BY rs.feature_id, rs.obs_date, rs.vintage_date DESC
),
hit AS (
  SELECT l.*, (SELECT o.value FROM resolved_series o
                WHERE o.feature_id = l.feature_id AND o.obs_date = l.obs_date AND o.vintage_date <> l.vintage_date
                ORDER BY o.vintage_date DESC LIMIT 1) AS other_value
  FROM live l
  WHERE l.vintage_date = DATE '2026-04-09'
    AND l.source_priority_used = (SELECT id FROM source_catalog WHERE name = 'yfinance')
)
SELECT name AS part, count(*)::text AS a,
       count(*) FILTER (WHERE other_value IS NULL)::text AS b,
       count(*) FILTER (WHERE other_value IS NOT NULL AND abs(value / NULLIF(other_value, 0) - 1) > 0.05)::text AS c,
       string_agg(obs_date::text || '=' || round(value::numeric, 2)::text || COALESCE('/' || round(other_value::numeric, 2)::text, ''), ',' ORDER BY obs_date) AS d
FROM hit GROUP BY name
ORDER BY 1;
