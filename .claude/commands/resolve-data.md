# Resolve Data

Resolve raw_series into resolved_series. Use direct SQL for bulk, Python resolver for incremental.

## Arguments

- `bulk` — direct SQL resolution for all YF:TICKER:close data (fast, seconds)
- `incremental` — Python resolver for recent pulls (slow, minutes)
- `status` — show current resolution coverage

## Instructions

### Bulk Resolution (preferred for backfills)

Use raw psycopg2 to bypass SQLAlchemy colon-escaping issues:

```python
import psycopg2
conn = psycopg2.connect("dbname=griddb user=grid password=gridmaster2026 host=localhost")
cur = conn.cursor()
cur.execute("""
    INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used, conflict_flag)
    SELECT fr.id, rs.obs_date, rs.pull_timestamp::date, rs.pull_timestamp::date, rs.value, rs.source_id, FALSE
    FROM raw_series rs
    JOIN feature_registry fr ON fr.name = LOWER(SPLIT_PART(rs.series_id, ':', 2)) || '_full'
    WHERE rs.series_id LIKE 'YF:%%:close' AND rs.pull_status = 'SUCCESS' AND rs.obs_date >= '2021-01-01'
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")
print(f"Inserted: {cur.rowcount:,}")
conn.commit()
```

### Incremental (Python resolver)
```python
from normalization.resolver import Resolver
from db import get_engine
resolver = Resolver(get_engine())
result = resolver.resolve_pending(lookback_days=7, workers=8)
```

### Status Check
```sql
SELECT COUNT(*) FROM resolved_series;
SELECT COUNT(DISTINCT fr.name) FROM feature_registry fr
JOIN resolved_series rs ON fr.id = rs.feature_id
WHERE fr.name LIKE '%_full' AND rs.obs_date >= '2024-01-01';
```

## Important Notes

- Direct SQL is 1000x faster than the Python resolver for bulk loads
- The Python resolver handles multi-source conflict detection (SEED_MAPPINGS)
- Always create feature_registry entries BEFORE resolving (needs name, family, description, transformation, normalization, missing_data_policy, eligible_from_date)
- SQLAlchemy `text()` treats `:close` as a bind parameter — use psycopg2 for raw SQL with colons
- The resolver scans raw_series (120M+ rows) — use `lookback_days` to limit scope
