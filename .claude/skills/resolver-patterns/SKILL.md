# resolver-patterns

Patterns for resolving raw_series into resolved_series in GRID. Covers the entity mapping system, multi-source conflict detection, and performance optimization.

## When to Use This Skill

- Adding new data sources or tickers
- Debugging why data isn't appearing in resolved_series
- Optimizing resolver performance for bulk loads
- Adding entity mappings (SEED_MAPPINGS)
- Understanding the PIT (point-in-time) data pipeline

## Data Pipeline

```
External API → raw_series → entity_map → feature_registry → resolved_series → panel_builder → signals
```

### raw_series
- Stores every data point from every source
- Key: (series_id, source_id, obs_date, pull_timestamp)
- series_id format: `{PREFIX}:{TICKER}:{field}` (e.g., `YF:SPY:close`)
- 120M+ rows

### entity_map (SEED_MAPPINGS)
- Maps raw series_id → feature_registry.name
- `"YF:SPY:close" → "spy_full"`
- Defined in `normalization/entity_map.py`
- V2 mappings in `NEW_MAPPINGS_V2` dict, merged at init

### feature_registry
- Canonical feature definitions
- Required fields: name, family, description, transformation, normalization, missing_data_policy, eligible_from_date
- Common families: equity, credit, commodity, vol, macro, sentiment, alternative

### resolved_series
- One canonical value per (feature_id, obs_date, vintage_date)
- Multi-source conflicts flagged with conflict_detail JSON
- Per-family conflict thresholds (equity: 1%, crypto: 3%, alt: 5%)

## Two Resolution Approaches

### 1. Direct SQL (preferred for bulk)
```sql
INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used, conflict_flag)
SELECT fr.id, rs.obs_date, rs.pull_timestamp::date, rs.pull_timestamp::date, rs.value, rs.source_id, FALSE
FROM raw_series rs
JOIN feature_registry fr ON fr.name = LOWER(SPLIT_PART(rs.series_id, ':', 2)) || '_full'
WHERE rs.series_id LIKE 'YF:%:close' AND rs.pull_status = 'SUCCESS'
ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING;
```
**Speed:** Seconds for 300K+ rows.

### 2. Python Resolver (for multi-source conflict detection)
```python
from normalization.resolver import Resolver
resolver = Resolver(engine)
result = resolver.resolve_pending(lookback_days=7, workers=8)
```
**Speed:** Minutes. Multithreaded, partitioned by series_id.

## Adding a New Ticker

1. Create feature_registry entry:
   ```sql
   INSERT INTO feature_registry (name, family, description, transformation, normalization, missing_data_policy, eligible_from_date)
   VALUES ('ticker_full', 'equity', 'Close TICKER', 'RAW', 'ZSCORE', 'FORWARD_FILL', '2020-01-01');
   ```

2. Add SEED_MAPPING in entity_map.py:
   ```python
   "YF:TICKER:close": "ticker_full",
   ```

3. Pull data via Tiingo or yfinance

4. Resolve via direct SQL

## Gotchas

- SQLAlchemy `text()` treats `:close` as a bind parameter — use psycopg2 for raw SQL
- The Python resolver's LIMIT/OFFSET approach is O(n²) on large tables — use direct SQL for bulk
- `entity_map.get_feature_id()` returns None for unmapped series (logged at DEBUG level)
- The `**{dict comprehension}` syntax works in SEED_MAPPINGS but watch for import-time evaluation
- `build_price_panel()` needs `drop_duplicates()` when multiple sources provide the same ticker
