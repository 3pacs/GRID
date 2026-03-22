---
name: resolve-conflicts
group: resolution
schedule: "daily 21:00 weekdays"
secrets: []
depends_on: ["pull-fred", "pull-ecb", "pull-yfinance"]
description: Resolve multi-source conflicts and produce point-in-time correct resolved_series
---

## Steps

1. Query `raw_series` for new pulls since last resolution run
2. For each (feature_id, obs_date), apply source priority from `source_catalog.priority_rank`
3. Handle revision vintages: pick FIRST_RELEASE or LATEST_AS_OF per policy
4. Flag conflicts where sources disagree by >0.5%
5. Insert/update `resolved_series` with conflict tracking metadata

## Output

- Updated `resolved_series` rows with `source_priority_used` and `conflict_flag`
- PIT-correct values available for downstream feature computation

## Notes

- MUST run after all ingestion workflows complete
- Conflict resolution follows source trust_score ordering
- Resolution is idempotent — safe to re-run
