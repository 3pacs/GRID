---
name: pull-ecb
group: ingestion
schedule: "daily 20:00 weekdays"
secrets: []
depends_on: []
description: Pull ECB Statistical Data Warehouse series (exchange rates, monetary aggregates)
---

## Steps

1. Compute incremental start date from `raw_series` for source `ECB_SDW`
2. Instantiate `ECBPuller(db_engine)`
3. Call `puller.pull_all(start_date=incremental)` with 30-day overlap
4. Log pull status to `source_catalog`

## Output

- Rows inserted into `raw_series` with `source_id` for ECB_SDW
- Pull status: SUCCESS / PARTIAL / FAILED

## Notes

- ECB rate limit: 10 requests/second
- Overlap window: 30 days to catch revisions
- Covers: EUR exchange rates, M3 monetary aggregates, MFI interest rates
