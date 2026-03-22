---
name: pull-bls
group: ingestion
schedule: "daily 20:00 weekdays"
secrets: ["BLS_API_KEY"]
depends_on: []
description: Pull Bureau of Labor Statistics data (employment, wages, PPI)
---

## Steps

1. Compute incremental start date for BLS source
2. Pull series: CES0000000001 (nonfarm payrolls), LNS14000000 (unemployment rate),
   CES0500000003 (avg hourly earnings), WPUFD49104 (PPI final demand)
3. Handle BLS API pagination (50-series limit per request)
4. Store in `raw_series`

## Output

- Employment, wage, and producer price data
- Monthly frequency with ~1 month reporting lag

## Notes

- BLS API v2 requires registration key for higher rate limits
- Data is subject to revisions for 2+ months after initial release
- PIT store handles vintage tracking automatically
