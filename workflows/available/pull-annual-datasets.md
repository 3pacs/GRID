---
name: pull-annual-datasets
group: ingestion
schedule: "annual january 15 04:30"
secrets: []
depends_on: []
description: Pull annual structural datasets (Atlas ECI, WIOD, EU KLEMS, Patents)
---

## Steps

1. Pull Atlas of Economic Complexity — ECI scores by country
2. Pull World Input-Output Database tables
3. Pull EU KLEMS productivity data
4. Pull USPTO patent filing counts by technology class

## Output

- Country economic complexity indices
- Inter-industry flow matrices
- Sector-level productivity and capital data
- Technology innovation indicators

## Notes

- These datasets update once per year with long lags (6-18 months)
- Used for structural regime context, not tactical signals
- January 15 allows time for prior-year data to be published
