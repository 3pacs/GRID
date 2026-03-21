---
name: pull-monthly-trade
group: ingestion
schedule: "monthly 2 04:00"
secrets: ["COMTRADE_API_KEY"]
depends_on: []
description: Pull monthly trade, physical, and satellite data (Comtrade, Eurostat, NOAA AIS, VIIRS)
---

## Steps

1. Run Comtrade puller for previous month's trade flows
2. Pull Eurostat macro indicators
3. Pull NOAA AIS shipping data (previous month summary)
4. Pull VIIRS nighttime lights data

## Output

- UN Comtrade bilateral trade flows
- Eurostat economic indicators
- Global shipping activity (AIS vessel counts)
- Nighttime light intensity (economic activity proxy)

## Notes

- Trade data typically has 2-month reporting lag
- VIIRS data is ~1 month delayed
- Comtrade API key required
