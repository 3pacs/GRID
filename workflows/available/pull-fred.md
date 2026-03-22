---
name: pull-fred
group: ingestion
schedule: "daily 20:00 weekdays"
secrets: ["FRED_API_KEY"]
depends_on: []
description: Pull Federal Reserve Economic Data (rates, yields, macro indicators)
---

## Steps

1. Compute incremental start date from `raw_series` for source `FRED`
2. Instantiate FRED puller with API key from `{{SECRET:FRED_API_KEY}}`
3. Pull key series: DFF, DGS10, DGS2, T10Y2Y, CPIAUCSL, UNRATE, INDPRO, M2SL
4. Store in `raw_series` with pull_timestamp and source_id

## Output

- Updated `raw_series` rows for FRED source
- Coverage: fed funds rate, treasury yields, CPI, unemployment, IP, M2

## Notes

- FRED API allows 120 requests/minute per key
- Revisions are common for macro data — overlap_days=30 catches most
- CPI and employment data have ~2 week reporting lag
