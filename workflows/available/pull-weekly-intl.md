---
name: pull-weekly-intl
group: ingestion
schedule: "weekly sunday 03:00"
secrets: ["KOSIS_API_KEY"]
depends_on: []
description: Pull weekly international data (OECD, BIS, IMF, RBI, ABS, KOSIS, DBnomics)
---

## Steps

1. Compute incremental starts for each international source
2. Run pullers sequentially to respect rate limits:
   - OECDPuller → OECD SDMX API
   - BISPuller → BIS statistical warehouse
   - IMFPuller → IMF IFS database
   - RBIPuller → Reserve Bank of India
   - ABSPuller → Australian Bureau of Statistics
   - KOSISPuller → Korean Statistical Information Service
   - USDAPuller → USDA NASS agricultural data
   - DBnomicsPuller → DBnomics aggregator
3. Log results per puller

## Output

- International macro/trade data across 8 sources
- Covers: OECD leading indicators, BIS credit, IMF reserves, EM central banks

## Notes

- KOSIS requires API key
- USDA NASS requires API key
- Weekly cadence is sufficient for these low-frequency sources
