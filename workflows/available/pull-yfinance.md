---
name: pull-yfinance
group: ingestion
schedule: "daily 20:00 weekdays"
secrets: []
depends_on: []
description: Pull market prices via yfinance (equities, commodities, FX, volatility)
---

## Steps

1. Compute incremental start for yfinance source
2. Pull tickers: ^GSPC, ^VIX, ^VIX3M, GC=F, HG=F, CL=F, DX-Y.NYB
3. Extract OHLCV + adjusted close
4. Store as individual series in `raw_series`

## Output

- S&P 500, VIX, VIX3M, Gold, Copper, Crude Oil, Dollar Index
- Daily OHLCV data

## Notes

- yfinance is unofficial — may rate-limit or break without notice
- Commodity futures use front-month continuous contracts
- Weekend/holiday gaps are normal
