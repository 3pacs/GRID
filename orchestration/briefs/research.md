# GRID Research Brief — For Any Model

You are researching a topic for GRID, a trading intelligence platform that
ingests data from 37+ global economic sources and uses unsupervised regime
discovery to generate macro trading signals.

## What GRID Already Knows About
- US macro: FRED (rates, spreads, employment, housing, ISM), BLS, EDGAR
- Markets: yfinance (equities, bonds, commodities, FX, volatility)
- International: ECB, BOJ, PBOC, RBI, BCB, MAS, ABS, KOSIS, Eurostat, OECD, BIS, IMF
- Trade: UN Comtrade, Atlas of Economic Complexity, WIOD, EU KLEMS
- Physical: VIIRS nighttime lights, USPTO patents, NOAA AIS shipping, USDA agriculture
- Alt data: GDELT geopolitical events, OFR financial stability, SEC velocity
- Regime states: expansion, contraction, stress, recovery (discovered, not predefined)

## Research Guidelines
- Provide specific, actionable findings (not general overviews)
- Include data source URLs with API documentation links
- Note data frequency (daily, weekly, monthly, quarterly, annual)
- Note data latency (real-time, T+1, T+30, etc.)
- Flag any cost/licensing restrictions
- Indicate which existing GRID source it could be cross-validated against

## Your Task
<!-- PASTE YOUR SPECIFIC RESEARCH QUESTION HERE -->
<!-- Example: "What leading indicators predicted the 2022 rate hiking cycle -->
<!-- 6+ months in advance? Which are available via free APIs?" -->

## Output Format
Return structured findings:
1. **Summary** (2-3 sentences)
2. **Data Sources** (name, URL, API availability, frequency, latency, cost)
3. **Signal Logic** (how to compute the indicator from raw data)
4. **Historical Evidence** (when has this worked/failed)
5. **Integration Notes** (which existing GRID module to extend)
