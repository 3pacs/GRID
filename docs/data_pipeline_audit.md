# GRID Data Pipeline Audit — 2026-04-07

## Summary

- **439M [[Raw Series Table|raw_series]] rows** across 135 [[Source Catalog Table|source_catalog]] entries
- **67 active sources**, 14 of which have **zero data**
- **1,342 features** registered, 1,188 model-eligible
- **2.2M [[Resolved Series Table|resolved_series]] rows** (features with PIT [[Conflict Resolution|conflict resolution]])
- **17 thesis scorer models**, all active
- **3,438 news_articles**, **50K+ signal_sources** entries

## BROKEN: Active Sources with Zero Data (14 sources)

These are marked `active = true` in [[Source Catalog Table|source_catalog]] but have NO SUCCESS rows in [[Raw Series Table|raw_series]].

| Source | Likely Issue | Fix Priority |
|--------|-------------|--------------|
| AAII_Sentiment | Puller not writing to raw_series | HIGH — sentiment is thesis-relevant |
| Cloudflare_Radar | Puller likely not scheduled | LOW |
| Discord_Solana_Scanner | Channel-specific, may need token | LOW |
| fmp | Financial Modeling Prep — API key needed? | MEDIUM |
| FOIA_CABLES | Data may be in different table | LOW |
| hf_financial_news | HuggingFace news — puller broken? | MEDIUM |
| Kalshi | Prediction market — API changed? | HIGH — prediction markets are gold |
| MarketWatch_News | Puller not inserting | MEDIUM |
| NOAA_AIS | Ship tracking — complex data | LOW |
| nowcast | GDP nowcast — should be flowing | HIGH |
| OpenCorporates | Corporate registry data | LOW |
| opensecrets | Lobbying data — already in QQ? | LOW |
| polygon | Market data — API key? | MEDIUM |
| SEC_EDGAR_Fundamentals | Likely writing to different table | HIGH |
| Telegram_Solana_Scanner | Channel token issue | LOW |
| USDA_NASS | Agricultural data | LOW |
| USPTO_PV | Patent data — Crucix covers this | LOW |
| VIIRS | Satellite data — Crucix covers this | LOW |

## GAP: Data Flowing but NOT Scored by Thesis

Data in raw_series that no thesis model touches:

| Data Source | Rows | What It Has | Why It Matters |
|------------|------|-------------|----------------|
| GDELT | 22K+ | Global event data, conflict counts | Geopolitical risk model uses news_articles, not GDELT raw |
| Supply_Chain | ? | Supply chain stress signals | Has a model, but check if it's reading the right series |
| INSTITUTIONAL_FLOWS | ? | ETF flows, fund flows | Capital flows model may not be reading all of this |
| Redfin | 12.9K | Housing market data | No housing model exists |
| EIA | ? | Energy inventory, production | No energy fundamentals model |
| Analyst_Ratings | ? | Consensus estimates, upgrades/downgrades | No analyst sentiment model |
| FINRA_MARGIN | ? | Margin debt levels | No leverage/margin model |
| Fear_Greed | ? | CNN Fear & Greed index | Should feed into sentiment |
| STOCKTWITS | ? | Social mentions, bullish/bearish | Social model may not read this |
| GoogleTrends | ? | Search interest spikes | No search trend model |
| Kalshi/Polymarket | 0 | Prediction market odds | No prediction market model (BLOCKED — no data) |

## GAP: Thesis Scorer Series Coverage

The 17 models query these series from raw_series:
- `WALCL`, `RRPONTSYD`, `WTREGEN` — Fed balance sheet (OK)
- `FEDFUNDS` — Fed funds rate (OK)
- `fed_tone_7d_avg`, `fomc_*` — Fed speeches (OK)
- `cftc.*` — COT positioning (FIXED this session)
- `TIINGO_FUND:*:pe_ratio` — Valuations (OK)
- `coingecko:bitcoin:usd`, `YF:BTC-USD:close`, `binance:BTCUSDT:close` — Crypto (OK)

**NOT queried by any model:**
- All Crucix signals (71 series)
- [[GDELT]] events
- Options flow data (12.9K signal_sources entries)
- QuiverQuant data (34K signal_sources entries)
- News article sentiment (3.4K articles)
- All alternative data (47 features)
- Most macro indicators from [[FRED]] (only 3 of 20+ FRED series used)

## GAP: Feature Registry → Model Pipeline

- 1,342 features registered, 1,188 model-eligible
- 2.2M resolved rows
- But the thesis scorer does NOT use the [[Feature Registry Table|feature_registry]] or [[Resolved Series Table|resolved_series]] AT ALL
- It queries raw_series directly
- The entire ML feature pipeline (features → resolution → model training) is separate from the thesis scorer

## Confidence Issues

Current thesis model confidence ranges: 50-85%. To get to 98%:
1. More data sources per model (cross-validation)
2. Historical accuracy tracking (model_accuracies table needs more snapshots)
3. Fresher data (some sources update weekly, not daily)
4. Ensemble methods (multiple independent signals per model)

## Action Items

### CRITICAL — Fix broken high-value sources
1. Kalshi prediction markets (was working, now 0 data)
2. AAII Sentiment (investor sentiment survey)
3. nowcast (GDP nowcasting)
4. SEC_EDGAR_Fundamentals (may be writing to wrong table)

### HIGH — Wire existing data into thesis scorer
5. Add Fear_Greed model (CNN index → contrarian signal)
6. Add Analyst_Ratings model (consensus upgrades/downgrades)  
7. Add FINRA_MARGIN model (margin debt → leverage risk)
8. Wire Crucix ACLED/OpenSky into geopolitical_risk model
9. Wire GoogleTrends spikes into social_sentiment model

### MEDIUM — Deepen existing models
10. Fed model should also read [[FRED]] series (DGS10, T10Y2Y, UNRATE)
11. Geopolitical model should read [[GDELT]] directly, not just news
12. Capital flows model should read INSTITUTIONAL_FLOWS
13. Add housing/Redfin model for macro context
