# State

## Current Position

Phase: 14 — Oracle & Data Completion
Plan: ROADMAP.md (16 phases planned)
Status: Phases 1–12.5 COMPLETE. Phase 13 (AstroGrid) IN PROGRESS. Phase 14 started this session.
Last activity: 2026-03-26 — Oracle engine built, 2M raw rows ingested, viz intelligence engine, 100x digest with supervised sanity checks, options puller near-expiry fix.

## Accumulated Context

### What shipped this session (2026-03-26)

#### Oracle Prediction Engine (ORACLE-01 through ORACLE-05)
- oracle/engine.py: 5 competing models (flow_momentum, regime_contrarian, options_flow, cross_asset, news_energy)
- Self-improving loop: score expired predictions → evolve weights → generate new → report
- Signal + anti-signal architecture: every prediction shows confirming AND contradicting evidence
- 615 predictions generated in first cycle (41 tickers × 5 models, expiry Apr 17)
- oracle_predictions, oracle_models, oracle_iterations tables
- oracle/report.py: Rich email with scorecard, model tournament, signal/anti-signal breakdown
- Wired into hermes_operator (6h cycle)

#### 100x Digest with Supervised Intelligence
- alerts/hundredx_digest.py: 3-layer filter pipeline
  1. Sanity check (data ranges: IV 3-250%, PCR < 20, max pain within 30% of spot, score differentiation)
  2. LLM review (Hermes evaluates each opportunity, PASS/FAIL/SUSPECT)
  3. Cross-verification (live yfinance chain check, spot price, recommended strikes)
- Killed individual email spam — only bundled 4h digest
- First run: correctly rejected all 18 garbage signals from near-expiry chain data
- Wired into hermes_operator (4h cycle)

#### Options Puller Fix
- ingestion/options.py: Skip expiries within 2 DTE (near-worthless chains had garbage data)
- Scanner quality filter: requires total_oi >= 1000 AND iv_atm >= 3%
- Clean data shows real signals: IWM CALL 3.5, SPY CALL 3.37 (modest, correct for calm market)

#### Visualization Intelligence Engine
- analysis/viz_intelligence.py: 11 learned rules mapping data patterns → optimal chart types
- VizSpec protocol: complete rendering instruction for any living graph
- Weight schedules: data sources "breathe" at natural cadence (real-time equity pulses, monthly macro is slow heartbeat)
- api/routers/viz.py: /recommend, /rules, /weights, pre-built specs for 6 chart types
- pwa/src/components/LivingGraph.jsx: Universal renderer (PhaseSpace, ForceNetwork, Orbital, TimeScrubber)

#### Bulk Historical Data Pull (2M+ rows)
- CBOE: VIX (35yr), VIX3M (17yr), VIX9D (15yr), SKEW (35yr) — 26,240 rows
- Binance: BTC/ETH/SOL/TAO daily klines (2021→2024) — 9,008 rows
- CoinGecko: 4 coins × 365 days — 2,920 rows
- DeFiLlama: Solana DEX volume (1,634 days) + TVL (1,835 days)
- Open-Meteo: 5 cities × HDD/CDD × 5yr — 18,260 rows
- EIA: 9 energy series (2016→2026) — 1,080 rows
- WorldNews: 33 features × 7 days — 198 rows
- Entity mappings added for 80+ new series IDs

#### Infrastructure Created
- ingestion/web_scraper.py — Multi-source web scraper with trust rankings + scrape_audit table
- scripts/fill_missing_features.py — Direct API bulk puller (FRED, yfinance, EIA, weather, analyst, OFR, GDELT, stablecoins, computed)
- scripts/bulk_historical_pull.py — ZIP/CSV bulk downloader (CBOE, Binance, CoinGecko, options, DeFi, Polymarket)
- scripts/scrape_missing_features.py — Original web scraper (DDG blocked, superseded by direct API approach)

### Known Issues
- WorldNews API key expired (402) — wn_* features in raw_series but not resolving
- FRED fedfred library returns dates in value column — parse fix needed
- Analyst ratings yfinance int64 serialization — needs numpy int conversion
- OFR Financial Stress API endpoint returns 400 — URL format changed
- 124 features still at zero (see breakdown below)
- Options daily signals for 2026-03-26 are garbage (near-expiry chain) — fix applied, will correct on next pull
- Oracle confidence normalization too generous (everything at 95%) — needs calibration after first scoring cycle
- Resolver not mapping all new series IDs to features — entity_map needs WN:* prefix mappings

### Data State
- Features at zero: 124 (down from 159)
- Total resolved_series: 328,294
- Raw series added today: 2,087,768
- Oracle predictions: 615 (expiry Apr 17)
- Coverage: equity 100%, earnings 100%, vol 99%, breadth 94%, crypto 89%, commodity 86%, rates 86%, sentiment 85%, credit 81%, fx 75%, macro 57%, alternative 0%, systemic 0%, trade 0%

### Disk usage
- /data drive: ~50GB of 11TB used (~1%)
