# State

## Current Position

Phase: 14 — Oracle, Intelligence Layer & Options Edge
Plan: ROADMAP.md (16 phases planned)
Status: Phases 1–12.5 COMPLETE. Phase 13 (AstroGrid) IN PROGRESS. Phase 14 major progress across two sessions.
Last activity: 2026-03-27 — Intelligence layer, options edge, altdata ingestion, frontend views.

## Accumulated Context

### What shipped 2026-03-27 (Intelligence + Options Edge night)

#### Intelligence Layer (NEW — `intelligence/`)
- `trust_scorer.py` — Bayesian trust scoring for all signal sources, 90-day recency half-life
- `lever_pullers.py` — identifies and tracks market-moving actors (Fed governors, fund managers, insiders)
- `actor_network.py` — 100+ named actors with wealth flow tracking and influence mapping
- `cross_reference.py` — government stats vs physical reality "lie detector" (CPI vs grocery prices, jobs vs UI claims)
- `source_audit.py` — source accuracy comparison + redundancy mapping across all data feeds
- `postmortem.py` — automated failure analysis for every bad trade, feeds back into trust scores

#### Options Edge (`trading/` + `discovery/`)
- `trading/options_recommender.py` — generates specific trade recommendations:
  - Ticker, direction (CALL/PUT), optimized strike, expiry (charm-based), entry price
  - Target price (GEX-derived expected move), stop loss (gamma flip / wall levels)
  - Expected return, max risk, Kelly fraction for position sizing
  - Strategy types: naked calls/puts, verticals, straddles
- `trading/options_tracker.py` — outcome tracking + self-improving scanner weights
  - Logs every recommendation with full context at generation time
  - Scores each signal source's contribution to winners vs losers
  - Feeds scores back into scanner weights (auto-researcher loop)
- `discovery/options_scanner.py` — upgraded with LLM sanity check layer
- `physics/dealer_gamma.py` — GEX profile, vanna, charm, gamma walls (already existed, now wired to recommender)

#### Alternative Data Ingestion (9 new modules in `ingestion/altdata/`)
- `congressional.py` — congressional trading disclosures (EDGAR/Quiver Quant)
- `insider_filings.py` — SEC Form 4 with cluster buy detection
- `dark_pool.py` — FINRA dark pool weekly data
- `unusual_whales.py` — whale options flow detection (>$1M premium)
- `prediction_odds.py` — Polymarket rapid probability shifts
- `smart_money.py` — Reddit + Finviz trust-scored social signals
- `supply_chain.py` — shipping rates, container index, ISM
- `fed_liquidity.py` — Fed net liquidity equation
- `institutional_flows.py` — ETF flows + SEC 13F holdings

#### Frontend Views (new + expanded)
- MoneyFlow — global money flow D3 Sankey visualization (Central Banks → Markets → Sectors)
- CrossReference — government stats vs physical reality lie detector dashboard
- Predictions — oracle scoreboard + calibration chart
- ActorNetwork — D3 force graph of financial power structure (building)
- IntelDashboard — unified intelligence command center (building)
- TrendTracker — momentum, regime, rotation, vol, liquidity trends (building)

### What shipped 2026-03-26 (Oracle + Data night)

#### Oracle Prediction Engine (ORACLE-01 through ORACLE-05)
- oracle/engine.py: 5 competing models (flow_momentum, regime_contrarian, options_flow, cross_asset, news_energy)
- Self-improving loop: score expired predictions → evolve weights → generate new → report
- Signal + anti-signal architecture: every prediction shows confirming AND contradicting evidence
- 615 predictions generated in first cycle (41 tickers × 5 models, expiry Apr 17)
- oracle/report.py: Rich email with scorecard, model tournament, signal/anti-signal breakdown
- Wired into hermes_operator (6h cycle)

#### 100x Digest with Supervised Intelligence
- alerts/hundredx_digest.py: 3-layer filter pipeline (sanity → LLM review → cross-verify)
- Killed individual email spam — only bundled 4h digest

#### Options Puller Fix
- ingestion/options.py: Skip expiries within 2 DTE, quality gate on scanner

#### Visualization Intelligence Engine
- analysis/viz_intelligence.py: 11 learned rules, VizSpec protocol, LivingGraph renderer

#### Bulk Historical Data Pull (2M+ rows)
- CBOE VIX/SKEW 35yr, Binance 5yr crypto, DeFiLlama, Open-Meteo 5yr, EIA
- 2M+ raw rows ingested, 328K resolved

### Current Data Coverage

| Family | Coverage | Notes |
|--------|----------|-------|
| Equity | 100% | 43+ tickers |
| Earnings | 100% | |
| Volatility | 99% | CBOE 35yr bulk loaded |
| Breadth | 94% | |
| Crypto | 89% | BTC/ETH/SOL/TAO + DeFi |
| Commodity | 86% | |
| Rates | 86% | |
| Sentiment | 85% | Social + FinBERT |
| Credit | 81% | |
| FX | 75% | Needs intl pullers |
| Macro | 57% | FRED loaded, intl gaps |
| Alternative | ~20% | 9 altdata modules built, wiring in progress |
| Systemic | 0% | OFR endpoint broken |
| Trade | 0% | Comtrade API key needed |

- Total resolved_series: 328,294+
- Raw series: 2,087,768+
- Oracle predictions: 615 (expiry Apr 17)
- Features at zero: ~124 (down from 159)

### Known Issues

- WorldNews API key expired (402) — wn_* features not resolving
- FRED fedfred library returns dates in value column — parse fix needed
- Analyst ratings yfinance int64 serialization — needs numpy int conversion
- OFR Financial Stress API endpoint returns 400 — URL format changed
- Oracle confidence normalization too generous (everything at 95%) — needs calibration after first scoring cycle
- Resolver not mapping all new series IDs to features — entity_map needs WN:* prefix mappings
- Missing API keys: KOSIS, COMTRADE, USDA_NASS, GDELT
- Eurostat pulling 0/3 series — endpoint change investigation needed

### What's Building (next priorities)

1. **Watchlist as primary UI** — ticker search, batch price refresh, interactive charts (see GSD-PLAN.md)
2. **Wire altdata modules to scheduler** — 9 modules built, need scheduler registration + entity_map entries
3. **ActorNetwork frontend** — D3 force graph rendering actor_network.py data
4. **IntelDashboard frontend** — unified view of trust scores, cross-references, postmortems
5. **TrendTracker frontend** — momentum/regime/rotation/vol/liquidity composite view
6. **GEX visualization** — D3 dealer gamma profile chart (VIZ-1 from GSD-OPTIONS-EDGE.md)
7. **Resolver fixes** — wn_*, FRED dates, analyst int64, entity_map for new series
8. **Fill remaining 124 zero features** — intl macro, systemic (OFR fix), trade (Comtrade key)
9. **Options self-improvement loop** — tracker scoring first outcomes, feeding back to scanner weights

### Disk Usage
- /data drive: ~50GB of 11TB used (~1%)
