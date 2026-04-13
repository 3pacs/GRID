---
source: /Users/anikdang/grid_obsidian/Architecture/Analysis-Layer.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# Analysis Layer

29 files providing capital flow analysis, money flow modeling, hypothesis testing, and market research.

## Capital Flows (`analysis/capital_flows.py`)

`CapitalFlowResearchEngine` — triggered by LLM queries about capital flows, sector rotation, or fund positioning. Performs deep data pull across:
1. Sector ETF price action + relative strength (yfinance)
2. Sector volume profiles and momentum
3. Cross-border flows (BIS)
4. FRED monetary aggregates (M2, reserves, bank credit)
5. SEC filing velocity by sector
6. Dark pool activity (FINRA ATS)
7. Credit spreads and bond flows
8. Options positioning (put/call by sector ETFs)

Results cached, logged as LLM insight. Refreshed every 4h by [[Cron-Schedule]].

## Money Flow Engine (`analysis/money_flow_engine/`, 12 files)

8-layer junction point model tracking ~$500T in global capital flows.

### Layers (in Sankey order)
| Layer | Module | What it tracks |
|-------|--------|----------------|
| 0. Monetary | `layer_monetary.py` | CB balance sheets, repo, TGA, global M2 |
| 1. Credit | `layer_credit.py` | Bank lending, bond spreads, money markets |
| 2. Institutional | `layer_institutional.py` | 13F filings, pension flows, SWF |
| 3. Market | `layer_market.py` | ETF flows, options, dark pools |
| 4. Corporate | `layer_corporate.py` | Buybacks, dividends, M&A, IPOs |
| 5. Sovereign | `layer_sovereign.py` | FX reserves, trade balance, tariffs |
| 6. Retail | `layer_retail.py` | Margin debt, sentiment, fund flows |
| 7. Crypto | `layer_crypto.py` | BTC, stablecoins, on-chain flows |

### Supporting files
- `flow_inference.py` — Infers flow edges between layers
- `helpers.py` — Shared utilities
- `types.py` — Type definitions (`FlowMap`, `FlowEdge`, etc.)

**API**: [[API-Endpoints-Master]] — `/api/v1/flows/money-map`, `/api/v1/flows/layers`, `/api/v1/flows/waterfall`

## Market Analysis

| Module | What it does |
|--------|-------------|
| `flow_aggregator.py` | Aggregates flows across multiple sources |
| `flow_thesis.py` | Tests flow-based trading theses |
| `money_flow.py` | Simplified money flow analysis |
| `hypothesis_tester.py` | Tests hypotheses via lagged cross-correlations (504 days, lags 0-20) |
| `thesis_scorer.py` | Scores thesis accuracy over time |
| `sector_map.py` | Sector classification and mapping |
| `market_universe.py` | Defines the tradeable universe |

## Volatility & Options

| Module | What it does |
|--------|-------------|
| `vol_surface.py` | Vol surface construction with SVI parameterization. Skew metrics, term structure, butterfly/calendar arbitrage detection, historical percentile ranking |
| `backtest_scanner.py` | Scans for backtest opportunities |

## Astrology & Alternative

| Module | What it does |
|--------|-------------|
| `astro_correlations.py` | `AstroCorrelationEngine` — planetary-market correlation analysis. Refreshed weekly (Sun 03:00) |
| `ephemeris.py` | Planetary ephemeris calculations |

## Research & Quality

| Module | What it does |
|--------|-------------|
| `research_agent.py` | Full automated research pipeline. Runs nightly at 02:00 |
| `taxonomy_audit.py` | Audits feature taxonomy. Auto-fixes + recommendations. Runs at 02:30 |
| `prompt_optimizer.py` | Optimizes LLM prompts for intelligence queries |
| `viz_intelligence.py` | Visualization intelligence (chart recommendations) |

## Dependencies

- **Reads from**: `resolved_series`, `feature_registry`, `raw_series`, `analytical_snapshots`
- **Writes to**: `analytical_snapshots`
- **Used by**: [[API-Layer]] (flows, discovery, physics routers), [[Intelligence-Layer]], [[Trading-Layer]]

## Related

- [[Data-Pipeline]] — Data sources feeding analysis
- [[Intelligence-Layer]] — Uses analysis outputs for intelligence synthesis
- [[Feature-Registry]] — Features analyzed

## Intelligence Modules (code-verified)

- [[Trust Scorer]]
- [[Company Analyzer]]
- [[Thesis Tracker]]
- [[Hypothesis Engine]]
