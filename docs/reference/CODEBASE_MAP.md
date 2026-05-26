# Codebase Map (reference)

> Load when: orienting in the tree, deciding where code lives, or checking whether a
> capability already exists before building. Authoritative full inventory is
> [`docs/MODULE_INVENTORY.md`](../MODULE_INVENTORY.md) (700+ modules across 30
> directories with docstrings, APIs, DB I/O, and import graphs). This file is the
> curated quick-map.

## Directory Structure

```
grid/
├── api/           # FastAPI routes, auth, middleware (93 routers)
├── alerts/        # Email alerting system (failure, regime, 100x, digest)
├── ingestion/     # 50+ data source pullers (FRED, BLS, ECB, altdata, etc.)
├── normalization/ # Multi-source conflict resolution
├── store/         # PIT-correct query engine (PostgreSQL DISTINCT ON)
├── features/      # Feature engineering + importance tracking
├── discovery/     # Unsupervised regime clustering + options scanner
├── validation/    # Walk-forward backtesting gates
├── inference/     # Live model scoring
├── journal/       # Immutable decision log
├── governance/    # Model lifecycle state machine
├── intelligence/  # Trust scoring, actor network, cross-reference, postmortem
├── trading/       # Options recommender, tracker, signal executor, exchanges
├── agents/        # TradingAgents multi-agent framework
├── hyperspace/    # Local LLM inference layer (P2P)
├── ollama/        # Ollama integration + market briefings
├── llamacpp/      # llama.cpp direct inference
├── outputs/       # LLM insight logging + scanner
├── server_log/    # Git-backed error logging + operator inbox
├── pwa/           # React 18 PWA frontend (Zustand, Vite)
├── docs/          # Architecture, API, deployment, development guides
├── tests/         # pytest suite (5,702 tests across 301 files)
└── scripts/       # Migration and utility scripts
```

## Intelligence Layer (143 modules, ~92,759 lines)

The intelligence layer tracks who moves markets and why. The top-level `intelligence/`
tree contains 143 modules (the original "14-module scaffold" is historical — do not
cite it). Most load-bearing modules:

- `intelligence/actor_network.py` (153 LOC façade) — thin re-export shim; the real
  [[Actor Network|actor network]] lives in the `intelligence/actors/` subpackage (db,
  registry, expand, etc.). Do not edit the façade — extend the subpackage.
- `intelligence/actor_discovery.py` (3,533 LOC) — automated actor discovery &
  enrichment at 250K+ scale (board interlocks, 3-degree expansion, ICIJ import)
- `intelligence/causation.py` (26 LOC re-export shim) — real logic split across
  `causation_core.py` (194), `causation_graph.py` (1,178), `causation_scoring.py`
  (1,089). Extend the split modules, not the shim.
- `intelligence/sector_networks/` (YAML-driven) — banking / energy / pharma / defense
  / tech_monopoly / real_estate / commodities / defi / media / sovereign_wealth actor
  meshes, loaded by `sector_networks/loader.py`. Replaces the prior standalone
  `*_network.py` modules.
- `intelligence/global_levers.py` (2,258 LOC) — macro lever identification
- `intelligence/hypothesis_engine.py` (2,137 LOC) — hypothesis generation, scoring, kill
- `intelligence/deep_graph.py` (1,772 LOC) — multi-hop graph traversal engine
- `intelligence/cross_reference.py` (1,435 LOC) — government stats vs physical reality
  ([[Cross Reference|lie detector]])
- `intelligence/entity_resolver.py` (1,411 LOC) — canonical actor disambiguation
- `intelligence/lever_pullers.py` (1,376 LOC) — identifies market-moving actors across
  5 categories
- `intelligence/postmortem.py` (1,344 LOC) — automated failure analysis for bad trades
- `intelligence/trust_scorer.py` (1,100 LOC) — [[Trust Scorer|Bayesian trust]] scoring
  with recency decay

**Canonical scoring/flow stack:** `trust_scorer`, `dollar_flows`, `flow_aggregator`,
`flow_thesis`, `forensics`, `event_sequence`, `thesis_tracker`, `sleuth`,
`source_audit`. Always reconcile against [`docs/MODULE_INVENTORY.md`](../MODULE_INVENTORY.md).

> **Location note:** `flow_thesis.py` and `flow_aggregator.py` live in `analysis/`, not
> `intelligence/`.

## "I almost built it but it already exists" (anti-duplication)

Known cases where an obvious-sounding capability already exists:

- `analysis/vol_surface.py` — SVI parameterization, skew, butterfly checks. Not wired
  into `discovery/options_scanner.py` or `trading/options_recommender.py`.
- `intelligence/earnings_transcript_analyzer.py` — tone / Q&A split / guidance extraction.
- `intelligence/hypothesis_engine.py` — LLM-driven hypothesis generation with kill criteria.
- `intelligence/prediction_calibration.py` — Brier / reliability tracking (but not
  persisted, not per-horizon).
- `intelligence/signal_registry.py` + `signal_backlinker.py` + `signal_extractor.py` —
  signal inventory.
- `physics/dealer_gamma.py` — [[Dealer Gamma|vanna]] and charm computed at lines 248-250
  but never used in scoring.
- **Sector networks** — consolidated into `intelligence/sector_networks/*.yaml` loaded by
  `intelligence/sector_networks/loader.py`. Extend the YAML files, not the deleted
  standalone `*_network.py` Python modules.

## Data Sources (expanded)

All 48 pullers are registered in `hermes_operator.py`. Recent altdata modules:

- `ingestion/altdata/congressional.py` — [[Congressional Trading|congressional trading]] disclosures
- `ingestion/altdata/insider_filings.py` — [[Insider Filings|SEC Form 4]] with cluster buy detection
- `ingestion/altdata/dark_pool.py` — [[Dark Pool|FINRA dark pool]] weekly data
- `ingestion/altdata/unusual_whales.py` — whale options flow detection
- `ingestion/altdata/prediction_odds.py` — [[Polymarket]] rapid probability shifts
- `ingestion/altdata/smart_money.py` — Reddit + Finviz trust-scored social signals
- `ingestion/altdata/supply_chain.py` — shipping rates, container index, ISM
- `ingestion/altdata/fed_liquidity.py` — Fed net liquidity equation
- `ingestion/altdata/institutional_flows.py` — [[Institutional Flows|ETF flows]] + SEC [[Institutional Flows|13F]] holdings
- `ingestion/altdata/fara.py` — DOJ [[FARA]] foreign agent lobbying
- `ingestion/altdata/foia_cables.py` — State Dept + NSA Archive declassified diplomatic cables
- `ingestion/altdata/gdelt.py` — actor-level tone, country-pair tension scoring, geopolitical events

> SEC ingestion is being rebuilt on `edgartools` — see
> [`docs/planning/SEC_TOOLS_REBUILD.md`](../planning/SEC_TOOLS_REBUILD.md).

## Frontend Views (51 total views, 45 routes)

- [[MoneyFlow View|MoneyFlow]] — global money flow D3 visualization (Central Banks → Markets → Sectors)
- [[Cross Reference View|CrossReference]] — government stats vs physical reality [[Cross Reference|lie detector]]
- Predictions — oracle scoreboard + calibration chart
- [[Actor Network View|ActorNetwork]] — D3 force graph of financial power structure
- [[Intel Dashboard View|IntelDashboard]] — unified intelligence command center
- [[TrendTracker View|TrendTracker]] — momentum, regime, rotation, vol, liquidity trends
- Timeline.jsx (1,129 lines) — forensic event timeline reconstruction
- WhyView.jsx (1,122 lines) — "why did this move?" [[Causation|causation]] reconstruction
