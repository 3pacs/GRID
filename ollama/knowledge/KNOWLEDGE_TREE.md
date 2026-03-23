# GRID Knowledge Tree

```
                                    ┌──────────────────┐
                                    │   G R I D        │
                                    │ Trading Intel    │
                                    │ Engine           │
                                    └────────┬─────────┘
                                             │
                 ┌───────────────────────────┼───────────────────────────┐
                 │                           │                           │
        ┌────────▼────────┐       ┌─────────▼─────────┐       ┌────────▼────────┐
        │  DATA LAYER     │       │ INTELLIGENCE LAYER │       │  OUTPUT LAYER   │
        │                 │       │                    │       │                 │
        └────────┬────────┘       └─────────┬─────────┘       └────────┬────────┘
                 │                           │                          │
    ┌────────────┼────────────┐    ┌────────┼─────────┐    ┌──────────┼──────────┐
    │            │            │    │        │         │    │          │          │
┌───▼───┐  ┌────▼────┐ ┌─────▼┐ ┌─▼──┐ ┌───▼──┐ ┌───▼┐ ┌─▼──┐ ┌────▼───┐ ┌───▼──┐
│02 Data│  │03 Feats │ │20 Tax│ │04  │ │11 AR │ │13 │ │15  │ │18 Paper│ │19 PWA│
│Sources│  │Families │ │onomy │ │Reg │ │Auto  │ │Agt│ │Jrnl│ │Trading │ │Front │
│       │  │         │ │      │ │Det │ │Rsrch │ │   │ │    │ │        │ │end   │
└───┬───┘  └────┬────┘ └──┬───┘ └─┬──┘ └───┬──┘ └─┬─┘ └─┬──┘ └───┬────┘ └───┬──┘
    │            │         │       │        │      │     │         │          │
    │            │         │       │        │      │     │         │          │
┌───▼────────────▼─────────▼───────▼────────▼──────▼─────▼─────────▼──────────▼───┐
│                           INFRASTRUCTURE LAYER                                    │
│                                                                                   │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐           │
│  │09 PIT│  │21 DB │  │16 Wkf│  │14 Gov│  │17 Phy│  │10 Rsk│  │12 Cry│           │
│  │Corrct│  │Schema│  │Compte│  │ernce │  │sics  │  │Mgmt  │  │pto   │           │
│  └──────┘  └──────┘  └──────┘  └──────┘  └──────┘  └──────┘  └──────┘           │
└──────────────────────────────────────────────────────────────────────────────────┘
                                             │
                              ┌──────────────┼──────────────┐
                              │              │              │
                        ┌─────▼────┐  ┌──────▼─────┐ ┌─────▼─────┐
                        │05 Derived│  │06 Analysis │ │07 Econ    │
                        │Signals   │  │Framework   │ │Mechanisms │
                        └──────────┘  └────────────┘ └───────────┘
                                             │
                                      ┌──────▼──────┐
                                      │08 Historical│
                                      │Regimes      │
                                      └─────────────┘
```

## Dependency Map

```
DATA LAYER (what goes IN)
├── 02_data_sources          37+ sources: FRED, ECB, OECD, BIS, AKShare, GDELT...
├── 03_feature_families      464+ features: rates, credit, breadth, vol, FX, commodity, macro
├── 12_crypto_signals        DexScreener + Pump.fun speculative metrics
└── 20_signal_taxonomy       10 domains × 72 subtypes classification
         │
         ▼
INFRASTRUCTURE (how it WORKS)
├── 09_pit_correctness       No-lookahead: release_date <= as_of_date
├── 21_database_schema       PostgreSQL: 17 tables, triggers, PIT indexes
├── 16_workflows_compute     Wave-based parallel execution, dependency ordering
├── 14_model_governance      CANDIDATE → SHADOW → STAGING → PRODUCTION
├── 17_physics_verification  OU params, Hurst exponent, energy, stationarity
└── 10_risk_management       Position sizing, confidence, scenario analysis
         │
         ▼
INTELLIGENCE (what it THINKS)
├── 04_regime_detection      Unsupervised clustering → 5 regime archetypes
├── 11_autoresearch          LLM hypothesis → backtest → critique → refine → repeat
├── 13_trading_agents        Multi-agent deliberation: analysts + debate + risk → decision
├── 05_derived_signals       China credit impulse, K-shape, Korea exports, VIIRS
├── 06_market_analysis       Briefing structure, cross-asset scan, contradictions
├── 07_economic_mechanisms   Transmission channels: monetary, credit, dollar, trade
└── 08_historical_regimes    GFC, Euro crisis, COVID, 2022, soft landing analogies
         │
         ▼
OUTPUT (what you SEE)
├── 15_decision_journal      Immutable log: decision → outcome → verdict
├── 18_paper_trading         Falsifiable predictions, scoring, track record
└── 19_pwa_frontend          15 pages: Dashboard, Regime, Agents, Discovery, Physics...
```

## Data Flow (End to End)

```
Raw Data (37+ sources)
    │
    ▼
Ingestion (BasePuller, rate-limited, retry)
    │
    ▼
raw_series table (obs_date, pull_timestamp, value)
    │
    ▼
Conflict Resolution (multi-source, per-family thresholds)
    │
    ▼
resolved_series table (PIT-correct, vintage-tracked)
    │
    ▼
Feature Engineering (z-score, slope, ratio, lag)
    │
    ├──────────────────────┐
    │                      │
    ▼                      ▼
Discovery                Autoresearch
(PCA, clustering,        (LLM hypothesis
 orthogonality)           → backtest
    │                      → critique
    │                      → refine)
    │                      │
    ▼                      ▼
Regime Detection         hypothesis_registry
(5 archetypes)           (CANDIDATE → PASSED)
    │                      │
    ├──────────────────────┤
    │                      │
    ▼                      ▼
Trading Agents           Model Governance
(multi-agent debate,     (CANDIDATE → SHADOW
 regime-anchored)         → STAGING → PROD)
    │                      │
    ├──────────────────────┤
    │                      │
    ▼                      ▼
Decision Journal         Paper Trading
(immutable log,          (falsifiable
 outcome tracking)        predictions)
    │                      │
    ▼                      ▼
PWA Dashboard            Track Record
(real-time UI,           (verifiable
 15 pages)                performance)
```

## Feature Domain Coverage

```
RATES ──────────── 5 features  ███████████████░░░░░░░░░░░░░░░░░
CREDIT ─────────── 3 features  █████████░░░░░░░░░░░░░░░░░░░░░░░
BREADTH ────────── 5 features  ███████████████░░░░░░░░░░░░░░░░░
VOLATILITY ─────── 3 features  █████████░░░░░░░░░░░░░░░░░░░░░░░
FX ─────────────── 2 features  ██████░░░░░░░░░░░░░░░░░░░░░░░░░░
COMMODITY ──────── 2 features  ██████░░░░░░░░░░░░░░░░░░░░░░░░░░
MACRO ──────────── 5 features  ███████████████░░░░░░░░░░░░░░░░░
CRYPTO ─────────── 11 features █████████████████████████████████
SENTIMENT ──────── 0 features  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  ← GAP
ALTERNATIVE ────── 0 features  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  ← GAP
                               ─────────────────────────────────
                               36 core features (model-eligible)
```

## System Health Indicators

```
┌─────────────┬──────────────┬───────────────┐
│  Database   │  Hyperspace  │  Ollama (LLM) │
│  ● GREEN    │  ● RED/GREEN │  ● RED/GREEN  │
│  (required) │  (optional)  │  (for AI)     │
└─────────────┴──────────────┴───────────────┘
```

- **Database**: Required. PostgreSQL with TimescaleDB.
- **Hyperspace**: Optional. Local LLM inference + P2P compute.
- **Ollama**: Required for autoresearch, briefings, and hypothesis generation.
  GRID operates without it but loses AI capabilities.
