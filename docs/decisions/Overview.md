---
source: /Users/anikdang/grid_obsidian/Architecture/Overview.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# GRID System Architecture

GRID is a self-improving intelligence amplifier for financial markets. It ingests data from 90+ sources, resolves it into a canonical feature store, runs ML inference and intelligence analysis, and serves everything through a [[FastAPI]] backend + React PWA.

## System Layers

```
┌─────────────────────────────────────────────────────┐
│  Frontend: React PWA + AstroGrid + Derivatives UI   │  ← [[Frontend-Overview]]
├─────────────────────────────────────────────────────┤
│  API Layer: 40+ FastAPI routers, WebSocket, Auth    │  ← [[API-Layer]]
├─────────────────────────────────────────────────────┤
│  Intelligence Layer: 60+ analyzers, actor networks  │  ← [[Intelligence-Layer]]
├─────────────────────────────────────────────────────┤
│  Analysis Layer: Capital flows, money flow engine   │  ← [[Analysis-Layer]]
├─────────────────────────────────────────────────────┤
│  Trading Layer: Paper engine, Hyperliquid, options  │  ← [[Trading-Layer]]
├─────────────────────────────────────────────────────┤
│  ML/Inference: Ensemble classifiers, Oracle, Alpha  │  ← [[ML-Inference]]
├─────────────────────────────────────────────────────┤
│  Data Pipeline: 90+ pullers → raw → resolve → PIT  │  ← [[Data-Pipeline]]
├─────────────────────────────────────────────────────┤
│  Orchestration: Event bus, task queue, Hermes       │  ← [[Orchestration-Layer]]
├─────────────────────────────────────────────────────┤
│  Storage: PostgreSQL + feature_registry + PIT store │  ← [[Database-Schema]]
└─────────────────────────────────────────────────────┘
```

## Module Map (by file count)

| Directory | Files | Purpose |
|-----------|-------|---------|
| `ingestion/` | 118 | Data pullers (altdata, international, physical, celestial, trade, ML) |
| `intelligence/` | 89 | Intelligence analysis (actors, adapters, regime, networks) |
| `scripts/` | 82 | Operational scripts (pipeline, backfill, research, trading) |
| `api/` | 69 | FastAPI routers (58 router files) + schemas + auth |
| `analysis/` | 29 | Capital flows, money flow engine, hypothesis testing, vol surface |
| `alpha_research/` | 21 | Evolutionary factor mining, signal validation, conviction scoring |
| `oracle/` | 14 | Self-improving prediction loop with weight evolution |
| `inference/` | 11 | ML model training, ensemble classification, live inference |
| `trading/` | 11 | Paper trading, Hyperliquid, Polymarket, options |
| `agents/` | 9 | Competing LLM agents with investor personas |
| `physics/` | 8 | Market physics (momentum, energy, OU processes, dealer gamma) |
| `orchestration/` | 7 | Event bus, LLM task queue, distributed worker |
| `ollama/` | 7 | Local LLM integration (briefings, reasoning) |
| `features/` | 5 | Feature registry, Alpha101, feature lab |
| `discovery/` | 5 | Changepoint detection, clustering, options scanning |
| `normalization/` | 3 | Entity resolution, SEED_MAPPINGS |
| `store/` | 4 | PIT store, snapshots, AstroGrid store |

## Key Infrastructure

- **Database**: [[PostgreSQL]] with ~15 core tables ([[Database-Schema]])
- **LLM Stack**: Qwen 32B (local via [[llama.cpp]]), [[Ollama]], Claude/GPT/HF cloud
- **Compute**: gridz4 worker node via Tailscale, BOINC-style distributed compute
- **[[deployment|Deployment]]**: systemd services, Cloudflare tunnel
- **Scheduling**: `schedule` library in-process, [[Hermes Scheduler|Hermes operator]] daemon

## Data Flow Summary

```
External APIs → Pullers → raw_series → Resolver → resolved_series → PIT Store → ML Models
                                                                        ↓
                                                                  Intelligence Modules
                                                                        ↓
                                                                  API → Frontend
```

See [[Data-Pipeline]] for detailed data flow documentation.

## Related Notes

- [[Project-Structure]] — **Full directory tree with every file and module**
- [[Cron-Schedule]] — what runs when
- [[API-Endpoints-Master]] — complete endpoint listing
- [[Feature-Registry]] — the 1,281 features
- [[Entity-Map]] — how raw data maps to features
- [[Planning-Docs]] — [[architecture]] and planning document index
- [[META-Agent-Brief-Template]] — template for agent briefing notes
