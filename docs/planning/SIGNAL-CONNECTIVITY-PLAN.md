# Signal Connectivity Architecture Plan

> Generated 2026-03-31 by planner agent. See ROADMAP.md for scheduling.

## The Problem

GRID has 14 intelligence modules (22,354 lines), 48 pullers, 10 sector network modules, and 5 Oracle models. But connectivity is sparse — most intelligence output only serves API endpoints. The Oracle reads from raw resolved_series, ignoring forensics, causation, event sequences, patterns, sector networks, and the unified thesis.

## The Vision

1000+ composable models, each pulling from any combination of signals with configurable weights. Agents create, test, score, and promote/kill models autonomously. Simple models compound into complex ensembles.

## Architecture

```
Signal Registry (typed, temporal, queryable)
    ├── Model Factory (spawn, configure, register)
    ├── Signal Aggregator (weighted combine, PIT-safe)
    ├── Model Evolver (mutate, crossover, discover, ablate)
    └── Ensemble Predictor (L0 individual → L1 sector → L2 regime → L3 meta)
```

## Disconnected Modules (currently API-only)

forensics, causation, event_sequence, pattern_engine, thesis_tracker, earnings_intel, news_intel, news_impact, market_diary, hypothesis_engine, prediction_calibration, global_levers, deep_graph, actor_discovery, influence_network, institutional_map, company_analyzer, trend_tracker + 10 sector network modules (defense, pharma, SWF, banking, energy, tech, real estate, commodities, DeFi, media)

## Implementation Phases

### Phase 1: Signal Registry (foundation)
- New table `signal_registry` with PIT timestamps
- SignalAdapter protocol — one adapter per intelligence module
- Adapters for: flow_thesis, forensics, causation, patterns, features, trust/convergence, lever_pullers
- Wire refresh into Hermes (every 2h)

### Phase 2: Model Factory + Aggregator
- ModelSpec dataclass with signal subscriptions + weight config
- Migrate existing 5 Oracle models to ModelSpec format
- SignalAggregator with PIT filtering, trust weighting, recency decay
- Feature flag: GRID_SIGNAL_REGISTRY (off by default, legacy path preserved)

### Phase 3: Model Evolution
- ModelEvolver: mutation, crossover, discovery, ablation, horizon strategies
- Autonomous 6h cycle: score → kill bottom → spawn variants of top → discover
- hypothesis_engine feeds discoveries into evolver
- Cap: 50 active models max

### Phase 4: Ensemble Composability
- 3-level hierarchy: individual → sector → regime-conditional → meta
- Weighted voting (weight × hit_rate)
- Per-model attribution on every ensemble prediction
- Optional ensemble consultation in signal_executor (feature flag)

### Phase 5: Remaining Adapters
- 10 sector network modules → single network_adapter
- earnings_intel, news_intel, global_levers, institutional_map, deep_graph
- hypothesis_engine into Hermes daily schedule

## Key Constraints
- Apr 17 Oracle scoring MUST NOT break (feature flag, legacy path preserved)
- PIT correctness: every signal has valid_from/valid_until, aggregator takes as_of
- Model death doesn't cascade (try/except per model in ensemble)
- Trust integration one-way (signal_registry reads trust scores, not vice versa)

## New Files
- intelligence/signal_registry.py (~300 lines)
- intelligence/adapters/ (base + ~12 adapter files, ~100-200 lines each)
- oracle/model_factory.py (~400 lines)
- oracle/signal_aggregator.py (~300 lines)
- oracle/model_evolver.py (~400 lines)
- oracle/ensemble.py (~350 lines)

## Success Criteria
- All 14+ intelligence modules produce typed signals in registry
- Oracle models configurable via signal subscriptions (no code changes)
- Flow thesis consumed by at least one model
- Evolver autonomously manages model lifecycle
- Ensemble predictions scored alongside individuals
- Existing 615 predictions unaffected
- PIT verified: no future signals in any prediction
