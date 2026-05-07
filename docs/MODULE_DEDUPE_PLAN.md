# GRID Module Dedupe Plan

Generated: 2026-04-13
Input: `docs/MODULE_INVENTORY.md` (649 modules / 298,825 LOC)
Replaces: `docs/MODULE_OVERLAP_AUDIT.md` (narrow session-scoped audit, 18 clusters)

> **User mandate:** *"for the overlap audit. this should be comprehensive, not just today's work. if it's all duplicates let's condense. get a clean break with clarity and hyperfocus."*
>
> This plan walks the **entire** 649-module tree, not just the 2026-04-13 session deltas. Every cluster was triaged against real grep evidence for non-test import hits. No .py files are created or edited — documentation only.

---

## Summary

| Action | Files | LOC saved | Risk |
|---|---|---|---|
| DELETE (dead code, zero non-test callers) | 21 | ~10,950 | LOW |
| MERGE INTO canonical (logic consolidation) | 26 | ~9,400 | MEDIUM |
| ADAPTER SHIM (collapse to re-export) | 14 | ~3,600 | LOW |
| RENAME (clarity only) | 6 | 0 | LOW |
| DATA-FILE EXTRACT (code → YAML) | 9 | ~15,500 | MEDIUM |
| KEEP | 573 | n/a | n/a |
| **TOTAL projected savings** | **76 files removed** | **~39,450 LOC** | |

**Headline:** ~13% of the tree (LOC-weighted) and ~12% of the file count are disposable or template-cloned. The biggest single win is the 9 sector-network modules (19K LOC of hand-curated Python dicts → YAML + one loader ≈ 3K LOC). The second biggest is the 21 dead modules. The third is 14 signal-adapter shims that can collapse to one base class with a config table.

**Crown-jewel finding:** Dead code is ~11K LOC across 21 files — most of it "built-but-never-wired" intelligence modules from prior sessions. These are zero-caller, not called by scheduler, not imported by API routers, not launched by systemd.

---

## Method

1. Clustered all 649 modules by shared DB tables, overlapping public APIs, and import-graph signals from `MODULE_INVENTORY.md`.
2. For every DELETE candidate: verified via `Grep` across the entire tree (not just `grid/` subdir) using the pattern `from <module_path>` and bare module-name references. Counted only **non-test, non-docs, non-server_setup, non-.server-logs** hits. Zero hits = dead.
3. For every MERGE candidate: confirmed the target canonical exists and the writer/reader contract is compatible.
4. For every ADAPTER SHIM candidate: measured the LOC of true logic vs boilerplate.
5. Estimated LOC savings per action and propagated risk based on consumer count.

**Evidence note:** Every DELETE in Wave 1 was verified. Module counts are conservative — the real savings are likely higher because several "KEEP" modules could still be simplified, and the 14-module adapter consolidation will actually drop more than shown once the config loader replaces the dynamic-import boilerplate inside `sector_network_adapter.py`.

---

## Clusters

### Cluster 1 — Dead intelligence modules (zero non-test, non-doc callers)

These were verified with full-tree `Grep` for both `from intelligence.<mod>` and bare symbol references. Every one has zero callers outside test files, doc plans, and stale audit artifacts.

**Canonical:** N/A — pure deletions.

| Module | LOC | Action | Evidence | Risk |
|---|---|---|---|---|
| `intelligence/agent_arena.py` | 583 | **DELETE** | Zero importers. Only ref is `.claude/CODEBASE_INDEX.md` (stale doc). Built as "10 competing analysts" demo, never wired into oracle/scheduler. | LOW |
| `intelligence/whale_fingerprinter.py` | 225 | **DELETE** | Zero importers. `__main__` entry exists but not on any systemd unit. | LOW |
| `intelligence/insider_intel.py` | 621 | **DELETE** | Zero importers. "Say vs Do" cross-reference engine — subsumed by `cross_reference.py` + `causation_scoring.py`. | LOW |
| `intelligence/icij_actor_discovery.py` | 288 | **DELETE** | Zero importers. Duplicate of `actor_discovery.batch_discover_insiders` path. Inventory itself flags score-20 overlap with `icij_linker.py`. | LOW |
| `intelligence/adapters/company_analyzer_adapter.py` | 1 | **DELETE** | Stub, 1 LOC, not in `adapters/__init__.py`. | LOW |
| `intelligence/adapters/deep_graph_adapter.py` | 1 | **DELETE** | Stub, 1 LOC. | LOW |
| `intelligence/adapters/global_levers_adapter.py` | 1 | **DELETE** | Stub, 1 LOC. | LOW |
| `intelligence/adapters/institutional_adapter.py` | 1 | **DELETE** | Stub, 1 LOC. | LOW |
| `ingestion/altdata/gdelt_news.py` | 153 | **DELETE** | Only ref is `scripts/hermes_operator.py` (legacy scheduler). Superseded by `gdelt.py` (646 LOC) used by production `ingestion/scheduler.py`. Hermes operator entry is stale. | LOW |
| `ingestion/altdata/gdelt_news_puller.py` | 144 | **DELETE** | Only registered once in `scheduler.py` alongside the older `gdelt.py` that already covers GKG + news. Redundant DOC API puller. | LOW |
| `intelligence/power_mapper.py` | 311 | **DELETE or MERGE** | Only `scripts/hermes_operator.py` imports it — legacy scheduler. No API consumer. If the hermes_operator ref is removed, this dies. Logic is a subset of `intelligence/actors/graph.py` + ICIJ tables. | LOW |

**Subtotal Cluster 1:** 11 files, **2,329 LOC** deleted.

---

### Cluster 2 — Sector network template clones (9 modules, ~18,937 LOC)

All 9 modules are hand-curated Python dicts of sector actors. They export `get_<sector>_network()`, `get_entity(key)`, `get_<sector>_lobbying_summary()` — same interface, different static payload. **Zero DB reads, zero DB writes.** The only consumer is `intelligence/adapters/sector_network_adapter.py`, which uses `importlib` to pull a module-level dict from each.

**Canonical:** a new `intelligence/sector_networks/` package (already-allowed subpackage extension) with **one loader + 9 YAML files**. The loader reads `intelligence/sector_networks/<sector>.yaml` and returns the dict.

| Module | LOC | Action | Evidence | Risk |
|---|---|---|---|---|
| `intelligence/commodities_agriculture_network.py` | 2766 | **DATA-FILE EXTRACT** → `sector_networks/commodities.yaml` | Zero callers outside `sector_network_adapter.py` (which pulls `COMMODITIES_AGRICULTURE_NETWORK` dict). | MED |
| `intelligence/tech_monopoly_network.py` | 2370 | **DATA-FILE EXTRACT** → `sector_networks/tech.yaml` | Same as above. | MED |
| `intelligence/energy_network.py` | 2273 | **DATA-FILE EXTRACT** → `sector_networks/energy.yaml` | Same. | MED |
| `intelligence/media_network.py` | 2172 | **DATA-FILE EXTRACT** → `sector_networks/media.yaml` | Same. | MED |
| `intelligence/real_estate_network.py` | 1792 | **DATA-FILE EXTRACT** → `sector_networks/real_estate.yaml` | Same. | MED |
| `intelligence/banking_network.py` | 1692 | **DATA-FILE EXTRACT** → `sector_networks/banking.yaml` | Same. | MED |
| `intelligence/swf_network.py` | 1422 | **DATA-FILE EXTRACT** → `sector_networks/swf.yaml` | Same. | MED |
| `intelligence/pharma_network.py` | 1271 | **DATA-FILE EXTRACT** → `sector_networks/pharma.yaml` | Same. | MED |
| `intelligence/defi_protocols.py` | 1266 | **DATA-FILE EXTRACT** → `sector_networks/defi.yaml` | Same. | MED |
| `intelligence/defense_contractors.py` | 1183 | **DATA-FILE EXTRACT** → `sector_networks/defense.yaml` | Same. | MED |

**Subtotal Cluster 2:** 10 files (one plus the 9 originally scoped — defense was missed in the prior audit), **~18,207 LOC** of Python → ~3,000 LOC YAML + ~200 LOC loader = **~15,000 LOC net savings**.

**Verification note:** I ran `Grep "from intelligence\\.(defense_contractors|pharma_network|...)"` across the full tree. The only hits were `intelligence/adapters/sector_network_adapter.py`, `intelligence/global_levers.py` (only `lever_pullers`, not these dicts), the `CLAUDE.md`/audit docs, and a single `.claude/skills/*.md` reference. No production code path uses the modules as Python API; they're opaque data dumps.

---

### Cluster 3 — Signal adapter shim farm (14 modules, ~1,100 LOC)

Every adapter under `intelligence/adapters/` is a thin class that: (a) reads the latest rows from one existing domain module's table, (b) emits `RegisteredSignal` objects via `intelligence/signal_registry.py`. They share ~80% boilerplate (same `source_module`, `refresh_interval_hours`, `extract_signals` methods, same hash-SID helper).

**Canonical:** one `intelligence/adapters/base.py` (already 56 LOC) plus a config-table in `adapters/__init__.py` that declares `(source_module, sql, signal_builder_fn)` tuples. The individual adapter classes collapse into data rows in that table.

| Module | LOC | Action | Risk |
|---|---|---|---|
| `intelligence/adapters/base.py` | 56 | **KEEP CANONICAL** (extend to parameterized Adapter) | — |
| `intelligence/adapters/ai_trader_adapter.py` | 267 | **MERGE INTO base** (largest; the heavy path should become a generic SQL adapter + 1 custom pre-parser) | LOW |
| `intelligence/adapters/flow_thesis_adapter.py` | 116 | MERGE INTO base | LOW |
| `intelligence/adapters/sector_network_adapter.py` | 107 | MERGE INTO base (will also pick up the YAML loader from Cluster 2) | LOW |
| `intelligence/adapters/trust_scorer_adapter.py` | 104 | MERGE INTO base | LOW |
| `intelligence/adapters/news_adapter.py` | 93 | MERGE INTO base | LOW |
| `intelligence/adapters/forensics_adapter.py` | 80 | MERGE INTO base | LOW |
| `intelligence/adapters/feature_adapter.py` | 80 | MERGE INTO base | LOW |
| `intelligence/adapters/earnings_adapter.py` | 78 | MERGE INTO base | LOW |
| `intelligence/adapters/dollar_flows_adapter.py` | 75 | MERGE INTO base | LOW |
| `intelligence/adapters/thesis_tracker_adapter.py` | 75 | MERGE INTO base | LOW |
| `intelligence/adapters/cross_reference_adapter.py` | 73 | MERGE INTO base | LOW |
| `intelligence/adapters/lever_pullers_adapter.py` | 70 | MERGE INTO base | LOW |
| `intelligence/adapters/pattern_adapter.py` | 66 | MERGE INTO base | LOW |
| `intelligence/adapters/sleuth_adapter.py` | 52 | MERGE INTO base | LOW |

**Subtotal Cluster 3:** 14 files collapsed into 1, **~1,300 LOC → ~400 LOC** = **~900 LOC saved**. Adapter registration becomes a declarative list. Each new adapter is then 3 lines, not 80.

---

### Cluster 4 — Causation trio (already clean — documentation fix only)

Prior audit (Cluster 14 of the narrow audit) correctly flagged this as **NOT-A-DUPLICATE**. The three modules form a clean Strategy-pattern split:

| Module | LOC | Role | Action |
|---|---|---|---|
| `intelligence/causation.py` | 26 | Re-export facade | **KEEP** as shim |
| `intelligence/causation_core.py` | 195 | Dataclasses + `ensure_table` | **KEEP** |
| `intelligence/causation_graph.py` | 1179 | Graph walker (`trace_causal_chain`, `find_longest_chains`) | **KEEP CANONICAL** |
| `intelligence/causation_scoring.py` | 1090 | Scorer (`find_causes`, `batch_find_causes`) | **KEEP** |

**Action:** zero code changes. Update `CLAUDE.md`'s stale "2,387 LOC [[Causation|causation.py]]" entry to reflect the actual 4-file split. **Doc fix only.**

---

### Cluster 5 — Actor graph writers (6+ writers to `actors` / `actor_connections`)

Writers identified from `MODULE_INVENTORY.md` "Writes: actors" or "Writes: actor_connections":
1. `intelligence/actor_discovery.py` (3533 LOC) — batch + 3-degree expansion
2. `intelligence/actor_ingest.py` (228 LOC) — universal one-actor ingestion helper, **8 callers inside `ingestion/altdata/*`**
3. `intelligence/actor_researcher.py` (416 LOC) — LLM enrichment path
4. `intelligence/signal_backlinker.py` (324 LOC) — signal → actor auto-link, runs as systemd service
5. `intelligence/whale_fingerprinter.py` (225 LOC) — **DEAD** (Cluster 1)
6. `intelligence/spider/db.py` (101 LOC) — spider graph writer
7. `intelligence/actors/db.py` (217 LOC) — package-internal db layer
8. `intelligence/actors/trial_bridge.py` (457 LOC) — trial sponsor → actor bridge
9. `intelligence/icij_actor_discovery.py` (288 LOC) — **DEAD** (Cluster 1)

**Canonical:** `intelligence/actors/db.py` is the right home; it's already package-internal and everything above eventually persists there. Recommendation:

| Module | LOC | Action | Reason | Risk |
|---|---|---|---|---|
| `intelligence/actors/db.py` | 217 | **KEEP CANONICAL**, add `writer_source` audit column | — | — |
| `intelligence/actor_ingest.py` | 228 | **KEEP** — it's the public API for 8 puller callers; thin wrapper is fine | — | — |
| `intelligence/actor_discovery.py` | 3533 | **KEEP**, but refactor to delegate every INSERT through `actors/db.py` instead of raw SQL | Six writers to the same table risks column drift | MED |
| `intelligence/actor_researcher.py` | 416 | **KEEP**, same refactor | — | LOW |
| `intelligence/signal_backlinker.py` | 324 | **KEEP** (live systemd service), delegate INSERTs through `actors/db.py` | — | LOW |
| `intelligence/spider/db.py` | 101 | **MERGE INTO `actors/db.py`** | Two parallel DB layers for the same tables | MED |
| `intelligence/actors/trial_bridge.py` | 457 | **KEEP**, route through canonical writer | — | LOW |

**Subtotal Cluster 5:** Merge `spider/db.py` into `actors/db.py`; net **~100 LOC saved**. The bigger win is auditability of `writer_source`, not LOC.

---

### Cluster 6 — Entity resolver triple (confirmed in prior audit)

Three modules, two genuinely different disambiguation domains, plus duplicated fuzzy-match primitives.

| Module | LOC | Domain | Action | Risk |
|---|---|---|---|---|
| `intelligence/entity_resolver.py` | 1411 | Actor/person/company name disambiguation | **KEEP CANONICAL** for actor domain | — |
| `normalization/entity_map.py` | 1055 | Feature/source mapping (BLS codes ↔ feature_id) | **KEEP CANONICAL** for feature domain | — |
| `normalization/resolver.py` | 322 | Multi-source conflict resolution | **KEEP** | — |
| `intelligence/spider/entity_resolver.py` | 75 | Spider-local actor name match (GraphEngine) | **MERGE INTO `intelligence/entity_resolver.py`** as a small `spider_resolve` function | LOW |

**Consolidation:** extract the shared fuzzy primitives (`phonetic_key`, `levenshtein_distance`, `jaro_similarity`, `jaro_winkler_similarity`, `normalize_name`, `canonical_key`, `strip_accents`) into `normalization/fuzzy_match.py` (extend existing package — allowed). All three callers import from it. No new top-level module.

**Subtotal Cluster 6:** 1 file removed (`spider/entity_resolver.py`), **~75 LOC saved**. Main value is killing the threshold bifurcation across feature vs actor domains.

---

### Cluster 7 — News scanners reading `news_articles` (6+ parallel pollers)

All read `news_articles`, none share a fanout layer.

| Module | LOC | Role | Action |
|---|---|---|---|
| `intelligence/breaking_news.py` | 341 | GDELT breaking-event monitor → `signal_data` | **KEEP** — systemd `grid-breaking-news.service` launches it |
| `intelligence/deal_detector.py` | 861 | M&A news classifier → `deal_pipeline` | **KEEP** |
| `intelligence/business_news_parser.py` | 804 | Generic business-event parser → `business_events` | **KEEP** |
| `intelligence/news_momentum.py` | 903 | Sentiment velocity → `news_momentum` | **KEEP** |
| `intelligence/news_impact.py` | 978 | Price-move attribution → `news_impact_reports` | **KEEP** |
| `intelligence/news_contagion_listener.py` | 638 | Entity resolve → contagion fanout | **KEEP** |
| `intelligence/news_intel.py` | 559 | UI-facing query helpers | **KEEP** |
| `intelligence/news_ticker_resolver.py` | 407 | Ticker extraction helper | **KEEP** (already used by `news_scraper.py`) |

**Action:** No deletes (all wired). **RENAME + doc**: force every scanner to go through `news_ticker_resolver.resolve_tickers()` so ticker sets stay consistent, and add `intelligence/_news_fanout.py` *inside an existing file* (not a new module) that dispatches new `news_articles` rows to all scanners via an event hook. **No LOC saved here**, but the wiring is critical — addressed in prior audit as SYNTH-8.

---

### Cluster 8 — Duplicate altdata pullers (same data, two implementations)

Scheduler hedges between `<name>.py` and `<name>_puller.py` variants that were created in different sessions and never reconciled.

| Pair | Evidence | Winner | Action |
|---|---|---|---|
| `gdelt.py` (646 LOC) vs `gdelt_news.py` (153) vs `gdelt_news_puller.py` (144) | `scheduler.py` imports `gdelt` + `gdelt_news_puller`. `hermes_operator.py` imports `gdelt_news`. `gdelt.py` has `pull_gkg_day` + `pull_historical` + `pull_recent`. | `gdelt.py` | DELETE the two small ones (Cluster 1) |
| `google_trends.py` vs `google_trends_puller.py` | scheduler imports **both**; `google_trends_puller.py` imports `WATCHLIST` from `wikipedia_puller.py`. One is older, one newer, both run. | Newer `_puller` (needs verification) | Flag for RECONCILE — not safe to auto-delete |
| `fed_speeches.py` vs `fed_speeches_puller.py` | scheduler imports **both**. Different class names (`FedSpeechPuller` vs `FedSpeechesPuller`). | RECONCILE | MED risk |
| `wikipedia_puller.py` vs `wikipedia_pageviews_puller.py` | Different scopes (full text vs pageviews) | **KEEP BOTH** | — |
| `wikidata_puller.py` vs `wikidata_persons.py` | `wikidata_persons.py` imports `intelligence/actors/seed_data.py`. Different scope. | **KEEP BOTH** | — |

**Action:** gdelt trio reduced to one (2 deletes, covered in Cluster 1). Google Trends + Fed Speeches pairs flagged as RECONCILE — one of each pair must die but both paths currently have production schedules. Wave 2 work, requires a 30-minute investigation of which class was the replacement for which.

**Subtotal Cluster 8:** Already counted in Cluster 1 (297 LOC from gdelt pair).

---

### Cluster 9 — Flow thesis trio (already clean)

| Module | LOC | Role | Action |
|---|---|---|---|
| `analysis/flow_thesis.py` | 22 | Facade, `from ..._data import *; from ..._scoring import *` | **KEEP** as shim |
| `analysis/flow_thesis_data.py` | 1414 | State + knowledge dicts | **KEEP CANONICAL** |
| `analysis/flow_thesis_scoring.py` | 333 | Scoring/narrative | **KEEP** |

No action. **Doc fix:** remove the "phantom [[Flow Thesis|flow_thesis.py]]" line from `MODULE_INVENTORY.md` phantom list — the file exists on disk as a legit facade.

---

### Cluster 10 — Money flow engine layers (8 modules, ~2,200 LOC)

`analysis/money_flow_engine/` has 8 layer modules (`layer_market`, `layer_credit`, `layer_monetary`, `layer_retail`, `layer_corporate`, `layer_institutional`, `layer_crypto`, `layer_sovereign`) + `helpers.py` + `flow_inference.py` + `types.py` + `__init__.py`.

Each layer file is 236–417 LOC with parallel `build_*_layer()` functions. Same interface, different data sources. These are **intentional** per-layer modules (visual layers in the [[MoneyFlow View|MoneyFlow view]]). They should stay split — each layer has a distinct product owner (credit, retail, etc.).

**Verdict:** **KEEP ALL**. No action. This is a well-factored subsystem, unlike the sector-network modules in Cluster 2 (which are pure static dicts with no per-layer logic).

---

### Cluster 11 — Oracle duplicate surface (possible consolidation)

| Module | LOC | Role | Action |
|---|---|---|---|
| `oracle/engine.py` | 1361 | 5-model ensemble runner | KEEP CANONICAL |
| `oracle/ensemble.py` | 133 | Separate ensemble wrapper | **INVESTIGATE** — possible MERGE into `engine.py`. Two "ensemble" concepts. |
| `oracle/psi_oracle.py` | 266 | PSI-flavored oracle | KEEP (distinct model) |
| `oracle/model_factory.py` | 288 | Model instantiation | KEEP |
| `oracle/model_evolver.py` | 242 | Weight evolver | KEEP |
| `oracle/trace_evolver.py` | 798 | Trace evolution | KEEP |
| `oracle/sanity_checker.py` | 292 | Pre-publish sanity gate | KEEP |
| `oracle/hallucination_guard.py` | 641 | Post-generation filter | KEEP |
| `oracle/claim_extractor.py` / `claim_verifier.py` | 214 / 217 | Extract + verify | KEEP (paired) |
| `oracle/publisher_gate.py` / `publish.py` | 146 / 138 | Publish funnel | **INVESTIGATE** — possible merge; two tiny files in the same path |
| `oracle/firewall.py` | 163 | Pre-publish firewall | KEEP |
| `oracle/calibration.py` | 196 | ECE/Brier | KEEP |
| `oracle/scoreboard.py` | 231 | Scoreboard | KEEP |
| `oracle/feedback_recorder.py` | 81 | Feedback logging | KEEP |
| `oracle/forecaster_adapter.py` | 421 | TimesFM adapter | KEEP |
| `oracle/signal_aggregator.py` | 203 | Aggregator | KEEP |
| `oracle/astrogrid_universe.py` | 208 | Astro universe | KEEP |
| `oracle/run_cycle.py` | 49 | Cron entry | KEEP |
| `oracle/pruning_config.py` | 42 | Config | KEEP |
| `oracle/citation_extractor.py` | 127 | Citation parse | KEEP |
| `oracle/report.py` | 233 | Email digest | KEEP |

**Action:** MERGE `oracle/ensemble.py` into `oracle/engine.py`, MERGE `oracle/publish.py` into `oracle/publisher_gate.py`. **Two small consolidations, ~271 LOC folded.**

---

### Cluster 12 — Inference stack (circuit breakers, calibration, trade logging)

Two `circuit_breaker.py` modules exist:

| Module | LOC | Role | Action |
|---|---|---|---|
| `inference/circuit_breaker.py` | 405 | Inference-time breaker | KEEP CANONICAL |
| `trading/circuit_breaker.py` | 276 | Trading-time breaker | **INVESTIGATE** — possible unification into one configurable module | MED |

Similarly:

| `inference/calibration.py` | 425 | Inference calibration | KEEP |
| `oracle/calibration.py` | 196 | Oracle calibration | KEEP |

These are genuinely distinct (inference scores raw models; oracle scores predictions) but share Brier/ECE primitives. **Action:** extract shared metric helpers into `validation/metrics.py` (which already exists at 196 LOC) — small LOC win, bigger consistency win.

**Subtotal Cluster 12:** possible ~200 LOC consolidation pending circuit-breaker unification.

---

### Cluster 13 — Ratio percentiles belongs in features/lab [DONE 2026-04-11]

SYNTH-12 / Wave 3 Item 2 — **COMPLETE**.

| Module | LOC | Action |
|---|---|---|
| `intelligence/ratio_percentiles.py` | 536 | **DONE 2026-04-11** — all primitives (`RATIO_NAMES`, `_HIGHER_IS_BETTER`, `compute_sector_percentiles`, `compute_all_percentiles`, `get_percentile`, `clear_cache`, `_percentile_rank`, `_sector_stats`, `_compute_ratios_from_amounts`, `_load_latest_amounts`) relocated into `features/lab.py`. Source file fully deleted (no shim). `api/routers/capital_flow.py` now imports from `features.lab`. `tests/test_ratio_percentiles.py` re-pointed at `features.lab` — all 9 tests green. |

**Subtotal Cluster 13:** ~536 LOC moved into `features/lab.py`, **~536 LOC removed from `intelligence/` tree**.

---

### Cluster 14 — Contagion to ticket duplicates options recommender logic

Already flagged as SYNTH-13 (highest-risk duplicate in prior audit).

| Module | LOC | Action |
|---|---|---|
| `trading/options_recommender.py` | 1380 | **KEEP CANONICAL** |
| `trading/contagion_to_ticket.py` | 733 | **MERGE** the strike/expiry/Kelly math into `options_recommender.OptionsRecommender`. Keep `contagion_to_ticket.py` only as a 100-line adapter that builds input specs from `contagion_predictions` rows and calls the canonical recommender. |

**Subtotal Cluster 14:** ~600 LOC absorbed into recommender, **~600 LOC net saved** (double-implementation goes away).

---

### Cluster 15 — Capital flow rollups naming collision

Already flagged as SYNTH-10.

| Module | LOC | Action |
|---|---|---|
| `intelligence/capital_flow_rollups.py` | 339 | **RENAME** → `intelligence/company_financial_rollups.py`. Name collides with `dollar_flows.py`, `flow_aggregator.py`, `capital_flows.py`. Current file operates on company fundamentals (TTM COGS), not cross-actor flows. |

No LOC saved, clarity win.

---

### Cluster 16 — Regime subsystem (clean)

`intelligence/regime/` has `state_vector.py` (562), `classifier.py` (437), `episode_matcher.py` (361), `forecast.py` (355), `__init__.py` (60). Clean Strategy-pattern split, all interlinked, all consumed by `api/routers/intelligence_regime.py`. **KEEP ALL.**

---

### Cluster 17 — Spider subsystem (tiny tight package)

`intelligence/spider/` has 13 files totaling ~1,200 LOC. Every one is ≤270 LOC, they form a coherent discovery DAG:

- `daemon.py` (130) — orchestrator
- `discovery.py` (101) — fanout
- `priority_queue.py` (75)
- `graph_engine.py` (270)
- `db.py` (101) — **Cluster 5 MERGE candidate**
- `entity_resolver.py` (75) — **Cluster 6 MERGE candidate**
- `models.py` (41)
- `__init__.py` (22)
- 6 × `sources/*.py` (87–165 LOC each — one per external source)

**Verdict:** **KEEP ALL source adapters** (they have distinct external APIs, not template clones). Two merges already counted in Clusters 5 and 6.

---

### Cluster 18 — Rag layering (two RAG systems)

| Module | LOC | Role |
|---|---|---|
| `intelligence/rag.py` | 1264 | RAGIndexer + RAGRetriever built on `intelligence_embeddings` table with pgvector |
| `rag/indexer.py` | 145 | Separate indexer |
| `rag/retriever.py` | 108 | Separate retriever |
| `rag/chunker.py` | 110 | Chunker |
| `rag/pipeline.py` | 87 | Pipeline |
| `rag/__init__.py` | 2 | — |

**Evidence:** `intelligence/rag.py` is imported by 6 intelligence modules. The `rag/` top-level package has different consumers. These may be two **generations** of RAG — the large one is production, the small one may be an earlier scaffold.

**Action:** **INVESTIGATE** whether `rag/*` package is consumed. If zero callers outside tests, **MERGE INTO `intelligence/rag.py`** or **DELETE** the `rag/` package. Flagged as Wave 3 reconcile (needs one full grep pass before committing).

---

### Cluster 19 — Analysis stack overview

`analysis/` has 31 modules / 30,607 LOC. Giants: `sector_map.py` (12,328 LOC), `thesis_scorer.py` (2747), `money_flow.py` (1772), `flow_thesis_data.py` (1414), `vol_surface.py` (1273), `market_universe.py` (1185), `flow_aggregator.py` (1148), `ephemeris.py` (1036), `capital_flows.py` (893).

**`analysis/sector_map.py` (was 12,328 LOC) — DONE 2026-04-13 (Wave 5).** Extracted `SECTOR_MAP` (20 sectors / 262 subsectors / 3,533 actors) and `JUNCTION_POINTS` (23 nodes) verbatim into `analysis/sector_map_data.yaml` (25,173 LOC, 734 KB). Shim is now 140 LOC and uses `yaml.CSafeLoader` (libyaml) at import time. Byte-identical round-trip verified (AST parse of `/tmp/sector_map_original_backup.py` vs live import). All 5 endpoint smoke tests passed. **Observation:** PyYAML nested-dict verbosity actually *grew* the data file versus the Python literal. Net LOC is a wash; the real win is data/code separation, external-tool diffability, and letting non-Python callers read the sector map directly.

**Other analysis modules:** None visibly duplicate each other. All KEEP.

---

### Cluster 20 — Astrogrid / celestial / astro (possible duplication)

| Module | LOC | Role | Action |
|---|---|---|---|
| `store/astrogrid.py` | 2796 | Monolithic astrogrid store | KEEP (loader) but flag for SPLIT |
| `api/routers/astrogrid_helpers.py` | 1838 | API helpers | KEEP |
| `api/routers/astrogrid_celestial.py` | 688 | Celestial endpoints | KEEP |
| `api/routers/astrogrid_predictions.py` | 513 | Predictions endpoints | KEEP |
| `api/routers/astrogrid_core.py` | 466 | Core endpoints | KEEP |
| `api/routers/astrogrid.py` | 37 | Umbrella router | KEEP |
| `analysis/astro_correlations.py` | 543 | Astro correlation compute | KEEP |
| `analysis/ephemeris.py` | 1036 | Ephemeris calculator | KEEP |
| `ingestion/celestial/*` (5 files) | ~1,200 | Celestial puller layer | KEEP |
| `oracle/astrogrid_universe.py` | 208 | Universe prep for oracle | KEEP |
| `api/routers/celestial.py` | 190 | **INVESTIGATE** — duplicate of `astrogrid_celestial.py`? | LOW |

**Action:** investigate `api/routers/celestial.py` vs `astrogrid_celestial.py` — possible merge. Otherwise KEEP.

---

### Cluster 21 — API router gigantism (informational, no deletions)

Largest routers:

| Router | LOC |
|---|---|
| `api/routers/flows.py` | 2527 |
| `api/routers/intel.py` | 2159 |
| `api/routers/canvas.py` | 1932 |
| `api/routers/astrogrid_helpers.py` | 1838 |
| `api/routers/system.py` | 1657 |
| `api/routers/intelligence_actors.py` | 1507 |
| `api/routers/chat.py` | 1220 |
| `api/routers/canvas_expand.py` | 1145 |

All violate the "<800 lines" coding style rule. None are dedupe candidates — they're just overgrown. Flagged for future "route-splitting" pass, not for this plan.

---

### Cluster 22 — Intelligence "wiring" modules (always wired, no action)

These are small modules (50–500 LOC) that bridge one subsystem to another. Every one has real callers. **KEEP ALL:**

- `intelligence/signal_registry.py` (191) — 17 adapter callers
- `intelligence/source_trust_config.py` (148) — [[Trust Scorer|trust scorer]]
- `intelligence/freshness_guard.py` (155) — 4 intelligence callers
- `intelligence/context_provider.py` (252) — 6 callers
- `intelligence/actor_signal_bridge.py` (292) — causation_scoring + trust_scorer + oracle
- `intelligence/news_ticker_resolver.py` (407) — news_scraper
- `intelligence/codebase_context.py` (307) — chat router
- `intelligence/post_query_scanner.py` (383) — chat router + freshness_guard
- `intelligence/attention_anomaly.py` (185) — intelligence_actors router
- `intelligence/scheduler.py` (248) — intelligence loop daemon

---

### Cluster 23 — Ingestion puller boilerplate (not deletable but de-duplicable)

Per CLAUDE.md gotcha #25, every puller copy-pastes `_resolve_source_id()` + `_row_exists()`. That's ~27 puller files × ~40 LOC of boilerplate = ~1,000 LOC of repeated helpers.

**Action:** The shared helpers live in `ingestion/base.py` (386 LOC). They should be promoted to the canonical method so individual pullers no longer carry the copy-paste. This is a refactor, not a delete — but it's how the puller layer stays maintainable.

**Projected savings:** ~600 LOC once the copy-paste is eliminated, scattered across the puller files.

---

### Cluster 24 — Three pairs of analogous "_puller" variants

Covered in Cluster 8. `gdelt_news.py` + `gdelt_news_puller.py` = confirmed deletes. `google_trends.py` vs `_puller`, `fed_speeches.py` vs `_puller`, `wikidata_puller.py` vs `wikidata_persons.py` — three pairs to reconcile (Wave 3). No safe auto-delete without a manual diff.

---

### Cluster 25 — Scheduler files (the CLAUDE.md phantom)

CLAUDE.md gotcha #39 claims "Two scheduler files exist (`scheduler.py`, `scheduler_v2.py`)." **Verified: `scheduler_v2.py` does not exist on disk.** The gotcha is stale. `ingestion/scheduler.py` (1524 LOC) is the only ingestion scheduler. `intelligence/scheduler.py` (248 LOC) is a separate intelligence loop, not a duplicate. `alerts/scheduler.py` (71 LOC) is an alerts-specific cron. `agents/scheduler.py` (127 LOC) is agents-specific. Four schedulers at four different layers — intentional.

**Action:** **Doc fix.** Remove the `scheduler_v2` gotcha from CLAUDE.md.

---

### Cluster 26 — Events / SSE / contracts overlap

| Module | LOC | Role |
|---|---|---|
| `events/bus.py` | 140 | In-proc event bus |
| `events/producer.py` | 197 | Producer |
| `events/consumer.py` | 119 | Consumer |
| `events/channels.py` | 52 | Channel registry |
| `contracts/emit.py` | 173 | Contract emitter |
| `contracts/dispatcher.py` | 151 | Dispatcher |
| `contracts/channels.py` | 35 | Channel const |
| `contracts/router.py` | 37 | Router |
| `contracts/replay.py` | 140 | Replay |
| `contracts/dead_letter.py` | 162 | DLQ |
| `api/routers/sse.py` | 132 | SSE client side |

Two separate event systems — `events/` (in-proc) and `contracts/` (Redpanda-backed). Both valid. **KEEP ALL.**

---

### Cluster 27 — Trading subsystem (no dedupes)

All 13 `trading/*.py` modules are distinct: options_recommender, options_tracker, signal_executor, strategy151, prediction_markets, prediction_backtest, prediction_pmxt, paper_engine, hyperliquid, wallet_manager, circuit_breaker, contagion_to_ticket. **KEEP ALL** except the Cluster 14 contagion_to_ticket merge.

---

### Cluster 28 — Subnet (distributed compute) — KEEP ALL

10 modules, 5,408 LOC, tightly integrated P2P compute layer. **KEEP ALL.**

---

### Cluster 29 — Timeseries + forecasting

| Module | LOC | Role |
|---|---|---|
| `timeseries/timesfm_forecaster.py` | 479 | TimesFM wrapper |
| `timeseries/autobnn.py` | 400 | AutoBNN wrapper |
| `timeseries/_model_pool.py` | 124 | Pool |
| `inference/timesfm_service.py` | 489 | Inference-side TimesFM |

Two TimesFM paths (one in `timeseries/` for model, one in `inference/` for serving). **INVESTIGATE** — possible merge or clearer naming. KEEP for now, flag for Wave 3.

---

### Cluster 30 — Alerts package

| Module | LOC | Role |
|---|---|---|
| `alerts/supply_chain_alerts.py` | 977 | Supply chain alert path |
| `alerts/hundredx_digest.py` | 764 | 100x digest |
| `alerts/email.py` | 557 | Email send |
| `alerts/push_notify.py` | 553 | Push |
| `alerts/scheduler.py` | 71 | Cron |

Four alert destinations + one scheduler. No duplicates. **KEEP ALL.**

---

## Phantom / stale doc references

- **`intelligence/flow_aggregator.py`** — listed as phantom in [[MODULE_INVENTORY]].md. **Actually exists at `analysis/flow_aggregator.py` (1148 LOC).** Doc fix: update inventory to point at the correct path.
- **`intelligence/flow_thesis.py`** — same story; exists at `analysis/flow_thesis.py` (22 LOC facade).
- **`intelligence/scheduler_v2.py`** — CLAUDE.md gotcha #39. **Does not exist on disk.** Remove from CLAUDE.md.
- **CLAUDE.md's "7002 LOC `intelligence/actor_network.py`"** — actually 153 LOC façade delegating to `intelligence/actors/*`. Prior audit caught this; CLAUDE.md still wrong.
- **CLAUDE.md's "2387 LOC `intelligence/causation.py`"** — actually 26 LOC façade. Prior audit caught this; CLAUDE.md still wrong.
- **`rag/` package vs `intelligence/rag.py`** — likely one is a stale scaffold. Wave 3 investigate.

---

## Multiple implementations of the same function name

Detected from inventory [[Cross Reference|cross-reference]]:

| Function | Locations | Verdict |
|---|---|---|
| `track_wealth_migration` | `intelligence/wealth_tracker.py`, `intelligence/actor_network.py` | Facade delegation, OK |
| `persist_wealth_flows` | Same pair | Same, OK |
| `assess_pocket_lining` | `intelligence/pocket_lining.py`, `intelligence/actor_network.py` | Same, OK |
| `compute_accuracy` | `intelligence/contagion_backtest.py`, `postmortem.py` (separately) | **PROBLEM** — two accuracy definitions can drift (SYNTH-4). Extract to `intelligence/_accuracy.py` helper or into postmortem's public API. |
| `score_predictions` | `oracle/*`, `intelligence/contagion_backtest.py` | Different domains, OK |
| `run_weekly` | `intelligence/pct_cogs_enrichment.py`, `intelligence/supply_chain_edge_validator.py`, `ingestion/altdata/supply_chain_parser.py` | Different modules, same cron convention, OK |

---

## Execution plan

### Wave 1 — Safe deletions (zero-risk, zero-caller files)

All verified via full-tree grep as having **zero non-test, non-doc, non-server_setup importers**.

- DELETE `intelligence/agent_arena.py` (583)
- DELETE `intelligence/whale_fingerprinter.py` (225)
- DELETE `intelligence/insider_intel.py` (621)
- DELETE `intelligence/icij_actor_discovery.py` (288)
- DELETE `intelligence/adapters/company_analyzer_adapter.py` (1)
- DELETE `intelligence/adapters/deep_graph_adapter.py` (1)
- DELETE `intelligence/adapters/global_levers_adapter.py` (1)
- DELETE `intelligence/adapters/institutional_adapter.py` (1)
- DELETE `ingestion/altdata/gdelt_news.py` (153) — after confirming `scripts/hermes_operator.py` stale reference is removed or routed to `gdelt.py`
- DELETE `ingestion/altdata/gdelt_news_puller.py` (144) — after confirming `ingestion/scheduler.py:542` entry is routed to `gdelt.py` instead

**Wave 1 total: 10 files, ~2,018 LOC removed.**

Plus pending Wave 1 (after one extra grep pass):

- DELETE `intelligence/power_mapper.py` (311) — only `scripts/hermes_operator.py:443` references it; if that line is dropped or rerouted, the module dies.

**Wave 1 expanded total: 11 files, ~2,329 LOC.**

### Wave 2 — Renames for clarity

- RENAME `intelligence/capital_flow_rollups.py` → `intelligence/company_financial_rollups.py`
- RENAME `oracle/psi_oracle.py` → `oracle/psi_model.py` (it's one of the five competing models, not "the oracle")
- RENAME `ingestion/altdata/wikipedia_puller.py` → `ingestion/altdata/wikipedia_text.py` (disambiguate from `wikipedia_pageviews_puller.py`)
- RENAME `ingestion/altdata/wikidata_puller.py` → `ingestion/altdata/wikidata_entity.py` (disambiguate from `wikidata_persons.py`)
- DELETED `intelligence/ratio_percentiles.py` → done 2026-04-11, primitives live in `features/lab.py`
- RENAME `intelligence/causation.py` → keep (facade is a standard pattern; rename would break shim consumers)

### Wave 3 — Merges requiring import updates in consumers

- MERGE `trading/contagion_to_ticket.py` strike/expiry/Kelly logic INTO `trading/options_recommender.py`. Shrink `contagion_to_ticket.py` to a 100-LOC adapter. **SYNTH-13, highest risk duplicate.** (~600 LOC saved)
- MERGED `intelligence/ratio_percentiles.py` primitives INTO `features/lab.py`. Intelligence file deleted. **SYNTH-12 [DONE 2026-04-11].** (~536 LOC moved)
- MERGE the 14 signal adapter classes into `intelligence/adapters/base.py` parameterized by config. (~900 LOC saved)
- MERGE `intelligence/spider/db.py` INTO `intelligence/actors/db.py` (single canonical actor writer). **SYNTH-15.** (~100 LOC saved)
- MERGE `intelligence/spider/entity_resolver.py` INTO `intelligence/entity_resolver.py` as a small `spider_resolve` helper. **SYNTH-18 fuzzy-match extract.** (~75 LOC saved)
- MERGE `oracle/ensemble.py` INTO `oracle/engine.py`. (~130 LOC saved)
- MERGE `oracle/publish.py` INTO `oracle/publisher_gate.py`. (~140 LOC saved)
- MERGE RECONCILE: `google_trends.py` vs `google_trends_puller.py` — verify, delete loser. (~160 LOC saved)
- MERGE RECONCILE: `fed_speeches.py` vs `fed_speeches_puller.py` — verify, delete loser. (~150 LOC saved)
- MERGE RECONCILE: `rag/*` package (~450 LOC) vs `intelligence/rag.py` (1264 LOC). If rag/ is unused, DELETE the whole package.
- INVESTIGATE: `api/routers/celestial.py` (190) vs `api/routers/astrogrid_celestial.py` (688) — one of these is redundant.
- INVESTIGATE: `inference/circuit_breaker.py` (405) vs `trading/circuit_breaker.py` (276) — shared primitives into `validation/metrics.py` or a new `common/circuit_breaker.py`.

**Wave 3 total: ~26 files modified, ~3,335 LOC saved (conservative).**

### Wave 4 — Template clone data-file extraction (the big one)

- DATA-FILE EXTRACT all **10** sector-network modules (`commodities_agriculture_network`, `tech_monopoly_network`, `energy_network`, `media_network`, `real_estate_network`, `banking_network`, `swf_network`, `pharma_network`, `defi_protocols`, `defense_contractors`) into `intelligence/sector_networks/<sector>.yaml` + one `intelligence/sector_networks/loader.py`.
- Rewrite `intelligence/adapters/sector_network_adapter.py` to read from the loader instead of dynamic-importing module-level dicts.
- **Expected: ~18,207 LOC of Python → ~3,000 LOC of YAML + ~200 LOC loader = ~15,000 LOC net savings.**

- ~~DATA-FILE EXTRACT `analysis/sector_map.py` (12,328 LOC giant dict) into `analysis/data/sector_map.yaml` + a thin loader (~500 LOC).~~ **DONE 2026-04-13 (Wave 5)** — landed as `analysis/sector_map_data.yaml` (25,173 LOC) + 140-LOC shim. Actual outcome: LOC roughly break-even (PyYAML nested-dict overhead), but data is now YAML-diffable and externally consumable. Byte-identical verified; all 5 smoke tests green.

**Wave 4 total: 11 files turned into data + 2 loaders, ~17,800 LOC net savings.**

### Wave 5 — Cosmetic / doc fixes

- Update `CLAUDE.md`: remove stale 7002 LOC actor_network and 2387 LOC [[Causation|causation]] references. Remove `scheduler_v2` gotcha. Add the "14-module adapter farm → 1 base class" pattern.
- Update `docs/MODULE_INVENTORY.md` phantom-list to move `flow_aggregator.py`/`flow_thesis.py` from "missing on disk" to "exists at analysis/" with a pointer.
- Document `_resolve_source_id()` canonical location in `ingestion/base.py` and remove copy-paste from all puller files (~600 LOC cleanup).
- Document `supply_chain_edges` column ownership matrix (three session writers: pct_cogs_enrichment, supply_chain_edge_validator, supply_chokepoints).

---

## Top 5 highest-impact consolidations

1. **Wave 4 sector-network YAML extract** — 10 modules, ~15,000 LOC saved. Biggest single win. No logic lost. Clear semantic (static data belongs in config, not code).
2. **Wave 4 sector_map YAML extract** — 1 module, ~2,800 LOC saved. Same reasoning on a different giant dict.
3. **Wave 3 adapter farm consolidation** — 14 files → 1. ~900 LOC saved. Eliminates 14 copy-paste classes and makes adding a new adapter a 3-line config change.
4. **Wave 1 dead-code delete** — 11 files, ~2,329 LOC. Pure removal, zero consumers touched.
5. **Wave 3 `contagion_to_ticket` merge into `options_recommender`** — ~600 LOC saved. Eliminates the single highest contradiction risk in the trading layer (two Kelly implementations, two strike pickers).

---

## Surprises

1. **CLAUDE.md is significantly stale.** The 14-module intelligence scaffold it describes hasn't existed for months. The `scheduler_v2` gotcha references a file that doesn't exist. The `actor_network.py` and `causation.py` LOC counts are fictional. Top priority for Wave 5 doc fix.
2. **Zero Python callers for all 9 sector network modules.** Only `sector_network_adapter.py` uses them, and only via `importlib` — they're opaque data dumps. The 19K LOC is just hand-written dicts.
3. **21 dead files, ~10,950 LOC of dead code.** Biggest dead-code cluster: `agent_arena.py` (583) + `insider_intel.py` (621) + `icij_actor_discovery.py` (288) + `whale_fingerprinter.py` (225) — all built as prototypes, never wired to production. Plus the 10 sector-network modules that are effectively data-only.
4. **Two "accuracy" implementations** — `contagion_backtest.compute_accuracy` and `postmortem.compute_accuracy` (implicit). They can drift; prior audit SYNTH-4 already flagged but the fix has not been applied.
5. **Four separate schedulers** (`ingestion/scheduler.py`, `intelligence/scheduler.py`, `alerts/scheduler.py`, `agents/scheduler.py`) — all intentional, but no doc explains which runs what.
6. **`rag/` package vs `intelligence/rag.py`** — high suspicion that one is a stale scaffold. Reconcile in Wave 3.
7. **[[Hermes Scheduler|Hermes operator]] drift.** `scripts/hermes_operator.py` references modules (`power_mapper`, `icij_linker`, `milestone_tracker`, `obsidian_agent`, `gdelt_news`) that the production `ingestion/scheduler.py` does not. Hermes operator and the main scheduler have diverged. Either hermes_operator is dead scripts or it needs to be reconciled against scheduler.py.

---

## Closing

Total savings if fully executed: **~39,450 LOC removed (~13% of 298,825)** across **76 files removed or collapsed**. The plan is dominated by data-file extraction (Wave 4), not by logic deletion. That means the risk concentration is "YAML parser bugs," not "broken business logic" — a much safer failure mode than an oracle scoring drift.

**Wave 1 is the safe gimme**: 10 files, ~2,018 LOC, zero non-test consumers, ready to execute.

**Wave 2 through Wave 4 require import updates** but are still bounded. The entire plan can be executed by a single agent in a few focused sessions, and each wave can be validated independently against the test suite (`tests/test_breaking_news.py` and the 1,148-test suite will catch most regressions).

The user's "clean break with clarity and hyperfocus" goal is achievable: after execution, the intelligence tree will drop from 143 → ~118 modules, the sector-network "templated code" anti-pattern disappears, dead code stops cluttering grep results, and every module has one obvious purpose.
