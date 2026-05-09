# GRID Module Overlap Audit

Generated: 2026-04-13
Source of truth: `docs/MODULE_INVENTORY.md` (649 modules, 298,825 LOC)
Clusters audited: 18

**Verdict distribution:**
- DUPLICATE (must merge): **1**
- OVERLAP_PARTIAL (must wire, keep both): **8**
- NOVEL_FOCUS (session work justified): **7**
- TEMPLATE_CLONES (refactor opportunity): **2**

**Key structural finding:** CLAUDE.md's "14-module intelligence scaffold" was a
historical artifact. The actor_network and [[Causation|causation]] modules it described (at 7,002
LOC and 2,387 LOC respectively) do **not exist at those sizes** — `actor_network.py`
is a 153-LOC façade, `causation.py` is a 26-LOC re-export shim. Both delegate to
submodule packages (`intelligence/actors/*`, `causation_core` + `causation_graph` +
`causation_scoring`). That means several "obvious overlaps" listed in the task brief
are actually clean Strategy-pattern splits, not duplications. The real duplication
risks are narrower than the brief suggested, but they still exist.

## Summary table

| # | Cluster | Canonical | Verdict | Priority | Synthesis task |
|---|---|---|---|---|---|
| 1 | chain_contagion | `chain_contagion.py` (new) + `supply_chain_edges` | OVERLAP_PARTIAL | HIGH | SYNTH-1 |
| 2 | supply_chokepoints | `supply_chokepoints.py` (new) | NOVEL_FOCUS | LOW | SYNTH-2 |
| 3 | cross_lens | `cross_lens.py` (new) | NOVEL_FOCUS | MEDIUM | SYNTH-3 |
| 4 | contagion_backtest | `postmortem.py` (extend) | OVERLAP_PARTIAL | HIGH | SYNTH-4 |
| 5 | sector_health | `sector_health.py` (new) | NOVEL_FOCUS | LOW | SYNTH-5 |
| 6 | fundamental_divergence | `fundamental_divergence.py` (new) | NOVEL_FOCUS | MEDIUM | SYNTH-6 |
| 7 | holder_deal_overlap | `holder_deal_overlap.py` (new) | NOVEL_FOCUS | LOW | SYNTH-7 |
| 8 | news_contagion_listener | `news_contagion_listener.py` (new) | OVERLAP_PARTIAL | HIGH | SYNTH-8 |
| 9 | supply_chain_edge_validator | `supply_chain_edge_validator.py` (new) | OVERLAP_PARTIAL | MEDIUM | SYNTH-9 |
| 10 | capital_flow_rollups | `capital_flow_rollups.py` (new) | NOVEL_FOCUS | MEDIUM | SYNTH-10 |
| 11 | pct_cogs_enrichment | `pct_cogs_enrichment.py` (new) | NOVEL_FOCUS | LOW | SYNTH-11 |
| 12 | ratio_percentiles | `ratio_percentiles.py` (new) | OVERLAP_PARTIAL | LOW | SYNTH-12 |
| 13 | trading/contagion_to_ticket | `options_recommender.py` (extend) | OVERLAP_PARTIAL | HIGH | SYNTH-13 |
| 14 | Causation trio | `causation.py` façade | NOT-A-DUPLICATE | LOW | SYNTH-14 |
| 15 | Actor graph trio | `intelligence/actors/*` package | OVERLAP_PARTIAL | MEDIUM | SYNTH-15 |
| 16 | institutional_map vs actor_network | separate concerns | NOVEL_FOCUS | LOW | SYNTH-16 |
| 17 | 9 sector network modules | shared static-seed contract | TEMPLATE_CLONES | MEDIUM | SYNTH-17 |
| 18 | Entity resolver triple | `normalization/resolver.py` (canonical) + `entity_resolver.py` (analytical) | TEMPLATE_CLONES / DUPLICATE | HIGH | SYNTH-18 |

---

## Detailed audits

### Cluster 1: chain_contagion

**Session-created:** `intelligence/chain_contagion.py` (727 LOC) — supply-chain shock BFS simulator. Public API: `simulate_contagion(engine, shock_node_id, shock_type, shock_magnitude, max_depth, pass_through)`. Reads `supply_chain_edges`, `supply_chain_nodes`, `capital_flows`. Already imports `intelligence.supply_chokepoints`. Already imported by `alerts/supply_chain_alerts.py`, `api/routers/contagion.py`, `intelligence/news_contagion_listener.py`.

**Pre-existing candidates:**
- `intelligence/causation_graph.py` (1179 LOC) — actor→price causal chain tracer. Reads `causal_chains`, `signal_sources`, `earnings_calendar`. Operates on the *actor signal graph*, not supply-chain edges.
- `intelligence/forensics.py` (968 LOC) — reconstructs a *single historical price move* from antecedent events. Reads `raw_series`, `options_daily_signals`, `feature_registry`. Post-hoc, not forward simulation.
- `intelligence/event_sequence.py` (999 LOC) — builds chronological event timelines. Not a propagation engine.
- `intelligence/deep_graph.py` (1772 LOC) — multi-hop actor overlap traversal. Reads `actors`, `causal_links`, `market_universe`. Not a magnitude-propagating shock simulator.

**Semantic check:**
- Does causation_graph.py already do upstream→downstream BFS over supply-chain edges? **NO** — it walks `causal_links` across signal sources.
- Does [[Forensics|forensics.py]] already reconstruct impact cascades? **NO** — it attributes a past move, doesn't forward-simulate.
- Does [[Event Sequence|event_sequence.py]] already support scenario simulation? **NO** — pure chronological aggregation.

**Signal overlap:**
- Shared table reads: *chain_contagion only* reads `supply_chain_edges` + `supply_chain_nodes` + `supply_shock_attributions`; *causation_graph only* reads `causal_chains` + `signal_sources`. Zero DB-level overlap.
- Shared inputs: both produce a "downstream impact ranking," so their *outputs* conflict for consumers that ask "what's at risk downstream of X?"

**Verdict:** **OVERLAP_PARTIAL** — chain_contagion operates on a fundamentally different graph (physical [[Supply Chain|supply chain]]) vs. the [[Causation|causation]] family (actor/signal graph). The risk is output contradiction: a downstream consumer could see a "HIGH impact" from causation_graph and "LOW impact" from chain_contagion for the same (ticker, horizon) and have no way to reconcile.

**Resolution task:** SYNTH-1 — Wire `chain_contagion.simulate_contagion` outputs through a unified contagion contract (`contracts/contagion_impact.json`?) that both causation_graph and [[Postmortem|postmortem.py]] already consume. `postmortem.py` already reads `contagion_backtest_results` and `contagion_predictions`, so the wiring half-exists. Extend causation_graph to join `supply_shock_attributions` when tracing a downstream impact chain so the two graphs converge at a single ticker-level impact score.

---

### Cluster 2: supply_chokepoints

**Session-created:** `intelligence/supply_chokepoints.py` (465 LOC) — scores each `supply_chain_edge` on chokepoint dimensions (substitution, buyer concentration, geography). Public API: `compute_chokepoint_score(edge, context)`, `score_all_edges(engine)`, `flag_chokepoint_nodes(engine, threshold)`. Writes back into `supply_chain_edges`. Imported by `chain_contagion.py`.

**Pre-existing candidates:**
- `intelligence/cross_reference.py` (1818 LOC) — "[[Cross Reference|lie detector]]" for government statistics (GDP vs physical). Reads `cross_reference_checks`, `raw_series`, `resolved_series`. **No supply-chain table access.**
- `intelligence/source_audit.py` (940 LOC) — data-source taxonomy audit (trust/agreement across catalog sources). **No supply-chain table access.**
- `intelligence/lever_pullers.py` (1377 LOC) — identifies market-moving *actors* (central banks, whales). **No supply-chain table access.**

**Semantic check:** None of the candidates touches `supply_chain_edges`. The name-collision in the brief ("chokepoint" vs "lever puller") is semantic confusion; the data lineage is entirely disjoint.

**Signal overlap:** Zero DB-level overlap. Zero function-name overlap.

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-2 — Ensure `supply_chokepoints.score_all_edges()` is added to the weekly [[Hermes Scheduler|Hermes]] schedule (`ingestion/scheduler.py`) so scores stay fresh for `chain_contagion`. No merging.

---

### Cluster 3: cross_lens

**Session-created:** `intelligence/cross_lens.py` (713 LOC) — supplier→buyer *price-series* cross-correlation detector. Reads `supply_chain_edges`, `raw_series`. Writes `supply_shock_attributions`. Imported by `supply_chain_edge_validator.py` and `api/routers/attributions.py`.

**Pre-existing candidates:**
- `intelligence/thesis_tracker.py` (1014 LOC) — scores snapshotted thesis hypotheses. Writes `thesis_postmortems`, `thesis_snapshots`. Does not compute upstream-downstream correlation.
- `intelligence/causation_graph.py` — walks `causal_chains`, not price-series lag correlation.
- `intelligence/trust_scorer.py` — [[Trust Scorer|Bayesian trust]] on source signals, not return-series correlation.

**Semantic check:** cross_lens is specifically "lagged return correlation between two supply-chain nodes with COGS weighting." No other module computes lagged return correlation on supply-chain pairs.

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-3 — Emit `supply_shock_attributions` rows to the unified signal bus so `trust_scorer.register_signal` picks them up as a new source_type `"supply_chain_correlation"`. Wire into weekly cron alongside `supply_chain_edge_validator.run_weekly`.

---

### Cluster 4: contagion_backtest

**Session-created:** `intelligence/contagion_backtest.py` (380 LOC) — scores `contagion_predictions` against realized `raw_series` price moves. Writes `contagion_backtest_results`. Public API: `score_predictions`, `score_all_windows`.

**Pre-existing candidates:**
- `intelligence/postmortem.py` (1818 LOC) — **was extended this session** to add `apply_contagion_feedback(since_hours, dry_run)`. Already reads `contagion_backtest_results` and `contagion_predictions`, writes `supply_chain_edge_adjustments`. This is the canonical failure-analysis module.
- `intelligence/thesis_tracker.py` (1014 LOC) — general thesis postmortems. Different scope (news/whale thesis, not contagion).
- `oracle/calibration.py` — Brier/ECE for oracle-level predictions, different table set.

**Semantic check:**
- Does [[Postmortem|postmortem.py]] already score contagion predictions? **PARTIAL** — it *consumes* the backtest results but does not *generate* them. The responsibility split is clean: `contagion_backtest.py` writes the scorecard, `postmortem.py` reads it and produces written analysis.
- Risk: contagion_backtest computes `compute_accuracy(predicted_margin_impact_pct, actual_price_move_pct)` and [[Postmortem|postmortem]] computes its own accuracy from oracle_predictions. **Two accuracy definitions can drift.**

**Verdict:** **OVERLAP_PARTIAL** — clean responsibility split but the accuracy metrics risk divergence.

**Resolution task:** SYNTH-4 — Extract a shared `intelligence/_accuracy.py` helper (keep the file small, no new intelligence module) or move `compute_accuracy` from contagion_backtest into [[Postmortem|postmortem]]'s public API. All callers reference one function. Ensures "failed contagion prediction" means the same thing to both modules.

---

### Cluster 5: sector_health

**Session-created:** `intelligence/sector_health.py` (559 LOC) — composite sector health score. Reads `capital_flows`, `insider_trades`, `congressional_trades`, `dark_pool_weekly`, `supply_chain_edges`. Writes `sector_health_snapshots`. Exposed at `api/routers/sector_health.py`.

**Pre-existing candidates:**
- `intelligence/hypothesis_engine.py` (2137 LOC) — discovers/scores/kills hypotheses, writes `discovered_hypotheses`. Not a composite score.
- `oracle/engine.py` — 5-model ensemble for oracle-level direction/magnitude predictions, not sector health.
- `intelligence/trust_scorer.py` — source-level [[Trust Scorer|Bayesian trust]], not sector aggregate.

**Semantic check:** No pre-existing module computes a single-number sector health rollup across insider, darkpool, and supply-chain inputs.

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-5 — Register `sector_health_snapshots` with `signal_registry` so oracle/engine can consume it as an input feature rather than a parallel scoring track. Stops sector_health from becoming a silent parallel oracle.

---

### Cluster 6: fundamental_divergence

**Session-created:** `intelligence/fundamental_divergence.py` (570 LOC) — tricotomy of fundamental-vs-price divergence per (sector, ticker). Writes `fundamental_divergence`. Imports `analysis.sector_map`.

**Pre-existing candidates:**
- `alpha_research/*` (20+ modules, 3,426 LOC total) — quant alpha research suite, heartbeat, adapters. `alpha_research/signals/credit_cycle.py`, `signals/quanta_alpha.py`, `signals/macro_regime.py` — all are *signal generators*, none compute a fundamental-vs-price residual.
- `discovery/orthogonality.py` (549 LOC) — feature [[Orthogonality Audit|orthogonality audit]], not divergence detection.
- `features/lab.py` (671 LOC) — feature transformation engine. Has `spread`, `ratio`, `zscore_normalize` primitives but no fundamental-divergence composite.

**Semantic check:** None touches `capital_flows` + `raw_series` to compute fundamental-vs-price divergence. Novel.

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-6 — Register the `fundamental_divergence` table with `signal_registry` as source type `"fundamental_divergence"` so it flows through `trust_scorer` and `oracle/engine`. Otherwise it's a silo.

---

### Cluster 7: holder_deal_overlap

**Session-created:** `intelligence/holder_deal_overlap.py` (553 LOC) — detects institutional holders that pre-position into a target before M&A announcement. Reads `capital_flows`, `institutional_holdings`, `corporate_actions_parser`. Writes `holder_deal_overlap`. Exposed at `api/routers/actor_detail.py`.

**Pre-existing candidates:**
- `intelligence/actor_network.py` (153 LOC) — pure façade over `intelligence/actors/*` package. Does not compute pre-positioning.
- `intelligence/sleuth.py` (1245 LOC) — investigative research engine. Generates `investigation_leads` but does not do M&A pre-positioning detection specifically.
- `intelligence/institutional_map.py` (1510 LOC) — builds institutional_graph for pension/hedge-fund mapping. Reads PII-style mappings, not `institutional_holdings` time series.
- `intelligence/causation_graph.py` — walks causal chains on signals, different scope.

**Semantic check:** No pre-existing module joins `institutional_holdings` to `corporate_actions_parser` to detect pre-announcement positioning. Truly novel.

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-7 — Emit each `holder_deal_overlap` row as a signal_sources entry (`source_type='pre_positioning'`) so trust_scorer and the oracle pick it up automatically. Without this, the detector's output rots unconsumed.

---

### Cluster 8: news_contagion_listener

**Session-created:** `intelligence/news_contagion_listener.py` (638 LOC) — scans `news_articles`, resolves supply-chain entities via `supply_chain_nodes`, then kicks off `chain_contagion.simulate_contagion`. Writes `contagion_predictions`. Already imports `intelligence.chain_contagion` and `analysis.sector_map`.

**Pre-existing candidates:**
- `intelligence/breaking_news.py` (341 LOC) — [[GDELT]]-based breaking-news monitor, writes `signal_data`. Not supply-chain aware.
- `intelligence/deal_detector.py` (861 LOC) — news-based M&A detection, writes `deal_pipeline`. Not contagion.
- `intelligence/business_news_parser.py` (804 LOC) — generic business-event parser, writes `business_events`. Not contagion.
- `intelligence/news_momentum.py` (903 LOC) — sentiment velocity/acceleration, writes `news_momentum`. Not contagion.
- `intelligence/news_impact.py` (978 LOC) — price-move attribution from news. Writes `news_impact_expectations`. Different direction of arrow.

**Semantic check:** Five pre-existing news scanners exist. None of them resolves entities against `supply_chain_nodes` or triggers a propagation simulator. **But all five scan `news_articles`** — so we have 6 scanners reading the same table independently.

**Verdict:** **OVERLAP_PARTIAL** — not a duplicate of function, but a duplicate of the *scanner pattern*. All 6 modules independently poll `news_articles`. Risk: inconsistent freshness, inconsistent ticker resolution (news_ticker_[[Conflict Resolution|resolver.py]] exists and none of them may use it).

**Resolution task:** SYNTH-8 — Introduce a single news event bus (`intelligence/_news_fanout.py` as a small helper, NOT a new intelligence module — add as a package-private helper inside an existing file). Each of the 6 scanners subscribes instead of polling. Short-term: verify all 6 use `news_ticker_resolver.resolve_tickers` so ticker sets agree.

---

### Cluster 9: supply_chain_edge_validator

**Session-created:** `intelligence/supply_chain_edge_validator.py` (450 LOC) — validates each `supply_chain_edges` row by checking upstream-downstream price correlation; marks weak relationships. Imports `intelligence.cross_lens`. Writes back to `supply_chain_edges`.

**Pre-existing candidates:**
- `intelligence/cross_reference.py` (1818 LOC) — government-stat [[Cross Reference|lie detector]]. Does not touch `supply_chain_edges`.
- `intelligence/source_audit.py` (940 LOC) — data-source trust audit. Does not touch `supply_chain_edges`.
- `intelligence/resolution_audit.py` (961 LOC) — data resolution supervisor (duplicates, staleness, sanity checks on `resolved_series`). Generic, does not target supply-chain edges.
- `intelligence/pct_cogs_enrichment.py` (1751 LOC, also session-created) — also writes `supply_chain_edges`.

**Semantic check:** Three session modules (`supply_chain_edge_validator`, `pct_cogs_enrichment`, `chain_contagion` via `supply_chokepoints`) all write to `supply_chain_edges`. **Three writers for one table.** `resolution_audit.py` has the general infrastructure for "audit table quality" but not edge-specific.

**Verdict:** **OVERLAP_PARTIAL** — legitimate scope (edge correlation validation) but multiple writers to the same table risks race conditions / column collision. Needs a write lease / column partitioning document.

**Resolution task:** SYNTH-9 — Document the `supply_chain_edges` column ownership matrix: which session module owns which column. Add a `supply_chain_edges.last_updated_by` field so we can see which writer touched the row last. Optional: move edge-validation logic into `resolution_audit.py` as a specialised check function so all audit findings live in one place.

---

### Cluster 10: capital_flow_rollups

**Session-created:** `intelligence/capital_flow_rollups.py` (339 LOC) — computes TTM windows + announcement fold-in for `capital_flows`. Writes back to `capital_flows`. Public API: `compute_ttm`, `fold_announcements`, `run_all`.

**Pre-existing candidates:**
- `intelligence/dollar_flows.py` (1081 LOC) — normalizes signal-level flows into the `dollar_flows` table. Reads `raw_series`, `resolved_series`, `signal_sources`. **Different table, different granularity** — per-signal not per-company-quarter.
- `analysis/flow_aggregator.py` (1148 LOC) — sector / actor tier / time aggregations on top of `dollar_flows`. Reads `dollar_flows`. Consumer, not derivation.
- `analysis/flow_thesis.py` (22 LOC) — façade; not a computation engine.
- `analysis/capital_flows.py` (893 LOC) — GRID Capital Flow Research Engine (LLM narrative generation). Writes `capital_flow_snapshots`. **Different table.**

**Semantic check:** CLAUDE.md mentioned `flow_aggregator`/`flow_thesis` as if they lived in `intelligence/`. They live in `analysis/`. `capital_flow_rollups` is targeting company-level fundamental rollups (TTM COGS, etc.) from the `capital_flows` table, which is distinct from the `dollar_flows` table. **Tables sound similar, payloads are different.**

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-10 — Write a short `docs/TABLES.md` note distinguishing `capital_flows` (quarterly company fundamentals) from `dollar_flows` (daily signal-normalized flows) from `capital_flow_snapshots` (LLM narrative payloads) so future agents don't blur them. Rename `capital_flow_rollups.py` → `company_financial_rollups.py` if acceptable (name ambiguity is the main risk here).

---

### Cluster 11: pct_cogs_enrichment

**Session-created:** `intelligence/pct_cogs_enrichment.py` (1751 LOC) — LLM-driven enrichment that estimates `pct_cogs` for each row in `supply_chain_edges`. Writes `supply_chain_edges`, `supply_chain_enrichment_log`, `supply_chain_nodes`.

**Pre-existing candidates:**
- `features/lab.py` (671 LOC) — generic feature transformation (zscore, slope, lag, ratio, spread). Does not do LLM enrichment.
- `intelligence/sleuth.py` (1245 LOC) — investigative research via LLM, writes `investigation_leads`. Different output table and different purpose.
- `intelligence/company_analyzer.py` (1079 LOC) — LLM-driven company profiling, writes `company_profiles`. Not supply-chain edge enrichment.

**Semantic check:** Zero overlap. supply_chain-specific LLM enrichment is novel.

**Verdict:** **NOVEL_FOCUS.**

**Resolution task:** SYNTH-11 — Make sure `pct_cogs_enrichment.PctCogsEnricher` respects `intelligence.freshness_guard` + checks `source_trust_config` so LLM-generated values are marked `confidence='derived'` (not `confirmed`) in output columns. Currently there's no sign it labels confidence.

---

### Cluster 12: ratio_percentiles

**Session-created:** `intelligence/ratio_percentiles.py` (536 LOC) — per-sector / per-subsector percentile rankings for `capital_flows` ratio columns. Uses `utils.ttl_cache`. Imported by `api/routers/capital_flow.py`.

**Pre-existing candidates:**
- `features/lab.py` — has `zscore_normalize` (standardization), but no percentile rank.
- `discovery/orthogonality.py` — [[Orthogonality Audit|orthogonality audit]], not percentile ranking.

**Semantic check:** percentile_rank is a common primitive; features/lab contains zscore but not percentile. This is a legitimate gap but a small one — 536 LOC for "percentile rank by group" is high.

**Verdict:** **OVERLAP_PARTIAL** — should live in `features/lab.py` as primitives, not as a standalone intelligence module.

**Resolution task:** SYNTH-12 — Move the core percentile-rank primitives (`compute_sector_percentiles`, `get_percentile`) into `features/lab.py` as public functions. Keep `ratio_percentiles.py` only as a thin scheduling shim if it runs on a cron. Savings: ~400 LOC of infra that belongs in features.

---

### Cluster 13: trading/contagion_to_ticket

**Session-created:** `trading/contagion_to_ticket.py` (733 LOC) — converts `contagion_predictions` rows into concrete options tickets (strike picker, expiry picker, Kelly fraction, journal persistence). Imports `journal.log`, `physics.dealer_gamma`. Exposed at `api/routers/trade_tickets.py`.

**Pre-existing candidates:**
- `trading/options_recommender.py` (1380 LOC) — THE canonical options recommendation engine. Already has `OptionsRecommendation` dataclass with `to_trade_ticket()` method and `all_sanity_passed` gate. Already writes `options_recommendations`. Imports `physics.dealer_gamma`, `discovery.options_scanner`.
- `trading/signal_executor.py` (297 LOC) — executes paper trades from signals.

**Semantic check:**
- Does options_recommender.py already convert signals to trade tickets? **YES** — `to_trade_ticket()` method already exists.
- Does it support Kelly sizing? **YES** — `options_recommender.OptionsRecommendation` has that surface area.
- Does it consume `contagion_predictions`? **NO** — it consumes `options_scanner` + `dealer_gamma` outputs.

**Verdict:** **OVERLAP_PARTIAL** (close to DUPLICATE). The two modules independently implement strike/expiry/Kelly logic. If they diverge, two different strikes ship for the same prediction. HIGH risk of contradictory trade tickets.

**Resolution task:** SYNTH-13 — Delete `trading/contagion_to_ticket.py`'s strike/expiry/Kelly logic and have it call into `options_recommender.OptionsRecommender.generate_recommendations(contagion_predictions=...)`. The adapter layer (take a contagion row → build an input spec) stays in `contagion_to_ticket.py` but ALL pricing/sizing lives in `options_recommender.py`. **This is the #1 duplication risk in the session.**

---

### Cluster 14: Causation trio

**Candidates:**
- `intelligence/causation.py` — **26 LOC**. Pure re-export façade. Imports `causation_core`, `causation_graph`, `causation_scoring`.
- `intelligence/causation_core.py` (195 LOC) — `CausalLink`, `CausalChain` dataclasses + `ensure_table`.
- `intelligence/causation_graph.py` (1179 LOC) — `trace_causal_chain`, `find_longest_chains`. Imports `causation_core`, `causation_scoring`, `forensics`, `rag`.
- `intelligence/causation_scoring.py` (1090 LOC) — `find_causes`, `batch_find_causes`. Imports `causation_core`, `freshness_guard`, `lever_pullers`, `rag`, `actor_signal_bridge`.

**Semantic check:** The three *do* reference each other (via imports). The brief's claim "three [[Causation|causation]] modules never referencing each other" is **wrong**. This is a clean Strategy-pattern split (dataclasses / graph walker / scorer), plus a 26-line facade. CLAUDE.md's "2,387 LOC [[Causation|causation.py]]" never existed — that number was stale.

**Verdict:** **NOT-A-DUPLICATE** — correctly factored. The real problem is that CLAUDE.md still claims a monolithic `causation.py` exists.

**Resolution task:** SYNTH-14 — Update CLAUDE.md's intelligence section to reflect the actual split (core/graph/scoring/facade). Already flagged as generally stale, but fix the [[Causation|causation]] entry explicitly. No code changes.

---

### Cluster 15: Actor graph trio

**Candidates:**
- `intelligence/actor_network.py` — **153 LOC.** Pure façade over `intelligence/actors/*` package + `pocket_lining` + `wealth_tracker`. The brief's "7,002 LOC actor_network.py" is CLAUDE.md stale. The LOC lives in `intelligence/actors/seed_data.py` (5619 LOC of static seed data).
- `intelligence/deep_graph.py` (1772 LOC) — multi-hop traversal. Reads `actors`, `causal_links`, `market_universe`, writes `graph_overlaps`. **Imports `intelligence.actor_network`.** Consumes the façade.
- `intelligence/actor_discovery.py` (3533 LOC) — automated discovery & enrichment at 250K+ scale. Writes `actor_connections`, `actors`. Orthogonal to deep_graph: discovery vs. traversal.

**Semantic check:**
- `actor_network` is the data-access façade.
- `actor_discovery` writes new rows.
- `deep_graph` walks existing rows for analysis.

They overlap only at the import-graph level: both `deep_graph` and `actor_discovery` import `actor_network`. That's correct layering, not duplication.

**Verdict:** **OVERLAP_PARTIAL** — the tables they write (`actors`, `actor_connections`) are shared with `signal_backlinker.py`, `actor_researcher.py`, `actor_ingest.py`, and seven other modules. **Six+ writers to `actors`.** No single source of truth for a canonical write path.

**Resolution task:** SYNTH-15 — Establish a single `intelligence/actors/writes.py` (inside existing package, not a new top-level module) that is the only path which inserts to `actors` and `actor_connections`. All current writers become callers. Add a `writer_source` column for audit.

---

### Cluster 16: institutional_map vs actor_network

**Candidates:**
- `intelligence/institutional_map.py` (1510 LOC) — private credit, hedge funds, pensions. `build_institutional_graph`, `trace_pension_dollars`, `find_conflicts_of_interest`. **No table writes** — pure derivation / query layer.
- `intelligence/actor_network.py` (153 LOC façade) — reads `actors` + writes `wealth_flows` via `wealth_tracker`.

**Semantic check:** institutional_map does not touch the `actors` table. It has its own in-memory graph of pension-fund → LP → GP relationships. The overlap is only semantic ("both involve institutions") not structural.

**Verdict:** **NOVEL_FOCUS** — they're genuinely different slices. No synthesis needed other than cross-linking.

**Resolution task:** SYNTH-16 — Add `intelligence.institutional_map.build_institutional_graph` results as rows in the `actor_connections` table (via the SYNTH-15 canonical writer) so the two graphs converge. Currently they're two disconnected graphs in the same DB.

---

### Cluster 17: 9 sector network modules

**Candidates (all static-data modules with hand-curated dicts):**
- `tech_monopoly_network.py` (2370 LOC)
- `energy_network.py` (2273 LOC)
- `media_network.py` (2172 LOC)
- `commodities_agriculture_network.py` (2766 LOC)
- `banking_network.py` (1692 LOC)
- `real_estate_network.py` (1792 LOC)
- `swf_network.py` (1422 LOC)
- `pharma_network.py` (1271 LOC)
- `defense_contractors.py` (1183 LOC)

Total: **~19,000 LOC.** All expose `get_<sector>_network()` + `get_entity(key)` + `get_<sector>_lobbying_summary()` etc. All are hand-curated Python dicts — zero DB reads, zero DB writes.

**Semantic check:** Template clones. Every module independently implements the same dict-of-dicts contract for its sector, with sector-specific keys.

**Verdict:** **TEMPLATE_CLONES.**

**Resolution task:** SYNTH-17 — Define a `intelligence/actors/sector_graph.py` schema dataclass. Convert each of the 9 modules into a YAML/JSON file plus a small Python shim that loads the dict. Target: reduce 19,000 LOC → ~3,000 LOC shared loader + 9 small data files. No semantics lost.

---

### Cluster 18: Entity resolver triple

**Candidates:**
- `intelligence/entity_resolver.py` (1411 LOC) — analytical entity resolution: phonetic keys, Levenshtein, Jaro-Winkler, `resolve`, `build_resolution_index`, `find_connections`. Writes `entity_resolution` table. Used in the actor/intelligence domain. Reads `actors`, `signal_data`, `wealth_flows`, `oracle_predictions`.
- `normalization/entity_map.py` (1055 LOC) — `EntityMap` class for feature-source mapping ([[BLS]] codes ↔ feature_id etc.). Writes *nothing directly* — `load_v2_mappings` populates the mapping table via `resolver.py`. Used in the data-ingestion domain. Reads `feature_registry`, `resolved_series`.
- `normalization/resolver.py` (322 LOC) — `Resolver` class, pending-feature-source [[Conflict Resolution|conflict resolution]]. Writes `resolved_series`. Imports `normalization.entity_map`.

**Semantic check:**
- `normalization/*` = feature/source disambiguation (which [[FRED]] code maps to which canonical feature).
- `intelligence/entity_resolver.py` = actor/person/company name disambiguation (which "Jamie Dimon" row is canonical).

They are **different disambiguation domains**. BUT:
- Both call themselves "entity resolver."
- Both contain `normalize_name` / `canonical_key` kind of logic.
- `intelligence/entity_resolver.py` would be useful to `normalization/entity_map.py` when a new source arrives with an unfamiliar entity label, and vice versa.

**Verdict:** **TEMPLATE_CLONES** at the fuzzy-match-primitives level, NOT at the pipeline level. HIGH priority because the two domains will eventually need to reconcile entities across feature sources AND actor sources, and having two fuzzy matchers with different thresholds creates silent bifurcation.

**Resolution task:** SYNTH-18 — Extract the shared primitives (`phonetic_key`, `levenshtein_distance`, `jaro_similarity`, `jaro_winkler_similarity`, `name_similarity`, `strip_accents`, `normalize_name`, `canonical_key`) into a single helper module (`normalization/fuzzy_match.py` — extend existing package, no new intelligence module). Both `entity_resolver.py` and `entity_map.py` import from it. Keep the two higher-level resolvers distinct because their domains are distinct.

---

## Synthesis task queue

Format: **SYNTH-N**: [canonical] [action] — [why]

- **SYNTH-1** [HIGH]: `chain_contagion.py` — emit unified contagion-impact contract; join with `causation_graph` downstream impacts — two contagion graphs must not produce contradictory ticker-level impact scores.
- **SYNTH-2** [LOW]: `supply_chokepoints.py` — add weekly [[Hermes Scheduler|Hermes]] schedule entry for `score_all_edges()` — keep chokepoint scores fresh so chain_contagion sees up-to-date weights.
- **SYNTH-3** [MEDIUM]: `cross_lens.py` — register `supply_shock_attributions` as signal source in `trust_scorer` — otherwise this module's output rots unconsumed.
- **SYNTH-4** [HIGH]: `postmortem.py` — extract shared `compute_accuracy()` between contagion_backtest and postmortem — two accuracy definitions diverging would silently miscalibrate the oracle.
- **SYNTH-5** [MEDIUM]: `sector_health.py` — register `sector_health_snapshots` with `signal_registry` so oracle consumes it as a feature — stops parallel scoring track.
- **SYNTH-6** [MEDIUM]: `fundamental_divergence.py` — register `fundamental_divergence` table with `signal_registry` — otherwise oracle never sees it.
- **SYNTH-7** [MEDIUM]: `holder_deal_overlap.py` — emit rows as `source_type='pre_positioning'` signal_sources — wire detector output into trust_scorer/oracle.
- **SYNTH-8** [HIGH]: news scanners (6 of them) — introduce single news fanout + force all 6 to use `news_ticker_resolver` — stops inconsistent ticker resolution across parallel polls of `news_articles`.
- **SYNTH-9** [MEDIUM]: `supply_chain_edges` — document column ownership matrix; add `last_updated_by` audit column — three session modules write to same table, needs a lease.
- **SYNTH-10** [LOW]: `capital_flow_rollups.py` — add `docs/TABLES.md` clarifying `capital_flows` vs `dollar_flows` vs `capital_flow_snapshots`; consider rename to `company_financial_rollups.py`.
- **SYNTH-11** [LOW]: `pct_cogs_enrichment.py` — label LLM-derived values with `confidence='derived'` per confidence-labels feedback rule.
- **SYNTH-12** [MEDIUM]: `features/lab.py` — move `compute_sector_percentiles` + `get_percentile` from `ratio_percentiles.py` into `features/lab.py`; shrink ratio_percentiles to a cron shim.
- **SYNTH-13** [HIGH]: `trading/contagion_to_ticket.py` — gut its strike/expiry/Kelly math and delegate to `options_recommender.OptionsRecommender` — #1 contradiction risk: two Kelly implementations, two strike pickers.
- **SYNTH-14** [LOW]: `CLAUDE.md` — update intelligence section to reflect actual [[Causation|causation]] split (core/graph/scoring/facade) and actor_network façade pattern; stop quoting phantom LOC counts.
- **SYNTH-15** [MEDIUM]: `intelligence/actors/writes.py` (inside existing package) — establish canonical writer for `actors` + `actor_connections`; add `writer_source` audit column; migrate 6+ current writers to call through it.
- **SYNTH-16** [LOW]: `intelligence/institutional_map.py` — emit `build_institutional_graph` results via SYNTH-15 canonical writer — converge the two actor graphs in one DB.
- **SYNTH-17** [MEDIUM]: 9 sector network modules — convert to YAML/JSON data files + single loader; target ~19K → ~3K LOC.
- **SYNTH-18** [HIGH]: `normalization/fuzzy_match.py` (extend existing package) — extract shared fuzzy-match primitives from `entity_resolver.py` and `entity_map.py` — stop silent threshold bifurcation across domains.

---

## Session-created modules that should be restructured

- **DELETE/MERGE logic**: `trading/contagion_to_ticket.py` — move strike/expiry/Kelly math into `trading/options_recommender.py` and keep only the adapter shim (SYNTH-13). **Highest contradiction risk in the session.**
- **SHRINK**: `intelligence/ratio_percentiles.py` — migrate core math into `features/lab.py`, reduce to cron shim (SYNTH-12).
- **RENAME**: `intelligence/capital_flow_rollups.py` → `intelligence/company_financial_rollups.py` — current name collides semantically with `dollar_flows` and `flow_aggregator` (SYNTH-10).
- **LABEL**: `intelligence/pct_cogs_enrichment.py` — no delete/rename, but must mark LLM outputs as `confidence='derived'` (SYNTH-11).

All other 9 session-created modules are legitimate novel focus or legitimate partial overlaps that need wiring, not deletion.

---

## Top 3 most-urgent synthesis tasks

1. **SYNTH-13** — `contagion_to_ticket` vs `options_recommender` duplicated strike/expiry/Kelly logic. Two pricing engines will silently disagree the first time they ship different tickets for the same (ticker, horizon).
2. **SYNTH-1** — `chain_contagion` (supply-chain graph) vs `causation_graph` (actor-signal graph) will produce contradictory downstream-impact rankings for the same ticker with no reconciliation layer.
3. **SYNTH-4** — `contagion_backtest.compute_accuracy` and `postmortem` each define "accuracy" independently. [[Oracle Calibration|Oracle calibration]] will drift silently if one changes and the other does not.

All three are HIGH priority because they produce contradictory *quantitative* predictions that the oracle feeds on. Everything else is wiring/cosmetic/LOC reduction.
