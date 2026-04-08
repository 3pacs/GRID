# GRID Codebase Index
<!-- Auto-generated. Rebuild with /grid-orient. Last updated: 2026-04-06 -->

## Intelligence Module Function Index

| Module | Function | Returns | Use For |
|--------|----------|---------|---------|
| hypothesis_engine | `auto_discover()` | list[dict] | Full pipeline: detect→hunt→generate→score |
| hypothesis_engine | `score_all()` | list[dict] | Batch score active hypotheses past window |
| hypothesis_engine | `score_hypothesis(id)` | dict | Score single hypothesis |
| hypothesis_engine | `_evaluate_criteria(criteria, created_at)` | "confirmed"/"invalidated"/"inconclusive" | Test hypothesis against data |
| hypothesis_engine | `_check_kills(id,ptype,criteria,created,conf,tested,outcome)` | kill_reason or None | 11 kill types |
| hypothesis_engine | `get_stats(engine)` | dict | Full engine stats + top hypotheses |
| lever_pullers | `identify_lever_pullers(engine)` | list[LeverPuller] | Top 50 actors by influence×trust |
| lever_pullers | `get_lever_context_for_ticker(engine, ticker)` | dict | Per-ticker: active_pullers, motivations, convergence |
| lever_pullers | `find_lever_convergence(engine)` | list[dict] | Multi-puller agreement events |
| trust_scorer | `get_trusted_sources(engine, min_signals=5, min_trust=0.6)` | list[SourceScore] | High-confidence signal sources |
| trust_scorer | `detect_convergence(engine)` | list[dict] | Multi-source agreement |
| trust_scorer | `run_trust_cycle(engine)` | dict | Full: score→update→convergence→report |
| causation_scoring | `find_causes(engine, actor, action, ticker, date)` | list[CausalLink] | Root cause attribution |
| causation_scoring | `get_suspicious_trades(engine, days=90)` | list[dict] | Information leakage detection |
| forensics | `analyze_move(engine, ticker, date, lookback=14)` | ForensicReport | Price move reconstruction |
| cross_reference | `run_all_checks(engine)` | LieDetectorReport | 8-category govt stats vs reality |
| agent_arena | `run_arena(engine)` | dict | 10-agent consensus with accuracy weights |
| sleuth | `_llm_investigate(question, evidence, context)` | Investigation | Investigative detective |
| company_analyzer | `analyze_company(engine, ticker)` | CompanyProfile | Full influence profile |
| thesis_tracker | `snapshot_thesis(engine)` | ThesisSnapshot | Current thesis state |
| thesis_tracker | `score_old_theses(engine)` | list[dict] | Score theses vs actual SPY |

## Canvas & Graph (V5 Phase 1-3)

| Module | File | Purpose |
|--------|------|---------|
| Canvas CRUD | `api/routers/canvas.py` (facade) | Board/node/edge CRUD at /api/v1/canvas/* |
| Canvas Core | `api/routers/canvas_core.py` | Board list/create/get/update/delete |
| Canvas Graph | `api/routers/canvas_graph.py` | Node/edge CRUD + bulk PUT /graph |
| Canvas Expand | `api/routers/canvas_expand.py` | Expand network (BFS), suggest connections, shortest path |
| Graph Store | `store/graph.py` | Apache AGE Cypher wrapper (expand, shortest_path, community) |
| Canvas View | `pwa/src/views/Canvas.jsx` | React Flow workspace with 5 node types |
| Canvas Store | `pwa/src/stores/canvasStore.js` | Zustand slice (boards, nodes, edges) |
| Node Types | `pwa/src/components/canvas/*.jsx` | ActorNode, CompanyNode, HypothesisNode, SignalNode, NoteNode |
| SendToCanvas | `pwa/src/components/SendToCanvas.jsx` | Reusable cross-view "Send to Canvas" button |
| Context Menu | `pwa/src/components/canvas/CanvasContextMenu.jsx` | Right-click: Expand, Suggest, Remove, Color |

**Apache AGE:** Installed on PG14, `shared_preload_libraries = 'age'`. Graph `grid_graph` with 5 vertex + 8 edge labels, 4141 actors synced. Cypher queries via `store/graph.py`.

**Canvas tables:** `canvas_boards`, `canvas_nodes`, `canvas_edges` (migration `a1b2c3d4e5f6`)

## DB Schema Quick Reference

| Table | Key Columns | Notes |
|-------|-------------|-------|
| discovered_hypotheses | id, thesis, pattern_type, evidence(J), test_criteria(J), confidence, status, role, pair_id, kill_reason, killed_at | thesis/antithesis pairs |
| hypothesis_postmortems | hypothesis_id, kill_reason, evidence(J), thesis_text, antithesis_text, confidence_at_death, lifespan_days | Kill autopsy |
| signal_data | signal_type, signal_date, ticker, actor, direction, magnitude, confidence | All signals |
| realtime_candles | symbol, asset_class, interval, ts, OHLCV, vwap, trade_count, source | 5-min candles |
| company_profiles | ticker, profile(JSON), suspicion_score, sector | Influence profiles |
| actors | id, name, category, ... | Named actors |
| analytical_snapshots | category, snapshot_date, actor, ticker, title, summary, data, confidence | Intelligence snapshots |
| oracle_predictions | ticker, direction, confidence, actual_move_pct, scored_at | Oracle model predictions |
| decision_journal | model_version_id, outcome_recorded_at, ... | Immutable decision log |
| signal_sources | source_type, source_id, ticker, direction, ... | Raw signal sources (trust scored) |
| causation_links | signal_id, probable_cause, cause_type, probability, lead_time_days | Root causes |
| canvas_boards | id, name, description, created_at, updated_at | Investigation boards |
| canvas_nodes | board_id, node_id, node_type, entity_id, label, position_x/y, data(J) | Canvas entities |
| canvas_edges | board_id, edge_id, source_node_id, target_node_id, edge_type, data(J) | Canvas connections |

## LLM Prompt Injection Points (for context layer work)

| Module | File | Injection Point | Add What |
|--------|------|-----------------|----------|
| Market Briefing | ollama/market_briefing.py | After `_build_data_context()` ~L302 | Hypotheses + postmortems + company profiles |
| Sleuth | intelligence/sleuth.py | In `_llm_investigate()` ~L201 | Kill postmortems + company profiles |
| Audio Briefing | intelligence/audio_briefing.py | In `_build_briefing_prompt()` ~L297 | Postmortem lessons + actor positions |
| Market Diary | intelligence/market_diary.py | In `_build_diary_prompt()` ~L383 | Active hypotheses + recent kills |
| Thesis Tracker | intelligence/thesis_tracker.py | In `_get_llm_thesis_postmortem()` ~L962 | Kill postmortem summaries |
| Cross Reference | intelligence/cross_reference.py | In `_generate_narrative()` ~L1140 | Historical divergence context |

## Server Operations

| Service | Port | Log | Restart |
|---------|------|-----|---------|
| grid-api | :8000 | journalctl -u grid-api | sudo systemctl restart grid-api |
| grid-realtime | daemon | /data/grid/logs/grid-realtime.log | sudo systemctl restart grid-realtime |
| grid-scheduler | daemon | /data/grid/logs/grid-scheduler.log | sudo systemctl restart grid-scheduler |
| grid-hermes | daemon | journalctl -u grid-hermes | sudo systemctl restart grid-hermes |
| grid-micro-classifier | :8082 | /data/grid/logs/micro-classifier.log | sudo systemctl restart grid-micro-classifier |
| grid-micro-narrator | :8083 | /data/grid/logs/micro-narrator.log | sudo systemctl restart grid-micro-narrator |
| grid-micro-extractor | :8084 | /data/grid/logs/micro-extractor.log | sudo systemctl restart grid-micro-extractor |
| grid-micro-mapper | :8085 | /data/grid/logs/micro-mapper.log | sudo systemctl restart grid-micro-mapper |

SSH: `ssh grid-svr` (User: grid, Tailscale 100.75.185.36)
DB: `PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost`
Public: https://grid.stepdad.finance

## Hypothesis Engine State

- Kill taxonomy: 14 named kills (ANTITHESIS_CONFIRMED, CONFIDENCE_COLLAPSED, EXPIRED, PATTERN_BROKEN, CORRELATION_COLLAPSED, WRONG_DIRECTION, NO_MOVE, NO_FOLLOW_THROUGH, FALSE_SPIKE, ACTOR_RETREATED, NO_CATALYST, LEVER_DIVERGED, TRUST_COLLAPSED, CAUSATION_INVALIDATED)
- Pattern types: lead_lag, convergence, volume_anomaly, actor_shift
- Scoring: Bayesian beta posterior, per-hypothesis windows (lag_days/window_days)
- Kills: universal (conf<0.10 after 3 tests, 2x window expired) + type-specific
- Auto-antithesis: every thesis gets an inverse, linked by pair_id

## Intelligence Integration Map

**Currently wired:**
- trust_scorer → oracle/engine.py (convergence detection)
- lever_pullers → company_analyzer (lever context per ticker)
- All 12 LLM prompts → LEVER→CONDITION→OUTCOME standard

**Wired 2026-04-06:**
- hypothesis_engine → lever_pullers (confidence boost/penalty based on puller alignment)
- hypothesis_engine → trust_scorer (boost for trusted actors, penalty for untrusted)
- hypothesis_engine → causation_scoring (boost for hypotheses with known root cause)
- hypothesis_engine → 3 intelligence-informed kills (LEVER_DIVERGED, TRUST_COLLAPSED, CAUSATION_INVALIDATED)
- LLM prompts → discovered_hypotheses (active thesis/antithesis pairs via context_provider)
- LLM prompts → hypothesis_postmortems (recent failure lessons via context_provider)
- LLM prompts → company_profiles (governance/lobbying context via context_provider)

**Wired 2026-04-06 (micro models):**
- edgar_extractor → edgar_transcripts.py (structured extraction: revenue/EPS/guidance from 8-K filings)
- knowledge_mapper → signal_classifier.py `map_signal_knowledge()` (wiki entries with [[backlinks]] for critical signals)
- signal_classifier → signal_classifier.py `classify_recent_signals()` (already wired prior)
- anomaly_narrator → signal_classifier.py `narrate_anomalies()` (already wired prior)

**NOT yet wired:**
- hypothesis_engine → forensics (validate predictions vs actual price moves)
- hypothesis_engine → cross_reference (macro reality check on hypothesis assumptions)
