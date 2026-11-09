# GRID Module Inventory

Generated: 2026-11-08
Total modules: 876
Total LOC: 430,102

This is the authoritative inventory of every `.py` file in the GRID intelligence/data/serving stack.
Excludes `.git/`, `.mypy_cache/`, `.next/`, `.pytest_cache/`, `.venv/`, `__pycache__/`, `build/`, `dist/`, `docs/`, `node_modules/`, `notebooks/`, `pwa/`, `pwa_dist/`, `tests/`, `venv/`.

## Directory summary

| Directory | Module count | LOC |
|---|---|---|
| `intelligence/` | 209 | 123,946 |
| `ingestion/` | 228 | 107,902 |
| `api/` | 119 | 58,502 |
| `analysis/` | 65 | 41,724 |
| `trading/` | 36 | 16,579 |
| `oracle/` | 35 | 13,622 |
| `store/` | 13 | 8,883 |
| `alpha_research/` | 24 | 6,642 |
| `ollama/` | 8 | 5,801 |
| `alerts/` | 9 | 5,414 |
| `subnet/` | 10 | 5,381 |
| `physics/` | 18 | 5,376 |
| `features/` | 7 | 4,529 |
| `normalization/` | 3 | 3,363 |
| `discovery/` | 7 | 3,041 |
| `contracts/` | 23 | 2,768 |
| `gemma/` | 7 | 2,641 |
| `inference/` | 8 | 2,584 |
| `agents/` | 9 | 1,882 |
| `validation/` | 4 | 1,845 |
| `backtest/` | 4 | 1,624 |
| `hyperspace/` | 6 | 1,332 |
| `timeseries/` | 4 | 1,045 |
| `llamacpp/` | 2 | 799 |
| `outputs/` | 4 | 745 |
| `a2a/` | 4 | 670 |
| `events/` | 5 | 599 |
| `governance/` | 3 | 516 |
| `journal/` | 2 | 347 |

## Retired and held capabilities

The modules below are still on disk, so they keep their entries, but they do not run on a schedule. Extend the replacement, not these.

| Capability | Module | Status | Use instead |
|---|---|---|---|
| `TIINGO_FUNDAMENTALS` | `ingestion/tiingo_fundamentals_pull.py` | Retired 2026-11-08 (owner): held in both schedulers, no staleness alert, readers stop serving its last rows | `sec_xbrl_shares` → `ticker_metrics_daily` (market cap, shares) |
| Dune on-chain queries | `ingestion/altdata/dune_puller.py` | Retired 2026-11-04 (SmartScheduler hold) | none |
| Baltic Exchange | `ingestion/altdata/baltic_dry.py` | Retired 2026-11-04 (SmartScheduler hold) | none |
| Trending news (last30days) | `ingestion/altdata/trending_news.py` | Retired 2026-11-07, unregistered | social sentiment pullers |
| FinDKG | `ingestion/altdata/findkg_puller.py` | Retired 2026-11-07, unregistered | none |
| OpenSecrets | `ingestion/altdata/opensecrets_puller.py` | Retired 2026-11-04, unregistered, catalog `active=FALSE` | none |
| Finviz fundamentals | `ingestion/altdata/finviz_scraper.py` | Retired 2026-09-29 (raising shim) | `SECEdgarCompanyPuller` |
| Stale-price auto-fallback | `ingestion/price_fallback.py` | Retired 2026-10-02 (E1-V7); fetch-only | `ingestion/tiingo_pull.py` routine closes |
| PumpFun | `ingestion/pumpfun.py` | Not scheduled (E1-V3; endpoints 404/403) | none |
| ICIJ Offshore Leaks matching | `ingestion/altdata/offshore_leaks.py` | Held 2026-10-02 (match quality) | none |
| QuiverQuant WSB as Sleuth evidence | `ingestion/altdata/quiverquant.py` | No vote while its snapshot is frozen; unchanged snapshots are not stored | other `retail_sentiment` sources |
| Scheduled Sleuth lead generators | `intelligence/sleuth.py` | API/chat-only since the 2026-11-07 audit | `intelligence/sleuth_intersections.py` |
| v1 hypothesis pipeline and scorer | `intelligence/hypothesis_engine.py` | v1 detect→hunt→generate is retired from `auto_discover`; the v1 scorer was replaced 2026-11-06 | `intelligence/sleuth_intersections.py` baselines, `intelligence/hypothesis_trials.py` |

## `intelligence/`

#### `intelligence/__init__.py` — 1 LOC
**Imported by:** `intelligence/causation_scoring.py`, `intelligence/hypothesis_engine.py`, `intelligence/signal_convergence_scanner.py`

#### `intelligence/actor_discovery.py` — 3553 LOC
**Docstring:** GRID Intelligence — Automated Actor Discovery & Enrichment (250K+ Scale).
**Functions:** `enrich_actor`, `discover_connections`, `enrich_all_actors`, `auto_discover_actors`, `auto_discover_connections`, `run_discovery_cycle`, `get_actor_stats`, `batch_discover_insiders`, `discover_all_13f_filers`, `discover_all_congress`, `import_icij_offshore`, `discover_board_interlocks`, `run_3_degree_expansion`, `run_scale_discovery`, `hermes_daily_actor_discovery`
**Reads:** `__future__`, `csv`, `datetime`, `ingestion`, `json`, `loguru`, `orchestration`, `pathlib`, `re`, `sqlalchemy`, `typing`

#### `intelligence/actor_identity.py` — 395 LOC
**Docstring:** Canonical actor identity for SEC filer names.
**Functions:** `ticker_actor_id`, `ticker_actor_name`, `is_placeholder_ticker`, `looks_like_identifier`, `FilerIdentity`, `parse_filer_display_name`, `person_name_key`, `CanonicalProposal`, `propose_canonical`, `build_person_index`, `resolve_canonical_actor_id`, `ensure_merged_into_column`
**Reads:** `__future__`, `dataclasses`, `db`, `grid`, `loguru`, `re`, `typing`
**Imported by:** `ingestion/altdata/insider_filings.py`, `ingestion/altdata/quiverquant.py`, `ingestion/tiingo_pull.py`, `intelligence/signal_backlinker.py`, `intelligence/sleuth_intersections.py`, `intelligence/spider/discovery.py`
**Notes:** `is_placeholder_ticker` (2026-11-07) is True for `None`, a blank string or a null-ish ticker in `PLACEHOLDER_TICKERS` (`-`, `NONE`, `NULL`, `NAN`, `NA`, `N/A`, `UNKNOWN`), ignoring case and whitespace. It is the one shared skip used by the Form 4 writer (`ingestion/altdata/insider_filings.py`), the QuiverQuant writer, Sleuth intersections, the Tiingo insider-buyer list and `scripts/enrich_connections.py`. `looks_like_identifier` flags feed ids (such as `whale_aapl_100` and `qq_insider_trading`) that were written into the actor-name column.

#### `intelligence/actor_ingest.py` — 227 LOC
**Docstring:** Universal Actor Ingestion — auto-discover and log actors from ANY data source.
**Functions:** `ingest_actor`, `ingest_actors_batch`, `extract_actors_from_payload`, `get_actor_count`, `get_actor_sources`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `ingestion/altdata/defi_llama_puller.py`, `ingestion/altdata/etherscan_puller.py`, `ingestion/altdata/findkg_puller.py`, `ingestion/altdata/icij_puller.py`, `ingestion/altdata/indeed_hiring_puller.py`, `ingestion/altdata/opensecrets_puller.py`, `ingestion/altdata/redfin_puller.py`, `ingestion/international/world_bank_puller.py`, … (+1)

#### `intelligence/actor_network.py` — 152 LOC
**Docstring:** GRID Intelligence — Actor Network & Power Structure Map.
**Functions:** `track_wealth_migration`, `assess_pocket_lining`, `persist_wealth_flows`
**Reads:** `__future__`, `intelligence`
**Imported by:** `analysis/money_flow.py`, `api/routers/intelligence_actors.py`, `ingestion/altdata/offshore_leaks.py`, `intelligence/actors/analysis.py`, `intelligence/company_analyzer.py`, `intelligence/deep_graph.py`, `intelligence/icij_linker.py`, `intelligence/pocket_lining.py`, … (+2)

#### `intelligence/actor_researcher.py` — 414 LOC
**Docstring:** Actor Researcher — local LLM agent that continuously enriches actor profiles.
**Functions:** `find_sparse_actors`, `gather_evidence`, `enrich_actor_with_llm`, `update_actor_profile`, `research_batch`, `run_continuous`
**Reads:** `__future__`, `datetime`, `db`, `intelligence`, `json`, `llm`, `loguru`, `sqlalchemy`, `time`, `typing`

#### `intelligence/actor_signal_bridge.py` — 290 LOC
**Docstring:** Actor Signal Bridge — injects actor intelligence into the prediction pipeline.
**Functions:** `get_actor_signals_for_ticker`, `get_actor_trust_weights`, `get_actor_context_for_causation`, `enrich_signals_with_actors`, `sync_actor_trust_to_signal_sources`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/causation_scoring.py`, `intelligence/trust_scorer.py`, `oracle/engine.py`

#### `intelligence/actor_trust_cog.py` — 319 LOC
**Docstring:** INTEL-2 — Actor trust-or-cog classifier.
**Functions:** `TrustCogScore`, `score_one_actor`, `score_all_actors`, `get_actor_trust_cog`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/actor_detail.py`, `intelligence/scheduler.py`

#### `intelligence/actors/__init__.py` — 59 LOC
**Docstring:** GRID Intelligence — actors subpackage.
**Reads:** `intelligence`
**Imported by:** `intelligence/actors/universe.py`

#### `intelligence/actors/analysis.py` — 371 LOC
**Docstring:** GRID Intelligence — Actor network analysis functions.
**Functions:** `get_actor_context_for_ticker`, `enrich_lever_pullers_with_actors`, `generate_actor_report`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`

#### `intelligence/actors/db.py` — 513 LOC
**Docstring:** GRID Intelligence — Actor Network database layer.
**Functions:** `ensure_spider_tables`, `save_actor`, `save_connection`
**Reads:** `__future__`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_spider.py`, `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/analysis.py`, `intelligence/actors/graph.py`, `intelligence/spider/daemon.py`

#### `intelligence/actors/graph.py` — 414 LOC
**Docstring:** GRID Intelligence — Actor graph construction and network traversal.
**Functions:** `build_actor_graph`, `find_connected_actions`
**Reads:** `__future__`, `collections`, `datetime`, `intelligence`, `loguru`, `os`, `sqlalchemy`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/analysis.py`

#### `intelligence/actors/ingestion.py` — 141 LOC
**Docstring:** GRID Intelligence — Actor Network data ingestion.
**Functions:** `ingest_panama_pandora_data`
**Reads:** `__future__`, `csv`, `intelligence`, `loguru`, `os`, `pathlib`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`

#### `intelligence/actors/models.py` — 61 LOC
**Docstring:** GRID Intelligence — Actor Network data models.
**Functions:** `Actor`, `WealthFlow`
**Reads:** `__future__`, `dataclasses`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/analysis.py`, `intelligence/actors/db.py`, `intelligence/actors/graph.py`

#### `intelligence/actors/provenance.py` — 337 LOC
**Docstring:** GRID Intelligence — Actor provenance labels.
**Functions:** `resolve_provenance`, `actor_source`, `source_as_of`, `stamp_actor_node`
**Reads:** `__future__`, `datetime`, `intelligence`
**Imported by:** `api/routers/intel.py`, `api/routers/intelligence_actors.py`, `intelligence/actors/db.py`, `intelligence/actors/graph.py`, `intelligence/actors/universe_store.py`

#### `intelligence/actors/sectors.py` — 349 LOC
**Docstring:** Actor sector tagging: word-boundary keywords (precompiled), "<Sector> — <TICKER>" titles, category signals, neighbour vote.
**Functions:** `sector_from_title_prefix`, `sector_from_keywords`, `tag_sector`, `tag_sectors_frame`, `propagate_sectors`
**Reads:** `__future__`, `collections`, `numpy`, `pandas`, `re`, `typing`
**Imported by:** `intelligence/actors/graph.py`

#### `intelligence/actors/seed_data.py` — 5618 LOC
**Docstring:** GRID Intelligence — Actor Network seed data.
**Functions:** `get_known_actors`
**Reads:** `__future__`, `sqlalchemy`
**Imported by:** `ingestion/altdata/wikidata_persons.py`, `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/db.py`, `intelligence/actors/ingestion.py`, `intelligence/actors/provenance.py`

#### `intelligence/actors/trial_bridge.py` — 456 LOC
**Docstring:** GRID Intelligence — Trial Sponsor → Actor Network bridge.
**Functions:** `sync_trial_sponsors_to_actors`
**Reads:** `__future__`, `collections`, `json`, `logging`, `os`, `psycopg2`, `typing`

#### `intelligence/actors/universe.py` — 915 LOC
**Docstring:** Offline full-graph analytics over all actors + actor_connections + entity_relationships + wealth_flows (igraph): PageRank, Leiden, components, k-core, sampled betweenness, sectors; writes versioned `actor_graph_*` tables. Launched by the intelligence scheduler as a niced subprocess.
**Functions:** `category_group`, `peak_rss_mb`, `LoadedUniverse`, `load_universe`, `resolve_endpoint_ids`, `AssembledGraph`, `assemble_graph`, `GraphMetrics`, `compute_metrics`, `build_actor_table`, `build_community_table`, `build_pool_edges`, `write_run`, `compute_universe`, `run_actor_universe_job`, `job_enabled`, `latest_complete_age_hours`, `launch_job_subprocess`, `maybe_launch`, `main`
**Reads:** `__future__`, `argparse`, `dataclasses`, `datetime`, `db`, `igraph`, `intelligence`, `io`, `json`, `loguru`, `numpy`, `os`, `pandas`, `random`, `resource`, `shutil`, `socket`, `sqlalchemy`, `subprocess`, `sys`, `time`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`, `intelligence/actors/universe_store.py`, `intelligence/scheduler.py`
**DB writes:** `actor_graph_runs`, `actor_graph_metrics`, `actor_graph_communities`, `actor_graph_edges`

#### `intelligence/actors/universe_store.py` — 779 LOC
**Docstring:** Request-time reads of the precomputed actor universe: overview (top-N by importance), bounded ego network, trigram search, community summaries.
**Functions:** `normalize_groups`, `latest_run`, `overview`, `ego_network`, `search_actors`, `top_actors`, `community_summaries`, `community_members`, `actor_metrics`
**Reads:** `__future__`, `intelligence`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/adapters/__init__.py` — 35 LOC
**Docstring:** Signal adapters — wrap intelligence modules into RegisteredSignal producers.
**Reads:** `intelligence`
**Imported by:** `api/routers/signal_registry.py`

#### `intelligence/adapters/ai_trader_adapter.py` — 269 LOC
**Docstring:** GRID Intelligence — AI-Trader Signal Adapter.
**Functions:** `AITraderAdapter`
**Reads:** `__future__`, `config`, `datetime`, `hashlib`, `intelligence`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/base.py` — 115 LOC
**Docstring:** GRID Intelligence — Signal Adapter Protocol, BaseAdapter, and Registry.
**Functions:** `SignalAdapter`, `sid`, `now_utc`, `clamp`, `BaseAdapter`, `AdapterRegistry`
**Reads:** `__future__`, `datetime`, `hashlib`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/signal_registry.py`, `intelligence/adapters/cross_reference_adapter.py`, `intelligence/adapters/dollar_flows_adapter.py`, `intelligence/adapters/earnings_adapter.py`, `intelligence/adapters/feature_adapter.py`, `intelligence/adapters/flow_thesis_adapter.py`, `intelligence/adapters/forensics_adapter.py`, `intelligence/adapters/lever_pullers_adapter.py`, … (+5)

#### `intelligence/adapters/cross_reference_adapter.py` — 62 LOC
**Docstring:** GRID Signal Adapter — Cross-Reference (Lie Detector). Divergence signals.
**Functions:** `CrossReferenceAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/dollar_flows_adapter.py` — 69 LOC
**Docstring:** GRID Signal Adapter — Dollar Flows. Net flow direction + magnitude per sector.
**Functions:** `DollarFlowsAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/earnings_adapter.py` — 82 LOC
**Docstring:** GRID Signal Adapter — Earnings Intel. Upcoming earnings + historical surprise signals.
**Functions:** `EarningsAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/feature_adapter.py` — 105 LOC
**Docstring:** GRID Signal Adapter — Feature Store bridge. Z-score signals from resolved_series.
**Functions:** `FeatureAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `math`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/flow_thesis_adapter.py` — 119 LOC
**Docstring:** GRID Intelligence — Flow Thesis Signal Adapter.
**Functions:** `FlowThesisAdapter`
**Reads:** `__future__`, `analysis`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/forensics_adapter.py` — 68 LOC
**Docstring:** GRID Signal Adapter — Forensic Analyzer. Warning count + directional signals per ticker.
**Functions:** `ForensicsAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `math`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/lever_pullers_adapter.py` — 61 LOC
**Docstring:** GRID Signal Adapter — Lever Pullers. Per-ticker directional signals from actor events.
**Functions:** `LeverPullersAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/news_adapter.py` — 90 LOC
**Docstring:** GRID Signal Adapter — News Intel. Sentiment momentum + volume signals per ticker.
**Functions:** `NewsAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/pattern_adapter.py` — 63 LOC
**Docstring:** GRID Signal Adapter — Pattern Engine. Active recognized patterns with hit rates.
**Functions:** `PatternAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/sector_network_adapter.py` — 340 LOC
**Docstring:** GRID Signal Adapter — Sector Networks. Actor density + per-ticker concentration.
**Functions:** `SectorNetworkAdapter`
**Reads:** `__future__`, `datetime`, `hashlib`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/sleuth_adapter.py` — 44 LOC
**Docstring:** GRID Signal Adapter — Sleuth. Active investigation leads as signals.
**Functions:** `SleuthAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/thesis_tracker_adapter.py` — 82 LOC
**Docstring:** GRID Signal Adapter — Thesis Tracker. Latest market thesis direction + accuracy.
**Functions:** `ThesisTrackerAdapter`
**Reads:** `__future__`, `datetime`, `intelligence`, `sqlalchemy`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/trust_scorer_adapter.py` — 99 LOC
**Docstring:** GRID Signal Adapter — Trust Scorer. Convergence + per-source trust signals.
**Functions:** `TrustScorerAdapter`
**Reads:** `__future__`, `collections`, `datetime`, `ingestion`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/api_precompute.py` — 877 LOC
**Docstring:** Background precompute for GRID's slowest read endpoints (W3-2).
**Functions:** `Output`, `BuildContext`, `Job`, `build_regime_analogs`, `build_globe`, `build_influence`, `build_signals_snapshot`, `build_smart_heatmap`, `build_associations`, `build_cross_reference_narrative`, `build_architecture`, `build_patterns`, `build_money_map`, `build_levers`, `build_oracle_evidence`, `build_options_100x`, `job_for_key`, `due_reason`, `run_job`, `run_due`, `enabled`, `start_background_tick`, `describe`
**Reads:** `__future__`, `analysis`, `api`, `collections`, `config`, `dataclasses`, `datetime`, `db`, `discovery`, `hashlib`, `intelligence`, `json`, `loguru`, `oracle`, `sqlalchemy`, `store`, `threading`, `time`, `typing`
**Imported by:** `api/precompute_serving.py`, `intelligence/scheduler.py`

#### `intelligence/attention_anomaly.py` — 184 LOC
**Docstring:** Attention Anomaly Detector — combines Wikipedia + Google Trends signals.
**Functions:** `AttentionSignal`, `score_attention`, `enrich_with_price_action`, `get_alerts`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/audio_briefing.py` — 1280 LOC
**Docstring:** GRID -- Daily Intelligence Audio Briefing Pipeline.
**Functions:** `LocalTTSUnavailable`, `BriefingResult`, `briefing_output_dir`, `generate_briefing_script`, `generate_briefing_audio`, `generate_briefing_video`, `get_latest_briefing`, `list_all_briefings`, `get_briefing_by_filename`
**Reads:** `__future__`, `analysis`, `config`, `dataclasses`, `datetime`, `db`, `google`, `intelligence`, `json`, `langfuse`, `llm`, `loguru`, `math`, `ollama`, `openai`, `os`, `pathlib`, `requests`, `subprocess`, `time`, `typing`
**Imported by:** `api/routers/flows.py`, `api/routers/intelligence_thesis.py`

#### `intelligence/bayesian_evidence.py` — 260 LOC
**Docstring:** CAT-178 — Bayesian evidence combiner.
**Functions:** `EvidenceItem`, `BayesianResult`, `logit`, `sigmoid`, `combine_evidence`, `from_oracle_votes`
**Reads:** `__future__`, `dataclasses`, `math`, `typing`

#### `intelligence/breaking_news.py` — 351 LOC
**Docstring:** Breaking news monitor — detects high-impact events in near-real-time.
**Functions:** `check_gdelt`, `detect_spike`, `infer_direction`, `inject_signal`, `invalidate_caches`, `run_monitor`
**Reads:** `__future__`, `argparse`, `datetime`, `db`, `json`, `loguru`, `pathlib`, `requests`, `sqlalchemy`, `sys`, `time`, `typing`

#### `intelligence/business_news_parser.py` — 803 LOC
**Docstring:** GRID Intelligence — Business News Parser.
**Functions:** `BusinessEvent`, `BusinessNewsParser`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/catalyst_aggregator.py` — 464 LOC
**Docstring:** ALPHA-4 — Unified catalyst calendar + catalyst-aware scoring.
**Functions:** `CatalystEvent`, `events_for_window`, `nearest_catalyst`, `proximity_score`, `upcoming_catalysts_summary`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `alerts/hundredx_digest.py`, `oracle/engine.py`

#### `intelligence/catalyst_ev.py` — 1340 LOC
**Docstring:** Catalyst expected value — scoring the event, not the trend.
**Functions:** `clamp`, `months_between`, `normalize_phase`, `indication_multiplier`, `designation_multiplier`, `success_probability`, `empirical_phase_outcomes`, `downside_multiple`, `upside_multiple_from_iv`, `runway_covers_catalyst`, `catalyst_ev`, `classify_trade_shape`, `build_catalyst_board`
**Reads:** `__future__`, `datetime`, `intelligence`, `json`, `loguru`, `math`, `sqlalchemy`, `typing`

#### `intelligence/causal_links.py` — 915 LOC
**Docstring:** Point-in-time causal-link edges for the Timeline / Causal Map / Why views.
**Functions:** `CausalLinksSchemaMissing`, `Action`, `AntecedentEvent`, `Edge`, `RunSummary`, `resolve_code_sha`, `day_start`, `end_of_day`, `parse_sec_filetime`, `normalize_actor`, `direction_of`, `edge_key_for`, `recency_score`, `canonical_actions`, `earnings_events`, `contract_events`, `build_edges`, `select_tickers`, `load_action_rows`, `load_event_rows`, `schema_ready`, `upsert_edges`, `compute_batch`, `run_causal_links`, `latest_run`, `read_links`, `serialize_link`, `read_links_payload`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `os`, `pathlib`, `re`, `sqlalchemy`, `subprocess`, `typing`, `uuid`, `zoneinfo`
**Imported by:** `api/routers/intelligence_causation.py`, `api/routers/intelligence_forensics.py`

#### `intelligence/causation.py` — 26 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine.
**Reads:** `intelligence`
**Imported by:** `api/routers/intelligence_forensics.py`

#### `intelligence/causation_core.py` — 202 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine (core module).
**Functions:** `CausalLink`, `CausalChain`, `ensure_table`
**Reads:** `__future__`, `dataclasses`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/causation.py`, `intelligence/causation_graph.py`, `intelligence/causation_scoring.py`

#### `intelligence/causation_graph.py` — 1188 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine (graph module).
**Functions:** `trace_causal_chain`, `find_longest_chains`, `generate_chain_narrative`, `detect_chain_in_progress`, `load_causal_chains`
**Reads:** `__future__`, `datetime`, `db`, `intelligence`, `json`, `llm`, `loguru`, `ollama`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/causation.py`

#### `intelligence/causation_scoring.py` — 659 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine (scoring module).
**Functions:** `find_causes`, `batch_find_causes`, `get_suspicious_trades`, `generate_causal_narrative`, `generate_causal_narrative_with_source`
**Reads:** `__future__`, `datetime`, `db`, `intelligence`, `llm`, `loguru`, `sqlalchemy`
**Imported by:** `intelligence/causation.py`, `intelligence/hypothesis_engine.py`

#### `intelligence/cds_tracker.py` — 407 LOC
**Docstring:** GRID — CDS (Credit Default Swap) Tracker.
**Functions:** `SpreadSnapshot`, `CDSDashboard`, `build_spread_snapshot`, `build_cds_dashboard`, `cds_to_dict`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `analysis/money_flow_engine/layer_credit.py`, `api/routers/flows.py`, `intelligence/audio_briefing.py`, `intelligence/deep_dive.py`

#### `intelligence/chain_contagion.py` — 804 LOC
**Docstring:** Chain contagion simulator.
**Functions:** `ShockSpec`, `ActorImpact`, `simulate_contagion`
**Reads:** `__future__`, `collections`, `contracts`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `alerts/supply_chain_alerts.py`, `api/routers/contagion.py`, `intelligence/news_contagion_listener.py`

#### `intelligence/codebase_context.py` — 306 LOC
**Docstring:** GRID Codebase Context — dynamic state injected into every LLM prompt.
**Functions:** `get_system_context`
**Reads:** `__future__`, `datetime`, `db`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/chat.py`

#### `intelligence/company_analyzer.py` — 1078 LOC
**Docstring:** GRID Intelligence — Company Analyzer Pipeline.
**Functions:** `CompanyProfile`, `ensure_table`, `analyze_company`, `run_analysis_queue`, `get_all_profiles`, `find_cross_company_patterns`, `generate_sector_influence_report`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `intelligence`, `json`, `llm`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `analysis/money_flow.py`, `api/routers/intelligence_companies.py`, `ingestion/altdata/company_profiles_puller.py`, `ingestion/altdata/small_cap_enrichment.py`

#### `intelligence/company_financial_rollups.py` — 674 LOC
**Docstring:** Capital-flow rollup derivations.
**Functions:** `ttm_statement_sql`, `TtmResult`, `compute_ttm`, `fold_announcements`, `run_all`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`

#### `intelligence/confidence_bucket_tracker.py` — 693 LOC
**Docstring:** Per-horizon, per-confidence-bucket calibration tracker (CAT-180).
**Functions:** `BucketCalibration`, `record_scored_prediction`, `get_bucket_calibration`, `conviction_multiplier_for_bucket`, `rank_buckets_by_calibration`, `bootstrap_from_oracle_predictions`
**Reads:** `__future__`, `dataclasses`, `datetime`, `features`, `loguru`, `scripts`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/confidence_calibration.py` — 392 LOC
**Docstring:** Per-model confidence reliability curves.
**Functions:** `ensure_tables`, `CalibrationCurve`, `invalidate_cache`, `calibrate_confidence`, `calibrate_confidence_default`, `build_reliability_curves`, `main`
**Reads:** `__future__`, `argparse`, `dataclasses`, `db`, `loguru`, `sqlalchemy`, `sys`, `typing`
**Imported by:** `intelligence/news_impact.py`, `oracle/contrast_distillation.py`, `oracle/engine.py`, `oracle/forecaster_adapter.py`, `oracle/psi_model.py`

#### `intelligence/consensus_crowdedness.py` — 415 LOC
**Docstring:** CAT-182 — Consensus crowdedness detector.
**Functions:** `CrowdednessComponent`, `CrowdednessResult`, `CrowdednessPenalty`, `compose_crowdedness`, `compute_penalty`, `compute_crowdedness`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `store`, `typing`
**Imported by:** `oracle/engine.py`

#### `intelligence/contagion_backtest.py` — 550 LOC
**Docstring:** Contagion backtest scorer.
**Functions:** `compute_accuracy`, `score_predictions`, `score_all_windows`
**Reads:** `__future__`, `contracts`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`, `uuid`

#### `intelligence/context_provider.py` — 250 LOC
**Docstring:** Context provider for LLM prompt injection.
**Functions:** `get_active_hypotheses`, `get_recent_postmortems`, `get_company_context`, `get_hypothesis_context_for_ticker`, `build_full_context`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`
**Imported by:** `intelligence/audio_briefing.py`, `intelligence/cross_reference.py`, `intelligence/market_diary.py`, `intelligence/sleuth.py`, `intelligence/thesis_tracker.py`, `ollama/market_briefing.py`

#### `intelligence/contra_indicator_ensemble.py` — 794 LOC
**Docstring:** Contra-indicator ensemble — "who is on the wrong side?" detector.
**Functions:** `IndicatorSpec`, `ContraIndicator`, `ContraEnsembleReport`, `build_contra_report`, `contra_conviction_multiplier`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/cot_extremes.py` — 293 LOC
**Docstring:** CAT-35 — CFTC COT extremes + non-commercial z-scores.
**Functions:** `COTExtreme`, `classify_extreme`, `rank_contrarian_signals`, `scan_extremes_report`, `scan_all_extremes`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `numpy`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/counterfactual_stress.py` — 518 LOC
**Docstring:** Counterfactual stress test engine (CAT-175).
**Functions:** `SignalPerturbation`, `FragilityFlag`, `StressTestReport`, `perturb_brier`, `perturbed_conviction_weight`, `compute_robustness_score`, `classify_robustness`, `identify_fragility_flags`, `build_advisory`, `run_stress_test`
**Reads:** `__future__`, `dataclasses`, `datetime`, `features`, `intelligence`, `typing`
**Imported by:** `intelligence/decision_gateway.py`

#### `intelligence/credit_event_probability.py` — 357 LOC
**Docstring:** CAT-162 — Credit event probability machine (per-name).
**Functions:** `CreditEventResult`, `merton_distance_to_default`, `merton_default_probability`, `credit_spread_default_probability`, `rating_trajectory_adjustment`, `compose_credit_event_probability`, `compute_credit_event_probability`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/cross_lens.py` — 712 LOC
**Docstring:** GRID Cross-Lens Correlation Detector.
**Functions:** `Attribution`, `resolve_price_series_id`, `fetch_close_series`, `compute_log_returns`, `lagged_correlation`, `detect_shock_events`, `list_candidate_pairs`, `build_lagged_evidence`, `build_event_evidence`, `build_actor_narrative`, `upsert_attributions`, `detect_attributions`, `get_attributions_for_actor`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/attributions.py`, `intelligence/supply_chain_edge_validator.py`

#### `intelligence/cross_reference.py` — 1991 LOC
**Docstring:** GRID Cross-Reference Engine — Lie Detector for Government Statistics.
**Functions:** `CrossRefCheck`, `LieDetectorReport`, `ensure_tables`, `check_gdp_vs_physical`, `check_trade_bilateral`, `check_inflation_vs_inputs`, `check_central_bank_actions_vs_words`, `check_employment_reality`, `get_cross_ref_for_ticker`, `generate_narrative_with_source`, `check_liquidity_reality`, `check_credit_housing`, `check_insider_divergence`, `run_all_checks`, `load_recent_report`, `get_historical_checks`
**Reads:** `__future__`, `dataclasses`, `datetime`, `db`, `intelligence`, `llm`, `loguru`, `math`, `pandas`, `sqlalchemy`, `time`, `typing`
**Imported by:** `api/routers/intel.py`, `api/routers/intel_cross_reference.py`, `api/routers/intelligence_risk.py`, `intelligence/api_precompute.py`, `intelligence/codebase_context.py`, `intelligence/hypothesis_engine.py`, `intelligence/prediction_calibration.py`, `intelligence/shipping_fudge_detector.py`, … (+1)

#### `intelligence/deal_detector.py` — 860 LOC
**Docstring:** GRID Intelligence — M&A / Deal Detection Engine.
**Functions:** `DealSignal`, `DealClassifier`, `DealTracker`, `DealDetector`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/decision_gateway.py` — 396 LOC
**Docstring:** Decision gateway — the capstone "should I trade this?" wrapper.
**Functions:** `combine_verdict`, `DecisionResponse`, `should_i_trade`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `oracle`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `api/routers/conviction.py`, `intelligence/pair_conviction.py`, `intelligence/universe_ranker.py`

#### `intelligence/deep_dive.py` — 821 LOC
**Docstring:** GRID Intelligence — Thesis Deep Dive Engine.
**Functions:** `DeepDiveResult`, `run_deep_dive`, `deep_dive_async`, `get_deep_dives`, `get_deep_dive`
**Reads:** `__future__`, `analysis`, `config`, `dataclasses`, `datetime`, `google`, `intelligence`, `json`, `langfuse`, `llm`, `loguru`, `os`, `requests`, `sqlalchemy`, `threading`, `time`, `typing`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`, `intelligence/thesis_tracker.py`

#### `intelligence/deep_graph.py` — 1771 LOC
**Docstring:** GRID Intelligence — Deep Graph Traversal Engine.
**Functions:** `GraphNode`, `GraphEdge`, `Overlap`, `LayerResult`, `ensure_table`, `deep_drill`, `find_overlaps`, `find_all_overlaps`, `generate_connection_map`, `discover_hidden_influence`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `intelligence`, `itertools`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_companies.py`

#### `intelligence/dollar_flows.py` — 1279 LOC
**Docstring:** GRID Intelligence — Dollar Flow Normalizer.
**Functions:** `FlowNormalizationSummary`, `normalize_all_flows`, `get_flows_by_ticker`, `get_flows_by_sector`, `get_aggregate_flows`, `get_biggest_movers`
**Reads:** `__future__`, `analysis`, `collections`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_govflow.py`

#### `intelligence/dune_smart_money.py` — 390 LOC
**Docstring:** Dune smart-money intelligence layer.
**Functions:** `WalletPnL`, `CEXFlow`, `HolderGrowth`, `smart_money_leaderboard`, `cex_flow_balance`, `narrative_heat`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `intelligence/earnings_intel.py` — 887 LOC
**Docstring:** GRID Intelligence — Earnings Analysis & Prediction System.
**Functions:** `EarningsPrediction`, `get_earnings_calendar`, `analyze_earnings_surprise`, `predict_earnings_reaction`, `get_prediction_scorecard`, `run_earnings_cycle`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `loguru`, `math`, `numpy`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/earnings.py`

#### `intelligence/earnings_transcript_analyzer.py` — 684 LOC
**Docstring:** GRID Intelligence — Earnings Transcript Analyzer.
**Functions:** `TranscriptAnalysis`, `ToneScorer`, `PhraseExtractor`, `SectionSplitter`, `EarningsTranscriptAnalyzer`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/edge_signals.py` — 585 LOC
**Docstring:** EDGE-signal multipliers derived from the backtest edge_table.
**Functions:** `set_enabled`, `EdgeRecord`, `reload`, `lookup_edge`, `edge_multiplier`, `multiplier_for_source_ticker`, `compute_aggregate_edge_multiplier`, `apply_multiplier`, `edge_multiplier_for_prediction`
**Reads:** `__future__`, `csv`, `dataclasses`, `loguru`, `math`, `os`, `pathlib`, `sqlalchemy`, `threading`, `typing`

#### `intelligence/eight_k_clustering.py` — 298 LOC
**Docstring:** CAT-61 — 8-K unusual clustering + item category tracker.
**Functions:** `EightKFiling`, `ClusterAlert`, `score_filing`, `classify_severity`, `detect_clusters`, `scan_for_clusters`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/entity_resolver.py` — 1663 LOC
**Docstring:** GRID Intelligence — Entity Resolution Engine.
**Functions:** `phonetic_key`, `levenshtein_distance`, `jaro_similarity`, `jaro_winkler_similarity`, `name_similarity`, `strip_accents`, `normalize_name`, `canonical_key`, `entity_id`, `ResolvedEntity`, `EntityResolver`, `SpiderEntityResolver`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `db`, `hashlib`, `json`, `loguru`, `pathlib`, `re`, `sqlalchemy`, `sys`, `typing`, `unicodedata`
**Imported by:** `api/routers/intelligence_spider.py`, `api/routers/watchlist_overview.py`, `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`, `intelligence/spider/discovery.py`

#### `intelligence/event_sequence.py` — 1058 LOC
**Docstring:** GRID Intelligence — Event Sequence Builder.
**Functions:** `Event`, `build_sequence`, `build_sector_sequence`, `compute_lead_times`, `find_recurring_patterns`, `build_sequence_with_lead_times`, `events_to_dicts`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `intelligence/forensics.py`, `intelligence/pattern_engine.py`

#### `intelligence/export_intel.py` — 438 LOC
**Docstring:** GRID Intelligence — Export Controls Analysis.
**Functions:** `ExportControlRecord`, `RevenueImpactAssessment`, `get_recent_controls`, `get_controls_for_ticker`, `assess_revenue_impact`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_forensics.py`, `intelligence/company_analyzer.py`

#### `intelligence/financial_conditions_index.py` — 304 LOC
**Docstring:** CAT-124 — Financial Conditions Index (multi-factor FCI).
**Functions:** `FCIComponent`, `FCIResult`, `compose_fci`, `compute_fci`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/scheduler.py`, `oracle/engine.py`

#### `intelligence/fleet_heartbeat.py` — 535 LOC
**Docstring:** Fleet heartbeat monitor — hermes-driven, full-fleet liveness + alerts.
**Functions:** `ensure_schema`, `probe_nodes`, `probe_gpus`, `probe_units`, `probe_endpoints`, `probe_freshness`, `run_fleet_heartbeat`
**Reads:** `__future__`, `alerts`, `argparse`, `datetime`, `ipaddress`, `json`, `loguru`, `os`, `re`, `shlex`, `sqlalchemy`, `subprocess`, `time`, `typing`, `urllib`

#### `intelligence/forced_flow_monitor.py` — 568 LOC
**Docstring:** GRID Intelligence — Forced Flow Monitor (Waterfall Early Warning System).
**Functions:** `GammaRegimeSnapshot`, `CalendarEvent`, `ForcedFlowThreshold`, `MorningBriefing`, `upcoming_calendar_events`, `check_gamma_regime`, `scan_thresholds`, `build_posture`, `build_morning_briefing`, `persist_briefing`, `run_forced_flow_cycle`
**Reads:** `__future__`, `alerts`, `dataclasses`, `datetime`, `json`, `loguru`, `physics`, `sqlalchemy`, `typing`
**Imported by:** `alerts/waterfall_watch.py`

#### `intelligence/forensic_journal.py` — 672 LOC
**Docstring:** GRID Intelligence — CAT-189 Forensic Journal of Failed Predictions.
**Functions:** `FailedPredictionPostmortem`, `FailingSignal`, `is_high_confidence_failure`, `compute_failure_multiplier`, `classify_root_cause`, `compose_narrative_template`, `ensure_postmortem_table`, `record_failure`, `get_failing_signals`, `get_recent_postmortems`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `intelligence/forensics.py` — 1006 LOC
**Docstring:** GRID Intelligence — Forensic Analyzer.
**Functions:** `ForensicReport`, `analyze_move`, `find_significant_moves`, `batch_forensics`, `generate_forensic_summary`, `load_forensic_reports`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `db`, `intelligence`, `json`, `llm`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_forensics.py`, `intelligence/causation_graph.py`, `intelligence/hypothesis_engine.py`

#### `intelligence/freshness_guard.py` — 153 LOC
**Docstring:** GRID — Feature Freshness Guard.
**Functions:** `FreshnessStatus`, `check_freshness`, `log_stale_features`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`
**Imported by:** `intelligence/causation_scoring.py`, `intelligence/company_analyzer.py`, `intelligence/forensics.py`, `intelligence/thesis_tracker.py`

#### `intelligence/fundamental_divergence.py` — 1003 LOC
**Docstring:** Fundamental-vs-price divergence detector.
**Functions:** `SectorTicker`, `compute_divergence`, `snapshot_all`
**Reads:** `__future__`, `analysis`, `contracts`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`, `uuid`

#### `intelligence/global_levers.py` — 2305 LOC
**Docstring:** GRID Intelligence -- Global Lever Map: Hierarchical Model of World Economic Power.
**Functions:** `get_lever_hierarchy`, `get_lever_domain`, `trace_lever_chain`, `find_cross_domain_actors`, `generate_lever_report`
**Reads:** `__future__`, `analysis`, `collections`, `copy`, `datetime`, `ingestion`, `intelligence`, `loguru`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/intelligence_deepdive.py`, `intelligence/api_precompute.py`

#### `intelligence/goal_queue.py` — 577 LOC
**Docstring:** Goal queue — Day 1 of the idle-fleet agent-loop PoC.
**Functions:** `Goal`, `enqueue_goal`, `enqueue_many`, `claim_goal`, `submit_result`, `mark_failed`, `extend_lease`, `reap_expired_leases`, `queue_stats`, `recent_results`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `intelligence/gov_intel.py` — 297 LOC
**Docstring:** GRID Intelligence — Government Contract Analysis.
**Functions:** `ContractRecord`, `InsiderContractOverlap`, `get_recent_contracts`, `get_contracts_for_ticker`, `detect_contract_insider_overlap`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_govflow.py`, `intelligence/company_analyzer.py`

#### `intelligence/grand_orchestrator.py` — 528 LOC
**Docstring:** Grand Orchestrator — the self-learning brain of GRID.
**Functions:** `LearningCycleResult`, `LearningModule`, `register_learning_module`, `run_due_cycles`, `run_cycle_for_module`, `auto_register_self_learning_modules`, `get_recent_log`, `get_module_state`, `get_all_registered`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `time`, `typing`

#### `intelligence/hermes/__init__.py` — 38 LOC
**Docstring:** Hermes — GRID's analyst bridge to a fine-tuned grid-analyst-v1.
**Reads:** `__future__`, `agent`, `codex_provider`, `config`, `prompts`, `provider`, `spend`
**Imported by:** `agents/runner.py`

#### `intelligence/hermes/__main__.py` — 8 LOC
**Docstring:** Enable ``python -m intelligence.hermes`` as an alias for the CLI.
**Reads:** `__future__`, `cli`

#### `intelligence/hermes/agent.py` — 175 LOC
**Docstring:** HermesAgent — the GRID analyst bridge.
**Functions:** `AnalysisResult`, `HermesAgent`
**Reads:** `__future__`, `codex_provider`, `config`, `dataclasses`, `json`, `llm`, `loguru`, `prompts`, `provider`, `typing`

#### `intelligence/hermes/cli.py` — 145 LOC
**Docstring:** Hermes CLI — ``python -m intelligence.hermes.cli {ping,ask}``.
**Functions:** `build_parser`, `main`
**Reads:** `__future__`, `agent`, `argparse`, `codex_provider`, `config`, `llm`, `prompts`, `provider`, `shutil`, `sys`

#### `intelligence/hermes/codex_provider.py` — 157 LOC
**Docstring:** Hermes Codex backend.
**Functions:** `CodexProvider`
**Reads:** `__future__`, `config`, `loguru`, `os`, `provider`, `shlex`, `shutil`, `subprocess`, `tempfile`, `time`

#### `intelligence/hermes/config.py` — 131 LOC
**Docstring:** Configuration for the Hermes analyst bridge.
**Functions:** `HermesConfig`, `load_hermes_config`
**Reads:** `__future__`, `config`, `dataclasses`, `os`, `typing`

#### `intelligence/hermes/prompts.py` — 80 LOC
**Docstring:** Hermes analyst prompts.
**Functions:** `build_messages`
**Reads:** `__future__`

#### `intelligence/hermes/provider.py` — 192 LOC
**Docstring:** Hermes OpenAI provider.
**Functions:** `TokenUsage`, `HermesResponse`, `HermesProvider`
**Reads:** `__future__`, `config`, `dataclasses`, `loguru`, `openai`, `spend`, `time`

#### `intelligence/hermes/spend.py` — 79 LOC
**Docstring:** Daily spend ledger for the Hermes analyst bridge.
**Functions:** `SpendLedger`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `os`, `pathlib`

#### `intelligence/historical_scenario_library.py` — 1075 LOC
**Docstring:** GRID Historical Scenario Library — macro-FEATURE-space analog matcher.
**Functions:** `ScenarioAnalog`, `ScenarioLibraryReport`, `find_analogs`, `scenario_conviction_multiplier`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `numpy`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/hmm_regime_transitions.py` — 297 LOC
**Docstring:** CAT-121 — HMM regime transition matrix.
**Functions:** `TransitionMatrix`, `RegimeForecast`, `fit_transition_matrix`, `next_state_distribution`, `forecast_horizon`, `compute_entropy`, `read_regime_history`, `fit_from_db`
**Reads:** `__future__`, `dataclasses`, `loguru`, `math`, `numpy`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/holder_deal_overlap.py` — 611 LOC
**Docstring:** Holder / deal overlap detector — "pre-positioning" cross-reference.
**Functions:** `OverlapRow`, `find_deals`, `detect_overlap_for_deal`, `upsert_rows`, `RunStats`, `run`, `fetch_overlaps_for_actor`
**Reads:** `__future__`, `contracts`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/routers/actor_detail.py`

#### `intelligence/hypothesis_engine.py` — 2189 LOC
**Docstring:** GRID Intelligence — Hypothesis Discovery Engine.
**Functions:** `DiscoveredPattern`, `Anomaly`, `Hypothesis`, `ensure_tables`, `TemporalPatternDetector`, `AnomalyHunter`, `HypothesisGenerator`, `cleanup_hypotheses`, `score_due_active_hypotheses`, `get_stats`, `main`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `db`, `hashlib`, `intelligence`, `json`, `loguru`, `math`, `numpy`, `re`, `scipy`, `sqlalchemy`, `sys`, `time`, `typing`
**Imported by:** `intelligence/hypothesis_trials.py`

#### `intelligence/hypothesis_trials.py` — 956 LOC
**Docstring:** GRID Intelligence — honest scoring of discovered hypotheses (one-look trials).
**Functions:** `TrialLedgerNotConfigured`, `is_self_generated`, `normalize_direction`, `TrialPlan`, `plan_trial`, `TrialResult`, `entry_session_floor`, `trial_window`, `scaled_move`, `ticker_move_test`, `lead_lag_test`, `ticker_move_close_universe`, `load_completed_closes`, `crowd_tickers`, `load_signal_series`, `run_trial`, `open_trial_ledger`, `dry_run_ledger`, `ensure_trial_columns`, `apply_verdicts`, `evaluate_batch`, `score_batch`, `summarize_dry_run`, `read_only_engine`, `main`
**Reads:** `__future__`, `analysis`, `argparse`, `collections`, `config`, `contextlib`, `dataclasses`, `datetime`, `db`, `evaluation`, `fcntl`, `hashlib`, `intelligence`, `json`, `math`, `numpy`, `pathlib`, `sqlalchemy`, `store`, `time`, `typing`, `zoneinfo`
**Imported by:** `ingestion/tiingo_pull.py`, `intelligence/hypothesis_engine.py`, `intelligence/sleuth_intersections.py`
**Notes:** The v2 trial scorer (PR #1008; `SCORER_VERSION = "hypothesis-trials/4"`, which since 2026-11-07 ranks ticker moves against the crowd). It replaced the v1 scorer that `ACTIVE_HYPOTHESIS_SCORING_HELD` had held since 2026-09-26; that flag is now `False`. Each hypothesis gets one look after its window closes. GRID's own outputs are `ineligible` (`is_self_generated`) and hypotheses with no market outcome are `untestable`; both are recorded at p = 1. Ticker-move trials read only completed raw closes dated after `created_at` (`load_completed_closes` → `store.observations.read_completed_closes`, with the boundary and sources from `evaluation/prices.py`). Each stock is ranked against a same-window crowd of `CROSS_SECTION_SIZE` = 500 US stocks, in units of its own pre-creation volatility. Lead-lag trials run at their declared lag and sign against a block-permutation null. Every batch is appended to the S11 ledger (`analysis/ledger_steered_exploration.py`, BH-FDR) before any row changes. With no ledger configured, `score_batch` writes nothing; `dry_run=True` touches neither the ledger nor the DB. **Close universe:** `ticker_move_close_universe` (`ACTIVE_TICKER_MOVE_TICKERS_SQL`) returns every ticker of an active ticker-move hypothesis. `ingestion/tiingo_pull.py` reads it so that declared trials get routine closes.
**DB writes:** `discovered_hypotheses` (verdict columns; `apply_verdicts`, after the ledger append)
**Tests:** `tests/test_hypothesis_trials.py`, `tests/test_hypothesis_kills.py`, `tests/test_hypothesis_scoring_hold.py`, `tests/test_oracle_close_coverage.py`

#### `intelligence/icij_linker.py` — 196 LOC
**Docstring:** ICIJ Linker — fuzzy-match ICIJ offshore entities against the actor network.
**Functions:** `ActorMatch`, `link_actors`, `get_offshore_connections`
**Reads:** `__future__`, `dataclasses`, `intelligence`, `loguru`, `sqlalchemy`, `typing`

#### `intelligence/image_gen.py` — 427 LOC
**Docstring:** GRID — AI Image Generation via Gemini Imagen.
**Functions:** `ImageResult`, `generate_flow_infographic`, `generate_sector_heatmap`, `generate_junction_dashboard`, `generate_market_briefing_image`, `generate_custom`, `generate_daily_briefing_pack`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `google`, `llm`, `loguru`, `os`, `pathlib`, `time`, `typing`
**Imported by:** `api/routers/flows.py`

#### `intelligence/influence_network.py` — 954 LOC
**Docstring:** GRID Intelligence — Influence Network (Crown Jewel Analysis).
**Functions:** `InfluenceLoop`, `ensure_table`, `build_influence_graph`, `detect_circular_flows`, `get_influence_for_ticker`, `vote_trade_hypocrisy`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_forensics.py`, `intelligence/api_precompute.py`, `intelligence/company_analyzer.py`

#### `intelligence/institutional_map.py` — 1508 LOC
**Docstring:** GRID Intelligence -- Institutional Map: Private Credit, Hedge Funds & Pensions.
**Functions:** `build_institutional_graph`, `trace_pension_dollars`, `find_conflicts_of_interest`, `get_fee_extraction_estimate`, `get_all_fund_managers`, `get_institutional_summary`
**Reads:** `__future__`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_companies.py`

#### `intelligence/legislative_intel.py` — 479 LOC
**Docstring:** GRID Intelligence — Legislative Trading Detection.
**Functions:** `LegislativeHearing`, `BillImpact`, `LegislativeTradeAlert`, `get_upcoming_hearings`, `get_bills_affecting_ticker`, `detect_legislative_trading`, `get_legislation_summary`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_govflow.py`, `intelligence/company_analyzer.py`

#### `intelligence/lever_pullers.py` — 2001 LOC
**Docstring:** GRID Intelligence — Lever Puller Identification & Tracking.
**Functions:** `LeverPuller`, `LeverEvent`, `puller_identity`, `puller_id_for`, `build_puller_index`, `merge_cross_feed_duplicates`, `puller_map_of`, `identify_lever_pullers`, `assess_motivation`, `insider_transaction_code`, `derive_motivation_model`, `get_active_lever_events`, `find_lever_convergence`, `generate_lever_report`, `get_lever_context_for_ticker`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `json`, `llm`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/chat.py`, `api/routers/flows.py`, `api/routers/intel.py`, `api/routers/intelligence_risk.py`, `intelligence/actors/analysis.py`, `intelligence/causation_scoring.py`, `intelligence/codebase_context.py`, `intelligence/company_analyzer.py`, … (+4)

#### `intelligence/liquidity_regime.py` — 299 LOC
**Docstring:** ALPHA-5 / task #108 — Liquidity regime classifier.
**Functions:** `LiquidityRegimeResult`, `classify_from_series`, `classify_current_regime`, `apply_to_confidence`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/contagion_backtest.py`, `oracle/engine.py`, `oracle/regime_router.py`

#### `intelligence/llm_harness.py` — 574 LOC
**Docstring:** llm_harness.py — self-learning wrapper on top of any LLM client.
**Functions:** `HarnessResponse`, `LLMHarness`, `update_temperature_from_outcomes`, `update_no_op`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `typing`

#### `intelligence/llm_narrator.py` — 380 LOC
**Docstring:** LLM narrator — plain-English trade thesis briefing.
**Functions:** `NarrativeReport`, `compose_template_narrative`, `build_narrative_prompt`, `narrate_trade`
**Reads:** `__future__`, `dataclasses`, `datetime`, `langfuse`, `typing`
**Imported by:** `api/routers/conviction.py`

#### `intelligence/llm_red_team.py` — 441 LOC
**Docstring:** CAT-181 — LLM red-team loop per oracle prediction.
**Functions:** `CounterArgument`, `RedTeamReport`, `build_red_team_prompt`, `parse_red_team_response`, `compute_epistemic_risk`, `red_team_prediction`
**Reads:** `__future__`, `dataclasses`, `json`, `langfuse`, `llm`, `loguru`, `re`, `typing`
**Imported by:** `intelligence/decision_gateway.py`

#### `intelligence/long_plays.py` — 1601 LOC
**Docstring:** Long Plays board — multi-year candidates with explicit multiple math.
**Functions:** `clamp`, `market_cap_bucket`, `asymmetry_score`, `trial_route_qualifies`, `coverage_route`, `evaluate_gate`, `classify_stance`, `multiple_math`, `build_universe`, `long_plays_universe`, `build_long_plays_board`, `entry_first`, `ensure_long_plays_table`, `persist_board`, `load_latest_board`
**Reads:** `__future__`, `analysis`, `datetime`, `intelligence`, `json`, `loguru`, `math`, `sqlalchemy`, `store`, `strategy`, `typing`
**Imported by:** `alerts/email.py`, `api/routers/conviction.py`, `intelligence/catalyst_ev.py`, `intelligence/scheduler.py`, `intelligence/trial_outcomes.py`

#### `intelligence/market_diary.py` — 1186 LOC
**Docstring:** GRID — Automated Daily Market Diary.
**Functions:** `ensure_table`, `write_diary_entry`, `get_diary_reference_review`, `get_diary_entry`, `list_diary_entries`, `search_diary`, `schedule_daily_diary`
**Reads:** `__future__`, `argparse`, `datetime`, `db`, `intelligence`, `json`, `llm`, `loguru`, `os`, `outputs`, `schedule`, `sqlalchemy`, `store`, `threading`, `time`, `typing`
**Imported by:** `api/routers/intelligence_thesis.py`

#### `intelligence/market_edge_scanner.py` — 1358 LOC
**Functions:** `PlaybookBlueprint`, `TickerSignalProfile`, `build_market_edge_snapshot`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `decimal`, `json`, `math`, `sqlalchemy`, `statistics`, `typing`
**Imported by:** `api/routers/intelligence_edges.py`, `intelligence/long_plays.py`, `intelligence/scheduler.py`

#### `intelligence/market_implied_prob.py` — 251 LOC
**Docstring:** ALPHA-8 / task #111 — Market-implied probability comparator.
**Functions:** `MarketImpliedProb`, `DivergenceReport`, `options_implied_probability_from_iv`, `options_implied_probability`, `compare_to_oracle`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `oracle/engine.py`

#### `intelligence/meta_learning_matrix.py` — 998 LOC
**Docstring:** Meta learning matrix — per-signal × per-condition edge learner (CAT-193 / #295).
**Functions:** `bucket_horizon`, `bucket_fci`, `bucket_vol`, `ConditionTuple`, `MetaEdgeRow`, `build_condition_tuple`, `record_scored_prediction`, `get_edge_row`, `get_weight_multiplier`, `get_aggregate_weight_multiplier`, `rank_signals_by_edge`, `bootstrap_from_oracle_predictions`, `iter_condition_cube`
**Reads:** `__future__`, `dataclasses`, `datetime`, `features`, `json`, `loguru`, `oracle`, `scripts`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/milestone_tracker.py` — 262 LOC
**Docstring:** Milestone Tracker — plot company milestones on a timeline, score execution.
**Functions:** `Milestone`, `build_earnings_timeline`, `score_execution`, `scan_all_tickers`
**Reads:** `__future__`, `dataclasses`, `datetime`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/money_flow_adapter.py` — 356 LOC
**Docstring:** Money flow conviction adapter — the 14th live-stack multiplier.
**Functions:** `MoneyFlowConvictionReport`, `compute_money_flow_conviction`, `money_flow_conviction_multiplier`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/news_contagion_listener.py` — 709 LOC
**Docstring:** News-driven contagion listener.
**Functions:** `Candidate`, `detect_patterns`, `resolve_entity`, `scan_news`, `run_once`
**Reads:** `__future__`, `analysis`, `contracts`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`, `uuid`

#### `intelligence/news_impact.py` — 980 LOC
**Docstring:** GRID News Impact Attribution Engine.
**Functions:** `Catalyst`, `MoveAttribution`, `Expectation`, `DeepDiveReport`, `CatalystClassifier`, `PriceDecomposer`, `ExpectationTracker`, `DeepDiveEngine`, `generate_deep_dive_tasks`, `run_deep_dive_task`, `ensure_tables`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `intelligence`, `json`, `llm`, `loguru`, `re`, `sqlalchemy`, `statistics`, `typing`
**Imported by:** `api/routers/intel.py`, `api/routers/intelligence_deepdive.py`

#### `intelligence/news_intel.py` — 558 LOC
**Docstring:** GRID Intelligence — News Intelligence & Narrative Analysis.
**Functions:** `get_news_feed`, `get_news_stats`, `detect_narrative_shift`, `find_news_before_move`, `generate_news_briefing`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_news.py`

#### `intelligence/news_momentum.py` — 902 LOC
**Docstring:** GRID Intelligence — News Momentum Signal Engine.
**Functions:** `SentimentSnapshot`, `MomentumSignal`, `SentimentTimeSeries`, `MomentumCalculator`, `DivergenceDetector`, `NewsMomentumEngine`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/news_ticker_resolver.py` — 472 LOC
**Docstring:** News ticker resolver — extract real ticker symbols from news title+content.
**Functions:** `resolve_tickers`
**Reads:** `__future__`, `analysis`, `functools`, `re`
**Imported by:** `ingestion/altdata/news_scraper.py`

#### `intelligence/null_hypothesis_forecaster.py` — 646 LOC
**Docstring:** Null-hypothesis forecaster — CAT-186 (baseline skeptic for aggregate conviction).
**Functions:** `NullBaselineResult`, `NullHypothesisReport`, `evaluate_null_hypothesis`, `null_hypothesis_penalty`
**Reads:** `__future__`, `dataclasses`, `datetime`, `features`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/obsidian_agent.py` — 508 LOC
**Docstring:** Obsidian Agent — active intelligence loop for the vault.
**Functions:** `extract_entities`, `rank_for_review`, `should_escalate_to_paid`, `enrich_note`, `act_on_approval`, `compute_preferences`, `build_proactive_note`, `create_proactive_note`, `run_agent_cycle`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `oracle`, `re`, `sqlalchemy`, `typing`

#### `intelligence/obsidian_log.py` — 136 LOC
**Docstring:** Write hermes activity to the Obsidian vault session log.
**Functions:** `append_cycle_entry`, `log_trust_cycle`, `log_intelligence_task`
**Reads:** `__future__`, `datetime`, `loguru`, `os`, `pathlib`, `typing`

#### `intelligence/opsec.py` — 456 LOC
**Docstring:** GRID Intelligence Operations Security (OPSEC) Module.
**Functions:** `user_can_view`, `get_user_tier`, `AuditLogger`, `EncryptedIntelStore`, `audit_sensitive`
**Reads:** `__future__`, `api`, `functools`, `hashlib`, `json`, `loguru`, `os`, `sqlalchemy`, `typing`
**Imported by:** `subnet/validator.py`

#### `intelligence/pair_conviction.py` — 830 LOC
**Docstring:** Pair trade conviction detector.
**Functions:** `PairLeg`, `PairTradeTicket`, `PairCandidate`, `compute_spread_sharpness`, `compute_pair_conviction_score`, `is_correlated_risk_trap`, `compose_pair_thesis`, `compute_pair_invalidation`, `verdict_from_pair_conviction`, `size_pair_legs`, `generate_pair_ticket`, `scan_candidate_pairs`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `typing`
**Imported by:** `api/routers/conviction.py`

#### `intelligence/pattern_engine.py` — 919 LOC
**Docstring:** GRID Intelligence -- Pattern Detection Engine.
**Functions:** `Pattern`, `discover_patterns`, `match_active_patterns`, `score_pattern_accuracy`, `get_patterns_for_ticker`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `hashlib`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `intelligence/event_sequence.py`

#### `intelligence/pattern_library.py` — 848 LOC
**Docstring:** GRID Pattern Library — Independent Conviction Layer via Historical Analog Matching.
**Functions:** `MarketStateVector`, `HistoricalAnalog`, `BaseRateDistribution`, `PatternMatchReport`, `cosine_similarity`, `normalize_zscore`, `normalize_minmax`, `normalize_clamp_div`, `normalize_ordinal`, `compute_base_rate`, `find_nearest_analogs`, `confidence_signal_from_base_rates`, `build_state_vector`, `query_historical_states`, `read_forward_returns`, `build_pattern_match_report`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/decision_gateway.py`

#### `intelligence/pct_cogs_enrichment.py` — 1790 LOC
**Docstring:** LLM-driven supplier-cost-concentration enrichment for ``supply_chain_edges``.
**Functions:** `LLMUnavailableError`, `EdgeRow`, `AttemptRecord`, `EnrichmentSummary`, `PctCogsEnricher`, `run_weekly`
**Reads:** `__future__`, `dataclasses`, `db`, `ingestion`, `json`, `llm`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `intelligence/people_events_materializer.py` — 445 LOC
**Docstring:** Materialize `people_events` rows from existing people-linked channels (GD2).
**Functions:** `round_up_to_next_session_open`, `normalize_actor_name`, `form4_transaction_direction`, `form4_dedup_key`, `qq_form4_known_at`, `MaterializeResult`, `materialize_qq_form4`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `re`, `sqlalchemy`, `store`, `typing`

#### `intelligence/people_events_pipeline/__init__.py` — 29 LOC
**Docstring:** People-events pipeline (GD2 materializer + GD3 Form 4 backfill planner).
**Imported by:** `intelligence/people_events_pipeline/adapters.py`, `intelligence/people_events_pipeline/dryrun.py`, `intelligence/people_events_pipeline/merge.py`, `intelligence/people_events_pipeline/security.py`, `intelligence/people_events_pipeline/writer.py`

#### `intelligence/people_events_pipeline/adapters.py` — 979 LOC
**Docstring:** Source rows -> canonical candidate rows, one adapter per source.
**Functions:** `SourceSpec`, `empty_candidates`, `form4_from_form345`, `form4_from_qq_insider`, `form4_from_edgar_native`, `congress_from_qq`, `congress_from_native`, `gov_contract_from_usaspending`, `federal_fiscal_quarter`, `gov_contract_qq_aggregate`, `lobbying_from_lda`, `lobbying_from_qq`, `fara_from_signal_sources`, `thirteen_f_changes`, `from_signal_sources`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `intelligence`, `json`, `numpy`, `pandas`, `typing`
**Imported by:** `intelligence/people_events_pipeline/merge.py`, `intelligence/people_events_pipeline/readonly.py`

#### `intelligence/people_events_pipeline/dryrun.py` — 206 LOC
**Docstring:** Dry run: what the materializer WOULD write, as counts. Never writes anything.
**Functions:** `form345_columns`, `qq_collision_estimate`, `form4_overlap`, `run_dry_run`, `to_jsonable`
**Reads:** `__future__`, `collections`, `datetime`, `intelligence`, `numpy`, `pandas`, `time`, `typing`

#### `intelligence/people_events_pipeline/merge.py` — 266 LOC
**Docstring:** Candidates -> canonical events: one row per (channel, dedup_key).
**Functions:** `MergeResult`, `content_hash_series`, `merge_candidates`, `pit_violations`, `link_echoes`, `numpy_safe`
**Reads:** `__future__`, `collections`, `dataclasses`, `hashlib`, `intelligence`, `numpy`, `pandas`, `typing`

#### `intelligence/people_events_pipeline/plan.py` — 332 LOC
**Docstring:** Canonical events vs. rows already stored -> an append-only write plan.
**Functions:** `current_rows`, `version_floors`, `build_write_plan`, `plan_counts`, `apply_in_memory`, `visible_at`, `max_visible_versions`
**Reads:** `__future__`, `collections`, `intelligence`, `json`, `pandas`, `typing`

#### `intelligence/people_events_pipeline/readonly.py` — 229 LOC
**Docstring:** The only database access in this package: read-only, bounded, outside the backup window.
**Functions:** `WindowClosed`, `assert_db_window_open`, `guard_sql`, `readonly_engine`, `read_stored_rows`, `read_security_identifiers`, `read_inputs`
**Reads:** `__future__`, `datetime`, `intelligence`, `pandas`, `re`, `sqlalchemy`, `typing`

#### `intelligence/people_events_pipeline/rules.py` — 372 LOC
**Docstring:** Per-channel rules for canonical people-events: known_at, keys, direction, confidence.
**Functions:** `section16_filing_known_at`, `next_session_open_after`, `section16_filing_known_at_series`, `next_session_open_after_series`, `to_utc`, `vendor_time_bound`, `parse_date`, `name_tokens`, `name_tokens_series`, `normalize_ticker`, `normalize_ticker_series`, `cik_int`, `cik_text`, `issuer_key`, `form4_dedup_key`, `form4_loose_key`, `amount_band_low`, `congress_dedup_key`, `thirteen_f_dedup_key`, `gov_contract_dedup_key`, `qq_gov_aggregate_dedup_key`, `lobbying_dedup_key`, `fara_dedup_key`, `form4_direction`, `congress_direction`, `confidence`, `stable_hash`, `first_present`
**Reads:** `__future__`, `datetime`, `hashlib`, `ingestion`, `json`, `pandas`, `re`, `typing`, `zoneinfo`
**Imported by:** `intelligence/people_events_pipeline/plan.py`

#### `intelligence/people_events_pipeline/security.py` — 120 LOC
**Docstring:** Point-in-time resolution of event issuers onto ``security_master.entity_id`` (GD1).
**Functions:** `resolve_securities`, `match_summary`
**Reads:** `__future__`, `intelligence`, `pandas`, `typing`

#### `intelligence/people_events_pipeline/writer.py` — 300 LOC
**Docstring:** Apply a write plan to ``people_events`` (requires migration ``people_events_v2_20261001``).
**Functions:** `event_row`, `apply_write_plan`
**Reads:** `__future__`, `contextlib`, `datetime`, `intelligence`, `json`, `pandas`, `sqlalchemy`, `typing`

#### `intelligence/pocket_lining.py` — 284 LOC
**Docstring:** GRID Intelligence — Pocket-Lining Detection.
**Functions:** `assess_pocket_lining`
**Reads:** `__future__`, `collections`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`
**Imported by:** `intelligence/actor_network.py`

#### `intelligence/post_query_scanner.py` — 382 LOC
**Docstring:** GRID — Post-Query Data Gap Scanner.
**Functions:** `scan_data_gaps`, `spawn_post_query_scan`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `re`, `sqlalchemy`, `threading`, `typing`
**Imported by:** `api/routers/chat.py`, `intelligence/freshness_guard.py`

#### `intelligence/postmortem.py` — 2307 LOC
**Docstring:** GRID Intelligence — Automated Post-Mortem Analysis for Failed Trades & Predictions.
**Functions:** `PostMortem`, `generate_postmortem`, `generate_prediction_postmortem`, `batch_postmortem`, `generate_lessons_learned`, `record_success_lesson`, `load_postmortems_top_n`, `count_postmortems`, `load_postmortems`, `apply_contagion_feedback`
**Reads:** `__future__`, `collections`, `contracts`, `dataclasses`, `datetime`, `db`, `decimal`, `intelligence`, `json`, `llm`, `loguru`, `oracle`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_risk.py`, `api/routers/postmortem_lessons.py`

#### `intelligence/power_mapper.py` — 91 LOC
**Docstring:** Power Mapper — unified power-mapping layer over multiple relationship sources.
**Functions:** `PowerEdge`, `resolve_edge_weight`
**Reads:** `__future__`, `dataclasses`

#### `intelligence/prediction_calibration.py` — 521 LOC
**Docstring:** GRID Prediction Market Calibration Checker.
**Functions:** `PredictionCalibrationChecker`
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `math`, `sqlalchemy`, `typing`

#### `intelligence/prediction_market_arbitrage.py` — 566 LOC
**Docstring:** GRID Prediction Market Arbitrage Detector (CAT-183 / #285).
**Functions:** `ArbitrageReport`, `get_market_implied_prob`, `get_oracle_vs_market_calibration`, `build_arbitrage_report`, `arbitrage_conviction_multiplier`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/rag.py` — 1264 LOC
**Docstring:** GRID RAG (Retrieval-Augmented Generation) Intelligence System.
**Functions:** `RAGIndexer`, `RAGRetriever`, `get_rag_context`, `ask`, `main`
**Reads:** `__future__`, `argparse`, `collections`, `config`, `db`, `json`, `loguru`, `numpy`, `os`, `requests`, `sentence_transformers`, `sklearn`, `sqlalchemy`, `sys`, `time`, `typing`
**Imported by:** `intelligence/causation_graph.py`, `intelligence/causation_scoring.py`, `intelligence/forensics.py`, `intelligence/postmortem.py`, `intelligence/sleuth.py`, `intelligence/thesis_tracker.py`

#### `intelligence/reasoning_bank.py` — 623 LOC
**Docstring:** GRID Intelligence — ReasoningBank-style memory layer.
**Functions:** `ReasoningLesson`, `write_reasoning_lesson`, `retrieve_lessons`, `lesson_count`, `build_fingerprint_from_decision_data`, `LessonEvidence`, `memory_lesson_evidence`, `memory_lesson_conviction_multiplier`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/postmortem.py`, `intelligence/signal_provenance.py`, `oracle/engine.py`

#### `intelligence/regime/__init__.py` — 59 LOC
**Docstring:** GRID Regime-Matched Historical Analog Engine.
**Reads:** `intelligence`

#### `intelligence/regime/classifier.py` — 435 LOC
**Docstring:** Regime classification engine.
**Functions:** `RegimeLabel`, `RegimeClassification`, `classify_regime`, `classify_regime_with_history`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `numpy`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_regime.py`, `intelligence/api_precompute.py`, `intelligence/regime/__init__.py`

#### `intelligence/regime/episode_matcher.py` — 376 LOC
**Docstring:** Episode matching engine for the regime analog system.
**Functions:** `MatchedEpisode`, `MatchResult`, `find_analogous_episodes`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/api_precompute.py`, `intelligence/regime/__init__.py`, `intelligence/regime/forecast.py`

#### `intelligence/regime/forecast.py` — 351 LOC
**Docstring:** Conditional forecast generation from matched historical episodes.
**Functions:** `OutcomeDistribution`, `ConditionalForecast`, `generate_conditional_forecast`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `numpy`, `typing`
**Imported by:** `intelligence/api_precompute.py`, `intelligence/regime/__init__.py`

#### `intelligence/regime/state_vector.py` — 1225 LOC
**Docstring:** State vector construction for the regime-matched analog engine.
**Functions:** `DimensionSpec`, `StateVector`, `compute_state_vector`, `compute_state_vector_series`, `cache_state_vector`, `load_cached_vectors`, `get_or_compute_state_vector`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `json`, `loguru`, `numpy`, `pandas`, `re`, `sqlalchemy`, `store`, `typing`, `zoneinfo`
**Imported by:** `api/routers/intelligence_regime.py`, `intelligence/api_precompute.py`, `intelligence/regime/__init__.py`, `intelligence/regime/classifier.py`, `intelligence/regime/episode_matcher.py`

#### `intelligence/resolution_audit.py` — 1059 LOC
**Docstring:** GRID resolution audit supervisor.
**Functions:** `AuditFinding`, `check_duplicates`, `check_stale_data`, `check_value_sanity`, `check_coverage_completeness`, `check_entity_map_consistency`, `check_cross_source_agreement`, `auto_fix_issues`, `run_full_audit`, `audit_after_resolve`, `get_latest_audit_results`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `math`, `normalization`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/system.py`

#### `intelligence/risk_factor_novelty.py` — 367 LOC
**Docstring:** CAT-152 — 10-K/10-Q Risk Factors (Item 1A) novelty detector.
**Functions:** `RiskFactorChange`, `RiskNoveltyResult`, `tokenize`, `split_sentences`, `ngrams`, `jaccard_similarity`, `token_change_ratio`, `detect_novelty`, `compute_novelty`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/runtime_schema.py` — 58 LOC
**Docstring:** One explicit bounded transaction for thesis and Task11 publication schema.
**Functions:** `prepare_runtime_schema`
**Reads:** `intelligence`

#### `intelligence/scheduler.py` — 1357 LOC
**Docstring:** GRID Intelligence Scheduler — background loop for periodic intelligence tasks.
**Functions:** `run_intelligence_loop`
**Reads:** `__future__`, `alpha_research`, `analysis`, `config`, `db`, `grid`, `ingestion`, `intelligence`, `loguru`, `ollama`, `oracle`, `schedule`, `scripts`, `sqlalchemy`, `time`, `trading`, `typing`

#### `intelligence/schema_readiness.py` — 162 LOC
**Docstring:** Read-only schema contracts and explicitly invoked bounded schema transactions.
**Functions:** `SchemaNotReady`, `SchemaPreparationTimeout`, `ColumnRequirement`, `require_relation`, `require_relations`, `quote_schema`, `bounded_schema_transaction`
**Reads:** `__future__`, `contextlib`, `dataclasses`, `re`, `sqlalchemy`, `time`
**Imported by:** `intelligence/runtime_schema.py`, `intelligence/thesis_schema.py`, `intelligence/thesis_tracker.py`, `ollama/market_briefing.py`

#### `intelligence/sec_filing_extractor.py` — 716 LOC
**Docstring:** GRID Intelligence — SEC Filing Content Extractor.
**Functions:** `MaterialFact`, `SECFilingExtractor`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/sector_health.py` — 728 LOC
**Docstring:** Sector health composite score.
**Functions:** `compute_sector_health`, `snapshot_all_sectors`
**Reads:** `__future__`, `analysis`, `collections`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/sector_health.py`

#### `intelligence/sector_networks/__init__.py` — 15 LOC
**Docstring:** Sector network YAML data + loader.
**Reads:** `loader`

#### `intelligence/sector_networks/loader.py` — 129 LOC
**Docstring:** Canonical YAML loader for sector network data.
**Functions:** `list_sectors`, `load_sector_network`, `get_sector_data`, `get_actors`, `clear_cache`
**Reads:** `__future__`, `os`, `time`, `typing`, `yaml`
**Imported by:** `intelligence/adapters/sector_network_adapter.py`, `intelligence/pair_conviction.py`, `intelligence/universe_ranker.py`

#### `intelligence/security_master.py` — 515 LOC
**Docstring:** GD1 — point-in-time security master (issuers/companies only).
**Functions:** `ensure_tables`, `entity_id_for_cik`, `entity_id_for_ticker`, `ResolvedEntity`, `resolve_entity`, `resolve_primary_sector`, `compute_sector_weights`, `propose_primary_sector`, `propose_primary_sector_with_sic`, `DelistingAssessment`, `evaluate_delisting_candidate`, `crosswalk_sector`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `analysis/people_density.py`

#### `intelligence/self_learning_loop.py` — 594 LOC
**Docstring:** Self-learning loop primitive — CAT-unnumbered.
**Functions:** `ScoredEmission`, `LoopState`, `SelfLearningLoop`, `list_learning_modules`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `intelligence/grand_orchestrator.py`, `intelligence/llm_harness.py`

#### `intelligence/sentiment_scorer.py` — 1093 LOC
**Docstring:** GRID Intelligence — Deterministic Market Sentiment Scorer.
**Functions:** `SentimentComponent`, `SentimentResult`, `compute_sentiment`, `log_prediction`, `score_past_predictions`, `run_sentiment_cycle`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/scheduler.py`, `ollama/market_briefing.py`

#### `intelligence/shapley_attribution.py` — 231 LOC
**Docstring:** ALPHA-9 / task #112 — Shapley-value attribution per prediction.
**Functions:** `ShapleyAttribution`, `shapley_exact`, `shapley_leave_one_out`, `attribute_votes`
**Reads:** `__future__`, `dataclasses`, `itertools`, `math`, `typing`
**Imported by:** `oracle/engine.py`

#### `intelligence/shipping_fudge_detector.py` — 591 LOC
**Docstring:** GRID Shipping Fudge Detector — Lie detector for shipping/port statistics.
**Functions:** `pairings_for_port`, `check_pairing`, `check_port_reported_vs_observed`, `run_shipping_fudge_detector`, `get_fudge_alerts`
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `math`, `sqlalchemy`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/short_squeeze_composite.py` — 547 LOC
**Docstring:** Short-Squeeze Composite Scorer (CAT-138 / #250).
**Functions:** `SqueezeComponent`, `SqueezeReport`, `compute_squeeze_report`, `squeeze_conviction_multiplier`, `rank_universe_by_squeeze`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `store`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/signal_backlinker.py` — 497 LOC
**Docstring:** Signal Backlinker — closes the loop between signals and the actor graph.
**Functions:** `is_real_actor`, `ensure_backlinker_state`, `backlink_signals`, `update_trust_from_signal_density`, `run_backlinker`
**Reads:** `__future__`, `argparse`, `config`, `db`, `intelligence`, `json`, `loguru`, `re`, `sqlalchemy`, `sys`, `time`, `typing`

#### `intelligence/signal_convergence_scanner.py` — 1418 LOC
**Docstring:** GRID Intelligence — Signal Convergence Scanner (Dot-Connector).
**Functions:** `StreamSignal`, `ConvergenceReport`, `scan_convergence`, `convergence_conviction_multiplier`, `rank_universe_by_convergence`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/signal_cooccurrence.py` — 792 LOC
**Docstring:** Pairwise signal co-occurrence tracker.
**Functions:** `SignalPair`, `CooccurrenceStats`, `ensure_cooccurrence_table`, `canonical_pair`, `compute_independence_baseline`, `compute_lift`, `get_firing_signals`, `compute_pair_lift_multiplier`, `record_joint_prediction`, `get_cooccurrence_stats`, `get_stats_for_signal`, `get_lift_multiplier`, `bootstrap_from_oracle_predictions`
**Reads:** `__future__`, `dataclasses`, `datetime`, `itertools`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/signal_provenance.py`

#### `intelligence/signal_extractor.py` — 390 LOC
**Docstring:** Signal Extractor — bridges raw_series → signal_data with actor attribution.
**Functions:** `extract_from_raw_series`, `extract_from_signal_sources`, `run_extractor`
**Reads:** `__future__`, `config`, `datetime`, `db`, `intelligence`, `json`, `loguru`, `os`, `sqlalchemy`, `sys`, `time`

#### `intelligence/signal_health_monitor.py` — 696 LOC
**Docstring:** GRID Signal Health Monitor — "is the signal even still alive?" check.
**Functions:** `SignalHealth`, `SignalHealthReport`, `match_cadence`, `classify_staleness`, `classify_nan_rate`, `classify_drift`, `combine_status`, `dampening_for_status`, `compose_summary`, `ensure_health_table`, `audit_one_series`, `audit_all_series`, `get_signal_dampening`, `persist_report`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/conviction.py`

#### `intelligence/signal_provenance.py` — 1042 LOC
**Docstring:** Signal provenance — the per-ticker "why" report.
**Functions:** `SignalEvidence`, `CausationChain`, `TradeProvenanceReport`, `ConvictionAggregate`, `compute_evidence_coverage`, `aggregate_conviction_with_coverage`, `compute_aggregate_conviction`, `build_provenance_report`
**Reads:** `__future__`, `dataclasses`, `datetime`, `features`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/counterfactual_stress.py`, `intelligence/decision_gateway.py`, `trading/trade_ticket_generator.py`

#### `intelligence/signal_registry.py` — 190 LOC
**Docstring:** GRID Intelligence — Signal Registry.
**Functions:** `SignalType`, `Direction`, `RegisteredSignal`, `make_signal_id`, `SignalRegistry`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `json`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `api/routers/signal_registry.py`, `intelligence/adapters/ai_trader_adapter.py`, `intelligence/adapters/base.py`, `intelligence/adapters/cross_reference_adapter.py`, `intelligence/adapters/dollar_flows_adapter.py`, `intelligence/adapters/earnings_adapter.py`, `intelligence/adapters/feature_adapter.py`, … (+9)

#### `intelligence/signal_weight_overrides.py` — 154 LOC
**Docstring:** Per-signal conviction multipliers derived from the auto-improve corpus.
**Functions:** `get_override`, `set_enabled`
**Reads:** `__future__`, `loguru`, `os`, `typing`
**Imported by:** `intelligence/signal_provenance.py`, `oracle/engine.py`

#### `intelligence/sleuth.py` — 1309 LOC
**Docstring:** GRID Intelligence — Investigative Research Engine (Sleuth).
**Functions:** `Lead`, `ensure_tables`, `Sleuth`, `get_sleuth`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `db`, `intelligence`, `json`, `langfuse`, `llm`, `loguru`, `ollama`, `sqlalchemy`, `store`, `typing`, `uuid`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`, `intelligence/sleuth_intersections.py`
**Status:** The lead generators are API/chat-only: Hermes never scheduled them (audit 2026-11-07). The scheduled path is `intelligence/sleuth_intersections.py`; this module still owns `investigation_leads` (`ensure_tables`, `_save_lead`).

#### `intelligence/sleuth_intersections.py` — 707 LOC
**Docstring:** Sleuth intersection leads: independent sources agreeing on one ticker.
**Functions:** `is_testable_ticker`, `Observation`, `FamilyVote`, `family_votes`, `IntersectionLead`, `classify_signal`, `robust_z`, `excess_log_returns`, `price_observation`, `find_intersections`, `collect_signal_observations`, `insider_actor`, `dedupe_insider_rows`, `collect_price_observations`, `price_candidates`, `generate_intersection_leads`, `evidence_direction`, `baseline_id`, `declare_baseline`, `declare_missing_baselines`, `declare_hypothesis`, `investigate_intersection_lead`
**Reads:** `__future__`, `dataclasses`, `datetime`, `evaluation`, `hashlib`, `intelligence`, `itertools`, `json`, `loguru`, `math`, `re`, `sqlalchemy`, `statistics`, `typing`
**Imported by:** `ingestion/tiingo_pull.py`, `intelligence/hypothesis_engine.py`
**Notes:** The scheduled Sleuth path since 2026-11-07 (`scripts/hermes_operator.py` calls `generate_intersection_leads`; `scripts/goal_worker.py` runs `investigate_intersection_lead`); the generators in `intelligence/sleuth.py` are API/chat-only. **Intersection leads:** a lead needs `MIN_FAMILIES` = 2 independent evidence families (`options_flow`, `insider`, `congress`, `gov_contract`, `retail_sentiment`; `classify_signal`) agreeing on one ticker's direction inside `WINDOW_DAYS` = 7. One source alone is never a lead. Every observation carries its citation and must be point-in-time (`created_at <= as_of`). Placeholder tickers are skipped (`is_placeholder_ticker`), and QuiverQuant WSB casts no vote while its snapshot is frozen (`_FROZEN_SOURCES`). **Construction v2** (`CONSTRUCTION = "v2"`, 2026-11-08, PR #1071; recorded in every lead's evidence): (1) `price_move` (robust excess-return z vs SPY, `robust_z`) is context only (`CONTEXT_FAMILIES`): it is cited but never counted for or against a lead. (2) QuiverQuant insider rows vote only for open-market trades, code P (up) and code S (down). Grants, exercises, tax withholding and gifts are not evidence. (3) One insider vote per (insider, day, direction), whichever feed carries it and however many fills it has (`insider_actor`, `dedupe_insider_rows`). **Baselines:** every saved lead immediately gets an unreviewed `intersection_baseline` hypothesis with the same test (`declare_baseline`, `declare_missing_baselines`). This is hypothesis generator v2, also reached from `HypothesisEngine.auto_discover`. **Investigator:** `investigate_intersection_lead` ends every lead in one of two declared outcomes. `hypothesis_declared` writes a `discovered_hypotheses` row (pattern `sleuth_intersection`) with a fixed test: ticker, the lead's direction, `HYPOTHESIS_WINDOW_DAYS` = 14 and `HYPOTHESIS_MIN_MOVE_PCT` = 2.0. That row is scored once by `intelligence/hypothesis_trials.py`. `dead_end` records the reason. The LLM may veto a lead or write its narrative, but never chooses the ticker, direction, window or threshold.
**DB writes:** `investigation_leads` (`origin='intersection'`, via `Sleuth._save_lead`), `discovered_hypotheses`
**Tests:** `tests/test_sleuth_intersections.py`, `tests/test_sleuth_lead_construction_v2.py`, `tests/test_placeholder_tickers.py`

#### `intelligence/source_audit.py` — 954 LOC
**Docstring:** GRID Intelligence — Data Source Taxonomy Audit.
**Functions:** `ensure_tables`, `build_redundancy_map`, `compare_sources`, `detect_discrepancies`, `run_full_audit`, `update_source_priorities`, `get_latest_audit_summary`
**Reads:** `__future__`, `collections`, `datetime`, `db`, `itertools`, `loguru`, `normalization`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intel_source_audit.py`, `api/routers/intelligence_risk.py`

#### `intelligence/source_quality_ablation.py` — 962 LOC
**Docstring:** Bounded source quality ablation for paid-vs-free data decisions.
**Functions:** `SourceOperationalStats`, `SourcePredictionStats`, `SourceRedundancyStats`, `SourceQualityAssessment`, `SourceQualityReport`, `cost_bucket`, `extract_prediction_source_names`, `aggregate_prediction_metrics`, `build_source_quality_report`, `load_source_operational_stats`, `load_prediction_metrics`, `load_prediction_rows`, `count_attributed_prediction_rows`, `load_redundancy_metrics`, `run_source_quality_ablation`, `write_source_quality_report`, `markdown_report`, `scrub_secret_text`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `math`, `outputs`, `pathlib`, `re`, `scripts`, `sqlalchemy`, `typing`

#### `intelligence/source_trust_config.py` — 147 LOC
**Docstring:** GRID Source Trust Configuration.
**Functions:** `get_trust`, `trust_color`, `trust_label`
**Imported by:** `intelligence/trust_scorer.py`

#### `intelligence/spider/__init__.py` — 21 LOC
**Docstring:** GRID Connection Mapping Spider — discovers and maps actor relationships.
**Reads:** `intelligence`

#### `intelligence/spider/daemon.py` — 251 LOC
**Docstring:** Spider daemon — continuous connection mapping loop.
**Functions:** `run_spider`
**Reads:** `__future__`, `argparse`, `config`, `db`, `intelligence`, `loguru`, `os`, `sys`, `time`, `typing`, `urllib`

#### `intelligence/spider/discovery.py` — 145 LOC
**Docstring:** Discovery orchestrator — fans out to source adapters and deduplicates results.
**Functions:** `DiscoveryOrchestrator`
**Reads:** `__future__`, `intelligence`, `loguru`, `typing`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`

#### `intelligence/spider/graph_engine.py` — 342 LOC
**Docstring:** In-memory actor graph with microsecond traversal.
**Functions:** `GraphEngine`
**Reads:** `__future__`, `collections`, `heapq`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `threading`, `typing`
**Imported by:** `api/routers/intelligence_spider.py`, `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`, `intelligence/spider/discovery.py`

#### `intelligence/spider/models.py` — 40 LOC
**Docstring:** Data models for the connection mapping spider.
**Functions:** `DiscoveredConnection`, `ConnectionMeta`, `SpiderStats`
**Reads:** `__future__`, `dataclasses`, `typing`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/discovery.py`, `intelligence/spider/graph_engine.py`, `intelligence/spider/sources/__init__.py`, `intelligence/spider/sources/google_kg.py`, `intelligence/spider/sources/icij_offshore.py`, `intelligence/spider/sources/news_cooccurrence.py`, `intelligence/spider/sources/opencorporates.py`, … (+3)

#### `intelligence/spider/priority_queue.py` — 73 LOC
**Docstring:** Composite-scored expansion queue for the spider daemon.
**Functions:** `PriorityQueue`
**Reads:** `__future__`, `heapq`, `typing`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/__init__.py` — 19 LOC
**Docstring:** Source adapter protocol for the connection mapping spider.
**Functions:** `BaseSourceAdapter`
**Reads:** `__future__`, `intelligence`, `typing`

#### `intelligence/spider/sources/google_kg.py` — 148 LOC
**Docstring:** Google Knowledge Graph adapter — discovers structured entity relationships.
**Functions:** `GoogleKgAdapter`
**Reads:** `__future__`, `intelligence`, `loguru`, `os`, `requests`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/icij_offshore.py` — 163 LOC
**Docstring:** ICIJ Offshore Leaks adapter — discovers offshore entity connections.
**Functions:** `IcijOffshoreAdapter`
**Reads:** `__future__`, `csv`, `intelligence`, `loguru`, `pathlib`, `requests`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/news_cooccurrence.py` — 113 LOC
**Docstring:** GDELT news co-occurrence adapter — discovers entity co-mentions in news.
**Functions:** `NewsCooccurrenceAdapter`
**Reads:** `__future__`, `collections`, `intelligence`, `loguru`, `re`, `requests`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/opencorporates.py` — 146 LOC
**Docstring:** OpenCorporates adapter — discovers corporate registry connections.
**Functions:** `OpenCorporatesAdapter`
**Reads:** `__future__`, `intelligence`, `loguru`, `requests`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/operator_input.py` — 115 LOC
**Docstring:** Operator input adapter — retrieves manually injected connections from DB.
**Functions:** `OperatorInputAdapter`
**Reads:** `__future__`, `db`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `sys`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/sec_crossref.py` — 118 LOC
**Docstring:** SEC EDGAR cross-reference adapter — discovers entity relationships from SEC filings.
**Functions:** `SecCrossRefAdapter`
**Reads:** `__future__`, `intelligence`, `loguru`, `requests`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/wikidata.py` — 119 LOC
**Docstring:** Wikidata SPARQL adapter — discovers structured relationships for public figures.
**Functions:** `build_query`, `WikidataAdapter`
**Reads:** `__future__`, `intelligence`, `loguru`, `requests`, `typing`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/supply_chain_edge_validator.py` — 529 LOC
**Docstring:** GRID Supply-Chain Edge Validator.
**Functions:** `EdgeRow`, `ValidationResult`, `list_edges`, `compute_edge_correlation`, `next_edge_state`, `persist_result`, `validate_edge`, `validate_all_edges`, `summarise_results`, `run_weekly`
**Reads:** `__future__`, `contracts`, `dataclasses`, `datetime`, `db`, `intelligence`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`

#### `intelligence/supply_chokepoints.py` — 560 LOC
**Docstring:** GRID Intelligence — Supply Chain Chokepoint Scoring.
**Functions:** `EdgeContext`, `ScoreBreakdown`, `substitution_penalty`, `buyer_concentration`, `geographic_concentration`, `historical_disruption`, `compute_chokepoint_score`, `find_alternatives`, `score_all_edges`, `flag_chokepoint_nodes`
**Reads:** `__future__`, `contracts`, `dataclasses`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `intelligence/chain_contagion.py`

#### `intelligence/thesis_invalidation_monitor.py` — 421 LOC
**Docstring:** CAT-190 — Automatic thesis invalidation monitor.
**Functions:** `InvalidationEvent`, `MonitorRun`, `evaluate_price_level`, `evaluate_event`, `evaluate_signal_flip`, `evaluate_condition`, `determine_size_down`, `run_monitor`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `intelligence/thesis_schema.py` — 86 LOC
**Docstring:** Thesis schema contracts; preparation must be called explicitly by an admitted owner.
**Functions:** `require_thesis_connection`, `require_thesis_schema`, `prepare_thesis_schema`
**Reads:** `intelligence`, `sqlalchemy`
**Imported by:** `analysis/thesis_scorer.py`, `intelligence/adapters/thesis_tracker_adapter.py`, `intelligence/runtime_schema.py`, `intelligence/thesis_tracker.py`

#### `intelligence/thesis_tracker.py` — 1068 LOC
**Docstring:** GRID Intelligence — Thesis Version Tracker & Post-Mortem System.
**Functions:** `ThesisSnapshot`, `ThesisPostMortem`, `snapshot_thesis`, `score_old_theses`, `generate_thesis_postmortem`, `get_thesis_history`, `get_thesis_accuracy`, `run_thesis_cycle`, `load_thesis_postmortems`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `db`, `decimal`, `intelligence`, `json`, `llm`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `analysis/thesis_scorer.py`, `api/routers/chat.py`, `api/routers/intelligence_thesis.py`, `intelligence/adapters/thesis_tracker_adapter.py`, `intelligence/codebase_context.py`

#### `intelligence/trend_tracker.py` — 968 LOC
**Docstring:** GRID Trend Tracker — Divergence Analysis for Market Trends.
**Functions:** `Trend`, `TrendReport`, `analyze_trends`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/trial_outcomes.py` — 370 LOC
**Docstring:** Score what actually happened at each trial readout.
**Functions:** `ensure_outcome_columns`, `realized_return`, `score_trial_outcomes`, `load_scored_outcomes`
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/catalyst_ev.py`, `intelligence/scheduler.py`

#### `intelligence/trump_proximity.py` — 691 LOC
**Docstring:** GRID Intelligence — Trump-Proximity Score (TPS) v0.
**Functions:** `EvidenceItem`, `TPSResult`, `compute_tps_for_ticker`, `compute_tps_batch`, `persist_snapshot`, `refresh_top_universe`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/tps.py`, `ingestion/scheduler.py`

#### `intelligence/trust_scorer.py` — 2730 LOC
**Docstring:** GRID Intelligence — Source Trust Scoring Framework.
**Functions:** `yf_symbol`, `is_priceable_ticker`, `SourceScore`, `ConvergenceEvent`, `score_pending_signals`, `rescore_source_type`, `corrected_outcome_version`, `use_corrected_outcomes`, `compute_trust_scores`, `update_trust_scores`, `write_trust_scores_cache`, `load_trust_scores_cached`, `PruneSet`, `parse_prune_list`, `active_prune_set`, `get_trusted_sources`, `get_insider_edge`, `detect_convergence`, `generate_trust_report`, `run_trust_cycle`, `register_signal`, `TrustScorer`
**Reads:** `__future__`, `api`, `config`, `dataclasses`, `datetime`, `db`, `evaluation`, `ingestion`, `intelligence`, `json`, `llm`, `logging`, `loguru`, `math`, `os`, `re`, `sqlalchemy`, `typing`, `yfinance`
**Imported by:** `analysis/flow_thesis_data.py`, `analysis/money_flow.py`, `analysis/thesis_scorer.py`, `api/routers/actor_detail.py`, `api/routers/flows.py`, `api/routers/intelligence_risk.py`, `contracts/handlers/trust.py`, `inference/live.py`, … (+10)

#### `intelligence/universe_ranker.py` — 1122 LOC
**Docstring:** Cross-ticker conviction ranker.
**Functions:** `TickerRanking`, `SectorDistribution`, `UniverseRankingReport`, `composite_score`, `classify_regime_signature`, `detect_sector_concentration`, `verdict_counts`, `rank_tickers`, `build_narrative`, `rank_universe`, `ensure_ranking_table`, `persist_ranking`, `load_latest_ranking`, `list_rankings`, `main`
**Reads:** `__future__`, `argparse`, `concurrent`, `dataclasses`, `datetime`, `db`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `sys`, `typing`
**Imported by:** `api/routers/conviction.py`, `intelligence/long_plays.py`, `intelligence/scheduler.py`

#### `intelligence/wealth_tracker.py` — 320 LOC
**Docstring:** GRID Intelligence — Wealth Tracking & Migration.
**Functions:** `track_wealth_migration`, `persist_wealth_flows`
**Reads:** `__future__`, `datetime`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/actor_network.py`

## `ingestion/`

#### `ingestion/__init__.py` — 6 LOC
**Docstring:** GRID ingestion layer.

#### `ingestion/altdata/__init__.py` — 13 LOC
**Docstring:** GRID alternative data ingestion modules.
**Imported by:** `ingestion/altdata/quiverquant.py`, `ingestion/altdata/reddit_options_pulse.py`, `ingestion/altdata/smart_money.py`, `ingestion/social_sentiment.py`, `intelligence/trust_scorer.py`

#### `ingestion/altdata/_deprecated/__init__.py` — 0 LOC

#### `ingestion/altdata/_deprecated/noaa_ais.py` — 251 LOC
**Docstring:** GRID NOAA AIS vessel traffic ingestion module.
**Functions:** `NOAAAISPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`

#### `ingestion/altdata/aaii_sentiment.py` — 509 LOC
**Docstring:** GRID AAII Sentiment Survey ingestion module.
**Functions:** `AAIISentimentPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/actor_news_puller.py` — 751 LOC
**Docstring:** Actor news puller — pull news mentions for every actor in sector_map.
**Functions:** `NewsRow`, `BioRow`, `slugify`, `score_sentiment`, `extract_stance`, `extract_loyalty`, `parse_rfc822`, `ActorNewsPuller`, `enumerate_sector_map_actors`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `html`, `ingestion`, `loguru`, `math`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `urllib`, `xml`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/ads_index.py` — 524 LOC
**Docstring:** GRID ADS Business Conditions Index ingestion module.
**Functions:** `ADSIndexPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `math`, `pandas`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/ag_commodity_futures.py` — 387 LOC
**Docstring:** GRID agricultural + industrial commodity futures ingestion.
**Functions:** `AgCommodityFuturesPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `loguru`, `pandas`, `sqlalchemy`, `time`, `typing`, `yfinance`

#### `ingestion/altdata/ais_ground_truth.py` — 740 LOC
**Docstring:** GRID AIS Ground-Truth Port Presence ingestion module.
**Functions:** `PortSpec`, `AISSnapshot`, `compute_capacity_utilization`, `AISGroundTruthPuller`, `run_ais_ground_truth_puller`
**Reads:** `__future__`, `bs4`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/alphavantage_sentiment.py` — 388 LOC
**Docstring:** GRID Alpha Vantage News Sentiment ingestion module.
**Functions:** `AlphaVantageSentimentPuller`
**Reads:** `__future__`, `datetime`, `dotenv`, `ingestion`, `loguru`, `os`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/analyst_ratings.py` — 340 LOC
**Docstring:** GRID analyst ratings ingestion module.
**Functions:** `AnalystRatingsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `time`, `typing`, `yfinance`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/apple_supplier_list.py` — 807 LOC
**Docstring:** GRID Apple Supplier List Puller.
**Functions:** `SupplierRecord`, `AppleSupplierListStats`, `AppleSupplierListPuller`, `run_annual`
**Reads:** `__future__`, `dataclasses`, `datetime`, `db`, `io`, `loguru`, `pypdf`, `re`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/asset_registries.py` — 840 LOC
**Docstring:** GRID Asset Registry ingestion module.
**Functions:** `AssetRegistryPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/baltic_dry.py` — 579 LOC
**Docstring:** GRID Baltic Dry Index and shipping indices ingestion module.
**Functions:** `BalticDryPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `fedfred`, `ingestion`, `json`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `time`, `typing`
**Status:** RETIRED 2026-11-04 (owner; SmartScheduler `baltic_exchange` hold). The public snapshot froze at 2026-05-20 and the FRED Baltic ids no longer exist.

#### `ingestion/altdata/binance_puller.py` — 404 LOC
**Docstring:** GRID Binance public market data ingestion module.
**Functions:** `BinanceAccessBlocked`, `BinancePuller`
**Reads:** `__future__`, `binance_close_contract`, `datetime`, `ingestion`, `loguru`, `math`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/crypto_signals.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/buyback_execution.py` — 546 LOC
**Docstring:** GRID buyback execution rate vs authorization ingestion module (CAT-67).
**Functions:** `BuybackSnapshot`, `BuybackExecutionPuller`, `run_buyback_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `math`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/campaign_finance.py` — 693 LOC
**Docstring:** GRID campaign finance tracker — FEC API ingestion module.
**Functions:** `CampaignFinancePuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/cboe_indices.py` — 540 LOC
**Docstring:** GRID CBOE volatility and strategy indices ingestion module.
**Functions:** `CBOEPermanentError`, `CBOEIndicesPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `math`, `pandas`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/cftc_cot.py` — 429 LOC
**Docstring:** GRID CFTC Commitments of Traders (COT) data ingestion module.
**Functions:** `ParsedReport`, `ParseOutcome`, `parse_market_records`, `build_payload`, `CFTCCOTPuller`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `json`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/cftc_markets.py` — 368 LOC
**Docstring:** CFTC COT market identity and publication-time rules (pure, no I/O).
**Functions:** `CFTCMarket`, `series_id`, `series_id_for_root`, `is_federal_holiday`, `ReleaseTime`, `compute_release`
**Reads:** `__future__`, `dataclasses`, `datetime`, `functools`, `pandas`, `zoneinfo`
**Imported by:** `analysis/thesis_scorer.py`, `ingestion/altdata/cftc_cot.py`, `ingestion/smart_scheduler.py`, `intelligence/cot_extremes.py`, `intelligence/global_levers.py`

#### `ingestion/altdata/cloudflare_radar_puller.py` — 772 LOC
**Docstring:** GRID Cloudflare Radar data ingestion module.
**Functions:** `CloudflareRadarPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `os`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/company_profiles_puller.py` — 242 LOC
**Docstring:** Company-profile / market-cap enrichment puller for GRID.
**Functions:** `shape_profile_row`, `CompanyProfilesPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/altdata/small_cap_enrichment.py`

#### `ingestion/altdata/congressional.py` — 649 LOC
**Docstring:** GRID congressional trading disclosure ingestion module.
**Functions:** `resolve_disclosure_date`, `CongressionalTradingPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/flow_materializer.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/container_freight.py` — 791 LOC
**Docstring:** GRID container freight ingestion module — CAT-82.
**Functions:** `ContainerFreightSnapshot`, `ContainerFreightPuller`, `run_container_freight_puller`
**Reads:** `__future__`, `akshare`, `bs4`, `config`, `dataclasses`, `datetime`, `db`, `fedfred`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/corporate_actions_parser.py` — 954 LOC
**Docstring:** SEC 8-K corporate actions parser → capital_flows rows.
**Functions:** `ExtractedEvent`, `CorporateActionsParser`
**Reads:** `__future__`, `dataclasses`, `datetime`, `httpx`, `loguru`, `re`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/credit_card_spending.py` — 441 LOC
**Docstring:** GRID credit card spending + delinquency FRED puller (CAT-75).
**Functions:** `CreditCardSnapshot`, `CreditCardSpendingPuller`, `run_credit_card_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `db`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/credit_index_proxies.py` — 581 LOC
**Docstring:** GRID credit-index proxy ingestion module (CAT-7 / CAT-13 / CAT-42).
**Functions:** `CreditProxySnapshot`, `CreditIndexBasis`, `compute_ig_hy_basis`, `CreditIndexProxiesPuller`, `run_credit_index_proxies_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/crypto_etf_flows.py` — 122 LOC
**Docstring:** Crypto ETF flow puller — tracks BTC/ETH ETF volume and flow signals.
**Functions:** `CryptoETFPuller`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`, `yfinance`

#### `ingestion/altdata/cryptoquant_puller.py` — 257 LOC
**Docstring:** CryptoQuant puller — on-chain analytics for BTC and ETH.
**Functions:** `CryptoQuantAuthError`, `CryptoQuantRateLimitedError`, `CryptoQuantPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `numpy`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/dark_pool.py` — 685 LOC
**Docstring:** GRID FINRA ADF/ATS dark pool transparency data ingestion module.
**Functions:** `parse_ats_weekly_records`, `DarkPoolPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/altdata/finra_ats.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/defi_llama_puller.py` — 669 LOC
**Docstring:** GRID DeFi Llama data ingestion module.
**Functions:** `DefiLlamaPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `ingestion`, `intelligence`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/discord_scanner.py` — 771 LOC
**Docstring:** GRID Solana Discord Scanner.
**Functions:** `DiscordUser`, `DiscordScanner`
**Reads:** `__future__`, `aiohttp`, `asyncio`, `concurrent`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `os`, `sqlalchemy`, `typing`, `websockets`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/dune_puller.py` — 654 LOC
**Docstring:** GRID Dune Analytics data ingestion module.
**Functions:** `DunePuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`
**Status:** RETIRED 2026-11-04 (owner; SmartScheduler hold). The saved Dune queries (`DUNE_QUERY_*`) were never created.

#### `ingestion/altdata/earnings_calendar.py` — 689 LOC
**Docstring:** GRID earnings calendar data ingestion module.
**Functions:** `EarningsCalendarPuller`, `get_upcoming_earnings`, `get_recent_earnings`, `get_earnings_history`
**Reads:** `__future__`, `collections`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `math`, `sqlalchemy`, `typing`, `yfinance`
**Imported by:** `api/routers/earnings.py`

#### `ingestion/altdata/earnings_puller.py` — 862 LOC
**Docstring:** GRID earnings data puller — fills the 'earnings' feature family in raw_series.
**Functions:** `EarningsStoreAborted`, `compute_surprise_pct`, `classify_beat_miss`, `EarningsPuller`, `main`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `logging`, `loguru`, `math`, `pandas`, `sqlalchemy`, `time`, `types`, `typing`, `yfinance`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/ecb_ltro.py` — 236 LOC
**Docstring:** Eurosystem longer-term refinancing operations outstanding (ECB Data Portal).
**Functions:** `parse_ecb_sdmx_json`, `ECBLtroPuller`, `run_ecb_ltro_puller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/ecb_tltro.py` — 523 LOC
**Docstring:** GRID ECB TLTRO-III outstanding balance + repayment calendar puller (CAT-12).
**Functions:** `TLTROSnapshot`, `compute_days_to_next_repayment`, `ECBTltroPuller`, `run_ecb_tltro_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/edgar_transcripts.py` — 372 LOC
**Docstring:** SEC EDGAR 8-K Transcript Puller — earnings call transcripts from SEC filings.
**Functions:** `EdgarTranscriptPuller`
**Reads:** `__future__`, `datetime`, `gemma`, `ingestion`, `json`, `llm`, `loguru`, `re`, `requests`, `time`, `typing`

#### `ingestion/altdata/eia_puller.py` — 334 LOC
**Docstring:** GRID EIA energy data ingestion module.
**Functions:** `EIAPuller`, `build_weekly_stocks_request`, `as_display_url`, `parse_weekly_stocks_response`, `fetch_weekly_stocks`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `urllib`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/etherscan_puller.py` — 411 LOC
**Docstring:** Etherscan puller — Ethereum on-chain intelligence.
**Functions:** `EtherscanPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `intelligence`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/export_controls.py` — 669 LOC
**Docstring:** GRID export controls tracker — BIS Entity List & Federal Register ingestion.
**Functions:** `ExportControlsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fara.py` — 1042 LOC
**Docstring:** GRID FARA (Foreign Agent Registration Act) ingestion module.
**Functions:** `FaraFetchError`, `FARAPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fear_greed.py` — 724 LOC
**Docstring:** GRID Fear & Greed Index ingestion module.
**Functions:** `FearGreedPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/fed_liquidity.py` — 820 LOC
**Docstring:** GRID Fed liquidity equation data ingestion module.
**Functions:** `format_liquidity_usd`, `net_liquidity_millions`, `FedLiquidityPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `fedfred`, `ingestion`, `loguru`, `math`, `pandas`, `sqlalchemy`, `time`, `typing`
**Imported by:** `analysis/flow_thesis_data.py`, `analysis/money_flow.py`, `analysis/thesis_scorer.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/fed_speeches.py` — 470 LOC
**Docstring:** GRID Federal Reserve communications ingestion module.
**Functions:** `FedSpeechPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `numpy`, `pandas`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/findkg_puller.py` — 205 LOC
**Docstring:** FinDKG puller — Financial Dynamic Knowledge Graph.
**Functions:** `FinDKGPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `pathlib`, `sqlalchemy`, `typing`
**Status:** RETIRED 2026-11-07 (owner). It is no longer registered: the data files were never downloaded, and the dataset is a static 2018-2023 KG in a format this puller does not read.

#### `ingestion/altdata/finra_ats.py` — 975 LOC
**Docstring:** GRID FINRA ATS (Dark Pool) volume ingestion module.
**Functions:** `FINRAATSPuller`
**Reads:** `__future__`, `datetime`, `hashlib`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/finra_margin_puller.py` — 112 LOC
**Docstring:** GRID FINRA margin debt statistics ingestion module.
**Functions:** `FINRAMarginPuller`
**Reads:** `__future__`, `datetime`, `httpx`, `ingestion`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/finra_short_volume.py` — 686 LOC
**Docstring:** GRID FINRA Daily Short Sale Volume puller (Regulation SHO daily files).
**Functions:** `parse_daily_short_volume_file`, `FINRAShortVolumePuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/altdata/finra_short_volume_agg.py`

#### `ingestion/altdata/finra_short_volume_agg.py` — 274 LOC
**Docstring:** FINRA daily short-volume aggregates: market-wide and index-ETF ratios.
**Functions:** `compute_daily_aggregates`, `FINRAShortVolumeAggPuller`
**Reads:** `__future__`, `argparse`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `typing`

#### `ingestion/altdata/finviz_scraper.py` — 15 LOC
**Docstring:** Retired Finviz import compatibility; use the existing SEC EDGAR source.
**Functions:** `FinvizScraperPuller`
**Reads:** `typing`
**Status:** RETIRED 2026-09-29. This is an import-compatibility shim that raises; use `SECEdgarCompanyPuller`.

#### `ingestion/altdata/fmp_puller.py` — 465 LOC
**Docstring:** Financial Modeling Prep puller — earnings, quotes, sector performance.
**Functions:** `FMPError`, `FMPRestricted`, `FMPAuthError`, `FMPLimitReached`, `FMPPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/altdata/company_profiles_puller.py`, `ingestion/altdata/small_cap_enrichment.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/foia_cables.py` — 1170 LOC
**Docstring:** GRID FOIA diplomatic cables ingestion module.
**Functions:** `FOIACablesPuller`
**Reads:** `__future__`, `datetime`, `feedparser`, `ingestion`, `json`, `loguru`, `playwright`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/freight_cass_ata.py` — 188 LOC
**Docstring:** CAT-81 — Cass Freight Index + ATA Truck Tonnage puller.
**Functions:** `FreightRow`, `FreightPuller`, `run_freight_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/fx_rates.py` — 295 LOC
**Docstring:** GRID FX rates ingestion module.
**Functions:** `FXRatesPuller`
**Reads:** `__future__`, `api`, `datetime`, `ingestion`, `loguru`, `pandas`, `sqlalchemy`, `typing`, `yfinance`

#### `ingestion/altdata/gdelt.py` — 816 LOC
**Docstring:** GRID GDELT news event data ingestion module.
**Functions:** `GDELTPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `os`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`, `zipfile`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/google_trends.py` — 479 LOC
**Docstring:** GRID Google Trends data ingestion module.
**Functions:** `GoogleTrendsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `pandas`, `pytrends`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/gov_contracts.py` — 555 LOC
**Docstring:** GRID government contract tracker — USASpending.gov ingestion module.
**Functions:** `GovContractsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/altdata/lobbying.py`, `ingestion/altdata/regulatory_events.py`, `ingestion/scheduler.py`, `intelligence/influence_network.py`

#### `ingestion/altdata/h8_bank_balance.py` — 250 LOC
**Docstring:** CAT-27 — H.8 bank balance sheet by size class (PUL, Tier A).
**Functions:** `H8Row`, `H8BankBalancePuller`, `run_h8_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/hf_financial_news.py` — 800 LOC
**Docstring:** GRID HuggingFace Financial News ingestion module.
**Functions:** `fetch_hf_dataset_last_modified`, `HFFinancialNewsPuller`
**Reads:** `__future__`, `datasets`, `datetime`, `hashlib`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/hyperliquid_puller.py` — 119 LOC
**Docstring:** Hyperliquid puller — OI, funding rates, liquidations from public API.
**Functions:** `HyperliquidPuller`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/icij_puller.py` — 346 LOC
**Docstring:** ICIJ Offshore Leaks puller — Panama Papers, Paradise Papers, Pandora Papers, etc.
**Functions:** `ICIJPuller`
**Reads:** `__future__`, `csv`, `ingestion`, `intelligence`, `loguru`, `pathlib`, `requests`, `sqlalchemy`, `typing`, `zipfile`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/indeed_hiring_puller.py` — 583 LOC
**Docstring:** GRID Indeed Hiring Lab ingestion module.
**Functions:** `IndeedHiringPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `intelligence`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/insider_filings.py` — 949 LOC
**Docstring:** GRID SEC Form 4 insider trading filings ingestion module.
**Functions:** `SECRateLimitedError`, `InsiderFilingsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `xml`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/institutional_flows.py` — 869 LOC
**Docstring:** GRID institutional money flow data ingestion module.
**Functions:** `InstitutionalFlowsPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `json`, `logging`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `time`, `typing`, `xml`, `yfinance`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/iron_ore_ports.py` — 779 LOC
**Docstring:** Chinese iron ore port stocks + daily throughput puller (CAT-52).
**Functions:** `IronOrePortSnapshot`, `compute_wow_delta`, `IronOrePortsPuller`, `run_iron_ore_ports_puller`
**Reads:** `__future__`, `akshare`, `bs4`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/jodi_oil.py` — 818 LOC
**Docstring:** GRID JODI (Joint Organisations Data Initiative) Oil World Database puller.
**Functions:** `jodi_csv_urls`, `JODIObservation`, `JODIOilPuller`, `run_jodi_oil_puller`
**Reads:** `__future__`, `csv`, `dataclasses`, `datetime`, `ingestion`, `io`, `loguru`, `requests`, `sqlalchemy`, `tempfile`, `typing`, `zipfile`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/kalshi.py` — 555 LOC
**Docstring:** GRID Kalshi Prediction Markets ingestion module.
**Functions:** `KalshiPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `loguru`, `math`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/kalshi_markets.py` — 299 LOC
**Docstring:** Kalshi prediction market puller — public API, no auth needed.
**Functions:** `KalshiMarketsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/legislation.py` — 1095 LOC
**Docstring:** GRID legislative tracker — bills, hearings, and votes from Congress.gov.
**Functions:** `CongressEndpointMissingError`, `LegislationPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `utils`
**Imported by:** `api/routers/intelligence_actors.py`, `ingestion/scheduler.py`, `intelligence/legislative_intel.py`

#### `ingestion/altdata/littlesis_puller.py` — 147 LOC
**Docstring:** LittleSis power-mapping puller -- board seats, donations, lobbying ties.
**Functions:** `LittleSisPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/lme_warehouse.py` — 729 LOC
**Docstring:** LME warehouse stocks + cancelled warrant ratio puller (CAT-51, P0, Tier A).
**Functions:** `LMEStockSnapshot`, `compute_cancelled_ratio`, `LMEWarehousePuller`, `run_lme_warehouse_puller`
**Reads:** `__future__`, `bs4`, `dataclasses`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/smart_scheduler.py`, `intelligence/scheduler.py`

#### `ingestion/altdata/lobbying.py` — 643 LOC
**Docstring:** GRID lobbying disclosure tracker.
**Functions:** `LobbyingPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/margin_debt.py` — 216 LOC
**Docstring:** GRID — Margin Debt Monthly puller and materializer.
**Functions:** `MarginDebtPuller`, `materialize_margin_debt_from_fred`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`

#### `ingestion/altdata/marketwatch_news.py` — 209 LOC
**Docstring:** GRID MarketWatch RSS news scraper.
**Functions:** `MarketWatchNewsPuller`
**Reads:** `__future__`, `datetime`, `email`, `hashlib`, `httpx`, `ingestion`, `loguru`, `sqlalchemy`, `time`, `typing`, `xml`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/memecoin_classifier.py` — 525 LOC
**Docstring:** GRID Solana Memecoin Message Classifier.
**Functions:** `SignalLabel`, `ClassifiedMessage`, `extract_token_addresses`, `extract_token_symbol`, `message_hash`, `classify_message`, `classify_full_message`, `MentionTracker`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `hashlib`, `re`, `typing`
**Imported by:** `ingestion/altdata/discord_scanner.py`, `ingestion/altdata/telegram_scanner.py`

#### `ingestion/altdata/mmf_composition.py` — 179 LOC
**Docstring:** CAT-30 — Money market fund composition (PUL, Tier A).
**Functions:** `MMFRow`, `MMFCompositionPuller`, `run_mmf_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/nasa_firms_puller.py` — 339 LOC
**Docstring:** NASA FIRMS puller — active fire/thermal anomaly detection from satellites.
**Functions:** `FIRMSPermanentError`, `NASAFirmsPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `math`, `os`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/news_scraper.py` — 1142 LOC
**Docstring:** GRID free news scraper — RSS-based financial news ingestion with LLM sentiment.
**Functions:** `FeedFetchError`, `NewsArticle`, `NewsScraperPuller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `ingestion`, `intelligence`, `json`, `llm`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `xml`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/nyfed.py` — 1054 LOC
**Docstring:** GRID NY Fed data ingestion module.
**Functions:** `NYFedPermanentError`, `NYFedPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `math`, `pandas`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/nyfed_gscpi.py` — 151 LOC
**Docstring:** NY Fed Global Supply Chain Pressure Index (GSCPI) puller.
**Functions:** `NYFedGSCPIPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/obsidian_sync.py` — 415 LOC
**Docstring:** Obsidian vault <-> Postgres bidirectional sync engine.
**Functions:** `domain_from_path`, `content_hash`, `parse_frontmatter`, `scan_vault`, `sync_inbound`, `domain_to_folder`, `build_frontmatter`, `build_note_file`, `sync_outbound`, `run_sync`, `generate_dashboard`, `regenerate_dashboard`
**Reads:** `__future__`, `config`, `datetime`, `hashlib`, `json`, `loguru`, `os`, `pathlib`, `sqlalchemy`, `typing`, `yaml`
**Imported by:** `api/routers/vault.py`, `intelligence/obsidian_agent.py`

#### `ingestion/altdata/offshore_leaks.py` — 968 LOC
**Docstring:** GRID ICIJ Offshore Leaks Database ingestion module.
**Functions:** `OffshoreLeaksPuller`, `check_actor_in_offshore_leaks`, `queue_offshore_investigation`
**Reads:** `__future__`, `csv`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `orchestration`, `os`, `pathlib`, `re`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`, `intelligence/actor_discovery.py`
**Status:** HELD 2026-10-02 (SmartScheduler hold): partial-substring name matches are under review (`docs/handoffs/2026-10-02/OFFSHORE-LEAKS-MATCH-QUALITY.md`).

#### `ingestion/altdata/onchain_rpc.py` — 198 LOC
**Docstring:** On-chain RPC poller — direct blockchain queries for price and activity.
**Functions:** `OnChainRPCPoller`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/opencorporates.py` — 629 LOC
**Docstring:** GRID OpenCorporates API ingestion module.
**Functions:** `OpenCorporatesPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/opensecrets_puller.py` — 304 LOC
**Docstring:** OpenSecrets puller — political donations, lobbying expenditures, revolving door.
**Functions:** `OpenSecretsPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `intelligence`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Status:** RETIRED 2026-11-04 (owner). It is no longer registered: the API returns an HTML page, and the catalog row is `active=FALSE`.

#### `ingestion/altdata/opportunity.py` — 270 LOC
**Docstring:** GRID Opportunity Insights Economic Tracker ingestion module.
**Functions:** `OppInsightsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/pboc_omo.py` — 442 LOC
**Docstring:** PBoC Open Market Operations + MLF renewals puller (CAT-3).
**Functions:** `PBOCOmoSnapshot`, `MLFRenewal`, `PBOCOmoPuller`, `run_pboc_omo_puller`
**Reads:** `__future__`, `akshare`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `pandas`, `sqlalchemy`, `typing`

#### `ingestion/altdata/pboc_omo_official.py` — 495 LOC
**Docstring:** PBoC open-market-operation announcements, read from pbc.gov.cn (official).
**Functions:** `IndexItem`, `ReverseRepoOp`, `Announcement`, `parse_index`, `parse_announcement`, `series_ids_for`, `PBOCOmoAnnouncementsPuller`, `run_pboc_omo_announcements_puller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `html`, `ingestion`, `loguru`, `math`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `urllib`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/pmxt_archive.py` — 188 LOC
**Docstring:** pmxt Archive puller — free hourly Parquet snapshots of prediction market data.
**Functions:** `PmxtArchivePuller`
**Reads:** `__future__`, `datetime`, `hashlib`, `ingestion`, `loguru`, `pandas`, `pathlib`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/polygon_puller.py` — 360 LOC
**Docstring:** Polygon.io puller — stocks, options with Greeks, crypto, forex, dividends.
**Functions:** `PolygonNotEntitled`, `PolygonPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/polymarket.py` — 199 LOC
**Docstring:** Polymarket prediction market puller — no auth, real-time odds.
**Functions:** `PolymarketPuller`
**Reads:** `__future__`, `datetime`, `hashlib`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/prediction_market_history.py` — 717 LOC
**Docstring:** GRID Prediction Market Historical Data Sync.
**Functions:** `PredictionMarketHistoryPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `math`, `os`, `pandas`, `pathlib`, `sqlalchemy`, `typing`

#### `ingestion/altdata/prediction_odds.py` — 644 LOC
**Docstring:** GRID Prediction Market Rapid-Change Detector.
**Functions:** `PredictionOddsPuller`
**Reads:** `__future__`, `datetime`, `db`, `hashlib`, `ingestion`, `json`, `loguru`, `math`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/prediction_pmxt.py` — 423 LOC
**Docstring:** GRID Prediction Market Multi-Platform Puller via pmxt SDK.
**Functions:** `PmxtPredictionPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `hashlib`, `ingestion`, `loguru`, `math`, `pmxt`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/pushshift_reddit.py` — 512 LOC
**Docstring:** Pushshift Reddit historical backfill ingestor.
**Functions:** `PushshiftRedditPuller`
**Reads:** `__future__`, `collections`, `datetime`, `db`, `ingestion`, `loguru`, `orjson`, `pathlib`, `re`, `sqlalchemy`, `sys`, `typing`, `zstandard`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/quiverquant.py` — 569 LOC
**Docstring:** GRID — QuiverQuant Expanded Puller.
**Functions:** `QuiverStoreAborted`, `QuiverQuantPuller`, `unchanged_snapshot_since`, `pull_endpoint`, `pull_all`
**Reads:** `__future__`, `collections`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `os`, `requests`, `sqlalchemy`, `time`, `typing`
**Notes:** Unchanged-snapshot skip (2026-11-07). Undated endpoints (`wsb`, `political_beta`) are stamped with the pull date. For those endpoints, `unchanged_snapshot_since` compares the pull's (ticker, `signal_value`) multiset with the rows of the previous signal date. If they are identical, nothing is stored and the endpoint result carries `unchanged_since`, so an old snapshot is never re-dated as today's. For example, `/live/wallstreetbets` returned one snapshot on all 133 pulls from 2026-04-03 to 2026-11-07. A failed comparison never blocks a pull: the data is stored as before. Placeholder tickers are skipped at write (`is_placeholder_ticker`). Insider, house, senate and lobbying rows are keyed per act (`quiverquant_identity.source_id_for`).
**DB writes:** `signal_sources` (`source_type='quiverquant:{endpoint}'`)

#### `ingestion/altdata/quiverquant_identity.py` — 412 LOC
**Docstring:** GRID - QuiverQuant act identity and period helpers (pure, no I/O).
**Functions:** `normalize_text`, `normalize_code`, `normalize_number`, `normalize_range`, `identity_parts`, `act_identity`, `has_bioguide`, `is_partial_identity`, `member_loose_key`, `source_id_for`, `legacy_source_id`, `feed_source_id`, `feed_source_id_sql`, `parse_payload`, `parse_year_qtr`, `fiscal_quarter_end`, `calendar_quarter_end`, `transition_marker_path`, `transition_marker_exists`, `transition_guard_blocks`
**Reads:** `__future__`, `datetime`, `decimal`, `hashlib`, `json`, `os`, `pathlib`, `re`, `typing`, `unicodedata`
**Imported by:** `ingestion/altdata/quiverquant.py`, `intelligence/adapters/trust_scorer_adapter.py`, `intelligence/global_levers.py`, `intelligence/lever_pullers.py`, `intelligence/trust_scorer.py`

#### `ingestion/altdata/quiverquant_transactions.py` — 156 LOC
**Docstring:** Bounded QuiverQuant writes; an uncertain COMMIT is never replayed.
**Functions:** `CommitUncertain`, `CommitAcknowledgedCleanupError`, `is_rolled_back_write_error`, `validate_batch_size`, `is_connection_error`, `write_transaction`
**Reads:** `__future__`, `contextlib`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/reddit_oauth.py` — 158 LOC
**Docstring:** Reddit Data API access via application-only OAuth (free tier).
**Functions:** `RedditNotConfigured`, `RedditAPIError`, `reddit_credentials_configured`, `RedditAppClient`
**Reads:** `__future__`, `config`, `loguru`, `os`, `requests`, `time`, `typing`

#### `ingestion/altdata/reddit_options_pulse.py` — 737 LOC
**Docstring:** Reddit daily-discussion pulse — retail options positioning signal.
**Functions:** `RedditOptionsPulse`, `extract_tickers`, `count_sentiment_tokens`, `compute_bull_bear_ratio`, `RedditOptionsPulsePuller`, `run_reddit_options_pulse_puller`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/reddit_rss.py` — 252 LOC
**Docstring:** Unauthenticated Reddit reads through RSS/Atom feeds, shared by every puller.
**Functions:** `RedditRateLimited`, `RssEntry`, `body_text`, `parse_feed`, `fetch_feed`
**Reads:** `__future__`, `collections`, `contextlib`, `dataclasses`, `datetime`, `fcntl`, `html`, `loguru`, `os`, `re`, `requests`, `stat`, `tempfile`, `time`, `typing`, `xml`

#### `ingestion/altdata/redfin_puller.py` — 547 LOC
**Docstring:** GRID Redfin Housing Data ingestion module.
**Functions:** `RedfinPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `intelligence`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/refinery_cracks.py` — 355 LOC
**Docstring:** CAT-54 — Refinery utilization + 3-2-1 crack spread puller.
**Functions:** `RefineryRow`, `Crack321`, `RefineryCracksPuller`, `run_refinery_cracks_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/regulatory_events.py` — 811 LOC
**Docstring:** GRID regulatory enforcement events puller.
**Functions:** `RegulatoryEvent`, `PullStats`, `RegulatoryEventsPuller`, `run_weekly`
**Reads:** `__future__`, `dataclasses`, `datetime`, `db`, `feedparser`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/repo_market.py` — 452 LOC
**Docstring:** GRID repo and money market stress indicator ingestion module.
**Functions:** `RepoMarketPuller`
**Reads:** `__future__`, `datetime`, `fedfred`, `ingestion`, `loguru`, `math`, `pandas`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/rocaf_pla_activity.py` — 378 LOC
**Docstring:** Daily PLA activity around Taiwan, from the ROC Air Force HQ (MND) site.
**Functions:** `ListItem`, `PLAActivityReport`, `parse_list`, `parse_report`, `ROCAFPLAActivityPuller`, `run_rocaf_pla_activity_puller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `html`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `urllib`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/sec_13f_live.py` — 1012 LOC
**Docstring:** Live SEC 13F-HR ingestor for the ``institutional_holdings`` table.
**Functions:** `CusipTickerMap`, `parse_infotable_xml`, `LatestFiling`, `list_recent_13f_filings`, `find_latest_13f`, `fetch_infotable`, `FilerResult`, `SEC13FLiveIngestor`, `run`
**Reads:** `__future__`, `csv`, `dataclasses`, `datetime`, `db`, `edgar`, `glob`, `ingestion`, `loguru`, `os`, `requests`, `sqlalchemy`, `threading`, `time`, `typing`, `xml`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/sec_edgar_company.py` — 282 LOC
**Docstring:** GRID SEC EDGAR company fundamentals scraper.
**Functions:** `SECEdgarCompanyPuller`
**Reads:** `__future__`, `datetime`, `httpx`, `ingestion`, `json`, `loguru`, `math`, `re`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/sec_ftd.py` — 703 LOC
**Docstring:** GRID SEC Fails-to-Deliver (FTD) puller.
**Functions:** `parse_ftd_file`, `extract_ftd_text_from_zip`, `SECFTDPuller`
**Reads:** `__future__`, `calendar`, `config`, `datetime`, `ingestion`, `io`, `loguru`, `requests`, `sqlalchemy`, `typing`, `zipfile`
**Imported by:** `ingestion/smart_scheduler.py`

#### `ingestion/altdata/sec_item_1c_cyber.py` — 687 LOC
**Docstring:** GRID SEC Item 1C Cybersecurity Puller.
**Functions:** `Item1CEdge`, `Item1CStats`, `SECItem1CCyberPuller`, `run_weekly`
**Reads:** `__future__`, `analysis`, `bs4`, `dataclasses`, `datetime`, `db`, `json`, `loguru`, `pathlib`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/sec_xbrl_financials.py` — 1140 LOC
**Docstring:** SEC XBRL Company Facts ingestor for normalized capital_flows.
**Functions:** `SECXBRLFinancialsPuller`
**Reads:** `__future__`, `analysis`, `datetime`, `db`, `json`, `loguru`, `os`, `pathlib`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/altdata/sec_xbrl_shares.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/sec_xbrl_shares.py` — 745 LOC
**Docstring:** SEC XBRL shares-outstanding ingestor for daily market_cap computation.
**Functions:** `ifrs_shares_tag_map`, `SECXBRLSharesPuller`
**Reads:** `__future__`, `analysis`, `datetime`, `db`, `ingestion`, `loguru`, `math`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/semi_book_to_bill.py` — 605 LOC
**Docstring:** GRID SEMI North American Semiconductor Equipment Book-to-Bill puller (CAT-89).
**Functions:** `SemiBookToBill`, `SemiBookToBillPuller`, `run_semi_book_to_bill_puller`
**Reads:** `__future__`, `bs4`, `dataclasses`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/sge_premium.py` — 659 LOC
**Docstring:** GRID Shanghai Gold Exchange (SGE) premium puller.
**Functions:** `GoldSpotSnapshot`, `cny_per_gram_to_usd_per_oz`, `classify_premium`, `SGEPremiumPuller`, `run_sge_premium_puller`
**Reads:** `__future__`, `akshare`, `dataclasses`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/small_cap_enrichment.py` — 929 LOC
**Docstring:** Small-cap fundamentals enrichment for ``company_profiles`` (extends
**Functions:** `compute_runway_months`, `derive_market_cap`, `sector_from_sic`, `parse_sec_companyfacts`, `merge_sources`, `shape_enrichment_row`, `SmallCapEnrichmentPuller`, `pull_all`
**Reads:** `__future__`, `config`, `datetime`, `grid`, `ingestion`, `intelligence`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/smart_money.py` — 799 LOC
**Docstring:** GRID Social Smart Money Tracker.
**Functions:** `SmartMoneyPuller`
**Reads:** `__future__`, `datetime`, `db`, `hashlib`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/social_attention.py` — 966 LOC
**Docstring:** GRID social attention data ingestion.
**Functions:** `WikipediaAttentionPuller`, `EdgarViewsPuller`, `GoogleTrendsPuller`, `main`
**Reads:** `__future__`, `argparse`, `datetime`, `db`, `ingestion`, `loguru`, `pytrends`, `requests`, `sqlalchemy`, `time`, `typing`, `urllib`

#### `ingestion/altdata/social_port_activity.py` — 879 LOC
**Docstring:** Ground-truth social-feed observation layer for port activity.
**Functions:** `SocialPortSpec`, `SocialActivitySnapshot`, `compute_composite_velocity`, `SocialPortActivityPuller`, `run_social_port_activity_puller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `os`, `random`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/stocktwits.py` — 187 LOC
**Docstring:** StockTwits social sentiment puller — no auth, real-time, built-in labels.
**Functions:** `StockTwitsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/supply_chain.py` — 883 LOC
**Docstring:** GRID Supply Chain Leading Indicators ingestion module.
**Functions:** `SupplyChainPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `fedfred`, `ingestion`, `json`, `loguru`, `pandas`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/supply_chain_parser.py` — 1166 LOC
**Docstring:** GRID 10-K Supply Chain Parser.
**Functions:** `DerivedEdge`, `DerivedNode`, `ParserStats`, `SupplyChain10KParser`, `run_weekly`
**Reads:** `__future__`, `analysis`, `bs4`, `dataclasses`, `datetime`, `db`, `json`, `loguru`, `pathlib`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/pct_cogs_enrichment.py`

#### `ingestion/altdata/taiwan_exports.py` — 616 LOC
**Docstring:** Taiwan export orders + semiconductor foundry utilization puller (CAT-9, Tier A).
**Functions:** `TaiwanExportSnapshot`, `FoundryUtilization`, `compute_yoy`, `TaiwanExportsPuller`, `run_taiwan_exports_puller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/taiwan_strait_osint.py` — 584 LOC
**Docstring:** GRID Taiwan Strait OSINT — CAT-91 (P0, Tier A).
**Functions:** `TaiwanStraitSnapshot`, `is_exercise_active`, `TaiwanStraitPuller`, `run_taiwan_strait_puller`
**Reads:** `__future__`, `bs4`, `dataclasses`, `datetime`, `db`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/telegram_scanner.py` — 676 LOC
**Docstring:** GRID Solana Telegram Scanner.
**Functions:** `TelegramUser`, `TelegramScanner`
**Reads:** `__future__`, `asyncio`, `concurrent`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `os`, `pathlib`, `sqlalchemy`, `telethon`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/tiingo_news.py` — 296 LOC
**Docstring:** Tiingo News puller — bulk financial news with tickers, tags, and sources.
**Functions:** `TiingoNewsPuller`
**Reads:** `__future__`, `config`, `datetime`, `hashlib`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/treasury_auction.py` — 266 LOC
**Docstring:** CAT-25 — Treasury auction tail + bid-to-cover puller.
**Functions:** `AuctionRow`, `TreasuryAuctionPuller`, `run_treasury_auction_puller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/trending_news.py` — 848 LOC
**Docstring:** GRID trending news ingestion via last30days-skill.
**Functions:** `TrendingItem`, `TrendingNewsPuller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `ingestion`, `json`, `lib`, `loguru`, `pathlib`, `sqlalchemy`, `sys`, `time`, `typing`
**Status:** RETIRED 2026-11-07 (owner). It is no longer registered in `ingestion/scheduler.py`: the last30days skill was never installed on grid-svr and would read browser cookie stores in-process.

#### `ingestion/altdata/uk_companies_house.py` — 652 LOC
**Docstring:** GRID UK Companies House ingestion module.
**Functions:** `UKCompaniesHousePuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/unusual_whales.py` — 785 LOC
**Docstring:** GRID Unusual Options Flow (Whale Tracking) ingestion module.
**Functions:** `WhaleStoreAborted`, `UnusualWhalesPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `math`, `sqlalchemy`, `time`, `typing`, `yfinance`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/usaspending_puller.py` — 89 LOC
**Docstring:** GRID USASpending.gov federal spending ingestion module.
**Functions:** `USASpendingPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/uspto_puller.py` — 104 LOC
**Docstring:** GRID USPTO patent application search ingestion module.
**Functions:** `USPTOPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/verified_13f_filers.py` — 249 LOC
**Docstring:** The single verified 13F filer-CIK map (GD0 §1.3 / §6 item 4 remediation).
**Functions:** `Filer`, `filer_by_key`, `filer_by_cik`
**Reads:** `__future__`, `dataclasses`
**Imported by:** `ingestion/altdata/institutional_flows.py`, `ingestion/altdata/sec_13f_live.py`, `ingestion/edgar.py`

#### `ingestion/altdata/wage_tracker.py` — 176 LOC
**Docstring:** CAT-49 — Real-time wage tracker puller.
**Functions:** `WageRow`, `WageTrackerPuller`, `run_wage_tracker_puller`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/scheduler.py`

#### `ingestion/altdata/warn_layoffs.py` — 283 LOC
**Docstring:** CAT-71 — WARN Act mass layoff filings puller.
**Functions:** `WARNFiling`, `WARNLayoffsPuller`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `ingestion/altdata/whale_alert.py` — 134 LOC
**Docstring:** Whale Alert puller — on-chain large transaction tracking.
**Functions:** `WhaleAlertPuller`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `os`, `requests`, `sqlalchemy`, `typing`

#### `ingestion/altdata/wikidata_entity.py` — 136 LOC
**Docstring:** Wikidata SPARQL entity relationship puller.
**Functions:** `WikidataPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/wikidata_persons.py` — 1067 LOC
**Docstring:** Wikidata SPARQL person-connection ingestion module for GRID.
**Functions:** `WikidataRateLimited`, `WikidataPersonPuller`
**Reads:** `__future__`, `datetime`, `email`, `ingestion`, `intelligence`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/altdata/wikipedia_pageviews_puller.py` — 133 LOC
**Docstring:** Wikipedia Pageviews puller -- daily pageview counts for financial topics.
**Functions:** `WikipediaPageviewsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/wikipedia_text.py` — 232 LOC
**Docstring:** Wikipedia Pageviews puller — attention anomaly detection.
**Functions:** `WikipediaPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `numpy`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/altdata/google_trends.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/world_news.py` — 595 LOC
**Docstring:** GRID WorldNewsAPI ingestion module.
**Functions:** `WorldNewsPaymentRequired`, `WorldNewsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `os`, `requests`, `sqlalchemy`, `store`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/yield_curve_full.py` — 522 LOC
**Docstring:** GRID full US Treasury yield curve ingestion module.
**Functions:** `FullYieldCurvePuller`
**Reads:** `__future__`, `datetime`, `fedfred`, `ingestion`, `loguru`, `math`, `pandas`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/base.py` — 810 LOC
**Docstring:** Base puller class for GRID data ingestion.
**Functions:** `log_pull_failure`, `log_fred_series_failure`, `retry_on_failure`, `BasePuller`
**Reads:** `__future__`, `datetime`, `functools`, `hashlib`, `ingestion`, `json`, `loguru`, `math`, `random`, `sqlalchemy`, `time`, `typing`, `utils`
**Imported by:** `ingestion/altdata/_deprecated/noaa_ais.py`, `ingestion/altdata/aaii_sentiment.py`, `ingestion/altdata/actor_news_puller.py`, `ingestion/altdata/ads_index.py`, `ingestion/altdata/ag_commodity_futures.py`, `ingestion/altdata/ais_ground_truth.py`, `ingestion/altdata/alphavantage_sentiment.py`, `ingestion/altdata/analyst_ratings.py`, … (+157)

#### `ingestion/bls.py` — 361 LOC
**Docstring:** GRID BLS data ingestion module.
**Functions:** `BLSPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/celestial/__init__.py` — 1 LOC
**Docstring:** Celestial and esoteric data sources for correlation analysis.

#### `ingestion/celestial/chinese.py` — 271 LOC
**Docstring:** GRID -- Chinese calendar and Feng Shui data ingestion.
**Functions:** `ChineseCalendarPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/celestial/lunar.py` — 192 LOC
**Docstring:** GRID — Lunar cycle data ingestion.
**Functions:** `LunarCyclePuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `analysis/astro_correlations.py`, `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/celestial/planetary.py` — 273 LOC
**Docstring:** GRID — Planetary aspect data ingestion.
**Functions:** `PlanetaryAspectPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `analysis/astro_correlations.py`, `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/celestial/solar.py` — 394 LOC
**Docstring:** GRID -- Solar activity data ingestion.
**Functions:** `SolarActivityPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/celestial/vedic.py` — 237 LOC
**Docstring:** GRID -- Vedic (Jyotish) astrological data ingestion.
**Functions:** `VedicAstroPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/coingecko.py` — 198 LOC
**Docstring:** CoinGecko crypto spot-price puller — free tier, no API key required.
**Functions:** `spot_series_id`, `CoinGeckoPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `os`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`, `intelligence/post_query_scanner.py`, `intelligence/scheduler.py`

#### `ingestion/crucix_bridge.py` — 640 LOC
**Docstring:** GRID — Crucix bridge puller.
**Functions:** `CrucixBridgePuller`
**Reads:** `__future__`, `datetime`, `db`, `email`, `hashlib`, `ingestion`, `json`, `loguru`, `pathlib`, `sqlalchemy`, `typing`, `urllib`
**Imported by:** `ingestion/scheduler.py`, `intelligence/scheduler.py`

#### `ingestion/crypto_signals.py` — 394 LOC
**Docstring:** Crypto Signal Bridge — transforms existing crypto raw data into signal_sources.
**Functions:** `CryptoSignalBridge`
**Reads:** `__future__`, `binance_close_contract`, `datetime`, `ingestion`, `json`, `loguru`, `numpy`, `sqlalchemy`, `typing`

#### `ingestion/dexscreener.py` — 260 LOC
**Docstring:** GRID DexScreener data ingestion module.
**Functions:** `DexScreenerPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`

#### `ingestion/earnings_events_puller.py` — 860 LOC
**Docstring:** SEC EDGAR + AlphaVantage earnings events puller (Task #151).
**Functions:** `EarningsEventsPuller`, `main`
**Reads:** `__future__`, `argparse`, `datetime`, `db`, `ingestion`, `json`, `loguru`, `os`, `re`, `requests`, `sqlalchemy`, `sys`, `time`, `typing`

#### `ingestion/edgar.py` — 638 LOC
**Docstring:** GRID SEC EDGAR data ingestion module.
**Functions:** `resolve_sec_identity`, `EDGARPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `edgar`, `ingestion`, `json`, `loguru`, `pandas`, `re`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/sec_velocity.py`

#### `ingestion/flow_materializer.py` — 963 LOC
**Docstring:** GRID Flow Materializer — transforms signal_sources and raw_series into
**Functions:** `sync_insider_trades`, `build_congressional_rows`, `sync_congressional_trades`, `sync_dark_pool_weekly`, `sync_etf_flows`, `sync_junction_points`, `sync_all`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/fred.py` — 1199 LOC
**Docstring:** GRID FRED data ingestion module.
**Functions:** `FREDPuller`
**Reads:** `__future__`, `config`, `datetime`, `db`, `fedfred`, `httpx`, `ingestion`, `json`, `loguru`, `pandas`, `re`, `sqlalchemy`, `tenacity`, `threading`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`, `intelligence/post_query_scanner.py`

#### `ingestion/international/__init__.py` — 6 LOC
**Docstring:** GRID international central bank and statistical agency ingestion modules.
**Imported by:** `ingestion/international/bis.py`, `ingestion/international/imf.py`, `ingestion/international/mas.py`, `ingestion/international/rbi.py`

#### `ingestion/international/abs_au.py` — 235 LOC
**Docstring:** GRID Australian Bureau of Statistics (ABS) ingestion module.
**Functions:** `abs_series_url`, `parse_abs_csv`, `ABSPuller`
**Reads:** `__future__`, `csv`, `datetime`, `ingestion`, `io`, `loguru`, `math`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/akshare_macro.py` — 420 LOC
**Docstring:** GRID AKShare China macro ingestion module.
**Functions:** `parse_akshare_period`, `AKShareMacroPuller`
**Reads:** `__future__`, `akshare`, `datetime`, `ingestion`, `loguru`, `pandas`, `re`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/bcb.py` — 261 LOC
**Docstring:** GRID Banco Central do Brasil (BCB) ingestion module.
**Functions:** `BCBPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/bis.py` — 158 LOC
**Docstring:** GRID BIS Statistics API ingestion module.
**Functions:** `BISPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/ecb.py` — 439 LOC
**Docstring:** GRID ECB Statistical Data Warehouse ingestion module.
**Functions:** `ECBPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/edinet.py` — 109 LOC
**Docstring:** GRID Japan FSA EDINET filings ingestion module.
**Functions:** `EDINETPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/eurostat.py` — 183 LOC
**Docstring:** GRID Eurostat bulk download ingestion module.
**Functions:** `EurostatPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/imf.py` — 302 LOC
**Docstring:** GRID IMF IFS and WEO ingestion module.
**Functions:** `is_upstream_outage`, `weo_series_key`, `weo_request_key`, `fetch_weo`, `IMFPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/jquants.py` — 223 LOC
**Docstring:** GRID Japan Exchange Group J-Quants API ingestion module (J-Quants API V2).
**Functions:** `JQuantsNotConfigured`, `JQuantsHTTPError`, `JQuantsPuller`
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `re`, `requests`, `server_log`, `sqlalchemy`, `tenacity`, `time`, `typing`, `urllib`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/kosis.py` — 191 LOC
**Docstring:** GRID Korea Statistical Information Service (KOSIS) ingestion module.
**Functions:** `KOSISPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/mas.py` — 146 LOC
**Docstring:** GRID Monetary Authority of Singapore (MAS) ingestion module.
**Functions:** `MASPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/oecd.py` — 440 LOC
**Docstring:** GRID OECD SDMX API ingestion module.
**Functions:** `OECDRow`, `parse_oecd_csv`, `cli_url`, `kei_url`, `OECDPuller`
**Reads:** `__future__`, `csv`, `dataclasses`, `datetime`, `ingestion`, `io`, `loguru`, `math`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/rbi.py` — 161 LOC
**Docstring:** GRID Reserve Bank of India (RBI) ingestion module.
**Functions:** `RBIPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/sdmx_json.py` — 251 LOC
**Docstring:** SDMX-JSON data-message helpers shared by the BIS and IMF pullers.
**Functions:** `parse_period`, `lookback_start_year`, `parse_sdmx_json`, `fetch_sdmx_json`, `bis_series_url`, `imf_series_url`, `imf_period_bound`, `fetch_imf_multi`, `fetch_bis_series`, `fetch_imf_series`
**Reads:** `__future__`, `datetime`, `math`, `requests`, `typing`
**Imported by:** `ingestion/international/abs_au.py`

#### `ingestion/international/world_bank_puller.py` — 493 LOC
**Docstring:** GRID World Bank Open Data ingestion module.
**Functions:** `WorldBankPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/market_calendar.py` — 150 LOC
**Docstring:** US equity market calendar — holidays, half-days, and trading day checks.
**Functions:** `market_holidays`, `is_weekend`, `is_market_holiday`, `is_market_open`, `last_trading_day`, `next_trading_day`, `trading_days_between`
**Reads:** `__future__`, `datetime`, `functools`
**Imported by:** `ingestion/options.py`, `ingestion/scheduler.py`, `ingestion/tiingo_pull.py`, `intelligence/dollar_flows.py`, `intelligence/people_events_materializer.py`, `intelligence/people_events_pipeline/rules.py`, `ollama/dealer_flow_briefing.py`, `physics/dealer_gamma.py`

#### `ingestion/ml/__init__.py` — 1 LOC
**Docstring:** ML-based ingestion enrichment pipelines (FinBERT, etc.).

#### `ingestion/ml/finbert_scorer.py` — 527 LOC
**Docstring:** FinBERT sentiment scoring pipeline for GRID.
**Functions:** `aggregate_daily_scores`, `FinBERTScorer`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `os`, `sqlalchemy`, `time`, `torch`, `transformers`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/nowcast_puller.py` — 154 LOC
**Docstring:** GRID Atlanta Fed GDPNow nowcast ingestion module.
**Functions:** `NowcastPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/openbb_pipeline.py` — 470 LOC
**Docstring:** OpenBB data ingestion pipeline for GRID.
**Functions:** `OpenBBPipeline`
**Reads:** `__future__`, `concurrent`, `datetime`, `db`, `json`, `loguru`, `numpy`, `openbb`, `os`, `pandas`, `sqlalchemy`, `sys`, `time`, `typing`
**Imported by:** `intelligence/post_query_scanner.py`

#### `ingestion/options.py` — 1543 LOC
**Docstring:** GRID — Options chain ingestion (Cboe delayed quotes, Yahoo fallback).
**Functions:** `OptionsPullResults`, `automatic_options_capture_guard`, `catalyst_options_universe`, `YahooOptionsClient`, `cboe_symbol`, `parse_occ_symbol`, `cboe_chain_pages`, `CboeOptionsClient`, `options_primary_provider`, `CboeFirstOptionsClient`, `OptionsPuller`, `compute_max_pain`, `compute_iv_skew`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `loguru`, `math`, `numpy`, `os`, `pandas`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `uuid`, `zoneinfo`
**Imported by:** `ingestion/scheduler.py`, `ingestion/smart_scheduler.py`, `physics/dealer_gamma.py`

#### `ingestion/physical/__init__.py` — 6 LOC
**Docstring:** GRID physical economy and real-world signal ingestion modules.

#### `ingestion/physical/dbnomics.py` — 177 LOC
**Docstring:** GRID DBnomics aggregated central bank data ingestion module.
**Functions:** `DBnomicsPuller`
**Reads:** `__future__`, `datetime`, `dbnomics`, `ingestion`, `loguru`, `pandas`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/euklems.py` — 127 LOC
**Docstring:** GRID EU KLEMS industry productivity ingestion module.
**Functions:** `EUKLEMSPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `os`, `pandas`, `sqlalchemy`, `tenacity`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/nasa_fire.py` — 129 LOC
**Docstring:** GRID NASA FIRMS fire data ingestion module.
**Functions:** `NASAFirePuller`
**Reads:** `__future__`, `collections`, `config`, `datetime`, `ingestion`, `io`, `loguru`, `os`, `pandas`, `requests`, `sqlalchemy`, `typing`, `utils`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/noaa_space_weather.py` — 101 LOC
**Docstring:** GRID NOAA Space Weather ingestion module.
**Functions:** `NOAASpaceWeatherPuller`
**Reads:** `__future__`, `collections`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/ofr.py` — 308 LOC
**Docstring:** GRID OFR Financial Stability Monitor ingestion module.
**Functions:** `OFRPuller`
**Reads:** `__future__`, `ingestion`, `io`, `loguru`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/patents.py` — 248 LOC
**Docstring:** GRID USPTO PatentsView ingestion module.
**Functions:** `PatentsPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/usda_nass.py` — 238 LOC
**Docstring:** GRID USDA NASS QuickStats ingestion module.
**Functions:** `USDAPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/viirs.py` — 206 LOC
**Docstring:** GRID NASA VIIRS Nighttime Lights ingestion module.
**Functions:** `VIIRSPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `os`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/weather_puller.py` — 103 LOC
**Docstring:** GRID Open-Meteo weather data ingestion module.
**Functions:** `WeatherPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/price_fallback.py` — 156 LOC
**Docstring:** Backup price data puller — runs when yfinance is unreliable.
**Functions:** `PriceFallbackRetired`, `PriceFallbackPuller`
**Reads:** `__future__`, `datetime`, `loguru`, `os`, `requests`, `time`, `typing`
**Imported by:** `intelligence/post_query_scanner.py`
**Status:** Auto-fallback RETIRED 2026-10-02 (E1-V7). The module is fetch-only; `save_to_db` refuses because it backdated `resolved_series` vintages.

#### `ingestion/pull_context.py` — 402 LOC
**Docstring:** GRID — Pull Context Manager.
**Functions:** `PullLogPersistenceError`, `PullContext`, `should_run_pull`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `server_log`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/scheduler.py`, `intelligence/scheduler.py`

#### `ingestion/pumpfun.py` — 236 LOC
**Docstring:** GRID Pump.fun data ingestion module.
**Functions:** `PumpFunPuller`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `time`, `typing`
**Status:** Not scheduled (E1-V3): the PumpFun endpoints return 404/403.

#### `ingestion/realtime/__init__.py` — 0 LOC

#### `ingestion/realtime/candle_builder.py` — 103 LOC
**Docstring:** In-memory OHLCV candle aggregator.
**Functions:** `CandleState`, `CandleBuilder`
**Reads:** `__future__`, `dataclasses`, `datetime`
**Imported by:** `ingestion/realtime/feeds/binance.py`, `ingestion/realtime/feeds/dex_scanner.py`, `ingestion/realtime/feeds/yahoo.py`, `ingestion/realtime/flusher.py`, `ingestion/realtime/ws_listener.py`

#### `ingestion/realtime/db_writer.py` — 105 LOC
**Docstring:** Bounds and backgrounds grid-realtime's raw DB writes.
**Functions:** `bounded_write`
**Reads:** `__future__`, `asyncio`, `threading`, `typing`
**Imported by:** `ingestion/realtime/feeds/dex_scanner.py`, `ingestion/realtime/flusher.py`, `ingestion/realtime/ws_listener.py`

#### `ingestion/realtime/feeds/__init__.py` — 0 LOC

#### `ingestion/realtime/feeds/binance.py` — 86 LOC
**Docstring:** Binance combined-stream WebSocket client for real-time crypto trades.
**Functions:** `run_binance_feed`
**Reads:** `__future__`, `asyncio`, `datetime`, `ingestion`, `json`, `loguru`, `websockets`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/realtime/feeds/dex_scanner.py` — 230 LOC
**Docstring:** DEX token scanner — GeckoTerminal + DexScreener liquidity spike detection.
**Functions:** `PoolData`, `detect_spikes`, `run_dex_scanner`
**Reads:** `__future__`, `aiohttp`, `asyncio`, `dataclasses`, `datetime`, `db`, `ingestion`, `json`, `loguru`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/realtime/feeds/yahoo.py` — 126 LOC
**Docstring:** Yahoo Finance HTTP poller for traditional market data.
**Functions:** `run_yahoo_feed`
**Reads:** `__future__`, `asyncio`, `datetime`, `ingestion`, `loguru`, `yfinance`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/realtime/flusher.py` — 117 LOC
**Docstring:** Batch DB writer for realtime candles.
**Functions:** `build_insert_values`, `run_flusher`
**Reads:** `__future__`, `alerts`, `asyncio`, `db`, `ingestion`, `loguru`, `psycopg2`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/realtime/ws_listener.py` — 221 LOC
**Docstring:** GRID Realtime Market Data Listener.
**Functions:** `main`
**Reads:** `__future__`, `asyncio`, `db`, `ingestion`, `loguru`, `psycopg2`, `signal`

#### `ingestion/sanity_ranges.py` — 147 LOC
**Docstring:** GRID — Sanity range definitions for data ingestion validation.
**Functions:** `get_range_for_series`
**Reads:** `__future__`
**Imported by:** `ingestion/base.py`, `oracle/sanity_checker.py`

#### `ingestion/scheduler.py` — 2770 LOC
**Docstring:** GRID unified ingestion scheduler.
**Functions:** `PullerReportedFailure`, `run_tiingo_prices`, `start_tiingo_prices_worker`, `run_flow_materialize_daily`, `run_pull_group`, `run_daily_binance_close`, `backfill_all`, `run_pushshift_backfill`, `run_daily_pulls`, `run_yfinance_morning_slot`, `run_monthly_pulls`, `start_scheduler`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `db`, `discovery`, `hashlib`, `ingestion`, `intelligence`, `json`, `loguru`, `os`, `pathlib`, `price_close_contract`, `schedule`, `scripts`, `socket`, `sqlalchemy`, `sys`, `threading`, `time`, `tqdm`, `typing`
**Imported by:** `ingestion/smart_scheduler.py`

#### `ingestion/schwab_chain_seam.py` — 428 LOC
**Docstring:** Schwab `/chains` body -> GEX `records[]` shape as pure synthetic normalization (the seam from `docs/reference/SCHWAB_GEX_CHAIN_SEAM.md` §3, implemented 2026-10-31). Reads the GEX owner's `contracts.json` universe (validated, never rewritten) and emits `{symbol, field, value, received_at, callback_seen}` records for `BID ASK IMPL_VOL OPEN_INT GAMMA DELTA VOLUME` per contract plus SPY `LAST BID ASK`, under its **own identity** (`schema: "spy-chain-schwab-v1"`, `host: "GRID"`) so `collectors/gamma_watch/broker.py`'s identity gate rejects it until the GEX owner decides otherwise; no GEX file is imported or changed. Identity (r2): the body `symbol`, every universe contract's root and the underlying identity (raw `underlying.symbol`, or an injected `PrivateQuote`'s requested + provider symbols) must equal the requested `symbol` or an explicitly passed alias; missing or mismatched identity raises `InvalidChainError`; underlying records are keyed by the requested symbol. Per contract, OCC symbol converted to the thinkorswim form + `putCall` + strike must all match the universe entry, and a parseable `quoteTimeInLong` is required (exchange time becomes `received_at`); anything else lands in `rejected[]` and is emitted `callback_seen: false`. Duplicate expiry/type/strike entries (r2) make the key ambiguous: every entry is dropped and the contract is emitted unseen with reason `ambiguous duplicate entries`. `volatility` percent -> fraction, sentinels/non-finite absent; sizes through the reader's exact-integer parser; nothing coerced to 0. Structurally invalid bodies raise `InvalidChainError`. No I/O, no clock, no credentials; strict JSON.
**Functions:** `InvalidChainError`, `Contract`, `tos_symbol`, `occ_to_tos`, `parse_contracts`, `schwab_chain_to_rtd_records`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `json`, `math`, `re`, `typing`
**Tests:** `tests/test_schwab_chain_seam.py` (synthetic chain bodies; identity, value and body negative controls; GEX-gate mirror)

#### `ingestion/schwab_market.py` — 852 LOC
**Docstring:** GRID Schwab Trader API market-data client (read-only). GET-only against `https://api.schwabapi.com/marketdata/v1` (quotes, option chains, expiration chain, price history, market hours) for the Schwab app "GRID" (Market Data Production, scope readonly). Auto-refreshes the access token via the OAuth token endpoint and rewrites the 0600 token file atomically; refuses token/credential files that are not mode 0600. Parser boundary (2026-10-30, with the private quote path): booleans and non-finite JSON numbers are never coerced to prices, counts are non-negative integral int64 or `None` (never truncated; integer strings parsed exactly with `int()`, fractional/exponent/overflow strings dropped), string fields are strings or `None`, and one malformed entry is skipped with its exception class logged rather than raised. **Weekly re-login:** Schwab refresh tokens expire after 7 days, so the operator re-runs the interactive login helper (`~/dev/grid-schwab/schwab_auth.py login`, outside the repo) weekly — nothing in GRID logs in. **No redistribution:** Schwab's terms forbid redistributing its data; nothing from this client may reach a public page (e.g. gex.stepdad.finance) without operator sign-off. No account/order endpoints, no DB writes, not registered in `ingestion/scheduler.py` (added 2026-10-29; intended to replace the thinkorswim RTD leg of the GEX feed later).
**Functions:** `SchwabError`, `SchwabConfigError`, `SchwabAuthError`, `SchwabTransientError`, `SchwabQuote`, `load_credentials`, `is_configured`, `SchwabMarketClient`, `parse_quotes`
**Reads:** `__future__`, `collections`, `config`, `dataclasses`, `errno`, `fcntl`, `json`, `math`, `os`, `pathlib`, `re`, `requests`, `stat`, `tempfile`, `time`, `typing`
**Imported by:** `api/routers/schwab_private.py`, `ingestion/schwab_chain_seam.py`, `ingestion/schwab_quote_path.py`
**Tests:** `tests/test_schwab_market.py` (all HTTP faked)

#### `ingestion/schwab_private_store.py` — 366 LOC
**Docstring:** Private in-process materialization of quote-path snapshots (newly wired 2026-10-31). Keeps the latest `MaterializedQuote` per symbol with identity (requested + provider symbol), `as_of` (provider `quoteTime`), `vintage` (GRID receipt time), source and status, and re-evaluates freshness at **read** time: a `fresh` row becomes `stale` (and `live=False`) once its provider time is older than the gate; rows are never promoted. `materialize()` refuses loss of provenance (non-snapshot input, non-private audience, redistribution label, measured row without provider quote time, live-but-not-fresh, naive timestamps, non-integer sizes, non-finite prices) with `ProvenanceError`. Store is bounded, thread-safe and process-local: no DB, file or network — durable persistence stays a separate gate (`persist_allowed=False`, Anik's redistribution sign-off). Integer sizes are carried as exact `int` (precision above 2**53 preserved in JSON) with `*_exact` decimal-string twins for JavaScript consumers (r2). `view()` is a strict-JSON availability-contract payload (`schema: schwab-private-quote-view-v1`).
**Functions:** `ProvenanceError`, `MaterializedQuote`, `materialize`, `PrivateQuoteStore`
**Reads:** `__future__`, `collections`, `config`, `dataclasses`, `datetime`, `ingestion`, `json`, `math`, `store`, `threading`, `typing`
**Imported by:** `api/routers/schwab_private.py`
**Tests:** `tests/test_schwab_private_store.py`

#### `ingestion/schwab_quote_path.py` — 630 LOC
**Docstring:** Private GRID quote path over the reviewed Schwab reader. Turns `SchwabQuote` rows from `ingestion/schwab_market.py` into availability-contract payloads with provenance (`as_of` = provider `quoteTime`, `vintage` = GRID receipt time), freshness (`max_quote_age_s`, default 20 s = gex-watch's RTD callback-age gate) and an explicit status per symbol (`fresh` / `stale` / `delayed` / `missing` / `invalid` / `unavailable`; only `fresh` is `live`). Missing values are `None`, never 0. Identity: the provider's own `symbol` (raw entry) must equal the requested symbol or an explicitly passed alias (missing -> `missing`, mismatch -> `invalid`; no built-in aliases; `provider_symbol` reported). Fail-closed: `live` only when the provider `realtime` flag is exactly `True` (missing -> `missing`, non-boolean -> `invalid`); non-finite/non-numeric prices and malformed or out-of-range timestamps -> `invalid`; malformed optional fields -> `None`; one bad row never aborts the snapshot; strict-JSON payloads. **Declared audience label, not access control:** `audience` is caller-supplied (`read_private_quotes` refuses any value but `"private"` as a declared-intent check); payloads carry `redistribution="prohibited"` and `persist_allowed=False` as labels; a real authorized private endpoint now exists as `api/routers/schwab_private.py` (admin JWT role gate + default-off `SCHWAB_PRIVATE_QUOTES_ENABLED`, wired 2026-10-31); the label itself is still not access control. `status_only()` is the projection intended for non-private consumers (statuses and age buckets, no measured values). Reader is injected (no network, credentials, token file, clock or DB in this module). Not scheduled, not wired into Hermes/GEX; materialized by `ingestion/schwab_private_store.py`; chain seam documented in `docs/reference/SCHWAB_GEX_CHAIN_SEAM.md` (added 2026-10-30) and implemented in `ingestion/schwab_chain_seam.py` (2026-10-31).
**Functions:** `AudienceError`, `QuoteSource`, `PrivateQuote`, `PrivateQuoteSnapshot`, `read_private_quotes`, `classify_quote`
**Reads:** `__future__`, `collections`, `config`, `dataclasses`, `datetime`, `ingestion`, `json`, `math`, `store`, `typing`
**Imported by:** `api/routers/schwab_private.py`, `ingestion/schwab_chain_seam.py`, `ingestion/schwab_private_store.py`
**Tests:** `tests/test_schwab_quote_path.py` (offline, fake reader, injected clock)

#### `ingestion/sec_velocity.py` — 424 LOC
**Docstring:** GRID SEC 8-K filing velocity module.
**Functions:** `SECVelocityPuller`
**Reads:** `__future__`, `datetime`, `db`, `edgar`, `ingestion`, `json`, `loguru`, `normalization`, `pandas`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/seed_v2.py` — 361 LOC
**Docstring:** GRID v2 database seed data.
**Functions:** `run_seed_v2`
**Reads:** `__future__`, `db`, `loguru`, `sqlalchemy`

#### `ingestion/signal_classifier.py` — 370 LOC
**Docstring:** Gemma 270M signal classification for the ingestion pipeline.
**Functions:** `ClassificationResult`, `classify_signal_text`, `classify_recent_signals`, `narrate_anomalies`, `map_signal_knowledge`
**Reads:** `__future__`, `dataclasses`, `gemma`, `loguru`, `sqlalchemy`, `typing`

#### `ingestion/smart_scheduler.py` — 2651 LOC
**Docstring:** GRID Smart Scheduler — runs only due/stale pullers per cycle.
**Functions:** `SecOnlyRefused`, `run_sec_only_job`, `FinraSelectedRefused`, `run_finra_selected_job`, `hold_reason_for`, `catalog_name_for`, `MissingPullerApiKey`, `SmartScheduler`
**Reads:** `__future__`, `analysis`, `calendar`, `config`, `datetime`, `hashlib`, `importlib`, `ingestion`, `inspect`, `json`, `loguru`, `math`, `os`, `re`, `scripts`, `socket`, `sqlalchemy`, `stat`, `sys`, `threading`, `time`, `types`, `typing`, `zoneinfo`
**Imported by:** `ingestion/scheduler.py`, `ingestion/source_freshness.py`

#### `ingestion/social_sentiment.py` — 479 LOC
**Docstring:** Social sentiment ingestor — Reddit, Bluesky, Google Trends.
**Functions:** `SocialSentimentPuller`
**Reads:** `__future__`, `datetime`, `html`, `ingestion`, `json`, `loguru`, `pytrends`, `re`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `intelligence/post_query_scanner.py`, `intelligence/scheduler.py`, `ollama/market_briefing.py`

#### `ingestion/solana/__init__.py` — 1 LOC
**Docstring:** Solana-specific ingestion modules.

#### `ingestion/solana/top_volume.py` — 714 LOC
**Docstring:** Solana top-volume universe snapshotter.
**Functions:** `TokenVolumeSnapshot`, `IngestSummary`, `TopVolumeProvider`, `JupiterDexScreenerProvider`, `TopVolumeIngestor`
**Reads:** `__future__`, `dataclasses`, `datetime`, `httpx`, `loguru`, `sqlalchemy`, `time`, `trading`, `typing`

#### `ingestion/source_freshness.py` — 510 LOC
**Docstring:** Source-level freshness for the SmartScheduler registry, read from pull_log.
**Functions:** `PullRow`, `SourceFreshness`, `stale_after_hours`, `error_class`, `rows_written`, `assess_source`, `assess`, `fetch_pull_history`, `check`, `CatalogRunSummary`, `merge_catalog_runs`, `run_outcome`, `catalog_freshness_hours`, `classify_catalog_source`, `newest_rows_at`, `safe_last_error`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `ingestion`, `re`, `sqlalchemy`, `typing`, `utils`
**Imported by:** `api/routers/system.py`

#### `ingestion/tiingo_fundamentals_pull.py` — 325 LOC
**Docstring:** Tiingo Fundamentals Puller — daily market cap, PE, PB, enterprise value.
**Functions:** `TiingoFundamentalsPuller`
**Reads:** `__future__`, `collections`, `datetime`, `ingestion`, `loguru`, `math`, `os`, `pandas`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`
**Status:** RETIRED 2026-11-08 (owner). `TIINGO_FUNDAMENTALS` is held in SmartScheduler (`hold_reason`) and checked in the legacy `ingestion/scheduler.py`. Its staleness is not alerted (`alerts/cadence.py` `RETIRED_SOURCE_EXCLUSIONS`), and readers no longer serve its last rows as current values. The Tiingo plan covers only the DOW 30 (89 of 138 tickers were refused). Market cap and shares come from `sec_xbrl_shares` (`ticker_metrics_daily`).

#### `ingestion/tiingo_news_pull.py` — 309 LOC
**Docstring:** Tiingo News Puller — per-ticker sentiment from Tiingo Pro.
**Functions:** `TiingoNewsPuller`
**Reads:** `__future__`, `datetime`, `hashlib`, `ingestion`, `loguru`, `os`, `requests`, `sqlalchemy`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/tiingo_pull.py` — 800 LOC
**Docstring:** GRID Tiingo data ingestion module — fallback for yfinance.
**Functions:** `is_tiingo_equity`, `expected_latest_session`, `TiingoPuller`
**Reads:** `__future__`, `analysis`, `concurrent`, `datetime`, `hashlib`, `ingestion`, `intelligence`, `loguru`, `math`, `oracle`, `os`, `pandas`, `requests`, `sqlalchemy`, `store`, `threading`, `time`, `typing`, `zoneinfo`
**Imported by:** `ingestion/scheduler.py`
**Notes:** The routine daily price list is `TiingoPuller.default_tickers(db_engine)`, the union of four sources. (1) **Static lists:** `ingestion.yfinance_pull.YF_TICKER_LIST` plus every `analysis.sector_map.SECTOR_MAP` ETF and actor ticker. (2) **Oracle:** `oracle_tickers` → `oracle.engine.oracle_close_universe` (2026-11-06). (3) **Active hypotheses:** `hypothesis_tickers` → `intelligence.hypothesis_trials.ticker_move_close_universe` (2026-11-07). (4) **2+ insider-buyer tickers:** `insider_buyer_tickers` / `INSIDER_BUYER_TICKERS_SQL` returns tickers with at least `INSIDER_MIN_BUYERS` = 2 distinct open-market buyers in one feed within `INSIDER_BUYER_DAYS` = 90 days. Buyers are SEC `insider` BUY/UNUSUAL_BUY rows or `quiverquant:insider` TransactionCode P rows. The list is capped at `INSIDER_MAX_TICKERS` = 1000 and drops placeholder and untestable tickers (2026-11-07; on 2026-11-08, 287 of the 332 such tickers had no routine close). The three DB-derived universes are equities only (`is_tiingo_equity`), and each degrades to `[]` with a warning when the DB is unreachable. `pull_incremental` is the SmartScheduler `tiingo` entry (every 4 h). It is single-flight (process lock plus advisory lock). Closes are stored under source `TIINGO`, which the completed-close readers pool with yfinance.

#### `ingestion/trade/__init__.py` — 6 LOC
**Docstring:** GRID trade flow and economic complexity ingestion modules.

#### `ingestion/trade/atlas_eci.py` — 228 LOC
**Docstring:** GRID Harvard Atlas Economic Complexity Index (ECI) ingestion module.
**Functions:** `AtlasECIPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `io`, `loguru`, `numpy`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/trade/cepii.py` — 182 LOC
**Docstring:** GRID CEPII BACI trade data ingestion module.
**Functions:** `CEPIIPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `os`, `pandas`, `sqlalchemy`, `tenacity`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/trade/comtrade.py` — 180 LOC
**Docstring:** GRID UN Comtrade v2 bilateral trade flow ingestion module.
**Functions:** `ComtradePuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/trade/wiod.py` — 185 LOC
**Docstring:** GRID World Input-Output Database (WIOD) ingestion module.
**Functions:** `WIODPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `numpy`, `os`, `pandas`, `requests`, `sqlalchemy`, `tenacity`, `time`, `typing`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/watchlist_resolver.py` — 221 LOC
**Docstring:** Shared ticker-universe resolver for ingestion pullers (Tasks #185 + #182).
**Functions:** `resolve_universe`
**Reads:** `__future__`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `ingestion/altdata/social_attention.py`, `ingestion/earnings_events_puller.py`

#### `ingestion/web_scraper.py` — 786 LOC
**Docstring:** GRID Web Scraper — multi-source data collection with cross-verification.
**Functions:** `WebScraperPuller`
**Reads:** `__future__`, `datetime`, `ingestion`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `time`, `typing`, `urllib`

#### `ingestion/wiki_history.py` — 267 LOC
**Docstring:** Wikipedia "This Day in History" and RSS news ingestor.
**Functions:** `WikiHistoryPuller`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `re`, `requests`, `sqlalchemy`, `typing`, `xml`
**Imported by:** `intelligence/scheduler.py`, `ollama/market_briefing.py`

#### `ingestion/yfinance_pull.py` — 1169 LOC
**Docstring:** GRID yfinance data ingestion module.
**Functions:** `YFinancePuller`, `oracle_and_chart_tickers`
**Reads:** `__future__`, `argparse`, `datetime`, `db`, `hashlib`, `ingestion`, `inspect`, `json`, `logging`, `loguru`, `math`, `pandas`, `price_close_contract`, `re`, `sqlalchemy`, `store`, `strategy`, `threading`, `typing`, `yfinance`
**Imported by:** `ingestion/altdata/small_cap_enrichment.py`, `ingestion/scheduler.py`, `ingestion/tiingo_news_pull.py`, `ingestion/tiingo_pull.py`

## `api/`

#### `api/__init__.py` — 1 LOC
**Docstring:** GRID Intelligence API — FastAPI backend.

#### `api/auth.py` — 805 LOC
**Docstring:** GRID JWT authentication with role-based access control.
**Functions:** `hash_password`, `verify_password`, `create_token`, `verify_token`, `decode_token`, `get_token_expiry`, `require_auth`, `require_role`, `normalize_stream_path`, `issue_stream_ticket`, `redeem_stream_ticket`, `require_stream_auth`, `login`, `register`, `logout`, `create_stream_ticket`, `verify`, `create_user`, `list_users`, `delete_user`
**Reads:** `__future__`, `api`, `collections`, `config`, `datetime`, `fastapi`, `hashlib`, `hmac`, `jose`, `loguru`, `os`, `passlib`, `pathlib`, `psycopg2`, `re`, `secrets`, `shelve`, `tempfile`, `threading`, `time`, `typing`, `urllib`
**Imported by:** `api/lf_helpers.py`, `api/main.py`, `api/routers/a2a.py`, `api/routers/actor_detail.py`, `api/routers/actor_news_api.py`, `api/routers/agents.py`, `api/routers/associations.py`, `api/routers/astrogrid_celestial.py`, … (+90)

#### `api/dad_sec_fundamentals.py` — 155 LOC
**Docstring:** Read filed SEC fundamentals for Dad without provider calls or writes.
**Functions:** `read_sec_profile`
**Reads:** `__future__`, `datetime`, `json`, `math`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/dad.py`

#### `api/dependencies.py` — 83 LOC
**Docstring:** Shared FastAPI dependencies.
**Functions:** `get_db_engine`, `get_pit_store`, `get_journal`, `get_model_registry`, `get_astrogrid_store`, `clear_singletons`
**Reads:** `__future__`, `db`, `governance`, `journal`, `sqlalchemy`, `store`
**Imported by:** `api/main.py`, `api/precompute_serving.py`, `api/routers/actor_detail.py`, `api/routers/actor_news_api.py`, `api/routers/associations.py`, `api/routers/astrogrid.py`, `api/routers/astrogrid_celestial.py`, `api/routers/astrogrid_core.py`, … (+84)

#### `api/lf_helpers.py` — 124 LOC
**Docstring:** Best-effort Langfuse helpers shared by API routers.
**Functions:** `set_input`, `set_output`, `propagate_attributes`, `user_id_from_token`
**Reads:** `__future__`, `api`, `contextlib`, `langfuse`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_deepdive.py`, `api/routers/intelligence_thesis.py`

#### `api/loop_watchdog.py` — 126 LOC
**Docstring:** Event-loop lag watchdog for grid-api.
**Functions:** `EventLoopWatchdog`
**Reads:** `__future__`, `asyncio`, `loguru`, `os`, `sys`, `threading`, `time`, `traceback`
**Imported by:** `api/main.py`

#### `api/main.py` — 893 LOC
**Docstring:** GRID Intelligence API — FastAPI application entry point.
**Functions:** `lifespan`, `SecurityHeadersMiddleware`, `PublicSchemaGuardMiddleware`, `RateLimitMiddleware`, `X402PaymentMiddleware`, `broadcast_event`, `recent_realtime_events`, `websocket_endpoint`
**Reads:** `__future__`, `agents`, `alerts`, `api`, `asyncio`, `collections`, `config`, `contextlib`, `contracts`, `datetime`, `db`, `events`, `fastapi`, `importlib`, `json`, `loguru`, `oracle`, `orchestration`, `os`, `pathlib`, `payments`, `starlette`, `subnet`, `sys`, `threading`, `time`, `typing`
**Imported by:** `api/routers/system.py`, `api/routers/watchlist_core.py`, `intelligence/trust_scorer.py`, `trading/options_recommender.py`

#### `api/precompute_serving.py` — 107 LOC
**Docstring:** Serve a precomputed endpoint result (W3-2).
**Functions:** `serve_precomputed`, `read_precomputed`
**Reads:** `__future__`, `api`, `collections`, `fastapi`, `intelligence`, `json`, `loguru`, `store`, `typing`
**Imported by:** `api/routers/associations.py`, `api/routers/discovery.py`, `api/routers/flows.py`, `api/routers/intel_cross_reference.py`, `api/routers/intelligence_deepdive.py`, `api/routers/intelligence_forensics.py`, `api/routers/intelligence_news.py`, `api/routers/intelligence_regime.py`, … (+5)

#### `api/route_warm.py` — 54 LOC
**Docstring:** Build FastAPI's per-route request contexts before the first request.
**Functions:** `warm_route_contexts`
**Reads:** `__future__`, `fastapi`, `loguru`, `typing`
**Imported by:** `api/main.py`

#### `api/routers/__init__.py` — 1 LOC
**Docstring:** GRID API routers.
**Imported by:** `api/routers/actor_detail.py`, `intelligence/api_precompute.py`

#### `api/routers/a2a.py` — 199 LOC
**Docstring:** A2A Protocol endpoints — Agent Card discovery and task management.
**Functions:** `TaskSubmitRequest`, `TaskResponse`, `get_agent_card`, `submit_task`, `get_task`, `cancel_task`, `list_tasks`
**Reads:** `__future__`, `a2a`, `api`, `config`, `fastapi`, `loguru`, `pydantic`, `threading`, `typing`

#### `api/routers/actor_detail.py` — 525 LOC
**Docstring:** Actor detail endpoint for SectorDive profile drawer.
**Functions:** `get_actor_trust_cog_endpoint`, `get_actor_detail_for_drawer`
**Reads:** `__future__`, `analysis`, `api`, `datetime`, `fastapi`, `intelligence`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/explain.py`

#### `api/routers/actor_news_api.py` — 181 LOC
**Docstring:** Actor news endpoint — serves rows from the actor_news table.
**Functions:** `get_actor_news`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `re`, `sqlalchemy`, `typing`

#### `api/routers/agents.py` — 191 LOC
**Docstring:** GRID API — TradingAgents router.
**Functions:** `RunRequest`, `BacktestRequest`, `agent_status`, `trigger_run`, `trigger_run_sync`, `list_runs`, `get_run`, `run_backtest`, `backtest_summary`, `get_schedule`, `start_schedule`, `stop_schedule`
**Reads:** `__future__`, `agents`, `api`, `config`, `datetime`, `db`, `fastapi`, `loguru`, `pydantic`, `tradingagents`, `typing`

#### `api/routers/associations.py` — 801 LOC
**Docstring:** Feature association discovery endpoints.
**Functions:** `load_raw_feature_matrix`, `get_correlation_matrix`, `get_lag_analysis`, `get_clusters`, `get_regime_features`, `get_anomalies`, `extreme_anomalies`, `send_extreme_anomaly_alert`
**Reads:** `__future__`, `alerts`, `api`, `datetime`, `discovery`, `fastapi`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/api_precompute.py`

#### `api/routers/astrogrid.py` — 36 LOC
**Docstring:** AstroGrid API — expanded celestial intelligence endpoints.
**Reads:** `__future__`, `api`, `fastapi`

#### `api/routers/astrogrid_celestial.py` — 686 LOC
**Docstring:** AstroGrid sub-router: ephemeris, correlations, timeline, briefing, compare,
**Functions:** `get_ephemeris`, `get_correlations`, `get_timeline`, `get_briefing`, `compare_dates`, `get_retrogrades`, `get_eclipses`, `get_nakshatra`, `get_lunar_calendar`, `get_solar_activity`
**Reads:** `__future__`, `analysis`, `api`, `calendar`, `datetime`, `fastapi`, `loguru`, `numpy`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid.py`

#### `api/routers/astrogrid_core.py` — 302 LOC
**Docstring:** AstroGrid sub-router: overview, snapshot, scorecard, universe, interpret.
**Functions:** `get_overview`, `get_snapshot`, `get_scorecard`, `get_scoreable_universe`, `interpret_snapshot`
**Reads:** `__future__`, `api`, `collections`, `datetime`, `fastapi`, `loguru`, `typing`
**Imported by:** `api/routers/astrogrid.py`

#### `api/routers/astrogrid_helpers.py` — 2039 LOC
**Docstring:** AstroGrid shared helpers — Pydantic models and computation utilities.
**Functions:** `CompareDatesRequest`, `AstrogridInterpretRequest`, `AstrogridPredictionRequest`, `AstrogridGuruRequest`, `AstrogridScoreRequest`, `AstrogridBacktestRequest`, `AstrogridReviewRequest`, `AstrogridWeightDecisionRequest`, `AstrogridLearningLoopRequest`, `build_snapshot`, `build_interpretation`
**Reads:** `__future__`, `analysis`, `api`, `datetime`, `ingestion`, `json`, `llm`, `loguru`, `math`, `oracle`, `pydantic`, `re`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/routers/astrogrid.py`, `api/routers/astrogrid_celestial.py`, `api/routers/astrogrid_core.py`, `api/routers/astrogrid_predictions.py`, `oracle/astrogrid_cycle.py`

#### `api/routers/astrogrid_predictions.py` — 515 LOC
**Docstring:** AstroGrid sub-router: predictions, backtest, weights, review, learning-loop.
**Functions:** `create_prediction`, `ask_guru`, `get_insights`, `get_latest_predictions`, `get_postmortems`, `score_predictions`, `get_prediction_scoreboard`, `run_backtest`, `get_backtest_summary`, `get_backtest_results`, `get_current_weights`, `generate_review_run`, `run_learning_loop`, `get_latest_review`, `get_weight_proposals`, `approve_weight_proposal`, `reject_weight_proposal`, `get_prediction_detail`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `loguru`, `oracle`, `price_close_contract`, `typing`, `uuid`
**Imported by:** `api/routers/astrogrid.py`

#### `api/routers/attributions.py` — 52 LOC
**Docstring:** Cross-lens supply-shock attribution endpoint.
**Functions:** `get_actor_attributions`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`

#### `api/routers/backtest.py` — 211 LOC
**Docstring:** GRID API — Backtest & paper trade endpoints.
**Functions:** `BacktestRequest`, `run_backtest`, `get_results`, `get_summary`, `generate_charts`, `get_chart`, `create_paper_trade`, `list_paper_trades`, `get_paper_trade`, `score_predictions`
**Reads:** `__future__`, `api`, `backtest`, `datetime`, `fastapi`, `outputs`, `pathlib`, `pydantic`, `re`, `typing`

#### `api/routers/blob.py` — 110 LOC
**Docstring:** Blob store API — upload, download, and manage stored files.
**Functions:** `blob_health`, `upload_blob`, `get_presigned_url`, `list_blobs`, `download_blob`, `delete_blob`
**Reads:** `__future__`, `api`, `fastapi`, `store`

#### `api/routers/briefing.py` — 261 LOC
**Docstring:** GRID API — Market Briefing & Sentiment Endpoints.
**Functions:** `get_current_sentiment`, `get_latest_briefing`, `get_briefing_history`, `get_sentiment_history`, `get_sentiment_accuracy`, `trigger_briefing`
**Reads:** `__future__`, `api`, `datetime`, `db`, `fastapi`, `json`, `loguru`, `ollama`, `sqlalchemy`

#### `api/routers/canvas.py` — 2284 LOC
**Docstring:** GRID Canvas — Unified graph intelligence API.
**Functions:** `BoardCreate`, `BoardUpdate`, `get_canvas_graph`, `get_node_detail`, `expand_node`, `create_board`, `list_boards`, `get_board`, `update_board`, `delete_board`, `fork_board`, `get_dot_connections`
**Reads:** `__future__`, `api`, `collections`, `datetime`, `fastapi`, `json`, `loguru`, `math`, `pydantic`, `sqlalchemy`, `threading`, `typing`, `utils`, `uuid`

#### `api/routers/canvas_board_store.py` — 622 LOC
**Docstring:** Shared helpers for Canvas investigation board graph storage.
**Functions:** `ensure_investigation_boards_table`, `ensure_legacy_canvas_tables`, `parse_json_value`, `normalize_graph_state`, `node_key`, `edge_key`, `edge_source`, `edge_target`, `graph_node_from_payload`, `graph_edge_from_payload`, `node_response`, `edge_response`, `get_board_graph_state`, `save_board_graph_state`, `upsert_graph_node`, `update_graph_node`, `delete_graph_node`, `upsert_graph_edge`, `delete_graph_edge`, `sync_legacy_canvas_from_board`, `sync_board_from_legacy_canvas`, `delete_legacy_canvas_board`, `row_to_dict`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/canvas.py`, `api/routers/canvas_expand.py`, `api/routers/canvas_graph.py`, `api/routers/canvas_investigate.py`, `api/routers/canvas_predict.py`

#### `api/routers/canvas_core.py` — 393 LOC
**Docstring:** Canvas sub-router: board CRUD endpoints.
**Functions:** `BoardCreate`, `BoardUpdate`, `list_boards`, `create_board`, `get_board`, `update_board`, `delete_board`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `json`, `loguru`, `pydantic`, `sqlalchemy`, `typing`, `uuid`

#### `api/routers/canvas_expand.py` — 1176 LOC
**Docstring:** Canvas sub-router: graph expansion — expand network, path finding, suggest connections.
**Functions:** `PathRequest`, `expand_node`, `find_path`, `suggest_connections`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `json`, `loguru`, `math`, `pydantic`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/routers/canvas.py`

#### `api/routers/canvas_graph.py` — 463 LOC
**Docstring:** Canvas sub-router: node and edge CRUD + bulk graph save.
**Functions:** `NodeCreate`, `NodeUpdate`, `EdgeCreate`, `BulkGraphSave`, `EvidenceCreate`, `add_node`, `update_node`, `delete_node`, `add_edge`, `delete_edge`, `bulk_save_graph`, `add_evidence`, `get_node_evidence`, `delete_evidence`
**Reads:** `__future__`, `api`, `fastapi`, `json`, `loguru`, `pydantic`, `sqlalchemy`
**Imported by:** `api/routers/canvas.py`

#### `api/routers/canvas_investigate.py` — 469 LOC
**Docstring:** Canvas sub-router: auto-investigate — build rich investigation boards from a search query.
**Functions:** `InvestigateRequest`, `InvestigateResponse`, `auto_investigate`
**Reads:** `__future__`, `api`, `events`, `fastapi`, `json`, `loguru`, `math`, `pydantic`, `sqlalchemy`, `uuid`
**Imported by:** `api/routers/canvas.py`

#### `api/routers/canvas_llm.py` — 274 LOC
**Docstring:** Canvas sub-router: LLM-powered intelligence features.
**Functions:** `ExplainRequest`, `ExplainResponse`, `explain_connection`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `json`, `llm`, `loguru`, `pydantic`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/canvas.py`

#### `api/routers/canvas_predict.py` — 297 LOC
**Docstring:** Canvas sub-router: convert canvas investigation to scored prediction.
**Functions:** `PredictionRequest`, `PredictionResponse`, `create_prediction`
**Reads:** `__future__`, `api`, `fastapi`, `json`, `loguru`, `pydantic`, `sqlalchemy`, `uuid`
**Imported by:** `api/routers/canvas.py`

#### `api/routers/capital_flow.py` — 1137 LOC
**Docstring:** Capital-flow endpoint for actor profile pages.
**Functions:** `get_capital_flow`
**Reads:** `__future__`, `analysis`, `api`, `datetime`, `fastapi`, `features`, `loguru`, `sqlalchemy`, `typing`, `utils`

#### `api/routers/celestial.py` — 189 LOC
**Docstring:** Celestial signals endpoint.
**Functions:** `get_celestial_signals`, `get_celestial_briefing`, `generate_celestial_briefing`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `loguru`, `ollama`, `sqlalchemy`, `typing`

#### `api/routers/chat.py` — 2835 LOC
**Docstring:** GRID API — Ask GRID conversational chat endpoint.
**Functions:** `ChatMessage`, `ChatAskRequest`, `ChatAskResponse`, `ComposeWidget`, `ComposeAllocationItem`, `ChatComposeRequest`, `ChatComposeResponse`, `ask_grid`, `compose_layout`, `CapabilityPingRequest`, `set_capability_ping`, `mark_capability_ready`, `list_capability_requests`, `capability_ready_for_me`, `ask_grid_stream`
**Reads:** `__future__`, `analysis`, `api`, `concurrent`, `config`, `contextlib`, `datetime`, `db`, `fastapi`, `inspect`, `intelligence`, `json`, `langfuse`, `llm`, `loguru`, `ollama`, `oracle`, `pandas`, `physics`, `pydantic`, `re`, `requests`, `scripts`, `sqlalchemy`, `subprocess`, `threading`, `time`, `timeseries`, `typing`, `uuid`
**Imported by:** `api/routers/price_alerts.py`

#### `api/routers/config.py` — 180 LOC
**Docstring:** System configuration endpoints.
**Functions:** `get_config`, `update_config`, `get_sources`, `update_source`, `get_features`, `update_feature`
**Reads:** `__future__`, `api`, `config`, `fastapi`, `sqlalchemy`, `typing`

#### `api/routers/contagion.py` — 608 LOC
**Docstring:** Chain contagion simulator endpoint.
**Functions:** `simulate`, `backtest`, `get_scenarios`, `get_contagion_matrix`
**Reads:** `__future__`, `analysis`, `api`, `fastapi`, `intelligence`, `json`, `loguru`, `re`, `sqlalchemy`, `typing`, `utils`
**Imported by:** `api/main.py`

#### `api/routers/contracts.py` — 76 LOC
**Docstring:** FastAPI router for contracts infrastructure endpoints.
**Functions:** `contracts_metrics`, `contracts_lineage`, `contracts_dead_letter_replay`
**Reads:** `__future__`, `api`, `contracts`, `fastapi`, `sqlalchemy`, `uuid`

#### `api/routers/conviction.py` — 588 LOC
**Docstring:** Conviction API surface — FastAPI endpoints exposing the full decision stack
**Functions:** `get_latest_sweep`, `list_sweeps`, `get_long_plays`, `refresh_long_plays`, `get_ticker_conviction`, `get_top_conviction`, `get_pair_candidates`, `get_pair_ticket`, `get_signal_health`, `get_ticker_narrative`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `intelligence`, `loguru`, `sqlalchemy`, `typing`

#### `api/routers/dad.py` — 2503 LOC
**Docstring:** Dad-facing ticker lookup endpoints.
**Functions:** `get_dad_ticker_gold`, `get_dad_ticker_evidence`, `get_dad_ticker_chart`, `get_dad_ticker_finviz`, `get_dad_ticker_options`, `stream_dad_ticker_gold`
**Reads:** `__future__`, `api`, `bs4`, `concurrent`, `copy`, `datetime`, `duckdb`, `fastapi`, `json`, `loguru`, `os`, `pathlib`, `re`, `sqlalchemy`, `threading`, `time`, `typing`

#### `api/routers/derivatives.py` — 1202 LOC
**Docstring:** GRID API — Derivatives / Dealer Flow Intelligence endpoints.
**Functions:** `get_overview`, `get_gex`, `get_options_batches`, `get_regime`, `get_walls`, `get_vanna_charm`, `get_vol_surface`, `get_skew`, `get_term_structure`, `get_oi_heatmap`, `get_flow_narrative`, `generate_flow_narrative`, `get_signals`, `get_scan`, `get_flow_timeline`, `get_history`
**Reads:** `__future__`, `api`, `calendar`, `collections`, `datetime`, `discovery`, `fastapi`, `loguru`, `math`, `ollama`, `physics`, `sqlalchemy`, `store`, `typing`, `yfinance`

#### `api/routers/discovery.py` — 895 LOC
**Docstring:** Discovery engine endpoints.
**Functions:** `trigger_orthogonality`, `trigger_clustering`, `get_jobs`, `latest_snapshot_result`, `get_orthogonality_results`, `get_clustering_results`, `list_sector_clustering_results`, `get_hypothesis_results`, `get_hypotheses`, `get_backtest_results`, `run_backtest_scan`, `run_hypothesis_review`, `promote_hypothesis_to_feature`, `resolve_cross_asset_features`, `get_correlation_matrix`, `smart_heatmap`
**Reads:** `__future__`, `analysis`, `api`, `asyncio`, `datetime`, `discovery`, `fastapi`, `governance`, `json`, `loguru`, `numpy`, `pandas`, `sklearn`, `sqlalchemy`, `threading`, `typing`, `uuid`
**Imported by:** `api/routers/associations.py`

#### `api/routers/divergence.py` — 192 LOC
**Docstring:** Fundamental-vs-price divergence endpoints.
**Functions:** `list_divergence`, `get_actor_divergence`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `loguru`, `sqlalchemy`, `typing`, `utils`

#### `api/routers/earnings.py` — 134 LOC
**Docstring:** Earnings calendar & prediction endpoints.
**Functions:** `get_earnings_calendar`, `get_recent_earnings`, `get_earnings_surprise`, `predict_earnings`, `get_earnings_scorecard`, `get_earnings_history`, `run_earnings_cycle`
**Reads:** `__future__`, `api`, `fastapi`, `ingestion`, `intelligence`, `loguru`, `typing`

#### `api/routers/evals_e2.py` — 62 LOC
**Docstring:** Read-only E2 forward-scoreboard endpoints.
**Functions:** `board_dir`, `get_scoreboard`, `get_scoreboard_markdown`
**Reads:** `__future__`, `api`, `evals`, `fastapi`, `os`, `pathlib`

#### `api/routers/explain.py` — 836 LOC
**Docstring:** Hero endpoint: "why did this actor move?" — ranked evidence synthesis.
**Functions:** `get_actor_explain`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `loguru`, `re`, `sqlalchemy`, `typing`, `utils`

#### `api/routers/feed.py` — 331 LOC
**Docstring:** GRID Signal Feed — running list of anomalies, discoveries, and interesting signals.
**Functions:** `get_signal_feed`, `get_latest_signals`, `get_rss_feed`, `get_atom_feed`, `get_live_feed`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `sqlalchemy`, `typing`

#### `api/routers/flows.py` — 2979 LOC
**Docstring:** Sector flow analysis API — serves the sector map with live data.
**Functions:** `start_sector_flow_warm_thread`, `get_sectors`, `get_sector_detail`, `get_sector_dive`, `get_sankey_data`, `get_gaps`, `run_research`, `fill_gaps`, `test_hypotheses`, `get_money_map`, `get_company_drill`, `get_aggregated_flows`, `get_flow_momentum`, `get_flow_map_v2`, `get_junction_points`, `get_flow_layers`, `get_flow_layer_detail`, `get_flow_waterfall`, `get_flow_orthogonality`, `generate_custom_image`, `generate_flow_image`, `get_cds_dashboard`, `get_cds_history`, `get_briefing`, `generate_briefing`, `get_briefing_audio`, `list_briefings`, `get_briefing_audio_by_name`, `get_briefing_detail`
**Reads:** `__future__`, `analysis`, `api`, `asyncio`, `datetime`, `fastapi`, `hashlib`, `inference`, `intelligence`, `json`, `loguru`, `math`, `numpy`, `ollama`, `pathlib`, `physics`, `re`, `sqlalchemy`, `store`, `threading`, `time`, `typing`, `utils`
**Imported by:** `api/main.py`, `api/routers/actor_detail.py`, `api/routers/supply_chain_helpers.py`

#### `api/routers/forecasts.py` — 230 LOC
**Docstring:** TimesFM forecast endpoints — generate and retrieve time-series forecasts.
**Functions:** `ForecastRequest`, `BatchForecastRequest`, `ForecastResponse`, `forecast_health`, `generate_forecast`, `batch_forecast`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `numpy`, `pydantic`, `sqlalchemy`, `timeseries`, `typing`

#### `api/routers/gamma_watch.py` — 147 LOC
**Docstring:** Read-only bridge to the co-located Gamma Watch collector and receipt journal.
**Functions:** `NoRedirect`, `get_state`, `get_journal`, `get_contracts`, `get_archive`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `json`, `math`, `os`, `pathlib`, `sqlite3`, `typing`, `urllib`

#### `api/routers/geo.py` — 257 LOC
**Docstring:** Geo-spatial data endpoints for flow visualization.
**Functions:** `get_geo_flows`, `get_geo_actors`, `get_signal_density`
**Reads:** `__future__`, `api`, `fastapi`, `json`, `sqlalchemy`, `typing`

#### `api/routers/godview.py` — 115 LOC
**Docstring:** God view API (slice G8): read-only, point-in-time pillar summary and history.
**Functions:** `get_godview_latest`, `get_godview_history`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `godview`, `loguru`, `sqlalchemy`, `typing`

#### `api/routers/intel.py` — 2273 LOC
**Docstring:** GRID Intelligence API Product — the core paid API.
**Functions:** `Tier`, `intel_search`, `intel_entity_profile`, `intel_actor_dossier`, `intel_ticker`, `intel_cross_reference`, `intel_deep_dive`, `intel_network`, `intel_market_brief`, `intel_predictions_active`, `intel_predictions_track_record`, `intel_briefing`
**Reads:** `__future__`, `api`, `dataclasses`, `datetime`, `enum`, `fastapi`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `time`, `typing`

#### `api/routers/intel_cross_reference.py` — 274 LOC
**Docstring:** Cross-reference intelligence endpoints — lie detector for government statistics.
**Functions:** `get_cross_reference`, `get_cross_reference_narrative`, `get_cross_reference_by_category`, `get_cross_reference_for_ticker`, `get_cross_reference_history`
**Reads:** `__future__`, `api`, `dataclasses`, `fastapi`, `intelligence`, `loguru`, `threading`, `time`, `typing`

#### `api/routers/intel_source_audit.py` — 120 LOC
**Docstring:** Source audit endpoints — track accuracy, redundancy, and discrepancies across data sources.
**Functions:** `get_source_audit`, `trigger_source_audit`, `get_redundancy_map`, `compare_feature_sources`, `get_discrepancies`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`

#### `api/routers/intelligence.py` — 59 LOC
**Docstring:** Cross-reference intelligence endpoints — lie detector for government statistics.
**Reads:** `__future__`, `fastapi`, `importlib`, `loguru`

#### `api/routers/intelligence_actors.py` — 2216 LOC
**Docstring:** Intelligence sub-router: Actor network, post-mortems, and trend endpoints.
**Functions:** `get_actor_network`, `latest_universe_run`, `get_flow_extras`, `start_actor_graph_rebuild`, `get_full_actor_graph`, `clear_actor_graph`, `warm_actor_graph_async`, `get_actor_detail`, `get_actor_analytics_endpoint`, `get_top_actors_endpoint`, `get_communities_endpoint`, `get_community_members_endpoint`, `get_postmortems`, `trigger_batch_postmortem`, `get_lessons_learned`, `get_milestone_scorecard`, `get_ticker_milestones`, `get_attention_alerts`, `get_actor_network_db`, `get_actor_enriched_profile`, `get_trends`, `get_sector_power_map`, `ego_graph_search`, `get_ego_graph`, `get_actor_universe_status`, `get_actor_universe_overview`, `get_actor_universe_ego`, `search_actor_universe`, `get_actor_universe_communities`, `get_actor_universe_community`, `get_actor_universe_actor`, `get_intel_expand`, `get_grand_power_map`
**Reads:** `__future__`, `analysis`, `api`, `fastapi`, `ingestion`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `store`, `threading`, `time`, `typing`, `utils`
**Imported by:** `api/main.py`

#### `api/routers/intelligence_causation.py` — 46 LOC
**Docstring:** Intelligence sub-router: persisted causal links for the Timeline view.
**Functions:** `get_causal_links`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`

#### `api/routers/intelligence_companies.py` — 429 LOC
**Docstring:** Intelligence sub-router: Company analyzer, deep graph, and institutional map.
**Functions:** `get_all_company_profiles`, `get_cross_company_patterns`, `get_sector_influence_report`, `trigger_company_analysis`, `get_company_profile`, `get_deep_graph`, `get_overlaps`, `get_all_overlaps`, `get_connection_map`, `get_institutional_map`, `trace_pension`, `get_fund_fees`, `get_institutional_conflicts`, `get_hidden_influence`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `time`, `typing`, `utils`

#### `api/routers/intelligence_deepdive.py` — 313 LOC
**Docstring:** Intelligence sub-router: Global lever map, deep-dive, and expectations endpoints.
**Functions:** `get_levers`, `trace_lever_chain_endpoint`, `get_cross_domain_actors_endpoint`, `get_lever_report_endpoint`, `get_lever_domain_endpoint`, `get_deep_dive`, `run_mag7_deep_dives`, `get_expectations`
**Reads:** `__future__`, `api`, `asyncio`, `fastapi`, `intelligence`, `loguru`, `threading`, `typing`, `utils`

#### `api/routers/intelligence_edges.py` — 30 LOC
**Docstring:** Intelligence sub-router: structural market-edge scanner.
**Functions:** `get_market_edges`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`

#### `api/routers/intelligence_forensics.py` — 464 LOC
**Docstring:** Intelligence sub-router: Forensics, causation, influence network, and export controls.
**Functions:** `get_forensic_reports`, `analyze_forensic_move`, `get_causation`, `refresh_causation`, `get_suspicious_trades_endpoint`, `get_causal_narrative_endpoint`, `get_causal_chains`, `refresh_causal_chains`, `get_active_causal_chains`, `get_influence_network`, `get_circular_flows`, `get_vote_trade_hypocrisy`, `get_export_controls`, `get_export_control_impact`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`, `utils`

#### `api/routers/intelligence_govflow.py` — 324 LOC
**Docstring:** Intelligence sub-router: Government contracts, dollar flows, and legislative intelligence.
**Functions:** `get_gov_contracts`, `get_contract_insider_overlaps`, `get_dollar_flows`, `trigger_dollar_flow_normalization`, `get_legislation_overview`, `get_legislation_hearings`, `get_legislation_trading_alerts`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`

#### `api/routers/intelligence_news.py` — 721 LOC
**Docstring:** Intelligence sub-router: News, event sequences, and pattern engine endpoints.
**Functions:** `get_news_feed_endpoint`, `get_news_stats_endpoint`, `get_narrative_shift_endpoint`, `get_news_before_move_endpoint`, `get_news_briefing_endpoint`, `get_event_sequence`, `get_recurring_patterns`, `get_discovered_patterns`, `get_active_patterns`, `get_patterns_for_ticker_endpoint`, `get_news_momentum`, `get_momentum_divergences`, `run_momentum_scan`, `get_active_deals`, `get_deal_pipeline_summary`, `get_deal_history`, `run_deal_scan`, `get_business_events`, `get_business_event_summary`, `run_business_event_scan`, `get_earnings_transcript_analysis`, `get_earnings_tone_shifts`, `run_earnings_transcript_analysis`, `get_sec_material_facts`, `get_high_impact_facts`, `run_sec_fact_extraction`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`
**Imported by:** `intelligence/api_precompute.py`

#### `api/routers/intelligence_regime.py` — 231 LOC
**Docstring:** Regime-matched analog engine API endpoints.
**Functions:** `get_regime`, `get_regime_analogs`, `get_regime_history`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `intelligence`, `loguru`, `numpy`, `sqlalchemy`, `timesfm`, `torch`, `typing`
**Imported by:** `intelligence/api_precompute.py`

#### `api/routers/intelligence_risk.py` — 1175 LOC
**Docstring:** Intelligence sub-router: Risk map, dashboard, and globe endpoints.
**Functions:** `get_risk_map`, `get_globe_data`, `get_intelligence_dashboard`
**Reads:** `__future__`, `api`, `asyncio`, `dataclasses`, `datetime`, `fastapi`, `intelligence`, `loguru`, `numpy`, `pandas`, `physics`, `sqlalchemy`, `typing`, `utils`
**Imported by:** `api/main.py`, `intelligence/api_precompute.py`

#### `api/routers/intelligence_search.py` — 189 LOC
**Docstring:** Full-text intelligence search across the entire GRID corpus.
**Functions:** `search_intelligence`, `refresh_intelligence_search`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `sqlalchemy`

#### `api/routers/intelligence_spider.py` — 505 LOC
**Docstring:** Spider API endpoints — status, stats, inject, neighborhood, path finding.
**Functions:** `refresh_graph`, `warm_graph_async`, `get_graph_info`, `get_graph`, `InjectActorRequest`, `get_neighborhood`, `get_shortest_path`, `get_actor_connections`, `get_spider_stats`, `graph_health`, `graph_reload`, `inject_actor`
**Reads:** `__future__`, `api`, `fastapi`, `intelligence`, `loguru`, `os`, `pydantic`, `threading`, `time`, `typing`
**Imported by:** `api/main.py`

#### `api/routers/intelligence_thesis.py` — 604 LOC
**Docstring:** Intelligence sub-router: Thesis, sleuth/leads, and market diary endpoints.
**Functions:** `get_unified_thesis`, `get_thesis_history_endpoint`, `get_thesis_accuracy_endpoint`, `get_thesis_postmortems_endpoint`, `get_deep_dives_endpoint`, `get_deep_dive_endpoint`, `trigger_deep_dive`, `get_research_archive`, `get_investigation_leads`, `get_investigation_lead`, `investigate_lead`, `generate_leads`, `run_daily_investigation`, `get_diary`, `list_diaries`, `search_diaries`, `generate_diary`
**Reads:** `__future__`, `analysis`, `api`, `asyncio`, `dataclasses`, `datetime`, `fastapi`, `intelligence`, `loguru`, `typing`, `utils`

#### `api/routers/journal.py` — 158 LOC
**Docstring:** Decision journal endpoints.
**Functions:** `get_all`, `get_stats`, `get_one`, `create`, `record_outcome`
**Reads:** `__future__`, `api`, `fastapi`, `sqlalchemy`, `typing`

#### `api/routers/knowledge.py` — 87 LOC
**Docstring:** GRID API — Knowledge tree endpoints.
**Functions:** `search_knowledge_endpoint`, `knowledge_summary`, `get_knowledge_entry`, `delete_knowledge_entry`
**Reads:** `__future__`, `api`, `fastapi`, `knowledge`, `loguru`, `typing`

#### `api/routers/mcp_export.py` — 450 LOC
**Docstring:** MCP export endpoints — lightweight JSON wrappers for GRID intelligence.
**Functions:** `mcp_trust_score`, `mcp_actor_profile`, `mcp_predictions`, `mcp_prediction_accuracy`, `mcp_data_freshness`, `mcp_signal_sources`, `mcp_wealth_flows`, `mcp_regime`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `math`, `sqlalchemy`, `typing`

#### `api/routers/model_comparison.py` — 134 LOC
**Docstring:** Model comparison and drift monitoring endpoints.
**Functions:** `shadow_vs_production`, `drift_report`, `metrics_comparison`
**Reads:** `__future__`, `api`, `fastapi`, `features`, `json`, `numpy`, `sqlalchemy`

#### `api/routers/models.py` — 311 LOC
**Docstring:** Model registry endpoints.
**Functions:** `get_all`, `get_production`, `get_one`, `transition_model`, `rollback_model`, `create_from_hypothesis`, `get_feature_importance`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `features`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `api/routers/notifications.py` — 183 LOC
**Docstring:** GRID Intelligence — Push notification API endpoints.
**Functions:** `SubscriptionKeys`, `SubscribeRequest`, `UnsubscribeRequest`, `PreferencesUpdate`, `PreferencesQuery`, `get_vapid_key`, `subscribe`, `unsubscribe`, `get_preferences`, `update_preferences`, `test_push`
**Reads:** `__future__`, `alerts`, `api`, `config`, `fastapi`, `pydantic`, `urllib`

#### `api/routers/ollama.py` — 342 LOC
**Docstring:** GRID API — LLM integration endpoints.
**Functions:** `BriefingRequest`, `AskRequest`, `ExplainRequest`, `HypothesisRequest`, `RegimeAnalysisRequest`, `ollama_status`, `generate_briefing`, `get_latest_briefing`, `list_briefings`, `read_briefing`, `ask_ollama`, `explain_relationship`, `generate_hypotheses`, `analyze_regime`, `CapitalFlowRequest`, `capital_flow_research`
**Reads:** `__future__`, `analysis`, `api`, `config`, `datetime`, `db`, `fastapi`, `knowledge`, `loguru`, `ollama`, `outputs`, `pathlib`, `pydantic`, `typing`

#### `api/routers/options.py` — 500 LOC
**Docstring:** Options scanner API endpoints.
**Functions:** `get_recommendations`, `refresh_recommendations`, `get_recommendation_history`, `get_options_signals`, `scan_mispricing`, `get_100x_opportunities`, `get_scan_history`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `json`, `loguru`, `sqlalchemy`, `trading`, `typing`

#### `api/routers/oracle.py` — 530 LOC
**Docstring:** Oracle prediction endpoints — predictions, scoreboard, latest cycle.
**Functions:** `OraclePublishRequest`, `get_predictions`, `get_scoreboard`, `get_evidence`, `get_latest`, `predict_live`, `publish_prediction`, `trigger_evolve`, `get_scorecard`, `get_guard_verdicts`
**Reads:** `__future__`, `api`, `dataclasses`, `datetime`, `fastapi`, `loguru`, `oracle`, `pydantic`, `sqlalchemy`, `typing`, `utils`

#### `api/routers/physics.py` — 456 LOC
**Docstring:** GRID API — Market physics endpoints.
**Functions:** `verify`, `momentum`, `list_conventions`, `get_convention`, `ou_parameters`, `hurst`, `energy_decomposition`, `news_energy`, `physics_dashboard`
**Reads:** `__future__`, `api`, `datetime`, `db`, `fastapi`, `features`, `loguru`, `physics`, `store`, `typing`

#### `api/routers/postmortem_lessons.py` — 292 LOC
**Docstring:** Async post-mortem lessons endpoint with 6h DB cache.
**Functions:** `get_postmortem_lessons`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `api/routers/prediction_backtest.py` — 195 LOC
**Docstring:** GRID API — Prediction Market Backtesting endpoints.
**Functions:** `HypothesisRequest`, `ExportRequest`, `list_strategies`, `search_markets`, `run_hypothesis`, `dataset_stats`, `export_trades`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `pydantic`, `sqlalchemy`, `trading`, `typing`

#### `api/routers/price_alerts.py` — 212 LOC
**Docstring:** Price alerts for stepdad.finance.
**Functions:** `ensure_alerts_table`, `current_price`, `create_alert_record`, `AlertCreate`, `create_alert`, `list_alerts`, `cancel_alert`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `pydantic`, `sqlalchemy`
**Imported by:** `api/routers/chat.py`

#### `api/routers/realized_alpha.py` — 53 LOC
**Docstring:** Realized-alpha truth gate endpoint (GRID-4 pivot §8.1).
**Functions:** `get_realized_alpha`
**Reads:** `__future__`, `alpha_research`, `api`, `fastapi`, `loguru`, `typing`

#### `api/routers/regime.py` — 527 LOC
**Docstring:** Regime state endpoints.
**Functions:** `WeightUpdateRequest`, `get_weights`, `update_weights`, `simulate_weights`, `get_current`, `get_all_active`, `get_synthesis`, `get_history`, `get_transitions`
**Reads:** `__future__`, `api`, `datetime`, `discovery`, `fastapi`, `json`, `llm`, `loguru`, `pydantic`, `scripts`, `sqlalchemy`

#### `api/routers/schwab_private.py` — 200 LOC
**Docstring:** Private, admin-only Schwab quote endpoint (newly wired 2026-10-31): `GET /api/v1/schwab/private/status` and `GET /api/v1/schwab/private/quotes?symbols=SPY,QQQ` (≤10 symbols, validated at the boundary). Authorization is the existing JWT role gate `api.auth.require_role("admin")` at router level (401 without a token, 403 for non-admin, no query-param tokens). Additionally **off by default** via `settings.SCHWAB_PRIVATE_QUOTES_ENABLED`: while false the quotes route answers 503 and never builds the reader, reads credentials or touches the network. When on, builds the reviewed `SchwabMarketClient` (credential handling stays entirely in `ingestion/schwab_market.py`), reads through `read_private_quotes`, materializes into the process-local `PrivateQuoteStore`, and returns the classified snapshot + store view (provenance, provider quote time as `as_of`, receipt time as `vintage`, identity, status; `live` only for `fresh`). Receipt time is read **after** the source operation returns or fails (r2) and used for classification and vintage; the view is evaluated again at response time with a second clock read; the clock is injectable. Every response the route handlers produce (200/503/500) carries `Cache-Control: no-store, private` and `X-GRID-Audience: private`; the role dependency's 401/403 and FastAPI's 422 validation responses are the framework's, carry no quote data and are not claimed to carry these headers. No persistence, scheduling or public projection. Registered in `api/main.py`'s optional router table.
**Functions:** `private_status`, `private_quotes`
**Reads:** `__future__`, `api`, `config`, `datetime`, `fastapi`, `ingestion`, `re`, `typing`
**Tests:** `tests/test_schwab_private_api.py` (minimal FastAPI app; auth, flag, validation, reader-failure and precision controls)

#### `api/routers/search.py` — 288 LOC
**Docstring:** Universal search endpoint — searches across all GRID registries.
**Functions:** `search_everything`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `sqlalchemy`

#### `api/routers/sector_health.py` — 69 LOC
**Docstring:** Sector health endpoint.
**Functions:** `get_sector_health`
**Reads:** `__future__`, `analysis`, `api`, `fastapi`, `intelligence`, `loguru`, `typing`, `utils`

#### `api/routers/signal_registry.py` — 171 LOC
**Docstring:** Signal Registry, Model Factory & Ensemble API endpoints.
**Functions:** `EnsemblePredictRequest`, `list_signals`, `signal_stats`, `signals_for_ticker`, `refresh_registry`, `list_models`, `get_model`, `ensemble_predict`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `intelligence`, `oracle`, `pydantic`, `typing`

#### `api/routers/signals.py` — 398 LOC
**Docstring:** Live signals endpoints.
**Functions:** `get_signals`, `get_snapshot`, `crucix_signals`, `get_timeseries`, `get_conviction_scores`, `get_conviction_ticker`, `get_timeframes`
**Reads:** `__future__`, `alpha_research`, `api`, `datetime`, `fastapi`, `inference`, `loguru`, `math`, `pandas`, `re`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/api_precompute.py`

#### `api/routers/snapshots.py` — 242 LOC
**Docstring:** Analytical snapshot query endpoints.
**Functions:** `get_latest_research_run`, `get_latest_snapshots`, `get_snapshot_history`, `compare_snapshots`, `list_categories`, `get_operator_issues`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `scripts`, `sqlalchemy`, `store`, `typing`

#### `api/routers/sse.py` — 164 LOC
**Docstring:** Server-Sent Events endpoint for real-time event streaming.
**Functions:** `event_stream`, `list_channels`, `list_topics`
**Reads:** `__future__`, `api`, `asyncio`, `contracts`, `events`, `fastapi`, `json`, `loguru`, `typing`
**Imported by:** `api/main.py`

#### `api/routers/strategy.py` — 108 LOC
**Docstring:** Strategy overlay endpoints — regime-independent strategy assignments.
**Functions:** `StrategyResponse`, `StrategyAssignRequest`, `get_active_strategies`, `get_strategy_for_regime`, `assign_strategy`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `pydantic`, `strategy`, `typing`

#### `api/routers/supply_chain.py` — 105 LOC
**Docstring:** Supply chain graph endpoint for actor profile drawer.
**Functions:** `get_supply_chain`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `typing`, `utils`

#### `api/routers/supply_chain_helpers.py` — 597 LOC
**Docstring:** Helpers for the supply_chain router.
**Reads:** `__future__`, `analysis`, `api`, `collections`, `datetime`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/supply_chain.py`

#### `api/routers/surfacer.py` — 2358 LOC
**Docstring:** GRID Surfacer API.
**Functions:** `list_candidates`
**Reads:** `__future__`, `api`, `collections`, `datetime`, `fastapi`, `json`, `loguru`, `math`, `re`, `sqlalchemy`, `typing`

#### `api/routers/system.py` — 2197 LOC
**Docstring:** System status and health endpoints.
**Functions:** `health`, `status`, `freshness`, `pipeline_health`, `get_logs`, `alerts`, `restart_hyperspace`, `trigger_ux_audit`, `list_ux_audits`, `trigger_daily_digest`, `run_taxonomy_audit_endpoint`, `set_hermes_state`, `hermes_status`, `get_settings`, `update_settings`, `get_api_keys`, `get_services`, `get_hermes_history`, `architecture`, `get_resolution_audit`, `run_resolution_audit`
**Reads:** `__future__`, `analysis`, `api`, `config`, `datetime`, `db`, `fastapi`, `glob`, `hyperspace`, `ingestion`, `inspect`, `intelligence`, `json`, `loguru`, `os`, `pathlib`, `psutil`, `scripts`, `shutil`, `sqlalchemy`, `store`, `subprocess`, `sys`, `threading`, `time`, `trading`, `types`, `urllib`
**Imported by:** `intelligence/api_precompute.py`

#### `api/routers/ten_year_portfolio.py` — 516 LOC
**Docstring:** Ten-year portfolio query endpoints.
**Functions:** `list_profiles`, `weekly_ten_year_portfolio`, `analyze_private_workbook`, `export_current_model_workbook`, `export_private_workbook_plan`
**Reads:** `__future__`, `api`, `collections`, `concurrent`, `datetime`, `fastapi`, `loguru`, `psycopg2`, `sqlalchemy`, `store`, `strategy`, `typing`

#### `api/routers/tps.py` — 210 LOC
**Docstring:** Trump-Proximity Score (TPS) endpoints — Phase 0.
**Functions:** `get_today`, `get_ticker`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `intelligence`, `json`, `sqlalchemy`, `typing`

#### `api/routers/trade_tickets.py` — 94 LOC
**Docstring:** Trade tickets derived from contagion predictions.
**Functions:** `recent_tickets`, `tickets_for_prediction`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `trading`, `typing`

#### `api/routers/trading.py` — 710 LOC
**Docstring:** Paper trading, Hyperliquid perp, and prediction market API endpoints.
**Functions:** `PolymarketBuyRequest`, `KalshiBuyRequest`, `HyperliquidTradeRequest`, `HyperliquidCloseRequest`, `RobinhoodTradeRequest`, `RobinhoodCloseRequest`, `TradeRequest`, `CloseTradeRequest`, `CreateWalletRequest`, `KillWalletRequest`, `trading_dashboard`, `register_all_strategies`, `open_trade`, `close_trade`, `list_strategies`, `strategy_trade_history`, `PromoteToStrategyRequest`, `promote_to_strategy`, `execute_signals_now`, `kill_strategy`, `wallet_dashboard`, `list_wallets`, `create_wallet`, `get_wallet`, `wallet_risk_check`, `kill_wallet`, `pause_wallet`, `resume_wallet`, `hyperliquid_balance`, `hyperliquid_positions`, `hyperliquid_trade`, `hyperliquid_close`, `robinhood_status`, `robinhood_balance`, `robinhood_positions`, `robinhood_orders`, `robinhood_trade`, `robinhood_close`, `robinhood_cancel`, `polymarket_markets`, `polymarket_portfolio`, `polymarket_buy`, `kalshi_events`, `kalshi_portfolio`, `kalshi_buy`, `options_recommendations`, `options_tracker_score`
**Reads:** `__future__`, `analysis`, `api`, `datetime`, `fastapi`, `functools`, `pydantic`, `sqlalchemy`, `threading`, `trading`, `typing`, `utils`

#### `api/routers/tradingview.py` — 238 LOC
**Docstring:** TradingView webhook integration.
**Functions:** `receive_webhook`, `get_signals`
**Reads:** `__future__`, `api`, `config`, `datetime`, `fastapi`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `api/routers/trials.py` — 186 LOC
**Docstring:** Trial gem hunter endpoints.
**Functions:** `get_gems`, `get_signals`, `get_catalysts`, `get_sponsors`, `get_stats`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `sqlalchemy`, `typing`

#### `api/routers/user_intel.py` — 389 LOC
**Docstring:** User-contributed intelligence router.
**Functions:** `IntelSubmission`, `VotePayload`, `VerifyPayload`, `submit_intel`, `get_actor_intel`, `vote_intel`, `flag_intel`, `verify_intel`, `list_pending_intel`, `http_submit_intel`, `http_get_actor_intel`, `http_vote_intel`, `http_flag_intel`, `http_verify_intel`, `http_list_pending`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `loguru`, `os`, `pydantic`, `sqlalchemy`, `typing`

#### `api/routers/valuation.py` — 574 LOC
**Docstring:** GRID API — Valuation & Derivatives Support endpoints.
**Functions:** `MilestoneCreate`, `MilestoneStatusUpdate`, `AnalysisResponse`, `analyze_ticker`, `generate_prompt`, `log_analysis_response`, `valuation_history`, `prediction_history`, `get_milestones`, `add_milestone`, `update_milestone_status`, `derivatives_support`, `catalyst_timeline`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `loguru`, `pydantic`, `sqlalchemy`, `typing`, `valuation`

#### `api/routers/vault.py` — 532 LOC
**Docstring:** Vault API router — Obsidian Bridge CRUD, FTS search, dashboard, and sync trigger.
**Functions:** `list_notes`, `get_note`, `create_note`, `update_note_status`, `search_notes`, `list_actions`, `get_dashboard`, `trigger_sync`, `generate_backlinks`, `generate_concept_stubs`
**Reads:** `__future__`, `api`, `asyncio`, `datetime`, `fastapi`, `hashlib`, `ingestion`, `json`, `loguru`, `pathlib`, `scripts`, `sqlalchemy`, `typing`

#### `api/routers/viz.py` — 241 LOC
**Docstring:** GRID Visualization Intelligence API.
**Functions:** `recommend_visualization`, `list_visualization_rules`, `get_source_weights`, `capital_flow_viz_spec`, `regime_phase_viz_spec`, `feature_network_viz_spec`, `energy_particle_viz_spec`, `sector_orbital_viz_spec`, `lead_lag_river_viz_spec`
**Reads:** `__future__`, `analysis`, `fastapi`, `typing`

#### `api/routers/watchlist.py` — 37 LOC
**Docstring:** Watchlist API — facade router.
**Reads:** `__future__`, `api`, `fastapi`
**Imported by:** `api/routers/astrogrid_core.py`, `api/routers/astrogrid_helpers.py`

#### `api/routers/watchlist_analysis.py` — 480 LOC
**Docstring:** Watchlist sub-router: per-ticker technical analysis endpoint.
**Functions:** `get_ticker_analysis`
**Reads:** `__future__`, `analysis`, `api`, `datetime`, `fastapi`, `json`, `loguru`, `sqlalchemy`, `store`, `typing`, `yfinance`
**Imported by:** `api/routers/watchlist.py`

#### `api/routers/watchlist_core.py` — 823 LOC
**Docstring:** Watchlist sub-router: core CRUD and utility endpoints.
**Functions:** `list_watchlist`, `refresh_watchlist_prices`, `get_watchlist_prices`, `get_portfolio`, `list_watchlist_enriched`, `search_tickers`, `add_to_watchlist`, `preload_watchlist`, `remove_from_watchlist`
**Reads:** `__future__`, `analysis`, `api`, `concurrent`, `datetime`, `fastapi`, `loguru`, `sqlalchemy`, `time`, `yfinance`
**Imported by:** `api/routers/watchlist.py`

#### `api/routers/watchlist_helpers.py` — 645 LOC
**Docstring:** Watchlist shared helpers — utilities imported by sub-routers and external callers.
**Functions:** `effective_asset_type`
**Reads:** `__future__`, `api`, `datetime`, `json`, `loguru`, `normalization`, `re`, `sqlalchemy`, `typing`, `utils`, `yfinance`
**Imported by:** `api/routers/dad.py`, `api/routers/price_alerts.py`, `api/routers/watchlist.py`, `api/routers/watchlist_analysis.py`, `api/routers/watchlist_core.py`, `api/routers/watchlist_overview.py`

#### `api/routers/watchlist_overview.py` — 892 LOC
**Docstring:** Watchlist sub-router: AI overview and insider-edge endpoints.
**Functions:** `get_ticker_overview`, `get_ticker_quote`, `get_ticker_edge`
**Reads:** `__future__`, `analysis`, `api`, `datetime`, `fastapi`, `intelligence`, `json`, `llm`, `loguru`, `math`, `ollama`, `sqlalchemy`
**Imported by:** `api/routers/watchlist.py`

#### `api/routers/workflows.py` — 176 LOC
**Docstring:** GRID API — Workflow management endpoints.
**Functions:** `list_workflows`, `list_enabled`, `enable`, `disable`, `run_workflow`, `validate`, `get_waves`, `get_schedule`
**Reads:** `__future__`, `api`, `fastapi`, `loguru`, `pathlib`, `physics`, `typing`, `workflows`

#### `api/schemas/__init__.py` — 1 LOC
**Docstring:** GRID API Pydantic schemas.

#### `api/schemas/auth.py` — 54 LOC
**Docstring:** Authentication schemas.
**Functions:** `LoginRequest`, `LoginResponse`, `TokenVerifyResponse`, `RegisterRequest`, `CreateUserRequest`, `UserResponse`, `StreamTicketRequest`, `StreamTicketResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/auth.py`

#### `api/schemas/journal.py` — 75 LOC
**Docstring:** Journal schemas.
**Functions:** `JournalEntryCreate`, `JournalOutcomeRecord`, `JournalEntryResponse`, `JournalStatsResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/journal.py`

#### `api/schemas/models.py` — 43 LOC
**Docstring:** Model registry schemas.
**Functions:** `ModelFromHypothesisRequest`, `ModelTransitionRequest`, `ModelRollbackRequest`, `ModelResponse`, `ProductionModelsResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/models.py`

#### `api/schemas/regime.py` — 56 LOC
**Docstring:** Regime schemas.
**Functions:** `RegimeDriver`, `RegimeCurrentResponse`, `RegimeHistoryEntry`, `RegimeHistoryResponse`, `RegimeTransition`, `RegimeTransitionsResponse`
**Reads:** `__future__`, `pydantic`
**Imported by:** `api/routers/regime.py`

#### `api/schemas/system.py` — 256 LOC
**Docstring:** System status schemas.
**Functions:** `HyperspaceStatus`, `DatabaseStatus`, `GridStats`, `ServerHealth`, `SystemStatusResponse`, `HealthResponse`, `LogsResponse`, `RestartResponse`, `FamilyFreshness`, `StaleSource`, `FreshnessAuditBucket`, `DailyFreshnessAudit`, `FreshnessResponse`, `HermesTaskStatus`, `HermesStatusResponse`, `PipelineSourceStatus`, `PipelineSummary`, `FamilyCoverage`, `ResolverStatus`, `PipelineError`, `PipelineHealthResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/system.py`

#### `api/schemas/watchlist.py` — 42 LOC
**Docstring:** Watchlist schemas.
**Functions:** `WatchlistItemCreate`, `WatchlistItemResponse`
**Reads:** `__future__`, `pydantic`
**Imported by:** `api/routers/watchlist_core.py`

## `analysis/`

#### `analysis/__init__.py` — 1 LOC
**Imported by:** `alpha_research/validation/null_farm.py`, `analysis/ledger_steered_exploration.py`, `analysis/panel_insider_density_sectors_v2.py`, `analysis/panel_insider_density_sectors_v3.py`, `analysis/panel_insider_density_sectors_v4.py`, `analysis/panel_insider_density_sectors_v5.py`, `analysis/panel_insider_density_sectors_v6.py`, `analysis/panel_insider_density_v2.py`, … (+10)

#### `analysis/astro_correlations.py` — 539 LOC
**Docstring:** Celestial-Market Correlation Engine.
**Functions:** `AstroCorrelationEngine`
**Reads:** `__future__`, `datetime`, `db`, `ingestion`, `loguru`, `math`, `numpy`, `pandas`, `sqlalchemy`
**Imported by:** `api/routers/astrogrid_celestial.py`, `intelligence/scheduler.py`

#### `analysis/backtest_scanner.py` — 503 LOC
**Docstring:** Automated cross-asset backtest scanner.
**Functions:** `scan_all_pairs`, `generate_hypotheses_from_winners`, `run_full_scan`, `review_existing_hypotheses`
**Reads:** `__future__`, `json`, `llm`, `loguru`, `numpy`, `ollama`, `pandas`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/discovery.py`, `api/routers/trading.py`

#### `analysis/capital_flows.py` — 897 LOC
**Docstring:** GRID Capital Flow Research Engine.
**Functions:** `CapitalFlowResearchEngine`
**Reads:** `__future__`, `datetime`, `hashlib`, `json`, `loguru`, `numpy`, `ollama`, `outputs`, `pathlib`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/ollama.py`, `intelligence/scheduler.py`

#### `analysis/ephemeris.py` — 1034 LOC
**Docstring:** GRID Ephemeris Engine -- Copernicus Module.
**Functions:** `Ephemeris`, `get_ephemeris`
**Reads:** `__future__`, `datetime`, `math`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`

#### `analysis/etf_price_features.py` — 997 LOC
**Docstring:** Synthetic numeric/session contracts and exact caller-qualified OR/VWAP.
**Functions:** `Adjustment`, `NotionalBasis`, `TimestampConvention`, `SessionConvention`, `BarInterval`, `PriceBasis`, `SessionWindow`, `NumericBar`, `ExpectedBarGrid`, `StructuralResult`, `compare_price_basis`, `validate_numeric_bar`, `validate_bar_grid`, `PriceFeatureMethod`, `PriceFeatureResult`, `opening_range`, `trade_vwap`, `ohlcv_hlc3_volume_proxy`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `decimal`, `enum`, `fractions`, `zoneinfo`
**Imported by:** `analysis/market_feature_display.py`

#### `analysis/feed_reliability_projection.py` — 835 LOC
**Docstring:** Pure synthetic ETF observation quality. No feed collection or admission authority.
**Functions:** `State`, `Presence`, `Finality`, `Mode`, `Check`, `SourceIdentity`, `FactReference`, `DelayEvidence`, `LifecycleFacts`, `OptionsFacts`, `SessionFacts`, `Observation`, `CapabilityPolicy`, `EvaluationPolicy`, `CheckResult`, `DescriptiveContext`, `CapabilityResult`, `InstrumentResult`, `EvaluationResult`, `evaluate_feed_quality`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `math`, `types`, `typing`, `zoneinfo`
**Imported by:** `analysis/etf_price_features.py`, `analysis/market_feature_display.py`

#### `analysis/flow_aggregator.py` — 1148 LOC
**Docstring:** GRID — Flow Aggregation Engine.
**Functions:** `aggregate_by_sector`, `aggregate_by_time`, `aggregate_by_actor_tier`, `compute_flow_momentum`, `build_sector_flow_matrix`, `aggregate_smart_vs_dumb`, `compute_sector_conviction`, `compute_flow_velocity`, `aggregate_confidence_weighted`, `get_full_aggregation`
**Reads:** `__future__`, `analysis`, `collections`, `datetime`, `loguru`, `sqlalchemy`, `threading`, `time`, `typing`
**Imported by:** `analysis/money_flow.py`, `analysis/thesis_scorer.py`, `api/routers/flows.py`, `intelligence/image_gen.py`

#### `analysis/flow_thesis.py` — 21 LOC
**Docstring:** GRID — Flow Thesis Knowledge Base.
**Reads:** `analysis`
**Imported by:** `api/routers/intelligence_thesis.py`, `intelligence/adapters/flow_thesis_adapter.py`, `intelligence/audio_briefing.py`, `intelligence/image_gen.py`, `intelligence/thesis_tracker.py`

#### `analysis/flow_thesis_data.py` — 1419 LOC
**Docstring:** GRID — Flow Thesis Knowledge Base (data module).
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `types`, `typing`
**Imported by:** `analysis/flow_thesis.py`, `analysis/flow_thesis_scoring.py`

#### `analysis/flow_thesis_scoring.py` — 336 LOC
**Docstring:** GRID — Flow Thesis Scoring and Narrative Generation.
**Functions:** `update_current_states`, `generate_unified_thesis`
**Reads:** `__future__`, `analysis`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `analysis/flow_thesis.py`

#### `analysis/generalization_gate.py` — 726 LOC
**Docstring:** Generalization gate (GD10b): when a sector finding may be called "general".
**Functions:** `GateSpec`, `canonical`, `spec_sha256`, `SectorResult`, `terminal_content_sha256`, `forward_content_sha256`, `GateVerdict`, `binomial_chance`, `survives`, `breadth_count`, `joint_null_means`, `sector_block_permutation_null`, `permutation_threshold`, `loso_pooled`, `top_entity_share`, `forward_confirmations`, `coverage_honesty`, `evaluate_gate`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `hashlib`, `json`, `math`, `numpy`, `pathlib`, `re`, `scipy`

#### `analysis/gex_intraday_prereg.py` — 446 LOC
**Docstring:** GEX P3 intraday hypothesis family v1: pre-registration registry (design only).
**Functions:** `PreregError`, `lf_sha256`, `prereg_body`, `body_sha256`, `read_body`, `family_block`, `validate_family`, `check_prereg`, `engine_matches_pin`, `registry`, `registration_records`, `register`, `export_anchors`, `verify`, `parse_now`, `main`
**Reads:** `__future__`, `analysis`, `argparse`, `datetime`, `hashlib`, `json`, `pathlib`, `re`, `sys`, `typing`

#### `analysis/hypothesis_tester.py` — 494 LOC
**Docstring:** Hypothesis backtesting orchestrator.
**Functions:** `compute_lagged_correlation`, `test_hypothesis`, `run_all_tests`
**Reads:** `__future__`, `datetime`, `db`, `json`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `analysis/offline_research_proof.py`, `api/routers/flows.py`

#### `analysis/lead_lag_backtest.py` — 309 LOC
**Docstring:** CAT-115 — Cross-asset lead-lag backtest framework.
**Functions:** `FoldResult`, `WalkForwardResult`, `run_walk_forward`
**Reads:** `__future__`, `dataclasses`, `math`, `numpy`, `typing`

#### `analysis/ledger_steered_exploration.py` — 2335 LOC
**Docstring:** Ledger-steered exploration for the offline research contract (S11).
**Functions:** `PermutationBudgetError`, `min_attainable_p`, `perms_for_threshold`, `first_step_threshold`, `run_alpha`, `now_iso`, `canonical`, `sha256_bytes`, `trial_id`, `scientific_identity`, `identity`, `catalog_from_dict`, `validate_catalog_extension`, `validate_extension_record`, `Catalog`, `catalog_from_specs`, `Anchor`, `Ledger`, `verify_chain`, `Policy`, `cached_stamp`, `overlaps`, `validate_windows`, `probability_best`, `split_budget`, `extend_catalog`, `allocate`, `abandon`, `protocol_for_allocation`, `default_perms`, `status_of`, `record_run`, `ingest_forward_outcomes`, `ingest_forward_log`, `OverfitGateRefused`, `overfit_gate_prereg_sha256`, `declared_trial_count`, `ledgered_tested_trials`, `evaluate_overfit_gate`, `record_overfit_gate`, `require_overfit_gate`, `hypothesis_batch_trials`, `validate_hypothesis_batch`, `record_hypothesis_batch`, `summarize_hypothesis_batches`, `summarize`, `allocation_counts`, `synthetic_catalog`, `Epoch`, `synthetic_epoch`, `run_synthetic_step`, `run_synthetic_dry_runs`
**Reads:** `__future__`, `alpha_research`, `analysis`, `dataclasses`, `datetime`, `functools`, `hashlib`, `json`, `math`, `numpy`, `pandas`, `pathlib`, `scipy`
**Imported by:** `alpha_research/validation/gauntlet.py`, `analysis/sector_proxy_rules.py`, `intelligence/hypothesis_trials.py`

#### `analysis/market_feature_display.py` — 117 LOC
**Docstring:** Pure display projection of a supplied synthetic PriceFeatureResult.
**Functions:** `project_price_feature`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `decimal`, `enum`, `fractions`

#### `analysis/market_universe.py` — 1184 LOC
**Docstring:** Comprehensive S&P 500 Market Universe — every GICS sector, industry, and major company.
**Functions:** `get_universe`, `get_sector`, `get_industry`, `get_peers`, `search_company`, `get_all_tickers`
**Reads:** `__future__`
**Imported by:** `api/routers/watchlist_analysis.py`, `intelligence/deep_graph.py`, `intelligence/long_plays.py`, `intelligence/market_edge_scanner.py`, `ollama/market_briefing.py`, `ollama/number_grounding.py`

#### `analysis/materialization_accounting.py` — 224 LOC
**Docstring:** Pure diagnostics for supplied accounting declarations, never persistence proof.
**Functions:** `CommitOutcome`, `CoverageState`, `TransactionDeclaration`, `TransactionDiagnostic`, `diagnose_transaction`, `CoverageDeclaration`, `CoverageDiagnostic`, `diagnose_coverage`, `RunDeclaration`, `RunDiagnostic`, `diagnose_run`
**Reads:** `dataclasses`, `enum`
**Imported by:** `ingestion/altdata/sec_xbrl_shares.py`, `ingestion/smart_scheduler.py`

#### `analysis/money_flow.py` — 1783 LOC
**Docstring:** GRID — Global Money Flow Map.
**Functions:** `build_flow_map`, `get_sector_drill`, `get_company_drill`
**Reads:** `__future__`, `analysis`, `datetime`, `ingestion`, `intelligence`, `json`, `loguru`, `numpy`, `ollama`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/flows.py`, `intelligence/api_precompute.py`

#### `analysis/money_flow_engine/__init__.py` — 114 LOC
**Docstring:** GRID — Money Flow Engine.
**Functions:** `build_flow_map`
**Reads:** `__future__`, `datetime`, `flow_inference`, `layer_corporate`, `layer_credit`, `layer_crypto`, `layer_institutional`, `layer_market`, `layer_monetary`, `layer_retail`, `layer_sovereign`, `loguru`, `sqlalchemy`, `types`
**Imported by:** `api/routers/chat.py`, `api/routers/flows.py`, `intelligence/audio_briefing.py`, `intelligence/deep_dive.py`, `intelligence/image_gen.py`, `intelligence/money_flow_adapter.py`

#### `analysis/money_flow_engine/flow_inference.py` — 245 LOC
**Docstring:** GRID — Multi-Directional Flow Inference Engine.
**Functions:** `infer_flow_edges`
**Reads:** `__future__`, `types`

#### `analysis/money_flow_engine/helpers.py` — 380 LOC
**Docstring:** GRID -- Money Flow Engine Helpers.
**Functions:** `compute_z_score`, `dominant_confidence`, `series_to_usd`, `compute_changes`
**Reads:** `__future__`, `collections`, `datetime`, `sqlalchemy`, `typing`

#### `analysis/money_flow_engine/layer_corporate.py` — 297 LOC
**Docstring:** GRID -- Money Flow Engine: Corporate Layer (Layer 5, order=4).
**Functions:** `build_corporate_layer`
**Reads:** `__future__`, `datetime`, `helpers`, `loguru`, `sqlalchemy`, `types`

#### `analysis/money_flow_engine/layer_credit.py` — 411 LOC
**Docstring:** GRID -- Credit Layer (Layer 2).
**Functions:** `build_credit_layer`
**Reads:** `__future__`, `datetime`, `helpers`, `intelligence`, `loguru`, `sqlalchemy`, `types`

#### `analysis/money_flow_engine/layer_crypto.py` — 284 LOC
**Docstring:** GRID -- Money Flow Engine: Crypto Layer (Layer 8, order=7).
**Functions:** `build_crypto_layer`
**Reads:** `__future__`, `datetime`, `helpers`, `loguru`, `sqlalchemy`, `types`

#### `analysis/money_flow_engine/layer_institutional.py` — 295 LOC
**Docstring:** GRID — Institutional Layer (Layer 3).
**Functions:** `build_institutional_layer`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `types`, `typing`

#### `analysis/money_flow_engine/layer_market.py` — 416 LOC
**Docstring:** GRID — Market Layer (Layer 4).
**Functions:** `build_market_layer`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `types`, `typing`

#### `analysis/money_flow_engine/layer_monetary.py` — 306 LOC
**Docstring:** GRID -- Monetary Layer (Layer 1).
**Functions:** `build_monetary_layer`
**Reads:** `__future__`, `datetime`, `helpers`, `loguru`, `sqlalchemy`, `types`

#### `analysis/money_flow_engine/layer_retail.py` — 304 LOC
**Docstring:** GRID -- Money Flow Engine: Retail Layer (Layer 7, order=6).
**Functions:** `build_retail_layer`
**Reads:** `__future__`, `datetime`, `helpers`, `loguru`, `sqlalchemy`, `types`

#### `analysis/money_flow_engine/layer_sovereign.py` — 237 LOC
**Docstring:** GRID -- Money Flow Engine: Sovereign Layer (Layer 6, order=5).
**Functions:** `build_sovereign_layer`
**Reads:** `__future__`, `datetime`, `helpers`, `loguru`, `sqlalchemy`, `types`

#### `analysis/money_flow_engine/types.py` — 102 LOC
**Docstring:** GRID -- Money Flow Engine Types.
**Functions:** `FlowNode`, `FlowLayer`, `FlowEdge`, `FlowMap`
**Reads:** `__future__`, `dataclasses`, `typing`
**Imported by:** `intelligence/money_flow_adapter.py`

#### `analysis/offline_research_proof.py` — 875 LOC
**Docstring:** Offline research contract proof. No registry, DB, provider, route or timer wiring.
**Functions:** `candidate_state`, `digest`, `stamp`, `Protocol`, `protocol_from_payload`, `excluded`, `outcome_horizon`, `validate_rows`, `lag1_autocorrelation`, `autocorrelation_block`, `permutation_block`, `holdout_block`, `build_family_rows`, `block_permutations`, `block_permutation_pvalue`, `bh_adjusted`, `holm_adjusted`, `corrected_p`, `measure`, `as_families`, `check_read_rows`, `self_lag_result`, `discover`, `evaluate_holdout`, `write_once`, `run_proof`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `functools`, `hashlib`, `json`, `math`, `numpy`, `pandas`, `pathlib`, `scipy`
**Imported by:** `alpha_research/validation/gauntlet.py`, `alpha_research/validation/overfitting.py`, `analysis/generalization_gate.py`, `analysis/ledger_steered_exploration.py`, `analysis/panel_artifact_reader.py`, `analysis/panel_insider_density.py`, `analysis/panel_insider_density_v2.py`, `analysis/panel_insider_density_v8.py`, … (+6)

#### `analysis/panel_artifact_reader.py` — 140 LOC
**Docstring:** Capability-gated reader for frozen feature artifacts (GD5 ``write_frozen_artifact`` output).
**Functions:** `receipt_path`, `read_frozen_artifact`
**Reads:** `__future__`, `analysis`, `datetime`, `hashlib`, `io`, `json`, `pandas`, `pathlib`, `typing`

#### `analysis/panel_insider_density.py` — 2746 LOC
**Docstring:** VS1 panel harness: insider open-market-buy density within one sector (research only).
**Functions:** `rankdata`, `discovery_start`, `discovery_window`, `run_alpha`, `trial_names`, `lf_sha256`, `prereg_body`, `prereg_body_sha256`, `check_prereg`, `file_sha256`, `primary_sectors`, `load_sector_map`, `load_issuer_map`, `sector_universe`, `read_table`, `parse_dates`, `filing_known_at`, `Form4Events`, `section16_accessions`, `build_events`, `data_sha256`, `load_events`, `decision_instants`, `proxy_sessions`, `density`, `active_mask`, `largest_value`, `entry_positions`, `feature_panel`, `PriceManifest`, `DiscoveryKey`, `HoldoutKey`, `check_holdout_request`, `PricePanel`, `load_price_panel`, `TrialPanel`, `window_bounds`, `relative_labels`, `build_trial_panels`, `validate_panel`, `rank_ic_series`, `block_signs`, `signflip_pvalues`, `time_alignment_pvalue`, `entity_shuffle_pvalue`, `buyer_excess`, `missing_labels`, `momentum_baseline`, `measure_trial`, `RunSpec`, `discover_panel`, `calibration`, `evaluate_panel_holdout`, `verdict`, `planted_power`, `power_settings`, `stage0_power`, `verify_power`, `power_features`, `registry`, `registration_records`, `chained_sha256`, `register`, `vs1_witness_census`, `canonical_witness_path`, `witnessed_versions`, `refuse_superseded`, `OffhostWitness`, `check_offhost`, `require_witness`, `export_anchors`, `record_prices_read`, `freeze_inputs`, `latest_frozen_inputs`, `open_discovery`, `resume_discovery`, `seal_discovery`, `open_holdout`, `resume_holdout`, `seal_holdout`, `write_frozen`
**Reads:** `__future__`, `analysis`, `contextlib`, `contextvars`, `dataclasses`, `datetime`, `hashlib`, `json`, `math`, `numpy`, `pandas`, `pathlib`, `pyarrow`, `re`, `store`, `subprocess`, `tempfile`, `typing`, `yaml`, `zoneinfo`
**Imported by:** `analysis/generalization_gate.py`

#### `analysis/panel_insider_density_sectors_v2.py` — 297 LOC
**Docstring:** VS1 other-10-sector joint run, pre-registration "sectors v2" (registry and universe only; research only).
**Functions:** `check_open`, `check_ranges`, `sic_sector`, `sector_universes`, `registration_records`, `registry`, `check_prereg`, `register`, `main`
**Reads:** `__future__`, `analysis`, `argparse`, `datetime`, `json`, `pandas`, `pathlib`, `sys`, `typing`
**Imported by:** `analysis/panel_insider_density_sectors_v3.py`

#### `analysis/panel_insider_density_sectors_v3.py` — 199 LOC
**Docstring:** VS1 other-10-sector joint run, pre-registration "sectors v3" (registry only; research only).
**Functions:** `check_open`, `stage0_attainable`, `registration_records`, `registry`, `check_prereg`, `register`, `main`
**Reads:** `__future__`, `analysis`, `argparse`, `datetime`, `json`, `pathlib`, `sys`
**Imported by:** `analysis/panel_insider_density_sectors_v4.py`

#### `analysis/panel_insider_density_sectors_v4.py` — 287 LOC
**Docstring:** VS1 other-10-sector joint run, pre-registration "sectors v4" (registry only; research only).
**Functions:** `technology_terminal`, `check_technology_run`, `check_census`, `check_open`, `registration_records`, `registry`, `check_prereg`, `register`, `main`
**Reads:** `__future__`, `analysis`, `argparse`, `datetime`, `importlib`, `json`, `pathlib`, `sys`, `types`, `typing`

#### `analysis/panel_insider_density_sectors_v5.py` — 191 LOC
**Docstring:** Sectors-v5 custody scaffold; the ten other sectors wait for v7 holdout_result.
**Functions:** `check_prereg`, `registry`, `verify_v7_registration`, `check_registration_census`, `register`, `verify_v7_holdout_result`, `check_census`, `registration_records`, `check_open`
**Reads:** `__future__`, `analysis`, `datetime`, `json`, `pathlib`, `re`, `tempfile`, `typing`

#### `analysis/panel_insider_density_sectors_v6.py` — 191 LOC
**Docstring:** Sectors-v6 custody scaffold: the ten-sector generalization test bound to VS1 v8 (GD-10a).
**Functions:** `check_prereg`, `registry`, `verify_v8_registration`, `check_registration_census`, `registration_records`, `register`, `check_open`
**Reads:** `__future__`, `analysis`, `datetime`, `json`, `pathlib`, `re`, `tempfile`, `typing`

#### `analysis/panel_insider_density_v2.py` — 1730 LOC
**Docstring:** VS1 v2 panel harness: SIC-expanded Technology insider open-market-buy density (research only).
**Functions:** `prereg_body_sha256`, `sic_in_ranges`, `sic_group`, `load_sic_map`, `load_issuer_map`, `canonical_symbol`, `filed_symbols`, `v2_universe`, `read_submissions`, `Admission`, `build_admission`, `form4_history_mask`, `ticker_mask`, `admitted_mask`, `feature_panel`, `load_inputs`, `PriceManifest`, `TrialPanel`, `build_trial_panels`, `delisting_sensitivity`, `industry_neutral_ic`, `measure_trial`, `admission_report`, `make_run_spec`, `calibration`, `verdict`, `contamination`, `power_features`, `DiscoveryKey`, `HoldoutKey`, `OffhostWitness`, `EarlierVersion`, `Pins`, `Harness`, `write_frozen`, `registration_records`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `json`, `numpy`, `pandas`, `pathlib`, `pyarrow`, `re`, `store`, `subprocess`, `sys`, `tempfile`, `typing`
**Imported by:** `analysis/panel_insider_density_v3.py`, `analysis/panel_insider_density_v4.py`, `analysis/panel_insider_density_v5.py`, `analysis/panel_insider_density_v6.py`

#### `analysis/panel_insider_density_v3.py` — 188 LOC
**Docstring:** VS1 v3 panel harness: v2's SIC-expanded Technology study with the 5-session trial primary (research only).
**Functions:** `registration_records`, `prereg_body_sha256`, `run_spec`, `calibration`, `verdict`
**Reads:** `__future__`, `analysis`, `datetime`, `pathlib`, `sys`, `typing`

#### `analysis/panel_insider_density_v4.py` — 302 LOC
**Docstring:** VS1 v4 panel harness: v3 plus a price-admission rule and a post-admission power gate (research only).
**Functions:** `CrossCheckRule`, `crosscheck_statistics`, `PriceManifest`, `registration_records`, `prereg_body_sha256`, `run_spec`, `calibration`, `verdict`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `math`, `pathlib`, `sys`, `typing`

#### `analysis/panel_insider_density_v5.py` — 338 LOC
**Docstring:** VS1 v5 panel harness: v4 plus source filtering, a pull-batch splice check and a ticker-reuse price bound.
**Functions:** `PriceManifest`, `splice_check`, `name_match`, `first_naming_dates`, `listing_bound`, `build_trial_panels`, `registration_records`, `prereg_body_sha256`, `run_spec`, `calibration`, `verdict`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `pandas`, `pathlib`, `re`, `sys`, `typing`

#### `analysis/panel_insider_density_v6.py` — 349 LOC
**Docstring:** VS1 v6 panel harness: v5 with the ticker-reuse price bound as candidate amendment C1 (research only).
**Functions:** `PriceManifest`, `interval_close_mask`, `build_trial_panels`, `append_stop_status`, `verify_terminal_stop`, `registration_records`, `prereg_body_sha256`, `run_spec`, `calibration`, `verdict`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `pandas`, `pathlib`, `sys`, `typing`

#### `analysis/panel_insider_density_v7.py` — 439 LOC
**Docstring:** VS1 v7: v6 rules with a preregistered 2011-10 discovery start.
**Functions:** `check_prereg`, `check_census`, `contamination`, `registration_records`, `V7Harness`, `e0_v7_power`, `append_stop_status`, `verify_terminal_stop`, `run_spec`, `prereg_body_sha256`
**Reads:** `__future__`, `analysis`, `datetime`, `hashlib`, `json`, `pathlib`, `re`, `sys`, `typing`

#### `analysis/panel_insider_density_v8.py` — 681 LOC
**Docstring:** VS1 v8: v7 rules with a 2008-01 discovery start, one confirmatory test and a two-model Stage-0.
**Functions:** `check_prereg`, `check_census`, `contamination`, `registration_records`, `planted_power_one_sided`, `e0_power`, `confirmatory_selection`, `V8Harness`, `append_stop_status`, `verify_terminal_stop`, `run_spec`, `prereg_body_sha256`
**Reads:** `__future__`, `analysis`, `datetime`, `evals`, `hashlib`, `importlib`, `json`, `math`, `numpy`, `pathlib`, `re`, `sys`, `typing`

#### `analysis/panel_mode.py` — 1634 LOC
**Docstring:** Panel mode: the VS1 panel statistic and one-shot custody for any registered construct.
**Functions:** `run_alpha`, `ConstructSpec`, `trial_horizon`, `family`, `family_key`, `PanelRunSpec`, `as_constructs`, `declared_trials`, `construct_of`, `constructs_digest`, `normalise_ticker`, `load_technology_denylist`, `V8TerminalWitness`, `SectorsV6Witness`, `is_sectors_v6_family`, `OutcomeWindowGuard`, `require_guard`, `issue_v8_terminal_witness`, `issue_sectors_v6_witness`, `measure_panel_trial`, `synthetic_decisions`, `e0_measure`, `validate_panel`, `entity_half`, `restrict_entities`, `panel_digest`, `discover_panel`, `calibration`, `evaluate_panel_holdout`, `verdict`, `construct_from_record`, `run_from_record`, `write_frozen`, `write_timing`, `check_registry_id`, `witness_path`, `check_witness_path`, `PanelInputs`, `PanelRegistry`, `check_prereg`, `registration_records`, `register`, `registered`, `GranularWitness`, `granular_census`, `check_offhost`, `check_census`, `require_witness`, `export_anchors`, `PanelDiscoveryKey`, `PanelHoldoutKey`, `freeze_inputs`, `open_discovery`, `resume_discovery`, `record_prices_read`, `discovery_inputs`, `universe`, `seal_discovery`, `open_holdout`, `resume_holdout`, `seal_holdout`, `verify`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `hashlib`, `json`, `numpy`, `pandas`, `pathlib`, `re`, `subprocess`, `tempfile`, `typing`

#### `analysis/panel_prices.py` — 271 LOC
**Docstring:** Generic sector-relative price panel for panel mode (GD6a).
**Functions:** `PanelPriceManifest`, `PanelPrices`, `load_panel_prices`, `relative_labels`, `build_label_panel`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `numpy`, `pandas`, `store`

#### `analysis/people_density.py` — 1190 LOC
**Docstring:** People-density features over ``people_events`` (GD5 of the granular-discovery plan).
**Functions:** `UndefinedFeature`, `ImpossibleEvent`, `EventContract`, `ChannelRule`, `DensitySpec`, `events_frame`, `load_events`, `CoverageSpan`, `coverage_mask`, `coverage_change_log`, `density_A`, `channel_count_C`, `signed_S`, `d_self`, `d_peer`, `weekly_decisions`, `sector_weekly_aggregates`, `event_set_sha256`, `membership_sha256`, `lf_sha256`, `code_sha256`, `write_frozen_artifact`, `coverage_sha256`, `build_receipt`
**Reads:** `__future__`, `dataclasses`, `datetime`, `hashlib`, `intelligence`, `json`, `numpy`, `pandas`, `pathlib`, `platform`, `pyarrow`, `scipy`, `sqlalchemy`, `store`, `typing`, `zoneinfo`

#### `analysis/price_admission_fetch.py` — 584 LOC
**Docstring:** Vendor fetches for the VS1 v6 price-admission probe: files only, never the database.
**Functions:** `td_start`, `discovery_vendor_window`, `FetchStopped`, `requests_get`, `api_key`, `sha256_bytes`, `write_gz`, `read_gz`, `FetchLog`, `check_request_window`, `td_symbol`, `td_params`, `td_summary`, `TdUsage`, `td_usage`, `needs_supplement`, `fetch_twelvedata`, `fetch_tiingo_meta`, `load_td_closes`, `load_tiingo_meta`, `fetch_log_sha256`, `tickers_from_file`
**Reads:** `__future__`, `analysis`, `contextlib`, `contextvars`, `dataclasses`, `datetime`, `gzip`, `hashlib`, `json`, `os`, `pathlib`, `requests`, `time`, `typing`

#### `analysis/price_admission_probe.py` — 998 LOC
**Docstring:** GD4 price-admission probe for VS1 v6 (§2.2 rules 3-4, §2.3; research only, read-only).
**Functions:** `preregistered_window`, `probe_rule`, `probe_identity`, `ProbeRefused`, `is_refused_source`, `check_source`, `check_window`, `series_id`, `Row`, `collapse_vintages`, `nice_ratio`, `split_label`, `factor_steps`, `match_td_splits`, `calendar_gaps`, `rows_sha256`, `pull_batch`, `batch_labels`, `interval_counts`, `assess_ticker`, `resolve_source`, `read_rows`, `read_selected`, `read_statuses`, `read_other_sources`, `read_other_source_rows`, `read_td_splits`, `VendorFiles`, `sec_names_by_ticker`, `C1Interval`, `probe_ticker`, `run_probe`, `build_crosscheck_report`, `build_tiingo_meta_report`, `build_report`, `build_manifest`, `file_sha256`, `write_json_once`, `write_outputs`, `Coverage`, `event_coverage`
**Reads:** `__future__`, `analysis`, `contextlib`, `contextvars`, `dataclasses`, `datetime`, `fractions`, `hashlib`, `json`, `math`, `pandas`, `pathlib`, `sqlalchemy`, `store`, `typing`

#### `analysis/prompt_optimizer.py` — 136 LOC
**Docstring:** Prompt feature selection via orthogonality analysis.
**Functions:** `select_prompt_features`, `format_features_for_prompt`
**Reads:** `__future__`, `loguru`, `pandas`, `typing`
**Imported by:** `ollama/market_briefing.py`

#### `analysis/research_agent.py` — 517 LOC
**Docstring:** GRID Research Agent — autonomous intelligence-gathering system.
**Functions:** `analyze_gaps`, `generate_hypotheses`, `research_actor`, `research_sector`, `run_full_research`, `fill_missing_stocks`
**Reads:** `__future__`, `analysis`, `datetime`, `db`, `json`, `llm`, `loguru`, `sqlalchemy`, `store`, `typing`, `yfinance`
**Imported by:** `api/routers/flows.py`, `intelligence/scheduler.py`

#### `analysis/research_forward_log.py` — 1144 LOC
**Docstring:** Prospective forward log for frozen research-loop candidates (S10).
**Functions:** `Refused`, `lf_sha256`, `prereg_file_sha256`, `canonical`, `ForwardLog`, `header_record`, `GitRepo`, `shift_sessions`, `scientific_identity`, `first_decision`, `admit_scan`, `decision_at`, `label_end`, `label_known_at`, `read_prediction`, `read_outcome`, `candidates_state`, `valid_pair`, `resolved`, `one_sided_p`, `verdict`, `run_forward`, `status_report`, `format_status`, `write_status`, `resolve_code_sha`
**Reads:** `__future__`, `analysis`, `contextlib`, `dataclasses`, `datetime`, `hashlib`, `json`, `math`, `numpy`, `os`, `pandas`, `pathlib`, `re`, `scipy`, `subprocess`, `time`, `typing`
**Imported by:** `analysis/gex_intraday_prereg.py`, `analysis/panel_insider_density.py`, `analysis/panel_insider_density_sectors_v2.py`, `analysis/panel_insider_density_sectors_v3.py`, `analysis/panel_insider_density_sectors_v4.py`, `analysis/panel_insider_density_sectors_v5.py`, `analysis/panel_insider_density_sectors_v6.py`, `analysis/panel_insider_density_v2.py`, … (+3)

#### `analysis/research_real_panel.py` — 854 LOC
**Docstring:** Read-only latest-vintage panel adapter for ``analysis.offline_research_proof`` (S09/S09b).
**Functions:** `Publication`, `publication_times`, `SeriesSpec`, `TargetSpec`, `revised`, `refusal`, `proxy_group`, `legs`, `required_proxies`, `self_lag_pairs`, `LatestVintagePanel`, `load_latest_vintage_panel`, `verify_latest_vintage_rows`, `relabel_frozen_candidates`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `math`, `numpy`, `pandas`, `store`
**Imported by:** `analysis/ledger_steered_exploration.py`, `analysis/offline_research_proof.py`, `analysis/research_forward_log.py`

#### `analysis/sec_valuation.py` — 146 LOC
**Docstring:** Trailing P/E from filed SEC EPS and Tiingo's raw close, point in time.
**Functions:** `TrailingPE`, `trailing_eps`, `trailing_pe`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `sqlalchemy`, `store`, `typing`
**Imported by:** `analysis/thesis_scorer.py`, `intelligence/global_levers.py`

#### `analysis/sector_map.py` — 140 LOC
**Docstring:** GRID Sector Map — shim loader for ``analysis/sector_map_data.yaml``.
**Functions:** `get_sector_features`, `get_actor_influence`, `get_all_sectors`, `get_junction_points_for_sector`, `get_junction_point`
**Reads:** `__future__`, `pathlib`, `typing`, `yaml`
**Imported by:** `analysis/flow_aggregator.py`, `analysis/money_flow.py`, `analysis/research_agent.py`, `analysis/taxonomy_audit.py`, `api/routers/actor_detail.py`, `api/routers/capital_flow.py`, `api/routers/contagion.py`, `api/routers/flows.py`, … (+18)

#### `analysis/sector_proxy_rules.py` — 844 LOC
**Docstring:** Declared PROXY / SELF_LAG rules for sector-relative targets (GD7a).
**Functions:** `ProxyRule`, `Target`, `parse_target`, `split_series`, `series_of`, `r6_refusal`, `proxy_rule`, `required_rel_proxies`, `rel_proxy_group`, `family_target`, `rel_self_lag_pairs`, `self_lag_reasons`, `contains_price`, `momentum_features`, `incremental_pvalue`, `momentum_gate`, `gate_selections`, `people_channel`, `Unclassified`, `vote_class`, `rel_catalog`, `pre_inception`, `rules_manifest`, `rules_sha256`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `hashlib`, `json`, `math`, `numpy`, `re`

#### `analysis/taxonomy_audit.py` — 285 LOC
**Docstring:** GRID Taxonomy Audit Engine.
**Functions:** `run_taxonomy_audit`
**Reads:** `__future__`, `analysis`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/system.py`, `intelligence/scheduler.py`

#### `analysis/temporal_feature_eligibility.py` — 385 LOC
**Docstring:** Dormant supplied-row temporal diagnostics. No readers, clocks or I/O.
**Functions:** `Status`, `SourceKind`, `Missing`, `FeatureKey`, `TemporalPolicy`, `Candidate`, `Component`, `Lineage`, `Classification`, `SelectionPolicy`, `SelectionResult`, `classify`, `select_latest_as_of`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`

#### `analysis/thesis_scorer.py` — 2778 LOC
**Docstring:** GRID — Granular Thesis Scoring Engine.
**Functions:** `score_thesis`, `snapshot_thesis`
**Reads:** `__future__`, `analysis`, `datetime`, `inference`, `ingestion`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `time`, `typing`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`

#### `analysis/transfer_entropy.py` — 292 LOC
**Docstring:** CAT-111 — Transfer entropy discovery engine.
**Functions:** `TransferEntropyResult`, `LeadLagScan`, `quantile_discretize`, `transfer_entropy`, `pair_transfer_entropy`, `scan_lead_lag`, `discover_leaders`
**Reads:** `__future__`, `dataclasses`, `math`, `numpy`, `typing`

#### `analysis/viz_intelligence.py` — 563 LOC
**Docstring:** GRID Visualization Intelligence Engine.
**Functions:** `ChartType`, `DataShape`, `RelationType`, `WeightSchedule`, `AnimationConfig`, `VizSpec`, `select_visualization`, `get_all_rules`, `compute_source_weights`
**Reads:** `__future__`, `dataclasses`, `datetime`, `db`, `enum`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/viz.py`

#### `analysis/vol_surface.py` — 1271 LOC
**Docstring:** GRID Vol Surface Engine.
**Functions:** `VolSurfaceEngine`
**Reads:** `__future__`, `datetime`, `db`, `loguru`, `math`, `numpy`, `pandas`, `physics`, `scipy`, `sqlalchemy`, `typing`
**Imported by:** `discovery/options_scanner.py`

## `trading/`

#### `trading/__init__.py` — 0 LOC

#### `trading/circuit_breaker.py` — 275 LOC
**Docstring:** Strategy-level circuit breaker for the signal executor.
**Functions:** `BreakerState`, `StrategyCircuitBreaker`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `enum`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `trading/signal_executor.py`

#### `trading/contagion_to_ticket.py` — 704 LOC
**Docstring:** GRID — Contagion → Dealer Gamma → Options Trade Ticket bridge (adapter).
**Functions:** `ContagionRow`, `write_ticket_to_journal`, `generate_tickets_for_prediction`, `generate_tickets_for_recent_predictions`, `finalize_ticket`
**Reads:** `__future__`, `contracts`, `dataclasses`, `datetime`, `decimal`, `journal`, `json`, `loguru`, `physics`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `api/routers/trade_tickets.py`

#### `trading/hyperliquid.py` — 413 LOC
**Docstring:** Hyperliquid perp trading integration.
**Functions:** `HyperliquidTrader`, `get_hyperliquid_trader`
**Reads:** `__future__`, `config`, `datetime`, `eth_account`, `hyperliquid`, `loguru`, `typing`
**Imported by:** `api/routers/trading.py`

#### `trading/options_recommender.py` — 1691 LOC
**Docstring:** GRID — Options trade recommendation engine.
**Functions:** `compute_kelly_fraction`, `compute_kelly_with_bounds`, `round_to_nickel`, `pick_strike`, `pick_expiry`, `estimate_premium`, `OptionsRecommendation`, `OptionsRecommender`
**Reads:** `__future__`, `api`, `config`, `dataclasses`, `datetime`, `db`, `discovery`, `json`, `loguru`, `math`, `pandas`, `physics`, `re`, `requests`, `scipy`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/options.py`, `api/routers/trading.py`, `intelligence/scheduler.py`, `trading/contagion_to_ticket.py`

#### `trading/options_tracker.py` — 952 LOC
**Docstring:** GRID — Options recommendation outcome tracker and self-improvement loop.
**Functions:** `score_expired_recommendations`, `compute_signal_scores`, `generate_improvement_report`, `update_scanner_weights`, `run_improvement_cycle`, `score_contract_outcome`, `load_digest_track_record`
**Reads:** `__future__`, `datetime`, `json`, `llm`, `loguru`, `sqlalchemy`, `typing`, `yfinance`
**Imported by:** `alerts/hundredx_digest.py`, `api/routers/trading.py`, `intelligence/scheduler.py`

#### `trading/paper_engine.py` — 375 LOC
**Docstring:** GRID Paper Trading Engine.
**Functions:** `PaperTradingEngine`
**Reads:** `__future__`, `datetime`, `governance`, `json`, `loguru`, `sqlalchemy`
**Imported by:** `api/routers/trading.py`, `trading/signal_executor.py`, `trading/solana/executor.py`, `trading/solana/exit_manager.py`

#### `trading/prediction_backtest.py` — 600 LOC
**Docstring:** GRID Prediction Market Backtesting Bridge.
**Functions:** `HypothesisResult`, `export_kalshi_trades`, `export_polymarket_trades`, `export_markets`, `register_strategy`, `BaseStrategy`, `MomentumReversalStrategy`, `MakerFlowStrategy`, `ValueDivergenceStrategy`, `LiquiditySpikeStrategy`, `run_hypothesis`, `list_strategies`, `list_available_markets`
**Reads:** `__future__`, `dataclasses`, `loguru`, `os`, `pandas`, `pathlib`, `sqlalchemy`, `statistics`, `typing`
**Imported by:** `api/routers/prediction_backtest.py`

#### `trading/prediction_markets.py` — 619 LOC
**Docstring:** GRID Prediction Market Integration — Polymarket + Kalshi.
**Functions:** `PolymarketTrader`, `KalshiTrader`
**Reads:** `__future__`, `config`, `httpx`, `loguru`, `py_clob_client`, `time`, `typing`
**Imported by:** `api/routers/trading.py`

#### `trading/prediction_pmxt.py` — 404 LOC
**Docstring:** GRID Unified Prediction Market Trader via pmxt SDK.
**Functions:** `PmxtTrader`
**Reads:** `__future__`, `config`, `loguru`, `pmxt`, `typing`

#### `trading/robinhood.py` — 1385 LOC
**Docstring:** Robinhood Crypto trading integration (official key-signed API).
**Functions:** `normalize_symbol`, `crypto_asset_code`, `cursor_from_next`, `load_signing_key`, `generate_keypair`, `sign_request`, `round_down_to_increment`, `format_quantity`, `format_price`, `deterministic_client_order_id`, `RobinhoodCryptoTrader`, `get_robinhood_trader`
**Reads:** `__future__`, `alerts`, `base64`, `config`, `contextlib`, `cryptography`, `datetime`, `db`, `decimal`, `json`, `loguru`, `re`, `requests`, `sys`, `threading`, `time`, `trading`, `typing`, `urllib`, `uuid`
**Imported by:** `api/routers/system.py`, `api/routers/trading.py`, `trading/signal_executor.py`

#### `trading/robinhood_risk_store.py` — 459 LOC
**Docstring:** Persisted risk state for GRID's exchange connectors (drawdown high-water
**Functions:** `RiskState`, `RiskStore`, `InMemoryRiskStore`, `PostgresRiskStore`, `utc_today`
**Reads:** `__future__`, `contextlib`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `threading`, `typing`
**Imported by:** `trading/robinhood.py`

#### `trading/signal_executor.py` — 515 LOC
**Docstring:** Paper Trading Signal Executor.
**Functions:** `resolve_hold_days`, `VenueTag`, `build_venue_tag`, `execute_signals`
**Reads:** `__future__`, `datetime`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `api/routers/trading.py`, `intelligence/scheduler.py`

#### `trading/solana/__init__.py` — 213 LOC
**Docstring:** Solana trading package.
**Reads:** `__future__`, `trading`

#### `trading/solana/cross_ref.py` — 350 LOC
**Docstring:** Cross-referencer — composite confidence scoring for new Solana launches.
**Functions:** `CrossRefWeights`, `LaunchEvent`, `NarrativeHit`, `NarrativeRegistry`, `ConvergenceProvider`, `CrossRefReport`, `CrossReferencer`
**Reads:** `__future__`, `dataclasses`, `loguru`, `trading`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/fast_entry.py`, `trading/solana/launch_monitor.py`

#### `trading/solana/deployer_registry.py` — 529 LOC
**Docstring:** Deployer track-record registry.
**Functions:** `DeployerScoreWeights`, `DeployerStats`, `DeployerScoreResult`, `score_deployer`, `DeployerRegistry`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `math`, `sqlalchemy`, `trading`
**Imported by:** `ingestion/solana/top_volume.py`, `trading/solana/__init__.py`, `trading/solana/cross_ref.py`

#### `trading/solana/executor.py` — 439 LOC
**Docstring:** Paper-first executor for Solana pipeline decisions.
**Functions:** `ExecutionResult`, `PaperSolanaExecutor`
**Reads:** `__future__`, `dataclasses`, `loguru`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `trading/solana/exit_manager.py`, `trading/solana/fast_entry.py`

#### `trading/solana/exit_decision.py` — 222 LOC
**Docstring:** Pure exit-decision function.
**Functions:** `ExitState`, `ExitAction`, `compute_pnl_pct`, `decide_exit`
**Reads:** `__future__`, `dataclasses`, `datetime`, `trading`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/exit_manager.py`

#### `trading/solana/exit_learner.py` — 284 LOC
**Docstring:** Self-learning exit-policy selector.
**Functions:** `VariantPosterior`, `ExitLearner`
**Reads:** `__future__`, `dataclasses`, `loguru`, `math`, `random`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/exit_manager.py`

#### `trading/solana/exit_manager.py` — 419 LOC
**Docstring:** Exit manager tick loop.
**Functions:** `TickSummary`, `ExitManager`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/executor.py`

#### `trading/solana/exit_policy.py` — 180 LOC
**Docstring:** Exit policy definitions for Solana trading.
**Functions:** `ExitRung`, `ExitPolicy`, `policy_by_id`
**Reads:** `__future__`, `dataclasses`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/exit_decision.py`, `trading/solana/exit_learner.py`, `trading/solana/exit_manager.py`

#### `trading/solana/exit_state.py` — 521 LOC
**Docstring:** Database-backed state for the Solana exit manager and learner.
**Functions:** `PositionStateRow`, `VariantStatsRow`, `ExitStateStore`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/exit_learner.py`, `trading/solana/exit_manager.py`

#### `trading/solana/fast_entry.py` — 218 LOC
**Docstring:** Fast entry path — deterministic LLM-bypass for launch events.
**Functions:** `FastEntryConfig`, `FastEntryResult`, `FastEntryPath`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `trading`
**Imported by:** `trading/solana/__init__.py`

#### `trading/solana/helius_client.py` — 523 LOC
**Docstring:** Helius HTTP client + webhook parser.
**Functions:** `DeployRecord`, `EarlyBuyer`, `WebhookEvent`, `DeployInfoProvider`, `HeliusError`, `HeliusClient`, `parse_webhook_payload`
**Reads:** `__future__`, `dataclasses`, `datetime`, `httpx`, `loguru`, `typing`
**Imported by:** `ingestion/solana/top_volume.py`, `trading/solana/__init__.py`, `trading/solana/deployer_registry.py`, `trading/solana/launch_monitor.py`

#### `trading/solana/jupiter_client.py` — 278 LOC
**Docstring:** Jupiter API client for Solana token pricing and Ultra swaps.
**Functions:** `JupiterError`, `SwapOrder`, `JupiterClient`
**Reads:** `__future__`, `dataclasses`, `httpx`, `json`, `loguru`, `trading`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/executor.py`, `trading/solana/exit_manager.py`, `trading/solana/pipeline.py`, `trading/solana/safety.py`

#### `trading/solana/launch_monitor.py` — 277 LOC
**Docstring:** Real-time launch monitor.
**Functions:** `IngestSummary`, `LaunchMonitor`
**Reads:** `__future__`, `collections`, `dataclasses`, `loguru`, `trading`, `typing`
**Imported by:** `trading/solana/__init__.py`

#### `trading/solana/limits.py` — 183 LOC
**Docstring:** Per-day trade caps for Solana trading.
**Functions:** `LimitConfig`, `LimitDecision`, `DailyLimits`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/executor.py`

#### `trading/solana/pipeline.py` — 325 LOC
**Docstring:** Solana 4-agent trading pipeline.
**Functions:** `PipelineDecision`, `SolanaPipeline`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `llm`, `loguru`, `re`, `trading`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/executor.py`, `trading/solana/fast_entry.py`

#### `trading/solana/safety.py` — 508 LOC
**Docstring:** Safety rails for Solana trading.
**Functions:** `SafetyConfig`, `parse_mint_blocklist`, `SafetyCheck`, `TokenSafetyReport`, `SolanaSafetyChecker`
**Reads:** `__future__`, `dataclasses`, `loguru`, `trading`, `typing`
**Imported by:** `ingestion/solana/top_volume.py`, `trading/solana/__init__.py`, `trading/solana/executor.py`

#### `trading/solana/smart_money.py` — 262 LOC
**Docstring:** Curated smart-money wallet registry.
**Functions:** `SmartMoneyWallet`, `SmartMoneyMatch`, `SmartMoneyMatchSet`, `SmartMoneyRegistry`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/cross_ref.py`

#### `trading/solana/solana_rpc.py` — 314 LOC
**Docstring:** Minimal Solana JSON-RPC client for safety checks.
**Functions:** `SolanaRPCError`, `MintInfo`, `TokenHolder`, `SolanaRPC`, `parse_mint_account`
**Reads:** `__future__`, `base64`, `dataclasses`, `httpx`, `loguru`, `struct`, `time`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/safety.py`

#### `trading/solana/universe.py` — 134 LOC
**Docstring:** Read-only view over ``solana_token_universe``.
**Functions:** `UniverseRank`, `UniverseRankSource`, `UniverseRegistry`, `rank_to_score`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/cross_ref.py`

#### `trading/solana/wallet.py` — 101 LOC
**Docstring:** Solana wallet helper with graceful degradation.
**Functions:** `WalletUnavailableError`, `SolanaWallet`
**Reads:** `__future__`, `base64`, `solders`, `typing`
**Imported by:** `trading/solana/__init__.py`, `trading/solana/jupiter_client.py`

#### `trading/strategy151.py` — 980 LOC
**Docstring:** GRID — Key strategies from Kakushadze & Serur (2018) '151 Trading Strategies'.
**Functions:** `StrategySignal`, `Strategy151Engine`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `statsmodels`, `store`, `typing`

#### `trading/trade_ticket_generator.py` — 553 LOC
**Docstring:** GRID — Trade Ticket Generator.
**Functions:** `TradeTicket`, `compute_invalidation_price`, `compute_target_price`, `kelly_size_from_report`, `compose_thesis`, `compose_invalidation_text`, `compose_evidence_summary`, `generate_ticket`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `math`, `oracle`, `typing`
**Imported by:** `intelligence/decision_gateway.py`

#### `trading/wallet_manager.py` — 374 LOC
**Docstring:** GRID Multi-Wallet Manager (EXCH-04).
**Functions:** `WalletManager`, `resolve_active_wallet`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/routers/trading.py`, `trading/robinhood.py`, `trading/signal_executor.py`

## `oracle/`

#### `oracle/__init__.py` — 0 LOC
**Imported by:** `oracle/astrogrid_insights.py`

#### `oracle/astrogrid_cycle.py` — 156 LOC
**Docstring:** AstroGrid celestial production cycle.
**Functions:** `run_celestial_cycle`
**Reads:** `__future__`, `api`, `datetime`, `loguru`, `typing`

#### `oracle/astrogrid_insights.py` — 383 LOC
**Docstring:** Evidence-backed AstroGrid observations and prospective forecast contracts.
**Functions:** `read_candles`, `read_triggers`, `causal_trigger`, `observe`, `build_payload`, `produce_insights`, `score_contract`, `public_insights`, `answer_from_insights`
**Reads:** `__future__`, `config`, `datetime`, `hashlib`, `json`, `math`, `oracle`, `pathlib`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/astrogrid_predictions.py`, `store/astrogrid.py`

#### `oracle/astrogrid_order_flow.py` — 222 LOC
**Docstring:** Separate, unvalidated 30m spot taker-flow research contract for Astrogrid.
**Functions:** `qualification`, `latest_receipts`, `build_forecast`, `produce_forecasts`, `score_contract`
**Reads:** `__future__`, `binance_close_contract`, `datetime`, `hashlib`, `json`, `math`, `sqlalchemy`, `store`, `typing`
**Imported by:** `store/astrogrid.py`

#### `oracle/astrogrid_universe.py` — 207 LOC
**Docstring:** Canonical AstroGrid scoring universe definitions.
**Functions:** `get_astrogrid_scoreable_universe`, `scoreable_universe_by_symbol`, `enrich_astrogrid_scoreable_universe`
**Reads:** `__future__`, `copy`, `datetime`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `store/astrogrid.py`

#### `oracle/calibration.py` — 1699 LOC
**Docstring:** GRID Oracle Calibration — measures how well predicted probabilities
**Functions:** `CalibrationBucket`, `OracleCalibrationReport`, `compute_calibration`, `update_running_metrics`, `compute_per_horizon_calibration`, `DriftAlert`, `snapshot_calibration_history`, `detect_calibration_drift`, `ScoredOutcome`, `model_family`, `load_scored_outcomes`, `default_block_length`, `block_bootstrap_weights`, `cluster_bootstrap_mean`, `reliability_with_bands`, `Recalibrator`, `fit_recalibrator`, `time_blocks`, `evaluate_holdout`, `causal_aci_intervals`, `exploratory_conformal_interval`, `load_latest_exploratory_state`, `run_exploratory_recalibration`
**Reads:** `__future__`, `bisect`, `collections`, `dataclasses`, `datetime`, `json`, `loguru`, `math`, `numpy`, `oracle`, `sklearn`, `sqlalchemy`, `typing`
**Imported by:** `contracts/handlers/calibration.py`, `intelligence/scheduler.py`, `oracle/engine.py`, `oracle/scoreboard.py`

#### `oracle/citation_extractor.py` — 125 LOC
**Docstring:** GRID — Extract feature citations from LLM output text.
**Functions:** `extract_citations`, `compute_citation_ratio`
**Reads:** `__future__`, `loguru`
**Imported by:** `api/routers/chat.py`

#### `oracle/claim_extractor.py` — 212 LOC
**Docstring:** GRID — Deterministic claim extraction from LLM output text.
**Functions:** `Claim`, `extract_claims`
**Reads:** `__future__`, `dataclasses`, `re`, `typing`
**Imported by:** `oracle/claim_verifier.py`, `oracle/firewall.py`

#### `oracle/claim_verifier.py` — 216 LOC
**Docstring:** GRID — Claim verification against database evidence.
**Functions:** `VerifiedClaim`, `verify_claims`
**Reads:** `__future__`, `dataclasses`, `loguru`, `oracle`, `sqlalchemy`, `typing`
**Imported by:** `oracle/firewall.py`, `oracle/sanity_checker.py`

#### `oracle/contrast_distillation.py` — 298 LOC
**Docstring:** Oracle Contrast-Distillation — ReasoningBank-style strategic lesson extraction.
**Functions:** `ContrastResult`, `compute_divergence`, `distill_contrast`
**Reads:** `__future__`, `dataclasses`, `intelligence`, `json`, `loguru`, `re`, `typing`
**Imported by:** `oracle/engine.py`

#### `oracle/dedup_index.py` — 73 LOC
**Docstring:** Idempotent guard for the oracle_predictions natural-key dedup index.
**Functions:** `ensure_dedup_index`
**Reads:** `__future__`, `schema_guard`, `sqlalchemy`
**Imported by:** `intelligence/obsidian_agent.py`, `oracle/publish.py`

#### `oracle/disagreement.py` — 182 LOC
**Docstring:** ALPHA-10 / task #113 — Ensemble disagreement as a meta-feature.
**Functions:** `DisagreementMetrics`, `directional_entropy`, `confidence_variance`, `disagreement_score`, `compute_metrics`
**Reads:** `__future__`, `dataclasses`, `math`, `typing`
**Imported by:** `oracle/engine.py`

#### `oracle/engine.py` — 3294 LOC
**Docstring:** GRID Oracle Engine — Self-improving prediction loop.
**Functions:** `PredictionType`, `Verdict`, `Signal`, `AntiSignal`, `OraclePrediction`, `OracleModel`, `ModelRegistry`, `oracle_close_universe`, `OracleEngine`, `EnsemblePrediction`, `EnsemblePredictor`
**Reads:** `__future__`, `ast`, `concurrent`, `dataclasses`, `datetime`, `enum`, `hashlib`, `intelligence`, `json`, `llm`, `loguru`, `numpy`, `oracle`, `os`, `schema_guard`, `sqlalchemy`, `store`, `typing`, `zoneinfo`
**Imported by:** `api/routers/oracle.py`, `contracts/handlers/oracle_weights.py`, `contracts/handlers/trade_outcomes.py`, `ingestion/tiingo_pull.py`, `intelligence/decision_gateway.py`, `oracle/calibration.py`, `oracle/forecaster_adapter.py`, `oracle/run_cycle.py`

#### `oracle/entry_price_policy.py` — 103 LOC
**Docstring:** Canonical reasons for an oracle entry price that cannot be divided by.
**Functions:** `entry_price_score_note`, `entry_price_pnl_basis`, `is_divisible_entry_price`
**Reads:** `__future__`, `math`, `typing`
**Imported by:** `api/routers/oracle.py`, `intelligence/postmortem.py`, `oracle/engine.py`, `oracle/publish.py`

#### `oracle/evidence.py` — 198 LOC
**Docstring:** Evidence behind each oracle model, on corrected outcomes.
**Functions:** `Outcome`, `load_outcomes`, `build_evidence`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `random`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/oracle.py`, `intelligence/api_precompute.py`

#### `oracle/feedback_recorder.py` — 79 LOC
**Docstring:** GRID — Record prompt feedback (features available vs cited) for utility scoring.
**Functions:** `record_prompt_feedback`
**Reads:** `__future__`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/chat.py`

#### `oracle/firewall.py` — 161 LOC
**Docstring:** GRID — Publishing firewall: single entry point for claim-level verification.
**Functions:** `FirewallResult`, `verify_output`
**Reads:** `__future__`, `dataclasses`, `json`, `loguru`, `oracle`, `sqlalchemy`
**Imported by:** `api/routers/chat.py`

#### `oracle/forecaster_adapter.py` — 480 LOC
**Docstring:** Oracle ↔ TimesFM Adapter.
**Functions:** `forecast_to_signals`, `forecast_to_anti_signals`, `forecast_to_prediction`, `recent_close_history`, `run_timesfm_forecast_cycle`
**Reads:** `__future__`, `datetime`, `hashlib`, `intelligence`, `loguru`, `numpy`, `oracle`, `sqlalchemy`, `store`, `timeseries`, `typing`
**Imported by:** `oracle/engine.py`

#### `oracle/hallucination_guard.py` — 639 LOC
**Docstring:** GRID Oracle Hallucination Guard — deterministic pre-storage verification layer.
**Functions:** `GuardCheck`, `GuardVerdict`, `verify_predictions`, `guard_summary`
**Reads:** `__future__`, `asyncio`, `config`, `dataclasses`, `functools`, `loguru`, `typing`, `verification`
**Imported by:** `api/routers/oracle.py`, `oracle/engine.py`

#### `oracle/model_evolver.py` — 248 LOC
**Docstring:** GRID Oracle — Model Evolver.
**Functions:** `EvolveResult`, `ModelEvolver`
**Reads:** `__future__`, `dataclasses`, `json`, `loguru`, `random`, `sqlalchemy`, `string`
**Imported by:** `oracle/engine.py`

#### `oracle/model_factory.py` — 348 LOC
**Docstring:** GRID Oracle — Model Factory.
**Functions:** `ModelSpec`, `ModelFactory`, `migrate_default_models`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `loguru`, `oracle`, `os`, `re`, `schema_guard`, `sqlalchemy`, `typing`
**Imported by:** `api/main.py`, `api/routers/signal_registry.py`, `oracle/engine.py`

#### `oracle/prediction_context.py` — 511 LOC
**Docstring:** Oracle prediction context enrichment.
**Functions:** `canonical_regime`, `fetch_vix_level`, `fetch_liquidity_regime`, `fetch_fci_regime`, `aggregate_signal_weights`, `extract_signal_contributions`, `build_prediction_context`, `enrich_signals_payload`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`, `store`, `typing`
**Imported by:** `oracle/engine.py`, `oracle/publish.py`

#### `oracle/pruning_config.py` — 41 LOC
**Docstring:** Prompt pruning system configuration — constants, anchors, thresholds.
**Functions:** `PruningThresholds`
**Reads:** `__future__`, `dataclasses`

#### `oracle/psi_model.py` — 270 LOC
**Docstring:** PSI Oracle — Planetary Stress Index market timing oracle.
**Functions:** `PSISignal`, `evaluate_psi_signals`, `build_astrogrid_prediction_payload`, `run_psi_oracle`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`, `uuid`

#### `oracle/publish.py` — 326 LOC
**Docstring:** Explicit publish contract for comparable AstroGrid oracle records.
**Functions:** `publish_astrogrid_prediction`
**Reads:** `__future__`, `datetime`, `json`, `math`, `oracle`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/oracle.py`, `oracle/publisher_gate.py`

#### `oracle/publisher_gate.py` — 161 LOC
**Docstring:** GRID — Publisher gate: decide publish / review / reject.
**Functions:** `PublishDecision`, `gate_decision`
**Reads:** `__future__`, `dataclasses`, `oracle`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `oracle/firewall.py`

#### `oracle/regime_router.py` — 303 LOC
**Docstring:** ALPHA-13 / task #116 — Per-regime submodel router.
**Functions:** `parse_regime_buckets`, `RegimeRouter`
**Reads:** `__future__`, `intelligence`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/meta_learning_matrix.py`, `oracle/calibration.py`, `oracle/engine.py`

#### `oracle/report.py` — 244 LOC
**Docstring:** Oracle cycle email: concise activity summary, then explicit diagnostic detail.
**Functions:** `build_oracle_report`, `send_oracle_report`
**Reads:** `__future__`, `alerts`, `asyncio`, `config`, `datetime`, `html`, `loguru`, `math`, `re`, `typing`, `verification`
**Imported by:** `oracle/run_cycle.py`

#### `oracle/risk.py` — 446 LOC
**Docstring:** GRID pre-trade risk gate (circuit breaker / kill switch).
**Functions:** `RiskCheckResult`, `CircuitBreakerConfig`, `RiskEvent`, `CircuitBreaker`, `get_global_circuit_breaker`, `reset_global_circuit_breaker`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `typing`
**Imported by:** `trading/trade_ticket_generator.py`

#### `oracle/run_cycle.py` — 62 LOC
**Docstring:** Run one Oracle cycle: score → evolve → predict → report.
**Functions:** `main`
**Reads:** `__future__`, `argparse`, `db`, `json`, `loguru`, `oracle`, `os`, `sys`

#### `oracle/sanity_checker.py` — 290 LOC
**Docstring:** GRID — Deterministic sanity checks on verified claims.
**Functions:** `SanityResult`, `CheckedClaim`, `run_sanity_checks`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `oracle`, `re`, `typing`
**Imported by:** `oracle/firewall.py`, `oracle/publisher_gate.py`

#### `oracle/scoreboard.py` — 255 LOC
**Docstring:** Shared Oracle scoreboard helpers used by GRID and AstroGrid.
**Functions:** `build_oracle_ticker_rollup`, `build_oracle_scoreboard`
**Reads:** `__future__`, `loguru`, `oracle`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `api/routers/chat.py`, `api/routers/oracle.py`

#### `oracle/signal_aggregator.py` — 201 LOC
**Docstring:** GRID Oracle — Signal Aggregator.
**Functions:** `WeightMode`, `WeightConfig`, `AggregatedSignal`, `SignalAggregator`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `math`, `typing`
**Imported by:** `api/routers/signal_registry.py`, `oracle/engine.py`, `oracle/model_factory.py`

#### `oracle/trace_evolver.py` — 801 LOC
**Docstring:** GRID Oracle — Trace-Based Self-Evolution Engine.
**Functions:** `FailurePattern`, `MutationProposal`, `EvolutionCycleResult`, `TraceAnalyzer`, `TargetedMutator`, `EvolutionGate`, `TraceEvolver`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `json`, `loguru`, `random`, `sqlalchemy`, `string`, `typing`
**Imported by:** `oracle/engine.py`

#### `oracle/uncertainty.py` — 388 LOC
**Docstring:** ALPHA-11 / task #114 — Uncertainty bounds + confidence intervals.
**Functions:** `ConfidenceInterval`, `compute_confidence_interval`, `binary_residual_score`, `conformal_quantile`, `ConformalInterval`, `conformal_interval_from_quantile`, `split_conformal_interval`, `AdaptiveConformal`
**Reads:** `__future__`, `collections`, `dataclasses`, `math`, `typing`
**Imported by:** `oracle/calibration.py`, `oracle/engine.py`

## `store/`

#### `store/__init__.py` — 6 LOC
**Docstring:** GRID point-in-time store.
**Imported by:** `analysis/panel_insider_density.py`, `analysis/panel_insider_density_v2.py`, `analysis/panel_prices.py`, `analysis/price_admission_probe.py`, `analysis/research_real_panel.py`, `api/precompute_serving.py`, `intelligence/api_precompute.py`, `intelligence/regime/episode_matcher.py`, … (+1)

#### `store/astrogrid.py` — 3868 LOC
**Docstring:** AstroGrid persistence helpers.
**Functions:** `is_excluded_legacy_score`, `AstroGridStore`
**Reads:** `__future__`, `binance_close_contract`, `collections`, `config`, `datetime`, `json`, `loguru`, `math`, `ollama`, `oracle`, `price_close_contract`, `re`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/dependencies.py`, `oracle/astrogrid_insights.py`, `physics/dealer_gamma.py`

#### `store/availability.py` — 186 LOC
**Docstring:** Availability and provenance contract for analytical outputs.
**Functions:** `Provenance`, `Freshness`, `freshness`, `unavailable`, `available`, `partial`, `is_unavailable`, `measured_or_none`, `mean_of_available`, `CacheDecision`, `cacheable`
**Reads:** `__future__`, `dataclasses`, `datetime`, `math`, `typing`
**Imported by:** `api/precompute_serving.py`, `api/routers/derivatives.py`, `ingestion/schwab_private_store.py`, `ingestion/schwab_quote_path.py`, `intelligence/api_precompute.py`, `ollama/dealer_flow_briefing.py`, `physics/dealer_gamma.py`, `physics/news_energy.py`, … (+1)

#### `store/availability_fields.py` — 247 LOC
**Docstring:** Per-field availability + provenance record (W2b extension of store/availability.py).
**Functions:** `FieldRecord`, `measured_field`, `derived_field`, `modeled_field`, `unavailable_field`, `invalid_field`, `coverage_or_none`
**Reads:** `__future__`, `dataclasses`, `datetime`, `store`, `typing`
**Imported by:** `api/routers/system.py`

#### `store/blob.py` — 224 LOC
**Docstring:** S3-compatible blob store backed by MinIO.
**Functions:** `BlobStore`
**Reads:** `__future__`, `config`, `datetime`, `io`, `loguru`, `minio`
**Imported by:** `api/routers/blob.py`

#### `store/graph.py` — 424 LOC
**Docstring:** Apache AGE graph query wrapper for GRID.
**Functions:** `GraphStore`, `analytics_table`, `get_actor_analytics`, `get_community_members`, `get_top_actors`, `get_community_list`, `get_community_category_mix`, `get_graph_store`
**Reads:** `__future__`, `api`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`, `intelligence/global_levers.py`

#### `store/materialization_provenance.py` — 306 LOC
**Docstring:** Read-only provenance envelope for the tables the flows API reads directly.
**Functions:** `TableSpec`, `table_provenance`, `endpoint_provenance`, `clear_cache`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`, `utils`
**Imported by:** `api/routers/flows.py`

#### `store/observations.py` — 897 LOC
**Docstring:** Vintage-safe, status-aware reads of ``raw_series`` for analytical code.
**Functions:** `MixedSourceError`, `Observation`, `read_window`, `read_latest`, `read_latest_n`, `values`, `PublicationLag`, `read_window_known_at`, `read_daily_policy_n`, `CompletedClose`, `read_completed_closes`, `split_series_id`, `SplitEvent`, `read_splits`, `cumulative_split_factor`, `price_in_basis`, `split_adjusted_return`, `split_jump_confirmed`, `adjust_history_for_splits`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `math`, `numpy`, `pandas`, `price_close_contract`, `sqlalchemy`, `typing`
**Imported by:** `alpha_research/data/panel_builder.py`, `analysis/sec_valuation.py`, `api/routers/ten_year_portfolio.py`, `ingestion/altdata/world_news.py`, `ingestion/tiingo_pull.py`, `ingestion/yfinance_pull.py`, `intelligence/consensus_crowdedness.py`, `intelligence/cot_extremes.py`, … (+10)

#### `store/people_events.py` — 436 LOC
**Docstring:** Typed read/write access to `people_events` (GD2 of the granular-discovery plan).
**Functions:** `PeopleEvent`, `upsert_event`, `read_events`, `read_event_versions`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `sqlalchemy`, `typing`
**Imported by:** `analysis/people_density.py`, `intelligence/people_events_materializer.py`

#### `store/pit.py` — 1116 LOC
**Docstring:** GRID Point-in-Time (PIT) query engine.
**Functions:** `validate_source_snapshot_request`, `retraction_cutoff`, `retractions_table_exists`, `PITStore`, `PITWindowMiss`, `PITWindowCache`
**Reads:** `__future__`, `contextlib`, `datetime`, `db`, `decimal`, `gzip`, `hashlib`, `json`, `loguru`, `math`, `normalization`, `numpy`, `os`, `pandas`, `pathlib`, `sqlalchemy`, `time`, `typing`
**Imported by:** `agents/context.py`, `alpha_research/conviction_scorer.py`, `alpha_research/data/panel_builder.py`, `alpha_research/realized_alpha.py`, `alpha_research/signals/credit_cycle.py`, `alpha_research/strategies/adaptive_rotation.py`, `alpha_research/strategies/rotation_variant_backtest.py`, `api/dependencies.py`, … (+22)

#### `store/precomputed.py` — 381 LOC
**Docstring:** Precomputed API results: one row per (endpoint key, parameter set).
**Functions:** `dumps`, `params_hash`, `PrecomputedRow`, `read_result`, `read_key_rows`, `write_success`, `record_attempt_failure`, `freshness_meta`, `splice_meta`, `log_write_outcome`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `hashlib`, `json`, `loguru`, `math`, `sqlalchemy`, `typing`

#### `store/publication_review_status.py` — 128 LOC
**Docstring:** Freshness of the daily publication review chain, for ``/api/v1/system/health``.
**Functions:** `ReviewChainStatus`, `latest_packet_day`, `review_chain_status`
**Reads:** `__future__`, `dataclasses`, `datetime`, `os`, `pathlib`, `re`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/system.py`

#### `store/snapshots.py` — 664 LOC
**Docstring:** GRID analytical snapshot persistence.
**Functions:** `clear_category_cache`, `ensure_analytical_snapshots_table`, `AnalyticalSnapshotStore`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`, `utils`
**Imported by:** `analysis/research_agent.py`, `api/routers/flows.py`, `api/routers/snapshots.py`, `discovery/clustering.py`, `discovery/orthogonality.py`, `features/alpha101.py`, `intelligence/sleuth.py`, `trading/strategy151.py`

## `alpha_research/`

#### `alpha_research/__init__.py` — 1 LOC
**Docstring:** GRID Alpha Research Engine — Evolutionary factor mining and signal validation.

#### `alpha_research/adapters/__init__.py` — 0 LOC

#### `alpha_research/adapters/signal_adapter.py` — 262 LOC
**Docstring:** Adapter to publish alpha research signals into GRID's SignalRegistry.
**Functions:** `publish_factor_signals`, `publish_regime_signal`, `publish_all_alpha_signals`
**Reads:** `__future__`, `alpha_research`, `datetime`, `intelligence`, `numpy`, `pandas`, `sqlalchemy`, `typing`

#### `alpha_research/conviction_scorer.py` — 673 LOC
**Docstring:** Conviction Scorer — 98% confidence trade detector.
**Functions:** `LayerResult`, `ConvictionReport`, `score_setup`, `score_company`, `score_smart_money`, `score_crowd`, `score_narrative`, `score_flow`, `score_confirmation`, `score_ticker`, `scan_all`, `print_report`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `loguru`, `pandas`, `sqlalchemy`, `store`
**Imported by:** `api/routers/signals.py`

#### `alpha_research/data/__init__.py` — 0 LOC

#### `alpha_research/data/panel_builder.py` — 393 LOC
**Docstring:** Build PIT-correct ticker panel data from GRID's resolved_series.
**Functions:** `rebase_vintages_to_as_of`, `build_price_panel`, `build_volume_panel`, `build_returns_panel`, `get_available_tickers`, `get_vix_series`
**Reads:** `__future__`, `datetime`, `loguru`, `os`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `alpha_research/signals/exposure_scaler.py`, `alpha_research/strategies/adaptive_rotation.py`, `alpha_research/strategies/rotation_variant_backtest.py`

#### `alpha_research/data/shares_tracker.py` — 183 LOC
**Docstring:** Shares outstanding & market cap tracker.
**Functions:** `fetch_daily_fundamentals`, `compute_shares_outstanding`, `detect_dilution_events`, `market_cap_adjusted_return`, `get_dilution_adjusted_price`
**Reads:** `__future__`, `datetime`, `loguru`, `os`, `pandas`, `requests`

#### `alpha_research/data/split_adjuster.py` — 270 LOC
**Docstring:** Heuristic stock split adjuster for price series that carry NO basis metadata.
**Functions:** `detect_splits`, `adjust_splits`, `adjust_panel`, `detect_panel_splits`, `get_post_split_series`, `compute_real_drawdown`
**Reads:** `__future__`, `loguru`, `numpy`, `pandas`
**Imported by:** `alpha_research/conviction_scorer.py`

#### `alpha_research/ensemble.py` — 163 LOC
**Docstring:** LightGBM ensemble for combining alpha research factors.
**Functions:** `EnsembleResult`, `train_ensemble`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `lightgbm`, `loguru`, `pandas`, `typing`

#### `alpha_research/heartbeat.py` — 154 LOC
**Docstring:** Alpha Research Heartbeat — autonomous monitoring job.
**Functions:** `HeartbeatAlert`, `run_heartbeat`, `format_alerts`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`

#### `alpha_research/realized_alpha.py` — 873 LOC
**Docstring:** Realized-alpha truth gate (GRID-4 pivot §8.1, LEVER-PACKAGE §7 T0.1).
**Functions:** `TradeAlpha`, `TradeRow`, `ScoredTrade`, `AlphaWindow`, `normalize_direction`, `price_at`, `trading_days_between`, `compute_trade_alpha`, `window_start`, `score_trades`, `bucket_windows`, `resolve_spy_feature`, `load_price_path`, `load_spy_path`, `mark_open_trade`, `collect_trades`, `compute_realized_alpha`, `persist`, `run_daily`, `fetch_realized_alpha`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `normalization`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/realized_alpha.py`, `intelligence/regime/state_vector.py`, `intelligence/scheduler.py`, `validation/backtest.py`

#### `alpha_research/signals/__init__.py` — 0 LOC

#### `alpha_research/signals/credit_cycle.py` — 165 LOC
**Docstring:** Credit Cycle Detector.
**Functions:** `compute_credit_cycle`
**Reads:** `__future__`, `datetime`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `alpha_research/adapters/signal_adapter.py`

#### `alpha_research/signals/exposure_scaler.py` — 128 LOC
**Docstring:** VIX/MA Continuous Exposure Scalar.
**Functions:** `compute_vix_exposure_scalar`, `compute_vix_exposure_series`
**Reads:** `__future__`, `alpha_research`, `datetime`, `numpy`, `pandas`, `sqlalchemy`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `alpha_research/heartbeat.py`

#### `alpha_research/signals/macro_regime.py` — 187 LOC
**Docstring:** Macro regime signals that enhance cross-sectional alpha.
**Functions:** `vix_regime_signal`, `vix_momentum_signal`, `credit_spread_signal`, `credit_momentum_signal`, `yield_curve_signal`, `financial_stress_signal`, `skew_signal`, `sector_dispersion_signal`, `relative_strength_signal`
**Reads:** `__future__`, `numpy`, `pandas`

#### `alpha_research/signals/quanta_alpha.py` — 250 LOC
**Docstring:** Proven signals from QuantaAlpha research (Saulius.io).
**Functions:** `vol_regime_adaptive_momentum`, `dual_horizon_momentum`, `trend_volume_gate`, `vol_price_divergence`, `vol_regime_adaptive_equity`, `dual_horizon_equity`, `compute_all_signals`, `compute_equity_signals`
**Reads:** `__future__`, `numpy`, `pandas`
**Imported by:** `alpha_research/adapters/signal_adapter.py`

#### `alpha_research/strategies/__init__.py` — 0 LOC
**Imported by:** `alpha_research/strategies/rotation_variant_backtest.py`

#### `alpha_research/strategies/adaptive_rotation.py` — 450 LOC
**Docstring:** Adaptive Rotation Strategy — adapted from FinRL-X.
**Functions:** `RegimeState`, `GroupScore`, `PositionState`, `RotationResult`, `detect_regime`, `score_groups`, `check_stops`, `run_rotation`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `store`, `typing`

#### `alpha_research/strategies/rotation_variant_backtest.py` — 563 LOC
**Docstring:** Walk-forward backtest harness for adaptive_rotation parameter variants.
**Functions:** `RotationConfig`, `patched_constants`, `rotation_pit_cache_enabled`, `rotation_universe`, `rotation_feature_names`, `rotation_window_bounds`, `load_rotation_pit_window`, `snapshot_covers`, `build_rotation_snapshot`, `find_rotation_snapshot`, `backtest_rotation_variant`
**Reads:** `__future__`, `alpha_research`, `argparse`, `contextlib`, `copy`, `dataclasses`, `datetime`, `db`, `hashlib`, `json`, `loguru`, `math`, `numpy`, `os`, `pandas`, `pathlib`, `socket`, `sqlalchemy`, `store`, `typing`

#### `alpha_research/validation/__init__.py` — 0 LOC
**Imported by:** `alpha_research/validation/null_farm.py`, `analysis/ledger_steered_exploration.py`

#### `alpha_research/validation/gauntlet.py` — 519 LOC
**Docstring:** False Discovery Gauntlet — 5 statistical tests to prevent self-deception.
**Functions:** `GauntletResult`, `PermutationResult`, `DeflatedSharpe`, `permutation_block_length`, `permutation_test`, `expected_max_sharpe`, `probabilistic_sharpe_ratio`, `trial_sharpe_variance`, `deflated_sharpe_ratio`, `subsample_stability`, `decay_analysis`, `cv_consistency`, `run_gauntlet`
**Reads:** `__future__`, `alpha_research`, `analysis`, `collections`, `dataclasses`, `math`, `numpy`, `pandas`, `scipy`, `typing`
**Imported by:** `alpha_research/validation/overfitting.py`

#### `alpha_research/validation/metrics.py` — 195 LOC
**Docstring:** Portfolio metrics for alpha research validation.
**Functions:** `rank_ic`, `rank_icir`, `long_short_returns`, `sharpe_ratio`, `annualized_return`, `max_drawdown`, `calmar_ratio`, `turnover`, `compute_signal_metrics`
**Reads:** `__future__`, `numpy`, `pandas`, `scipy`
**Imported by:** `alpha_research/ensemble.py`, `alpha_research/validation/gauntlet.py`, `alpha_research/validation/null_farm.py`

#### `alpha_research/validation/null_farm.py` — 611 LOC
**Docstring:** Null-calibration farm: seeded synthetic worlds through GRID's real tests (S7, 2026-11-06).
**Functions:** `canonical`, `cell_id`, `make_cell`, `make_shards`, `replicate_seed`, `generate`, `world_specs`, `panel_world`, `strategy_returns`, `s09_world`, `replicate`, `run_shard`, `exact_permutation_level`, `rate`, `nominal_level`, `is_null`, `size_status`, `summarize`, `describe`, `markdown_table`
**Reads:** `__future__`, `alpha_research`, `analysis`, `collections`, `hashlib`, `json`, `math`, `numpy`, `pandas`, `scipy`, `typing`

#### `alpha_research/validation/overfitting.py` — 602 LOC
**Docstring:** Overfitting checks over a full trial matrix: CPCV, PBO (CSCV) and the deflated Sharpe.
**Functions:** `OverfitInputError`, `OverfitGateConfig`, `group_bounds`, `label_ends`, `per_period_sharpe`, `validate_matrix`, `matrix_sha256`, `CPCVSplit`, `cpcv_splits`, `cpcv_paths`, `CPCVResult`, `cpcv_selection`, `PBOResult`, `probability_of_backtest_overfitting`, `matrix_deflated_sharpe`, `embargo_rows`, `evaluate_trial_matrix`
**Reads:** `__future__`, `alpha_research`, `analysis`, `collections`, `dataclasses`, `hashlib`, `itertools`, `json`, `math`, `numpy`, `pandas`, `scipy`, `typing`

## `ollama/`

#### `ollama/__init__.py` — 7 LOC
**Docstring:** GRID Ollama integration — local LLM inference for market analysis.

#### `ollama/celestial_briefing.py` — 545 LOC
**Docstring:** AstroGrid Celestial Narrative Synthesis.
**Functions:** `generate_celestial_briefing`, `get_latest_briefing`
**Reads:** `__future__`, `datetime`, `json`, `loguru`, `ollama`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/celestial.py`, `intelligence/scheduler.py`

#### `ollama/client.py` — 659 LOC
**Docstring:** GRID LLM client.
**Functions:** `OllamaClient`, `OpenAIClient`, `get_client`
**Reads:** `__future__`, `config`, `knowledge`, `llamacpp`, `llm`, `loguru`, `os`, `requests`, `time`, `typing`
**Imported by:** `analysis/backtest_scanner.py`, `analysis/capital_flows.py`, `analysis/money_flow.py`, `api/routers/chat.py`, `api/routers/flows.py`, `api/routers/ollama.py`, `api/routers/watchlist_overview.py`, `intelligence/causation_graph.py`, … (+7)

#### `ollama/dealer_flow_briefing.py` — 681 LOC
**Docstring:** DerivativesGrid Dealer Flow Narrative Synthesis.
**Functions:** `valid_spy_gex_profile`, `generate_dealer_flow_briefing`, `get_latest_flow_briefing`
**Reads:** `__future__`, `calendar`, `datetime`, `ingestion`, `json`, `loguru`, `math`, `ollama`, `physics`, `sqlalchemy`, `store`, `typing`, `uuid`, `zoneinfo`
**Imported by:** `api/routers/derivatives.py`, `intelligence/scheduler.py`

#### `ollama/market_briefing.py` — 2012 LOC
**Docstring:** GRID Hourly Market Briefing Engine.
**Functions:** `PublicationTextDigest`, `PublicationGuardReceipt`, `PublicationReceipt`, `MarketBriefingEngine`, `start_hourly_briefings`
**Reads:** `__future__`, `analysis`, `copy`, `dataclasses`, `datetime`, `db`, `hashlib`, `ingestion`, `intelligence`, `json`, `llm`, `loguru`, `math`, `ollama`, `outputs`, `pathlib`, `price_close_contract`, `schedule`, `sqlalchemy`, `store`, `sys`, `time`, `typing`, `uuid`, `zoneinfo`
**Imported by:** `api/routers/briefing.py`, `api/routers/ollama.py`, `intelligence/scheduler.py`

#### `ollama/number_grounding.py` — 1311 LOC
**Docstring:** GRID — Post-generation numeric grounding check for LLM-authored market briefings.
**Functions:** `publication_context_facts`, `annotate_publication_context`, `flow_evidence_kind`, `check_publication_claims`, `NumericMention`, `GroundingResult`, `extract_numeric_mentions`, `check_numbers_grounded`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `loguru`, `math`, `re`, `typing`, `unicodedata`
**Imported by:** `intelligence/audio_briefing.py`, `ollama/market_briefing.py`

#### `ollama/reasoner.py` — 297 LOC
**Docstring:** GRID Ollama-powered reasoning layer.
**Functions:** `OllamaReasoner`
**Reads:** `__future__`, `loguru`, `ollama`, `outputs`, `re`, `typing`
**Imported by:** `api/routers/ollama.py`

#### `ollama/router.py` — 289 LOC
**Docstring:** Dual-LLM task router for GRID.
**Functions:** `TaskComplexity`, `classify_task`, `TaskRouter`, `get_router`
**Reads:** `__future__`, `config`, `enum`, `gemma`, `llamacpp`, `llm`, `loguru`, `ollama`, `re`, `typing`

## `alerts/`

#### `alerts/__init__.py` — 1 LOC
**Docstring:** GRID email alerting subsystem.

#### `alerts/cadence.py` — 297 LOC
**Docstring:** Cadence-aware source staleness.
**Functions:** `Cadence`, `cadence_for_source`, `is_excluded`, `age_hours`, `is_stale`
**Reads:** `__future__`, `dataclasses`, `datetime`

#### `alerts/email.py` — 1210 LOC
**Docstring:** GRID Intelligence — Premium newsletter email system.
**Functions:** `send_alert`, `alert_on_failure`, `alert_on_regime_change`, `alert_on_100x_opportunity`, `send_insight`, `send_agent_report`, `send_weekly_review`, `daily_digest`, `alert_on_failure_with_fix`, `alert_on_transition_leaders`, `alert_on_discovery_insight`, `send_test_email`
**Reads:** `__future__`, `collections`, `config`, `datetime`, `db`, `email`, `html`, `intelligence`, `loguru`, `math`, `re`, `smtplib`, `sqlalchemy`, `threading`, `typing`
**Imported by:** `agents/runner.py`, `alerts/health_alerter.py`, `alerts/hundredx_digest.py`, `alerts/push_notify.py`, `alerts/scheduler.py`, `alerts/supply_chain_alerts.py`, `alerts/waterfall_watch.py`, `api/routers/associations.py`, … (+9)

#### `alerts/health_alerter.py` — 507 LOC
**Docstring:** Health-derived alerting (audit item #31).
**Functions:** `check_and_alert`
**Reads:** `__future__`, `alerts`, `collections`, `dataclasses`, `datetime`, `json`, `loguru`, `os`, `pathlib`, `typing`

#### `alerts/hundredx_digest.py` — 1321 LOC
**Docstring:** GRID 100x Bundled Digest — every 4 hours.
**Functions:** `run_100x_digest`, `schedule_100x_digest`
**Reads:** `__future__`, `alerts`, `argparse`, `config`, `datetime`, `db`, `discovery`, `html`, `intelligence`, `json`, `loguru`, `math`, `numpy`, `pandas`, `re`, `requests`, `signal`, `sqlalchemy`, `threading`, `time`, `trading`, `typing`, `uuid`, `yfinance`

#### `alerts/push_notify.py` — 555 LOC
**Docstring:** GRID Intelligence — Web Push notification system.
**Functions:** `save_subscription`, `remove_subscription`, `get_all_subscriptions`, `get_preferences`, `update_preferences`, `send_push`, `broadcast_push`, `notify_trade_recommendation`, `notify_convergence_alert`, `notify_regime_change`, `notify_red_flag`, `notify_price_alert`, `integrate_with_email_alerts`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `db`, `json`, `loguru`, `pywebpush`, `sqlalchemy`, `threading`, `typing`
**Imported by:** `api/main.py`, `api/routers/notifications.py`, `contracts/handlers/alerts.py`

#### `alerts/scheduler.py` — 256 LOC
**Docstring:** GRID alert scheduler.
**Functions:** `schedule_alerts`, `stop_alerts`
**Reads:** `__future__`, `alerts`, `datetime`, `db`, `json`, `loguru`, `sqlalchemy`, `threading`, `typing`
**Imported by:** `api/main.py`

#### `alerts/supply_chain_alerts.py` — 1024 LOC
**Docstring:** GRID Intelligence — Supply Chain Pulse watchdog.
**Functions:** `Finding`, `detect_new_suppliers`, `detect_concentration_shifts`, `detect_chokepoint_degradation`, `detect_new_high_chokepoints`, `detect_geographic_spikes`, `detect_large_acquisitions`, `detect_contagion_risk`, `refresh_snapshots`, `run_all`, `render_digest_html`, `send_digest`
**Reads:** `__future__`, `alerts`, `dataclasses`, `datetime`, `html`, `intelligence`, `json`, `loguru`, `math`, `sqlalchemy`, `typing`, `urllib`

#### `alerts/waterfall_watch.py` — 243 LOC
**Docstring:** GRID Alerts — Waterfall Watch.
**Functions:** `send_waterfall_alert`, `send_waterfall_alert_if_triggered`, `build_alert_subject`
**Reads:** `__future__`, `alerts`, `datetime`, `html`, `intelligence`, `loguru`, `math`, `typing`
**Imported by:** `intelligence/forced_flow_monitor.py`

## `subnet/`

#### `subnet/distributed_compute.py` — 1445 LOC
**Docstring:** GRID Distributed Compute Engine.
**Functions:** `TaskStatus`, `RewardType`, `MinerIdentity`, `TaskAssignment`, `EarningsSnapshot`, `ComputeCoordinator`, `GPUDetector`, `EdgeMiner`, `ComputeScheduler`, `main`
**Reads:** `__future__`, `api`, `argparse`, `asyncio`, `dataclasses`, `datetime`, `db`, `enum`, `fastapi`, `hashlib`, `json`, `loguru`, `os`, `platform`, `pydantic`, `requests`, `secrets`, `sqlalchemy`, `subnet`, `subprocess`, `sys`, `time`, `typing`
**Imported by:** `api/main.py`, `subnet/oauth_miner.py`

#### `subnet/dynamic_scorer.py` — 329 LOC
**Docstring:** GRID Subnet Dynamic Scoring.
**Functions:** `DynamicScorer`
**Reads:** `__future__`, `hashlib`, `hmac`, `math`, `os`, `re`, `struct`, `time`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/honeypot.py` — 668 LOC
**Docstring:** GRID Subnet Honeypot Calibration System.
**Functions:** `HoneypotInjector`
**Reads:** `__future__`, `hashlib`, `json`, `loguru`, `os`, `random`, `re`, `sqlalchemy`, `typing`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/miner.py` — 257 LOC
**Docstring:** GRID Bittensor Subnet Miner.
**Functions:** `LocalInference`, `GRIDMiner`, `StandaloneMiner`, `main`
**Reads:** `__future__`, `argparse`, `asyncio`, `config`, `loguru`, `os`, `requests`, `time`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/oauth_miner.py` — 568 LOC
**Docstring:** GRID Mobile Miner — OAuth-based task processing via ChatGPT/Copilot/Claude.
**Functions:** `OAuthManager`, `AIProviderRouter`
**Reads:** `__future__`, `api`, `datetime`, `fastapi`, `hashlib`, `loguru`, `os`, `pydantic`, `requests`, `sqlalchemy`, `subnet`, `urllib`
**Imported by:** `api/main.py`

#### `subnet/reputation.py` — 349 LOC
**Docstring:** GRID Subnet Bayesian Reputation System.
**Functions:** `ReputationUpdate`, `BayesianReputation`, `ReputationManager`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `math`, `sqlalchemy`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/semantic_scorer.py` — 750 LOC
**Docstring:** GRID Subnet Semantic Scorer.
**Functions:** `SemanticScorer`
**Reads:** `__future__`, `json`, `loguru`, `numpy`, `os`, `re`, `sentence_transformers`, `sklearn`, `sqlalchemy`, `sys`, `typing`, `urllib`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/stake_verifier.py` — 294 LOC
**Docstring:** GRID Subnet Stake Verification.
**Functions:** `StakeVerifier`
**Reads:** `__future__`, `datetime`, `hashlib`, `loguru`, `os`, `requests`, `sqlalchemy`, `time`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/sybil_detector.py` — 312 LOC
**Docstring:** GRID Subnet Sybil Detection.
**Functions:** `BehavioralProfile`, `SybilDetector`
**Reads:** `__future__`, `collections`, `datetime`, `json`, `loguru`, `math`, `sqlalchemy`, `statistics`, `time`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/validator.py` — 409 LOC
**Docstring:** GRID Bittensor Subnet Validator.
**Functions:** `TaskDistributor`, `ResponseScorer`, `ResultStore`, `GRIDValidator`, `main`
**Reads:** `__future__`, `argparse`, `asyncio`, `datetime`, `db`, `intelligence`, `json`, `llm`, `loguru`, `os`, `re`, `sqlalchemy`, `sys`, `typing`

## `physics/`

#### `physics/__init__.py` — 9 LOC
**Docstring:** GRID market physics framework.

#### `physics/conventions.py` — 349 LOC
**Docstring:** GRID financial convention locking system.
**Functions:** `Convention`, `get_convention`, `validate_convention`, `validate_feature_set`, `check_unit_compatibility`, `list_conventions`
**Reads:** `__future__`, `dataclasses`, `typing`
**Imported by:** `api/routers/physics.py`, `physics/verify.py`

#### `physics/dealer_flow/__init__.py` — 48 LOC
**Docstring:** GRID — physics.dealer_flow subpackage.
**Reads:** `__future__`, `physics`

#### `physics/dealer_flow/adapters/__init__.py` — 18 LOC
**Docstring:** GRID — physics.dealer_flow.adapters subpackage.
**Reads:** `__future__`, `physics`

#### `physics/dealer_flow/adapters/base.py` — 77 LOC
**Docstring:** GRID — Abstract venue-adapter base class for dealer_flow (GEX V2 §8).
**Functions:** `VenueAdapter`
**Reads:** `__future__`, `abc`, `physics`, `typing`
**Imported by:** `physics/dealer_flow/__init__.py`, `physics/dealer_flow/adapters/__init__.py`, `physics/dealer_flow/adapters/deribit.py`

#### `physics/dealer_flow/adapters/deribit.py` — 85 LOC
**Docstring:** GRID — Deribit venue adapter for the dealer_flow subpackage (GEX V2 §8.2).
**Functions:** `DeribitAdapter`
**Reads:** `__future__`, `ccxt`, `physics`, `typing`
**Imported by:** `physics/dealer_flow/__init__.py`, `physics/dealer_flow/adapters/__init__.py`

#### `physics/dealer_flow/confidence.py` — 46 LOC
**Docstring:** GRID — Per-contract confidence scoring for dealer_flow (GEX V2 §12).
**Functions:** `score_contract`
**Reads:** `__future__`, `typing`
**Imported by:** `physics/dealer_flow/__init__.py`

#### `physics/dealer_flow/exposures.py` — 84 LOC
**Docstring:** GRID — Dealer-flow exposure aggregators (GEX V2 §6, §11).
**Functions:** `dealer_gex`, `dealer_vanna`, `dealer_charm`
**Reads:** `__future__`, `physics`, `typing`
**Imported by:** `physics/dealer_flow/__init__.py`

#### `physics/dealer_flow/pipeline.py` — 75 LOC
**Docstring:** GRID — Orchestrator for the dealer_flow pipeline (GEX V2 §4).
**Functions:** `run`
**Reads:** `__future__`, `typing`
**Imported by:** `physics/dealer_flow/__init__.py`, `trading/contagion_to_ticket.py`

#### `physics/dealer_flow/schemas.py` — 147 LOC
**Docstring:** GRID — Normalized schemas for the dealer_flow subpackage (GEX V2 §5).
**Functions:** `OptionContract`, `OptionSnapshot`, `OptionExposure`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `physics/dealer_flow/__init__.py`, `physics/dealer_flow/adapters/base.py`, `physics/dealer_flow/adapters/deribit.py`

#### `physics/dealer_gamma.py` — 897 LOC
**Docstring:** GRID — Dealer gamma exposure and hedging flow mechanics.
**Functions:** `bs_gamma`, `bs_delta_call`, `bs_delta_put`, `bs_vanna`, `bs_charm`, `DealerGammaEngine`
**Reads:** `__future__`, `datetime`, `ingestion`, `itertools`, `loguru`, `math`, `numpy`, `pandas`, `physics`, `sqlalchemy`, `store`, `typing`, `uuid`, `zoneinfo`
**Imported by:** `analysis/vol_surface.py`, `api/routers/chat.py`, `api/routers/derivatives.py`, `api/routers/flows.py`, `api/routers/intelligence_risk.py`, `discovery/options_scanner.py`, `intelligence/forced_flow_monitor.py`, `ollama/dealer_flow_briefing.py`, … (+2)

#### `physics/greeks/__init__.py` — 35 LOC
**Docstring:** GRID — physics/greeks package.
**Reads:** `physics`
**Imported by:** `physics/dealer_flow/exposures.py`, `physics/dealer_gamma.py`

#### `physics/greeks/black_scholes.py` — 565 LOC
**Docstring:** GRID — Vectorized Black-Scholes Greek primitives (crypto + equity shared).
**Functions:** `d1`, `d2`, `delta`, `gamma`, `vanna`, `charm`, `vomma`, `speed`, `color`, `zomma`
**Reads:** `__future__`, `math`, `numpy`
**Imported by:** `physics/greeks/__init__.py`

#### `physics/momentum.py` — 549 LOC
**Docstring:** GRID news momentum analysis.
**Functions:** `MomentumResult`, `NewsMomentumAnalyzer`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `pandas`, `scipy`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/flows.py`, `api/routers/physics.py`, `physics/verify.py`

#### `physics/news_energy.py` — 669 LOC
**Docstring:** GRID physics — News Energy Decomposition Engine.
**Functions:** `NewsEnergyEngine`
**Reads:** `__future__`, `datetime`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/flows.py`, `api/routers/physics.py`

#### `physics/transforms.py` — 626 LOC
**Docstring:** GRID physics-inspired market transforms.
**Functions:** `kinetic_energy`, `potential_energy`, `total_energy`, `market_temperature`, `entropy_rate`, `phase_velocity`, `estimate_ou_parameters`, `ou_mean_reversion_signal`, `ou_displacement`, `langevin_drift`, `langevin_diffusion`, `fokker_planck_density`, `relaxation_time`, `half_life`, `hurst_exponent`, `rolling_hurst`
**Reads:** `__future__`, `loguru`, `numpy`, `pandas`, `scipy`, `sklearn`
**Imported by:** `api/routers/physics.py`, `features/lab.py`

#### `physics/verify.py` — 845 LOC
**Docstring:** GRID market physics verification layer.
**Functions:** `VerificationResult`, `MarketPhysicsVerifier`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `physics`, `sqlalchemy`, `statsmodels`, `store`, `typing`
**Imported by:** `api/routers/physics.py`

#### `physics/waves.py` — 252 LOC
**Docstring:** GRID wave-based pipeline execution.
**Functions:** `WaveTask`, `build_execution_waves`, `execute_waves`, `build_grid_pipeline_waves`
**Reads:** `__future__`, `concurrent`, `dataclasses`, `loguru`, `time`, `typing`
**Imported by:** `api/routers/workflows.py`

## `features/`

#### `features/__init__.py` — 6 LOC
**Docstring:** GRID feature engineering layer.

#### `features/alpha101.py` — 712 LOC
**Docstring:** GRID WorldQuant 101 Formulaic Alphas engine.
**Functions:** `ts_sum`, `sma`, `stddev`, `correlation`, `covariance`, `ts_rank`, `ts_min`, `ts_max`, `delta`, `delay`, `rank`, `scale`, `ts_argmax`, `ts_argmin`, `decay_linear`, `product`, `signed_power`, `Alpha101Engine`
**Reads:** `__future__`, `datetime`, `loguru`, `numpy`, `pandas`, `scipy`, `sqlalchemy`, `store`, `typing`

#### `features/importance.py` — 1134 LOC
**Docstring:** GRID feature importance tracking module.
**Functions:** `FeatureImportanceTracker`
**Reads:** `__future__`, `datetime`, `loguru`, `numpy`, `pandas`, `scipy`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/model_comparison.py`, `api/routers/models.py`

#### `features/lab.py` — 1243 LOC
**Docstring:** GRID feature transformation engine.
**Functions:** `zscore_normalize`, `rolling_slope`, `pct_change_lagged`, `ratio`, `spread`, `FeatureLab`, `clear_cache`, `compute_sector_percentiles`, `compute_all_percentiles`, `get_percentile`
**Reads:** `__future__`, `analysis`, `datetime`, `db`, `loguru`, `numpy`, `pandas`, `physics`, `scipy`, `sqlalchemy`, `store`, `tsfresh`, `typing`, `utils`
**Imported by:** `api/routers/capital_flow.py`, `api/routers/physics.py`, `inference/live.py`

#### `features/per_signal_brier.py` — 458 LOC
**Docstring:** Per-signal per-horizon Brier tracker (ALPHA-15 / #118).
**Functions:** `SignalScorecard`, `ensure_tables`, `record_scored_prediction`, `compute_conviction_weight`, `get_signal_scorecard`, `rank_signals_by_horizon`, `get_full_scorecard_table`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `features/regime_conditional_brier.py`, `intelligence/confidence_bucket_tracker.py`, `intelligence/counterfactual_stress.py`, `intelligence/meta_learning_matrix.py`, `intelligence/signal_provenance.py`

#### `features/regime_conditional_brier.py` — 810 LOC
**Docstring:** Regime-conditional calibration sibling to features/per_signal_brier.py.
**Functions:** `ensure_regime_brier_table`, `record_scored_prediction`, `get_regime_conditional_scorecard`, `get_scorecard_with_regime_fallback`, `rank_signals_by_regime`, `bootstrap_from_oracle_predictions`
**Reads:** `__future__`, `datetime`, `features`, `json`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/null_hypothesis_forecaster.py`, `intelligence/signal_provenance.py`

#### `features/registry.py` — 166 LOC
**Docstring:** GRID feature registry query interface.
**Functions:** `FeatureRegistry`
**Reads:** `__future__`, `datetime`, `db`, `loguru`, `pandas`, `sqlalchemy`, `typing`

## `normalization/`

#### `normalization/__init__.py` — 6 LOC
**Docstring:** GRID normalization layer.

#### `normalization/entity_map.py` — 1918 LOC
**Docstring:** GRID entity mapping module.
**Functions:** `validate_selected_request`, `series_family`, `is_feature_shaped`, `canonical_feature_name`, `price_basis_conflicts`, `static_feeds`, `pattern_collision`, `yf_ticker_stems`, `yf_candidate_feature_names`, `EntityMap`
**Reads:** `__future__`, `datetime`, `db`, `difflib`, `loguru`, `re`, `sqlalchemy`, `typing`
**Imported by:** `alpha_research/realized_alpha.py`, `api/routers/watchlist_helpers.py`, `ingestion/sec_velocity.py`, `intelligence/resolution_audit.py`, `intelligence/source_audit.py`, `normalization/resolver.py`, `store/pit.py`

#### `normalization/resolver.py` — 1439 LOC
**Docstring:** GRID conflict resolution module.
**Functions:** `Resolver`, `main`
**Reads:** `__future__`, `argparse`, `binance_close_contract`, `concurrent`, `datetime`, `db`, `decimal`, `json`, `loguru`, `math`, `normalization`, `pandas`, `price_close_contract`, `sqlalchemy`, `threading`, `time`, `typing`
**Imported by:** `intelligence/resolution_audit.py`

## `discovery/`

#### `discovery/__init__.py` — 6 LOC
**Docstring:** GRID discovery engine.

#### `discovery/changepoint_detector.py` — 240 LOC
**Docstring:** AutoBNN-powered changepoint detection for GRID's discovery pipeline.
**Functions:** `ChangeReport`, `scan_for_changepoints`, `publish_regime_signals`, `run_changepoint_cycle`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `sqlalchemy`, `time`, `timeseries`, `typing`

#### `discovery/clustering.py` — 752 LOC
**Docstring:** GRID cluster discovery engine.
**Functions:** `ClusterDiscovery`
**Reads:** `__future__`, `datetime`, `db`, `discovery`, `hyperspace`, `json`, `loguru`, `matplotlib`, `numpy`, `os`, `pandas`, `pathlib`, `scipy`, `sklearn`, `sqlalchemy`, `store`, `tempfile`, `typing`
**Imported by:** `api/routers/discovery.py`, `api/routers/regime.py`

#### `discovery/matrix_guard.py` — 72 LOC
**Docstring:** Staleness guard for wide PIT feature matrices.
**Functions:** `last_valid_dates`, `drop_stale_columns`, `matrix_window`
**Reads:** `__future__`, `datetime`, `pandas`, `typing`
**Imported by:** `api/routers/associations.py`, `api/routers/discovery.py`, `discovery/clustering.py`, `discovery/orthogonality.py`

#### `discovery/options_scanner.py` — 1160 LOC
**Docstring:** GRID — Options mispricing scanner.
**Functions:** `MispricingOpportunity`, `OptionsScanner`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `db`, `json`, `loguru`, `pandas`, `physics`, `sqlalchemy`, `typing`
**Imported by:** `alerts/hundredx_digest.py`, `api/routers/derivatives.py`, `ingestion/scheduler.py`, `intelligence/api_precompute.py`, `trading/options_recommender.py`

#### `discovery/orthogonality.py` — 641 LOC
**Docstring:** GRID orthogonality audit module.
**Functions:** `OrthogonalityAudit`
**Reads:** `__future__`, `datetime`, `db`, `discovery`, `hyperspace`, `loguru`, `matplotlib`, `numpy`, `os`, `pandas`, `pathlib`, `seaborn`, `sklearn`, `sqlalchemy`, `store`, `typing`
**Imported by:** `api/routers/discovery.py`, `discovery/smart_heatmap.py`

#### `discovery/smart_heatmap.py` — 170 LOC
**Docstring:** Smart heatmap payloads (``GET /api/v1/discovery/smart-heatmap``).
**Functions:** `heatmap_payload`, `build_smart_heatmap_payloads`
**Reads:** `__future__`, `datetime`, `discovery`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/api_precompute.py`

## `contracts/`

#### `contracts/__init__.py` — 22 LOC
**Docstring:** GRID contracts infrastructure.
**Reads:** `__future__`, `contracts`
**Imported by:** `api/routers/contracts.py`, `contracts/dispatcher.py`, `contracts/emit.py`

#### `contracts/channels.py` — 34 LOC
**Docstring:** Contract-type → event-bus channel mapping.
**Functions:** `channel_for`, `contract_for_channel`
**Reads:** `__future__`, `contracts`, `re`
**Imported by:** `api/routers/sse.py`, `contracts/dispatcher.py`, `contracts/emit.py`

#### `contracts/correlation.py` — 41 LOC
**Docstring:** Correlation id propagation for the contracts layer.
**Functions:** `new_correlation_id`, `get_current_correlation_id`, `correlation_scope`
**Reads:** `__future__`, `contextlib`, `contextvars`, `typing`, `uuid`
**Imported by:** `contracts/__init__.py`, `contracts/emit.py`, `intelligence/chain_contagion.py`, `intelligence/contagion_backtest.py`, `intelligence/fundamental_divergence.py`, `intelligence/holder_deal_overlap.py`, `intelligence/news_contagion_listener.py`, `intelligence/postmortem.py`, … (+3)

#### `contracts/dead_letter.py` — 161 LOC
**Docstring:** Dead-letter store for the contracts layer.
**Functions:** `DeadLetterEntry`, `record_failure`, `pending_retries`, `mark_resolved`, `bump_retry`, `schedule_next_retry`
**Reads:** `__future__`, `dataclasses`, `datetime`, `json`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/main.py`, `contracts/replay.py`, `contracts/retry_scheduler.py`

#### `contracts/dispatcher.py` — 159 LOC
**Docstring:** Contract dispatcher.
**Functions:** `Dispatcher`
**Reads:** `__future__`, `concurrent`, `contracts`, `loguru`, `pydantic`, `threading`, `time`, `typing`, `uuid`
**Imported by:** `api/main.py`, `contracts/__init__.py`

#### `contracts/emit.py` — 227 LOC
**Docstring:** Emit helpers for the contracts layer.
**Functions:** `notify_payload`, `emit`, `pull_lifecycle`
**Reads:** `__future__`, `api`, `contextlib`, `contracts`, `events`, `hashlib`, `json`, `loguru`, `sqlalchemy`, `time`, `typing`, `uuid`
**Imported by:** `contracts/__init__.py`, `intelligence/chain_contagion.py`, `intelligence/contagion_backtest.py`, `intelligence/fundamental_divergence.py`, `intelligence/holder_deal_overlap.py`, `intelligence/news_contagion_listener.py`, `intelligence/postmortem.py`, `intelligence/supply_chain_edge_validator.py`, … (+2)

#### `contracts/handlers/__init__.py` — 11 LOC
**Docstring:** Phase 2 contract handlers.

#### `contracts/handlers/alerts.py` — 158 LOC
**Docstring:** Operator-alert handlers (SYNTH-31 / Wave-D, §7.3 closure).
**Functions:** `on_cross_reference_anomaly`, `on_regime_transition`
**Reads:** `__future__`, `alerts`, `contracts`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/calibration.py` — 111 LOC
**Docstring:** Calibration metrics handler (SYNTH-21).
**Functions:** `on_prediction_scored`, `on_options_trade_outcome`
**Reads:** `__future__`, `contracts`, `loguru`, `oracle`, `sqlalchemy`, `typing`

#### `contracts/handlers/edges.py` — 93 LOC
**Docstring:** Edge-validation handler (SYNTH-39).
**Functions:** `on_edge_validated`
**Reads:** `__future__`, `contracts`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/journal.py` — 231 LOC
**Docstring:** Provisional decision_journal handler (SYNTH-42).
**Functions:** `on_signal_fired`, `on_prediction_scored`
**Reads:** `__future__`, `contracts`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/oracle_anti_signals.py` — 163 LOC
**Docstring:** Oracle anti-signal handler (SYNTH-28 / SYNTH-32 follow-up).
**Functions:** `on_cross_reference_anomaly`
**Reads:** `__future__`, `contracts`, `datetime`, `decimal`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/oracle_regime.py` — 120 LOC
**Docstring:** Oracle regime handler (SYNTH-30 closure, §7.3).
**Functions:** `on_regime_transition`
**Reads:** `__future__`, `contracts`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/oracle_signals.py` — 126 LOC
**Docstring:** Oracle SignalFired fanout handler (SYNTH-24..27).
**Functions:** `on_signal_fired`
**Reads:** `__future__`, `contracts`, `datetime`, `json`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/oracle_weights.py` — 59 LOC
**Docstring:** Oracle weight evolver handlers (SYNTH-20, SYNTH-23).
**Functions:** `on_prediction_scored`, `on_postmortem_completed`
**Reads:** `__future__`, `contracts`, `loguru`, `oracle`, `sqlalchemy`, `typing`

#### `contracts/handlers/pull_lifecycle.py` — 37 LOC
**Docstring:** Pull lifecycle contract handler.
**Functions:** `on_pull_lifecycle`
**Reads:** `__future__`, `contracts`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/handlers/trade_outcomes.py` — 102 LOC
**Docstring:** Options trade outcome handler (SYNTH-40).
**Functions:** `on_options_trade_outcome`
**Reads:** `__future__`, `contracts`, `loguru`, `oracle`, `sqlalchemy`, `typing`

#### `contracts/handlers/trust.py` — 249 LOC
**Docstring:** Trust scorer handlers for dispatched contract events.
**Functions:** `on_prediction_scored`, `on_postmortem_completed`, `on_signal_fired`, `on_edge_validated`
**Reads:** `__future__`, `contracts`, `intelligence`, `loguru`, `sqlalchemy`, `typing`

#### `contracts/observability.py` — 107 LOC
**Docstring:** In-process contracts metrics.
**Functions:** `emitted`, `dispatched`, `failed`, `record_duration`, `snapshot`, `reset`, `render_prometheus`
**Reads:** `__future__`, `collections`, `threading`, `typing`

#### `contracts/replay.py` — 139 LOC
**Docstring:** Manual replay for dead-letter entries.
**Functions:** `replay_entry`, `replay_many`, `replay_filtered`, `build_parser`, `main`
**Reads:** `__future__`, `api`, `argparse`, `contracts`, `json`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imported by:** `api/routers/contracts.py`

#### `contracts/retry_scheduler.py` — 81 LOC
**Docstring:** Background retry scheduler for dead-letter entries.
**Functions:** `RetryScheduler`
**Reads:** `__future__`, `contracts`, `loguru`, `threading`, `typing`
**Imported by:** `api/main.py`

#### `contracts/router.py` — 109 LOC
**Docstring:** Contract routing table.
**Functions:** `resolve_handler`
**Reads:** `__future__`, `contracts`, `importlib`, `typing`
**Imported by:** `contracts/dispatcher.py`, `contracts/replay.py`, `contracts/retry_scheduler.py`

#### `contracts/schemas.py` — 228 LOC
**Docstring:** Contract schemas for the GRID information-flow layer.
**Functions:** `SignalRef`, `BaseContract`, `PostmortemCompleted`, `PredictionScored`, `BacktestGateVerdict`, `OptionsTradeOutcome`, `CrossReferenceAnomaly`, `LeverageRiskUpdate`, `RegimeTransition`, `SignalFired`, `HypothesisGenerated`, `ActorMaterialized`, `PullLifecycle`, `ForensicsTrace`, `InvestigationProgress`, `EdgeValidated`
**Reads:** `__future__`, `datetime`, `decimal`, `pydantic`, `typing`, `uuid`
**Imported by:** `contracts/channels.py`, `contracts/dispatcher.py`, `contracts/emit.py`, `contracts/handlers/alerts.py`, `contracts/handlers/calibration.py`, `contracts/handlers/edges.py`, `contracts/handlers/journal.py`, `contracts/handlers/oracle_anti_signals.py`, … (+18)

## `gemma/`

#### `gemma/__init__.py` — 17 LOC
**Docstring:** GRID Gemma 3 integration.
**Reads:** `gemma`

#### `gemma/client.py` — 510 LOC
**Docstring:** GRID Gemma 4 27B QAT client.
**Functions:** `GemmaClient`, `get_client`
**Reads:** `__future__`, `config`, `knowledge`, `llm`, `loguru`, `requests`, `time`, `typing`
**Imported by:** `gemma/__init__.py`, `ollama/router.py`

#### `gemma/micro.py` — 533 LOC
**Docstring:** GRID Gemma 4 — Task-Specific Fine-Tuned Models.
**Functions:** `MicroModelConfig`, `GemmaMicroClient`, `GemmaMicroPool`, `get_micro_pool`
**Reads:** `__future__`, `config`, `dataclasses`, `llm`, `loguru`, `requests`, `threading`, `time`, `typing`
**Imported by:** `ingestion/altdata/edgar_transcripts.py`, `ingestion/signal_classifier.py`

#### `gemma/training/__init__.py` — 20 LOC
**Docstring:** GRID Gemma Training — Fine-tune Gemma 4 / Gemma 3 for GRID-specific tasks.
**Reads:** `gemma`

#### `gemma/training/config.py` — 281 LOC
**Docstring:** Training configuration for GRID Gemma fine-tuning.
**Functions:** `TaskType`, `LoRAConfig`, `TrainingConfig`, `get_preset_config`
**Reads:** `__future__`, `dataclasses`, `enum`, `pathlib`
**Imported by:** `gemma/training/__init__.py`, `gemma/training/datasets.py`, `gemma/training/train.py`

#### `gemma/training/datasets.py` — 688 LOC
**Docstring:** Dataset generators for GRID Gemma fine-tuning.
**Functions:** `build_signal_classifier_dataset`, `build_anomaly_narrator_dataset`, `build_edgar_extractor_dataset`, `build_knowledge_mapper_dataset`, `build_hermes_operator_dataset`, `build_dataset`, `save_dataset_jsonl`, `load_dataset_for_training`
**Reads:** `__future__`, `datasets`, `gemma`, `json`, `pathlib`, `random`, `typing`
**Imported by:** `gemma/training/train.py`

#### `gemma/training/train.py` — 592 LOC
**Docstring:** GRID Gemma Fine-Tuning with Unsloth.
**Functions:** `train`, `export_gguf`, `merge_and_save`, `test_inference`, `main`
**Reads:** `__future__`, `argparse`, `gemma`, `loguru`, `pathlib`, `torch`, `transformers`, `trl`, `typing`, `unsloth`

## `inference/`

#### `inference/__init__.py` — 6 LOC
**Docstring:** GRID inference layer.

#### `inference/calibration.py` — 424 LOC
**Docstring:** GRID probability calibration scoring.
**Functions:** `CalibrationReport`, `CalibrationScorer`
**Reads:** `__future__`, `dataclasses`, `loguru`, `numpy`, `pandas`, `typing`
**Imported by:** `inference/tuning.py`

#### `inference/kv_cache_manager.py` — 151 LOC
**Docstring:** KV Cache Manager — transparent compress/decompress lifecycle for TurboQuant.
**Functions:** `CacheMetrics`, `KVCacheManager`
**Reads:** `__future__`, `dataclasses`, `inference`, `loguru`, `numpy`, `time`, `typing`
**Imported by:** `inference/live.py`

#### `inference/live.py` — 450 LOC
**Docstring:** GRID live inference module.
**Functions:** `LiveInference`
**Reads:** `__future__`, `config`, `datetime`, `db`, `features`, `inference`, `intelligence`, `json`, `loguru`, `pandas`, `sqlalchemy`, `store`, `typing`
**Imported by:** `agents/context.py`, `api/routers/flows.py`, `api/routers/signals.py`

#### `inference/timesfm_service.py` — 520 LOC
**Docstring:** GRID — TimesFM Forecasting Service.
**Functions:** `SignalForecast`, `forecast_signals`, `get_forecast`, `get_forecasts_by_family`, `get_forecast_summary`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `numpy`, `sqlalchemy`, `time`, `timeseries`, `typing`
**Imported by:** `analysis/thesis_scorer.py`, `timeseries/timesfm_forecaster.py`

#### `inference/trained_models.py` — 284 LOC
**Docstring:** Trained model abstractions for GRID inference.
**Functions:** `TrainedModelBase`, `GradientBoostingRegimeClassifier`, `RandomForestRegimeClassifier`, `RuleBasedClassifier`
**Reads:** `__future__`, `abc`, `datetime`, `hashlib`, `joblib`, `loguru`, `numpy`, `pandas`, `pathlib`, `sklearn`, `xgboost`
**Imported by:** `inference/live.py`

#### `inference/tuning.py` — 479 LOC
**Docstring:** GRID strategy parameter tuning.
**Functions:** `BacktestResult`, `TuningResult`, `StrategyTuner`
**Reads:** `__future__`, `dataclasses`, `inference`, `itertools`, `loguru`, `numpy`, `pandas`, `sklearn`, `typing`, `validation`

#### `inference/turboquant.py` — 270 LOC
**Docstring:** TurboQuant — KV Cache Quantization (arXiv:2504.19874).
**Functions:** `CompressedKV`, `get_rotation`, `get_codebook`, `quantize_kv`, `dequantize_kv`, `compression_ratio`, `distortion`
**Reads:** `__future__`, `dataclasses`, `numpy`
**Imported by:** `inference/kv_cache_manager.py`

## `agents/`

#### `agents/__init__.py` — 7 LOC
**Docstring:** GRID TradingAgents integration.

#### `agents/adapter.py` — 175 LOC
**Docstring:** Adapter between TradingAgents output and GRID's decision journal.
**Functions:** `parse_agent_decision`, `compute_conviction_score`
**Reads:** `__future__`, `json`, `loguru`, `numpy`, `typing`
**Imported by:** `agents/runner.py`

#### `agents/backtest.py` — 203 LOC
**Docstring:** Agent decision backtesting.
**Functions:** `AgentBacktester`
**Reads:** `__future__`, `config`, `loguru`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/agents.py`

#### `agents/config.py` — 152 LOC
**Docstring:** TradingAgents LLM provider configuration.
**Functions:** `scale_debate_rounds`, `build_agent_config`
**Reads:** `__future__`, `config`, `llm`, `loguru`, `os`, `typing`
**Imported by:** `agents/runner.py`

#### `agents/context.py` — 114 LOC
**Docstring:** GRID context builder for TradingAgents.
**Functions:** `GRIDContext`
**Reads:** `__future__`, `config`, `datetime`, `inference`, `loguru`, `sqlalchemy`, `store`, `typing`
**Imported by:** `agents/runner.py`

#### `agents/personas.py` — 420 LOC
**Docstring:** Investor persona system for TradingAgents.
**Functions:** `InvestorPersona`, `get_persona`, `list_personas`, `format_persona_context`
**Reads:** `__future__`, `dataclasses`, `loguru`
**Imported by:** `agents/runner.py`

#### `agents/progress.py` — 110 LOC
**Docstring:** Agent run progress tracking via WebSocket broadcast.
**Functions:** `register_broadcast`, `emit_progress`, `emit_run_complete`
**Reads:** `__future__`, `asyncio`, `datetime`, `loguru`, `threading`, `typing`
**Imported by:** `agents/runner.py`, `api/main.py`

#### `agents/runner.py` — 575 LOC
**Docstring:** TradingAgents orchestration runner.
**Functions:** `AgentRunner`
**Reads:** `__future__`, `agents`, `alerts`, `concurrent`, `config`, `dataclasses`, `datetime`, `intelligence`, `json`, `loguru`, `outputs`, `sqlalchemy`, `time`, `tradingagents`, `typing`
**Imported by:** `agents/scheduler.py`, `api/routers/agents.py`

#### `agents/scheduler.py` — 126 LOC
**Docstring:** Scheduled TradingAgents runs.
**Functions:** `start_agent_scheduler`, `stop_agent_scheduler`, `get_schedule_status`
**Reads:** `__future__`, `agents`, `config`, `datetime`, `db`, `loguru`, `schedule`, `threading`, `time`
**Imported by:** `api/routers/agents.py`

## `validation/`

#### `validation/__init__.py` — 6 LOC
**Docstring:** GRID validation layer.

#### `validation/backtest.py` — 1029 LOC
**Docstring:** GRID walk-forward backtesting engine.
**Functions:** `WalkForwardBacktest`, `run_hold_validation`
**Reads:** `__future__`, `alpha_research`, `argparse`, `datetime`, `db`, `json`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `store`, `typing`

#### `validation/execution_sim.py` — 542 LOC
**Docstring:** GRID execution simulation layer.
**Functions:** `OrderSide`, `OrderType`, `Order`, `EdgeEstimate`, `MarketState`, `PortfolioPosition`, `Portfolio`, `RiskLimits`, `RiskCheckResult`, `RiskManager`, `ExecutionSimConfig`, `ExecutionSimulator`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `loguru`, `numpy`, `pandas`, `typing`
**Imported by:** `inference/tuning.py`

#### `validation/gates.py` — 268 LOC
**Docstring:** GRID promotion gate enforcement module.
**Functions:** `GateChecker`
**Reads:** `__future__`, `db`, `governance`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `governance/registry.py`

## `backtest/`

#### `backtest/__init__.py` — 1 LOC
**Docstring:** GRID pitch backtest engine.

#### `backtest/charts.py` — 333 LOC
**Docstring:** GRID Backtest Chart Generator.
**Functions:** `generate_all_charts`
**Reads:** `__future__`, `backtest`, `json`, `matplotlib`, `numpy`, `outputs`, `pathlib`, `typing`
**Imported by:** `api/routers/backtest.py`

#### `backtest/engine.py` — 847 LOC
**Docstring:** GRID Pitch Backtest Engine.
**Functions:** `half_kelly_fraction`, `regime_adjusted_size`, `compute_metrics`, `compute_regime_stats`, `compute_transition_returns`, `PitchBacktester`
**Reads:** `__future__`, `datetime`, `db`, `json`, `loguru`, `numpy`, `outputs`, `pandas`, `pathlib`, `sklearn`, `sqlalchemy`, `store`, `sys`, `typing`, `yfinance`
**Imported by:** `api/routers/backtest.py`, `backtest/charts.py`, `backtest/paper_trade.py`

#### `backtest/paper_trade.py` — 443 LOC
**Docstring:** GRID Live Paper Trade System.
**Functions:** `PaperTradeTracker`
**Reads:** `__future__`, `backtest`, `datetime`, `db`, `json`, `loguru`, `outputs`, `pathlib`, `sqlalchemy`, `sys`, `typing`, `yfinance`
**Imported by:** `api/routers/backtest.py`

## `hyperspace/`

#### `hyperspace/__init__.py` — 10 LOC
**Docstring:** GRID Hyperspace integration layer.

#### `hyperspace/client.py` — 269 LOC
**Docstring:** GRID Hyperspace API client.
**Functions:** `HyperspaceClient`, `get_client`
**Reads:** `__future__`, `config`, `loguru`, `requests`, `time`, `typing`
**Imported by:** `api/routers/system.py`, `discovery/clustering.py`, `hyperspace/embeddings.py`, `hyperspace/monitor.py`, `hyperspace/reasoner.py`

#### `hyperspace/embeddings.py` — 348 LOC
**Docstring:** GRID semantic embedding layer.
**Functions:** `GRIDEmbeddings`
**Reads:** `__future__`, `hyperspace`, `llm`, `loguru`, `numpy`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `discovery/orthogonality.py`

#### `hyperspace/monitor.py` — 263 LOC
**Docstring:** GRID Hyperspace node monitoring module.
**Functions:** `HyperspaceMonitor`, `print_status_dashboard`
**Reads:** `__future__`, `hyperspace`, `loguru`, `pathlib`, `re`, `subprocess`, `typing`
**Imported by:** `api/routers/system.py`

#### `hyperspace/reasoner.py` — 245 LOC
**Docstring:** GRID LLM-assisted reasoning layer.
**Functions:** `GRIDReasoner`
**Reads:** `__future__`, `hyperspace`, `loguru`, `outputs`, `re`
**Imported by:** `discovery/clustering.py`

#### `hyperspace/research_agent.py` — 197 LOC
**Docstring:** GRID Hyperspace research agent definition.
**Functions:** `GRIDResearchAgent`
**Reads:** `__future__`, `datetime`, `loguru`, `pathlib`

## `timeseries/`

#### `timeseries/__init__.py` — 17 LOC
**Docstring:** GRID Time-Series Forecasting.
**Reads:** `timeseries`

#### `timeseries/_model_pool.py` — 122 LOC
**Docstring:** Shared TimesFM model pool.
**Functions:** `get_timesfm_model`
**Reads:** `__future__`, `loguru`, `threading`, `timesfm`, `torch`, `typing`
**Imported by:** `inference/timesfm_service.py`, `timeseries/timesfm_forecaster.py`

#### `timeseries/autobnn.py` — 399 LOC
**Docstring:** GRID AutoBNN — Interpretable Signal Decomposition.
**Functions:** `DecompositionResult`, `RegimeChangeSignal`, `AutoBNNDecomposer`, `get_decomposer`
**Reads:** `__future__`, `autobnn`, `config`, `dataclasses`, `datetime`, `jax`, `loguru`, `numpy`, `pandas`, `typing`
**Imported by:** `discovery/changepoint_detector.py`

#### `timeseries/timesfm_forecaster.py` — 507 LOC
**Docstring:** GRID TimesFM Forecaster.
**Functions:** `ForecastResult`, `BatchForecastResult`, `TimesFMForecaster`, `get_forecaster`, `signal_forecast_to_forecast_result`, `forecast_result_to_signal_forecast`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `inference`, `loguru`, `numpy`, `pandas`, `time`, `timeseries`, `timesfm`, `typing`
**Imported by:** `api/routers/chat.py`, `api/routers/forecasts.py`, `oracle/forecaster_adapter.py`, `timeseries/__init__.py`

## `llamacpp/`

#### `llamacpp/__init__.py` — 7 LOC
**Docstring:** GRID llama.cpp integration.

#### `llamacpp/client.py` — 792 LOC
**Docstring:** GRID llama.cpp server client.
**Functions:** `LlamaCppClient`, `get_client`
**Reads:** `__future__`, `config`, `knowledge`, `llm`, `loguru`, `re`, `requests`, `time`, `typing`
**Imported by:** `ollama/client.py`, `ollama/router.py`

## `outputs/`

#### `outputs/__init__.py` — 1 LOC
**Docstring:** GRID outputs — LLM insight logging and periodic review.

#### `outputs/insight_scanner.py` — 355 LOC
**Docstring:** Periodic scanner for accumulated LLM insights.
**Functions:** `run_insight_review`, `schedule_reviews`
**Reads:** `__future__`, `alerts`, `argparse`, `collections`, `datetime`, `json`, `loguru`, `pathlib`, `re`, `threading`, `time`, `typing`

#### `outputs/llm_logger.py` — 348 LOC
**Docstring:** Timestamped markdown logger for all LLM outputs and insights.
**Functions:** `log_insight`, `log_agent_deliberation`, `cleanup_old_insights`, `get_recent_insights`
**Reads:** `__future__`, `alerts`, `asyncio`, `config`, `datetime`, `json`, `loguru`, `pathlib`, `typing`, `verification`
**Imported by:** `agents/runner.py`, `analysis/capital_flows.py`, `hyperspace/reasoner.py`, `intelligence/market_diary.py`, `ollama/reasoner.py`

#### `outputs/path_utils.py` — 41 LOC
**Docstring:** Helpers for output directories that may be dangling symlinks locally.
**Functions:** `ensure_output_dir`
**Reads:** `__future__`, `loguru`, `pathlib`
**Imported by:** `api/routers/backtest.py`, `api/routers/ollama.py`, `backtest/charts.py`, `backtest/engine.py`, `backtest/paper_trade.py`, `intelligence/source_quality_ablation.py`, `ollama/market_briefing.py`

## `a2a/`

#### `a2a/__init__.py` — 22 LOC
**Docstring:** GRID Agent-to-Agent (A2A) Protocol.
**Reads:** `a2a`

#### `a2a/agent_card.py` — 211 LOC
**Docstring:** A2A Agent Card — JSON capability descriptor.
**Functions:** `AgentSkill`, `AgentCard`, `build_grid_agent_card`
**Reads:** `__future__`, `config`, `dataclasses`, `loguru`, `typing`
**Imported by:** `a2a/__init__.py`, `api/routers/a2a.py`

#### `a2a/client.py` — 232 LOC
**Docstring:** A2A Client — discover and delegate to remote agents.
**Functions:** `TaskState`, `A2ATask`, `A2AClient`
**Reads:** `__future__`, `dataclasses`, `enum`, `loguru`, `requests`, `time`, `typing`, `uuid`
**Imported by:** `a2a/__init__.py`, `a2a/server.py`, `api/routers/a2a.py`

#### `a2a/server.py` — 205 LOC
**Docstring:** A2A Server — receive and process task requests from external agents.
**Functions:** `A2ATaskManager`
**Reads:** `__future__`, `a2a`, `loguru`, `typing`, `uuid`
**Imported by:** `a2a/__init__.py`, `api/routers/a2a.py`

## `events/`

#### `events/__init__.py` — 15 LOC
**Docstring:** GRID event system -- durable event streaming via Redpanda with PG NOTIFY fallback.
**Reads:** `events`

#### `events/bus.py` — 220 LOC
**Docstring:** GRID Event Bus — PG LISTEN/NOTIFY wrapper with in-process fan-out.
**Functions:** `RecentEventIds`, `normalize_listen_dsn`, `EventBus`
**Reads:** `__future__`, `asyncpg`, `collections`, `datetime`, `events`, `json`, `loguru`, `typing`
**Imported by:** `api/main.py`, `api/routers/sse.py`, `contracts/emit.py`

#### `events/channels.py` — 51 LOC
**Docstring:** Event channel constants and payload schemas.
**Functions:** `Event`
**Reads:** `__future__`, `dataclasses`, `json`, `typing`
**Imported by:** `api/routers/sse.py`, `events/bus.py`

#### `events/consumer.py` — 118 LOC
**Docstring:** Durable event consumer -- reads events from Redpanda topics.
**Functions:** `consume`, `get_topic_info`
**Reads:** `__future__`, `config`, `events`, `json`, `kafka`, `loguru`, `typing`
**Imported by:** `api/routers/sse.py`, `events/__init__.py`

#### `events/producer.py` — 195 LOC
**Docstring:** Durable event producer -- sends structured events to Redpanda topics.
**Functions:** `emit`, `emit_async`, `flush`, `close`
**Reads:** `__future__`, `config`, `datetime`, `db`, `json`, `kafka`, `loguru`, `time`
**Imported by:** `api/routers/canvas_investigate.py`, `api/routers/sse.py`, `events/__init__.py`, `events/consumer.py`

## `governance/`

#### `governance/__init__.py` — 6 LOC
**Docstring:** GRID governance layer.

#### `governance/hypothesis_labels.py` — 197 LOC
**Docstring:** GRID governance — additive labels for legacy ``hypothesis_registry`` verdicts.
**Functions:** `label_legacy_registry`, `passed_validation`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/discovery.py`, `trading/paper_engine.py`, `validation/gates.py`

#### `governance/registry.py` — 313 LOC
**Docstring:** GRID model governance registry.
**Functions:** `ModelRegistry`
**Reads:** `__future__`, `datetime`, `db`, `loguru`, `sqlalchemy`, `typing`, `validation`
**Imported by:** `api/dependencies.py`

## `journal/`

#### `journal/__init__.py` — 6 LOC
**Docstring:** GRID decision journal.

#### `journal/log.py` — 341 LOC
**Docstring:** GRID immutable decision journal.
**Functions:** `DecisionJournal`
**Reads:** `__future__`, `datetime`, `db`, `json`, `loguru`, `math`, `pandas`, `sqlalchemy`, `typing`
**Imported by:** `api/dependencies.py`, `trading/contagion_to_ticket.py`

## Outside the scanned directories: `evaluation/`

`evaluation/` is not in `SCAN_DIRS` (`scripts/lint_module_inventory.py`), so `scripts/rebuild_module_inventory.py` omits it. These entries are hand-maintained; their level-5 headings keep them out of the linter's file-drift check.

##### `evaluation/__init__.py` — 11 LOC
**Docstring:** GRID evaluation package.

##### `evaluation/astrogrid_celestial.py` — 214 LOC
**Docstring:** Controlled walk-forward evaluation of implemented Astrogrid mystical inputs.
**Functions:** `ridge_predict`, `build_frame`, `interaction_columns`, `walk_forward`, `net_returns`, `bootstrap_means`, `case_stats`, `evaluate`
**Reads:** `__future__`, `analysis`, `calendar`, `datetime`, `json`, `numpy`, `pathlib`

##### `evaluation/astrogrid_edge.py` — 281 LOC
**Docstring:** Fee-aware, frozen-rule Astrogrid research; no inference or ledger writes.
**Functions:** `Bar`, `parse_archive`, `load_archives`, `iso`, `replay_events`, `strategy_returns`, `daily_bootstrap`, `sample_summary`, `summarize`, `historical_report`
**Reads:** `__future__`, `binance_close_contract`, `collections`, `csv`, `dataclasses`, `datetime`, `evals`, `hashlib`, `json`, `numpy`, `oracle`, `pathlib`, `typing`, `zipfile`

##### `evaluation/continuous_forecasts.py` — 348 LOC
**Docstring:** Offline exact-72h evaluation; no provider, database, scheduler or trade I/O.
**Functions:** `ContractError`, `utc`, `number`, `reference`, `VenueContract`, `Bar`, `Forecast`, `CostScenario`, `preflight`, `evaluate`
**Reads:** `__future__`, `dataclasses`, `datetime`, `math`, `typing`

##### `evaluation/edge_report.py` — 888 LOC
**Docstring:** Forward-only edge report and the pre-registered ``prune_rule_v1`` (S16).
**Functions:** `DateCell`, `UnitData`, `UnitStats`, `clustered_effective_n`, `choose_block`, `block_bootstrap_p`, `classify`, `unit_stats`, `evaluate_family`, `exploratory_stats`, `FamilyColumns`, `feed_unit_key`, `build_signal_families`, `build_oracle_family`, `build_astrogrid_family`, `load_all`, `build_report`, `candidate_prune_list`, `report_json`, `render_markdown`, `trust_diff`
**Reads:** `__future__`, `analysis`, `collections`, `dataclasses`, `datetime`, `evals`, `evaluation`, `ingestion`, `intelligence`, `json`, `math`, `numpy`, `re`, `sqlalchemy`, `store`, `typing`

##### `evaluation/prices.py` — 259 LOC
**Docstring:** Read-only raw-close accessor for the provisional signal evaluation.
**Functions:** `UnsupportedInstrumentError`, `AmbiguousPriceError`, `UnverifiedCutoverError`, `PricePoint`, `PITPriceAccessor`, `us_listed_close_boundary`, `CompletedCloseAccessor`
**Reads:** `__future__`, `bisect`, `dataclasses`, `datetime`, `evaluation`, `ingestion`, `math`, `re`, `sqlalchemy`, `store`, `typing`, `zoneinfo`
**Imported by:** `evaluation/signal_outcomes.py`, `intelligence/hypothesis_trials.py`, `intelligence/sleuth_intersections.py`
**Notes:** A read-only price accessor with no network calls. `PITPriceAccessor` reads the exact `YF:{ticker}:{field}` raw series for the provisional signal evaluator (`evaluation/signal_outcomes.py`, `scripts/evaluate_signals.py`). It never reads resolved features, and it refuses crypto, ambiguous multi-valued dates and unverified raw-close cutovers. `CompletedCloseAccessor` maps `(instrument, as_of)` to a `PricePoint` over completed US closes, pooling yfinance and TIINGO (`COMPLETED_CLOSE_SOURCES`) through `store.observations.read_completed_closes`, with vintages known at `evaluated_at`. A close counts as complete after 17:00 ET on NYSE sessions (`us_listed_close_boundary`), and closes whose vintages disagree are refused as ambiguous. `evaluation/signal_outcomes.py` uses it for corrected scoring. `intelligence/hypothesis_trials.py` reuses `us_listed_close_boundary` and `COMPLETED_CLOSE_SOURCES`, and `intelligence/sleuth_intersections.py` mirrors its ticker patterns.
**Tests:** `tests/test_evaluation_prices.py`, `tests/test_signal_outcomes.py`, `tests/test_signal_outcome_persistence.py`

##### `evaluation/signal_outcomes.py` — 1036 LOC
**Docstring:** Versioned, honest per-signal outcome evaluator (workstream W3b, wired S8).
**Functions:** `validate_scoring_parameters`, `UnsupportedInstrumentError`, `AmbiguousPriceError`, `UnverifiedCutoverError`, `SignalRecord`, `PricePoint`, `make_default_price_accessor`, `SignalOutcomeRecord`, `evaluate_signal`, `dedupe_records`, `dedupe_outcomes`, `CohortSummary`, `summarize_outcomes`, `resolve_known_at`, `SignalSourceRow`, `parse_signal_payload`, `record_from_row`, `is_final`, `outcome_params`, `OutcomeTally`, `evaluate_ticker_rows`, `write_outcomes`, `outcomes_table_exists`, `replay_signal_sources`, `score_forward`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `evaluation`, `ingestion`, `intelligence`, `json`, `math`, `sqlalchemy`, `typing`, `zoneinfo`
**Imported by:** `evaluation/edge_report.py`, `evaluation/prices.py`, `intelligence/trust_scorer.py`
