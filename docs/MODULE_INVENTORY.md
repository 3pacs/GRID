# GRID Module Inventory

Generated: 2026-04-13
Total modules: 682
Total LOC: 298,825

This is the authoritative inventory of every `.py` file in the GRID intelligence/data/serving stack.
Excludes `tests/`, `__pycache__/`, `.git/`, `pwa/`, `pwa_dist/`, `docs/`, `notebooks/`.

## Directory summary

| Directory | Module count | LOC |
|---|---|---|
| `intelligence/` | 143 | 92,759 |
| `ingestion/` | 185 | 71,104 |
| `api/` | 100 | 43,392 |
| `analysis/` | 31 | 30,607 |
| `trading/` | 13 | 7,175 |
| `oracle/` | 24 | 6,691 |
| `subnet/` | 10 | 5,408 |
| `inference/` | 13 | 4,378 |
| `store/` | 6 | 4,154 |
| `physics/` | 8 | 3,659 |
| `alpha_research/` | 21 | 3,426 |
| `ollama/` | 7 | 3,110 |
| `alerts/` | 6 | 2,924 |
| `features/` | 5 | 2,571 |
| `gemma/` | 7 | 2,474 |
| `discovery/` | 5 | 2,208 |
| `agents/` | 9 | 1,687 |
| `backtest/` | 4 | 1,573 |
| `normalization/` | 3 | 1,384 |
| `hyperspace/` | 6 | 1,316 |
| `validation/` | 4 | 1,179 |
| `contracts/` | 12 | 1,150 |
| `timeseries/` | 4 | 1,021 |
| `outputs/` | 3 | 707 |
| `a2a/` | 4 | 675 |
| `events/` | 5 | 524 |
| `rag/` | 5 | 452 |
| `llamacpp/` | 2 | 447 |
| `journal/` | 2 | 349 |
| `governance/` | 2 | 321 |
| **TOTAL** | **649** | **298,825** |

## By directory

### `intelligence/` (143 modules, 92,759 LOC)

#### `intelligence/actors/seed_data.py` — 5619 LOC
**Docstring:** GRID Intelligence — Actor Network seed data.
**Functions:** `get_known_actors(engine)`
**Reads:** `__future__`, `chicago`, `editors`, `firm`, `insider_trades`, `lever_pullers`, `redstone`, `saic`, `sqlalchemy`, `top`
**Imported by:** `ingestion/altdata/wikidata_persons.py`, `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/db.py`, `intelligence/actors/ingestion.py`

#### `intelligence/actor_discovery.py` — 3533 LOC
**Docstring:** GRID Intelligence — Automated Actor Discovery & Enrichment (250K+ Scale).
**Functions:** `enrich_actor(engine, actor_id)`, `discover_connections(engine)`, `enrich_all_actors(engine, batch_size)`, `auto_discover_actors(engine)`, `auto_discover_connections(engine)`, `run_discovery_cycle(engine, scale_phase)`, `get_actor_stats(engine)`, `batch_discover_insiders(engine, days_back)`, `discover_all_13f_filers(engine)`, `discover_all_congress(engine)`, `import_icij_offshore(engine, data_dir, cross_reference)`, `discover_board_interlocks(engine)`, `run_3_degree_expansion(engine, max_per_degree)`, `run_scale_discovery(engine, target_phase)`, `hermes_daily_actor_discovery(engine)`
**Reads:** `__future__`, `actor_connections`, `actor_network`, `actors`, `auto_discover_actors`, `congressional`, `contract`, `data`, `datetime`, `degree`, `existing`, `filing`, `gov_contracts`, `grid`, `historical`, `icij`, `ingestion`, `known`, `lobbying`, `loguru`, `offshore_leaks`, `orchestration`, `pathlib`, `pre`, `raw_series`
**Writes:** `actor_connections`, `actors`
**Imports from GRID:** `ingestion.altdata.offshore_leaks`

#### `intelligence/global_levers.py` — 2258 LOC
**Docstring:** GRID Intelligence -- Global Lever Map: Hierarchical Model of World Economic Power.
**Functions:** `get_lever_hierarchy(engine)`, `get_lever_domain(domain)`, `trace_lever_chain(event)`, `find_cross_domain_actors(engine)`, `generate_lever_report(engine)`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `intelligence`, `lever_pullers`, `loguru`, `news_articles`, `raw_series`, `signal`, `signal_sources`, `sqlalchemy`, `trust`, `typing`
**Imports from GRID:** `intelligence.lever_pullers`
**Imported by:** `api/routers/intelligence_deepdive.py`

#### `intelligence/hypothesis_engine.py` — 2137 LOC
**Docstring:** GRID Intelligence — Hypothesis Discovery Engine.
**Classes:** `DiscoveredPattern` [is_significant]; `Anomaly`; `Hypothesis`; `TemporalPatternDetector` [__init__, scan_lead_lag, discover_patterns]; `AnomalyHunter` [__init__, scan_volume_anomalies, scan_actor_anomalies, scan_convergence]; `HypothesisGenerator` [__init__, generate, score_hypothesis, score_all, auto_discover]
**Functions:** `ensure_tables(engine)`, `cleanup_hypotheses(engine, dry_run)`, `get_stats(engine)`, `main()`
**Reads:** `__future__`, `actor`, `actor_history`, `analytical_snapshots`, `both`, `collections`, `convergence`, `criteria`, `dataclasses`, `datetime`, `discovered`, `discovered_hypotheses`, `generated`, `historical`, `hypothesis_boost_log`, `hypothesis_postmortems`, `hypothesis_registry`, `intelligence`, `lead`, `loguru`, `math`, `monthly`, `oracle_predictions`, `patterns`, `scipy`
**Writes:** `discovered_hypotheses`, `hypothesis_boost_log`, `hypothesis_postmortems`
**Imports from GRID:** `db`, `intelligence.causation_scoring`, `intelligence.cross_reference`, `intelligence.forensics`, `intelligence.lever_pullers`, `intelligence.trust_scorer`
**Imported by:** `ingestion/scheduler.py`

#### `intelligence/power_mapper.py` — 91 LOC
**Docstring:** Power Mapper — unified power-mapping layer over multiple relationship sources.
**Classes:** `PowerEdge` [__post_init__]
**Functions:** `_categorize_littlesis(category_id)`, `resolve_edge_weight(edge_type)`
**Reads:** `__future__`, `dataclasses`

#### `features/per_signal_brier.py` — 443 LOC
**Docstring:** ALPHA-15 / #118 — Per-signal per-horizon Brier tracker. The conviction dial that closes the gap between 'we have data' and 'we know it's predictive.' Decomposes scored-prediction confidence into Shapley signal contributions and updates running Brier/ECE/hit counters per (signal_source, horizon) bucket via Welford incremental averaging. After ~30 days of scored predictions becomes the operator's signal-weight knob.
**Classes:** `SignalScorecard`
**Functions:** `ensure_tables(engine)`, `_canonical_horizon(horizon_days)`, `record_scored_prediction(engine, horizon_days, confidence, outcome, signal_contributions)`, `compute_conviction_weight(running_brier, scored_count)`, `get_signal_scorecard(engine, signal_source, horizon_days)`, `rank_signals_by_horizon(engine, horizon_days, min_samples)`, `get_full_scorecard_table(engine)`
**Reads:** `__future__`, `dataclasses`, `per_signal_brier_history`, `sqlalchemy`
**Writes:** `per_signal_brier_history`

#### `intelligence/signal_provenance.py` — 451 LOC
**Docstring:** Per-ticker provenance/conviction report builder. For a given ticker assembles oracle prediction + per-signal Brier scorecards + Shapley contributions + red-team epistemic risk + recent shipping fudge alerts + lever→flow→actor causation chain into a single TradeProvenanceReport with an aggregate conviction score and a verdict (high/medium/low/no_trade). The 'should I trade this?' answer in one structured object.
**Classes:** `SignalEvidence`; `CausationChain`; `TradeProvenanceReport`
**Functions:** `_classify_evidence(scorecard)`, `compute_aggregate_conviction(...)`, `_verdict_from_aggregate(conviction, confidence)`, `_extract_signal_contributions(prediction)`, `_extract_causation(prediction)`, `_recent_fudge_alerts(engine, ticker, window_days)`, `build_provenance_report(engine, prediction, red_team_epistemic_risk)`
**Reads:** `__future__`, `cross_reference_checks`, `dataclasses`, `per_signal_brier_history`
**Writes:** (none — read-only assembler)
**Imports from GRID:** `features.per_signal_brier`

#### `intelligence/shipping_fudge_detector.py` — 438 LOC
**Docstring:** Capstone divergence detector — compares reported shipping statistics (CAT-51 LME, CAT-52 Mysteel iron ore, CAT-82 Drewry/SCFI) against AIS ground truth + social port activity observed deltas. Emits CrossRefCheck rows with category='shipping' into the existing cross_reference_checks table so the lie-detector dashboard consumes them uniformly.
**Classes:** (reuses CrossRefCheck + LieDetectorReport from intelligence.cross_reference)
**Functions:** `pairings_for_port(port_slug)`, `check_pairing(...)`, `check_port_reported_vs_observed(engine, port_slug)`, `run_shipping_fudge_detector(engine)`, `get_fudge_alerts(engine, window_days)`
**Reads:** `__future__`, `cross_reference_checks`, `raw_series`
**Writes:** `cross_reference_checks`

#### `intelligence/llm_red_team.py` — 406 LOC
**Docstring:** CAT-181 — LLM red-team loop per prediction. Generates 3 counter-arguments using the local LLM, grades them, and returns an epistemic_risk_score for oracle confidence dampening.
**Classes:** `CounterArgument`; `RedTeamReport` [to_dict]
**Functions:** `build_red_team_prompt(ticker, direction, horizon_days, score, signal_summaries)`, `parse_red_team_response(raw)`, `compute_epistemic_risk(counters)`, `red_team_prediction(ticker, direction, horizon_days, score, signal_summaries, llm_client)`
**Reads:** `__future__`, `dataclasses`, `json`, `re`

#### `ingestion/altdata/refinery_cracks.py` — 355 LOC
**Docstring:** CAT-54 — US refinery utilization + 3-2-1 crack spreads weekly puller (FRED).
**Classes:** `Crack321`; `RefineryCracksPuller`
**Functions:** `run_refinery_cracks_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/credit_card_spending.py` — 441 LOC
**Docstring:** CAT-75 — Consumer credit card outstanding + delinquency + charge-off weekly puller (FRED).
**Classes:** `CreditCardSnapshot`; `CreditCardSpendingPuller`
**Functions:** `run_credit_card_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/buyback_execution.py` — 515 LOC
**Docstring:** CAT-67 — Corporate buyback execution rate vs authorization (Z.1 Flow of Funds via FRED).
**Classes:** `BuybackSnapshot`; `BuybackExecutionPuller`
**Functions:** `run_buyback_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/semi_book_to_bill.py` — 605 LOC
**Docstring:** CAT-89 — SEMI North American semiconductor equipment book-to-bill ratio monthly puller (FRED primary, SEMI.org HTML fallback).
**Classes:** `SemiBookToBill`; `SemiBookToBillPuller`
**Functions:** `run_semi_book_to_bill_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/ecb_tltro.py` — 516 LOC
**Docstring:** CAT-12 — ECB TLTRO-III outstanding balance + repayment calendar (FRED primary, ECB SDW SDMX-JSON fallback).
**Classes:** `TLTROSnapshot`; `ECBTltroPuller`
**Functions:** `compute_days_to_next_repayment(as_of, calendar)`, `run_ecb_tltro_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/pboc_omo.py` — 450 LOC
**Docstring:** CAT-3 — PBoC 7-day reverse repo + MLF renewal daily puller (akshare macro_china_cb_operation / repo_rate_hist + macro_china_mlf_rate / macro_china_lpr fallbacks).
**Classes:** `PBOCOmoSnapshot`; `MLFRenewal`; `PBOCOmoPuller`
**Functions:** `run_pboc_omo_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/taiwan_exports.py` — 591 LOC
**Docstring:** CAT-9 — Taiwan export orders + semiconductor foundry utilization (FRED → MOEA open-data API + cold-start historical foundry seed).
**Classes:** `TaiwanExportSnapshot`; `FoundryUtilization`; `TaiwanExportsPuller`
**Functions:** `compute_yoy(current, prior_year)`, `run_taiwan_exports_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/container_freight.py` — 792 LOC
**Docstring:** CAT-82 — Drewry World Container Index + Shanghai Containerized Freight Index weekly puller (FRED → akshare → Drewry/SSE HTML scrape fallback).
**Classes:** `ContainerFreightSnapshot`; `ContainerFreightPuller`
**Functions:** `run_container_freight_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/lme_warehouse.py` — 678 LOC
**Docstring:** CAT-51 — LME daily warehouse stocks + cancelled-warrant ratio for 6 base metals (JSON probe + HTML scrape fallback).
**Classes:** `LMEStockSnapshot`; `LMEWarehousePuller`
**Functions:** `compute_cancelled_ratio(total, cancelled)`, `run_lme_warehouse_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/iron_ore_ports.py` — 779 LOC
**Docstring:** CAT-52 — Chinese iron ore port stocks + daily throughput via akshare probe ladder + Mysteel 45-port survey HTML fallback.
**Classes:** `IronOrePortSnapshot`; `IronOrePortsPuller`
**Functions:** `compute_wow_delta(current_mt, prior_mt)`, `run_iron_ore_ports_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/taiwan_strait_osint.py` — 580 LOC
**Docstring:** CAT-91 — Taiwan Strait OSINT via Taiwan MND daily ADIZ incursion count + hardcoded PLA exercise calendar. OpenSky/AISHub reserved for future enhancement.
**Classes:** `TaiwanStraitSnapshot`; `TaiwanStraitPuller`
**Functions:** `is_exercise_active(as_of, calendar, window_days)`, `run_taiwan_strait_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/credit_index_proxies.py` — 580 LOC
**Docstring:** CAT-7 + CAT-13 + CAT-42 unified puller — FRED ICE BofA cash-bond OAS proxies for paywalled Markit/S&P indices (iBoxx USD Asia HY, iBoxx EUR CoCo, CDX NA IG/HY + iTraxx Main/Xover). Documents the proxy relationship honestly; CDS-cash basis is an acknowledged gap.
**Classes:** `CreditProxySnapshot`; `CreditIndexBasis`; `CreditIndexProxiesPuller`
**Functions:** `compute_ig_hy_basis(ig_oas, hy_oas)`, `run_credit_index_proxies_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/ais_ground_truth.py` — 737 LOC
**Docstring:** Novel ground-truth observation layer for port activity — 15 global ports via VesselFinder HTML primary + AISHub fallback. Cross-checks reported shipping statistics (CAT-51 / CAT-52 / CAT-82) against real vessel counts at berth. Paired with social_port_activity.py and intelligence/shipping_fudge_detector.py.
**Classes:** `PortSpec`; `AISSnapshot`; `AISGroundTruthPuller`
**Functions:** `compute_capacity_utilization(at_berth, at_anchor)`, `run_ais_ground_truth_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/social_port_activity.py` — 879 LOC
**Docstring:** Novel ground-truth social-feed layer — Reddit + YouTube (graceful-degrade) + nitter + Bilibili post-velocity across 15 global ports. Cross-check for CAT-51 / CAT-52 / CAT-82 reported statistics. Paired with ais_ground_truth.py for the shipping_fudge_detector.
**Classes:** `SocialPortSpec`; `SocialActivitySnapshot`; `SocialPortActivityPuller`
**Functions:** `compute_composite_velocity(reddit, youtube, nitter, bilibili)`, `run_social_port_activity_puller(engine)`
**Reads:** `__future__`, `bs4`, `dataclasses`, `random`, `requests`, `sqlalchemy`, `time`
**Writes:** `raw_series`

#### `ingestion/altdata/jodi_oil.py` — 630 LOC
**Docstring:** Novel — Joint Organisations Data Initiative (JODI) global oil inventory monthly puller. 15 countries × 7 products × 4 flows = 420 potential series covering Saudi/UAE/Kuwait/Iraq/Russia/Iran/Venezuela/Nigeria/etc. production + imports + exports + closing stocks. Fills the gap where EIA/IEA don't detail non-OECD producers. CSV primary + SDMX-JSON fallback with header-drift tolerance.
**Classes:** `JODIObservation`; `JODIOilPuller`
**Functions:** `run_jodi_oil_puller(engine)`
**Reads:** `__future__`, `csv`, `dataclasses`, `json`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/sge_premium.py` — 659 LOC
**Docstring:** Novel — Shanghai Gold Exchange au9999 premium vs London PM fix daily puller. Cleanest real-time public China physical gold demand signal. Leads global gold ETF flows by 2-4 weeks. akshare probe ladders across SGE (7 functions), London (4 functions), USDCNY (4 functions). Composite premium classified into distress/elevated/neutral/discount severity buckets. Defensive calendar-misalignment anchoring on the Shanghai session.
**Classes:** `GoldSpotSnapshot`; `SGEPremiumPuller`
**Functions:** `cny_per_gram_to_usd_per_oz(cny_per_gram, usdcny)`, `classify_premium(premium_usd)`, `run_sge_premium_puller(engine)`
**Reads:** `__future__`, `dataclasses`, `sqlalchemy`
**Writes:** `raw_series`

#### `ingestion/altdata/reddit_options_pulse.py` — 771 LOC
**Docstring:** Novel — Reddit /r/options daily discussion thread pulse. Fetches the auto-posted daily thread via Reddit's public JSON API, flattens the comment tree, extracts post count + unique authors + bull/bear sentiment (Laplace-smoothed ratio) + 0DTE reference count + top 20 ticker mentions. Retail positioning leads meme/AI-momentum moves by 1-3 days.
**Classes:** `RedditOptionsPulse`; `RedditOptionsPulsePuller`
**Functions:** `extract_tickers(text)`, `count_sentiment_tokens(text, token_set)`, `compute_bull_bear_ratio(bull, bear)`, `run_reddit_options_pulse_puller(engine)`
**Reads:** `__future__`, `collections`, `dataclasses`, `re`, `requests`, `sqlalchemy`
**Writes:** `raw_series`

#### `intelligence/postmortem.py` — 1818 LOC
**Docstring:** GRID Intelligence — Automated Post-Mortem Analysis for Failed Trades & Predictions.
**Classes:** `PostMortem` [to_dict]
**Functions:** `generate_postmortem(engine, trade_id)`, `generate_prediction_postmortem(engine, prediction_id)`, `batch_postmortem(engine, days)`, `generate_lessons_learned(engine, postmortems)`, `load_postmortems(engine, days, ticker, category)`, `apply_contagion_feedback(engine, since_hours, dry_run)`
**Reads:** `__future__`, `actual`, `capital_flow_snapshots`, `collections`, `contagion_backtest_results`, `contagion_predictions`, `dataclasses`, `datetime`, `decision_journal`, `entry`, `intelligence`, `llm`, `loguru`, `options_daily_signals`, `options_recommendations`, `oracle_predictions`, `prediction`, `raw_series`, `sqlalchemy`, `supply_chain_edges`, `trade_postmortems`, `typing`
**Writes:** `supply_chain_edge_adjustments`, `supply_chain_edges`, `trade_postmortems`
**Imports from GRID:** `db`, `intelligence.rag`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_risk.py`

#### `intelligence/cross_reference.py` — 1818 LOC
**Docstring:** GRID Cross-Reference Engine — Lie Detector for Government Statistics.
**Classes:** `CrossRefCheck`; `LieDetectorReport`
**Functions:** `ensure_tables(engine)`, `check_gdp_vs_physical(engine, country)`, `check_trade_bilateral(engine)`, `check_inflation_vs_inputs(engine)`, `check_central_bank_actions_vs_words(engine)`, `check_employment_reality(engine)`, `get_cross_ref_for_ticker(engine, ticker)`, `check_liquidity_reality(engine)`, `check_credit_housing(engine)`, `check_insider_divergence(engine)`, `run_all_checks(engine, skip_narrative)`, `get_historical_checks(engine, category, days, assessment)`
**Reads:** `__future__`, `cpi`, `cross_reference_checks`, `dataclasses`, `datetime`, `eurozone`, `feature_registry`, `gdp`, `headline`, `industrial`, `intelligence`, `its`, `loguru`, `official`, `ollama`, `partner`, `physical`, `production`, `raw_series`, `resolved_series`, `sqlalchemy`, `static`, `typical`, `typing`
**Writes:** `cross_reference_checks`
**Imports from GRID:** `db`, `intelligence.context_provider`, `ollama.client`
**Imported by:** `api/routers/intel.py`, `api/routers/intel_cross_reference.py`, `api/routers/intelligence_risk.py`, `intelligence/codebase_context.py`, `intelligence/hypothesis_engine.py`, `intelligence/prediction_calibration.py`, `intelligence/sleuth.py`

#### `intelligence/deep_graph.py` — 1772 LOC
**Docstring:** GRID Intelligence — Deep Graph Traversal Engine.
**Classes:** `GraphNode`; `GraphEdge`; `Overlap` [to_dict]; `LayerResult`
**Functions:** `ensure_table(engine)`, `deep_drill(engine, ticker, max_depth)`, `find_overlaps(engine, ticker_a, ticker_b)`, `find_all_overlaps(engine, tickers)`, `generate_connection_map(engine, ticker, depth)`, `discover_hidden_influence(engine)`
**Reads:** `__future__`, `actors`, `analysis`, `another`, `any`, `causal_links`, `collections`, `companies`, `company`, `congressional`, `dataclasses`, `datetime`, `each`, `graph_overlaps`, `ids`, `insider`, `intelligence`, `itertools`, `layer`, `layers`, `legislation`, `loguru`, `market_universe`, `one`, `raw_series`
**Writes:** `graph_overlaps`
**Imports from GRID:** `analysis.market_universe`, `intelligence.actor_network`
**Imported by:** `api/routers/intelligence_companies.py`

#### `intelligence/pct_cogs_enrichment.py` — 1751 LOC
**Docstring:** LLM-driven supplier-cost-concentration enrichment for ``supply_chain_edges``.
**Classes:** `LLMUnavailableError`; `EdgeRow`; `AttemptRecord`; `EnrichmentSummary` [bump_reject, as_dict]; `PctCogsEnricher` [__init__, run]
**Functions:** `run_weekly(engine)`
**Reads:** `__future__`, `buyer`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `seller`, `sqlalchemy`, `supply_chain_edges`, `supply_chain_nodes`, `typing`
**Writes:** `supply_chain_edges`, `supply_chain_enrichment_log`, `supply_chain_nodes`
**Imports from GRID:** `db`, `ingestion.altdata.supply_chain_parser`

#### `intelligence/trust_scorer.py` — 1572 LOC
**Docstring:** GRID Intelligence — Source Trust Scoring Framework.
**Classes:** `SourceScore`; `ConvergenceEvent`; `TrustScorer` [__init__, get_recent_signals, get_trust_score, get_convergence_alerts]
**Functions:** `score_pending_signals(engine)`, `update_trust_scores(engine)`, `get_trusted_sources(engine, min_signals, min_trust)`, `get_insider_edge(engine, ticker)`, `detect_convergence(engine, ticker)`, `generate_trust_report(engine)`, `run_trust_cycle(engine)`, `register_signal(engine, source_type, source_id, ticker, signal_type, signal_date, signal_value, metadata)`
**Reads:** `__future__`, `capital_flows`, `dataclasses`, `datetime`, `intelligence`, `llm`, `loguru`, `options_daily_signals`, `raw_series`, `scored`, `signal_date`, `signal_sources`, `signal_value`, `source_trust_config`, `sources`, `sqlalchemy`, `supply_chain_edges`, `supply_chain_nodes`, `today`, `typing`, `update_trust_scores`, `yfinance`
**Writes:** `signal_sources`
**Imports from GRID:** `api.main`, `db`, `intelligence.actor_signal_bridge`, `intelligence.source_trust_config`
**Imported by:** `analysis/flow_thesis_data.py`, `analysis/money_flow.py`, `analysis/thesis_scorer.py`, `api/routers/actor_detail.py`, `api/routers/flows.py`, `api/routers/intelligence_risk.py`, `api/routers/watchlist_overview.py`, `inference/live.py`, `ingestion/altdata/kalshi.py`, `intelligence/codebase_context.py` (+7)

#### `intelligence/institutional_map.py` — 1510 LOC
**Docstring:** GRID Intelligence -- Institutional Map: Private Credit, Hedge Funds & Pensions.
**Functions:** `build_institutional_graph(engine)`, `trace_pension_dollars(pension_name)`, `find_conflicts_of_interest()`, `get_fee_extraction_estimate(fund_name)`, `get_all_fund_managers()`, `get_institutional_summary()`
**Reads:** `__future__`, `blue`, `loguru`, `management`, `multiple`, `pension`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_companies.py`

#### `intelligence/entity_resolver.py` — 1411 LOC
**Docstring:** GRID Intelligence — Entity Resolution Engine.
**Classes:** `ResolvedEntity` [domain_count, to_dict]; `EntityResolver` [__init__, resolve, build_resolution_index, find_connections, discover_bridges, stats]
**Functions:** `phonetic_key(name)`, `levenshtein_distance(s1, s2)`, `jaro_similarity(s1, s2)`, `jaro_winkler_similarity(s1, s2, prefix_weight)`, `name_similarity(name1, name2)`, `strip_accents(s)`, `normalize_name(raw, entity_type)`, `canonical_key(name)`, `entity_id(name, entity_type)`
**Reads:** `__future__`, `actors`, `analytical_snapshots`, `any`, `collections`, `dataclasses`, `datetime`, `entity_relationships`, `entity_resolution`, `loguru`, `oracle_predictions`, `pathlib`, `person`, `signal_data`, `sqlalchemy`, `typing`, `wealth_flows`
**Writes:** `entity_resolution`
**Imports from GRID:** `db`

#### `intelligence/lever_pullers.py` — 1377 LOC
**Docstring:** GRID Intelligence — Lever Puller Identification & Tracking.
**Classes:** `LeverPuller`; `LeverEvent`
**Functions:** `identify_lever_pullers(engine)`, `assess_motivation(puller, action, engine)`, `get_active_lever_events(engine, days)`, `find_lever_convergence(engine)`, `generate_lever_report(engine)`, `get_lever_context_for_ticker(engine, ticker)`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `identification`, `identified`, `known`, `loguru`, `ollama`, `pattern`, `signal`, `signal_sources`, `sqlalchemy`, `stated`, `trust_scorer`, `typing`
**Writes:** `lever_pullers`
**Imports from GRID:** `ollama.client`
**Imported by:** `api/routers/chat.py`, `api/routers/flows.py`, `api/routers/intel.py`, `api/routers/intelligence_risk.py`, `api/routers/watchlist_overview.py`, `intelligence/actors/analysis.py`, `intelligence/causation_scoring.py`, `intelligence/codebase_context.py`, `intelligence/company_analyzer.py`, `intelligence/global_levers.py` (+2)

#### `intelligence/rag.py` — 1264 LOC
**Docstring:** GRID RAG (Retrieval-Augmented Generation) Intelligence System.
**Classes:** `RAGIndexer` [__init__, ensure_tables, index_snapshots, index_actors, index_predictions, index_all]; `RAGRetriever` [__init__, search, search_and_rerank]
**Functions:** `get_rag_context(engine, query, top_k, source_types, max_chars)`, `ask(engine, query, top_k, llm_url, timeout)`, `main()`
**Reads:** `__future__`, `actors`, `analytical_snapshots`, `collections`, `config`, `each`, `intelligence`, `intelligence_embeddings`, `loguru`, `metadata`, `oracle_predictions`, `payload`, `pg_extension`, `sentence_transformers`, `sklearn`, `sqlalchemy`, `typing`
**Writes:** `intelligence_embeddings`
**Imports from GRID:** `config`, `db`
**Imported by:** `intelligence/causation_graph.py`, `intelligence/causation_scoring.py`, `intelligence/forensics.py`, `intelligence/postmortem.py`, `intelligence/sleuth.py`, `intelligence/thesis_tracker.py`

#### `intelligence/sleuth.py` — 1245 LOC
**Docstring:** GRID Intelligence — Investigative Research Engine (Sleuth).
**Classes:** `Lead`; `Sleuth` [__init__, get_leads, count_leads, generate_leads, investigate_lead, follow_rabbit_hole, daily_investigation]
**Functions:** `ensure_tables(engine)`, `get_sleuth(engine)`
**Reads:** `__future__`, `all`, `collections`, `conditions`, `dataclasses`, `datetime`, `evidence`, `feature_registry`, `intelligence`, `investigation_leads`, `llm`, `loguru`, `multiple`, `ollama`, `physical`, `recent`, `resolved_series`, `signal_sources`, `sqlalchemy`, `store`, `typing`
**Writes:** `investigation_leads`
**Imports from GRID:** `db`, `intelligence.actor_network`, `intelligence.context_provider`, `intelligence.cross_reference`, `intelligence.lever_pullers`, `intelligence.rag`, `intelligence.trust_scorer`, `ollama.client`, `store.snapshots`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`

#### `intelligence/causation_graph.py` — 1179 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine (graph module).
**Functions:** `trace_causal_chain(engine, ticker, end_date, max_hops)`, `find_longest_chains(engine, days)`, `generate_chain_narrative(engine, chain)`, `detect_chain_in_progress(engine)`, `load_causal_chains(engine, ticker, min_hops, limit)`
**Reads:** `__future__`, `causal_chains`, `datetime`, `earnings_calendar`, `intelligence`, `llm`, `loguru`, `ollama`, `price`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `causal_chains`
**Imports from GRID:** `db`, `intelligence.causation_core`, `intelligence.causation_scoring`, `intelligence.forensics`, `intelligence.rag`, `ollama.client`
**Imported by:** `intelligence/causation.py`

#### `intelligence/causation_scoring.py` — 1090 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine (scoring module).
**Functions:** `find_causes(engine, actor, action, ticker, action_date, signal_id)`, `batch_find_causes(engine, days)`, `get_suspicious_trades(engine, days)`, `generate_causal_narrative(engine, ticker)`
**Reads:** `__future__`, `conditions`, `datetime`, `earnings_calendar`, `intelligence`, `loguru`, `ollama`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `causal_links`
**Imports from GRID:** `db`, `intelligence.actor_signal_bridge`, `intelligence.causation_core`, `intelligence.freshness_guard`, `intelligence.lever_pullers`, `intelligence.rag`, `ollama.client`
**Imported by:** `intelligence/causation.py`, `intelligence/causation_graph.py`, `intelligence/hypothesis_engine.py`

#### `intelligence/dollar_flows.py` — 1081 LOC
**Docstring:** GRID Intelligence — Dollar Flow Normalizer.
**Functions:** `normalize_all_flows(engine, days)`, `get_flows_by_ticker(engine, ticker, days)`, `get_flows_by_sector(engine, sector, days)`, `get_aggregate_flows(engine, days)`, `get_biggest_movers(engine, days)`
**Reads:** `__future__`, `analysis`, `collections`, `datetime`, `dollar_flows`, `etf_flow`, `foreign`, `loguru`, `raw_series`, `resolved_series`, `signal_sources`, `signal_value`, `sqlalchemy`, `typing`, `validated`
**Writes:** `dollar_flows`
**Imports from GRID:** `analysis.sector_map`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_govflow.py`

#### `intelligence/company_analyzer.py` — 1079 LOC
**Docstring:** GRID Intelligence — Company Analyzer Pipeline.
**Classes:** `CompanyProfile` [to_dict]
**Functions:** `ensure_table(engine)`, `analyze_company(engine, ticker)`, `run_analysis_queue(engine, batch_size)`, `get_all_profiles(engine)`, `find_cross_company_patterns(engine)`, `generate_sector_influence_report(engine, sector)`
**Reads:** `__future__`, `all`, `analysis_queue`, `collections`, `company_profiles`, `dataclasses`, `datetime`, `filings`, `influence`, `intelligence`, `llm`, `loguru`, `raw_series`, `sqlalchemy`, `stored`, `typing`, `valve`
**Writes:** `company_profiles`
**Imports from GRID:** `intelligence.actor_network`, `intelligence.export_intel`, `intelligence.freshness_guard`, `intelligence.gov_intel`, `intelligence.influence_network`, `intelligence.legislative_intel`, `intelligence.lever_pullers`, `intelligence.trust_scorer`
**Imported by:** `analysis/money_flow.py`, `api/routers/intelligence_companies.py`

#### `intelligence/sentiment_scorer.py` — 1076 LOC
**Docstring:** GRID Intelligence — Deterministic Market Sentiment Scorer.
**Classes:** `SentimentComponent`; `SentimentResult` [to_dict]
**Functions:** `compute_sentiment(engine)`, `log_prediction(engine, result)`, `score_past_predictions(engine)`, `run_sentiment_cycle(engine)`
**Reads:** `__future__`, `advance`, `all`, `data`, `dataclasses`, `datetime`, `decision_journal`, `dollar_flows`, `dominating`, `existing`, `hermes`, `intelligence`, `latest`, `loguru`, `net`, `options`, `options_daily_signals`, `raw_series`, `sentiment_predictions`, `sentiment_weights`, `signal_sources`, `source_accuracy`, `spy`, `sqlalchemy`, `trust`
**Writes:** `sentiment_predictions`, `sentiment_weights`
**Imports from GRID:** `intelligence.trust_scorer`
**Imported by:** `api/routers/briefing.py`, `ollama/market_briefing.py`

#### `intelligence/thesis_tracker.py` — 1014 LOC
**Docstring:** GRID Intelligence — Thesis Version Tracker & Post-Mortem System.
**Classes:** `ThesisSnapshot` [to_dict]; `ThesisPostMortem` [to_dict]
**Functions:** `snapshot_thesis(engine, thesis_data)`, `score_old_theses(engine, lookback_days)`, `generate_thesis_postmortem(engine, snapshot_id)`, `get_thesis_history(engine, days)`, `get_thesis_accuracy(engine)`, `run_thesis_cycle(engine)`, `load_thesis_postmortems(engine, days)`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `flow_thesis`, `intelligence`, `llm`, `loguru`, `options_daily_signals`, `raw_series`, `sqlalchemy`, `thesis_postmortems`, `thesis_snapshots`, `typing`
**Writes:** `thesis_postmortems`, `thesis_snapshots`
**Imports from GRID:** `analysis.flow_thesis`, `db`, `intelligence.context_provider`, `intelligence.deep_dive`, `intelligence.freshness_guard`, `intelligence.rag`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`, `intelligence/codebase_context.py`

#### `intelligence/event_sequence.py` — 999 LOC
**Docstring:** GRID Intelligence — Event Sequence Builder.
**Classes:** `Event`
**Functions:** `build_sequence(engine, ticker, days)`, `build_sector_sequence(engine, sector, days)`, `compute_lead_times(events, price_series)`, `find_recurring_patterns(engine, min_occurrences)`, `build_sequence_with_lead_times(engine, ticker, days)`, `events_to_dicts(events)`
**Reads:** `__future__`, `all`, `analysis`, `collections`, `cross_reference_checks`, `dataclasses`, `datetime`, `decision_journal`, `each`, `earnings_calendar`, `every`, `implication`, `intelligence`, `loguru`, `news_articles`, `options_daily_signals`, `raw_series`, `recommendation`, `signal_sources`, `sqlalchemy`, `typing`, `watchlist`
**Imports from GRID:** `analysis.sector_map`, `intelligence.pattern_engine`
**Imported by:** `api/routers/intelligence_news.py`, `intelligence/forensics.py`, `intelligence/pattern_engine.py`

#### `intelligence/news_impact.py` — 978 LOC
**Docstring:** GRID News Impact Attribution Engine.
**Classes:** `Catalyst`; `MoveAttribution`; `Expectation`; `DeepDiveReport`; `CatalystClassifier` [__init__, classify_news, classify_signal]; `PriceDecomposer` [__init__, decompose_move, decompose_history]; `ExpectationTracker` [__init__, get_active_expectations, create_expectation, update_baked_in, resolve_expectation, compute_net_expectations]; `DeepDiveEngine` [__init__, generate_deep_dive, generate_all_mag7]
**Functions:** `generate_deep_dive_tasks()`, `run_deep_dive_task(engine, context)`, `ensure_tables(engine)`
**Reads:** `__future__`, `content`, `dataclasses`, `datetime`, `llm`, `loguru`, `news_articles`, `news_impact_catalysts`, `news_impact_expectations`, `options_daily_signals`, `pattern`, `raw_series`, `signal`, `signal_sources`, `sqlalchemy`, `type`, `typing`
**Writes:** `news_impact_expectations`, `news_impact_reports`
**Imported by:** `api/routers/intel.py`, `api/routers/intelligence_deepdive.py`

#### `intelligence/trend_tracker.py` — 969 LOC
**Docstring:** GRID Trend Tracker — Divergence Analysis for Market Trends.
**Classes:** `Trend` [to_dict]; `TrendReport` [to_dict]
**Functions:** `analyze_trends(engine, lookback_days)`
**Reads:** `__future__`, `banking`, `dataclasses`, `datetime`, `expected`, `feature_catalog`, `loguru`, `markets`, `regime_history`, `resolved_series`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/forensics.py` — 968 LOC
**Docstring:** GRID Intelligence — Forensic Analyzer.
**Classes:** `ForensicReport` [to_dict]
**Functions:** `analyze_move(engine, ticker, move_date, lookback_days)`, `find_significant_moves(engine, ticker, days, threshold)`, `batch_forensics(engine, ticker, days, threshold)`, `generate_forensic_summary(engine, ticker, days)`, `load_forensic_reports(engine, ticker, days)`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `feature_registry`, `forensic_reports`, `intelligence`, `llm`, `loguru`, `options_daily_signals`, `preceding`, `raw_series`, `reports`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `forensic_reports`
**Imports from GRID:** `db`, `intelligence.event_sequence`, `intelligence.freshness_guard`, `intelligence.rag`
**Imported by:** `api/routers/intelligence_forensics.py`, `intelligence/causation_graph.py`, `intelligence/hypothesis_engine.py`

#### `intelligence/resolution_audit.py` — 961 LOC
**Docstring:** GRID resolution audit supervisor.
**Classes:** `AuditFinding`
**Functions:** `check_duplicates(engine, feature_filter)`, `check_stale_data(engine, feature_filter)`, `check_value_sanity(engine, feature_filter)`, `check_coverage_completeness(engine, feature_filter)`, `check_entity_map_consistency(engine)`, `check_cross_source_agreement(engine, feature_filter)`, `auto_fix_issues(engine, findings, dry_run)`, `run_full_audit(engine)`, `audit_after_resolve(engine, features_resolved)`, `get_latest_audit_results(engine, limit)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `different`, `feature_registry`, `loguru`, `normalization`, `raw_series`, `resolution_audits`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `resolution_audits`
**Imports from GRID:** `normalization.entity_map`, `normalization.resolver`
**Imported by:** `api/routers/system.py`

#### `intelligence/source_audit.py` — 940 LOC
**Docstring:** GRID Intelligence — Data Source Taxonomy Audit.
**Functions:** `ensure_tables(engine)`, `build_redundancy_map(engine)`, `compare_sources(engine, feature_name)`, `detect_discrepancies(engine, threshold)`, `run_full_audit(engine)`, `update_source_priorities(engine, audit_results)`, `get_latest_audit_summary(engine)`
**Reads:** `__future__`, `collections`, `current`, `datetime`, `entity_map`, `feature_registry`, `loguru`, `normalization`, `raw_series`, `resolved_series`, `run_full_audit`, `source_accuracy`, `source_catalog`, `source_discrepancies`, `sqlalchemy`, `typing`
**Writes:** `source_accuracy`, `source_catalog`, `source_discrepancies`
**Imports from GRID:** `db`, `normalization.entity_map`
**Imported by:** `api/routers/intel_source_audit.py`, `api/routers/intelligence_risk.py`

#### `intelligence/influence_network.py` — 923 LOC
**Docstring:** GRID Intelligence — Influence Network (Crown Jewel Analysis).
**Classes:** `InfluenceLoop` [to_dict]
**Functions:** `ensure_table(engine)`, `build_influence_graph(engine)`, `detect_circular_flows(engine)`, `get_influence_for_ticker(engine, ticker)`, `vote_trade_hypocrisy(engine)`
**Reads:** `__future__`, `chips`, `collections`, `contractor_ticker_map`, `dataclasses`, `datetime`, `influence_loops`, `ingestion`, `loguru`, `raw_series`, `recipient`, `recipients`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `influence_loops`
**Imports from GRID:** `ingestion.altdata.gov_contracts`
**Imported by:** `api/routers/intelligence_actors.py`, `api/routers/intelligence_forensics.py`, `intelligence/company_analyzer.py`

#### `intelligence/pattern_engine.py` — 910 LOC
**Docstring:** GRID Intelligence -- Pattern Detection Engine.
**Classes:** `Pattern` [to_dict]
**Functions:** `discover_patterns(engine, min_occurrences, max_sequence_length)`, `match_active_patterns(engine)`, `score_pattern_accuracy(engine)`, `get_patterns_for_ticker(engine, ticker)`
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `event_patterns`, `intelligence`, `loguru`, `options_daily_signals`, `raw_series`, `signal_sources`, `sqlalchemy`, `storage`, `typing`, `watchlist`
**Writes:** `event_patterns`
**Imports from GRID:** `intelligence.event_sequence`
**Imported by:** `api/routers/intelligence_news.py`, `intelligence/event_sequence.py`

#### `intelligence/news_momentum.py` — 903 LOC
**Docstring:** GRID Intelligence — News Momentum Signal Engine.
**Classes:** `SentimentSnapshot`; `MomentumSignal` [to_dict]; `SentimentTimeSeries` [__init__, get_snapshot, get_market_snapshot]; `MomentumCalculator` [compute_velocity, compute_acceleration, classify_momentum]; `DivergenceDetector` [__init__, get_price_direction, detect_divergence]; `NewsMomentumEngine` [__init__, analyze_ticker, analyze_market, run_full_scan, get_recent_signals, get_divergences]
**Reads:** `__future__`, `article`, `current`, `dataclasses`, `datetime`, `derivatives`, `loguru`, `long`, `news_articles`, `news_momentum`, `price`, `raw_series`, `recent`, `sqlalchemy`, `typing`
**Writes:** `news_momentum`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/earnings_intel.py` — 863 LOC
**Docstring:** GRID Intelligence — Earnings Analysis & Prediction System.
**Classes:** `EarningsPrediction` [to_dict]
**Functions:** `get_earnings_calendar(engine, days_ahead)`, `analyze_earnings_surprise(engine, ticker)`, `predict_earnings_reaction(engine, ticker)`, `get_prediction_scorecard(engine)`, `run_earnings_cycle(engine)`
**Reads:** `__future__`, `capital_flow_snapshots`, `dataclasses`, `datetime`, `earnings_calendar`, `earnings_predictions`, `loguru`, `options`, `options_daily_signals`, `raw_series`, `sqlalchemy`, `typing`
**Writes:** `earnings_predictions`
**Imported by:** `api/routers/earnings.py`

#### `intelligence/deal_detector.py` — 861 LOC
**Docstring:** GRID Intelligence — M&A / Deal Detection Engine.
**Classes:** `DealSignal` [to_dict]; `DealClassifier` [classify]; `DealTracker` [__init__, update_or_create]; `DealDetector` [__init__, scan_recent_news, get_active_deals, get_deal_history, get_pipeline_summary]
**Reads:** `__future__`, `dataclasses`, `datetime`, `deal_pipeline`, `headlines`, `loguru`, `news`, `news_articles`, `sqlalchemy`, `stage`, `trending`, `trending_items`, `typing`
**Writes:** `deal_pipeline`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/market_diary.py` — 811 LOC
**Docstring:** GRID — Automated Daily Market Diary.
**Functions:** `ensure_table(engine)`, `write_diary_entry(engine, target_date, ollama_client)`, `get_diary_entry(engine, target_date)`, `list_diary_entries(engine, limit, offset)`, `search_diary(engine, query, limit)`, `schedule_daily_diary(engine)`
**Reads:** `__future__`, `analysis`, `cross_reference_reports`, `datetime`, `decision_journal`, `intelligence`, `loguru`, `market_diary`, `ollama`, `outputs`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `market_diary`
**Imports from GRID:** `analysis.flow_thesis`, `db`, `intelligence.context_provider`, `ollama.client`, `outputs.llm_logger`
**Imported by:** `api/routers/intelligence_thesis.py`

#### `intelligence/business_news_parser.py` — 804 LOC
**Docstring:** GRID Intelligence — Business News Parser.
**Classes:** `BusinessEvent` [to_dict]; `BusinessNewsParser` [__init__, parse_article, scan_recent_news, get_recent_events, get_event_summary]
**Reads:** `__future__`, `aliases`, `business_events`, `company_profiles`, `dataclasses`, `datetime`, `headline`, `loguru`, `news`, `news_articles`, `sqlalchemy`, `trending`, `trending_items`, `typing`
**Writes:** `business_events`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/audio_briefing.py` — 769 LOC
**Docstring:** GRID -- Daily Intelligence Audio Briefing Pipeline.
**Classes:** `BriefingResult` [to_dict]
**Functions:** `generate_briefing_script(engine)`, `generate_briefing_audio(engine)`, `generate_briefing_video(engine)`, `get_latest_briefing()`, `list_all_briefings()`, `get_briefing_by_filename(filename)`
**Reads:** `__future__`, `all`, `analysis`, `briefing`, `dataclasses`, `datetime`, `google`, `grid`, `intelligence`, `loguru`, `openai`, `pathlib`, `typing`
**Imports from GRID:** `analysis.flow_thesis`, `analysis.money_flow_engine`, `db`, `intelligence.cds_tracker`, `intelligence.context_provider`
**Imported by:** `api/routers/flows.py`, `api/routers/intelligence_thesis.py`

#### `intelligence/deep_dive.py` — 758 LOC
**Docstring:** GRID Intelligence — Thesis Deep Dive Engine.
**Classes:** `DeepDiveResult` [to_dict]
**Functions:** `run_deep_dive(engine, thesis_data, snapshot_id)`, `deep_dive_async(engine, thesis_data, snapshot_id)`, `get_deep_dives(engine, days, limit)`, `get_deep_dive(engine, dive_id)`
**Reads:** `__future__`, `analysis`, `config`, `convergence_alerts`, `dataclasses`, `datetime`, `google`, `intelligence`, `investigation_leads`, `loguru`, `openai`, `recurring_patterns`, `sqlalchemy`, `thesis_deep_dives`, `thesis_snapshots`, `typing`
**Writes:** `thesis_deep_dives`
**Imports from GRID:** `analysis.money_flow_engine`, `config`, `intelligence.cds_tracker`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`, `intelligence/thesis_tracker.py`

#### `intelligence/chain_contagion.py` — 727 LOC
**Docstring:** Chain contagion simulator.
**Classes:** `ShockSpec` [as_dict]; `ActorImpact` [worst_path]
**Functions:** `simulate_contagion(engine, shock_node_id, shock_type, shock_magnitude, max_depth, pass_through)`
**Reads:** `__future__`, `capital_flows`, `collections`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `shock`, `sqlalchemy`, `supply_chain_edges`, `supply_chain_nodes`, `supply_shock_attributions`, `typing`
**Imports from GRID:** `intelligence.supply_chokepoints`
**Imported by:** `alerts/supply_chain_alerts.py`, `api/routers/contagion.py`, `intelligence/news_contagion_listener.py`

#### `intelligence/sec_filing_extractor.py` — 716 LOC
**Docstring:** GRID Intelligence — SEC Filing Content Extractor.
**Classes:** `MaterialFact` [to_dict]; `SECFilingExtractor` [__init__, extract_from_text, run_extraction, get_recent_facts, get_high_impact_facts]
**Reads:** `__future__`, `content`, `dataclasses`, `datetime`, `first`, `item`, `loguru`, `raw_series`, `sec`, `sec_material_facts`, `sqlalchemy`, `typing`
**Writes:** `sec_material_facts`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/cross_lens.py` — 713 LOC
**Docstring:** GRID Cross-Lens Correlation Detector.
**Classes:** `Attribution` [as_dict]
**Functions:** `resolve_price_series_id(node_id)`, `fetch_close_series(engine, series_id, lookback_days)`, `compute_log_returns(df)`, `lagged_correlation(upstream_returns, downstream_returns, lag_window)`, `detect_shock_events(upstream_returns, downstream_df, window_days, stdev_threshold, max_events)`, `list_candidate_pairs(engine, min_cogs, include_commodity_edges_without_cogs)`, `build_lagged_evidence(upstream_id, downstream_id, correlation, lag, n_obs, pct_cogs)`, `build_event_evidence(upstream_id, downstream_id, shock_magnitude, downstream_move, window_days, pct_cogs)`, `build_actor_narrative(rows)`, `upsert_attributions(engine, attributions)`, `detect_attributions(engine, lookback_days, min_correlation, lag_window)`, `get_attributions_for_actor(engine, actor_id, lookback_days)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `raw_series`, `sqlalchemy`, `supply_chain_edges`, `supply_shock_attributions`, `typing`
**Writes:** `supply_shock_attributions`
**Imported by:** `api/routers/attributions.py`, `intelligence/supply_chain_edge_validator.py`

#### `intelligence/earnings_transcript_analyzer.py` — 685 LOC
**Docstring:** GRID Intelligence — Earnings Transcript Analyzer.
**Classes:** `TranscriptAnalysis` [to_dict]; `ToneScorer` [score_text, classify_tone]; `PhraseExtractor` [extract_guidance, extract_risks, extract_forward, extract_hedges]; `SectionSplitter` [split]; `EarningsTranscriptAnalyzer` [__init__, analyze_transcript, run_analysis, get_analysis, get_tone_shifts]
**Reads:** `__future__`, `dataclasses`, `datetime`, `earnings_analysis`, `edgar_transcripts`, `loguru`, `raw_series`, `sqlalchemy`, `transcript`, `typing`
**Writes:** `earnings_analysis`
**Imported by:** `api/routers/intelligence_news.py`, `ingestion/scheduler.py`

#### `intelligence/news_contagion_listener.py` — 638 LOC
**Docstring:** News-driven contagion listener.
**Classes:** `Candidate` [as_dict]
**Functions:** `detect_patterns(title)`, `resolve_entity(conn, name)`, `scan_news(engine, since_hours, limit)`, `run_once(engine, since_hours, dry_run, limit)`
**Reads:** `__future__`, `analysis`, `contagion_predictions`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `news_articles`, `sqlalchemy`, `supply_chain_nodes`, `typing`
**Writes:** `contagion_predictions`
**Imports from GRID:** `analysis.sector_map`, `intelligence.chain_contagion`

#### `intelligence/fundamental_divergence.py` — 570 LOC
**Docstring:** Fundamental-vs-price divergence detector.
**Classes:** `SectorTicker`
**Functions:** `compute_divergence(engine, as_of)`, `snapshot_all(engine, as_of)`
**Reads:** `__future__`, `analysis`, `capital_flows`, `dataclasses`, `datetime`, `loguru`, `per`, `raw_series`, `sector_map`, `sqlalchemy`, `trichotomy`, `typing`
**Writes:** `fundamental_divergence`
**Imports from GRID:** `analysis.sector_map`

#### `intelligence/regime/state_vector.py` — 562 LOC
**Docstring:** State vector construction for the regime-matched analog engine.
**Classes:** `DimensionSpec`; `StateVector` [array, mask, to_dict]
**Functions:** `compute_state_vector(engine, as_of)`, `compute_state_vector_series(engine, start, end, freq_days)`, `cache_state_vector(engine, sv)`, `load_cached_vectors(engine, min_completeness)`, `get_or_compute_state_vector(engine, as_of, force_recompute)`
**Reads:** `__future__`, `cache`, `cross`, `cross_reference_checks`, `dataclasses`, `datetime`, `loguru`, `other`, `price`, `raw_series`, `regime_state_vectors`, `sec`, `sqlalchemy`, `typing`
**Writes:** `regime_state_vectors`
**Imported by:** `api/routers/intelligence_regime.py`, `intelligence/regime/__init__.py`, `intelligence/regime/classifier.py`, `intelligence/regime/episode_matcher.py`

#### `intelligence/sector_health.py` — 559 LOC
**Docstring:** Sector health composite score.
**Functions:** `compute_sector_health(engine, sector_name)`, `snapshot_all_sectors(engine)`
**Reads:** `__future__`, `analysis`, `capital_flows`, `congressional_trades`, `dark_pool_weekly`, `dataclasses`, `datetime`, `insider_trades`, `loguru`, `ranked`, `sector_health_snapshots`, `sqlalchemy`, `supply_chain_edges`, `typing`
**Writes:** `sector_health_snapshots`
**Imports from GRID:** `analysis.sector_map`
**Imported by:** `api/routers/sector_health.py`

#### `intelligence/news_intel.py` — 559 LOC
**Docstring:** GRID Intelligence — News Intelligence & Narrative Analysis.
**Functions:** `get_news_feed(engine, ticker, hours)`, `get_news_stats(engine, hours)`, `detect_narrative_shift(engine, ticker, days)`, `find_news_before_move(engine, ticker, move_date)`, `generate_news_briefing(engine)`
**Reads:** `__future__`, `business_events`, `datetime`, `loguru`, `signal_data`, `sqlalchemy`, `top`, `typing`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_news.py`

#### `intelligence/holder_deal_overlap.py` — 553 LOC
**Docstring:** Holder / deal overlap detector — "pre-positioning" cross-reference.
**Classes:** `OverlapRow`; `RunStats` [to_dict]
**Functions:** `find_deals(engine)`, `detect_overlap_for_deal(engine)`, `upsert_rows(engine, rows)`, `run(engine)`, `fetch_overlaps_for_actor(engine, actor_id)`
**Reads:** `__future__`, `acquirer_cutoff`, `acquirer_leg`, `announcement`, `capital_flows`, `corporate_actions_parser`, `dataclasses`, `datetime`, `holder_deal_overlap`, `institutional_holdings`, `loguru`, `next_reports`, `sqlalchemy`, `target_cutoff`, `target_leg`, `typing`
**Writes:** `holder_deal_overlap`
**Imported by:** `api/routers/actor_detail.py`

#### `intelligence/prediction_calibration.py` — 523 LOC
**Docstring:** GRID Prediction Market Calibration Checker.
**Classes:** `PredictionCalibrationChecker` [__init__, check_cross_platform, check_fundamental, run_checks]
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `prediction`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.cross_reference`

#### `intelligence/legislative_intel.py` — 481 LOC
**Docstring:** GRID Intelligence — Legislative Trading Detection.
**Classes:** `LegislativeHearing`; `BillImpact`; `LegislativeTradeAlert`
**Functions:** `get_upcoming_hearings(engine, days)`, `get_bills_affecting_ticker(engine, ticker)`, `detect_legislative_trading(engine, days_back)`, `get_legislation_summary(engine)`
**Reads:** `__future__`, `both`, `collections`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.altdata.legislation`
**Imported by:** `api/routers/intelligence_govflow.py`, `intelligence/company_analyzer.py`

#### `intelligence/supply_chokepoints.py` — 465 LOC
**Docstring:** GRID Intelligence — Supply Chain Chokepoint Scoring.
**Classes:** `EdgeContext`; `ScoreBreakdown` [as_dict]
**Functions:** `substitution_penalty(alt_count)`, `buyer_concentration(pct_downstream_cogs, annual_usd, downstream_total_annual_usd)`, `geographic_concentration(country_counts)`, `historical_disruption()`, `compute_chokepoint_score(edge, context)`, `find_alternatives(conn, downstream_id, input_type)`, `score_all_edges(engine)`, `flag_chokepoint_nodes(engine, threshold)`
**Reads:** `__future__`, `dataclasses`, `high_edges`, `loguru`, `sqlalchemy`, `supply_chain_edges`, `supply_chain_nodes`, `typing`
**Writes:** `supply_chain_edges`
**Imported by:** `intelligence/chain_contagion.py`

#### `intelligence/obsidian_agent.py` — 465 LOC
**Docstring:** Obsidian Agent — active intelligence loop for the vault.
**Functions:** `extract_entities(body)`, `rank_for_review(items)`, `should_escalate_to_paid(result)`, `enrich_note(conn, note_id, body)`, `act_on_approval(conn, note)`, `compute_preferences(actions)`, `build_proactive_note(event_type, title, body, domain, tags, priority)`, `create_proactive_note(engine, event_type, title, body, domain, tags, priority)`, `run_agent_cycle(engine)`
**Reads:** `__future__`, `actors`, `alpha`, `approval`, `datetime`, `grid`, `ingestion`, `loguru`, `note`, `obsidian_actions`, `obsidian_notes`, `signal_registry`, `sqlalchemy`, `typing`, `user`
**Writes:** `obsidian_actions`, `obsidian_notes`, `oracle_predictions`
**Imports from GRID:** `ingestion.altdata.obsidian_sync`

#### `intelligence/opsec.py` — 459 LOC
**Docstring:** GRID Intelligence Operations Security (OPSEC) Module.
**Classes:** `AuditLogger` [__init__, log_access, log_sensitive_query, get_recent]; `EncryptedIntelStore` [__init__, store, retrieve, search]
**Functions:** `user_can_view(user_tier, data_confidence)`, `get_user_tier(role)`, `audit_sensitive(action, risk_level)`
**Reads:** `__future__`, `datetime`, `encrypted_intelligence`, `functools`, `grid_intel_key`, `grid_jwt_secret`, `loguru`, `security_audit_log`, `sqlalchemy`, `static`, `typing`
**Writes:** `encrypted_intelligence`, `security_audit_log`
**Imports from GRID:** `api.dependencies`
**Imported by:** `subnet/validator.py`

#### `intelligence/actors/trial_bridge.py` — 457 LOC
**Docstring:** GRID Intelligence — Trial Sponsor → Actor Network bridge.
**Functions:** `sync_trial_sponsors_to_actors(conn)`
**Reads:** `__future__`, `actors`, `collections`, `griddb`, `sponsor`, `trial_cache`, `trial_ingestor`, `typing`
**Writes:** `actors`, `wealth_flows`

#### `intelligence/supply_chain_edge_validator.py` — 450 LOC
**Docstring:** GRID Supply-Chain Edge Validator.
**Classes:** `EdgeRow`; `ValidationResult`
**Functions:** `list_edges(engine, limit)`, `compute_edge_correlation(engine, upstream_id, downstream_id, lookback_days)`, `next_edge_state(correlation, prior_weak_since, today, floor, min_duration_days)`, `persist_result(engine, edge_id, correlation, weak_since, relationship_weak)`, `validate_edge(engine, edge, today, lookback_days)`, `validate_all_edges(engine, limit, lookback_days, today)`, `summarise_results(results)`, `run_weekly(engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `five`, `intelligence`, `loguru`, `sqlalchemy`, `supply_chain_edges`, `typing`
**Writes:** `supply_chain_edges`
**Imports from GRID:** `db`, `intelligence.cross_lens`

#### `intelligence/export_intel.py` — 439 LOC
**Docstring:** GRID Intelligence — Export Controls Analysis.
**Classes:** `ExportControlRecord` [to_dict]; `RevenueImpactAssessment` [to_dict]
**Functions:** `get_recent_controls(engine, days)`, `get_controls_for_ticker(engine, ticker)`, `assess_revenue_impact(engine, ticker)`
**Reads:** `__future__`, `critical`, `dataclasses`, `datetime`, `export`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_forensics.py`, `intelligence/company_analyzer.py`

#### `intelligence/regime/classifier.py` — 437 LOC
**Docstring:** Regime classification engine.
**Classes:** `RegimeLabel` [to_dict]; `RegimeClassification` [to_dict]
**Functions:** `classify_regime(sv)`, `classify_regime_with_history(engine, sv, lookback_days)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `historical`, `history`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.regime.state_vector`
**Imported by:** `api/routers/intelligence_regime.py`, `intelligence/regime/__init__.py`

#### `intelligence/actors/graph.py` — 418 LOC
**Docstring:** GRID Intelligence — Actor graph construction and network traversal.
**Functions:** `build_actor_graph(engine)`, `find_connected_actions(engine, actor_id)`
**Reads:** `__future__`, `actor`, `actor_connections`, `collections`, `datetime`, `intelligence`, `loguru`, `signal_sources`, `sqlalchemy`
**Imports from GRID:** `intelligence.actors.db`, `intelligence.actors.models`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/analysis.py`

#### `intelligence/actor_researcher.py` — 416 LOC
**Docstring:** Actor Researcher — local LLM agent that continuously enriches actor profiles.
**Functions:** `find_sparse_actors(engine, limit)`, `gather_evidence(engine, actor_name, actor_id)`, `enrich_actor_with_llm(engine, actor, evidence)`, `update_actor_profile(engine, actor_id, profile)`, `research_batch(engine, batch_size)`, `run_continuous(engine, rounds, batch_size)`
**Reads:** `__future__`, `actor_connections`, `actors`, `anywhere`, `datetime`, `evidence`, `intelligence`, `llm`, `loguru`, `provided`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `actors`, `raw_series`
**Imports from GRID:** `db`, `intelligence.actor_ingest`

#### `intelligence/image_gen.py` — 410 LOC
**Docstring:** GRID — AI Image Generation via Gemini Imagen.
**Classes:** `ImageResult` [to_dict]
**Functions:** `generate_flow_infographic(engine, style, model_tier)`, `generate_sector_heatmap(engine, style, model_tier)`, `generate_junction_dashboard(engine, style, model_tier)`, `generate_market_briefing_image(engine, style, model_tier)`, `generate_custom(prompt, style, model_tier)`, `generate_daily_briefing_pack(engine, style)`
**Reads:** `__future__`, `analysis`, `any`, `dataclasses`, `datetime`, `google`, `grid`, `image`, `left`, `live`, `loguru`, `pathlib`, `source`, `typing`
**Imports from GRID:** `analysis.flow_aggregator`, `analysis.flow_thesis`, `analysis.money_flow_engine`
**Imported by:** `api/routers/flows.py`

#### `intelligence/cds_tracker.py` — 408 LOC
**Docstring:** GRID — CDS (Credit Default Swap) Tracker.
**Classes:** `SpreadSnapshot`; `CDSDashboard`
**Functions:** `build_spread_snapshot(engine, key, cfg, as_of)`, `build_cds_dashboard(engine, as_of)`, `cds_to_dict(dashboard)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `raw_series`, `spread`, `sqlalchemy`, `typing`
**Imported by:** `analysis/money_flow_engine/layer_credit.py`, `api/routers/flows.py`, `intelligence/audio_briefing.py`, `intelligence/deep_dive.py`

#### `intelligence/news_ticker_resolver.py` — 407 LOC
**Docstring:** News ticker resolver — extract real ticker symbols from news title+content.
**Functions:** `resolve_tickers(title, description, fallback_payload_tickers)`
**Reads:** `__future__`, `analysis`, `article`, `functools`, `news`, `sector_map`, `tiingo`
**Imports from GRID:** `analysis.sector_map`
**Imported by:** `ingestion/altdata/news_scraper.py`

#### `intelligence/post_query_scanner.py` — 383 LOC
**Docstring:** GRID — Post-Query Data Gap Scanner.
**Functions:** `scan_data_gaps(engine, question, ticker, sources_used)`, `spawn_post_query_scan(engine, question, ticker, sources_used)`
**Reads:** `__future__`, `chat`, `core_signals`, `datetime`, `feature_registry`, `ingestion`, `loguru`, `question`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `query_data_gaps`
**Imports from GRID:** `ingestion.coingecko`, `ingestion.fred`, `ingestion.openbb_pipeline`, `ingestion.price_fallback`, `ingestion.social_sentiment`
**Imported by:** `api/routers/chat.py`, `intelligence/freshness_guard.py`

#### `intelligence/contagion_backtest.py` — 380 LOC
**Docstring:** Contagion backtest scorer.
**Functions:** `compute_accuracy(predicted_margin_impact_pct, actual_price_move_pct)`, `score_predictions(engine, as_of_days_ago)`, `score_all_windows(engine)`
**Reads:** `__future__`, `contagion_predictions`, `dataclasses`, `datetime`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Writes:** `contagion_backtest_results`

#### `intelligence/regime/episode_matcher.py` — 361 LOC
**Docstring:** Episode matching engine for the regime analog system.
**Classes:** `MatchedEpisode` [to_dict]; `MatchResult` [to_dict]
**Functions:** `find_analogous_episodes(engine, query_vector, n, min_quality, exclusion_window_days, max_candidates)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `different`, `intelligence`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.regime.state_vector`
**Imported by:** `api/routers/intelligence_regime.py`, `intelligence/regime/__init__.py`, `intelligence/regime/forecast.py`

#### `intelligence/regime/forecast.py` — 355 LOC
**Docstring:** Conditional forecast generation from matched historical episodes.
**Classes:** `OutcomeDistribution` [to_dict]; `ConditionalForecast` [to_dict]
**Functions:** `generate_conditional_forecast(engine, match_result, tickers, horizons)`
**Reads:** `__future__`, `all`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `match`, `matched`, `typing`
**Imports from GRID:** `intelligence.regime.episode_matcher`
**Imported by:** `api/routers/intelligence_regime.py`, `intelligence/regime/__init__.py`

#### `intelligence/breaking_news.py` — 341 LOC
**Docstring:** Breaking news monitor — detects high-impact events in near-real-time.
**Functions:** `check_gdelt(query, minutes)`, `detect_spike(article_count, baseline_per_hour, timespan_minutes, multiplier)`, `infer_direction(query, titles)`, `inject_signal(engine, event)`, `invalidate_caches()`, `run_monitor(interval, once)`
**Reads:** `__future__`, `article`, `datetime`, `loguru`, `pathlib`, `query`, `sqlalchemy`, `typing`
**Writes:** `signal_data`
**Imports from GRID:** `db`

#### `intelligence/signal_backlinker.py` — 324 LOC
**Docstring:** Signal Backlinker — closes the loop between signals and the actor graph.
**Functions:** `is_real_actor(name, signal_type)`, `backlink_signals(engine, batch_size, since_minutes)`, `update_trust_from_signal_density(engine)`, `run_backlinker(interval, lookback_minutes)`
**Reads:** `__future__`, `actors`, `datetime`, `last`, `loguru`, `signal`, `signal_data`, `sqlalchemy`, `typing`
**Writes:** `actor_connections`, `actors`
**Imports from GRID:** `db`

#### `intelligence/signal_extractor.py` — 322 LOC
**Docstring:** Signal Extractor — bridges raw_series → signal_data with actor attribution.
**Functions:** `extract_from_raw_series(engine, batch_size)`, `extract_from_signal_sources(engine, batch_size)`, `run_extractor(interval)`
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `signal_data`, `signal_sources`, `sqlalchemy`, `typing`, `value`
**Writes:** `signal_data`
**Imports from GRID:** `db`

#### `intelligence/actors/analysis.py` — 317 LOC
**Docstring:** GRID Intelligence — Actor network analysis functions.
**Functions:** `get_actor_context_for_ticker(engine, ticker)`, `enrich_lever_pullers_with_actors(engine)`, `generate_actor_report(engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `intelligence`, `lever_pullers`, `loguru`, `signal_sources`, `sqlalchemy`, `tracked`, `typing`
**Writes:** `lever_pullers`
**Imports from GRID:** `intelligence.actor_network`, `intelligence.actors.db`, `intelligence.actors.graph`, `intelligence.actors.models`, `intelligence.lever_pullers`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`

#### `intelligence/codebase_context.py` — 307 LOC
**Docstring:** GRID Codebase Context — dynamic state injected into every LLM prompt.
**Functions:** `get_system_context()`
**Reads:** `__future__`, `analytical_snapshots`, `datetime`, `dollar_flows`, `feature_registry`, `intelligence`, `loguru`, `oracle_predictions`, `resolved_series`, `sqlalchemy`, `thesis_tracker`, `trust_scorer`, `typing`
**Imports from GRID:** `db`, `intelligence.cross_reference`, `intelligence.lever_pullers`, `intelligence.thesis_tracker`, `intelligence.trust_scorer`
**Imported by:** `api/routers/chat.py`

#### `intelligence/gov_intel.py` — 297 LOC
**Docstring:** GRID Intelligence — Government Contract Analysis.
**Classes:** `ContractRecord` [to_dict]; `InsiderContractOverlap` [to_dict]
**Functions:** `get_recent_contracts(engine, days)`, `get_contracts_for_ticker(engine, ticker)`, `detect_contract_insider_overlap(engine, lookback_days, pre_contract_window_days)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `raw_series`, `signal_sources`, `source_catalog`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_govflow.py`, `intelligence/company_analyzer.py`

#### `intelligence/actor_signal_bridge.py` — 292 LOC
**Docstring:** Actor Signal Bridge — injects actor intelligence into the prediction pipeline.
**Functions:** `get_actor_signals_for_ticker(engine, ticker, days)`, `get_actor_trust_weights(engine, ticker, days)`, `get_actor_context_for_causation(engine, ticker, days)`, `enrich_signals_with_actors(engine, signals, ticker)`, `sync_actor_trust_to_signal_sources(engine)`
**Reads:** `__future__`, `actor_connections`, `actors`, `datetime`, `loguru`, `signal_data`, `signal_sources`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/causation_scoring.py`, `intelligence/trust_scorer.py`, `oracle/engine.py`

#### `intelligence/pocket_lining.py` — 286 LOC
**Docstring:** GRID Intelligence — Pocket-Lining Detection.
**Functions:** `assess_pocket_lining(engine)`
**Reads:** `__future__`, `collections`, `datetime`, `fund`, `fund_trades`, `insider_trades`, `intelligence`, `loguru`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.actor_network`
**Imported by:** `intelligence/actor_network.py`

#### `intelligence/spider/graph_engine.py` — 270 LOC
**Docstring:** In-memory actor graph with microsecond traversal.
**Classes:** `GraphEngine` [__init__, add_actor, add_connection, remove_actor, actor_count, connection_count, has_actor, get_actor]
**Reads:** `__future__`, `actor_connections`, `actors`, `collections`, `intelligence`, `loguru`, `postgres`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`, `intelligence/spider/discovery.py`, `intelligence/spider/entity_resolver.py`

#### `intelligence/adapters/ai_trader_adapter.py` — 267 LOC
**Docstring:** GRID Intelligence — AI-Trader Signal Adapter.
**Classes:** `AITraderAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `agent`, `config`, `datetime`, `intelligence`, `leaderboard`, `loguru`, `parts`, `sqlalchemy`, `top`, `typing`
**Imports from GRID:** `config`, `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/milestone_tracker.py` — 264 LOC
**Docstring:** Milestone Tracker — plot company milestones on a timeline, score execution.
**Classes:** `Milestone`
**Functions:** `build_earnings_timeline(engine, ticker)`, `score_execution(milestones)`, `scan_all_tickers(engine, tickers)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `earnings`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/context_provider.py` — 252 LOC
**Docstring:** Context provider for LLM prompt injection.
**Functions:** `get_active_hypotheses(engine, limit)`, `get_recent_postmortems(engine, days, limit)`, `get_company_context(engine, tickers, limit)`, `get_hypothesis_context_for_ticker(engine, ticker)`, `build_full_context(engine, tickers, max_hypotheses, max_postmortems, max_companies)`
**Reads:** `__future__`, `company_profiles`, `datetime`, `discovered_hypotheses`, `failed`, `hypothesis_postmortems`, `loguru`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/audio_briefing.py`, `intelligence/cross_reference.py`, `intelligence/market_diary.py`, `intelligence/sleuth.py`, `intelligence/thesis_tracker.py`, `ollama/market_briefing.py`

#### `intelligence/scheduler.py` — 248 LOC
**Docstring:** GRID Intelligence Scheduler — background loop for periodic intelligence tasks.
**Functions:** `run_intelligence_loop()`
**Reads:** `__future__`, `analysis`, `config`, `feature_registry`, `ingestion`, `lateral`, `loguru`, `ollama`, `resolved_series`, `sqlalchemy`, `trading`
**Imports from GRID:** `analysis.astro_correlations`, `analysis.capital_flows`, `analysis.research_agent`, `analysis.taxonomy_audit`, `config`, `db`, `ingestion.coingecko`, `ingestion.crucix_bridge`, `ingestion.price_fallback`, `ingestion.social_sentiment`, `ingestion.wiki_history`, `ollama.celestial_briefing`, `ollama.dealer_flow_briefing`, `ollama.market_briefing`, `trading.options_recommender` (+2)

#### `intelligence/wealth_tracker.py` — 233 LOC
**Docstring:** GRID Intelligence — Wealth Tracking & Migration.
**Functions:** `track_wealth_migration(engine, days)`, `persist_wealth_flows(engine, flows)`
**Reads:** `__future__`, `actor_network`, `datetime`, `intelligence`, `loguru`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `wealth_flows`
**Imports from GRID:** `intelligence.actor_network`
**Imported by:** `intelligence/actor_network.py`

#### `intelligence/actor_ingest.py` — 228 LOC
**Docstring:** Universal Actor Ingestion — auto-discover and log actors from ANY data source.
**Functions:** `ingest_actor(engine, name, actor_type, source, country, confidence, metadata)`, `ingest_actors_batch(engine, actors, source, country)`, `extract_actors_from_payload(engine, payload, source, name_fields)`, `get_actor_count(engine)`, `get_actor_sources(engine)`
**Reads:** `__future__`, `actors`, `all`, `any`, `datetime`, `field`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `actors`
**Imported by:** `ingestion/altdata/defi_llama_puller.py`, `ingestion/altdata/etherscan_puller.py`, `ingestion/altdata/findkg_puller.py`, `ingestion/altdata/icij_puller.py`, `ingestion/altdata/indeed_hiring_puller.py`, `ingestion/altdata/opensecrets_puller.py`, `ingestion/altdata/redfin_puller.py`, `ingestion/international/world_bank_puller.py`, `intelligence/actor_researcher.py`

#### `intelligence/actors/db.py` — 217 LOC
**Docstring:** GRID Intelligence — Actor Network database layer.
**Reads:** `__future__`, `actors`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `actors`
**Imports from GRID:** `intelligence.actors.models`, `intelligence.actors.seed_data`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/analysis.py`, `intelligence/actors/graph.py`

#### `intelligence/icij_linker.py` — 197 LOC
**Docstring:** ICIJ Linker — fuzzy-match ICIJ offshore entities against the actor network.
**Classes:** `ActorMatch`
**Functions:** `link_actors(engine, min_similarity, limit)`, `get_offshore_connections(engine, actor_name)`
**Reads:** `__future__`, `actors`, `dataclasses`, `icij_actor_matches`, `icij_entities`, `icij_officers`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `icij_actor_matches`
**Imports from GRID:** `intelligence.actor_network`

#### `intelligence/causation_core.py` — 195 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine (core module).
**Classes:** `CausalLink` [to_dict]; `CausalChain` [to_dict]
**Functions:** `ensure_table(engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `first`, `loguru`, `root`, `sqlalchemy`, `typing`
**Imported by:** `intelligence/causation.py`, `intelligence/causation_graph.py`, `intelligence/causation_scoring.py`

#### `intelligence/signal_registry.py` — 191 LOC
**Docstring:** GRID Intelligence — Signal Registry.
**Classes:** `SignalType`; `Direction`; `RegisteredSignal`; `SignalRegistry` [register, query, query_for_ticker, query_by_source, prune_expired, get_signal_count]
**Functions:** `make_signal_id(source_module, key)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `loguru`, `signal_registry`, `sqlalchemy`, `typing`
**Writes:** `signal_registry`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `api/routers/signal_registry.py`, `intelligence/adapters/ai_trader_adapter.py`, `intelligence/adapters/base.py`, `intelligence/adapters/cross_reference_adapter.py`, `intelligence/adapters/dollar_flows_adapter.py`, `intelligence/adapters/earnings_adapter.py`, `intelligence/adapters/feature_adapter.py`, `intelligence/adapters/flow_thesis_adapter.py`, `intelligence/adapters/forensics_adapter.py` (+7)

#### `intelligence/attention_anomaly.py` — 185 LOC
**Docstring:** Attention Anomaly Detector — combines Wikipedia + Google Trends signals.
**Classes:** `AttentionSignal`
**Functions:** `score_attention(engine, lookback_days)`, `enrich_with_price_action(engine, signals)`, `get_alerts(engine, threshold)`
**Reads:** `__future__`, `attention_anomaly`, `dataclasses`, `datetime`, `feature_registry`, `loguru`, `resolved_series`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/intelligence_actors.py`

#### `intelligence/spider/sources/icij_offshore.py` — 165 LOC
**Docstring:** ICIJ Offshore Leaks adapter — discovers offshore entity connections.
**Classes:** `IcijOffshoreAdapter` [discover]
**Reads:** `__future__`, `intelligence`, `loguru`, `pathlib`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/freshness_guard.py` — 155 LOC
**Docstring:** GRID — Feature Freshness Guard.
**Classes:** `FreshnessStatus`
**Functions:** `check_freshness(engine, feature_names)`, `log_stale_features(statuses, caller)`
**Reads:** `__future__`, `check_freshness`, `dataclasses`, `datetime`, `feature_registry`, `intelligence`, `loguru`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.post_query_scanner`
**Imported by:** `intelligence/causation_scoring.py`, `intelligence/company_analyzer.py`, `intelligence/forensics.py`, `intelligence/thesis_tracker.py`

#### `intelligence/actor_network.py` — 153 LOC
**Docstring:** GRID Intelligence — Actor Network & Power Structure Map.
**Functions:** `track_wealth_migration(engine, days)`, `assess_pocket_lining(engine)`, `persist_wealth_flows(engine, flows)`
**Reads:** `__future__`, `intelligence`
**Imports from GRID:** `intelligence.actors.analysis`, `intelligence.actors.db`, `intelligence.actors.graph`, `intelligence.actors.ingestion`, `intelligence.actors.models`, `intelligence.actors.seed_data`, `intelligence.pocket_lining`, `intelligence.wealth_tracker`
**Imported by:** `analysis/money_flow.py`, `api/routers/intelligence_actors.py`, `api/routers/watchlist_overview.py`, `ingestion/altdata/offshore_leaks.py`, `intelligence/actors/analysis.py`, `intelligence/company_analyzer.py`, `intelligence/deep_graph.py`, `intelligence/icij_actor_discovery.py`, `intelligence/icij_linker.py`, `intelligence/pocket_lining.py` (+2)

#### `intelligence/spider/sources/google_kg.py` — 149 LOC
**Docstring:** Google Knowledge Graph adapter — discovers structured entity relationships.
**Classes:** `GoogleKgAdapter` [discover]
**Reads:** `__future__`, `description`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/source_trust_config.py` — 148 LOC
**Docstring:** GRID Source Trust Configuration.
**Functions:** `get_trust(source_key)`, `trust_color(score)`, `trust_label(score)`
**Imported by:** `intelligence/trust_scorer.py`

#### `intelligence/spider/sources/opencorporates.py` — 147 LOC
**Docstring:** OpenCorporates adapter — discovers corporate registry connections.
**Classes:** `OpenCorporatesAdapter` [discover]
**Reads:** `__future__`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/actors/ingestion.py` — 142 LOC
**Docstring:** GRID Intelligence — Actor Network data ingestion.
**Functions:** `ingest_panama_pandora_data(data_dir)`
**Reads:** `__future__`, `intelligence`, `loguru`, `pathlib`
**Imports from GRID:** `intelligence.actors.seed_data`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`

#### `intelligence/spider/daemon.py` — 130 LOC
**Docstring:** Spider daemon — continuous connection mapping loop.
**Functions:** `run_spider(max_rounds, sleep_between)`
**Reads:** `__future__`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `db`, `intelligence.spider.db`, `intelligence.spider.discovery`, `intelligence.spider.entity_resolver`, `intelligence.spider.graph_engine`, `intelligence.spider.priority_queue`, `intelligence.spider.sources.google_kg`, `intelligence.spider.sources.icij_offshore`, `intelligence.spider.sources.news_cooccurrence`, `intelligence.spider.sources.opencorporates`, `intelligence.spider.sources.operator_input`, `intelligence.spider.sources.sec_crossref`, `intelligence.spider.sources.wikidata`

#### `intelligence/spider/sources/sec_crossref.py` — 119 LOC
**Docstring:** SEC EDGAR cross-reference adapter — discovers entity relationships from SEC filings.
**Classes:** `SecCrossRefAdapter` [discover]
**Reads:** `__future__`, `actor`, `display_names`, `intelligence`, `loguru`, `sec`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/adapters/flow_thesis_adapter.py` — 116 LOC
**Docstring:** GRID Intelligence — Flow Thesis Signal Adapter.
**Classes:** `FlowThesisAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `analysis`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `analysis.flow_thesis`, `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/spider/sources/news_cooccurrence.py` — 114 LOC
**Docstring:** GDELT news co-occurrence adapter — discovers entity co-mentions in news.
**Classes:** `NewsCooccurrenceAdapter` [discover]
**Reads:** `__future__`, `collections`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/adapters/sector_network_adapter.py` — 336 LOC
**Docstring:** GRID Signal Adapter — Sector Networks. Actor density + concentration signals from 10 sector modules.
**Classes:** `SectorNetworkAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/trust_scorer_adapter.py` — 104 LOC
**Docstring:** GRID Signal Adapter — Trust Scorer. Convergence + per-source trust signals.
**Classes:** `TrustScorerAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `collections`, `datetime`, `intelligence`, `loguru`, `signal_sources`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/spider/discovery.py` — 101 LOC
**Docstring:** Discovery orchestrator — fans out to source adapters and deduplicates results.
**Classes:** `DiscoveryOrchestrator` [__init__, expand]
**Reads:** `__future__`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `intelligence.spider.entity_resolver`, `intelligence.spider.graph_engine`, `intelligence.spider.models`, `intelligence.spider.sources`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`

#### `intelligence/adapters/news_adapter.py` — 93 LOC
**Docstring:** GRID Signal Adapter — News Intel. Sentiment momentum + volume signals per ticker.
**Classes:** `NewsAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `collections`, `datetime`, `intelligence`, `loguru`, `news_articles`, `news_impact_catalysts`, `recent`, `sqlalchemy`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/spider/sources/operator_input.py` — 87 LOC
**Docstring:** Operator input adapter — retrieves manually injected connections from DB.
**Classes:** `OperatorInputAdapter` [discover]
**Reads:** `__future__`, `actor_connections`, `intelligence`, `loguru`, `postgresql`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`, `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/spider/sources/wikidata.py` — 87 LOC
**Docstring:** Wikidata SPARQL adapter — discovers structured relationships for public figures.
**Classes:** `WikidataAdapter` [discover]
**Reads:** `__future__`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/daemon.py`

#### `intelligence/adapters/forensics_adapter.py` — 80 LOC
**Docstring:** GRID Signal Adapter — Forensic Analyzer. Warning count + directional signals per ticker.
**Classes:** `ForensicsAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `forensic_reports`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/feature_adapter.py` — 80 LOC
**Docstring:** GRID Signal Adapter — Feature Store bridge. Z-score signals from resolved_series.
**Classes:** `FeatureAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `feature_registry`, `intelligence`, `loguru`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/earnings_adapter.py` — 78 LOC
**Docstring:** GRID Signal Adapter — Earnings Intel. Upcoming earnings + historical surprise signals.
**Classes:** `EarningsAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `earnings_calendar`, `intelligence`, `loguru`, `sqlalchemy`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/dollar_flows_adapter.py` — 75 LOC
**Docstring:** GRID Signal Adapter — Dollar Flows. Net flow direction + magnitude per sector.
**Classes:** `DollarFlowsAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `dollar_flows`, `intelligence`, `loguru`, `sqlalchemy`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/thesis_tracker_adapter.py` — 75 LOC
**Docstring:** GRID Signal Adapter — Thesis Tracker. Latest market thesis direction + accuracy.
**Classes:** `ThesisTrackerAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `postmortems`, `sqlalchemy`, `thesis_postmortems`, `thesis_snapshots`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/spider/priority_queue.py` — 75 LOC
**Docstring:** Composite-scored expansion queue for the spider daemon.
**Classes:** `PriorityQueue` [__init__, compute_score, push, pop, mark_done, depth, total_done]
**Reads:** `__future__`, `loguru`, `typing`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/daemon.py`

#### `intelligence/adapters/cross_reference_adapter.py` — 73 LOC
**Docstring:** GRID Signal Adapter — Cross-Reference (Lie Detector). Divergence signals.
**Classes:** `CrossReferenceAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `cross_reference_checks`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/lever_pullers_adapter.py` — 70 LOC
**Docstring:** GRID Signal Adapter — Lever Pullers. Per-ticker directional signals from actor events.
**Classes:** `LeverPullersAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `actor`, `datetime`, `intelligence`, `lever_pullers`, `loguru`, `signal_sources`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/adapters/pattern_adapter.py` — 66 LOC
**Docstring:** GRID Signal Adapter — Pattern Engine. Active recognized patterns with hit rates.
**Classes:** `PatternAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `event_patterns`, `intelligence`, `loguru`, `sqlalchemy`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/actors/__init__.py` — 60 LOC
**Docstring:** GRID Intelligence — actors subpackage.
**Reads:** `intelligence`
**Imports from GRID:** `intelligence.actors.analysis`, `intelligence.actors.db`, `intelligence.actors.graph`, `intelligence.actors.ingestion`, `intelligence.actors.models`, `intelligence.actors.seed_data`

#### `intelligence/regime/__init__.py` — 60 LOC
**Docstring:** GRID Regime-Matched Historical Analog Engine.
**Reads:** `intelligence`
**Imports from GRID:** `intelligence.regime.classifier`, `intelligence.regime.episode_matcher`, `intelligence.regime.forecast`, `intelligence.regime.state_vector`

#### `intelligence/adapters/base.py` — 56 LOC
**Docstring:** GRID Intelligence — Signal Adapter Protocol and Registry.
**Classes:** `SignalAdapter` [source_module, refresh_interval_hours, extract_signals]; `AdapterRegistry` [__init__, adapters, refresh_all]
**Reads:** `__future__`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `api/routers/signal_registry.py`

#### `intelligence/actors/models.py` — 55 LOC
**Docstring:** GRID Intelligence — Actor Network data models.
**Classes:** `Actor`; `WealthFlow`
**Reads:** `__future__`, `dataclasses`
**Imported by:** `intelligence/actor_network.py`, `intelligence/actors/__init__.py`, `intelligence/actors/analysis.py`, `intelligence/actors/db.py`, `intelligence/actors/graph.py`

#### `intelligence/adapters/sleuth_adapter.py` — 52 LOC
**Docstring:** GRID Signal Adapter — Sleuth. Active investigation leads as signals.
**Classes:** `SleuthAdapter` [source_module, refresh_interval_hours, extract_signals]
**Reads:** `__future__`, `datetime`, `intelligence`, `investigation_leads`, `loguru`, `sqlalchemy`
**Imports from GRID:** `intelligence.signal_registry`
**Imported by:** `intelligence/adapters/__init__.py`

#### `intelligence/spider/models.py` — 41 LOC
**Docstring:** Data models for the connection mapping spider.
**Classes:** `DiscoveredConnection`; `ConnectionMeta`; `SpiderStats`
**Reads:** `__future__`, `dataclasses`, `typing`
**Imported by:** `intelligence/spider/__init__.py`, `intelligence/spider/db.py`, `intelligence/spider/discovery.py`, `intelligence/spider/graph_engine.py`, `intelligence/spider/sources/__init__.py`, `intelligence/spider/sources/google_kg.py`, `intelligence/spider/sources/icij_offshore.py`, `intelligence/spider/sources/news_cooccurrence.py`, `intelligence/spider/sources/opencorporates.py`, `intelligence/spider/sources/operator_input.py` (+2)

#### `intelligence/adapters/__init__.py` — 36 LOC
**Docstring:** Signal adapters — wrap intelligence modules into RegisteredSignal producers.
**Reads:** `intelligence`
**Imports from GRID:** `intelligence.adapters.ai_trader_adapter`, `intelligence.adapters.cross_reference_adapter`, `intelligence.adapters.dollar_flows_adapter`, `intelligence.adapters.earnings_adapter`, `intelligence.adapters.feature_adapter`, `intelligence.adapters.flow_thesis_adapter`, `intelligence.adapters.forensics_adapter`, `intelligence.adapters.lever_pullers_adapter`, `intelligence.adapters.news_adapter`, `intelligence.adapters.pattern_adapter`, `intelligence.adapters.sector_network_adapter`, `intelligence.adapters.sleuth_adapter`, `intelligence.adapters.thesis_tracker_adapter`, `intelligence.adapters.trust_scorer_adapter`
**Imported by:** `api/routers/signal_registry.py`

#### `intelligence/causation.py` — 26 LOC
**Docstring:** GRID Intelligence — Causal Connection Engine.
**Reads:** `intelligence`
**Imports from GRID:** `intelligence.causation_core`, `intelligence.causation_graph`, `intelligence.causation_scoring`
**Imported by:** `api/routers/intelligence_forensics.py`

#### `intelligence/spider/__init__.py` — 22 LOC
**Docstring:** GRID Connection Mapping Spider — discovers and maps actor relationships.
**Reads:** `intelligence`
**Imports from GRID:** `intelligence.spider.discovery`, `intelligence.spider.entity_resolver`, `intelligence.spider.graph_engine`, `intelligence.spider.models`, `intelligence.spider.priority_queue`

#### `intelligence/spider/sources/__init__.py` — 20 LOC
**Docstring:** Source adapter protocol for the connection mapping spider.
**Classes:** `BaseSourceAdapter` [discover]
**Reads:** `__future__`, `intelligence`, `typing`
**Imports from GRID:** `intelligence.spider.models`
**Imported by:** `intelligence/spider/discovery.py`

#### `intelligence/__init__.py` — 2 LOC

### `ingestion/` (185 modules, 71,104 LOC)

#### `ingestion/scheduler.py` — 1524 LOC
**Docstring:** GRID unified ingestion scheduler.
**Functions:** `run_pull_group(group_name, db_engine, config, skip_sources, step_callback)`, `backfill_all(start_date)`, `run_pushshift_backfill(data_dir)`, `run_daily_pulls(start_date)`, `run_monthly_pulls(start_date)`, `start_scheduler()`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `discovery`, `feature_registry`, `fred`, `ingestion`, `intelligence`, `lateral`, `loguru`, `news`, `pushshiftredditpuller`, `raw_series`, `realtime_candles`, `resolved_series`, `scripts`, `source_catalog`, `sqlalchemy`, `tqdm`, `typing`, `yfinance`
**Imports from GRID:** `alerts.email`, `config`, `db`, `discovery.options_scanner`, `ingestion.altdata.aaii_sentiment`, `ingestion.altdata.alphavantage_sentiment`, `ingestion.altdata.analyst_ratings`, `ingestion.altdata.binance_puller`, `ingestion.altdata.campaign_finance`, `ingestion.altdata.cboe_indices`, `ingestion.altdata.cloudflare_radar_puller`, `ingestion.altdata.congressional`, `ingestion.altdata.cryptoquant_puller`, `ingestion.altdata.dark_pool`, `ingestion.altdata.defi_llama_puller` (+109)

#### `ingestion/altdata/foia_cables.py` — 1172 LOC
**Docstring:** GRID FOIA diplomatic cables ingestion module.
**Classes:** `FOIACablesPuller` [__init__, pull_all, pull_recent]
**Reads:** `__future__`, `datetime`, `document`, `foia`, `heading`, `ingestion`, `loguru`, `nsa`, `playwright`, `publicly`, `rendered`, `sqlalchemy`, `state`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/supply_chain_parser.py` — 1167 LOC
**Docstring:** GRID 10-K Supply Chain Parser.
**Classes:** `DerivedEdge`; `DerivedNode`; `ParserStats` [bump]; `SupplyChain10KParser` [__init__, process_ticker, run]
**Functions:** `run_weekly(db_engine)`
**Reads:** `__future__`, `analysis`, `bs4`, `dataclasses`, `datetime`, `for`, `ghana`, `item`, `loguru`, `pathlib`, `sqlalchemy`, `supply_chain_nodes`, `typing`
**Writes:** `supply_chain_edges`, `supply_chain_nodes`
**Imports from GRID:** `analysis.sector_map`, `db`
**Imported by:** `intelligence/pct_cogs_enrichment.py`

#### `ingestion/altdata/sec_xbrl_financials.py` — 1140 LOC
**Docstring:** SEC XBRL Company Facts ingestor for normalized capital_flows.
**Classes:** `SECXBRLFinancialsPuller` [__init__, pull_all]
**Reads:** `__future__`, `analysis`, `capital_flows`, `datetime`, `fred`, `loguru`, `pathlib`, `raw_series`, `sector_map`, `sqlalchemy`, `typing`
**Writes:** `capital_flows`
**Imports from GRID:** `analysis.sector_map`, `db`
**Imported by:** `ingestion/altdata/sec_xbrl_shares.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/legislation.py` — 1050 LOC
**Docstring:** GRID legislative tracker — bills, hearings, and votes from Congress.gov.
**Classes:** `LegislationPuller` [__init__, pull_member_votes, pull_bills, pull_hearings, pull_votes, pull_all, pull_recent]
**Reads:** `__future__`, `action`, `both`, `committee`, `congress`, `datetime`, `environment`, `ingestion`, `last`, `loguru`, `sqlalchemy`, `typing`, `vote`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`, `intelligence/legislative_intel.py`

#### `ingestion/altdata/news_scraper.py` — 975 LOC
**Docstring:** GRID free news scraper — RSS-based financial news ingestion with LLM sentiment.
**Classes:** `NewsArticle` [to_dict]; `NewsScraperPuller` [__init__, pull_source, pull_all, get_recent]
**Reads:** `__future__`, `article`, `dataclasses`, `datetime`, `description`, `free`, `ingestion`, `intelligence`, `llm`, `loguru`, `micro`, `news_articles`, `reuters`, `scraped`, `sqlalchemy`, `typing`, `various`, `xml`
**Writes:** `feature_registry`, `news_articles`, `signal_sources`
**Imports from GRID:** `ingestion.base`, `intelligence.news_ticker_resolver`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/wikidata_persons.py` — 940 LOC
**Docstring:** Wikidata SPARQL person-connection ingestion module for GRID.
**Classes:** `WikidataPersonPuller` [__init__, search_person, pull_all, get_graph_data, get_person_connections, get_stats]
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `loguru`, `sqlalchemy`, `typing`, `wikidata_connections`, `wikidata_persons`
**Writes:** `wikidata_connections`, `wikidata_persons`
**Imports from GRID:** `ingestion.base`, `intelligence.actors.seed_data`

#### `ingestion/altdata/corporate_actions_parser.py` — 931 LOC
**Docstring:** SEC 8-K corporate actions parser → capital_flows rows.
**Classes:** `ExtractedEvent`; `CorporateActionsParser` [__init__, pull, close]
**Reads:** `__future__`, `dataclasses`, `datetime`, `deal`, `ingestion`, `loguru`, `sec`, `sqlalchemy`, `typing`
**Writes:** `capital_flows`

#### `ingestion/altdata/fara.py` — 902 LOC
**Docstring:** GRID FARA (Foreign Agent Registration Act) ingestion module.
**Classes:** `FARAPuller` [__init__, pull_registrants, pull_activities, pull_all, pull_recent]
**Reads:** `__future__`, `activity`, `datetime`, `doj`, `fara`, `ingestion`, `last`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/offshore_leaks.py` — 893 LOC
**Docstring:** GRID ICIJ Offshore Leaks Database ingestion module.
**Classes:** `OffshoreLeaksPuller` [__init__, ensure_data, match_actors, store_matches, pull]
**Functions:** `check_actor_in_offshore_leaks(engine, actor_name, actor_id)`, `queue_offshore_investigation(engine, actor_name, actor_id, offshore_matches)`
**Reads:** `__future__`, `_build_known_names_index`, `_known_actors`, `check_actor_in_offshore_leaks`, `datetime`, `grid`, `icij`, `ingestion`, `intelligence`, `loguru`, `match_actors`, `orchestration`, `pathlib`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `signal_sources`
**Imports from GRID:** `ingestion.base`, `intelligence.actor_network`
**Imported by:** `ingestion/scheduler.py`, `intelligence/actor_discovery.py`

#### `ingestion/altdata/institutional_flows.py` — 888 LOC
**Docstring:** GRID institutional money flow data ingestion module.
**Classes:** `InstitutionalFlowsPuller` [__init__, pull_all, pull_etf_only, pull_13f_only]
**Reads:** `__future__`, `datetime`, `each`, `edgar`, `etf`, `filing`, `ingestion`, `loguru`, `sec`, `sqlalchemy`, `thousands`, `typing`, `yfinance`
**Writes:** `signal_sources`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/supply_chain.py` — 848 LOC
**Docstring:** GRID Supply Chain Leading Indicators ingestion module.
**Classes:** `SupplyChainPuller` [__init__, pull_all]
**Reads:** `__future__`, `config`, `datetime`, `drewry`, `fedfred`, `fred`, `freightos`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `various`
**Imports from GRID:** `config`, `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/asset_registries.py` — 817 LOC
**Docstring:** GRID Asset Registry ingestion module.
**Classes:** `AssetRegistryPuller` [__init__, search_aircraft, search_vessels, cross_reference_icij, pull_all]
**Reads:** `__future__`, `cell`, `datetime`, `exc`, `ingestion`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/regulatory_events.py` — 812 LOC
**Docstring:** GRID regulatory enforcement events puller.
**Classes:** `RegulatoryEvent`; `PullStats` [bump_source, bump_severity, as_dict]; `RegulatoryEventsPuller` [__init__, pull]
**Functions:** `run_weekly(db_engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `trailing`, `typing`
**Writes:** `regulatory_events`, `supply_chain_edges`, `supply_chain_nodes`
**Imports from GRID:** `db`, `ingestion.altdata.gov_contracts`, `ingestion.base`

#### `ingestion/altdata/apple_supplier_list.py` — 808 LOC
**Docstring:** GRID Apple Supplier List Puller.
**Classes:** `SupplierRecord`; `AppleSupplierListStats` [as_dict]; `AppleSupplierListPuller` [__init__, run]
**Functions:** `run_annual(db_engine)`
**Reads:** `__future__`, `apple`, `dataclasses`, `datetime`, `exc`, `loguru`, `pypdf`, `sqlalchemy`, `typing`
**Writes:** `supply_chain_edges`, `supply_chain_nodes`
**Imports from GRID:** `db`

#### `ingestion/altdata/trending_news.py` — 796 LOC
**Docstring:** GRID trending news ingestion via last30days-skill.
**Classes:** `TrendingItem`; `TrendingNewsPuller` [__init__, pull_topic, pull_all, get_recent]
**Reads:** `__future__`, `any`, `dataclasses`, `datetime`, `ingestion`, `last30days`, `lib`, `loguru`, `multiple`, `pathlib`, `reddit`, `sqlalchemy`, `trending_items`, `typing`
**Writes:** `feature_registry`, `signal_sources`, `trending_items`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/smart_money.py` — 792 LOC
**Docstring:** GRID Social Smart Money Tracker.
**Classes:** `SmartMoneyPuller` [__init__, pull_reddit, pull_finviz_insiders, pull_all]
**Reads:** `__future__`, `accounts`, `cells`, `datetime`, `finviz`, `ingestion`, `loguru`, `post`, `raw_series`, `reddit`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/web_scraper.py` — 788 LOC
**Docstring:** GRID Web Scraper — multi-source data collection with cross-verification.
**Classes:** `WebScraperPuller` [__init__, cross_verify, pull_feature, pull_batch]
**Reads:** `__future__`, `datetime`, `ddg`, `feature`, `highest`, `ingestion`, `loguru`, `multiple`, `page`, `raw_series`, `snippet`, `sqlalchemy`, `title`, `typing`, `unverified`, `urllib`
**Writes:** `raw_series`, `scrape_audit`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/discord_scanner.py` — 762 LOC
**Docstring:** GRID Solana Discord Scanner.
**Classes:** `DiscordUser` [__init__]; `DiscordScanner` [__init__, pull_recent, pull_all, run_realtime]
**Reads:** `__future__`, `all`, `alpha`, `datetime`, `discord`, `each`, `environment`, `gateway`, `guilds`, `individual`, `ingestion`, `loguru`, `specified`, `sqlalchemy`, `sync`, `typing`
**Imports from GRID:** `db`, `ingestion.altdata.memecoin_classifier`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/insider_filings.py` — 759 LOC
**Docstring:** GRID SEC Form 4 insider trading filings ingestion module.
**Classes:** `InsiderFilingsPuller` [__init__, pull_recent, pull_all]
**Reads:** `__future__`, `accession`, `components`, `datetime`, `edgar`, `filing`, `ingestion`, `loguru`, `sec`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fear_greed.py` — 725 LOC
**Docstring:** GRID Fear & Greed Index ingestion module.
**Classes:** `FearGreedPuller` [__init__, pull_cnn, pull_crypto, pull, pull_all]
**Reads:** `__future__`, `alternative`, `cnn`, `datetime`, `ingestion`, `loguru`, `milliseconds`, `response`, `sqlalchemy`, `typing`, `volatility`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/cloudflare_radar_puller.py` — 725 LOC
**Docstring:** GRID Cloudflare Radar data ingestion module.
**Classes:** `CloudflareRadarPuller` [__init__, detect_traffic_anomalies, pull]
**Reads:** `__future__`, `cloudflare`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/flow_materializer.py` — 723 LOC
**Docstring:** GRID Flow Materializer — transforms signal_sources and raw_series into
**Functions:** `sync_insider_trades(engine)`, `sync_congressional_trades(engine)`, `sync_dark_pool_weekly(engine)`, `sync_etf_flows(engine)`, `sync_junction_points(engine)`, `sync_all(engine)`
**Reads:** `__future__`, `datetime`, `junction_series`, `loguru`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `congressional_trades`, `dark_pool_weekly`, `etf_flows`, `insider_trades`, `junction_point_readings`

#### `ingestion/altdata/finra_ats.py` — 715 LOC
**Docstring:** GRID FINRA ATS (Dark Pool) volume ingestion module.
**Classes:** `FINRAATSPuller` [__init__, pull_ats_volume, pull_short_interest, pull_all]
**Reads:** `__future__`, `datetime`, `finra`, `ingestion`, `loguru`, `records`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/prediction_market_history.py` — 693 LOC
**Docstring:** GRID Prediction Market Historical Data Sync.
**Classes:** `PredictionMarketHistoryPuller` [__init__, pull_all]
**Reads:** `__future__`, `available`, `datetime`, `direct`, `ingestion`, `loguru`, `parquet`, `pathlib`, `polygon`, `prediction_market_trades`, `sqlalchemy`, `trades`, `typing`
**Writes:** `prediction_market_markets`, `prediction_market_trades`, `raw_series`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/sec_item_1c_cyber.py` — 688 LOC
**Docstring:** GRID SEC Item 1C Cybersecurity Puller.
**Classes:** `Item1CEdge`; `Item1CStats` [as_dict]; `SECItem1CCyberPuller` [__init__, process_ticker, run]
**Functions:** `run_weekly(db_engine)`
**Reads:** `__future__`, `analysis`, `bs4`, `dataclasses`, `datetime`, `fiscal`, `loguru`, `pathlib`, `single`, `sqlalchemy`, `supply_chain_nodes`, `typing`
**Writes:** `supply_chain_edges`, `supply_chain_nodes`
**Imports from GRID:** `analysis.sector_map`, `db`

#### `ingestion/altdata/export_controls.py` — 671 LOC
**Docstring:** GRID export controls tracker — BIS Entity List & Federal Register ingestion.
**Classes:** `ExportControlsPuller` [__init__, pull_all]
**Reads:** `__future__`, `china`, `datetime`, `document`, `entity`, `federal`, `ingestion`, `loguru`, `restriction_types`, `sqlalchemy`, `title`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/telegram_scanner.py` — 668 LOC
**Docstring:** GRID Solana Telegram Scanner.
**Classes:** `TelegramUser` [__init__]; `TelegramScanner` [__init__, pull_recent, pull_all, run_realtime]
**Reads:** `__future__`, `all`, `dataclasses`, `datetime`, `each`, `environment`, `individual`, `ingestion`, `loguru`, `pathlib`, `specified`, `sqlalchemy`, `sync`, `telethon`, `typing`
**Imports from GRID:** `db`, `ingestion.altdata.memecoin_classifier`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/campaign_finance.py` — 664 LOC
**Docstring:** GRID campaign finance tracker — FEC API ingestion module.
**Classes:** `CampaignFinancePuller` [__init__, pull_pac_contributions, pull_individual_contributions, pull_all, pull_recent]
**Reads:** `__future__`, `datetime`, `employees`, `executives`, `fec`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/sec_13f_live.py` — 662 LOC
**Docstring:** Live SEC 13F-HR ingestor for the ``institutional_holdings`` table.
**Classes:** `Filer`; `CusipTickerMap` [__init__, lookup, size]; `LatestFiling`; `FilerResult`; `SEC13FLiveIngestor` [__init__, run]
**Functions:** `filer_by_key(key)`, `parse_infotable_xml(xml_bytes)`, `find_latest_13f(cik)`, `fetch_infotable(cik, filing)`, `run(engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `edgar`, `loguru`, `populate_institutional_holdings`, `reported`, `sec`, `sqlalchemy`, `typing`
**Writes:** `institutional_holdings`
**Imports from GRID:** `db`

#### `ingestion/altdata/actor_news_puller.py` — 660 LOC
**Docstring:** Actor news puller — pull news mentions for every actor in sector_map.
**Classes:** `NewsRow`; `BioRow`; `ActorNewsPuller` [__init__, pull_one_actor]
**Functions:** `slugify(name)`, `score_sentiment(text_blob)`, `extract_stance(text_blob)`, `extract_loyalty(text_blob)`, `parse_rfc822(s)`, `enumerate_sector_map_actors(priority_only)`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `google`, `ingestion`, `loguru`, `sector_map`, `sqlalchemy`, `typing`, `urllib`
**Writes:** `actor_bio`, `actor_news`
**Imports from GRID:** `analysis.sector_map`, `ingestion.base`

#### `ingestion/altdata/uk_companies_house.py` — 653 LOC
**Docstring:** GRID UK Companies House ingestion module.
**Classes:** `UKCompaniesHousePuller` [__init__, search_companies, get_company, get_psc, get_officers, pull_company, search_and_pull, pull_watchlist]
**Reads:** `__future__`, `datetime`, `exc`, `https`, `ingestion`, `loguru`, `natures_of_control`, `psc`, `pull_watchlist`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/defi_llama_puller.py` — 648 LOC
**Docstring:** GRID DeFi Llama data ingestion module.
**Classes:** `DefiLlamaPuller` [__init__, pull_protocols, pull_chain_tvl, pull_stablecoins, pull_bridges, pull_all]
**Reads:** `__future__`, `config`, `datetime`, `defi`, `ingestion`, `intelligence`, `loguru`, `protocols`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `db`, `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/gdelt.py` — 646 LOC
**Docstring:** GRID GDELT news event data ingestion module.
**Classes:** `GDELTPuller` [__init__, pull_gkg_day, pull_historical, pull_recent]
**Reads:** `__future__`, `datetime`, `gdelt`, `ingestion`, `loguru`, `raw_series`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/crucix_bridge.py` — 641 LOC
**Docstring:** GRID — Crucix bridge puller.
**Classes:** `CrucixBridgePuller` [__init__, pull_all]
**Reads:** `__future__`, `crucix`, `datetime`, `each`, `email`, `file`, `http`, `ingestion`, `loguru`, `news_articles`, `pathlib`, `raw_series`, `sqlalchemy`, `typing`
**Writes:** `news_articles`, `raw_series`, `signal_sources`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`, `intelligence/scheduler.py`

#### `ingestion/altdata/prediction_odds.py` — 633 LOC
**Docstring:** GRID Prediction Market Rapid-Change Detector.
**Classes:** `PredictionOddsPuller` [__init__, pull_shifts, pull_all]
**Reads:** `__future__`, `datetime`, `history`, `ingestion`, `loguru`, `market`, `outcomeprices`, `polymarket`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/options.py` — 629 LOC
**Docstring:** GRID — Options chain ingestion via Yahoo Finance API.
**Classes:** `YahooOptionsClient` [__init__, is_available, get_options]; `OptionsPuller` [__init__, pull_all]
**Functions:** `compute_max_pain(calls_df, puts_df, spot_price)`, `compute_iv_skew(puts_df, spot_price)`
**Reads:** `__future__`, `datetime`, `feature_registry`, `first`, `ingestion`, `loguru`, `nearest`, `query2`, `sqlalchemy`, `typing`, `yahoo`
**Writes:** `feature_registry`, `options_daily_signals`, `options_snapshots`, `resolved_series`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`, `physics/dealer_gamma.py`

#### `ingestion/altdata/lobbying.py` — 614 LOC
**Docstring:** GRID lobbying disclosure tracker.
**Classes:** `LobbyingPuller` [__init__, pull_all, pull_recent]
**Reads:** `__future__`, `datetime`, `description`, `environment`, `gov_contracts`, `ingestion`, `last`, `lda`, `lobbying`, `loguru`, `opensecrets`, `raw_series`, `senate`, `sqlalchemy`, `two`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.altdata.gov_contracts`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/hf_financial_news.py` — 613 LOC
**Docstring:** GRID HuggingFace Financial News ingestion module.
**Classes:** `HFFinancialNewsPuller` [__init__, pull_subset, pull_all]
**Reads:** `__future__`, `article`, `content`, `dataset`, `dataset_configs`, `datasets`, `datetime`, `huggingface`, `ingestion`, `loguru`, `longer`, `publicly`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fed_liquidity.py` — 604 LOC
**Docstring:** GRID Fed liquidity equation data ingestion module.
**Classes:** `FedLiquidityPuller` [__init__, pull_all]
**Reads:** `__future__`, `config`, `datetime`, `fedfred`, `fred`, `ingestion`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/earnings_puller.py` — 596 LOC
**Docstring:** GRID earnings data puller — fills the 'earnings' feature family in raw_series.
**Classes:** `EarningsPuller` [__init__, pull_ticker, pull_all, get_summary]
**Functions:** `compute_surprise_pct(actual, estimate)`, `classify_beat_miss(surprise_pct)`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `pull`, `pull_all`, `quarterly_earnings`, `sqlalchemy`, `typing`, `yahoo`, `yfinance`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fmp_puller.py` — 590 LOC
**Docstring:** Financial Modeling Prep puller — earnings, financials, transcripts, calendars.
**Classes:** `FMPPuller` [__init__, pull_earnings_history, pull_earnings_calendar, pull_analyst_estimates, pull_income_statement, pull_balance_sheet, pull_cash_flow, pull_transcript]
**Reads:** `__future__`, `config`, `datetime`, `financial`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/congressional.py` — 578 LOC
**Docstring:** GRID congressional trading disclosure ingestion module.
**Classes:** `CongressionalTradingPuller` [__init__, pull_recent, pull_all]
**Reads:** `__future__`, `components`, `datetime`, `edgar`, `ingestion`, `loguru`, `quiverquant`, `sec`, `source`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/nyfed.py` — 574 LOC
**Docstring:** GRID NY Fed data ingestion module.
**Classes:** `NYFedPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `response`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/bookmarks.py` — 569 LOC
**Docstring:** Twitter/X Bookmark Intelligence Pipeline.
**Functions:** `triage_bookmark(bookmark)`, `compare_results(results)`, `write_inbox_entry(bookmark, llm_results, comparison)`, `write_dashboard()`, `run_triage(limit, force)`
**Reads:** `__future__`, `bookmarks`, `config`, `datetime`, `ingestion`, `llm`, `pathlib`, `sqlalchemy`, `typing`
**Writes:** `bookmarks`, `obsidian_notes`
**Imports from GRID:** `config`, `db`, `ingestion.altdata.obsidian_sync`

#### `ingestion/altdata/opencorporates.py` — 567 LOC
**Docstring:** GRID OpenCorporates API ingestion module.
**Classes:** `OpenCorporatesPuller` [__init__, search_company, get_company, search_officer, cross_reference_icij, pull_all]
**Reads:** `__future__`, `datetime`, `icij`, `ingestion`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/kalshi.py` — 544 LOC
**Docstring:** GRID Kalshi Prediction Markets ingestion module.
**Classes:** `KalshiPuller` [__init__, pull_markets, pull_all]
**Reads:** `__future__`, `close_time`, `datetime`, `ingestion`, `intelligence`, `kalshi`, `loguru`, `sqlalchemy`, `title`, `typing`, `yes_price`
**Imports from GRID:** `ingestion.base`, `intelligence.trust_scorer`

#### `ingestion/altdata/dark_pool.py` — 542 LOC
**Docstring:** GRID FINRA ADF/ATS dark pool transparency data ingestion module.
**Classes:** `DarkPoolPuller` [__init__, pull_weekly, pull_all]
**Reads:** `__future__`, `datetime`, `finra`, `ingestion`, `loguru`, `records`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/cftc_cot.py` — 538 LOC
**Docstring:** GRID CFTC Commitments of Traders (COT) data ingestion module.
**Classes:** `CFTCCOTPuller` [__init__, pull_contract, pull_all]
**Reads:** `__future__`, `contract_map`, `datetime`, `each`, `ingestion`, `loguru`, `metric`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `db`, `ingestion.base`

#### `ingestion/edgar.py` — 534 LOC
**Docstring:** GRID SEC EDGAR data ingestion module.
**Classes:** `EDGARPuller` [__init__, pull_13f_holdings, pull_form4_transactions, pull_8k_counts, pull_all]
**Reads:** `__future__`, `datetime`, `edgar`, `loguru`, `raw_series`, `sec`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imports from GRID:** `db`

#### `ingestion/altdata/gov_contracts.py` — 532 LOC
**Docstring:** GRID government contract tracker — USASpending.gov ingestion module.
**Classes:** `GovContractsPuller` [__init__, pull_all, fetch_award_detail]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `usaspending`
**Writes:** `signal_sources`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/altdata/lobbying.py`, `ingestion/altdata/regulatory_events.py`, `ingestion/scheduler.py`, `intelligence/influence_network.py`

#### `ingestion/altdata/redfin_puller.py` — 530 LOC
**Docstring:** GRID Redfin Housing Data ingestion module.
**Classes:** `RedfinPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `loguru`, `redfin`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`, `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/memecoin_classifier.py` — 526 LOC
**Docstring:** GRID Solana Memecoin Message Classifier.
**Classes:** `SignalLabel`; `ClassifiedMessage`; `MentionTracker` [__init__, add, get_hot_tokens, clear_old]
**Functions:** `extract_token_addresses(text)`, `extract_token_symbol(text)`, `message_hash(text, channel_id)`, `classify_message(text)`, `classify_full_message(text, source, channel_name, channel_id, user_id, username, timestamp, raw_payload)`
**Reads:** `__future__`, `chat`, `dataclasses`, `datetime`, `dex`, `enum`, `message`, `telegram`, `typing`
**Imported by:** `ingestion/altdata/discord_scanner.py`, `ingestion/altdata/telegram_scanner.py`

#### `ingestion/altdata/unusual_whales.py` — 523 LOC
**Docstring:** GRID Unusual Options Flow (Whale Tracking) ingestion module.
**Classes:** `UnusualWhalesPuller` [__init__, pull_ticker, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `yfinance`
**Writes:** `signal_sources`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/pushshift_reddit.py` — 513 LOC
**Docstring:** Pushshift Reddit historical backfill ingestor.
**Classes:** `PushshiftRedditPuller` [__init__, ingest_directory, process_dump_file, extract_ticker_mentions, score_sentiment, aggregate_daily]
**Reads:** `__future__`, `collections`, `datetime`, `ingestion`, `loguru`, `pathlib`, `social_sentiment`, `sqlalchemy`, `target`, `typing`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/indeed_hiring_puller.py` — 511 LOC
**Docstring:** GRID Indeed Hiring Lab ingestion module.
**Classes:** `IndeedHiringPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `indeed`, `ingestion`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`, `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/aaii_sentiment.py` — 511 LOC
**Docstring:** GRID AAII Sentiment Survey ingestion module.
**Classes:** `AAIISentimentPuller` [__init__, pull_sentiment, pull_all]
**Reads:** `__future__`, `aaii`, `any`, `column`, `datetime`, `html`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `xls`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/world_news.py` — 507 LOC
**Docstring:** GRID WorldNewsAPI ingestion module.
**Classes:** `WorldNewsPuller` [__init__, pull_category, pull_day, pull_all]
**Reads:** `__future__`, `datetime`, `environment`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `worldnewsapi`
**Writes:** `feature_registry`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/fred.py` — 494 LOC
**Docstring:** GRID FRED data ingestion module.
**Classes:** `FREDPuller` [__init__, pull_series, pull_all, get_release_dates]
**Reads:** `__future__`, `config`, `datetime`, `fedfred`, `fred`, `ingestion`, `last`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `config`, `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`, `intelligence/post_query_scanner.py`

#### `ingestion/altdata/sec_xbrl_shares.py` — 481 LOC
**Docstring:** SEC XBRL shares-outstanding ingestor for daily market_cap computation.
**Classes:** `SECXBRLSharesPuller` [__init__, pull_all]
**Reads:** `__future__`, `all`, `datetime`, `ingestion`, `loguru`, `raw_series`, `sqlalchemy`, `today`, `typing`
**Writes:** `ticker_metrics_daily`
**Imports from GRID:** `db`, `ingestion.altdata.sec_xbrl_financials`

#### `ingestion/altdata/ads_index.py` — 481 LOC
**Docstring:** GRID ADS Business Conditions Index ingestion module.
**Classes:** `ADSIndexPuller` [__init__, pull_ads_index, pull_all]
**Reads:** `__future__`, `datetime`, `excel`, `ingestion`, `loguru`, `philadelphia`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`

#### `ingestion/openbb_pipeline.py` — 470 LOC
**Docstring:** OpenBB data ingestion pipeline for GRID.
**Classes:** `OpenBBPipeline` [__init__, run_crypto, run_macro, run_equity, run_all]
**Reads:** `__future__`, `concurrent`, `datetime`, `loguru`, `openbb`, `sqlalchemy`, `sum`, `typing`
**Writes:** `analytical_snapshots`
**Imports from GRID:** `db`
**Imported by:** `intelligence/post_query_scanner.py`

#### `ingestion/international/world_bank_puller.py` — 463 LOC
**Docstring:** GRID World Bank Open Data ingestion module.
**Classes:** `WorldBankPuller` [__init__, pull_indicator, detect_gdp_anomalies, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `intelligence`, `loguru`, `raw_series`, `sqlalchemy`, `today`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fed_speeches.py` — 458 LOC
**Docstring:** GRID Federal Reserve communications ingestion module.
**Classes:** `FedSpeechPuller` [__init__, score_text, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `latest`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/sec_velocity.py` — 451 LOC
**Docstring:** GRID SEC 8-K filing velocity module.
**Classes:** `SECVelocityPuller` [__init__, pull_weekly_velocity, pull_historical_velocity]
**Reads:** `__future__`, `company`, `datetime`, `edgar`, `feature_registry`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `start_year`, `today`, `typing`
**Writes:** `feature_registry`, `raw_series`, `source_catalog`
**Imports from GRID:** `db`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/earnings_calendar.py` — 451 LOC
**Docstring:** GRID earnings calendar data ingestion module.
**Classes:** `EarningsCalendarPuller` [__init__, pull_ticker_earnings, pull_all]
**Functions:** `get_upcoming_earnings(engine, days_ahead)`, `get_recent_earnings(engine, days_back)`, `get_earnings_history(engine, ticker, limit)`
**Reads:** `__future__`, `datetime`, `earnings_calendar`, `ingestion`, `loguru`, `options_daily_signals`, `sqlalchemy`, `typing`, `watchlist`, `yahoo`
**Writes:** `earnings_calendar`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `api/routers/earnings.py`

#### `ingestion/altdata/yield_curve_full.py` — 430 LOC
**Docstring:** GRID full US Treasury yield curve ingestion module.
**Classes:** `FullYieldCurvePuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `fedfred`, `fred`, `ingestion`, `loguru`, `other`, `raw_series`, `sqlalchemy`, `stored`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/smart_scheduler.py` — 423 LOC
**Docstring:** GRID Smart Scheduler — runs only due/stale pullers per cycle.
**Classes:** `SmartScheduler` [__init__, tick, get_status]
**Reads:** `__future__`, `datetime`, `hermes`, `ingestion`, `loguru`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `source_catalog`

#### `ingestion/ml/finbert_scorer.py` — 415 LOC
**Docstring:** FinBERT sentiment scoring pipeline for GRID.
**Classes:** `FinBERTScorer` [__init__, load_model, score_batch, score_source, score_all_sources]
**Reads:** `__future__`, `cache`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `transformers`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/obsidian_sync.py` — 412 LOC
**Docstring:** Obsidian vault <-> Postgres bidirectional sync engine.
**Functions:** `domain_from_path(vault_path)`, `content_hash(text_content)`, `parse_frontmatter(raw)`, `scan_vault(vault_path)`, `sync_inbound(engine, vault_path)`, `domain_to_folder(domain)`, `build_frontmatter(fm)`, `build_note_file(fm, body)`, `sync_outbound(engine, vault_path)`, `run_sync(engine, vault_path)`, `generate_dashboard(notes, recent_actions)`, `regenerate_dashboard(engine, vault_path)`
**Reads:** `__future__`, `config`, `current`, `datetime`, `loguru`, `markdown`, `obsidian_actions`, `obsidian_notes`, `pathlib`, `postgres`, `sqlalchemy`, `typing`, `vault`
**Writes:** `obsidian_actions`, `obsidian_notes`
**Imports from GRID:** `config`
**Imported by:** `api/routers/vault.py`, `ingestion/altdata/bookmarks.py`, `intelligence/obsidian_agent.py`

#### `ingestion/altdata/prediction_pmxt.py` — 412 LOC
**Docstring:** GRID Prediction Market Multi-Platform Puller via pmxt SDK.
**Classes:** `PmxtPredictionPuller` [__init__, pull, pull_all]
**Reads:** `__future__`, `all`, `config`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `db`, `ingestion.base`

#### `ingestion/altdata/repo_market.py` — 406 LOC
**Docstring:** GRID repo and money market stress indicator ingestion module.
**Classes:** `RepoMarketPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `fedfred`, `fred`, `ingestion`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/base.py` — 386 LOC
**Docstring:** Base puller class for GRID data ingestion.
**Classes:** `BasePuller` [__init__, validate_row]
**Functions:** `retry_on_failure(max_attempts, backoff, retryable_exceptions)`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `previous`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imports from GRID:** `ingestion.sanity_ranges`
**Imported by:** `ingestion/altdata/aaii_sentiment.py`, `ingestion/altdata/actor_news_puller.py`, `ingestion/altdata/ads_index.py`, `ingestion/altdata/ag_commodity_futures.py`, `ingestion/altdata/alphavantage_sentiment.py`, `ingestion/altdata/analyst_ratings.py`, `ingestion/altdata/asset_registries.py`, `ingestion/altdata/baltic_dry.py`, `ingestion/altdata/binance_puller.py`, `ingestion/altdata/campaign_finance.py` (+100)

#### `ingestion/altdata/ag_commodity_futures.py` — 385 LOC
**Docstring:** GRID agricultural + industrial commodity futures ingestion.
**Classes:** `AgCommodityFuturesPuller` [__init__, pull_all]
**Reads:** `__future__`, `batch`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `yfinance`, `yfinance_pull`
**Imports from GRID:** `db`, `ingestion.base`

#### `ingestion/altdata/social_attention.py` — 381 LOC
**Docstring:** GRID social attention data ingestion.
**Classes:** `WikipediaAttentionPuller` [pull_ticker, pull_all]; `EdgarViewsPuller` [pull_ticker, pull_all]; `GoogleTrendsPuller` [pull_ticker, pull_all]
**Reads:** `__future__`, `datetime`, `free`, `ingestion`, `july`, `loguru`, `pytrends`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`

#### `ingestion/celestial/solar.py` — 377 LOC
**Docstring:** GRID -- Solar activity data ingestion.
**Classes:** `SolarActivityPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `noaa`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/altdata/edgar_transcripts.py` — 375 LOC
**Docstring:** SEC EDGAR 8-K Transcript Puller — earnings call transcripts from SEC filings.
**Classes:** `EdgarTranscriptPuller` [get_recent_8k, fetch_filing_text, extract_guidance, pull]
**Reads:** `__future__`, `datetime`, `filing`, `gemma`, `html`, `ingestion`, `llm`, `loguru`, `model`, `response`, `sec`, `sqlalchemy`, `typing`
**Imports from GRID:** `gemma.micro`, `ingestion.base`

#### `ingestion/signal_classifier.py` — 372 LOC
**Docstring:** Gemma 270M signal classification for the ingestion pipeline.
**Classes:** `ClassificationResult`
**Functions:** `classify_signal_text(signal_text)`, `classify_recent_signals(engine, limit)`, `narrate_anomalies(engine, z_threshold, limit)`, `map_signal_knowledge(engine, urgency_filter, limit)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `gemma`, `loguru`, `signal_registry`, `sqlalchemy`, `typing`
**Writes:** `signal_registry`
**Imports from GRID:** `gemma.micro`

#### `ingestion/altdata/alphavantage_sentiment.py` — 367 LOC
**Docstring:** GRID Alpha Vantage News Sentiment ingestion module.
**Classes:** `AlphaVantageSentimentPuller` [__init__, pull_ticker, pull_all]
**Reads:** `__future__`, `alpha`, `datetime`, `dotenv`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/seed_v2.py` — 356 LOC
**Docstring:** GRID v2 database seed data.
**Functions:** `run_seed_v2(db_engine)`
**Reads:** `__future__`, `ais`, `bis`, `comtrade`, `ecb`, `loguru`, `sqlalchemy`, `tss`, `wiod`
**Writes:** `feature_registry`, `source_catalog`
**Imports from GRID:** `db`

#### `ingestion/crypto_signals.py` — 353 LOC
**Docstring:** Crypto Signal Bridge — transforms existing crypto raw data into signal_sources.
**Classes:** `CryptoSignalBridge` [__init__, bridge_all, bridge_coingecko, bridge_binance_realtime, bridge_defi_llama, bridge_cryptoquant]
**Reads:** `__future__`, `coingecko`, `datetime`, `feature_registry`, `loguru`, `raw_series`, `realtime_candles`, `resolved_series`, `series`, `series_id`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`

#### `ingestion/altdata/etherscan_puller.py` — 350 LOC
**Docstring:** Etherscan puller — Ethereum on-chain intelligence.
**Classes:** `EtherscanPuller` [__init__, pull_eth_price, pull_gas_oracle, pull_wallet_balance, pull_wallet_token_balance, pull_recent_transactions, pull_token_supply, pull_eth_supply]
**Reads:** `__future__`, `config`, `datetime`, `etherscan`, `ingestion`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/ecb.py` — 349 LOC
**Docstring:** GRID ECB Statistical Data Warehouse ingestion module.
**Classes:** `ECBPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `ecb`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/cboe_indices.py` — 342 LOC
**Docstring:** GRID CBOE volatility and strategy indices ingestion module.
**Classes:** `CBOEIndicesPuller` [__init__, pull_index, pull_all]
**Reads:** `__future__`, `cboe`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/analyst_ratings.py` — 341 LOC
**Docstring:** GRID analyst ratings ingestion module.
**Classes:** `AnalystRatingsPuller` [__init__, pull_ticker, pull_all]
**Reads:** `__future__`, `datetime`, `each`, `feature_registry`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `yfinance`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/ofr.py` — 341 LOC
**Docstring:** GRID OFR Financial Stability Monitor ingestion module.
**Classes:** `OFRPuller` [__init__, pull_fsm, pull_fsi, pull_stfm, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `ofr`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/google_trends.py` — 335 LOC
**Docstring:** GRID Google Trends data ingestion module.
**Classes:** `GoogleTrendsPuller` [__init__, pull_keyword, pull_all]
**Reads:** `__future__`, `coercion`, `datetime`, `ingestion`, `loguru`, `pytrends`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/baltic_dry.py` — 327 LOC
**Docstring:** GRID Baltic Dry Index and shipping indices ingestion module.
**Classes:** `BalticDryPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `config`, `datetime`, `fedfred`, `fred`, `ingestion`, `last`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `config`, `db`, `ingestion.base`

#### `ingestion/pull_context.py` — 321 LOC
**Docstring:** GRID — Pull Context Manager.
**Classes:** `PullContext` [__init__, record_rows, set_expected, add_features, rows_inserted]
**Functions:** `should_run_pull(engine, puller_name, period)`
**Reads:** `__future__`, `datetime`, `historical`, `loguru`, `mean`, `pull_log`, `sqlalchemy`, `typing`
**Writes:** `event_bus`, `pull_log`

#### `ingestion/international/akshare_macro.py` — 315 LOC
**Docstring:** GRID AKShare China macro ingestion module.
**Classes:** `AKShareMacroPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/oecd.py` — 313 LOC
**Docstring:** GRID OECD SDMX API ingestion module.
**Classes:** `OECDPuller` [__init__, pull_cli, pull_mei, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `oecd`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/icij_puller.py` — 307 LOC
**Docstring:** ICIJ Offshore Leaks puller — Panama Papers, Paradise Papers, Pandora Papers, etc.
**Classes:** `ICIJPuller` [__init__, download_data, pull]
**Reads:** `__future__`, `all`, `icij_entities`, `icij_intermediaries`, `icij_officers`, `icij_relationships`, `ingestion`, `intelligence`, `loguru`, `pathlib`, `sqlalchemy`, `typing`
**Writes:** `icij_addresses`, `icij_entities`, `icij_intermediaries`, `icij_officers`, `icij_relationships`
**Imports from GRID:** `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/opensecrets_puller.py` — 307 LOC
**Docstring:** OpenSecrets puller — political donations, lobbying expenditures, revolving door.
**Classes:** `OpenSecretsPuller` [__init__, pull_top_contributors, pull_candidate_industries, pull_org_summary, pull]
**Reads:** `__future__`, `config`, `contributors`, `corporations`, `datetime`, `ingestion`, `intelligence`, `loguru`, `opensecrets`, `sqlalchemy`, `top`, `typing`
**Imports from GRID:** `config`, `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/polygon_puller.py` — 303 LOC
**Docstring:** Polygon.io puller — stocks, options with Greeks, crypto, forex, dividends.
**Classes:** `PolygonPuller` [__init__, pull_options_chain, pull_options_greeks_summary, pull_stock_snapshot, pull_market_snapshot, pull_dividends, pull_ticker_details, pull]
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `polygon`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/fx_rates.py` — 297 LOC
**Docstring:** GRID FX rates ingestion module.
**Classes:** `FXRatesPuller` [__init__, pull, backfill_days]
**Reads:** `__future__`, `datetime`, `foreign`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `yahoo`, `yfinance`
**Imports from GRID:** `api.dependencies`, `ingestion.base`

#### `ingestion/altdata/tiingo_news.py` — 297 LOC
**Docstring:** Tiingo News puller — bulk financial news with tickers, tags, and sources.
**Classes:** `TiingoNewsPuller` [__init__, pull_day, pull_range, pull_recent]
**Reads:** `__future__`, `article`, `config`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `start_date`, `tiingo`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `config`, `ingestion.base`

#### `ingestion/social_sentiment.py` — 295 LOC
**Docstring:** Social sentiment ingestor — Reddit, Bluesky, Google Trends.
**Classes:** `SocialSentimentPuller` [__init__, pull_all, save_to_db]
**Reads:** `__future__`, `all`, `datetime`, `loguru`, `pytrends`, `reddit`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `intelligence/post_query_scanner.py`, `intelligence/scheduler.py`, `ollama/market_briefing.py`

#### `ingestion/altdata/bookmarks_sync.py` — 292 LOC
**Docstring:** Twitter/X Bookmark Sync via Playwright.
**Functions:** `init_db()`, `upsert_bookmark(conn, item)`, `find_chrome_profile()`, `sync_bookmarks(max_scrolls, headless)`
**Reads:** `__future__`, `datetime`, `pathlib`, `playwright`, `twitter`
**Writes:** `bookmarks`, `bookmarks_fts`, `sync_log`

#### `ingestion/bls.py` — 282 LOC
**Docstring:** GRID BLS data ingestion module.
**Classes:** `BLSPuller` [__init__, pull_series]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/patents.py` — 279 LOC
**Docstring:** GRID USPTO PatentsView ingestion module.
**Classes:** `PatentsPuller` [__init__, pull_cpc_velocity, compute_innovation_cycle, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tech`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/kalshi_markets.py` — 275 LOC
**Docstring:** Kalshi prediction market puller — public API, no auth needed.
**Classes:** `KalshiMarketsPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `kalshi`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/celestial/planetary.py` — 275 LOC
**Docstring:** GRID — Planetary aspect data ingestion.
**Classes:** `PlanetaryAspectPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `orbital`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `analysis/astro_correlations.py`, `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/celestial/chinese.py` — 273 LOC
**Docstring:** GRID -- Chinese calendar and Feng Shui data ingestion.
**Classes:** `ChineseCalendarPuller` [__init__, pull_all]
**Reads:** `__future__`, `date`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `traditional`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/international/imf.py` — 273 LOC
**Docstring:** GRID IMF IFS and WEO ingestion module.
**Classes:** `IMFPuller` [__init__, pull_ifs, pull_weo, pull_all]
**Reads:** `__future__`, `datetime`, `ifs`, `imf`, `imfdatapy`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/opportunity.py` — 264 LOC
**Docstring:** GRID Opportunity Insights Economic Tracker ingestion module.
**Classes:** `OppInsightsPuller` [__init__, pull_file, compute_k_shape, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `opportunity`, `raw_series`, `same`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/dexscreener.py` — 259 LOC
**Docstring:** GRID DexScreener data ingestion module.
**Classes:** `DexScreenerPuller` [__init__, pull_aggregate_signals]
**Reads:** `__future__`, `datetime`, `dexscreener`, `loguru`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imports from GRID:** `db`

#### `ingestion/trade/atlas_eci.py` — 259 LOC
**Docstring:** GRID Harvard Atlas Economic Complexity Index (ECI) ingestion module.
**Classes:** `AtlasECIPuller` [__init__, download_eci_data, pull_all]
**Reads:** `__future__`, `any`, `datetime`, `harvard`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/coingecko.py` — 258 LOC
**Docstring:** CoinGecko crypto price puller — free tier, no API key required.
**Classes:** `CoinGeckoPuller` [__init__, pull_all, pull_history]
**Reads:** `__future__`, `coingecko`, `datetime`, `feature_registry`, `loguru`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `feature_registry`, `resolved_series`
**Imported by:** `ingestion/scheduler.py`, `intelligence/post_query_scanner.py`, `intelligence/scheduler.py`

#### `ingestion/altdata/finviz_scraper.py` — 258 LOC
**Docstring:** GRID Finviz fundamentals scraper.
**Classes:** `FinvizScraperPuller` [__init__, pull_ticker, pull_all, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `playwright`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/usda_nass.py` — 253 LOC
**Docstring:** GRID USDA NASS QuickStats ingestion module.
**Classes:** `USDAPuller` [__init__, pull_query, pull_all]
**Reads:** `__future__`, `datetime`, `jan`, `loguru`, `nass`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`, `usda`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/noaa_ais.py` — 252 LOC
**Docstring:** GRID NOAA AIS vessel traffic ingestion module.
**Classes:** `NOAAAISPuller` [__init__, pull_monthly_summary, compute_congestion_index, pull_all]
**Reads:** `__future__`, `arrival`, `datetime`, `ingestion`, `loguru`, `noaa`, `raw_series`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/yfinance_pull.py` — 246 LOC
**Docstring:** GRID yfinance data ingestion module.
**Classes:** `YFinancePuller` [__init__, pull_ticker, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `yahoo`
**Writes:** `raw_series`
**Imports from GRID:** `db`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`, `ingestion/tiingo_news_pull.py`, `ingestion/tiingo_pull.py`

#### `ingestion/celestial/vedic.py` — 239 LOC
**Docstring:** GRID -- Vedic (Jyotish) astrological data ingestion.
**Classes:** `VedicAstroPuller` [__init__, pull_all]
**Reads:** `__future__`, `astronomical`, `datetime`, `ingestion`, `loguru`, `rahu`, `sidereal`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/physical/viirs.py` — 237 LOC
**Docstring:** GRID NASA VIIRS Nighttime Lights ingestion module.
**Classes:** `VIIRSPuller` [__init__, download_monthly_vcmslcfg, aggregate_lights, compute_viirs_divergence, pull_all]
**Reads:** `__future__`, `akshare`, `datetime`, `loguru`, `noaa`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/pumpfun.py` — 234 LOC
**Docstring:** GRID Pump.fun data ingestion module.
**Classes:** `PumpFunPuller` [__init__, pull_aggregate_signals]
**Reads:** `__future__`, `datetime`, `loguru`, `pump`, `response`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imports from GRID:** `db`

#### `ingestion/realtime/feeds/dex_scanner.py` — 227 LOC
**Docstring:** DEX token scanner — GeckoTerminal + DexScreener liquidity spike detection.
**Classes:** `PoolData`
**Functions:** `detect_spikes(pools)`, `async run_dex_scanner(builder)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `geckoterminal`, `ingestion`, `loguru`
**Writes:** `signal_data`
**Imports from GRID:** `db`, `ingestion.realtime.candle_builder`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/tiingo_news_pull.py` — 224 LOC
**Docstring:** Tiingo News Puller — per-ticker sentiment from Tiingo Pro.
**Classes:** `TiingoNewsPuller` [__init__, pull_ticker_news, pull_all, pull_bulk_history]
**Reads:** `__future__`, `article`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `tiingo`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`, `ingestion.yfinance_pull`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/wiki_history.py` — 223 LOC
**Docstring:** Wikipedia "This Day in History" and RSS news ingestor.
**Classes:** `WikiHistoryPuller` [__init__, pull_today, save_to_db]
**Reads:** `__future__`, `datetime`, `loguru`, `source_catalog`, `sqlalchemy`, `typing`, `wikipedia`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `intelligence/scheduler.py`, `ollama/market_briefing.py`

#### `ingestion/altdata/quiverquant.py` — 223 LOC
**Docstring:** GRID — QuiverQuant Expanded Puller.
**Functions:** `pull_endpoint(engine, endpoint_key)`, `pull_all(engine)`
**Reads:** `__future__`, `datetime`, `endpoint`, `environment`, `loguru`, `quiverquant`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`

#### `ingestion/altdata/sec_edgar_company.py` — 220 LOC
**Docstring:** GRID SEC EDGAR company fundamentals scraper.
**Classes:** `SECEdgarCompanyPuller` [__init__, pull_ticker, pull_all, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sec`, `sqlalchemy`, `typing`, `xbrl`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/margin_debt.py` — 217 LOC
**Docstring:** GRID — Margin Debt Monthly puller and materializer.
**Classes:** `MarginDebtPuller` [__init__, pull]
**Functions:** `materialize_margin_debt_from_fred(engine)`
**Reads:** `__future__`, `datetime`, `finra`, `fred`, `ingestion`, `loguru`, `margin_debt_monthly`, `raw_series`, `sqlalchemy`, `two`, `typing`
**Writes:** `margin_debt_monthly`
**Imports from GRID:** `ingestion.base`

#### `ingestion/trade/wiod.py` — 216 LOC
**Docstring:** GRID World Input-Output Database (WIOD) ingestion module.
**Classes:** `WIODPuller` [__init__, download_wiot, compute_gvc_participation, pull_all]
**Reads:** `__future__`, `datetime`, `input`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`, `wiot`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/trade/comtrade.py` — 211 LOC
**Docstring:** GRID UN Comtrade v2 bilateral trade flow ingestion module.
**Classes:** `ComtradePuller` [__init__, pull_query, pull_all]
**Reads:** `__future__`, `comtrade`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/marketwatch_news.py` — 210 LOC
**Docstring:** GRID MarketWatch RSS news scraper.
**Classes:** `MarketWatchNewsPuller` [__init__, pull_feed, pull_all, pull]
**Reads:** `__future__`, `datetime`, `email`, `ingestion`, `loguru`, `marketwatch`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/nasa_firms_puller.py` — 209 LOC
**Docstring:** NASA FIRMS puller — active fire/thermal anomaly detection from satellites.
**Classes:** `NASAFirmsPuller` [__init__, pull]
**Reads:** `__future__`, `config`, `datetime`, `ingestion`, `loguru`, `nasa`, `satellites`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/tiingo_pull.py` — 208 LOC
**Docstring:** GRID Tiingo data ingestion module — fallback for yfinance.
**Classes:** `TiingoPuller` [__init__, pull_ticker, pull_all]
**Reads:** `__future__`, `analysis`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `tiingo`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `analysis.sector_map`, `ingestion.base`, `ingestion.yfinance_pull`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/cryptoquant_puller.py` — 206 LOC
**Docstring:** CryptoQuant puller — on-chain analytics for BTC and ETH.
**Classes:** `CryptoQuantPuller` [__init__, pull_metric, pull]
**Reads:** `__future__`, `config`, `cryptoquant`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/kosis.py` — 206 LOC
**Docstring:** GRID Korea Statistical Information Service (KOSIS) ingestion module.
**Classes:** `KOSISPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `kosis`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/mas.py` — 206 LOC
**Docstring:** GRID Monetary Authority of Singapore (MAS) ingestion module.
**Classes:** `MASPuller` [__init__, pull_resource, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/bis.py` — 205 LOC
**Docstring:** GRID BIS Statistics API ingestion module.
**Classes:** `BISPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/onchain_rpc.py` — 200 LOC
**Docstring:** On-chain RPC poller — direct blockchain queries for price and activity.
**Classes:** `OnChainRPCPoller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`

#### `ingestion/international/abs_au.py` — 198 LOC
**Docstring:** GRID Australian Bureau of Statistics (ABS) ingestion module.
**Classes:** `ABSPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/celestial/lunar.py` — 193 LOC
**Docstring:** GRID — Lunar cycle data ingestion.
**Classes:** `LunarCyclePuller` [__init__, pull_all]
**Reads:** `__future__`, `astronomical`, `datetime`, `ingestion`, `loguru`, `phase`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `analysis/astro_correlations.py`, `api/routers/astrogrid_helpers.py`, `ingestion/scheduler.py`

#### `ingestion/international/eurostat.py` — 185 LOC
**Docstring:** GRID Eurostat bulk download ingestion module.
**Classes:** `EurostatPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/tiingo_fundamentals_pull.py` — 184 LOC
**Docstring:** Tiingo Fundamentals Puller — daily market cap, PE, PB, enterprise value.
**Classes:** `TiingoFundamentalsPuller` [pull_ticker, pull_all]
**Reads:** `__future__`, `datetime`, `feature_registry`, `ingestion`, `loguru`, `resolved_series`, `sqlalchemy`, `tiingo`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/dbnomics.py` — 183 LOC
**Docstring:** GRID DBnomics aggregated central bank data ingestion module.
**Classes:** `DBnomicsPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `datetime`, `dbnomics`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/bcb.py` — 183 LOC
**Docstring:** GRID Banco Central do Brasil (BCB) ingestion module.
**Classes:** `BCBPuller` [__init__, pull_series, pull_all]
**Reads:** `__future__`, `bcb`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/rbi.py` — 179 LOC
**Docstring:** GRID Reserve Bank of India (RBI) ingestion module.
**Classes:** `RBIPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `rbi`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/crypto_bootstrap.py` — 178 LOC
**Docstring:** Bootstrap crypto-native data sources and features into GRID.
**Functions:** `bootstrap()`
**Reads:** `__future__`, `dexscreener`, `feature_registry`, `loguru`, `top`
**Writes:** `feature_registry`
**Imports from GRID:** `db`

#### `ingestion/trade/cepii.py` — 177 LOC
**Docstring:** GRID CEPII BACI trade data ingestion module.
**Classes:** `CEPIIPuller` [__init__, pull_year, pull_all]
**Reads:** `__future__`, `cepii`, `datetime`, `https`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/jquants.py` — 173 LOC
**Docstring:** GRID Japan Exchange Group J-Quants API ingestion module.
**Classes:** `JQuantsPuller` [__init__, pull_index_prices, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/polymarket.py` — 162 LOC
**Docstring:** Polymarket prediction market puller — no auth, real-time odds.
**Classes:** `PolymarketPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `outcomeprices`, `polymarket`, `question`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`

#### `ingestion/altdata/pmxt_archive.py` — 161 LOC
**Docstring:** pmxt Archive puller — free hourly Parquet snapshots of prediction market data.
**Classes:** `PmxtArchivePuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `pathlib`, `pmxt`, `polymarket`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`

#### `ingestion/physical/euklems.py` — 161 LOC
**Docstring:** GRID EU KLEMS industry productivity ingestion module.
**Classes:** `EUKLEMSPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/price_fallback.py` — 156 LOC
**Docstring:** Backup price data puller — runs when yfinance is unreliable.
**Classes:** `PriceFallbackPuller` [__init__, pull_price, pull_many, save_to_db]
**Reads:** `__future__`, `datetime`, `feature_registry`, `loguru`, `multiple`, `sqlalchemy`, `typing`
**Writes:** `resolved_series`
**Imported by:** `ingestion/scheduler.py`, `intelligence/post_query_scanner.py`, `intelligence/scheduler.py`

#### `ingestion/altdata/stocktwits.py` — 156 LOC
**Docstring:** StockTwits social sentiment puller — no auth, real-time, built-in labels.
**Classes:** `StockTwitsPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `stocktwits`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/nowcast_puller.py` — 155 LOC
**Docstring:** GRID Atlanta Fed GDPNow nowcast ingestion module.
**Classes:** `NowcastPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `embedded`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/market_calendar.py` — 151 LOC
**Docstring:** US equity market calendar — holidays, half-days, and trading day checks.
**Functions:** `market_holidays(year)`, `is_weekend(d)`, `is_market_holiday(d)`, `is_market_open(d)`, `last_trading_day(d)`, `next_trading_day(d)`, `trading_days_between(start, end)`
**Reads:** `__future__`, `datetime`, `functools`, `ingestion`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/sanity_ranges.py` — 148 LOC
**Docstring:** GRID — Sanity range definitions for data ingestion validation.
**Functions:** `get_range_for_series(series_id, family)`
**Reads:** `__future__`, `prior`
**Imported by:** `ingestion/base.py`, `oracle/sanity_checker.py`

#### `ingestion/altdata/littlesis_puller.py` — 148 LOC
**Docstring:** LittleSis power-mapping puller -- board seats, donations, lobbying ties.
**Classes:** `LittleSisPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/binance_puller.py` — 147 LOC
**Docstring:** GRID Binance public market data ingestion module.
**Classes:** `BinancePuller` [__init__, pull]
**Reads:** `__future__`, `binance`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/international/edinet.py` — 140 LOC
**Docstring:** GRID Japan FSA EDINET filings ingestion module.
**Classes:** `EDINETPuller` [__init__, pull_filings, pull_all]
**Reads:** `__future__`, `datetime`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tenacity`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/whale_alert.py` — 136 LOC
**Docstring:** Whale Alert puller — on-chain large transaction tracking.
**Classes:** `WhaleAlertPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`

#### `ingestion/altdata/wikipedia_pageviews_puller.py` — 134 LOC
**Docstring:** Wikipedia Pageviews puller -- daily pageview counts for financial topics.
**Classes:** `WikipediaPageviewsPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/nasa_fire.py` — 125 LOC
**Docstring:** GRID NASA FIRMS fire data ingestion module.
**Classes:** `NASAFirePuller` [__init__, pull]
**Reads:** `__future__`, `collections`, `config`, `datetime`, `ingestion`, `loguru`, `nasa`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/hyperliquid_puller.py` — 120 LOC
**Docstring:** Hyperliquid puller — OI, funding rates, liquidations from public API.
**Classes:** `HyperliquidPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`

#### `ingestion/altdata/findkg_puller.py` — 117 LOC
**Docstring:** FinDKG puller — Financial Dynamic Knowledge Graph.
**Classes:** `FinDKGPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `findkg`, `github`, `ingestion`, `intelligence`, `local`, `loguru`, `pathlib`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`, `intelligence.actor_ingest`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/crypto_etf_flows.py` — 114 LOC
**Docstring:** Crypto ETF flow puller — tracks BTC/ETH ETF volume and flow signals.
**Classes:** `CryptoETFPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `loguru`, `price`, `sqlalchemy`, `typing`
**Writes:** `signal_sources`

#### `ingestion/altdata/finra_margin_puller.py` — 113 LOC
**Docstring:** GRID FINRA margin debt statistics ingestion module.
**Classes:** `FINRAMarginPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `finra`, `html`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/realtime/feeds/yahoo.py` — 108 LOC
**Docstring:** Yahoo Finance HTTP poller for traditional market data.
**Functions:** `async run_yahoo_feed(builder)`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`
**Imports from GRID:** `ingestion.realtime.candle_builder`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/altdata/eia_puller.py` — 105 LOC
**Docstring:** GRID EIA energy data ingestion module.
**Classes:** `EIAPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/realtime/candle_builder.py` — 105 LOC
**Docstring:** In-memory OHLCV candle aggregator.
**Classes:** `CandleState` [vwap]; `CandleBuilder` [__init__, ingest, drain, flush_all, active_symbols, pending_flush]
**Reads:** `__future__`, `dataclasses`, `datetime`, `flush`, `loguru`, `multiple`
**Imported by:** `ingestion/realtime/feeds/binance.py`, `ingestion/realtime/feeds/dex_scanner.py`, `ingestion/realtime/feeds/yahoo.py`, `ingestion/realtime/flusher.py`, `ingestion/realtime/ws_listener.py`

#### `ingestion/altdata/nyfed_gscpi.py` — 104 LOC
**Docstring:** NY Fed Global Supply Chain Pressure Index (GSCPI) puller.
**Classes:** `NYFedGSCPIPuller` [__init__, pull_all]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `raw_series`
**Imports from GRID:** `ingestion.base`

#### `ingestion/physical/weather_puller.py` — 104 LOC
**Docstring:** GRID Open-Meteo weather data ingestion module.
**Classes:** `WeatherPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `open`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/physical/noaa_space_weather.py` — 102 LOC
**Docstring:** GRID NOAA Space Weather ingestion module.
**Classes:** `NOAASpaceWeatherPuller` [__init__, pull]
**Reads:** `__future__`, `collections`, `datetime`, `ingestion`, `loguru`, `noaa`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/altdata/uspto_puller.py` — 101 LOC
**Docstring:** GRID USPTO patent application search ingestion module.
**Classes:** `USPTOPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `uspto`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/realtime/flusher.py` — 93 LOC
**Docstring:** Batch DB writer for realtime candles.
**Functions:** `build_insert_values(candles)`, `async run_flusher(builder)`
**Reads:** `__future__`, `alerts`, `ingestion`, `loguru`, `psycopg2`
**Writes:** `realtime_candles`
**Imports from GRID:** `alerts.email`, `db`, `ingestion.realtime.candle_builder`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/altdata/usaspending_puller.py` — 91 LOC
**Docstring:** GRID USASpending.gov federal spending ingestion module.
**Classes:** `USASpendingPuller` [__init__, pull]
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`, `sqlalchemy`, `typing`, `usaspending`
**Imports from GRID:** `ingestion.base`
**Imported by:** `ingestion/scheduler.py`

#### `ingestion/realtime/ws_listener.py` — 91 LOC
**Docstring:** GRID Realtime Market Data Listener.
**Functions:** `async main()`
**Reads:** `__future__`, `ingestion`, `loguru`, `psycopg2`
**Writes:** `realtime_candles`
**Imports from GRID:** `db`, `ingestion.realtime.candle_builder`, `ingestion.realtime.feeds.binance`, `ingestion.realtime.feeds.dex_scanner`, `ingestion.realtime.feeds.yahoo`, `ingestion.realtime.flusher`

#### `ingestion/realtime/feeds/binance.py` — 87 LOC
**Docstring:** Binance combined-stream WebSocket client for real-time crypto trades.
**Functions:** `async run_binance_feed(builder)`
**Reads:** `__future__`, `datetime`, `ingestion`, `loguru`
**Imports from GRID:** `ingestion.realtime.candle_builder`
**Imported by:** `ingestion/realtime/ws_listener.py`

#### `ingestion/altdata/__init__.py` — 14 LOC
**Docstring:** GRID alternative data ingestion modules.

#### `ingestion/__init__.py` — 7 LOC
**Docstring:** GRID ingestion layer.
**Reads:** `external`

#### `ingestion/trade/__init__.py` — 7 LOC
**Docstring:** GRID trade flow and economic complexity ingestion modules.

#### `ingestion/physical/__init__.py` — 7 LOC
**Docstring:** GRID physical economy and real-world signal ingestion modules.

#### `ingestion/international/__init__.py` — 7 LOC
**Docstring:** GRID international central bank and statistical agency ingestion modules.

#### `ingestion/ml/__init__.py` — 2 LOC
**Docstring:** ML-based ingestion enrichment pipelines (FinBERT, etc.).

#### `ingestion/celestial/__init__.py` — 2 LOC
**Docstring:** Celestial and esoteric data sources for correlation analysis.

#### `ingestion/realtime/__init__.py` — 1 LOC

#### `ingestion/realtime/feeds/__init__.py` — 1 LOC


### `api/` (100 modules, 43,392 LOC)

#### `api/routers/flows.py` — 2527 LOC
**Docstring:** Sector flow analysis API — serves the sector map with live data.
**Functions:** `async get_sectors(_token)`, `async get_sector_detail(sector_name, _token)`, `async get_sector_dive(sector_name, _token)`, `async get_sankey_data(as_of, _token)`, `async get_gaps(_token)`, `async run_research(_token)`, `async fill_gaps(_token)`, `async test_hypotheses(_token)`, `async get_money_map(_token)`, `async get_company_drill(ticker, _token)`, `async get_aggregated_flows(sector, period, days, _token)`, `async get_flow_momentum(ticker, days, _token)`, `async get_flow_map_v2(_token)`, `async get_junction_points(_token)`, `async get_flow_layers(_token)`, `async get_flow_layer_detail(layer_id, _token)`, `async get_flow_waterfall(source, _token)`, `async get_flow_orthogonality(_token)`, `async generate_flow_image(image_type, style, model_tier, _token)`, `async generate_custom_image(prompt, style, model_tier, _token)` (+7 more)
**Reads:** `__future__`, `analysis`, `any`, `capital_flow_snapshots`, `congressional_trades`, `dark_pool_weekly`, `datetime`, `disk`, `edges`, `etf_flows`, `fastapi`, `feature_registry`, `features`, `fred`, `holdings`, `inference`, `insider_trades`, `institutional_holdings`, `intelligence`, `junction_point_readings`, `live`, `loguru`, `node`, `normalized`, `ollama`
**Imports from GRID:** `analysis.flow_aggregator`, `analysis.hypothesis_tester`, `analysis.money_flow`, `analysis.money_flow_engine`, `analysis.research_agent`, `analysis.sector_map`, `api.auth`, `api.dependencies`, `inference.live`, `intelligence.audio_briefing`, `intelligence.cds_tracker`, `intelligence.image_gen`, `intelligence.lever_pullers`, `intelligence.trust_scorer`, `ollama.client` (+4)
**Imported by:** `api/routers/actor_detail.py`, `api/routers/supply_chain_helpers.py`

#### `api/routers/intel.py` — 2159 LOC
**Docstring:** GRID Intelligence API Product — the core paid API.
**Classes:** `Tier`
**Functions:** `async intel_search(q, type, limit, offset, _token)`, `async intel_entity_profile(name, _token)`, `async intel_actor_dossier(name, _token)`, `async intel_ticker(symbol, days, _token)`, `async intel_cross_reference(indicator, _token)`, `async intel_deep_dive(ticker, days, _token)`, `async intel_network(entity, depth, _token)`, `async intel_market_brief(_token)`, `async intel_predictions_active(ticker, model, limit, offset, _token)`, `async intel_predictions_track_record(model, ticker, timeframe, _token)`, `async intel_briefing(since, _token)`
**Reads:** `__future__`, `actors`, `analytical_snapshots`, `causal_chains`, `conditions`, `cross_reference_checks`, `dataclasses`, `datetime`, `enum`, `exc`, `fails`, `fastapi`, `flows`, `hard`, `icij_relationships`, `intelligence`, `loguru`, `options_mispricing_scans`, `oracle_predictions`, `regime_history`, `signal_data`, `signal_sources`, `sqlalchemy`, `thesis_snapshots`, `trust_scores`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.cross_reference`, `intelligence.lever_pullers`, `intelligence.news_impact`

#### `api/routers/canvas.py` — 1932 LOC
**Docstring:** GRID Canvas — Unified graph intelligence API.
**Classes:** `BoardCreate`; `BoardUpdate`
**Functions:** `async get_canvas_graph(center, depth, layers, since, limit, _token)`, `async get_node_detail(node_type, node_id, _token)`, `async expand_node(node_type, node_id, depth, layers, existing_ids, _token)`, `async create_board(body, _token)`, `async list_boards(_token)`, `async get_board(board_id, _token)`, `async update_board(board_id, body, _token)`, `async delete_board(board_id, _token)`, `async fork_board(board_id, _token)`, `async get_dot_connections(center, days, _token)`
**Reads:** `__future__`, `actor`, `actor_connections`, `actors`, `collections`, `datetime`, `discovered`, `dollar_flows`, `fastapi`, `hardcoded`, `investigation_boards`, `loguru`, `non`, `pydantic`, `signal_data`, `signal_registry`, `sqlalchemy`, `ticker`, `typing`, `wealth_flows`
**Writes:** `investigation_boards`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/astrogrid_helpers.py` — 1838 LOC
**Docstring:** AstroGrid shared helpers — Pydantic models and computation utilities.
**Classes:** `CompareDatesRequest`; `AstrogridInterpretRequest`; `AstrogridPredictionRequest`; `AstrogridGuruRequest`; `AstrogridScoreRequest`; `AstrogridBacktestRequest`; `AstrogridReviewRequest`; `AstrogridWeightDecisionRequest`; `AstrogridLearningLoopRequest`
**Reads:** `__future__`, `analysis`, `current`, `datetime`, `exc`, `feature_registry`, `grid`, `ingestion`, `loguru`, `ollama`, `oracle`, `pydantic`, `regime_history`, `resolved_series`, `sqlalchemy`, `typing`, `uuid`
**Imports from GRID:** `analysis.ephemeris`, `api.dependencies`, `api.routers.watchlist`, `ingestion.celestial.chinese`, `ingestion.celestial.lunar`, `ingestion.celestial.planetary`, `ingestion.celestial.solar`, `ingestion.celestial.vedic`, `ollama.client`, `oracle.astrogrid_universe`, `oracle.publish`, `oracle.scoreboard`
**Imported by:** `api/routers/astrogrid.py`, `api/routers/astrogrid_celestial.py`, `api/routers/astrogrid_core.py`, `api/routers/astrogrid_predictions.py`

#### `api/routers/system.py` — 1657 LOC
**Docstring:** System status and health endpoints.
**Functions:** `async health()`, `async status(_token)`, `async freshness(_token)`, `async pipeline_health(_token)`, `async get_logs(source, lines, _token)`, `async alerts(_token)`, `async restart_hyperspace(_token)`, `async trigger_ux_audit(_token)`, `async list_ux_audits(limit, _token)`, `async trigger_daily_digest(_token)`, `async run_taxonomy_audit_endpoint(_token)`, `set_hermes_state(state)`, `async hermes_status(_token)`, `async get_settings(_token)`, `async update_settings(payload, _token)`, `async get_api_keys(_token)`, `async get_services(_token)`, `async get_hermes_status(limit, _token)`, `async architecture(_token)`, `async get_resolution_audit(_token)` (+1 more)
**Reads:** `__future__`, `analysis`, `analytical_snapshots`, `config`, `datetime`, `decision_journal`, `fastapi`, `feature_registry`, `hyperspace`, `hypothesis_registry`, `intelligence`, `lateral`, `llm`, `loguru`, `model_registry`, `operator_issues`, `pathlib`, `pg_class`, `pwa`, `raw_series`, `resolved_series`, `running`, `scripts`, `server_log`, `source_catalog`
**Imports from GRID:** `analysis.taxonomy_audit`, `api.auth`, `api.dependencies`, `api.main`, `api.schemas.system`, `config`, `hyperspace.client`, `hyperspace.monitor`, `intelligence.resolution_audit`, `scripts.daily_digest`, `scripts.ux_auditor`

#### `api/routers/intelligence_actors.py` — 1507 LOC
**Docstring:** Intelligence sub-router: Actor network, post-mortems, and trend endpoints.
**Functions:** `async get_actor_network(limit, sector, _token)`, `async get_actor_detail(actor_id, _token)`, `async get_actor_analytics_endpoint(actor_id, _token)`, `async get_top_actors_endpoint(metric, limit, _token)`, `async get_communities_endpoint(_token)`, `async get_community_members_endpoint(community_id, limit, _token)`, `async get_postmortems(days, ticker, category, _token)`, `async trigger_batch_postmortem(days, _token)`, `async get_lessons_learned(days, _token)`, `async get_milestone_scorecard(_token)`, `async get_ticker_milestones(ticker, _token)`, `async get_attention_alerts(threshold, _token)`, `async get_actor_network_db(limit, min_degree, include_icij, _token)`, `async get_actor_enriched_profile(actor_id, _token)`, `async get_trends(days, _token)`, `async get_sector_power_map(sector_name, _token)`, `async ego_graph_search(q, limit, _token)`, `async get_ego_graph(actor_id, depth, max_nodes, _token)`, `async get_grand_power_map(limit, _token)`
**Reads:** `__future__`, `actor_connections`, `actors`, `all`, `analysis`, `company_profiles`, `conn_counts`, `dataclasses`, `datetime`, `each`, `existing`, `fastapi`, `influence_network`, `intelligence`, `loguru`, `other`, `ranked`, `sector_map`, `sqlalchemy`, `store`, `stored`, `typing`, `wealth_flows`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `intelligence.actor_network`, `intelligence.attention_anomaly`, `intelligence.dollar_flows`, `intelligence.influence_network`, `intelligence.milestone_tracker`, `intelligence.postmortem`, `intelligence.trend_tracker`, `store.graph`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/chat.py` — 1220 LOC
**Docstring:** GRID API — Ask GRID conversational chat endpoint.
**Classes:** `ChatMessage` [validate_role]; `ChatAskRequest` [validate_ticker, validate_timeframe]; `ChatAskResponse`
**Functions:** `async ask_grid(req)`
**Reads:** `__future__`, `analysis`, `chat`, `config`, `congressional_trades`, `cross_reference_checks`, `datetime`, `dealer`, `fastapi`, `feature_registry`, `history`, `insider_trades`, `intelligence`, `its`, `llm`, `loguru`, `ollama`, `options_daily_signals`, `oracle`, `physics`, `predictions`, `pydantic`, `reaching`, `regime_history`, `resolved_series`
**Imports from GRID:** `analysis.money_flow_engine`, `analysis.thesis_scorer`, `api.auth`, `config`, `db`, `intelligence.codebase_context`, `intelligence.deep_dive`, `intelligence.lever_pullers`, `intelligence.news_intel`, `intelligence.post_query_scanner`, `intelligence.sleuth`, `intelligence.thesis_tracker`, `ollama.client`, `oracle.citation_extractor`, `oracle.feedback_recorder` (+4)

#### `api/routers/canvas_expand.py` — 1145 LOC
**Docstring:** Canvas sub-router: graph expansion — expand network, path finding, suggest connections.
**Classes:** `PathRequest`
**Functions:** `async expand_node(board_id, node_id, depth, _token)`, `async find_path(board_id, body, _token)`, `async suggest_connections(board_id, _token)`
**Reads:** `__future__`, `actor_connections`, `actor_id_map`, `actors`, `bidirectional`, `canvas_boards`, `canvas_edges`, `canvas_nodes`, `company_profiles`, `congressional_trades`, `cross_reference_checks`, `datetime`, `discovered_hypotheses`, `dollar_flows`, `fastapi`, `insider_trades`, `investigation_leads`, `lever_pullers`, `loguru`, `oracle_predictions`, `pydantic`, `signal_data`, `signal_sources`, `sqlalchemy`, `typing`
**Writes:** `canvas_boards`, `canvas_edges`, `canvas_nodes`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/intelligence_risk.py` — 1137 LOC
**Docstring:** Intelligence sub-router: Risk map, dashboard, and globe endpoints.
**Functions:** `async get_risk_map(_token)`, `async get_globe_data(_token)`, `async get_intelligence_dashboard(_token)`
**Reads:** `__future__`, `all`, `batch`, `comtrade`, `dataclasses`, `datetime`, `dealer`, `fastapi`, `feature_registry`, `intelligence`, `loguru`, `physical`, `physics`, `raw_series`, `resolved_series`, `sqlalchemy`, `typing`, `utils`, `watchlist`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.cross_reference`, `intelligence.lever_pullers`, `intelligence.postmortem`, `intelligence.source_audit`, `intelligence.trust_scorer`, `physics.dealer_gamma`, `utils.ttl_cache`
**Imported by:** `api/main.py`, `api/routers/intelligence.py`

#### `api/routers/capital_flow.py` — 1121 LOC
**Docstring:** Capital-flow endpoint for actor profile pages.
**Functions:** `async get_capital_flow(actor_id, periods, period_type, _token)`
**Reads:** `__future__`, `analysis`, `capital_flows`, `datetime`, `fastapi`, `fiscal_period`, `intelligence`, `loguru`, `ranked`, `sqlalchemy`, `ticker_metrics_daily`, `typing`, `utils`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `features.lab` (percentile primitives), `utils.fx`, `utils.ttl_cache`

#### `api/routers/derivatives.py` — 995 LOC
**Docstring:** GRID API — Derivatives / Dealer Flow Intelligence endpoints.
**Functions:** `async get_overview()`, `async get_gex(ticker)`, `async get_regime()`, `async get_walls(ticker)`, `async get_vanna_charm(ticker)`, `async get_vol_surface(ticker)`, `async get_skew(ticker)`, `async get_term_structure(ticker)`, `async get_oi_heatmap(ticker)`, `async get_flow_narrative()`, `async generate_flow_narrative()`, `async get_signals(limit)`, `async get_scan(min_score)`, `async get_flow_timeline(ticker, days)`, `async get_history(ticker, days)`
**Reads:** `__future__`, `charm`, `collections`, `datetime`, `discovery`, `fastapi`, `from_date`, `gamma`, `latest`, `live`, `loguru`, `ollama`, `options_daily_signals`, `options_snapshots`, `physics`, `sqlalchemy`, `stabilizing`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `discovery.options_scanner`, `ollama.dealer_flow_briefing`, `physics.dealer_gamma`

#### `api/routers/explain.py` — 821 LOC
**Docstring:** Hero endpoint: "why did this actor move?" — ranked evidence synthesis.
**Functions:** `async get_actor_explain(actor_id, date, window_days, _token)`
**Reads:** `__future__`, `capital_flows`, `congressional_trades`, `contagion_predictions`, `corporate_actions`, `dark_pool_weekly`, `datetime`, `exc`, `fastapi`, `insider_trades`, `loguru`, `options_daily_signals`, `raw_series`, `sqlalchemy`, `supply_chain_nodes`, `supply_shock_attributions`, `typing`, `utils`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.routers.actor_detail`, `utils.ttl_cache`

#### `api/routers/watchlist_core.py` — 788 LOC
**Docstring:** Watchlist sub-router: core CRUD and utility endpoints.
**Functions:** `async list_watchlist(limit, offset, _token)`, `async refresh_watchlist_prices(_token)`, `async get_watchlist_prices(_token)`, `async get_portfolio(_token)`, `async list_watchlist_enriched(limit, _token)`, `async search_tickers(q, _token)`, `async add_to_watchlist(body, _token)`, `async preload_watchlist(_token)`, `async remove_from_watchlist(ticker, _token)`
**Reads:** `__future__`, `analysis`, `datetime`, `decision_journal`, `fastapi`, `feature_registry`, `loguru`, `options_daily_signals`, `options_recommendations`, `recommendation`, `resolved_series`, `sector`, `sector_map`, `sqlalchemy`, `watchlist`, `yfinance`
**Writes:** `watchlist`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `api.main`, `api.routers.watchlist_helpers`, `api.schemas.watchlist`
**Imported by:** `api/routers/watchlist.py`

#### `api/routers/discovery.py` — 762 LOC
**Docstring:** Discovery engine endpoints.
**Functions:** `async trigger_orthogonality(_token)`, `async trigger_clustering(n_components, _token)`, `async get_jobs(_token)`, `async get_orthogonality_results(_token)`, `async get_clustering_results(_token)`, `async get_hypothesis_results(verdict, sector, min_correlation, limit, _token)`, `async get_hypotheses(state, limit, offset, _token)`, `async get_backtest_results(min_sharpe, min_win_rate, family, limit, _token)`, `async run_backtest_scan(min_sharpe, min_win_rate, _token)`, `async run_hypothesis_review(_token)`, `async promote_hypothesis_to_feature(hypothesis_id, _token)`, `async get_correlation_matrix(period, regime, _token)`, `async smart_heatmap(family, orthogonal_only, corr_threshold, _token)`
**Reads:** `__future__`, `analysis`, `datetime`, `decision_journal`, `discovery`, `fastapi`, `feature_registry`, `hypothesis_registry`, `lateral`, `loguru`, `registry`, `resolved_series`, `sklearn`, `sqlalchemy`, `try`, `typing`, `validated`, `validation_results`
**Writes:** `feature_registry`, `hypothesis_registry`
**Imports from GRID:** `analysis.backtest_scanner`, `api.auth`, `api.dependencies`, `discovery.clustering`, `discovery.orthogonality`
**Imported by:** `api/routers/associations.py`

#### `api/routers/intelligence_news.py` — 695 LOC
**Docstring:** Intelligence sub-router: News, event sequences, and pattern engine endpoints.
**Functions:** `async get_news_feed_endpoint(ticker, hours, _token)`, `async get_news_stats_endpoint(hours, _token)`, `async get_narrative_shift_endpoint(ticker, days, _token)`, `async get_news_before_move_endpoint(ticker, move_date, _token)`, `async get_news_briefing_endpoint(_token)`, `async get_event_sequence(ticker, sector, days, with_lead_times, _token)`, `async get_recurring_patterns(min_occurrences, _token)`, `async get_discovered_patterns(min_occurrences, max_sequence_length, _token)`, `async get_active_patterns(_token)`, `async get_patterns_for_ticker_endpoint(ticker, _token)`, `async get_news_momentum(ticker, signal_type, hours, _token)`, `async get_momentum_divergences(hours, _token)`, `async run_momentum_scan(_token)`, `async get_active_deals(deal_type, ticker, _token)`, `async get_deal_pipeline_summary(_token)`, `async get_deal_history(ticker, days, _token)`, `async run_deal_scan(hours, _token)`, `async get_business_events(category, ticker, direction, hours, _token)`, `async get_business_event_summary(hours, _token)`, `async run_business_event_scan(hours, _token)` (+6 more)
**Reads:** `__future__`, `bullish`, `fastapi`, `intelligence`, `loguru`, `news`, `price`, `recent`, `sec`, `signal_sources`, `today`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.business_news_parser`, `intelligence.deal_detector`, `intelligence.earnings_transcript_analyzer`, `intelligence.event_sequence`, `intelligence.news_intel`, `intelligence.news_momentum`, `intelligence.pattern_engine`, `intelligence.sec_filing_extractor`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/astrogrid_celestial.py` — 688 LOC
**Docstring:** AstroGrid sub-router: ephemeris, correlations, timeline, briefing, compare,
**Functions:** `async get_ephemeris(date_str, _token)`, `async get_correlations(market_feature, celestial_category, lookback_days, _token)`, `async get_timeline(start, end, types, _token)`, `async get_briefing(_token)`, `async compare_dates(body, _token)`, `async get_retrogrades(_token)`, `async get_eclipses(_token)`, `async get_nakshatra(_token)`, `async get_lunar_calendar(month, year, _token)`, `async get_solar_activity(_token)`
**Reads:** `__future__`, `analysis`, `briefings`, `datetime`, `fastapi`, `feature_registry`, `loguru`, `pydantic`, `regime_history`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `analysis.astro_correlations`, `api.auth`, `api.dependencies`, `api.routers.astrogrid_helpers`
**Imported by:** `api/routers/astrogrid.py`

#### `api/routers/associations.py` — 664 LOC
**Docstring:** Feature association discovery endpoints.
**Functions:** `async get_correlation_matrix(days, min_corr, show_trivial, _token)`, `async get_lag_analysis(feature_a, feature_b, max_lag, days, _token)`, `async get_clusters(_token)`, `async get_regime_features(days, _token)`, `async get_anomalies(sigma_threshold, days, _token)`
**Reads:** `__future__`, `alerts`, `datetime`, `decision_journal`, `discovery`, `eliminating`, `fastapi`, `feature_id`, `feature_registry`, `loguru`, `metrics`, `real`, `sqlalchemy`, `transition`, `typing`
**Imports from GRID:** `alerts.email`, `api.auth`, `api.dependencies`, `api.routers.discovery`

#### `api/auth.py` — 656 LOC
**Docstring:** GRID JWT authentication with role-based access control.
**Functions:** `hash_password(password)`, `verify_password(plain, hashed)`, `create_token(role, username, expires_hours)`, `verify_token(token)`, `decode_token(token)`, `get_token_expiry(token)`, `async require_auth(request, credentials)`, `require_role()`, `async login(body, request)`, `async register(body, request)`, `async logout(_token)`, `async verify(token)`, `async create_user(body, _token)`, `async list_users(_token)`, `async delete_user(username, _token)`
**Reads:** `__future__`, `config`, `datetime`, `fastapi`, `grid_rate_limits`, `grid_users`, `jose`, `loguru`, `passlib`, `pathlib`, `token`, `typing`
**Writes:** `grid_rate_limits`, `grid_users`
**Imports from GRID:** `api.schemas.auth`, `config`
**Imported by:** `api/main.py`, `api/routers/a2a.py`, `api/routers/actor_detail.py`, `api/routers/actor_news_api.py`, `api/routers/agents.py`, `api/routers/associations.py`, `api/routers/astrogrid_celestial.py`, `api/routers/astrogrid_core.py`, `api/routers/astrogrid_predictions.py`, `api/routers/attributions.py` (+74)

#### `api/routers/watchlist_overview.py` — 635 LOC
**Docstring:** Watchlist sub-router: AI overview and insider-edge endpoints.
**Functions:** `async get_ticker_overview(ticker, _token)`, `async get_ticker_edge(ticker, user, engine)`
**Reads:** `__future__`, `analysis`, `datetime`, `decision_journal`, `fastapi`, `feature_registry`, `intelligence`, `investigation_leads`, `llm`, `loguru`, `ollama`, `options_daily_signals`, `resolved_series`, `signal_sources`, `spot`, `sqlalchemy`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `api.routers.watchlist_helpers`, `intelligence.actor_network`, `intelligence.lever_pullers`, `intelligence.trust_scorer`, `ollama.client`
**Imported by:** `api/routers/watchlist.py`

#### `api/routers/watchlist_helpers.py` — 624 LOC
**Docstring:** Watchlist shared helpers — utilities imported by sub-routers and external callers.
**Reads:** `__future__`, `datetime`, `decision_journal`, `feature_registry`, `loguru`, `normalization`, `options_daily_signals`, `raw_series`, `resolved_series`, `source_catalog`, `sqlalchemy`, `ticker`, `typing`, `utils`, `watchlist`, `yfinance`
**Writes:** `raw_series`, `source_catalog`
**Imports from GRID:** `api.dependencies`, `normalization.entity_map`, `utils.ttl_cache`
**Imported by:** `api/routers/watchlist.py`, `api/routers/watchlist_analysis.py`, `api/routers/watchlist_core.py`, `api/routers/watchlist_overview.py`

#### `api/main.py` — 613 LOC
**Docstring:** GRID Intelligence API — FastAPI application entry point.
**Classes:** `SecurityHeadersMiddleware` [dispatch]; `RateLimitMiddleware` [dispatch]; `X402PaymentMiddleware` [dispatch]
**Functions:** `async lifespan(app)`, `broadcast_event(event_type, data)`, `async websocket_endpoint(websocket)`
**Reads:** `__future__`, `agents`, `alerts`, `any`, `config`, `connection`, `contextlib`, `contracts`, `datetime`, `events`, `fastapi`, `importlib`, `loguru`, `orchestration`, `pathlib`, `payments`, `source`, `starlette`, `subnet`, `typing`
**Imports from GRID:** `agents.progress`, `alerts.push_notify`, `api.auth`, `api.dependencies`, `api.routers.contagion`, `api.routers.intelligence_risk`, `config`, `contracts.dead_letter`, `contracts.dispatcher`, `contracts.retry_scheduler`, `db`, `events.bus`, `subnet.distributed_compute`, `subnet.oauth_miner`
**Imported by:** `api/routers/system.py`, `api/routers/watchlist_core.py`, `intelligence/trust_scorer.py`, `trading/options_recommender.py`

#### `api/routers/contagion.py` — 590 LOC
**Docstring:** Chain contagion simulator endpoint.
**Functions:** `async simulate(shock_node, shock_type, magnitude, max_depth, source, pass_through, caller_id, _token)`, `async backtest(days, _token)`, `async get_scenarios(_token)`, `async get_contagion_matrix(sector_name, _token)`
**Reads:** `__future__`, `analysis`, `contagion_backtest_results`, `contagion_predictions`, `fastapi`, `intelligence`, `loguru`, `sqlalchemy`, `typing`, `utils`
**Writes:** `contagion_predictions`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `intelligence.chain_contagion`, `utils.ttl_cache`
**Imported by:** `api/main.py`

#### `api/routers/canvas_graph.py` — 574 LOC
**Docstring:** Canvas sub-router: node and edge CRUD + bulk graph save.
**Classes:** `NodeCreate`; `NodeUpdate`; `EdgeCreate`; `BulkGraphSave`; `EvidenceCreate`
**Functions:** `async add_node(board_id, body, _token)`, `async update_node(board_id, node_id, body, _token)`, `async delete_node(board_id, node_id, _token)`, `async add_edge(board_id, body, _token)`, `async delete_edge(board_id, edge_id, _token)`, `async bulk_save_graph(board_id, body, _token)`, `async add_evidence(board_id, node_id, body, _token)`, `async get_node_evidence(board_id, node_id, _token)`, `async delete_evidence(board_id, evidence_id, _token)`
**Reads:** `__future__`, `board`, `body`, `canvas_boards`, `canvas_edges`, `canvas_nodes`, `datetime`, `fastapi`, `hardcoded`, `investigation_evidence`, `loguru`, `provided`, `pydantic`, `sqlalchemy`, `typing`
**Writes:** `canvas_boards`, `canvas_edges`, `canvas_nodes`, `investigation_evidence`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/supply_chain_helpers.py` — 561 LOC
**Docstring:** Helpers for the supply_chain router.
**Reads:** `__future__`, `analysis`, `bfs`, `collections`, `datetime`, `edge`, `flows`, `hardcoded`, `loguru`, `sqlalchemy`, `supply_chain_edges`, `supply_chain_nodes`, `typing`
**Imports from GRID:** `analysis.sector_map`, `api.routers.flows`
**Imported by:** `api/routers/supply_chain.py`

#### `api/routers/intelligence_thesis.py` — 548 LOC
**Docstring:** Intelligence sub-router: Thesis, sleuth/leads, and market diary endpoints.
**Functions:** `async get_unified_thesis(_token)`, `async get_thesis_history_endpoint(days, _token)`, `async get_thesis_accuracy_endpoint(_token)`, `async get_thesis_postmortems_endpoint(days, _token)`, `async get_deep_dives_endpoint(days, limit, _token)`, `async get_deep_dive_endpoint(dive_id, _token)`, `async trigger_deep_dive(_token)`, `async get_research_archive(days, limit, _token)`, `async get_investigation_leads(status, category, limit, offset, _token)`, `async get_investigation_lead(lead_id, _token)`, `async investigate_lead(lead_id, _token)`, `async generate_leads(_token)`, `async run_daily_investigation(_token)`, `async get_diary(date, _token)`, `async list_diaries(limit, offset, _token)`, `async search_diaries(q, limit, _token)`, `async generate_diary(date, _token)`
**Reads:** `__future__`, `analysis`, `dataclasses`, `datetime`, `fastapi`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `analysis.flow_thesis`, `analysis.thesis_scorer`, `api.auth`, `api.dependencies`, `intelligence.audio_briefing`, `intelligence.deep_dive`, `intelligence.market_diary`, `intelligence.sleuth`, `intelligence.thesis_tracker`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/valuation.py` — 548 LOC
**Docstring:** GRID API — Valuation & Derivatives Support endpoints.
**Classes:** `MilestoneCreate`; `MilestoneStatusUpdate`; `AnalysisResponse`
**Functions:** `async analyze_ticker(ticker)`, `async generate_prompt(ticker)`, `async log_analysis_response(body)`, `async valuation_history(ticker, days)`, `async prediction_history(ticker, limit)`, `async get_milestones(ticker, status)`, `async add_milestone(body)`, `async update_milestone_status(milestone_id, body)`, `async derivatives_support(ticker)`, `async catalyst_timeline(ticker, months_forward, months_back, _token)`
**Reads:** `__future__`, `catalyst_calendar`, `company_milestones`, `company_valuations`, `datetime`, `discovered_hypotheses`, `fastapi`, `loguru`, `oracle_predictions`, `pydantic`, `raw_series`, `sqlalchemy`, `typing`, `valuation`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/trading.py` — 533 LOC
**Docstring:** Paper trading, Hyperliquid perp, and prediction market API endpoints.
**Classes:** `PolymarketBuyRequest`; `KalshiBuyRequest`; `HyperliquidTradeRequest`; `HyperliquidCloseRequest`; `TradeRequest`; `CloseTradeRequest`; `CreateWalletRequest`; `KillWalletRequest`; `PromoteToStrategyRequest`
**Functions:** `async trading_dashboard(_token)`, `async register_all_strategies(_token)`, `async open_trade(req, _token)`, `async close_trade(trade_id, req, _token)`, `async list_strategies(_token)`, `async strategy_trade_history(strategy_id, limit, _token)`, `async promote_to_strategy(req, _token)`, `async execute_signals_now(_token)`, `async kill_strategy(strategy_id, _token)`, `async wallet_dashboard(_token)`, `async list_wallets(exchange, status, _token)`, `async create_wallet(req, _token)`, `async get_wallet(wallet_id, _token)`, `async wallet_risk_check(wallet_id, _token)`, `async kill_wallet(wallet_id, req, _token)`, `async pause_wallet(wallet_id, _token)`, `async resume_wallet(wallet_id, _token)`, `async hyperliquid_balance(_token)`, `async hyperliquid_positions(_token)`, `async hyperliquid_trade(req, _token)` (+9 more)
**Reads:** `__future__`, `analysis`, `datetime`, `fastapi`, `loguru`, `paper_strategies`, `paper_trades`, `pydantic`, `sqlalchemy`, `trading`, `typing`
**Writes:** `paper_strategies`
**Imports from GRID:** `analysis.backtest_scanner`, `api.auth`, `api.dependencies`, `trading.hyperliquid`, `trading.options_recommender`, `trading.options_tracker`, `trading.paper_engine`, `trading.prediction_markets`, `trading.signal_executor`, `trading.wallet_manager`

#### `api/routers/vault.py` — 533 LOC
**Docstring:** Vault API router — Obsidian Bridge CRUD, FTS search, dashboard, and sync trigger.
**Functions:** `async list_notes(domain, status_filter, limit, offset)`, `async get_note(note_id)`, `async create_note(payload)`, `async update_note_status(note_id, payload)`, `async search_notes(q, domain, limit, offset)`, `async list_actions(note_id, limit, offset)`, `async get_dashboard()`, `async trigger_sync()`, `async generate_backlinks()`, `async generate_concept_stubs()`
**Reads:** `__future__`, `datetime`, `fastapi`, `ingestion`, `loguru`, `obsidian_actions`, `obsidian_notes`, `pathlib`, `scripts`, `sqlalchemy`, `typing`
**Writes:** `obsidian_actions`, `obsidian_notes`
**Imports from GRID:** `api.auth`, `api.dependencies`, `ingestion.altdata.obsidian_sync`, `scripts.create_concept_stubs`, `scripts.obsidian_backlinks`

#### `api/routers/astrogrid_predictions.py` — 513 LOC
**Docstring:** AstroGrid sub-router: predictions, backtest, weights, review, learning-loop.
**Functions:** `async create_prediction(req, _token)`, `async ask_guru(req, request)`, `async get_latest_predictions(limit, offset)`, `async get_postmortems(limit, offset)`, `async score_predictions(req, _token)`, `async get_prediction_scoreboard(_token)`, `async run_backtest(req, _token)`, `async get_backtest_summary(limit, _token)`, `async get_backtest_results(strategy_variant, limit, _token)`, `async get_current_weights(_token)`, `async generate_review_run(req, _token)`, `async run_learning_loop(req, _token)`, `async get_latest_review(_token)`, `async get_weight_proposals(status, limit, _token)`, `async approve_weight_proposal(weight_proposal_id, req, _token)`, `async reject_weight_proposal(weight_proposal_id, req, _token)`, `async get_prediction_detail(prediction_id, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `loguru`, `typing`, `uuid`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.routers.astrogrid_helpers`
**Imported by:** `api/routers/astrogrid.py`

#### `api/routers/actor_detail.py` — 490 LOC
**Docstring:** Actor detail endpoint for SectorDive profile drawer.
**Functions:** `async get_actor_detail_for_drawer(actor_id, sector, _token)`
**Reads:** `__future__`, `analysis`, `congressional_trades`, `dark_pool_weekly`, `datetime`, `fastapi`, `flows`, `insider_trades`, `institutional_holdings`, `intelligence`, `loguru`, `options_daily_signals`, `raw_series`, `sqlalchemy`, `ticker_metadata`, `ticker_metrics_daily`, `typing`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `api.routers`, `api.routers.flows`, `intelligence.holder_deal_overlap`, `intelligence.trust_scorer`
**Imported by:** `api/routers/explain.py`

#### `api/routers/canvas_investigate.py` — 470 LOC
**Docstring:** Canvas sub-router: auto-investigate — build rich investigation boards from a search query.
**Classes:** `InvestigateRequest`; `InvestigateResponse`
**Functions:** `async auto_investigate(req, engine, _)`
**Reads:** `__future__`, `actor_connections`, `actors`, `canvas_edges`, `canvas_nodes`, `datetime`, `events`, `fastapi`, `loguru`, `pydantic`, `signal_data`, `sqlalchemy`, `typing`, `wealth_flows`
**Writes:** `canvas_boards`, `canvas_edges`, `canvas_nodes`
**Imports from GRID:** `api.auth`, `api.dependencies`, `events.producer`

#### `api/routers/astrogrid_core.py` — 466 LOC
**Docstring:** AstroGrid sub-router: overview, snapshot, scorecard, universe, interpret.
**Functions:** `async get_overview(_token)`, `async get_snapshot(date_str, _token)`, `async get_scorecard(_token)`, `async get_scoreable_universe(_token)`, `async interpret_snapshot(req, _token)`
**Reads:** `__future__`, `collections`, `datetime`, `fastapi`, `loguru`, `ollama`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.routers.astrogrid_helpers`, `api.routers.watchlist`, `ollama.client`
**Imported by:** `api/routers/astrogrid.py`

#### `api/routers/intelligence_forensics.py` — 466 LOC
**Docstring:** Intelligence sub-router: Forensics, causation, influence network, and export controls.
**Functions:** `async get_forensic_reports(ticker, days, _token)`, `async analyze_forensic_move(ticker, date, lookback, _token)`, `async get_causation(ticker, days, _token)`, `async get_suspicious_trades_endpoint(days, _token)`, `async get_causal_narrative_endpoint(ticker, _token)`, `async get_causal_chains(ticker, hops, days, _token)`, `async get_active_causal_chains(_token)`, `async get_influence_network(ticker, _token)`, `async get_circular_flows(_token)`, `async get_vote_trade_hypocrisy(_token)`, `async get_export_controls(ticker, days, _token)`, `async get_export_control_impact(ticker, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `intelligence`, `loguru`, `signal_sources`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.causation`, `intelligence.export_intel`, `intelligence.forensics`, `intelligence.influence_network`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/physics.py` — 454 LOC
**Docstring:** GRID API — Market physics endpoints.
**Functions:** `async verify(as_of)`, `async momentum(as_of, lookback_days)`, `async list_conventions()`, `async get_convention(domain)`, `async ou_parameters(feature, window)`, `async hurst(feature, max_lag)`, `async energy_decomposition(feature, short_window, long_window)`, `async news_energy(lookback_days, as_of)`, `async physics_dashboard(as_of)`
**Reads:** `__future__`, `baseline`, `crucix`, `datetime`, `equilibrium`, `fastapi`, `features`, `loguru`, `physics`, `regime`, `store`, `typing`
**Imports from GRID:** `api.auth`, `db`, `features.lab`, `physics.conventions`, `physics.momentum`, `physics.news_energy`, `physics.transforms`, `physics.verify`, `store.pit`

#### `api/routers/mcp_export.py` — 452 LOC
**Docstring:** MCP export endpoints — lightweight JSON wrappers for GRID intelligence.
**Functions:** `async mcp_trust_score(actor, window_days, _token)`, `async mcp_actor_profile(name, _token)`, `async mcp_predictions(symbol, lookback_days, limit, _token)`, `async mcp_prediction_accuracy(group_by, lookback_days, _token)`, `async mcp_data_freshness(_token)`, `async mcp_signal_sources(symbol, lookback_days, _token)`, `async mcp_wealth_flows(actor, lookback_days, limit, _token)`, `async mcp_regime(_token)`
**Reads:** `__future__`, `actors`, `datetime`, `decision_journal`, `fastapi`, `hardcoded`, `loguru`, `oracle_predictions`, `raw_series`, `signal_sources`, `source_catalog`, `sqlalchemy`, `typing`, `wealth_flows`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/regime.py` — 450 LOC
**Docstring:** Regime state endpoints.
**Classes:** `WeightUpdateRequest`
**Functions:** `async get_weights(_token)`, `async update_weights(req, _token)`, `async simulate_weights(req, _token)`, `async get_current(_token)`, `async get_all_active(_token)`, `async get_synthesis(_token)`, `async get_history(days, _token)`, `async get_transitions(_token)`
**Reads:** `__future__`, `counterfactual`, `datetime`, `decision_journal`, `discovery`, `exc`, `fastapi`, `feature_registry`, `loguru`, `model_registry`, `ollama`, `pathlib`, `pydantic`, `resolved_series`, `scripts`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.schemas.regime`, `discovery.clustering`, `ollama.client`, `scripts.auto_regime`

#### `api/routers/options.py` — 436 LOC
**Docstring:** Options scanner API endpoints.
**Functions:** `async get_recommendations(ticker, _token)`, `async refresh_recommendations(_token)`, `async get_recommendation_history(ticker, outcome, limit, offset, _token)`, `async get_options_signals(ticker, limit, _token)`, `async scan_mispricing(min_score, _token)`, `async get_100x_opportunities(_token)`, `async get_scan_history(ticker, days, only_100x, limit, _token)`
**Reads:** `__future__`, `datetime`, `discovery`, `fastapi`, `loguru`, `options_daily_signals`, `options_mispricing_scans`, `options_recommendations`, `sqlalchemy`, `trading`, `typing`
**Writes:** `options_recommendations`
**Imports from GRID:** `api.auth`, `api.dependencies`, `discovery.options_scanner`, `trading.options_recommender`

#### `api/routers/intelligence_companies.py` — 429 LOC
**Docstring:** Intelligence sub-router: Company analyzer, deep graph, and institutional map.
**Functions:** `async get_all_company_profiles(_token)`, `async get_cross_company_patterns(_token)`, `async get_sector_influence_report(sector, _token)`, `async trigger_company_analysis(ticker, _token)`, `async get_company_profile(ticker, _token)`, `async get_deep_graph(ticker, depth, _token)`, `async get_overlaps(ticker_a, ticker_b, _token)`, `async get_all_overlaps(_token)`, `async get_connection_map(ticker, depth, _token)`, `async get_institutional_map(_token)`, `async trace_pension(pension_name, _token)`, `async get_fund_fees(fund_name, _token)`, `async get_institutional_conflicts(_token)`, `async get_hidden_influence(_token)`
**Reads:** `__future__`, `beneficiary`, `both`, `fastapi`, `intelligence`, `loguru`, `pension`, `typing`, `utils`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.company_analyzer`, `intelligence.deep_graph`, `intelligence.institutional_map`, `utils.ttl_cache`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/oracle.py` — 420 LOC
**Docstring:** Oracle prediction endpoints — predictions, scoreboard, latest cycle.
**Classes:** `OraclePublishRequest`
**Functions:** `async get_predictions(ticker, model, status, limit, offset, _token)`, `async get_scoreboard(_token)`, `async get_latest(_token)`, `async publish_prediction(req, _token)`, `async trigger_evolve(_token)`, `async get_scorecard(_token)`, `async get_guard_verdicts(_token)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `exc`, `fastapi`, `last`, `latest`, `loguru`, `most`, `options_daily_signals`, `oracle`, `oracle_models`, `oracle_predictions`, `pydantic`, `sqlalchemy`, `static`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `oracle.engine`, `oracle.hallucination_guard`, `oracle.publish`, `oracle.scoreboard`

#### `api/routers/canvas_core.py` — 394 LOC
**Docstring:** Canvas sub-router: board CRUD endpoints.
**Classes:** `BoardCreate`; `BoardUpdate`
**Functions:** `async list_boards(limit, offset, _token)`, `async create_board(body, _token)`, `async get_board(board_id, _token)`, `async update_board(board_id, body, _token)`, `async delete_board(board_id, _token)`
**Reads:** `__future__`, `actors`, `board`, `canvas_boards`, `canvas_edges`, `canvas_nodes`, `company_profiles`, `datetime`, `fastapi`, `hardcoded`, `loguru`, `provided`, `pydantic`, `raw_series`, `signal_data`, `sqlalchemy`, `typing`
**Writes:** `canvas_boards`, `canvas_nodes`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/user_intel.py` — 390 LOC
**Docstring:** User-contributed intelligence router.
**Classes:** `IntelSubmission`; `VotePayload`; `VerifyPayload`
**Functions:** `async submit_intel(actor_id, body, submitted_by)`, `async get_actor_intel(actor_id, limit, viewer_id)`, `async vote_intel(intel_id, vote, user_id)`, `async flag_intel(intel_id, user_id)`, `async verify_intel(intel_id, action, verifier)`, `async list_pending_intel(limit)`, `async http_submit_intel(actor_id, body, token)`, `async http_get_actor_intel(actor_id, limit, token)`, `async http_vote_intel(intel_id, body, token)`, `async http_flag_intel(intel_id, token)`, `async http_verify_intel(intel_id, body, _token)`, `async http_list_pending(limit, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `ledger`, `loguru`, `pydantic`, `same`, `sqlalchemy`, `typing`, `user_intel`, `user_intel_votes`
**Writes:** `user_intel`, `user_intel_votes`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/signals.py` — 387 LOC
**Docstring:** Live signals endpoints.
**Functions:** `async get_signals(_token)`, `async get_snapshot(_token)`, `async crucix_signals(_token, engine)`, `async get_timeseries(features, days, _token)`, `async get_conviction_scores(min_score, _token)`, `async get_conviction_ticker(ticker, _token)`, `async get_timeframes(feature, periods, _token)`
**Reads:** `__future__`, `alpha_research`, `datetime`, `exc`, `fastapi`, `feature`, `feature_registry`, `historical`, `inference`, `loguru`, `raw_series`, `registry`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `alpha_research.conviction_scorer`, `api.auth`, `api.dependencies`, `inference.live`

#### `api/routers/ollama.py` — 340 LOC
**Docstring:** GRID API — LLM integration endpoints.
**Classes:** `BriefingRequest`; `AskRequest`; `ExplainRequest`; `HypothesisRequest`; `RegimeAnalysisRequest`; `CapitalFlowRequest`
**Functions:** `async ollama_status()`, `async generate_briefing(req)`, `async get_latest_briefing(briefing_type)`, `async list_briefings(briefing_type, limit)`, `async read_briefing(filename)`, `async ask_ollama(req)`, `async explain_relationship(req)`, `async generate_hypotheses(req)`, `async analyze_regime(req)`, `async capital_flow_research(req)`
**Reads:** `__future__`, `all`, `analysis`, `config`, `datetime`, `fastapi`, `knowledge`, `loguru`, `ollama`, `pathlib`, `pydantic`, `typing`
**Imports from GRID:** `analysis.capital_flows`, `api.auth`, `config`, `db`, `knowledge.tree`, `ollama.client`, `ollama.market_briefing`, `ollama.reasoner`

#### `api/routers/feed.py` — 334 LOC
**Docstring:** GRID Signal Feed — running list of anomalies, discoveries, and interesting signals.
**Functions:** `async get_signal_feed(limit, offset, signal_type, severity, ticker, _auth)`, `async get_latest_signals(hours, _auth)`, `async get_rss_feed(limit)`, `async get_atom_feed(limit)`, `async get_live_feed(limit, signal_type, entities, _auth)`
**Reads:** `__future__`, `config`, `datetime`, `fastapi`, `grid`, `loguru`, `signal_data`, `signal_feed`, `sqlalchemy`, `static`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `config`

#### `api/routers/intelligence_govflow.py` — 325 LOC
**Docstring:** Intelligence sub-router: Government contracts, dollar flows, and legislative intelligence.
**Functions:** `async get_gov_contracts(ticker, days, _token)`, `async get_contract_insider_overlaps(days, window, _token)`, `async get_dollar_flows(ticker, sector, days, _token)`, `async trigger_dollar_flow_normalization(days, _token)`, `async get_legislation_overview(ticker, committee, _token)`, `async get_legislation_hearings(days, _token)`, `async get_legislation_trading_alerts(days, severity, _token)`
**Reads:** `__future__`, `fastapi`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.dollar_flows`, `intelligence.gov_intel`, `intelligence.legislative_intel`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/models.py` — 311 LOC
**Docstring:** Model registry endpoints.
**Functions:** `async get_all(layer, state, limit, offset, _token)`, `async get_production(_token)`, `async get_one(model_id, _token)`, `async transition_model(model_id, body, _token)`, `async rollback_model(model_id, _token)`, `async create_from_hypothesis(hypothesis_id, body, _token)`, `async get_feature_importance(model_id, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `features`, `hypothesis`, `hypothesis_registry`, `loguru`, `model_registry`, `produces`, `sqlalchemy`, `typing`, `validation_results`
**Writes:** `model_registry`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.schemas.models`, `features.importance`

#### `api/routers/canvas_predict.py` — 291 LOC
**Docstring:** Canvas sub-router: convert canvas investigation to scored prediction.
**Classes:** `PredictionRequest`; `PredictionResponse`
**Functions:** `async create_prediction(req, _token)`
**Reads:** `__future__`, `board`, `canvas`, `canvas_nodes`, `fastapi`, `loguru`, `pydantic`, `sqlalchemy`
**Writes:** `canvas_boards`, `canvas_nodes`, `discovered_hypotheses`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/intelligence_deepdive.py` — 288 LOC
**Docstring:** Intelligence sub-router: Global lever map, deep-dive, and expectations endpoints.
**Functions:** `async get_levers(_token)`, `async get_lever_domain_endpoint(domain, _token)`, `async trace_lever_chain_endpoint(event, _token)`, `async get_cross_domain_actors_endpoint(_token)`, `async get_lever_report_endpoint(_token)`, `async get_deep_dive(ticker, days, _token)`, `async run_mag7_deep_dives(days, _token)`, `async get_expectations(ticker, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.global_levers`, `intelligence.news_impact`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/canvas_llm.py` — 276 LOC
**Docstring:** Canvas sub-router: LLM-powered intelligence features.
**Classes:** `ExplainRequest`; `ExplainResponse`
**Functions:** `async explain_connection(req, engine, _)`
**Reads:** `__future__`, `actor_connections`, `canvas_nodes`, `datetime`, `fastapi`, `llm`, `loguru`, `patterns`, `pydantic`, `signal_data`, `source`, `sqlalchemy`, `typing`, `wealth_flows`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/watchlist_analysis.py` — 276 LOC
**Docstring:** Watchlist sub-router: per-ticker technical analysis endpoint.
**Functions:** `async get_ticker_analysis(ticker, period, _token)`
**Reads:** `__future__`, `both`, `datetime`, `decision_journal`, `fastapi`, `feature_registry`, `loguru`, `options_daily_signals`, `raw_series`, `resolved_series`, `source_catalog`, `sqlalchemy`, `typing`, `watchlist`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.routers.watchlist_helpers`
**Imported by:** `api/routers/watchlist.py`

#### `api/routers/geo.py` — 259 LOC
**Docstring:** Geo-spatial data endpoints for flow visualization.
**Functions:** `async get_geo_flows(flow_type, days, min_amount, engine, _)`, `async get_geo_actors(min_influence, category, limit, engine, _)`, `async get_signal_density(days, engine, _)`
**Reads:** `__future__`, `actor`, `actors`, `common`, `dollar_flows`, `fastapi`, `loguru`, `name`, `signal_data`, `sqlalchemy`, `typing`, `wealth_flows`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/search.py` — 256 LOC
**Docstring:** Universal search endpoint — searches across all GRID registries.
**Functions:** `async search_everything(q, _user, engine)`
**Reads:** `__future__`, `actors`, `fastapi`, `feature_registry`, `hypotheses`, `loguru`, `source_catalog`, `sqlalchemy`, `watchlist`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/viz.py` — 245 LOC
**Docstring:** GRID Visualization Intelligence API.
**Functions:** `async recommend_visualization(description, features, relation, question)`, `async list_visualization_rules()`, `async get_source_weights(families)`, `async capital_flow_viz_spec()`, `async regime_phase_viz_spec()`, `async feature_network_viz_spec()`, `async energy_particle_viz_spec()`, `async sector_orbital_viz_spec()`, `async lead_lag_river_viz_spec()`
**Reads:** `__future__`, `analysis`, `datetime`, `fastapi`, `loguru`, `typing`
**Imports from GRID:** `analysis.viz_intelligence`

#### `api/routers/forecasts.py` — 234 LOC
**Docstring:** TimesFM forecast endpoints — generate and retrieve time-series forecasts.
**Classes:** `ForecastRequest`; `BatchForecastRequest`; `ForecastResponse`
**Functions:** `async forecast_health(_token)`, `async generate_forecast(req, _token)`, `async batch_forecast(req, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `loguru`, `pydantic`, `resolved_series`, `sqlalchemy`, `timeseries`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `timeseries.timesfm_forecaster`

#### `api/routers/tradingview.py` — 227 LOC
**Docstring:** TradingView webhook integration.
**Functions:** `async receive_webhook(request, _key)`, `async get_signals(limit, ticker, _token)`
**Reads:** `__future__`, `config`, `datetime`, `fastapi`, `header`, `loguru`, `raw_series`, `source_catalog`, `sqlalchemy`, `tradingview`, `typing`
**Writes:** `raw_series`, `source_catalog`
**Imports from GRID:** `api.auth`, `api.dependencies`, `config`

#### `api/routers/intelligence_regime.py` — 224 LOC
**Docstring:** Regime-matched analog engine API endpoints.
**Functions:** `async get_regime(_token)`, `async get_regime_analogs(n, min_quality, include_timesfm, _token)`, `async get_regime_history(days, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `intelligence`, `loguru`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.regime.classifier`, `intelligence.regime.episode_matcher`, `intelligence.regime.forecast`, `intelligence.regime.state_vector`

#### `api/routers/briefing.py` — 213 LOC
**Docstring:** GRID API — Market Briefing & Sentiment Endpoints.
**Functions:** `async get_current_sentiment(_token)`, `async get_latest_briefing(briefing_type, _token)`, `async get_briefing_history(briefing_type, days, _token)`, `async get_sentiment_history(days, _token)`, `async get_sentiment_accuracy(_token)`, `async trigger_briefing(briefing_type, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `intelligence`, `loguru`, `market_briefings`, `ollama`, `sentiment_predictions`, `sentiment_weights`, `sqlalchemy`
**Imports from GRID:** `api.auth`, `db`, `intelligence.sentiment_scorer`, `ollama.market_briefing`

#### `api/routers/intel_cross_reference.py` — 193 LOC
**Docstring:** Cross-reference intelligence endpoints — lie detector for government statistics.
**Functions:** `async get_cross_reference(_token)`, `async get_cross_reference_by_category(category, _token)`, `async get_cross_reference_for_ticker(ticker, _token)`, `async get_cross_reference_history(category, days, assessment, _token)`
**Reads:** `__future__`, `dataclasses`, `fastapi`, `ground`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.cross_reference`

#### `api/routers/agents.py` — 193 LOC
**Docstring:** GRID API — TradingAgents router.
**Classes:** `RunRequest`; `BacktestRequest`
**Functions:** `async agent_status()`, `async trigger_run(req, background_tasks)`, `async trigger_run_sync(req)`, `async list_runs(limit)`, `async get_run(run_id)`, `async run_backtest(req)`, `async backtest_summary(days_back)`, `async get_schedule()`, `async start_schedule()`, `async stop_schedule()`
**Reads:** `__future__`, `agents`, `backgroundtasks`, `config`, `datetime`, `fastapi`, `loguru`, `pydantic`, `typing`
**Imports from GRID:** `agents.backtest`, `agents.runner`, `agents.scheduler`, `api.auth`, `config`, `db`

#### `api/routers/divergence.py` — 193 LOC
**Docstring:** Fundamental-vs-price divergence endpoints.
**Functions:** `async list_divergence(classification, limit, _token)`, `async get_actor_divergence(actor_id, _token)`
**Reads:** `__future__`, `datetime`, `exc`, `fastapi`, `fundamental_divergence`, `loguru`, `sqlalchemy`, `typing`, `utils`
**Imports from GRID:** `api.auth`, `api.dependencies`, `utils.ttl_cache`

#### `api/routers/a2a.py` — 192 LOC
**Docstring:** A2A Protocol endpoints — Agent Card discovery and task management.
**Classes:** `TaskSubmitRequest`; `TaskResponse`
**Functions:** `async get_agent_card()`, `async submit_task(req, _token)`, `async get_task(task_id, _token)`, `async cancel_task(task_id, _token)`, `async list_tasks(state, limit, _token)`
**Reads:** `__future__`, `a2a`, `config`, `fastapi`, `loguru`, `pydantic`, `typing`
**Imports from GRID:** `a2a.agent_card`, `a2a.client`, `a2a.server`, `api.auth`, `config`

#### `api/routers/celestial.py` — 190 LOC
**Docstring:** Celestial signals endpoint.
**Functions:** `async get_celestial_signals(_token)`, `async get_celestial_briefing(_token)`, `async generate_celestial_briefing(_token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `feature_registry`, `loguru`, `ollama`, `sqlalchemy`, `typing`, `validated`
**Imports from GRID:** `api.auth`, `api.dependencies`, `ollama.celestial_briefing`

#### `api/routers/trials.py` — 187 LOC
**Docstring:** Trial gem hunter endpoints.
**Functions:** `async get_gems(_token)`, `async get_signals(_token, signal_type, limit, offset)`, `async get_catalysts(_token)`, `async get_sponsors(_token)`, `async get_stats(_token)`
**Reads:** `__future__`, `actors`, `exc`, `fastapi`, `loguru`, `sqlalchemy`, `trial_gems`, `trial_signals`, `typing`, `upcoming_catalysts`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/notifications.py` — 184 LOC
**Docstring:** GRID Intelligence — Push notification API endpoints.
**Classes:** `SubscriptionKeys`; `SubscribeRequest` [validate_endpoint_url]; `UnsubscribeRequest`; `PreferencesUpdate`; `PreferencesQuery`
**Functions:** `async get_vapid_key()`, `async subscribe(req)`, `async unsubscribe(req)`, `async get_preferences(endpoint)`, `async update_preferences(req)`, `async test_push(req)`
**Reads:** `__future__`, `alerts`, `config`, `fastapi`, `pydantic`, `urllib`
**Imports from GRID:** `alerts.push_notify`, `api.auth`, `config`

#### `api/routers/prediction_backtest.py` — 184 LOC
**Docstring:** GRID API — Prediction Market Backtesting endpoints.
**Classes:** `HypothesisRequest`; `ExportRequest`
**Functions:** `list_strategies()`, `search_markets(platform, search, limit, engine)`, `run_hypothesis(req, engine)`, `dataset_stats(engine)`, `export_trades(req, engine)`
**Reads:** `__future__`, `fastapi`, `loguru`, `prediction_market_markets`, `prediction_market_trades`, `pydantic`, `sqlalchemy`, `trading`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `trading.prediction_backtest`

#### `api/routers/config.py` — 181 LOC
**Docstring:** System configuration endpoints.
**Functions:** `async get_config(_token)`, `async update_config(body, _token)`, `async get_sources(limit, offset, _token)`, `async update_source(source_id, body, _token)`, `async get_features(limit, offset, _token)`, `async update_feature(feature_id, body, _token)`
**Reads:** `__future__`, `config`, `fastapi`, `feature_registry`, `source_catalog`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `config`

#### `api/routers/intelligence_spider.py` — 179 LOC
**Docstring:** Spider API endpoints — status, stats, inject, neighborhood, path finding.
**Classes:** `InjectActorRequest`
**Functions:** `get_graph()`, `async get_neighborhood(actor_id, depth, max_nodes, _token)`, `async get_shortest_path(actor_id, target_id, _token)`, `async get_actor_connections(actor_id, _token)`, `async get_spider_stats(_token)`, `async inject_actor(body, _token)`
**Reads:** `__future__`, `fastapi`, `intelligence`, `loguru`, `pydantic`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.spider.db`, `intelligence.spider.entity_resolver`

#### `api/routers/workflows.py` — 177 LOC
**Docstring:** GRID API — Workflow management endpoints.
**Functions:** `async list_workflows()`, `async list_enabled()`, `async enable(name)`, `async disable(name)`, `async run_workflow(name)`, `async validate(name)`, `async get_waves()`, `async get_schedule()`
**Reads:** `__future__`, `fastapi`, `loguru`, `pathlib`, `physics`, `typing`, `workflows`
**Imports from GRID:** `api.auth`, `physics.waves`

#### `api/routers/backtest.py` — 174 LOC
**Docstring:** GRID API — Backtest & paper trade endpoints.
**Classes:** `BacktestRequest`
**Functions:** `async run_backtest(req)`, `async get_results()`, `async get_summary()`, `async generate_charts()`, `async get_chart(name)`, `async create_paper_trade()`, `async list_paper_trades()`, `async get_paper_trade(filename)`, `async score_predictions()`
**Reads:** `__future__`, `backtest`, `datetime`, `fastapi`, `latest`, `loguru`, `pathlib`, `pydantic`, `typing`
**Imports from GRID:** `api.auth`, `backtest.charts`, `backtest.engine`, `backtest.paper_trade`

#### `api/routers/signal_registry.py` — 173 LOC
**Docstring:** Signal Registry, Model Factory & Ensemble API endpoints.
**Classes:** `EnsemblePredictRequest`
**Functions:** `async list_signals(ticker, source_module, signal_type, limit)`, `async signal_stats()`, `async signals_for_ticker(ticker, limit)`, `async refresh_registry(_token)`, `async list_models()`, `async get_model(model_name)`, `async ensemble_predict(body, _token)`
**Reads:** `__future__`, `datetime`, `fastapi`, `intelligence`, `loguru`, `oracle`, `pydantic`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.adapters`, `intelligence.adapters.base`, `intelligence.signal_registry`, `oracle.model_factory`, `oracle.signal_aggregator`

#### `api/routers/intelligence_search.py` — 168 LOC
**Docstring:** Full-text intelligence search across the entire GRID corpus.
**Functions:** `async search_intelligence(q, types, limit, offset, _user)`, `async refresh_intelligence_search(_user)`
**Reads:** `__future__`, `fastapi`, `intelligence_search`, `loguru`, `sqlalchemy`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/actor_news_api.py` — 167 LOC
**Docstring:** Actor news endpoint — serves rows from the actor_news table.
**Functions:** `async get_actor_news(actor_id, limit, _token)`
**Reads:** `__future__`, `actor_news`, `fastapi`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/journal.py` — 162 LOC
**Docstring:** Decision journal endpoints.
**Functions:** `async get_all(limit, offset, verdict, _token)`, `async get_stats(_token)`, `async get_one(entry_id, _token)`, `async create(body, _token)`, `async record_outcome(entry_id, body, _token)`
**Reads:** `__future__`, `decision_journal`, `fastapi`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.schemas.journal`

#### `api/routers/snapshots.py` — 154 LOC
**Docstring:** Analytical snapshot query endpoints.
**Functions:** `get_latest_snapshots(category, n, _user)`, `get_snapshot_history(category, start_date, end_date, _user)`, `compare_snapshots(category, date_a, date_b, _user)`, `list_categories(_user)`, `get_operator_issues(days_back, category, severity, _user)`
**Reads:** `__future__`, `datetime`, `different`, `fastapi`, `loguru`, `operator_issues`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `store.snapshots`

#### `api/schemas/system.py` — 148 LOC
**Docstring:** System status schemas.
**Classes:** `HyperspaceStatus`; `DatabaseStatus`; `GridStats`; `ServerHealth`; `SystemStatusResponse`; `HealthResponse`; `LogsResponse`; `RestartResponse`; `FamilyFreshness`; `FreshnessResponse`; `HermesTaskStatus`; `HermesStatusResponse`; `PipelineSourceStatus`; `PipelineSummary`; `FamilyCoverage`; `ResolverStatus`; `PipelineError`; `PipelineHealthResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/system.py`

#### `api/routers/intelligence_causation.py` — 139 LOC
**Docstring:** Intelligence sub-router: Causal links for the Timeline forensic visualization.
**Functions:** `async get_causal_links(ticker, days, _token)`
**Reads:** `__future__`, `causal_links`, `datetime`, `evidence`, `fastapi`, `loguru`, `signal_data`, `sqlalchemy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`
**Imported by:** `api/routers/intelligence.py`

#### `api/routers/model_comparison.py` — 137 LOC
**Docstring:** Model comparison and drift monitoring endpoints.
**Functions:** `async shadow_vs_production(layer, days_back, _token)`, `async drift_report(model_id, _token)`, `async metrics_comparison(model_ids, _token)`
**Reads:** `__future__`, `fastapi`, `features`, `loguru`, `model_artifacts`, `model_registry`, `shadow_scores`, `sqlalchemy`, `typing`, `validation_results`
**Imports from GRID:** `api.auth`, `api.dependencies`, `features.importance`

#### `api/routers/earnings.py` — 135 LOC
**Docstring:** Earnings calendar & prediction endpoints.
**Functions:** `async get_earnings_calendar(days_ahead, _token)`, `async get_recent_earnings(days_back, _token)`, `async get_earnings_surprise(ticker, _token)`, `async predict_earnings(ticker, _token)`, `async get_earnings_scorecard(_token)`, `async get_earnings_history(ticker, limit, _token)`, `async run_earnings_cycle(_token)`
**Reads:** `__future__`, `fastapi`, `ingestion`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `ingestion.altdata.earnings_calendar`, `intelligence.earnings_intel`

#### `api/routers/sse.py` — 132 LOC
**Docstring:** Server-Sent Events endpoint for real-time event streaming.
**Functions:** `async event_stream(request, channels, _token)`, `async list_channels(_token)`, `async list_topics(_token)`
**Reads:** `__future__`, `all`, `events`, `fastapi`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `events.bus`, `events.channels`, `events.consumer`, `events.producer`

#### `api/routers/intel_source_audit.py` — 121 LOC
**Docstring:** Source audit endpoints — track accuracy, redundancy, and discrepancies across data sources.
**Functions:** `async get_source_audit(_token)`, `async trigger_source_audit(_token)`, `async get_redundancy_map(_token)`, `async compare_feature_sources(feature_name, _token)`, `async get_discrepancies(threshold, _token)`
**Reads:** `__future__`, `fastapi`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.source_audit`

#### `api/routers/strategy.py` — 109 LOC
**Docstring:** Strategy overlay endpoints — regime-independent strategy assignments.
**Classes:** `StrategyResponse`; `StrategyAssignRequest`
**Functions:** `async get_active_strategies(_token)`, `async get_strategy_for_regime(regime_state, _token)`, `async assign_strategy(body, _token)`
**Reads:** `__future__`, `exc`, `fastapi`, `loguru`, `pydantic`, `strategy`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`

#### `api/routers/blob.py` — 109 LOC
**Docstring:** Blob store API — upload, download, and manage stored files.
**Functions:** `async blob_health(_)`, `async upload_blob(bucket, key, file, _)`, `async get_presigned_url(bucket, key, expires, _)`, `async list_blobs(bucket, prefix, _)`, `async download_blob(bucket, key, _)`, `async delete_blob(bucket, key, _)`
**Reads:** `__future__`, `fastapi`, `loguru`, `store`
**Imports from GRID:** `api.auth`, `store.blob`

#### `api/routers/supply_chain.py` — 100 LOC
**Docstring:** Supply chain graph endpoint for actor profile drawer.
**Functions:** `async get_supply_chain(actor_id, direction, depth, _token)`
**Reads:** `__future__`, `fastapi`, `loguru`, `typing`, `utils`
**Imports from GRID:** `api.auth`, `api.dependencies`, `api.routers.supply_chain_helpers`, `utils.ttl_cache`

#### `api/routers/trade_tickets.py` — 90 LOC
**Docstring:** Trade tickets derived from contagion predictions.
**Functions:** `async recent_tickets(since_hours, write_journal, _token)`, `async tickets_for_prediction(prediction_id, write_journal, _token)`
**Reads:** `__future__`, `contagion`, `fastapi`, `historical`, `loguru`, `trading`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `trading.contagion_to_ticket`

#### `api/routers/knowledge.py` — 88 LOC
**Docstring:** GRID API — Knowledge tree endpoints.
**Functions:** `async search_knowledge_endpoint(q, category, limit, offset)`, `async knowledge_summary()`, `async get_knowledge_entry(entry_id)`, `async delete_knowledge_entry(entry_id)`
**Reads:** `__future__`, `fastapi`, `knowledge`, `llm`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `knowledge.tree`

#### `api/dependencies.py` — 80 LOC
**Docstring:** Shared FastAPI dependencies.
**Functions:** `get_db_engine()`, `get_pit_store()`, `get_journal()`, `get_model_registry()`, `get_astrogrid_store()`, `clear_singletons()`
**Reads:** `__future__`, `governance`, `journal`, `sqlalchemy`, `store`
**Imports from GRID:** `db`, `governance.registry`, `journal.log`, `store.astrogrid`, `store.pit`
**Imported by:** `api/main.py`, `api/routers/actor_detail.py`, `api/routers/actor_news_api.py`, `api/routers/associations.py`, `api/routers/astrogrid.py`, `api/routers/astrogrid_celestial.py`, `api/routers/astrogrid_core.py`, `api/routers/astrogrid_helpers.py`, `api/routers/astrogrid_predictions.py`, `api/routers/attributions.py` (+69)

#### `api/routers/contracts.py` — 77 LOC
**Docstring:** FastAPI router for contracts infrastructure endpoints.
**Functions:** `contracts_metrics(user)`, `contracts_lineage(correlation_id, user)`, `contracts_dead_letter_replay(entry_id, user)`
**Reads:** `__future__`, `contracts`, `contracts_audit`, `fastapi`, `sqlalchemy`, `uuid`
**Imports from GRID:** `api.auth`, `api.dependencies`, `contracts`, `contracts.replay`

#### `api/schemas/journal.py` — 76 LOC
**Docstring:** Journal schemas.
**Classes:** `JournalEntryCreate` [validate_confidence]; `JournalOutcomeRecord` [validate_verdict]; `JournalEntryResponse`; `JournalStatsResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/journal.py`

#### `api/routers/sector_health.py` — 60 LOC
**Docstring:** Sector health endpoint.
**Functions:** `async get_sector_health(sector_name, _token)`
**Reads:** `__future__`, `analysis`, `exc`, `fastapi`, `intelligence`, `loguru`, `typing`, `utils`
**Imports from GRID:** `analysis.sector_map`, `api.auth`, `api.dependencies`, `intelligence.sector_health`, `utils.ttl_cache`

#### `api/routers/attributions.py` — 53 LOC
**Docstring:** Cross-lens supply-shock attribution endpoint.
**Functions:** `async get_actor_attributions(actor_id, lookback_days, _token)`
**Reads:** `__future__`, `fastapi`, `intelligence`, `loguru`, `typing`
**Imports from GRID:** `api.auth`, `api.dependencies`, `intelligence.cross_lens`

#### `api/schemas/regime.py` — 46 LOC
**Docstring:** Regime schemas.
**Classes:** `RegimeDriver`; `RegimeCurrentResponse`; `RegimeHistoryEntry`; `RegimeHistoryResponse`; `RegimeTransition`; `RegimeTransitionsResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/regime.py`

#### `api/schemas/auth.py` — 45 LOC
**Docstring:** Authentication schemas.
**Classes:** `LoginRequest`; `LoginResponse`; `TokenVerifyResponse`; `RegisterRequest`; `CreateUserRequest`; `UserResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/auth.py`

#### `api/schemas/models.py` — 44 LOC
**Docstring:** Model registry schemas.
**Classes:** `ModelFromHypothesisRequest`; `ModelTransitionRequest`; `ModelRollbackRequest`; `ModelResponse`; `ProductionModelsResponse`
**Reads:** `__future__`, `pydantic`, `typing`
**Imported by:** `api/routers/models.py`

#### `api/routers/intelligence.py` — 42 LOC
**Docstring:** Cross-reference intelligence endpoints — lie detector for government statistics.
**Reads:** `__future__`, `fastapi`
**Imports from GRID:** `api.routers.intelligence_actors`, `api.routers.intelligence_causation`, `api.routers.intelligence_companies`, `api.routers.intelligence_deepdive`, `api.routers.intelligence_forensics`, `api.routers.intelligence_govflow`, `api.routers.intelligence_news`, `api.routers.intelligence_risk`, `api.routers.intelligence_thesis`

#### `api/routers/watchlist.py` — 40 LOC
**Docstring:** Watchlist API — facade router.
**Reads:** `__future__`, `fastapi`
**Imports from GRID:** `api.routers.watchlist_analysis`, `api.routers.watchlist_core`, `api.routers.watchlist_helpers`, `api.routers.watchlist_overview`
**Imported by:** `api/routers/astrogrid_core.py`, `api/routers/astrogrid_helpers.py`

#### `api/schemas/watchlist.py` — 40 LOC
**Docstring:** Watchlist schemas.
**Classes:** `WatchlistItemCreate` [validate_ticker, validate_asset_type]; `WatchlistItemResponse`
**Reads:** `__future__`, `pydantic`
**Imported by:** `api/routers/watchlist_core.py`

#### `api/routers/astrogrid.py` — 37 LOC
**Docstring:** AstroGrid API — expanded celestial intelligence endpoints.
**Reads:** `__future__`, `fastapi`, `helpers`
**Imports from GRID:** `api.dependencies`, `api.routers.astrogrid_celestial`, `api.routers.astrogrid_core`, `api.routers.astrogrid_helpers`, `api.routers.astrogrid_predictions`

#### `api/__init__.py` — 2 LOC
**Docstring:** GRID Intelligence API — FastAPI backend.

#### `api/routers/__init__.py` — 2 LOC
**Docstring:** GRID API routers.
**Imported by:** `api/routers/actor_detail.py`

#### `api/schemas/__init__.py` — 2 LOC
**Docstring:** GRID API Pydantic schemas.


### `analysis/` (31 modules, 30,607 LOC)

#### `analysis/sector_map.py` — 140 LOC (shim loader, post-Wave 5)
**Docstring:** GRID Sector Map — shim loader for `analysis/sector_map_data.yaml`. Dataset (20 sectors / 262 subsectors / 3,533 actors / 23 junction points) lives in YAML; this module loads it at import time and re-exposes `SECTOR_MAP` and `JUNCTION_POINTS` as module-level constants. Historical `from analysis.sector_map import SECTOR_MAP` contract preserved. Data file: `analysis/sector_map_data.yaml` (25,173 LOC, 734 KB; byte-identical to pre-extract dict literal; cold load ~425 ms on Python 3.10 + libyaml). Wave 5 extract complete 2026-04-13.
**Functions:** `get_sector_features(sector)`, `get_actor_influence(sector)`, `get_all_sectors()`, `get_junction_points_for_sector(sector)`, `get_junction_point(junction_id)`
**Reads:** `aig`, `chipotle`, `danaher`, `just`, `legacy`, `nash`, `sectors`, `shortage`, `south`, `trade`, `ups`, `utx`, `ycc`
**Imported by:** `analysis/flow_aggregator.py`, `analysis/money_flow.py`, `analysis/research_agent.py`, `analysis/taxonomy_audit.py`, `api/routers/actor_detail.py`, `api/routers/capital_flow.py`, `api/routers/contagion.py`, `api/routers/flows.py`, `api/routers/intelligence_actors.py`, `api/routers/sector_health.py` (+15)

#### `analysis/thesis_scorer.py` — 2747 LOC
**Docstring:** GRID — Granular Thesis Scoring Engine.
**Functions:** `score_thesis(engine, force_refresh)`, `snapshot_thesis(engine, thesis)`
**Reads:** `__future__`, `analysis`, `atlanta`, `cftc`, `coingecko`, `country`, `data`, `datetime`, `decision_journal`, `direction`, `dollar_flows`, `fed`, `inference`, `intelligence`, `loguru`, `major`, `max`, `meeting`, `most`, `news`, `news_articles`, `options_daily_signals`, `past`, `raw_series`, `recent`
**Writes:** `thesis_snapshots`
**Imports from GRID:** `analysis.flow_aggregator`, `inference.timesfm_service`, `intelligence.trust_scorer`
**Imported by:** `api/routers/chat.py`, `api/routers/intelligence_thesis.py`, `intelligence/agent_arena.py`

#### `analysis/money_flow.py` — 1772 LOC
**Docstring:** GRID — Global Money Flow Map.
**Functions:** `build_flow_map(engine, as_of)`, `get_sector_drill(engine, sector)`, `get_company_drill(engine, ticker)`
**Reads:** `__future__`, `actor_network`, `analysis`, `balance`, `datetime`, `feature_registry`, `flow_aggregator`, `fred`, `intelligence`, `live`, `loguru`, `millions`, `ollama`, `options`, `options_daily_signals`, `price`, `raw_series`, `recent`, `relative`, `resolved_series`, `safety`, `signal_sources`, `sqlalchemy`, `typing`
**Imports from GRID:** `analysis.flow_aggregator`, `analysis.sector_map`, `intelligence.actor_network`, `intelligence.company_analyzer`, `intelligence.trust_scorer`, `ollama.client`
**Imported by:** `api/routers/flows.py`

#### `analysis/flow_thesis_data.py` — 1415 LOC
**Docstring:** GRID — Flow Thesis Knowledge Base (data module).
**Reads:** `__future__`, `bank`, `cross_reference_reports`, `datetime`, `feature`, `feature_registry`, `intelligence`, `loguru`, `margin_debt_monthly`, `markets`, `max`, `options_daily_signals`, `physical`, `raw_series`, `resolved_series`, `risk`, `signal_sources`, `sqlalchemy`, `typing`, `usd`
**Imports from GRID:** `intelligence.trust_scorer`
**Imported by:** `analysis/flow_thesis.py`, `analysis/flow_thesis_scoring.py`

#### `analysis/vol_surface.py` — 1273 LOC
**Docstring:** GRID Vol Surface Engine.
**Classes:** `VolSurfaceEngine` [__init__, build_surface, fit_svi_slice, compute_skew, compute_term_structure, detect_arbitrage, compute_greeks_grid, historical_percentile]
**Reads:** `__future__`, `build_surface`, `datetime`, `feature_registry`, `fred`, `loguru`, `nearest`, `options`, `options_daily_signals`, `options_snapshots`, `physics`, `resolved_series`, `scipy`, `sqlalchemy`, `svi`, `typing`
**Imports from GRID:** `db`, `physics.dealer_gamma`

#### `analysis/market_universe.py` — 1185 LOC
**Docstring:** Comprehensive S&P 500 Market Universe — every GICS sector, industry, and major company.
**Functions:** `get_universe()`, `get_sector(name)`, `get_industry(name)`, `get_peers(ticker)`, `search_company(query)`, `get_all_tickers()`
**Reads:** `__future__`, `analysis`, `results`
**Imported by:** `intelligence/deep_graph.py`

#### `analysis/flow_aggregator.py` — 1148 LOC
**Docstring:** GRID — Flow Aggregation Engine.
**Functions:** `aggregate_by_sector(engine, days)`, `aggregate_by_time(engine, ticker_or_sector, period, days)`, `aggregate_by_actor_tier(engine, days)`, `compute_flow_momentum(engine, ticker, days)`, `build_sector_flow_matrix(engine, days)`, `aggregate_smart_vs_dumb(engine, days)`, `compute_sector_conviction(engine, days)`, `compute_flow_velocity(engine, days)`, `aggregate_confidence_weighted(engine, days)`, `get_full_aggregation(engine, sector, period, days)`
**Reads:** `__future__`, `actor_network`, `analysis`, `collections`, `datetime`, `dollar_flows`, `each`, `intelligence`, `loguru`, `one`, `simultaneous`, `sqlalchemy`, `typing`
**Imports from GRID:** `analysis.sector_map`
**Imported by:** `analysis/money_flow.py`, `analysis/thesis_scorer.py`, `api/routers/flows.py`, `intelligence/agent_arena.py`, `intelligence/image_gen.py`

#### `analysis/ephemeris.py` — 1036 LOC
**Docstring:** GRID Ephemeris Engine -- Copernicus Module.
**Classes:** `Ephemeris` [julian_date, centuries_since_j2000, compute_position, compute_all_positions, compute_aspects, compute_lunar_phase, compute_nakshatra, compute_planetary_hours]
**Functions:** `get_ephemeris(dt)`
**Reads:** `__future__`, `birth_date`, `datetime`, `jpl`, `meeus`, `phase`, `sun`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`

#### `analysis/capital_flows.py` — 893 LOC
**Docstring:** GRID Capital Flow Research Engine.
**Classes:** `CapitalFlowResearchEngine` [__init__, run_research]
**Reads:** `__future__`, `all`, `as_of`, `datetime`, `every`, `feature_registry`, `latest`, `loguru`, `ollama`, `options_daily_signals`, `outputs`, `pathlib`, `raw_series`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `capital_flow_snapshots`
**Imports from GRID:** `ollama.client`, `outputs.llm_logger`
**Imported by:** `api/routers/ollama.py`, `intelligence/scheduler.py`

#### `analysis/viz_intelligence.py` — 564 LOC
**Docstring:** GRID Visualization Intelligence Engine.
**Classes:** `ChartType`; `DataShape`; `RelationType`; `WeightSchedule`; `AnimationConfig`; `VizSpec` [to_dict]
**Functions:** `select_visualization(data_description, features, relation, question)`, `get_all_rules()`, `compute_source_weights(families, as_of, engine)`
**Reads:** `__future__`, `center`, `dataclasses`, `datetime`, `discovery`, `enum`, `equilibrium`, `peak`, `source_catalog`, `spot`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`
**Imported by:** `api/routers/viz.py`

#### `analysis/astro_correlations.py` — 543 LOC
**Docstring:** Celestial-Market Correlation Engine.
**Classes:** `AstroCorrelationEngine` [__init__, compute_correlations, compute_event_impact, get_cached_or_compute]
**Reads:** `__future__`, `astro_correlations`, `datetime`, `feature_registry`, `ingestion`, `loguru`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `astro_correlations`
**Imports from GRID:** `db`, `ingestion.celestial.lunar`, `ingestion.celestial.planetary`
**Imported by:** `api/routers/astrogrid_celestial.py`, `intelligence/scheduler.py`

#### `analysis/research_agent.py` — 518 LOC
**Docstring:** GRID Research Agent — autonomous intelligence-gathering system.
**Functions:** `analyze_gaps(engine)`, `generate_hypotheses(engine)`, `research_actor(actor, sector, engine)`, `research_sector(sector_name, engine)`, `run_full_research(engine)`, `fill_missing_stocks(engine)`
**Reads:** `__future__`, `actor`, `analysis`, `datetime`, `existing`, `feature_registry`, `llm`, `loguru`, `resolved_series`, `source_catalog`, `sqlalchemy`, `store`, `typing`
**Writes:** `feature_registry`, `hypothesis_registry`, `resolved_series`
**Imports from GRID:** `analysis.sector_map`, `db`, `store.snapshots`
**Imported by:** `api/routers/flows.py`, `intelligence/scheduler.py`

#### `analysis/backtest_scanner.py` — 505 LOC
**Docstring:** Automated cross-asset backtest scanner.
**Functions:** `scan_all_pairs(engine, families, min_sharpe, min_win_rate, min_trades, lookback_days)`, `generate_hypotheses_from_winners(engine, winners, max_hypotheses)`, `run_full_scan(engine)`, `review_existing_hypotheses(engine)`
**Reads:** `__future__`, `backtest`, `datetime`, `feature_registry`, `hypothesis_registry`, `llm`, `loguru`, `ollama`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `hypothesis_registry`
**Imports from GRID:** `ollama.client`
**Imported by:** `api/routers/discovery.py`, `api/routers/trading.py`

#### `analysis/hypothesis_tester.py` — 495 LOC
**Docstring:** Hypothesis backtesting orchestrator.
**Functions:** `compute_lagged_correlation(leader, follower, max_lag)`, `test_hypothesis(engine, hypothesis_id, leader_features, follower_features, expected_lag, days, max_lag)`, `run_all_tests(engine)`
**Reads:** `__future__`, `autoresearch`, `datetime`, `feature_registry`, `hypothesis_registry`, `lag_structure`, `loguru`, `research_agent`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`
**Imported by:** `api/routers/flows.py`

#### `analysis/money_flow_engine/layer_market.py` — 417 LOC
**Docstring:** GRID — Market Layer (Layer 4).
**Functions:** `build_market_layer(engine, as_of)`
**Reads:** `__future__`, `datetime`, `dollar_flows`, `etf`, `etf_flows`, `loguru`, `options`, `options_daily_signals`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`

#### `analysis/money_flow_engine/layer_credit.py` — 409 LOC
**Docstring:** GRID -- Credit Layer (Layer 2).
**Functions:** `build_credit_layer(engine, as_of)`
**Reads:** `__future__`, `bank`, `datetime`, `intelligence`, `loguru`, `resolved_series`, `sofr`, `sqlalchemy`
**Imports from GRID:** `intelligence.cds_tracker`

#### `analysis/money_flow_engine/helpers.py` — 381 LOC
**Docstring:** GRID -- Money Flow Engine Helpers.
**Functions:** `compute_z_score(values, current)`, `dominant_confidence(nodes)`, `series_to_usd(series_id, raw_value)`, `compute_changes(engine, series_id, as_of)`
**Reads:** `__future__`, `analysis`, `collections`, `datetime`, `feature_registry`, `raw_series`, `resolved_series`, `sqlalchemy`, `typing`

#### `analysis/flow_thesis_scoring.py` — 334 LOC
**Docstring:** GRID — Flow Thesis Scoring and Narrative Generation.
**Functions:** `update_current_states(engine)`, `generate_unified_thesis(engine)`
**Reads:** `__future__`, `analysis`, `datetime`, `loguru`, `model_outcomes`, `scored`, `sqlalchemy`, `thesis`, `thesis_snapshots`, `track`, `typing`
**Imports from GRID:** `analysis.flow_thesis_data`
**Imported by:** `analysis/flow_thesis.py`

#### `analysis/money_flow_engine/layer_monetary.py` — 307 LOC
**Docstring:** GRID -- Monetary Layer (Layer 1).
**Functions:** `build_monetary_layer(engine, as_of)`
**Reads:** `__future__`, `datetime`, `loguru`, `policy`, `rrp`, `sqlalchemy`, `system`

#### `analysis/money_flow_engine/layer_retail.py` — 306 LOC
**Docstring:** GRID -- Money Flow Engine: Retail Layer (Layer 7, order=6).
**Functions:** `build_retail_layer(engine, as_of)`
**Reads:** `__future__`, `aaii`, `datetime`, `dollar_flows`, `fred`, `loguru`, `margin_debt_monthly`, `signal_sources`, `sqlalchemy`

#### `analysis/money_flow_engine/layer_corporate.py` — 300 LOC
**Docstring:** GRID -- Money Flow Engine: Corporate Layer (Layer 5, order=4).
**Functions:** `build_corporate_layer(engine, as_of)`
**Reads:** `__future__`, `datetime`, `loguru`, `sec`, `signal_sources`, `sqlalchemy`

#### `analysis/money_flow_engine/layer_institutional.py` — 296 LOC
**Docstring:** GRID — Institutional Layer (Layer 3).
**Functions:** `build_institutional_layer(engine, as_of)`
**Reads:** `__future__`, `datetime`, `dollar_flows`, `etf`, `loguru`, `raw_series`, `signal_sources`, `sqlalchemy`, `typing`

#### `analysis/taxonomy_audit.py` — 287 LOC
**Docstring:** GRID Taxonomy Audit Engine.
**Functions:** `run_taxonomy_audit(engine)`
**Reads:** `__future__`, `analysis`, `datetime`, `feature_registry`, `lateral`, `loguru`, `model_eligible`, `resolved_series`, `sector`, `sqlalchemy`, `stats`, `typing`
**Writes:** `feature_registry`
**Imports from GRID:** `analysis.sector_map`
**Imported by:** `api/routers/system.py`, `intelligence/scheduler.py`

#### `analysis/money_flow_engine/layer_crypto.py` — 286 LOC
**Docstring:** GRID -- Money Flow Engine: Crypto Layer (Layer 8, order=7).
**Functions:** `build_crypto_layer(engine, as_of)`
**Reads:** `__future__`, `datetime`, `etf_flows`, `loguru`, `raw_series`, `sqlalchemy`

#### `analysis/money_flow_engine/flow_inference.py` — 246 LOC
**Docstring:** GRID — Multi-Directional Flow Inference Engine.
**Functions:** `infer_flow_edges(layers)`
**Reads:** `__future__`, `both`, `generating`, `profits`

#### `analysis/money_flow_engine/layer_sovereign.py` — 236 LOC
**Docstring:** GRID -- Money Flow Engine: Sovereign Layer (Layer 6, order=5).
**Functions:** `build_sovereign_layer(engine, as_of)`
**Reads:** `__future__`, `datetime`, `fred`, `loguru`, `sqlalchemy`, `wiki_tariff`

#### `analysis/prompt_optimizer.py` — 138 LOC
**Docstring:** Prompt feature selection via orthogonality analysis.
**Functions:** `select_prompt_features(features, max_count, corr_threshold, history)`, `format_features_for_prompt(features, include_value, include_family)`
**Reads:** `__future__`, `loguru`, `remaining`, `select_prompt_features`, `typing`
**Imported by:** `ollama/market_briefing.py`

#### `analysis/money_flow_engine/__init__.py` — 115 LOC
**Docstring:** GRID — Money Flow Engine.
**Functions:** `build_flow_map(engine, as_of)`
**Reads:** `__future__`, `datetime`, `loguru`, `monetary`, `sqlalchemy`, `structural`
**Imported by:** `api/routers/chat.py`, `api/routers/flows.py`, `intelligence/audio_briefing.py`, `intelligence/deep_dive.py`, `intelligence/image_gen.py`

#### `analysis/money_flow_engine/types.py` — 103 LOC
**Docstring:** GRID -- Money Flow Engine Types.
**Classes:** `FlowNode` [to_dict]; `FlowLayer` [to_dict]; `FlowEdge` [to_dict]; `FlowMap` [to_dict]
**Reads:** `__future__`, `dataclasses`, `typing`

#### `analysis/flow_thesis.py` — 22 LOC
**Docstring:** GRID — Flow Thesis Knowledge Base.
**Reads:** `analysis`
**Imports from GRID:** `analysis.flow_thesis_data`, `analysis.flow_thesis_scoring`
**Imported by:** `api/routers/intelligence_thesis.py`, `intelligence/adapters/flow_thesis_adapter.py`, `intelligence/audio_briefing.py`, `intelligence/image_gen.py`, `intelligence/market_diary.py`, `intelligence/thesis_tracker.py`

#### `analysis/__init__.py` — 2 LOC


### `trading/` (13 modules, 7,175 LOC)

#### `trading/options_recommender.py` — 1380 LOC
**Docstring:** GRID — Options trade recommendation engine.
**Classes:** `OptionsRecommendation` [to_dict, to_trade_ticket, risk_reward_ratio, all_sanity_passed]; `OptionsRecommender` [__init__, generate_recommendations, format_report]
**Reads:** `__future__`, `bid`, `black`, `config`, `dataclasses`, `datetime`, `dealergammaengine`, `decision`, `decision_journal`, `discovery`, `feature_registry`, `gamma`, `gex`, `latest`, `loguru`, `options_daily_signals`, `options_mispricing_scans`, `options_recommendations`, `options_scanner`, `options_snapshots`, `physics`, `reading`, `resolved_series`, `scanner`, `scipy`
**Writes:** `options_recommendations`
**Imports from GRID:** `api.main`, `config`, `db`, `discovery.options_scanner`, `physics.dealer_gamma`
**Imported by:** `api/routers/options.py`, `api/routers/trading.py`, `intelligence/scheduler.py`

#### `trading/strategy151.py` — 981 LOC
**Docstring:** GRID — Key strategies from Kakushadze & Serur (2018) '151 Trading Strategies'.
**Classes:** `StrategySignal`; `Strategy151Engine` [__init__, mean_reversion_scan, pairs_trading_scan, cross_sectional_momentum, volatility_risk_premium, ou_mean_reversion, sector_rotation, generate_composite_score]
**Reads:** `__future__`, `dataclasses`, `datetime`, `equilibrium`, `kakushadze`, `log`, `loguru`, `market_daily`, `moving`, `options_daily_signals`, `sqlalchemy`, `statsmodels`, `store`, `typing`
**Imports from GRID:** `store.pit`, `store.snapshots`

#### `trading/options_tracker.py` — 765 LOC
**Docstring:** GRID — Options recommendation outcome tracker and self-improvement loop.
**Functions:** `score_expired_recommendations(engine)`, `compute_signal_scores(engine)`, `generate_improvement_report(engine)`, `update_scanner_weights(engine, signal_scores)`, `run_improvement_cycle(engine)`
**Reads:** `__future__`, `datetime`, `discovery`, `llm`, `loguru`, `options_daily_signals`, `options_recommendations`, `raw_series`, `scanner_weights`, `sqlalchemy`, `typing`, `yfinance`
**Writes:** `options_recommendations`, `scanner_weights`
**Imported by:** `api/routers/trading.py`, `intelligence/scheduler.py`

#### `trading/contagion_to_ticket.py` — 733 LOC
**Docstring:** GRID — Contagion → Dealer Gamma → Options Trade Ticket bridge.
**Classes:** `ContagionRow`
**Functions:** `compute_kelly_fraction(accuracy, payout_ratio, cap)`, `pick_strike(spot, direction, gamma_context, max_pain)`, `pick_expiry(simulated_at, margin_impact_pct, min_dte, scale)`, `estimate_premium(spot, iv_atm, dte)`, `write_ticket_to_journal(engine, ticket, model_version_id)`, `generate_tickets_for_prediction(engine, prediction_id, journal)`, `generate_tickets_for_recent_predictions(engine, since_hours, journal)`
**Reads:** `__future__`, `contagion_backtest_results`, `contagion_predictions`, `dataclasses`, `datetime`, `historical`, `journal`, `loguru`, `model_registry`, `options_daily_signals`, `physics`, `sqlalchemy`, `typing`
**Imports from GRID:** `journal.log`, `physics.dealer_gamma`
**Imported by:** `api/routers/trade_tickets.py`

#### `trading/prediction_markets.py` — 620 LOC
**Docstring:** GRID Prediction Market Integration — Polymarket + Kalshi.
**Classes:** `PolymarketTrader` [__init__, get_markets, get_market, get_position, get_portfolio, buy, sell]; `KalshiTrader` [__init__, get_events, get_event, get_position, get_portfolio, get_balance, buy, sell]
**Reads:** `__future__`, `config`, `kalshi`, `loguru`, `polymarket`, `py_clob_client`, `typing`
**Imports from GRID:** `config`
**Imported by:** `api/routers/trading.py`

#### `trading/prediction_backtest.py` — 603 LOC
**Docstring:** GRID Prediction Market Backtesting Bridge.
**Classes:** `HypothesisResult` [summary]; `BaseStrategy` [__init__, on_trade, on_market_close]; `MomentumReversalStrategy` [on_trade]; `MakerFlowStrategy` [on_trade]; `ValueDivergenceStrategy` [on_trade]; `LiquiditySpikeStrategy` [on_trade]
**Functions:** `export_kalshi_trades(engine, output_dir, market_filter)`, `export_polymarket_trades(engine, output_dir, market_filter)`, `export_markets(engine, platform, output_dir)`, `register_strategy(name)`, `run_hypothesis(engine, name, market_filter, strategy, params, start_date, end_date, position_size, description)`, `list_strategies()`, `list_available_markets(engine, platform, search, limit)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `fundamental`, `grid`, `loguru`, `pathlib`, `prediction_market_markets`, `prediction_market_trades`, `sqlalchemy`, `trading`, `typing`
**Imported by:** `api/routers/prediction_backtest.py`

#### `trading/hyperliquid.py` — 414 LOC
**Docstring:** Hyperliquid perp trading integration.
**Classes:** `HyperliquidTrader` [__init__, get_balance, get_positions, get_trade_history, open_position, close_position, check_risk_limits]
**Functions:** `get_hyperliquid_trader()`
**Reads:** `__future__`, `config`, `datetime`, `grid`, `hyperliquid`, `loguru`, `private`, `typing`
**Imports from GRID:** `config`
**Imported by:** `api/routers/trading.py`

#### `trading/prediction_pmxt.py` — 405 LOC
**Docstring:** GRID Unified Prediction Market Trader via pmxt SDK.
**Classes:** `PmxtTrader` [__init__, get_markets, get_market, get_portfolio, buy, sell]
**Reads:** `__future__`, `config`, `grid`, `loguru`, `typing`
**Imports from GRID:** `config`

#### `trading/paper_engine.py` — 351 LOC
**Docstring:** GRID Paper Trading Engine.
**Classes:** `PaperTradingEngine` [__init__, register_strategy, open_trade, close_trade, get_dashboard, register_all_passed, kelly_position_size]
**Reads:** `__future__`, `datetime`, `hypothesis_registry`, `loguru`, `paper_strategies`, `paper_trades`, `passed`, `sqlalchemy`, `tactical`, `typing`
**Writes:** `paper_strategies`, `paper_trades`
**Imported by:** `api/routers/trading.py`, `trading/signal_executor.py`

#### `trading/wallet_manager.py` — 349 LOC
**Docstring:** GRID Multi-Wallet Manager (EXCH-04).
**Classes:** `WalletManager` [__init__, create_wallet, get_wallet, get_all_wallets, update_pnl, check_risk, kill_wallet, pause_wallet]
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `trading_wallets`, `typing`
**Writes:** `trading_wallets`
**Imported by:** `api/routers/trading.py`

#### `trading/signal_executor.py` — 297 LOC
**Docstring:** Paper Trading Signal Executor.
**Functions:** `execute_signals(engine)`
**Reads:** `__future__`, `close_trade`, `closed`, `datetime`, `feature_registry`, `hypothesis_registry`, `intelligence`, `loguru`, `paper_strategies`, `paper_trades`, `resolved_series`, `sqlalchemy`, `trading`, `typing`
**Imports from GRID:** `intelligence.trust_scorer`, `trading.circuit_breaker`, `trading.paper_engine`
**Imported by:** `api/routers/trading.py`, `intelligence/scheduler.py`

#### `trading/circuit_breaker.py` — 276 LOC
**Docstring:** Strategy-level circuit breaker for the signal executor.
**Classes:** `BreakerState`; `StrategyCircuitBreaker` [__init__, get_state, should_execute, record_success, record_failure, reset, get_all_states]
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `enum`, `loguru`, `open`, `paper_strategy_breaker_state`, `sqlalchemy`, `typing`
**Writes:** `paper_strategy_breaker_state`
**Imports from GRID:** `alerts.email`, `config`
**Imported by:** `trading/signal_executor.py`

#### `trading/__init__.py` — 1 LOC


### `oracle/` (24 modules, 6,691 LOC)

#### `oracle/engine.py` — 1361 LOC
**Docstring:** GRID Oracle Engine — Self-improving prediction loop.
**Classes:** `PredictionType`; `Verdict`; `Signal`; `AntiSignal`; `OraclePrediction` [to_dict]; `OracleModel` [hit_rate, total_scored]; `OracleEngine` [__init__, generate_predictions, score_expired_predictions, evolve_weights, run_cycle]
**Reads:** `__future__`, `capital_flow_snapshots`, `dataclasses`, `datetime`, `decision_journal`, `enum`, `feature_registry`, `intelligence`, `internal`, `loguru`, `options_daily_signals`, `oracle`, `oracle_models`, `oracle_predictions`, `postmortem`, `raw_series`, `recent`, `relative_strength`, `resolved_series`, `scoring`, `signal_registry`, `sqlalchemy`, `timeseries_forecasts`, `typing`
**Writes:** `oracle_iterations`, `oracle_models`, `oracle_predictions`
**Imports from GRID:** `intelligence.actor_signal_bridge`, `intelligence.trust_scorer`, `oracle.calibration`, `oracle.forecaster_adapter`, `oracle.hallucination_guard`, `oracle.model_evolver`, `oracle.model_factory`, `oracle.trace_evolver`
**Imported by:** `api/routers/oracle.py`, `oracle/forecaster_adapter.py`, `oracle/run_cycle.py`

#### `oracle/trace_evolver.py` — 798 LOC
**Docstring:** GRID Oracle — Trace-Based Self-Evolution Engine.
**Classes:** `FailurePattern` [to_dict]; `MutationProposal` [to_dict]; `EvolutionCycleResult` [to_dict]; `TraceAnalyzer` [__init__, analyze, get_trace_summary]; `TargetedMutator` [__init__, propose, apply]; `EvolutionGate` [__init__, check]; `TraceEvolver` [__init__, evolve_cycle]
**Reads:** `__future__`, `collections`, `dataclasses`, `datetime`, `decision_journal`, `failure`, `loguru`, `model_evolver`, `oracle_models`, `oracle_predictions`, `postmortem`, `scoring`, `signal_registry`, `signal_sources`, `sqlalchemy`, `trade_postmortems`, `trust_scorer`, `typing`
**Writes:** `oracle_iterations`, `oracle_models`
**Imported by:** `oracle/engine.py`

#### `oracle/hallucination_guard.py` — 641 LOC
**Docstring:** GRID Oracle Hallucination Guard — deterministic pre-storage verification layer.
**Classes:** `GuardCheck`; `GuardVerdict`
**Functions:** `verify_predictions(predictions, calibration_report, model_stats)`, `guard_summary(verdicts)`
**Reads:** `__future__`, `config`, `dataclasses`, `functools`, `loguru`, `models`, `typing`, `verification`, `verify_predictions`, `what`
**Imports from GRID:** `config`
**Imported by:** `api/routers/oracle.py`, `oracle/engine.py`

#### `oracle/forecaster_adapter.py` — 421 LOC
**Docstring:** Oracle ↔ TimesFM Adapter.
**Functions:** `forecast_to_signals(forecast_result, current_price)`, `forecast_to_anti_signals(forecast_result, current_signals)`, `forecast_to_prediction(forecast_result, ticker, current_price, signals, anti_signals)`, `run_timesfm_forecast_cycle(engine, tickers, horizon)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `existing`, `forecast`, `horizon`, `interval`, `loguru`, `options_daily_signals`, `oracle`, `other`, `raw_series`, `sqlalchemy`, `timeseries`, `timesfm`, `timesfmforecaster`, `typing`
**Writes:** `timeseries_forecasts`
**Imports from GRID:** `oracle.engine`, `timeseries.timesfm_forecaster`
**Imported by:** `oracle/engine.py`

#### `oracle/sanity_checker.py` — 292 LOC
**Docstring:** GRID — Deterministic sanity checks on verified claims.
**Classes:** `SanityResult`; `CheckedClaim`
**Functions:** `run_sanity_checks(claims)`
**Reads:** `__future__`, `claim`, `dataclasses`, `datetime`, `ingestion`, `loguru`, `oracle`, `typing`
**Imports from GRID:** `ingestion.sanity_ranges`, `oracle.claim_verifier`
**Imported by:** `oracle/firewall.py`, `oracle/publisher_gate.py`

#### `oracle/model_factory.py` — 288 LOC
**Docstring:** GRID Oracle — Model Factory.
**Classes:** `ModelSpec` [to_jsonb_dict]; `ModelFactory` [__init__, create_model, spawn_variant, get_model_spec, get_signals_for_model, list_active_models, retire_model]
**Functions:** `migrate_default_models(engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `oracle`, `oracle_models`, `signal_registry`, `sqlalchemy`, `typing`
**Writes:** `oracle_models`
**Imports from GRID:** `oracle.signal_aggregator`
**Imported by:** `api/routers/signal_registry.py`, `oracle/engine.py`, `oracle/ensemble.py`

#### `oracle/model_evolver.py` — 242 LOC
**Docstring:** GRID Oracle — Model Evolver.
**Classes:** `EvolveResult` [to_dict]; `ModelEvolver` [__init__, evolve_cycle]
**Reads:** `__future__`, `dataclasses`, `discovered_hypotheses`, `hypothesis`, `loguru`, `oracle_models`, `oracle_predictions`, `signal_registry`, `sqlalchemy`, `typing`
**Writes:** `oracle_iterations`, `oracle_models`
**Imported by:** `oracle/engine.py`

#### `oracle/report.py` — 233 LOC
**Docstring:** GRID Oracle Report — prediction digest with scorecard.
**Functions:** `send_oracle_report(cycle_result)`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `last`, `loguru`, `typing`, `verification`
**Imports from GRID:** `alerts.email`, `config`
**Imported by:** `oracle/run_cycle.py`

#### `oracle/scoreboard.py` — 231 LOC
**Docstring:** Shared Oracle scoreboard helpers used by GRID and AstroGrid.
**Functions:** `build_oracle_ticker_rollup(engine, tickers, ticker_aliases, include_calibration, limit)`, `build_oracle_scoreboard(engine, ticker_limit)`
**Reads:** `__future__`, `loguru`, `oracle`, `oracle_models`, `oracle_predictions`, `sqlalchemy`, `typing`
**Imports from GRID:** `oracle.calibration`
**Imported by:** `api/routers/astrogrid_helpers.py`, `api/routers/chat.py`, `api/routers/oracle.py`

#### `oracle/claim_verifier.py` — 217 LOC
**Docstring:** GRID — Claim verification against database evidence.
**Classes:** `VerifiedClaim`
**Functions:** `verify_claims(claims, engine)`
**Reads:** `__future__`, `dataclasses`, `feature_registry`, `loguru`, `oracle`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `oracle.claim_extractor`
**Imported by:** `oracle/firewall.py`, `oracle/sanity_checker.py`

#### `oracle/claim_extractor.py` — 214 LOC
**Docstring:** GRID — Deterministic claim extraction from LLM output text.
**Classes:** `Claim`
**Functions:** `extract_claims(text)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `llm`, `typing`
**Imported by:** `oracle/claim_verifier.py`, `oracle/firewall.py`

#### `oracle/astrogrid_universe.py` — 208 LOC
**Docstring:** Canonical AstroGrid scoring universe definitions.
**Functions:** `get_astrogrid_scoreable_universe()`, `scoreable_universe_by_symbol()`, `enrich_astrogrid_scoreable_universe(conn)`
**Reads:** `__future__`, `copy`, `datetime`, `feature_registry`, `resolved_series`, `sqlalchemy`, `typing`
**Imported by:** `api/routers/astrogrid_helpers.py`, `store/astrogrid.py`

#### `oracle/signal_aggregator.py` — 203 LOC
**Docstring:** GRID Oracle — Signal Aggregator.
**Classes:** `WeightMode`; `WeightConfig`; `AggregatedSignal`; `SignalAggregator` [aggregate]
**Reads:** `__future__`, `dataclasses`, `datetime`, `enum`, `loguru`, `typing`
**Imported by:** `api/routers/signal_registry.py`, `oracle/ensemble.py`, `oracle/model_factory.py`

#### `oracle/calibration.py` — 196 LOC
**Docstring:** GRID Oracle Calibration — measures how well predicted probabilities
**Classes:** `CalibrationBucket`; `CalibrationReport` [to_dict]
**Functions:** `compute_calibration(engine, n_bins, model_name, ticker)`
**Reads:** `__future__`, `dataclasses`, `loguru`, `oracle_predictions`, `scored`, `sqlalchemy`, `typing`
**Imported by:** `oracle/engine.py`, `oracle/scoreboard.py`

#### `oracle/firewall.py` — 163 LOC
**Docstring:** GRID — Publishing firewall: single entry point for claim-level verification.
**Classes:** `FirewallResult`
**Functions:** `verify_output(text, engine)`
**Reads:** `__future__`, `claim`, `dataclasses`, `datetime`, `llm`, `loguru`, `oracle`, `sqlalchemy`
**Writes:** `claim_audit`
**Imports from GRID:** `oracle.claim_extractor`, `oracle.claim_verifier`, `oracle.publisher_gate`, `oracle.sanity_checker`
**Imported by:** `api/routers/chat.py`

#### `oracle/publisher_gate.py` — 146 LOC
**Docstring:** GRID — Publisher gate: decide publish / review / reject.
**Classes:** `PublishDecision`
**Functions:** `gate_decision(claims)`
**Reads:** `__future__`, `dataclasses`, `oracle`, `typing`
**Imports from GRID:** `oracle.sanity_checker`
**Imported by:** `oracle/firewall.py`

#### `oracle/citation_extractor.py` — 127 LOC
**Docstring:** GRID — Extract feature citations from LLM output text.
**Functions:** `extract_citations(llm_output, features_available, feature_families)`, `compute_citation_ratio(cited, available)`
**Reads:** `__future__`, `features_available`, `llm`, `loguru`
**Imported by:** `api/routers/chat.py`

#### `oracle/feedback_recorder.py` — 81 LOC
**Docstring:** GRID — Record prompt feedback (features available vs cited) for utility scoring.
**Functions:** `record_prompt_feedback(db_engine, source, features_available, features_cited, prediction_id, ticker, model_name, llm_model, prompt_token_count, response_length, metadata)`
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `prompt_feedback`
**Imported by:** `api/routers/chat.py`

#### `oracle/run_cycle.py` — 49 LOC
**Docstring:** Run one Oracle cycle: score → evolve → predict → report.
**Functions:** `main()`
**Reads:** `__future__`, `loguru`, `oracle`
**Imports from GRID:** `db`, `oracle.engine`, `oracle.report`

#### `oracle/pruning_config.py` — 42 LOC
**Docstring:** Prompt pruning system configuration — constants, anchors, thresholds.
**Classes:** `PruningThresholds`
**Reads:** `__future__`, `dataclasses`, `pruning`

#### `oracle/__init__.py` — 1 LOC


### `subnet/` (10 modules, 5,408 LOC)

#### `subnet/distributed_compute.py` — 1446 LOC
**Docstring:** GRID Distributed Compute Engine.
**Classes:** `TaskStatus`; `RewardType`; `MinerIdentity`; `TaskAssignment`; `EarningsSnapshot`; `ComputeCoordinator` [__init__, register_miner, pull_task, submit_result, get_miner_stats, get_leaderboard, record_stake, expire_stale_assignments]; `GPUDetector` [detect]; `EdgeMiner` [__init__, start, get_dashboard]; `ComputeScheduler` [__init__, run_forever]
**Functions:** `main()`
**Reads:** `__future__`, `authorization`, `average`, `compute_assignments`, `compute_miners`, `compute_rewards`, `dataclasses`, `datetime`, `enum`, `fastapi`, `llm_task_backlog`, `loguru`, `peers`, `pydantic`, `sqlalchemy`, `subnet`, `typing`
**Writes:** `compute_assignments`, `compute_miners`, `compute_rewards`, `llm_task_backlog`
**Imports from GRID:** `api.auth`, `db`, `subnet.dynamic_scorer`, `subnet.honeypot`, `subnet.miner`, `subnet.reputation`, `subnet.semantic_scorer`, `subnet.stake_verifier`, `subnet.sybil_detector`
**Imported by:** `api/main.py`, `subnet/oauth_miner.py`

#### `subnet/semantic_scorer.py` — 752 LOC
**Docstring:** GRID Subnet Semantic Scorer.
**Classes:** `SemanticScorer` [__init__, embed, cosine_similarity, cross_validate, detect_collusion, score_quality, score_against_ground_truth, extract_claims]
**Reads:** `__future__`, `actors`, `collections`, `encrypted_intelligence`, `extract_claims`, `loguru`, `sentence_transformers`, `sklearn`, `sqlalchemy`, `subnet`, `typing`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/honeypot.py` — 670 LOC
**Docstring:** GRID Subnet Honeypot Calibration System.
**Classes:** `HoneypotInjector` [__init__, ensure_tables, generate_batch, is_honeypot, score_honeypot, get_calibration_divergence, get_current_ratio, needs_injection]
**Reads:** `__future__`, `compute_task_results`, `contexts`, `datetime`, `encrypted_intelligence`, `honeypot_registry`, `intel`, `llm_task_backlog`, `loguru`, `real`, `regular`, `sqlalchemy`, `typing`, `verified`
**Writes:** `honeypot_registry`, `llm_task_backlog`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/oauth_miner.py` — 572 LOC
**Docstring:** GRID Mobile Miner — OAuth-based task processing via ChatGPT/Copilot/Claude.
**Classes:** `OAuthManager` [__init__, get_auth_url, exchange_code, store_token, get_token, store_api_key]; `AIProviderRouter` [__init__, process_task, get_connected_providers]
**Reads:** `__future__`, `compute_miners`, `data`, `datetime`, `fastapi`, `llm_task_backlog`, `loguru`, `miner_oauth_tokens`, `official`, `pydantic`, `sqlalchemy`, `subnet`, `typing`, `urllib`
**Writes:** `miner_oauth_tokens`
**Imports from GRID:** `api.auth`, `api.dependencies`, `subnet.distributed_compute`
**Imported by:** `api/main.py`

#### `subnet/validator.py` — 413 LOC
**Docstring:** GRID Bittensor Subnet Validator.
**Classes:** `TaskDistributor` [__init__, get_batch]; `ResponseScorer` [__init__, score]; `ResultStore` [__init__, store_result, record_miner_score]; `GRIDValidator` [__init__, validation_step, run_forever]
**Functions:** `main()`
**Reads:** `__future__`, `actors`, `datetime`, `intelligence`, `llm`, `llm_task_backlog`, `loguru`, `response`, `score`, `sqlalchemy`, `typing`
**Writes:** `llm_task_backlog`, `subnet_miner_scores`, `subnet_task_log`
**Imports from GRID:** `db`, `intelligence.opsec`

#### `subnet/reputation.py` — 351 LOC
**Docstring:** GRID Subnet Bayesian Reputation System.
**Classes:** `ReputationUpdate`; `BayesianReputation` [__init__, reputation, confidence, tier, is_banned, update_success, update_failure, update_honeypot_fail]; `ReputationManager` [__init__, get_reputation, save_reputation, update_after_task, update_sybil, update_deadline_miss, get_tier, is_banned]
**Reads:** `__future__`, `building`, `compute_miners`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `compute_miners`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/dynamic_scorer.py` — 332 LOC
**Docstring:** GRID Subnet Dynamic Scoring.
**Classes:** `DynamicScorer` [__init__, get_current_epoch, get_epoch_weights, get_active_dimensions, score, score_dimensions]
**Reads:** `__future__`, `hmac`, `loguru`, `typing`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/sybil_detector.py` — 315 LOC
**Docstring:** GRID Subnet Sybil Detection.
**Classes:** `BehavioralProfile` [__init__, add_submission, to_feature_vector, to_dict]; `SybilDetector` [__init__, check_rate_limit, record_submission, detect_clusters, check_collusion, save_profiles]
**Reads:** `__future__`, `collections`, `datetime`, `edge`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `miner_behavioral_profiles`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/stake_verifier.py` — 296 LOC
**Docstring:** GRID Subnet Stake Verification.
**Classes:** `StakeVerifier` [__init__, verify_stake, is_verified, get_deposit_info]
**Reads:** `__future__`, `bittensor`, `claiming`, `compute_miners`, `datetime`, `loguru`, `sqlalchemy`, `their`, `typing`
**Writes:** `compute_miners`, `compute_state_log`
**Imported by:** `subnet/distributed_compute.py`

#### `subnet/miner.py` — 261 LOC
**Docstring:** GRID Bittensor Subnet Miner.
**Classes:** `LocalInference` [__init__, generate]; `GRIDMiner` [__init__, forward, get_stats]; `StandaloneMiner` [__init__, pull_and_process, run_forever]
**Functions:** `main()`
**Reads:** `__future__`, `config`, `grid`, `loguru`, `typing`, `validator`
**Imports from GRID:** `config`
**Imported by:** `subnet/distributed_compute.py`


### `inference/` (13 modules, 4,378 LOC)

#### `inference/failure_analysis.py` — 527 LOC
**Docstring:** GRID failure regime analysis.
**Classes:** `TradeRecord`; `PerformanceReport`; `FailureDiagnostic` [to_dict]; `FailureAnalyzer` [from_execution_results, from_journal_entries, from_predictions]
**Reads:** `__future__`, `dataclasses`, `datetime`, `decision`, `decisionjournal`, `execution`, `executionsimulator`, `failure`, `grid`, `loguru`, `trade`, `typing`

#### `inference/timesfm_service.py` — 489 LOC
**Docstring:** GRID — TimesFM Forecasting Service.
**Classes:** `SignalForecast`
**Functions:** `forecast_signals(engine, feature_ids, horizon, context, force)`, `get_forecast(engine, feature_name)`, `get_forecasts_by_family(engine, family)`, `get_forecast_summary(engine)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `feature_registry`, `loguru`, `resolved_series`, `signal`, `signal_forecasts`, `sqlalchemy`, `timeseries`, `typing`
**Writes:** `signal_forecasts`
**Imports from GRID:** `timeseries._model_pool`
**Imported by:** `analysis/thesis_scorer.py`, `timeseries/timesfm_forecaster.py`

#### `inference/tuning.py` — 480 LOC
**Docstring:** GRID strategy parameter tuning.
**Classes:** `BacktestResult` [score, to_dict]; `TuningResult` [to_dict]; `StrategyTuner` [__init__, tune_execution, tune_ensemble_weights, refine_params]
**Reads:** `__future__`, `argmax`, `calibration`, `dataclasses`, `inference`, `itertools`, `loguru`, `per`, `sklearn`, `typing`, `validation`
**Imports from GRID:** `inference.calibration`, `validation.execution_sim`

#### `inference/live.py` — 451 LOC
**Docstring:** GRID live inference module.
**Classes:** `LiveInference` [__init__, get_production_models, run_inference, get_feature_snapshot]
**Reads:** `__future__`, `config`, `datetime`, `feature`, `feature_registry`, `features`, `inference`, `intelligence`, `loguru`, `model_artifacts`, `model_registry`, `parameter_snapshot`, `sqlalchemy`, `store`, `trust_scorer`, `typing`
**Imports from GRID:** `config`, `db`, `features.lab`, `inference.kv_cache_manager`, `inference.trained_models`, `intelligence.trust_scorer`, `store.pit`
**Imported by:** `agents/context.py`, `api/routers/flows.py`, `api/routers/signals.py`

#### `inference/calibration.py` — 425 LOC
**Docstring:** GRID probability calibration scoring.
**Classes:** `CalibrationReport` [to_dict, is_well_calibrated, has_strong_resolution]; `CalibrationScorer` [__init__, score, score_shadow]
**Reads:** `__future__`, `calibration`, `dataclasses`, `loguru`, `typing`
**Imported by:** `inference/tuning.py`

#### `inference/circuit_breaker.py` — 405 LOC
**Docstring:** GRID circuit breaker / kill switch.
**Classes:** `RiskCheckResult`; `CircuitBreakerConfig` [to_risk_config]; `RiskEvent`; `CircuitBreaker` [__init__, check_recommendation, record_outcome, activate_kill_switch, reset_kill_switch, is_halted, get_status, get_events]
**Reads:** `__future__`, `dataclasses`, `datetime`, `loguru`, `recorded`, `typing`

#### `inference/trade_logger.py` — 368 LOC
**Docstring:** GRID execution-granularity trade logging.
**Classes:** `TradeLog` [to_dict, from_dict]; `GridTradeLogger` [__init__, log_dir, log_execution_trades, log_journal_decision, load_all, load_recent, update_outcomes]
**Reads:** `__future__`, `all`, `dataclasses`, `datetime`, `execution`, `executionsimulator`, `journal`, `loguru`, `pathlib`, `typing`

#### `inference/training.py` — 345 LOC
**Docstring:** PIT-correct model training pipeline for GRID.
**Classes:** `ModelTrainer` [__init__, build_training_set, train_and_validate]
**Reads:** `__future__`, `analytical_snapshots`, `bucketed`, `datetime`, `feature_registry`, `inference`, `loguru`, `pathlib`, `pit`, `resolved_series`, `sklearn`, `sqlalchemy`, `typing`
**Imports from GRID:** `inference.trained_models`

#### `inference/trained_models.py` — 286 LOC
**Docstring:** Trained model abstractions for GRID inference.
**Classes:** `TrainedModelBase` [fit, predict, predict_proba, get_feature_importance, classes_, feature_names, save, load]; `GradientBoostingRegimeClassifier` [__init__, fit, predict, predict_proba, get_feature_importance, classes_]; `RandomForestRegimeClassifier` [__init__, fit, predict, predict_proba, get_feature_importance, classes_]; `RuleBasedClassifier` [__init__, fit, predict, predict_proba, get_feature_importance, classes_]
**Reads:** `__future__`, `abc`, `datetime`, `disk`, `loguru`, `pathlib`, `sklearn`, `typing`, `xgboost`
**Imported by:** `inference/ensemble.py`, `inference/live.py`, `inference/training.py`

#### `inference/turboquant.py` — 273 LOC
**Docstring:** TurboQuant — KV Cache Quantization (arXiv:2504.19874).
**Classes:** `CompressedKV`
**Functions:** `get_rotation(head_dim)`, `get_codebook(bits, head_dim)`, `quantize_kv(tensor, bits, mode)`, `dequantize_kv(compressed)`, `compression_ratio(compressed)`, `distortion(original, compressed)`
**Reads:** `__future__`, `dataclasses`, `inference`, `loguru`, `typing`, `unit`
**Imported by:** `inference/kv_cache_manager.py`

#### `inference/ensemble.py` — 170 LOC
**Docstring:** Weighted ensemble classifier for GRID regime inference.
**Classes:** `EnsembleClassifier` [__init__, fit, predict, predict_proba, get_disagreement, get_feature_importance, classes_, feature_names]
**Reads:** `__future__`, `inference`, `loguru`, `model`, `typing`
**Imports from GRID:** `inference.trained_models`

#### `inference/kv_cache_manager.py` — 152 LOC
**Docstring:** KV Cache Manager — transparent compress/decompress lifecycle for TurboQuant.
**Classes:** `CacheMetrics` [record_quantize, record_dequantize, summary]; `KVCacheManager` [__init__, store, retrieve, clear, layer_count, get_metrics]
**Reads:** `__future__`, `dataclasses`, `inference`, `loguru`, `typing`
**Imports from GRID:** `inference.turboquant`
**Imported by:** `inference/live.py`

#### `inference/__init__.py` — 7 LOC
**Docstring:** GRID inference layer.


### `store/` (6 modules, 4,154 LOC)

#### `store/astrogrid.py` — 2796 LOC
**Docstring:** AstroGrid persistence helpers.
**Classes:** `AstroGridStore` [__init__, save_snapshot, ensure_lens_set, save_interpretation, save_prediction_stub_postmortem, save_prediction, ensure_active_weight_version, generate_review_run]
**Reads:** `__future__`, `collections`, `config`, `datetime`, `feature_registry`, `lateral`, `loguru`, `ollama`, `oracle`, `raw_series`, `regime_history`, `resolved_series`, `sqlalchemy`, `typing`, `uuid`
**Imports from GRID:** `config`, `ollama.client`, `oracle.astrogrid_universe`
**Imported by:** `api/dependencies.py`

#### `store/snapshots.py` — 431 LOC
**Docstring:** GRID analytical snapshot persistence.
**Classes:** `AnalyticalSnapshotStore` [__init__, save_snapshot, save_pipeline_snapshots, get_latest, get_history, compare_snapshots]
**Reads:** `__future__`, `analytical_snapshots`, `datetime`, `different`, `loguru`, `run_full_pipeline`, `sqlalchemy`, `typing`
**Writes:** `analytical_snapshots`
**Imported by:** `analysis/research_agent.py`, `api/routers/snapshots.py`, `discovery/clustering.py`, `discovery/orthogonality.py`, `features/alpha101.py`, `intelligence/sleuth.py`, `trading/strategy151.py`

#### `store/graph.py` — 367 LOC
**Docstring:** Apache AGE graph query wrapper for GRID.
**Classes:** `GraphStore` [__init__, available, actor_count, expand, shortest_path, connected_actors_by_type, multi_hop_search, community_members]
**Functions:** `get_actor_analytics(actor_id, engine)`, `get_community_members(community_id, limit, engine)`, `get_top_actors(metric, limit, engine)`, `get_community_list(engine)`, `get_graph_store(engine)`
**Reads:** `__future__`, `actor_analytics`, `actors`, `ag_graph`, `cypher`, `loguru`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `api.dependencies`
**Imported by:** `api/routers/intelligence_actors.py`

#### `store/pit.py` — 339 LOC
**Docstring:** GRID Point-in-Time (PIT) query engine.
**Classes:** `PITStore` [__init__, get_pit, get_feature_matrix, assert_no_lookahead, safe_inference_context, get_latest_values]
**Reads:** `__future__`, `contextlib`, `datetime`, `feature_registry`, `loguru`, `persisting`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`
**Imported by:** `agents/context.py`, `api/dependencies.py`, `api/routers/physics.py`, `backtest/engine.py`, `discovery/clustering.py`, `discovery/orthogonality.py`, `features/alpha101.py`, `features/importance.py`, `features/lab.py`, `inference/live.py` (+5)

#### `store/blob.py` — 214 LOC
**Docstring:** S3-compatible blob store backed by MinIO.
**Classes:** `BlobStore` [__init__, available, put, get, get_url, delete, list, exists]
**Reads:** `__future__`, `bucket`, `config`, `datetime`, `loguru`, `minio`, `store`
**Imports from GRID:** `config`
**Imported by:** `api/routers/blob.py`

#### `store/__init__.py` — 7 LOC
**Docstring:** GRID point-in-time store.


### `physics/` (8 modules, 3,659 LOC)

#### `physics/verify.py` — 850 LOC
**Docstring:** GRID market physics verification layer.
**Classes:** `VerificationResult` [to_dict]; `MarketPhysicsVerifier` [__init__, verify_all, check_conservation, check_limiting_cases, check_dimensional_consistency, check_regime_boundaries, check_stationarity, check_numerical_stability]
**Reads:** `__future__`, `dataclasses`, `datetime`, `decision_journal`, `feature_registry`, `get`, `loguru`, `physics`, `resolved_series`, `scipy`, `sqlalchemy`, `statsmodels`, `store`, `typing`
**Imports from GRID:** `physics.conventions`, `physics.momentum`, `store.pit`
**Imported by:** `api/routers/physics.py`

#### `physics/transforms.py` — 681 LOC
**Docstring:** GRID physics-inspired market transforms.
**Functions:** `kinetic_energy(price_series, window)`, `potential_energy(series, window)`, `total_energy(price_series, short_window, long_window)`, `market_temperature(returns, window)`, `entropy_rate(labels, window)`, `phase_velocity(features, n_components)`, `estimate_ou_parameters(series, dt)`, `ou_mean_reversion_signal(series, window, dt)`, `ou_displacement(series, window)`, `langevin_drift(series, window, bins)`, `langevin_diffusion(series, window)`, `fokker_planck_density(series, window, n_points)`, `relaxation_time(series, window, dt)`, `half_life(series, window, dt)`, `hurst_exponent(series, max_lag)`, `rolling_hurst(series, window, max_lag)`, `transfer_entropy(source, target, lag, bins)`
**Reads:** `__future__`, `current`, `equilibrium`, `estimated`, `loguru`, `long`, `scipy`, `sklearn`, `source`
**Imported by:** `api/routers/physics.py`, `features/lab.py`

#### `physics/news_energy.py` — 592 LOC
**Docstring:** GRID physics — News Energy Decomposition Engine.
**Classes:** `NewsEnergyEngine` [__init__, analyze]
**Reads:** `__future__`, `crucix`, `datetime`, `equilibrium`, `feature_registry`, `loguru`, `rate`, `rolling`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `store.pit`
**Imported by:** `api/routers/flows.py`, `api/routers/physics.py`

#### `physics/dealer_gamma.py` — 495 LOC
**Docstring:** GRID — Dealer gamma exposure and hedging flow mechanics.
**Classes:** `DealerGammaEngine` [__init__, compute_gex_profile, compute_all_tickers, get_market_gex_summary]
**Functions:** `bs_gamma(S, K, T, r, sigma)`, `bs_delta_call(S, K, T, r, sigma)`, `bs_delta_put(S, K, T, r, sigma)`, `bs_vanna(S, K, T, r, sigma)`, `bs_charm(S, K, T, r, sigma, is_call)`
**Reads:** `__future__`, `_prepare_chain_arrays`, `chain`, `datetime`, `dealer`, `feature_registry`, `ingestion`, `loguru`, `options`, `options_snapshots`, `resolved_series`, `scipy`, `sqlalchemy`, `typing`
**Imports from GRID:** `ingestion.options`
**Imported by:** `analysis/vol_surface.py`, `api/routers/chat.py`, `api/routers/derivatives.py`, `api/routers/flows.py`, `api/routers/intelligence_risk.py`, `ollama/dealer_flow_briefing.py`, `trading/contagion_to_ticket.py`, `trading/options_recommender.py`

#### `physics/momentum.py` — 427 LOC
**Docstring:** GRID news momentum analysis.
**Classes:** `MomentumResult` [to_dict]; `NewsMomentumAnalyzer` [__init__, analyze]
**Reads:** `__future__`, `dataclasses`, `datetime`, `feature_registry`, `gdelt`, `loguru`, `scipy`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `store.pit`
**Imported by:** `api/routers/flows.py`, `api/routers/physics.py`, `physics/verify.py`

#### `physics/conventions.py` — 351 LOC
**Docstring:** GRID financial convention locking system.
**Classes:** `Convention`
**Functions:** `get_convention(family)`, `validate_convention(feature_name, value, family)`, `validate_feature_set(features, family_map)`, `check_unit_compatibility(feature_a, family_a, feature_b, family_b, operation)`, `list_conventions()`
**Reads:** `__future__`, `dataclasses`, `get`, `loguru`, `typing`
**Imported by:** `api/routers/physics.py`, `physics/verify.py`

#### `physics/waves.py` — 253 LOC
**Docstring:** GRID wave-based pipeline execution.
**Classes:** `WaveTask`
**Functions:** `build_execution_waves(tasks)`, `execute_waves(waves, max_workers, dry_run)`, `build_grid_pipeline_waves(enabled_workflows)`
**Reads:** `__future__`, `build_execution_waves`, `dataclasses`, `each`, `enabled`, `get`, `loader`, `loguru`, `typing`
**Imported by:** `api/routers/workflows.py`

#### `physics/__init__.py` — 10 LOC
**Docstring:** GRID market physics framework.
**Reads:** `physical`


### `alpha_research/` (21 modules, 3,426 LOC)

#### `alpha_research/conviction_scorer.py` — 500 LOC
**Docstring:** Conviction Scorer — 98% confidence trade detector.
**Classes:** `LayerResult`; `ConvictionReport`
**Functions:** `score_setup(conn, ticker, price)`, `score_company(conn, ticker)`, `score_smart_money(conn, ticker)`, `score_crowd(conn, ticker)`, `score_narrative(conn, ticker)`, `score_flow(conn, ticker, price)`, `score_confirmation(price)`, `score_ticker(engine, ticker)`, `scan_all(engine, min_score)`, `print_report(report)`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `feature_registry`, `loguru`, `price`, `raw_series`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `alpha_research.data.split_adjuster`
**Imported by:** `api/routers/signals.py`

#### `alpha_research/strategies/adaptive_rotation.py` — 375 LOC
**Docstring:** Adaptive Rotation Strategy — adapted from FinRL-X.
**Classes:** `RegimeState`; `GroupScore`; `PositionState` [update_peak]; `RotationResult`
**Functions:** `detect_regime(spy_prices, vix_series, as_of_date)`, `score_groups(prices, benchmark_prices, ranking_weeks)`, `check_stops(positions, current_prices, as_of_date)`, `run_rotation(engine, as_of_date, positions)`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `entry`, `finrl`, `peak`, `sqlalchemy`, `typing`
**Imports from GRID:** `alpha_research.data.panel_builder`, `alpha_research.signals.exposure_scaler`

#### `alpha_research/validation/gauntlet.py` — 284 LOC
**Docstring:** False Discovery Gauntlet — 5 statistical tests to prevent self-deception.
**Classes:** `GauntletResult`
**Functions:** `permutation_test(signal, forward_returns, n_shuffles, top_n, cost_bps)`, `deflated_sharpe_ratio(observed_sharpe, n_models_tested, n_observations, skewness, kurtosis)`, `subsample_stability(signal, forward_returns, n_splits, top_n, cost_bps)`, `decay_analysis(signal, forward_returns, horizons)`, `cv_consistency(signal, forward_returns, n_folds, top_n, cost_bps)`, `run_gauntlet(signal, forward_returns, n_models_tested, top_n, cost_bps, n_permutations, n_subsample_splits)`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `noise`, `quantaalpha`, `scipy`, `typing`
**Imports from GRID:** `alpha_research.validation.metrics`

#### `alpha_research/signals/quanta_alpha.py` — 251 LOC
**Docstring:** Proven signals from QuantaAlpha research (Saulius.io).
**Functions:** `vol_regime_adaptive_momentum(prices, short_window, long_window, vol_window, vol_zscore_window, vol_threshold)`, `dual_horizon_momentum(prices, short_window, medium_window, short_weight, medium_weight)`, `trend_volume_gate(prices, volume, fast_ema, slow_ema, momentum_window)`, `vol_price_divergence(prices, volume, sma_window, zscore_window, price_threshold)`, `vol_regime_adaptive_equity(prices, short_window, long_window, vol_window, vol_zscore_window, vol_threshold)`, `dual_horizon_equity(prices, short_window, medium_window, short_weight, medium_weight)`, `compute_all_signals(prices, volume)`, `compute_equity_signals(prices, volume)`
**Reads:** `__future__`, `quantaalpha`
**Imported by:** `alpha_research/adapters/signal_adapter.py`

#### `alpha_research/adapters/signal_adapter.py` — 217 LOC
**Docstring:** Adapter to publish alpha research signals into GRID's SignalRegistry.
**Functions:** `publish_factor_signals(engine, signal_name, signal_panel, as_of_date, top_pct, confidence, valid_hours)`, `publish_regime_signal(engine, signal_name, state, confidence, metadata, valid_hours)`, `publish_all_alpha_signals(engine, as_of_date)`
**Reads:** `__future__`, `alpha_research`, `datetime`, `intelligence`, `sqlalchemy`, `typing`
**Imports from GRID:** `alpha_research.data.panel_builder`, `alpha_research.signals.credit_cycle`, `alpha_research.signals.exposure_scaler`, `alpha_research.signals.quanta_alpha`, `intelligence.signal_registry`

#### `alpha_research/data/split_adjuster.py` — 210 LOC
**Docstring:** Universal stock split adjuster for GRID price data.
**Functions:** `detect_splits(prices, threshold)`, `adjust_splits(prices, threshold)`, `adjust_panel(panel, threshold)`, `detect_panel_splits(panel, threshold)`, `get_post_split_series(prices, threshold)`, `compute_real_drawdown(prices)`
**Reads:** `__future__`, `alpha_research`, `datetime`, `loguru`, `multiple`
**Imported by:** `alpha_research/conviction_scorer.py`, `alpha_research/data/panel_builder.py`

#### `alpha_research/data/panel_builder.py` — 209 LOC
**Docstring:** Build PIT-correct ticker panel data from GRID's resolved_series.
**Functions:** `build_price_panel(engine, tickers, start_date, end_date, as_of_date)`, `build_volume_panel(engine, tickers, start_date, end_date, as_of_date)`, `build_returns_panel(price_panel)`, `get_available_tickers(engine)`, `get_vix_series(engine, start_date, end_date, as_of_date)`
**Reads:** `__future__`, `alpha_research`, `close`, `datetime`, `feature_registry`, `fred`, `grid`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `alpha_research.data.split_adjuster`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `alpha_research/strategies/adaptive_rotation.py`

#### `alpha_research/validation/metrics.py` — 196 LOC
**Docstring:** Portfolio metrics for alpha research validation.
**Functions:** `rank_ic(signal, forward_returns, horizon)`, `rank_icir(ic_series)`, `long_short_returns(signal, forward_returns, top_n, cost_bps)`, `sharpe_ratio(returns, annualize)`, `annualized_return(returns, periods_per_year)`, `max_drawdown(returns)`, `calmar_ratio(returns)`, `turnover(signal, top_n)`, `compute_signal_metrics(signal, forward_returns, top_n, cost_bps)`
**Reads:** `__future__`, `daily`, `quantaalpha`, `scipy`, `validation`
**Imported by:** `alpha_research/ensemble.py`, `alpha_research/validation/gauntlet.py`

#### `alpha_research/signals/macro_regime.py` — 188 LOC
**Docstring:** Macro regime signals that enhance cross-sectional alpha.
**Functions:** `vix_regime_signal(vix, panel_index, panel_columns, zscore_window)`, `vix_momentum_signal(vix, panel_index, panel_columns, fast, slow)`, `credit_spread_signal(hy_spread, panel_index, panel_columns, zscore_window)`, `credit_momentum_signal(hy_spread, panel_index, panel_columns, window)`, `yield_curve_signal(yc_2s10s, panel_index, panel_columns, zscore_window)`, `financial_stress_signal(stress, panel_index, panel_columns, zscore_window)`, `skew_signal(skew, panel_index, panel_columns, zscore_window)`, `sector_dispersion_signal(prices, window)`, `relative_strength_signal(prices, lookback)`
**Reads:** `__future__`

#### `alpha_research/debate.py` — 187 LOC
**Docstring:** Bull/Bear Debate Agent — LLM-powered adversarial analysis.
**Classes:** `DebateResult`
**Functions:** `run_debate(ticker, signals, regime, journal_summary)`, `run_debate_batch(tickers, signals_by_ticker, regime, journal_summary)`
**Reads:** `__future__`, `alpha_research`, `config`, `credit`, `dataclasses`, `datetime`, `loguru`, `response`, `typing`
**Imports from GRID:** `config`

#### `alpha_research/data/shares_tracker.py` — 187 LOC
**Docstring:** Shares outstanding & market cap tracker.
**Functions:** `fetch_daily_fundamentals(ticker, start_date, end_date)`, `compute_shares_outstanding(price_series, market_cap_series)`, `detect_dilution_events(shares, threshold_pct, window)`, `market_cap_adjusted_return(price_series, market_cap_series, periods)`, `get_dilution_adjusted_price(price_series, shares_series)`
**Reads:** `__future__`, `buybacks`, `datetime`, `loguru`, `market_cap`, `marketcap`, `offerings`, `sqlalchemy`, `tiingo`, `typing`, `yfinance`

#### `alpha_research/ensemble.py` — 165 LOC
**Docstring:** LightGBM ensemble for combining alpha research factors.
**Classes:** `EnsembleResult`
**Functions:** `train_ensemble(signal_panels, forward_returns, train_frac, lgb_params, n_rounds, early_stopping)`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `loguru`, `long`, `multiple`, `typing`
**Imports from GRID:** `alpha_research.validation.metrics`

#### `alpha_research/heartbeat.py` — 154 LOC
**Docstring:** Alpha Research Heartbeat — autonomous monitoring job.
**Classes:** `HeartbeatAlert`
**Functions:** `run_heartbeat(engine)`, `format_alerts(alerts)`
**Reads:** `__future__`, `alpha_research`, `dataclasses`, `datetime`, `openclaw`, `raw_series`, `resolved_series`, `source_catalog`, `sqlalchemy`, `typing`
**Imports from GRID:** `alpha_research.signals.credit_cycle`, `alpha_research.signals.exposure_scaler`

#### `alpha_research/signals/credit_cycle.py` — 151 LOC
**Docstring:** Credit Cycle Detector.
**Functions:** `compute_credit_cycle(engine, as_of_date)`
**Reads:** `__future__`, `datetime`, `factor`, `resolved_series`, `sqlalchemy`, `typing`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `alpha_research/heartbeat.py`

#### `alpha_research/signals/exposure_scaler.py` — 145 LOC
**Docstring:** VIX/MA Continuous Exposure Scalar.
**Functions:** `compute_vix_exposure_scalar(engine, as_of_date, ma_window)`, `compute_vix_exposure_series(engine, start_date, end_date, ma_window)`
**Reads:** `__future__`, `datetime`, `quantconnect`, `resolved_series`, `sqlalchemy`
**Imported by:** `alpha_research/adapters/signal_adapter.py`, `alpha_research/heartbeat.py`, `alpha_research/strategies/adaptive_rotation.py`

#### `alpha_research/__init__.py` — 2 LOC
**Docstring:** GRID Alpha Research Engine — Evolutionary factor mining and signal validation.

#### `alpha_research/strategies/__init__.py` — 1 LOC

#### `alpha_research/signals/__init__.py` — 1 LOC

#### `alpha_research/adapters/__init__.py` — 1 LOC

#### `alpha_research/data/__init__.py` — 1 LOC

#### `alpha_research/validation/__init__.py` — 1 LOC


### `ollama/` (7 modules, 3,110 LOC)

#### `ollama/market_briefing.py` — 794 LOC
**Docstring:** GRID Hourly Market Briefing Engine.
**Classes:** `MarketBriefingEngine` [__init__, generate_briefing, cleanup_old_briefings, get_latest_briefing]
**Functions:** `start_hourly_briefings(db_engine)`
**Reads:** `__future__`, `analysis`, `data`, `datetime`, `decision_journal`, `feature_registry`, `grid`, `ingestion`, `intelligence`, `last`, `loguru`, `ollama`, `pathlib`, `raw_series`, `resolved_series`, `signal_sources`, `sqlalchemy`, `trust`, `typing`, `wiki`
**Writes:** `market_briefings`
**Imports from GRID:** `analysis.prompt_optimizer`, `db`, `ingestion.social_sentiment`, `ingestion.wiki_history`, `intelligence.context_provider`, `intelligence.sentiment_scorer`, `intelligence.trust_scorer`, `ollama.client`
**Imported by:** `api/routers/briefing.py`, `api/routers/ollama.py`, `intelligence/scheduler.py`

#### `ollama/dealer_flow_briefing.py` — 596 LOC
**Docstring:** DerivativesGrid Dealer Flow Narrative Synthesis.
**Functions:** `generate_dealer_flow_briefing(engine)`, `get_latest_flow_briefing(engine)`
**Reads:** `__future__`, `dampening`, `datetime`, `dealer_flow_briefings`, `gex`, `jsonb`, `loguru`, `ollama`, `options_daily_signals`, `physics`, `raw_series`, `sqlalchemy`, `top`, `typing`
**Writes:** `dealer_flow_briefings`
**Imports from GRID:** `ollama.client`, `physics.dealer_gamma`
**Imported by:** `api/routers/derivatives.py`, `intelligence/scheduler.py`

#### `ollama/client.py` — 591 LOC
**Docstring:** GRID LLM client.
**Classes:** `OllamaClient` [__init__, load_knowledge, load_all_knowledge, chat, generate, embed, list_models, get_model_names]; `OpenAIClient` [__init__, load_knowledge, load_all_knowledge, chat, generate, embed, list_models, get_model_names]
**Functions:** `get_client()`
**Reads:** `__future__`, `config`, `knowledge`, `llamacpp`, `loguru`, `typing`
**Imports from GRID:** `config`, `knowledge.loader`, `llamacpp.client`
**Imported by:** `agents/runner.py`, `analysis/backtest_scanner.py`, `analysis/capital_flows.py`, `analysis/money_flow.py`, `api/routers/astrogrid_core.py`, `api/routers/astrogrid_helpers.py`, `api/routers/chat.py`, `api/routers/flows.py`, `api/routers/ollama.py`, `api/routers/regime.py` (+14)

#### `ollama/celestial_briefing.py` — 545 LOC
**Docstring:** AstroGrid Celestial Narrative Synthesis.
**Functions:** `generate_celestial_briefing(engine)`, `get_latest_briefing(engine)`
**Reads:** `__future__`, `astro_correlations`, `celestial_briefings`, `datetime`, `feature`, `feature_registry`, `loguru`, `ollama`, `resolved_series`, `sqlalchemy`, `typing`
**Writes:** `celestial_briefings`
**Imports from GRID:** `ollama.client`
**Imported by:** `api/routers/celestial.py`, `intelligence/scheduler.py`

#### `ollama/reasoner.py` — 298 LOC
**Docstring:** GRID Ollama-powered reasoning layer.
**Classes:** `OllamaReasoner` [__init__, explain_relationship, generate_hypothesis_candidates, critique_backtest_result, analyze_regime_transition]
**Reads:** `__future__`, `loguru`, `ollama`, `outputs`, `pattern`, `typing`
**Imports from GRID:** `ollama.client`, `outputs.llm_logger`
**Imported by:** `api/routers/ollama.py`

#### `ollama/router.py` — 278 LOC
**Docstring:** Dual-LLM task router for GRID.
**Classes:** `TaskComplexity`; `TaskRouter` [__init__, quick_client, deep_client, route]
**Functions:** `classify_task(prompt)`, `get_router()`
**Reads:** `__future__`, `config`, `enum`, `gemma`, `llamacpp`, `loguru`, `ollama`, `typing`, `weak`
**Imports from GRID:** `config`, `gemma.client`, `llamacpp.client`, `ollama.client`

#### `ollama/__init__.py` — 8 LOC
**Docstring:** GRID Ollama integration — local LLM inference for market analysis.


### `alerts/` (6 modules, 2,924 LOC)

#### `alerts/supply_chain_alerts.py` — 977 LOC
**Docstring:** GRID Intelligence — Supply Chain Pulse watchdog.
**Classes:** `Finding` [as_dict]
**Functions:** `detect_new_suppliers(engine, since_hours)`, `detect_concentration_shifts(engine, threshold_pp)`, `detect_chokepoint_degradation(engine, delta_threshold)`, `detect_new_high_chokepoints(engine, threshold)`, `detect_geographic_spikes(engine, since_hours, min_nodes)`, `detect_large_acquisitions(engine, min_usd, since_hours)`, `detect_contagion_risk(engine, score_threshold)`, `refresh_snapshots(engine)`, `run_all(engine, since_hours, send_email)`, `render_digest_html(findings)`, `send_digest(findings)`
**Reads:** `__future__`, `alert_state`, `alerts`, `below`, `capital_flows`, `dataclasses`, `datetime`, `intelligence`, `loguru`, `sqlalchemy`, `supply_chain_edge_snapshots`, `supply_chain_edges`, `supply_chain_nodes`, `typing`
**Writes:** `alert_state`, `supply_chain_edge_snapshots`
**Imports from GRID:** `alerts.email`, `intelligence.chain_contagion`

#### `alerts/hundredx_digest.py` — 764 LOC
**Docstring:** GRID 100x Bundled Digest — every 4 hours.
**Functions:** `run_100x_digest(force)`, `schedule_100x_digest(interval_hours)`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `decision_journal`, `discovery`, `hermes_operator`, `live`, `loguru`, `multiple`, `response`, `spot`, `sqlalchemy`, `typing`
**Writes:** `options_daily_signals`, `raw_series`
**Imports from GRID:** `alerts.email`, `config`, `db`, `discovery.options_scanner`

#### `alerts/email.py` — 557 LOC
**Docstring:** GRID Intelligence — Premium newsletter email system.
**Functions:** `send_alert(subject, body, severity)`, `alert_on_failure(source, error)`, `alert_on_regime_change(from_regime, to_regime, confidence)`, `alert_on_100x_opportunity(ticker, score, direction, thesis)`, `send_insight(category, title, content, metadata)`, `send_agent_report(ticker, decision, reasoning, regime_state, confidence, duration)`, `send_weekly_review(review_content)`, `daily_digest()`, `alert_on_failure_with_fix(source, error, fix_commands)`, `alert_on_transition_leaders(leaders, cluster_result)`, `alert_on_discovery_insight(title, description, data)`, `send_test_email()`
**Reads:** `__future__`, `config`, `datetime`, `decision_journal`, `email`, `loguru`, `options_mispricing_scans`, `raw_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`, `db`
**Imported by:** `agents/runner.py`, `alerts/hundredx_digest.py`, `alerts/push_notify.py`, `alerts/scheduler.py`, `alerts/supply_chain_alerts.py`, `api/routers/associations.py`, `ingestion/realtime/flusher.py`, `ingestion/scheduler.py`, `oracle/report.py`, `outputs/insight_scanner.py` (+2)

#### `alerts/push_notify.py` — 553 LOC
**Docstring:** GRID Intelligence — Web Push notification system.
**Functions:** `save_subscription(endpoint, p256dh_key, auth_key, user_agent)`, `remove_subscription(endpoint)`, `get_all_subscriptions(category)`, `get_preferences(endpoint)`, `update_preferences(endpoint, prefs)`, `send_push(subscription, title, body, tag, url, require_interaction)`, `broadcast_push(title, body, tag, url, category)`, `notify_trade_recommendation(ticker, direction, strike, expiry)`, `notify_convergence_alert(description, severity)`, `notify_regime_change(from_regime, to_regime, confidence)`, `notify_red_flag(title, description)`, `notify_price_alert(ticker, price, change_pct, threshold)`, `integrate_with_email_alerts()`
**Reads:** `__future__`, `config`, `datetime`, `existing`, `loguru`, `notification_preferences`, `push_subscriptions`, `pywebpush`, `sqlalchemy`, `typing`
**Writes:** `notification_preferences`, `push_subscriptions`
**Imports from GRID:** `alerts.email`, `config`, `db`
**Imported by:** `api/main.py`, `api/routers/notifications.py`

#### `alerts/scheduler.py` — 71 LOC
**Docstring:** GRID alert scheduler.
**Functions:** `schedule_alerts()`, `stop_alerts()`
**Reads:** `__future__`, `alerts`, `datetime`, `loguru`
**Imports from GRID:** `alerts.email`

#### `alerts/__init__.py` — 2 LOC
**Docstring:** GRID email alerting subsystem.


### `features/` (5 modules, 2,571 LOC)

#### `features/importance.py` — 1013 LOC
**Docstring:** GRID feature importance tracking module.
**Classes:** `FeatureImportanceTracker` [__init__, record_importance, get_importance_history, get_current_rankings, detect_importance_drift, compute_permutation_importance, compute_regime_correlation, compute_rolling_stability]
**Reads:** `__future__`, `datetime`, `decision_journal`, `feature_importance_log`, `feature_registry`, `loguru`, `model_registry`, `scipy`, `sqlalchemy`, `store`, `typing`
**Writes:** `feature_importance_log`
**Imports from GRID:** `store.pit`
**Imported by:** `api/routers/model_comparison.py`, `api/routers/models.py`

#### `features/alpha101.py` — 713 LOC
**Docstring:** GRID WorldQuant 101 Formulaic Alphas engine.
**Classes:** `Alpha101Engine` [__init__, compute_alpha, compute_all_alphas, compute_composite_signal, run_alpha_scan]
**Functions:** `ts_sum(df, window)`, `sma(df, window)`, `stddev(df, window)`, `correlation(x, y, window)`, `covariance(x, y, window)`, `ts_rank(df, window)`, `ts_min(df, window)`, `ts_max(df, window)`, `delta(df, period)`, `delay(df, period)`, `rank(df)`, `scale(df, k)`, `ts_argmax(df, window)`, `ts_argmin(df, window)`, `decay_linear(df, period)`, `product(df, window)`, `signed_power(df, exp)`
**Reads:** `__future__`, `datetime`, `kakushadze`, `loguru`, `market_daily`, `scipy`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `store.pit`, `store.snapshots`

#### `features/lab.py` — 671 LOC
**Docstring:** GRID feature transformation engine.
**Classes:** `FeatureLab` [__init__, compute_feature, compute_derived_features, run_tsfresh_extraction, run_tsfresh_batch]
**Functions:** `zscore_normalize(series, window)`, `rolling_slope(series, window)`, `pct_change_lagged(series, lag_days)`, `ratio(series_a, series_b)`, `spread(series_a, series_b)`
**Reads:** `__future__`, `datetime`, `equilibrium`, `feature_registry`, `fred`, `lag_days`, `loguru`, `physics`, `scipy`, `spot`, `sqlalchemy`, `store`, `tsfresh`, `typing`
**Writes:** `feature_registry`
**Imports from GRID:** `db`, `physics.transforms`, `store.pit`
**Imported by:** `api/routers/physics.py`, `inference/live.py`

#### `features/registry.py` — 167 LOC
**Docstring:** GRID feature registry query interface.
**Classes:** `FeatureRegistry` [__init__, get_all, get_eligible, get_by_family, get_by_name, get_feature_ids, list_families]
**Reads:** `__future__`, `datetime`, `feature_registry`, `loguru`, `registry`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`

#### `features/__init__.py` — 7 LOC
**Docstring:** GRID feature engineering layer.


### `gemma/` (7 modules, 2,474 LOC)

#### `gemma/training/datasets.py` — 654 LOC
**Docstring:** Dataset generators for GRID Gemma fine-tuning.
**Functions:** `build_signal_classifier_dataset()`, `build_anomaly_narrator_dataset()`, `build_edgar_extractor_dataset()`, `build_knowledge_mapper_dataset()`, `build_dataset(task, shuffle, seed)`, `save_dataset_jsonl(task, output_path)`, `load_dataset_for_training(task, dataset_path)`
**Reads:** `__future__`, `china`, `cold`, `combined`, `company`, `datasets`, `drug`, `gemma`, `grid`, `highs`, `pathlib`, `tech`, `typing`
**Imports from GRID:** `gemma.training.config`
**Imported by:** `gemma/training/train.py`

#### `gemma/training/train.py` — 587 LOC
**Docstring:** GRID Gemma Fine-Tuning with Unsloth.
**Functions:** `train(config)`, `export_gguf(config, quantization)`, `merge_and_save(config)`, `test_inference(config, prompt)`, `main(argv)`
**Reads:** `__future__`, `gemma`, `loguru`, `pathlib`, `transformers`, `trl`, `unsloth`
**Imports from GRID:** `gemma.training.config`, `gemma.training.datasets`

#### `gemma/client.py` — 511 LOC
**Docstring:** GRID Gemma 4 27B QAT client.
**Classes:** `GemmaClient` [__init__, load_knowledge, load_all_knowledge, chat, generate, embed, list_models, get_model_names]
**Functions:** `get_client()`
**Reads:** `__future__`, `config`, `knowledge`, `llm`, `loguru`, `server`, `typing`
**Imports from GRID:** `config`, `knowledge.loader`
**Imported by:** `gemma/__init__.py`, `ollama/router.py`

#### `gemma/micro.py` — 408 LOC
**Docstring:** GRID Gemma 4 — Task-Specific Fine-Tuned Models.
**Classes:** `MicroModelConfig`; `GemmaMicroClient` [__init__, run, health_check]; `GemmaMicroPool` [__init__, classify_signal, narrate_anomaly, extract_edgar, map_knowledge, get_client, health_check, available_count]
**Functions:** `get_micro_pool()`
**Reads:** `__future__`, `config`, `dataclasses`, `loguru`, `sec`, `typing`
**Imports from GRID:** `config`
**Imported by:** `ingestion/altdata/edgar_transcripts.py`, `ingestion/signal_classifier.py`

#### `gemma/training/config.py` — 275 LOC
**Docstring:** Training configuration for GRID Gemma fine-tuning.
**Classes:** `TaskType`; `LoRAConfig`; `TrainingConfig` [resolved_model_name, model_output_dir, system_prompt]
**Functions:** `get_preset_config(base_model, task)`
**Reads:** `__future__`, `dataclasses`, `enum`, `pathlib`, `unsloth`
**Imported by:** `gemma/training/__init__.py`, `gemma/training/datasets.py`, `gemma/training/train.py`

#### `gemma/training/__init__.py` — 21 LOC
**Docstring:** GRID Gemma Training — Fine-tune Gemma 4 / Gemma 3 for GRID-specific tasks.
**Reads:** `gemma`, `grid`
**Imports from GRID:** `gemma.training.config`

#### `gemma/__init__.py` — 18 LOC
**Docstring:** GRID Gemma 3 integration.
**Reads:** `gemma`
**Imports from GRID:** `gemma.client`


### `discovery/` (5 modules, 2,208 LOC)

#### `discovery/options_scanner.py` — 785 LOC
**Docstring:** GRID — Options mispricing scanner.
**Classes:** `MispricingOpportunity` [is_100x]; `OptionsScanner` [__init__, scan_all, get_100x_opportunities, format_report, persist_scan]
**Reads:** `__future__`, `buying`, `dataclasses`, `datetime`, `loguru`, `max`, `options_daily_signals`, `sqlalchemy`, `typing`
**Writes:** `options_mispricing_scans`
**Imports from GRID:** `db`
**Imported by:** `alerts/hundredx_digest.py`, `api/routers/derivatives.py`, `api/routers/options.py`, `ingestion/scheduler.py`, `trading/options_recommender.py`

#### `discovery/clustering.py` — 625 LOC
**Docstring:** GRID cluster discovery engine.
**Classes:** `ClusterDiscovery` [__init__, run_cluster_discovery, identify_transition_leaders, get_transition_leaders]
**Reads:** `__future__`, `analytical_snapshots`, `datetime`, `feature_registry`, `hyperspace`, `kmeans`, `latest`, `loguru`, `metrics`, `pathlib`, `scipy`, `sklearn`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `db`, `hyperspace.client`, `hyperspace.reasoner`, `store.pit`, `store.snapshots`
**Imported by:** `api/routers/discovery.py`, `api/routers/regime.py`

#### `discovery/orthogonality.py` — 549 LOC
**Docstring:** GRID orthogonality audit module.
**Classes:** `OrthogonalityAudit` [__init__, run_full_audit, get_orthogonal_features]
**Reads:** `__future__`, `datetime`, `each`, `feature_registry`, `hyperspace`, `loguru`, `pathlib`, `sklearn`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `db`, `hyperspace.client`, `hyperspace.embeddings`, `store.pit`, `store.snapshots`
**Imported by:** `api/routers/discovery.py`

#### `discovery/changepoint_detector.py` — 242 LOC
**Docstring:** AutoBNN-powered changepoint detection for GRID's discovery pipeline.
**Classes:** `ChangeReport`
**Functions:** `scan_for_changepoints(engine, min_confidence, lookback_days, max_features)`, `publish_regime_signals(engine, report)`, `run_changepoint_cycle(engine, min_confidence)`
**Reads:** `__future__`, `dataclasses`, `datetime`, `feature_registry`, `loguru`, `resolved_series`, `scan_for_changepoints`, `signal_registry`, `sqlalchemy`, `timeseries`, `typing`
**Writes:** `signal_registry`
**Imports from GRID:** `timeseries.autobnn`

#### `discovery/__init__.py` — 7 LOC
**Docstring:** GRID discovery engine.


### `agents/` (9 modules, 1,687 LOC)

#### `agents/runner.py` — 568 LOC
**Docstring:** TradingAgents orchestration runner.
**Classes:** `AgentRunner` [__init__, run, get_runs, get_run]
**Reads:** `__future__`, `agent_runs`, `agents`, `alerts`, `config`, `datetime`, `debate`, `loguru`, `model_registry`, `ollama`, `outputs`, `single`, `sqlalchemy`, `tradingagents`, `typing`
**Writes:** `agent_runs`, `decision_journal`
**Imports from GRID:** `agents.adapter`, `agents.config`, `agents.context`, `agents.personas`, `agents.progress`, `alerts.email`, `config`, `ollama.client`, `outputs.llm_logger`
**Imported by:** `agents/scheduler.py`, `api/routers/agents.py`

#### `agents/personas.py` — 239 LOC
**Docstring:** Investor persona system for TradingAgents.
**Classes:** `InvestorPersona`
**Functions:** `get_persona(name)`, `list_personas()`, `format_persona_context(persona)`
**Reads:** `__future__`, `dataclasses`, `loguru`, `typing`
**Imported by:** `agents/runner.py`

#### `agents/backtest.py` — 206 LOC
**Docstring:** Agent decision backtesting.
**Classes:** `AgentBacktester` [__init__, run_backtest, get_comparison_summary]
**Reads:** `__future__`, `agent_runs`, `config`, `datetime`, `decision_journal`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `config`
**Imported by:** `api/routers/agents.py`

#### `agents/adapter.py` — 176 LOC
**Docstring:** Adapter between TradingAgents output and GRID's decision journal.
**Functions:** `parse_agent_decision(raw_decision)`, `compute_conviction_score(parsed)`
**Reads:** `__future__`, `agents`, `debate`, `loguru`, `tradingagentsgraph`, `typing`
**Imported by:** `agents/runner.py`

#### `agents/config.py` — 137 LOC
**Docstring:** TradingAgents LLM provider configuration.
**Functions:** `scale_debate_rounds(position_size)`, `build_agent_config(position_size)`
**Reads:** `__future__`, `config`, `grid`, `llm`, `loguru`, `typing`
**Imports from GRID:** `config`
**Imported by:** `agents/runner.py`

#### `agents/scheduler.py` — 127 LOC
**Docstring:** Scheduled TradingAgents runs.
**Functions:** `start_agent_scheduler()`, `stop_agent_scheduler()`, `get_schedule_status()`
**Reads:** `__future__`, `agents`, `config`, `datetime`, `loguru`
**Imports from GRID:** `agents.runner`, `config`, `db`
**Imported by:** `api/routers/agents.py`

#### `agents/context.py` — 115 LOC
**Docstring:** GRID context builder for TradingAgents.
**Classes:** `GRIDContext` [__init__, build]
**Reads:** `__future__`, `config`, `context`, `datetime`, `inference`, `loguru`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `config`, `inference.live`, `store.pit`
**Imported by:** `agents/runner.py`

#### `agents/progress.py` — 111 LOC
**Docstring:** Agent run progress tracking via WebSocket broadcast.
**Functions:** `register_broadcast(fn, loop)`, `emit_progress(run_id, stage, ticker, detail, progress_pct, data)`, `emit_run_complete(run_id, ticker, final_decision, duration, error)`
**Reads:** `__future__`, `datetime`, `loguru`, `typing`
**Imported by:** `agents/runner.py`, `api/main.py`

#### `agents/__init__.py` — 8 LOC
**Docstring:** GRID TradingAgents integration.


### `backtest/` (4 modules, 1,573 LOC)

#### `backtest/engine.py` — 796 LOC
**Docstring:** GRID Pitch Backtest Engine.
**Classes:** `PitchBacktester` [__init__, run_historical_regime, get_asset_prices, simulate_portfolio, run_full_backtest, get_latest_results, get_summary]
**Functions:** `half_kelly_fraction(win_rate, avg_win, avg_loss)`, `regime_adjusted_size(kelly_frac, regime, confidence)`, `compute_metrics(daily_returns, risk_free_rate, trading_days)`, `compute_regime_stats(daily_returns, regime_series)`, `compute_transition_returns(daily_returns, regime_series, horizons)`
**Reads:** `__future__`, `datetime`, `feature_registry`, `loguru`, `pathlib`, `pit`, `sklearn`, `sqlalchemy`, `store`, `typing`
**Imports from GRID:** `db`, `store.pit`
**Imported by:** `api/routers/backtest.py`, `backtest/charts.py`, `backtest/paper_trade.py`

#### `backtest/paper_trade.py` — 443 LOC
**Docstring:** GRID Live Paper Trade System.
**Classes:** `PaperTradeTracker` [__init__, create_snapshot, list_snapshots, get_snapshot, score_predictions]
**Reads:** `__future__`, `backtest`, `datetime`, `decision_journal`, `feature_registry`, `loguru`, `model_registry`, `pathlib`, `sqlalchemy`, `typing`
**Writes:** `decision_journal`
**Imports from GRID:** `backtest.engine`, `db`
**Imported by:** `api/routers/backtest.py`

#### `backtest/charts.py` — 332 LOC
**Docstring:** GRID Backtest Chart Generator.
**Functions:** `generate_all_charts(result)`
**Reads:** `__future__`, `backtest`, `disk`, `matplotlib`, `orthogonality`, `pathlib`, `typing`
**Imports from GRID:** `backtest.engine`
**Imported by:** `api/routers/backtest.py`

#### `backtest/__init__.py` — 2 LOC
**Docstring:** GRID pitch backtest engine.


### `normalization/` (3 modules, 1,384 LOC)

#### `normalization/entity_map.py` — 1055 LOC
**Docstring:** GRID entity mapping module.
**Classes:** `EntityMap` [__init__, get_feature_id, get_all_mappings, load_v2_mappings, suggest_mapping]
**Reads:** `__future__`, `bls`, `datetime`, `existing`, `feature_registry`, `ingestion`, `loguru`, `resolved_series`, `sqlalchemy`, `typing`
**Imports from GRID:** `db`
**Imported by:** `api/routers/watchlist_helpers.py`, `intelligence/resolution_audit.py`, `intelligence/source_audit.py`, `normalization/resolver.py`

#### `normalization/resolver.py` — 322 LOC
**Docstring:** GRID conflict resolution module.
**Classes:** `Resolver` [__init__, resolve_pending, get_conflict_report]
**Reads:** `__future__`, `concurrent`, `datetime`, `feature_registry`, `loguru`, `normalization`, `raw_series`, `resolved_series`, `source_catalog`, `sqlalchemy`, `typing`
**Writes:** `resolved_series`
**Imports from GRID:** `db`, `normalization.entity_map`
**Imported by:** `intelligence/resolution_audit.py`

#### `normalization/__init__.py` — 7 LOC
**Docstring:** GRID normalization layer.


### `hyperspace/` (6 modules, 1,316 LOC)

#### `hyperspace/embeddings.py` — 326 LOC
**Docstring:** GRID semantic embedding layer.
**Classes:** `GRIDEmbeddings` [__init__, embed_features, semantic_similarity_matrix, find_similar_features, embed_hypothesis, hypothesis_dedup_check]
**Reads:** `__future__`, `feature_registry`, `hyperspace`, `loguru`, `sqlalchemy`, `typing`
**Imports from GRID:** `hyperspace.client`
**Imported by:** `discovery/orthogonality.py`

#### `hyperspace/client.py` — 270 LOC
**Docstring:** GRID Hyperspace API client.
**Classes:** `HyperspaceClient` [__init__, chat, embed, get_available_models, health_check]
**Functions:** `get_client()`
**Reads:** `__future__`, `config`, `loguru`, `typing`
**Imports from GRID:** `config`
**Imported by:** `api/routers/system.py`, `discovery/clustering.py`, `discovery/orthogonality.py`, `hyperspace/embeddings.py`, `hyperspace/monitor.py`, `hyperspace/reasoner.py`

#### `hyperspace/monitor.py` — 264 LOC
**Docstring:** GRID Hyperspace node monitoring module.
**Classes:** `HyperspaceMonitor` [__init__, get_node_status, get_system_info, get_points_summary, tail_log, is_earning]
**Functions:** `print_status_dashboard()`
**Reads:** `__future__`, `hyperspace`, `loguru`, `pathlib`, `typing`, `whoami`
**Imports from GRID:** `hyperspace.client`
**Imported by:** `api/routers/system.py`

#### `hyperspace/reasoner.py` — 246 LOC
**Docstring:** GRID LLM-assisted reasoning layer.
**Classes:** `GRIDReasoner` [__init__, explain_relationship, generate_hypothesis_candidates, critique_backtest_result]
**Reads:** `__future__`, `hyperspace`, `loguru`, `outputs`, `pattern`
**Imports from GRID:** `hyperspace.client`, `outputs.llm_logger`
**Imported by:** `discovery/clustering.py`

#### `hyperspace/research_agent.py` — 199 LOC
**Docstring:** GRID Hyperspace research agent definition.
**Classes:** `GRIDResearchAgent` [__init__, setup_soul, create_project_definition, log_experiment]
**Reads:** `__future__`, `datetime`, `grid`, `loguru`, `pathlib`, `typing`

#### `hyperspace/__init__.py` — 11 LOC
**Docstring:** GRID Hyperspace integration layer.


### `validation/` (4 modules, 1,179 LOC)

#### `validation/execution_sim.py` — 543 LOC
**Docstring:** GRID execution simulation layer.
**Classes:** `OrderSide`; `OrderType`; `Order`; `EdgeEstimate` [edge, abs_edge, direction]; `MarketState` [total_liquidity, spread_bps]; `PortfolioPosition`; `Portfolio` [total_value, update_cash]; `RiskLimits`; `RiskCheckResult`; `RiskManager` [__init__, check_order, update_position]; `ExecutionSimConfig`; `ExecutionSimulator` [__init__, simulate_era, estimate_execution_cost]
**Reads:** `__future__`, `annualised`, `dataclasses`, `datetime`, `enum`, `grid`, `loguru`, `typing`, `volatility`
**Imported by:** `inference/tuning.py`

#### `validation/backtest.py` — 359 LOC
**Docstring:** GRID walk-forward backtesting engine.
**Classes:** `WalkForwardBacktest` [__init__, run_validation]
**Reads:** `__future__`, `datetime`, `loguru`, `sqlalchemy`, `store`, `typing`
**Writes:** `validation_results`
**Imports from GRID:** `db`, `store.pit`

#### `validation/gates.py` — 270 LOC
**Docstring:** GRID promotion gate enforcement module.
**Classes:** `GateChecker` [__init__, check_candidate_to_shadow, check_shadow_to_staging, check_staging_to_production, check_all_gates]
**Reads:** `__future__`, `decision_journal`, `hypothesis_registry`, `loguru`, `model_artifacts`, `model_registry`, `shadow_scores`, `sqlalchemy`, `typing`, `validation_results`
**Imports from GRID:** `db`
**Imported by:** `governance/registry.py`

#### `validation/__init__.py` — 7 LOC
**Docstring:** GRID validation layer.


### `contracts/` (12 modules, 1,150 LOC)

#### `contracts/schemas.py` — 195 LOC
**Docstring:** Contract schemas for the GRID information-flow layer.
**Classes:** `SignalRef`; `BaseContract`; `PostmortemCompleted`; `PredictionScored`; `BacktestGateVerdict`; `OptionsTradeOutcome`; `CrossReferenceAnomaly`; `LeverageRiskUpdate`; `RegimeTransition`; `SignalFired`; `HypothesisGenerated`; `ActorMaterialized`; `PullLifecycle`; `ForensicsTrace`; `InvestigationProgress`
**Reads:** `__future__`, `datetime`, `decimal`, `pydantic`, `typing`, `uuid`
**Imported by:** `contracts/channels.py`, `contracts/dispatcher.py`, `contracts/emit.py`, `contracts/replay.py`, `contracts/retry_scheduler.py`, `contracts/router.py`

#### `contracts/emit.py` — 173 LOC
**Docstring:** Emit helpers for the contracts layer.
**Functions:** `emit(contract)`, `pull_lifecycle(puller_name)`
**Reads:** `__future__`, `contextlib`, `contracts`, `events`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Writes:** `contracts_audit`
**Imports from GRID:** `api.dependencies`, `contracts`, `contracts.channels`, `contracts.correlation`, `contracts.schemas`, `events.bus`
**Imported by:** `contracts/__init__.py`

#### `contracts/dead_letter.py` — 162 LOC
**Docstring:** Dead-letter store for the contracts layer.
**Classes:** `DeadLetterEntry`
**Functions:** `record_failure(engine)`, `pending_retries(engine, now, limit)`, `mark_resolved(engine, entry_id)`, `bump_retry(engine, entry_id, retry_count)`, `schedule_next_retry(retry_count, now)`
**Reads:** `__future__`, `contracts_dead_letter`, `dataclasses`, `datetime`, `sqlalchemy`, `typing`, `uuid`
**Writes:** `contracts_dead_letter`
**Imported by:** `api/main.py`, `contracts/replay.py`, `contracts/retry_scheduler.py`

#### `contracts/dispatcher.py` — 151 LOC
**Docstring:** Contract dispatcher.
**Classes:** `Dispatcher` [__init__, start, wait_idle]
**Reads:** `__future__`, `concurrent`, `contracts`, `fakebus`, `loguru`, `pydantic`, `typing`, `uuid`
**Imports from GRID:** `contracts`, `contracts.channels`, `contracts.router`, `contracts.schemas`
**Imported by:** `api/main.py`, `contracts/__init__.py`

#### `contracts/replay.py` — 140 LOC
**Docstring:** Manual replay for dead-letter entries.
**Functions:** `replay_entry(engine, entry)`, `replay_many(engine, entries)`, `replay_filtered(engine, contract_type, limit)`, `build_parser()`, `main(argv)`
**Reads:** `__future__`, `contracts`, `contracts_dead_letter`, `loguru`, `sqlalchemy`, `typing`, `uuid`
**Imports from GRID:** `api.dependencies`, `contracts.dead_letter`, `contracts.router`, `contracts.schemas`
**Imported by:** `api/routers/contracts.py`

#### `contracts/observability.py` — 108 LOC
**Docstring:** In-process contracts metrics.
**Functions:** `emitted(contract_type)`, `dispatched(contract_type, consumer)`, `failed(contract_type, consumer, error_type)`, `record_duration(contract_type, consumer, seconds)`, `snapshot()`, `reset()`, `render_prometheus()`
**Reads:** `__future__`, `collections`, `typing`

#### `contracts/retry_scheduler.py` — 82 LOC
**Docstring:** Background retry scheduler for dead-letter entries.
**Classes:** `RetryScheduler` [__init__, start, stop, run_once]
**Reads:** `__future__`, `contracts`, `loguru`, `typing`
**Imports from GRID:** `contracts.dead_letter`, `contracts.router`, `contracts.schemas`
**Imported by:** `api/main.py`

#### `contracts/correlation.py` — 42 LOC
**Docstring:** Correlation id propagation for the contracts layer.
**Functions:** `new_correlation_id()`, `get_current_correlation_id()`, `correlation_scope(cid)`
**Reads:** `__future__`, `contextlib`, `contextvars`, `typing`, `uuid`
**Imported by:** `contracts/__init__.py`, `contracts/emit.py`

#### `contracts/router.py` — 37 LOC
**Docstring:** Contract routing table.
**Functions:** `resolve_handler(dotted_path)`
**Reads:** `__future__`, `contracts`, `typing`
**Imports from GRID:** `contracts.schemas`
**Imported by:** `contracts/dispatcher.py`, `contracts/replay.py`, `contracts/retry_scheduler.py`

#### `contracts/channels.py` — 35 LOC
**Docstring:** Contract-type → event-bus channel mapping.
**Functions:** `channel_for(contract_cls)`, `contract_for_channel(channel)`
**Reads:** `__future__`, `contracts`
**Imports from GRID:** `contracts.schemas`
**Imported by:** `contracts/dispatcher.py`, `contracts/emit.py`

#### `contracts/__init__.py` — 23 LOC
**Docstring:** GRID contracts infrastructure.
**Reads:** `__future__`, `contracts`
**Imports from GRID:** `contracts.correlation`, `contracts.dispatcher`, `contracts.emit`
**Imported by:** `api/routers/contracts.py`, `contracts/dispatcher.py`, `contracts/emit.py`

#### `contracts/handlers/__init__.py` — 2 LOC
**Docstring:** Phase 2 contract handlers — empty in Phase 1.


### `timeseries/` (4 modules, 1,021 LOC)

#### `timeseries/timesfm_forecaster.py` — 479 LOC
**Docstring:** GRID TimesFM Forecaster.
**Classes:** `ForecastResult`; `BatchForecastResult`; `TimesFMForecaster` [__init__, is_available, forecast, batch_forecast, health_check]
**Functions:** `get_forecaster()`, `signal_forecast_to_forecast_result(sf, series_id)`, `forecast_result_to_signal_forecast(fr, feature_id, feature_name)`
**Reads:** `__future__`, `config`, `dataclasses`, `datetime`, `inference`, `interval`, `loguru`, `point`, `predictions`, `timeseries`, `typing`
**Imports from GRID:** `config`, `inference.timesfm_service`, `timeseries._model_pool`
**Imported by:** `api/routers/chat.py`, `api/routers/forecasts.py`, `oracle/forecaster_adapter.py`, `timeseries/__init__.py`

#### `timeseries/autobnn.py` — 400 LOC
**Docstring:** GRID AutoBNN — Interpretable Signal Decomposition.
**Classes:** `DecompositionResult`; `RegimeChangeSignal`; `AutoBNNDecomposer` [__init__, is_available, decompose, detect_regime_changes, health_check]
**Functions:** `get_decomposer()`
**Reads:** `__future__`, `autobnn`, `config`, `dataclasses`, `datetime`, `loguru`, `trend`, `typing`
**Imports from GRID:** `config`
**Imported by:** `discovery/changepoint_detector.py`

#### `timeseries/_model_pool.py` — 124 LOC
**Docstring:** Shared TimesFM model pool.
**Functions:** `get_timesfm_model(context_len, horizon_len, batch_size)`
**Reads:** `__future__`, `any`, `loguru`, `typing`
**Imported by:** `inference/timesfm_service.py`, `timeseries/timesfm_forecaster.py`

#### `timeseries/__init__.py` — 18 LOC
**Docstring:** GRID Time-Series Forecasting.
**Reads:** `timeseries`
**Imports from GRID:** `timeseries.timesfm_forecaster`


### `outputs/` (3 modules, 707 LOC)

#### `outputs/insight_scanner.py` — 356 LOC
**Docstring:** Periodic scanner for accumulated LLM insights.
**Functions:** `run_insight_review(days)`, `schedule_reviews()`
**Reads:** `__future__`, `alerts`, `cli`, `collections`, `content`, `datetime`, `loguru`, `outputs`, `pathlib`, `typing`
**Imports from GRID:** `alerts.email`

#### `outputs/llm_logger.py` — 349 LOC
**Docstring:** Timestamped markdown logger for all LLM outputs and insights.
**Functions:** `log_insight(category, title, content, metadata, provider)`, `log_agent_deliberation(ticker, regime_state, confidence, parsed, provider, model, duration)`, `cleanup_old_insights(max_age_days)`, `get_recent_insights(category, days, limit)`
**Reads:** `__future__`, `alerts`, `config`, `datetime`, `filename`, `loguru`, `outputs`, `pathlib`, `typing`, `verification`
**Imports from GRID:** `alerts.email`, `config`
**Imported by:** `agents/runner.py`, `analysis/capital_flows.py`, `hyperspace/reasoner.py`, `intelligence/market_diary.py`, `ollama/reasoner.py`

#### `outputs/__init__.py` — 2 LOC
**Docstring:** GRID outputs — LLM insight logging and periodic review.


### `a2a/` (4 modules, 675 LOC)

#### `a2a/client.py` — 233 LOC
**Docstring:** A2A Client — discover and delegate to remote agents.
**Classes:** `TaskState`; `A2ATask`; `A2AClient` [__init__, discover, send_task, get_task, clear_cache]
**Reads:** `__future__`, `dataclasses`, `enum`, `loguru`, `typing`
**Imported by:** `a2a/__init__.py`, `a2a/server.py`, `api/routers/a2a.py`

#### `a2a/agent_card.py` — 212 LOC
**Docstring:** A2A Agent Card — JSON capability descriptor.
**Classes:** `AgentSkill`; `AgentCard` [to_dict]
**Functions:** `build_grid_agent_card(base_url)`
**Reads:** `__future__`, `config`, `dataclasses`, `loguru`, `typing`
**Imports from GRID:** `config`
**Imported by:** `a2a/__init__.py`, `api/routers/a2a.py`

#### `a2a/server.py` — 207 LOC
**Docstring:** A2A Server — receive and process task requests from external agents.
**Classes:** `A2ATaskManager` [__init__, register_handler, submit_task, get_task, cancel_task, list_tasks, task_count, registered_skills]
**Reads:** `__future__`, `a2a`, `datetime`, `external`, `loguru`, `typing`
**Imports from GRID:** `a2a.client`
**Imported by:** `a2a/__init__.py`, `api/routers/a2a.py`

#### `a2a/__init__.py` — 23 LOC
**Docstring:** GRID Agent-to-Agent (A2A) Protocol.
**Reads:** `a2a`, `external`
**Imports from GRID:** `a2a.agent_card`, `a2a.client`, `a2a.server`


### `events/` (5 modules, 524 LOC)

#### `events/producer.py` — 197 LOC
**Docstring:** Durable event producer -- sends structured events to Redpanda topics.
**Functions:** `emit(topic_key, payload, key)`, `emit_async(topic_key, payload, key)`, `flush()`, `close()`
**Reads:** `__future__`, `config`, `datetime`, `events`, `kafka`, `loguru`, `typing`
**Imports from GRID:** `config`, `db`
**Imported by:** `api/routers/canvas_investigate.py`, `api/routers/sse.py`, `events/__init__.py`, `events/consumer.py`

#### `events/bus.py` — 140 LOC
**Docstring:** GRID Event Bus — PG LISTEN/NOTIFY wrapper with in-process fan-out.
**Classes:** `EventBus` [__init__, subscribe, emit_sync, start, stop, emit]
**Reads:** `__future__`, `collections`, `datetime`, `events`, `loguru`, `postgresql`, `typing`
**Imports from GRID:** `events.channels`
**Imported by:** `api/main.py`, `api/routers/sse.py`, `contracts/emit.py`

#### `events/consumer.py` — 119 LOC
**Docstring:** Durable event consumer -- reads events from Redpanda topics.
**Functions:** `consume(topic_key, group, callback, timeout_ms, max_messages)`, `get_topic_info(topic_key)`
**Reads:** `__future__`, `config`, `events`, `kafka`, `loguru`, `redpanda`, `typing`
**Imports from GRID:** `config`, `events.producer`
**Imported by:** `api/routers/sse.py`, `events/__init__.py`

#### `events/channels.py` — 52 LOC
**Docstring:** Event channel constants and payload schemas.
**Classes:** `Event` [to_sse]
**Reads:** `__future__`, `dataclasses`, `typing`
**Imported by:** `api/routers/sse.py`, `events/bus.py`

#### `events/__init__.py` — 16 LOC
**Docstring:** GRID event system -- durable event streaming via Redpanda with PG NOTIFY fallback.
**Reads:** `events`
**Imports from GRID:** `events.consumer`, `events.producer`


### `rag/` (5 modules, 452 LOC)

### `llamacpp/` (2 modules, 447 LOC)

#### `llamacpp/client.py` — 439 LOC
**Docstring:** GRID llama.cpp server client.
**Classes:** `LlamaCppClient` [__init__, load_knowledge, load_all_knowledge, chat, generate, embed, list_models, get_model_names]
**Functions:** `get_client()`
**Reads:** `__future__`, `config`, `knowledge`, `llama`, `loguru`, `server`, `typing`
**Imports from GRID:** `config`, `knowledge.loader`
**Imported by:** `ollama/client.py`, `ollama/router.py`

#### `llamacpp/__init__.py` — 8 LOC
**Docstring:** GRID llama.cpp integration.


### `journal/` (2 modules, 349 LOC)

#### `journal/log.py` — 342 LOC
**Docstring:** GRID immutable decision journal.
**Classes:** `DecisionJournal` [__init__, log_decision, record_outcome, get_performance_summary, get_recent]
**Reads:** `__future__`, `datetime`, `decision_journal`, `journal`, `loguru`, `sqlalchemy`, `typing`
**Writes:** `decision_journal`
**Imports from GRID:** `db`
**Imported by:** `api/dependencies.py`, `trading/contagion_to_ticket.py`

#### `journal/__init__.py` — 7 LOC
**Docstring:** GRID decision journal.


### `governance/` (2 modules, 321 LOC)

#### `governance/registry.py` — 314 LOC
**Docstring:** GRID model governance registry.
**Classes:** `ModelRegistry` [__init__, transition, get_production_model, flag_model, rollback]
**Reads:** `__future__`, `datetime`, `loguru`, `model_registry`, `monitoring`, `sqlalchemy`, `typing`, `validation`
**Imports from GRID:** `db`, `validation.gates`
**Imported by:** `api/dependencies.py`

#### `governance/__init__.py` — 7 LOC
**Docstring:** GRID governance layer.


## CLAUDE.md Intelligence Section Diff

CLAUDE.md lists **14** intelligence modules at ~14,402 LOC.
Actual `intelligence/` tree (recursive): **143** modules, **92,759** LOC.
Top-level `intelligence/*.py` only: **94** modules.

### Listed in CLAUDE.md but missing from disk: 0 (resolved 2026-04-13)
- ~~`intelligence/flow_aggregator.py`~~ — never lived under `intelligence/`; the real canonical module is `analysis/flow_aggregator.py` (1,147 LOC). Prior CLAUDE.md wording was misleading.
- ~~`intelligence/flow_thesis.py`~~ — same story. Real module is `analysis/flow_thesis.py` (21 LOC facade) which composes `analysis/flow_thesis_data.py` (1,415 LOC) + `analysis/flow_thesis_scoring.py` (334 LOC). The CLAUDE.md "scoring/flow stack" bullet is correct in spirit but names the wrong directory.

### Present on disk but missing from CLAUDE.md: 82
- `intelligence/__init__.py` (2 LOC) — (no docstring)
- `intelligence/actor_discovery.py` (3533 LOC) — GRID Intelligence — Automated Actor Discovery & Enrichment (250K+ Scale).
- `intelligence/actor_ingest.py` (228 LOC) — Universal Actor Ingestion — auto-discover and log actors from ANY data source.
- `intelligence/actor_researcher.py` (416 LOC) — Actor Researcher — local LLM agent that continuously enriches actor profiles.
- `intelligence/actor_signal_bridge.py` (292 LOC) — Actor Signal Bridge — injects actor intelligence into the prediction pipeline.
- `intelligence/agent_arena.py` (583 LOC) — GRID Intelligence — Agent Arena: 10 Competing Trading Analysts.
- `intelligence/attention_anomaly.py` (185 LOC) — Attention Anomaly Detector — combines Wikipedia + Google Trends signals.
- `intelligence/audio_briefing.py` (769 LOC) — GRID -- Daily Intelligence Audio Briefing Pipeline.
- `intelligence/banking_network.py` (1692 LOC) — GRID Intelligence -- Global Banking & Financial Services Power Network.
- `intelligence/breaking_news.py` (341 LOC) — Breaking news monitor — detects high-impact events in near-real-time.
- `intelligence/business_news_parser.py` (804 LOC) — GRID Intelligence — Business News Parser.
- `intelligence/capital_flow_rollups.py` (339 LOC) — Capital-flow rollup derivations.
- `intelligence/causation_core.py` (195 LOC) — GRID Intelligence — Causal Connection Engine (core module).
- `intelligence/causation_graph.py` (1179 LOC) — GRID Intelligence — Causal Connection Engine (graph module).
- `intelligence/causation_scoring.py` (1090 LOC) — GRID Intelligence — Causal Connection Engine (scoring module).
- `intelligence/cds_tracker.py` (408 LOC) — GRID — CDS (Credit Default Swap) Tracker.
- `intelligence/chain_contagion.py` (727 LOC) — Chain contagion simulator.
- `intelligence/codebase_context.py` (307 LOC) — GRID Codebase Context — dynamic state injected into every LLM prompt.
- `intelligence/commodities_agriculture_network.py` (2766 LOC) — GRID Intelligence — Global Commodities & Agriculture Power Network Map.
- `intelligence/company_analyzer.py` (1079 LOC) — GRID Intelligence — Company Analyzer Pipeline.
- `intelligence/contagion_backtest.py` (380 LOC) — Contagion backtest scorer.
- `intelligence/context_provider.py` (252 LOC) — Context provider for LLM prompt injection.
- `intelligence/cross_lens.py` (713 LOC) — GRID Cross-Lens Correlation Detector.
- `intelligence/deal_detector.py` (861 LOC) — GRID Intelligence — M&A / Deal Detection Engine.
- `intelligence/deep_dive.py` (758 LOC) — GRID Intelligence — Thesis Deep Dive Engine.
- `intelligence/deep_graph.py` (1772 LOC) — GRID Intelligence — Deep Graph Traversal Engine.
- `intelligence/defense_contractors.py` (1183 LOC) — GRID Intelligence — US Defense Contractor Network Map.
- `intelligence/defi_protocols.py` (1266 LOC) — GRID Intelligence Platform — DeFi Protocol Analysis
- `intelligence/earnings_intel.py` (863 LOC) — GRID Intelligence — Earnings Analysis & Prediction System.
- `intelligence/earnings_transcript_analyzer.py` (685 LOC) — GRID Intelligence — Earnings Transcript Analyzer.
- `intelligence/energy_network.py` (2273 LOC) — GRID Intelligence — Global Energy Sector Power Network Map.
- `intelligence/entity_resolver.py` (1411 LOC) — GRID Intelligence — Entity Resolution Engine.
- `intelligence/export_intel.py` (439 LOC) — GRID Intelligence — Export Controls Analysis.
- `intelligence/freshness_guard.py` (155 LOC) — GRID — Feature Freshness Guard.
- `intelligence/fundamental_divergence.py` (570 LOC) — Fundamental-vs-price divergence detector.
- `intelligence/global_levers.py` (2258 LOC) — GRID Intelligence -- Global Lever Map: Hierarchical Model of World Economic Power.
- `intelligence/gov_intel.py` (297 LOC) — GRID Intelligence — Government Contract Analysis.
- `intelligence/holder_deal_overlap.py` (553 LOC) — Holder / deal overlap detector — "pre-positioning" cross-reference.
- `intelligence/hypothesis_engine.py` (2137 LOC) — GRID Intelligence — Hypothesis Discovery Engine.
- `intelligence/icij_actor_discovery.py` (288 LOC) — ICIJ Actor Discovery — automatically discover and add new actors from ICIJ data.
- `intelligence/icij_linker.py` (197 LOC) — ICIJ Linker — fuzzy-match ICIJ offshore entities against the actor network.
- `intelligence/image_gen.py` (410 LOC) — GRID — AI Image Generation via Gemini Imagen.
- `intelligence/influence_network.py` (923 LOC) — GRID Intelligence — Influence Network (Crown Jewel Analysis).
- `intelligence/insider_intel.py` (621 LOC) — GRID Intelligence — Insider Intel: Say vs Do Cross-Reference Engine.
- `intelligence/institutional_map.py` (1510 LOC) — GRID Intelligence -- Institutional Map: Private Credit, Hedge Funds & Pensions.
- `intelligence/legislative_intel.py` (481 LOC) — GRID Intelligence — Legislative Trading Detection.
- `intelligence/market_diary.py` (811 LOC) — GRID — Automated Daily Market Diary.
- `intelligence/media_network.py` (2172 LOC) — GRID Intelligence — Global Media, Entertainment & Information Control Network.
- `intelligence/milestone_tracker.py` (264 LOC) — Milestone Tracker — plot company milestones on a timeline, score execution.
- `intelligence/news_contagion_listener.py` (638 LOC) — News-driven contagion listener.
- `intelligence/news_impact.py` (978 LOC) — GRID News Impact Attribution Engine.
- `intelligence/news_intel.py` (559 LOC) — GRID Intelligence — News Intelligence & Narrative Analysis.
- `intelligence/news_momentum.py` (903 LOC) — GRID Intelligence — News Momentum Signal Engine.
- `intelligence/news_ticker_resolver.py` (407 LOC) — News ticker resolver — extract real ticker symbols from news title+content.
- `intelligence/obsidian_agent.py` (465 LOC) — Obsidian Agent — active intelligence loop for the vault.
- `intelligence/opsec.py` (459 LOC) — GRID Intelligence Operations Security (OPSEC) Module.
- `intelligence/pattern_engine.py` (910 LOC) — GRID Intelligence -- Pattern Detection Engine.
- `intelligence/pct_cogs_enrichment.py` (1751 LOC) — LLM-driven supplier-cost-concentration enrichment for ``supply_chain_edges``.
- `intelligence/pharma_network.py` (1271 LOC) — GRID Intelligence -- Big Pharma Power Network.
- `intelligence/pocket_lining.py` (286 LOC) — GRID Intelligence — Pocket-Lining Detection.
- `intelligence/post_query_scanner.py` (383 LOC) — GRID — Post-Query Data Gap Scanner.
- `intelligence/power_mapper.py` (91 LOC) — Power Mapper — unified power-mapping layer combining multiple sources.
- `intelligence/prediction_calibration.py` (523 LOC) — GRID Prediction Market Calibration Checker.
- `intelligence/rag.py` (1264 LOC) — GRID RAG (Retrieval-Augmented Generation) Intelligence System.
- `intelligence/ratio_percentiles.py` — DELETED 2026-04-11 (SYNTH-12); merged into `features/lab.py`.
- `intelligence/real_estate_network.py` (1792 LOC) — GRID Intelligence -- Global Real Estate & REIT Power Network.
- `intelligence/resolution_audit.py` (961 LOC) — GRID resolution audit supervisor.
- `intelligence/scheduler.py` (248 LOC) — GRID Intelligence Scheduler — background loop for periodic intelligence tasks.
- `intelligence/sec_filing_extractor.py` (716 LOC) — GRID Intelligence — SEC Filing Content Extractor.
- `intelligence/sector_health.py` (559 LOC) — Sector health composite score.
- `intelligence/sentiment_scorer.py` (1076 LOC) — GRID Intelligence — Deterministic Market Sentiment Scorer.
- `intelligence/signal_backlinker.py` (324 LOC) — Signal Backlinker — closes the loop between signals and the actor graph.
- `intelligence/signal_extractor.py` (322 LOC) — Signal Extractor — bridges raw_series → signal_data with actor attribution.
- `intelligence/signal_registry.py` (191 LOC) — GRID Intelligence — Signal Registry.
- `intelligence/source_trust_config.py` (148 LOC) — GRID Source Trust Configuration.
- `intelligence/supply_chain_edge_validator.py` (450 LOC) — GRID Supply-Chain Edge Validator.
- `intelligence/supply_chokepoints.py` (465 LOC) — GRID Intelligence — Supply Chain Chokepoint Scoring.
- `intelligence/swf_network.py` (1422 LOC) — Sovereign Wealth Fund Intelligence Network
- `intelligence/tech_monopoly_network.py` (2370 LOC) — GRID Intelligence — Tech Monopoly & Surveillance Capitalism Network Map.
- `intelligence/trend_tracker.py` (969 LOC) — GRID Trend Tracker — Divergence Analysis for Market Trends.
- `intelligence/wealth_tracker.py` (233 LOC) — GRID Intelligence — Wealth Tracking & Migration.
- `intelligence/whale_fingerprinter.py` (225 LOC) — Whale Fingerprinter — clusters anonymous options flow into behavioral profiles.

## Overlap candidates (for INDEX-3 synthesis audit)

Modules that may overlap with existing detectors, ranked by shared-table-read signal.
For each candidate, we list the top 5 other modules with highest semantic/IO overlap.

- **`intelligence/deep_graph.py`** (1772 LOC)
  - purpose: GRID Intelligence — Deep Graph Traversal Engine.
  - overlaps with `intelligence/event_sequence.py` (score 26, shared tables: __future__, analysis, collections, dataclasses, datetime, each)
  - overlaps with `intelligence/pattern_engine.py` (score 22, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/insider_intel.py` (score 20, shared tables: __future__, actors, collections, datetime, insider, loguru)
  - overlaps with `intelligence/global_levers.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `analysis/flow_aggregator.py` (score 20, shared tables: __future__, analysis, collections, datetime, each, intelligence)

- **`intelligence/pattern_engine.py`** (910 LOC)
  - purpose: GRID Intelligence -- Pattern Detection Engine.
  - overlaps with `intelligence/event_sequence.py` (score 24, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/deep_graph.py` (score 22, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/postmortem.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/sentiment_scorer.py` (score 20, shared tables: __future__, dataclasses, datetime, intelligence, loguru, options_daily_signals)
  - overlaps with `intelligence/global_levers.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)

- **`intelligence/sentiment_scorer.py`** (1076 LOC)
  - purpose: GRID Intelligence — Deterministic Market Sentiment Scorer.
  - overlaps with `intelligence/event_sequence.py` (score 24, shared tables: __future__, all, dataclasses, datetime, decision_journal, intelligence)
  - overlaps with `analysis/thesis_scorer.py` (score 24, shared tables: __future__, data, datetime, decision_journal, dollar_flows, intelligence)
  - overlaps with `intelligence/postmortem.py` (score 20, shared tables: __future__, dataclasses, datetime, decision_journal, intelligence, loguru)
  - overlaps with `intelligence/global_levers.py` (score 20, shared tables: __future__, dataclasses, datetime, intelligence, loguru, raw_series)
  - overlaps with `intelligence/trust_scorer.py` (score 20, shared tables: __future__, dataclasses, datetime, intelligence, loguru, options_daily_signals)

- **`intelligence/agent_arena.py`** (583 LOC)
  - purpose: GRID Intelligence — Agent Arena: 10 Competing Trading Analysts.
  - overlaps with `analysis/thesis_scorer.py` (score 23, shared tables: __future__, analysis, datetime, loguru, max, options_daily_signals)
  - overlaps with `intelligence/thesis_tracker.py` (score 21, shared tables: __future__, analysis, datetime, llm, loguru, options_daily_signals)
  - overlaps with `intelligence/event_sequence.py` (score 18, shared tables: __future__, analysis, datetime, each, loguru, options_daily_signals)
  - overlaps with `intelligence/trust_scorer.py` (score 18, shared tables: __future__, datetime, llm, loguru, options_daily_signals, scored)
  - overlaps with `analysis/money_flow.py` (score 18, shared tables: __future__, analysis, datetime, loguru, ollama, options_daily_signals)

- **`intelligence/global_levers.py`** (2258 LOC)
  - purpose: GRID Intelligence -- Global Lever Map: Hierarchical Model of World Economic Power.
  - overlaps with `intelligence/event_sequence.py` (score 22, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/sentiment_scorer.py` (score 20, shared tables: __future__, dataclasses, datetime, intelligence, loguru, raw_series)
  - overlaps with `intelligence/news_impact.py` (score 20, shared tables: __future__, dataclasses, datetime, loguru, news_articles, raw_series)
  - overlaps with `intelligence/deep_graph.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/pattern_engine.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)

- **`intelligence/business_news_parser.py`** (804 LOC)
  - purpose: GRID Intelligence — Business News Parser.
  - overlaps with `intelligence/deal_detector.py` (score 20, shared tables: __future__, dataclasses, datetime, loguru, news, news_articles)
  - overlaps with `intelligence/cross_reference.py` (score 14, shared tables: __future__, dataclasses, datetime, headline, loguru, sqlalchemy)
  - overlaps with `intelligence/event_sequence.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)
  - overlaps with `intelligence/news_momentum.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)
  - overlaps with `intelligence/global_levers.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)

- **`intelligence/causation_graph.py`** (1179 LOC)
  - purpose: GRID Intelligence — Causal Connection Engine (graph module).
  - overlaps with `intelligence/causation_scoring.py` (score 20, shared tables: __future__, datetime, earnings_calendar, intelligence, loguru, ollama)
  - overlaps with `analysis/money_flow.py` (score 20, shared tables: __future__, datetime, intelligence, loguru, ollama, price)
  - overlaps with `intelligence/event_sequence.py` (score 18, shared tables: __future__, datetime, earnings_calendar, intelligence, loguru, raw_series)
  - overlaps with `intelligence/sleuth.py` (score 18, shared tables: __future__, datetime, intelligence, llm, loguru, ollama)
  - overlaps with `intelligence/trust_scorer.py` (score 18, shared tables: __future__, datetime, intelligence, llm, loguru, raw_series)

- **`intelligence/causation_scoring.py`** (1090 LOC)
  - purpose: GRID Intelligence — Causal Connection Engine (scoring module).
  - overlaps with `intelligence/causation_graph.py` (score 20, shared tables: __future__, datetime, earnings_calendar, intelligence, loguru, ollama)
  - overlaps with `intelligence/event_sequence.py` (score 18, shared tables: __future__, datetime, earnings_calendar, intelligence, loguru, raw_series)
  - overlaps with `intelligence/sleuth.py` (score 18, shared tables: __future__, conditions, datetime, intelligence, loguru, ollama)
  - overlaps with `intelligence/market_diary.py` (score 18, shared tables: __future__, datetime, intelligence, loguru, ollama, raw_series)
  - overlaps with `analysis/money_flow.py` (score 18, shared tables: __future__, datetime, intelligence, loguru, ollama, raw_series)

- **`intelligence/chain_contagion.py`** (727 LOC)
  - purpose: Chain contagion simulator.
  - overlaps with `intelligence/trust_scorer.py` (score 20, shared tables: __future__, capital_flows, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/postmortem.py` (score 18, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/event_sequence.py` (score 16, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/sector_health.py` (score 16, shared tables: __future__, capital_flows, dataclasses, datetime, loguru, sqlalchemy)
  - overlaps with `intelligence/sleuth.py` (score 16, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)

- **`intelligence/company_analyzer.py`** (1079 LOC)
  - purpose: GRID Intelligence — Company Analyzer Pipeline.
  - overlaps with `intelligence/postmortem.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, llm)
  - overlaps with `intelligence/event_sequence.py` (score 20, shared tables: __future__, all, collections, dataclasses, datetime, intelligence)
  - overlaps with `intelligence/sleuth.py` (score 20, shared tables: __future__, all, collections, dataclasses, datetime, intelligence)
  - overlaps with `intelligence/forensics.py` (score 20, shared tables: __future__, collections, dataclasses, datetime, intelligence, llm)
  - overlaps with `intelligence/thesis_tracker.py` (score 18, shared tables: __future__, dataclasses, datetime, intelligence, llm, loguru)

- **`intelligence/deal_detector.py`** (861 LOC)
  - purpose: GRID Intelligence — M&A / Deal Detection Engine.
  - overlaps with `intelligence/business_news_parser.py` (score 20, shared tables: __future__, dataclasses, datetime, loguru, news, news_articles)
  - overlaps with `intelligence/event_sequence.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)
  - overlaps with `intelligence/news_momentum.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)
  - overlaps with `intelligence/global_levers.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)
  - overlaps with `intelligence/news_impact.py` (score 14, shared tables: __future__, dataclasses, datetime, loguru, news_articles, sqlalchemy)

- **`intelligence/entity_resolver.py`** (1411 LOC)
  - purpose: GRID Intelligence — Entity Resolution Engine.
  - overlaps with `intelligence/hypothesis_engine.py` (score 20, shared tables: __future__, analytical_snapshots, collections, dataclasses, datetime, loguru)
  - overlaps with `intelligence/deep_graph.py` (score 18, shared tables: __future__, actors, any, collections, dataclasses, datetime)
  - overlaps with `intelligence/postmortem.py` (score 16, shared tables: __future__, collections, dataclasses, datetime, loguru, oracle_predictions)
  - overlaps with `intelligence/actor_discovery.py` (score 16, shared tables: __future__, actors, datetime, loguru, pathlib, sqlalchemy)
  - overlaps with `intelligence/rag.py` (score 16, shared tables: __future__, actors, analytical_snapshots, collections, loguru, oracle_predictions)

- **`intelligence/hypothesis_engine.py`** (2137 LOC)
  - purpose: GRID Intelligence — Hypothesis Discovery Engine.
  - overlaps with `intelligence/entity_resolver.py` (score 20, shared tables: __future__, analytical_snapshots, collections, dataclasses, datetime, loguru)
  - overlaps with `intelligence/postmortem.py` (score 18, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/event_sequence.py` (score 16, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/sleuth.py` (score 16, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/chain_contagion.py` (score 16, shared tables: __future__, collections, dataclasses, datetime, intelligence, loguru)

- **`intelligence/icij_actor_discovery.py`** (288 LOC)
  - purpose: ICIJ Actor Discovery — automatically discover and add new actors from ICIJ data.
  - overlaps with `intelligence/icij_linker.py` (score 20, shared tables: __future__, actors, dataclasses, icij_actor_matches, icij_entities, icij_officers)
  - overlaps with `intelligence/actor_discovery.py` (score 19, shared tables: __future__, actor_network, actors, datetime, icij, loguru)
  - overlaps with `intelligence/power_mapper.py` (score 18, shared tables: __future__, dataclasses, icij, icij_entities, icij_officers, icij_relationships)
  - overlaps with `intelligence/actor_researcher.py` (score 17, shared tables: __future__, actors, datetime, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/actor_ingest.py` (score 17, shared tables: __future__, actors, datetime, intelligence, loguru, sqlalchemy)

- **`intelligence/icij_linker.py`** (197 LOC)
  - purpose: ICIJ Linker — fuzzy-match ICIJ offshore entities against the actor network.
  - overlaps with `intelligence/icij_actor_discovery.py` (score 20, shared tables: __future__, actors, dataclasses, icij_actor_matches, icij_entities, icij_officers)
  - overlaps with `intelligence/deep_graph.py` (score 14, shared tables: __future__, actors, dataclasses, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/power_mapper.py` (score 14, shared tables: __future__, dataclasses, icij_entities, icij_officers, loguru, sqlalchemy)
  - overlaps with `intelligence/postmortem.py` (score 12, shared tables: __future__, dataclasses, intelligence, loguru, sqlalchemy, typing)
  - overlaps with `intelligence/cross_reference.py` (score 12, shared tables: __future__, dataclasses, intelligence, loguru, sqlalchemy, typing)

- **`intelligence/insider_intel.py`** (621 LOC)
  - purpose: GRID Intelligence — Insider Intel: Say vs Do Cross-Reference Engine.
  - overlaps with `intelligence/deep_graph.py` (score 20, shared tables: __future__, actors, collections, datetime, insider, loguru)
  - overlaps with `intelligence/actor_discovery.py` (score 16, shared tables: __future__, actors, datetime, loguru, raw_series, signal_sources)
  - overlaps with `intelligence/event_sequence.py` (score 16, shared tables: __future__, collections, datetime, loguru, raw_series, signal_sources)
  - overlaps with `intelligence/pocket_lining.py` (score 16, shared tables: __future__, collections, datetime, loguru, raw_series, signal_sources)
  - overlaps with `intelligence/global_levers.py` (score 16, shared tables: __future__, collections, datetime, loguru, raw_series, signal_sources)

- **`intelligence/market_diary.py`** (811 LOC)
  - purpose: GRID — Automated Daily Market Diary.
  - overlaps with `intelligence/event_sequence.py` (score 20, shared tables: __future__, analysis, datetime, decision_journal, intelligence, loguru)
  - overlaps with `analysis/money_flow.py` (score 20, shared tables: __future__, analysis, datetime, intelligence, loguru, ollama)
  - overlaps with `analysis/thesis_scorer.py` (score 20, shared tables: __future__, analysis, datetime, decision_journal, intelligence, loguru)
  - overlaps with `intelligence/causation_graph.py` (score 18, shared tables: __future__, datetime, intelligence, loguru, ollama, raw_series)
  - overlaps with `intelligence/sentiment_scorer.py` (score 18, shared tables: __future__, datetime, decision_journal, intelligence, loguru, raw_series)

- **`intelligence/news_impact.py`** (978 LOC)
  - purpose: GRID News Impact Attribution Engine.
  - overlaps with `intelligence/event_sequence.py` (score 20, shared tables: __future__, dataclasses, datetime, loguru, news_articles, options_daily_signals)
  - overlaps with `intelligence/global_levers.py` (score 20, shared tables: __future__, dataclasses, datetime, loguru, news_articles, raw_series)
  - overlaps with `intelligence/trust_scorer.py` (score 20, shared tables: __future__, dataclasses, datetime, llm, loguru, options_daily_signals)
  - overlaps with `analysis/thesis_scorer.py` (score 20, shared tables: __future__, datetime, loguru, news_articles, options_daily_signals, raw_series)
  - overlaps with `intelligence/postmortem.py` (score 18, shared tables: __future__, dataclasses, datetime, llm, loguru, options_daily_signals)

- **`intelligence/actor_discovery.py`** (3533 LOC)
  - purpose: GRID Intelligence — Automated Actor Discovery & Enrichment (250K+ Scale).
  - overlaps with `intelligence/actor_researcher.py` (score 19, shared tables: __future__, actor_connections, actors, datetime, loguru, raw_series)
  - overlaps with `intelligence/icij_actor_discovery.py` (score 19, shared tables: __future__, actor_network, actors, datetime, icij, loguru)
  - overlaps with `intelligence/signal_backlinker.py` (score 18, shared tables: __future__, actor_connections, actors, datetime, loguru, sqlalchemy)
  - overlaps with `intelligence/sentiment_scorer.py` (score 18, shared tables: __future__, data, datetime, existing, loguru, raw_series)
  - overlaps with `intelligence/deep_graph.py` (score 18, shared tables: __future__, actors, congressional, datetime, loguru, raw_series)

- **`intelligence/actor_researcher.py`** (416 LOC)
  - purpose: Actor Researcher — local LLM agent that continuously enriches actor profiles.
  - overlaps with `intelligence/actor_discovery.py` (score 19, shared tables: __future__, actor_connections, actors, datetime, loguru, raw_series)
  - overlaps with `intelligence/icij_actor_discovery.py` (score 17, shared tables: __future__, actors, datetime, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/actor_ingest.py` (score 17, shared tables: __future__, actors, datetime, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/postmortem.py` (score 16, shared tables: __future__, datetime, intelligence, llm, loguru, raw_series)
  - overlaps with `intelligence/sleuth.py` (score 16, shared tables: __future__, datetime, evidence, intelligence, llm, loguru)

- **`intelligence/supply_chain_edge_validator.py`** (450 LOC)
  - purpose: GRID Supply-Chain Edge Validator.
  - overlaps with `intelligence/postmortem.py` (score 19, shared tables: __future__, dataclasses, datetime, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/pct_cogs_enrichment.py` (score 17, shared tables: __future__, dataclasses, datetime, loguru, sqlalchemy, supply_chain_edges)
  - overlaps with `intelligence/chain_contagion.py` (score 16, shared tables: __future__, dataclasses, datetime, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/trust_scorer.py` (score 16, shared tables: __future__, dataclasses, datetime, intelligence, loguru, sqlalchemy)
  - overlaps with `intelligence/supply_chokepoints.py` (score 15, shared tables: __future__, dataclasses, loguru, sqlalchemy, supply_chain_edges, typing)

- **`intelligence/audio_briefing.py`** (769 LOC)
  - purpose: GRID -- Daily Intelligence Audio Briefing Pipeline.
  - overlaps with `intelligence/image_gen.py` (score 18, shared tables: __future__, analysis, dataclasses, datetime, google, grid)
  - overlaps with `intelligence/deep_dive.py` (score 18, shared tables: __future__, analysis, dataclasses, datetime, google, intelligence)
  - overlaps with `intelligence/event_sequence.py` (score 16, shared tables: __future__, all, analysis, dataclasses, datetime, intelligence)
  - overlaps with `intelligence/sleuth.py` (score 14, shared tables: __future__, all, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/thesis_tracker.py` (score 14, shared tables: __future__, analysis, dataclasses, datetime, intelligence, loguru)

- **`intelligence/deep_dive.py`** (758 LOC)
  - purpose: GRID Intelligence — Thesis Deep Dive Engine.
  - overlaps with `intelligence/audio_briefing.py` (score 18, shared tables: __future__, analysis, dataclasses, datetime, google, intelligence)
  - overlaps with `intelligence/thesis_tracker.py` (score 18, shared tables: __future__, analysis, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/event_sequence.py` (score 16, shared tables: __future__, analysis, dataclasses, datetime, intelligence, loguru)
  - overlaps with `intelligence/sleuth.py` (score 16, shared tables: __future__, dataclasses, datetime, intelligence, investigation_leads, loguru)
  - overlaps with `intelligence/deep_graph.py` (score 16, shared tables: __future__, analysis, dataclasses, datetime, intelligence, loguru)

- **`intelligence/earnings_intel.py`** (863 LOC)
  - purpose: GRID Intelligence — Earnings Analysis & Prediction System.
  - overlaps with `intelligence/postmortem.py` (score 18, shared tables: __future__, capital_flow_snapshots, dataclasses, datetime, loguru, options_daily_signals)
  - overlaps with `intelligence/event_sequence.py` (score 18, shared tables: __future__, dataclasses, datetime, earnings_calendar, loguru, options_daily_signals)
  - overlaps with `intelligence/sentiment_scorer.py` (score 18, shared tables: __future__, dataclasses, datetime, loguru, options, options_daily_signals)
  - overlaps with `intelligence/thesis_tracker.py` (score 16, shared tables: __future__, dataclasses, datetime, loguru, options_daily_signals, raw_series)
  - overlaps with `intelligence/forensics.py` (score 16, shared tables: __future__, dataclasses, datetime, loguru, options_daily_signals, raw_series)

- **`intelligence/freshness_guard.py`** (155 LOC)
  - purpose: GRID — Feature Freshness Guard.
  - overlaps with `intelligence/cross_reference.py` (score 18, shared tables: __future__, dataclasses, datetime, feature_registry, intelligence, loguru)
  - overlaps with `intelligence/sleuth.py` (score 18, shared tables: __future__, dataclasses, datetime, feature_registry, intelligence, loguru)
  - overlaps with `intelligence/forensics.py` (score 18, shared tables: __future__, dataclasses, datetime, feature_registry, intelligence, loguru)
  - overlaps with `intelligence/attention_anomaly.py` (score 16, shared tables: __future__, dataclasses, datetime, feature_registry, loguru, resolved_series)
  - overlaps with `intelligence/resolution_audit.py` (score 16, shared tables: __future__, dataclasses, datetime, feature_registry, loguru, resolved_series)


---
End of inventory. 649 modules cataloged.
## Appendix — drift adds (auto-merged 2026-04-13)

#### `contracts/handlers/calibration.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `contracts/handlers/edges.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `contracts/handlers/journal.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `contracts/handlers/oracle_signals.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `contracts/handlers/oracle_weights.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `contracts/handlers/trade_outcomes.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `contracts/handlers/trust.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `ingestion/altdata/wikidata_entity.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `ingestion/altdata/wikipedia_text.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `intelligence/company_financial_rollups.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `intelligence/sector_networks/__init__.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `intelligence/sector_networks/loader.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `oracle/psi_model.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/__init__.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/adapters/__init__.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/adapters/base.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/adapters/deribit.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/confidence.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/exposures.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/pipeline.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/dealer_flow/schemas.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/greeks/__init__.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._

#### `physics/greeks/black_scholes.py` — N/A LOC

_Auto-added during MODULE_INVENTORY drift reconciliation. Run scripts/rebuild_module_inventory.py for full docstring extraction._


#### `intelligence/actor_trust_cog.py` — N/A LOC

_INTEL-2 trust-or-cog classifier for lever_pullers actors._


#### `intelligence/catalyst_aggregator.py` — N/A LOC

_ALPHA-4 unified catalyst calendar (earnings + trials + FOMC + OPEX) with proximity scoring._


#### `oracle/disagreement.py` — N/A LOC

_ALPHA-10 ensemble disagreement meta-feature (entropy + confidence variance)._


#### `intelligence/liquidity_regime.py` — N/A LOC

_ALPHA-5 five-state liquidity regime classifier over Fed net-liquidity stack._


#### `oracle/uncertainty.py` — N/A LOC

_ALPHA-11 confidence intervals via t/normal critical values over per-head variance._


#### `intelligence/market_implied_prob.py` — N/A LOC

_ALPHA-8 market-implied probability comparator (closed-form options IV via Black-Scholes)._


#### `intelligence/shapley_attribution.py` — N/A LOC

_ALPHA-9 Shapley attribution per prediction with Herfindahl fragility multiplier._


#### `intelligence/financial_conditions_index.py` — N/A LOC

_CAT-124 Financial Conditions Index (6-component composite z-score)._


#### `oracle/regime_router.py` — N/A LOC

_ALPHA-13 per-regime sub-oracle routing (subagent in flight)._


#### `intelligence/consensus_crowdedness.py` — N/A LOC

_CAT-182 consensus crowdedness detector (5-component composite with directional penalty)._


#### `intelligence/bayesian_evidence.py` — N/A LOC

_CAT-178 Bayesian evidence combiner with correlation-adjusted LLR accumulation._


#### `intelligence/credit_event_probability.py` — N/A LOC

_CAT-162 per-name credit event probability via Merton + credit spread + rating trajectory._


#### `intelligence/risk_factor_novelty.py` — N/A LOC

_CAT-152 10-K/10-Q Risk Factors novelty detector via sentence-level Jaccard diff._


#### `intelligence/thesis_invalidation_monitor.py` — N/A LOC

_CAT-190 automatic thesis invalidation monitor (price_level + event + signal_flip)._


#### `intelligence/hmm_regime_transitions.py` — N/A LOC

_CAT-121 Markov transition matrix + multi-step forecast over liquidity regime states._


#### `analysis/transfer_entropy.py` — N/A LOC

_CAT-111 unsupervised lead-lag discovery via transfer entropy._

#### `analysis/lead_lag_backtest.py` — N/A LOC

_CAT-115 walk-forward lead-lag validation framework._


#### `ingestion/altdata/h8_bank_balance.py` — N/A LOC

_CAT-27 H.8 bank balance sheet weekly FRED puller (8 core series)._


#### `intelligence/cot_extremes.py` — N/A LOC

_CAT-35 CFTC COT extremes + percentile z-scores over cftc_cot data._


#### `ingestion/altdata/mmf_composition.py` — N/A LOC

_CAT-30 money market fund composition weekly puller (FRED — 4 series)._


#### `ingestion/altdata/treasury_auction.py` — N/A LOC

_CAT-25 Treasury auction puller (bid-to-cover, stop yield, bidder split)._


#### `ingestion/altdata/warn_layoffs.py` — N/A LOC

_CAT-71 WARN Act mass layoff filings puller + query helpers._


#### `ingestion/altdata/freight_cass_ata.py` — N/A LOC

_CAT-81 Cass Freight + ATA Truck Tonnage monthly puller (FRED — 4 series)._


#### `ingestion/altdata/wage_tracker.py` — N/A LOC

_CAT-49 Atlanta Fed Wage Growth Tracker monthly puller (FRED — 4 series)._


#### `intelligence/eight_k_clustering.py` — N/A LOC

_CAT-61 8-K clustering + item category severity scoring._

