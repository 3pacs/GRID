---
source: /Users/anikdang/grid_obsidian/Architecture/Intelligence-Layer.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# Intelligence Layer

89 files across 4 subdirectories providing deep analytical intelligence on actors, markets, networks, and patterns.

## Core Intelligence Modules (60+ standalone analyzers)

Each module typically: queries DB -> analyzes -> returns structured intelligence -> optionally saves to `analytical_snapshots`.

### Actor & Network Intelligence
| Module | What it does |
|--------|-------------|
| [[Intel-Actor-Network]] | Maps relationships between financial actors (politicians, executives, funds) |
| `actor_discovery.py` | Discovers new actors from data patterns |
| `actor_network.py` | Graph-based actor relationship mapping |
| `influence_network.py` | Tracks influence flows between entities |
| `institutional_map.py` | Maps institutional ownership, pension flows, fee structures |
| `media_network.py` | Media ownership and narrative control mapping |
| `wealth_tracker.py` | Tracks wealth movements of key actors |
| `pocket_lining.py` | Detects self-dealing patterns (insider + legislative) |

### Market Intelligence
| Module | What it does |
|--------|-------------|
| `company_analyzer.py` | Deep company analysis with multi-source cross-reference |
| `deep_dive.py` | Thesis deep dive engine — full investigation on a ticker |
| `deep_graph.py` | Graph-based company relationship exploration |
| `earnings_intel.py` | Earnings analysis with surprise tracking |
| `market_diary.py` | Daily market diary with regime context |
| `pattern_engine.py` | Recognizes recurring market patterns with hit rates |
| `trend_tracker.py` | Tracks emerging and fading market trends |

### Forensic & Causal Intelligence
| Module | What it does |
|--------|-------------|
| [[Intel-Forensics]] | Financial forensics per ticker (anomaly detection) |
| `causation.py` | Causal chain analysis — traces cause-effect relationships |
| `cross_reference.py` | "Lie detector" — flags divergences between what actors say vs do |
| `forensics.py` | Deep forensic analysis with suspicious activity detection |
| `opsec.py` | Operational security intelligence |

### Government & Geopolitical
| Module | What it does |
|--------|-------------|
| `gov_intel.py` | Government contract intelligence |
| `legislative_intel.py` | Legislation tracking with trading alerts |
| `dollar_flows.py` | Dollar flow tracking across sectors |
| `export_intel.py` | Export control impact analysis |
| `defense_contractors.py` | Defense contractor network mapping |

### Sector Network Modules
| Module | What it does |
|--------|-------------|
| `banking_network.py` | Banking sector relationship network |
| `energy_network.py` | Energy sector flow mapping |
| `pharma_network.py` | Pharma sector network (trials, patents, lobbying) |
| `real_estate_network.py` | Real estate sector intelligence |
| `tech_monopoly_network.py` | Tech monopoly power mapping |
| `commodities_agriculture_network.py` | Agriculture + commodity flows |
| `swf_network.py` | Sovereign wealth fund tracking |
| `defi_protocols.py` | DeFi protocol analysis |

### Prediction & Scoring
| Module | What it does |
|--------|-------------|
| `prediction_calibration.py` | Scores prediction accuracy over time |
| `hypothesis_engine.py` | Generates and tracks market hypotheses |
| `thesis_tracker.py` | Tracks thesis accuracy and evolution |
| `trust_scorer.py` | Multi-source trust scoring with convergence signals |
| `sentiment_scorer.py` | Aggregated sentiment from multiple sources |

### Supporting Infrastructure
| Module | What it does |
|--------|-------------|
| `signal_registry.py` | Central registry of all intelligence signals |
| `entity_resolver.py` | Intelligence-layer entity resolution |
| `source_audit.py` | Audits source reliability and freshness |
| `source_trust_config.py` | Per-source trust weights |
| `freshness_guard.py` | Flags stale data sources |
| `resolution_audit.py` | Audits entity resolution quality |
| `codebase_context.py` | Self-aware codebase description for LLM context |

### Media & Output
| Module | What it does |
|--------|-------------|
| `audio_briefing.py` | Daily intelligence audio briefing pipeline |
| `image_gen.py` | AI image generation via Gemini Imagen |
| `news_impact.py` | News impact scoring on price |
| `news_intel.py` | News intelligence with narrative shift detection |
| `rag.py` | Retrieval-augmented generation for intelligence queries |
| `post_query_scanner.py` | Post-query enrichment scanner |
| `postmortem.py` | Trade/prediction postmortem analysis |

## Intelligence Adapters (`intelligence/adapters/`, 19 files)

Bridge between intelligence modules and the [[Signal-Registry]]. Each adapter wraps an intelligence module and exposes its outputs as standardized signals.

| Adapter | Source Module | Signal Type |
|---------|--------------|-------------|
| `cross_reference_adapter` | `cross_reference.py` | Divergence signals (lie detector) |
| `dollar_flows_adapter` | `dollar_flows.py` | Net flow direction + magnitude |
| `earnings_adapter` | `earnings_intel.py` | Earnings surprise + upcoming |
| `feature_adapter` | Feature store | Z-score signals from resolved_series |
| `flow_thesis_adapter` | Flow thesis | Capital flow thesis signals |
| `forensics_adapter` | `forensics.py` | Warning count + directional |
| `lever_pullers_adapter` | `lever_pullers.py` | Per-ticker from actor behavior |
| `news_adapter` | `news_intel.py` | Sentiment momentum + volume |
| `pattern_adapter` | `pattern_engine.py` | Active patterns with hit rates |
| `sector_network_adapter` | Sector networks | Actor density + concentration |
| `sleuth_adapter` | `sleuth.py` | Active investigation leads |
| `thesis_tracker_adapter` | `thesis_tracker.py` | Thesis direction + accuracy |
| `trust_scorer_adapter` | `trust_scorer.py` | Convergence + per-source trust |

## Intelligence Actors System (`intelligence/actors/`, 7 files)

Dedicated actor tracking subsystem:
- `models.py` — Actor data models
- `db.py` — Actor database operations (creates `actors` and `actor_connections` tables)
- `ingestion.py` — Actor data ingestion from multiple sources
- `graph.py` — Actor relationship graph construction
- `analysis.py` — Actor behavior analysis
- `seed_data.py` — Initial actor seed data

## Regime Detection (`intelligence/regime/`, 5 files)

Market regime classification system:
- `classifier.py` — Regime state classifier
- `state_vector.py` — Constructs regime state vectors from features
- `episode_matcher.py` — Matches current regime to historical episodes
- `forecast.py` — Regime transition forecasting

## Dependencies

- Reads from: `resolved_series`, `feature_registry`, `raw_series`, `actors`, `actor_connections`, `analytical_snapshots`
- Writes to: `analytical_snapshots`, `actors`, `actor_connections`
- Used by: [[API-Layer]] (intelligence routers), [[Agents-System]], [[Oracle-Engine]]
- See also: [[_Intelligence Index]] for the full module-by-module index, [[Knowledge-System]] for RAG and knowledge tree

## Module Cross-References (code-verified)

- [[Sleuth]]
- [[Forensics]]
- [[Causation]]
- [[Trust Scorer]]
- [[Cross Reference]]
- [[Deep Graph]]
- [[Deep Dive]]
- [[Influence Network]]
- [[Company Analyzer]]
- [[Thesis Tracker]]
- [[Hypothesis Engine]]
- [[Signal Registry]]
- [[Lever Pullers]]
- [[Actor Network]]
- [[Actor Discovery]]
- [[Institutional Map]]
- [[Wealth Tracker]]
- [[Pocket Lining]]
- [[Sentiment Scorer]]
- [[Dollar Flows]]
- [[News Intel]]
- [[News Impact]]
- [[Trend Tracker]]
- [[Market Diary]]
- [[Earnings Intel]]
- [[Postmortem]]
- [[RAG]]
- [[Audio Briefing]]
- [[Image Gen]]
- [[Freshness Guard]]
- [[Entity Resolver]]
- [[Event Sequence]]
- [[Pattern Engine]]
- [[Prediction Calibration]]
- [[Gov Intel]]
- [[Legislative Intel]]
- [[Export Intel]]
- [[OPSEC]]
- [[Regime Subsystem]]
