---
source: /Users/anikdang/grid_obsidian/Architecture/Module-Sizes.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
---
tags: [architecture, code-quality, metrics]
created: 2026-04-04
---

# Module Sizes — Line Count Report

Total Python codebase: **282,292 lines** across all `.py` files.

Related: [[Frontend-Views]], [[All-Scripts]], [[Config-Map]]

---

## 80 Largest Files

| # | File | Lines | Flag |
|---|------|-------|------|
| 1 | `intelligence/actors/seed_data.py` | 5,594 | CRITICAL - data file, acceptable |
| 2 | `intelligence/actor_discovery.py` | 3,327 | CRITICAL - needs splitting |
| 3 | `store/astrogrid.py` | 2,795 | CRITICAL - needs splitting |
| 4 | `intelligence/commodities_agriculture_network.py` | 2,765 | CRITICAL |
| 5 | `intelligence/causation.py` | 2,428 | CRITICAL |
| 6 | `intelligence/tech_monopoly_network.py` | 2,369 | CRITICAL |
| 7 | `orchestration/llm_taskqueue.py` | 2,282 | CRITICAL |
| 8 | `intelligence/energy_network.py` | 2,272 | CRITICAL |
| 9 | `intelligence/media_network.py` | 2,171 | CRITICAL |
| 10 | `api/routers/intel.py` | 2,158 | CRITICAL - route file too large |
| 11 | `scripts/hermes_operator.py` | 2,142 | CRITICAL |
| 12 | `intelligence/global_levers.py` | 1,868 | CRITICAL |
| 13 | `api/routers/astrogrid_helpers.py` | 1,818 | CRITICAL |
| 14 | `intelligence/cross_reference.py` | 1,799 | CRITICAL |
| 15 | `intelligence/real_estate_network.py` | 1,791 | CRITICAL |
| 16 | `intelligence/deep_graph.py` | 1,771 | CRITICAL |
| 17 | `analysis/money_flow.py` | 1,771 | CRITICAL |
| 18 | `analysis/flow_thesis.py` | 1,700 | CRITICAL |
| 19 | `intelligence/banking_network.py` | 1,691 | CRITICAL |
| 20 | `api/routers/flows.py` | 1,687 | CRITICAL |
| 21 | `api/routers/system.py` | 1,656 | CRITICAL |
| 22 | `intelligence/institutional_map.py` | 1,509 | CRITICAL |
| 23 | `subnet/distributed_compute.py` | 1,445 | CRITICAL |
| 24 | `.claude/skills/.../instinct-cli.py` | 1,426 | Tool file, acceptable |
| 25 | `intelligence/swf_network.py` | 1,421 | CRITICAL |
| 26 | `intelligence/entity_resolver.py` | 1,410 | CRITICAL |
| 27 | `intelligence/lever_pullers.py` | 1,376 | CRITICAL |
| 28 | `intelligence/postmortem.py` | 1,358 | CRITICAL |
| 29 | `intelligence/hypothesis_engine.py` | 1,350 | CRITICAL |
| 30 | `trading/options_recommender.py` | 1,321 | CRITICAL |
| 31 | `analysis/sector_map.py` | 1,319 | CRITICAL |
| 32 | `analysis/vol_surface.py` | 1,272 | CRITICAL |
| 33 | `intelligence/pharma_network.py` | 1,270 | CRITICAL |
| 34 | `intelligence/defi_protocols.py` | 1,265 | CRITICAL |
| 35 | `scripts/export_astrogrid_local_data.py` | 1,243 | CRITICAL |
| 36 | `intelligence/sleuth.py` | 1,237 | CRITICAL |
| 37 | `oracle/engine.py` | 1,223 | CRITICAL |
| 38 | `analysis/thesis_scorer.py` | 1,212 | CRITICAL |
| 39 | `intelligence/rag.py` | 1,206 | CRITICAL |
| 40 | `analysis/market_universe.py` | 1,184 | CRITICAL |
| 41 | `intelligence/defense_contractors.py` | 1,182 | CRITICAL |
| 42 | `intelligence/trust_scorer.py` | 1,147 | CRITICAL |
| 43 | `analysis/flow_aggregator.py` | 1,147 | CRITICAL |
| 44 | `scripts/parse_datasets.py` | 1,131 | CRITICAL |
| 45 | `api/routers/intelligence_risk.py` | 1,085 | CRITICAL |
| 46 | `ingestion/scheduler.py` | 1,083 | CRITICAL |
| 47 | `intelligence/dollar_flows.py` | 1,081 | CRITICAL |
| 48 | `mcp_server.py` | 1,080 | CRITICAL |
| 49 | `intelligence/sentiment_scorer.py` | 1,075 | CRITICAL |
| 50 | `tests/test_resolver.py` | 1,073 | Test file, acceptable |
| 51 | `intelligence/company_analyzer.py` | 1,072 | CRITICAL |
| 52 | `ingestion/altdata/legislation.py` | 1,049 | CRITICAL |
| 53 | `normalization/entity_map.py` | 1,046 | CRITICAL |
| 54 | `analysis/ephemeris.py` | 1,035 | CRITICAL |
| 55 | `features/importance.py` | 1,012 | CRITICAL |
| 56 | `intelligence/thesis_tracker.py` | 1,001 | CRITICAL |
| 57 | `intelligence/event_sequence.py` | 998 | CRITICAL |
| 58 | `api/routers/chat.py` | 996 | CRITICAL |
| 59 | `api/routers/derivatives.py` | 994 | CRITICAL |
| 60 | `.claude/skills/.../test_parse_instinct.py` | 984 | Tool file |
| 61 | `trading/strategy151.py` | 980 | CRITICAL |
| 62 | `intelligence/news_impact.py` | 977 | CRITICAL |
| 63 | `intelligence/trend_tracker.py` | 968 | CRITICAL |
| 64 | `intelligence/resolution_audit.py` | 960 | CRITICAL |
| 65 | `intelligence/forensics.py` | 955 | CRITICAL |
| 66 | `scripts/fill_missing_features.py` | 950 | CRITICAL |
| 67 | `intelligence/source_audit.py` | 939 | CRITICAL |
| 68 | `tests/test_features_lab.py` | 937 | Test file |
| 69 | `intelligence/influence_network.py` | 922 | CRITICAL |
| 70 | `intelligence/pattern_engine.py` | 907 | CRITICAL |
| 71 | `ingestion/altdata/news_scraper.py` | 903 | CRITICAL |
| 72 | `ingestion/altdata/fara.py` | 901 | CRITICAL |
| 73 | `ingestion/altdata/offshore_leaks.py` | 892 | CRITICAL |
| 74 | `analysis/capital_flows.py` | 892 | CRITICAL |
| 75 | `intelligence/earnings_intel.py` | 856 | CRITICAL |
| 76 | `ingestion/altdata/institutional_flows.py` | 851 | CRITICAL |
| 77 | `physics/verify.py` | 849 | CRITICAL |
| 78 | `ingestion/altdata/supply_chain.py` | 847 | CRITICAL |
| 79 | `tests/test_astrogrid_predictions.py` | 833 | Test file |
| 80 | `ingestion/altdata/foia_cables.py` | ~830 | CRITICAL |

---

## Hotspot Analysis

**Worst offenders by directory:**
- `intelligence/`: 30+ files over 800 lines. The actor network files alone are 5,500+ lines. This is the largest module cluster.
- `api/routers/`: 6 files over 800 lines (intel, flows, system, astrogrid_helpers, chat, derivatives)
- `analysis/`: 6 files over 800 lines
- `ingestion/altdata/`: 5 files over 800 lines

**Files that should be split first:**
1. `intelligence/actors/seed_data.py` (5,594) — data file, could be JSON/YAML
2. `intelligence/actor_discovery.py` (3,327) — extract network-specific discovery
3. `store/astrogrid.py` (2,795) — extract query builders, schema, helpers
4. `intelligence/causation.py` (2,428) — extract chain builder from scorer
5. `orchestration/llm_taskqueue.py` (2,282) — extract dispatchers from queue logic

**Acceptable large files:**
- Test files (>800 lines is fine for comprehensive test suites)
- Seed data files (static data, not logic)
- `.claude/skills/` tooling files
