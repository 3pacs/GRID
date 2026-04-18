# Connection Mapping Spider — Design Spec

**Date:** 2026-04-07
**Status:** Approved
**Goal:** Build a continuous connection mapping spider that discovers and maps actor relationships up to 11 degrees of separation, keeps the full graph in RAM (512GB server), and renders confidence-colored connections in the actor network visualization.

---

## 1. Architecture Overview

The system has four layers:

1. **In-Memory Actor Graph** — The full actor dataset (1.6M → 5M+) lives in RAM as Python dicts. BFS, shortest path, community detection, and subgraph extraction all run in microseconds. Estimated memory: ~1.5 GB per million actors with full adjacency (~8 GB for 5M actors). Trivial for 512 GB.

2. **Spider Daemon** — A continuous background service (`grid-spider` systemd unit) that walks the graph, discovers new connections from 13 data sources, resolves entities, and enriches actors via LLM. Runs 24/7. Processes ~50-200 actors/hour. When all actors are explored to degree 11, re-enriches stale actors (oldest first).

3. **Data Sources** — 13 sources across 4 confidence tiers feed the spider. Existing pullers (SEC, FARA, FEC, ICIJ, etc.) plus new adapters (Wikidata, OpenCorporates, GDELT co-occurrence, Google News, LLM speculation, operator input).

4. **PostgreSQL** — Persistence layer. `actors`, `actor_connections`, `spider_queue`, `spider_runs` tables. The in-memory graph syncs from Postgres on startup and receives atomic updates from the spider daemon.

## 2. Phased Rollout

**Phase 1 — Evidence Crawler (first):** BFS from 489 seed actors. Every new actor must have a source citation. LLM enriches each node. Builds the trust backbone. Target: 50K-100K high-confidence actors.

**Phase 2 — Bulk Skeleton (second):** Import ICIJ (785K entities), SEC 13F filers, FEC donors, OpenCorporates officers. Entity resolution matches against Phase 1 actors. Fill degrees 4-11. Target: 1M-5M actors.

The spider never stops after Phase 2 — it continuously re-enriches, discovers rabbit holes, and processes operator leads.

## 3. Spider Daemon — 4-Stage Pipeline

### Stage 1: Priority Queue

Ranks all unexplored actors by composite score:

```
priority = influence * w1 + evidence_density * w2 + frontier_ratio * w3
```

- `influence` — actor's current influence_score (0-1)
- `evidence_density` — number of distinct data sources mentioning this actor, normalized
- `frontier_ratio` — fraction of this actor's connections pointing to unresolved actors

Weights self-tune from graph growth metrics. Initial values: w1=0.4, w2=0.3, w3=0.3. After each batch, measure actors_discovered_per_expansion and adjust weights toward the strategy that yields the most new connections.

### Stage 2: Discover

Queries all applicable sources for the target actor's connections. Each source adapter returns:

```python
@dataclass
class DiscoveredConnection:
    target_name: str           # raw name from source
    target_hint: dict          # any identifying info (title, org, ticker)
    relationship: str          # e.g. "board_member", "donates_to", "co_insider"
    strength: float            # 0-1 confidence in this specific link
    evidence: list[dict]       # citations: [{source, url, date, excerpt}]
    confidence_tier: int       # 1=hard_data, 2=public_record, 3=inferred, 4=rumor
```

Sources queried in parallel where possible. Deduplicates across sources (same target appearing in multiple sources increases confidence).

### Stage 3: Entity Resolution

Matches discovered names to existing actors in the graph:

1. **Exact match** — normalized name lookup in `name_index`
2. **Fuzzy match** — Levenshtein distance < 3, same category/org context
3. **LLM disambiguation** — when fuzzy match returns multiple candidates, local LLM picks the correct one given the evidence context

Creates new actors only for genuinely new entities. Every new actor gets:
- `degree` = parent's degree + 1
- `source` = which source discovered it
- `credibility` = based on confidence tier of the evidence
- Queued for future expansion in the priority queue

### Stage 4: LLM Enrichment

Local LLM (Hermes/Nemotron on the z4 server) generates a structured profile from all gathered evidence:

- Key relationships with context
- Trading patterns (if insider/congressional)
- Lobbying activity
- Offshore connections
- Risk flags
- **Rabbit holes** — new actors mentioned in evidence that should be queued
- Investigation leads for the operator

Rules: ONLY report facts that appear in the evidence. Label any speculation explicitly as `confidence_tier: 4` (rumor). Every claim must cite its source.

## 4. Data Sources — 13 Sources, 4 Confidence Tiers

### Tier 1 — Hard Data (confirmed)
| Source | Connection Types | Status |
|--------|-----------------|--------|
| SEC Form 4 | `insider_at`, `co_insider` | Ingesting |
| SEC 13F | `holds_position_in`, `co_holder` | Puller exists |
| EDGAR 10-K/DEF 14A | `executive_at`, `subsidiary_of` | Partial |
| Congressional Disclosures | `trades_stock_of`, `committee_peer` | Ingesting |
| FEC/OpenSecrets | `donates_to`, `co_donor` | Ingesting |
| Senate LDA | `lobbied_by`, `co_lobbying_target` | Ingesting |
| FARA | `lobbies_for`, `represents` | Ingesting |
| ICIJ | `officer_of`, `intermediary_for`, `co_entity` | In DB (785K) |
| OpenCorporates | `director_of`, `shareholder_of`, `co_director` | New |

### Tier 2 — Public Record (derived)
| Source | Connection Types | Status |
|--------|-----------------|--------|
| Wikidata | `board_member`, `employer`, `spouse`, `alma_mater`, `political_party` | New |
| Wikipedia | career history, relationships, controversies (unstructured → LLM parses) | New |
| GDELT | event co-occurrence, country-pair actions | Ingesting |
| FOIA Cables | diplomatic relationships, hidden motivations | Ingesting |

### Tier 3 — Inferred (estimated)
| Source | Connection Types | Status |
|--------|-----------------|--------|
| News Co-Occurrence | GDELT + Google News: people mentioned together | New |
| Google Knowledge Graph | entity relationships from Google's structured data | New |
| LLM Analysis | enrichment discovers relationships from cross-referencing evidence | Exists (actor_researcher) |
| Co-Activity Patterns | actors trading same stocks, lobbying same bills, donating to same PACs | New |

### Tier 4 — Rumor / Operator (unverified)
| Source | Connection Types | Status |
|--------|-----------------|--------|
| Operator Input | manually added actors, connections, hunches, leads | New |
| Social/Reddit/Finviz | crowd-sourced intelligence | Exists (smart_money puller) |
| LLM Speculation | "likely connected because..." with explicit rumor label | New |

## 5. In-Memory Graph Engine

### Data Structures

```python
# All in RAM — loaded from Postgres on startup
actors: dict[str, Actor]                              # full actor objects by ID
adjacency: dict[str, dict[str, ConnectionMeta]]       # actor_id → {neighbor_id → meta}
reverse_source: dict[str, set[str]]                   # source_name → set of actor_ids it mentions
name_index: dict[str, str]                            # normalized_name → actor_id
degree_index: dict[int, set[str]]                     # degree → set of actor_ids
```

`ConnectionMeta` is a lightweight struct: `(relationship: str, strength: float, confidence_tier: int, sources: list[str])`.

### Graph Operations

All in-memory, microsecond latency:

- `bfs(start, max_depth=11)` — find all actors within N degrees
- `shortest_path(a, b)` — Dijkstra with `1/strength` as edge weight
- `find_bridges(a, b)` — actors connecting two distant clusters
- `community_detect()` — Louvain modularity for actor clusters
- `centrality(actor)` — betweenness, closeness, eigenvector centrality
- `subgraph(actor, depth=3)` — extract neighborhood for frontend rendering (max 2000 nodes)

### Sync Strategy

- Spider daemon writes to Postgres AND updates in-memory graph atomically
- API reads from memory only (never hits DB for graph queries)
- Full reload from Postgres on startup (~2-5 min for 5M actors)
- Graph engine exposes a thread-safe read interface; spider writes behind a lock

## 6. Visualization — Confidence-Colored Network

### Connection Rendering
| Tier | Style | Color |
|------|-------|-------|
| Tier 1 (hard data) | Solid, bright, full opacity | Green/white |
| Tier 2 (public record) | Solid, dimmer | Blue/white |
| Tier 3 (inferred) | Dashed | Orange/yellow |
| Tier 4 (rumor) | Dotted, faint | Red, low opacity |

### Node Rendering
- **Size** = influence score (bigger = more powerful)
- **Color** = category (central_bank=gold, government=blue, fund=green, corporation=white, insider=cyan, politician=red)
- **Glow/pulse** = recent activity (traded, lobbied, filed in last 7 days)
- **Ring** = operator-flagged or LLM-flagged for investigation

### Frontend Interactions
- Click actor → panel shows full profile, evidence, connections, LLM analysis
- Click connection → shows evidence chain (which sources proved this link)
- Search → highlights actor and dims everything beyond degree 3
- Path finder → "show me how X connects to Y" → highlights shortest path
- Degree slider → show degree 1-11, progressively reveal the web

### Performance
Frontend receives top 2000 nodes via `subgraph()` extraction (not all 5M). User navigates by clicking deeper. Each click fetches the next neighborhood from the in-memory graph (~1ms API response).

## 7. API Endpoints

```
GET  /api/v1/intelligence/actor-network                          — top N actors (existing, now from RAM)
GET  /api/v1/intelligence/actor/{id}/neighborhood?depth=3        — subgraph around actor
GET  /api/v1/intelligence/actor/{id}/path/{target_id}            — shortest path between two actors
GET  /api/v1/intelligence/actor/{id}/connections                 — all connections with evidence
GET  /api/v1/intelligence/spider/status                          — queue depth, actors explored, degree frontier
GET  /api/v1/intelligence/spider/stats                           — total actors, connections, by degree, by source
POST /api/v1/intelligence/spider/inject                          — operator adds actor/connection/lead
POST /api/v1/intelligence/spider/prioritize/{actor_id}           — bump actor to top of queue
GET  /api/v1/intelligence/communities                            — detected actor clusters
GET  /api/v1/intelligence/bridges?from_community=X&to_community=Y — bridging actors
```

## 8. New Files

```
intelligence/spider/
  __init__.py
  daemon.py              — main spider loop (systemd service)
  priority_queue.py      — composite-scored expansion queue
  discovery.py           — orchestrates 13-source connection discovery
  entity_resolver.py     — fuzzy match + LLM disambiguation
  graph_engine.py        — in-memory graph with BFS, shortest path, communities
  sources/
    __init__.py
    wikidata.py          — Wikidata SPARQL + Wikipedia API
    opencorporates.py    — OpenCorporates company officer lookup
    gdelt_people.py      — GDELT person co-occurrence extraction
    google_kg.py         — Google Knowledge Graph API
    news_cooccurrence.py — Google News co-mention scoring
    operator.py          — manual actor/connection injection
    sec.py               — Form 4 + 13F cross-referencing
    political.py         — FEC + LDA + FARA + congressional
    icij.py              — ICIJ offshore leak graph traversal
    llm_speculate.py     — LLM-generated speculative connections (tier 4)

api/routers/
  intelligence_spider.py — spider status, inject, prioritize endpoints

pwa/src/views/
  ActorNetwork.jsx       — enhanced: path finder, degree slider, evidence panel
```

## 9. Database Schema Additions

```sql
-- Spider expansion queue
CREATE TABLE spider_queue (
    actor_id    TEXT PRIMARY KEY REFERENCES actors(id),
    priority    NUMERIC NOT NULL DEFAULT 0,
    degree      INT NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending, processing, done, failed
    queued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at  TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    sources_checked JSONB DEFAULT '[]',
    connections_found INT DEFAULT 0,
    actors_created INT DEFAULT 0
);

CREATE INDEX idx_spider_queue_priority ON spider_queue (priority DESC) WHERE status = 'pending';

-- Spider run log
CREATE TABLE spider_runs (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    actors_processed INT DEFAULT 0,
    connections_found INT DEFAULT 0,
    new_actors       INT DEFAULT 0,
    max_degree_reached INT DEFAULT 0,
    errors          JSONB DEFAULT '[]'
);
```

## 10. Systemd Service

```ini
[Unit]
Description=GRID Connection Mapping Spider
After=grid-api.service postgresql.service

[Service]
Type=simple
User=grid
WorkingDirectory=/home/grid/grid_v4/grid_repo
ExecStart=/usr/bin/python3 -m intelligence.spider.daemon
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```
