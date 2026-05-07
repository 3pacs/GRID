# GRID v5 Transformation Plan — The Financial Palantir

> **Written:** 2026-04-08 | **Scope:** Full platform transformation from intelligence dashboard to investigative canvas
>
> This plan is based on exhaustive reconnaissance of the entire GRID codebase:
> 326K lines Python (837 files), 51.5K lines frontend (115 JSX files),
> 448 API endpoints (62 routers), 170+ data source integrations,
> 51 DB tables, 21 systemd services, 2016 tests across 118 files.

---

## Executive Summary

GRID v4 answers the seven Palantir questions (WHERE, WHO, HOW MUCH, WHEN, WHY, WHAT NEXT, WHAT NOW). But it answers them in **separate dashboards**. Palantir's edge is not answering questions — it's letting analysts **discover questions they didn't know to ask** by traversing relationships on a unified canvas.

**v5 transforms GRID from a dashboard collection into an investigative platform.**

The transformation has 6 architectural vectors:

| # | Vector | What Changes | Why |
|---|--------|-------------|-----|
| 1 | **Investigation Canvas** | New React Flow workspace for freeform investigation | The killer feature — dashboards answer known questions, canvas discovers unknown ones |
| 2 | **Event-Driven Backbone** | PostgreSQL LISTEN/NOTIFY + SSE replaces in-process broadcast | Enables real-time canvas updates, decouples producers from consumers |
| 3 | **Graph Query Layer** | Apache AGE on PostgreSQL for multi-hop traversals | Actor network needs real graph queries (shortest path, community detection, influence propagation) |
| 4 | **Full-Text Intelligence Search** | PostgreSQL FTS + tsvector across all text corpus | Investigators need to search filings, cables, news, theses — not just ticker lookup |
| 5 | **Frontend Modernization** | TypeScript migration, component decomposition, data hooks | 47/67 views missing error states, 52 duplicated loading patterns, 5 god components (1900+ LOC) |
| 6 | **CI/CD + Operational Maturity** | GitHub Actions, asyncpg, structured logging | No automated testing on deploy, sync DB driver blocks event loop |

---

## Current State Assessment

### What v4 Does Well (DON'T BREAK)

- **[[PIT Store|PIT-correct]] data pipeline** — `store/pit.py` with [[PIT Store|DISTINCT ON]], vintage policies, lookahead guard
- **170+ data source integrations** — 7 subpackages, unified scheduler, [[Hermes Scheduler|Hermes]] 24/7 operator
- **Custom in-memory actor graph** — `intelligence/spider/graph_engine.py`, microsecond traversals, 495+ actors
- **Self-improving Oracle** — 6 models, signal/anti-signal, weight evolution, 10,893 predictions pending scoring
- **Self-tuning [[Options Scanner|options scanner]]** — 7 signals with adaptive weights, Kelly sizing, 5-layer sanity gates
- **22+ D3 visualization components** — [[Actor Network View|ActorNetwork]], [[MoneyFlow View|MoneyFlow]], [[Cross Reference View|CrossReference]], Timeline, 3D ActorUniverse
- **WebSocket real-time** — auto-reconnect, 15+ event types, exponential backoff
- **Immutable [[Decision Journal|decision journal]]** — full provenance, outcome tracking, [[Postmortem|postmortem]] automation
- **[[Model Governance|Model governance]]** — CANDIDATE → SHADOW → STAGING → PRODUCTION state machine with gate checks
- **3-tier LLM routing** — LOCAL/REASON/ORACLE with automatic fallback chain
- **Multi-source [[Conflict Resolution|conflict resolution]]** — family-specific thresholds, priority ranking, conflict logging

### What v4 Gets Wrong (FIX)

| Problem | Impact | Evidence |
|---------|--------|----------|
| No investigation canvas | Users can't follow threads across views | 55 separate routes, no cross-view linking |
| In-process event broadcast | Single-server only, no persistence, lost on restart | `asyncio.run_coroutine_threadsafe()` in main.py |
| SQL-only actor queries | Multi-hop queries require O(n) Python loops | graph_engine.py uses dict adjacency, not graph DB |
| No full-text search | Can't search across filings, cables, news, theses | Only SQL LIKE in search.py, pgvector for RAG |
| 47/67 views missing error UI | Users see blank screens on API failures | Only 20 views have `if (error)` checks |
| 52 duplicated loading patterns | Every component reinvents `useState(false)` | 52 instances of manual try/catch boilerplate |
| 5 god components (1900+ LOC) | Untestable, unmaintainable | ActorNetwork 1959, CrossReference 1922, WatchlistAnalysis 1516 |
| No TypeScript | No compile-time safety, prop drilling | Pure JS, no JSDoc, no type checking |
| No CI/CD | Manual deploy.sh only | No GitHub Actions, no automated test runs |
| Sync DB driver | psycopg2 blocks event loop | All DB via run_in_executor workaround |
| Single monolithic Zustand store | 50+ actions in one file | store.js manages auth, UI, domain, real-time in one blob |

---

## v5 Target Architecture

```
                    ┌─────────────────────────────────────────┐
                    │           INVESTIGATION CANVAS           │
                    │  React Flow + D3 embedded visualizations │
                    │  Drag entities, draw connections, pin    │
                    │  evidence, save/share investigation      │
                    │  boards, right-click → LLM explain       │
                    └──────────────┬──────────────────────────┘
                                   │
                    ┌──────────────┴──────────────────────────┐
                    │         WORLD VIEWS (existing)           │
                    │  Flow │ Power │ Truth │ Globe │ Risk │   │
                    │  Signal │ Timeline │ Why │ Canvas        │
                    │  (click any entity → send to canvas)     │
                    └──────────────┬──────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
    ┌─────────┴──────┐  ┌─────────┴──────┐  ┌─────────┴──────┐
    │   SSE Stream    │  │  REST API      │  │  WebSocket     │
    │  (canvas live   │  │  (queries,     │  │  (prices,      │
    │   updates)      │  │   CRUD)        │  │   alerts)      │
    └─────────┬──────┘  └─────────┬──────┘  └─────────┬──────┘
              │                    │                    │
    ┌─────────┴────────────────────┴────────────────────┴──────┐
    │                    FastAPI + asyncpg                       │
    │  62 routers │ auth │ rate limiting │ security headers      │
    └──────────────────────────┬───────────────────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         │                     │                     │
  ┌──────┴───────┐   ┌────────┴────────┐   ┌────────┴────────┐
  │ PostgreSQL   │   │  Apache AGE     │   │  PostgreSQL     │
  │ + TimescaleDB│   │  (graph layer   │   │  FTS            │
  │ (relational) │   │   on same DB)   │   │  (tsvector)     │
  │              │   │                 │   │                 │
  │ 51 tables    │   │ Actor graph     │   │ Filings, cables │
  │ PIT store    │   │ Multi-hop       │   │ News, theses    │
  │ Time series  │   │ Shortest path   │   │ Predictions     │
  └──────────────┘   │ Communities     │   └─────────────────┘
                     └─────────────────┘
                               │
    ┌──────────────────────────┼───────────────────────────────┐
    │              LISTEN/NOTIFY Event Bus                      │
    │  Channels: actor_update, signal_fire, regime_change,     │
    │  prediction_scored, flow_shift, investigation_update      │
    └──────────────────────────┬───────────────────────────────┘
                               │
    ┌──────────────────────────┴───────────────────────────────┐
    │                 INTELLIGENCE LAYER                        │
    │  110+ modules │ 66,880 lines │ trust scoring │ causation  │
    │  spider daemon │ regime discovery │ forensics │ RAG        │
    │  (unchanged — v5 adds graph queries + event emission)     │
    └──────────────────────────────────────────────────────────┘
```

---

## Vector 1: Investigation Canvas (THE KILLER FEATURE)

### What It Is

A freeform workspace where an analyst can drag entities from any view onto a canvas, draw connections, pin evidence (charts, filings, quotes), and build investigation boards. Think Figma meets Bloomberg Terminal.

**This is what separates Palantir from dashboards.** Dashboards answer known questions. The canvas helps you discover questions you didn't know to ask.

### User Flow

```
1. See unusual dark pool activity on NVDA in the Flow view
2. Right-click NVDA → "Investigate on Canvas"
3. NVDA entity appears as a node on the canvas
4. Click "Expand Network" → board members, fund holders, congressional traders appear
5. Notice Senator X has a position → drag Senator X onto canvas
6. "Show Timeline" on Senator X → see their trades overlaid with committee hearings
7. Notice they bought before a defense AI hearing → drag that hearing onto canvas
8. "Find Connections" → system shows: Senator X sits on Armed Services Committee,
   NVDA has $2B in defense contracts, hearing is about AI procurement
9. Pin the gov contract as evidence, add a note: "Informed trading?"
10. Save board as "NVDA-Congressional-Investigation-2026-04"
11. Set alert: "Notify me if Senator X trades NVDA again"
```

### Technical Implementation

**Frontend:**
```
Package: @xyflow/react (React Flow v12)
Canvas state: Zustand store (useCanvasStore)
Node types:
  - EntityNode (actor, ticker, sector, country)
  - EvidenceNode (chart snapshot, filing excerpt, quote)
  - TimelineNode (embedded mini-timeline)
  - ChartNode (embedded D3 visualization)
  - NoteNode (freeform text/markdown)
Edge types:
  - RelationshipEdge (owns, trades, influences, sits-on)
  - CausalEdge (caused, preceded, correlated-with)
  - MoneyFlowEdge (amount, direction, confidence)
```

**Backend:**
```
New tables:
  - investigation_boards (id, user_id, title, description, created_at, updated_at)
  - investigation_nodes (id, board_id, node_type, entity_id, position_x, position_y, data JSONB)
  - investigation_edges (id, board_id, source_node_id, target_node_id, edge_type, label, data JSONB)
  - investigation_evidence (id, node_id, evidence_type, content, source_url, captured_at)

New API endpoints:
  POST   /api/v1/canvas/boards              — create board
  GET    /api/v1/canvas/boards              — list boards
  GET    /api/v1/canvas/boards/:id          — load board with nodes/edges
  PUT    /api/v1/canvas/boards/:id          — save board state
  DELETE /api/v1/canvas/boards/:id          — archive board
  POST   /api/v1/canvas/boards/:id/nodes    — add node
  POST   /api/v1/canvas/boards/:id/edges    — add edge
  POST   /api/v1/canvas/expand/:entity_type/:entity_id — expand 1-hop connections
  POST   /api/v1/canvas/explain             — LLM explains connection between two entities
  POST   /api/v1/canvas/alert               — set watch on entity for changes
```

**Integration with existing views:**
- Every entity in every view gets a right-click → "Send to Canvas" action
- [[Actor Network View|ActorNetwork]]: click actor → "Investigate"
- [[MoneyFlow View|MoneyFlow]]: click flow → "Trace on Canvas"
- Timeline: select events → "Pin to Canvas"
- [[Cross Reference View|CrossReference]]: click divergence → "Explain on Canvas"

### Files to Create

```
pwa/src/views/Canvas.jsx              — main canvas view (~800 LOC)
pwa/src/components/canvas/            — canvas component directory
  EntityNode.jsx                      — actor/ticker/sector node (~200 LOC)
  EvidenceNode.jsx                    — pinned evidence card (~150 LOC)
  TimelineNode.jsx                    — embedded mini-timeline (~250 LOC)
  ChartNode.jsx                       — embedded D3 chart (~200 LOC)
  NoteNode.jsx                        — freeform note (~100 LOC)
  CanvasToolbar.jsx                   — tools: add node, draw edge, save, share (~200 LOC)
  CanvasContextMenu.jsx               — right-click menu (~150 LOC)
  ConnectionExplainer.jsx             — LLM explanation panel (~200 LOC)
pwa/src/stores/canvasStore.js         — Zustand canvas state (~150 LOC)
api/routers/canvas.py                 — canvas CRUD + expand + explain (~400 LOC)
```

### Dependencies

```
@xyflow/react: ^12.0.0               — canvas framework
@xyflow/background: ^12.0.0          — grid background
@xyflow/controls: ^12.0.0            — zoom/pan controls
@xyflow/minimap: ^12.0.0             — minimap navigation
```

---

## Vector 2: Event-Driven Backbone

### Problem

GRID v4 broadcasts events via in-process asyncio. Events are lost on restart, can't scale beyond one process, and there's no event history for replay.

### Solution: PostgreSQL LISTEN/NOTIFY + SSE

**Why not Kafka/Redpanda?** GRID is a single-server system with 512GB RAM. Adding a message broker adds operational complexity for no benefit. [[PostgreSQL]]'s built-in LISTEN/NOTIFY gives us:
- Zero additional infrastructure
- Transactional event emission (event fires only if the INSERT commits)
- Channel-based routing
- Works with asyncpg natively

**SSE (Server-Sent Events) over WebSocket for canvas:**
- WebSocket stays for bidirectional (chat, prices)
- SSE added for unidirectional event streams (canvas updates, investigation alerts)
- SSE auto-reconnects natively in browsers, simpler than WebSocket

### Channels

```sql
-- Event channels (PostgreSQL NOTIFY)
actor_update        — actor position change, new filing, trust score update
signal_fire         — new signal crosses threshold
regime_change       — regime transition detected
prediction_scored   — oracle prediction scored (hit/miss/partial)
flow_shift          — capital flow direction reversal
investigation_alert — watched entity changed (canvas alerts)
pull_complete       — data source pull finished
model_promoted      — governance state change
```

### Implementation

```python
# New file: events/bus.py (~200 LOC)
class EventBus:
    """PostgreSQL LISTEN/NOTIFY event bus."""

    async def emit(self, channel: str, payload: dict):
        """Emit event — called from any module."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                channel, json.dumps(payload)
            )

    async def subscribe(self, channel: str) -> AsyncIterator[dict]:
        """Subscribe to channel — yields events as they arrive."""
        async with self.pool.acquire() as conn:
            await conn.add_listener(channel, self._on_notify)
            ...

# New file: api/routers/sse.py (~150 LOC)
@router.get("/events/{channel}")
async def event_stream(channel: str, request: Request):
    """SSE endpoint for canvas live updates."""
    async def generate():
        async for event in bus.subscribe(channel):
            yield f"data: {json.dumps(event)}\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream")
```

### Migration Path

1. Add asyncpg to requirements (needed for LISTEN/NOTIFY)
2. Create EventBus class with emit/subscribe
3. Add SSE endpoint for canvas subscriptions
4. Instrument existing modules to emit events (non-breaking — just add `await bus.emit()` calls)
5. Canvas subscribes to relevant channels via SSE
6. Existing WebSocket continues unchanged for prices/alerts

### Files to Create

```
events/__init__.py
events/bus.py                         — PostgreSQL LISTEN/NOTIFY wrapper (~200 LOC)
events/channels.py                    — channel definitions + payload schemas (~100 LOC)
api/routers/sse.py                    — SSE streaming endpoint (~150 LOC)
pwa/src/hooks/useEventStream.js       — SSE client hook (~80 LOC)
```

---

## Vector 3: Graph Query Layer (Apache AGE)

### Problem

GRID's actor graph is a custom Python dict-based adjacency structure (`intelligence/spider/graph_engine.py`). It handles simple lookups fast but can't do:
- Multi-hop traversals: "Find all actors within 3 degrees of Senator X who also traded NVDA"
- Community detection: "Which actors form clusters?"
- Influence propagation: "If Actor A moves, who is affected?"
- Shortest path with constraints: "How is this hedge fund connected to this congressional committee?"

### Solution: Apache AGE (PostgreSQL Extension)

**Why AGE over Neo4j?** Same [[PostgreSQL]] instance. No new infrastructure. SQL + Cypher in one query. Transactional consistency with relational data.

### Schema

```sql
-- Load AGE extension
CREATE EXTENSION IF NOT EXISTS age;
SELECT create_graph('actor_graph');

-- Vertex labels (map to existing actors table)
SELECT create_vlabel('actor_graph', 'Actor');
SELECT create_vlabel('actor_graph', 'Ticker');
SELECT create_vlabel('actor_graph', 'Committee');
SELECT create_vlabel('actor_graph', 'Institution');
SELECT create_vlabel('actor_graph', 'Sector');

-- Edge labels
SELECT create_elabel('actor_graph', 'TRADES');        -- actor → ticker
SELECT create_elabel('actor_graph', 'SITS_ON');       -- actor → committee
SELECT create_elabel('actor_graph', 'MANAGES');       -- actor → institution
SELECT create_elabel('actor_graph', 'HOLDS');         -- institution → ticker
SELECT create_elabel('actor_graph', 'INFLUENCES');    -- actor → actor
SELECT create_elabel('actor_graph', 'CONTRACTED');    -- institution → ticker (gov contracts)
SELECT create_elabel('actor_graph', 'LOBBIES_FOR');   -- actor → institution (FARA/lobbying)
```

### Query Examples

```sql
-- "How is Senator X connected to NVDA?" (shortest path)
SELECT * FROM cypher('actor_graph', $$
    MATCH p = shortestPath(
        (a:Actor {name: 'Nancy Pelosi'})-[*..5]-(t:Ticker {symbol: 'NVDA'})
    )
    RETURN p
$$) as (path agtype);

-- "Who traded tickers in sectors they regulate?" (informed trading detection)
SELECT * FROM cypher('actor_graph', $$
    MATCH (a:Actor)-[:SITS_ON]->(c:Committee)-[:OVERSEES]->(s:Sector)<-[:IN_SECTOR]-(t:Ticker)<-[:TRADES]-(a)
    RETURN a.name, t.symbol, c.name, s.name
$$) as (actor agtype, ticker agtype, committee agtype, sector agtype);

-- "Expand 2-hop neighborhood for canvas" (investigation expand)
SELECT * FROM cypher('actor_graph', $$
    MATCH (a:Actor {id: $actor_id})-[r*1..2]-(connected)
    RETURN a, r, connected
$$) as (source agtype, rels agtype, target agtype);
```

### Migration Path

1. Install AGE extension on [[PostgreSQL]] 15
2. Create graph schema with vertex/edge labels
3. Sync existing `actors` table → Actor vertices (one-time migration + trigger)
4. Sync existing connections → edges
5. Add `store/graph.py` with Cypher query helpers
6. Update `intelligence/spider/graph_engine.py` to use AGE for complex queries, keep dict for hot-path lookups
7. Canvas "Expand" uses AGE for multi-hop traversals

### Files to Create/Modify

```
store/graph.py                        — AGE query wrapper (~300 LOC)
scripts/migrations/add_age_graph.sql  — graph schema creation (~100 LOC)
scripts/sync_actors_to_graph.py       — one-time migration (~200 LOC)
intelligence/spider/graph_engine.py   — add AGE backend for complex queries (modify)
api/routers/canvas.py                 — expand endpoint uses graph queries (modify)
```

---

## Vector 4: Full-Text Intelligence Search

### Problem

Investigators need to search across the entire intelligence corpus: filings, diplomatic cables, news articles, theses, predictions, postmortems. Current search is SQL LIKE on ticker/actor names only.

### Solution: PostgreSQL Full-Text Search (tsvector/tsquery)

**Why not Elasticsearch?** Same reason as AGE — keep it in PostgreSQL. FTS with GIN indexes handles GRID's corpus size (millions of documents) with sub-second queries. No new infrastructure.

### Implementation

```sql
-- Add tsvector columns to searchable tables
ALTER TABLE oracle_predictions ADD COLUMN search_vector tsvector;
ALTER TABLE decision_journal ADD COLUMN search_vector tsvector;
ALTER TABLE thesis_snapshots ADD COLUMN search_vector tsvector;

-- Create materialized search index across all intelligence
CREATE MATERIALIZED VIEW intelligence_search AS
SELECT
    'prediction' as doc_type,
    id::text as doc_id,
    ticker || ' ' || COALESCE(score_notes, '') as content,
    to_tsvector('english', ticker || ' ' || COALESCE(score_notes, '')) as search_vector,
    timestamp as created_at
FROM oracle_predictions
UNION ALL
SELECT
    'journal' as doc_type,
    id::text as doc_id,
    COALESCE(grid_recommendation, '') || ' ' || COALESCE(action_taken, '') as content,
    to_tsvector('english', COALESCE(grid_recommendation, '') || ' ' || COALESCE(action_taken, '')) as search_vector,
    created_at
FROM decision_journal
UNION ALL
SELECT
    'thesis' as doc_type,
    id::text as doc_id,
    title || ' ' || COALESCE(description, '') as content,
    to_tsvector('english', title || ' ' || COALESCE(description, '')) as search_vector,
    created_at
FROM thesis_snapshots;

CREATE INDEX idx_intel_search_vector ON intelligence_search USING GIN(search_vector);

-- Search query
SELECT doc_type, doc_id, content,
       ts_rank(search_vector, query) as rank
FROM intelligence_search, plainto_tsquery('english', 'NVDA defense contract') as query
WHERE search_vector @@ query
ORDER BY rank DESC
LIMIT 20;
```

### API

```
GET /api/v1/search/intelligence?q=NVDA+defense+contract&type=prediction,journal&limit=20
```

Returns ranked results across all intelligence documents. Canvas can use this to find evidence to pin.

### Files to Create

```
store/search.py                              — FTS query builder (~200 LOC)
scripts/migrations/add_fts_indexes.sql       — tsvector columns + GIN indexes (~100 LOC)
api/routers/search.py                        — update existing search router (~150 LOC modify)
pwa/src/components/IntelligenceSearch.jsx     — search panel for canvas (~250 LOC)
```

---

## Vector 5: Frontend Modernization

### Problem Assessment

| Issue | Scope | Impact |
|-------|-------|--------|
| 47/67 views missing error UI | 70% of views | Silent failures, blank screens |
| 52 duplicated loading/error patterns | Every component | Maintenance burden, inconsistency |
| 5 god components (1900+ LOC each) | ActorNetwork, CrossReference, WatchlistAnalysis, ActorUniverse, Timeline | Untestable, unmaintainable |
| No TypeScript | 115 JSX files | No compile-time safety, no IDE autocomplete |
| Single monolithic Zustand store | 50+ actions, 242 LOC | Doesn't scale, hard to test |
| 2 test files total | Near-zero coverage | Regressions undetected |
| D3 cleanup on unmount unverified | 22+ D3 components | Potential memory leaks |
| LoadingSkeleton exists but used in 3 places | 40+ views show blank during fetch | Poor perceived performance |

### Solution: Incremental Modernization (NOT a rewrite)

**Rule: No big bang. Each change ships independently. The app works after every PR.**

#### 5A. Universal Data Hook

Replace 52 instances of duplicated loading/error boilerplate with one hook:

```typescript
// pwa/src/hooks/useAsyncData.ts
function useAsyncData<T>(
  fetcher: () => Promise<T>,
  deps: any[] = []
): { data: T | null; loading: boolean; error: Error | null; refetch: () => void }
```

**Migration:** grep for `const [loading, setLoading] = useState`, replace one view at a time.

#### 5B. Error Boundaries per View

```typescript
// pwa/src/components/ViewErrorBoundary.tsx
// Wraps each route — catches D3/Three.js crashes without taking down the app
```

Currently one ErrorBoundary wraps the entire app. A D3 crash in [[Actor Network View|ActorNetwork]] kills everything.

#### 5C. Store Decomposition

Split `store.js` (242 LOC, 50+ actions) into focused stores:

```
pwa/src/stores/
  authStore.ts          — token, user, role, login/logout
  uiStore.ts            — theme, activeView, notifications, loading/error per key
  domainStore.ts        — regime, signals, journal, models
  realtimeStore.ts      — WebSocket state, live prices, live alerts
  canvasStore.ts        — (new) investigation board state
```

#### 5D. TypeScript Migration

**Strategy:** Rename `.jsx` → `.tsx` one file at a time, starting with new files (canvas) and shared hooks/stores. Existing views migrate opportunistically when touched for other work.

```
Phase 1: New files in TypeScript (canvas, stores, hooks)
Phase 2: Shared components (NavBar, LoadingSkeleton, ErrorBoundary)
Phase 3: Smaller views (<300 LOC) when touched
Phase 4: God components last (with decomposition)
```

#### 5E. God Component Decomposition

**ActorNetwork.jsx (1959 LOC) → 6 files:**
```
ActorNetwork.tsx          — orchestrator (~300 LOC)
ActorGraph.tsx            — D3 force simulation (~400 LOC)
ActorTooltip.tsx          — hover tooltip (~150 LOC)
ActorDetail.tsx           — click detail panel (~200 LOC)
MoneyParticles.tsx        — animated particle system (~200 LOC)
useActorNetworkData.ts    — data fetching hook (~150 LOC)
```

Same pattern for [[Cross Reference View|CrossReference]], WatchlistAnalysis, ActorUniverse, Timeline.

#### 5F. Loading Skeletons

Extend existing LoadingSkeleton (92 LOC) with view-specific variants:
- `ChartSkeleton` — shimmer placeholder for D3 charts
- `TableSkeleton` — row placeholders for data tables
- `CardGridSkeleton` — card grid placeholder

Apply to all 40+ views that currently show blank during fetch.

### Files to Create

```
pwa/src/hooks/useAsyncData.ts                 — universal data hook (~80 LOC)
pwa/src/components/ViewErrorBoundary.tsx       — per-view error boundary (~60 LOC)
pwa/src/stores/authStore.ts                    — auth state (~60 LOC)
pwa/src/stores/uiStore.ts                      — UI state (~80 LOC)
pwa/src/stores/domainStore.ts                  — domain state (~100 LOC)
pwa/src/stores/realtimeStore.ts                — real-time state (~80 LOC)
pwa/src/components/skeletons/                  — skeleton variants (~200 LOC total)
tsconfig.json                                  — TypeScript config
```

---

## Vector 6: CI/CD + Operational Maturity

### 6A. GitHub Actions

```yaml
# .github/workflows/ci.yml
name: GRID CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: timescale/timescaledb:latest-pg15
        env:
          POSTGRES_USER: grid_user
          POSTGRES_PASSWORD: testpass
          POSTGRES_DB: grid_test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt
      - run: python -m pytest tests/ -v --tb=short
      - run: cd pwa && npm ci && npm run build
```

### 6B. asyncpg Migration

Replace psycopg2 (sync) with asyncpg (native async) for the API layer:

```python
# db.py — add async pool alongside existing sync engine
import asyncpg

async def get_async_pool():
    return await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=10, max_size=40,
        command_timeout=30
    )
```

**Migration path:** New endpoints use asyncpg. Existing endpoints unchanged. Gradual migration.

### 6C. Structured Logging

Replace `loguru` text logs with JSON-structured logging for machine parsing:

```python
# Already using loguru — add JSON sink
log.add("logs/grid.jsonl", serialize=True, rotation="100 MB")
```

Enables future Grafana/Loki integration without changing log calls.

### Files to Create

```
.github/workflows/ci.yml             — CI pipeline (~60 LOC)
.github/workflows/deploy.yml         — CD pipeline (~40 LOC)
events/async_pool.py                  — asyncpg pool manager (~100 LOC)
```

---

## Phased Execution Plan

### Guiding Principles

1. **Never break production.** Every phase ships independently. The app works after every merge.
2. **Canvas first.** It's the highest-impact feature with the lowest coupling to existing code.
3. **Infrastructure before features.** Event bus and graph layer enable canvas, so they go first.
4. **Frontend modernization is continuous.** TypeScript, hooks, and error boundaries apply to every PR, not a dedicated phase.
5. **Parallelize aggressively.** Backend and frontend work streams are independent.

---

### Phase 0: Foundation (Week 1-2)

**Goal:** Infrastructure that all other phases depend on.

**Backend (can parallelize all 3):**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Event bus (LISTEN/NOTIFY) | `events/bus.py`, `events/channels.py` | ~300 | Nothing |
| asyncpg pool (alongside psycopg2) | `events/async_pool.py`, `db.py` modify | ~150 | Nothing |
| GitHub Actions CI | `.github/workflows/ci.yml` | ~60 | Nothing |

**Frontend (can parallelize all 4):**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| TypeScript config | `tsconfig.json`, `vite.config.ts` | ~40 | Nothing |
| `useAsyncData` hook | `hooks/useAsyncData.ts` | ~80 | TS config |
| `ViewErrorBoundary` | `components/ViewErrorBoundary.tsx` | ~60 | TS config |
| Store decomposition | `stores/*.ts` | ~400 | TS config |

**Deliverable:** Event bus emitting on existing data writes. TS compiling. New hooks available. CI running on every push.

---

### Phase 1: Investigation Canvas MVP (Week 3-5)

**Goal:** Canvas view where you can add entities, draw connections, save boards.

**Backend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Canvas DB tables | `scripts/migrations/add_canvas.sql` | ~80 | Nothing |
| Canvas CRUD API | `api/routers/canvas.py` | ~400 | Canvas tables |
| SSE endpoint | `api/routers/sse.py` | ~150 | Event bus |

**Frontend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Install @xyflow/react | `package.json` | — | Nothing |
| Canvas view shell | `views/Canvas.tsx` | ~800 | React Flow |
| Entity nodes | `components/canvas/EntityNode.tsx` | ~200 | Canvas view |
| Evidence nodes | `components/canvas/EvidenceNode.tsx` | ~150 | Canvas view |
| Note nodes | `components/canvas/NoteNode.tsx` | ~100 | Canvas view |
| Canvas toolbar | `components/canvas/CanvasToolbar.tsx` | ~200 | Canvas view |
| Canvas context menu | `components/canvas/CanvasContextMenu.tsx` | ~150 | Canvas view |
| Canvas Zustand store | `stores/canvasStore.ts` | ~150 | Store decomp |
| SSE client hook | `hooks/useEventStream.ts` | ~80 | SSE endpoint |

**Integration:**
- Add "Send to Canvas" context menu to ActorNetwork, [[MoneyFlow View|MoneyFlow]], Timeline views
- Route: `/canvas` and `/canvas/:boardId`
- Nav: add Canvas to primary tab bar

**Deliverable:** Working canvas. Create boards, add entity nodes manually, draw edges, save/load. No auto-expand yet.

---

### Phase 2: Graph Layer + Canvas Intelligence (Week 6-8)

**Goal:** Canvas "Expand" traverses real graph. Multi-hop queries. Intelligent connections.

**Backend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Install Apache AGE | Server admin | — | PostgreSQL 15 |
| Graph schema | `scripts/migrations/add_age_graph.sql` | ~100 | AGE installed |
| Actor → graph sync | `scripts/sync_actors_to_graph.py` | ~200 | Graph schema |
| Graph query wrapper | `store/graph.py` | ~300 | Graph schema |
| Canvas expand endpoint (graph-backed) | `api/routers/canvas.py` modify | ~100 | Graph queries |
| Canvas explain endpoint (LLM) | `api/routers/canvas.py` modify | ~100 | LLM router |

**Frontend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Expand animation | `components/canvas/ExpandAnimation.tsx` | ~150 | Canvas MVP |
| Connection explainer panel | `components/canvas/ConnectionExplainer.tsx` | ~200 | Explain endpoint |
| Timeline node (embedded) | `components/canvas/TimelineNode.tsx` | ~250 | Canvas MVP |
| Chart node (embedded D3) | `components/canvas/ChartNode.tsx` | ~200 | Canvas MVP |
| Canvas minimap | built into @xyflow | — | Canvas MVP |

**Deliverable:** Click entity → "Expand 2 hops" → graph query returns connected entities → animated expansion on canvas. Right-click connection → "Explain" → LLM narrative.

---

### Phase 3: Full-Text Search + Evidence (Week 9-10)

**Goal:** Search across all intelligence. Pin search results as evidence on canvas.

**Backend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| FTS indexes + materialized view | `scripts/migrations/add_fts_indexes.sql` | ~100 | Nothing |
| Search query builder | `store/search.py` | ~200 | FTS indexes |
| Update search router | `api/routers/search.py` modify | ~150 | Search builder |

**Frontend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Intelligence search panel | `components/IntelligenceSearch.tsx` | ~250 | Search API |
| Search → canvas pin flow | `components/canvas/EvidenceNode.tsx` modify | ~50 | Canvas + Search |
| Command palette search upgrade | `components/CommandPalette.tsx` modify | ~50 | Search API |

**Deliverable:** Search "NVDA defense contract" → results from predictions, journal, theses, news → click result → pin as evidence node on canvas.

---

### Phase 4: Frontend Quality Pass (Week 11-13)

**Goal:** Error states, loading skeletons, and god component decomposition.

| Task | Scope | LOC |
|------|-------|-----|
| Add error UI to 47 missing views | 47 files, ~5 lines each | ~235 |
| Add LoadingSkeleton to 40 views | 40 files, ~3 lines each | ~120 |
| Decompose ActorNetwork.jsx (1959→6 files) | 6 new files | ~1400 |
| Decompose CrossReference.jsx (1922→5 files) | 5 new files | ~1200 |
| Decompose Timeline.jsx (1144→4 files) | 4 new files | ~900 |
| Decompose WatchlistAnalysis.jsx (1516→4 files) | 4 new files | ~1000 |
| Migrate 20 smallest views to TypeScript | 20 files rename | ~0 net new |
| Add vitest component tests for canvas | 10 test files | ~800 |

**Deliverable:** No more blank screens on errors. Loading skeletons everywhere. God components split. Canvas has test coverage.

---

### Phase 5: Real-Time Canvas + Alerts (Week 14-15)

**Goal:** Canvas nodes update live. Investigation alerts fire when watched entities change.

**Backend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| Instrument actor writes with event emission | `intelligence/actors/db.py` modify | ~20 | Event bus |
| Instrument oracle scoring with events | `oracle/engine.py` modify | ~20 | Event bus |
| Instrument signal registry with events | `signals/signal_registry.py` modify | ~20 | Event bus |
| Canvas alert system | `api/routers/canvas.py` modify | ~100 | Event bus |
| Alert persistence | `scripts/migrations/add_canvas_alerts.sql` | ~30 | Nothing |

**Frontend:**

| Task | Files | LOC | Depends On |
|------|-------|-----|------------|
| SSE → canvas node update | `stores/canvasStore.ts` modify | ~50 | SSE hook |
| Alert badge on canvas nodes | `components/canvas/EntityNode.tsx` modify | ~30 | Alerts |
| Investigation alert panel | `components/canvas/AlertPanel.tsx` | ~200 | Alerts |

**Deliverable:** Open a canvas board. An actor you pinned files a new trade → the node pulses, badge appears, alert panel shows what changed. You're watching the investigation evolve in real-time.

---

### Phase 6: Geo-Spatial Layer (Week 16-18) — OPTIONAL

**Goal:** World map showing physical flows (shipping, commodities, capital).

**This phase is optional and depends on user demand.** The canvas is the priority.

| Task | Files | LOC |
|------|-------|-----|
| deck.gl integration | `views/Globe.tsx` rewrite | ~800 |
| Shipping route layer (AIS data) | `components/globe/ShippingLayer.tsx` | ~300 |
| Capital flow arrows | `components/globe/FlowArrowLayer.tsx` | ~200 |
| Commodity trade routes | `components/globe/CommodityLayer.tsx` | ~200 |
| Night lights overlay (VIIRS) | `components/globe/NightLightsLayer.tsx` | ~150 |
| Click country → canvas integration | modify Canvas | ~50 |

**Dependencies:** `deck.gl`, `@deck.gl/geo-layers`, `@loaders.gl/csv`

---

## Dependency Graph

```
Phase 0: Foundation
    ├── Event Bus ─────────────────┐
    ├── asyncpg ──────────────────┤
    ├── CI/CD ────────────────────┤
    ├── TypeScript config ────────┤
    ├── useAsyncData hook ────────┤
    ├── ViewErrorBoundary ────────┤
    └── Store decomposition ──────┤
                                  │
Phase 1: Canvas MVP ◄─────────────┘
    ├── Canvas tables + CRUD
    ├── SSE endpoint
    ├── Canvas view + nodes
    └── "Send to Canvas" integration
         │
Phase 2: Graph + Intelligence ◄───┘
    ├── Apache AGE install
    ├── Graph schema + sync
    ├── Expand + Explain
    └── Timeline/Chart nodes
         │
Phase 3: Full-Text Search ◄──────┘  (can run parallel with Phase 2)
    ├── FTS indexes
    ├── Search builder
    └── Evidence pinning
         │
Phase 4: Frontend Quality ◄──────┘  (can run parallel with Phase 3)
    ├── Error states (47 views)
    ├── Loading skeletons (40 views)
    ├── God component decomposition
    └── TypeScript migration batch 1
         │
Phase 5: Real-Time Canvas ◄──────┘
    ├── Event instrumentation
    ├── Live node updates via SSE
    └── Investigation alerts
         │
Phase 6: Geo-Spatial (OPTIONAL) ◄┘
    └── deck.gl globe with layers
```

---

## Effort Estimates

| Phase | Backend | Frontend | Total | Parallel? |
|-------|---------|----------|-------|-----------|
| 0: Foundation | 2 days | 3 days | 3 days (parallel) | Yes — all tasks independent |
| 1: Canvas MVP | 3 days | 5 days | 5 days (parallel) | Yes — BE/FE independent |
| 2: Graph + Intelligence | 4 days | 3 days | 4 days (parallel) | Yes — BE/FE independent |
| 3: Full-Text Search | 2 days | 2 days | 2 days (parallel) | Yes — can overlap Phase 2 |
| 4: Frontend Quality | 0 days | 5 days | 5 days | Parallel with Phase 3 |
| 5: Real-Time Canvas | 2 days | 2 days | 2 days (parallel) | After Phase 4 |
| 6: Geo-Spatial (optional) | 0 days | 5 days | 5 days | Independent |
| **Total** | **13 days** | **25 days** | **~21 days** (with parallelism) | |

**Calendar time: ~5 weeks** with aggressive parallelism, ~8 weeks sequential.

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Apache AGE incompatible with TimescaleDB | Medium | High | Test in Docker first. Fallback: keep dict graph + add NetworkX for complex queries |
| React Flow performance with 500+ nodes | Low | Medium | React Flow handles 10K+ nodes. Use virtualization if needed |
| asyncpg migration breaks existing queries | Low | High | Run alongside psycopg2 — new endpoints only. Never remove sync driver |
| FTS materialized view refresh too slow | Low | Low | Concurrent refresh + partial updates. Corpus is <10M docs |
| Canvas state too large for Zustand | Low | Low | Paginate boards. Lazy-load nodes. IndexedDB for offline |
| TypeScript migration causes regressions | Medium | Low | Incremental. CI catches type errors. No big-bang conversion |

---

## What v5 DOESN'T Do (Explicit Non-Goals)

1. **No Kafka/Redpanda** — PostgreSQL LISTEN/NOTIFY is sufficient for single-server
2. **No Neo4j** — Apache AGE gives Cypher on the same PostgreSQL instance
3. **No Elasticsearch** — PostgreSQL FTS with GIN indexes handles the corpus
4. **No Redis** — In-process caching + PostgreSQL materialized views are enough
5. **No microservices** — GRID is a single-operator system. Monolith is correct.
6. **No React Native** — PWA with Capacitor wrapper (already planned for Q4 2026)
7. **No full TypeScript rewrite** — Incremental migration, new files only
8. **No frontend framework change** — React 18 + [[Zustand]] stays. No Next.js, no Remix.

---

## Success Criteria

GRID v5 earns the "Palantir" label when an operator can:

1. **Start from any signal** (unusual [[Dark Pool|dark pool]], congressional trade, regime shift)
2. **Open a canvas** and drag the signal onto it
3. **Expand the network** — see connected actors, tickers, committees, institutions
4. **Search for evidence** — find filings, cables, predictions, theses that mention the entities
5. **Pin evidence** to the canvas as cards
6. **Draw causal connections** — "this trade → this hearing → this contract"
7. **Ask "why?"** — right-click → LLM explains the connection with sourced evidence
8. **Set alerts** — "notify me if any entity on this board changes"
9. **See it live** — nodes update in real-time as new data arrives
10. **Save and share** — board persists, can be reopened, compared to outcomes

**That's the investigation workflow. That's what makes it Palantir.**

---

## Appendix: New Dependencies

### Python (add to requirements.txt)
```
asyncpg>=0.29.0        # async PostgreSQL driver (for event bus + SSE)
# AGE requires: postgresql-15-age system package (not pip)
```

### Node.js (add to pwa/package.json)
```json
{
  "@xyflow/react": "^12.0.0",
  "@types/react": "^18.3.0",
  "@types/d3": "^7.4.0",
  "typescript": "^5.4.0"
}
```

### PostgreSQL Extensions
```sql
CREATE EXTENSION IF NOT EXISTS age;    -- graph queries (Vector 3)
-- pgvector already installed (RAG)
-- TimescaleDB already installed (time series)
```

---

## Appendix: File Manifest (all new files)

```
# Backend (~1,800 LOC new)
events/__init__.py
events/bus.py                                   — LISTEN/NOTIFY wrapper
events/channels.py                              — channel definitions
events/async_pool.py                            — asyncpg pool
store/graph.py                                  — AGE Cypher wrapper
store/search.py                                 — FTS query builder
api/routers/canvas.py                           — canvas CRUD + expand + explain
api/routers/sse.py                              — SSE streaming
scripts/migrations/add_canvas.sql               — canvas tables
scripts/migrations/add_age_graph.sql            — graph schema
scripts/migrations/add_fts_indexes.sql          — FTS indexes
scripts/migrations/add_canvas_alerts.sql        — alert tables
scripts/sync_actors_to_graph.py                 — one-time migration
.github/workflows/ci.yml                        — CI pipeline
.github/workflows/deploy.yml                    — CD pipeline

# Frontend (~5,500 LOC new)
pwa/tsconfig.json
pwa/src/hooks/useAsyncData.ts
pwa/src/hooks/useEventStream.ts
pwa/src/stores/authStore.ts
pwa/src/stores/uiStore.ts
pwa/src/stores/domainStore.ts
pwa/src/stores/realtimeStore.ts
pwa/src/stores/canvasStore.ts
pwa/src/views/Canvas.tsx
pwa/src/components/ViewErrorBoundary.tsx
pwa/src/components/IntelligenceSearch.tsx
pwa/src/components/canvas/EntityNode.tsx
pwa/src/components/canvas/EvidenceNode.tsx
pwa/src/components/canvas/TimelineNode.tsx
pwa/src/components/canvas/ChartNode.tsx
pwa/src/components/canvas/NoteNode.tsx
pwa/src/components/canvas/CanvasToolbar.tsx
pwa/src/components/canvas/CanvasContextMenu.tsx
pwa/src/components/canvas/ConnectionExplainer.tsx
pwa/src/components/canvas/AlertPanel.tsx
pwa/src/components/canvas/ExpandAnimation.tsx
pwa/src/components/skeletons/ChartSkeleton.tsx
pwa/src/components/skeletons/TableSkeleton.tsx
pwa/src/components/skeletons/CardGridSkeleton.tsx
```

