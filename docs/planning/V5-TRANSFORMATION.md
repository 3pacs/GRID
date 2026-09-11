# GRID v5 Transformation — Verified Status and Remaining Work

> **Revision:** 2026-09-09. Supersedes the 2026-04-08 draft, preserved verbatim at
> [`archive/V5-TRANSFORMATION-2026-04-08.md`](archive/V5-TRANSFORMATION-2026-04-08.md).
> Verified against the tree at commit `b2b4385` (main, 2026-09-04).
>
> **Read this first:** the April plan proposed building six things. Five months later,
> most of the *files* exist and much of the *behaviour* does not. The remaining v5 work is
> consolidation and adoption, not construction. Anyone who starts "Phase 0" from the April
> draft will duplicate an event bus, a CI pipeline, and a canvas that already exist.

---

## 0. How this document was produced

Three read-only audits ran against the current tree, one per pair of vectors, each
instructed to return a DONE / PARTIAL / MISSING / OBSOLETE verdict with a `file:line`
citation per plan item. Every claim below carries its evidence. Where the audits could not
establish something, this document says so rather than guessing.

**Caveats that bound what can be known from this clone:**

- The clone is **shallow**: 174 commits, oldest `0c810ef` (2026-05-29). The April–May build
  period is not in history. Dating relies on Alembic `Create Date` headers and revision
  labels, not on commits.
- `docs/planning/V5-TRANSFORMATION.md` itself arrived in commit `998952c`
  (`[hermes-operator] analytical outputs 2026-05-31`), an autonomous daemon commit. The
  April PR for it ([#21](https://github.com/3pacs/GRID/pull/21)) was closed unmerged.
- The entire frontend "Phase 0 foundation" (`hooks/useAsyncData.js`, `stores/*`,
  `tsconfig.json`, `ViewErrorBoundary.jsx`, `api.ts`, `store.ts`) landed in that same
  daemon commit. That single fact explains the dominant pattern in Vector 5: files
  scaffolded to match the plan's manifest, never integrated.
- No Postgres, Docker, or server access from this environment. Server-side state
  (whether the `age` extension is actually installed on `griddb`) is unverifiable here;
  what *is* verifiable is that no code path depends on it.

**Status legend used throughout:**

| Status | Meaning |
|---|---|
| DONE | Exists and is reachable from a live code path |
| DONE+ | Exists and exceeds the plan's spec |
| PARTIAL | Exists on disk but is unwired, unreached, or incomplete |
| MISSING | Not present anywhere in the repo |
| OBSOLETE | Plan item superseded by a different design that shipped |
| GHOST | Present, internally coherent, and has zero callers |

---

## 1. Executive summary — what actually happened

**April 8:** plan written. Six vectors, six phases, ~7,300 LOC of new files listed.

**April 8–13:** Phases 1–3 built, fast. `.claude/CODEBASE_INDEX.md` (dated 2026-04-13)
already carries a "Canvas & Graph (V5 Phase 1-3)" section. Alembic revisions for the
canvas tables and FTS carry `Create Date: 2026-04-08`. The canvas backend grew to
**5,815 LOC across eight routers** — 14× the plan's 400-LOC estimate.

**Then the stack diverged from the spec, silently.** The canvas shipped on
**Sigma.js + graphology** (`pwa/src/canvas/GothamCanvas.jsx`, 1,548 LOC), not React
Flow. The React Flow implementation the plan specified was *also* written
(`pwa/src/components/canvas/`, 13 files, 1,902 LOC) and is now dead: every file in it is
imported only by itself. The `@xyflow/react` dependency serves only that dead code.

**May 31:** the frontend foundation was scaffolded by the Hermes daemon in one commit and
never adopted. `useAsyncData.js` has zero importers. `LoadingSkeleton.jsx` has zero
importers — a regression from three in April. Four `.ts` files totalling 2,125 LOC are
parallel copies of live `.js` files that nothing imports and Vite never resolves.

**May 30–June 1:** the product pivoted. Six `feat(stepdad)` PRs (#276–#290) added a
per-role simple shell (`DadNav`, `isSimpleUser()` → role `contributor`), rebuilt Home for
a 73-year-old, and made `ten-year` the default landing view. The canvas — the plan's
"killer feature" — sits in the drawer under Research, not in the primary nav.

**June–September:** dependency bumps, audit routines, punch-list test coverage. No commit
after 2026-05-31 touches `pwa/src/hooks/`, `pwa/src/stores/`, or `tsconfig.json`.

### The three findings that matter

1. **The event backbone fractured into three systems, and the one the plan designed is
   the one nobody uses.** `events/bus.py` (LISTEN/NOTIFY) is never started —
   `bus.start()` has no production caller, `asyncpg` is not installed, so it degrades to
   an in-memory callback dict. Its eight channels have zero emitters. The *real* backbone
   is `contracts/` — 15 typed Pydantic contracts, 9 producers, a dispatcher with retries,
   dead-lettering, correlation IDs, and an audit table — on `grid_contracts_*` channels
   that the SSE endpoint does not subscribe to. **The SSE stream therefore carries
   nothing but keepalives**, and the frontend hook that would consume it has zero
   importers. Separately, a Redpanda/Kafka producer and consumer were built (violating the
   plan's first non-goal), fed only by Prefect flows with no importers. A fourth,
   table-backed queue lives in `orchestration/event_bus.py`.

2. **The frontend has an adoption problem, not an authoring problem.** Error boundaries
   and store decomposition are genuinely done. Everything else in Vector 5 exists on disk
   with no consumers, while the codebase grew 39% (51.5K → 71.5K LOC), every god component
   got larger, and five new components over 1,300 LOC appeared. `tsconfig.json` has
   `strict: false, checkJs: false`, so the `tsc --noEmit` gate CI runs validates almost
   nothing, while the 12 real vitest suites are never run in CI.

3. **Apache AGE is a ghost.** `store/graph.py` contains a correct Cypher wrapper
   (`GraphStore`) with zero callers. No migration creates the extension or the graph. Two
   sync scripts target a `grid_graph` nothing defines. The canvas traverses
   `actor_connections` with hand-written SQL BFS, the shortest-path endpoint returns
   `"2-hop search not yet implemented"`, and expand depths 4–6 are accepted and inert.
   Meanwhile the parts of `store/graph.py` that *are* used query a relational
   `actor_analytics` table populated by an offline NetworkX job — which works.

### What this revision changes

The April draft measured progress in files created. This revision measures it in
**call sites migrated, channels carrying events, and dead code removed**. Section 5 makes
eight explicit decisions. Section 6 re-scopes the remaining work into phases R0–R5 that
each ship independently. Section 8 adds CI ratchets so the "scaffolded but never adopted"
pattern cannot recur unnoticed.

---

## 2. Scorecard by vector

| # | Vector | April plan | September verdict | Load-bearing evidence |
|---|---|---|---|---|
| 1 | Investigation Canvas | React Flow view ~800 LOC, 5 node types, CRUD API ~400 LOC | **DONE+ on a different stack**, with 1,902 LOC of dead React Flow beside it | `pwa/src/canvas/GothamCanvas.jsx` (Sigma.js); `api/routers/canvas*.py` 5,815 LOC; `components/canvas/*` zero importers |
| 2 | Event-Driven Backbone | LISTEN/NOTIFY bus + SSE, 8 channels | **FRAGMENTED** — 3 systems + Kafka; plan's bus never started; SSE streams keepalives only | `events/bus.py:65-68` asyncpg ImportError branch always taken; `api/main.py:47-123` no `bus.start()`; `contracts/` is the real bus |
| 3 | Graph Query Layer (AGE) | AGE extension, Cypher expand / shortest path / communities | **GHOST** — wrapper exists, zero callers, no migration | `store/graph.py:104-176` Cypher, no `GraphStore(` outside the file; no `CREATE EXTENSION age` repo-wide |
| 4 | Full-Text Search | tsvector + GIN + materialized view + router | **DONE+** — 5-branch MV, triggers, `ts_headline`, tests, DOMPurify | `migrations/versions/phase4_fts_intelligence_search.py`; `api/routers/intelligence_search.py`; `IntelligenceSearch.jsx` 509 LOC |
| 5 | Frontend Modernization | hook, boundaries, stores, TS, decomposition, skeletons | **BUILT-NOT-ADOPTED** — 2 of 6 done; 0 `.tsx`; 2,125 LOC dead `.ts`; god components grew | `useAsyncData.js` 0 importers; `LoadingSkeleton.jsx` 0 importers; `api.ts` 0 vs `api.js` 94 imports |
| 6 | CI/CD + Ops | ci.yml, deploy.yml, asyncpg, JSON logs | **DONE+ on CI; MISSING on asyncpg** | `.github/workflows/test.yml` 3 jobs; `deploy.yml` post-deploy assertions; no `asyncpg` in requirements |
| P6 | Geo-Spatial (optional) | deck.gl globe | **PARTIAL, unplanned route** | `pwa/src/views/GeoFlows.jsx` 393 LOC — deck.gl Arc/Scatter/Heatmap on MapLibre; `Globe.jsx` 735 LOC D3 |

### 2.1 Vector 1 — Canvas, in detail

**Backend (all under `/api/v1/canvas`, mounted from `api/routers/canvas.py:57-61`):**

| File | LOC | What it does | Note |
|---|---|---|---|
| `canvas.py` | 2,147 | Facade; `GET /graph` (SQL BFS over `actor_connections`, depth ≤ 7), board CRUD, `POST /boards/{id}/fork`, `GET /dots` | Owns board CRUD that `canvas_core.py` duplicates |
| `canvas_expand.py` | 1,150 | `POST /boards/{id}/expand/{node}` depth 1–6, `/path`, `/suggest-connections` | Depth is *enrichment tiers*: only `>=2` (`:587`) and `>=3` (`:849`) branches exist; `/path` is a stub (`:1034`) |
| `canvas_board_store.py` | 622 | DDL + bidirectional sync between `investigation_boards.graph_state` JSONB and legacy `canvas_nodes`/`canvas_edges` | Migration debt: two schemas kept in sync |
| `canvas_investigate.py` | 469 | `POST /investigate` — one query → fully built board | Unplanned; collapses the plan's 10-step user flow into one call |
| `canvas_graph.py` | 463 | Node/edge CRUD, bulk `PUT /graph`, evidence pinning | Not a graph-database module despite the name |
| `canvas_core.py` | 393 | Board CRUD | **Dead — intentionally not mounted** (`canvas.py:47-49`: routes collide, "refactor still mid-flight") |
| `canvas_predict.py` | 297 | `POST /predict` — board thesis → `discovered_hypotheses` | Unplanned; closes canvas → prediction journal |
| `canvas_llm.py` | 274 | `POST /explain` — LLM narrative for a connection | Plan's explain endpoint, delivered |

**Persistence:** `investigation_boards` (JSONB `graph_state`, `camera_state`, `filters`,
`pinned_nodes`, `annotations`; **no `user_id`**), legacy `canvas_boards` / `canvas_nodes`
/ `canvas_edges` (Alembic `a1b2c3d4e5f6`), `investigation_evidence` (Alembic
`phase4_investigation_evidence`, richer than planned). The plan's `investigation_nodes`,
`investigation_edges`, and `canvas_alerts` do not exist.

**Frontend, live:** `pwa/src/canvas/` — `GothamCanvas.jsx` 1,548, `panels/DetailPanel.jsx`
1,051, `CanvasStore.js` 429 (Zustand wrapping a graphology `Graph`), `SigmaGraph.jsx`,
`ContextMenu.jsx` (19 actions), `CommunityHulls.jsx`, `TemporalScrubber.jsx`,
`LayerControls.jsx`, `hooks/useCommunities.js`, `hooks/useKeyboardShortcuts.js`. Lenses at
`pwa/src/views/canvas_lenses/` (`CapitalLens.jsx` 668, `SupplyLens.jsx` 394), routed as
`#/canvas/{actorId}/{lens}`. `views/Canvas.jsx` is a 4-line re-export.

**Frontend, dead:** `pwa/src/components/canvas/` (13 files, 1,902 LOC — nine import
`Handle, Position` from `@xyflow/react`; `GothamCanvas.jsx` imports only `EDGE_LEGEND` from
`nodeStyles.js`); `pwa/src/components/SendToCanvas.jsx` (0 importers);
`pwa/src/stores/canvasStore.js` (0 importers; the live store is `canvas/CanvasStore.js`).

**Integration with other views:** deep links only — `ActorProfileDrawer.jsx:14`,
`SectorDive.jsx:43`, `WhyView.jsx:703,781` (→ `/investigate`). The plan's context-menu
actions on MoneyFlow, Timeline, and CrossReference do not exist.

**Navigation:** `routes.js:305` puts canvas in the drawer (`group: 'research'`). Default
landing is `ten-year` (`stores/uiStore.js:8`). Yet `app.jsx:587` sets the error-boundary
escape hatch to `navigate('canvas')` — a crash sends users to a view they cannot reach
from the tab bar.

### 2.2 Vector 2 — Event backbone, in detail

Four event systems coexist:

| System | Files | Mechanism | Producers | Consumers | Status |
|---|---|---|---|---|---|
| Plan's bus | `events/bus.py` (139), `events/channels.py` (52) | LISTEN/NOTIFY *designed*; in-memory dict *in practice* | none on the 8 `grid_*` channels | `api/routers/sse.py` | **PARTIAL** — `bus.start()` never called; `asyncpg` absent |
| Contracts | `contracts/` (schemas, channels, emit, dispatcher, retry_scheduler, dead_letter, correlation, observability, handlers/) | `contracts_audit` table write + `bus.emit_sync` on `grid_contracts_<snake>` | 9 modules (`intelligence/postmortem.py:698`, `chain_contagion.py:621`, `fundamental_divergence.py:642`, `supply_chokepoints.py:507`, `supply_chain_edge_validator.py:313`, `holder_deal_overlap.py:459`, `news_contagion_listener.py:71`, `contagion_backtest.py:54`, `trading/contagion_to_ticket.py:626`) | `ContractsDispatcher` (wired in lifespan, `api/main.py:90-113`) | **DONE, unplanned — the real backbone** |
| Redpanda | `events/producer.py`, `events/consumer.py`, `kafka-python-ng`, `REDPANDA_ENABLED=True` (`config.py:518-519`) | Kafka topics `grid.signals/predictions/alerts/canvas/ingestion` | `orchestration/tasks.py:146-152` via Prefect flows with no importers | none live | **Violates non-goal #1; fallback broken** (`producer.py:167` notifies `grid_<topic>` names not in `ALL_CHANNELS`) |
| Table queue | `orchestration/event_bus.py` (391), `migrations/add_event_bus.sql` | Append-only `event_bus` + `task_queue` tables, priority/claim/retry | — | — | Separate system; the migration the April recon mistook for the bus |

The 15 typed contracts (`contracts/schemas.py`): `PostmortemCompleted`, `PredictionScored`,
`BacktestGateVerdict`, `OptionsTradeOutcome`, `CrossReferenceAnomaly`,
`LeverageRiskUpdate`, `RegimeTransition`, `SignalFired`, `HypothesisGenerated`,
`ActorMaterialized`, `PullLifecycle`, `ForensicsTrace`, `InvestigationProgress`,
`EdgeValidated`. They cover six of the plan's eight channels directly
(actor_update → `ActorMaterialized`, signal_fire → `SignalFired`, regime_change →
`RegimeTransition`, prediction_scored → `PredictionScored`, pull_complete →
`PullLifecycle`, investigation_alert ≈ `InvestigationProgress`). `flow_shift` and
`model_promoted` have no contract.

**SSE:** `api/routers/sse.py` (132 LOC) — `GET /api/v1/events/stream`, `/channels`,
`/topics`; auth via `require_auth` with query-param token fallback (`api/auth.py:232`),
correct for `EventSource`. Subscribes to `events/channels.py:ALL_CHANNELS` only
(`sse.py:38-40`), so it emits the `connected` frame and `: keepalive` every 30 s and
nothing else. `pwa/src/hooks/useEventStream.js` (104 LOC) is a correct client with zero
importers. A *different* SSE endpoint is load-bearing: `/api/v1/dad/ticker/{t}/gold/stream`,
consumed by `api.js:380` and asserted by `deploy.yml`.

### 2.3 Vector 3 — AGE, in detail

- `store/graph.py` (366 LOC): `GraphStore.expand()` `:104`, `.shortest_path()` `:114`,
  `.multi_hop_search()` `:139`, `.community_members()` `:155` — real Cypher over
  `grid_graph`. `grep "GraphStore(" --include=*.py` matches only the file itself.
- The module-level functions that *are* consumed (`intelligence_actors.py:353,377,460,494`)
  are plain SQL over `actor_analytics` (`store/graph.py:191-197, 301-308`), populated by
  `scripts/graph_analytics.py` (NetworkX PageRank + Louvain). This works and is used.
- `scripts/sync_actors_to_age.py`, `scripts/age_fast_sync.py` (the plan's
  `sync_actors_to_graph.py`) target a graph no migration creates. PR #379 (open) patches
  a Cypher-injection escape in the fast sync — effort is still being spent on a ghost.
- `.claude/CODEBASE_INDEX.md:49` states AGE is installed on the server with 4,141 actors
  synced. That is a server-side claim this clone cannot verify; even if true, nothing
  queries it.

### 2.4 Vector 4 — FTS, in detail

DONE+: `intelligence_search` materialized view with five UNION branches (actor, signal,
hypothesis, snapshot, **news** — `phase4_fts_news.py`), per-table GIN indexes, four
`BEFORE INSERT OR UPDATE` triggers, unique index for `REFRESH CONCURRENTLY`, `ts_rank` +
`ts_headline` with `<mark>` snippets, `limit/offset/has_more` (PR #239), `POST /refresh`,
`IntelligenceSearch.jsx` with DOMPurify restricted to `<mark>`, and
`__tests__/intelligenceSearch.test.jsx`.

Gaps: the plan's three target tables (`oracle_predictions`, `decision_journal`,
`thesis_snapshots`) were **not** instrumented; `store/search.py` does not exist (logic is
inline at `intelligence_search.py:62-124`); no `websearch_to_tsquery` (no phrase/boolean
queries); the only scheduled refresh is a Prefect task in `orchestration/tasks.py:130-142`
whose module has no importers outside tests — **the index drifts unless someone calls
`POST /refresh`**; `CommandPalette.jsx` is not wired to FTS; search is a standalone route
(`routes.js:419`), not a canvas panel, though `onAddToCanvas` exists
(`IntelligenceSearch.jsx:275`); `api/routers/search.py` (SQL `LIKE`) coexists under the
same `/api/v1/search` prefix. Counting pgvector RAG (`intelligence/rag.py`), pg_trgm
(`actor_discovery.py`, `icij_linker.py`) and vault FTS (`vault.py:260-283`), GRID has five
disconnected search backends.

### 2.5 Vector 5 — Frontend, in detail

| April measurement | April | September | Direction |
|---|---|---|---|
| `.jsx` files / LOC | 115 / 51.5K | 150 / 71,526 | +30% / +39% |
| `.tsx` files | 0 | 0 | — |
| `.ts` files | 0 | 4 / 2,125 LOC, all orphaned | worse |
| Views | 67 | 71 | +4 |
| Routes | 55 | 61 | +6 |
| Views with error UI | 20 (30%) | 35 (49%) | better, 36 remain |
| `setLoading` boilerplate | 52 | 79 in 67 files | worse |
| Shared `LoadingSkeleton` importers | 3 | 0 | **regressed** |
| Store | 242-LOC monolith | 6 slices, 621 LOC; `store.js` is a 109-LOC facade | **done** |
| Error boundary | 1 app-level | `ViewErrorBoundary` keyed per view, `app.jsx:587` | **done** |
| Test files | 2 | 12 (1,838 LOC) — **not run in CI** | better, ungated |
| ActorNetwork / CrossReference / WatchlistAnalysis / ActorUniverse / Timeline | 1959 / 1922 / 1516 / 1360 / 1144 | 2081 / 1942 / 1524 / 1360 / 1282 | all grew |
| New >1,300-LOC components | — | `ActorProfileDrawer` 2021, `SectorDive` 1793, `TenYearPortfolio` 1678, `EdgeScanner` 1501, `TickerLookup` 1309 | scope doubled |

Dead TypeScript shadow: `api.ts` (1,378) vs live `api.js` (1,556; 94 importers, 0 to `.ts`);
`store.ts` (49) vs `store.js` (21 importers + 1 extensionless `../store` in
`LivingGraph.jsx:18` which Vite resolves to `.js`); `styles/shared.ts` (378) vs `shared.js`
(99 importers); `types/index.ts` (320) referenced only by the two dead `.ts` files. Also
dead: legacy `components/ErrorBoundary.jsx` (0 importers).

Dependency drift: `react ^18.3.0` with `@types/react ^19.2.14`; `vitest ^1.3.0` with
`vite ^8.1.3`; `typescript ^6.0.3`. Open Dependabot PRs #372–#375 touch `@xyflow/react`,
`react`/`@types/react`, `deck.gl`, `vitest`.

### 2.6 Vector 6 — CI/CD, in detail

`test.yml`: `backend-tests` (Postgres 15 service, `pytest --co -q` collection gate then
full run with `--maxfail=50 --timeout=60`, grep-based SQL-injection heuristic),
`frontend-build` (`npm ci` → `npx tsc --noEmit` → `npm run build` → bundle-size report),
`lint` (ruff, bandit blocking at `-lll`, `obsidian_backlinks.py --check`). `deploy.yml`:
`verify` job runs `test_api.py` + `test_pit.py` against a manually pulled Postgres with
retry; `deploy` job on self-hosted `grid-svr` does `git reset --hard`, pip, `npm ci &&
npm run build`, `alembic upgrade head`, systemd restart, then asserts health, process cwd,
that `/gold/stream` is in the shipped bundle, and two `openapi.json` paths.
`claude-code-review.yml` is gated off (`b2b4385`, expired credential).

Gaps: Postgres image is `postgres:15`, not TimescaleDB, so Timescale-dependent tests
cannot run in CI; vitest is never invoked; `asyncpg` is absent from both requirements
files; structured logging exists only for ERROR+ via `server_log/git_sink.py` →
`.server-logs/errors.jsonl` (no `serialize=True` anywhere).

---

## 3. Built since April that the plan never anticipated

| Capability | Where | Why it matters for the rewrite |
|---|---|---|
| **`contracts/` typed event layer** | `contracts/*`, `scripts/migrations/20260411_contracts_infrastructure.sql` | Is the de-facto backbone. Decision D1 promotes it. |
| Canvas → prediction | `api/routers/canvas_predict.py` → `discovered_hypotheses` | Closes the loop the plan left open ("convert thesis to a scored prediction") |
| One-shot investigation | `api/routers/canvas_investigate.py` | Replaces the plan's manual 10-step flow |
| Canvas lenses | `pwa/src/views/canvas_lenses/` | Graph / capital / supply views over one board |
| Temporal scrubber, community hulls, layer controls | `pwa/src/canvas/` | Time-travel and Louvain overlays the plan lacked |
| Suggest-connections, fork, "connect the dots" | `canvas_expand.py:1039`, `canvas.py:1748,1804` | System-proposed edges |
| Per-role simple shell | `pwa/src/authSession.js:57-61`, `components/DadNav.jsx`, `app.jsx:216-321` | A second persona (`contributor`) with an allow-listed view set. **Constraint, not a bug.** |
| `ten-year` default landing, `TenYearPortfolio.jsx` | `stores/uiStore.js:8`, `views/TenYearPortfolio.jsx` (1,678) | The product's front door is no longer an intelligence view |
| Mobile track | `views/mobile/`, `hooks/useDevice.js` (11 consumers) | Third UI axis |
| Geo flows | `views/GeoFlows.jsx` — deck.gl `ArcLayer`/`ScatterplotLayer`/`HeatmapLayer` on MapLibre | Phase 6 started without being scheduled |
| Load-bearing SSE | `/api/v1/dad/ticker/{t}/gold/stream` | The *unplanned* SSE works; the planned one is empty |
| Local-first LLM gate | `GRID_ALLOW_PAID_LLM` (`501ca7a`, `c363c65`) | Any canvas "explain" work must respect it |
| Deploy discipline | `scripts/deploy.py`, `scripts/smoke_endpoints.sh`, `scripts/pre_create_check.py`, `scripts/dispatch_agent.py`, `docs/AGENT_PROMPT_TEMPLATE.md` | Every backend change now has a mandated path to production |
| Orchestration | `orchestration/` (Prefect flows, table event bus) | Mostly unreached; needs a keep/kill decision |

---

## 4. Constraints that changed underneath the plan

1. **Two personas.** `admin` gets the operator cockpit; `contributor` gets `DadNav` and
   `DAD_VIEWS`. The canvas is an operator surface. Nothing in the remaining work may
   change what a `contributor` sees without an explicit product decision.
2. **Default landing is `ten-year`, not an intelligence view.** Promoting the canvas to the
   tab bar is a nav decision for the operator persona only.
3. **Local-first inference.** Paid LLM providers are hard-gated off by default. Canvas
   `/explain` and `/investigate` must work against the local Nemotron stack
   (`llm/router.py`) and degrade gracefully.
4. **Grep-before-create is mechanical policy.** `scripts/pre_create_check.py` before any new
   module; `docs/MODULE_INVENTORY.md` is authoritative (700+ modules); CLAUDE.md's
   "assume it already exists" rule applies to this plan's own manifest.
5. **`deploy.py` + `smoke_endpoints.sh` are the only route to the server.** No plan item is
   "done" until the smoke script exits 0 on `grid-svr`.
6. **The repo's own dead-code and >800-LOC rules** (`docs/MODULE_DEDUPE_PLAN.md:376-385`
   flags `canvas.py` and `canvas_expand.py`; `docs/PUNCH-LIST-2026-05-13.md:58` asks for
   `expand_node` to be split into stage helpers *in the same file*, no new modules).
7. **Shallow history.** Provenance questions about April–May must be answered from
   migration headers or the server, not from `git log` in a fresh clone.

---

## 5. Decisions this revision makes

Each decision names what it settles, why, and what it rules out. Reversing one requires
editing this section, not silently building the alternative.

**D1 — `contracts/` is the event backbone. `events/bus.py` is its transport, not a peer.**
Rationale: contracts has 15 typed schemas, 9 producers, retries, dead-lettering, and an
audit table; the plan's eight `grid_*` channels have zero emitters after five months.
Consequences: SSE subscribes to `contracts.channels.ALL_CHANNELS`; the eight unused
constants in `events/channels.py` are removed once SSE no longer references them (the
`Event` dataclass and `to_sse()` stay — `sse.py` uses them); `events/__init__.py` exports
`bus` and `Event`, not the Kafka surface. Emitters run in **at least two processes**
(`grid-api` and `grid-hermes`; e.g. `intelligence/postmortem.py:698` runs under the Hermes
cycle), so an in-memory bus can never feed SSE — the cross-process leg must actually work.
The transactional guarantee the April plan wanted is delivered by issuing `pg_notify`
**inside** the `engine.begin()` block that already writes `contracts_audit`
(`contracts/emit.py:69`), not by a separate connection.

**D2 — Redpanda/Kafka is removed.** `events/producer.py`, `events/consumer.py`,
`kafka-python-ng`, `REDPANDA_*` settings, and `GET /api/v1/events/topics` go. Its only
producer is a Prefect task in a module nothing imports; its `pg_notify` fallback emits on
channel names no listener knows. This restores the plan's first non-goal.

**D3 — The Sigma.js `GothamCanvas` is the canvas. The React Flow implementation is
deleted.** Twelve files in `pwa/src/components/canvas/` (1,676 LOC), `pwa/src/stores/
canvasStore.js`, and the `@xyflow/react` dependency are removed. `nodeStyles.js` (226 LOC,
three live importers) moves to `pwa/src/canvas/`. `SendToCanvas.jsx` is **kept and
rewritten** to the deep-link pattern the live canvas actually uses
(`#/canvas/{actorId}/{lens}`), then wired (R3). Dependabot #372 (`@xyflow/react` bump)
becomes obsolete and should be closed, not merged.

**D4 — Apache AGE is dormant, not dead, with explicit revival triggers.** No repo
migration creates it, no code queries it, and the relational BFS plus offline NetworkX
analytics serve today's canvas. `GraphStore` and the two sync scripts get a `DORMANT`
docstring banner (no deletion — PR #379 is open against `age_fast_sync.py`). The API
stops lying: `POST /boards/{id}/path` returns HTTP 501 with the honest message instead of
a 200 with `degrees: -1`; `expand` clamps `depth` to `le=3` to match the tiers that exist.
Revive AGE only when (a) an interactive query needs more than three hops or a constrained
shortest path that BFS cannot serve within the smoke-test budget, or (b) `actor_connections`
grows past what SQL BFS handles in that budget. Either trigger reopens Vector 3 as a
scheduled item, starting with the missing `CREATE EXTENSION` migration.

**D5 — Delete the TypeScript shadow first; turn on checking second; rename files last.**
`api.ts`, `store.ts`, `styles/shared.ts`, `types/index.ts` (2,125 LOC, zero importers,
never bundled) are removed. Then `checkJs` is enabled per directory using `// @ts-check`
pragmas on files as they are touched. No `.jsx → .tsx` rename happens on a file until
`checkJs` passes for it. The `@types/react` 19 vs `react` 18 mismatch is fixed in the same
change that enables checking, because until then `tsc` output is noise either way. React 19
itself (Dependabot #374) is a separate decision with its own test pass.

**D6 — Adoption is measured in call sites and enforced by CI ratchets.** The plan's
"create `useAsyncData`" is done and achieved nothing. The unit of work becomes "migrate
view X", and a ratchet script fails CI if any adoption counter moves backward (Section 8).
Vitest joins CI; the Postgres service becomes the TimescaleDB image so Timescale-dependent
tests stop being silently unrunnable.

**D7 — Real-time canvas is delivered on contracts, client-side first.** Wiring
`useEventStream` into `GothamCanvas` and highlighting nodes whose `actor_id`/`ticker`
appears in an `ActorMaterialized`, `SignalFired`, `PredictionScored`, or `RegimeTransition`
payload delivers the plan's Phase 5 with **zero new tables**. Persisted per-board watches
(the plan's `POST /canvas/alert`) come after, only if the client-side version proves
useful. `scripts/pre_create_check.py "canvas alert"` reports no existing coverage.

**D8 — Persona surfaces are frozen without operator sign-off.** `Home.jsx`,
`TenYearPortfolio.jsx`, `DadNav.jsx`, `views/mobile/`, and anything in `DAD_VIEWS` are not
touched by the adoption campaign or decomposition work unless the operator explicitly
includes them. The error-boundary escape hatch (`app.jsx:587`, currently
`navigate('canvas')`) is changed to the persona's default view. Promoting the canvas to
the operator tab bar is **recommended** (it is the plan's headline feature and currently
lives in a drawer) but is a one-line nav change that the operator approves in review, not
one this document makes unilaterally.

**D9 — Prefect `orchestration/` gets a keep-or-kill decision in R4, not now.** Its flows
have no importers, but `orchestration/tasks.py:130-142` is the only place the FTS
materialized view is refreshed on a schedule. R4 moves that refresh to the live scheduler;
after that, `orchestration/flows.py` and `tasks.py` have no remaining purpose and the
decision becomes trivial. `orchestration/event_bus.py` (table queue) is audited for
callers in the same pass.

---

## 6. Remaining work — phases R0 to R5

Every phase ships on its own and leaves `main` working. R0 is prerequisite for all;
R1, R2, and R3 are mutually independent after R0; R4 is independent after R0; R5 is
continuous. Sizes are working days for one engineer or one agent wave.

### R0 — Truth and cleanup (1–2 days). No user-visible behaviour change.

| # | Item | Files | Verification |
|---|---|---|---|
| R0.1 | Delete the TypeScript shadow | `pwa/src/api.ts`, `store.ts`, `styles/shared.ts`, `types/index.ts` | `grep -r` shows 0 importers before; `npm run typecheck && npm run build` after |
| R0.2 | Delete the React Flow implementation | 12 files in `pwa/src/components/canvas/` (all but `nodeStyles.js`); `pwa/src/stores/canvasStore.js`; `pwa/src/components/ErrorBoundary.jsx` | Same; `npm run test` (`canvasStore.test.js` imports the *live* store, unaffected) |
| R0.3 | Move `nodeStyles.js` → `pwa/src/canvas/nodeStyles.js` | 3 import paths (`GothamCanvas.jsx`, `IntelligenceSearch.jsx`, one more per grep) | Build |
| R0.4 | `npm uninstall @xyflow/react` | `pwa/package.json`, lockfile | Build; close Dependabot #372 as obsolete |
| R0.5 | CI runs the frontend tests; Postgres service → TimescaleDB image | `.github/workflows/test.yml`: add `npm run test` between `TypeScript check` and `Build` (line 113–119); `services.postgres.image: timescale/timescaledb:latest-pg15` (line 20) | CI green on the PR |
| R0.6 | Add the adoption ratchet | `scripts/pwa_ratchet.py` (new — `pre_create_check "ratchet"` reports no coverage) + a CI step in `frontend-build` | Script exits 0 on current tree; fails when a counter regresses (Section 8) |
| R0.7 | Honest graph API | `api/routers/canvas_expand.py:222` `le=6` → `le=3`; `:955-1034` `/path` → `HTTPException(501, ...)` | `tests/test_canvas*` + new test asserting 501 |
| R0.8 | `DORMANT` banners on AGE ghosts | `store/graph.py` (class docstring only), `scripts/sync_actors_to_age.py`, `scripts/age_fast_sync.py` | Docstring-only; no behaviour |
| R0.9 | Escape hatch → persona default | `pwa/src/app.jsx:587` | `routes.test.js` still green; manual check for `contributor` role |
| R0.10 | Correct the auto-loaded index | `.claude/CODEBASE_INDEX.md` "Canvas & Graph" section | Done in this revision (see Appendix C) |

### R1 — One event backbone, and SSE that carries it (2–3 days)

| # | Item | Files | Verification |
|---|---|---|---|
| R1.1 | Transactional, cross-process emit | `contracts/emit.py`: inside the existing `engine.begin()` (`:69`), `SELECT pg_notify(:channel, :payload)` with an 8,000-byte guard (port the truncation logic from `events/producer.py:153-161` before deleting that file) | Unit test with mock engine asserts the notify statement is issued in the same transaction |
| R1.2 | API listens | `api/main.py` lifespan: `await bus.start(settings.DB_URL)` / `await bus.stop()`; add `asyncpg>=0.29` to `requirements.txt` **for LISTEN only** — the general async pool (April Vector 6B) stays deferred | `tests/test_event_bus.py` extended: `start()` against a mock; lifespan test |
| R1.3 | SSE subscribes to contracts | `api/routers/sse.py:38-40`: `ALL_CHANNELS` becomes `events.channels.ALL_CHANNELS + contracts.channels.ALL_CHANNELS` (then only the latter after R1.5); `/channels` lists both | Streaming test: emit `SignalFired` via `contracts.emit` → frame appears on `/api/v1/events/stream` |
| R1.4 | Remove Redpanda | `events/producer.py`, `events/consumer.py`, `orchestration/tasks.py:146-152` `emit_event`, `config.py:518-519`, `requirements.txt` `kafka-python-ng`, `sse.py:102-131` `/topics`, `events/__init__.py` exports | `pytest tests/test_event_bus.py`; grep for `kafka`/`REDPANDA` returns nothing |
| R1.5 | Retire the eight dead channels | `events/channels.py:15-33` constants and `ALL_CHANNELS`; keep `Event`, `to_sse()` | `sse.py` and tests import nothing removed |
| R1.6 | Canvas goes live | `pwa/src/canvas/GothamCanvas.jsx` imports `hooks/useEventStream.js`; `CanvasStore.js` gains `markActivity(entityId, event)`; `SigmaGraph.jsx` pulses matching nodes; `DetailPanel.jsx` shows recent activity | vitest: store reducer maps a `grid_contracts_signal_fired` payload to the right node id; manual: open a board, fire a signal, watch the node pulse |
| R1.7 | Persisted watches (optional, after R1.6 proves useful) | Migration `canvas_watches(id SERIAL, board_id, entity_type, entity_id, created_at)` with the `_TEMPLATE.sql` GRANT footer; `POST/DELETE /boards/{id}/watch`; dispatcher handler emits `InvestigationProgress` when a watched entity appears in any contract | `tests/test_canvas_watch.py`; `smoke_endpoints.sh` |

Deliverable: the April plan's Phase 5 ("nodes update in real time as new data arrives"),
built on the backbone that already has producers.

### R2 — Frontend adoption campaign (3–5 days, parallel by view)

Mechanical, one PR per ~10 views, ordered by user impact. Per view:

1. Replace `useState(loading)` / `useState(error)` / `useEffect` + `try/catch` with
   `const { data, loading, error, refetch } = useAsyncData(() => api.x(), { fallback })`
   (`pwa/src/hooks/useAsyncData.js` — API is `fetcher, { fallback, deps, skip }`).
2. Render `<LoadingSkeleton variant="chart" | "card" | "text" count={n} />`
   (`pwa/src/components/LoadingSkeleton.jsx`) while `loading && !data`.
3. Render a shared error block with a retry that calls `refetch`. If no shared error
   component exists at execution time, create one small `components/ErrorState.jsx`; do
   not create a per-view variant.
4. Confirm the ratchet counters moved the right way.

Order: the **36 views with no error UI** first, then the remaining `setLoading` sites.
**Excluded without sign-off (D8):** `Home.jsx`, `TenYearPortfolio.jsx`, `views/mobile/*`,
`DadNav.jsx`, anything in `DAD_VIEWS`.

Done when: `setLoading` occurrences < 10 (from 79), every operator view has error UI,
`LoadingSkeleton` importers ≥ 40 (from 0).

### R3 — Canvas integration and backend consolidation (2–3 days)

| # | Item | Files | Verification |
|---|---|---|---|
| R3.1 | `SendToCanvas` rewritten to deep links and wired | `pwa/src/components/SendToCanvas.jsx`; call sites in `ActorNetwork.jsx`, `MoneyFlow.jsx`, `Timeline.jsx`, `CrossReference.jsx` (the plan's four) | vitest for the link builder; `routing.test.js` |
| R3.2 | Search inside the canvas | `GothamCanvas.jsx` hosts `IntelligenceSearch` as a panel; `onAddToCanvas` (`IntelligenceSearch.jsx:275`) pins via existing `POST /boards/{id}/nodes/{node}/evidence` | Existing `intelligenceSearch.test.jsx` + a pin test |
| R3.3 | Command palette → FTS | `pwa/src/components/CommandPalette.jsx` adds an "Intelligence" result group calling `/api/v1/search/intelligence` | vitest |
| R3.4 | Canvas in the operator tab bar | `pwa/src/routes.js:305` `nav: 'drawer'` → `'tab'` for the operator persona only | **Operator approves in review (D8)** |
| R3.5 | Finish the `canvas_core` extraction | Move board CRUD out of `api/routers/canvas.py` (2,147 LOC) into `canvas_core.py`, mount it at the facade (`canvas.py:50-61`), delete the duplicates; split `expand_node` (737 LOC) into `_collect_neighbor_actors` / `_attach_signals` / `_attach_evidence` / `_build_layout` **in the same file** per `PUNCH-LIST-2026-05-13.md:58` | `tests/test_canvas*`; `scripts/smoke_endpoints.sh` exits 0 after `deploy.py --smoke` |
| R3.6 | Collapse the dual board schema | Decide `investigation_boards.graph_state` JSONB vs `canvas_nodes`/`canvas_edges` as the source of truth; remove `canvas_board_store.py`'s bidirectional sync (622 LOC) once one side is authoritative | Data migration + tests; **needs server DB inspection first** |

### R4 — FTS completion (1–2 days)

| # | Item | Files | Verification |
|---|---|---|---|
| R4.1 | Scheduled refresh on the live scheduler | Hermes cycle (`scripts/hermes_operator.py`) or `ingestion/scheduler.py` calls the refresh function already behind `POST /refresh` in `api/routers/intelligence_search.py:156` every 6 h (`pre_create_check` confirms that is the existing coverage — extend it, do not duplicate the SQL) | Log line per refresh; `POST /refresh` stays as manual override |
| R4.2 | Add the plan's three sources as MV branches | `oracle_predictions`, `decision_journal`, `thesis_snapshots` join the `UNION` in a new Alembic revision. **`decision_journal` gets no `search_vector` column and no trigger** — an `UPDATE` trigger on it would violate the journal-immutability rule (CLAUDE.md); index it only through the view | Migration lint (GRANT footer); `test_intelligence_search.py` |
| R4.3 | Phrase and boolean queries | `intelligence_search.py:104`: `websearch_to_tsquery` with `plainto_tsquery` fallback on syntax error | Unit tests for both paths |
| R4.4 | Fix the dead assertion | `tests/test_intelligence_search.py:477` `assert True` → let the refresh raise (punch-list item) | Test still passes against Postgres |
| R4.5 | Prefect keep-or-kill (D9) | After R4.1, `orchestration/flows.py` and `tasks.py` have no purpose; remove them and `prefect` from requirements unless a caller is found; audit `orchestration/event_bus.py` callers | grep; pytest collection |
| R4.6 | Document the two search routers | `api/routers/search.py` (navigation: views, tickers, sectors) vs `intelligence_search.py` (corpus FTS). Keep both; state the split in each module docstring. Extract `store/search.py` **only** if a second caller of the query builder appears | — |

### R5 — Component decomposition (continuous)

A size ratchet (Section 8) stops any file from growing past its current LOC and caps new
files at 800. Decomposition happens when a file is touched for another reason, largest
and most-churned first: `ActorNetwork.jsx` 2,081 → the April plan's six-file split
still applies; `ActorProfileDrawer.jsx` 2,021; `CrossReference.jsx` 1,942;
`SectorDive.jsx` 1,793; `GothamCanvas.jsx` 1,548; `api.js` 1,556 (split by domain, keep
the singleton); `WatchlistAnalysis.jsx` 1,524; `EdgeScanner.jsx` 1,501;
`ActorUniverse.jsx` 1,360; `TickerLookup.jsx` 1,309; `Timeline.jsx` 1,282.
`TenYearPortfolio.jsx` (1,678) is a persona surface — D8 applies.

### Deferred, with the trigger that reopens each

| Item | Why deferred | Reopens when |
|---|---|---|
| Apache AGE (Vector 3) | Zero callers; BFS + NetworkX serve today's canvas | D4 triggers (a) or (b) |
| General `asyncpg` pool (April 6B) | No measured event-loop blocking; `run_in_executor` works | A profile shows DB waits stalling the loop. R1.2 adds `asyncpg` for LISTEN only |
| `.jsx → .tsx` renames (April 5D) | `checkJs` is off; renames would add noise, not safety | `checkJs` passes on the directory in question |
| Geo layer (April Phase 6) | `GeoFlows.jsx` (393 LOC, deck.gl on MapLibre, three `/api/v1/geo/*` endpoints) already exists | Assess data quality behind the three endpoints; no new build is scheduled |
| React 19 (Dependabot #374) | Major upgrade, unrelated to v5 | Its own test pass |
| Multi-user boards (`user_id` on `investigation_boards`) | Single-operator system; schema has no `user_id` | Q4 multi-tenant decision in `ROADMAP.md` |

### Effort

| Phase | Days | Parallel with |
|---|---|---|
| R0 | 1–2 | — (prerequisite) |
| R1 | 2–3 | R2, R3, R4 |
| R2 | 3–5 (by view) | R1, R3, R4 |
| R3 | 2–3 | R1, R2 |
| R4 | 1–2 | R1, R2, R3 |
| R5 | continuous | everything |
| **Total** | **~2–3 weeks** with R2 running alongside R1/R3 | |

---

## 7. Dependency graph

```
R0  Truth & cleanup
 ├─ R0.1–R0.4  delete TS shadow, React Flow set, @xyflow ──┐
 ├─ R0.5–R0.6  vitest in CI, ratchet script ───────────────┤
 ├─ R0.7–R0.8  honest graph API, DORMANT banners ──────────┤
 └─ R0.9–R0.10 escape hatch, index correction ─────────────┤
                                                           │
        ┌──────────────────┬───────────────────┬───────────┴───────┐
        ▼                  ▼                   ▼                   ▼
R1  Event backbone    R2  Adoption         R3  Canvas           R4  FTS
 ├─ R1.1 pg_notify     campaign             integration          completion
 │   in emit.py         (by view,            ├─ R3.1 SendToCanvas ├─ R4.1 scheduled
 ├─ R1.2 bus.start      ratchet-             ├─ R3.2 search panel │   refresh
 ├─ R1.3 SSE ←          enforced)            ├─ R3.3 palette      ├─ R4.2 +3 sources
 │   contracts                               ├─ R3.4 tab bar (D8) ├─ R4.3 websearch
 ├─ R1.4 rm Redpanda                         ├─ R3.5 core extract ├─ R4.4 assert True
 ├─ R1.5 rm 8 channels                       └─ R3.6 one schema   ├─ R4.5 Prefect (D9)
 ├─ R1.6 canvas live ◄── needs R3.2? no ──   (needs server DB)    └─ R4.6 docstrings
 └─ R1.7 watches (opt)

R5  Decomposition — continuous, gated by the size ratchet from R0.6
```

Hard edges: everything depends on R0 (the ratchet must exist before adoption work, or
progress cannot be measured; the dead code must be gone before anyone "fixes" it).
R1.6 needs only R1.3 and R0.2. R3.6 needs server DB inspection and is the only item that
cannot be verified from a clone.

---

## 8. Verification and CI ratchets

The April plan had no mechanism to notice that its foundation was never adopted. This
section adds one.

**`scripts/pwa_ratchet.py`** (R0.6) counts four things over `pwa/src/` and compares them
to `scripts/pwa_ratchet_baseline.json`, exiting 1 on any regression:

| Counter | Baseline (2026-09-09) | Direction | Done target |
|---|---|---|---|
| `setLoading` occurrences in `views/` | 79 | must not rise | < 10 |
| Views with no `error` rendering | 36 | must not rise | 0 (operator views) |
| Files importing `components/LoadingSkeleton` | 0 | must not fall | ≥ 40 |
| Files over 800 LOC, and each file's own LOC | 21 views; per-file map | no file may grow; count must not rise | new files ≤ 800 |

`--update-baseline` lowers thresholds after real progress; CI never raises them. The
script runs in `frontend-build` after `npm run test`.

**What "done" means per phase:**

| Phase | Locally verifiable | Requires server |
|---|---|---|
| R0 | `npm run typecheck && npm run test && npm run build`; `pytest tests/test_canvas* tests/test_event_bus.py`; ratchet exits 0 | — |
| R1 | pytest for emit/listen/SSE with mock engine; vitest for the store reducer | Watching a node pulse on a live board (`deploy.py --smoke`) |
| R2 | Ratchet counters move; vitest | Visual spot-check |
| R3 | vitest; pytest canvas tests | `smoke_endpoints.sh` exit 0; R3.6 needs DB inspection |
| R4 | pytest against CI Postgres (Timescale image after R0.5) | Refresh log line on `grid-svr` |
| R5 | Size ratchet | — |

**Deployment path is unchanged and mandatory:** `python3 scripts/deploy.py --snapshot
--restart --smoke <files>`; raw `scp`/`rsync` is forbidden (`docs/AGENT_PROMPT_TEMPLATE.md`).
Backend agent prompts go through `scripts/dispatch_agent.py` so `pre_create_check` output
is embedded. This document's own new-file proposals were checked on 2026-09-09: `"canvas alert"` and
`"ratchet"` report no existing coverage. `"watch"` reports coverage in
`api/routers/watchlist_core.py` — a keyword collision with the watchlist feature, not the
canvas watch of R1.7; re-run as `"canvas watch"` at execution time and record the result.
`"materialized view refresh"` points at `api/routers/intelligence_search.py` (the manual
`POST /refresh`), which is why R4.1 reuses that function rather than writing a second one.

**Local verification limits in a fresh clone:** no Postgres, no Docker. Tests that need
`pg_engine` skip; everything else runs after `pip install` of the test toolchain and
`npm ci`. Any claim of a passing full suite must come from CI or the server, and this
document says which.

---

## 9. Non-goals (revised)

1. **No Kafka/Redpanda** — and the one that was built is removed (D2).
2. **No Neo4j.** AGE stays the only graph candidate, and it is dormant (D4).
3. **No Elasticsearch.** Postgres FTS is done and exceeds the plan.
4. **No Redis.** Not needed; nothing in R0–R5 introduces one.
5. **No second canvas stack.** Sigma.js + graphology is the canvas (D3).
6. **No TypeScript rewrite, and no renames before checking is on** (D5).
7. **No changes to persona surfaces without operator sign-off** (D8).
8. **No new module when `pre_create_check` finds coverage.** Extend and wire.
9. **No "done" without a call site.** Creating a hook, store, or component counts for
   nothing until the ratchet shows adoption.

---

## Appendix A — Dead-code inventory (input to R0)

| Path | LOC | Importers | Action |
|---|---|---|---|
| `pwa/src/api.ts` | 1,378 | 0 (`api.js` has 94) | delete |
| `pwa/src/store.ts` | 49 | 0 | delete |
| `pwa/src/styles/shared.ts` | 378 | 0 (`shared.js` has 99) | delete |
| `pwa/src/types/index.ts` | 320 | only the two files above | delete |
| `pwa/src/components/canvas/ActorNode.jsx` | 81 | 0 | delete |
| `pwa/src/components/canvas/CanvasContextMenu.jsx` | 273 | 0 (live one is `canvas/ContextMenu.jsx`) | delete |
| `pwa/src/components/canvas/ChartNode.jsx` | 93 | 0 | delete |
| `pwa/src/components/canvas/CompanyNode.jsx` | 28 | 0 | delete |
| `pwa/src/components/canvas/EvidenceNode.jsx` | 50 | 0 | delete |
| `pwa/src/components/canvas/HypothesisNode.jsx` | 34 | 0 | delete |
| `pwa/src/components/canvas/IntelFeed.jsx` | 478 | 0 (named only in a `GothamCanvas.jsx:8` comment) | delete; the idea is R1.6's activity feed |
| `pwa/src/components/canvas/NewsNode.jsx` | 100 | 0 | delete |
| `pwa/src/components/canvas/NoteNode.jsx` | 59 | 0 | delete |
| `pwa/src/components/canvas/PredictionModal.jsx` | 379 | 0 | delete; `POST /predict` UI belongs in `DetailPanel.jsx` |
| `pwa/src/components/canvas/SignalNode.jsx` | 35 | 0 | delete |
| `pwa/src/components/canvas/TimelineNode.jsx` | 66 | 0 | delete |
| `pwa/src/components/canvas/nodeStyles.js` | 226 | **3** | **keep**, move to `pwa/src/canvas/` |
| `pwa/src/stores/canvasStore.js` | 102 | 0 (live store is `canvas/CanvasStore.js`) | delete |
| `pwa/src/components/ErrorBoundary.jsx` | 55 | 0 (superseded by `ViewErrorBoundary.jsx`) | delete |
| `pwa/src/components/SendToCanvas.jsx` | ~180 | 0 | **keep**, rewrite in R3.1 |
| `pwa/src/hooks/useEventStream.js` | 104 | 0 | **keep**, wire in R1.6 |
| `pwa/src/hooks/useAsyncData.js` | 72 | 0 | **keep**, adopt in R2 |
| `pwa/src/components/LoadingSkeleton.jsx` | 91 | 0 | **keep**, adopt in R2 |
| `@xyflow/react` (dependency) | — | only the deleted files | `npm uninstall` |
| `api/routers/canvas_core.py` | 393 | not mounted (`canvas.py:47-49`) | finish extraction in R3.5, do not delete |
| `events/producer.py`, `events/consumer.py` | — | Prefect tasks only (no importers) | delete in R1.4 |
| `events/channels.py` eight `grid_*` constants | — | `sse.py` only | remove in R1.5 |
| `store/graph.py::GraphStore`, `scripts/sync_actors_to_age.py`, `scripts/age_fast_sync.py` | 366 + 2 scripts | 0 | DORMANT banner (D4); PR #379 open |
| `orchestration/flows.py`, `orchestration/tasks.py` | — | tests only | decide in R4.5 |

Frontend totals removable in R0: **4,251 LOC** (2,125 TypeScript + 1,676 React Flow +
102 store + 55 boundary + the dependency).

## Appendix B — Provenance

- April plan: authored 2026-04-08 in session `finance-visualization-stack-tpDSI`; PR #21
  closed unmerged; file reached `main` via daemon commit `998952c` (2026-05-31).
- Phases 1–3 build: Alembic `Create Date: 2026-04-08` on `phase4_fts_intelligence_search`
  and `a1b2c3d4e5f6_canvas_tables`; `.claude/CODEBASE_INDEX.md` dated 2026-04-13 already
  describes them. Revision labels `phase4_*` show the numbering drifted from this plan.
- Frontend foundation: single commit `998952c`, unreviewed, never integrated.
- Persona pivot: PRs #276–#290 (2026-05-30 to 2026-06-01).
- Clone limits: shallow (174 commits, oldest 2026-05-29); no Postgres/Docker; server
  state unverifiable. Every "0 importers" claim above was made by `grep` over `pwa/src`
  on 2026-09-09 and should be re-run immediately before each deletion.

## Appendix C — Companion documents changed in this revision

- `docs/planning/ROADMAP.md`: addendum under the header and a "V5 Transformation —
  verified status" section before "WIRING GAPS". The March snapshot is left as history.
- `.claude/CODEBASE_INDEX.md`: the "Canvas & Graph (V5 Phase 1-3)" section is corrected —
  it told every session the canvas was React Flow with `stores/canvasStore.js` and that
  AGE was live. Regenerate with `/grid-orient` when server access allows; until then the
  hand correction stands.
- `docs/planning/archive/V5-TRANSFORMATION-2026-04-08.md`: the original, verbatim.
