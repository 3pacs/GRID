# Session Handoff - 2026-04-18

**Current local repo:** `/Users/anikdang/.codex/worktrees/540f/GRID`  
**Current branch:** `codex/edge-scanner-reload-guard`  
**Current head:** latest local commit on `codex/edge-scanner-reload-guard` at handoff time; replay reconnect buffering is included.  
**GitHub state at handoff:** PR #41 was merged on 2026-04-19 UTC, `main` was fast-forwarded locally, the old feature branch was deleted locally and remotely, and draft PR #45 now carries the follow-on hardening work from `codex/edge-scanner-reload-guard`.  
**Scope:** Edge Scanner hardening, real-data-only market-edge ranking, laggard downgrade logic, mobile-readability cleanup, route-level drill-throughs into downstream modules, watchlist-analysis fallback coverage for unsaved tickers, options recommendation graceful degradation, and auth dependency cleanup.  
**Result:** Edge Scanner is materially tighter and now routes directly into the right downstream module with seeded ticker context. The scanner can drill into watchlist analysis, options, influence, timeline, and catalyst timeline without hitting dead-end links or transport errors. Unsaved but valid lead tickers now load cleanly, persisted options recommendations degrade cleanly when the live recommender is unavailable, and the final browser verification for the exposed GD drill-through path finished with `0` console errors and `200` responses across the page dependencies. After that, PR #41 was merged and a follow-on regression branch was cut to lock down the login -> edge-scanner -> reload flow in automated frontend tests. The latest passes also reduce idle `/ws` churn by keeping the socket on live views only while the document is visible, preserving reconnect backoff across failed handshakes, refreshing live snapshots after reconnect, hardening backend broadcast fanout against client-set mutation during reconnect churn, and replaying missed non-price realtime events after reconnect through a bounded recent-event buffer.

---

## Start Here

Use the worktree:

```bash
cd /Users/anikdang/.codex/worktrees/540f/GRID
git status --short
git fetch origin
git rev-parse HEAD origin/codex/edge-scanner-reload-guard origin/main
```

Local dev services at handoff:

```bash
# Both local services were intentionally stopped after PR #41 merged.
# Restart only if you need a fresh browser verification pass.
```

Quick health checks after restart:

```bash
lsof -nP -iTCP:4173 -sTCP:LISTEN
lsof -nP -iTCP:8000 -sTCP:LISTEN
curl -s http://127.0.0.1:4173/#/edge-scanner >/dev/null
```

---

## What Shipped

### Edge Scanner backend

- Added [api/routers/intelligence_edges.py](/Users/anikdang/.codex/worktrees/540f/GRID/api/routers/intelligence_edges.py).
- Wired it into [api/routers/intelligence.py](/Users/anikdang/.codex/worktrees/540f/GRID/api/routers/intelligence.py).
- Added [intelligence/market_edge_scanner.py](/Users/anikdang/.codex/worktrees/540f/GRID/intelligence/market_edge_scanner.py).

Behavioral changes:

- New endpoint: `GET /api/v1/intelligence/edges?limit=N`
- Real-data-only edge feed. No synthetic fallback opportunities.
- Company-only ticker targeting. Broad ETF and proxy junk is filtered out.
- Sector-specific playbooks now score off actual live clue families and named-company breadth.
- Weak setups are penalized and labeled `tight`, `mixed`, or `lagging`.
- Each setup now carries:
  - `decision_window`
  - `driver_stack`
  - `confirmation_board`
  - `stakes`
  - `lagging_factors`
  - `upgrade_trigger`
  - `quality_label`

Observed live ranking at handoff:

- `active`: 4
- `arming`: 2
- `watch`: 4
- `live_count`: 10
- `coverage_gap_count`: 0
- `top_setup`: `defense-procurement-stack`

Notable laggard behavior:

- `homebuilder-policy-ladder` remains `arming` but `lagging`
- `nuclear-fuel-policy` downgraded to `watch`
- `tax-admin-software` downgraded to `watch`
- `aviation-certification-cycle` downgraded to `watch`
- `healthcare-policy-pressure` downgraded to `watch`

### Edge Scanner frontend

- Added [pwa/src/views/EdgeScanner.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/EdgeScanner.jsx).
- Wired routing in [pwa/src/routes.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/routes.js).
- Added API calls in [pwa/src/api.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/api.js) and [pwa/src/api.ts](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/api.ts).
- Proxy support for local API/WebSocket paths is in [pwa/vite.config.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/vite.config.js).

UX changes:

- Page copy is cleaner and less internal-jargon-heavy.
- Mobile layout holds up across dense cards.
- New plain-English labels:
  - `Last Proof`
  - `Need More By`
  - `Wrong If Quiet`
  - `Names Carrying It`
  - `Proof Types`
  - `Held`
  - `No Print`
  - `Open`
- Users can see:
  - what is driving a setup
  - what is dragging it
  - what would upgrade it
  - when confirmation should land
  - when silence should be treated as negation

### Priority rail

Files touched:

- [pwa/src/views/EdgeScanner.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/EdgeScanner.jsx)

Behavioral changes:

- Added a top-of-page `Move First` rail above the filters and main card stack.
- Rail shows the top 3 setups with:
  - current status
  - quality label
  - expected edge
  - primary trigger
  - `Act By`
  - names breadth
  - `Why It Moves`
  - `Watch Closely`
- Rail CTAs route directly into the relevant downstream module view.
- Layout holds at mobile width with no horizontal overflow.

### Downstream drill-throughs

Files touched:

- [pwa/src/routing.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/routing.js)
- [pwa/src/app.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/app.jsx)
- [pwa/src/views/EdgeScanner.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/EdgeScanner.jsx)
- [pwa/src/views/Options.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/Options.jsx)
- [pwa/src/views/Timeline.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/Timeline.jsx)
- [pwa/src/views/CatalystTimeline.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/CatalystTimeline.jsx)
- [pwa/src/views/InfluenceNetwork.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/InfluenceNetwork.jsx)

Behavioral changes:

- Confirmation rows inside each Edge Scanner card are now actionable instead of dead text.
- Route selection is source-aware:
  - `Gov Contracts`, `Influence Loops`, `Congressional` -> `#/influence`
  - `Options Flow`, `Export Controls` -> `#/options?ticker=...`
  - `Legislation` -> route hint, usually `#/catalyst-timeline?ticker=...`
  - `Breadth` -> `#/watchlist/<ticker>?from=edge-scanner`
  - `Negation Risk` -> route hint for the parent playbook
- Ticker-aware routes now preserve `from=edge-scanner` and seed the downstream page with the lead ticker.
- `Options`, `Timeline`, `Catalyst Timeline`, and `Influence Network` consume the seeded ticker and land in the right tab/state on first render.

### Watchlist and options fallbacks

Files touched:

- [api/routers/watchlist_analysis.py](/Users/anikdang/.codex/worktrees/540f/GRID/api/routers/watchlist_analysis.py)
- [api/routers/options.py](/Users/anikdang/.codex/worktrees/540f/GRID/api/routers/options.py)
- [pwa/src/views/WatchlistAnalysis.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/WatchlistAnalysis.jsx)
- [pwa/src/views/Options.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/Options.jsx)
- [tests/test_drillthrough_fallbacks.py](/Users/anikdang/.codex/worktrees/540f/GRID/tests/test_drillthrough_fallbacks.py)

Fixes:

- `GET /api/v1/watchlist/{ticker}/analysis` no longer hard-fails for real tickers that are not saved on the watchlist yet.
- Unsaved tickers now get a real-data analysis page with a synthesized watchlist shell only for display metadata:
  - `watchlist_saved: false`
  - `display_name` pulled from the market universe when available
  - `asset_type` inferred from ticker conventions
- `GET /api/v1/options/recommendations` and `/refresh` now fall back to persisted open recommendations instead of returning `501` when the optional recommender module is unavailable.
- The frontend now treats API error envelopes as errors instead of trusting them as payloads.
- Options history cards now consume the backend’s `history` field correctly.

### WebSocket stabilization

Files touched:

- [pwa/src/app.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/app.jsx)
- [pwa/src/api.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/api.js)
- [pwa/src/api.ts](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/api.ts)
- [pwa/src/hooks/useWebSocket.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/hooks/useWebSocket.js)
- [pwa/src/store.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/store.js)
- [pwa/src/stores/realtimeStore.js](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/stores/realtimeStore.js)
- [pwa/src/views/Agents.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/Agents.jsx)
- [pwa/src/views/Dashboard.jsx](/Users/anikdang/.codex/worktrees/540f/GRID/pwa/src/views/Dashboard.jsx)
- [api/main.py](/Users/anikdang/.codex/worktrees/540f/GRID/api/main.py)
- [tests/test_api.py](/Users/anikdang/.codex/worktrees/540f/GRID/tests/test_api.py)

Fixes:

- Intentional socket closes no longer schedule stale reconnects.
- Reconnect timers are cancelled correctly on disconnect/reconnect.
- The dashboard hook no longer opens its own second competing socket.
- Backend WebSocket rate-limit threshold was raised to tolerate normal reload/tab churn.
- The root app now opens `/ws` only for live views (`dashboard`, `agents`, `settings`, `regime`, `hyperspace`) and only while the document is visible.
- Reconnect backoff now survives failed handshakes instead of snapping back to `1s` on each retry attempt.
- `wsConnected` is cleared immediately on manual close and socket close so status badges do not stay falsely green.
- `Dashboard` and `Agents` now pull a fresh REST snapshot after reconnect so a hidden tab does not stay stale until the next push event.
- Backend broadcast fanout now iterates a snapshot of connected clients and logs failed broadcast futures instead of iterating the live mutable set.
- Backend now exposes `GET /api/v1/realtime/recent` with a bounded replay buffer for non-price websocket events (`alert`, `recommendation`, `regime_change`, `regime_update`, `signal_update`, `node_update`, `agent_progress`, `agent_run_complete`).
- The frontend tracks `lastSocketEventAt` and replays anything newer than that timestamp but not newer than the reconnect boundary, so hidden tabs and off-route reconnects catch up without double-applying live events that arrive after the socket returns.

Result:

- Clean reload on `#/edge-scanner`
- No browser-console WebSocket errors on final verification
- Fewer background WebSocket accepts from non-live routes and hidden tabs
- Live views regain fresh state immediately after reconnect instead of waiting for the next push
- Missed alerts, recommendations, regime changes, node updates, and agent completion events are replayed after reconnect instead of disappearing during hidden-tab gaps

### Auth dependency cleanup

Files touched:

- [requirements.txt](/Users/anikdang/.codex/worktrees/540f/GRID/requirements.txt)
- [requirements-api.txt](/Users/anikdang/.codex/worktrees/540f/GRID/requirements-api.txt)
- [requirements.lock](/Users/anikdang/.codex/worktrees/540f/GRID/requirements.lock)

Fix:

- Pinned `bcrypt` below `4.1` and locked it to `4.0.1` to restore compatibility with `passlib==1.7.4`.

Result:

- Master-password login succeeds without the trapped bcrypt version warning in API logs.

---

## Verification

Backend tests:

```bash
PYTHONPYCACHEPREFIX=/tmp/grid_pycache MPLCONFIGDIR=/tmp/mplconfig ./.venv/bin/pytest -q tests/test_market_edge_scanner.py tests/test_intelligence_edges.py tests/test_api.py
```

Result:

```text
15 passed
```

Frontend tests:

```bash
cd /Users/anikdang/.codex/worktrees/540f/GRID/pwa
npm test -- --run src/__tests__/routing.test.js src/__tests__/routes.test.js
```

Result:

- `17 passed`

Type/build:

```bash
cd /Users/anikdang/.codex/worktrees/540f/GRID/pwa
npm run typecheck
npm run build
```

Result:

- `typecheck` passed
- `build` passed

Focused fallback tests:

```bash
cd /Users/anikdang/.codex/worktrees/540f/GRID
./.venv/bin/pytest -q tests/test_drillthrough_fallbacks.py tests/test_market_edge_scanner.py
```

Result:

- `4 passed`

Focused auth smoke:

```bash
./.venv/bin/pytest tests/test_api.py -q -k 'TestLoginInvalidPassword or TestLoginValidReturnsToken'
```

Result:

```text
2 passed, 8 deselected
```

Realtime lifecycle guard:

```bash
cd /Users/anikdang/.codex/worktrees/540f/GRID/pwa
npm test -- --run src/__tests__/edgeScannerReload.test.jsx src/__tests__/routing.test.js src/__tests__/api.test.js src/__tests__/store.test.js
```

Result:

- `50 passed`
- one harmless Node warning about `--localstorage-file` without a valid path during Vitest startup

Realtime API smoke:

```bash
cd /Users/anikdang/.codex/worktrees/540f/GRID
./.venv/bin/pytest -q tests/test_api.py -k 'recent_realtime_events_replays_buffered_events or TestLoginInvalidPassword or TestLoginValidReturnsToken'
```

Result:

- `3 passed, 8 deselected`

Browser verification:

- Cold load of `http://127.0.0.1:4173/#/edge-scanner`
- Auth restored
- Final Playwright check showed `0` console errors
- Breadth drill-through routed to `#/watchlist/GD?from=edge-scanner`
- Watchlist page requests all returned `200`:
  - `/api/v1/watchlist/GD/analysis`
  - `/api/v1/watchlist/GD/overview`
  - `/api/v1/watchlist/GD/edge`
  - `/api/v1/options/recommendations?ticker=GD`
  - `/api/v1/derivatives/gex/GD`
  - `/api/v1/derivatives/vanna-charm/GD`
  - `/api/v1/derivatives/flow-timeline/GD?days=90`
- Options drill-through routed to `#/options?ticker=GD&from=edge-scanner`
- Options page loaded with `0` console errors
- Desktop and mobile verification from earlier remained intact

Inventory gate:

```bash
python3 scripts/lint_module_inventory.py --verbose
```

Result:

```text
OK — inventory is up-to-date.
```

---

## Git State

Committed:

```text
latest local commit: Replay missed realtime events after reconnect
b192a926 Harden realtime socket lifecycle
501e2fbf Refresh session handoff after PR merge
60d6637d Add edge scanner reload regression test
```

Pushed:

```text
origin/codex/edge-scanner-reload-guard matches local HEAD
origin/claude/analyze-derivatives-metals-aTllj deleted
draft PR #45 open against main
```

Working tree at handoff:

```text
clean
```

---

## Next Useful Work

1. Add source drill-through from confirmation rows so a user can jump straight to the underlying clue family evidence.
2. Expand browser-level coverage to cover live-view route transitions like `regime` and `hyperspace` if the socket allowlist changes again.
3. Decide whether price pushes need their own bounded replay or whether the current REST snapshot remains enough once event volume ramps next week.
4. Consider a per-view subscription model if `/ws` event volume grows materially beyond the current replay buffer assumptions.
