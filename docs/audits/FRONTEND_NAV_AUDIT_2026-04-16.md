# Frontend Navigation Audit - 2026-04-16

## Fixed in this pass

- Added app-level hash synchronization so direct `window.location.hash` writes, Canvas deep links, and browser back/forward update the active React view.
- Added a shared routing helper for canonical hash parsing/building and covered it with unit tests.
- Repaired the Trial Gems ticker link from `#/watchlist-analysis?ticker=...` to `#/watchlist/...`.
- Repaired Cross Reference ticker chips to open the watchlist analysis child route.
- Stopped unknown module IDs from silently rendering Canvas. Unknown routes now show an explicit module-missing state.
- Passed `onNavigate` to routed views consistently so module-level links such as Risk Map -> Cross Reference are live instead of no-ops.
- Wired the `intelligence-search` drawer item to a routed search view instead of sending users to Canvas under the wrong label.
- Emitted a `grid:auth-expired` event on API 401 responses so the app store can leave authenticated mode cleanly.

## Larger fixes to plan

- Navigation test coverage: add an integration test around `App` hashchange behavior once the test harness can mount authenticated routes without real backend calls.

## Fixed in follow-up

- Collapsed generic route lazy-loading in `app.jsx` onto `routes.js` via `import.meta.glob`, so `routes.js` is now the source for regular module routes.
- Added route registry tests for duplicate route ids, missing view files, and the previously stale `operator` / `snapshots` routes.
- Registered `operator` and `snapshots` as real operations drawer routes.
- Removed the unused stale `MobileShell.jsx` component instead of preserving a second hard-coded mobile route vocabulary.
- Wired Intel Search additions into persisted Canvas investigation boards (`investigation_boards.graph_state`) and taught Canvas to open `#/canvas?board=<id>`.
- Updated `SendToCanvas` to use the same investigation-board graph state path instead of the incompatible `canvas_boards` node CRUD path.
- Centralized browser auth-session storage behind `authSession.js` so API and Zustand auth state no longer duplicate storage key handling.
- Unified granular Canvas node/edge/graph endpoints with the investigation-board `graph_state` model and added a best-effort legacy table mirror for older Canvas routers that still read `canvas_boards`, `canvas_nodes`, and `canvas_edges`.
- Fixed stale `canvas_edges(edge_id)` inserts in the expansion/investigation routers; the current schema column is `id`.

## Sanity findings

- The NavBar route list is not the main source of broken links: every route ID in `routes.js` has a matching lazy entry in `app.jsx`, and every listed component file exists.
- No missing frontend `api.*` methods were found in `views`, `components`, or `canvas`; the API client surface is not the immediate frontend-breakage source.
- The biggest process smell is silent fallback behavior. Rendering Canvas for unknown route IDs hid link mistakes and made unrelated modules look like Canvas bugs.
