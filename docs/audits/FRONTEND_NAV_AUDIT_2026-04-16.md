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

- Route registry duplication: `routes.js` claims to be the single source of truth, but `app.jsx` still keeps a separate `routeComponents` map. Collapse these into one registry or add a static integrity test that fails when they drift.
- Stale mobile shell: `MobileShell.jsx` is unused and still references `operator` and `snapshots`, which are not registered routes. Decide whether mobile needs its own shell; if yes, register the missing modules or map those tabs to current operations routes.
- Intel Search product flow: the new routed view makes the menu target honest, but adding results directly to an active Canvas board needs a shared board/session action instead of the local working-set placeholder.
- Auth flow ownership: API 401 handling now notifies the app, but auth state should eventually live behind one store/API contract rather than mixing `api.token`, local storage, hash redirects, and Zustand state.
- Navigation test coverage: add an integration test around `App` hashchange behavior once the test harness can mount authenticated routes without real backend calls.

## Sanity findings

- The NavBar route list is not the main source of broken links: every route ID in `routes.js` has a matching lazy entry in `app.jsx`, and every listed component file exists.
- No missing frontend `api.*` methods were found in `views`, `components`, or `canvas`; the API client surface is not the immediate frontend-breakage source.
- The biggest process smell is silent fallback behavior. Rendering Canvas for unknown route IDs hid link mistakes and made unrelated modules look like Canvas bugs.
