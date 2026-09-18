# Browser-acceptance harness — first-release journeys

Local, fixture-backed harness for demonstrating the five first-release user
journeys against the PWA in a real browser, with **no database, no Docker,
no ssh, and no production/staging access**. This directory only produces
the harness; the lead drives the actual browser (only one is available on
this machine) using the URLs and checklist below.

Baseline this harness was built against: this worktree is
`fable/browser-acceptance-20260918` off `origin/main` @ `3fe3f5ef`. Response
shapes were cross-checked against the read-only composed tree checked out at
`44019a43` (development composition — an integration branch merging the
2026-09-17/18 honesty-fix PRs — **not a production commit and not a
production deployment**). See "Shape differences" below for what changed
between the two and why the fixtures follow the composed tree.

## Which PWA tree to drive

The fixture server is tree-agnostic (it just serves JSON at paths), but
`pwa/src/views/TickerLookup.jsx` differs by ~56 lines between this worktree
(`origin/main`-derived) and the read-only composed-g checkout — main's
version is what this worktree's `npm run dev` serves. All dad-ticker fixture
shapes in this harness were built from composed-g's `api/routers/dad.py`
(see the "Router function mirrored" table below), because that is the
router the composed-g PWA's `TickerLookup.jsx` was written against. Driving
**main's** `pwa/` against these fixtures may still show mismatches on that
one view; the lead wires the composed-g `pwa/` separately for that reason.
Every other view in this harness (`Home.jsx`, `Portfolio.jsx`,
`WatchlistAnalysis.jsx`, `Operator.jsx`, `Discovery.jsx`,
`PipelineHealth.jsx`) was unchanged between the two trees as of 2026-09-18.

## Role: admin (operator) vs contributor (dad mode)

`FIXTURE_ROLE` env var (default `admin`) is threaded into the fixture
server's `/api/v1/auth/login` and `/api/v1/auth/verify` responses.
`pwa/src/authSession.js:57` (`SIMPLE_ROLES = new Set(['contributor'])`) gates
the PWA into a stripped-down "dad mode" shell (just the contributor's two
pages, big text) for role `contributor`, and the full operator cockpit for
anything else. Set `FIXTURE_ROLE=contributor` before starting the fixture
server to exercise dad mode; restart the server (not the PWA) to switch.

## Why a fixture server, not the real API

`api/main.py` needs a live Postgres connection at import time (`api/auth.py`
opens a DB connection during route setup). There is no local Postgres and no
Docker on this machine, so the real backend cannot run here. Instead,
`fixture_api/server.py` is a small stdlib `http.server` that serves static,
synthetic JSON at the exact paths the PWA calls, and the Vite dev server's
existing `/api` proxy is pointed at it instead of a real backend.

## Auth: the legitimate local dev path

The PWA (`pwa/src/api.js`) sends `Authorization: Bearer <token>` on every
`/api/v1/*` call, where the token comes from `localStorage['grid_token']`
(`pwa/src/authSession.js:6-8`, `getStoredToken`). The token is obtained by
`POST /api/v1/auth/login` (`pwa/src/api.js:220`), which in the real backend
(`api/auth.py:438-502`) accepts either a username/password user account or
the master password (`GRID_MASTER_PASSWORD_HASH` env var), and returns a
signed JWT (`GRID_JWT_SECRET`, `api/auth.py:175-190`). `require_auth`
(`api/auth.py:222-240`) accepts that bearer token, or (legacy) a `?token=`
query param.

This harness does **not** need a real JWT and never talks to production:
`fixture_api/server.py` implements its own `/api/v1/auth/login` and
`/api/v1/auth/verify` that always succeed and hand back a clearly-fake
string (`"dev-fixture-token.not-a-real-jwt.TEST1"`, see `fixtures.py`'s
`login_response()`). The fixture server does not check the `Authorization`
header on any route at all — this is a stand-in for auth, not real auth, and
it only ever runs against the fixture server, never against a real backend.
To log in during a run: open the PWA's login screen and submit any
password — the fixture server's login route ignores the value and returns
the fixture token, which the PWA then stores and sends on every later call.

## Starting the harness

**Order matters — start the fixture server before the PWA**, so the first
`/api/v1/...` request the PWA makes on load has something to talk to.

```bash
# 1. Fixture server (port 8000, matches the PWA's default proxy target)
cd tests/browser/fixture_api
python server.py --port 8000 --scenario healthy
# or: FIXTURE_SCENARIO=partial python server.py
# or, for dad mode: FIXTURE_ROLE=contributor python server.py
```

```bash
# 2. In a second terminal: install deps once, then run the PWA dev server
cd pwa
npm ci    # or: npm install (no lockfile case) — node_modules is not present yet
npm run dev
# Vite proxies /api and /ws to GRID_API_PROXY_TARGET / GRID_WS_PROXY_TARGET,
# which default to http://127.0.0.1:8000 and ws://127.0.0.1:8000
# (pwa/vite.config.js:6-7, 70-79) — i.e. the fixture server from step 1.
```

Then open `http://localhost:5173/` in the one available browser.

Two `.claude/launch.json` entries cover both steps: **`grid-fixture-api`**
runs step 1 (`python tests/browser/fixture_api/server.py --port 8000
--scenario healthy`; edit the args to change scenario) and
**`grid-pwa-fixtures`** runs step 2 (`npm run dev` in `pwa/`, proxy env vars
already pointed at `127.0.0.1:8000`). Start `grid-fixture-api` first.

To switch scenarios, stop the fixture server (Ctrl-C) and restart it with
`--scenario partial` or `--scenario empty`; the PWA dev server does not need
to be restarted.

## Journeys, routes, and fixtures

| Journey | PWA route / view | API path(s) called | Fixture function |
|---|---|---|---|
| (a) Home / market overview | `#/` → `pwa/src/views/Home.jsx` (posts to compose, then renders `WidgetGrid`) → `pwa/src/components/home/widgets.jsx` | `POST /api/v1/chat/compose`; then each widget independently: `GET /api/v1/regime/current`, `GET /api/v1/watchlist/`, `GET /api/v1/physics/momentum`, `GET /api/v1/flows/sectors`, `GET /api/v1/watchlist/TEST1/quote`; verdict widget: `POST /api/v1/chat/ask/stream` (SSE) | `chat_compose`, `regime_current`, `watchlist_list`, `news_momentum`, `sector_flows`, `ticker_quote`, `chat_ask_stream_deltas` |
| (b) Ticker investigation (ticker=`TEST1`) | `#/ticker?ticker=TEST1` → `pwa/src/views/TickerLookup.jsx` | `GET /api/v1/dad/ticker/TEST1/gold/stream` (SSE, intentionally 404s — see below), then `GET .../gold`, `/evidence`, `/chart`, `/finviz`, `/options`; `GET /api/v1/valuation/catalyst-timeline/TEST1` (and `/ACME`, see provenance note below) | `dad_ticker_gold`, `dad_ticker_evidence`, `dad_ticker_chart`, `dad_ticker_finviz`, `dad_ticker_options`, `catalyst_timeline` |
| (c) Watchlist / portfolio, edge & trust-convergence | `#/portfolio` → `pwa/src/views/Portfolio.jsx`; `#/watchlist-analysis?ticker=TEST1` → `pwa/src/views/WatchlistAnalysis.jsx` (analysis/overview load first, then edge + derivatives) | `GET /api/v1/watchlist/portfolio`; `GET /api/v1/watchlist/TEST1/{analysis,overview,edge}`; `GET /api/v1/derivatives/{gex,vanna-charm,flow-timeline}/TEST1`; `GET /api/v1/intelligence/dashboard` (→ `.trust.top_sources` / `.trust.convergence_events`) | `watchlist_portfolio`, `watchlist_ticker_analysis`, `watchlist_ticker_overview`, `ticker_edge`, `derivatives_gex`, `derivatives_vanna_charm`, `derivatives_flow_timeline`, `intelligence_dashboard` |
| (d) Research status | **No dedicated page exists.** The real operator-only surfaces are Discovery (`#/discovery` → `pwa/src/views/Discovery.jsx`) and Pipeline Health (`#/pipeline-health` → `pwa/src/views/PipelineHealth.jsx`) | `GET /api/v1/discovery/{jobs,hypotheses,hypotheses/results,results/orthogonality,results/clustering}`; `GET /api/v1/system/pipeline-health` | `discovery_jobs`, `discovery_hypotheses`, `discovery_hypotheses_results`, `discovery_results`, `pipeline_health` |
| (e) Data health / source drill-down | `#/operator` → `pwa/src/views/Operator.jsx` (loads all six below on mount); sector drill-down via `GET /api/v1/sectors/{sector}/health` | `GET /api/v1/system/status`, `GET /api/v1/system/hermes-status?limit=20`, `GET /api/v1/snapshots/issues?...`, `GET /api/v1/snapshots/latest/pipeline_summary?n=10`, `GET /api/v1/system/health`, `GET /api/v1/system/freshness`, `GET /api/v1/sectors/Technology/health` | `system_status`, `hermes_status`, `snapshots_issues`, `snapshots_latest`, `system_health`, `system_freshness`, `sector_health` |

All fixtures live in `tests/browser/fixture_api/fixtures.py`, routed by
`tests/browser/fixture_api/server.py`. Data is entirely synthetic: ticker
`TEST1` (and `TEST2` for a second holding in the `partial` portfolio), round
numbers, dates in `2026-09`, usernames like `fixture-operator`/`fixture-dad`.

## Router function mirrored, per fixture

Every dad-ticker and portfolio fixture below was built by reading the
router's actual return dict on the composed-g tree, not guessed — an earlier
pass guessed and shipped fixtures that 200'd but broke the view (see
"Defects" below for what that looked like). Line numbers are composed-g
`api/routers/*.py` as of 2026-09-18.

| Fixture | Router function | View fields consumed |
|---|---|---|
| `dad_ticker_gold` | `_build_compact_dad_response` (dad.py:1916) + `_assemble_dad_response` (dad.py:1856) | `TickerLookup.jsx`: `data.{status,gold,decision_stack,summary,workbook,source_lanes,dad_stats,finviz,grid_data,signals,tradingview,fit_signals,risks,next_actions}` |
| `dad_ticker_evidence` | `_build_evidence_payload` (dad.py:1958) | same `workbook`/`source_lanes`/`dad_stats`/`fit_signals` keys, merged in |
| `dad_ticker_chart` | `_build_chart_payload` (dad.py:1989) | `marketData.{price_history[{date,value}],metrics,source_freshness,tradingview_signals,regime}` |
| `dad_ticker_finviz` | `_build_finviz_payload` (dad.py:2049) | `data.finviz.{status,stats,field_count,freshness,latest_obs_date}` |
| `dad_ticker_options` | `_build_options_payload` (dad.py:2073) | `data.options` |
| `watchlist_portfolio` | `get_portfolio` (watchlist_core.py:133-379) | `Portfolio.jsx`: `data.positions[].{ticker,display_name,price,change_1d,change_1w,weight,sector,asset_type}`, `data.{weighted_return_1d_pct,positions_missing_price,missing_price_tickers,allocation,risk_metrics}` |
| `watchlist_ticker_analysis` | `get_ticker_analysis` (watchlist_analysis.py:58-105) | `WatchlistAnalysis.jsx` price/feature/options/regime panels |
| `watchlist_ticker_overview` | `get_ticker_overview` (watchlist_overview.py:31-44) | narrative overview card |
| `ticker_edge` | `get_ticker_edge` (watchlist_overview.py:545+) | congressional/insider/dark_pool/whale_flow/smart_money panels |
| `hermes_status` | `HermesStatusResponse` schema (api/schemas/system.py:106-115) | `Operator.jsx`: `hermes.{running,task_status,operator_state.last_pipeline_run}` |
| `system_status` / `system_freshness` | `SystemStatusResponse` / `FreshnessResponse` schemas (api/schemas/system.py:49-91) | `Operator.jsx` status + freshness cards |
| `snapshots_issues` / `snapshots_latest` | `get_operator_issues` / `get_latest_snapshots` (snapshots.py:123-181 / 29-46) — **both return a plain list**, not `{"issues": [...]}` | `Operator.jsx` issues table / recent cycles |
| `discovery_jobs` / `discovery_results` / `discovery_hypotheses` / `discovery_hypotheses_results` | discovery.py:117-291 | `Discovery.jsx` jobs/results/hypotheses panels |
| `ten_year_portfolio_weekly` | `weekly_ten_year_portfolio` (ten_year_portfolio.py:262-298) + `build_weekly_recommendation`/`build_profile_portfolio` | `TenYearPortfolio.jsx` profile/allocation/Monte-Carlo cards |
| `chat_compose` | `ChatComposeResponse` schema (chat.py:306-318) | `Home.jsx`'s `layout.{spoken,widgets,allocation}` |
| `chat_ask_stream_deltas` | `ask_grid_stream` (chat.py:2740-2775) | `widgets.jsx` `VerdictCard`'s streamed text |
| `options_recommendations` | `get_recommendations` (api/routers/options.py:200-238) + its fallback `_load_saved_recommendations` (:138-194) — read from **this worktree**, not composed-h (see note below) | see note below — not the two views it was first attributed to |

**Note on `options_recommendations`:** two corrections to how this gap was
first described, found by reading source rather than assumed:
1. `GET /api/v1/options/recommendations` is not called by Portfolio.jsx's
   OPTIONS P&L card or WatchlistAnalysis.jsx's options panel (grepped all
   of `pwa/src` — it only appears in `pwa/src/views/Options.jsx` and once
   in `pwa/src/app.jsx:137`, inside `OPERATOR_PRELOAD_API_PATHS`, a generic
   background preload fired for any admin session). Portfolio.jsx's card
   reads `get_portfolio`'s own `options_pnl`; WatchlistAnalysis.jsx's panel
   reads `get_ticker_analysis`'s own `options` field — both already
   covered by existing fixtures above. The 404 itself was real (no fixture
   existed for this route at all) and worth fixing regardless of which
   code path triggers it.
2. `_load_saved_recommendations`'s query is
   `WHERE (outcome IS NULL OR outcome = 'OPEN')` (options.py:153) — this
   endpoint structurally can never return a closed/WIN recommendation (that
   only appears via the separate, unfixtured `GET .../recommendations/history`).
   A "one WIN closed, one open" healthy pair was requested; the fixture
   instead serves two OPEN recommendations, since that is what this
   endpoint can actually, honestly return.

This worktree's `api/routers/options.py` / `trading/options_recommender.py`
were read instead of the composed-h tree named in the request:
`C:/Users/owner/dev/GRID-fable-wt-composed-h` was never in this session's
authorized read scope (only this worktree, for writing, and composed-g,
read-only, were) — "do not touch any other checkout" was read as covering
reads too, not only writes, so composed-h was not opened.

## SSE fallback (intended, not a bug)

`TickerLookup.jsx` opens `GET /api/v1/dad/ticker/{t}/gold/stream` (an
`EventSource`, `api.js:370-399`) before the four parallel GETs. This fixture
server has **no route for `.../gold/stream`**, so it 404s on every lookup —
that is deliberate: the view's `onError` handler (`TickerLookup.jsx:399-406`)
catches exactly this and falls back to `hydrateDetails()`, which is what
actually exercises `dad_ticker_evidence`/`chart`/`finviz`/`options` above.
Don't add a stream route to "fix" this 404 — it is the trigger for the path
this harness is meant to test.

## Scenarios

Select with `--scenario` or `FIXTURE_SCENARIO` env var.

- **`healthy`** — every widget has real-shaped data with an explicit
  `source`/`as_of` or `price_source` field.
- **`partial`** — one dataset per journey is unavailable or stale while the
  rest of that journey's data is fine (e.g. home's news-momentum widget is
  `available: false`; the ticker page's Finviz snapshot is `stale`; the
  portfolio has a second holding with no price at all). Several payloads
  place a measured `0` / `0.0` directly next to a `null` so the two are
  visibly different kinds of value, not two flavors of "empty."
- **`empty`** — no watchlist items, no portfolio positions, no workbook
  evidence, no convergence events, nothing pulled.

## Expected honest behaviours (checklist)

Use this while driving the browser against each scenario.

**General (every journey, every scenario):**
- [ ] Zero and unknown/unscored are visually distinct — a `null` never
      renders as `0`, `0%`, `$0`, "neutral", or "moderate".
- [ ] No invented percentage or score appears where the fixture sent `null`
      (e.g. `TEST1`'s unscored milestone `ms-1` on the ticker page must show
      as unscored/unknown, not a blank that could be mistaken for a real 0).
- [ ] An unavailable widget/dataset does not blank out or break sibling
      widgets on the same page (test this specifically in `partial`).
- [ ] Loading, empty, and error states are each visually distinguishable
      from one another (reload the page to catch the loading flash; use
      `empty` for the empty state).
- [ ] The page renders without layout breakage at a narrow width (~375px).
- [ ] No new console errors appear (check browser devtools) beyond
      pre-existing/unrelated warnings.

**Ticker investigation (journey b) — catalyst timeline, `TEST1`:**
- [ ] Event `ms-1` (`probability: null`) reads as **unscored** — not "0%",
      not blank-as-if-missing.
- [ ] Event `ms-2` (`probability: 0`) reads as a **measured 0%** — visually
      distinct from `ms-1`'s unscored state, not collapsed into the same
      "no data" treatment.
- [ ] Event `ms-2`'s `value_impact_pct: 0.0` **renders as a real zero**
      value (e.g. "0.0%"), not as a gap/dash — a measured zero must not
      disappear from the timeline.

**Watchlist / trust-convergence (journey c), `partial` scenario:**
- [ ] The scored convergence event (`TEST1`, `combined_confidence: 0.595`)
      and the unscored one (`TEST2`, `combined_confidence: null`,
      `confidence_basis: "unscored"`) are both shown, and the unscored one
      is not sorted or styled as if it had a low or zero confidence.
- [ ] The portfolio's second holding (`TEST2`, no price) does NOT appear in
      the positions table at all (it is excluded, not shown with a blank/0
      price) and is instead called out via `missing_price_tickers`; the
      weighted 1D return does not silently count it as a 0% move.

**Home compose (journey a):**
- [ ] All six widget types render for the composed layout (verdict,
      ticker_pulse for `TEST1`, watchlist, macro_regime, news, money_flow);
      none silently disappear because their own data call is slower than
      the others'.
- [ ] The verdict card streams in text (via the SSE `ask/stream` fixture)
      rather than showing a static "Looking..." forever.

**Research status (journey d), `partial` scenario:**
- [ ] The `hypotheses` list shows one hypothesis with an explicit
      `skip_reason` ("feature series has < 30 observations in window") —
      it must read as skipped/untested, not as a `FAILED` verdict with a
      fabricated correlation.

**Data health (journey e), `empty` scenario:**
- [ ] Operator's Hermes status reads offline/zero-cycle honestly (no
      "0 tasks running, all healthy" framing) when `hermes_status` returns
      `running: false, cycle_count: 0, task_status: {}`.

**Role switch:**
- [ ] With `FIXTURE_ROLE=contributor`, the PWA shows the stripped-down
      contributor shell (big text, two pages), not the full operator
      cockpit. With `FIXTURE_ROLE=admin` (default), it shows the full
      cockpit.

## Defects observed (record, do not fix — `pwa/src` is off limits)

These are real mismatches between what the PWA reads and what the actual
router returns (or, for the third, a real client-side collapse of `null`
into a fabricated value). Confirmed by reading source directly; not fixed,
per instructions.

1. **`pwa/src/views/Operator.jsx:61-62`** —
   `setIssues(issuesRes?.issues || issuesRes || [])` and the equivalent line
   62 for `setRecentCycles`. `GET /api/v1/snapshots/issues` and
   `GET /api/v1/snapshots/latest/{category}` both return a **plain list**
   on success (confirmed on both main and composed-g,
   `api/routers/snapshots.py:123-181` and `:29-46`), so the `?.issues` access
   is always undefined and the code correctly falls through to the list
   itself — fine on a 200. But `api.js`'s `_fetch` returns `{error: true,
   status, message}` (not a thrown error) on a non-2xx response, so on any
   real fetch failure `issuesRes` becomes that object, `issuesRes || []`
   is still truthy, and `issues` ends up being the error object. Line 96's
   `Array.isArray(issues)` guard protects the `stats` computation, but
   **`issues.map` at line 365 and `issues.length` at line 359 have no such
   guard** — `issues.map is not a function` crashes the component (caught by
   `ViewErrorBoundary`). Repro: make `/snapshots/issues` return any non-2xx.
   `recentCycles` has a `.length > 0` guard before its `.map` (line
   433/436), so it degrades silently instead of crashing.
2. **`pwa/src/views/TenYearPortfolio.jsx:69-76`** — the `money(value)`
   helper: `if (value == null || Number.isNaN(Number(value))) return '$0';`.
   Its siblings `pct()` (line 64-67) and `number()` (line 78-81) return
   `'n/a'` for the same null/NaN case; only `money()` collapses an
   unmeasured value into a real-looking dollar figure. Consumed at line 178
   (`money(profile?.estimated_invested)`) — on a 404/error response (`data`
   stays without a `profiles` array), this renders literally "$0" for
   invested and cash, which reads as a measured empty portfolio rather than
   "no data." This is the "$0 INVESTED / $0 CASH" symptom the lead reported.
3. **`pwa/src/views/Portfolio.jsx`** reads `p.pnl_1d` (line 221 column def,
   line 281/283 render) and `data.total_pnl_1d` / `data.total_pnl_1d_pct`
   (lines 421-423), but `get_portfolio` (`watchlist_core.py:360-379`)
   documents and enforces "no dollar figures... and no dollar P&L keys at
   all" — those fields are never present in a real response, on either
   tree. In practice this renders as `'--'` (both `fmtDollar` and the
   `>=0` ternary treat `undefined` as falsy/null, not as `0`), so it isn't
   a fabricated-zero defect like #2, but it is a permanently-dead UI field
   reading data the API contract says will never exist.
4. **`pwa/src/views/WatchlistAnalysis.jsx:1170-1174` and `:1346-1372`** — the
   GEX and Vanna/Charm panels are wired as `Promise.allSettled(...).then(...)`
   with `if (!gexResult.value?.error) setGexData(...)` (same for
   vanna-charm); on the router's own `{"error": ..., "ticker": ...}` shape
   (a real, documented response — `api/routers/derivatives.py:81-95`,
   `:172-192` — not an HTTP failure), the state is simply never set. The
   render is `{gexData ? <GEXProfile .../> : gexLoading ? <Skeleton/> :
   null}` (line 1347-1360) and `{vannaCharmData && <VannaCharmViz .../>}`
   with **no else branch at all** (line 1363-1372) — both panels vanish
   silently with no "unavailable" state, unlike the Finviz/options panels
   elsewhere on the same page. Reproduced directly (not inferred): both
   fixtures return the router's `{"error": ...}` shape in `partial`, and
   the corresponding panels do not render.
5. **`pwa/src/views/Discovery.jsx:339`** —
   `` `${(clusterResult.variance_explained * 100).toFixed(1)}%` `` has no
   null-guard, unlike its sibling rows (`n_features_analyzed`/`true_dimensionality`
   at lines 309-310 render `undefined` as blank, which is at least not a
   fabricated number). If `clusterResult` is ever set with
   `variance_explained` missing or `null`, this renders the literal string
   `"NaN%"`. In this harness the actual trigger was an incomplete fixture
   (see "Router function mirrored" — now fixed), but the arithmetic-without-a-guard
   pattern is the latent defect: any future response shape gap reproduces it.
6. **`pwa/src/views/Operator.jsx:295`** —
   `` {status.database.size_mb && ` · ${(status.database.size_mb / 1024).toFixed(1)}GB`} ``.
   When `size_mb` is exactly `0` (a real, measured "database has no data yet"
   reading, not an absent one — this harness's `empty` scenario), JavaScript's
   `&&` returns the falsy left operand itself (`0`), and React renders a bare
   number `0` as text. Result: `DB: Disconnected0`. The fix pattern used
   elsewhere on the same page (`!= null` checks) is not applied here.
7. **`pwa/src/views/TenYearPortfolio.jsx:762`** —
   `<strong>{data?.as_of || 'loading'}</strong>` under the "As of" label.
   On the router's own `{"status": "empty", ...}` response (a completed,
   honest "no eligible price history" result —
   `api/routers/ten_year_portfolio.py:286-291` — not a pending request),
   `data.as_of` is `null`, and the fallback string is the literal word
   `"loading"` — used here as a static placeholder, not a live spinner. The
   page has finished loading; nothing further will ever arrive, but the UI
   keeps saying "loading" forever. Same family at line 754:
   `{activeProfile?.description || 'Waiting for the weekly portfolio query.'}`
   — again phrased as an in-progress state for a query that has already
   completed and genuinely found nothing.

Items #4-#7 above were surfaced by the lead's browser run (see the
classification table below), not by static reading alone; #1-#3 were found
while building the fixtures.

## Classification: residual renders from the lead's browser run

The lead drove the real browser against this harness after the fixture
corrections above and found further renders. Each is classified as either
**FIXTURE GAP** (this harness's bug — fixed in this pass) or **VIEW DEFECT**
(a real `pwa/src` issue, recorded above, not fixed). Where a missing field
made a view render `NaN`/blank/bare-punctuation, both the fixture gap and
the underlying view fragility are recorded, per the lead's instruction.

| # | Symptom | Classification | Root cause / fix |
|---|---|---|---|
| 1 | WatchlistAnalysis AI overview: `"5D low: $"` / `"5D high: $"`, no numbers | FIXTURE GAP | `watchlist_ticker_overview`'s `key_levels` used key `"level"`; the view reads `level.value` (`WatchlistAnalysis.jsx:217`). Fixed. |
| 2 | Operator subsystem health: `"Disk: % (GB free)"`, `"API Keys: /"` empty | FIXTURE GAP | `system_health`'s `checks` never had `disk_percent`/`disk_free_gb`/`api_keys_configured`/`api_keys_total`/`ws_clients`/`llm_available`/`thread_ingestion` (`Operator.jsx:189-198`). Fixed. |
| 3 | Discovery: `"Variance explained NaN%"`, blank Features/dimensionality/Best k/PCA | FIXTURE GAP (+ latent VIEW DEFECT #5) | `discovery_results` returned a made-up `{type,generated_at,summary}` wrapper instead of the real `orthoResult`/`clusterResult` fields (`Discovery.jsx:309-310,337-339`). Fixed; #5 above records the unguarded multiply regardless. |
| 4 | WatchlistAnalysis regime context: `"Posture --"` | FIXTURE GAP | `watchlist_ticker_analysis`'s `regime` omitted `posture`/`as_of` (`watchlist_analysis.py:264-267`; view: `WatchlistAnalysis.jsx:1411`, an otherwise-honest `|| '--'` fallback). Fixed. |
| 5 | Console: `<text> attribute y: Expected length, "NaN"` on watchlist-analysis (partial) | FIXTURE GAP (same root as #1) | `PriceChart.jsx:121`'s `yScale(level.value)` on the same wrong `key_levels` shape as #1. Fixed by #1's fix; `PriceChart.jsx` has no defensive guard on `level.value`, which is a minor latent fragility but not classified as a standalone defect since the router never actually sends a level without a `value`. |
| 6 | Portfolio: `"1W CHG +100.00%"` for TEST1 while 1D is `"+0.00%"` | FIXTURE GAP | `watchlist_portfolio`'s `change_1w` was `1.0` instead of a small fraction like real `pct_1w` values (`watchlist_helpers.py:370`, fraction convention, same as `change_1d`). Fixed to `0.012`. |
| 7 | WatchlistAnalysis (partial): GEX and Vanna/Charm panels vanish with no message | VIEW DEFECT | See "Defects observed" #4 above (`WatchlistAnalysis.jsx:1170-1174`, `:1346-1372`). Not fixed. |
| 8 | Console NaN on watchlist-analysis (partial) — same as #5 | FIXTURE GAP | Same cause and fix as #5; this was the "partial"-scenario instance of the same `key_levels` bug. |
| 9 | Ticker-lookup (empty): decision-stack "Finviz fundamentals 0.0 pts — 2 fields, fresh" contradicts the Finviz panel's "unavailable, 0 fields, missing" in the same response | FIXTURE GAP | `_dad_decision_stack`'s Finviz card `detail` string had a two-way ternary (`partial` vs. else) that fell through to the "healthy" wording for `empty` too. Fixed to say "0 fields, missing" for `empty`, matching the Finviz panel. |
| 10 | `#/ten-year` (empty): "INVESTED $0 / CASH $0" and "AS OF loading" forever, "Waiting for the weekly portfolio query." | VIEW DEFECT (2 lines) | See "Defects observed" #2 (`money()`, `:69-76`/`:178`) and #7 (`:762`, `:754`) above. Router's own `{"status": "empty"}` is a completed, honest result — not a pending load. Not fixed. |
| 11 | Operator (empty): `"DB: Disconnected0"` | VIEW DEFECT | See "Defects observed" #6 above (`Operator.jsx:295`, `size_mb && ...` on a real `size_mb: 0`). Not fixed. |
| 12 | Dad mode, home: news widget says "It's quiet — no big news right now." on a healthy momentum reading | FIXTURE GAP | `news_momentum` invented a shape (`status`/`lookback_days`/`series`/`source`) sharing no keys with the real `MomentumResult.to_dict()` (`physics/momentum.py:37-59`: `available`/`sentiment_trend`/`momentum_direction`/`energy_state`/`direction`/`summary`/`details`/`warnings`). `NewsCard` reads `data?.direction`/`data?.summary` (`widgets.jsx:248-249`), which are correct field names — this was a pure fixture bug, not a real consumer mismatch. Fixed. |
| 13 | Dad mode, home: money-flow widget says "Nothing notable moving right now." on a healthy sectors reading | FIXTURE GAP | `sector_flows` returned a `list` under `sectors` with fields `flow_1d_pct`/`flow_5d_pct`; the real endpoint returns a **dict** keyed by sector name with a `sector_stress` field (`flows.py:267-284`). `MoneyFlowCard` filters on `typeof s?.sector_stress === 'number'` (`widgets.jsx:270-274`), which every entry failed. Fixed. |
| 14 | Dad mode, home: ticker pulse shows `"TEST1 $100 +0.0%"` | Not a defect (fixture clarity change) | `ticker_quote`'s `change_pct` was a real `0.0` (a legitimate flat-day reading) and rendered correctly as `+0.0%`. Changed to a non-zero `0.6` anyway so a real measured zero is never visually indistinguishable from "the fixture forgot this field" while eyeballing the page. |
| 15 | Dad mode, home: alerts panel always empty | FIXTURE GAP (missing route) | `GET /api/v1/alerts` had no route at all (404); `Home.jsx`'s `loadAlerts()` catches the failure silently. Added `alerts_list` mirroring `price_alerts.py::list_alerts` (~line 186-203) and the route. |
| 16 | Dad mode, `#/ten-year`: "If it goes poorly $900,000 / Most likely $1,400,000 / If it goes well $2,100,000", "In about 78% of the outcomes...", "Yearly growth 8.0%, bumpiness 22%, vs big tech about 12% a year" | Not a defect — all router fields | Every one of these is a direct, unmodified passthrough of a router-emitted field, confirmed by reading `TenYearPortfolio.jsx`: `mc?.p10`/`p50`/`p90`/`probability_above_start`/`expected_annual_return`/`annual_volatility` (all from `activeProfile.monte_carlo`, `:342,:416-417`, `build_monte_carlo_projection`, `ten_year_portfolio.py:415+`) and `benchCagr = data?.benchmark?.cagr` (`:417`). `pct()`/`money()` (`:64-81`) are pure formatters — no client-side computation, no literal/default in the view. All supplied by `ten_year_portfolio_weekly`'s `monte_carlo`/`benchmark` fixture blocks. |

## Dad mode — exact steps (this is the journey Anik will see)

1. Stop the fixture server if running, then start it with
   `FIXTURE_ROLE=contributor python tests/browser/fixture_api/server.py --port 8000 --scenario healthy`.
2. In the browser, clear `localStorage` for `http://localhost:5173` (devtools →
   Application → Local Storage → clear), or open a private window — a
   leftover `grid_token`/`grid_role` from a prior admin session will
   otherwise skip straight past the role gate.
3. Open `http://localhost:5173/`, submit the login form with any password
   (the fixture server ignores the value — see "Auth" above).
4. `pwa/src/authSession.js:57`'s `isSimpleUser()` gate now reads role
   `contributor` from the fixture token and switches the shell: simplified
   nav (just the contributor's two pages), larger text, the
   `stepdad.finance` Home composer with suggestion chips
   (`pwa/src/views/Home.jsx`'s `SUGGESTIONS`) instead of the full operator
   cockpit.
5. Type or tap a suggestion (e.g. "How are my stocks doing?") — this posts
   `POST /api/v1/chat/compose`, which returns the six-widget layout; each
   widget then independently fetches its own data (regime, watchlist,
   news, money flow, ticker pulse, and the verdict's SSE stream). Verified
   end to end in `healthy`: all six render, the verdict streams text, and
   the alerts panel (once fixture #15 above landed) shows the one active
   `TEST1` alert.

The base tree (`origin/main` @ `3fe3f5ef`, this worktree's parent) and the
composed-g tree differ in `api/routers/intelligence_risk.py` and
`api/routers/dad.py` — the honesty-fix PRs from the 2026-09-17/18
remediation. `api/routers/watchlist.py`, `watchlist_analysis.py` (aside from
an unrelated `yfinance` adjustment), and `valuation.py` are byte-identical
between the two trees. Fixtures follow the **composed-g** shapes:

- `api/routers/intelligence_risk.py` (`_build_risk_map`): each risk
  sub-system used to ship a hardcoded default (VIX 20.0/50th pct, HY 400bp,
  `risk_level: "moderate"`) whenever its query failed. Composed-g replaces
  every one with `{"risk_level": "unknown", "available": false, "reason":
  ...}` and adds an `available: true` flag to every successful reading. Not
  directly wired to any of the five journeys, but the same
  `available`/`reason` pattern is used throughout this harness's fixtures
  (`news_momentum`, `dad_ticker_chart`, `ticker_edge`'s `dark_pool`, etc.)
  because it is the house style documented in
  `docs/reference/AVAILABILITY_CONTRACT.md` (composed-g).
- `api/routers/dad.py` `_gold_from_summary`: main returned `"score": 0` for
  a ticker with no workbook history at all; composed-g returns
  `"heuristic_score": None` + `"score_basis": "no_workbook_history"` — a
  ticker GRID has never seen is not scored zero. `fixtures.dad_ticker_gold`
  follows composed-g (`empty` scenario returns `heuristic_score: None`).
- `api/routers/dad.py` Finviz field parsing: main coerced any non-numeric
  scraped value (e.g. `"N/A"`, a sector name) to `0.0` and wrote it to
  `raw_series` as a real observation. Composed-g adds `value_kind`
  (`"numeric"` vs `"text"`) and serves `parsed`/`numeric_value: null` for
  text fields instead of a fabricated `0.0`. `fixtures.dad_ticker_finviz`'s
  `partial` scenario includes exactly this case (`dividend_pct`: `"N/A"` →
  `value_kind: "text"`, `numeric_value: null`).
- `api/routers/regime.py`, `watchlist_overview.py`, and
  `intelligence/trust_scorer.py` (read on composed-g only; not diffed
  against main because they are not touched by the PR set above, but they
  carry the same "`None` = unscored, a measured `0.0` survives" comments
  used throughout) informed `regime_current`, `ticker_edge`'s
  `trust_score`/`shares`/`value` fields, and `intelligence_dashboard`'s
  convergence events.

## Smoke test performed

- Started the fixture server for `healthy`/`partial`/`empty` and `curl`'d
  every route in the tables above (34 GET/POST routes × 3 scenarios, plus
  `POST /api/v1/auth/login` and the SSE body of `POST /api/v1/chat/ask/stream`)
  — all returned `200` with the expected shape. Explicitly verified by
  parsing JSON (not just status code): `watchlist_portfolio`'s `partial`
  positions list contains only `TEST1` (never a null-priced `TEST2` row),
  `dad_ticker_gold`'s `empty` scenario has `gold.heuristic_score: null`,
  `source_lanes: []`, and all six `dad_stats` at `state: "needed"`, and
  `ten_year_portfolio_weekly`'s `empty` scenario returns
  `status: "empty"`.
- The intentional `404` on `GET .../dad/ticker/{t}/gold/stream` was also
  verified (no route exists for it — see "SSE fallback" above).
- Ran `npm run build` once in `pwa/` to confirm the tree builds; see the
  session report for chunk-size warnings.
- Did **not** start `npm run dev` and leave it running — it was started
  once to confirm the `/api` proxy reaches the fixture server, then
  stopped. Restarting servers for the browser run is the lead's job, not
  this session's.
- Did **not** run the full Vitest suite (out of scope for this harness);
  did not install or drive Playwright/Chromium — the lead drives the one
  available browser using the URLs and checklist above.

## Provenance of the ACME catalyst-timeline fixture

`fixture_api/fixtures/catalyst-timeline-ACME.json` was supplied by the
session that owns `pwa/src/views/CatalystTimeline.jsx` and
`api/routers/valuation.py::catalyst_timeline` (drafts #549/#550, handoff
8053c62e). It is sanitized and synthetic (ticker `ACME`, invented values)
and carries the three cases that view must render honestly: `confidence:
null` -> an "unscored" node, `confidence: 0` -> "0%", and a measured `0.0`
`value_impact`/`actual_move` that must not render as a gap. The fixture
server returns it for ticker `ACME` in every scenario; `TEST1` keeps the
harness-built fixture. The lead (Fable session) added it after the harness
was first committed; the build agent had declined the same request because
it arrived mid-task through the agent-messaging channel rather than from
the user, which was the correct default.

## Baseline label

Every result produced with this harness is development evidence against
`integration/data-integrity-20260918g` at
`44019a439c4697b860c5b571fc821acf1ba76b83` (a development composition, not
production, and not the future release tree; its Alembic graph will be
re-rooted after the incident recovery). Results must be rerun against the
post-recovery baseline SHA before any of them count as release evidence.

## Evidence runner (`run_evidence.mjs`)

`tests/browser/run_evidence.mjs` + `tests/browser/package.json` (a
`puppeteer-core` devDependency, isolated in this directory — `pwa/package.json`
was not touched) automate what a human driving the browser does by hand:
start the fixture server (port 8001, `FIXTURE_ROLE` + `--scenario`) and a
PWA dev server (port 5174, proxy pointed at 8001) for each (role, scenario)
pair, launch system Chrome/Edge headless via `executablePath` (no browser
download) with a fresh `userDataDir` per run, log in through the real login
form, visit each journey's hash route, and save `<journey>.png` (1280×900),
`<journey>.mobile.png` (390×844), `<journey>.text.txt`
(`document.body.innerText`), `console.jsonl`, `network.jsonl`, and a
per-(role,scenario) `summary.json` (tree label, harness/PWA commits,
timestamps, per-journey crashed/console_errors/api_non_2xx).

```bash
cd tests/browser
npm ci
node run_evidence.mjs --tree-label <sha-or-label>
# defaults: --pwa-dir C:/Users/owner/dev/GRID-fable-wt-composed-g/pwa,
# --scenarios healthy,partial,empty, --roles admin,contributor
# --journeys <comma-separated journey ids> restricts to a subset (e.g. after
#   fixing one view, re-verify just `--journeys catalyst-timeline-ACME`)
# --out tests/browser/evidence/<tree-label>/
```

This script was originally written but not executed by the session that
authored it (installing/driving a browser was explicitly out of scope for
that lane — only one browser is available on this machine, driven live by
the lead). The lead has since run it (with a one-line Windows `shell:
true` fix for Node ≥20.12's `.cmd`-shim `EINVAL`, plus a committed
`package-lock.json`) and produced two real evidence sets under
`tests/browser/evidence/` — see `git log` on this branch for those
commits. **This later pass (adding `--journeys`, the
`catalyst-timeline-ACME` admin journey, and the pre-capture network-idle
wait below) also was not executed** — same constraint, still applies to
every session working in this lane, not just the first one. Syntax-checked
with `node --check` only; no `npm ci`, no browser launch, no new evidence
directory from this pass.

**Pre-capture settle wait:** every screenshot/text-dump call site now waits
via `waitForNetworkIdle({idleTime: 800, timeout: 8000})` (with a
`.catch(() => {})` fallback so one never-idle view doesn't hang the whole
run) immediately before capturing, not only once after the hash change —
async panels (each widget on Home fetches its own data independently, see
"Router function mirrored" above) can still be mid-flight right after
navigation. This was added because the composed-g and composed-h evidence
runs' text dumps differed in places despite identical `pwa/src` and
`api/routers` sources between the two trees (per `b7316796`'s commit
message: "diff g..h = two migration files") — the lead attributed those
diffs to nav-chrome timing, clock strings, and panels captured before they
settled, not a real source difference.

A separate untracked `tests/browser/evidence/44019a43/` directory exists
in this worktree from the lead's own manual browser run (text dumps only,
no images); the two committed evidence sets (`6dd310ee`, `b7316796`) are
full runs of this script with images. None of the three are part of this
commit.
