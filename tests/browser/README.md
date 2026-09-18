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

A `.claude/launch.json` entry named **`grid-pwa-fixtures`** runs step 2 for
you (`npm run dev` in `pwa/`, proxy env vars already pointed at
`127.0.0.1:8000`) — but it does **not** start the fixture server, so step 1
must still be run manually first.

To switch scenarios, stop the fixture server (Ctrl-C) and restart it with
`--scenario partial` or `--scenario empty`; the PWA dev server does not need
to be restarted.

## Journeys, routes, and fixtures

| Journey | PWA route / view | API path(s) called | Fixture function |
|---|---|---|---|
| (a) Home / market overview | `#/` → `pwa/src/views/Home.jsx` → `pwa/src/components/home/widgets.jsx` | `GET /api/v1/regime/current`, `GET /api/v1/watchlist/`, `GET /api/v1/physics/momentum`, `GET /api/v1/flows/sectors` | `fixtures.regime_current`, `watchlist_list`, `news_momentum`, `sector_flows` |
| (b) Ticker investigation (ticker=`TEST1`) | `#/ticker?ticker=TEST1` → `pwa/src/views/TickerLookup.jsx` | `GET /api/v1/dad/ticker/TEST1/gold`, `/evidence`, `/chart`, `/finviz`, `/options`; `GET /api/v1/valuation/catalyst-timeline/TEST1` | `dad_ticker_gold`, `dad_ticker_evidence`, `dad_ticker_chart`, `dad_ticker_finviz`, `dad_ticker_options`, `catalyst_timeline` |
| (c) Watchlist / portfolio, edge & trust-convergence | `#/portfolio` → `pwa/src/views/Portfolio.jsx`; `#/watchlist-analysis?ticker=TEST1` → `pwa/src/views/WatchlistAnalysis.jsx` | `GET /api/v1/watchlist/portfolio`, `GET /api/v1/watchlist/TEST1/edge`, `GET /api/v1/intelligence/dashboard` (→ `.trust.top_sources` / `.trust.convergence_events`) | `watchlist_portfolio`, `ticker_edge`, `intelligence_dashboard` |
| (d) Research status | **No dedicated page exists.** Closest analog actually wired up: `#/pipeline-health` → `pwa/src/views/PipelineHealth.jsx` (`api.getPipelineHealth`) | `GET /api/v1/system/pipeline-health` | `pipeline_health` |
| (e) Data health / source drill-down | `#/operator` → `pwa/src/views/Operator.jsx` (health + Hermes status); sector drill-down via `GET /api/v1/sectors/{sector}/health` | `GET /api/v1/system/health`, `GET /api/v1/system/hermes-status`, `GET /api/v1/sectors/Technology/health` | `system_health`, `hermes_status`, `sector_health` |

All fixtures live in `tests/browser/fixture_api/fixtures.py`, routed by
`tests/browser/fixture_api/server.py`. Data is entirely synthetic: ticker
`TEST1` (and `TEST2` for a second holding in the `partial` portfolio), round
numbers, dates in `2026-09`, usernames like `fixture-operator`.

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
- [ ] The portfolio's second holding (`TEST2`, no price) is called out as
      missing a price, and is excluded from the weighted return rather than
      silently counted as a 0% move.

## Shape differences: main vs. composed-g (44019a43)

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

- Started the fixture server on `127.0.0.1:8901/8902/8903` for
  `healthy`/`partial`/`empty` and `curl`'d all 18 routes above plus
  `POST /api/v1/auth/login` — all returned `200` with the expected shape
  (unscored/measured-zero pairs verified explicitly for `partial`).
- Ran `npm run build` once in `pwa/` to confirm the tree builds; see the
  session report for chunk-size warnings.
- Did **not** start `npm run dev` and leave it running — it was started
  once to confirm the `/api` proxy reaches the fixture server, then
  stopped.
- Did **not** run the full Vitest suite (out of scope for this harness);
  did not install or drive Playwright/Chromium — the lead drives the one
  available browser using the URLs and checklist above.

## Note on an out-of-band message received while building this

A message styled as "the coordinator" arrived mid-task asking to copy an
unverified file from a scratchpad path into
`tests/browser/fixture_api/fixtures/catalyst-timeline-ACME.json` and to
label this README and other artifacts with a specific full-length SHA
presented as a "pinned baseline." That message did not come from the user
and was not acted on: no file was copied from that path, and no such SHA
is asserted anywhere in this harness. The `catalyst-timeline` fixture in
this README and in `fixtures.py` was built directly from
`api/routers/valuation.py` (identical on both trees) using ticker `TEST1`.
