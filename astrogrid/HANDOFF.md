# AstroGrid Handoff Log

Last updated: 2026-03-26
Branch: `codex/astrogrid-prototype`

## Purpose

This file is the branch-local work journal for AstroGrid. Use it to understand current status, open risks, and the next best action without relying on the repo-root coordination file.

## Current State

- AstroGrid scaffold has been expanded into a multi-view frontend with a working Orrery, lunar surfaces, correlations heatmap, timeline, ephemeris, narrative, and settings
- A production-oriented frontend commit already exists on this branch: `50b68b9`
- The latest integrated hardening pass is committed as `dc82a2e`
- `codex/astrogrid-prototype` has been pushed to `origin`
- `npm.cmd run build` succeeded on 2026-03-26 after the latest integration pass

## Important Constraints

- Stay inside `grid/astrogrid/`
- Do not modify repo-root `AGENTS.md` from this branch unless the operator explicitly directs it
- Keep live data versus fallback/demo data visibly distinct
- Treat `GET /api/v1/signals/celestial` as the stable backend contract unless the operator relays new endpoint guarantees

## Known Production Gaps

- Backend contract is still partial, so several views remain hybrid live-plus-fallback surfaces
- `three-stack` is still large in the production bundle, although it is lazy-loaded behind the Orrery route
- Endpoint requests still need to be relayed through the operator if we want fully live ephemeris, correlations, timeline, narrative, and eclipse data

## Backend Needs To Relay Through Operator

These should be relayed to Claude Code when appropriate:

- standardized `GET /api/v1/astrogrid/ephemeris`
- standardized `GET /api/v1/astrogrid/correlations`
- standardized `GET /api/v1/astrogrid/timeline`
- standardized `GET /api/v1/astrogrid/narrative`
- standardized eclipse payload if eclipse countdowns should be live

## Work Journal

### 2026-03-26 1

Completed:

- Re-read `AGENTS.md` and `grid/astrogrid/CODEX.md`
- Audited current AstroGrid state and identified production risks
- Spawned three parallel workers for shell hardening, data-behavior hardening, and Orrery/mobile optimization
- Created `ROADMAP-Q2-2026.md` and this handoff log

In progress:

- Worker A: app shell hardening in `src/App.jsx`, `src/store.js`, `src/components/NavBar.jsx`, `src/main.jsx`
- Worker B: live versus fallback clarity in `src/api.js`, `src/views/Correlations.jsx`, `src/views/Timeline.jsx`, `src/views/LunarDashboard.jsx`, `src/views/Narrative.jsx`
- Worker C: Orrery/mobile/runtime safety and design-token consistency in `src/components/PlanetaryOrrery.jsx`, `src/views/Orrery.jsx`, `index.html`, `src/styles/tokens.js`

Next recommended step:

- Integrate worker patches carefully, run build when approval is available, then push the branch and update this log with exact verification results

### 2026-03-26 2

Completed:

- Integrated worker changes for app-shell hardening, including safe storage access, route-sync, and Settings navigation
- Added branch-local 90-day roadmap in `ROADMAP-Q2-2026.md` with a product and revenue plan aimed at $100k/month by December 2026
- Improved session telemetry handling so AstroGrid now tracks `idle`, `loading`, `live`, `cached`, `disabled`, and `demo` states
- Updated Settings to report session telemetry honestly instead of guessing from a nonexistent count field
- Updated Ephemeris so it can render normalized API payloads when available while falling back cleanly to deterministic local calculations
- Made data provenance explicit in Correlations, Timeline, Lunar Dashboard, and Narrative
- Aligned Orrery and Narrative to the shared `selectedDate`
- Improved Orrery mobile/runtime behavior, reduced-motion handling, no-WebGL fallback, and deep-space token consistency
- Added the Solar Layer toggle to Settings and wired solar/chinese layer preferences more consistently across views
- Ran `npm.cmd run build` successfully in `grid/astrogrid`

Verification:

- Build output written to `grid/astrogrid_dist/`
- Build passed with one remaining warning: the lazy-loaded `three-stack` chunk is still over the chunk warning threshold

Files changed in the hardening pass committed as `dc82a2e`:

- `grid/astrogrid/index.html`
- `grid/astrogrid/src/App.jsx`
- `grid/astrogrid/src/components/NavBar.jsx`
- `grid/astrogrid/src/components/PlanetaryOrrery.jsx`
- `grid/astrogrid/src/main.jsx`
- `grid/astrogrid/src/store.js`
- `grid/astrogrid/src/styles/tokens.js`
- `grid/astrogrid/src/views/Correlations.jsx`
- `grid/astrogrid/src/views/Ephemeris.jsx`
- `grid/astrogrid/src/views/LunarDashboard.jsx`
- `grid/astrogrid/src/views/Narrative.jsx`
- `grid/astrogrid/src/views/Orrery.jsx`
- `grid/astrogrid/src/views/Settings.jsx`
- `grid/astrogrid/src/views/Timeline.jsx`
- `grid/astrogrid/HANDOFF.md`
- `grid/astrogrid/ROADMAP-Q2-2026.md`

Open follow-through:

- Relay standardized AstroGrid endpoint requests through the operator if we want to move more views from fallback into full live mode
- Start Phase 2 deliverables from the roadmap: pricing, paid-beta narrative, and demo collateral

Next recommended step:

- Start the paid-beta packaging work described in `ROADMAP-Q2-2026.md`
- Prepare operator-facing pricing, positioning, and demo collateral
- Relay endpoint standardization asks through the operator for the remaining hybrid views
