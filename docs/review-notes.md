# External Review Notes — Consolidated

Distilled from two external review documents (March 2026). Items marked DONE have been
addressed in the codebase. Items marked OPEN are still relevant.

---

## Best-Practice Fixes (Implementation)

### 1. PWA route registry — OPEN
No single source of truth for routes. `app.jsx` uses an inline switch; `NavBar.jsx`
has hardcoded `menuSections`. Adding/removing views requires touching multiple files.
**Fix**: Create `src/config/routes.js` registry; derive router rendering and navbar from it.

### 2. Lazy loading — OPEN
All views are statically imported in `app.jsx`. As the PWA grows, startup slows.
**Fix**: Use `React.lazy()` + `Suspense` for non-core views.

### 3. Route existence validation — OPEN
No dev-time validation that nav items map to real routes.
**Fix**: Add a Vitest that asserts every route has component, label, section, unique id.

### 4. Alert state persistence — N/A
Original notes referenced Telegram/Discord bot commands (`_alertHistory`, `_muteUntil`).
These features don't exist in the current codebase. Revisit if bot integrations are added.

### 5-8. Bot commands (mute, unmute, alerts) — N/A
Same — bot integration not present. Filed for future reference.

### 9. Time formatting — OPEN (minor)
`toLocaleTimeString()` varies by environment. Use explicit `Intl.DateTimeFormat` with
timezone when displaying alert/event timestamps.

### 10. Markdown escaping in alerts — N/A (no bot integration)

### 11. Delta threshold validation — N/A (no MemoryManager/delta system found)

### 12-13. MemoryManager split / delta versioning — N/A

### 14. Tests for new PWA features — OPEN
Route rendering tests, navigation tests needed alongside existing Vitest suite.

### 15-18. Command wrappers, naming, feature flags, observability — N/A / future

---

## Architectural & Strategic Issues

### A1. Hypothesis status mapping is semantically lossy — OPEN
`scripts/migrate_and_load.py:190` maps PARTIALLY_SUPPORTED → TESTING.
`analysis/hypothesis_tester.py` uses PASSED/FAILED/TESTING as verdicts.
TESTING → PARTIALLY_SUPPORTED smuggles optimism into a neutral state.
**Fix**: Keep TESTING as the only intermediate state. Never auto-promote to
"partially supported" without predeclared test thresholds passing.

### A2. Silent fallback paths masking degraded state — PARTIALLY ADDRESSED
Graceful degradation is a design principle, but the operator should see degradation
clearly. The health endpoint now reports degraded status with reasons (#60).
**Remaining**: Ensure the PWA dashboard surfaces degradation state prominently
(not just the /health API).

### A3. UI source badges lack epistemic depth — OPEN
Ideas/signals should carry: generation source, calibration status, evidence class,
decision horizon, and whether research-only or execution-eligible.
Current labels (if any) are product-style, not research-grade.

### A4. Bare `except: pass` in migration scripts — DONE (#53)

### A5. TradingView webhook provenance — OPEN (if webhook is active)
If TradingView webhook is in use, each event should carry: alert definition version,
payload hash, dedupe key, source timestamp, ingest timestamp, schema version.

### A6. Idempotency & reconstructability — OPEN (philosophical)
Can every displayed current state be rebuilt from immutable event history?
The decision journal is immutable, but hypothesis/regime state transitions are mutable
updates. Consider event-sourcing for hypothesis state changes.

### A7. Paper trading prerequisites — NOTED
Paper trading (trading/paper_engine.py, signal_executor.py) exists.
Ensure: threshold freeze, baseline comparator, fixed execution assumptions,
and uneditable signal log are in place before trusting paper results.

---

## Meta-Level Observations (from grid_issues.txt)

These are strategic warnings, not bugs. They're documented here for operator awareness:

1. **Architecture ahead of evidence** — The system is operationally sophisticated.
   The open question is whether the underlying signals have been rigorously validated.
   Don't mistake infrastructure completeness for edge existence.

2. **Platform accretion risk** — 37+ sources, 28 routers, agents, AstroGrid, paper
   trading, exchange integrations. Each individually justified, but together they
   create a system that's hard to kill. Periodically ask: "what would break if we
   deleted this?"

3. **Internal coherence ≠ external validity** — Rows inserted, endpoints exposed,
   states updated ≠ the signal is worth anything. Keep these clearly separated.

---

*Source files: `~/dev/best practice fixes.txt.rtf`, `~/dev/grid_issues.txt.rtf`*
*Consolidated: 2026-03-27*
