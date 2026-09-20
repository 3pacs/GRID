# Hermes repair pulls: bounded window, per-cycle budget, resumable backlog, no overlap (review-only)

Branch: `fable/hermes-repair-bound-20260919` off `origin/main` @ `314e88d7a606faaf709a0fb8a387386bf9c533b2`
Scope: development only, draft PR, **no merge, no production access, no SSH, no database connections**.

## Traced facts (production, read-only, 2026-09-19 — from the controller; not re-audited here)

All line numbers below are on this branch's base commit `314e88d7`.

1. Every 6th cycle, `run_cycle` step 5 calls `run_self_diagnostics` inside a
   plain `try/except` with **no** `_run_with_timeout`
   (`scripts/hermes_operator.py:2364-2371`):

   ```python
   # 5. Self-diagnostics — only every 6th cycle (30 min)
   if state.cycle_count % 6 == 0:
       try:
           state.current_step = "diagnostics"
           diag = run_self_diagnostics(engine, hermes_ok, health, state, dry_run=dry_run)
           cycle_result["diagnostics"] = diag
       except Exception as exc:
           log.warning("Self-diagnostics failed: {e}", e=str(exc))
   ```

   The LLM emits `ACTION: REPULL:yfinance` because `health["db"]["stale_sources"]`
   lists `yfinance` — its `source_catalog.last_pull_at` was 2026-07-14, 67 days
   old, against `DATA_FRESHNESS_THRESHOLD_HOURS = 26`. `_execute_hermes_repair_command`
   (`scripts/hermes_fixers.py:839-850`) runs `_retry_source(source, engine, attempt=1)`
   synchronously.

2. `_retry_source`'s docstring (`scripts/hermes_fixers.py:1184-1223`) said attempt 1
   was "standard pull (recent data only)", but for `pull_all(start_date=...)` pullers
   it passed **no** `start_date`, so `YFinancePuller.pull_all`
   (`ingestion/yfinance_pull.py:300-304` at base) defaulted to
   `start_date="1990-01-01"`: 70 tickers x 6 fields, each doing
   `SELECT DISTINCT obs_date FROM raw_series WHERE series_id=…` over
   1.3-2.3M rows per series, then skipping every existing date -> `inserted 0
   rows` per ticker, 7-40s per ticker. The equity data was **not** actually
   stale: `YF:SPY:close` etc. had `obs_date` through 2026-09-18, pulled daily
   at ~02:00Z by the regular scheduler. The zero inserts were duplicate-only.
   (FX pairs like `USDCAD=X` did get a few real rows — a small genuine gap,
   separate from this fix.)

3. Cycle 6300 stayed in that repull for 71 minutes until the cycle watchdog
   (`CYCLE_TIMEOUT_SECONDS = 4500`, `_run_cycle_with_timeout`) logged
   `Cycle 6300 TIMED OUT after 4500s (stuck on: diagnostics) — blacklisting
   and starting fresh`. The worker thread was abandoned, not killed
   (`_run_with_timeout`'s own documented behavior): it was still pulling
   tickers 25 minutes into cycle 6301. Because the call never returned,
   `_retry_source`'s `UPDATE source_catalog SET last_pull_at = NOW()` and the
   handler's `state.cooldowns.record_attempt(source, success=True)` were
   never reached, so `last_pull_at` stayed 2026-07-14, the source stayed
   "stale", the cooldown never engaged, and the next diagnostics cycle
   repeated the same full-history repull — possibly while the previous
   orphan was still running. Every step scheduled after diagnostics that
   cycle (including the sector-health step from #580 and `intelligence_tasks`)
   was starved.

4. Design constraints from the controller: bound repair work so it cannot
   starve due maintenance; no overlap with abandoned workers; prove
   progress under slow provider responses and repeated zero-insert
   results; do NOT perform or enable any historical repair/backfill; no
   larger cycle timeout.

## Design

### A. Repair never pulls full history

`scripts/hermes_fixers.py::_retry_source` now always computes a bounded
window for any puller whose pull method accepts `start_date` (checked via
`inspect.signature`, not a `hasattr(puller, "pull_all")` guess like the
code it replaces):

```python
REPAIR_LOOKBACK_DAYS = 7   # module constant

start_date = date.today() - timedelta(days=REPAIR_LOOKBACK_DAYS * attempt)
```

Attempt 1 -> 7 days back, attempt 2 -> 14, attempt 3 -> 21 — still capped,
never full history. The module docstring is explicit that a full-history
backfill is a separately authorised operation (a human running one of the
existing one-off scripts, e.g. `scripts/backfill_celestial.py`), never a
side effect of a self-diagnostics repair command. Pullers keyed by
`days_back` instead of `start_date` are untouched — they were never the
source of this defect — and keep the pre-existing widen-on-retry scaling
(`days_back *= attempt + 1` from attempt 2 on).

The docstring that claimed attempt-1 was already "recent data only" is
corrected to describe the actual bounded behavior.

### B. Cooperative budget

`REPAIR_BUDGET_SECONDS = 180` (must stay under
`DIAGNOSE_PULLS_TIMEOUT_SECONDS = 240` and the new
`DIAGNOSTICS_TIMEOUT_SECONDS = 300`; pinned by
`tests/test_hermes_repair_bounded.py`). `run_self_diagnostics` and
`diagnose_and_fix_pulls` each compute `deadline = time.monotonic() +
REPAIR_BUDGET_SECONDS` once at entry and build
`should_continue = lambda: time.monotonic() < deadline`, threaded through
`_execute_hermes_repair_command` -> `_retry_source` -> the puller, when the
puller accepts a `should_continue` keyword.

`YFinancePuller.pull_all` (and only `pull_all` — `pull_ticker` was left
alone; its per-field work inside one ticker is not where the unbounded
loop lived) gained an optional `should_continue: Callable[[], bool] | None
= None` parameter, checked between tickers. When it returns `False`,
`pull_all` stops and returns a dict (not its usual `list[dict]`):
`{"status": "PARTIAL", "stopped_by_budget": True, "results": [...
per-ticker results attempted so far...], "tickers_not_attempted": [...]}`.
Every existing caller of `pull_all` (there are ~15 across `scripts/` and
`ingestion/`) never passes `should_continue`, so they keep getting the
plain `list[dict]` they always got — this is purely additive.

`_retry_source` reads that dict: if `stopped_by_budget`, it persists the
remainder into `state.repair_backlog[source]` (new field on
`OperatorState`, `scripts/hermes_health.py`, included in `to_dict()` and
restored in `hydrate_from_snapshot()`) instead of advancing
`last_pull_at`, and does **not** advance `last_pull_at` (the pull didn't
actually finish). The REPULL handler in `_execute_hermes_repair_command`
records `state.cooldowns.record_attempt(source, success=False,
error="budget")` on a budget stop, so the per-source cooldown engages —
the next diagnostics cycle 30 minutes later, not immediately. On the next
attempt, if a backlog exists for the source and the puller accepts an
explicit `ticker_list`, `_retry_source` passes the backlog as
`ticker_list` instead of the puller's default full list, so the pull
resumes from where it stopped rather than restarting from the first
ticker. The backlog is cleared once a repair for that source completes
without being stopped by budget.

Pullers that don't accept `should_continue` are unchanged — called exactly
as before, one call, uninterruptible for its duration. `pull_ticker`
callers, `_FunctionPuller`-registry entries, and every non-yfinance puller
fall in this category.

### C. The diagnostics step itself is bounded

`run_cycle`'s step 5 is extracted into
`_run_diagnostics_step(engine, hermes_ok, health, state, dry_run,
cycle_result)` (mirroring the `_run_sector_and_intelligence_steps`
extraction from #580), which wraps `run_self_diagnostics` in
`_run_with_timeout("diagnostics", ..., DIAGNOSTICS_TIMEOUT_SECONDS)` with
`DIAGNOSTICS_TIMEOUT_SECONDS = 300` (LLM call observed ~60s +
`REPAIR_BUDGET_SECONDS` (180) + headroom). This is the backstop for
anything inside diagnostics that doesn't participate in the cooperative
budget (the LLM call itself, or a puller that doesn't accept
`should_continue`).

Deliberately **no** `can_retry("diagnostics")` check added on timeout —
same reasoning as `_run_sector_and_intelligence_steps`'s own note: the
blacklist entry `_run_with_timeout` writes on a timeout is honoured only
by four existing call sites (`oracle_cycle`, `signal_classification`,
`anomaly_narration`, `knowledge_mapping`); `diagnostics` is not one of
them, and adding that check now would make a single timeout block every
diagnostics cycle for `TIMEOUT_BLACKLIST_HOURS` (24h) on top of a step
that already has its own budget.

### D. No overlap with abandoned workers

Module-level `_REPAIRS_IN_FLIGHT: dict[str, dict]` + `threading.Lock` in
`scripts/hermes_fixers.py`. `_retry_source` registers
`{"started": monotonic, "token": n, "thread": current_thread_ident}` under
the lock at the start of the call and removes its own entry in `finally`
(only if its token still owns the slot). If an entry exists for the source
whose thread is still alive (checked via `threading.enumerate()`), a new
`_retry_source` call for the same source returns
`{"status": "skipped", "reason": "in_flight", "age_s": ...}` immediately,
without pulling; the REPULL handler logs it and records nothing (does not
touch the cooldown, since the in-flight attempt owns that outcome). The
`should_continue` closure `_retry_source` builds also checks that its own
token is still the current entry for the source, so if that entry were
ever taken over by a fresher attempt, an abandoned worker's own puller
loop would see `should_continue()` go `False` and stop at its next ticker
boundary. (Under the guard above, a genuinely live thread is never
superseded — this token check is defense-in-depth for the case a
`_REPAIRS_IN_FLIGHT` entry is otherwise replaced — and is covered directly
by `tests/test_hermes_repair_bounded.py`'s
`test_should_continue_reflects_a_superseded_token`.)

### E. Freshness signal

Traced `ingestion/scheduler.py` (the module `CLAUDE.md` already documents
as "the authoritative scheduler") and `ingestion/smart_scheduler.py`
(a second, separate ingestion scheduler) for what actually advances
`source_catalog.last_pull_at` on a regular (non-repair) pull:

- `ingestion/scheduler.py::start_scheduler()` wires the real 4x/day cron
  (`13:30`, `16:00`, `20:00`, `22:00` UTC, every weekday) to
  `run_daily_pulls(start_date=date.today().isoformat())` — a bounded,
  today-only pull. This is the function that actually keeps `YF:SPY:close`
  etc. current through 2026-09-18 (traced fact #2 above). Inside
  `run_daily_pulls`, the yfinance block (`yf_puller.pull_all(start_date=...)`,
  originally lines 1248-1276) had **no** call updating
  `source_catalog.last_pull_at` on success — confirmed by grep: the only
  call site of the module's own `_touch_source_catalog_last_pull` helper
  (defined at lines 183-212) is inside the separate, newer
  `run_pull_group`/`_get_pullers_for_group` path (line 357), which
  `start_scheduler()` does not use for the daily domestic pulls. This is
  exactly the freshness-signal bug traced fact #1 describes: `last_pull_at`
  stuck at 2026-07-14 while the data itself was current.
- `ingestion/smart_scheduler.py` is a third, independent scheduling path
  (used by Hermes's own cycle step 3, "smart ingestion") that *does*
  correctly call `self._update_last_pull(name)` on success. It is not the
  path that actually keeps yfinance current in production (its own
  `timeout_s=120` for yfinance's default `pull_all()` — no bounded
  `start_date` passed here either — would very likely time out against the
  same existing-dates-scan cost described in fact #2, for the same reason
  a bounded repair needed fixing; that is a second, related but distinct
  latent defect in `smart_scheduler.py`'s yfinance registry entry, **not
  fixed here** — it's out of scope for a repair-path fix and needs its own
  trace of whether `smart_scheduler` ever actually completes a yfinance
  tick in production).

This was a single, clearly-correct place: added the same one-line,
best-effort, swallow-on-error `UPDATE source_catalog SET last_pull_at =
NOW() WHERE LOWER(name) = LOWER('yfinance')` immediately after the
existing success log line in `run_daily_pulls`'s yfinance block
(`ingestion/scheduler.py`), matching the exact pattern already used by
`_retry_source` and `smart_scheduler._update_last_pull`. Scoped to
yfinance only (the source in the traced defect) to keep the change
minimal — the same gap likely exists for FRED's block in the same
function, but that's a separate, unverified claim and not fixed here.

## Not done (explicitly out of scope)

- **No backfill.** No code path added or changed performs, enables, or
  schedules a full-history repull. `REPAIR_LOOKBACK_DAYS` bounds every
  repair attempt; a full-history backfill remains a separately authorised,
  manually-run operation.
- **No larger cycle timeout.** `CYCLE_TIMEOUT_SECONDS` (4500s) is
  unchanged. `DIAGNOSTICS_TIMEOUT_SECONDS` (300s) is a new, much smaller
  sub-budget nested inside it (pinned by
  `tests/test_hermes_repair_bounded.py`).
- The LLM can still emit `ACTION: REPULL:<source>` — that's unchanged and
  intentional (Hermes's own diagnosis stays in control of *what* to
  repair). What changed is that the repair itself is now bounded (window +
  budget), resumable (backlog), non-overlapping, and cools down properly
  on a budget stop.
- The FX gap noted in traced fact #2 (`USDCAD=X` etc. getting a few real
  rows on the old full-history repull) is a small, separate, genuine data
  gap — not addressed here.
- `smart_scheduler.py`'s own yfinance registry entry (no bounded
  `start_date`, a `timeout_s=120` that likely can't complete a full
  `pull_all()` default call) is flagged above as a related but distinct
  latent defect, not fixed in this change.
- FRED's block in `run_daily_pulls` likely has the same missing
  `last_pull_at` update as yfinance's did — not verified or fixed here;
  scoped strictly to the traced yfinance defect.

## Tests

`tests/test_hermes_repair_bounded.py` (new), no DB/network — fake
engines/pullers and monkeypatched `_resolve_puller` /
`run_self_diagnostics` throughout:

1. `TestBoundedRepairWindow` — attempt 1 never requests a `start_date`
   earlier than `today - 7 days`; attempt 3 -> 21 days; `days_back`-style
   pullers keep their pre-existing widen-on-retry behavior unchanged.
2. `TestCooperativeBudgetAndBacklog` — a fake puller whose per-ticker call
   sleeps 0.3s over 20 tickers, with a 1s budget: `_retry_source` returns
   in well under 1.5s with `stopped_by_budget`, the remainder lands in
   `state.repair_backlog`; a follow-up call resumes from exactly that
   remainder instead of the full ticker list; going through
   `_execute_hermes_repair_command`'s `REPULL:` path on a budget stop
   records a failed cooldown attempt.
3. `TestRepeatedZeroInsertResultsStayBounded` — three consecutive
   zero-insert repairs each complete within budget, each advances
   `last_pull_at` (asserted against a fake engine's executed SQL), and the
   cooldown engages after every one (`can_retry` False immediately after).
4. `TestNoOverlappingRepairWorkers` — a second `_retry_source` call for a
   source whose first call is blocked on a `threading.Event` is skipped
   `in_flight` without pulling; releasing the event lets the first finish
   and the in-flight entry is cleaned up so a third call is not blocked
   forever; a focused unit test on the `should_continue` closure confirms
   it goes `False` once its token is no longer the current in-flight entry.
5. `TestDiagnosticsStepHasItsOwnTimeout` — `_run_diagnostics_step` returns
   in well under 3s when `run_self_diagnostics` is patched to block past a
   patched `DIAGNOSTICS_TIMEOUT_SECONDS = 1`, proving due maintenance
   scheduled after diagnostics in `run_cycle` is reachable regardless of a
   hang inside this step; also confirms the step is a no-op off the
   every-6th-cycle schedule.
6. `TestBudgetConstantsPinInsideStepTimeouts` — `REPAIR_BUDGET_SECONDS <
   DIAGNOSE_PULLS_TIMEOUT_SECONDS`, `REPAIR_BUDGET_SECONDS <
   DIAGNOSTICS_TIMEOUT_SECONDS`, `DIAGNOSTICS_TIMEOUT_SECONDS <
   CYCLE_TIMEOUT_SECONDS`, and `REPAIR_LOOKBACK_DAYS` stays small.

Run: `DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_hermes_*.py
tests/test_hermes_repair_bounded.py -q` — see the PR description for the
pass count from this branch. One pre-existing Windows-only path failure in
a `fix_output_dirs` test is a known, unrelated environment difference; it
is reported if seen, not treated as a regression from this change.

## Not authorized

This branch and its draft PR do not merge, deploy, restart any service,
force a Hermes cycle, or perform/enable any backfill. All of the above is
development-only, read-only against production.
