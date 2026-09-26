# Hermes sector-health snapshot: due-period scheduler (review-only)

Branch: `fable/hermes-sector-schedule-20260919` off `origin/main` @ `ae726251`
Scope: development only, draft PR, **no merge, no production access**.

## Traced facts (production, read-only evidence)

- The daily sector-health step in `run_intelligence_tasks`
  (`scripts/hermes_operator.py`, ~lines 1139-1164 at `ae726251`) ran only
  when `now.hour == 3 and now.minute < 10` **and**
  `_hours_since(state.last_sector_health) >= 20`.
- `run_intelligence_tasks` is invoked late in `run_cycle` (call at ~line
  2396), after obsidian sync, pull fixes, stale refresh, smart ingestion,
  conflict resolution, diagnostics, autoresearch, digests, supply chain,
  news contagion, astrogrid, etc. Inside `run_intelligence_tasks` itself,
  the 30-minute active-hypothesis scorer runs **first** (up to 600s; step
  cap 900s).
- Cycles are nominally 5 minutes apart but frequently run much longer — a
  cycle that started 02:55Z had not reached the sector-health check by
  03:13Z.
- Production `analytical_snapshots` (category `pipeline_summary`,
  subcategory `hermes_operator`) show `last_sector_health` took only two
  values in the last 400 snapshots: `2026-07-13T03:00:03Z` and
  `2026-09-13T03:08:17Z`.
- `sector_health_snapshots` has exactly **one** write day in the trailing
  30 days (2026-09-13, 20 rows).
- Conclusion supported by the evidence: the step is **executed** only on
  the rare day the check happens to be evaluated inside the 10-minute
  window (absent execution, not absent writes). The state marker
  `state.last_sector_health = now` was set even when the snapshot failed,
  so a failed execution also blocked retries for 20h.

## Design

### `daily_task_due(last_success, now, boundary_hour)`

Pure helper (`scripts/hermes_operator.py`). A due *period* is the UTC day
starting at `boundary_hour:00`. The task is due at any evaluation at or
after the most recent boundary crossing, as long as no successful run has
landed since that boundary — no minute window, no upper bound on lateness.
Naive datetimes are normalised to UTC rather than rejected (matching
`OperatorState.hydrate_from_snapshot`'s convention for old snapshots).

### `_maybe_run_sector_health_snapshot(engine, state, now, results)`

Extracted the sector-health block out of `run_intelligence_tasks` into its
own function so it can be unit-tested without exercising the rest of the
(DB/network-touching) intelligence pipeline. Wiring:

1. `sector_health_period_due = daily_task_due(state.last_sector_health, now, SECTOR_HEALTH_BOUNDARY_HOUR)` — gates on the due period.
2. If due, checks whether `state.last_sector_health_attempt` already falls
   inside the *same* due period (via the same `daily_task_due` predicate
   applied to the attempt marker). If it's a fresh period, run
   immediately and reset the attempt counter.
3. If an attempt already happened this period (i.e. a prior failure),
   apply retry backoff: `SECTOR_HEALTH_RETRY_BACKOFF_MINUTES = 60` between
   attempts, capped at `SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY = 5` attempts
   per due period.
4. On success, `state.last_sector_health = now` (marks the day done). On
   failure, only `state.last_sector_health_attempt` and
   `state.sector_health_attempt_count` advance — the day is not marked
   done, so a later evaluation (subject to backoff/cap) can retry.

### Idempotency

`intelligence.sector_health.snapshot_all_sectors` already upserts one row
per `(sector_name, snapshot_date)` via
`INSERT ... ON CONFLICT (sector_name, snapshot_date) DO UPDATE` — cited in
a comment on the new block. The state marker exists to skip redundant
compute/DB work, not to prevent duplicate rows; a test exercises the real
upsert SQL against an in-memory fake engine to confirm two executions in
the same due period write no duplicate rows independent of the scheduler.

### State persistence (`scripts/hermes_health.py`)

Two new fields on `OperatorState`, persisted and hydrated the same way as
`last_sector_health` (`to_dict()` ~line 410+, `hydrate_from_snapshot()`
restorable-fields list ~418-479):

- `last_sector_health_attempt: datetime | None` — last attempt (success or
  failure), drives the retry backoff.
- `sector_health_attempt_count: int` — attempts made in the current due
  period, hydrated the same way as the other cumulative int counters
  (`cycle_count`, etc.) so a restart doesn't reset an in-progress backoff
  cap.

Two more fields added for the three findings fixed in this update:

- `last_sector_health_outcome: str | None` — `"success"` /
  `"no_eligible_sectors"` / `"failure"`, persisted in `to_dict()` and
  hydrated in `hydrate_from_snapshot()` via a small string-field loop
  (it isn't a datetime, so it can't reuse the `datetime.fromisoformat`
  loop the other fields share).
- `sector_health_attempt_token: int` — NOT persisted/hydrated across
  restarts (deliberately — it only needs to be correct within one live
  process to guard against an abandoned in-process worker; a restart has
  no in-flight orphan thread from before it, so starting back at 0 is
  correct).

## Follow-up: three review findings fixed (this update)

A review of the original cut found three remaining correctness gaps in the
due-period design above. All three are fixed in this update, still within
the same scope (`scripts/hermes_operator.py`, `scripts/hermes_health.py`,
`intelligence/sector_health.py` additive only, plus tests).

### 1. Snapshot-date identity across a due period

`daily_task_due`/`_period_boundary` define "due period" against a 03:00 UTC
boundary, but `snapshot_all_sectors` stamped `date.today()` — the *local*
calendar date computed fresh on every call. A retry that crosses midnight
(23:30 UTC attempt fails, 00:30 UTC retry succeeds) is **one** due period to
the scheduler but would have written two different `snapshot_date` values,
splitting what should be one idempotent `(sector_name, snapshot_date)` row
into two.

Fix: `snapshot_all_sectors(engine, snapshot_date: date | None = None)` now
takes the date to stamp explicitly. Its default (when called without the
kwarg) is `datetime.now(timezone.utc).date()` — deliberately not
`date.today()`, documented in the docstring. `_maybe_run_sector_health_snapshot`
computes `due_period_date = _period_boundary(now, SECTOR_HEALTH_BOUNDARY_HOUR).date()`
once per attempt and passes it through, so every attempt and retry inside
one due period targets the same row regardless of which calendar date the
wall clock reads at the moment each attempt runs.

### 2. Outcome semantics (success / no_eligible_sectors / failure)

Previously `state.last_sector_health = now` was set unconditionally after
any non-raising call — "0 written, K unavailable" and a partial upsert
failure both counted as success and marked the due period done. Three
explicit outcomes now exist:

- **`success`** — at least one row written and zero upsert failures.
- **`no_eligible_sectors`** — zero rows written, every sector reported
  unavailable, zero upsert failures. This is a legitimate empty day, so the
  due period **is** marked done (no retry storm on a day with genuinely no
  eligible sectors) and the log says so explicitly ("executed, no eligible
  sectors, nothing to write").
- **`failure`** — any upsert failure, or an exception. The due period is
  **not** marked done; the existing backoff/cap retry logic applies.

`snapshot_all_sectors` now returns `upsert_failed` (additive to its
existing warning log at the per-sector upsert `except`) so the scheduler
can tell partial DB failure apart from a clean run. The outcome string is
persisted on `state.last_sector_health_outcome` (hydrated the same
"only if currently unset" way as the timestamp fields, via a small
string-field loop in `hydrate_from_snapshot` since it isn't a datetime).

### 3. Cross-cycle race guard

`run_intelligence_tasks` (which calls `_maybe_run_sector_health_snapshot`)
runs under `_run_with_timeout`, which — by design (see its docstring) —
abandons a timed-out worker thread rather than killing it, so the orphan
keeps running. If it finishes after a later cycle has already started (and
possibly completed) its own attempt, the orphan's belated write could
clobber the newer state.

Fix: a monotonic `state.sector_health_attempt_token` is incremented at the
start of every attempt; the attempt captures its own value locally and
only commits `last_sector_health` / `last_sector_health_outcome` /
`results["sector_health_snapshot"]` if `state.sector_health_attempt_token`
still equals the captured value when the call completes. A mismatch means
a newer attempt has started since, so the result is discarded and logged
("stale sector-health worker result ignored") instead of written.

## Regressions (`tests/test_hermes_sector_schedule.py`, no network/DB)

`TestDailyTaskDue` (pure helper):
- (a) missed window — evaluation at 03:12 with last success yesterday → due once.
- (b) evaluation at 02:59 (already satisfied since yesterday's boundary) → not due.
- (c) second evaluation later the same day → not due.
- (d) restart with hydrated marker → not due.
- never-run and idle/late-catch-up cases.
- (f) naive datetimes are normalised to UTC, not rejected — asserted equal
  to the aware equivalent.
- (g) the old `minute < 10` rule reproduced verbatim and shown to skip the
  03:12 evaluation that the fix now runs.

`TestSectorHealthSchedulerWiring` (the scheduler function, MagicMock
engine + monkeypatched `snapshot_all_sectors`):
- due period runs once and advances the success marker.
- second evaluation in the same due period does not re-run.
- restart with a hydrated `last_sector_health` does not re-run.
- (e) failure path — marker not advanced as success; no retry inside the
  60-minute backoff; retries after backoff; capped at 5 attempts/day.
- a new due period resets the attempt counter even after yesterday's cap
  was exhausted.

`TestSnapshotAllSectorsIdempotency`:
- two calls to the real `snapshot_all_sectors` against a fake in-memory
  engine (with `compute_sector_health` stubbed) write the same number of
  `(sector, date)` rows both times — no duplicates.

`TestSnapshotDateIdentity` (finding #1):
- a 23:30 UTC attempt that fails and its 00:35-next-day retry (still one
  due period) pass the identical `snapshot_date` to `snapshot_all_sectors`.

`TestSectorHealthOutcomeSemantics` (finding #2):
- `success` marks the due period done.
- `no_eligible_sectors` (0 written, all unavailable, 0 upsert failures)
  marks the due period done AND does not retry on a later evaluation in
  the same period.
- a partial upsert failure (some rows written, `upsert_failed > 0`) is
  `failure`, not `success` — due period not marked done, retries after
  backoff.
- an exception is `failure`.

`TestSectorHealthCrossCycleRace` (finding #3):
- an abandoned first attempt that (via a nested real call, not a
  hand-rolled stand-in) finishes after a later attempt has already
  committed its result must not clobber that newer result — the stale
  token is detected and discarded.
- sanity check: the non-racy single-attempt path still commits normally.

`tests/test_sector_health.py` additions (`intelligence/sector_health.py`
changes, additive):
- `snapshot_all_sectors`'s default date comes from
  `datetime.now(timezone.utc).date()`, not `date.today()`.
- an explicit `snapshot_date` is the value bound into every upsert.
- `upsert_failed` counts raised exceptions during the per-sector upsert.

`python -m pytest tests/test_hermes_sector_schedule.py tests/test_hermes_timeout_budgets.py tests/test_sector_health.py -q`
— 35 passed, 2 pre-existing failures unrelated to this change
(`test_endpoint_does_not_cache_unavailable`,
`test_endpoint_shape_and_unknown_sector` fail at import time on
`DB_PASSWORD` / `.env` not being sourced in this worktree's shell —
`api.routers.sector_health` pulls in `config.settings`, which this PR's
diff does not touch).

## Other daily tasks: NOT touched in this PR

Only `scripts/hermes_operator.py` (the sector-health block + its two
helper functions), `scripts/hermes_health.py` (the sector-health state
fields), `intelligence/sector_health.py` (additive: `snapshot_date` param,
`upsert_failed` return field), `tests/test_hermes_sector_schedule.py`,
`tests/test_sector_health.py`, and this handoff doc were changed. No other
daily task's scheduling or behaviour was modified.

`POST /vault/backlinks` (`api/routers/vault.py:433-472`) rewrites tracked
repo files in place (`_run_backlinks` reads every collected markdown file
and calls `f.write_text(...)` when it adds wikilinks) — a
tracked-file-rewrite path noted here because it's the same general class of
"idempotency/identity" concern as this PR's snapshot-date fix, but it is
unrelated to Hermes scheduling and was **not** touched by this PR.

## Sibling minute-window steps (candidates for the same helper, later)

`grep -n "now.hour ==\|now.minute <" scripts/hermes_operator.py` (excluding
this PR's own code/docstrings) finds five more call sites using the same
pattern, all still using minute windows today:

- `is_daily_window = (now.hour == 2 and now.minute < 10)` — daily intel
  batch (source audit, backtest scan, postmortem, options improvement,
  hypothesis review). Already has a partial `is_catch_up` OR-condition for
  restarts, but still ORs in the raw minute window.
- `is_forced_flow_window = (now.hour == 6 and now.minute < 40)` — daily
  forced-flow waterfall briefing.
- `is_enrich_window = (now.hour == 4 and now.minute < 10)` — daily
  connection enrichment.
- `is_weekly_window = is_sunday and (now.hour == 3 and now.minute < 10)` —
  weekly (Sunday) intelligence reports.
- `is_contagion_bt_window = now.minute < 10` — hourly (not daily) contagion
  backtest scoring; same minute-window smell but a different cadence, so
  `daily_task_due` as written (day + boundary hour) would need a sibling
  hourly variant rather than a direct reuse.

None of these were changed in this PR.

## Parent-timeout blocker and own-step fix (2026-09-19)

This section documents a second, follow-up change on top of the due-period
scheduler above (branch `fable/hermes-sector-step-20260919`, off
`origin/main` @ `cee8fc99`). Same scope: development only, draft PR, no
merge, no production access.

### The two defects are separate; neither alone explains the whole gap

1. **Already-merged (above in this doc): the 10-minute evaluation window.**
   `now.hour == 3 and now.minute < 10` made the sector-health step
   *unreachable most of the time even when execution reached it* — it only
   ran on the rare cycle whose evaluation happened to land inside that
   10-minute slice. Fixed by `daily_task_due` (due-period scheduling, no
   minute window).
2. **This change: the 900s `intelligence_tasks` parent timeout is a
   confirmed CURRENT blocker**, independent of (1). Production journal
   evidence — 2026-09-19 03:48, 04:35, 05:02 and 06:05 UTC, and the
   May-2026 log — shows the whole `run_intelligence_tasks` step timing out
   at `INTELLIGENCE_TASKS_TIMEOUT_SECONDS` (900s) on EVERY observed cycle.
   Inside that step the order is: earnings sync → 30-minute active-
   hypothesis scorer (up to 240s) → 4h/6h tasks → the daily intelligence
   batch (the `daily_due` block, which runs with `catch_up=True` on every
   cycle because it never advances `state.last_daily_intel` far enough to
   stop being due) → **then** `_maybe_run_sector_health_snapshot(...)` →
   forced-flow briefing, etc. Because the daily block runs long enough
   (with LLM calls) to consume the step's remaining budget on essentially
   every cycle, `state.last_daily_intel = now` is never reached, and
   nothing placed after it in the function — including the old
   sector-health call — was ever reached either.

Neither defect alone is claimed to explain the entire historical gap
(2026-07-13 to 2026-09-13, 62 days): (1) explains why execution was rare
even when the parent step returned in time; (2) explains why, once (1) was
about to be fixed, the sector-health call would still have been
unreachable on essentially every cycle because the step wrapping it times
out before reaching it. Both are real, independently confirmed, and this
PR's fix (giving sector-health its own dispatch and its own timeout) makes
it reachable regardless of either one.

### What changed

- **`SECTOR_HEALTH_TIMEOUT_SECONDS = 120`** (`scripts/hermes_operator.py`),
  next to the other per-step timeout constants. Observed sector-health run
  time for ~20 sectors is 3-8s; 120s is generous headroom, and — unlike
  `INTELLIGENCE_TASKS_TIMEOUT_SECONDS` — is deliberately NOT sized against
  or coupled to that budget, because the whole point of the split is that
  the two are independent.
- The `_maybe_run_sector_health_snapshot(...)` call was removed from
  `run_intelligence_tasks` (it used to run right after the `daily_due`
  block, which is exactly the code that was never reached — see above).
- New `_run_sector_health_step(engine, state, dry_run)`: thin wrapper that
  builds `now` and a fresh `results` dict and delegates to
  `_maybe_run_sector_health_snapshot` (due-period/backoff/idempotency logic
  unchanged); dry-run short-circuits to `{"skipped": "dry_run"}`.
- New `_run_sector_and_intelligence_steps(engine, state, dry_run,
  cycle_result)`: dispatches the sector-health step FIRST, under
  `_run_with_timeout("sector_health", ..., SECTOR_HEALTH_TIMEOUT_SECONDS,
  state)`, then dispatches `intelligence_tasks` exactly as before (900s,
  unchanged). `run_cycle`'s step 7f now calls this helper instead of
  dispatching `intelligence_tasks` directly.

### Blacklist trace — why the new step does not consult it

`_run_with_timeout` calls `state.cooldowns.blacklist_for_timeout(name)` on
every timeout, sector-health included. Traced: that blacklist entry is
only ever honoured by a call site that explicitly checks
`state.cooldowns.can_retry(<name>)` before running — exactly four call
sites do this (`oracle_cycle`, `signal_classification`,
`anomaly_narration`, `knowledge_mapping`). `intelligence_tasks` and
`resolution` do not consult it either (see `_run_resolution_step`'s own
docstring for the same trace on `resolution`), so for those steps a
timeout's blacklist entry is written but never read. The new
`sector_health` step deliberately joins that second group and does NOT add
a `can_retry("sector_health")` check: doing so would make a single timeout
block every retry for `TIMEOUT_BLACKLIST_HOURS` (24h), reintroducing a
multi-day stall on top of a step that already has its own bounded
retry/backoff (`SECTOR_HEALTH_RETRY_BACKOFF_MINUTES` = 60,
`SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY` = 5, both inside
`_maybe_run_sector_health_snapshot`, unchanged by this PR).

### Abandoned workers cannot duplicate writes or overwrite newer state

`_run_with_timeout` abandons (does not kill) the worker thread on timeout,
so a timed-out sector-health attempt can still be running — and can still
write to the database — after the cycle has moved on. Two layers now guard
against that:

- **In-process, before `run_cycle` even starts a new attempt:**
  `_run_sector_and_intelligence_steps` bumps
  `state.sector_health_attempt_token` itself right when it reports a
  timeout, so an abandoned worker's own later attempt-token check inside
  `_maybe_run_sector_health_snapshot` sees a stale token even if no LATER
  cycle's attempt ever starts. (The pre-existing token check in
  `_maybe_run_sector_health_snapshot` already covered the case where a
  later attempt DOES start; this closes the other half.)
  `snapshot_all_sectors` also now accepts a `should_continue` callable,
  checked before each sector's compute and again immediately before each
  sector's upsert; `_maybe_run_sector_health_snapshot` passes
  `lambda: state.sector_health_attempt_token == attempt_token`, so an
  abandoned attempt stops issuing further DB writes as soon as it notices
  it has been superseded, instead of running the whole sector list to
  completion first. When this trips, the result carries `"aborted_stale":
  True` and `_maybe_run_sector_health_snapshot` returns without touching
  `state.last_sector_health*` or `results` at all.
- **At the database row level, for whatever write is already in flight
  when the in-process check above hasn't caught it yet:**
  `snapshot_all_sectors` now writes an explicit `computed_at` timestamp
  (default `datetime.now(timezone.utc)` at call start) as `as_of` on every
  row, and the upsert's `ON CONFLICT ... DO UPDATE` carries a `WHERE
  sector_health_snapshots.as_of IS NULL OR sector_health_snapshots.as_of <=
  EXCLUDED.as_of` guard. An older attempt's write against a
  (sector_name, snapshot_date) that a newer attempt already wrote is
  rejected by Postgres itself (`rowcount == 0`), counted as
  `snapshots_stale_skipped` — not a failure, not a duplicate write, just
  the guard doing its job.

### Tests

- `tests/test_hermes_sector_step.py` (new): starvation (sector step runs
  and returns quickly even when `run_intelligence_tasks` blocks past its
  patched budget, using the REAL `_run_with_timeout`), source-order +
  "no longer calls the helper" pins via `inspect.getsource`, the
  sector-health step's own timeout with an abandoned-worker check, the
  blacklist trace (blacklisted yet still retried after backoff, and
  `run_cycle`'s source does not check it), and the `snapshot_all_sectors`
  DB-row guard (`as_of` bound + WHERE clause present, `rowcount == 0` →
  `snapshots_stale_skipped` not a failure, `should_continue=False` before
  the first upsert writes nothing, and an end-to-end token-bump scenario
  where a stale in-flight attempt stops writing once a newer one starts).
- `tests/test_hermes_timeout_budgets.py`: added a pin that
  `SECTOR_HEALTH_TIMEOUT_SECONDS` is a positive int, actually used by the
  dispatch, and not derived from/tied to `INTELLIGENCE_TASKS_TIMEOUT_SECONDS`.
- `tests/test_hermes_sector_schedule.py`: unchanged in intent; its fakes
  were widened to accept the new `computed_at`/`should_continue` keyword
  arguments `_maybe_run_sector_health_snapshot` now passes through to
  `snapshot_all_sectors`, and the idempotency fixture's fake connection now
  returns an explicit `rowcount = 1` (matching what real Postgres returns
  there, since each call in that test uses a later `computed_at`).
- `tests/test_sector_health.py`: unchanged; still green against the
  additive `snapshot_all_sectors` signature (`computed_at`/`should_continue`
  are keyword-only with defaults).

Run: `python -m pytest tests/test_hermes_sector_schedule.py
tests/test_hermes_timeout_budgets.py tests/test_sector_health.py
tests/test_hermes_sector_step.py -q` — 48 passed.

### Review amendments (2026-09-19)

A release-controller review of this PR found three defects in the above;
all three are fixed on this branch.

**1. Equal-`as_of` handling.** The upsert guard's `WHERE` clause was
`sector_health_snapshots.as_of IS NULL OR sector_health_snapshots.as_of <=
EXCLUDED.as_of`. With `<=`, an attempt whose `computed_at` happened to
exactly EQUAL the already-stored row's `as_of` could overwrite it even
though it was not strictly newer — e.g. an abandoned worker resuming after
a newer attempt had already committed with the same timestamp. Changed to
strict `<`. **Tie rule: first committed wins on equal `as_of`** —
whichever write reaches Postgres first for a given `(sector_name,
snapshot_date)` keeps its row; a second write at the identical `as_of` is
rejected (`rowcount == 0`, counted as `snapshots_stale_skipped`, same as
any other stale write) rather than silently overwriting it.

**2. Check-to-write race on the state marker.** `_commit` (inside
`_maybe_run_sector_health_snapshot`) checks
`state.sector_health_attempt_token == attempt_token` and then, if it
matches, assigns `state.last_sector_health` /
`state.last_sector_health_outcome`. The attempt-start block does the
token bump (`state.sector_health_attempt_token += 1`) and sets the
attempt fields. Neither pairing was atomic: Python can switch threads
between `_commit`'s check and its assignments, so an abandoned worker's
`_commit` could pass the check, a newer attempt could then start AND
commit, and the old worker's assignments could still land last —
overwriting the newer attempt's marker even though the pre-existing DB
row guard (finding #1) had already correctly protected the actual
`sector_health_snapshots` rows. Fixed with a module-level
`threading.Lock`, `_SECTOR_HEALTH_STATE_LOCK` — deliberately NOT an
attribute on `OperatorState`, since `OperatorState` is serialised via
`to_dict()` (analytical-snapshot persistence) and a lock is not
picklable/JSON-able. The lock is held around (a) the attempt-start block
(token bump + attempt fields), (b) the whole of `_commit` (check +
writes), and (c) the timeout-path token bump in
`_run_sector_and_intelligence_steps`. This closes only the in-process
marker race; the DB row race for `sector_health_snapshots` itself remains
closed by the `as_of` `WHERE` guard, evaluated atomically on the locked
conflicting row by PostgreSQL itself. A test-only seam,
`_SECTOR_HEALTH_COMMIT_TEST_HOOK` (module-level, default no-op, called
inside `_commit` between the token check and the writes), lets a test
force a specific interleaving at that exact point without needing an
unreliable real-timing race —
`tests/test_sector_health_upsert_ordering_pg.py`'s
`TestStateMarkerLockClosesTheRace` uses it.

**3. Superseded outcome.** If a call returns `snapshots_written == 0`,
`upsert_failed == 0`, and `snapshots_stale_skipped > 0` (every row this
attempt tried already had an as-new-or-newer row from a different
attempt), the outcome is now classified as `"superseded"` — logged,
recorded as `state.last_sector_health_outcome = "superseded"`, and the
due period is marked done via the same `_commit` token-gated path as
`"success"`/`"no_eligible_sectors"` (so it is only actually marked done
if this attempt's token is still current; a newer in-process attempt
already owns its own marker either way). Previously this case fell
through to `"success"` (technically true — no failures — but misleading,
since nothing was actually written) alongside legitimate empty-day runs.
`state.last_sector_health_outcome`'s documented value set (in
`scripts/hermes_health.py`) and the outcome docstring in
`_maybe_run_sector_health_snapshot` both now include `"superseded"`.
`aborted_stale` results remain uncommitted, unchanged.

**New test file: `tests/test_sector_health_upsert_ordering_pg.py`.** Real
PostgreSQL, real threads, real transactions, against a per-test schema
(`shs_order_<pid>_<n>`) shaped exactly like
`migrations/0028_sector_health_snapshots.sql`, via the `pg_engine`
fixture (skips cleanly with no PostgreSQL reachable — this file is meant
to be executed by the coordinator against a disposable database via
`GRID_TEST_DB_URL`, not merged CI-green without ever running for real).
Covers: an old attempt paused mid-compute while a newer attempt commits
first (both the in-process `should_continue` short-circuit and, in a
separate variant with `should_continue=None`, the pure DB-level guard);
the equal-`as_of` tie rule in both thread orders; newer-`as_of`-always-wins
regardless of order; and the state-marker lock closing the check-to-write
race via the `_SECTOR_HEALTH_COMMIT_TEST_HOOK` seam. Pure/no-DB coverage
for the `"superseded"` classification and for the lock actually being
acquired (via a recording-lock stand-in) lives alongside the existing
scheduler tests in `tests/test_hermes_sector_schedule.py`
(`TestSupersededOutcome`, `TestStateMarkerLock`).

Run (no DB, PG file skips): `DB_PASSWORD=x PYTHONUTF8=1 python -m pytest
tests/test_hermes_sector_schedule.py tests/test_hermes_timeout_budgets.py
tests/test_sector_health.py tests/test_hermes_sector_step.py
tests/test_sector_health_upsert_ordering_pg.py -q` — 54 passed, 6 skipped.

Run (coordinator, disposable DB): `GRID_TEST_DB_URL=<disposable-postgres-url>
DB_PASSWORD=x PYTHONUTF8=1 python -m pytest
tests/test_sector_health_upsert_ordering_pg.py -q` — expected 6 passed.

### NOT fixed here

The daily intelligence block itself still cannot reliably complete inside
`INTELLIGENCE_TASKS_TIMEOUT_SECONDS` (900s) — hypothesis discovery
(`auto_discover()`), source audit, backtest scanning, postmortem, and the
other members of the `daily_due` block remain starved on cycles where the
30-minute active-hypothesis scorer plus whatever ran before it eats most of
the budget. This PR does not touch `INTELLIGENCE_TASKS_TIMEOUT_SECONDS`,
`run_intelligence_tasks`'s internal ordering, or the daily block's own
budget — sector-health is now independent of that problem, but the
problem itself is a separate, real, unaddressed issue and needs its own
follow-up (candidates: splitting `intelligence_tasks` the same way this PR
split sector-health out of it, or re-examining whether the daily block
needs its own sub-timeout the way `RESOLUTION_SCAN_BUDGET_SECONDS` bounds
the resolution step's cold-scan case).

---

**DO NOT MERGE — no production run, backfill, restart or scheduling
change is authorised by this PR.**
