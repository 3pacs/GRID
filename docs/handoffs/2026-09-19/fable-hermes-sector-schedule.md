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

---

**DO NOT MERGE — no production run, backfill, restart or scheduling
change is authorised by this PR.**
