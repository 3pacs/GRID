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

`python -m pytest tests/test_hermes_sector_schedule.py tests/test_hermes_timeout_budgets.py -q` — 18 passed.

## Other daily tasks: NOT touched in this PR

Only `scripts/hermes_operator.py` (the sector-health block + the two new
helper functions), `scripts/hermes_health.py` (the two new state fields),
`tests/test_hermes_sector_schedule.py`, and this handoff doc were changed.
No other daily task's scheduling or behaviour was modified.

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
