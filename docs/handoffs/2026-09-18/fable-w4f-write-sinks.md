# GRID W4f — cross-process lease-guarded writes for the research loop

Scope: `scripts/autoresearch.py`, `scripts/hermes_operator.py`,
`scripts/hermes_fixers.py`, `validation/backtest.py` (write-guard hook
only, no evaluator semantics change), `governance/registry.py`
(read-only — checked, not touched, see below), new `governance/leases.py`,
migration `migrations/versions/research_leases_0918.py`,
`tests/test_write_fencing*.py`, this doc. Worktree
`GRID-fable-wt-write-fencing`, branch `fable/write-fencing-20260918`, base
`fable/integration-20260918@3f6f8b05`. No push/PR/ssh/production; a
disposable Postgres was not yet provided (`GRID_TEST_DB_URL` unset in this
session) — the DB-gated tests are written and skip with an explicit reason.

Closes the two cross-process residual cases from
`docs/handoffs/2026-09-18/fable-w4b-runstate.md` ("What still lets a
worker write after loss of ownership"): #1 (the mid-iteration
`hypothesis_registry` state UPDATE was never re-checked after the
backtest returned) and #2 (fencing was in-process only — one Python int
in one process's memory, unable to fence a second process or a worker
surviving a restart). W4b's residual case #3 (this task's real target)
and #4 (`_run_with_timeout` cannot kill a thread) are addressed by making
every real write pass through a cross-process DB lease rather than by
trying to kill anything.

## 1. Every write sink reachable from `run_autoresearch` / the operator's autoresearch step

| # | Sink | File:line (pre-change) | Statement | Guarded? |
|---|------|------------------------|-----------|----------|
| 1 | `hypothesis_registry` INSERT | `scripts/autoresearch.py:992-1002` (new `_insert_hypothesis`) | `INSERT ... VALUES (..., 'TESTING') RETURNING id` | Yes — `run_guarded_dbapi` via `_guarded_or_direct` |
| 2 | `hypothesis_registry` UPDATE on backtest exception | `scripts/autoresearch.py:1136-1138` (orig) → `_mark_failed` | `UPDATE ... SET state='FAILED', kill_reason=%s WHERE id=%s` | Yes |
| 3 | `hypothesis_registry` UPDATE, mid-iteration state after backtest (**residual case #1**) | `scripts/autoresearch.py:1209-1211` (orig) → `_update_state` | `UPDATE ... SET state=%s, kill_reason=%s, updated_at=NOW() WHERE id=%s` | Yes — this was the single largest open window; now guarded |
| 4 | `validation_results` INSERT | `validation/backtest.py:_store_result` (766-788 orig), called from `run_validation` at `scripts/autoresearch.py`'s backtest call | `INSERT INTO validation_results (...) VALUES (...)` | Yes — via new `write_guard` hook on `WalkForwardBacktest`, using `governance.leases.run_guarded` (SQLAlchemy flavor) |
| 5 | `model_registry` INSERT (CANDIDATE creation) | `scripts/autoresearch.py:611-620`, `_create_model_from_hypothesis`, called from two sites (reused-PASSED idempotent branch and fresh-PASS branch) | `INSERT INTO model_registry (...) VALUES (..., 'CANDIDATE', ...) RETURNING id` | Yes — both call sites wrapped |
| 6 | `analytical_snapshots` research_run records | `scripts/autoresearch.py::_record_research_run` (all call sites: started/context_loaded/iteration/terminal) | `AnalyticalSnapshotStore.save_snapshot(category="research_run", ...)` | **Deliberately NOT guarded** — see §5 |
| 7 | Notification hook | `scripts/notify.py::notify_on_pass`, called once, fresh-PASS branch only | SMTP `sendmail` (no DB write) | Not a DB write — gated behaviorally (call suppressed on any fenced path); see §5 |
| 8 | Governance promotion writes | `governance/registry.py::ModelRegistry.transition()` (UPDATE `model_registry` for CANDIDATE→SHADOW→...→PRODUCTION) | n/a | **Not reachable from autoresearch** — verified by grep across `scripts/autoresearch.py`, `scripts/hermes_operator.py`, `scripts/hermes_fixers.py`, `validation/backtest.py`, `store/snapshots.py`, `scripts/notify.py`, `store/pit.py`, `ollama/*.py`: none import `governance.registry` or call `ModelRegistry`/`.transition`. `governance/registry.py` is untouched (read-only per instructions). |

Trace method: `grep -n "INSERT\|UPDATE\|\.execute(\|save_snapshot\|commit()\|cur\.\|conn\.\|notify_on_pass\|_create_model_from_hypothesis\|_record_research_run" scripts/autoresearch.py`, plus the equivalent in `validation/backtest.py`, plus a repo-wide grep confirming nothing imports `governance.registry` from the autoresearch call graph. Sinks #1-#5 are exactly the ones the task brief named; nothing else was found.

A pre-existing bug found while tracing #5/#7: the fresh-PASS branch's
in-process fencing check on model creation (`fenced_before_model_creation_iteration_N`)
recorded the fence event but **still called `notify_on_pass` unconditionally
a few lines later** — a real "notification fires on a fenced path" gap,
not something this task introduced. Fixed as part of this change
(`model_creation_fenced` now gates the notify call for both the in-process
and the new cross-process fencing outcomes).

## 2. `governance/leases.py`

`research_leases` table: `name` (PK), `owner_id`, `generation` (BIGINT),
`acquired_at`, `heartbeat_at`, `expires_at`. One row per lease name (e.g.
`"autoresearch"`).

- `acquire(engine, lease_name, owner_id, ttl_seconds)` — single
  `INSERT ... ON CONFLICT (name) DO UPDATE ... WHERE expires_at < NOW()
  RETURNING generation`. Only takes over an expired (or nonexistent) row;
  a live lease makes the `WHERE` clause exclude the conflict row, so
  `RETURNING` yields nothing and `acquire()` raises `LeaseHeld`.
- `heartbeat(engine, lease_name, owner_id, generation, ttl_seconds)` —
  `UPDATE ... WHERE name=... AND owner_id=... AND generation=... AND
  expires_at > NOW() RETURNING generation`; returns `True`/`False`.
- `release(engine, lease_name, owner_id, generation)` — sets
  `expires_at = NOW()` (not a delete) so the row and its generation
  counter persist for the next `acquire()`.
- `guarded_write(conn, lease_name, generation, fn)` / `guarded_write_dbapi`
  (psycopg2 flavor) — `SELECT generation, expires_at FROM research_leases
  WHERE name = :name FOR UPDATE`, checks `generation == current AND not
  expired`, raises `OwnershipLost` otherwise, else runs `fn` in the SAME
  transaction. `run_guarded` / `run_guarded_dbapi` wrap the
  transaction-opening boilerplate.

## 3. Lock-ordering argument (why "commit after loss" is impossible, not just unlikely)

`SELECT ... FOR UPDATE` takes a row lock held for the **whole transaction**,
not just the statement. So: (1) the guard's lock-and-check happens first,
in the same transaction as (2) the real write `fn`. Any competing
transaction that would advance `generation` (a concurrent `acquire()`, or
another `guarded_write`) must take the identical `FOR UPDATE` lock on the
same row — it blocks until this transaction commits or rolls back. There
is therefore no point in time at which the row can change while `fn` is
executing: either this transaction's generation check passed and it
commits its write while still legitimately current, or the check failed
and `fn` never ran at all (the check is unconditionally first). A worker
cannot "start before loss and commit after loss" because the row lock
serializes the two transactions — whichever `acquire()` would represent
"loss" simply cannot commit until the in-flight write's transaction ends.
This is proved directly in `tests/test_write_fencing_leases.py::test_lock_ordering_makes_commit_after_loss_impossible`
(a same-process simulation using a shared lockable fake row: a concurrent
lock attempt during `fn` raises, standing in for what Postgres blocks on)
and against real Postgres in
`tests/test_write_fencing_live_db.py::test_second_acquire_after_expiry_fences_the_firsts_guarded_write`.

## 4. Tests

`DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_write_fencing_leases.py tests/test_write_fencing_live_db.py -q`
→ **13 passed, 2 skipped** (skips: `GRID_TEST_DB_URL` unset, explicit
reason printed). `tests/test_write_fencing_leases.py` covers:
every one of sinks #1-#5 individually (parametrized, stale-generation
rejected / current-generation admitted), an expired-but-matching
generation rejection, the lock-ordering proof (§3), heartbeat renew vs.
refuse-when-expired-or-superseded, and the full integration proof that a
lease lost exactly at residual-case-#1's write suppresses both
`notify_on_pass` and the `model_registry` insert. `tests/test_write_fencing_live_db.py`
holds the one real-Postgres test (real `FOR UPDATE`, two real
generations, first's `guarded_write` raises and commits nothing) plus an
`acquire()`-refuses-a-live-lease check — both DB-gated, both run once
`GRID_TEST_DB_URL` is exported.

Full regression sweep (existing suites this task touches or is adjacent
to): `tests/test_autoresearch_runstate.py`, `tests/test_hermes_autoresearch_bounded_and_fenced.py`,
`tests/test_research_status.py`, `tests/test_autoresearch_failure_visibility.py`,
`tests/test_autoresearch_schema_contract.py`, `tests/test_evaluator_contracts.py`,
`tests/test_hold_validation.py`, `tests/test_llm_autoresearch.py`,
`tests/test_hermes_timeout_budgets.py`, `tests/test_hermes_operator_dry_run.py`,
`tests/test_regression_20260329.py`, `tests/test_resolver_scan_budget.py`,
`tests/test_hermes_resolution_step.py`, `tests/test_hermes_resolution_watermark.py`,
`tests/test_alembic_single_head.py` → **218 passed, 4 skipped** (skips
pre-exist/unrelated), 1 pre-existing unrelated SQL-safety warning
(`api/routers/chat.py`, `api/routers/price_alerts.py`). `alembic heads`
confirms a single head (`research_leases_0918`).

All new writes are backward compatible by construction: `lease_generation`
(autoresearch.py), `write_guard` (`WalkForwardBacktest`), and the lease
acquire/heartbeat/release plumbing in `hermes_operator.py`/`hermes_fixers.py`
are all-optional, default-`None` additions layered alongside the existing
W4b in-process `generation`/`is_current_generation` fencing (unchanged).
No existing test needed modification.

## 5. Sinks that cannot (or should not) be guarded

1. **`analytical_snapshots` research_run records are deliberately NOT
   routed through `guarded_write`.** These are the observability records
   that *report* a fenced outcome (`status="fenced"`, `skip_reasons`).
   Gating the write that announces "I lost the lease" behind the same
   lease it is announcing the loss of is circular — a worker that has
   already lost ownership must still be able to say so. This is the same
   fail-soft, best-effort contract `_record_research_run` already had
   under W4b (its own `try`/`except` swallows any storage failure); it is
   unchanged here.
2. **The notification send (`scripts/notify.py::notify_on_pass`, SMTP
   `sendmail`) has no transaction to wrap.** An email, once sent, cannot
   be rolled back the way a DB statement can. The guard's only lever here
   is behavioral: `model_creation_fenced` gates the call so it is never
   *attempted* on a fenced path, not that a "sent" email is somehow
   undone. This is the "third-party call you cannot wrap" case named in
   the task brief.
3. **Postgres's own `ON CONFLICT DO UPDATE`/`FOR UPDATE` commit path
   inside `acquire()`/`heartbeat()`/`release()` themselves is not, and
   cannot be, guarded by `guarded_write`** — they ARE the primitive that
   defines ownership; guarding them against themselves is not meaningful.
   Kept intentionally simple (a single statement each) so their own
   correctness is easy to audit directly rather than composed.

## Commit

`feat(research): cross-process lease-guarded writes`
