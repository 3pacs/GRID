# GRID W4b — research-loop run-state, bounded work, generation fencing

Scope: `scripts/hermes_operator.py`, `scripts/hermes_fixers.py`,
`scripts/autoresearch.py`, new tests, this doc. Worktree
`GRID-fable-wt-hermes-runstate`, branch `fable/hermes-runstate-20260918`, base
`origin/main@a2f71acf`. No DB/ssh/.env/production touched, no local Postgres
used (all tests use fakes), no scheduler/systemd change, no gate widened, no
real notification send (all notify calls in tests are recorder stubs).
**Nothing here activates autoresearch** — the cycle-6 gate (every 12 cycles)
is unchanged; this task only makes the loop observable and safe to restart.

## Commits

1. `2b33bbc3` — cherry-picked `05ce5a9847c7d81404aac56036e8f069f4bf7ac8` (`-x`).
   This is the commit that actually changes `scripts/autoresearch.py` to
   return `status`/`iterations`/`phase`/`error` (the task brief named
   `047eb0e7` for this; that SHA is `05ce5a98`'s docs-only child — see
   below). Verified linear ancestry: `047eb0e7` → `05ce5a98` → `a2f71acf`
   (this worktree's base), so both cherry-picked cleanly with no conflicts.
2. `01068056` — cherry-picked `047eb0e740ee93bdb81cc9ed5686496805d5ea32` (`-x`),
   the docs-only follow-up naming the activation gate.
3. New commit (this task) — `feat(hermes): persisted research run state,
   bounded autoresearch, generation fencing`.

## What changed, by file

### `scripts/autoresearch.py`
- Applied the hermes_fixers one-line patch's *intent* directly at its real
  home: `docs/handoffs/2026-09-18/fable-w4-hermes-operator.patch` targeted
  `scripts/hermes_fixers.py` (in scope for this task, unlike the prior
  slice), so `maybe_run_autoresearch` now logs `log.error` on
  `result["status"] == "failed"` (hermes_fixers.py, see below) instead of
  living only as an unapplied patch file.
- `run_autoresearch()` gained `run_id`, `generation`, `is_current_generation`
  parameters (signature at line 631) and now writes a `research_run`
  snapshot (`category="research_run"`, `subcategory="autoresearch"`, via
  the existing `AnalyticalSnapshotStore` — no new table) at every stage:
  `_record_research_run` (line 224) is the single write helper, called at
  `"started"`/`"init"` right after `run_id` is minted, on every early-exit
  failure (`ollama_availability`, the now-wrapped `db_connect`, and each
  `AutoresearchDataError` phase), once more as `"running"`/`"context_loaded"`
  with the inputs manifest once feature data is loaded, once per iteration
  as `"running"`/`"iteration"`, and finally as `"ok"`/`"failed"`/`"abandoned"`
  at the end.
- `_get_code_sha()` (line 202): best-effort `git rev-parse HEAD`, `None` on
  any failure (no git binary, not a checkout, etc.).
- `_generation_current()` (line 290): the fencing predicate. `None`
  generation/callable ⇒ always current (a standalone CLI run has no
  operator to fence against).
- **Fencing checkpoints** (all call `_generation_current`): top of every
  iteration before Step 1 (line ~797 in the loop body), immediately before
  the walk-forward backtest call (guards the `validation_results` write
  that happens *inside* `validation/backtest.py`, which is read-only for
  this task — fenced at the call boundary since I cannot instrument that
  module directly), immediately before `_create_model_from_hypothesis` in
  the fresh-PASS branch, and again as part of the final end-record's
  status decision (`ok` downgrades to `"abandoned"` if any fence event
  fired or the generation went stale between the last check and here).
- **Idempotent retry** (line ~899 onward): before inserting into
  `hypothesis_registry`, `SELECT id, state ... WHERE statement = %s AND
  layer = %s` — a hit reuses the row instead of inserting a duplicate.
  If the reused row is already `PASSED`, the backtest and
  `notify_on_pass` are skipped entirely (notify is only ever called from
  the one branch that reaches a **fresh** `PASS`, at line ~1138); a
  `model_registry` row is created only if one doesn't already exist for
  that `hypothesis_id` (checked before every `_create_model_from_hypothesis`
  call, both in the reused-PASSED short-circuit and the fresh-PASS branch).
  If the reused row is already `FAILED`, the iteration skips straight to
  refining rather than re-running the backtest.
- `AUTORESEARCH_TIMEOUT_SECONDS` (line 56): `GRID_AUTORESEARCH_TIMEOUT_SECONDS`
  env var, default 1800s. Duplicated (deliberately, same reasoning as
  `AUTORESEARCH_MAX_ITER`'s existing duplication in `hermes_fixers.py`) as
  `scripts/hermes_operator.py:96`, since `scripts/autoresearch.py` is only
  ever imported lazily to avoid a circular import.

### `scripts/hermes_fixers.py`
- `maybe_run_autoresearch()` gained `run_id`, `generation`,
  `is_current_generation` params, forwarded unchanged into
  `run_autoresearch()`. Added the one-line `log.error(...)` on
  `result.get("status") == "failed"`, the exact change
  `fable-w4-hermes-operator.patch` specified (that patch targeted this
  file, not `hermes_operator.py` — its own header says so).

### `scripts/hermes_operator.py`
- `_AutoresearchGenerationTracker` (line 281) + module singleton
  `_autoresearch_generation` (line 330): in-process generation counter.
  `.next()` bumps and returns; `.is_current(g)` checks against the latest.
- Cycle-6 gate (the `state.cycle_count % 12 == 0` block) now wraps
  `maybe_run_autoresearch` in `_run_with_timeout` — previously called it
  directly inside a plain `try/except` with **no timeout at all** (the
  exact gap `docs/handoffs/2026-09-18/fable-w4-research-states.md`'s
  "Activation condition" section names). Each invocation mints a
  `run_id = uuid4()` and `generation = _autoresearch_generation.next()`
  before the call. On `ok=False` (timeout), the generation is bumped
  *immediately* (line ~1920 region) — not on the next gate firing an hour
  later — and the operator itself writes the `"timeout"` run-record via
  `from scripts.autoresearch import _record_research_run` (the worker
  thread cannot be trusted to do this: it's still running).

### `scripts/research_status.py` (new)
- `latest_research_run(engine)`: reads the newest
  `analytical_snapshots` row for `category="research_run"`,
  `subcategory="autoresearch"`, merges its `payload` to the top level.
  Fail-soft (`None` on any DB error). Deliberately outside `api/` — W4c
  wires this into an endpoint; this module has no FastAPI/route
  dependency so a status read doesn't need to import psycopg2/Ollama/the
  backtester the way `scripts/autoresearch.py` does.

## Record schema (`analytical_snapshots.payload`, category=`research_run`, subcategory=`autoresearch`)

```
run_id            str   (uuid4, or the caller-supplied retry key)
status            "started" | "running" | "ok" | "failed" | "timeout" | "abandoned"
phase             str | None   (e.g. "init", "context_loaded", "iteration",
                                 "ollama_availability", "db_connect",
                                 "feature_list"/"feature_name_map"/"market_snapshot",
                                 "complete", "operator_timeout")
error             str | None
error_category    str | None   ("ollama_unavailable" | "db_connect_failure" |
                                 "db_load_failure" | "timeout" | None)
iteration         int | None   (current iteration on a "running" checkpoint)
iterations        int | None   (final count on the terminal record)
skip_reasons      list[str]    (e.g. ["fenced_before_iteration_2"])
failure_reasons   list[str]    (reserved; unused by any current write site)
duration_s        float | None (monotonic elapsed seconds since function entry)
generation        int | None   (operator-assigned; None outside the operator)
code_sha          str | None   (git rev-parse HEAD at call time)
inputs            dict | None  ({"feature_ids_count", "market_snapshot_keys",
                                  "evaluation_version": None — no such
                                  concept exists anywhere in this codebase
                                  today, checked validation/backtest.py and
                                  config.py})
```

## Tests

Before this task: `tests/test_autoresearch_failure_visibility.py` (4) and
`tests/test_autoresearch_schema_contract.py` (5) — both cherry-picked in,
both still pass unmodified (verified — the new `_record_research_run` calls
are best-effort and swallow errors against the `object()`-as-engine fakes
those tests already use).

After, all fakes/no DB (`DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest
tests/test_autoresearch_runstate.py tests/test_hermes_autoresearch_bounded_and_fenced.py
tests/test_research_status.py tests/test_autoresearch_failure_visibility.py
tests/test_autoresearch_schema_contract.py -q` → **24 passed**):

- `tests/test_autoresearch_runstate.py` (5): started→running(context_loaded)
  →running(iteration)→ok sequence with exact payloads on a success run;
  started→failed sequence with zeroed iterations on a DB-load failure; a
  stale-generation worker is fenced before any DB write (asserted via an
  `INSERT`-tracking fake cursor) and the reason lands in both the returned
  attempt and the end record's `skip_reasons`; a current generation is
  *not* falsely fenced; a retry with the same `run_id` against a shared
  in-memory fake DB does not duplicate the `hypothesis_registry` insert,
  the `model_registry` insert, or the `notify_on_pass` call (a recorder
  stands in — no real email).
- `tests/test_hermes_autoresearch_bounded_and_fenced.py` (7):
  `_AutoresearchGenerationTracker` unit behavior; an end-to-end orphan-write
  test using the real tracker + real `run_autoresearch` fencing (simulates
  the timeout by calling `.next()` the way the operator's timeout branch
  does, then proves the orphan reaches context-loading reads but never a
  write); the operator's exact `_record_research_run(..., "timeout", ...)`
  call shape; `hypotheses_tested` `+= 0` on a real failure never zeroes an
  already-accumulated counter, `+= iterations` on success does advance it;
  the `log.error` on `status == "failed"` fires with phase+error in the
  message; `run_id`/`generation`/`is_current_generation` are forwarded
  verbatim through `maybe_run_autoresearch`.
- `tests/test_research_status.py` (3): `None` on no rows, correct
  category/subcategory filter + payload merge on a hit, fail-soft on an
  engine error.

Also ran targeted regression checks on adjacent modules (no changes
expected or found): `test_llm_autoresearch.py`, `test_hermes_timeout_budgets.py`,
`test_hermes_operator_dry_run.py`, `test_regression_20260329.py`,
`test_resolver_scan_budget.py`, `test_hermes_resolution_step.py`,
`test_hermes_resolution_watermark.py` — **120 passed, 2 skipped** (skips
pre-exist, unrelated; one pre-existing SQL-safety warning in
`api/routers/chat.py` / `api/routers/price_alerts.py`, also unrelated).

## What still lets a worker write after loss of ownership (exact)

Fencing here is **in-process, per-thread, best-effort against a plain int
under the GIL** — not a lock, not a DB lease. Concretely, still open:

1. **The mid-iteration state UPDATE is not re-checked.** The fencing check
   before the backtest call (`scripts/autoresearch.py`, the block right
   before `log.info("[3/4] Running walk-forward backtest...")`) is the
   *last* check before `cur.execute("UPDATE hypothesis_registry SET
   state=%s, kill_reason=%s, updated_at=NOW() WHERE id=%s", ...)` at line
   1078. If the operator bumps the generation *while the backtest itself
   is running* (its own duration is unbounded from this module's
   perspective — `validation/backtest.py` is out of scope), the orphan
   will still execute that `UPDATE` and the `attempts.append(...)`/
   `best_result` bookkeeping after it, because there is no fencing check
   between "backtest returned" and "state written". This is the single
   largest remaining window.
2. **Cross-process fencing does not exist.** `_AutoresearchGenerationTracker`
   is one Python object in one Hermes process's memory
   (`scripts/hermes_operator.py:330`). A second Hermes process, or the
   same process after a restart, starts its own tracker at generation 0
   and has no way to know about — let alone fence — a worker thread left
   running in a process that no longer exists conceptually to it. Actually
   killing that process's thread is impossible from outside it; the
   documented fix (both in this doc and inline at
   `_AutoresearchGenerationTracker`'s docstring) is a DB-backed lease: a
   row with an owner/epoch every writer re-checks transactionally
   (`SELECT ... FOR UPDATE` or an optimistic version column) — not
   implemented.
3. **`validation/backtest.py`'s own `validation_results` insert is
   unfenced from the inside.** This task fences the *call* to
   `run_validation()`, not the write it performs internally — that module
   is read-only for this task. A worker that passes the pre-backtest fence
   check and then goes stale mid-backtest will still complete that insert;
   nothing downstream rejects it.
4. **`ex.shutdown(wait=False, cancel_futures=True)` still does not cancel
   a running thread** (`scripts/hermes_operator.py`'s `_run_with_timeout`,
   unchanged by this task, docstring at line 219). Fencing is the
   compensating control for *this task's own instrumented writes*
   (hypothesis insert, model insert, the run-record's own writes); it is
   not a general guarantee for anything else a runaway worker might touch.

Given (1)-(3), autoresearch remains **not activated** by this task (no
scheduler/gate change, as instructed) and should not be scheduled more
tightly until at minimum (1) is closed and a real decision is made about
(2)/(3) — this doc's job was observability and safe-restart, not closing
every write race.
