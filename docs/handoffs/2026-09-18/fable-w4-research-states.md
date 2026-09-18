# GRID W4 slice 1 — autoresearch state-separation audit

Scope: read-only reading of `scripts/autoresearch.py` and `scripts/hermes_operator.py`
(worktree `GRID-fable-wt-hermes-research`, branch `fable/hermes-research-20260918`,
base `origin/main@3fe3f5ef`). Facts with `file:line` only; no proposals beyond one line
each.

## State pipeline: where each stage is produced/counted, or "not tracked"

| Stage | Produced/counted at | Notes |
|---|---|---|
| hypothesis generated | `scripts/autoresearch.py:622` (`ollama.chat(...)`) → `scripts/autoresearch.py:634` (`parse_hypothesis_json(response)`) | On `None`/unparseable response, recorded only as an in-memory `attempts` entry (`autoresearch.py:546`, `:552`), never persisted. |
| executable test | `scripts/autoresearch.py:650` `INSERT INTO hypothesis_registry ... state='TESTING'` | Row exists in `hypothesis_registry` with `state='TESTING'` before any backtest runs. |
| valid evaluation | `scripts/autoresearch.py:676` (`backtester.run_validation(...)`) → `scripts/autoresearch.py:698` (`result.get("overall_verdict", "FAIL")`) | The `validation_results` row itself is written inside `validation/backtest.py` (not read for this audit — out of the two-file scope); autoresearch.py only reads `overall_verdict` back from the returned dict. |
| accepted result | `scripts/autoresearch.py:715-719` (`UPDATE hypothesis_registry SET state=%s ...` → `'PASSED'` when `verdict == "PASS"`) | This is the only "acceptance" state written by autoresearch.py — a `hypothesis_registry.state` flip, not a separate acceptance table. |
| prediction published | **Not tracked** in either file. | Neither file writes to `decision_journal` or any live-inference/prediction table. `_create_model_from_hypothesis` (`autoresearch.py:430`) only inserts a `model_registry` row with `state='CANDIDATE'` (`autoresearch.py:462`, invoked from `autoresearch.py:752` on `verdict == "PASS"`, `autoresearch.py:745`). A CANDIDATE model is not a published prediction. |
| outcome scored | **Not tracked** in either file. | `hermes_operator.py` has scoring code for other subsystems (`milestone_scoring` at `hermes_operator.py:990-991`, contagion backtests at `hermes_operator.py:1224`/`:1248`) but none of it touches `hypothesis_registry`, `model_registry`, or `validation_results`. |
| calibration | **Not tracked** in either file. | No `calibrat*` reference in `scripts/hermes_operator.py`; `oracle_calibration_history` (schema.sql / `migrations/0043_oracle_calibration_history.sql`) is never referenced by either file. |
| advisory | **Not tracked** in either file. | No `advisory` reference in either file. |
| authorized promotion | **Not tracked** in either file. | No `governance`, `promote`, `SHADOW`, `STAGING`, or `PRODUCTION` reference anywhere in `scripts/hermes_operator.py` (checked by grep across the whole file). `model_registry.state` (`schema.sql:194-216`) has a `CANDIDATE → SHADOW → STAGING → PRODUCTION → FLAGGED → RETIRED` machine, but nothing in these two files drives a transition past the initial `CANDIDATE` insert at `autoresearch.py:462`. |

**Summary**: within `scripts/autoresearch.py` + `scripts/hermes_operator.py`, the tracked
pipeline runs exactly hypothesis → executable test → valid evaluation → accepted result
→ CANDIDATE model, and stops there. Everything from "prediction published" onward is
either handled by code outside these two files (unverified here, out of scope) or does
not exist yet.

## The counter that actually tracks "how much research happened"

`OperatorState.hypotheses_tested` is incremented at `scripts/hermes_fixers.py:1698`
(`state.hypotheses_tested += result.get("iterations", 0)`) from the dict
`run_autoresearch()` returns. Before this slice's fix, that function's returned dict only
had the key `"iterations_run"` (pre-fix `scripts/autoresearch.py`, see
`tests/test_autoresearch_failure_visibility.py`), so `result.get("iterations", 0)` was
always `0` — on a failed run **and** on a fully successful one. `hypotheses_tested`
staying at 0 therefore carried no information about failure; it was already broken by a
key-name mismatch, independent of any DB error. This slice's fix adds a matching
`"iterations"` key to that dict (`scripts/autoresearch.py`, end of `run_autoresearch`) so
the counter now reflects real activity. `scripts/hermes_fixers.py` was not edited (outside
this task's allowed file set).

## Hermes per-step timeout: cancel or abandon, and can an abandoned worker still write?

**Autoresearch specifically has no per-step timeout at all.** The cycle 6 gate at
`scripts/hermes_operator.py:1837` (`if state.cycle_count % 12 == 0 ...`) calls
`maybe_run_autoresearch(state, dry_run=dry_run)` at `scripts/hermes_operator.py:1840`
directly inside a plain `try/except` (`scripts/hermes_operator.py:1839-1843`) — unlike
`resolution`, `oracle_cycle`, `signal_classification`, etc., it is never passed through
`_run_with_timeout`. There is no `AUTORESEARCH_TIMEOUT_SECONDS` constant (checked by grep
for `AUTORESEARCH` across both files — only `AUTORESEARCH_MAX_ITER` at
`scripts/hermes_operator.py:82` / `scripts/hermes_fixers.py:38` exists, which bounds
*loop iterations*, not wall-clock time).

For the steps that *are* wrapped in `_run_with_timeout` (`scripts/hermes_operator.py:219`):
it **merely abandons** the worker, it does not cancel it.
- On timeout, `_run_with_timeout` calls `ex.shutdown(wait=False, cancel_futures=True)`
  (`scripts/hermes_operator.py:236`) rather than `wait=True` — the docstring at
  `scripts/hermes_operator.py:219-238` explains this is deliberate: `wait=True` would block
  the whole cycle until the orphaned thread finishes, which is the exact regression being
  guarded against ("This bug silently broke every stage timeout in Hermes for months").
  `cancel_futures=True` only cancels *queued, not-yet-started* futures; the one already
  running is unaffected — Python's `ThreadPoolExecutor`/`concurrent.futures` has no API to
  forcibly kill a running thread.
- Yes, an abandoned worker can still write. `scripts/hermes_operator.py:1499-1502`
  (inside `_run_resolution_step`'s docstring): "a run abandoned at the timeout also leaves
  its worker thread alive with an open transaction (see `_run_with_timeout`), and a
  long-lived snapshot blocks `CREATE/DROP INDEX CONCURRENTLY` database-wide." And
  `scripts/hermes_operator.py:1515-1517`: "A run abandoned on timeout keeps going as an
  orphan thread; that is safe here because every insert is `ON CONFLICT ... DO NOTHING`."
  That safety argument is specific to the resolution step's idempotent inserts — it is not
  a general guarantee, and does not apply to autoresearch (which is not run through
  `_run_with_timeout` in the first place, so the question is moot for it today, but would
  need its own idempotency argument if a timeout wrapper were ever added).
