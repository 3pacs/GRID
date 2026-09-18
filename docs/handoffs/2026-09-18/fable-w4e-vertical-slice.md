# GRID W4e — isolated research vertical slice

Scope: `tests/integration/test_research_vertical_slice.py` (new),
`tests/fixtures/research_slice/feature_window.json` (new), plus the
cherry-picked `evaluation/signal_outcomes.py` (draft #562, commit
`5889f5be`, `sig-eval-1`). Worktree `GRID-fable-wt-research-slice`, branch
`fable/research-slice-20260918`, base `549d7fb0` (draft #566, which already
includes #552's structured autoresearch results, run-state records,
per-step timeout, and generation fencing). No push/PR, no DB/ssh/.env, no
local Postgres used by default (one test is Postgres-gated and skips
cleanly), no production, no live LLM, no activation of anything.

## Commits

1. `790ee0f2` — `git cherry-pick -x 5889f5be` (draft #562:
   `evaluation/signal_outcomes.py` + its migration + its own test file,
   clean, no conflicts).
2. `<this commit>` — `test(research): isolated end-to-end research
   vertical slice with honest outcomes and fencing`.

`b56ad1d4` was **not** cherry-picked, per the task brief (stacks on #556, a
different base). `validation/backtest.py` is used exactly as it exists on
this branch — it carries no `VALIDATION_VERSION`-style constant to cite;
the version to reference for this slice is this branch's own HEAD sha.

## What the slice proves

`tests/integration/test_research_vertical_slice.py` drives
`scripts/autoresearch.py::run_autoresearch()` end to end, in one process,
against three kinds of fakes: a pure-Python `FakePITStore` (real PIT
vintage-resolution semantics, no database), a fake psycopg2
cursor/connection (`_FakeAutoresearchDB`), and a deterministic fake Ollama
client. The real, unmodified `validation/backtest.py::WalkForwardBacktest`
and `evaluation/signal_outcomes.py` run underneath every assertion — nothing
about their behavior is reimplemented or mocked away.

Asserted behaviours (file, one test class per bullet unless noted):

| # | Behaviour | Result |
|---|---|---|
| 1 | Real-shaped feature set (2 `feature_registry` rows + `resolved_series` rows with a mid-window revision) loaded into a pure-Python fake PIT store | PASS |
| 1b | Same fixture loaded into a real Postgres via `PITStore` when reachable | PASS (SKIPPED locally — see below) |
| 2a | Well-formed hypothesis JSON parses and drives the loop | PASS |
| 2b | Malformed hypothesis JSON is recorded as `attempts[i]["error"] == "JSON parse failed"`, no crash | PASS |
| 3a | research_run record: `started` → `running`(context_loaded) → `running`(iteration) → `ok` | PASS |
| 3b | Iterations counted correctly; hypothesis inserted once; retry with same `run_id` does not duplicate it | PASS |
| 3c | Validation result comes from the real `validation/backtest.py`, fed through honest PIT-filtered data | PASS (see structural finding #1 below for what this could *not* show) |
| 4a | Empty feature window ⇒ explicit `INSUFFICIENT_DATA` era + `FAIL` verdict, never a fabricated Sharpe | PASS |
| 4b | `evaluation/signal_outcomes.py` cohort summary reports `INELIGIBLE`(`missing_entry_price`) for a no-price signal, with correct denominators | PASS |
| 4c | SQL failure during context load ⇒ top-level `status="failed"`, `phase`, `error_category`, `iterations=0` | PASS |
| 4d | SQL failure mid-loop (hypothesis INSERT) does **not** flip top-level status — documented boundary, see finding #2 | PASS |
| 5 | Generation advanced mid-run ⇒ no write after the fence point, run ends `"abandoned"`, fence reason recorded | PASS |
| 6 | Notification hook: exactly one call on a genuine PASS, zero on FAIL, zero on retry-duplicate | PASS |
| 7a | Provider identity (`get_client()`) follows effective config (`settings.OLLAMA_*`) when OpenAI/llama.cpp are unavailable | PASS |
| 7b | Disabled/unavailable Ollama ⇒ visible `status="failed"`, `phase="ollama_availability"` — not a silent skip | PASS |

Local run: **12 passed, 1 skipped** (`DB_PASSWORD=testpass PYTHONUTF8=1
python -m pytest tests/integration/test_research_vertical_slice.py -v`).
Run together with the existing autoresearch/evaluator test files
(`test_autoresearch_runstate.py`, `test_autoresearch_failure_visibility.py`,
`test_autoresearch_schema_contract.py`, `test_signal_outcomes.py`): **46
passed, 1 skipped**, no cross-test interference (module globals
`autoresearch._ortho_cache` and `ollama.client._client_instance` are reset
per test).

## What this does NOT prove

- **No real LLM.** Every Ollama call in this file is a deterministic fake
  (`_DeterministicOllama`). Nothing here validates prompt quality, model
  behavior, or real Ollama/llama.cpp/OpenAI wire protocols.
- **No real prices.** `FakePITStore` is pure Python; the one test that
  touches a real database (`TestRealPostgresPITBoundary`) only proves
  `store/pit.py`'s vintage-resolution SQL against a real Postgres — it does
  not touch price data, ticker resolution, or `alpha_research/realized_alpha.py`.
- **No scheduler.** `hermes_operator.py`/`hermes_fixers.py` are not touched
  or exercised — generation fencing is tested by calling
  `run_autoresearch(generation=..., is_current_generation=...)` directly,
  the same seam `hermes_operator.py` uses, not by running the operator.
- **No activation.** Nothing here changes the cycle-6 gate, any systemd
  unit, or any schedule. This is a test file plus one cherry-picked module.

## Two exact findings the tests encode (not just assert around)

1. **`WalkForwardBacktest.run_validation`'s `predict_fn` parameter is dead
   code.** `_compute_era_metrics(matrix, predict_fn, cost_bps)` never reads
   `predict_fn` — the "strategy" return series is always
   `matrix.iloc[:, 0].pct_change()`, the *exact same* series
   `_compute_baseline_metrics` uses for "baseline". The only difference
   between them is a constant `cost_bps` drag subtracted from every day of
   the strategy series. A constant per-day drag cannot raise a Sharpe ratio
   (it lowers cumulative return while leaving volatility unchanged), so
   `_determine_verdict`'s `full_metrics.sharpe <= baseline.sharpe → FAIL`
   gate is **always** true, for any feature matrix, at any `cost_bps >= 0`.
   Verified directly in `TestTimeCorrectEvaluation::
   test_real_walk_forward_backtest_never_returns_pass_on_this_branch`:
   `cost_bps=0.0` gives `sharpe == baseline_sharpe` (still FAILs on `<=`);
   `cost_bps=10.0` (this branch's real default) gives `sharpe <
   baseline_sharpe`. **Consequence for this task's item 3**: "a cheating
   hypothesis whose predictor uses the future must NOT pass" is true here
   only because *nothing* passes through the real, unmodified function —
   cheating or honest. The task's implied contrast case (an honest
   hypothesis passing while a cheating one fails) is not constructible
   against `validation/backtest.py` as it exists on this branch. What
   *is* constructible, and what `TestTimeCorrectEvaluation::
   test_honest_pit_store_excludes_future_revision_leaky_one_does_not`
   shows instead, is the real lookahead-safety property one level down:
   `FakePITStore` (mirroring `store/pit.py`'s actual `DISTINCT ON`
   query) never lets a revision released after `as_of_date` leak into the
   matrix, while a deliberately-broken "leaky" variant does — proven by
   direct comparison of the matrix values, not by verdict.
2. **A SQL failure mid-loop does not flip the top-level run status.** Only
   a failure while loading the research context (`_load_research_context`
   — `feature_list`/`feature_name_map`/`market_snapshot`) produces the
   structured `{"status": "failed", "phase": ..., "error_category": ...}`
   result. A SQL failure during the hypothesis_registry INSERT, the
   backtest call, or the model_registry INSERT is caught locally inside the
   `for iteration in ...` loop and recorded only as
   `attempts[i]["error"]` — the run still ends `status="ok"` (or
   `"abandoned"` if separately fenced). `TestSQLFailureVisibility` has one
   test for each half of this boundary side by side. This means the task
   brief's item 4 wording ("a case where the SQL fails ... yields status
   failed") holds only for the context-load phase, not uniformly for any
   SQL failure in the loop — documented exactly rather than glossed over.

A smaller, related note: `run_autoresearch` has no per-attempt
`skip_reasons` concept for a parse failure — that field is populated only
by fence events. A malformed-LLM-output case is visible solely via
`all_attempts[i]["error"]`; the terminal run record's own `skip_reasons`
list stays empty even though zero usable hypotheses were produced that
run. Asserted explicitly in `TestMalformedLLMOutput`.

## What skips locally, and why

`TestRealPostgresPITBoundary::test_pit_correct_vintage_filtering_against_real_postgres`
is the only DB-gated test. It uses the shared `pg_engine` fixture
(`tests/conftest.py`) exactly like every other DB-gated test in this suite
— on collection it attempts a real connection and calls `pytest.skip
("PostgreSQL not available")` on any failure, never fabricating a result.
Locally (no `GRID_TEST_DB_URL`, no local Postgres) it skips with that exact
reason. If pointed at a live, disposable Postgres via `GRID_TEST_DB_URL`
but the schema hasn't been applied yet, it skips instead with an explicit,
different reason naming the missing tables — it does not assume `schema.sql`
was already run the way the older `tests/test_pit.py` fixture does.
Per `tests/conftest.py`'s own note, CI's `test.yml` currently sets `DB_URL`
(which `Settings.DB_URL` cannot read — it's a computed `@property`) rather
than `GRID_TEST_DB_URL`, and never applies `schema.sql` to the ephemeral
container either, so this test — like the ~91 other `pg_engine`-gated
tests already in the suite — does not actually run in CI today. That
wiring gap is out of scope for this task; this test is written to activate
correctly the moment it's closed, without any change to this file.

## CI command

```bash
DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/integration/test_research_vertical_slice.py -v
```

To also exercise the Postgres-gated test, point `GRID_TEST_DB_URL` at a
disposable database with `schema.sql` already applied:

```bash
GRID_TEST_DB_URL=postgresql://grid:testpass@localhost:5432/griddb_test \
DB_PASSWORD=testpass PYTHONUTF8=1 \
python -m pytest tests/integration/test_research_vertical_slice.py -v
```
