# GRID Fable — Database-backed proofs + real-API acceptance

Tree: `C:/Users/owner/dev/GRID-fable-wt-integration` at `6e8df878a2f3893deb01ea6ec99a38edeb534440` (verified via `git rev-parse HEAD`, matched exactly).

DB: disposable PostgreSQL through SSH tunnel `127.0.0.1:55432`, database name `griddb_fable_20260918_1740` (name only). Verified `current_database()` starts with `griddb_fable_` before any write. PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg24.04+2).

Date: 2026-09-18. No commits/pushes/rebases/stashes/edits made in the integration worktree. No other database touched. No ssh, no production.

## Step 1 — Migration application

Commands (env: `DB_HOST/PORT/NAME/USER/PASSWORD` + `ENVIRONMENT=development`):

1. `python -c "import db; db.apply_schema()"` — applied `schema.sql` (107,698 bytes) to the empty DB. OK.
2. `python -m alembic stamp 7e4dfecce247`
3. `python -m alembic upgrade head` — **first attempt failed**, see "Finding" below.
4. Retried after a repo-code-based (not source-edited) unblock — see below — and **succeeded**, landing at `research_leases_0918 (head)`.
5. `alembic current` → `research_leases_0918 (head)`. `alembic heads` → `research_leases_0918 (head)` — single head, as expected.
6. `alembic downgrade signal_evaluations_0918` → `alembic upgrade head` — succeeded both directions. Verified `cftc_positioning_daily` and `godview_generations` (the god-view tables) existed with 0 rows both before and after this round-trip. DB left at head.

**Finding (real, load-bearing, not fixed in source):** a fresh `schema.sql` -> `stamp` -> `upgrade head` bootstrap -- exactly the pattern `tests/godview/conftest.py` documents and depends on -- **fails** on this tree. `migrations/versions/phase4_fts_intelligence_search.py` (`phase4_fts_001`, an ancestor of `research_leases_0918`) `ALTER`s and later `SELECT`s from `discovered_hypotheses`, `analytical_snapshots`, `actors`, and `news_articles`; none of these tables is created by `schema.sql` or by any tracked migration. They are instead created lazily, at runtime, by app-level `ensure_tables()`/`_ensure_tables()` functions scattered across `intelligence/hypothesis_engine.py`, `store/snapshots.py`, `intelligence/actors/db.py`, and `ingestion/altdata/news_scraper.py` (a pattern used by 30+ modules in this codebase). A later migration (`god_view_market_tables_20260918`) additionally assumes `market_briefings` exists, which is created lazily inside `ollama/market_briefing.py::_persist_to_db` -- also never migration-tracked.

Because I could not edit source, I unblocked this by calling the tree's own idempotent table-creation functions directly against the scratch DB (in this order, before re-running `alembic upgrade head`): `intelligence.hypothesis_engine.ensure_tables(engine)`, `store.snapshots.ensure_analytical_snapshots_table(engine)`, `intelligence.actors.db._ensure_tables(engine)`, `NewsScraperPuller._ensure_news_table()` (called on a bypassed-`__init__` instance to avoid an unrelated `source_catalog` seed-row bug), and the literal `CREATE TABLE IF NOT EXISTS market_briefings ...` DDL copied verbatim from `ollama/market_briefing.py`. This reproduces what a long-lived, previously-used app instance would already have done before this migration ever ran -- it is not new schema. **This is a real onboarding/fresh-install gap in the migration chain and should be fixed in source** (either by tracking these tables in migrations, or by having the affected migrations call the same `ensure_tables()` functions defensively).

## Step 2 — DB-gated suites (GRID_TEST_DB_URL set, `-rs -rA`)

| Suite | Executed | Result |
|---|---|---|
| `tests/godview` | 8 DB tests + 20 pure tests, all executed | 27 passed, **1 failed** |
| `tests/test_evaluate_signals_cli.py` | 11/11 executed | 11 passed |
| `tests/test_write_fencing_live_db.py` | 2/2 executed | 2 passed |
| `tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | 25 executed | 24 passed, 1 skipped (in-scope) |
| `tests/test_pit.py` | 5/5 executed | 5 passed |
| `tests/test_promotion_ledger.py`, `test_signal_outcomes.py`, `test_signal_weight_overrides.py`, `test_write_fencing_leases.py` (grep hits on signal_evaluations/promotion_ledger/research_leases/godview_generations/cftc_positioning_daily) | 52/52 executed | 52 passed |

**FAILED** (real assertion failure, not a skip): `tests/godview/test_cftc_pillar_db.py::test_partial_refresh_cannot_expose_a_mixed_generation` -- `assert call_count["n"] == 1` failed with `2 == 1`. The test simulates a crash inside `record_generation` mid-materialization and expects exactly one call before the transaction aborts; it was called twice (likely a retry path in `materialize_cftc_pillar` or a double-invocation on this tree). Reported verbatim, not investigated further.

**SKIPPED** (in-scope, explained by the suite itself): `tests/integration/test_research_vertical_slice.py:806` -- "predict_fn is live on this (composed) tree -- see `TestPredictFnLiveComposed556::test_honest_predictor_passes_cheating_same_bar_predictor_fails`" (that replacement test executed and passed).

`tests/integration/acceptance_summary.json`:
```json
{
  "mode": "acceptance",
  "required_behaviours": {
    "availability_vintage": {"executed": true, "passed": true},
    "cheating_predictor_rejected": {"executed": true, "passed": true},
    "chronological_holdout": {"executed": true, "passed": true},
    "feature_order_invariance": {"executed": true, "passed": true},
    "postgres_pit_vintage_proof": {"executed": true, "passed": true},
    "predictor_used": {"executed": true, "passed": true},
    "some_required_behaviour": {"executed": true, "passed": false}
  },
  "tree_sha": "6e8df878a2f3893deb01ea6ec99a38edeb534440"
}
```
`postgres_pit_vintage_proof` executed and passed against the real disposable Postgres, as required. `some_required_behaviour: passed=false` is a self-test artifact -- it is the dummy name `TestAcceptanceModeItself` uses to unit-test `require_capability`'s own fail path (module-level results dict is session-scoped, not per-test), not a real required-behaviour failure.

`alembic current`/`heads` after step 2: unchanged, still `research_leases_0918 (head)`.

**Total: 105 DB-gated/integration tests executed, 103 passed, 1 failed (real), 1 skipped (in-scope, explained).**

## Step 3 — Real-API acceptance

Started `api.main:app` via uvicorn (scratch venv, fastapi 0.139) on `127.0.0.1:8010` against the disposable DB, with a freshly generated 48-char `GRID_JWT_SECRET` and a freshly generated master password (bcrypt-hashed into `GRID_MASTER_PASSWORD_HASH`) -- neither value recorded anywhere; the plaintext password lived only in a local, never-printed, deleted-at-end scratch env file and process memory. `GRID_ALLOW_PAID_LLM=0`. No other startup-disable flag exists in `api/main.py` (checked `config.py` and the lifespan function) -- the deferred-startup thread, contracts dispatcher/retry, spider-graph warm, dashboard pre-warm, and sector-flow warm threads all started as they would in any dev run; several logged handled warnings for tables that don't exist on this minimal DB (`contracts_dead_letter`, `oracle_models`, `actor_connections`) -- all non-fatal, server served requests fine.

Fixture data loaded through the tree's own code (no raw SQL where a loader existed): `godview.cftc_pillar.materialize_cftc_pillar()` on a `build_cot_records()`-generated CFTC fixture -> one generation, `status=SUCCESS`. Then the `feature_registry`/`resolved_series`/`signal_sources` fixture for ticker `TEST1` (mirrors `test_evaluate_signals_cli.py::seeded_signal`, left in place). Then `python scripts/evaluate_signals.py --source-type dbproof_test --persist --i-understand-this-writes-evaluations` -> `persisted_count: 1`.

Logged in via `POST /api/v1/auth/login` with the master password; got a bearer token. All 12 journey endpoints returned **200** with honest states -- no fabricated numbers anywhere:

| Endpoint | State |
|---|---|
| `/system/health` | `degraded`, reason: ingestion thread not running |
| `/system/freshness` | all families `RED`/stale (no live pullers ran) |
| `/system/pipeline-health` | 5 healthy / 12 broken of 17 sources |
| `/snapshots/research/latest` | `{"status":"no_runs"}` |
| `/godview/pillars/cftc?as_of=2023-02-21` (in-fixture) | `available:true, status:partial, coverage:0.25` (1/4 contracts -- only the fixture contract has data) |
| `/godview/pillars/cftc?as_of=2020-01-01` (before any release) | `available:true, status:partial, coverage:0.0` -- correctly empty, no lookahead |
| `/regime/current` | `UNCALIBRATED`, explicit "no data" message |
| `/watchlist/` | empty list, `total:0` |
| `/ten-year-portfolio/weekly` | `status:empty`, explicit sparse-history reason |
| `/dad/ticker/TEST1/gold` | `status:unavailable`, "not in Dad's workbook corpus yet" |
| `/watchlist/TEST1/edge` | all-empty arrays, `edge_summary:"Limited intelligence signals."` |
| `/snapshots/latest/pipeline_summary?n=1` | `[]` |

Browser acceptance: started the integration worktree's `pwa/` dev server on port 5175 (`GRID_API_PROXY_TARGET=http://127.0.0.1:8010`) and drove it with a small puppeteer-core wrapper (`dbproof/browser_wrapper.mjs`, reimplementing `run_evidence.mjs`'s `loginThroughRealForm`/`waitSettled`/`waitForReady`/`detectCrash` logic -- that file has no `export`s so it can't be imported cross-file without editing the other worktree). Logged in through the real form with the real master password. Visited `home`, `ticker-lookup`, `#/watchlist/TEST1`, `#/godview`: none crashed (`detectCrash` false on all four); `ticker-lookup` and `godview` matched their ready-text; `home` and `watchlist-analysis` timed out waiting for ready-text (home never got the scripted "click a suggested question" step in this minimal wrapper; watchlist-analysis's chart panel showed an honest "Loading chart data... / No price data for this period", not a fabricated number). Screenshots/text/console/network saved under `dbproof/evidence-real-api/`.

Stopped uvicorn and the vite dev server (PIDs 13448 and 10904) when done; only stale TIME_WAIT sockets remain, no listeners.

## What this proves / does not prove

This proves: on a genuinely empty, disposable, uniquely-named PostgreSQL 14 database, this tree's Fable-composed migrations, PIT store, godview CFTC materializer, signal evaluator persist path, write-fencing leases, promotion ledger, and the real FastAPI app (real JWT auth, real DB, no fixture server) all function end-to-end through the repo's own code paths, and every checked endpoint returns honest data-or-explicit-unavailable states rather than fabricated numbers. It also surfaces one real test regression (`test_partial_refresh_cannot_expose_a_mixed_generation`) and one real onboarding bug (the alembic chain cannot bootstrap cleanly from `schema.sql` alone -- it silently depends on app-level lazy table creation that a fresh scratch DB never gets).

This does **not** prove: production readiness, behavior under concurrent real load, correctness of the many endpoints not on the journey list, or that the lazy-table-creation workaround used here happens automatically anywhere in the real deploy/test pipeline -- that gap should be fixed in source, not routed around again.

## Run 2 (part 1) — BLOCKED at step 1 (merge denied by permission system)

Attempted `git merge --no-ff --no-edit origin/fable/godview-20260918` in `C:/Users/owner/dev/GRID-fable-wt-integration`. The environment's own auto-mode permission classifier **denied** the command: "Permission for this action was denied by the Claude Code auto mode classifier. Reason: [Modify Shared Resources]." This worktree is apparently treated as a shared resource (other sessions may have work in progress there), so a real merge commit could not be created by me.

Per the tool's own guidance on a denial like this, I did not attempt to bypass it. Instead I ran two fully read-only checks that touch no ref and no working-tree file:
- `git merge-tree --write-tree HEAD origin/fable/godview-20260918` -> resolved cleanly to tree `2669e4e6432860fcaf2ec01041ffa84867c4f065`, **no conflicts**.
- Built an unreferenced simulated commit (`git commit-tree`, parents `HEAD` + the godview branch tip, pointing at that tree) and ran `git merge-tree --write-tree <sim-commit> origin/fable/write-fencing-20260918` -> resolved cleanly to tree `94d18e1fcfce5bcf0cb1b11bb903eafd85a92d53`, **no conflicts**. This unreferenced commit was never attached to any branch; nothing on disk changed.

Confirmed afterward: `git status --short` empty, `git rev-parse HEAD` still `6e8df878a2f3893deb01ea6ec99a38edeb534440`. The coordinator then performed both merges directly and asked me to resume from step 3.

## Run 2 — composition 783ff735 (base), re-verified on composition 42df4362

Merges performed by the coordinator. Verified `git rev-parse HEAD` == `783ff7353580e6774e48c27b4645816daada659f` for the bulk of Run 2, then later == `42df4362b4398bdbae4cc2ddaf9be8681475051a` (783ff735 + additive commit `a828f4bf`, Fed-liquidity unit normalization) for the re-run the coordinator requested mid-task. Tree clean (`git status --short` empty) at both checks. No git commands that change refs were run by me in either case.

### Step 2 — static check
`python -m pytest tests/test_alembic_single_head.py -q` -> **3 passed**, confirming the single head `research_leases_0918` with the chain `god_view_market_tables_20260918 -> signal_evaluations_0918 -> promotion_ledger_0918 -> godview_pit_cftc_0918 -> godview_avail_basis_0918 -> godview_pit_fed_0918 -> godview_pit_cmdty_0918 -> research_leases_0918`.

### Step 3 — scratch DB revision-graph handling (exact actions taken)
Verified DB name prefix `griddb_fable_20260918_1740` first. `alembic current` reported `research_leases_0918 (head)` -- but this was **stale**: the revision-id *string* stamped by Run 1 happened to equal the new head's id too (the revision was re-parented under the same name, `down_revision` changed from `godview_pit_cftc_0918` to `godview_pit_cmdty_0918`), so alembic considered nothing pending even though the new pillar migrations had never run on this DB.

Per the coordinator's specified handling order, I ran `alembic downgrade godview_pit_cftc_0918` **first** (not the manual-surgery fallback). It worked cleanly with no error -- it walked `research_leases_0918 -> godview_pit_cmdty_0918 -> godview_pit_fed_0918 -> godview_avail_basis_0918 -> godview_pit_cftc_0918`, since every intervening `downgrade()` is written idempotently (`DROP COLUMN/TABLE IF EXISTS`), so replaying them against a DB that never ran the matching `upgrade()` was a harmless no-op except for `research_leases_0918`'s own downgrade, which genuinely dropped the real `research_leases` table Run 1 had created. Verified: `alembic current` -> `godview_pit_cftc_0918`; `research_leases` table confirmed dropped; `cftc_positioning_daily`/`godview_generations`/`fed_net_liquidity_daily`/`commodity_warehouse_inventories` (all pre-existing since `god_view_market_tables_20260918`, upstream of this range) confirmed still present. **The manual "drop table + UPDATE alembic_version" fallback was not needed** -- I did not touch `alembic_version` directly.

Then `alembic upgrade head` -- walked cleanly through `godview_pit_cftc_0918 -> godview_avail_basis_0918 -> godview_pit_fed_0918 -> godview_pit_cmdty_0918 -> research_leases_0918`. Verified: `research_leases` table exists again; `availability_basis` column now genuinely present on `cftc_positioning_daily`, `fed_net_liquidity_daily`, and `commodity_warehouse_inventories`. `alembic current`/`heads` both -> `research_leases_0918 (head)`. Prior Run-1 data survived the whole round-trip untouched: `godview_generations` 24 rows, `cftc_positioning_daily` 76 rows, `signal_evaluations` 1 row, `signal_sources` TEST1 fixture intact.

### Step 4 — DB-gated suites, composition 783ff735
| Suite | Executed | Result |
|---|---|---|
| `tests/godview` | 71 (23 DB-gated + 48 pure) | **68 passed, 3 failed** |
| `tests/test_write_fencing_live_db.py` | 2/2 | 2 passed |
| `tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | 25 | 24 passed, 1 skipped (same in-scope reason as Run 1: superseded by `TestPredictFnLiveComposed556`, which passed) |
| `tests/test_evaluate_signals_cli.py` | 11/11 | 11 passed |

`acceptance_summary.json`: all 6 real required behaviours `passed:true` (incl. `postgres_pit_vintage_proof`); `tree_sha` = `783ff7353580e6774e48c27b4645816daada659f`; `some_required_behaviour:false` is the same self-test artifact noted in Run 1.

**3 failures on `tests/godview` (composition 783ff735), verbatim:**
1. `test_cftc_pillar_api_db.py::test_api_router_returns_available_generation_end_to_end` -- `assert response["include_inferred"] is False` -> `assert Query(False) is False`. Calling the router function directly (bypassing FastAPI's DI) left the `include_inferred: bool = Query(...)` parameter as an unresolved `Query` object instead of a plain bool.
2. `test_cftc_pillar_api_db.py::test_api_router_exposes_availability_basis_and_admits_inferred_rows_when_flagged` -- `assert contract_code not in default_response["fields"]` failed: the backfilled contract code **was** present in the default (non-flagged) response's fields, i.e. an inferred/backfilled row was **not excluded by default** at the API-router layer.
3. `test_cftc_pillar_db.py::test_pit_read_excludes_rows_released_after_as_of` -- `assert last_report_date in after_dates` failed with `assert datetime.date(2023, 3, 7) in set()`: the newest report's row was still **not visible** even as-of its own release date.

**Confirmations requested:**
- `test_partial_refresh_cannot_expose_a_mixed_generation` -- **now passes** on real Postgres (both compositions).
- Missing component -> unavailable, no fallback constant: `test_missing_component_leaves_the_wednesday_unavailable_no_fallback` (Fed) and `test_missing_total_leaves_the_day_unmaterialized_no_fallback` (commodity) -- **both passed** (both compositions).
- Inferred row excluded by default, admitted only with `include_inferred=true`: **holds at the store/materializer level** -- `test_availability_basis_backfill_is_inferred_and_excluded_unless_flagged` passed (both compositions) -- but **fails at the API-router layer** -- failure #2 above shows the router's default response admits the inferred row anyway, on both compositions. Real, reproducible gap between store-level and API-level behavior.

### Step 4 re-run — composition 42df4362 (coordinator's mid-task additive merge)
Per the coordinator's instruction, re-ran only `tests/godview` and `tests/test_fed_liquidity_units.py` on `42df4362` (same scratch DB, no new migration in this commit). `tests/test_fed_liquidity_units.py`: **11/11 passed**. `tests/godview`: **76 passed, 6 failed** -- the same 3 failures as composition 783ff735 (identical verbatim errors) **plus 3 new failures introduced by `a828f4bf`**:
4. `test_fed_liquidity_pillar_db.py::test_materializer_writes_a_row_on_a_wednesday_when_all_three_components_present` -- `assert result.status == "SUCCESS"` -> `assert 'SUCCESS_NOOP' == 'SUCCESS'`.
5. `test_fed_liquidity_pillar_db.py::test_pit_read_excludes_a_row_released_after_as_of` -- same: `assert 'SUCCESS_NOOP' == 'SUCCESS'`.
6. `test_new_pillars_api_db.py::test_fed_liquidity_route_exposes_per_component_basis_and_derived_fields` -- same: `assert 'SUCCESS_NOOP' == 'SUCCESS'`.

All three fail identically: `materialize_fed_liquidity_pillar()` now returns `SUCCESS_NOOP` instead of `SUCCESS` for fixtures that previously materialized successfully -- a real regression introduced by the unit-normalization commit, reported verbatim, not investigated further.

### Step 5 — real API, God View routes (composition 783ff735; not re-run on 42df4362 per coordinator's "everything else unchanged")
Routes confirmed from `api/routers/godview_pillars.py`: `GET /api/v1/godview/pillars/cftc`, `.../pillars/fed_net_liquidity`, `.../pillars/commodity_warehouses`. Started uvicorn on `127.0.0.1:8010` with freshly generated (never recorded) JWT secret + master password, same disposable DB. All requests **200**, all honest states:

| Endpoint | Result |
|---|---|
| `/pillars/cftc?as_of=2023-02-21&include_inferred=false` | `available:true, status:partial, coverage:0.0, contracts_with_data:0/4` |
| `/pillars/cftc?as_of=2023-02-21&include_inferred=true` | `available:true, status:partial, coverage:0.25, contracts_with_data:1/4` -- visibly different from the `false` case, consistent with an inferred row existing (ties to failure #2 above: it shows up even when it shouldn't at `false`) |
| `/pillars/fed_net_liquidity?as_of=2023-02-21` | `available:true, status:partial, coverage:0.0, fields:{}` -- no fixture data loaded for this pillar in Run 2; honest empty state, no fabrication |
| `/pillars/commodity_warehouses?as_of=2023-02-21` | `available:true, status:partial, coverage:0.0, metals_with_data:0/6` -- honest empty state |

Stopped uvicorn afterward; port confirmed free. Secrets file and token deleted immediately after use, never printed.

### What Run 2 adds
Confirms the previously-failing atomicity test now passes on real Postgres under the new composition, and that both new pillars honestly report "unavailable, no fallback" when a component is missing. Surfaces two real, reproducible defects: (a) the CFTC pillar's API router admits an inferred/backfilled row by default when the underlying store correctly excludes it -- a store/API inconsistency; (b) commit `a828f4bf` (Fed-liquidity unit normalization) regressed three previously-passing Fed-pillar tests to `SUCCESS_NOOP`. Neither was fixed here -- no source was edited.

## Run 3 — composition c9e34036

Composition `c9e3403671a8fd4fc748ec0f46a7c4d9938aa1aa` (= `42df4362` + `fable/godview` commit `37111a8b`: Annotated `Query` defaults on the pillar routes, per-test isolation for the Fed-pillar DB tests, the PIT-boundary test fixed, plus a pure router-default test and a `TestClient`-over-real-DB test). Verified `git rev-parse HEAD` == `c9e3403671a8fd4fc748ec0f46a7c4d9938aa1aa`, `git status --short` empty. No git commands that change refs were run.

**Alembic:** `alembic heads` (repo) -> `research_leases_0918 (head)`. `alembic current` (scratch DB, before) -> `research_leases_0918 (head)`, already the same value as after Run 2. `alembic upgrade head` produced **no output lines at all** -- a genuine no-op, confirming no migration graph change came with this commit, as expected. `alembic current` (after) unchanged -> `research_leases_0918 (head)`.

**`tests/godview`** (`GRID_TEST_DB_URL` set, `-rs -rA`): **76 passed, 0 failed, 0 skipped** (up from 71 tests in Run 2 -- this commit added `test_router_via_real_http_excludes_inferred_row_by_default_admits_when_flagged`, `test_router_defaults_pure.py`'s 2 tests, and split the Fed-pillar isolation cases). All 24 DB-gated tests executed (none skipped -- `grep -c SKIPPED` on the run log returned `0`). One benign `DeprecationWarning` from `starlette/testclient.py` (anyio `BlockingPortal` alias), unrelated to any of this.

**`tests/test_fed_liquidity_units.py`**: 11/11 passed. **`tests/test_alembic_single_head.py`**: 3/3 passed (`test_single_alembic_head`, `test_revision_ids_fit_the_version_column`, `test_migration_warnings_are_not_swallowed`).

**No failures to report verbatim -- every test in all three suites passed.**

**Confirmations requested, all verified PASSED in this run's log:**
- `tests/godview/test_cftc_pillar_api_db.py::test_api_router_exposes_availability_basis_and_admits_inferred_rows_when_flagged` -- PASSED (previously failed in Run 2 on both 783ff735 and 42df4362).
- `tests/godview/test_cftc_pillar_api_db.py::test_router_via_real_http_excludes_inferred_row_by_default_admits_when_flagged` -- new `TestClient`-over-real-DB test -- PASSED.
- `tests/godview/test_cftc_pillar_db.py::test_pit_read_excludes_rows_released_after_as_of` -- PASSED (previously failed in Run 2).
- The three Fed-pillar tests that regressed to `SUCCESS_NOOP` on composition `42df4362` (Run 2) -- `tests/godview/test_fed_liquidity_pillar_db.py::test_materializer_writes_a_row_on_a_wednesday_when_all_three_components_present`, `tests/godview/test_fed_liquidity_pillar_db.py::test_pit_read_excludes_a_row_released_after_as_of`, `tests/godview/test_new_pillars_api_db.py::test_fed_liquidity_route_exposes_per_component_basis_and_derived_fields` -- **all PASSED**. (A fourth, related test in the same file, `test_missing_component_leaves_the_wednesday_unavailable_no_fallback`, never regressed and also PASSED here.)

### What Run 3 adds
Every defect surfaced in Run 2 (the CFTC API-router inferred-row leak, the CFTC PIT-boundary miss, and the three Fed-liquidity `SUCCESS_NOOP` regressions) is now fixed on composition `c9e34036`, confirmed by real Postgres execution, not just a diff read. `tests/godview` is fully green (76/76) with zero skips, `test_fed_liquidity_units.py` and `test_alembic_single_head.py` are both fully green, and the migration graph is unchanged (a genuine no-op `upgrade head`) since Run 2's schema state. This is the first composition across all three runs with no outstanding DB-gated failures.

Per the coordinator's instruction, stopping here -- the coordinator will tear down the scratch database themselves. Nothing left running: no uvicorn or dev-server process was started in this run (only `pytest` and `alembic`/`psql` checks against the already-provisioned scratch DB).

## Evidence attribution by tested SHA

Full table and analysis: `dbproof/ATTRIBUTION.md` (reproduced below verbatim). Reconstructed from this session's transcript and saved logs only -- no new database access (the scratch DB is gone), no new test runs, no source edits.

Four compositions appear across the three runs: `6e8df878`, `783ff735`, `42df4362`, `c9e34036`.

| # | Run | Command / suite | Composition SHA at execution | DB revision state at execution (`alembic current`) | Result | Notes |
|---|---|---|---|---|---|---|
| 1 | 1 | `db.apply_schema()` + `alembic stamp 7e4dfecce247` + `alembic upgrade head` (initial, failed; unblocked via repo `ensure_tables()` calls; retried) | `6e8df878` | empty -> `research_leases_0918 (head)` [old chain: `...godview_pit_cftc_0918 -> research_leases_0918`] | schema+migrations applied OK after unblock | first attempt failed (`discovered_hypotheses` etc. missing); see Step 1 above |
| 2 | 1 | `alembic downgrade signal_evaluations_0918` -> `alembic upgrade head` (round-trip check) | `6e8df878` | `research_leases_0918 (head)` before and after | round-trip OK, godview tables/rows survived | |
| 3 | 1 | `pytest tests/godview -rs -rA` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 27 passed, 1 failed (28 total; 8 DB-gated + 20 pure) | failure: `test_partial_refresh_cannot_expose_a_mixed_generation` |
| 4 | 1 | `pytest tests/test_evaluate_signals_cli.py` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 11 passed | |
| 5 | 1 | `pytest tests/test_write_fencing_live_db.py` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 2 passed | |
| 6 | 1 | `pytest tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | `6e8df878` | `research_leases_0918 (head)` [old chain] | 24 passed, 1 skipped (25 total) | skip in-scope; `postgres_pit_vintage_proof` passed |
| 7 | 1 | `pytest tests/test_pit.py` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 5 passed | |
| 8 | 1 | `pytest tests/test_promotion_ledger.py tests/test_signal_outcomes.py tests/test_signal_weight_overrides.py tests/test_write_fencing_leases.py` (one combined invocation) | `6e8df878` | `research_leases_0918 (head)` [old chain] | 52 passed | |
| 9 | 1 | Real API: uvicorn + 12 journey endpoints under `/api/v1/godview/pillars/cftc` etc. | `6e8df878` | `research_leases_0918 (head)` [old chain] | all 12 -> HTTP 200, honest states | fixture: one CFTC generation via `materialize_cftc_pillar`, TEST1 signal fixture |
| 10 | 1 | Browser wrapper (4 journeys via PWA dev server -> uvicorn) | `6e8df878` | `research_leases_0918 (head)` [old chain] | 4/4 no crash; 2 ready, 2 timeout (honest loading/empty state) | |
| 11 | 2 (blocked) | `git merge --no-ff --no-edit origin/fable/godview-20260918` | `6e8df878` | `research_leases_0918 (head)` [old chain] -- DB untouched, command never ran | DENIED by auto-mode permission classifier | no ref change, no DB access |
| 12 | 2 (blocked) | `git merge-tree --write-tree HEAD origin/fable/godview-20260918` (read-only dry run) | `6e8df878` | n/a -- no DB access | clean, tree `2669e4e6...`, no conflicts | diagnostic only, no ref/file changed |
| 13 | 2 (blocked) | simulated `git commit-tree` + `git merge-tree` vs `origin/fable/write-fencing-20260918` (read-only dry run) | `6e8df878` | n/a -- no DB access | clean, tree `94d18e1f...`, no conflicts | unreferenced commit object, no ref changed |
| 14 | 2 | `pytest tests/test_alembic_single_head.py -q` | `783ff735` | (static test, no DB touch -- DB was still stale `research_leases_0918 (head)` [old chain] from row 2 at this moment) | 3 passed | ran before the row-15 DB surgery in this session |
| 15 | 2 | `alembic current` (before surgery) | `783ff735` | `research_leases_0918 (head)` -- stale: string coincidentally equals the new head id, but DB never ran the new pillar migrations | reported stale head | see Step 3 above |
| 16 | 2 | `alembic downgrade godview_pit_cftc_0918` | `783ff735` | `research_leases_0918 (head)` [stale] -> `godview_pit_cftc_0918` | succeeded, no manual surgery needed | idempotent downgrades no-op'd through never-applied revisions; `research_leases` table genuinely dropped |
| 17 | 2 | `alembic upgrade head` (post-surgery) | `783ff735` | `godview_pit_cftc_0918` -> `research_leases_0918 (head)` [genuine, new chain] | succeeded | `availability_basis` columns verified present |
| 18 | 2 | `pytest tests/godview -rs -rA` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 68 passed, 3 failed (71 total) | failures: `test_api_router_returns_available_generation_end_to_end`, `test_api_router_exposes_availability_basis_and_admits_inferred_rows_when_flagged`, `test_pit_read_excludes_rows_released_after_as_of` |
| 19 | 2 | `pytest tests/test_write_fencing_live_db.py` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 2 passed | |
| 20 | 2 | `pytest tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 24 passed, 1 skipped (25 total) | same in-scope skip; `postgres_pit_vintage_proof` passed |
| 21 | 2 | `pytest tests/test_evaluate_signals_cli.py` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 11 passed | |
| 22 | 2 | Real API: uvicorn + `/pillars/cftc` (x2 `include_inferred`), `/pillars/fed_net_liquidity`, `/pillars/commodity_warehouses` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | all 4 -> HTTP 200, honest states | not re-run on 42df4362 or c9e34036 -- see below |
| 23 | 2 | `pytest tests/godview tests/test_fed_liquidity_units.py -rs -rA` (one combined invocation) | `42df4362` | `research_leases_0918 (head)` [genuine, new chain -- this commit added no migration] | 82 total: 76 passed, 6 failed | see correction below -- my earlier Run 2 text mislabeled this as "tests/godview: 76 passed, 6 failed"; corrected split below |
| 24 | 3 | `alembic current`/`heads`, `alembic upgrade head` | `c9e34036` | `research_leases_0918 (head)` before and after; `upgrade head` produced no output (genuine no-op) | confirmed no-op | |
| 25 | 3 | `pytest tests/godview -rs -rA` | `c9e34036` | `research_leases_0918 (head)` [genuine, new chain] | 76 passed, 0 failed, 0 skipped | all 24 DB-gated tests executed |
| 26 | 3 | `pytest tests/test_fed_liquidity_units.py` | `c9e34036` | `research_leases_0918 (head)` [genuine, new chain] | 11 passed | |
| 27 | 3 | `pytest tests/test_alembic_single_head.py` | `c9e34036` | (static test, no DB touch) | 3 passed | |

**Correction (row 23):** the earlier "Run 2" section above says *"`tests/godview`: 76 passed, 6 failed"* for composition `42df4362`. That is the **combined** total of one pytest invocation covering both `tests/godview` and `tests/test_fed_liquidity_units.py` together, not `tests/godview` alone. Split correctly: `tests/test_fed_liquidity_units.py` alone = 11 passed, 0 failed; `tests/godview` alone (by subtraction) = **71 total, 65 passed, 6 failed** -- the same 71-test total as row 18's `783ff735` run (this commit added no new tests inside `tests/godview` itself). The 6 failures themselves were reported correctly and verbatim; only the "76 passed" headline number was attributable to the wrong scope.

**Mid-run tree changes, explicit.** During Run 2 (one heading, two tree states): rows 11-13 ran at `6e8df878` before either merge (the merge itself was denied); the coordinator then merged both branches directly -> `783ff735`; rows 14-22 ran there; mid-task the coordinator merged one more additive commit (`a828f4bf`, Fed-liquidity unit fix, no migration change) -> `42df4362`; row 23 ran there. Between Run 2 and Run 3, the coordinator merged `fable/godview` commit `37111a8b` on top of `42df4362` -> `c9e34036`; rows 24-27 (Run 3) ran there.

**Results whose tested SHA differs from the composition current at end of session (`c9e34036`):**
- Rows 9, 10 (Run 1 real-API + browser) -- tested at `6e8df878`, three compositions behind; the Fed/commodity pillars and availability-basis mechanism did not exist yet.
- Row 22 (Run 2 real-API God View routes) -- tested at `783ff735`, two compositions behind. The `include_inferred=false` vs `=true` difference captured there reflects the now-fixed inferred-row-admits-by-default defect (row 18 failure #2), not `c9e34036`'s corrected behavior. This endpoint evidence was never re-captured against `42df4362` or `c9e34036` -- no live server checks ran in Run 3.
- Row 3 (Run 1 `tests/godview`) -- its 1 failure is the same test later confirmed passing at rows 18 and 25.
- Row 18 (Run 2 `tests/godview` at `783ff735`) -- 3 of its failures were later confirmed fixed at row 25 (`c9e34036`).
- Row 23 (`42df4362`) -- see the correction above; its 6 failures were confirmed fixed at row 25 (`c9e34036`).

No database access was used to produce this table. No source was edited.
