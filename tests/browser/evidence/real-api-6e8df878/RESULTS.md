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

**Finding (real, load-bearing, not fixed in source):** a fresh `schema.sql` → `stamp` → `upgrade head` bootstrap — exactly the pattern `tests/godview/conftest.py` documents and depends on — **fails** on this tree. `migrations/versions/phase4_fts_intelligence_search.py` (`phase4_fts_001`, an ancestor of `research_leases_0918`) `ALTER`s and later `SELECT`s from `discovered_hypotheses`, `analytical_snapshots`, `actors`, and `news_articles`; none of these tables is created by `schema.sql` or by any tracked migration. They are instead created lazily, at runtime, by app-level `ensure_tables()`/`_ensure_tables()` functions scattered across `intelligence/hypothesis_engine.py`, `store/snapshots.py`, `intelligence/actors/db.py`, and `ingestion/altdata/news_scraper.py` (a pattern used by 30+ modules in this codebase). A later migration (`god_view_market_tables_20260918`) additionally assumes `market_briefings` exists, which is created lazily inside `ollama/market_briefing.py::_persist_to_db` — also never migration-tracked.

Because I could not edit source, I unblocked this by calling the tree's own idempotent table-creation functions directly against the scratch DB (in this order, before re-running `alembic upgrade head`): `intelligence.hypothesis_engine.ensure_tables(engine)`, `store.snapshots.ensure_analytical_snapshots_table(engine)`, `intelligence.actors.db._ensure_tables(engine)`, `NewsScraperPuller._ensure_news_table()` (called on a bypassed-`__init__` instance to avoid an unrelated `source_catalog` seed-row bug), and the literal `CREATE TABLE IF NOT EXISTS market_briefings ...` DDL copied verbatim from `ollama/market_briefing.py`. This reproduces what a long-lived, previously-used app instance would already have done before this migration ever ran — it is not new schema. **This is a real onboarding/fresh-install gap in the migration chain and should be fixed in source** (either by tracking these tables in migrations, or by having the affected migrations call the same `ensure_tables()` functions defensively).

## Step 2 — DB-gated suites (GRID_TEST_DB_URL set, `-rs -rA`)

| Suite | Executed | Result |
|---|---|---|
| `tests/godview` | 8 DB tests + 20 pure tests, all executed | 27 passed, **1 failed** |
| `tests/test_evaluate_signals_cli.py` | 11/11 executed | 11 passed |
| `tests/test_write_fencing_live_db.py` | 2/2 executed | 2 passed |
| `tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | 25 executed | 24 passed, 1 skipped (in-scope) |
| `tests/test_pit.py` | 5/5 executed | 5 passed |
| `tests/test_promotion_ledger.py`, `test_signal_outcomes.py`, `test_signal_weight_overrides.py`, `test_write_fencing_leases.py` (grep hits on signal_evaluations/promotion_ledger/research_leases/godview_generations/cftc_positioning_daily) | 52/52 executed | 52 passed |

**FAILED** (real assertion failure, not a skip): `tests/godview/test_cftc_pillar_db.py::test_partial_refresh_cannot_expose_a_mixed_generation` — `assert call_count["n"] == 1` failed with `2 == 1`. The test simulates a crash inside `record_generation` mid-materialization and expects exactly one call before the transaction aborts; it was called twice (likely a retry path in `materialize_cftc_pillar` or a double-invocation on this tree). Reported verbatim, not investigated further.

**SKIPPED** (in-scope, explained by the suite itself): `tests/integration/test_research_vertical_slice.py:806` — "predict_fn is live on this (composed) tree — see `TestPredictFnLiveComposed556::test_honest_predictor_passes_cheating_same_bar_predictor_fails`" (that replacement test executed and passed).

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
`postgres_pit_vintage_proof` executed and passed against the real disposable Postgres, as required. `some_required_behaviour: passed=false` is a self-test artifact — it is the dummy name `TestAcceptanceModeItself` uses to unit-test `require_capability`'s own fail path (module-level results dict is session-scoped, not per-test), not a real required-behaviour failure.

`alembic current`/`heads` after step 2: unchanged, still `research_leases_0918 (head)`.

**Total: 105 DB-gated/integration tests executed, 103 passed, 1 failed (real), 1 skipped (in-scope, explained).**

## Step 3 — Real-API acceptance

Started `api.main:app` via uvicorn (scratch venv, fastapi 0.139) on `127.0.0.1:8010` against the disposable DB, with a freshly generated 48-char `GRID_JWT_SECRET` and a freshly generated master password (bcrypt-hashed into `GRID_MASTER_PASSWORD_HASH`) — neither value recorded anywhere; the plaintext password lived only in a local, never-printed, deleted-at-end scratch env file and process memory. `GRID_ALLOW_PAID_LLM=0`. No other startup-disable flag exists in `api/main.py` (checked `config.py` and the lifespan function) — the deferred-startup thread, contracts dispatcher/retry, spider-graph warm, dashboard pre-warm, and sector-flow warm threads all started as they would in any dev run; several logged handled warnings for tables that don't exist on this minimal DB (`contracts_dead_letter`, `oracle_models`, `actor_connections`) — all non-fatal, server served requests fine.

Fixture data loaded through the tree's own code (no raw SQL where a loader existed): `godview.cftc_pillar.materialize_cftc_pillar()` on a `build_cot_records()`-generated CFTC fixture → one generation, `status=SUCCESS`. Then the `feature_registry`/`resolved_series`/`signal_sources` fixture for ticker `TEST1` (mirrors `test_evaluate_signals_cli.py::seeded_signal`, left in place). Then `python scripts/evaluate_signals.py --source-type dbproof_test --persist --i-understand-this-writes-evaluations` → `persisted_count: 1`.

Logged in via `POST /api/v1/auth/login` with the master password; got a bearer token. All 12 journey endpoints returned **200** with honest states — no fabricated numbers anywhere:

| Endpoint | State |
|---|---|
| `/system/health` | `degraded`, reason: ingestion thread not running |
| `/system/freshness` | all families `RED`/stale (no live pullers ran) |
| `/system/pipeline-health` | 5 healthy / 12 broken of 17 sources |
| `/snapshots/research/latest` | `{"status":"no_runs"}` |
| `/godview/pillars/cftc?as_of=2023-02-21` (in-fixture) | `available:true, status:partial, coverage:0.25` (1/4 contracts — only the fixture contract has data) |
| `/godview/pillars/cftc?as_of=2020-01-01` (before any release) | `available:true, status:partial, coverage:0.0` — correctly empty, no lookahead |
| `/regime/current` | `UNCALIBRATED`, explicit "no data" message |
| `/watchlist/` | empty list, `total:0` |
| `/ten-year-portfolio/weekly` | `status:empty`, explicit sparse-history reason |
| `/dad/ticker/TEST1/gold` | `status:unavailable`, "not in Dad's workbook corpus yet" |
| `/watchlist/TEST1/edge` | all-empty arrays, `edge_summary:"Limited intelligence signals."` |
| `/snapshots/latest/pipeline_summary?n=1` | `[]` |

Browser acceptance: started the integration worktree's `pwa/` dev server on port 5175 (`GRID_API_PROXY_TARGET=http://127.0.0.1:8010`) and drove it with a small puppeteer-core wrapper (`dbproof/browser_wrapper.mjs`, reimplementing `run_evidence.mjs`'s `loginThroughRealForm`/`waitSettled`/`waitForReady`/`detectCrash` logic — that file has no `export`s so it can't be imported cross-file without editing the other worktree). Logged in through the real form with the real master password. Visited `home`, `ticker-lookup`, `#/watchlist/TEST1`, `#/godview`: none crashed (`detectCrash` false on all four); `ticker-lookup` and `godview` matched their ready-text; `home` and `watchlist-analysis` timed out waiting for ready-text (home never got the scripted "click a suggested question" step in this minimal wrapper; watchlist-analysis's chart panel showed an honest "Loading chart data... / No price data for this period", not a fabricated number). Screenshots/text/console/network saved under `dbproof/evidence-real-api/`.

Stopped uvicorn and the vite dev server (PIDs 13448 and 10904) when done; only stale TIME_WAIT sockets remain, no listeners.

## What this proves / does not prove

This proves: on a genuinely empty, disposable, uniquely-named PostgreSQL 14 database, this tree's Fable-composed migrations, PIT store, godview CFTC materializer, signal evaluator persist path, write-fencing leases, promotion ledger, and the real FastAPI app (real JWT auth, real DB, no fixture server) all function end-to-end through the repo's own code paths, and every checked endpoint returns honest data-or-explicit-unavailable states rather than fabricated numbers. It also surfaces one real test regression (`test_partial_refresh_cannot_expose_a_mixed_generation`) and one real onboarding bug (the alembic chain cannot bootstrap cleanly from `schema.sql` alone — it silently depends on app-level lazy table creation that a fresh scratch DB never gets).

This does **not** prove: production readiness, behavior under concurrent real load, correctness of the many endpoints not on the journey list, or that the lazy-table-creation workaround used here happens automatically anywhere in the real deploy/test pipeline — that gap should be fixed in source, not routed around again.
