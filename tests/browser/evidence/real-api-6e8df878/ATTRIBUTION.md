# Evidence attribution by tested SHA

Reconstructed from this session's own transcript and the saved log files under `dbproof/`, `dbproof/run2/`, `dbproof/run3/` — no new database access, no new test runs, no source edits. Where a saved log exists it was re-read to get exact counts (`grep` on the pass/fail summary line); Run 1's individual suite counts (no per-suite log file was saved for those, only inline output) are taken as recorded in the transcript and in `dbproof/RESULTS.md`.

Four compositions appear across the three runs: `6e8df878` (`6e8df878a2f3893deb01ea6ec99a38edeb534440`), `783ff735` (`783ff7353580e6774e48c27b4645816daada659f`), `42df4362` (`42df4362b4398bdbae4cc2ddaf9be8681475051a`), `c9e34036` (`c9e3403671a8fd4fc748ec0f46a7c4d9938aa1aa`).

## Table — one row per (suite/command, run)

| # | Run | Command / suite | Composition SHA at execution | DB revision state at execution (`alembic current`) | Result | Notes |
|---|---|---|---|---|---|---|
| 1 | 1 | `db.apply_schema()` + `alembic stamp 7e4dfecce247` + `alembic upgrade head` (initial, failed; unblocked via repo `ensure_tables()` calls; retried) | `6e8df878` | empty → `research_leases_0918 (head)` [old chain: `...godview_pit_cftc_0918 → research_leases_0918`] | schema+migrations applied OK after unblock | first attempt failed (`discovered_hypotheses` etc. missing); see RESULTS.md Step 1 |
| 2 | 1 | `alembic downgrade signal_evaluations_0918` → `alembic upgrade head` (round-trip check) | `6e8df878` | `research_leases_0918 (head)` before and after | round-trip OK, godview tables/rows survived | |
| 3 | 1 | `pytest tests/godview -rs -rA` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 27 passed, 1 failed (28 total; 8 DB-gated + 20 pure) | failure: `test_partial_refresh_cannot_expose_a_mixed_generation` |
| 4 | 1 | `pytest tests/test_evaluate_signals_cli.py` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 11 passed | |
| 5 | 1 | `pytest tests/test_write_fencing_live_db.py` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 2 passed | |
| 6 | 1 | `pytest tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | `6e8df878` | `research_leases_0918 (head)` [old chain] | 24 passed, 1 skipped (25 total) | skip in-scope; `postgres_pit_vintage_proof` passed |
| 7 | 1 | `pytest tests/test_pit.py` | `6e8df878` | `research_leases_0918 (head)` [old chain] | 5 passed | |
| 8 | 1 | `pytest tests/test_promotion_ledger.py tests/test_signal_outcomes.py tests/test_signal_weight_overrides.py tests/test_write_fencing_leases.py` (one combined invocation) | `6e8df878` | `research_leases_0918 (head)` [old chain] | 52 passed | |
| 9 | 1 | Real API: uvicorn + 12 journey endpoints under `/api/v1/godview/pillars/cftc` etc. | `6e8df878` | `research_leases_0918 (head)` [old chain] | all 12 → HTTP 200, honest states | fixture: one CFTC generation via `materialize_cftc_pillar`, TEST1 signal fixture |
| 10 | 1 | Browser wrapper (4 journeys via PWA dev server → uvicorn) | `6e8df878` | `research_leases_0918 (head)` [old chain] | 4/4 no crash; 2 ready, 2 timeout (honest loading/empty state) | |
| 11 | 2 (blocked) | `git merge --no-ff --no-edit origin/fable/godview-20260918` | `6e8df878` | `research_leases_0918 (head)` [old chain] — **DB untouched, command never ran** | **DENIED** by auto-mode permission classifier | no ref change, no DB access |
| 12 | 2 (blocked) | `git merge-tree --write-tree HEAD origin/fable/godview-20260918` (read-only dry run) | `6e8df878` | n/a — no DB access | clean, tree `2669e4e6...`, no conflicts | diagnostic only, no ref/file changed |
| 13 | 2 (blocked) | simulated `git commit-tree` + `git merge-tree` vs `origin/fable/write-fencing-20260918` (read-only dry run) | `6e8df878` | n/a — no DB access | clean, tree `94d18e1f...`, no conflicts | unreferenced commit object, no ref changed |
| 14 | 2 | `pytest tests/test_alembic_single_head.py -q` | `783ff735` | (static test, no DB touch — DB was still stale `research_leases_0918 (head)` [old chain] from row 2 at this moment) | 3 passed | ran **before** the row-15 DB surgery in this session |
| 15 | 2 | `alembic current` (before surgery) | `783ff735` | `research_leases_0918 (head)` — **stale**: string coincidentally equals the new head id, but DB never ran the new pillar migrations | reported stale head | see RESULTS.md Step 3 |
| 16 | 2 | `alembic downgrade godview_pit_cftc_0918` | `783ff735` | `research_leases_0918 (head)` [stale] → `godview_pit_cftc_0918` | succeeded, no manual surgery needed | idempotent downgrades no-op'd through never-applied revisions; `research_leases` table genuinely dropped |
| 17 | 2 | `alembic upgrade head` (post-surgery) | `783ff735` | `godview_pit_cftc_0918` → `research_leases_0918 (head)` [genuine, new chain] | succeeded | `availability_basis` columns verified present |
| 18 | 2 | `pytest tests/godview -rs -rA` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 68 passed, 3 failed (71 total) | failures: `test_api_router_returns_available_generation_end_to_end`, `test_api_router_exposes_availability_basis_and_admits_inferred_rows_when_flagged`, `test_pit_read_excludes_rows_released_after_as_of` |
| 19 | 2 | `pytest tests/test_write_fencing_live_db.py` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 2 passed | |
| 20 | 2 | `pytest tests/integration/test_research_vertical_slice.py` (`GRID_ACCEPTANCE_TREE=1 -rA`) | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 24 passed, 1 skipped (25 total) | same in-scope skip; `postgres_pit_vintage_proof` passed |
| 21 | 2 | `pytest tests/test_evaluate_signals_cli.py` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | 11 passed | |
| 22 | 2 | Real API: uvicorn + `/pillars/cftc` (×2 `include_inferred`), `/pillars/fed_net_liquidity`, `/pillars/commodity_warehouses` | `783ff735` | `research_leases_0918 (head)` [genuine, new chain] | all 4 → HTTP 200, honest states | **not re-run on 42df4362 or c9e34036** — see "differs from later SHA" below |
| 23 | 2 | `pytest tests/godview tests/test_fed_liquidity_units.py -rs -rA` (**one combined invocation**) | `42df4362` | `research_leases_0918 (head)` [genuine, new chain — this commit added no migration] | **82 total: 76 passed, 6 failed** | **see correction below** — my Run 2 RESULTS.md text mislabeled this as "tests/godview: 76 passed, 6 failed"; corrected split below |
| 24 | 3 | `alembic current`/`heads`, `alembic upgrade head` | `c9e34036` | `research_leases_0918 (head)` before and after; `upgrade head` produced no output (genuine no-op) | confirmed no-op | |
| 25 | 3 | `pytest tests/godview -rs -rA` | `c9e34036` | `research_leases_0918 (head)` [genuine, new chain] | 76 passed, 0 failed, 0 skipped | all 24 DB-gated tests executed |
| 26 | 3 | `pytest tests/test_fed_liquidity_units.py` | `c9e34036` | `research_leases_0918 (head)` [genuine, new chain] | 11 passed | |
| 27 | 3 | `pytest tests/test_alembic_single_head.py` | `c9e34036` | (static test, no DB touch) | 3 passed | |

## Correction — row 23 count was mislabeled in `dbproof/RESULTS.md`'s Run 2 section

`RESULTS.md`'s "Step 4 re-run — composition 42df4362" text says *"`tests/godview`: 76 passed, 6 failed"*. That is the **combined** total of one pytest invocation covering both `tests/godview` **and** `tests/test_fed_liquidity_units.py` together, not `tests/godview` alone. Splitting it correctly, using `tests/test_fed_liquidity_units.py`'s independently-confirmed 11/11-passed count (row 23, and reconfirmed standalone in Run 3 at row 26):

- `tests/test_fed_liquidity_units.py` alone: 11 passed, 0 failed.
- `tests/godview` alone (by subtraction): **71 total — 65 passed, 6 failed.** (Same 71-test total as row 18's `783ff735` run — this commit added no new tests to `tests/godview` itself, only the separate `test_fed_liquidity_units.py` file.)

The 6 failures themselves were reported correctly and verbatim in RESULTS.md (3 carried over from row 18 + 3 new `SUCCESS_NOOP` regressions from commit `a828f4bf`); only the "76 passed" headline number was attributable to the wrong scope. This does not change any pass/fail verdict, only the row-18-vs-row-23 `tests/godview` totals (71 in both, composition-for-composition).

## Mid-run tree changes, explicit

**During Run 2** (single "Run 2" heading, two tree states):
- Rows 11–13 ran at `6e8df878` (before either merge; the merge itself was denied).
- The coordinator merged `origin/fable/godview-20260918` and `origin/fable/write-fencing-20260918` directly → composition `783ff735`.
- Rows 14–22 ran at `783ff735`.
- Mid-task, the coordinator merged one more additive commit (`a828f4bf`, Fed-liquidity unit normalization, no migration change) → composition `42df4362`.
- Row 23 ran at `42df4362`.

**Between Run 2 and Run 3:** the coordinator merged `fable/godview` commit `37111a8b` (Annotated `Query` defaults, per-test Fed-pillar isolation, PIT-boundary fix, new pure/TestClient tests) on top of `42df4362` → composition `c9e34036`. Rows 24–27 (Run 3) ran there.

## Results whose tested SHA differs from the composition current at end of session (`c9e34036`)

- **Row 9, 10** (Run 1 real-API + browser acceptance) — tested at `6e8df878`, three compositions behind `c9e34036`. The Fed/commodity pillars and the availability-basis mechanism did not exist yet at that SHA.
- **Row 22** (Run 2 real-API God View routes) — tested at `783ff735`, two compositions behind `c9e34036`. At `783ff735` the CFTC router had the inferred-row-admits-by-default defect (row 18, failure #2); the `include_inferred=false` vs `=true` response difference captured there **reflects that now-fixed defect**, not `c9e34036`'s corrected behavior. This endpoint evidence was never re-captured against `42df4362` or `c9e34036` — no live server checks were run in Run 3 (DB-gated pytest only).
- **Row 3** (Run 1 `tests/godview`) — tested at `6e8df878`, before the Fed/commodity/availability-basis pillars existed at all; its 1 failure (`test_partial_refresh_cannot_expose_a_mixed_generation`) is the same test later confirmed passing at rows 18, 25 on `783ff735` and `c9e34036`.
- **Row 18** (Run 2 `tests/godview` at `783ff735`) — 3 of its failures were later confirmed fixed at row 25 (`c9e34036`); those 3 tests' pass/fail status differs by composition and must not be read as still-failing on the current tree.
- **Row 23** (`42df4362`) — see the correction above; its 6 failures were confirmed fixed at row 25 (`c9e34036`).

No database access was used to produce this table (the scratch DB is gone, per the coordinator). No source was edited.
