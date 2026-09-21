# packet 2a extraction — #547 null-safe readers + #544 oracle/confidence slice

Branch A (this doc): `fable/packet2a-20260921`, base `main` @ `bd990927124a6c8353667635e2e73156c50a0e87`.
Branch B (recovery target): `fable/packet2a-recovery-20260921`, branched from branch A's final head.

Development-only extraction. No production access, no SSH, no DB used by this
session (the coordinator runs the PostgreSQL-backed tests separately). Worked
entirely in worktree `GRID-fable-wt-packet2a`.

## Combined head

`9198bb2a6a2cda63a15476b8147367cf5f760b15` — "tests: pin the historical-write
hold; mark the b1d3dd8b-dependent tests xfail"

## Recovery head

`fcfff061e6154748e3dc9c67702ffd3763c7fb5a` — "recovery: revert
oracle/publish.py's writer to pre-#544 literal defaults"

## Commit provenance

### Step 1 — #547 (hotfix/oracle-null-readers), head `351e9ca22c2493b29637c07548bd78697ee54857`

0 conflicts vs main, as specified. Single commit on that branch.

| Original SHA | New SHA | Subject |
|---|---|---|
| `351e9ca22c2493b29637c07548bd78697ee54857` | `83dd0eedc2c8983f2a2c0bb48c8074b822650eb5` | hotfix(oracle readers): NULL-safe entry_price and confidence on origin/main |

### Step 2 — #544's slice, `19b17f04..d7cc5756` (14 commits, oldest first)

| Original SHA | New SHA | Subject | Notes |
|---|---|---|---|
| `19b17f04bede7d1d234bc3c4b355dbdb4f9a7a87` | `5cd254d4e6d1f493245f4281a509a503791b42ce` | Drop fabricated confidence from /chat/ask and the canvas graph | clean |
| `2d24f61b3f93aafb4cd352d2ec91b5de718af756` | `c5494a0b9a4387dd33a88dc3e6ae83e8155c5c69` | knowledge_tree.confidence and the oracle's three-way placeholder | 2 comment-only conflicts resolved (see below) |
| `1e107766f4b71c7156b423aaf2442dc054a06b5b` | `0149fbcedaf18edd49d417d7a18e78079a02d206` | AstroGrid: seer confidence, scorecard coverage, review confidence | clean auto-merge (4 files) |
| `b1d3dd8b7775cae0b95298dea88fc07d23a064d7` | — **NOT APPLIED** | intel.py: confidence labels become source_class naming the branch | **STOPPED — see Dependency stop below** |
| `719aa89a4100d448fd70605293c054e71b7870d9` | `b480900190510ed8f6bb507cfb72d6c97feb1c4c` | Document and enforce the confidence policy | clean |
| `17b130638c25c5019a39368f4370f8134f9ed87a` | `68b2624efc55941090ed5fdbac74779748fa4e12` | astrogrid_web: stop re-inventing the numbers the API stopped sending | clean |
| `6beea5d691852f3692e55a171cebd0bbc1524da1` | `3124e61b7f7a56ce0caa9b1c10baf6fd4d01f5f1` | DetailPanel: an unscored actor reads "unrated", not 0% | clean |
| `1d6beeaa5eb2ce3647657a670025eda9d681cf72` | `2065c09ded1ab2fd7cf6c5f39d56593282e83eff` | Tidy /canvas/dots inputs indentation | clean |
| `af65b71606b2a59514457f9fb184bdcf27fb1234` | `5c6c566c3be9e5868bb9bb17324f8661fe64ae5f` | test(confidence-policy): key the allowlist by source text, not line number | clean |
| `22a0b0a3ae8f2bd02a4f5a6565f207550dec25f9` | `554644a337d9c6da28f4c9db3db3a829a324e8fa` | An oracle entry price is the measured spot, or NULL (D-M32) | 1 comment-only conflict, 3 files (see below) |
| `ef51d14820a6523a4ad86137130291b6380e4c4a` | `ccb2b0aa852b6ebc109923ca78cfc9d89d20913a` | oracle_predictions: relax entry_price/confidence NOT NULL in alembic, not in the engine bootstrap | clean; adds the migration |
| `91dc028268ea35664c5f6af6f775471415b74b3e` | — **SKIPPED (true no-op)** | Port #547's oracle reader NULL-safety onto the release branch | confirmed no-op (see below) |
| `a5a19a58de3736d9808f805dc5cdf5cfe631c37d` | `41abfddd9192a4382664cbc57e3f5ae2a4e1b555` | An unscored confidence reads "unscored"; a zero entry price says why | 1 comment/whitespace conflict (see below); adds oracle/entry_price_policy.py |
| `d7cc575682aa0c506ebb6ff2b47aee98cceec22d` | `b0a9c01398c39093353591c000d1b62d227fe84c` | test(oracle): a row closed for an invalid entry price counts nowhere | clean |

All applied commits carry `-x` provenance (`(cherry picked from commit
<original SHA>)` in the message body).

### Extraction-only commits (this session, not cherry-picks)

| SHA | Subject |
|---|---|
| `96f573e51b2e6f71995db593e2dcf29c334a83d5` | migrations: re-parent oracle_pred_nullable_0918 onto capital_flow_ttm_state_20260920 (Step 3) |
| `830d78028387c60db9e5fe1eebf85a58752e6979` | oracle: enforce the historical-write hold on entry_price/confidence in code (Step 4) |
| `9198bb2a6a2cda63a15476b8147367cf5f760b15` | tests: pin the historical-write hold; mark the b1d3dd8b-dependent tests xfail (Step 5) |

### Recovery branch

| SHA | Subject |
|---|---|
| `fcfff061e6154748e3dc9c67702ffd3763c7fb5a` | recovery: revert oracle/publish.py's writer to pre-#544 literal defaults (Step 6) |

## Dependency stop: commit `b1d3dd8b` NOT cherry-picked

`b1d3dd8b7775cae0b95298dea88fc07d23a064d7` ("intel.py: confidence labels
become source_class naming the branch") only touches
`api/routers/intel.py`, but that version of the file imports:

```python
from intelligence.actors.provenance import actor_source, source_as_of
```

`intelligence/actors/provenance.py` does not exist anywhere in this
extraction's history — `git log -S"def actor_source"` finds nothing
reachable. Tracing the import forward on the #544 branch (`d7cc5756`)
finds it added by commit `e37bdf18d48c5c8a8fe6255bca4b99608acf12bb`
("Label seeded actors and the institutional map as curated, not
observed"), which `git merge-base --is-ancestor e37bdf18 19b17f04` confirms
is an **ancestor of this slice's own starting commit** (`19b17f04`) — i.e.
it belongs to the #544 PR's lower stack (#539/#540/#541/#542), which is
explicitly out of scope for this extraction.

Per instructions, this commit was **stopped, not applied**, and the stack
was not expanded to pull in `e37bdf18` or its dependents.

- **File:** `api/routers/intel.py`
- **Missing symbols:** `actor_source`, `source_as_of`
- **Originating PR:** #539–#542 (lower stack), specifically introduced by
  `e37bdf18d48c5c8a8fe6255bca4b99608acf12bb`

**Consequence:** `api/routers/intel.py` on branch A still has the
pre-#544 `"confidence": "confirmed"` / `"confidence": r[6] or "derived"`
literals this commit would have renamed to `source_class`. Two tests in
`tests/test_confidence_policy.py::TestSourceClassLabels`
(`test_intel_has_no_per_branch_confidence_literal`,
`test_intel_label_is_never_a_numeric_threshold`) assert the *post*-b1d3dd8b
shape and were marked `@pytest.mark.xfail(strict=True, reason=...)` rather
than left red or silently deleted. The commit's own message notes "No PWA
view consumes these endpoints" (IntelSubmit/IntelModeration read a
different router), so this has no live UI impact on branch A.

**Coordinator decision needed:** whether to pull `e37bdf18` (and whatever
else from #539–#542 it needs) into a follow-up packet, at which point
`b1d3dd8b` can be cherry-picked cleanly and the two xfails removed.

## Conflict resolutions

All conflicts across the whole extraction were comment/prose-only or a
single blank-comment-line diff — never a logic conflict. Each was resolved
by keeping the more complete/accurate wording (usually the incoming
commit's, which cited the D-M32/D-H11/A-M11 audit tags) with nothing
functionally changed on either side:

1. **`2d24f61b` → `c5494a0b`** (2 files):
   - `api/routers/oracle.py` — comment above `ORDER BY confidence DESC NULLS LAST`
     in `get_latest`: kept the longer explanation (Postgres sorts NULLs FIRST
     under DESC).
   - `oracle/calibration.py` — comment above `AND confidence IS NOT NULL` in
     `compute_calibration`: merged both sides (kept the `(D-H11)` tag and the
     `float(r[0])`/TypeError detail).
2. **`22a0b0a3` → `554644a3`** (3 files, one hunk each, all comment-only):
   `api/routers/oracle.py` (tracking-P&L comment), `pwa/src/views/Predictions.jsx`
   (entry-price JSX comment), `scripts/score_oracle_trades.py` (no_data sweep
   comment) — kept the `22a0b0a3` (incoming, D-M32-tagged) wording in all three.
3. **`91dc0282`** — `tests/test_oracle_null_readers.py` add/add conflict:
   resolved with `git checkout --ours` (Step-1's version). Confirmed a true
   no-op: `git status` reported "nothing to commit" after resolution, and
   every other file in the commit (`oracle/engine.py`,
   `features/regime_conditional_brier.py`, etc.) auto-merged with zero
   diff against what Step 1 had already applied. Skipped with
   `git cherry-pick --skip`, HEAD unchanged.
4. **`a5a19a58` → `41abfddd`** (1 file): `api/routers/oracle.py` — a
   blank-vs-comment-line diff immediately above the tracking-P&L comment
   block in `get_predictions`; resolved to the incoming (non-blank
   `#`-only) form. The `entry_price_pnl_basis`/`PNL_BASIS_MEASURED` import
   this commit adds merged in cleanly on its own (no conflict).

## Migration re-parent (Step 3)

`migrations/versions/oracle_pred_nullable_0918.py`: changed only the
`down_revision` line from `"journal_unscored_conf_0918"` (a #539 revision
not on this branch) to `"capital_flow_ttm_state_20260920"` (this repo's
live migration head). No other line in that file, no other migration file,
`schema.sql`, `requirements*.txt`, workflow file, or `config.py` was
touched. `oracle_predictions` is bootstrapped by
`oracle/engine.py::OracleEngine._ensure_tables` (`CREATE TABLE IF NOT
EXISTS`, already nullable) and is not represented in `schema.sql` at all —
confirmed by the migration's own docstring and by
`tests/test_oracle_predictions_schema_parity.py` not referencing
`schema.sql` — so no `schema.sql` edit was needed or made.

Verified:

```
$ DB_PASSWORD=x DB_USER=x DB_NAME=x DB_HOST=127.0.0.1 PYTHONUTF8=1 python -m alembic heads
oracle_pred_nullable_0918 (head)
```

Single head, as required.

## Historical-write hold — implementation (Step 4)

**Two scoring paths, both enforced.** `scripts/score_oracle_trades.py` is a
**manual CLI** — not invoked by any systemd unit, timer, cron entry, or by
Hermes (confirmed: no reference in the release tree's `hermes_operator.py`,
no journal lines in 24h on grid-svr). The **automatic** prediction →
outcome → score path Hermes actually runs is
`oracle/engine.py::OracleEngine.score_expired_predictions`, called directly
from Hermes's oracle step (`hermes_operator.py:3964`: `from oracle.engine
import OracleEngine`). The hold had to be — and is — enforced in **both**.

The slice's own close-out sweep (`oracle/engine.py::score_expired_predictions`,
comment "Rows closed as 'no_data' because their entry price was NULL, zero
or negative") and the scorer script's equivalent sweep both closed **any**
unusable `entry_price` (`NULL`, `0`, or negative) to `'no_data'`. A non-null
`0`/negative `entry_price` can only be a **legacy** row — the column was
`NOT NULL` before this migration, so the only way to get an invalid,
non-null value pre-migration was the old `0.0` default. The new publish
path writes `NULL`, never `0`/negative, when nothing is measured. Closing a
legacy row is historical rescoring/repair, which is **held**.

Fixed in commit `830d7802`:

1. **`oracle/engine.py::score_expired_predictions`** — the close-out branch
   now checks `entry is None` before writing. `NULL` → closed to `no_data`
   with `SCORE_NOTE_ENTRY_NULL` (unchanged). Non-null invalid (`0`/negative)
   → left exactly as it is (no UPDATE at all), counted in a new
   `held_legacy_entry_price` result key instead of `unscorable_entry_price`.
2. **`scripts/score_oracle_trades.py` Step 2 ("Backfill entry prices")** —
   previously wrote a historical price (fetched just then) into any row
   with a missing/zero/negative `entry_price`. That is repair of an
   already-published row for a legacy 0.0 row, and fabrication of a price
   nothing measured at publish time for a NULL row — both held. The step
   now only counts (`held_legacy`, `held_new_policy`); it issues zero
   `UPDATE`s.
3. **`scripts/score_oracle_trades.py`'s no_data sweep** — `WHERE` narrowed
   from `entry_price IS NULL OR entry_price <= 0` to `entry_price IS NULL`
   only; the three-way `CASE` (null/zero/negative) collapsed to the single
   `SCORE_NOTE_ENTRY_NULL` reason, since the zero/negative branch is no
   longer reachable by design. `SCORE_NOTE_ENTRY_ZERO`/`_NEGATIVE` imports
   removed from that file (still defined in `oracle/entry_price_policy.py`
   for anything else that needs them).

### Every UPDATE/DELETE reviewed against `oracle_predictions`/scoring tables

Scope: every file touched by the applied slice commits
(`git diff bd990927..9198bb2a --name-only`, filtered to `oracle/`,
`intelligence/`, `scripts/`).

| File | Statement(s) | Disposition |
|---|---|---|
| `oracle/engine.py::score_expired_predictions` | `UPDATE oracle_predictions` (close-out), `UPDATE oracle_predictions` (score), `UPDATE oracle_models` | **Gated** (see above). The scoring/`oracle_models` UPDATEs only run after the entry-price guard `continue`s for a held row, so they can no longer reach a legacy row either. |
| `scripts/score_oracle_trades.py` Step 2 (backfill) | `UPDATE oracle_predictions SET entry_price = :price` | **Gated** — removed entirely; the step is now read-only. |
| `scripts/score_oracle_trades.py` no_data sweep | `UPDATE oracle_predictions` | **Gated** — narrowed to `entry_price IS NULL`. |
| `scripts/score_oracle_trades.py::score_one_chunk` | `UPDATE oracle_predictions` (entry-note close, price-missing close, score), `UPDATE oracle_models` | **Reviewed, no change needed.** The chunk's own `SELECT` already filters `entry_price IS NOT NULL AND entry_price > 0`; a legacy or NULL row can never reach this function's rows. The `entry_price_score_note` check inside it is unreachable defensive code, left as-is. |
| `oracle/publish.py` | `INSERT ... ON CONFLICT ... DO UPDATE SET confidence = CASE ...` (dedup upsert) | **Out of scope.** Only conflicts against a row from the *same* publish cycle/day (`ticker, direction, expiry, prediction_type, model_version, created_at::date` with `dedup_keep = TRUE`) — never a historical row from before this deploy. |
| `intelligence/postmortem.py::_write_edge_update` / `_write_edge_confirmation` | `UPDATE supply_chain_edges` | **Out of scope.** Different table (backtest validation of supply-chain graph edges), unrelated to `entry_price`/`confidence`. The slice's only change to this file was reader-side (NULL-safe postmortem narration). |
| `intelligence/hypothesis_engine.py` | `UPDATE discovered_hypotheses`, `UPDATE hypothesis_boost_log`, `DELETE FROM discovered_hypotheses`, `DELETE FROM hypothesis_registry` | **Out of scope.** Different tables (hypothesis-engine housekeeping), pre-existing and unrelated. The slice's only change here was a reader-side NULL-safe confidence string in `AnomalyHunter`. |
| `intelligence/rag.py` | `DELETE FROM intelligence_embeddings` (×3) | **Out of scope.** Pre-existing re-index housekeeping, unrelated table. The slice's change here was reader-side (RAG chunk text omits an unstated confidence). |
| `scripts/backfill_surfacer_calibration.py` | `UPDATE surfacer_data_requirements`, `ON CONFLICT ... DO UPDATE` (ticker/signal calibration) | **Out of scope.** Writes to materialized calibration tables computed via `AVG()` (which already skips NULL), not to `oracle_predictions` itself. The slice's change here was query-side (stopped imputing a missing confidence at 0.5 inside the `AVG(POWER(...))` Brier/ECE expressions). |
| `oracle/calibration.py`, `oracle/engine.py` (score branch) | `UPDATE oracle_models` (running Brier/ECE/hit-rate counters) | **Out of scope directly; protected indirectly.** Model-level aggregates, never row-level prediction data. Only reachable from the now-gated scoring code paths, so a legacy/NULL row can no longer feed them either. |

No other `UPDATE`/`DELETE` against `oracle_predictions` or a scoring table
was found in the slice's touched files.

## Tests (Step 5)

Green (`DB_PASSWORD=x PYTHONUTF8=1 python -m pytest ... -q`):

```
tests/test_confidence_policy.py tests/test_oracle_null_readers.py \
tests/test_oracle_predictions_schema_parity.py \
tests/test_oracle_publish_entry_price.py tests/test_knowledge.py \
tests/test_hermes_timeout_budgets.py
```
→ 119 passed, 2 xfailed (the b1d3dd8b-dependent pair, see above).

Plus, discovered by grepping `tests/` for `score_oracle_trades`,
`oracle.publish`, `entry_price_policy`, `score_expired_predictions`:

```
tests/test_astrogrid_predictions.py tests/test_firewall.py \
tests/test_oracle_dedup_write_guard.py tests/test_oracle_engine_spot_price_basis.py \
tests/test_oracle_horizon_days.py tests/test_publish_astrogrid_canonical.py \
tests/test_publisher_gate.py tests/test_sanity_checker.py \
tests/test_score_oracle_trades_auto_adjust.py tests/test_score_oracle_trades_db_config.py \
tests/test_score_oracle_trades_success_lesson.py
```
→ 166 passed.

Test-file changes on branch A:

- `tests/test_oracle_null_readers.py` — `test_no_data_sweep_names_null_explicitly`
  rewritten for the new sweep shape (`WHERE entry_price IS NULL` only, no
  `CASE`). `TestEngineScoringLoopEntryPrice` — renamed/rewrote
  `test_unusable_entries_are_closed_with_their_own_reasons` →
  `test_unusable_entries_are_closed_or_held_by_the_hold_policy` (NULL closed,
  zero/negative held, asserting the new `held_legacy_entry_price` counter);
  `test_a_zero_entry_never_reaches_a_division` updated for the same held
  outcome instead of a closed one.
- `tests/test_oracle_publish_entry_price.py` —
  `test_a_null_entry_is_swept_to_no_data_with_a_reason` updated to assert
  the narrowed sweep shape (`AND entry_price IS NULL`, no `<= 0` variant).
- `tests/test_confidence_policy.py` — the two `b1d3dd8b`-dependent tests
  marked `xfail(strict=True)` (see Dependency stop above).

### New PostgreSQL tests

`tests/test_oracle_null_policy_pg.py` (new), using the `pg_engine` fixture
pattern from `tests/test_capital_flow_rollups_pg.py` (skips cleanly when no
Postgres is reachable / `GRID_TEST_DB_URL` unset — confirmed: 5 skipped in
this sandbox, which has no local Postgres). Proves, against a real
PostgreSQL instance:

1. `test_migration_allows_null_entry_price_and_confidence_after_upgrade` —
   after the migration, a prediction row inserts with `entry_price IS NULL`
   and `confidence IS NULL`.
2. `test_547_readers_handle_null_rows_honestly` — `oracle/calibration.py::
   compute_calibration` excludes a NULL-confidence row (`total_predictions
   == 0` for a ticker whose only scored row has NULL confidence) rather
   than imputing 0.5; `api/routers/oracle.py::get_predictions`/`get_latest`
   never raise and return honest `None`/named `tracking_pnl_basis` for a
   NULL-entry pending row.
3. `test_preservation_legacy_rows_survive_a_real_scorer_run` — the hold on
   the **manual** path. Seeds two legacy pending rows (`entry_price=0.0`,
   non-null confidence) and one new-policy row (`entry_price IS NULL`),
   runs the real `scripts/score_oracle_trades.py::main()` (network calls
   stubbed via monkeypatching `fetch_prices`; `create_engine` monkeypatched
   to the test engine) and asserts the two legacy rows come back
   **byte-identical** (`verdict`, `entry_price`, `confidence`,
   `actual_price`, `actual_move_pct`, `pnl_pct`, `scored_at`, `score_notes`
   all compared as one dict-equality — this table has no `updated_at`
   column, so `scored_at` stands in as the "last touched" column) while the
   NULL row is closed to `no_data` with `SCORE_NOTE_ENTRY_NULL`.
4. `test_preservation_legacy_rows_survive_engine_score_expired_predictions`
   — the same hold, proved on the **automatic** path (the one Hermes
   actually calls): `OracleEngine.score_expired_predictions()` against a
   stubbed price lookup (`__init__` bypassed via `object.__new__`, mirroring
   `tests/test_oracle_null_readers.py::TestEngineScoringLoopEntryPrice`'s
   own helper — `__init__` runs CREATE TABLE/INDEX statements and loads the
   model registry, unrelated to this test). Same two legacy rows
   byte-identical, same new-policy row closed with the same reason.
5. `test_migration_downgrade_with_null_rows_present` — calls the real
   migration's `downgrade()` (via `alembic.operations.Operations` bound to
   a `MigrationContext`) with a NULL row present, asserts it does **not**
   raise and leaves both columns nullable (`information_schema.columns.
   is_nullable = 'YES'`) — entirely inside one connection's transaction
   that is **always rolled back**, so this test can never permanently
   alter the shared database's real schema regardless of what other data
   exists there.

Runs against the whole shared `oracle_predictions` table where the real
top-level functions require it (tests 3/4, following the same precedent as
`test_capital_flow_rollups_pg.py::compute_ttm` calls), scoped to this
file's own unique `pkt2a_pg_<uuid>` ids / `ZPKT2A<n>` tickers for every
assertion; the autouse `cleanup_test_rows` fixture deletes this
file's own rows by id afterward.

## Coordinator PostgreSQL proof — round 1 findings and fixes

The coordinator ran `tests/test_oracle_null_policy_pg.py` against a
disposable PostgreSQL 14 database (`oracle_predictions`/`oracle_models`
bootstrapped with main's `OracleEngine._ensure_tables` DDL, then this
branch's migration applied — nullability NO→YES confirmed; the downgrade
test passed as written). Found two test-harness defects and one scope gap,
plus escalated one of the harness defects into a real production bug fixed
in the same round.

**1. Dedup-key collisions in the seed helper (harness defect).**
`test_547_readers_handle_null_rows_honestly` and
`test_preservation_legacy_rows_survive_a_real_scorer_run` failed with
`UniqueViolation: duplicate key value violates unique constraint
"oracle_predictions_dedup_unique"` — a real partial unique index (from the
engine bootstrap DDL: `(ticker, direction, expiry, prediction_type,
COALESCE(model_version,''), (created_at AT TIME ZONE 'UTC')::date) WHERE
dedup_keep`) that this extraction does not touch or weaken. Multiple seeded
rows shared a ticker, direction, expiry and creation day. Fixed by varying
`direction` (test 2's two rows) or `expiry` (tests 3/4's three rows) per
seeded row, keeping the same semantics (two legacy `entry_price=0.0` rows,
one new-policy NULL row). Commit `128d413a`.

**2. `tests/test_oracle_null_readers.py` failed collection on the gridz4
proof host** with `ModuleNotFoundError: No module named
'oracle.entry_price_policy'`, although `python -c "import
oracle.entry_price_policy"` succeeded with the same `PYTHONPATH`. Root
cause: `scripts/score_oracle_trades.py` does `sys.path.insert(0,
"/data/grid_v4/grid_repo")` at import time; that directory is real on the
gridz4 host and its own `oracle` package predates
`oracle/entry_price_policy.py`. `tests/test_oracle_null_readers.py`
imports `scripts.score_oracle_trades` before `oracle.entry_price_policy`,
so the stale tree's `oracle` package shadowed the real one.
  - **Harness fix** (commit `128d413a`): both
    `tests/test_oracle_null_readers.py` and
    `tests/test_oracle_null_policy_pg.py` now import `oracle.engine` and
    `oracle.entry_price_policy` eagerly, at module top, before
    `scripts.score_oracle_trades` — caching the real `oracle` package in
    `sys.modules` first means nothing added to `sys.path` afterward can
    shadow it, in this file or anywhere else in the process.
  - **Escalation — this is a production bug, not just a proof-host
    artefact** (coordinator, read-only check): `/data/grid_v4/grid_repo`
    exists on **grid-svr** too — a stale checkout at revision `5facbdf0`
    with no `oracle/entry_price_policy.py`. Hermes runs from
    `/data/grid_v4/grid_release`. `scripts/score_oracle_trades.py`'s own
    top-level import order put the `sys.path.insert` **before** its own
    `from oracle.entry_price_policy import ...` — so the very first time
    anything in a process imports that module (before `oracle` is
    otherwise cached), the stale tree can shadow the real package for
    that process, exactly as reproduced on gridz4.
  - **Production fix** (commit `fbce2862`): reordered
    `scripts/score_oracle_trades.py` so its own `from
    oracle.entry_price_policy import ...` runs **before** the
    `sys.path.insert`. The insert itself and its target string are
    unchanged — this is an import-order fix only. New regression test
    `tests/test_score_oracle_trades_stale_repo_shadow.py`: exercises the
    real, unmodified `sys.path.insert("/data/grid_v4/grid_repo")` line,
    redirecting *only* that literal target (via a `sys.path` list
    subclass) to a harmless `tmp_path` built to look exactly like the
    stale checkout — never touches the real absolute path on disk.
    Verified locally to fail against the pre-fix import order
    (`ModuleNotFoundError`, as expected) and pass against the fix, before
    landing.
  - **Confirmed on `scripts/score_oracle_trades.py` only**: `scripts/score_oracle_trades.py` is a
    manual CLI — not invoked by any systemd unit, timer, cron entry, or
    Hermes (no reference in the release tree's `hermes_operator.py`, no
    journal lines in 24h). Three other scripts carry the identical
    `sys.path.insert(0, "/data/grid_v4/grid_repo")` (confirmed via `git
    grep` on `main`): `scripts/paper_trading_review.py`,
    `scripts/run_forensics_batch.py`,
    `scripts/run_intelligence_cycles.py`. **Not touched** — out of scope
    for this extraction. **Follow-up for the coordinator**: the same
    import-order class of bug may exist in any of these three if they
    import an `oracle.*`/other repo submodule after their own insert;
    worth the same audit.

**3. Scope gap: the automatic scoring path wasn't proved.**
`scripts/score_oracle_trades.py` (test 3) is the manual CLI; the automatic
prediction → outcome → score cycle Hermes actually runs is
`oracle/engine.py::OracleEngine.score_expired_predictions`
(`hermes_operator.py:3964`). Added
`test_preservation_legacy_rows_survive_engine_score_expired_predictions`
(test 4) to prove the hold on that path too. Commit `128d413a`. See
"Historical-write hold — implementation (Step 4)" above, now corrected to
name both paths.

All fixes mirrored onto `fable/packet2a-recovery-20260921` (cherry-picked
cleanly — none of these files were touched by that branch's writer-revert
commit).

## Recovery tree (Step 6)

`fable/packet2a-recovery-20260921`, one commit (`fcfff061`) on top of the
combined head (`9198bb2a`).

**Reverts:** `oracle/publish.py::publish_astrogrid_prediction`'s write
values, back to the pre-#544 (`origin/main`) literals —
`entry_price=0.0`; `confidence`/`signal_strength`/`coherence` =
`payload.get("confidence") or 0.5`; the dedup upsert's `confidence =
GREATEST(...)` without NULL-branching. `_measured_entry_price`,
`_measured_or_none`, `_as_of_date` removed (they existed only to serve this
write path).

**Keeps:** the #547 readers, the Step-4 historical-write hold, the
migration (schema stays nullable — this tree does **not** attempt to
tighten the columns back to `NOT NULL`), `oracle/entry_price_policy.py`
(used only by readers/the scorer, nothing to revert there), and every
reader/hold/schema test.

**Test adjustment (skipped, not deleted):**
`tests/test_oracle_publish_entry_price.py::TestMeasuredEntryPrice` and
`::TestPublishedRow` (class-level `pytest.mark.skip`, reason names the
branch), plus `tests/test_confidence_policy.py::TestOraclePublish::
test_missing_metrics_become_null_not_a_midpoint` and
`::test_the_three_metrics_read_three_different_keys` (per-test skip) — all
pin the reverted write-path contract. Verified: 151 passed / 11 skipped / 2
xfailed on the core suites, 158 passed / 4 skipped on the rest, all on the
recovery branch.

**Why not revert the whole batch:** reverting all 15 packet2a commits
instead of just the writer would also remove the #547 readers and the
Step-4 hold. Any NULL `entry_price`/`confidence` row already published
before the back-out is deployed — or written by a still-running #544-era
publisher process during the rollout window — would then hit code that
assumes those columns are never NULL: a `TypeError` on the arithmetic in
`oracle/engine.py::score_expired_predictions`, a fabricated 0%/0.5 in
`oracle/calibration.py` and the scorers, a crash in the `/oracle/predictions`
tracking-P&L divide. The recovery tree is deliberately narrower — it stops
new NULL rows from being written without reintroducing that crash/
fabrication risk against rows that already exist.

## For the coordinator: PostgreSQL test commands

```bash
export GRID_TEST_DB_URL=postgresql://<user>:<pass>@<host>:<port>/<disposable_db>
DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_oracle_null_policy_pg.py -v
```

Also re-run the full non-PG suite listed under Tests above on both branches
if re-verifying this handoff.
