# Combined migration-graph proof — reproducible manifest (2026-09-18)

**Named ref (pushed):** `fable/combined-graph-proof-20260918-ref` → `b534b32b451931310c8a21523ba83ccbfe3151a7` on `3pacs/GRID`. Local branch `fable/combined-graph-proof-20260918` (worktree `GRID-fable-wt-combined`) points at the same commit. This is a proof artifact, not a release candidate and not a merge request.

## Composition (first-parent history of b534b32b)

| Commit | Kind | Second parent / content |
|---|---|---|
| `8be08c03` | start | release lead's re-rooted composition h = `integration/data-integrity-20260918h` (a2f71acf + packet-1/2 stack) |
| `094bf685` | `--no-ff` merge | `fable/integration-20260918-ref` = `c9e34036` (pass-3 Fable composition) |
| `44e0dfb1` | re-parent | one line in `migrations/versions/signal_evaluations_0918.py` (patch below) |
| `5da97e09` | merge | `fable/integration-v6-20260918` = `0f0451aa` |
| `d52319df` | merge | `fable/integration-v7-20260918` = `d92ca9fc` |
| `6c441518` | merge | `fable/integration-v8-20260918` = `d7ffa7f1` |
| `b534b32b` | merge | `fable/integration-v9-20260918` = `d9a960ab` (final Fable composition, pass 4) |

Constituent packet-2 heads present in h (verified `git cat-file -e` on origin): #534 `8e6d1729`, #535 `4094c50b`, #536 `721992fd`, #537 `cda5ca60`, #538 `a043c83a`, #539 `5e30f82e`, #540 `7ac4bc6f`, #541 `588b15c1`, #542 `9f667f16`, #544 `d7cc5756`, #545 `ce1a7f55`, #546 `ac2551bb`, #547 `351e9ca2`. Fable lane heads inside v9: #551 566089c0 · #552 047eb0e7 · #553 3de9c44e · #554 0a4c75ad · #555 f6702b59 · #556 f8ad5635 · #562 5c4e26ba · #563 b56ad1d4 · #564 5b566e19 (docs since: 55eeaf34, not in v9) · #565 c2fe08b3 · #566 2218e88d · #567 28b536df · #568 2b52ef9e · #570 38a9ca7d · #571 d0ecb908. Excluded by design: #569 (held policy flip).

## The re-parent patch (44e0dfb1, the only non-merge change)

```diff
--- a/migrations/versions/signal_evaluations_0918.py
+++ b/migrations/versions/signal_evaluations_0918.py
@@ -24,7 +24,7 @@
 revision: str = "signal_evaluations_0918"
-down_revision: Union[str, Sequence[str], None] = "god_view_market_tables_20260918"
+down_revision: Union[str, Sequence[str], None] = "signal_sources_trust_nodefault"
```

When packet 2 lands on `main`, this same one-line change (as a new commit on `fable/signal-eval-20260918`) is what sequences Fable's revisions after it. It must never be applied to a database that has already stamped `signal_evaluations_0918` under the old parent (pass-3 lesson).

## Resulting Alembic graph (single head; `tests/test_alembic_single_head.py` 3/3 at every merge)

`<base> → 7e4dfecce247 → … → snapshot_actor_col_20260914 → god_view_market_tables_20260918 → earnings_pred_move_basis_0918 → actors_provenance_20260917 → options_rec_scanner_score_0917 → journal_unscored_conf_0918 → oracle_pred_nullable_0918 → signal_sources_trust_nodefault → signal_evaluations_0918 → promotion_ledger_0918 → godview_pit_cftc_0918 → godview_avail_basis_0918 → godview_pit_fed_0918 → godview_pit_cmdty_0918 → godview_pit_finra_0918 → godview_pit_secftd_0918 → godview_pit_buyback_0918 → godview_pit_gex_0918 → research_leases_0918`

## Reproduction (exact commands, run 4, 2026-09-18 20:19–20:22Z; log `dbproof/run4/combined_from_empty_b534b32b.log`, script `combined_from_empty.sh`)

```bash
git -C /path/to/GRID fetch origin refs/heads/fable/combined-graph-proof-20260918-ref
git worktree add --detach ../GRID-combined FETCH_HEAD          # b534b32b
# disposable PostgreSQL 14: one uniquely named DB + role, SSH tunnel to 127.0.0.1:55432
export DB_HOST=127.0.0.1 DB_PORT=55432 DB_NAME=<db> DB_USER=<role> DB_PASSWORD=<pw> GRID_TEST_DB_URL=postgresql://<role>:<pw>@127.0.0.1:55432/<db>
python -c "import db; db.apply_schema()"                          # schema.sql baseline
python -m alembic stamp 7e4dfecce247
# PREPARED-DATABASE STEP (problem B, unchanged): the four runtime creators —
#   intelligence.hypothesis_engine.ensure_tables(engine); store.snapshots.ensure_analytical_snapshots_table(engine);
#   NewsScraperPuller._ensure_news_table(); market_briefings DDL from ollama/market_briefing.py:598-612
python -m alembic upgrade head                                    # 38 revisions → research_leases_0918 (head)
python -m alembic downgrade signal_sources_trust_nodefault        # 11 downgrades across the seam
python -m alembic upgrade head                                    # 11 upgrades back
GRID_ACCEPTANCE_TREE=1 python -m pytest tests/test_alembic_single_head.py tests/godview -q -rs
```

Results on 2026-09-18: `alembic current` = `research_leases_0918 (head)` after the upgrade; seam round-trip 11/11 both ways; `147 passed, 0 skipped`. Database and role dropped afterwards (receipt in `dbproof/TEARDOWN-RECEIPT.md`, run 4).

## What this proves / does not prove

Proves: packet 2 and Fable's revisions form one linear, applicable, reversible chain on the code as of these heads. Does not prove: a tracked-migrations-only fresh install (see `ci-fidelity/FRESH-INSTALL-RECONCILIATION.md`), release sequencing (the release lead's), or anything about production.
