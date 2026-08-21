# Routine Orchestrator Handoff

Newest at top. Older-than-14-day entries are trimmed unless tagged `[KEEP]`.

## 2026-08-21 23:10 UTC — 2026-08-21-2310

**Why this matters next run:** The PUNCH-LIST lags implementation by weeks. Before picking any item, `grep -l` for the target symbols/test files — you may find the fix already landed and just never got the `[ ]->[x]` doc flip.

Historical pattern: PRs #344/#361/#377/#379/#381/#385/#387 all explicitly deferred the doc-flip to "a later reconciliation sweep" that rarely happens. PR #360 was the last bulk reconciliation (2026-07-01). PR #389 (this run) reconciled two more contracts/ items but plenty of already-shipped items remain marked `[ ]`. Candidates worth spot-checking before picking as fresh work:

- L106 conviction_scorer.py `as_of_date` param — check `alpha_research/conviction_scorer.py` signatures
- L112 split_adjuster.py tests — `tests/test_split_adjuster.py` exists per PR #339
- L116 rotation_variant logging — resolved by PR #358
- L117 signal_adapter dangling TODO doc ref — resolved by PR #387 (still open)
- L118 PositionState `object.__setattr__` — resolved by PR #385 (still open)
- L126 transfer_entropy drift — resolved by PR #340
- L148 ephemeris solvers tests — resolved by PR #352
- L150 lead_lag_backtest docstring — resolved by PR #355
- L173 download_pm_data shell — resolved by PR #377 (still open)
- L174 age_fast_sync Cypher escape — resolved by PR #379 (still open)
- Line 57 f-string SQL in prediction_backtest — resolved by PR #381 (still open)

A single "bulk reconciliation" PR after PRs #377/#379/#381/#385/#387 land would clean this up in one shot. Not urgent, but worth noting.

Everything else this run captured is in `.grid_backups/routine_log.jsonl` line 67 and PR #389.
