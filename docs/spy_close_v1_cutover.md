# `spy_close_v1` release hold and manual cutover contract

This document makes the separate AstroGrid runtime cutover visible in the selected GRID release PR. It is an **operator plan, not deploy automation**. PR #606 is draft. The owner has released the earlier #604 production pause and granted operational authority over the AstroGrid timer, scheduler, and services needed for the reviewed cutover with restoration preparedness. **The exact #606 candidate merge/deployment still needs a concrete release decision.** No merge, migration, unit change, provider pull, prediction, or score is performed by this document.

## Reviewed identities and policy

- Reviewed GRID PR snapshot before this authority update: head `e537f5f8e6818a12c7efb2bd6cc0865c33233719`, main `0d057b86aa7f3665df1fb7c5e724c3e84baa9f19`, exact-head CI [35822495442](https://github.com/3pacs/GRID/actions/runs/35822495442) green with 14 disposable-PostgreSQL cases. Recheck the actual PR head, then-current main, and CI at release-decision and merge time; the merge SHA will differ from the PR head.
- Clean AstroGrid API/scorer candidate: `bc096d863fbf7b1cd2281323f6e8175ce27f2c84`, based on committed live AstroGrid base `3ee27b1bbfa77b0e5af07f72c966c16be19c0466`. Preserve the dirty `/data/grid_v4/astrogrid_dedup` checkout unchanged.
- Accepted close identity: unadjusted `YF:SPY:close` → `spy_full`, only with a post-UTC-day policy marker and an atomic `astrogrid.price_close_receipt`. The marker does not claim Yahoo certified finality. A new single-SPY live prediction may anchor a receipt available at creation and at most four calendar days old. Swing/macro outcome targets are creation UTC date +7/+30 days, first verified close within four days. Missing/unverified outcomes are unscored. Historical, backdated, mixed-target, and unanchored SPY runs stay unscored.
- `YF:SPY:adj_close` still enters raw ingestion and maps to `spy_full`, but the new resolver deliberately excludes it from new `spy_full` resolution even when no valid unadjusted close exists. This protects the exclusive receipt vintage and can leave generic `spy_full` readers with an older observation. Do not restore adjusted close as a scoring fallback. Focused PostgreSQL tests in `tests/test_spy_binance_composition_pg.py` cover marked/unmarked close with adjusted close in both raw insertion orders. Existing resolved rows are not deleted or rewritten.

## Gates before any action

1. Obtain the **concrete #606 candidate merge/deployment decision** naming exact PR head, current main, selected merge mechanism, expected automatic deploy behavior, immutable AstroGrid candidate, additive migration, and restoration path. The former #604 pause has been released; do not treat that as approval of this candidate. Do not merge until this decision covers the resulting release effects.
2. For any later production window, set a hard server UTC cutoff. Check server UTC before every remote action/read. After cutoff, only service-state and release-pointer reads are permitted; retain the sanitized receipt and stop. Record exact prior release pointer, service unit/override state, PIDs and actual `/proc/<PID>/cwd`, database migration revision, and timer/oneshot state before changes. Save timestamped sanitized evidence durably before any cleanup or rollback. Keep secrets and raw payloads out of reports.
3. Stage the candidate in a **new immutable directory**, for example `/data/grid_v4/astrogrid_spy_close_v1.releases/bc096d863fbf7b1cd2281323f6e8175ce27f2c84`; verify `git rev-parse HEAD` equals the full candidate SHA. Reuse the existing `EnvironmentFile=/data/grid_v4/astrogrid_dedup/.env` by reference; do not copy secret contents. No production directory has been staged by this PR.

## Guard cutover under a separately authorized change window

1. Under the granted operational authority **after the candidate release decision**, stop `astrogrid-learning.timer` and verify `astrogrid-learning.service` is **inactive with `MainPID=0`**. Stopping the timer does not stop an already-running oneshot scorer. Prefer waiting for normal completion; if interruption is necessary, capture its state and restoration path before acting within the UTC cutoff. Leave the broad timer stopped through all mixed-version states. Learning, research, promotion, and weight changes remain held.
2. Merge/deploy the exact reviewed GRID tree only after the merge gate. Normal main deployment builds/swaps `/data/grid_v4/grid_release`, applies the additive `spy_close_receipt_20260922` migration (new empty receipt table/index), and restarts `grid-api` and `grid-hermes`. It **does not** move `astrogrid-api`, reload the long-lived scheduler, or manage the learning timer. Verify the resolved release pointer, deployed SHA, migration revision, `grid-api`/`grid-hermes` actual process cwd, and score route registration. A normal main push alone is not safe activation.
3. Cut over the separate AstroGrid API/scorer to the immutable candidate. The prepared systemd overrides are below; they are **not installed by this PR**. Keep the existing environment-file reference. Restart `astrogrid-api` only in the authorized window; do not start the learning timer. Verify effective `WorkingDirectory`/`ExecStart`, candidate SHA, actual API PID cwd, and score route on port 8010. Both callable scoring APIs must now use guarded code before any new anchored prediction can be exposed.

```ini
# /etc/systemd/system/astrogrid-api.service.d/spy-close-v1.conf
[Service]
WorkingDirectory=/data/grid_v4/astrogrid_spy_close_v1.releases/bc096d863fbf7b1cd2281323f6e8175ce27f2c84
```

The API's existing `ExecStart` stays `/usr/bin/python3 -m uvicorn astrogrid_api.main:app --host 0.0.0.0 --port 8010`.

```ini
# /etc/systemd/system/astrogrid-learning.service.d/spy-close-v1.conf
[Service]
WorkingDirectory=/data/grid_v4/astrogrid_spy_close_v1.releases/bc096d863fbf7b1cd2281323f6e8175ce27f2c84
ExecStart=
ExecStart=/usr/bin/flock -n /tmp/astrogrid-learning.lock /bin/bash /data/grid_v4/astrogrid_spy_close_v1.releases/bc096d863fbf7b1cd2281323f6e8175ce27f2c84/scripts/astrogrid_hourly_catchup.sh
```

The learning service still runs both scoring and backtests if later started; preparing its override is not permission to start the broad timer. Verify `astrogrid-learning.timer` remains stopped and the oneshot remains inactive. The bounded read-only acceptance runner in the operator packet checks exact tree SHAs, service state/PID cwd, both API score routes, migration revision, and receipt metadata; it does not make changes.

## Forward-only proof, separately gated

1. One owner-authorized public Binance BTCUSDT GET from `grid-svr` returned HTTP 200 on 2026-09-23 UTC with one completed 2026-09-22 UTC daily bar and one open 2026-09-23 bar. Do **not** repeat it. This proves momentary BTC endpoint reachability, not ETH access, producer execution, or data resolution.
2. After both guarded APIs and the timer/oneshot hold are verified, the granted scheduler authority permits its **explicit** interruption/activation route using `workflow_dispatch` with `activate_scheduler=true` and `acknowledge_scheduler_interruption=true`; keep `activate_realtime=false` and `activate_intelligence=false`. Verify its new PID cwd and exact release SHA. Ordinary forward pulls may write SPY/Binance raw rows, Binance 24-hour ticker rows, pull logs/watermark, resolved rows, and SPY receipts. A BTC success followed by ETH failure can leave BTC raw committed while the cycle logs `FAILED` and watermark stays unchanged; inspect both row lineage and cycle status. No backfill or remap.
3. First observe a fresh completed Binance BTC/ETH raw → canonical observation **and** fresh source-linked SPY receipt. Then request **separate approval** for one new eligible live single-SPY prediction and, later, for its natural-horizon score. Until approved, do not write either. Verify the prediction's creation-time entry receipt and pending/no-score status. After its natural +7/+30-day horizon and verified outcome, bound the separately approved scorer to that exact prediction ID with `limit=1`; verify one linked score. The natural horizon may remain pending beyond the release window. Do not score historical predictions, shorten the horizon, change weights, activate research, or restart the broad learning timer. Calendar-window exhaustion stays explicitly unscored.

## Recovery boundary

On a failed gate, stop before scheduler, prediction, or score. Preserve receipts and inspect service/pointer state under the UTC cutoff. Use the prepared restoration authority to restore the prior approved GRID release pointer and prior AstroGrid API unit target within the decision's recovery boundary; verify the resulting old runtime. The additive receipt table may remain in place. Keep the learning timer held: an old scorer ignores the new anchor contract and must not run against anchored predictions. Retain new raw/resolved/receipt rows as evidence; do not delete, rewrite, backfill, or rescore them. A scheduler rollback requires a verified retained release tree, not its prior deleted cwd.
