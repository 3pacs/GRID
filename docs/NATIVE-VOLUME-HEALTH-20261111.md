# Native volume health and recovery readiness

Status: offline implementation only. No production writer, timer, monitoring
route, consumer, provider call, credential change, or live feed was enabled or
changed. The separately scheduled one-shot bootstrap was not modified or repeated.

## Read-only health contract

`python -m scripts.native_volume_health` is disabled by default and does not open
the configured store or calendar. Use `--enabled` only after the separate cadence
activation decision. The command reads the existing private root and pinned
calendar; it does not create a root, fetch data, refresh credentials, reconcile
receipts, send alerts, or change signals.

For an authorized operational integration:

```sh
python -m scripts.native_volume_health --enabled \
  --root /operator/configured/private-root \
  --calendar-manifest /operator/reviewed/calendar.json \
  --calendar-sha256 OPERATOR_PINNED_SHA256 \
  --last-exit-code LAST_OBSERVED_WRITER_EXIT_CODE
```

Use the actual observed exit code, not a guessed zero. Omission means invocation
exit status is not checked; snapshot/checkpoint evidence is still evaluated.
The JSON contains fixed reason codes and no raw errors, paths, credentials or
volume values. Exit codes:

- 0: healthy, disabled, or verified idle. Inspect `status` and `ready`; zero alone
  is not a readiness assertion.
- 1: blocked and needs the existing operations alert route.
- 2: pending or an observation race. Recheck; never label this healthy.

The same owner-only root, bounded regular files, no symlink/hardlink following,
and duplicate-key rejection protect file reads. Receipt inventory and sizes are
bounded. Calendar FIFO inputs fail immediately instead of blocking. Trusted
owner-controlled ancestors remain a deployment prerequisite.

During the verified session acquisition window, evaluation checks invocation
failure, checkpoint identity/completeness/freshness, snapshot hash and provenance,
calendar and exact baseline identity, source clocks, metrics availability, and
reserved disk capacity. Staleness thresholds are 180 seconds. Existing snapshot
validation still enforces its 120-second generation-time data-age bound. A
calendar failure is blocked, not treated as a closed exchange. Outside the
verified window, freshness demand is idle, not proof of producer health.

The writer commits its checkpoint before publishing its snapshot. A valid current
invocation gets at most 45 seconds from its original `started_at` for an absent
or older snapshot to be published. A valid prior-session snapshot can also be
pending during a new-session bootstrap, but is never ready. Missing/mismatched
output after that deadline alerts. Corrupt provenance, nonzero exit, custody or
disk errors are not hidden by publication grace. A double checkpoint read catches
other concurrent observations and reports unknown rather than success.

## Recovery drills

Synthetic full-session fixtures exercise both existing recovery paths:

1. Receipt captured, retention append fails, immutable receipt replay repairs the
   checkpoint and snapshot; repeated local republish inserts no duplicate bars.
2. Snapshot write fails after reconciliation, then local republish restores it.

Both drills retain the original checkpoint archives and make exactly one mock
history request. They do not use real provider data and do not authorize a new
fetch after uncertain acquisition. Missing or incomplete receipts remain blocked.

## Verification and remaining gates

- Final focused suite: 231 passed, 1 skipped, plus 47 subtests passed. It covers
  intervals, retention, private APIs, volume arithmetic, cache, calendar, health,
  one-shot worker, reconciliation, snapshot custody, and bounded Schwab transport.
- The skipped optional calendar export was separately exercised with the existing
  pinned calendar interpreter: all 19 calendar/manifest tests passed.
- All 40 health/reader/recovery tests passed; Ruff and `git diff --check` passed.
- Review corrected malformed provenance acceptance, false publication alerts,
  contradictory ready status, and new-session publication grace.
- A first broader run under system Python had one missing-FastAPI dependency
  failure. The existing local test interpreter with FastAPI ran the final focused
  suite successfully; no dependencies or shared environments were changed.

This is not full-repository CI, live source reconciliation, alert delivery proof,
or provider-chart acceptance. Before unattended activation, integrate and verify
one existing operations alert route, review cadence ownership and disk policy,
run the authorized bounded bootstrap, inspect actual current-session coverage,
and explicitly approve the separate private consumer/cadence release. Never
unlock legacy volume signals or alter quote/chain/rollover selection through this
health command. Historical or synthetic success does not establish trading edge.
