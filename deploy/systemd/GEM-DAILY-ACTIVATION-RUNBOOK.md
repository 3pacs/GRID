# GEM daily capture: held activation packet

Owner authorization: conditional daily capture, no earlier than 2026-10-01 UTC.
Installing or enabling `grid-gem-daily.timer` **is activation**. These files are
templates only; this PR must not install units, create `ACTIVATED`, or call a
provider. There is no retry or backfill path.

## Scope and prerequisites

- Eight tickers: OPCH, SPGI, FLUT, GEHC, GLND, SPY, QQQ, IWM. BHRB is excluded.
- The 2026-09-29 GEM SPY batch was overwritten by the old scheduler, so direct
  GEX acceptance of *that batch* is **UNOBSERVED**. A later replacement batch
  being accepted does not satisfy this condition. The Sep29 original cannot
  now be checked directly; hold activation until the owner explicitly resolves
  that missing prerequisite with a new, identified SPY-batch acceptance
  criterion. Any accepted batch must be session-dated with an
  `astrogrid.price_close_receipt` spot age 1–4 days. Do not infer a pass from
  the old scheduler's replacement batch.
- Wednesday's scheduler restart must be independently verified stable and
  running the append-only writer. Hermes must also run this writer. There must
  be no old scheduler, Hermes, legacy direct-insert, or pinned GEM process that
  can delete `options_snapshots` ticker/day rows. The new pinned GEM checkout
  must have #653 (`01b19194`) as an ancestor, current migration applied, and
  unit WorkingDirectory/ExecStart bound to its exact SHA. Old `77c50c3c` is
  **not** a valid ongoing-capture pin because its writer deletes rows.
- The `options_append_only_batches_20260930` migration needs its own explicit
  production approval, then backup, dry-run, execute, verify. Check the live
  existing unique constraint/index names and lock/size before running it. Its
  downgrade intentionally refuses to discard captured batches.
- Keep paid APIs and live trading off. Prove that other options writers and GEM
  use the same `/run/lock/grid-options-pull.lock` lease. A stuck Hermes worker
  keeps the lease until its actual pull exits. No manual service start.

## Reviewed activation sequence, for a later authorized window

1. Back up current unit/drop-in bytes, migration state, scheduler/Hermes/GEM
   identities, timer state and the `ACTIVATED`/attempt-marker state. Verify
   `grid-gem-daily.timer` is not installed/active and the old one-run service
   remains contained.
2. Render `@PIN_SHA@` and `@DB_ENV_FILE@` from verified runtime paths into a
   private staging directory. Never print environment values. `systemd-analyze
   verify` the rendered service/timer. Run a no-provider dry-run of the date,
   journal, lock, schema, code-SHA and GEX prerequisites.
3. Verify exact one post-13:29 `market_open=True` scheduler daily-pull start
   and one subsequent `Options daily pull complete` for the intended day. If
   missing, duplicate, failed or late, skip that day. The timer itself fires
   at 14:05 UTC on weekdays, with `Persistent=false`; NYSE holiday and NY
   market-hour checks run before any provider call.
4. Only after all prerequisites, install service/timer and create the reviewed
   `ACTIVATED` marker under `/data/grid_v4/gem_daily`. Then enable/start the
   timer once. Do not enable the service. This step is production activation;
   verify exact unit bytes, active timer, next UTC trigger and marker.
5. On each invocation the date claim is atomic and single use. If a gate or
   shared writer lease fails, it skips without retry. The puller has #653's
   session/quote checks and a six-expiry cap. Every ticker is validated by
   exact `capture_batch_id` and `capture_ordinal`, row count, calls/puts,
   quote/session times. The service hard timeout is 15 minutes; systemd kills
   its control group and `ExecStopPost` records containment. An absolute
   14:20 UTC alarm also interrupts a delayed start. Review the journal and attempt receipt after every
   session; a failed ticker is unavailable, never a zero SUCCESS. A service
   exit 0 with `GEM_SKIP` is a skipped capture, not a successful capture.
6. After ten actual NYSE sessions, write an outcomes/reliability note to
   `C:/Users/owner/Documents/Codex/2026-09-14/wha/outputs`. Count skips,
   partials, per-ticker validated rows, GEX acceptance, containment and
   append-only table growth. Do not prune retained batches without a separate
   reviewed retention decision.

## Rollback boundary

Disabling/stopping the timer and removing `ACTIVATED` prevents future starts.
Preserve all captured batches, day receipts, lock file, and prior unit bytes.
Do not roll back the schema by deleting captured data. Investigate any hang,
writer overlap, or missing validation before a later authorized activation.
