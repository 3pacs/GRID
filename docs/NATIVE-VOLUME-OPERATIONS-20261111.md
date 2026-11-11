# Native volume operational guards: source-only candidate

This follow-up starts at GRID f168c1834. It does not change the original staged
7ff8a664 bootstrap, the 14:40Z one-shot, production files, credentials, services,
alerts or schedules. The user performs any separately reviewed GRID/GEX apply.

## Fixed source gaps

1. Writer and health now share bounded stat-only disk inventory. It counts the
   SQLite store/journal, snapshot, checkpoint/recovery archives and receipts,
   using the larger logical/allocated size. Previously the writer and health
   counted only the store and receipts. Unknown directories, symlinks, hardlinks,
   FIFOs, foreign/group-accessible files and oversized inventories fail closed.
   Health uses a nonblocking shared lock on the existing writer lock to avoid
   the receipt link/unlink publication race. A valid active invocation has only
   its existing 45-second non-ready grace; a persistent busy lock, invalid
   custody, corrupt snapshot or low real capacity still alerts. No lock is created.
2. Both inspect actual filesystem available capacity. The candidate retains a
   64,000,000-byte residual free-space floor plus the existing 24,000,000-byte
   per-write allowance, and the existing 128,000,000-byte private-root budget.
   Low capacity stops before provider access and before a new checkpoint.
   Explicit first-store initialization may already have created the local empty
   store. No automatic deletion, pruning, compaction or history rewrite occurs.
3. New --incremental-only mode refuses initialization, missing/old-session
   checkpoints and any bootstrap plan. It retains the existing failure/unknown
   checkpoint, three-minute catch-up, no-retry and invocation-lock restrictions.
   It cannot silently become another bootstrap when invoked by a timer.
4. notification_condition converts the existing health report to fixed-text
   conditions for the existing GEX operations notifier. Unknown/future/stale or
   contradictory reports are bad. Pending/idle/disabled observations have no
   recovery transition, so they cannot clear an active incident. Provider data,
   private paths and arbitrary error text are never included in alert text.

## Cadence proposal, not installed or enabled

Two templates live under deploy/systemd/grid-native-volume.*.template.
They are default-disabled, run only incremental acquisitions at second 10 of
weekday minutes 09:00–16:59 New York time, and use the pinned exchange calendar
in the existing CLI for real session/holiday/early-close eligibility. No catch-up
replay is enabled (Persistent=false), no restart/retry is configured, and the
55-second supervisor timeout exceeds the existing 45-second CLI deadline.
There is still one lifetime invocation lease and one history GET at most per
eligible invocation. The second-10 offset exceeds the five-second settlement
buffer. Existing normal optional OAuth refresh remains under the shared lock.

The owner must resolve every @PLACEHOLDER@ to reviewed paths and pins, reuse the
existing GRID environment without copying credentials, and explicitly set
GRID_NATIVE_VOLUME_CADENCE_ENABLED=1 only AFTER an accepted current-session
bootstrap and alert-route integration. No activation or install command is
provided here. Do not add a second scheduler. On the next session the candidate
refuses automatic bootstrap: a separately reviewed initial-session acquisition
is still required until a retained-history-only session transition is designed
and accepted. Thus this is safe cadence preparation, not unattended-ready.

## Existing alert route integration, not applied

The existing GEX health Monitor._health engine owns confirmation/de-flapping,
incident persistence, deduplication, repeat budgets, recovery and Notifier.send.
The adapter must be called by that same owner process, not a second process
writing its state.json. For each fresh read-only health report:

```python
condition = notification_condition(report, now=current_utc)
if condition['bad'] is not None:
    monitor._health(current_utc.timestamp(), condition['key'], condition['bad'],
                    condition['message'], condition['message'])
```

The existing owner should obtain the report with the new health CLI, configure
its actual observed writer exit code, and treat command failure/unreadable JSON
as an invalid report. The normal polling/confirmation interval must detect a
persistent bad report without treating a 45-second publish grace as recovery.
The 65-second report-age bound is separate from 180-second producer freshness.
Do not enable monitoring for a staged one-shot as if it were an accepted cadence.
The adapter and fake interface tests never instantiate a real notifier or send.

Do not replace the running GEX health service or overlap its separate pending
scoreboard-health patch. Its owner must integrate/review this call against the
then-current serving source, run the existing alert regression suite, verify
one authorized test notification through the configured destination and obtain
the user's device acknowledgment. HTTP acceptance is not delivery proof.

## Acceptance remaining

- Actual bootstrap/current-session coverage and provider-scope reconciliation.
- Exact serving-source integration and full required CI.
- Existing alert owner integration and authorized device receipt.
- A separately accepted daily session-transition plan; the timer intentionally
  refuses bootstrap, including tomorrow's new-session transition.
- Explicit user-performed release and cadence/consumer activation.
