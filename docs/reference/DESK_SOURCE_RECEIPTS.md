# Desk source receipts

Module: `intelligence/desk_source_receipts.py`. Tests: `tests/test_desk_source_receipts.py`.
Job: `scripts/desk_receipts_run.sh`, `deploy/systemd/grid-desk-receipts.{service,timer}.template`
(tests: `tests/test_desk_receipts_run.py`). Part of 3pacs/GRID#1164.

**Running on grid-svr since 2026-11-19**, on the owner's word, before review: a `grid` user
timer, weekdays 16:20 and 19:20 New York, from a checkout pinned to one commit. Log folder
`/data/grid_ledgers/desk-receipts`. First recorded session: 2026-11-18.

## Why it exists

The Research Desk brief reads the factory ledger, Sleuth's leads with their declared
tests, and a thesis log. Two of those cannot show their own past:

- `investigation_leads` and `discovered_hypotheses` are updated in place and keep no
  history. Their clocks are written by the database.
- There is no thesis log yet (#1165).

So a brief written on Tuesday could not prove on Friday what it read. This log records,
once per session, the exact bytes each source returned and when. From the first night it
runs, a brief for that session can cite a receipt instead of a live table.

It cannot reach backwards. A session that was never recorded stays unknown.

## What one record holds

```json
{"kind": "receipt",
 "session": "2026-11-18",
 "evidence_cutoff": "2026-11-18T21:00:00+00:00",
 "read_started_at": "...", "read_finished_at": "...",
 "sources": {
   "factory_ledger": {"status": "read", "rows": 3, "bytes": 1234,
                      "payload_sha256": "...", "scope": {...}},
   "sleuth_leads":   {"status": "read", "rows": 15, "payload_sha256": "...", "scope": {...}},
   "thesis_log":     {"status": "unknown", "failure": "SourceUnavailable", "reason": "..."}},
 "promotion_allowed": false,
 "prev_sha256": "..."}
```

Each `payload_sha256` names a file `payloads/<sha256>.json` holding the canonical JSON the
source returned. `ReceiptLog.payload(sha)` returns those bytes after re-hashing them.

| Source | What is read | Payload |
|---|---|---|
| `factory_ledger` | The module-factory ledger, reopened through its anchor with `verify(full=True)` | Ledger id, head hash, file hashes, `summarize()` |
| `sleuth_leads` | Leads with `origin = 'intersection'` created in the 60 days up to the cutoff, each with `declared_tests` and `test_link_problems` | The rows, sorted by id |
| `thesis_log` | Nothing: no log exists | None. Recorded as `unknown` on every receipt until #1165 lands |

## Rules

1. **The cutoff is the session's close.** It is computed from the session
   (`ingestion.market_calendar`, early closes included) and cannot be passed in.
2. **Only the latest closed session can be recorded.** Not a session still open, and not
   an older one once a later session has closed. This is what stops a brief date from one
   day being paired with a cutoff from another.
3. **One receipt per session.** A retry writes nothing and returns the first record, even
   if the sources have changed since.
4. **Unreadable is not empty.** A source that raises is `status: "unknown"`. A source that
   reads no rows is `status: "read", rows: 0`. The receipt is written in both cases, so a
   missing feed is on the record as missing.
5. **Append only.** Canonical JSON lines chained by `prev_sha256`, one chained anchor line
   per append, payload files created once. The formats are the S10 forward log's and
   E2's. The helpers are restated in this module, because E2 forbids imports of its chain
   from outside its package.
6. **No secrets on the record.** A failed read is recorded by exception type. Its message
   goes to the run log only.

`verify()` re-checks every one of these on the stored records, not only at write time: a
record whose cutoff is not its session's close, a second record for a session, or sessions
out of order all fail.

## What a receipt proves

It proves the payload bytes were what the source returned to this process between the two
read clocks, **if** the anchors are also held off-host. On one host, a person who rewrites
the log can rewrite the anchors with it.

`export-anchors` appends the missing anchor lines to
`05-GRID/Paper-Log/desk-receipts/grid-desk-source-receipts.anchors.jsonl` in a vault
worktree. It writes the file only. Committing and pushing it is a separate step, as for
the E2 witness, and nothing does that step for this log yet.

It does not prove a source was right. `created_at <= cutoff` keeps later leads out of the
read, but a lead or test that existed before the cutoff and was changed between the cutoff
and the read shows its changed state. The read clocks are on the record for that reason.

## Commands

```bash
python -m intelligence.desk_source_receipts record \
    --dir /data/grid_ledgers/desk-receipts \
    --factory-ledger /data/grid_ledgers/factory/ledger/grid-module-factory.jsonl \
    --factory-anchor /data/grid_ledgers/factory/anchor/grid-module-factory.anchors.jsonl

python -m intelligence.desk_source_receipts verify --dir /data/grid_ledgers/desk-receipts \
    [--external-anchors <witness file>]

python -m intelligence.desk_source_receipts export-anchors \
    --dir /data/grid_ledgers/desk-receipts --vault-worktree <vault clone>
```

`record` exits 0 when it wrote a receipt or the session already had one, and 2 when it
refused (session not closed, or superseded). `verify` exits 1 on a broken log.

## The nightly job

`scripts/desk_receipts_run.sh` runs `record`, then `verify`, then posts to the agent hub:

| Outcome | Hub post | Exit |
|---|---|---|
| Receipt written | `desk-receipt-<session>`: cutoff, log head sha256, record count, sources recorded as unknown | 0 |
| Session already recorded | none | 0 |
| Refused (not closed, superseded) | none | 2 |
| Recorder crashed | `desk-receipts-crash-<date>` | 3 |
| Log failed verification | `desk-receipts-broken-<date>` | 4 |

The post for a written receipt is the log's off-host copy for now. The hub is on another
host and is mirrored to the vault on GitHub, so each night's full head hash is held, with
a timestamp, where grid-svr cannot rewrite it. It is weaker than a committed anchor file:
a person compares it with `verify`; `verify --external-anchors` does not read it.

Owner of failures: Claude (builder). Backup: Anik. A failed night cannot be made up after
the next close, so a crash or broken-log post needs attention the same evening.

To check it on grid-svr:

```bash
systemctl --user list-timers grid-desk-receipts.timer
journalctl --user -u grid-desk-receipts -n 40
```

## Not done

- No job commits and pushes the witness file. The hub post above stands in for it.
- The running copy is ahead of review. A format change after review starts a second log;
  this one is never rewritten.
- The Desk brief does not read receipts yet. That adapter is in the consumer lane.
- Live factory slots (#1161) are not a source yet; they do not exist.
