# Desk source receipts

Module: `intelligence/desk_source_receipts.py`. Tests: `tests/test_desk_source_receipts.py`.
Job: `scripts/desk_receipts_run.sh`, `deploy/systemd-user/grid-desk-receipts.{service,timer}.template`
(tests: `tests/test_desk_receipts_run.py`, `tests/test_systemd_user_template_invariants.py`). Part of 3pacs/GRID#1164.

**Running on grid-svr since 2026-11-19**, on the owner's word, before review: a `grid` user
timer, weekdays 16:20 and 19:20 New York, from a checkout pinned to one commit. Log folder
`/data/grid_ledgers/desk-receipts`. First recorded session: 2026-11-18.

The running copy is the commit it was pinned to. Nothing in this file changes it; a newer
commit reaches grid-svr only when the owner points the unit at a new pinned checkout.

**Why it is not a release-tree unit.** Every template in `deploy/systemd/` must run from
`/data/grid_v4/grid_release`, which each deploy moves to `main`. This job appends to a log
whose line format later readers verify, so its code must not change because `main` did. It
runs from a checkout pinned to one reviewed commit, as a `grid` user unit. Its templates
therefore live in `deploy/systemd-user/`, which has its own guard
(`tests/test_systemd_user_template_invariants.py`): `@CODE@` for the working directory and
every repository path, never the release tree, the usual `.env`, no `User=`. The
release-tree guard is unchanged and still covers every template in `deploy/systemd/`, and a
`@CODE@` template placed there fails.

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
 "session": "2026-11-19",
 "evidence_cutoff": "2026-11-19T21:00:00+00:00",
 "calendar_sha256": "efa8897e...",
 "state_as_of": "read",
 "read_started_at": "...", "read_finished_at": "...",
 "sources": {
   "factory_ledger": {"status": "read", "rows": 3, "bytes": 1234, "payload_sha256": "...",
                      "scope": {...}, "read_started_at": "...", "read_finished_at": "..."},
   "sleuth_leads":   {"status": "read", "rows": 15, "payload_sha256": "...", "scope": {...},
                      "read_started_at": "...", "read_finished_at": "..."},
   "thesis_log":     {"status": "unknown", "failure": "SourceUnavailable", "reason": "...",
                      "read_started_at": "...", "read_finished_at": "..."}},
 "promotion_allowed": false,
 "prev_sha256": "..."}
```

`calendar_sha256`, `state_as_of` and the per-source clocks were added after the first
receipt. The 2026-11-18 record has none of them and stays valid as written: `verify` checks
them when present and does not require them.

Each `payload_sha256` names a file `payloads/<sha256>.json` holding the canonical JSON the
source returned. `ReceiptLog.payload(sha)` returns those bytes after re-hashing them.

| Source | What is read | Payload |
|---|---|---|
| `factory_ledger` | One stable read of the module-factory ledger and its anchor (two passes that agree), then `verify(full=True)` and `summarize()` on those bytes only | Ledger id, head hash, file hashes, `summarize()`: all of one version |
| `sleuth_leads` | Leads with `origin = 'intersection'` created in the 60 days up to the cutoff, with their tests, in one database snapshot | The rows, sorted by id, each with `declared_tests` (verified only), `unverified_tests` and `test_link_problems` |
| `thesis_log` | Nothing: no log exists | None. Recorded as `unknown` on every receipt until #1165 lands |
| `panel_trials` (version 2 receipts only) | gex-signals' live trial ledger (`signals.db`, read-only, one transaction): the `panel_trials` chain and the stored looks of factory slots (`factory_looks`) | Chain head, row count, every row hash, trials per family, each `panel-factory` slot's registration hash, and for each stored look its time, last day and the sha256 of its exact stored text |

### Receipt versions

A receipt without `receipt_version` is version 1: the three original sources, the shape the
first consumer reads strictly. With `--panel-trials-db` the receipt carries
`"receipt_version": 2` and a fourth source, `panel_trials`. Without the option the receipt
is version 1 exactly as before: the new source is not added as `unknown`, it is not there.
`verify` refuses a version 1 receipt that carries the new source, a version 2 receipt
without it, and any other version number. One log can hold both (tested: version 1, then
2, then 1 again). A consumer that only knows version 1 should refuse, or skip, any receipt
with `receipt_version`.

### What `panel_trials` is for, and its limits

It is the off-host witness of the live record that #1162 asks for. A slot's exported verdict
is joined to a receipt on the slot's registration hash and the stored look's digest, time
and last day (`ledger_steered_exploration.witness_factory_verdicts`); the chain head is
only where the chain stood.

- **The proof is limited** (`proof: row_links_only`): each row links to the one before it.
  A row's hash is not recomputed from its fields; that recipe is gex-signals'
  (`panel.verify_trials`) and is not copied here.
- **History may only be added to, for trials and for looks.** The source compares what it
  reads with the last payload the log holds (read back as the bytes its hash names). The
  trial rows read then must still be the first rows now, and every stored look read then
  must still be there with the same time, last day and exact text. A look that is gone or
  changed is `unknown` for the session even when no trial row moved; so is a set of factory
  tables that has gone away. `continuity` in the payload says which check was made
  (`first_read`, `extends_last_receipt`, or `not_checked` when the source was asked without
  a log to compare with).
- **Registrations are bound by content, not only by hash.** The payload keeps a digest of
  every registration row's contents (`trials`: the registration columns named in
  `TRIAL_CONTENT_COLUMNS`, canonical JSON, sha256) and each factory slot's exact parameter
  text. The next receipt must find every one unchanged; a registration whose time,
  parameters or any other registration column was changed under an unchanged row hash is
  `unknown`. What a row held before any receipt first read it is not known.
- **The look itself is in the payload**: its exact stored text beside the text's sha256, so
  a consumer recomputes the digest and reads the look's own verdict and numbers from the
  receipt, not from an export.
- **The factory tables come as a set.** `factory_looks_table` is `absent` on a database
  from before the factory and `present` when all of `FACTORY_TABLES` are there. Any other
  layout, the looks table alone included, is `unknown` (an incomplete generation). A look
  must belong to a registered `panel-factory` slot.
- **A passed slot's retirement checks are read too** (#1163). gex-signals re-checks a
  passed slot at registered checkpoints and a check below the registered floor lapses it
  for good (`factory_retirement_checks`, panel-factory/2). The verdict export carries no
  lapse, so the receipt is where GRID learns of one. The payload holds each check's slot,
  checkpoint, time, last day, lapse flag, exact stored text and that text's sha256.
  `factory_retirement_checks_table` is `absent` on a generation without the table, which
  is not "none lapsed". Checks are append-only and are held to that against the last
  receipt that read them; a check for a slot with no stored look is `unknown`.
- **How a lapse reaches the factory ledger.** The nightly `verdicts-witness` command also
  calls `ledger_steered_exploration.read_factory_retirement` on the same verified receipt.
  It appends one `factory_retirement_reading` covering every passed verdict whose
  registration and look the receipt retains, lapsed or not, or it appends nothing and says
  why. `summarize()["live"]["lapsed"]` is the last reading's count, as of that session. It
  is `null` while any passed verdict is covered by no reading, and `0` when no verdict has
  passed. A reading takes nothing back: a recorded lapse stays that lapse.
- It is a nightly reading: a change made and undone between two closes is not seen.

## Rules

1. **The cutoff is the session's close.** It comes from the pinned XNYS manifest
   `docs/reference/calendar-manifests/xnys-rth-20261001-20271231.json` (the one
   `ingestion.gex_native_volume` pins, sha256 in `CALENDAR_SHA256`), early closes included,
   and cannot be passed in. A date the manifest does not cover is refused; there is no
   fallback to the scheduler's approximate helpers. A test walks every day the manifest
   covers and requires the helpers to give the same sessions and closes, so the two are
   reconciled, not assumed. After 2027-12-31 the job refuses until a new manifest is pinned.
   The manifest file must not be group- or world-writable, or it is refused the same way.
   On grid-svr the `grid` user's umask is 002, so a pinned checkout has to be made under
   `umask 022` (the file should be mode 644). A checkout made the default way would refuse
   every session with exit 2 and write no receipt.
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

7. **One version per source.** See the table above. If the factory files change during every
   read attempt, the source is `unknown`.
8. **An interrupted write is finished, not redone.** The receipt line is written and synced
   before its anchor line. A run that stops between the two leaves one append unanchored:
   one record, or the header and first receipt with no anchor file yet. The next run appends
   the missing anchor, marked `"recovered": true` (its `run_at` is the recovery), and writes
   no receipt for a session that already has one. No line is rewritten. More than one
   unanchored record, or a missing anchor file on a longer log, is not what an interruption
   leaves: `verify` fails and nothing is appended until a person looks.

`verify()` re-checks every one of these on the stored records, not only at write time: a
record whose cutoff is not its session's close, a second record for a session, or sessions
out of order all fail.

## The four clocks

A receipt is a read made after the close. It is not the state at the close.

| Clock | Where it is | Who writes it |
|---|---|---|
| Session close | `evidence_cutoff` | Computed from the pinned calendar. It dates the session and bounds which leads are in scope by `created_at` |
| Read | `read_started_at` / `read_finished_at`, on the receipt and on each source | This recorder. The payload is the source's state at these times |
| Witness | The time the hub (or a pushed witness file) received the head | The witness. This log does not hold it and cannot vouch for it |
| Consumer availability | When the Desk had both the capture and the witness | The consumer |

Sleuth rows are changed in place and the factory ledger grows, so a payload can contain
state from after the close. `state_as_of: "read"` says so on the record. A reader that needs
"what was known when" must use the read clocks, and must not replay a payload as if it had
been known at the cutoff.

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

python -m intelligence.desk_source_receipts witness-status --dir /data/grid_ledgers/desk-receipts \
    [--pending] [--lines]

python -m intelligence.desk_source_receipts witness-mark --dir /data/grid_ledgers/desk-receipts \
    --session 2026-11-18 --head <full sha256> [--via hub|operator]
```

`verify` prints `anchored_records`, `unanchored_records`, `witnessed_records` and
`qualified`. `ok` means the chain and the anchors given are consistent. Whether the log can
be used as evidence is `qualified`, in two parts that never stand in for each other:

- `local_anchor`: every record is under this host's anchor.
- `external_witness`: every record is under the off-host anchor copy passed with
  `--external-anchors`. Without that argument it is `null`: unknown, not a pass.

## Handing a head to the witness

Each receipt has one head to witness: the sha256 of its own log line, with the log's length
up to it. Both are fixed once written, so the pair (session, head) can be delivered again at
any time, whatever was appended since.

`witness-delivery/<session>.<head>.json` is this host's note that it handed that pair over.
`witness-status` lists every receipt as `delivered` (a note exists) or `pending`.
`witness-mark` writes the note; it refuses a head that is not exactly that session's, and a
receipt not yet anchored on this host, and a second call for the same pair writes nothing.

The note is the recorder's own bookkeeping. It exists so a failed delivery is retried and a
good one is not repeated. It is not the witness: the recorder does not vouch for its own
witness, and `qualified.external_witness` never reads these notes.

Receipts written before notes existed (2026-11-18, and any night recorded by the earlier
pinned commit) show as `pending` and would be posted once more by the first run of this
version. To avoid that, the owner can run `witness-mark --via operator` for a head he has
seen on the hub. The original hub post and its arrival time are untouched either way.

`record` exits 0 when it wrote a receipt or the session already had one, and 2 when it
refused (session not closed, or superseded). `verify` exits 1 on a broken log.

## The nightly job

`scripts/desk_receipts_run.sh` runs `record`, then `verify`, then hands every pending head to
the agent hub, oldest first:

| Outcome | Hub post | Exit |
|---|---|---|
| Receipt written or already there, every pending head posted | `desk-receipt-<session>` per pending head: session close, receipt head sha256, records up to it, sources recorded as unknown | 0 |
| Receipt safe, a hub post failed or its note was refused | whatever did go through | 5 |
| Nothing pending | none | 0 |
| Refused (not closed, superseded) | none | 2 |
| Recorder crashed | `desk-receipts-crash-<date>`, best effort | 3 |
| Log failed verification | `desk-receipts-broken-<date>`, best effort | 4 |

A post that fails is not a success. `agent-report` is fail-soft: it exits 0 even when the hub
refused the post and it only spooled it. So a delivery counts only when the reporter exits 0
and prints its `agent-report sent: desk-receipts/<slug>` line. Otherwise the run exits 5, the head stays `pending`, and the next
run (19:20, or the evening check's extra start) posts the same session and the same head. A
run that records nothing new still delivers what is pending. A head already noted as
delivered is not posted again. The text of a post depends only on the session and head, so
a repeat is the same bytes. The hub de-duplicates on date, agent, host and slug, so a repeat on
a later UTC date, or a spooled post flushed after a retry got through, can leave two hub
entries for one head. They say the same thing; the earliest arrival is the witness time. With `DESK_RECEIPTS_REPORT` empty, delivery is off: the run says
which sessions are pending and exits 0.

A hub post is the log's off-host copy for now. The hub is on another host and is mirrored to
the vault on GitHub, so each receipt's full head hash is held, with the hub's arrival time,
where grid-svr cannot rewrite it. It is weaker than a committed anchor file: a person
compares it with `witness-status`; `verify --external-anchors` does not read it.

A failed night cannot be made up after the next close, so a failure needs attention the
same evening. Who hears about it: a second user timer on grid-svr,
`grid-desk-receipts-check.timer` (weekdays 19:45 New York, after both attempts), checks
that the latest closed session has a receipt, the log verifies, the hub holds the post and
no source other than `thesis_log` is unknown. That check predates exit code 5 and reads the
hub itself; it does not yet read `witness-status`. A good night is silent. A missing receipt
gets one more start of the job. Anything still wrong is pushed to the owner's phone. That
check lives outside this repo (`/data/agent-home/anikdang/desk-receipts/check/` on
grid-svr) because it depends on the owner's push tool. Owner of failures: Anik, by that
push. Builder to call on: Claude.

To check it on grid-svr:

```bash
systemctl --user list-timers grid-desk-receipts.timer
journalctl --user -u grid-desk-receipts -n 40
```

## Not done

- The witness file is committed and pushed by `scripts/desk_receipts_witness_sync.sh` (2026-11-21), run as
  `ExecStopPost` of `grid-desk-receipts.service` from the dedicated clone `/home/grid/dev/obsidian-vault-deskwitness`
  (export-anchors, append-only check, commit only `05-GRID/Paper-Log/desk-receipts/`, push; loud on any refusal or
  an unpushed head). It takes effect when the owner clones that vault copy and moves the unit to a pinned checkout
  that has the script; until then the hub post stands in for it.
- The running copy is the first pinned commit and does not have the review fixes (pinned
  calendar, one-version reads, interrupted-write recovery, retryable delivery). They reach
  grid-svr when the owner applies a new pinned checkout. The fixes add optional fields and
  an anchor flag; they do not change or invalidate the existing log, which is never
  rewritten.
- The seam check with the consumer uses the retained 2026-11-18 capture and the original
  hub post, through the private handoff routes. No capture is re-run for it.
- A receipt is not the native Factory report, not a research-disposition thesis row
  (#1165) and not a live Factory slot (#1161/#1162). Those stay unknown or not supplied.
- The Desk brief does not read receipts yet. That adapter is in the consumer lane.
- The `panel_trials` source is in the code but not in the running copy, which is pinned to
  an earlier commit and writes version 1 receipts. Version 2 starts when the owner moves the
  unit to a new pinned checkout whose unit sets `DESK_PANEL_TRIALS_DB` (the template does).
- The evening check on grid-svr treats any unknown source other than `thesis_log` as a
  failure. That is the wanted behaviour for `panel_trials` once it is on.
- The consumer of receipts must learn `receipt_version` before version 2 receipts are read
  into a brief.
- No factory slot is registered (0 of 10), so the slots and looks in the payload are empty.
