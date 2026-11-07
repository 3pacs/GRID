# Daily publication packet handoff

Job ID: `daily_publication_packet_handoff`. **Codex daily-review owner is
accountable for closing each day**, including a visible failure with an owner
and next action. Claude is the existing capture producer. Waiting for Claude,
reviewer acceptance, or a receipt does not transfer that accountability.

This is the versioned handoff contract, the in-repo capture producer
(`scripts/daily_publication_packet.py`, one scheduled run per UTC day on
grid-svr; see "Producer job" below) and an offline status reporter. The existing
daily research automation remains the sole scheduler for its two outputs (the
daily brief and the retrospective). Its existing schedule and fresh dated-task
policy stay in force. The daily review consumes the packet the producer wrote;
it does not register a second capture, cron, Hermes job or heartbeat for it.

## Existing boundaries and coordination

- Diary generation remains with `scripts/run_market_diary.py` and its existing
  service/timer. Its `--previous-session` target is a US session date. A packet
  uses a **UTC publication day**, not that session date: `[D 00:00Z, D+1 00:00Z)`.
  For example, a next-morning diary can describe a prior session. Preserve both
  clocks. Never infer diary execution from packet absence or an exit-zero claim.
- Capture is `scripts/daily_publication_packet.py`, scheduled by
  `grid-publication-packet.timer` on grid-svr (06:15Z daily, previous UTC day).
  It replaces the per-day external caller/admission/one-use-registry ceremony
  (r3–r13 lineage on precision5520) that required a fresh source bundle,
  independent review, sealed admission, reviewer runtime acceptance and
  prior-day dispositions for every day, and which produced nothing after Nov 3.
  The operator's standing approval for the daily capture class (A10, 2026-11-02:
  "it should be automatic") is the authority for this scheduled job. Retained
  bundles, admissions and receipts of the old lineage stay as history; nothing
  replays them.
- The producer reads the database only (one read-only `REPEATABLE READ`
  snapshot through the repo engine), screens, hashes and writes files. It never
  writes a table, calls a provider or model, delivers anything externally, or
  authorizes a downstream stage.
- Reuse [M0 acceptance](M0-ACCEPTANCE.md) and the existing capture verification
  for custody/byte checks. Issue [#958](https://github.com/3pacs/GRID/issues/958)
  tracks end-to-end scientific acceptance, not another capture lane; this job
  does not close it. [#960](https://github.com/3pacs/GRID/issues/960) owns research
  protocol preparation. Keep their source/outcome/PIT gates separate.
- Source inspection at `6815be60` found the diary writer, M0 checker and their
  tests, but no daily publication handoff implementation or `.agents/skills`
  directory. The options-only `gem_daily_capture.py` is a different job.
  Open PRs #982 and #1001 concern browser acceptance and actor analytics,
  respectively; neither supplies this handoff. Do not modify their work.

## One day, one owner, one chain

The logical key is `daily_publication_packet_handoff:YYYY-MM-DD`. At the
existing automation's start, Codex resolves D, records the existing run/task ID
and configured completion deadline, and reconciles that key in the **existing
private custody/dedup ledger** before requesting any action. Reuse an existing
owner's run; competing or uncertain ownership blocks side effects. A changed
packet hash is not a new day and must not bypass deduplication. A fresh task is
an execution context, not a new capture entitlement.

Record a metadata observation at start, at every handoff/result, and at exit.
Use append-only revisions in the existing private record; retain original
candidate, admissions, claims and failures. Never overwrite an old day's
failure with today's success. Keep task/lease/attempt identities, exact source
release, receipt hashes and private artifact locations in that ledger. The
public status summary uses only the fields below and opaque hashes.

| Stage | Action owner | Evidence required before reporting `passed` |
| --- | --- | --- |
| `diary_run` | Claude capture producer (job) | `receipts/diary_run.json`: the `market_diary` rows whose `generated_at` falls in the window (row id, session date, generated_at, model/fallback, row sha256) plus `systemctl show grid-market-diary.service` when its last start lies in the window. `passed` with an explicit `non_session_verified` outcome on Sun/Mon UTC (no generation scheduled); `failed` when generation was scheduled and no row exists; `unknown` when the query failed. Never guessed from packet absence. |
| `candidate` | Claude capture producer (job) | `receipts/candidate.json`: exact D window, snapshot `captured_at` and database receipt, per-channel counts/truncation/complete flags, and a body-free record manifest (publication id, published_at, canonical source-row sha256). Empty or truncated channels are explicit. |
| `capture` | Claude capture producer (job) | `receipts/capture.json`: `capture.json` sha256 (= `packet.capture_receipt.export_sha256`), `packet.json` sha256, projected counts by channel, every quarantine/exclusion with its text sha256 and reasons, screens applied, caps, late-capture flag. `capture_attempt` is `spent` when this receipt exists. |
| `admission_review` | Codex daily-review owner | Acceptance of the captured packet for review: `packet.json` sha256 read back equals the capture receipt, window and day identity match, the producer stages validate (`daily_publication_packet_status.py` on the producer's `observation.json`), no second owner for the day. Under the standing daily-capture approval this is the consumer's check, not a per-day operator question. |
| `custody` | Codex daily-review owner | Exact capture/export/packet/receipt hash readback, coverage, day identity, private custody, unique owner and dedup reconciliation. Directory presence or positive count is insufficient. |
| `privacy_screen` | Codex daily-review owner | Review of the exact proposed external payload and excerpts: exclude credentials, patient information, personal balances/positions and other standing exclusions. Bind the screened artifact hash. |
| `local_review` | Codex daily-review owner | Exact claim extraction, source gaps and local review retained against the screened packet; outcome/performance stays unscorable without matched qualified lineage. |
| `gemini_review` | Codex daily-review owner | Existing authorized Gemini/Antigravity account/model verified; retained request identity and returned review bound to that screened payload; dedup ledger reconciled. Attempted upload alone is not completion. |

Every receipt must identify the same logical day and link its preceding receipt.
An observation's receipt SHA refers to the private **stage receipt**, which must
retain those underlying bindings, not an invented hash of a status string.
These metadata declarations summarize checks performed by the existing owners;
the reporter does not authenticate the receipts or inspect their contents.
`passed` means that owner's check succeeded, not that a publication is true,
accurate, predictive, fully covered, or scientifically accepted.

## Failure reporting and retries

Finish each run with a visible status for each of the automation's two outputs.
The brief can complete while the retrospective is blocked; report them
separately. On missing/failed/unknown/blocked evidence, publish a concise status
in the existing task result: day, first blocked stage, accountable and action
owners, what is established, what is unknown, receipt reference, next action,
and the configured deadline. Never substitute a generic market summary for a
completed retrospective. Codex owns follow-through and escalation through its
existing workflow; do not silently exit or ask to pause the recurring job.

- Before the configured deadline, missing evidence is `PENDING`; a declared
  failure/unknown/block is `BLOCKED`. At or after the deadline any incomplete
  day is `OVERDUE`. Report the blocker immediately, then mark overdue at the
  next existing run/checkpoint. This does not create an alert poller.
- Re-reading retained local evidence and rerunning this offline reporter is
  safe. Resume at the first incomplete stage; reuse completed evidence.
- For an unstarted capture, the producer checks the existing A10 conditions
  and one-use registry. This report never gives permission to execute it.
- For `started`, `spent` or `unknown` attempts, reconcile the original claim,
  receipt and cleanup state first. Never replay an admission, remove a spent
  marker, issue a replacement merely to bypass the registry, or automatically
  recapture. Failed post-consumption work stays spent. Any separately justified
  recovery follows the existing controller/reviewer procedure.
- A Gemini timeout after submission is also uncertain: inspect the original
  request/session and dedup record before any resubmission. Receipt repair or
  local review resumption does not authorize another upload.
- Missing Nov 5 admission/capture records in the records checked establish a
  handoff evidence gap, **not** a diary failure. The Nov 3 prepared/unadmitted
  case is a distinct admission-review failure. Do not manufacture historical
  success receipts or run a backfill to make either day green.

## Read-only status command

The producer writes `<root>/<D>/observation.json` with the three producer
stages declared and chained (`diary_run → candidate → capture`). The
daily-review owner copies it into its private ledger, appends its five stages
as it completes them, refreshes `observed_at`, and runs the reporter. The file
has exactly:

```json
{
  "schema": "daily-publication-packet-observation-v1",
  "publication_day": "2026-11-05",
  "observed_at": "2026-11-06T07:00:00Z",
  "due_at": "2026-11-06T08:00:00Z",
  "capture_attempt": "spent",
  "stages": {"diary_run": {"state": "passed", "publication_day": "2026-11-05",
                           "receipt_sha256": "<sha256 of receipts/diary_run.json>",
                           "upstream_receipt_sha256": null},
             "candidate": {"...": "chained to diary_run"},
             "capture": {"...": "chained to candidate"}}
}
```

These timestamps are illustrative. `due_at` is the window end plus
`GRID_PUBLICATION_PACKET_DUE_HOURS` (8 by default: 08:00Z); the daily review
uses its own fresh `observed_at`. No stage key means `missing`; a non-success stage has
only `{"state":"unknown"}` (or `missing`, `failed`, `blocked`). A successful
stage has exactly `state`, `publication_day`, `receipt_sha256` and
`upstream_receipt_sha256`: lowercase 64-hex receipt hashes, with null upstream
only for `diary_run`. After a non-success stage, later declarations cannot make
the overall day complete. A passed capture also requires `capture_attempt` =
`spent`; other attempt states are `not_started`, `started`, `unknown`.

```bash
python -B scripts/daily_publication_packet_status.py PRIVATE_OBSERVATION.json --day 2026-11-05
python -B tests/test_daily_publication_packet_status.py
```

The explicit `--day` comes from the caller's resolved publication window, so a
prior day's file is rejected. The command reads at most 64 KiB, rejects duplicate
JSON keys, unknown fields, wrong-day receipts and broken known hash links, and
prints metadata-only JSON. It never echoes input content, exception text or file
paths. Exit 0 = `COMPLETE_REPORTED`, 1 = pending/blocked/overdue, 2 = invalid or
unreadable observation. All outputs have `authorizes_execution:false`.
`observation_sha256` identifies the exact input bytes, not source truth.

This is a report of the supplied observation time, **not a live monitor** or a
receipt verifier. A stale observation must not be presented as current. Retain
JSON privately through the existing job ledger and include the concise status
in the task result; do not put private receipts or publication text in GitHub.

## Producer job (in-repo, scheduled)

`scripts/daily_publication_packet.py` runs on grid-svr from the deployed
release tree under `grid-publication-packet.timer` (templates in
`deploy/systemd/`; 06:15Z daily, after the 05:30Z diary timer). For the previous
UTC day it writes, under `GRID_PUBLICATION_PACKET_ROOT/<D>/`
(`/home/grid/research/publication-packets/<D>/` on grid-svr):

| file | mode | content |
| --- | --- | --- |
| `capture.json` | 0600 | raw rows as read (`publication-capture-v1`); its sha256 is the export sha |
| `packet.json` | 0644 | screened publications (`publication-review-v1`, the shape the M0 checker and the daily review already read); quarantined and text-missing ids listed in `capture_receipt` |
| `receipts/{diary_run,candidate,capture}.json` | 0644 | the three chained stage receipts (`upstream_receipt_sha256` = sha256 of the previous file) |
| `observation.json` | 0644 | `daily-publication-packet-observation-v1` with the producer stages declared |
| `receipt.json` | 0644 | summary (counts, hashes, next owner/stage); its presence means the day is produced and is never rewritten |
| `FAILED-<stamp>.json` | 0644 | only when the database read failed; a later run may still produce the day |

Sources: `market_briefings` (`created_at`), `thesis_snapshots` (`timestamp`),
`market_diary` (`generated_at`) rows inside the window, up to 500 per channel
(truncation flagged); `market_diary` rows *about* `[D-7, D]` as retrospective
context; audio briefing metadata files; briefing grounding receipts when the
text association is exact. Screens: credential assignments and tokens,
connection strings, private-key blocks, health identifiers, hidden/control
characters. A hit quarantines the whole record (listed by hash, never
truncated or edited). Size caps exclude whole records only (1 MiB per text,
8,000,000 B aggregate) and are recorded as exclusions.

```bash
# on grid-svr, from the release tree
/data/grid_v4/grid_venv-314/bin/python scripts/daily_publication_packet.py --day 2026-11-06
python3 scripts/daily_publication_packet_status.py ~/research/publication-packets/2026-11-06/observation.json --day 2026-11-06
```

Exit 0 = produced (or already present); 1 = failed (FAILED receipt written);
2 = refused (window still open, bad arguments). Tests:
`tests/test_daily_publication_packet.py` (offline fake engine; no database).

### Delivery to the daily review

A pull mirror on precision5520 (`systemd --user` timer, documented below) copies
`packet.json`, `receipts/`, `observation.json` and `receipt.json` — never
`capture.json` — into the established Codex pickup directory
`~/Documents/Codex/_inbox-from-claude/grid-publication-packets/<D>/`. The
daily-review consumer reads the packet there (or directly on grid-svr over the
fleet SSH route), performs `admission_review` by hash readback, and continues
its own stages. Codex-owned stages do not change the producer's files.

```ini
# ~/.config/systemd/user/grid-publication-packet-mirror.service (precision5520)
[Service]
Type=oneshot
ExecStart=/usr/bin/rsync -rt --chmod=D0755,F0644 --include='*/' --include='packet.json' \
  --include='receipt.json' --include='observation.json' --include='FAILED-*.json' --include='receipts/**' --exclude='*' \
  grid-svr:research/publication-packets/ %h/Documents/Codex/_inbox-from-claude/grid-publication-packets/
# grid-publication-packet-mirror.timer: OnCalendar=*-*-* 06:45:00 UTC, Persistent=true
```

## Consumer adoption and operational acceptance

The daily review owner adopts the packet in its existing job instructions.
Runtime acceptance of the repaired pipeline is a retained run with these
receipts, none of which the producer can supply alone:

1. One naturally scheduled 06:15Z run produced `<D>/receipt.json` on grid-svr
   and the mirror delivered the screened files to the pickup directory.
2. The daily review found the day under its own key, read `packet.json` back
   by hash, recorded `admission_review` and `custody`, and ran the status
   reporter on the appended observation.
3. `privacy_screen` reviewed the exact external payload before the existing
   authorized Gemini path; `local_review` and `gemini_review` were retained
   against the screened packet hash. Scientific acceptance (#958) stays
   separate.
4. Both outputs (brief, retrospective) reported their own status; a blocked
   day showed the first blocked stage, owners and deadline in the task result.

Historical days: Nov 4–6 packets are produced by hand from the same job (late
captures, flagged `late_capture: true`). They are real captures of the retained
rows, not manufactured success receipts; the old lineage's Nov 3 r13 candidate
stays sealed and unadmitted.
