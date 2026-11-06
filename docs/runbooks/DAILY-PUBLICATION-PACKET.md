# Daily publication packet handoff

Job ID: `daily_publication_packet_handoff`. **Codex daily-review owner is
accountable for closing each day**, including a visible failure with an owner
and next action. Claude is the existing capture producer. Waiting for Claude,
reviewer acceptance, or a receipt does not transfer that accountability.

This is the versioned handoff contract and an offline status reporter, not a new
capture implementation or an activated integration. The existing daily research
automation remains the sole scheduler for its two outputs (the daily brief and
the retrospective). Its existing schedule and fresh dated-task policy stay in
force. Run this job as a step of that automation; do not register a second
automation, cron, systemd timer, Hermes job or heartbeat for it.

## Existing boundaries and coordination

- Diary generation remains with `scripts/run_market_diary.py` and its existing
  service/timer. Its `--previous-session` target is a US session date. A packet
  uses a **UTC publication day**, not that session date: `[D 00:00Z, D+1 00:00Z)`.
  For example, a next-morning diary can describe a prior session. Preserve both
  clocks. Never infer diary execution from packet absence or an exit-zero claim.
- Capture preparation, sealed admission, its one-use registry and capture
  receipt remain with Claude's existing external adapter. That adapter and its
  runtime ledger are not tracked in this repository; do not replace them here.
- A10 already covers routine daily capture **when its existing conditions are
  met**, including new reviewer acceptance and an unrun sealed admission. The
  Codex owner must obtain that acceptance and make the handoff concrete, rather
  than asking the user to reapprove routine in-scope work. A job state, deadline,
  hash or this document cannot substitute for those conditions.
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
| `diary_run` | Claude capture producer | Existing diary execution receipt with actual target session, observed start/end, exit/outcome and output row IDs; an explicitly verified no-output/non-session result is recorded as such, never guessed. |
| `candidate` | Claude capture producer | Exact D window, reviewed adapter/source identity, bounded corpus/coverage manifest and candidate artifact hashes. Empty or partial corpus is explicit. |
| `admission_review` | Codex daily-review owner | Existing reviewer acceptance and matching sealed admission, exact window/source/limits/expiry/registry bindings and A10 conditions. Merely prepared is not admitted. |
| `capture` | Claude capture producer | Actual terminal receipt for that admission/attempt, observed effect and cleanup outcome, captured artifacts/manifest and hashes; claim is spent. A missing receipt is unknown. |
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

The owner normalizes its private receipts to a JSON file with exactly:

```json
{
  "schema": "daily-publication-packet-observation-v1",
  "publication_day": "2026-11-05",
  "observed_at": "2026-11-06T07:00:00Z",
  "due_at": "2026-11-06T06:00:00Z",
  "capture_attempt": "unknown",
  "stages": {}
}
```

These timestamps are synthetic examples, **not a new schedule/deadline or a
live Nov 5 observation**. Use the actual existing automation's deadline and a
fresh observation time. No stage key means `missing`; a non-success stage has
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

## Adoption and operational acceptance

The PR supplies this contract, reporter and offline controls only. The external
automation/producer owners must adopt it in their existing job instructions and
receipt adapter before claiming the runtime stall is fixed. Bind the actual
scheduler/run ID, current producer/reviewer/consumer task IDs, private ledger
location and existing deadline there. Do not install or enable anything from
this PR. Runtime adoption remains pending until these receipts exist:

1. Same-day run has a single owner/key and observes the existing diary execution
   separately from packet capture. Confirm UTC-day versus session-date handling,
   including weekends, holidays and DST, without changing generation timers.
2. The owner-normalized candidate-without-admission case visibly assigns Codex
   `admission_review`; a missing whole day reports diary evidence as missing.
3. An already spent or uncertain admission is reconciled without capture replay;
   local resume keeps the same key and retained evidence. A concurrent owner and
   a lost Gemini ACK block duplicate effects.
4. Private custody and exclusions pass against exact bytes before the existing
   authorized Gemini path. Reject wrong-day, changed hash, partial corpus and
   missing dedup evidence; keep scientific acceptance separate.
5. Retain one naturally scheduled run's start/transition/exit status, two output
   statuses and failure notification, with no new schedule or production change.

The local tests exercise declarations and CLI behavior, not these runtime
acceptance steps. No capture, replay, provider/production DB read, external model
call, timer change, merge or deployment is part of this delivery.
