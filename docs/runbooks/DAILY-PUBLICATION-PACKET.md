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
  `grid-publication-packet.timer` on grid-svr (10:35Z daily, previous UTC day).
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
  "due_at": "2026-11-06T12:00:00Z",
  "capture_attempt": "spent",
  "stages": {"diary_run": {"state": "passed", "publication_day": "2026-11-05",
                           "receipt_sha256": "<sha256 of receipts/diary_run.json>",
                           "upstream_receipt_sha256": null},
             "candidate": {"...": "chained to diary_run"},
             "capture": {"...": "chained to candidate"}}
}
```

These timestamps are illustrative. `due_at` is the window end plus
`GRID_PUBLICATION_PACKET_DUE_HOURS` (12 by default: 12:00Z); the daily review
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
`deploy/systemd/`; 10:35Z daily, after the 05:30Z diary timer and the 03:30-10:30Z
backup window). For the previous
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

### Delivery: the public screened tree on grid-svr (canonical)

The producer keeps the private day folder (`$GRID_PUBLICATION_PACKET_ROOT/<D>/`, mode
0700, holds `capture.json` 0600) and, after every run, copies the SCREENED files to a
world-readable tree: `$GRID_PUBLICATION_PACKET_PUBLIC_ROOT/<D>/` =
`/data/grid/outputs/publication-packets/<D>/` on grid-svr (dirs 0755, files 0644). The copy
is an allow-list — `packet.json`, `receipt.json`, `observation.json`, `receipts/*.json`,
`FAILED-*.json` — so `capture.json` (raw rows) can never reach it by omission, and the bytes
are unchanged so every receipt hash verifies in either tree. A re-run for an already
produced day (`--day <D>`) publishes it without rewriting anything private, which is how
the Nov 4–6 backlog was published. `--no-publish` skips the copy; publishing never blocks
production (a copy failure is logged and reported in the result, the private tree is
authoritative).

Any agent on the fleet (Alien's daily-review owner included) reads from there over the
fleet SSH route, for example:

```bash
rsync -rt grid-svr:/data/grid/outputs/publication-packets/2026-11-06/ ./2026-11-06/
# or just the hashes:  ssh grid-svr cat /data/grid/outputs/publication-packets/2026-11-06/receipt.json
```

### Delivery: precision5520 mirror (convenience copy)

**Retired 2026-11-08** with the in-repo grader: both precision5520 user timers
(`grid-publication-packet-mirror`, `grid-publication-review-push`) are disabled. Agents read the
public tree on grid-svr directly. The notes below describe the units as they were.

A pull mirror on precision5520 (`systemd --user` timer) copies the public tree into the
established Codex pickup directory `~/Documents/Codex/_inbox-from-claude/grid-publication-packets/<D>/`.
Its source is the public tree, so even the mirror's rsync never sees `capture.json`.

```ini
# ~/.config/systemd/user/grid-publication-packet-mirror.service (precision5520)
[Service]
Type=oneshot
ExecStart=/usr/bin/rsync -rt --chmod=D0755,F0644 \
  grid-svr:/data/grid/outputs/publication-packets/ %h/Documents/Codex/_inbox-from-claude/grid-publication-packets/
# grid-publication-packet-mirror.timer: OnCalendar=*-*-* 11:05:00 UTC, Persistent=true
```

The daily-review consumer reads the packet in either place, performs `admission_review` by
hash readback, and continues its own stages. Codex-owned stages do not change the
producer's files.

## Grader (in-repo, scheduled) — the daily review itself

`scripts/grade_publication_review.py` is the daily review. It runs on grid-svr from the
release tree at 11:35Z (`deploy/systemd/grid-publication-review-grader.{service,timer}.template`;
first install by hand, then deploys keep the installed unit in sync), reads the public screened
tree, and for each of the last ten publication days (`--lookback 10`, so every horizon up to a week
resolves) writes `$GRID_PUBLICATION_REVIEW_ROOT/<D>/review.jsonl` and hands it
to the ingest in-process. No agent, inbox or mirror is in the loop; nothing has to be nudged.

What a day's rows look like (schema `publication-review-verdicts-v1`, the same contract the
ingest enforces; the grader constructs rows so they pass by construction):

- `thesis_snapshots` → one `direction` row per snapshot: the stored `overall_direction` against
  SPY's split-adjusted close (`TIINGO`, `YF:SPY:adj_close`) from the last completed close before
  the snapshot to the close of the first session on/after `stated_at` + 3 calendar days — the
  rule `intelligence/thesis_tracker.py` already uses, ±0.5 % band and its matrix: same side
  `confirmed`, opposite sides `refuted`, any flat leg `partially_confirmed`; `unresolved` (horizon
  stated) until the horizon close exists; `unscorable` when the baseline session has no usable close.
- `market_briefings`, `market_diary` → a record whose text says its AI narrative was withheld
  or unavailable has nothing to grade and gets one `unscorable` `data_quality` row saying so.
  That count, per day, is the machine-readable measure of how often the generators publish
  data dumps instead of forecasts (every hourly briefing on 2026-11-04..07 did). A record with a
  narrative goes through claim extraction on the LLM router (`GRID_PUBLICATION_GRADER_TIER`,
  default `reason`; `GRID_PUBLICATION_GRADER_PROVIDER` to pin one, e.g. `gemini` behind
  `GRID_ALLOW_PAID_LLM`); `direction` claims on priced instruments (index and commodity names map to
  their ETF proxy) and `level` claims on the ticker itself are graded from closes; a level on an
  alias ("SPX above 6800") is `unscorable` because the proxy's scale differs; `event`/`causal` stay
  `unresolved` (a causal claim is never confirmed from co-movement). Extractions are cached per
  publication sha + extraction setup (prompt version, tier, provider) under `<D>/extractions/`,
  re-validated against the text on read, so re-grades never call a model twice and a better model
  re-extracts instead of inheriting a weaker one's answer.
- `market_briefings` narratives are mostly descriptive ("The VIX stands at 28.18"): every
  current-state figure on a series the generator reads (`ollama/market_briefing.py` key_series:
  VIX, VIX9D, VIX3M, S&P 500, Nasdaq, Dow, Russell 2000, HYG/LQD/JNK/EMB, GLD, UUP, T10Y2Y,
  T10Y3M, DFF, DGS2, DGS5) becomes a `data_quality` row judged on whether the number was real
  at the instant it was stated, from every capture GRID holds around it
  (`store.observations.read_vintages`: intraday pulls, the completed close, the session's
  high/low, later revisions). During a session: confirmed when it lies between the in-session
  captures within 90 minutes of the statement, or equals the previous completed close;
  partially confirmed when it is only inside the session's high/low. After a close: confirmed
  when it equals the newest completed close (even one GRID pulled after the statement:
  `knowable_at_stated_at` says which); partially confirmed as stale when it equals an older
  close while a newer one was already known. FRED rates use the same newest-vs-stale rule over
  observation dates. Anything else is refuted, with the nearest captures and the range as
  evidence. Derived quantities (z-scores, momentum, ratios, moves in %) are skipped.
- A day whose verdict rows and header (packet sha, models) did not change is left alone
  (`UNCHANGED`); a change rewrites the file and the ingest records a new run that supersedes the
  previous one. A hand-off that failed (database down) is retried on the next run until the
  grader receipt records the file's sha as ingested. Nothing writes to
  `thesis_postmortems`.

Readable by any fleet agent: `grid-svr:/data/grid/outputs/publication-reviews/<D>/` holds
`review.jsonl`, `grader-receipt.json` and `ingest-receipt.json` (bytes unchanged, 0644), next to
the public packet tree. The review root itself is under the grid user's 0700 home.

Receipts, per day, under `$GRID_PUBLICATION_REVIEW_ROOT/<D>/`: `grader-receipt.json`
(state WRITTEN/UNCHANGED/REFUSED/FAILED, verdict counts, review sha, the ingest's result) and
the ingest's own `ingest-receipt.json`. Exit 3 = graded but the ingest rejected rows.

```bash
# on grid-svr, from the release tree, as grid (same env as the unit)
GRID_PUBLICATION_PACKET_PUBLIC_ROOT=/data/grid/outputs/publication-packets \
GRID_PUBLICATION_REVIEW_ROOT=/home/grid/research/publication-reviews \
  /data/grid_v4/grid_venv-314/bin/python scripts/grade_publication_review.py --day 2026-11-07 --dry-run
# the scheduled form (last 6 days, write + ingest):
  /data/grid_v4/grid_venv-314/bin/python scripts/grade_publication_review.py
```

Health: `/api/v1/system/health` carries `checks.publication_review` (latest packet day in the
public tree, latest ingested review day and timestamps) and degrades with a named reason when
either is older than two days (`store/publication_review_status.py`). That is where a missing
day shows up; nobody has to go and look in a directory.

The external daily-review agent (Codex task-10 on Alien) is no longer a writer of
`review.jsonl`: its reverse-mirror leg on precision5520 (`grid-publication-review-push`, 12:35Z)
must stay disabled, otherwise two writers race for the same file. It may read the public tree
and the ingested rows like any other agent.

## Consumer adoption and operational acceptance

Runtime acceptance of the chain is three receipts for one naturally scheduled day, none of
which a person supplies:

1. 10:35Z: `<D>/receipt.json` in the public tree on grid-svr (producer).
2. 11:35Z: `<D>/grader-receipt.json` with `state: WRITTEN` (or `UNCHANGED` on a re-grade) and
   `<D>/ingest-receipt.json` with `state: INGESTED` under the review root (grader + ingest).
3. `/api/v1/system/health` shows `publication_review.latest_review_day = <D>` and no
   `publication review` reason under `degraded_reasons`.

The external daily-review agent's own stages (`admission_review`, `custody`, `privacy_screen`,
`local_review`, `gemini_review`) are its business for its own brief and are not part of this
acceptance; scientific acceptance (#958) stays separate.

Historical days: Nov 4–6 packets are produced by hand from the same job (late
captures, flagged `late_capture: true`). They are real captures of the retained
rows, not manufactured success receipts; the old lineage's Nov 3 r13 candidate
stays sealed and unadmitted.
