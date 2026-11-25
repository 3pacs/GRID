# D1 SEC collector — read-only verification and shadow-observation proposal v0

Status: **proposal**. Nothing in this file changes `grid-earnings-events` (unit, timer, wrapper, env), adds a
collector or feed, writes the database, or fetches from SEC. Each step below needs Anik's approval and
Codex's review.

Owner: the Claude D-track session (SEC collector owner, named 2026-11-25). Answers Codex's handoff
`GRID-D1-DELL-COLLECTOR-OFFLINE-20261125`, which asked the owner to verify three things read-only first.

## 1. Read-only verification (done 2026-11-25 around 03:10Z)

| Codex asked | Finding |
|---|---|
| Loaded service and checkout | `grid-earnings-events.timer` is enabled, daily at 06:00 UTC, `Persistent=true`. The wrapper runs `/data/grid_v4/grid_release` on venv-314 (#1242). Release HEAD is `b2629045`, equal to `origin/main` at the check. The puller blob `3ac71a55` equals main and is the base blob of Codex's Dell patch. The last 7 runs ended rc=0. The Task #151 06:00 UTC timer is still current. |
| Interval versus release capture target | One run a day with a 14-day lookback. GRID's availability clock is ingest time. A typical after-close Item 2.02 8-K (about 21:05Z) lands about 9 h later, before the next open. A pre-open one lands after that day's session. A D1 card that needs a same-session reaction cannot use this collector as it runs today. A next-open card can. |
| Archive trust boundary | The collector writes `earnings_events` rows (form, accession, SEC `items` text) and nothing else. It never writes the company-documents archive. The archive's only writer is the manual `scripts/ingest_company_document.py --archive`. No producer of a `primary_post_commit_readback` receipt exists, so #1424's diagnostic has no real input today. That matches what #1424 itself says. |

## 2. Proposed bounded shadow observation (one filing, not activated)

The goal is to observe the real path once for one new Item 2.02 filing, without touching the running service.

1. **Trigger:** wait for the next watchlist issuer's 8-K whose `items` contain 2.02 to appear in
   `earnings_events` from a normal scheduled run. The run itself is not changed.
2. **One-shot, owner-run, off the service:** run as a script from a dev checkout of an exact reviewed head, not a
   timer or unit. Steps:
   - Read that one row from griddb in a read-only session.
   - Fetch that accession's primary document and its EX-99.1 from SEC once, under SEC fair-access rules.
   - Write the bytes to a scratch archive with `ingest_document`, then read them back.
   - Emit a receipt labelled `rehearsal`, never eligible for production.
   - Run #1424's `diagnose_sec_exhibit_evidence` with a recording consumer.
3. **Output:** the receipt, the document and claim hashes, the clocks and the status, published to the vault (not the consumer's full input; see section 3). No
   database write, no change to the earnings-release hold, no ranking, and no price or outcome read.
4. **Code:** Codex's Dell delta (sha256 `e284bd17…70550528`, verified; base #1424 `6e7e010d`) is the
   candidate. It adds a default-off `EarningsEventsPuller.rehearse_sec_exhibit_handoff` that `pull()` never calls.
   It needs an independent reviewer other than the owner.

## 3. Tightened conditions (added after review 6051360420)

Seeing a filing in the 06:00Z run does not guarantee a timely, usable pre-open claim. Before any fetch:

1. **Freeze first.** Write down the one eligible accession, the scope (primary document plus EX-99.1), the cutoff
   and a maximum request budget (a fixed count of SEC requests) before fetching anything.
2. **Real clocks.** Record the actual retrieval time and the scratch readback time, not the 06:00Z run time.
3. **Two checks, both documented.** (a) The Dell delta's recording consumer runs only inside an isolated fixture
   injection. (b) The delta's exported `offline_rehearsal_only` receipt must be refused by the unchanged
   production diagnostic in #1424. The export is never relabelled as `primary_post_commit_readback`.
4. **Publish little.** Only the necessary hashes, clocks and status go to the vault, not the recording
   consumer's full input.

## 4. Decisions for Anik

- Approve or decline step 2, now under the conditions in section 3: one SEC fetch of one pre-frozen filing,
  from a dev checkout, with no service change. Review 6051360420 holds running it until section 3 is in the
  one-shot's own written plan.
- Name the independent reviewer for the Dell delta. The owner cannot review their own change. This session
  reviewed #1424 at `6e7e010d` before it was named owner; Anik may want that review re-done by Codex or another
  reviewer.
- Item 2.02 classification by the producer and authenticated receipts stay open (#1424's stated limits).
