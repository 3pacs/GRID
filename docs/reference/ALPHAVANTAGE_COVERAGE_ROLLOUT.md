# Alpha Vantage corporate-data coverage: source-only rollout plan

## Scope and status

This change extends the existing `earnings_events_puller`, not the daily report
pipeline. It does not deploy, edit a live wrapper, register a scheduler, increase
an account's request allocation, or run the historical AV bulk downloader.

Existing reusable components:

- `ingestion/earnings_events_puller.py`: SEC filing index and AV transcripts.
- `ingestion/altdata/sec_edgar_company.py`: stored SEC fundamental facts with
  issuer, fiscal year/period, actual period end, filing date and accession.
- `scripts/av_bulk_download.py`: existing AV earnings, income statement, balance
  sheet and cash-flow adapters. This script remains unchanged and unrun.
- Merged PR #207 introduced the transcript CLI's 400-call per-run default. This
  is a local cap, not evidence of the account's entitlement or the live command.

Official provider contract: the transcript `quarter` parameter is a **fiscal**
quarter, not the current calendar quarter:
https://www.alphavantage.co/documentation/#earnings-call-transcript

## Changes in this branch

1. Match existing SEC fiscal metadata to the exact filing accession and report
   date. Comparative facts cannot inherit a newer filing's fiscal label. Reject
   missing, conflicting and future metadata. Request at most the two newest
   verified periods; never guess the issuer's fiscal year from the month.
2. Collect SEC filings before AV work, preserving filings coverage even when
   the transcript budget runs out. A recent transcript no longer hides another
   newly reported quarter. Legacy `--mode` and `--incremental-max-age-days`
   arguments remain accepted; both modes deduplicate exact verified periods.
3. Persist a symbol/quarter cursor in a local SQLite ledger. Advance before HTTP,
   including empty results and uncertain network failures. A cap that ends
   mid-symbol resumes at the next quarter instead of restarting at the alphabet.
4. Reserve calls and pacing slots atomically. Keep an explicit per-run cap, add
   `--av-requests-per-minute` and optional `--av-daily-limit` (UTC day). Stop on
   HTTP 401/403/429 or provider Information/Note notices without printing keys
   or response-body details. State/storage failures do not trigger a full pull.
5. Validate response symbol, quarter and nonempty transcript content. Store the
   actual SEC period end and provenance. The existing required `filing_date`
   column uses the matched SEC filing date, explicitly labeled in raw metadata
   as **not the call date**. `observed_at` records retrieval time. This does not
   establish an independently verified earnings-call date or historical PIT
   availability; consumers must not silently reinterpret either date.
6. Legacy rows with matching period keys but missing verified provenance are
   reported as `av_legacy_unverified_periods`, not complete coverage. They are
   neither automatically rewritten nor duplicated. Correcting historical dates
   requires an explicit migration because filing_date participates in uniqueness.

The default ledger is `GRID_AV_TRANSCRIPT_STATE`, if set, otherwise
`~/.local/state/grid/av_transcripts.sqlite3`. Supply a durable, writable, local
path through `--av-state-path` in the eventual service configuration. Do not use
an ephemeral checkout directory or an unreliable network filesystem. The ledger
contains request keys, counters and times, never credentials or transcripts.

The pacing and day cap cover **only callers using this same state file**. This
is not an account-wide limiter across other AV workers, hosts or scripts. The
five-requests/minute default is deliberately conservative and makes no claim
about the subscription. Raising it is a separate operational allocation decision.

## Milestones and minimum acceptance requirements

### M1: Account and source inventory (before increasing requests)

- Verify the actual account plan and endpoint entitlements without printing the
  key. Inventory every worker sharing it and the exact live wrapper arguments.
- Confirm which existing bulk-script sources and normalized series are already
  populated, their coverage and ages. An adapter in source is not proof of an
  installed scheduler, executed download, vendor entitlement or usable data.
- Record an allocated per-minute rate, per-run cap and optional daily cap for
  this worker after reserving capacity for the other consumers.

### M2: Offline correctness (this source change)

- Fiscal fixtures include calendar issuers, September and January fiscal ends,
  52/53-week dates, next-year fiscal labels, comparative facts and amendments.
- Restart/cap tests prove later symbols and second quarters get attempts even
  when earlier responses are empty or fail. UTC rollover and concurrent ledger
  reservations respect the configured local budget.
- Provider notices stop promptly. HTTP errors never log credential-bearing URLs.
- Exact valid stored periods skip; ambiguous metadata and legacy coverage are
  visible gaps. State failure spends no untracked requests.

### M3: Database and metadata readiness (not performed)

- Validate the parameterized PostgreSQL read/upsert contracts against a throwaway
  database matching production schema. Do not run tests against production.
- Run a read-only coverage inventory of stored SEC fiscal metadata and candidate
  transcript periods. Populate missing SEC metadata through the existing approved
  fundamentals ingestion workflow before expecting these issuers to be eligible.
- Audit old AV fiscal labels and fabricated period/call dates. Agree an explicit
  historical reconciliation plan; do not silently relabel or append replacements.
- Verify all event/timeline/point-in-time consumers respect date basis and
  retrieval availability. Add a dedicated nullable call date/known-at contract
  if the existing compatibility schema cannot express it safely.

### M4: Bounded canary and rollout (separate approval)

- After database and consumer checks, run a small, explicitly budgeted canary
  spanning calendar, September and January fiscal years. Reconcile requested
  quarter, returned identity, actual report end, text and source provenance.
- Confirm empty/unavailable differs from throttled, entitlement failure and
  malformed response. Measure latency, observed request count and duplicate rate.
- Only then update the live wrapper and cadence. Compare requested/attempted/
  stored/unknown/legacy counts by ticker and quarter, and verify cursor progress
  over multiple runs. Rollback is code/config rollback, retaining ledger state
  and ingested evidence; never delete history to force a refresh.

### M5: Fundamental/valuation completeness (existing adapters, later scope)

- Exercise the existing AV statement/earnings adapters through a bounded,
  resumable job with the same account allocation, not an indiscriminate bulk run.
- Minimum machine-readable record: stable issuer identity; statement/field;
  period start/end and annual/quarterly/TTM basis; currency and units; value;
  filing/publication and first-observed times; source/accession; revision basis;
  and explicit missing/error status. Preserve reported data separately from
  estimated or derived data. Never convert missing statement values to zero.
- Valuation inputs additionally require reconciled price as-of time, diluted
  shares, cash/debt, comparable periods, formulas and source links. Corporate
  data availability alone does not prove a complete or correct valuation.

## Validation performed and limits

56 focused offline regression tests passed on Python 3.12 with outbound socket
connections forbidden and no live database fixtures. Changed-file Ruff E/W/F
(excluding E501), Python compilation and `git diff --check` passed. Independent
review found a legacy-provenance deduplication issue; it was fixed and covered.

No production queries, paid provider calls, real PostgreSQL integration, full
repository test suite, remote CI, push, PR publication, merge or deployment were
performed. Local tests establish the source contract, not live coverage.
