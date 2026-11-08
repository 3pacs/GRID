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

## Follow-on qualification, 2026-11-07

The previous validation statement describes the initial commit only. Subsequent
work added bounded **read-only** production metadata inspection and real
PostgreSQL testing in a disposable local cluster. No production writes or paid
provider requests were made.

### Current source overlap check

Rechecked open AV/transcript/fiscal/earnings PRs: none matched. Upstream main had
advanced from `3958255` to `29281ac`, including daily-packet and Hermes work.
No upstream changes affected earnings_events_puller.py, sec_edgar_company.py or
av_bulk_download.py between those commits. The AV candidate stayed on its
existing local branch; no daily-pipeline implementation was duplicated.

### Stored-data readiness, not provider coverage

Inspection scope: grid-svr/griddb, 2026-11-07 21:07:24–21:09:23 UTC. Queries used
BEGIN READ ONLY and 15–20-second statement timeouts. Only schema and aggregate
counts were returned; no transcript text or credentials were exported.

- SEC_EDGAR_Fundamentals was registered and active (id 734), last pull recorded
  2026-11-06 20:50:48 UTC. Overall source: 16,338 rows, 301 series. Its provenance-
  preserving sec_filed_fundamentals subset contained 8,185 rows, 153 series and
  21 parsed tickers. All those rows had SUCCESS status.
- The candidate's 550-day, three-field, successful/nonfuture-observation filter
  selected 373 rows across 59 series and 21 tickers. Every selected row had all
  five required fiscal/date/accession metadata fields.
- Matching exact ticker, accession, period end and filing date against stored
  SEC 10-K/10-Q rows left 158 fact rows across 20 tickers and 56 distinct
  ticker/end/accession tuples. All passed the fiscal/form/date checks. There
  were no conflicting period labels in this bounded set.
- Accession-only matching would have accepted another 108 rows whose period
  ends did not match. These comparative facts demonstrate why accession alone
  is insufficient for fiscal-quarter selection.
- The newest two verified periods per eligible ticker produced 40 period pairs.
  One overlapped a legacy AV record, leaving 39 potentially requestable pairs.
  This is a **stored-table approximation**, not reconciliation with fresh SEC
  submissions, verified provider availability, or full watchlist readiness.
- All 240 stored AV EARNINGS_CALL rows (160 tickers) contained text but lacked
  date-basis, matched-fiscal-accession and observed-at metadata. All used period
  end as filing date, including three future-dated records on the inspection
  date. None meets the new provenance test. Historical reconciliation remains
  necessary; no rows were modified.

### Real PostgreSQL contract test

Added `tests/test_av_transcript_pg.py`. The fixture refuses arbitrary DB_URL/TCP
targets and requires a disposable /tmp/grid-av-qualification.* cluster, a
private Unix socket, the fixed av_coverage_test database, matching data_directory
and no TCP listeners. It mirrors the production table columns and relevant
constraints discovered read-only. No shared CI or production database is used.

Five PostgreSQL integration tests cover JSONB/ANY parameter binding, source and
availability filters, exact SEC joins, real upsert insert/update idempotency,
verified/legacy separation, complete mocked-provider pull cycles, repeated-run
deduplication and rotation after empty results. All five passed on PostgreSQL
15.19, together with the 56 offline tests (61 total). Provider HTTP calls were
blocked. This is not a full repository or production-service acceptance run.

### Shared account-budget design (not implemented or activated)

Source inventory found eight direct AV client modules/scripts. Their existence
does not establish current runtime activity:

- earnings_events_puller: transcript requests, currently local ledger only.
- altdata/alphavantage_sentiment: NEWS_SENTIMENT; statically registered in the
  scheduler and smart scheduler. Its 12-second delay is not a daily-call cap.
- price_fallback: GLOBAL_QUOTE fallback; independent of transcript pacing.
- av_bulk_download: statements, earnings, economic and commodity histories.
- av_bulk_prices and fix_spy_data: daily-price download/repair tools.
- full_universe_pull: overview, income, balance, insider and earnings functions.
- load_wave2: RSI/MACD technical-indicator requests.

Before raising the transcript allocation, choose one explicit account-wide
control path:

1. **Near-term allocation:** inventory which callers actually run, disable no
   jobs implicitly, and assign conservative per-worker minute budgets whose sum
   remains below the confirmed account rate with spare capacity. Bound retry
   attempts and exclude unscheduled bulk jobs from overlapping runs. This is an
   operating convention, not a technically enforced global guarantee.
2. **Durable shared control:** extend the existing HTTP call sites to a common
   AV request client backed by one PostgreSQL reservation ledger/coordinator
   across hosts. Keep the existing statement parsers and destinations. Use a
   configured account alias, never credentials, as the ledger identity. Every
   attempt including retry consumes a reservation before HTTP. Do not merely
   allocate future sleep slots: delayed workers can otherwise bunch requests.
3. The shared gate must enforce a rolling request window, short-lived dispatch
   authorization, worker quotas/fairness, shared provider-notice cooldown, and
   fail-closed behavior if coordination is unavailable. Use separate priority
   classes for current reports/quotes and historical backfill, plus per-run and
   optional operator UTC-day caps. Do not infer a provider daily cap or reset
   timezone from old source comments.
4. Record reservation/dispatched/outcome/uncertain states without URLs containing
   keys. HTTP 401/403/429 and Information/Note responses stop or cool down the
   correct scope; an entitlement failure is not an empty dataset. Never refund
   an uncertain dispatched request merely because its response was lost.
5. Acceptance tests must include concurrent callers on multiple processes,
   crashes before/after dispatch, late wakeups, clock changes, restart, rolling
   boundaries, day rollover, retries and provider errors. A fake provider must
   observe actual request-start timestamps and verify the rolling-window cap.
   A source scan must prove every enabled call path uses the gate before it is
   described as account-wide protection.

No numerical production allocation is selected here: existing plan entitlements
and active caller concurrency must be verified together. The candidate's local
ledger remains explicitly scoped and is not represented as this shared gate.

### Next work, in order

1. Build a read-only, per-issuer missing-metadata and legacy-reconciliation
   manifest. Expand SEC fiscal metadata through the existing approved feed.
2. Define known-at and nullable actual call-date handling for every consumer.
   The compatibility view currently maps period_end to earnings_date and
   created_at to pull_timestamp; that mapping is not actual call timing.
3. Implement/review the chosen shared-budget control across enabled callers.
4. Only after those gates, seek the separate approval for a small paid-provider
   canary and eventual deployment. No wider history download is authorized here.


## Consumer contract implementation, 2026-11-08 (draft source only)

Integrated main's `_utc_today` rendering helper while preserving both fresh and
stale digest regressions. The forward `av_event_dates_20261108` migration changes
the compatibility view's `earnings_date` to NULL and appends explicit `period_end`
and `earnings_date_basis`. No current filing/transcript writer independently
verifies an actual earnings event date. SEC filing dates remain filing dates;
period ends are not promoted to event dates. The compatible sync function now
returns zero inserts rather than making misleading calendar stubs. Its source
bootstrap is aligned so a later setup cannot reinstall the unsafe mapping.

This intentionally removes the unverified filing-to-calendar fallback. The
separate actual/estimated earnings-calendar feeds are unchanged. Historical
calendar rows remain untouched because this bridge did not preserve reliable
origin attribution. Neither this migration nor the analyzer repairs old data.
The view's legacy `pull_timestamp` is storage time, not a historical publication
or point-in-time guarantee. Migration downgrade refuses to restore false dates.

The transcript analyzer now includes AV read-through results separately from
its existing EDGAR stored results. AV evidence requires nonempty matching fiscal
provenance and aware retrieval/storage timestamps; availability is the later of
`observed_at` and `created_at`. A historical `as_of` excludes later evidence.
Actual `call_date` stays NULL. AV analysis IDs are source-specific, repeated
period copies do not count twice, and the incompatible legacy ticker/date
analysis table is not written. Historical output is explicitly recomputed from
currently stored evidence, not an immutable historical snapshot; original
analysis timestamps and revisions cannot be reconstructed from this schema.
AV prior-tone shifts remain unknown rather than comparing against EDGAR rows.
No AV analysis persistence or UI endpoint exposure is claimed by this change.

Qualification: 155 offline AV/analyzer/digest tests passed with outbound socket
connections forbidden. Six PostgreSQL 15 contract tests passed in the existing
disposable private-socket cluster, including actual migration upgrade/grants and repeat execution, NULL event
dates, unchanged legacy rows, repeated no-op sync and old-view replacement.
The test cluster was stopped afterward. Changed-file Ruff and diff checks passed.
This is not a full migration-chain or production acceptance run. New-head CI
must finish after publication. No provider requests, production queries/writes,
merge, deployment or scheduler changes were performed.

Remaining rollout gates: historical reconciliation and metadata breadth;
consumer/UI adoption beyond the analyzer; immutable revision-aware PIT storage;
confirmed provider entitlements and shared-account budgeting; explicit canary
approval. The SQLite pacing ledger remains local and can bunch delayed workers'
actual dispatches; it is not an account-wide rolling-window guarantee.


### CI guard reconciliation

The first consumer-contract head failed only the raw-read allowlist ratchet on
Python 3.11: 19,759 tests passed, 944 skipped and 3 xfailed. Inspection of both
heads showed the removed match was prose saying "from raw_series", not an SQL
query. The real EDGAR SELECT already required SUCCESS and remains unchanged.
Removed that obsolete allowlist slot without altering the scanner or weakening
the guard. All 157 focused tests (including both guard tests) passed together
with outbound sockets blocked. A fresh full CI run is required for this fix.
