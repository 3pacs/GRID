# SEC financial period integrity

Status: isolated source candidate, not a production-data repair. Do not promote
this change until the rollout gates below are closed.

## Confirmed failure

The November 7, 2026 read-only production audit found NVIDIA July 26 revenue of
$177.837 billion labeled `quarter` in `capital_flows`. The primary 10-Q identifies
that as six-month revenue; the three-month value is $96.221 billion. COGS, R&D,
operating expenses and tax showed the same cumulative-column problem.

Primary filing:
https://www.sec.gov/Archives/edgar/data/1045810/000104581026000075/nvda-20260726.htm

In `ingestion/altdata/sec_xbrl_financials.py`, the old classifier mapped `10-Q`
and `6-K` directly to `quarter` and annual forms to `annual`. It ignored the
fact's `start`. Deduplication then collapsed different durations sharing
end/form/fp; equal filing dates left the first array entry as the winner.
Annual filings can also contain quarter comparatives, so form is not a period.
The filing's `fp` is not proof of the fiscal quarter of every comparative fact.

## Source-only change

- Use explicit start/end, with inclusive 80–100-day quarter and 350–380-day year
  acceptance windows. These cover ordinary calendar and 52/53-week fiscal
  reporting, including 14-week quarters. The bounds are an intentionally
  conservative supported-duration policy, not a complete issuer fiscal calendar.
- Omit instant, malformed, YTD-only, transition and other unsupported contexts.
  Do not divide YTD values or synthesize a standalone quarter. Q2/Q3 cash-flow
  metrics often have only YTD facts and will honestly remain unavailable.
- Require traceable accession and filed date. Keep exact start/end, filed date,
  accession, taxonomy and tag in each normalized result. The existing
  `source_filing` column also persists them, with the actual containing form
  and filing `fp` explicitly labeled as source metadata.
- Select one normalized current fact per end/type. Preserve tag priority,
  then use the newest filed date. Equal-ranked contradictory amount, currency
  or start contexts are omitted, independent of array order.
- Net debt only from explicitly reported operands sharing start/end, accession,
  filed date, taxonomy and currency. Missing repayment is unknown, not zero.
  Explicit zero repayments are allowed. Resolve conflicting/latest candidates
  before omitting zero/negative net inflows.
- Generated source strings have a versioned marker immediately after a known
  filing form. The writer replaces earlier marked rows only for the emitted
  actor/end/type/flow/NULL-counterparty key, avoiding duplicate economic rows
  when accession/tag metadata changes. Unmarked legacy rows are not swept.

## What this does not establish

This is a normalized current snapshot, not an immutable point-in-time archive.
The filed date is a date, not the SEC acceptance timestamp or first GRID receipt.
No provider response was fetched for this patch. The NVDA test is a deliberately
small, hand-transcribed regression shape, not an archived Company Facts payload;
its example filing metadata is test input, not independently verified evidence.

Existing rows are unchanged, including the reported NVDA error. An omitted,
withdrawn, conflicted or newly nonpositive fact does not currently invalidate a
previously stored row. The writer's empty-input behavior is explicitly tested
as this limitation. Legacy rows can still win downstream ranking. Merely
rerunning the puller, changing an `as_of` timestamp, or rebuilding a rollup is
not sufficient evidence of correction.

This patch does not fix the existing FX fallback policy, accounting-tag
substitutions (for example total expenses used as COGS), absolute-value sign
convention, calendar-quarter UI labels, or fiscal-year inference. Those need
separate qualification before authoritative valuation. No TTM value is created
by this parser.

## Milestones and minimum acceptance gates

1. **Parser prevention, source-only.** Freeze the tested commit; review actual
   durations against archived SEC Company Facts inputs for domestic 52/53-week,
   non-calendar-year and IFRS issuers. Inventory accepted/rejected facts and
   reasons per required metric. Test expected missing quarters explicitly.
2. **Identity and revision contract.** Choose a durable structured source
   identity/provenance schema for immutable observations and a separate current
   view. Define how withdrawals, conflicts, unsupported revised contexts and
   nonpositive debt invalidate earlier normalized rows. Test amendments and
   out-of-order ingestion. The bounded marker replacement here is not the
   complete revision/withdrawal contract.
3. **Controlled historical reconciliation.** Prepare a read-only discrepancy
   manifest binding each affected old row to exact reported period/accession,
   proposed replacement or unavailable status, and impacted consumers. Obtain
   explicit approval for any production writes. Validate the writer and
   rollback/uniqueness behavior on a disposable PostgreSQL database first.
   Keep legacy/unrelated rows untouched until an approved repair is executed.
4. **Derived-data and report qualification.** After approved repair, recompute
   or invalidate affected TTM/annual rollups and cached reports. Verify all
   required periods, units, currency, dates and primary citations together.
   An NVDA valuation remains blocked while its underlying financial set is
   inconsistent or required fields are unavailable.

No production database queries or corrections, paid/provider requests, PR
publication, merge, deployment, scheduler changes or checkpoint changes were
performed for this source candidate.

## Local validation

- 69 focused regression cases pass. The first 51-case suite produced 42 failures
  against the unmodified source, including the $177.837B versus $96.221B case.
  Review-added debt conflict/amendment cases were also demonstrated failing
  before their fixes.
- Combined selected offline suite: 146 passed, 3 explicitly deselected. The
  three neighboring Dad/retired-Finviz tests require unavailable FastAPI or
  POSIX `fcntl` in this Windows dependency set; they are not claimed passed.
- Outbound sockets and PostgreSQL connections were blocked by the test harness.
  SQLite writer tests register `NOW` and `split_part`; these do not qualify
  PostgreSQL transactions, concurrency, deployed schemas or full CI behavior.
- Changed-file Ruff E/W/F (except E501), Python compilation and `git diff --check`
  pass. Existing Python 3.12 SQLite date-adapter deprecation warnings remain.
- No live SEC Company Facts capture, PostgreSQL integration, complete repository
  suite or deployed acceptance run was performed.
- Independent final review found no further actionable issue within this source
  candidate and independently reran all 69 focused tests successfully. The
  documented production qualification gates remain open.
