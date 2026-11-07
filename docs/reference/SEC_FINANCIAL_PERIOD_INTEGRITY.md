# SEC financial period integrity

Status: isolated source candidate with archived-input and disposable PostgreSQL
qualification. No production repair, migration, publication or deployment has
been performed. The rollout gates below remain required.

## Confirmed failure

The November 7, 2026 read-only production audit found NVIDIA July 26 revenue of
$177.837 billion labeled `quarter` in `capital_flows`. The primary 10-Q identifies
that as six-month revenue; the three-month value is $96.221 billion. The archived
Company Facts response and the filing's inline XBRL independently confirm the
same accession, amounts and date contexts.

Primary filing:
https://www.sec.gov/Archives/edgar/data/1045810/000104581026000075/nvda-20260726.htm

The old `ingestion/altdata/sec_xbrl_financials.py` mapped `10-Q`/`6-K` directly to
quarter and annual forms to annual, ignoring `start`. Deduplication collapsed
unequal durations sharing end/form/fp, then chose equal-date facts by input
order. Annual filings can contain quarterly comparatives. Filing `fp` does not
establish the fiscal quarter of every comparative fact.

## Duration and normalization contract

- Require valid start/end, accession and filed date. Accept inclusive 80-100-day
  quarters and 350-380-day years, including ordinary 52/53-week fiscal reporting.
  These are conservative supported windows, not a complete issuer calendar.
- Omit instant, YTD-only, transition and other unsupported contexts. Never divide
  cumulative amounts or fabricate a standalone quarter. Missing is not zero.
- Persist exact start/end, accession, filed date, taxonomy and tag in normalized
  evidence and `source_filing`. Label filing `fp` as source metadata.
- Keep existing concept priority, then newest filed date. Equal-ranked amount,
  currency or start conflicts remain unavailable, independent of array order.
- Net debt only from reported proceeds and repayments sharing exact duration,
  accession, filed date, taxonomy and currency. Missing repayment is unknown;
  explicit zero is valid. Resolve conflicts/amendments before filtering the
  positive-inflow view, so an old positive result cannot survive a newer zero.

## Evidence and current-projection contract

`sec_financial_snapshots` preserves original response bytes as deterministic
gzip, source URL/hash, request/receipt timestamps, normalized JSON/hash, FX inputs,
actor/CIK and normalizer version. Content identity binds all normalization inputs
and output. `sec_financial_snapshot_heads` is the separate current pointer.
Identical content is idempotent; repeat captures advance the head's observation
times without overwriting the first archived capture. This is application-level
append-only behavior, not a tamper-proof archive or complete request-event log.
The migration gives the ordinary `grid` role SELECT/INSERT on evidence, but
ownership, inherited/default grants and privileged access require deployment
review. Existing database roles/privileges were not inspected or changed here.

Admission checks issuer identity, recognized nonempty taxonomy, fact structure,
numeric values, filing contexts, duplicate JSON keys and bounded body size. A
malformed/failed response never reaches the writer. Unmapped instant or future
concepts are allowed because real whole-company responses contain them; mapped
financial flows require duration context. Structural admission does not prove
all issuer concepts are present in the provider response.

The writer recomputes snapshot identity at its boundary and serializes per actor
with a PostgreSQL advisory transaction lock. It archives and replaces only this
producer's marked rows atomically with its current pointer. Older-started requests
cannot supersede newer captures; equal-time conflicting captures fail closed;
newer captures that regress an existing shared key's filed date are archived but
not promoted. Fetch/admission/SQL failure retains the prior projection.

A valid current response can yield no supported rows. Its absent, conflicted,
unsupported or nonpositive results remove previous owned projection rows rather
than leaving stale values. Absence means unavailable in this projection, not a
proven issuer withdrawal. **Unmarked legacy rows remain untouched**, including
known incorrect rows; downstream readers may still choose them until reconciled.
The older `_write_rows` compatibility helper remains bounded per emitted key and
now propagates SQL failures; it is not the normal snapshot ingestion path.

Checkpoints bind normalizer version and separate attempted issuers from successful
ones. Partial sweeps resume; all-attempted sweeps restart. A persistent failed
issuer cannot freeze successful issuers' future revision checks or starve later
tickers under a small invocation limit. Failed/NO_CIK attempts are not success.

## Source-backed qualification

`tests/fixtures/sec_financial_periods/` contains exact original SEC response bytes,
deterministic gzip, provenance receipts and `qualification.json`. Four public
SEC reads captured three current Company Facts responses plus the NVDA filing.
A fifth NVDA read separately preserved true request/receipt boundaries for the
local reconciliation proposal. No paid-provider calls were made.

- NVDA: actual `us-gaap:Revenues`, accession `0001045810-26-000075`. April 27-July
  26, 2026 is $96.221B; January 26-July 26 is $177.837B YTD and is rejected.
- Apple: real 371-day fiscal year and 98-day quarter are correctly admitted.
- TSMC: real IFRS annual TWD facts qualify; approximate existing FX conversion
  remains explicitly estimated. This sample does not establish IFRS quarters.
- Full responses produce 587 NVDA, 599 Apple and 64 TSMC normalized rows, invariant
  to reversal of all fact arrays. Counts describe this mapping, not completeness.

Company Facts captures are current observations containing later comparatives
and amendments, not proof of what GRID knew historically. Their receipt times
cannot be backdated. Filed dates are not SEC acceptance timestamps.

## Historical reconciliation proposal, not repair

`scripts/plan_sec_financial_reconciliation.py` reads only local capture receipts,
original source bytes and frozen before-images. It has no network, database or
execution path. It verifies source hashes and preserves PostgreSQL NUMERIC values
losslessly through Decimal handling. Each proposed row includes its exact ID,
full before-image hash and a current normalized candidate or unavailable state.
Legacy labels do not establish their original accession or duration.

The November 7 dry run used an independent REPEATABLE READ READ ONLY export of
798 NVDA annual/quarter rows (287 annual, 511 quarter), captured 21:17:51.485479Z.
The source request ran 21:20:46.986464Z-21:20:47.727032Z. Results:

- 252 rows have a different supported current value.
- 338 match values but require provenance review.
- 208 have no supported current replacement.

These are review dispositions, not 798 approved corrections and not proof that
every difference is the quarter/YTD bug. July 26 quarterly revenue, COGS, opex,
R&D, tax and dividends have different supported values; buybacks and acquisitions
remain unavailable in the standalone-quarter projection.

Frozen before-image artifact SHA-256:
`21e0b721675dea3615c0c7e4d0c7fc4fe46e7e07f7a75d5663606288d536665e`

Proposal artifact SHA-256:
`d514befed7b39fd3b1eea689cef0cbd20ca3b17c526fb2a97d57a2ca6a5a79b5`

These private operational artifacts are retained outside the repository; only
public SEC fixtures are committed. The proposal includes its capture receipt
and source hashes. No repair executor is included.

## Minimum rollout and repair gates

1. Review this exact source revision in normal CI and the deployed migration
   graph/schema. Confirm writer ownership, archive grants, source-marker ownership
   and all consumer read priorities. Migration creates evidence/head tables only;
   downgrade intentionally preserves evidence. No existing row repair is implicit.
2. Approve a precise historical scope after resolving ambiguous/unavailable cases
   against primary filings and concepts. Do not bulk-delete or rewrite all 798
   rows. Distinguish current restated truth from historical point-in-time truth.
3. Build and test a separate explicit repair operation in a disposable database.
   Bind approved IDs, full before-image hashes, source capture, normalizer/FX
   inputs, replacements and unavailable decisions. Lock affected actors and use
   compare-and-swap checks in one transaction; abort on drift or scope mismatch.
   Preserve before-images, a transaction receipt and an exact rollback plan.
4. Obtain explicit production-write approval. Refresh read-only before-images and
   verify unchanged hashes immediately before any approved repair; a November 7
   proposal is not authorization and may become stale. Keep unrelated actors,
   counterparties, sources and historical evidence untouched.
5. Invalidate/recompute dependent TTM/annual rollups and cached reports under the
   approved scope. Verify every required period, unit, currency, date and citation,
   including unavailable fields, then independently read back a canary sample.
   Simply rerunning ingestion or changing `as_of` is not reconciliation.

Existing FX fallback, accounting concept substitutions, absolute-value signs,
calendar-quarter display labels and fiscal-year inference are not fixed here.
No TTM values are synthesized. Authoritative valuation remains blocked where the
financial set is inconsistent, unqualified or missing required metrics.

## Local validation

- 270 selected offline tests passed; 3 Dad/retired-Finviz tests were explicitly
  deselected for unavailable FastAPI/POSIX `fcntl` dependencies. Network/PG calls
  were blocked; repository live-DB conftest and plugin autoload were excluded.
  This includes parser, real archives, snapshot admission, runner, dependent
  readers, shares-fetch compatibility and Alembic single-head/ID checks.
- 14 real PostgreSQL tests passed against a freshly initialized private native
  PostgreSQL 15.19 cluster: actual uniqueness, atomic rollback, source archive,
  idempotency, replacement/empty projections, legacy preservation, regression,
  equal-time rejection, out-of-order captures, locking, concurrent newest-wins and
  actual clean-application-role UPDATE/DELETE/TRUNCATE denial on evidence.
  The cluster was stopped and removed, without accessing existing services/data.
- Independent review found and verified fixes for malformed empty admission,
  Decimal before-image preservation and failed-issuer sweep starvation. The
  reviewer independently reran the 141-test earlier focused suite and the final
  12-test runner suite, reporting no remaining actionable source findings.
- Changed-file Ruff E/W/F (except E501), Python compilation and diff checks pass.
- Full repository CI, production migrations, provider-wide coverage and deployed
  consumer acceptance have not run. Python 3.12 SQLite deprecation warnings remain.
