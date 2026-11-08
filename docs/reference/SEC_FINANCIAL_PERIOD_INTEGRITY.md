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


## Draft continuation: snapshot-aware consumers (2026-11-08)

The additional `sec_financial_current_20261108` migration creates
`capital_flows_current`. Ledger rows are retained unchanged. For actors with a
snapshot head, this read projection admits supported direct annual/quarter
financial rows only when every financial/provenance field matches that head's
normalized evidence. Missing metrics cannot fall back to an unmarked SEC row,
seed, announcement-folded estimate, or differently cased actor spelling. Other
actors, explicit counterparties, announcement events, and unsupported flow types
remain outside this snapshot authority. Supported direct annual/quarter facts
are deduplicated before consumers aggregate them, also for legacy-only actors.

Read-only financial consumers use the shared projection. Writers and explicit
historical auditing still use `capital_flows`. This is a current-truth read
contract, not a rewrite or a point-in-time history reconstruction. Existing
materialized reports/caches still require explicit invalidation at rollout.

TTM state adds `sec_snapshot_id`. A snapshot-managed TTM row is readable only
when it is `ttm_rollup` output computed against the current head. The rollup SQL
reads qualified quarters and records the observed snapshot identity in the same
statement/transaction as its fingerprint and outputs. Head changes hide stale
TTM immediately, even when normalized values are unchanged. First empty heads
also enter the dirty-actor set so obsolete physical rollups can be removed. An
old racing recompute is hidden until a current recompute succeeds. This does not
make the raw ledger a safe read interface or coordinate unrelated external
writers automatically.

The new migration must precede the corresponding consumer code. There is no
silent raw-ledger fallback if the view is missing. Downgrading this read-view
migration does not remove archived snapshots, but requires reverting consumer
code together; deployment/rollback must be coordinated. Privileged mutation of
archive/head/state remains outside the application integrity guarantee.

Before rollout, validate query plans and representative production-scale latency:
the view checks membership against normalized JSON and ranks direct facts. The
disposable fixture proves correctness, not acceptable production query costs.
Historical repair remains a separate explicitly approved operation. No repair
executor or blanket correction of the 798-row proposal is included here.


Missing-input guards now require every term used by composite financial ratios
and scores; an explicit reported zero remains valid. Incomplete fundamental
scores do not emit a new trading verdict. This does NOT erase prior persisted
`fundamental_divergence` rows: its snapshot writer only upserts newly produced
rows, and the existing actor-detail endpoint can still select its latest older
record. Persisted derivative invalidation/source binding and API/percentile
cache invalidation remain mandatory rollout gates, not solved by these guards.

A disposable full-NVDA fixture retained 587 qualified rows beside 587 synthetic
legacy duplicates. The final relational evidence join returned exactly 587
current rows; its annual aggregate took about 17 ms on the local private cluster
(one illustrative run, not a production latency promise). The earlier correlated
JSON membership variant took about 128 ms and was replaced. Production-scale
query planning and concurrency remain separate acceptance checks.


Continuation validation: 418 selected offline tests passed, 8 optional/integration
cases skipped, and 2 subtests passed with external sockets and database discovery
blocked. All 24 disposable PostgreSQL tests passed, including a real two-connection
race: an H1 rollup was held at its insert, H2 committed, and H1 then finished last.
The read projection hid that stale TTM until the next H2 recompute. Changed-file
Ruff E/W/F (excluding E501), compilation and whitespace checks passed. Full CI on
the newly published head and actual deployment acceptance are still separate gates.


The first continuation CI run caught two clean-run-only issues: missing raw-SQL
ledger bootstrap in schema.sql/Alembic replay, and unordered set parametrization
under pytest-xdist. The migration now creates the canonical empty capital-flow
ledger (including currency/indexes/minimum app grants) only when it is absent;
it never replays raw 0024's data backfill/deduplication against an existing ledger.
A new disposable regression checks both the absent-table path and preservation
of a populated ledger. All 25 private-PG tests pass. Test IDs are now sorted and
collection order was verified under three different Python hash seeds. These
corrections require fresh exact-head CI; earlier-head results do not qualify them.
