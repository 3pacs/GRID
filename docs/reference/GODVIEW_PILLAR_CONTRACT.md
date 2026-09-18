# God View pillar contract — CFTC positioning, Fed net liquidity, commodity warehouses (W6)

**Status:** adopted 2026-09-18, originally for the CFTC positioning pillar (sections 1-10)
and extended the same day (Slice B) to the Fed net liquidity pillar (section 11) and the
commodity warehouse pillar (section 12). This is the reference contract every other God
View pillar (FINRA short volume, SEC Reg SHO FTD, buyback blackouts, dealer GEX) must
satisfy before it is wired into `GET /api/v1/godview/pillars/<pillar>` — see the status
table near the bottom for exactly which pillars are built and why the rest aren't yet.

Builds on `docs/reference/AVAILABILITY_CONTRACT.md` (whole-result available/partial/
unavailable) and `store/availability_fields.py`'s `FieldRecord` (per-field availability +
provenance). Nothing here overrides either.

## 1. Observation period

`report_date` — always a **Tuesday**. The CFTC's Commitments of Traders (COT) "futures
only" report is a snapshot of open positions as of the close of business on Tuesday each
week. `ingestion/altdata/cftc_cot.py` already writes this as `obs_date` in `raw_series`
under `cftc.<CONTRACT>.<metric>` series ids (e.g. `cftc.SP500.net_speculative`); the
pillar reuses that column unchanged.

## 2. Publication

Per the CFTC's own release-schedule page
(<https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm>,
fetched 2026-09-18):

> "The Commitments of Traders reports are released at 3:30 p.m. Eastern time. The
> Futures Only reports and Futures and Options Combined reports are usually released
> on Friday. The release usually includes data from the previous Tuesday."

So: a report with `report_date` = Tuesday `T` is published on the Friday three days
later, `T + 3 days`, at 15:30 ET. `godview/cftc_pillar.py::compute_release_date()`
encodes exactly this rule and nothing more — it does not special-case holiday shifts,
because the CFTC page does not document one and inventing a holiday calendar here would
be exactly the kind of unstated assumption `store/availability.py` exists to prevent.
`release_date` is stored as a `DATE` (the Friday), matching the `as_of=YYYY-MM-DD`
granularity of the API below; the 15:30 ET instant is documented here, not modelled as a
second timestamp column, to avoid a column nothing reads at day granularity.

`available_at` (`TIMESTAMPTZ`, row-level) is a **different, narrower** concept: the
first moment *this system* could have used the row, taken from the puller's own
`raw_series.pull_timestamp` — specifically the earliest `pull_timestamp` across the five
raw metrics (`comm_positions_long_all`, `comm_positions_short_all`,
`noncomm_positions_long_all`, `noncomm_positions_short_all`, `open_interest_all`) that
make up one `(contract, report_date)` row. `available_at` can be *after* `release_date`
(GRID's puller ran late) but should never need to be *before* it for data acquired going
forward; a puller that back-fills history will naturally show `available_at` values
long after every historical `release_date`, which is expected and not a bug.

## 3. Units

**Contracts** — every raw positioning column (`total_open_interest`,
`commercial_long/short/net`, `noncommercial_long/short/net`) is a count of futures
contracts, exactly as CFTC reports them. `spec_net_pct_oi` is a percentage (0-100) of
open interest, not a contract count. Z-scores and percentile ranks are dimensionless.

## 4. Provenance — measured vs. derived

Two provenance concepts exist at two different layers and must not be conflated:

* **Row-level `provenance` column** (added by this migration, `TEXT`, nullable) is a
  coarse per-row tag. For the CFTC pillar it is always `"measured"` — the row's primary
  content is the five raw positioning fields taken directly from the CFTC report. This
  column exists so a DB-level audit query can filter "rows backed by a direct
  observation" without joining anything.
* **Per-field provenance**, surfaced only at the API layer via `FieldRecord`
  (`store/availability_fields.py`), is where "measured for raw positions, derived for
  z-scores" is actually expressed:
  * `total_open_interest`, `commercial_long/short/net`, `noncommercial_long/short/net`,
    `spec_net_pct_oi` → `FieldRecord(availability="available", provenance="measured", ...)`.
  * `z_score_1y`, `z_score_3y`, `percentile_3y`, `crowding_regime` →
    `FieldRecord(..., provenance="derived", ...)` when computed, or
    `unavailable_field(stale_reason=...)` when the trailing window is too short
    (`_MIN_HISTORY = 8` weekly observations; see `godview/cftc_pillar.py`).
  * **Window, stated explicitly:** `z_score_1y` uses the trailing 52 weekly
    observations (or fewer if history is shorter than 52 but at least
    `_MIN_HISTORY`); `z_score_3y` and `percentile_3y` use the trailing 156 weekly
    observations on the same basis. These windows are the same constants
    `intelligence/cot_extremes.py` already uses (`_Z_SCORE_WINDOW = 52`,
    `_PERCENTILE_WINDOW = 156`), imported rather than re-picked, so the two modules
    never silently disagree about what "1 year" or "3 years" of weekly COT data means.
  * The row's own `coverage_fraction` column (see §6) measures exactly this — how much
    of the ideal 156-week derived-window history was actually available for that row's
    z-scores — and is a different number from the pillar-level `coverage` in §6 of the
    API contract (§8 below).

## 5. Stale rule

A CFTC pillar read is **stale** when the most recent qualifying report's `release_date`
is more than **10 days** before `as_of`. Derivation: the report cadence is 7 days
(Tuesday to Tuesday) plus the up-to-3-day publication lag (Tuesday to Friday) — 10 days
covers exactly one full cycle. Anything older than that means a report was missed, not
just "not yet published this week." `STALE_AFTER_DAYS = 10` in `godview/cftc_pillar.py`.

## 6. Coverage

Two distinct coverage numbers exist; both are honest fractions, never defaulted to 0 or 1:

* **Pillar-level (API top-level `coverage`)** = (number of the 4 tracked CFTC contracts
  with a qualifying row as of `as_of`) / 4. The v1 CFTC pillar tracks exactly the four
  contracts the pre-existing `market_god_view_daily` materialized view already joins by
  `contract_code` (`ES`, `ZN`, `GC`, `CL` — S&P 500 e-mini, 10-Year Note, Gold, WTI
  Crude), so this pillar's coverage denominator matches what the rest of GRID already
  calls "the CFTC contracts God View knows about" instead of inventing a new list.
* **Row-level `coverage_fraction` column** = (number of weekly observations actually
  available for that row's derived-window computation) / 156, capped at 1.0. `NULL`
  when no derived computation was attempted (history shorter than `_MIN_HISTORY`).

## 7. Generation id and atomic publication

Every materializer run gets a fresh `generation_id` (`uuid4`, stored as `TEXT` on both
`cftc_positioning_daily` rows it writes and the bookkeeping table `godview_generations`
this migration also creates).

**Chosen design: a single database transaction**, not a separate "final UPDATE" step.
`godview/cftc_pillar.py::materialize_cftc_pillar()` opens one `engine.begin()` block that
(a) inserts every new `(report_date, contract_code)` row this run discovered (existing
rows for dates already persisted are left untouched — INSERT only, per §9), and (b) as
the last statement in that same transaction, upserts the `godview_generations` bookkeeping
row to `status='complete'` with `published_at=NOW()`. Postgres's own transaction
isolation means no other connection can see (a) partially — either both (a) and (b)
commit together, or an exception anywhere in the block rolls back the whole transaction
and neither the new rows nor a "complete" bookkeeping row ever exist. This is strictly
stronger than "insert rows now, mark complete with a later UPDATE": there is no window,
however small, in which a reader could see newly-inserted rows whose generation has not
yet been marked complete.

`cftc_positioning_daily` keeps its existing `UNIQUE (report_date, contract_code)`
constraint (unchanged by this migration), so a "generation" here is **not** a full
snapshot-replace of the table — it is the incremental set of new report_dates one run
persisted. `godview_generations` is therefore a per-run audit/health record ("did the
last run succeed, and what did it add"), while a strict-PIT read (§8) still queries the
full, ever-growing `cftc_positioning_daily` table directly, exactly as `store/pit.py`
queries all of `resolved_series` rather than one "generation" of it. This is the only
design available without either changing the unique constraint or allowing
UPDATE/DELETE on existing rows, both of which are out of scope for this migration.

## 8. Quarantine of rows without a release_date

`release_date` is set **only** when `report_date.weekday() == 1` (Tuesday) — the one
case the CFTC page documents. When that holds, `source_ref` records the rule that was
applied (e.g. `"cftc_release_schedule_v1: report_date is Tuesday -> release_date =
report_date + 3d (Friday 15:30 ET, https://www.cftc.gov/MarketReports/
CommitmentsofTraders/ReleaseSchedule/index.htm)"`). When `report_date` is not a Tuesday —
a data anomaly, since the puller itself only ever stores Tuesday `obs_date`s, but the
rule must hold regardless of upstream cleanliness — `release_date` stays `NULL` and
`source_ref` records why (`"report_date <weekday> is not Tuesday; release_date withheld
per cftc_release_schedule_v1 quarantine rule"`).

**Rows with `release_date IS NULL` are never deleted.** They stay in
`cftc_positioning_daily` permanently for audit/backfill purposes. They are excluded only
from *strict-PIT* reads: `GET /api/v1/godview/pillars/cftc?as_of=` filters
`WHERE release_date IS NOT NULL AND release_date <= :as_of`. A row lacking a
`release_date` is quarantined from that one query path, not erased from the table.

## API states (`GET /api/v1/godview/pillars/cftc?as_of=YYYY-MM-DD`)

| condition | response |
|---|---|
| `godview_generations` has no row for `pillar='cftc_positioning'` | `unavailable(reason="never_configured")`, HTTP 200 |
| no complete generation has ever produced a qualifying row (`release_date <= as_of`), and the latest attempt failed | `unavailable(reason="materializer_failed")`, HTTP 200 |
| a complete generation exists; qualifying rows found for < 4 of the 4 tracked contracts | `available=true`, `coverage < 1.0` |
| a complete generation exists; the newest qualifying `release_date` is more than 10 days before `as_of` | `available=true`, `stale_reason="stale"` |
| a complete generation exists; ≥1 qualifying row, no staleness | `available=true`, `coverage` as computed |

"Qualifying" as of section 10 (2026-09-18 addendum) also requires
`availability_basis = 'observed_acquisition'` unless the caller passes
`include_inferred=true` — see section 10 below; `coverage` is computed over
whichever set that query parameter selected, never over rows the request
excluded.

The endpoint never raises a 500 for a missing table — `to_regclass` / a caught
`ProgrammingError` on `cftc_positioning_daily` or `godview_generations` degrades to the
`never_configured` unavailable state.

## 9. Idempotent re-run

Re-running the materializer with unchanged upstream `raw_series` data inserts zero new
rows (every `(report_date, contract_code)` it would produce already exists) and records
a new `complete` generation with `row_count=0` — a legitimate, side-effect-free
re-confirmation, not an error. A run that finds **zero** usable history for **every**
tracked contract (not just "nothing new") is treated differently — see §7's failure
path — because that means the upstream adapter itself produced nothing, not that this
run happened to be a quiet week.

## 10. Availability basis — observed vs. inferred vs. unknown

**Addendum, 2026-09-18 (operator direction, Slice A):** "a published CFTC release
schedule alone does not establish historical availability for revised or backfilled
records." Section 8's `release_date` says WHEN a report was scheduled to publish, purely
from the rule (Tuesday → Friday+3d). It says nothing about whether GRID actually *saw*
that publication happen, versus reconstructing the date years later from a backfill run.
`availability_basis` is the answer to that second question, stored per row (nullable
`TEXT` on `cftc_positioning_daily`, added by `migrations/versions/godview_avail_basis_0918.py`,
CHECK-constrained to the three values below) and exposed per field at the API layer.

| value | meaning |
|---|---|
| `observed_acquisition` | The puller's own `pull_timestamp` (→ `available_at`) lands within `AVAILABILITY_BASIS_TOLERANCE_DAYS` (3 days) on-or-after `release_date`, and `raw_series` shows exactly one pull for this `(contract, report_date)`. We actually watched this get published. |
| `inferred_schedule` | Either the pull landed more than 3 days after `release_date` (a historical backfill — the schedule, not an observation, is placing the date), or `raw_series` shows **more than one** distinct `pull_timestamp` for this report_date (CFTC revised the report, or GRID re-pulled it). A revised/backfilled record is **never** labelled `observed_acquisition` for the original release, even if the winning (latest) pull's own timing alone would otherwise look on-schedule. |
| `unknown` | No `release_date` at all (section 8's non-Tuesday quarantine — nothing to compare against), no `available_at` at all, or an acquisition implausibly (>1 day) *before* the schedule says the report could have existed. |

`godview/cftc_pillar.py::classify_availability_basis` is the pure classifier;
`_distinct_pull_counts` supplies the revision signal by counting every distinct
`pull_timestamp` `raw_series` has ever recorded for a report_date (not just the winning
one `_read_contract_history`'s `LATEST_AS_OF` pick keeps) — within one real puller run
every metric shares one transaction-constant `NOW()`, so more than one distinct timestamp
only happens across two separate runs.

**Strict-PIT reads default to `observed_acquisition` rows only.** `read_cftc_pillar(...,
include_inferred=False)` (the default) adds `AND availability_basis = 'observed_acquisition'`
to the query; `include_inferred=True` admits `inferred_schedule`/`unknown` rows too, and
every admitted row still carries its own `availability_basis` — the API never silently
upgrades an inferred row to "observed." The route is
`GET /api/v1/godview/pillars/cftc?as_of=...&include_inferred=true`.

**Per-field exposure:** each field's `FieldRecord.to_dict()` output (section 4) carries
two additional sibling keys, `availability_basis` and `availability_basis_note` — added
beside the vendored `FieldRecord` output rather than inside it, since that module is
vendored verbatim from `feat/availability-provenance-contract` and must not diverge
further before that branch merges. `availability_basis_note` is `None` exactly when the
basis is `observed_acquisition`; otherwise it is the fixed string
`"availability inferred from schedule; record revised/backfilled"` (inferred) or
`"acquisition observed before the scheduled release; basis unclear"` (unknown).

**Why two vintages of one row can't literally coexist:** `cftc_positioning_daily` keeps
its `UNIQUE (report_date, contract_code)` constraint unchanged, and rows are INSERT-only
(section 7) — so a "revised report_date" scenario is tested as two separate report_dates
side by side (one pulled once, on schedule → observed; one `raw_series` shows was pulled
twice → its single materialized row is the later, inferred-labelled vintage), not as two
rows for the same report_date. See `tests/godview/test_cftc_pillar_pure.py`'s
`test_availability_basis_revised_vs_first_release_side_by_side` and
`tests/godview/test_cftc_pillar_db.py`'s
`test_availability_basis_revised_report_date_labels_the_materialized_vintage_inferred`.

## 11. Fed net liquidity pillar (Slice B)

`godview/fed_liquidity_pillar.py`. Net Liquidity = WALCL − WTREGEN − RRPONTSYD, read
directly from the raw FRED series in `raw_series` (`ingestion/fred.py`, read-only) — NOT
from `ingestion/altdata/fed_liquidity.py`'s `COMPUTED:fed_net_liquidity`, which has a real
unit bug (flagged separately, not fixed here; see that module's docstring cross-reference).

**Units, confirmed via WebFetch 2026-09-18, never assumed:**

| series | FRED units page | quote |
|---|---|---|
| WALCL | fred.stlouisfed.org/series/WALCL | "Millions of U.S. Dollars, Not Seasonally Adjusted" |
| WTREGEN | fred.stlouisfed.org/series/WTREGEN | "Millions of U.S. Dollars, Not Seasonally Adjusted" |
| RRPONTSYD | fred.stlouisfed.org/series/RRPONTSYD | "Billions of US Dollars, Not Seasonally Adjusted" |

`net_liquidity_usd_m` (the tracked table's own column name) is therefore computed as
`WALCL - WTREGEN - (RRPONTSYD * 1000)` — the `RRP_BILLIONS_TO_MILLIONS` scale factor is
explicit in code, never implicit.

**Release schedule, quoted 2026-09-18** from
<https://www.federalreserve.gov/releases/h41/about.htm>:

> "The H.4.1 statistical release, 'Factors Affecting Reserve Balances of Depository
> Institutions and Condition Statement of Federal Reserve Banks,' is typically published
> on Thursday afternoon around 4:30 p.m."

WALCL and WTREGEN are both confirmed (via WebFetch on their own FRED pages) as
"Weekly, as of / ending Wednesday" — so `obs_date` (Wednesday) → `release_date` =
obs_date + 1 day (Thursday), set only when `obs_date` really is a Wednesday; otherwise
withheld, same quarantine pattern as CFTC's Tuesday rule.

**No fallback constants:** RRPONTSYD is a genuinely daily series, but this pillar takes
ONLY the exact Wednesday's value — a missing exact-day observation (a holiday, a gap)
leaves that whole Wednesday unmaterialized rather than substituting the nearest day.

**Per-component availability basis and age:** unlike the CFTC pillar (one row-level
`availability_basis`), this pillar stores THREE additional pull timestamps
(`walcl_pulled_at`, `wtregen_pulled_at`, `rrp_pulled_at`) so the API can report each raw
component's own basis/age independently, using the same shared
`godview/availability_basis.py::classify_availability_basis` — the row-level
`availability_basis` column is the worst case across all three (any component inferred
→ the row is inferred).

`forward_impulse_score` (a column on the tracked table) is always `NULL` in this slice —
no forward-return model has been built or validated for it; `NULL` says "not implemented,"
never a fabricated number.

## 12. Commodity warehouse pillar (Slice B)

`godview/commodity_warehouse_pillar.py`. Two independent fields:

* **LME cancelled-warrant ratio** — reuses (read-only) `ingestion/altdata/lme_warehouse.py`'s
  own `raw_series` writes (`lme:stocks_total_mt:<metal>`, `lme:stocks_cancelled_mt:<metal>`,
  `lme:stocks_live_mt:<metal>`, `lme:cancelled_ratio:<metal>`) — the puller already computes
  the ratio; this pillar reads it rather than recomputing it, so the two can never disagree.
* **Cushing, OK crude stocks** — grepped the whole ingestion tree 2026-09-18 for a real EIA
  Cushing series id (e.g. `WCSSTUS1`) and found **none**. `WCESTUS1`
  (`ingestion/altdata/refinery_cracks.py`) looks similar but is US refiner **gasoline**
  stocks, not Cushing crude — using it would silently substitute the wrong quantity, exactly
  the defect `AVAILABILITY_CONTRACT.md` calls out by name. Per the operator's instruction,
  this field is **permanently** `unavailable(reason="never_configured")` — `CUSHING_SERIES_ID`
  is `None` on purpose, and no literal placeholder stands in for it anywhere in the code.

**No cited release schedule:** unlike CFTC/Fed, no official LME publication-schedule page
was found (`ingestion/altdata/lme_warehouse.py`'s own docstring says the same). Rather than
assume a schedule exists, `release_date` is always `NULL` and `availability_basis` is
always `'unknown'` for this pillar's rows — an honest admission, not a bug. The strict-PIT
read therefore bounds results by `report_date <= as_of` only (the one temporal fact this
pillar actually has), with no `include_inferred` gate (there is nothing for that flag to
admit/exclude here).

## 13. FINRA short-volume pillar

`godview/finra_short_volume_pillar.py`. Consumes (read-only) ``raw_series`` under
``finra:short_volume:<symbol>:<market>``, written by
`ingestion/altdata/finra_short_volume.py` — that module lives on
`origin/fable/sources-finra-ftd-20260918` (commit `40a9f1ae`), not merged into
this branch; read via `git show`, never checked out.

**Emphatically not short interest, never a squeeze score.** Per FINRA's own
catalog page (https://www.finra.org/finra-data/browse-catalog/short-sale-volume):

> "Short Sale Files do not — and are not intended to — equate to bi-monthly
> reported short interest position information. The short interest data
> reflects short positions held by market participants at a specific moment
> in time on two discrete days each month, while the Daily File reflects the
> aggregate volume of short trades effected on each trade date..."

`short_ratio` = `short_volume / total_volume` for that trade date — a
description of that day's trading mix, nothing more.

**Release schedule**, quoted 2026-09-18 via WebFetch against
https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files:

> "FINRA posts the Daily Short Sale Volume Files to this no later than
> 6:00:00pm ET of the same day on the relevant trade date."

So `release_date = trade_date` always (no weekly gate, unlike CFTC/Fed).

**Missing input, named exactly:** `FINRAShortVolumePuller` is deliberately
NOT registered in `ingestion/scheduler.py` — unauthorised live pulls. This
pillar's materializer reads whatever rows already exist in `raw_series`
(a manual pull, a fixture); with none, `GET /api/v1/godview/pillars/finra_short_volume`
returns `unavailable(never_configured)` — realistically, in production
today, that is the state this endpoint returns, and that is correct, not
a bug. `coverage` here means "of the symbols `raw_series` currently
offers, how many produced a valid ratio row" — there is no curated
watchlist to define a target universe against, because no scheduled pull
has ever populated one.

## 14. SEC Fails-to-Deliver (FTD) pillar

`godview/sec_ftd_pillar.py`. Consumes (read-only) `raw_series` under
`sec:ftd_balance:<cusip>`, written by `ingestion/altdata/sec_ftd.py` on
`origin/fable/sources-finra-ftd-20260918` (commit `40a9f1ae`, not merged
into this branch; read via `git show`).

**Each row is an outstanding balance as of one settlement date — never
summed across dates, no T+35 buy-in timeline, no squeeze score.** Per the
SEC's own page (https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data):

> "The values of total fails-to-deliver shares represent the aggregate net
> balance of shares that failed to be delivered as of a particular
> settlement date."

> "Fails-to-deliver can occur for a number of reasons on both long and
> short sales. Therefore, fails-to-deliver are not necessarily the result
> of short selling, and are not evidence of abusive short selling or
> 'naked' short selling."

`mandatory_buyin_date`/`days_remaining`/`squeeze_risk_score` (already
columns on the tracked `sec_regsho_ftd_cns` table) stay permanently NULL.

**"Age"** means `as_of - settlement_date` — how stale the observation is,
computed at read time, never persisted. Explicitly NOT an attempt to
determine the age of the underlying fails: the same SEC page states "the
age of fails cannot be determined by looking at these numbers."

**Display symbol:** no CUSIP→ticker mapping table exists in `schema.sql`
(grepped 2026-09-18) — but `sec_ftd.py`'s own `raw_payload` already
carries the FTD file's self-reported `symbol` per row. This pillar uses
that (measured, same source) when present; when a row's payload lacks a
usable symbol, it exposes the CUSIP itself and labels the row
`ticker_source=cusip_fallback` rather than guessing.

`closing_price` (measured, from the FTD file's own PRICE field) and
`total_failed_usd` (derived = `failed_shares * closing_price`, same
settlement date only) are populated when a price is present; `None`
otherwise.

**Release schedule** — quoted 2026-09-18 from the same SEC page:

> "The first half of a given month is available at the end of the month.
> The second half of a given month is available at about the 15th of the
> next month."

`release_date` is therefore INFERRED from this half-month rule (there is
no published release calendar to observe directly); `availability_basis`
uses a wider tolerance (10 days, vs. 3 elsewhere) reflecting the SEC's own
approximate language ("about the 15th").

## 15. Corporate buyback blackout pillar

`godview/buyback_pillar.py`. Implements ONLY what is measurable or
explicitly modeled:

* **Measured input:** earnings dates from `earnings_calendar` — a lazily
  created, untracked table (`ingestion/altdata/earnings_calendar.py::
  _ensure_earnings_table`, not in `schema.sql` or any migration; grepped
  2026-09-18, the only earnings/catalyst-dated table anywhere in this
  codebase). When an issuer has no earnings date there, its window is
  unavailable — nothing to derive it from, never invented.
* **Modeled output:** a per-issuer quiet-window calendar, `earnings_date − 14
  calendar days` through `earnings_date + 2 calendar days`. `provenance` is
  always `'modeled'`.

**This window is NOT an SEC-mandated rule for issuers — a documented
assumption, disclosed in every row's `source_ref`.** Rule 10b5-1's 2022
amendments impose a cooling-off period on directors/officers, quoted from
SEC Chair Gensler's statement
(https://www.sec.gov/newsroom/speeches-statements/gensler-insider-trading-20221214):

> "90 days or two days after the release of financial statements,
> whichever is longer, but no more than 120 days"

but the same statement says plainly:

> "we are not adopting a cooling-off period for issuers"

So this pillar's window models the common Rule 10b-18 self-imposed
compliance PRACTICE many issuers follow, not an SEC requirement — never
presented as measured fact.

**Explicitly never computed: any dollar amount, any "% of market in
blackout."** Those require issuer-level repurchase EXECUTION disclosures
(10-Q/10-K share-repurchase tables via EDGAR), which do not exist
anywhere in this database — named exactly as `MISSING_INPUT`, returned in
every API response. The tracked `corporate_buyback_blackouts` table
(market-wide `sp500_cap_blackout_pct`/`active_corporate_bid_m`) is left
completely untouched — this pillar writes to a NEW table,
`issuer_buyback_blackout_windows`, because the aggregate table's grain
cannot represent a per-issuer figure without blurring measured vs.
assumed.

## 16. Dealer gamma exposure (GEX) pillar

`godview/dealer_gex_pillar.py`. A from-scratch engine over the tracked
`options_snapshots` table — the untracked, incident-evidence
`derivatives/dealer_gex_engine.py` in the sibling `GRID` checkout was never
read (explicitly out of bounds for this lane's read-only-cross-branch
rules). Every output field is `provenance='modeled'`; nothing here is
`measured` (except the resolved spot price, which is a real PIT-resolved
price read through `store/pit.py`).

**Sign convention (a stated modeling assumption, not a derivation):**
`options_snapshots` carries no real dealer/customer position split, so
dealers are modeled net short the customer side of both calls and puts.
Following the standard public GEX methodology, CALL open interest
contributes **+gamma** and PUT open interest contributes **-gamma** to net
dealer exposure at each strike — disclosed verbatim in every API response
as `sign_convention_note` (`SIGN_CONVENTION_NOTE`).

**Gamma:** hand-rolled Black-Scholes Gamma, `r=0`, `q=0` (both assumed
zero, a standard simplification for gamma specifically — disclosed as
`gamma_assumptions_note`). `implied_vol` is read directly from
`options_snapshots`, never solved for or defaulted; a contract with a
missing/non-positive IV, or `expiry <= snap_date`, is skipped and counted
against `coverage_fraction` (`contracts_used`/`contracts_present`), never
substituted with a literal (e.g. never a 0.25 IV default).

**Spot price:** resolved via the same candidate-name rule
`api/routers/watchlist_helpers.py::_resolve_feature_names` uses, then read
through `store/pit.py::PITStore.get_pit` (`LATEST_AS_OF`) — replicating
(not importing, since it lives on the unmerged
`origin/fable/signal-eval-20260918`) `evaluation/prices.py`'s own resolver:
zero or multiple `feature_registry` matches, or no PIT-available price,
means the WHOLE ticker/date is `unavailable` — never a median-strike or
other proxy for a real spot.

**Gamma flip:** cumulative net gamma across strikes in ascending order; the
flip is the linearly interpolated strike where that cumulative sum's SIGN
changes. No sign change anywhere in the chain → `gamma_flip_strike:
unavailable` — never an endpoint strike, never a guess. Validated against
three synthetic cases in `tests/godview/test_dealer_gex_pillar_pure.py`: a
symmetric put/call chain (flip lands between the two strikes, near spot),
an all-calls chain (never crosses → `unavailable`), and a chain with half
its contracts missing IV (coverage < 1, those contracts skipped rather
than defaulted).

**Numerical validation against a real, sourced chain (2026-09-18):**
`tests/godview/test_dealer_gex_validation.py` runs this engine's
`_compute_gex_for_chain` against a live SPY options chain captured via
`yfinance` (`tests/godview/fixtures/options_chain_SPY_20260918.json`;
exact provenance — source URL/API, capture timestamp, spot, expiry — in
the sidecar `options_chain_SPY_20260918.SOURCE.md`), and compares it to
an INDEPENDENTLY written expected-value calculation (its own
`math.erf`-based normal pdf, its own Black-Scholes gamma, its own
gamma-flip/max-pain/ATM-IV/put-call-ratio code — none of it imported from
the engine). Net/call/put GEX, gamma-flip strike, max pain, put/call OI
ratio, and ATM IV agree within a stated `1e-6` relative tolerance; the
engine's `contracts_used`/`contracts_present` match the test's own count
of usable rows exactly. **What this proves:** the engine's math is
numerically correct under its stated, disclosed assumptions (r=q=0,
chain-reported implied_vol, the call-positive/put-negative sign
convention, a 100-share contract multiplier), on a real chain, not just
on the three synthetic cases below. **What this does NOT prove: any
claim that a figure here matches a real dealer's actual book.**
`options_snapshots` still carries no dealer-vs-customer position split
at all — no table, no column, nowhere in this database records who
actually holds which side of an option — so the "dealers are short both
sides" convention remains a MODELED assumption, never a measurement.
Every field this pillar produces stays `provenance='modeled'` for
exactly that reason (checked by
`test_router_still_reports_gex_fields_as_modeled`, not just asserted in
prose), named exactly as `MISSING_INPUT`, returned in every API response
as `missing_input`: a claim of accuracy against real dealer positioning,
as opposed to internal mechanical correctness (which the sourced-chain
test above and the three synthetic cases below both validate).

## Operational readiness of all seven God View pillars (2026-09-18)

Seven implementations are not seven working production feeds.

| Pillar | Implemented | Adapter verified | DB/API tested (real PG) | Model validated | Scheduled | Fresh data observed | Production verified |
|---|---|---|---|---|---|---|---|
| CFTC positioning | yes | existing puller; live not re-verified this cycle | yes (run 4, composition d9a960ab, 162/0/0) | n/a (measured or simple derived, unit-tested) | no | no | no |
| Fed net liquidity | yes | existing FRED puller; units fix a828f4bf; live not re-verified | yes (run 4, composition d9a960ab, 162/0/0) | n/a (measured or simple derived, unit-tested) | no | no | no |
| Commodity warehouses | yes | LME puller registered but has "never written a row" (unexplained, pass-1 finding); Cushing leg `never_configured` | yes (run 4, composition d9a960ab, 162/0/0) | n/a (measured or simple derived, unit-tested) | no | no | no |
| FINRA short volume | yes | parser verified on one real captured file (#564); puller unscheduled | yes (run 4, composition d9a960ab, 162/0/0) | n/a (measured or simple derived, unit-tested) | no | no | no |
| SEC Reg SHO FTD | yes | parser verified on one real captured zip (#564); puller unscheduled | yes (run 4, composition d9a960ab, 162/0/0) | n/a (measured or simple derived, unit-tested) | no | no | no |
| Corporate buyback blackouts | yes | no adapter — consumes untracked `earnings_calendar` | yes (run 4, composition d9a960ab, 162/0/0) | modeled window, not validatable (no measured source) | no | no | no |
| Dealer GEX | yes | consumes `options_snapshots`; its writer not verified this cycle | yes (run 4, composition d9a960ab, 162/0/0) | numerically validated on a sourced chain, commit `b5782a8a` (this cycle) — dealer positioning NOT validated (modeled) — see section 16 | no | no | no |

Every God View pillar named in this contract is **implemented** in code
(2026-09-18). "Implemented" means exactly the "Implemented" column above
— it does not mean scheduled, activated, or deployed: every pillar reads
"no" across Scheduled / Fresh data observed / Production verified,
because nothing in this row is scheduled, activated, or deployed. Any
pillar name this router does not recognize still renders the honest "not
built yet" state via `api/routers/godview_pillars.py`'s
`_KNOWN_UNBUILT_PILLARS` map / catch-all route (currently empty, kept for
future pillars) — never a silent 404, never a fabricated value.
