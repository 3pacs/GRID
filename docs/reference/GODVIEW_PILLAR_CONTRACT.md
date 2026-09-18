# God View pillar contract — CFTC positioning (W6, first slice)

**Status:** adopted 2026-09-18 for the CFTC positioning pillar. This is the reference
contract every other God View pillar (FINRA short volume, SEC Reg SHO FTD, commodity
warehouses, Fed net liquidity, buyback blackouts, dealer GEX) must satisfy before it is
wired into `GET /api/v1/godview/pillars/<pillar>` — see "What remains for other pillars"
at the bottom.

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

## What remains for other pillars

FINRA short volume, SEC Reg SHO FTD, commodity warehouse inventories, Fed net liquidity,
corporate buyback blackouts, and dealer GEX are **not built** in this slice. Their tables
already exist (same tracked `god_view_market_tables_20260918` migration) but have none of
the PIT columns this migration adds to `cftc_positioning_daily`, no materializer, and no
route — `GET /api/v1/godview/pillars/<name>` for any of them, and the corresponding card
in `GodViewPillars.jsx`, must render the honest "not built yet — no data" state rather
than silently 404 or fabricate a number.
