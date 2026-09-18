# W5b — FINRA Daily Short Sale Volume + SEC FTD: source contracts

Branch: `fable/sources-finra-ftd-20260918` (base `origin/main` @ `a2f71acf`)
Status: **contract-first, not activated.** Neither puller is imported by
`ingestion/scheduler.py`. A proposed (unapplied) registration patch is at
`fable-w5b-scheduler-registration.patch` in this same directory.

New files:

- `ingestion/altdata/finra_short_volume.py` — `FINRAShortVolumePuller`
- `ingestion/altdata/sec_ftd.py` — `SECFTDPuller`
- `tests/fixtures/sources/finra_short_volume/**`, `tests/fixtures/sources/sec_ftd/**`
- `tests/test_source_finra_short_volume.py`, `tests/test_source_sec_ftd.py`

No calls were made to any FINRA or SEC API/download endpoint while building
this. `WebFetch` was used only against the public documentation pages
quoted below.

## 1. The confusion this plan explicitly forbids

| Data set | What it is | Where it lives | Series-id namespace |
|---|---|---|---|
| FINRA daily short sale **volume** (this PR) | Aggregate share volume of short-sale trades *executed on a trade date* | `finra_short_volume.py` | `finra:short_volume:<symbol>:<market>` |
| FINRA short **interest** | Bi-monthly point-in-time short position snapshot | `finra_ats.py::pull_short_interest` (existing) | `finra.short_interest_total` |
| FINRA **ATS** (dark pool) volume | Weekly off-exchange trading volume | `finra_ats.py::pull_ats_volume` (existing) | `finra.ats_total_volume`, `finra.ats_dark_pct` |
| SEC fails-to-deliver **balance** (this PR) | Outstanding balance of fails as of a settlement date | `sec_ftd.py` | `sec:ftd_balance:<cusip>` |

The new colon-delimited namespaces (`finra:short_volume:*`, `sec:ftd_balance:*`)
are deliberately disjoint from the existing dot-delimited `finra.*`
namespace and from each other. `tests/test_source_sec_ftd.py::test_finra_short_volume_and_sec_ftd_never_share_series_ids`
pulls both fixtures into one fake store and asserts the resulting series-id
sets are disjoint and correctly prefixed.

## 2. FINRA Daily Short Sale Volume

**Documentation quoted:**

- `https://www.finra.org/finra-data/browse-catalog/short-sale-volume`

  > "Short Sale Files do not — and are not intended to — equate to
  > bi-monthly reported short interest position information. The short
  > interest data reflects short positions held by market participants at
  > a specific moment in time on two discrete days each month, while the
  > Daily File reflects the aggregate volume of short trades effected on
  > each trade date..."

  > "...aggregated volume by security for all short sale trades executed
  > and reported to a TRF, the ADF, or the ORF during normal market hours."

  Reporting facilities: **Trade Reporting Facility (TRF)** and
  **Alternative Display Facility (ADF)** ("for off-exchange trades in
  exchange-listed securities"), **Over the Counter Reporting Facility
  (ORF)** ("for trades in securities traded over-the-counter").

- `https://www.finra.org/sites/default/files/2021-07/DailyShortSaleVolumeFileLayout.pdf`
  ("Regulation SHO Daily Short Sale Volume File Layout", effective as of
  2011-02-28):

  > `Date|Symbol|ShortVolume|Short Exempt Volume|TotalVolume|Market`

  > "The first row of every file shall contain a header with the column
  > names described above. The last row of every file shall be a trailer
  > denoting the number of records produced for the file."

  > "In the event there is no short data for a particular day, a file
  > will still be produced for that day that will only contain a header
  > and a trailer with a count of '0'."

  Field table (verbatim from the PDF):

  | Field | Attribute | Description |
  |---|---|---|
  | Date | 8 numeric characters | Trade Date YYYYMMDD |
  | Symbol | Up to 14 characters | Security symbol |
  | Short Volume | Numeric, no decimals | Aggregate reported share volume of executed short sale and short sale exempt trades during regular trading hours |
  | Short Exempt Volume | Numeric, no decimals | Aggregate reported share volume of executed short sale exempt trades during regular trading hours |
  | Total Volume | Numeric, no decimals | Aggregate reported share volume of all executed trades during regular trading hours |
  | Market | 1 alpha character | Reporting Facility identifier: N=NYSE TRF, Q=NASDAQ TRF Carteret, B=NASDAQ TRF Chicago, D=ADF, O=ORF |

**Identifier / units / cadence table:**

| | |
|---|---|
| Identifier | Symbol (up to 14 chars) + 1-char Market code; series-id per (symbol, market) pair |
| Units | Shares (whole numbers, no decimals per the PDF) |
| Obs date | Trade date (YYYYMMDD in the file) |
| Cadence | One file per trade date ("Daily File"); mapped to DB `latency_class='EOD'` (closest allowed value — see gap below) |
| Revision | Not described in FINRA's docs as revised after publication → `revision_behavior='NEVER'` |
| Delimiter | Pipe (`\|`), 6 fields, header row + trailer row |

**Not confirmed without calling the API:** the exact production download
URL. `_DAILY_FILE_URL_TEMPLATE` in `finra_short_volume.py` is the
commonly-documented `cdn.finra.org` naming convention, not verified
against a live response. `_fetch_raw_text` raises `NotImplementedError`
unless a caller passes an explicit `url=`, so this can never be silently
trusted.

## 3. SEC Fails-to-Deliver (FTD)

**Documentation quoted (`https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data`):**

> "The values of total fails-to-deliver shares represent the aggregate net
> balance of shares that failed to be delivered as of a particular
> settlement date."

> "Fails to deliver on a given day are a cumulative number of all fails
> outstanding until that day, plus new fails that occur that day, less
> fails that settle that day."

> "The figure is not a daily amount of fails, but a combined figure that
> includes both new fails on the reporting day as well as existing fails."

> "The data items are provided as a 'pipe delimited' text file." Fields,
> in order: SETTLEMENT DATE (8 digits), CUSIP (9 characters), SYMBOL (10
> characters), QUANTITY/FAILS (unlimited), DESCRIPTION (30 characters),
> PRICE (unlimited).

> "The first half of a given month is available at the end of the month.
> The second half of a given month is available at about the 15th of the
> next month."

> "Fails-to-deliver can occur for a number of reasons on both long and
> short sales. Therefore, fails-to-deliver are not necessarily the result
> of short selling, and are not evidence of abusive short selling or
> 'naked' short selling."

> "We cannot guarantee the accuracy of the data" ... "the age of fails
> cannot be determined by looking at these numbers."

File names observed on the page follow a `cnsfails<YYYYMM><a|b>.zip`
pattern (e.g. `cnsfails202608a.zip`, `cnsfails202608b.zip`), where `a` =
first half of the month, `b` = second half.

**No T+35 timeline, no daily-fails summation:** per the quotes above, a row
is an *outstanding balance*, not new fails. `sec_ftd.py` never diffs or
sums balances across days and implements no forced-buy-in / T+35 logic —
that would require assumptions not supported by anything quoted above.

**Identifier / units / cadence table:**

| | |
|---|---|
| Identifier | CUSIP (9 chars); series-id per CUSIP |
| Units | Shares (outstanding balance, not a flow) |
| Obs date | **Settlement date** (from the file) — NOT the file's publication date |
| Cadence | Twice monthly: first half at month-end, second half ~15th of next month |
| Revision | Not explicitly documented either way; SEC disclaims accuracy generally. Marked `revision_behavior='RARE'` as an **assumption**, not a documented fact |
| Delimiter | Pipe (`\|`), 6 fields, inside a ZIP archive |

**Not confirmed without calling the API:**
- The exact absolute URL prefix for the zip files (only the file-naming
  convention was observed on the page; `_fetch_zip_bytes` raises
  `NotImplementedError` unless a caller passes an explicit `url=`).
- Whether a header row is always present (the parser detects one
  defensively — first field `SETTLEMENT DATE` — but treats its absence as
  fine, not fatal, since this wasn't confirmed either way).
- Whether/how corrections to a previously published half-month file are
  distributed (hence `RARE` rather than `NEVER`, flagged as an assumption).

## 4. `raw_series` schema gaps found

1. **No `release_date` column.** `ingestion/base.py::BasePuller._insert_raw`
   only writes `(series_id, source_id, obs_date, value, raw_payload,
   pull_status)`. For SEC FTD, the settlement date (stored as `obs_date`)
   and the file's actual publication date are genuinely different things
   per the docs quoted above (first-half-of-month files aren't published
   until month-end; second-half files not until ~the 15th of the *next*
   month). `sec_ftd.py` does **not** work around this by stuffing a guess
   into `obs_date` or any other column — it records `publication_half` (a
   free-form descriptive tag like `"a"`/`"b"`, when the caller supplies
   one) in `raw_payload` purely as traceability metadata, explicitly not a
   verified release_date. This gap should be fixed at the schema level
   before any point-in-time ("as of the date we knew about it") backtesting
   is attempted on FTD data.

2. **No semi-monthly `latency_class`.** `schema.sql`'s `source_catalog`
   CHECK constraint only allows `('REALTIME', 'EOD', 'WEEKLY', 'MONTHLY')`.
   SEC FTD's real cadence (twice a month, with an ~2-week publication lag)
   doesn't fit any of those cleanly; `SECFTDPuller.SOURCE_CONFIG` uses
   `'MONTHLY'` as the closest available value, flagged here as an
   approximation rather than a documented fact.

## 5. Tests

`tests/test_source_finra_short_volume.py` (15 tests) and
`tests/test_source_sec_ftd.py` (18 tests, including the cross-source
isolation test) — 33 tests total. All pure Python: a small in-memory
`FakeEngine`/`FakeConn` stands in for Postgres (records INSERT params,
answers the `SELECT DISTINCT obs_date` query
`BasePuller._get_existing_dates` issues), and the puller's fetch method is
monkeypatched as a fake HTTP layer — no real network, no local Postgres.

Coverage: parser/schema (good/empty/malformed/unrecognized-header),
status handling (empty file → no `value=0` rows written; fetch/parse
failure → `FAILED` status, zero writes; malformed rows are skipped, not
fatal), idempotency (repeated pulls of the same file never duplicate
rows), `dry_run=True` (fetches/parses, opens at most a read-only
connection, writes nothing), and that FINRA short-volume and SEC FTD rows
are never written under each other's series-id namespace.

Before this branch: 0 tests for either module (files did not exist).
After: 33 passing (`python -m pytest tests/test_source_finra_short_volume.py
tests/test_source_sec_ftd.py -q`).

## 6. Scheduler

`ingestion/scheduler.py` was **not edited**. A proposed registration is
in `fable-w5b-scheduler-registration.patch` (unapplied) in this directory,
following the existing `try/except ImportError`-guarded append pattern
used for every other puller in `_build_altdata_pullers`-style functions.
It must not be applied until:

- the real FINRA/SEC download URLs are confirmed against a live response
  (see the "not confirmed" notes above), and
- the `release_date` / `latency_class` schema gaps in section 4 are
  resolved or explicitly accepted.
