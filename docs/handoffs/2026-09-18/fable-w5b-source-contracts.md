# W5b/W5c — FINRA Daily Short Sale Volume + SEC FTD + EIA weekly stocks: source contracts

Branch: `fable/sources-finra-ftd-20260918` (base `origin/main` @ `a2f71acf`)
Status: **contract-first, still not activated in the scheduler**, but as
of W5c (2026-09-18) the FINRA and SEC download URLs are confirmed live
against real one-off captures (see section 7), and an EIA v2 weekly
Cushing-stocks request contract has been added (section 8). Neither
puller is imported by `ingestion/scheduler.py`. A proposed (unapplied)
registration patch is at `fable-w5b-scheduler-registration.patch` in this
same directory, updated in W5c to reflect the now-live URLs.

New/changed files (W5b + W5c):

- `ingestion/altdata/finra_short_volume.py` — `FINRAShortVolumePuller`
  (W5c: real `_fetch_raw_text` URL construction, no more `NotImplementedError`)
- `ingestion/altdata/sec_ftd.py` — `SECFTDPuller` (W5c: real
  `_fetch_zip_bytes` URL construction from `yyyymm`/`half`; `SEC_USER_AGENT`
  now required, fails closed)
- `ingestion/altdata/eia_puller.py` (W5c: added
  `build_weekly_stocks_request`/`fetch_weekly_stocks`/
  `parse_weekly_stocks_response` for the weekly Cushing-stocks route;
  Brent/WTI spot-price `pull()` untouched — that hardening is tracked on
  a separate branch, #553)
- `config.py` / `.env.example` (W5c: added `EIA_BASE_URL`,
  `FINRA_SHORT_VOLUME_BASE_URL`, `SEC_USER_AGENT` — no secret values)
- `tests/fixtures/sources/finra_short_volume/**`, `tests/fixtures/sources/sec_ftd/**`
  (W5c: added real-capture fixtures + `REAL_CAPTURE_NOTE.txt` provenance
  files alongside the original W5b constructed fixtures)
- `tests/test_source_finra_short_volume.py`, `tests/test_source_sec_ftd.py`
  (W5c: added real-fixture + URL-building tests)
- `tests/test_source_eia_weekly_stocks.py` (W5c: new)

W5b made no calls to any FINRA or SEC API/download endpoint (`WebFetch`
only, against the public documentation pages quoted below). W5c made
exactly one real HTTP GET per source (FINRA, SEC) to capture a fixture —
see section 7 for the exact URLs, timestamps, and response codes. No
call was made to the EIA API (would require spending a real key) — see
section 8.

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

**Confirmed live in W5c** (was "not confirmed without calling the API" in
W5b): `_DAILY_FILE_URL_TEMPLATE` in `finra_short_volume.py` builds
`https://cdn.finra.org/equity/regsho/daily/<MarketPrefix>shvol<YYYYMMDD>.txt`
from `settings.FINRA_SHORT_VOLUME_BASE_URL`. Confirmed two ways:
WebFetch of
https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files
(which lists the market-prefix codes CNMS/FNQC/FNRA/FNSQ/FNYX/FORF and
example links), and one real `GET` of
`CNMSshvol20260916.txt` (200 OK) — see section 7. `_fetch_raw_text` still
accepts an explicit `url=` override for tests/manual use.

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

**Confirmed live in W5c** (was "not confirmed without calling the API"
in W5b): the absolute URL prefix
`https://www.sec.gov/files/data/fails-deliver-data/cnsfails<YYYYMM><a|b>.zip`
is confirmed both by WebFetch of the SEC page (which links
`cnsfails202608b.zip`, `cnsfails202607a.zip` directly under that path)
and by one real `GET` of `cnsfails202608b.zip` (200 OK, 1,393,881 bytes)
— see section 7. `_fetch_zip_bytes` builds this from `yyyymm=`/`half=`
now, or still accepts an explicit `url=` override.

**Still not confirmed:**
- Whether a header row is always present (the parser detects one
  defensively — first field `SETTLEMENT DATE` — but treats its absence as
  fine, not fatal). The one real file captured DID have a header row.
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

Before W5b: 0 tests for either module (files did not exist). After W5b:
33 passing. After W5c (added URL-building tests + real-fixture parse/
reconciliation tests for both, plus the new EIA contract test file): 50
passing across the two source test files
(`python -m pytest tests/test_source_finra_short_volume.py
tests/test_source_sec_ftd.py -q`) + 9 more in
`tests/test_source_eia_weekly_stocks.py` = **59 passing total** for this
workstream's test files.

## 6. Scheduler

`ingestion/scheduler.py` was **not edited** (W5b or W5c). A proposed
registration is in `fable-w5b-scheduler-registration.patch` (unapplied,
updated in W5c) in this directory, following the existing
`try/except ImportError`-guarded append pattern used for every other
puller in `_build_altdata_pullers`-style functions. As of W5c, the URL
confirmation blocker from W5b is resolved for both sources, but it must
still not be applied until:

- **FINRA:** the live-response anomaly in section 7 (fractional share
  volumes; multi-facility comma-joined `Market` values) is confirmed as
  expected-or-not, and a real "which trade date to pull, and has FINRA
  published it yet" rolling rule exists (`pull()` still takes a required
  `trade_date`).
- **SEC:** `settings.SEC_USER_AGENT` is set in every environment that
  would run this puller, and a real "which half-month is currently
  available" rolling rule exists (`pull()` still takes `yyyymm`/`half`,
  with no "latest" default).
- Either way, the `release_date` / `latency_class` schema gaps in
  section 4 are resolved or explicitly accepted.

## 7. W5c real-fixture captures (2026-09-18)

Per this workstream's constraints, at most 2 real HTTP requests were
allowed per source, with `User-Agent: GRID/1.0 (aniksrobot@gmail.com)`,
no retry loops. Exactly 1 of 2 was used for each source; both requests
returned `200 OK` on the first attempt so no retry/fallback path was
exercised.

### FINRA short volume

```
GET https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260916.txt
User-Agent: GRID/1.0 (aniksrobot@gmail.com)
-> 200 OK  (Server: cloudflare, x-cache: Hit from cloudfront)
   12,283 data rows, 543,878 bytes
```

Stored as `tests/fixtures/sources/finra_short_volume/real_capture_sample.txt`
— a 60-row random sample of the real response (header kept; trailer
count rewritten to match the sample size) plus
`REAL_CAPTURE_NOTE.txt` documenting the full capture.

**Anomaly found, not silently normalized:** the real response does not
match the layout PDF exactly. `ShortVolume`/`TotalVolume` are fractional
(e.g. `638095.307120`), not "no decimals" as documented, and ~65% of
rows (7,932 / 12,283) carry a comma-joined multi-facility `Market` value
(e.g. `"B,Q,N"`) instead of the documented single alpha character. The
parser tolerates both shapes (values pass through `float()`; `market` is
stored verbatim, uppercased) and the added
`test_parse_real_captured_finra_fixture_matches_documented_columns` /
`test_real_fixture_reconciles_short_le_total` tests pass against this
exact fixture — `ShortVolume <= TotalVolume` holds for every one of the
60 sampled rows. **Open question, recorded rather than resolved:**
whether the multi-facility `Market` value is expected CNMS-file behavior
(as opposed to the single-facility FNQC/FNRA/FNSQ/FNYX/FORF files) or an
artifact of this environment's network path — not something this pass
could determine, and a reason the scheduler patch stays unapplied.

### SEC fails-to-deliver

```
GET https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip
User-Agent: GRID/1.0 (aniksrobot@gmail.com)
-> 200 OK  (Server: nginx, X-AH-Environment: prod)
   1,393,881 bytes zip; single member cnsfails202608b.txt,
   3,874,330 bytes uncompressed, 61,550 data rows,
   settlement dates 2026-08-17 .. 2026-08-31
```

3,874,330 bytes is over the ~2 MB threshold, so the extracted text was
NOT stored whole. Two small real (not constructed) slices were kept
instead — see `tests/fixtures/sources/sec_ftd/REAL_CAPTURE_NOTE.txt` for
full provenance:

- `real_capture_first500.txt`: header + the real file's first 500 data
  rows, verbatim (all settlement date 2026-08-17 — each date's block runs
  ~5,000+ rows).
- `real_capture_multi_date_cusip.txt`: header + all 11 real rows for one
  CUSIP (`Y4000A102` / symbol `HQ`) that recurs across all 11 settlement
  dates in the file, with 11 different `QUANTITY (FAILS)` balances (373,
  2245, 71, 1258, 3348, 145601, 9502, 16511, 26351, 3294, 179).

No format discrepancy found — the real file matches the documented
pipe-delimited 6-field layout exactly, unlike FINRA's capture above.
`test_parse_real_first500_matches_documented_columns` asserts the
settlement-date/CUSIP/quantity columns on the real 500-row slice.
`test_real_fixture_same_cusip_across_dates_not_summed` runs the real
11-row multi-date slice through `pull_from_text()` and asserts all 11
dates are stored as 11 independent rows under
`sec:ftd_balance:Y4000A102` with their original per-date values (e.g.
`2026-08-17 -> 373.0`, `2026-08-24 -> 145601.0`, `2026-08-31 -> 179.0`) —
i.e. never summed into one balance.

## 8. EIA weekly Cushing, OK crude oil stocks — v2 request contract

Documented at https://www.eia.gov/opendata/browser/petroleum/stoc/wstk
(route + facet shape, confirmed via WebFetch — the page itself is a JS
browser UI and did not render the literal request URL to WebFetch's
markdown conversion, so the route `petroleum/stoc/wstk` and series id
`W_EPC0_SAX_YCUOK_MBBL` were corroborated via WebSearch against
https://www.eia.gov/dnav/pet/pet_stoc_wstk_a_EPC0_SAX_mbbl_w.htm, the
EIA's own "Stocks of Crude Oil, Commercial (Excl. Lease Stock)" series
page for that same series id) and
https://www.eia.gov/opendata/documentation.php (general v2 query-param
shape: `data[0]=value`, `facets[<dim>][]=...`, `frequency`, `api_key`,
`sort[...]`, `length` — confirmed with a verbatim quoted example URL for
a different route, electricity retail-sales, since the documentation
page's own examples don't cover petroleum/stoc/wstk specifically).

**No call was made to the EIA API** — this would require spending a real
`EIA_API_KEY`, which this workstream's constraints do not permit.
`ingestion/altdata/eia_puller.py` adds three pure functions instead:

- `build_weekly_stocks_request(series_id="W_EPC0_SAX_YCUOK_MBBL", length=5)`
  — returns `{"url": ..., "params": {...}}` for
  `<EIA_BASE_URL>petroleum/stoc/wstk/data/` with `api_key` from
  `settings.EIA_API_KEY` (never a hardcoded/default value).
- `as_display_url(request)` — renders the full URL with `api_key`
  masked (`api_key=***`) via a minimal `_redact_api_key()` helper added
  in this file (no existing redaction helper was found anywhere under
  `ingestion/` on this branch to reuse — checked first).
- `parse_weekly_stocks_response(payload)` — parses the documented
  `response.data[]` envelope (`period`/`value`/`units`/`series`), tested
  against a constructed response, not a live one.
- `fetch_weekly_stocks(...)` — wires the above together; fails closed
  with `RuntimeError` if `EIA_API_KEY` is unset, and is not called
  anywhere in this pass (`tests/test_source_eia_weekly_stocks.py`
  includes a guard test that fails loudly if anything ever calls
  `requests.get` from this function without a test explicitly mocking
  it).

**Exact bounded request the operator can run once, live, to confirm this
contract** (reads `$EIA_API_KEY` from the environment, never pastes it
inline; `length=5` bounds it to 5 rows):

```bash
curl -s "https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key=${EIA_API_KEY}&frequency=weekly&data[0]=value&facets[series][]=W_EPC0_SAX_YCUOK_MBBL&sort[0][column]=period&sort[0][direction]=desc&length=5"
```

What to check in the reply:

1. HTTP 200, and a top-level `"response"` object (not an `"error"` key —
   a wrong/expired key returns `{"error": "..."}` with a 4xx/200 status
   depending on the failure mode).
2. `response.data` is a list of ~5 objects, each with `period` (a
   `YYYY-MM-DD` weekly date), `value` (a numeric string, thousand
   barrels), `units` (expect `"MBBL"`), and a `series` /
   `series-description` field naming Cushing, OK.
3. The 5 `period` values are consecutive weeks in descending order (per
   `sort[0][direction]=desc`) — confirms `sort`/`length` behave as
   documented for this specific route, which was not directly observed
   in this pass since the route wasn't called live.

## 9. Still unverified / left for a follow-up pass

- Whether the FINRA CNMS-file anomaly (section 7) is real FINRA behavior
  or a network-path artifact of this environment.
- Whether the EIA v2 weekly-stocks route accepts exactly the same
  `sort[0][column]`/`sort[0][direction]` parameter names as the
  electricity example quoted in section 8, or whether petroleum routes
  use a different sort-param shape — the operator curl above will
  confirm this in one call.
- Whether FINRA's single-facility files (FNQC/FNRA/FNSQ/FNYX/FORF)
  report one row per Market code the way the layout PDF describes, given
  that the consolidated CNMS file did not (section 7). Not tested — no
  second real request was spent on this per the 2-request budget.
