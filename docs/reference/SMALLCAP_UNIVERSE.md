# Small-cap universe — every company on a permanent id, with a price file the factory reads

Code: `analysis/smallcap_universe.py`. Tests: `tests/test_smallcap_universe.py`. Issue: #1210.
It reads no database and places no order.

## Why it exists

The cross-section factory's first universe was a hand-built list of index members with a rename table,
and #1210 records what went wrong: Tiingo reuses tickers, so a ticker read for a past date can return a
different company. A small-cap universe has thousands of ended companies, and a rename table cannot be
kept by hand at that size.

## What was measured on 2026-11-19

- Tiingo's company list (`/tiingo/fundamentals/meta`) has 20,802 companies, each with a unique permanent
  id. 12,685 are marked inactive. 877 tickers are shared by more than one company.
- **The daily price endpoint accepts the permanent id.** `/tiingo/daily/PARA/prices` served the company
  that holds PARA today for a 2023 date (close $10.10, volume 1,742). The same request with
  `US000000013059` served the earlier holder (close $17.07, volume 12.7M).
- **Over half of the ended companies have no price history at Tiingo.** In a seeded sample of 400 ended
  companies, 227 returned no bars. For 217 of those the ticker is not in Tiingo's price list at all.
- **The ended companies that do have prices are mostly recent.** Of the 173 with bars, 19 stopped trading
  in 2009 to 2014 and 148 in 2016 to 2026. Scaled to the list, that is roughly 100 a year before 2015
  against roughly 400 a year after 2016.
- This plan serves no sector, industry, location or SEC number outside the Dow 30. The company list
  gives only id, ticker, name, active flag and depositary-receipt flag.
- An ended company's price file continues after its last trade as flat bars with zero volume.

What follows: a small-cap history built on these files is missing most failures before about 2016.
Discovery on this universe should not start in 2010 unless another price source fills the gap.

## Checked against SEC's record (2026-11-20)

SEC's quarterly filing index lists every notice of removal from listing (Form 25 and 25-NSE). The count
of issuers filing one is steady, so the thin early years are Tiingo's gap and not a quiet period. The
table is from the pulled price files; a first pass against Tiingo's price list gave the same picture.

| Notice year | Issuers with a notice | Matched to an ended company in the universe | Of those, with a pulled price history ending near the notice |
|---|---|---|---|
| 2009 to 2012 | 576 to 725 a year | 230 to 300 a year | 10 % to 20 % |
| 2013 | 660 | 234 | 36 % |
| 2014 | 538 | 184 | 45 % |
| 2015 | 567 | 222 | 69 % |
| 2016 to 2021 | 648 to 854 a year | 166 to 250 a year | 74 % to 77 % |
| 2022 to 2026 | 744 to 1,022 a year | 182 to 210 a year | 69 % to 97 % |

Method: the first notice per SEC company number per year; an exact match on the company name with legal
suffixes removed; the matched company is one Tiingo marks inactive and the universe includes; "ending near
the notice" means its last traded day in the pulled file is within 400 days of the notice. Per-year
figures: `raw/sec-form25-vs-pulled-20261120.json` in the data directory (the first pass is
`raw/sec-form25-vs-tiingo-20261120.json`).

Limits of this check:

- A notice covers any listed security, so issuers include funds, notes and preferreds. Those mostly fall
  in the 182 to 343 issuers a year with no name match, which are not in the percentages.
- That unmatched group also holds real companies: some are not in Tiingo's list at all (Rohm and Haas in
  2009), and some are in it under a slightly different name with no prices (Merrill Lynch). True coverage
  is therefore lower than the table shows.
- Names that match more than one Tiingo company (27 to 278 a year) are left out.
- A company still trading after one of its securities was removed is counted as active, not ended.
- 19 to 90 matched companies a year have a price history that ends more than 400 days from the notice.
  They are counted as not covered; some are a wrong name match or a notice for one security of a company
  that ended later.

Reading: from 2016 about three ended companies in four have prices; before 2013 at most one in five.
Even the later years are not complete.

**Owner decision, 2026-11-20: discovery on this universe starts at 2016-01-04.** A run file sets
`first_session` to that date; the factory accepts any window inside its development days, which end
2022-12-29. Bars before 2016 are still read for a signal's lookback, and those belong to companies that
were trading in 2016 or later, so the early gap does not enter the ranked universe. The remaining gap
does: about a quarter of the companies that ended from 2016 are absent, which flatters any rule that buys
fallen names. State that beside every discovery result from this store.

## The built store (2026-11-20)

Pulled 2026-11-20 from bars dated 2009-01-02 onward; last bar 2026-11-19.

| | Companies |
|---|---|
| In Tiingo's list | 20,802 |
| Left out: depositary receipts, units, funds, preferreds, blank-check names, two non-US ids | 4,331 |
| In the universe | 16,471 |
| With a traded price history (the panel) | 9,471 |
| With a price file but no traded day | 105 |
| With no prices at Tiingo | 6,895 |

Of the 10,652 universe companies Tiingo marks inactive, 3,975 (37 %) have a traded history. Of the 5,819
it marks active, 323 have none. 3,135 price files had rows cut outside the traded life, 156,330 rows in all.

Companies whose traded history ends, by the year of the last traded day:

| 2009 | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 42 | 64 | 64 | 79 | 124 | 127 | 215 | 271 | 303 | 305 | 270 | 264 | 340 | 306 |

After 2022 the count mixes real endings with stale files: 717 companies Tiingo still marks active have a
history that stops in 2023 to 2026 (405 of them in 2026). That is after the discovery window. It has to be
settled before any confirmation look uses those years.

Companies that qualify on the last session of each year (`counts`, as it was on 2026-11-20):

| Floors | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 |
|---|---|---|---|---|---|---|---|
| $2 and $1M a day (owner decision, 2026-11-20) | 2,758 | 2,806 | 2,726 | 2,748 | 3,121 | 3,360 | 2,805 |
| $5 and $10M a day (the factory's default) | 1,556 | 1,618 | 1,667 | 1,607 | 1,846 | 1,960 | 1,661 |

So these floors add about 1,100 to 1,400 companies a year below the default ones. These counts are of
companies with prices; the missing quarter of ended companies is not in them.

**Load check (2026-11-20, the old in-place panel).** The factory change in PR #1291 loaded this panel (bars and the universe mask only:
no signal, no outcome, no ledger): 9,471 price files, 3,524 sessions from 2009-01-02 to 2022-12-30, no
sector labels, 316 seconds and 6.9 GB of memory at peak. Its count of rankable companies at $2 and $1M
equals `counts` on the last session of every year from 2009 to 2022. The first attempt failed: the
members row for 9,471 companies is 142,000 characters, longer than Python's default CSV field limit, which
the PR now raises for that read.

## What the builder does

| Command | Reads | Writes |
|---|---|---|
| `fetch` | The company list; Tiingo by permanent id | `fetch-state.json`, `prices/<id>.csv`, `no-prices/<id>`, `fetch-log.jsonl` |
| `bind-existing` | The company list; the price files on disk | `fetch-state.json` for a store pulled before that record existed |
| `build` | The company list; the price files; `fetch-state.json` | One panel generation `panel/g-<id>/`, `panel/CURRENT`, `companies.csv`, `universe-report.json` |
| `counts` | The current generation | Companies the factory would rank on the last session of each year, for given floors |

**Which companies are included.** Common shares only. Left out: depositary receipts (Tiingo's flag), and
by name or ticker suffix: units, warrants, rights, preferreds, funds and blank-check shells (a name with
"acquisition"). `companies.csv` lists every company with its kind, so the choice can be checked and
changed. A company with no name is taken as common shares.

**One pull per store.** A pull is a start day and a vintage, the last day asked for. Both go into every
request (`startDate`, `endDate`), so a file asked for today and one asked for a week later hold the same
rows. The vintage must be a finished day. `fetch-state.json` records the start, the vintage and the hash
of the company list. A resume that asks for another start, vintage or company list is refused before any
request is made: a different pull needs its own data directory.

**Fetch.** One request per included company, key in a header, about one request a second. It can be
stopped and started inside one pull: an id with a price file or a no-prices mark is skipped. Tiingo has
two answers for an id with no bars, and only those count as "no prices": an empty list (`[]`), or the
column header alone (seen for Merrill Lynch and Aleris International, among others).

**An answer is written only when all of it is a daily file.** `price_rows` checks the whole body first:
the exact header, thirteen fields on every row, a date that is a day, eleven prices and factors that are
finite numbers, a volume that is not negative, days in order with none repeated, and every day inside the
start and the vintage. Anything else, a `date,error` body or one bad row after a thousand good ones, is
logged as failed with the reason, leaves no file, and is asked again on the next run. The file is written
to a temporary name and renamed, so a price file on disk is always a whole checked answer. `build` and
`bind-existing` run the same check on every file they read.

**Life of a company.** First and last bar are the first and last day with volume above zero. A company
has ended when its last traded day is more than 5 sessions before the store's last.

**Who is in the universe on a day** is not decided here. `counts` and `universe_on` call the factory's
`universe` on the current generation, the function `load_panel` itself builds on, so a count and a run
cannot disagree. Its rule: a valid bar on session d (positive unadjusted and adjusted open and close,
open and close inside the day's range), the unadjusted close on the store's previous session at least
the price floor, and the median dollar volume over the 21 sessions ending there at least the dollar
floor. A company with no bar on the previous session is out, whatever its own last row says. Sessions
are the factory's: days on which at least `min_session_bars` price files have a bar (300 unless
`--min-session-bars` says otherwise), and no bar after its last development bar, 2022-12-30.

**What the factory gets** is one generation, `panel/g-<id>/`, not the files as served:

- `prices/<id>.csv`: each price file cut to the company's traded life, from its first day with volume
  to its last. This matters. Tiingo's flat no-volume bars after a delisting pass the factory's valid-bar
  rule, and the dollar-volume median takes 11 sessions to fall to zero, so on the files as served an ended
  company stays rankable for 11 sessions with a return of exactly zero (measured on a test store,
  2026-11-20). Cut, it leaves the day after its last trade, and a pick held into that day is dropped and
  counted by the factory (`dropped_pick_share`). A day without volume inside a company's life (a halt) is
  kept.
- `members.csv`: one row with every included id that ever traded. The factory already requires a
  valid bar on the day, so a company is in on the days it trades and out otherwise.
- `renames.json`: empty, because ids do not change.
- `sectors.csv`: the header alone, which the factory reads as "no sector labels".
- `panel-manifest.json`: the factory's export manifest (`seal_export`): every file's size and hash, the
  pull it came from with how it ended (companies with prices, companies confirmed to have none), and
  `discovery_first_session`. The factory refuses a run whose window starts earlier, before it declares
  anything.

**2016-01-04 is a floor.** `--discovery-first` may name a later day and never an earlier one: `build`
refuses an earlier day before it writes anything, so no generation of this source can carry one.

**Only a finished pull is sealed.** Every included company must have ended one of two ways: a price
file, or a no-prices mark, which `fetch` writes only for Tiingo's two "no bars" answers. A company with
neither was never asked or its request failed. `build` refuses the store while one remains, names the
first few, and asks Tiingo nothing itself; `fetch` is run again until none is left. A mark beside a price
file, or a mark for a company the list leaves out, is refused too.

**A generation is published whole and never edited.** `build` assembles it in a hidden directory, seals
it, and renames it to `panel/g-<id>` in one step, then marks every file and directory read-only. The id
is the hash of the manifest's content, so the same inputs give the same generation and a build that
finds it already there writes nothing into it. Other inputs give another generation beside the first;
the first stays as it was. `panel/CURRENT` names the latest and is replaced in one step. `counts` and
`universe_on` read the generation `CURRENT` names, check every file against its manifest, and refuse a
store that has not been built or a generation that has been touched.

## Review fixes (2026-11-21)

Review of #1294 at `e5796b2c` found four things, all fixed above:

| Finding | Fix |
|---|---|
| `counts` read each company's own adjacent rows, so with a session missing it admitted a company the factory rejects, and it did not check the adjusted bar | `counts` and `universe_on` call the factory's `universe`; the copy of the rule is gone |
| A 200 answer holding `date,error` and a malformed row was written as a price file and skipped on resume | Whole-body check before anything is written |
| A resume was not tied to the start or vintage it began with | `fetch-state.json`; the vintage is in every request |
| `build` replaced panel files one at a time in place | One sealed generation per build, renamed into place, read-only |

A second review, at `7b1113a9`, accepted those four and found two more, fixed the same day:

| Finding | Fix |
|---|---|
| `--discovery-first` could seal a day before 2016, and the factory then ran a 2015 window on it | 2016-01-04 is a floor in `build`; a later day is allowed |
| A company whose request failed had no file and no mark, and `build` still sealed the panel as complete | `build` refuses a pull with any unresolved company; the manifest records how the pull ended |

Tests are on synthetic stores only (`tests/test_smallcap_universe.py`): no request to Tiingo, no read of
the real store, no run.

**The store on grid-svr predates these fixes.** It has no `fetch-state.json`, its requests carried no
`endDate`, and its `panel/` is the old in-place layout, which nothing reads now. Before any run it needs
`bind-existing` (start 2009-01-02; the vintage is its last bar, 2026-11-19), which checks every price file
on disk and asks Tiingo nothing, and then `build`. Neither has been done. The counts and the load check
recorded above were made on the old layout on 2026-11-20; the load check compared them with the factory's
own count at $2 and $1M on the last session of each year and they were equal, but they have not been
re-made through the shared function.

## What is not done

- **The factory on main cannot load this store.** It stops with an error when `sectors.csv` has no labels,
  and its floors are constants ($5 and $10M a day). The change is on its own branch,
  `claude/xs-factory-floors-nosector-20261120` (PR #1291, not merged): floors become optional run-file
  fields; a header-only `sectors.csv` loads as "no labels", pooled rankings only; a no-label store must be
  a sealed export; and every file a run reads is checked against the hashes it declared. This branch is
  stacked on that one, because `counts` and `build` call its `universe` and `seal_export`. A run file
  without the new fields gives a byte-identical report on a store without an export. No signal has been
  run on real small-cap data.
- The store on grid-svr has not been bound and rebuilt (above).
- No sector source, so no within-sector ranking, sector gate or sector card for small caps yet.
- The factory's default cost is 1 bp a side, set for index members. A small-cap run file must set its
  own figure. Owner decision, 2026-11-20: the first run file assumes 25 bp a side. That is a stand-in
  from a rough spread estimate on the daily bars (about 34 bp a side for stocks trading $1M to $3M a day,
  22 bp for $3M to $10M, 18 bp for $10M to $50M, 8 bp above; single years read from zero to 48 bp for
  the whole universe). It is not measured. Real tickets, the way #1214 does for index members, replace it.
- Share-kind filtering is by name. It has not been checked against a list of security types.
- Whether Tiingo's daily file writes a date as a day alone or with a time is taken either way; the check
  has not been run against the files on grid-svr.

## Where the data is

grid-svr, the `smallcap-data/` directory of the research data home: `raw/` (the company list as served, the
ended-company sample, SEC's quarterly indexes and the two comparison files), `prices/` (as served),
`no-prices/` (one mark per id Tiingo has no bars for), `panel/` (the old in-place layout until the store is
rebuilt; then one directory per generation and `CURRENT`), `companies.csv`, `universe-report.json`,
`fetch-log.jsonl`, `code/` (the `analysis/` copy that ran) and `run-fetch.sh`.
