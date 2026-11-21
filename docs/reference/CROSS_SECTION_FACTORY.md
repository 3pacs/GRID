# Cross-section factory — rank an index's stocks on a signal, long the top, short the bottom

Code: `analysis/cross_section_factory.py`. Tests: `tests/test_cross_section_factory.py`. Issue: #1211 (part of #1153).
It never places an order and promotes nothing: a selected module is a candidate for one confirmation look.

## Why it exists beside the SPY factory

`analysis/module_factory.py` searches one index within the day. Its first run selected 0 of 16 modules (best t 0.86
against a bar of 2.25), in line with eleven earlier searches of the same ground. Ranking about 500 stocks a night
has breadth that one index lacks: the first cross-section effect tried by hand (overnight return persistence, edge
search 12) cleared a best-of-72 bar at t 4.33, and ranked inside each sector its t was 11.6 on 2010–2022. That rule
then **failed** its one confirmation look on 2023–2026, which is why this module produces discovery cards and no
verdicts.

## What a run does

1. Reads the run file and hashes every input file (the data manifest). No price, member or state row is parsed
   yet. A run id or run file the ledger declared before is refused, and so is an output directory that already
   exists: one look per run file. A store that is a completed export is checked against its own manifest here, and a
   window that starts before the export's discovery start is refused (see "A completed export" below).
2. Declares the run on a factory ledger (`record_factory_declared`): run file, manifest hash, trial count.
3. Loads bars, **dropping every row after 2022-12-30**. Each input file is read once and its bytes must hash to what
   step 1 declared; a file replaced, added or removed since then abandons the run. A run file whose window leaves 2010-01-04 .. 2022-12-29 is
   refused. The later years are kept unread for a confirmation look.
4. For each module (signal × ranking × fraction × horizon) computes one spread per night: mean outcome of the top
   fraction minus the bottom fraction, pooled or inside each sector (pick-weighted across sectors).
5. Judges all modules together: each of `perms` draws flips the sign of blocks of `null_block_sessions`
   consecutive nights (default 10), the same flips for every module, and modules pass by step-down adjusted p
   (Westfall-Young, one-sided). The state record is hashed again after it is read; a change since declaration
   abandons the run.
6. Writes `discovery-frozen.json`, `report.json`, `report.md`, then closes the run on the ledger
   (`record_factory_result`). Any failure, an interrupt included, closes it as abandoned.

The SPY factory's null permutes the session order of the target, which asks whether a signal's timing matters. A
long-short spread trades every night, so that permutation leaves its mean where it was. Sign flips test the mean.

**Why blocks.** Flipping each night on its own assumes the nights are independent. Long-short spreads are not: they
run in streaks (a factor episode, a crash year), and an independent flip then gives a null that is too narrow and
p-values that are too small. Flipping whole blocks keeps each streak together, so a clustered result meets a wider
null; on independent nights the two nulls agree. The test `test_block_flips_widen_the_null_for_clustered_nights`
shows the size: a series whose nights share 19 of 20 shocks has p 0.0002 under single-night flips, 0.001 with
10-night blocks and 0.011 with 20-night blocks, for the same t. A run file may set `null_block_sessions` from 1
(single nights) to 63.

**What the printed bar is.** `max_t.critical_t` is the first step's bar: the 1−alpha quantile of each draw's best
flipped t. Later steps compare each module with the best flipped t of the modules ranked below it, so a module may
pass with a t under that figure; `max_t.lowest_passing_t` gives the lowest t that passed. Multiplicity is counted
within the run only (`max_t.multiplicity`): earlier runs and looks are not in it.

## Universe and timing (the same rules edge search 12 registered)

- Members by date from `members.csv` (start set plus dated rows), through `renames.json`.
  A row may list thousands of members: the read allows a field of up to 16 MB.
- A stock is ranked on day d if it has a valid bar on d, its unadjusted close on d−1 is at least $5, and its median
  dollar volume over the 21 sessions ending d−1 is at least $10M. The floor also screens out price files that hold
  another company (Tiingo reuses tickers).
- A valid bar has all eight prices of its row present and positive (open, high, low, close and the four adjusted
  values), with the open and the close inside the day's range (see "All eight prices" below).
- A run file may set its own floors: `min_price` ($1 to $10,000) and `min_dollar_volume` ($100,000 to $10 billion).
  Left out, they are $5 and $10M, and the run and its report are byte for byte what they were before the fields
  existed. A run that sets either has both written under `coverage.floors` in its report. Lower floors are only safe
  on a store whose price files are keyed by a permanent company id, because the default floor is also what screens
  out a reused ticker.
- Every signal uses only what is known before the close of d: closes through d−1, plus the open of d for the
  overnight mean. Outcomes start at the close of d.
- A day with fewer than `min_ranked` ranked stocks is skipped.
- Sector: `sectors.csv` (today's GICS label). A member with no label takes the labelled sector its daily returns
  follow most closely; the report gives that method's accuracy on labelled names (77 % on the first run).
- A store with no sector labels says so with a `sectors.csv` that holds its header (`Symbol,GICS Sector`) and no
  rows. Such a run ranks pooled only. A run file that asks for the `within_sector` ranking, a sector gate
  (`sector_phase`, `sector_leading`, `groups`) or a `sector_momentum` signal is refused, and its cards have no sector
  cuts. The refusal comes when the data is loaded, which is after the run is declared, so the ledger records that run
  as abandoned: check the run file against the store before running on the shared ledger. A missing `sectors.csv`
  still stops the run with a file-not-found error before anything is declared, as it did before.
- A store with no sector labels must be a completed export. Without `panel-manifest.json` the run is refused before
  anything is declared.

## A completed export (2026-11-21)

Review of #1291 found two gaps. The run hashed every input file before declaring, but the load then read the files
again from disk without checking them, so a store being rewritten file by file could put two versions under one run
id. And nothing tied a store to the first day discovery may use on it: the small-cap store's 2016 start was a line
in a document.

**Every read is checked.** `load_panel` now takes the declared file table. It reads each file's bytes once, compares
their hash with the declared one, and parses those bytes. A file that changed, appeared or went missing between the
declaration and the read stops the run with "changed between the run's declaration and its read", and the ledger
records it as abandoned. This holds for every store, labelled or not. A run on an unchanged store gives the same
four output files byte for byte (checked against main on a plain and a gated run, 2026-11-21).

**An export is a directory written once.** `seal_export(dir, discovery_first_session=..., source=...)` writes
`panel-manifest.json` over a finished directory and refuses to write it twice:

| Field | Meaning |
|---|---|
| `schema` | `cross-section-panel-export/1` |
| `complete` | `true`; anything else is refused |
| `discovery_first_session` | The first day a discovery window may start on this store |
| `source` | Where the files came from: provider, requested start, vintage |
| `files` | Every panel file with its size and sha256 |
| `generation` | `g-` and the first 16 hex digits of the hash of every other field |

Before declaring, the factory reads that manifest and refuses the run when:

- the manifest is not complete, or its generation id is not the hash of its own content (an edited manifest);
- the directory's files are not exactly the ones listed, by name and by hash;
- the run file's `first_session` is earlier than `discovery_first_session`.

Nothing is declared in any of those cases. Otherwise the manifest's hash, generation and discovery start go into the
data manifest under `panel_export`, so the declared `data_manifest_sha256` binds them, and the report repeats them
under `coverage.panel_export`. A store without `panel-manifest.json` has no `panel_export` entry and its manifest
hash is what it was.

**One definition of the universe.** `universe(dir, min_session_bars=..., min_price=..., min_dollar_volume=...)`
answers which stock may be ranked on which session. `load_panel` builds on it and the small-cap builder's `counts`
calls it, so a count and a run cannot disagree. "The session before" is the store's previous session, not the
stock's previous bar: a stock with no bar on that session has no prior close and is out. A valid bar needs positive
adjusted prices as well as the unadjusted checks.

Tests (synthetic stores only, no run on real data): a no-label store without an export refused before declaration;
an export dated 2016-01-04 refusing a 2015 window; a window one session early refused; a replaced, added or removed
file and an edited manifest refused before declaration; a price file, `members.csv` or `sectors.csv` changed after
declaration abandoning the run; a missing session and a zero adjusted close taking a stock out of the universe.

## All eight prices (2026-11-21)

**The rule.** A Tiingo daily row carries eight prices: `open`, `high`, `low`, `close`, `adjOpen`, `adjHigh`, `adjLow`,
`adjClose`. `universe()` reads all eight, and a row is a valid bar only when every one of them is present and
positive. A blank in any one takes that stock out of `universe.ok`, `universe.base` and `load_panel(...).base` for
that session, and no return is computed into or out of it. Files are still read as served: a blank is never filled.

**The reason.** Until this change the reader took six prices and never opened `adjHigh` or `adjLow`. A row whose
only blank was one of those two was a valid, rankable bar. Review of the small-cap builder (#1294) reproduced it on
two paths that builder does not guard: a directory sealed directly with `seal_export`, and an older sealed generation
read back. The builder refuses such a row when it fetches, binds or builds, but the rule belongs in the one
definition of the universe, where every store and every caller meets it. Tiingo does serve blank prices on some days
without a trade, so the case is real on thinly traded names.

**What it changes on the index store: nothing.** The three cross-section runs on the factory ledger declared one data
manifest (`31513c21…`, 814 price files), and the nine scratch runs that wrote a manifest list the same files. The
store was checked against that manifest by hash, then every row was counted, read-only, on 2026-11-21:

| Measure | Count |
|---|---|
| Rows read (814 files, through each file's last row) | 2,867,858 |
| Rows on or before 2022-12-30, the ones a run loads | 2,224,008 |
| Rows blank in `adjHigh` or `adjLow` with the other six prices present | 0 |
| Rows valid under the six-price rule and not valid under the eight-price rule | 0 |
| Blank cells in any of the eight price columns | 0 |

The count read price cells in rows after 2022-12-30 for presence and sign only; no return was computed from them.
With main's reader and the changed one, `universe` and `load_panel` on that store give the same `ok`, `base`,
overnight, intraday and close-to-close arrays, adjusted closes and sector placement (compared by hash; 3,357
sessions, 814 files, 1,563,261 rankable stock-sessions). No ledgered result can move.

A run on an unchanged store gives the same `report.json`, `discovery-frozen.json`, `report.md` and
`data-manifest.json` byte for byte on main's factory (f8a2425b) and the changed one: checked on the synthetic test
store with a plain run and a gated run.

**What it costs.** Two more columns per file. On the index store, four loads each: `universe()` took 20.9 s against
18.4 s (+2.5 s, 14 %), peak memory 905 MB against 823 MB (+82 MB, 10 %), and the bar frames it returns hold 188 MB
against 146 MB.

Tests (synthetic stores only): each of the eight prices blank on its own, at volume 0 and at a positive volume, is
out of `ok` and `base` in `universe` and `load_panel` for that session and back the session before; a 25-company
store with one row blank only in `adjHigh`, sealed directly with `seal_export` and read through the verified path,
loses that company for that session only.

## Signals and horizons

| Signal kind | Meaning (high = long) |
|---|---|
| `mean_overnight` | mean overnight return over `lookback` sessions ending at the open of d, each clipped to ±10 % |
| `mean_intraday` | mean open-to-close return over `lookback` sessions ending d−1 |
| `momentum` | return over `lookback` sessions ending `skip` sessions before the close of d−1 |
| `reversal` | the opposite of the last `lookback` sessions' return |
| `high_52w` | close of d−1 over the highest close of the last `lookback` sessions |
| `sector_momentum` | the equal-weighted mean `momentum` of the stock's sector's members (at least 5 that night); pooled ranking only |

`"sign": -1` turns a signal around. Horizons: `overnight` (close of d to open of d+1), `intraday` (open to close of
d+1), `close_1d` (close of d to close of d+1). Each signal lists its own horizons.

**Cost.** `cost_bp_per_side` of traded notional × four sides × turnover, per $1 long and $1 short. Overnight and
intraday open and close every position, so their turnover is 1. On `close_1d` turnover is the share of names
replaced since the previous traded night (1 on the first night of a stretch), so a slow signal pays for what it
trades. The ranking shows gross, net and mean turnover.

## Gates

A signal may carry a `gate` on the state recorded the evening before the decision day:

| Gate | Applies to | Values |
|---|---|---|
| `market_phase`, `fed_balance_sheet`, `vix_third`, `spike_state` | the whole night | a list of allowed values |
| `sector_phase`, `groups` | each sector that night | a list of allowed values |
| `sector_leading` | each sector that night | `true` or `false` |

Values must be the state record's own words, or the run file is refused (a misspelled value used to trade no night
at all): phases `rising, speeding up`, `rising, slowing`, `falling, speeding up`, `falling, slowing`; Fed balance
sheet `expanding`, `flat`, `shrinking`; VIX third `calm`, `middling`, `fearful`; spike state `spike day`,
`aftermath`, `normal`; groups `cyclical`, `growth`, `defensive`.

**A gated run must say how its gates were chosen.** `gates_chosen_after_seeing` is required in any run file with a
gate. `true` means a gate was picked after seeing an earlier result, so the step-down test does not count the search
that chose it: the run becomes a source of hypotheses only. Its report sets `hypothesis_only`, selects no module
(`selected` is empty, and so the evidence adapter reports none), and keeps `passed_step_down` on each row so a module
that passed can be registered as a new hypothesis for fresh data. `false` is a statement that every gate was fixed
before any result it could be tuned on was seen.

A night the gate turns away is not traded and is not in the module's sample. With a sector gate only stocks of the
sectors let through are ranked (at least `min_ranked_gated` of them). A gated signal is its own module, so the
condition is judged against the run's bar with everything else instead of being read off a card afterwards.

## The card

For the best `cards` modules and every selected one, `report.json` holds where the module paid: by year, and by the
state recorded the evening before each decision day in the sector state record (`desk-sector-state/1`, #1209):
market phase, Fed balance-sheet phase, VIX third, spike state, and for within-sector modules each sector, the three
groups, the sector's own phase, and leading against lagging sectors. Cuts are descriptions of a searched sample.

Every card also carries `where_it_paid` and `where_it_did_not`: each cut with at least 100 traded nights, split by
the sign of its net return and sorted by size, and `report.md` prints the six largest of each. A run file may list
`prior_dev_looks` (earlier development runs at the same effect); they are named in `max_t.multiplicity`.

## The queue of published effects (#1212)

`docs/research/cross-section-queue.json` is the file the factory takes its next run from
(`analysis/cross_section_queue.py`). Each entry is a documented effect: the paper, its sample, the size it states and
how that size was checked (`paper`, `secondary` with the source named, or `not_checked`), the mechanism, the data it
needs, how this test departs from the paper, and the signal. `run` takes the first `queued` entry the ledger has not
declared, writes its run file once, and runs it; `status` lists every entry and its run. An entry's run id carries its
version (`xsq-<id>-v<version>`), so a changed entry is a new version and the factory's one-look rule holds per version.
A `blocked` entry says what it waits for (post-earnings drift waits for earnings surprises by date, #1210); a
`retired` entry says why (overnight persistence failed its confirmation look).

```bash
python -m analysis.cross_section_queue status [--ledger L.jsonl --anchor A.jsonl]
python -m analysis.cross_section_queue run --data-dir DIR --state-db state.db --ledger L.jsonl --anchor A.jsonl \
  --out-root ROOT                                        # add --genesis LEDGER_ID once to create the ledger
```

## Run it

```bash
python -m analysis.cross_section_factory run --run-file RUN.json --data-dir DIR --state-db state.db \
  --ledger L.jsonl --anchor A.jsonl --out OUT            # add --genesis LEDGER_ID once to create the ledger
```

`DIR` holds `prices/<TICKER>.csv` (Tiingo daily columns), `members.csv`, `sectors.csv`, `renames.json`.
A price file may be named by any key the members file uses, a permanent company id included.

## Evidence adapter

`report.json` carries the fields `analysis/desk_factory_report.py` reads, with `net_dollars` holding the net return
per $1 a side per night (named in `target_unit`). Its `state` is `CROSS_SECTION_DEV_RANKING_ONLY`; the adapter
admits it beside `INTRADAY_DEV_RANKING_ONLY` (since 1c02fb0f on main). The test feeds a report to the adapter as
written.

## Point-in-time check of the state record (2026-11-19)

The gates and cards read `desk-sector-state/1` (#1209, `desk_state.py` in the gex desk tree), the record of
sessions[r−1] for decision day r. Its fields use only what was known at that close: the VIX third ranks the close
against the 252 closes before it, the spike state compares it with the median of the 63 before it, the Fed phase
uses only balance-sheet weeks dated at least seven days before the close, and market and sector phases are trailing
21- and 63-session curves. The one leak is shared with this factory: past members with no label are placed in a
sector by correlation over the whole sample, so a sector's membership in 2010 uses later returns.

## Known limits

- A pick whose outcome bar is missing (a halt, a delisting, a bad print) is dropped from its side's mean, not charged
  a loss; the data holds no delisting return. Each row's `dropped_pick_share` sizes it.
- Names with no price file never enter, so if a delisted member's file is missing the universe is survivor-biased.
- Sector labels are today's; past members without one are placed by correlation over the whole window (above).
- The flat cost per side was set for index members at the auctions. A run with lower floors ranks stocks that cost
  more to trade, so its run file should set `cost_bp_per_side` to a measured figure; the default 1 bp is not one.
- Cost is a floor: a flat rate per side at the auctions, four sides a night, no borrow fee on the short side.
- A night with fewer than `min_session_bars` priced names is dropped, so an overnight or close-to-close return after
  it spans more than one session.

## Used data

S&P 500 members, overnight and intraday horizons: 2010-01-04 .. 2026-11-13 is consumed for rules that rank on a
stock's own past overnight or intraday returns (edge search 12; #1133). Other signals and the `close_1d` horizon
have been searched on 2010–2022 only by runs on a factory ledger. Nothing here has read 2023 onward.

## First runs (2026-11-15, scratch ledger, development days 2010-01-04 .. 2022-12-29)

Run files: `docs/research/cross-section-runs/`. Both were declared on a scratch factory ledger (genesis, two
declared, two results), not on `/data/grid_ledgers/factory`. Cost 1 bp a side; 10,000 draws; seed 20261107.

**`xs-overnight-persistence-20261115`** (the edge search 12 card, from a run file): 12 modules, bar t 2.33, 7 cleared.

| Module | Gross bp | Net bp | t | By hand (edge search 12) |
|---|---|---|---|---|
| overnight mean 252, pooled 5 %, overnight | 9.06 | 5.06 | 4.33 | 9.06, 5.06, 4.33 |
| pooled 10 % | 7.27 | 3.27 | 3.69 | 7.27, 3.27, 3.69 |
| pooled 20 % | 5.76 | 1.76 | 2.63 | 5.76, 1.76 |
| within sector 10 % | 6.08 | 2.08 | 3.27 | 6.06 gross |
| within sector 20 % | 4.85 | 0.85 | 1.93 | 4.77 gross |
| faded intraday, pooled 5 % | 8.26 | 4.26 | 2.28 | 8.26 gross |

The pooled modules match to the last digit. The within-sector ones differ by under 0.1 bp because this run places
unlabelled past members from development bars only (282 placed, 78 % accuracy on 439 labelled names), where the
by-hand card used bars through 2026. Report sha256 `437d89d7…`.

**`xs-published-effects-1-20261115`** (12-1 momentum, 5-day reversal, nearness to the 52-week high; one-day holds):
12 modules, bar t 2.27, **none cleared**. Best: 5-day reversal, within sector 10 %, 4.53 bp gross, 0.53 net, t 0.33.
Momentum earned 1.0 bp gross or less and the 52-week-high modules were negative. Report sha256 `ef0e10ec…`.

The reversal card shows why the card matters even for a module that fails: before cost it earned 23.8 bp a night
while the market was falling and speeding up (343 nights) and 33.5 on VIX spike days (265 nights), against 0.2 to 2.0
in rising markets. That is a description of a searched sample, and the daily full-turnover cost is harsh on slow
signals, but it is the kind of conditional statement a pooled verdict hides.

**`xs-gated-1-20261115`** (gates, turnover-aware cost): 16 modules, bar t 2.39, 13 cleared. Report sha256 `e65d694c…`.

| Module (within sector 20 %) | Nights | Turnover | Gross bp | Net bp | t |
|---|---|---|---|---|---|
| overnight mean 252, rising market and rising non-defensive sectors | 2,401 | 1.00 | 5.64 | 1.64 | 3.19 |
| 5-day reversal, falling market only | 752 | 0.37 | 13.42 | 11.93 | 2.91 |
| 5-day reversal, VIX spike days only | 265 | 0.50 | 27.77 | 25.78 | 2.73 |
| 5-day reversal, every night | 3,271 | 0.36 | 4.09 | 2.66 | 2.15 |

Read these with three cautions.

1. **The reversal gates were chosen after seeing run 2's card.** The bar covers the 16 modules of this run, not
   that earlier look, so these t values are flattered.
2. **Half the falling-market result is one year.** 2020 earned 72.3 bp net on 69 nights, 56 % of the module's total.
   The other eight years with 30 or more gated nights average about 5.6 bp net; six of them are positive. The
   spike-day module has only four years with 30 or more nights, and 2020 dominates it too (83.2 bp on 42 nights).
3. **The unread years cannot settle it.** 2023–2026 holds 200 falling-market nights and 60 spike days (counted from
   the state record, no return read). At the development effect size a confirmation at 0.05 would have about a
   44 % chance of passing for the falling-market module and 36 % for spike days, before allowing for caution 1.

The overnight rule here is the one `overnight-shadow/1` records forward (#1215); its gross (5.64) matches the
by-hand figure (5.63).

## Re-run with the review fixes (2026-11-19, a new scratch ledger)

The four run files were run again on grid-svr with the 10-session block null, on a fresh scratch ledger (the old one
already declares these run ids, which the factory now refuses) and a fixed copy of the state record taken that day.
Development days only, as before. The tables above were made with single-night flips and an earlier state record;
these are the figures that stand.

| Run | First-step bar | Passed step-down | Selected | Lowest passing t |
|---|---|---|---|---|
| `xs-overnight-persistence-20261115` | 2.30 (was 2.33) | 7 of 12 | 7 | 2.14 |
| `xs-published-effects-1-20261115` | 2.19 (was 2.27) | 0 of 12 | 0 | — |
| `xs-gated-1-20261115` (`gates_chosen_after_seeing: true`) | 2.50 (was 2.39) | 13 of 16 | 0: hypotheses only | 2.15 |
| `xs-playbook-1-20261115` | 2.33 | 0 of 7 | 0 | — |

Blocks of 10 nights moved the bars by less than 0.12: these spreads cluster less than feared, and the overnight
modules' t values (4.33 at best) stand. The gated run now selects nothing; its 13 passing modules, among them the
5-day reversal in falling markets (13.42 bp gross, 11.93 net, t 2.91, adjusted p 0.009), are hypotheses for the
unread years only. Dropped picks are about 0.002 % of all picks in every module. With the newer state record the
gated overnight module (within sector 20 %) reads 5.70 bp gross, 1.70 net, t 3.34 (was 5.64, 1.64, 3.19). Report
sha256: overnight `8530bde4…`, published `0b8bda1f…`, gated `b0d6d7bb…`, playbook `faa61e09…`.

## First three queue runs (2026-11-19, scratch ledger, development days only)

`run` three times on grid-svr on a new scratch ledger, with the state record copied that day. One-session holds,
1 bp a side, blocks of 10 nights. Report sha256: monthly reversal `6cb75e34…`, industry momentum `82c3c088…`,
52-week high `9728a9fe…`.

| Entry | Best module | Turnover | Gross bp | Net bp | t | Passed step-down |
|---|---|---|---|---|---|---|
| `short-term-reversal-monthly` (Jegadeesh 1990) | within sector 10 % | 0.23 | 4.43 | 3.53 | 2.17 | yes (adjusted p 0.015) |
| `industry-momentum` (Moskowitz and Grinblatt 1999) | pooled 20 % | 0.09 | 0.37 | 0.00 | 0.00 | no |
| `52-week-high` (George and Hwang 2004) | pooled 20 % | 0.11 | −0.98 | −1.41 | −0.59 | no |

Where each card says it paid, cuts of 100+ nights, net bp:

- **Monthly reversal.** It paid in 42 of 46 cuts.
  - Largest: 2020 (+14.2), markets falling and speeding up (+13.5), VIX spike days (+11.8) and fearful VIX nights (+6.7).
  - It did not pay in 2015 (−2.6) or 2017 (−2.3), or inside Energy (−0.9) and Utilities (−0.7).
  - Eleven of thirteen years are positive. It is a candidate for one confirmation look, not a finding: like the 5-day reversal it earns most when markets are under stress.
- **Industry momentum.** It is flat overall.
  - Positive on spike days (+8.0) and in 2013, 2015 and 2017.
  - Negative in 2019 (−9.0) and 2012 (−5.4), and while the Fed's balance sheet was shrinking (−3.3).
  - Eleven sectors and one-session holds are a long way from the paper's 20 industries and monthly holds.
- **52-week high.** It is negative in both rankings, as in the November 15 published-effects run.
  - It paid only in 2015 (+9.7), 2022 (+4.2) and while the balance sheet was shrinking (+3.8).
  - It lost most in falling, slowing markets (−10.5).

Two entries stay queued (weekly reversal, 12-1 momentum, both seen before on development days), one is blocked
(post-earnings drift) and one retired (overnight persistence).
