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

1. Reads the run file and hashes every input file (the data manifest). Nothing is parsed yet.
2. Declares the run on a factory ledger (`record_factory_declared`): run file, manifest hash, trial count.
3. Loads bars, **dropping every row after 2022-12-30**. A run file whose window leaves 2010-01-04 .. 2022-12-29 is
   refused. The later years are kept unread for a confirmation look.
4. For each module (signal × ranking × fraction × horizon) computes one spread per night: mean outcome of the top
   fraction minus the bottom fraction, pooled or inside each sector (pick-weighted across sectors).
5. Judges all modules together: each of `perms` draws flips the sign of every night's net spread, the same flip for
   every module; the best flipped t of each draw sets the bar (step-down adjusted p, one-sided).
6. Writes `discovery-frozen.json`, `report.json`, `report.md`, then closes the run on the ledger
   (`record_factory_result`). Any failure closes it as abandoned.

The SPY factory's null permutes the session order of the target, which asks whether a signal's timing matters. A
long-short spread trades every night, so that permutation leaves its mean where it was. Sign flips test the mean.

## Universe and timing (the same rules edge search 12 registered)

- Members by date from `members.csv` (start set plus dated rows), through `renames.json`.
- A stock is ranked on day d if it has a valid bar on d, its unadjusted close on d−1 is at least $5, and its median
  dollar volume over the 21 sessions ending d−1 is at least $10M. The floor also screens out price files that hold
  another company (Tiingo reuses tickers).
- Every signal uses only what is known before the close of d: closes through d−1, plus the open of d for the
  overnight mean. Outcomes start at the close of d.
- A day with fewer than `min_ranked` ranked stocks is skipped.
- Sector: `sectors.csv` (today's GICS label). A member with no label takes the labelled sector its daily returns
  follow most closely; the report gives that method's accuracy on labelled names (77 % on the first run).

## Signals and horizons

| Signal kind | Meaning (high = long) |
|---|---|
| `mean_overnight` | mean overnight return over `lookback` sessions ending at the open of d, each clipped to ±10 % |
| `mean_intraday` | mean open-to-close return over `lookback` sessions ending d−1 |
| `momentum` | return over `lookback` sessions ending `skip` sessions before the close of d−1 |
| `reversal` | the opposite of the last `lookback` sessions' return |
| `high_52w` | close of d−1 over the highest close of the last `lookback` sessions |

`"sign": -1` turns a signal around. Horizons: `overnight` (close of d to open of d+1), `intraday` (open to close of
d+1), `close_1d` (close of d to close of d+1). Each signal lists its own horizons.

**Cost.** `cost_bp_per_side` of traded notional, four sides a night per $1 long and $1 short, on every horizon. For
slow signals held close to close this overstates turnover and so understates the net; the ranking also shows the
gross. A turnover-aware cost is not built.

## The card

For the best `cards` modules and every selected one, `report.json` holds where the module paid: by year, and by the
state recorded the evening before each decision day in the sector state record (`desk-sector-state/1`, #1209):
market phase, Fed balance-sheet phase, VIX third, spike state, and for within-sector modules each sector, the three
groups, the sector's own phase, and leading against lagging sectors. Cuts are descriptions of a searched sample.

## Run it

```bash
python -m analysis.cross_section_factory run --run-file RUN.json --data-dir DIR --state-db state.db \
  --ledger L.jsonl --anchor A.jsonl --out OUT            # add --genesis LEDGER_ID once to create the ledger
```

`DIR` holds `prices/<TICKER>.csv` (Tiingo daily columns), `members.csv`, `sectors.csv`, `renames.json`.

## Evidence adapter

`report.json` carries the fields `analysis/desk_factory_report.py` reads, with `net_dollars` holding the net return
per $1 a side per night (named in `target_unit`). Its `state` is `CROSS_SECTION_DEV_RANKING_ONLY`; the adapter
accepts only `INTRADAY_DEV_RANKING_ONLY` today, so a cross-section report is refused until that file's owner admits
the new state. The test proves every other field passes.

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
