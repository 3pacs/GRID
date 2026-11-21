# Pre-registration: high-Sharpe ensemble v2 — trend and dual momentum

Status: registered 2026-11-08 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`.
Decision that opened this set: Anik, 2026-11-08, after reading v1's FAIL: "lets keep running
candidate sets until we hit something worth watching". Code: `trading/high_sharpe_ensemble.py`
(`REGISTRATIONS["v2"]` pins this file's LF sha256; `candidate_set("v2")`),
`scripts/high_sharpe_ensemble_backtest.py --set v2`, `tests/test_high_sharpe_ensemble.py`.
Nothing below may change after the look. Any change is v3 with a new file and a new pin.
Paper only; read-only database session; no table written; no order; no governance row above
SHADOW, and none at all unless the watch criterion passes.

## 0. What was seen before writing this

v1 (`high-sharpe-ensemble-v1-preregistration.md`) was registered and its one look was spent on
2026-11-08T07:26Z: FAIL (ensemble Sharpe 0.78 vs SPY 0.87; adaptive_rotation and the VIX/MA SPY
scalar 0.94 each; no positive excess bound). Its selector, cadence, costs, window and the Tiingo
price basis are reused unchanged here; only the candidate universe changes. Before this file was
written, one read-only coverage check of Tiingo `adj_close` observation counts, first/last dates,
counts of day jumps above 10 % and first-to-last ratios was run for the tickers below (all have
full 2014-12 → 2026-11 coverage except XLC from 2018-06-19 and XLRE from 2015-10-08; BIL, AGG and
ACWX only from 2020, so SHY and IEF stand in). No candidate return, Sharpe or selection of any v2
arm was computed on any window before this registration; the only run of the v2 code was one
synthetic random-walk smoke test (seed 2).

**Cumulative trials.** 240 rotation-sweep configurations + v1's 7 candidates + v1's ensemble
= 248 trials before this set. v2 adds 12 arms + 1 ensemble = **261 cumulative trials**, which is
the `n_trials` the deflated Sharpe uses here. Every later set adds its own and never resets.

## 1. Question

Does a walk-forward top-3 ensemble of documented trend-following and dual-momentum rules
(Jegadeesh–Titman 12-1 momentum, Moskowitz–Ooi–Pedersen time-series momentum, Antonacci's Global
Equities Momentum, Faber's 10-month moving-average rule) on liquid ETFs beat SPY buy-and-hold net
of 10 bp per side, at the watch criterion in §5?

## 2. Universe and arms

Tickers (fixed, `UNIVERSE_V2`): asset classes SPY, QQQ, IWM, EFA, EEM, TLT, IEF, GLD, DBC; the
eleven SPDR select sectors XLE, XLU, XLV, XLB, XLI, XLF, XLK, XLP, XLY, XLC, XLRE; SHY as the
T-bill hurdle. Benchmark SPY. Cash earns zero. The §3 coverage rule of v1 applies unchanged, so
XLC (70 % of SPY's dates) is expected to be excluded and the sector arms will rank ten sectors;
the look report lists the actual exclusions.

Six rules, each run as two arms — `_W` decides on every weekly segment date, `_M` decides on the
first weekly segment date of each calendar month and holds in between (`Cadenced`) — for twelve
candidates:

| rule | definition |
|---|---|
| `tsmom_12_1_assets` | for each of the 9 asset ETFs, long 1/9 if its return from 252 to 21 sessions ago is positive, else that 1/9 in cash |
| `tsmom_12_1_assets_ma10` | the same, and only if the last close is above its 210-session simple moving average |
| `tsmom_12_1_sectors_top3` | the 3 sectors with the highest 12-1 return, 1/3 each, only those with positive 12-1 return |
| `tsmom_12_1_sectors_top3_ma10` | the same with the 210-session MA filter |
| `gem_dual_momentum` | SPY vs EFA by 252-session return; hold the winner (100 %) if its return beats SHY's over the same window, else 100 % IEF |
| `ma10_spy` | 100 % SPY when SPY is above its 210-session SMA, else cash |

Gross exposure of every arm is at most 1; no shorts. Nothing here is fit.

## 3. Data

Exactly v1 §3: `raw_series` `YF:{T}:adj_close` through `store.observations.read_window`, Tiingo
when it covers ≥ 95 % of SPY's dates else yfinance, the same exclusion screen (coverage < 90 %,
weekend dates, > 25 reversing jumps, stale > 10 sessions), VIX unused by any v2 arm but loaded,
vintages as of the run date, decisions from data on or before the decision date
(`PanelPITStore`), realised returns from the prices that followed. Walk-forward backtest, not a
forward record.

## 4. Selector and portfolio

Exactly v1 §4: weekly `W-FRI` segments, window 2016-01-04 → 2026-11-06 (prices loaded from
2014-12), candidate net returns after 10 bp per side on turnover, selection every 4 segments
from the trailing 52 segments by shrunk Sharpe `SR × n/(n+52)`, greedy top-`k = 3` with a 0.5 ×
max-correlation penalty, only shrunk Sharpe > 0, inverse-volatility weights summing to one,
costs charged once on the ensemble's underlying book, warm-up 52 segments, out-of-sample = every
later segment (≈ 513 weeks, 2017-01 → 2026-10), controls SPY buy-and-hold, 1/N over the twelve
arms, best single arm in hindsight.

## 5. One look, the watch criterion

The look is `python -m scripts.high_sharpe_ensemble_backtest --set v2 --start 2016-01-04 --end
2026-11-06 --source raw-adj-close`, once, on grid-svr against the production database from a
read-only checkout of this branch at this registration's commit or later, with the v2 pin
matching this file. Its JSON and Markdown are committed to the branch as the record.

"Worth watching" (fixed now for v2 and every later set) — PASS iff all four hold on the
out-of-sample segments:

1. the ensemble's annualised Sharpe exceeds SPY buy-and-hold's on the same segments;
2. the mean weekly net excess return over SPY has a one-sided 95 % circular-block-bootstrap
   lower bound above zero (10,000 resamples, seed 20261108, block from
   `autocorrelation_block`), after 10 bp per side;
3. the deflated Sharpe probability exceeds 0.5 at the cumulative trial count (261): the observed
   per-period Sharpe is above the expected maximum of that many null trials (Bailey & López de
   Prado), with the cross-trial Sharpe variance taken over this set's twelve arms;
4. the ensemble's maximum drawdown is no worse than SPY's over the same segments.

Everything else (annual return, turnover, per-arm Sharpe and Bonferroni α/12 excess bounds, PBO
over the twelve-arm matrix, selection history, v1's stricter DSR > 0.95 criterion) is reported
and decides nothing.

## 6. What a result means

- PASS: the ensemble gets a SHADOW paper book — a `paper_strategies`/`paper_trades` strategy with
  no capital and a `model_registry` row no higher than SHADOW through `governance/registry.py` —
  in a separate reviewed change, and its forward record starts. Nothing more.
- FAIL: no book, no row; the next set (v3, volatility-managed and risk parity) is registered with
  the cumulative count 261 + its arms + 1, before its look.
- The look is spent either way; v2 is never re-run on this data.

## 7. Integrity

As v1 §7, with `REGISTRATIONS["v2"]` as the pin and `candidate_set("v2").n_trials_cumulative ==
261` asserted by the tests. The look records the set name, the pins, the commit, the host, the
source per ticker, exclusions, panel shape, PIT reads, candidate failures and missing segment
returns.

## 8. Deviations (declared)

- EFA replaces ACWX (no history before 2020) in GEM; IEF replaces AGG as the defensive leg; SHY
  replaces T-bills as the absolute-momentum hurdle. Cash still earns zero in the portfolio.
- "Monthly" is the first weekly decision date of each calendar month, not the last trading day.
- The 10-month MA is 210 sessions; 12 months is 252 sessions; the skip month is 21 sessions.
