# Pre-registration: high-Sharpe ensemble v4 — short-horizon mean reversion on SPY and QQQ

Status: registered 2026-11-08 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`.
Opened under Anik's 2026-11-08 decision to keep running candidate sets until one is worth
watching. Code: `trading/high_sharpe_ensemble.py` (`REGISTRATIONS["v4"]`, `candidate_set("v4")`,
`V4_CONFIG`), `scripts/high_sharpe_ensemble_backtest.py --set v4 --secondary-cost-bps 10`,
`tests/test_high_sharpe_ensemble.py`. Nothing below may change after the look; a change is v5.
Paper only; read-only database session; no governance row above SHADOW, none unless PASS.

## 0. What was seen before writing this

v1, v2 and v3 are spent (all FAIL; inventory §4–§6). Two things learned from them shape nothing
in the arms below but are recorded: every v2/v3 arm had a lower return than SPY, and the
trailing-Sharpe selector lost to 1/N of its own arms in both sets. The same Tiingo coverage check
covers SPY and QQQ (full 2014-12 → 2026-11). No v4 arm's return, Sharpe or selection was computed
on any window before this file; the v4 code ran only on the synthetic random-walk smoke panel
(seed 3), where all arms lost, as mean reversion on a random walk must.

**Cumulative trials.** 270 before this set. v4 adds 6 arms + 2 ensemble configurations (the
primary at 20 bp per side and the secondary at 10 bp) = **278 cumulative trials**.

## 1. Question

Do documented short-horizon mean-reversion rules on the two most liquid index ETFs (Connors'
RSI(2) pullback, the three-lower-closes pullback, turn-of-month seasonality) survive realistic
costs and, combined by the registered selector, beat SPY buy-and-hold at the watch criterion
fixed in v2 §5? These rules trade daily, so this set runs on **daily segments**.

## 2. Universe and arms

Tickers (fixed, `UNIVERSE_V4`): SPY, QQQ. Benchmark SPY. Cash earns zero. Six arms, all daily:

| arm | definition |
|---|---|
| `rsi2_spy`, `rsi2_qqq` | long 100 % when the 2-day RSI (simple up/down sum form) is below 10 and the close is above its 200-session mean; stay long until the 2-day RSI exceeds 60; evaluated statelessly over the last 10 sessions (most recent entry after most recent exit = in) |
| `three_down_spy`, `three_down_qqq` | long 100 % after three consecutive lower closes with the close above its 200-session mean; stay long while no close has risen above the previous close, at most 5 sessions |
| `turn_of_month_spy`, `turn_of_month_qqq` | long 100 % on the first 3 sessions of a calendar month and when fewer than 4 business days remain in the month (business-day calendar, holidays not removed), cash otherwise |

No shorts, no leverage, gross ≤ 1. Nothing here is fit.

## 3. Data

Exactly v1 §3 (Tiingo `adj_close` via `store.observations.read_window`, same source rule and
exclusion screen, vintages as of the run date, decisions from data on or before the decision
date through `PanelPITStore`, realised returns from the prices that followed).

## 4. Selector and portfolio — daily variant (`V4_CONFIG`)

Segments: every business day (`rebalance = "B"`, 252 periods per year), decision at the close,
realised return close-to-close, window 2016-01-04 → 2026-11-06 (prices from 2014-12). Costs:
**20 bp per side** on turnover for the primary run; 10 bp per side for the secondary run.
Selection every 21 segments from the trailing 252 segments of candidate net returns by shrunk
Sharpe `SR × n/(n + 52)` (the `shrink_n0 = 52` constant is unchanged; with n = 252 the factor is
0.83 rather than v1's 0.5, and this is declared), greedy top-`k = 3` with the 0.5 ×
max-correlation penalty, only shrunk Sharpe > 0, inverse-volatility weights summing to one, costs
charged once on the ensemble's underlying book. Warm-up 252 segments; out-of-sample = every later
segment (≈ 2,470 sessions, 2017-01 → 2026-11). Controls: SPY buy-and-hold, 1/N over the six arms,
best arm in hindsight.

## 5. One look, the watch criterion

`python -m scripts.high_sharpe_ensemble_backtest --set v4 --start 2016-01-04 --end 2026-11-06
--source raw-adj-close --secondary-cost-bps 10`, once, on grid-svr from a read-only checkout of
this branch at this commit or later, pin matching. The verdict is the **20 bp run**: PASS iff all
four parts of the v2 §5 watch criterion hold on daily segments (Sharpe annualised by √252, mean
daily excess over SPY with its one-sided 95 % block-bootstrap lower bound, deflated Sharpe
probability > 0.5 at **278** trials with the cross-trial variance over the six arms, drawdown no
worse than SPY's). The 10 bp run is attached to the record as descriptive secondary output and
decides nothing.

## 6. What a result means

PASS → SHADOW paper book and a `model_registry` row no higher than SHADOW in a separate reviewed
change; forward record starts. FAIL → no book, no row; v5 (combinations) is registered with
278 + its arms + its ensembles, before its look, and if v5 fails the programme stops and reports.
The look is spent either way.

## 7. Integrity

As v1 §7 with `REGISTRATIONS["v4"]`; the tests assert `candidate_set("v4").n_trials_cumulative
== 278`, the daily configuration, and that every arm name appears in this file.

## 8. Deviations (declared)

- Close-to-close execution at the decision close is optimistic for a rule that decides on that
  close; the 20 bp primary cost is the declared offset.
- Connors' original exits on RSI(2) > 60 **or** close above the 5-day mean; only the RSI exit is
  used. The 10-session stateless lookback can miss an entry older than 10 sessions whose exit has
  not fired; that shortens, never lengthens, holding periods.
- The turn-of-month calendar counts business days, not NYSE sessions, for "days remaining".
- Cash earns zero; these arms are in cash most of the time, which is the conservative direction.
