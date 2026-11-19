# Pre-registration: high-Sharpe ensemble v3 — volatility-managed and risk parity

Status: registered 2026-11-08 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`.
Opened under Anik's 2026-11-08 decision to keep running candidate sets until one is worth
watching. Code: `trading/high_sharpe_ensemble.py` (`REGISTRATIONS["v3"]` pins this file;
`candidate_set("v3")`), `scripts/high_sharpe_ensemble_backtest.py --set v3`,
`tests/test_high_sharpe_ensemble.py`. Nothing below may change after the look; a change is v4.
Paper only; read-only database session; no governance row above SHADOW, none unless PASS.

## 0. What was seen before writing this

v1 and v2 are spent (both FAIL; v2's record is in inventory §5: trend arms reached Sharpe 1.06
with drawdown −6 % but half of SPY's return; the 1/N control beat the selector). The same
coverage check as v2 §0 covers SPY, QQQ, TLT, IEF, GLD, DBC (full Tiingo coverage 2014-12 →
2026-11). No v3 arm's return, Sharpe or selection was computed on any window before this file;
the v3 code has run only on the synthetic random-walk smoke panel.

**Cumulative trials.** 261 before this set (240 sweep + v1's 8 + v2's 13). v3 adds 8 arms + 1
ensemble = **270 cumulative trials** for the deflated Sharpe.

## 1. Question

Does a walk-forward top-3 ensemble of volatility-managed and risk-parity rules (Moreira & Muir
volatility targeting, inverse-volatility risk parity, a vol-scaled 60/40) beat SPY buy-and-hold
net of 10 bp per side at the watch criterion fixed in v2 §5? Note the known tension: without
leverage (`MAX_LEVERAGE = 1`) these rules can only de-risk, so they win on drawdown and lose on
return; the criterion needs a positive excess return, which is the honest bar.

## 2. Universe and arms

Tickers (fixed, `UNIVERSE_V3`): SPY, QQQ, TLT, IEF, GLD, DBC. Benchmark SPY. Cash earns zero.
Four rules × {`_M` monthly (first weekly decision date of each month), `_W` weekly} = 8 arms:

| rule | definition |
|---|---|
| `vol_target_spy` | SPY weight = min(10 % ÷ annualised 20-session realised vol of SPY, 1); rest cash |
| `vol_target_qqq` | the same on QQQ |
| `risk_parity_4` | SPY, TLT, GLD, DBC weighted by inverse 60-session realised vol, summing to one |
| `sixty_forty_vol_scaled` | 60 % SPY / 40 % IEF scaled by min(10 % ÷ 20-session realised vol of that mix, 1) |

Realised vol = standard deviation of daily log returns × √252. No shorts, no leverage, gross ≤ 1.

## 3. Data, 4. Selector and portfolio

Exactly v1 §3 and §4 (Tiingo `adj_close` through `store.observations.read_window`, the same
source rule and exclusion screen, weekly `W-FRI` segments 2016-01-04 → 2026-11-06, 10 bp per side,
trailing-52 shrunk-Sharpe top-3 with the 0.5 correlation penalty, inverse-vol weights, 52-segment
warm-up, controls SPY buy-and-hold, 1/N over the eight arms, best arm in hindsight).

## 5. One look, the watch criterion

`python -m scripts.high_sharpe_ensemble_backtest --set v3 --start 2016-01-04 --end 2026-11-06
--source raw-adj-close`, once, on grid-svr from a read-only checkout of this branch at this
commit or later, pin matching. PASS iff all four parts of the v2 §5 watch criterion hold, with
the deflated Sharpe at **270** trials and the cross-trial Sharpe variance over the eight v3 arms.

## 6. What a result means

PASS → SHADOW paper book and a `model_registry` row no higher than SHADOW in a separate reviewed
change; forward record starts. FAIL → no book, no row; v4 (short-horizon mean reversion on
SPY/QQQ at 10 bp and 20 bp) is registered with 270 + its arms + 1. The look is spent either way.

## 7. Integrity

As v1 §7 with `REGISTRATIONS["v3"]`; the tests assert `candidate_set("v3").n_trials_cumulative
== 270` and that every rule name appears in this file.

## 8. Deviations (declared)

- No leverage: the published vol-managed results use leverage up to 1.5–2×; here the arms only
  de-risk, which caps their return below the underlying's in calm years.
- 60/40 uses IEF (7–10 yr Treasuries) for the 40, not an aggregate bond index (AGG/BND have no
  history before 2020 in GRID).
- Cash earns zero; a T-bill return would lift every arm that holds cash, which is the
  conservative direction for the criterion.
