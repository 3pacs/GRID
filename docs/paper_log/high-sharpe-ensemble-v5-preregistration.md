# Pre-registration: high-Sharpe ensemble v5 — combinations of the arms that beat SPY's Sharpe

Status: registered 2026-11-08 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`.
Opened under Anik's 2026-11-08 decision; this is the last set of the programme he named
("stop if v5 fails"). Code: `trading/high_sharpe_ensemble.py` (`REGISTRATIONS["v5"]`,
`candidate_set("v5")`, `V5_QUALIFIED`, `V5_PRIMARY`, `V5_SECONDARY`),
`scripts/high_sharpe_ensemble_backtest.py --set v5`, `tests/test_high_sharpe_ensemble.py`.
Nothing below may change after the look. Paper only; read-only database session; no governance
row above SHADOW, none unless PASS.

## 0. What was seen before writing this

Sets v1–v4 are spent (all FAIL; inventory §4–§7). Their records are the input to this set: the
qualification rule in §2 is applied to the per-arm Sharpes in those records, and a test
re-derives the list from the committed JSON files. Two findings from those records are used
deliberately and declared: (a) the trailing-Sharpe top-3 selector lost to a naive 1/N of its own
arms in v2 (0.62 vs 0.77) and v3 (0.82 vs 1.07), so v5's **primary** combination holds every
qualified arm with no selection; (b) every qualified arm has a lower return than SPY, so the only
route to a positive excess bound is diversification across arms with different return sources
(rotation, trend, volatility management). No v5 combination's return, Sharpe or selection was
computed on any window before this file; the v5 code ran only on the synthetic smoke panel.

**Cumulative trials.** 278 before this set. v5 adds no new arm (all thirteen were counted in
their own sets) and two ensemble configurations = **280 cumulative trials**.

## 1. Question

Does a diversified, inverse-volatility combination of every GRID-resident or documented rule that
beat SPY's Sharpe in its own registered look also beat SPY on excess return at the watch
criterion fixed in v2 §5?

## 2. Qualification rule and arms

An arm qualifies iff, in its own set's registered look, its out-of-sample annualised Sharpe on
the same segments exceeded SPY buy-and-hold's on those segments (v1: SPY 0.867; v2: 0.867; v3:
0.867; v4: 0.837, daily). Applied to the committed records this yields thirteen arms, all weekly:

| arm | set | own-look Sharpe |
|---|---|---|
| `adaptive_rotation` | v1 | 0.939 |
| `vix_ma_exposure_spy` | v1 | 0.944 |
| `tsmom_12_1_assets_ma10_M` | v2 | 0.882 |
| `tsmom_12_1_assets_ma10_W` | v2 | 1.055 |
| `ma10_spy_W` | v2 | 0.876 |
| `vol_target_spy_M` / `_W` | v3 | 0.922 / 0.933 |
| `vol_target_qqq_M` / `_W` | v3 | 1.033 / 1.074 |
| `risk_parity_4_M` / `_W` | v3 | 0.920 / 0.930 |
| `sixty_forty_vol_scaled_M` / `_W` | v3 | 0.883 / 0.898 |

No v4 arm qualified (best 0.349 at 20 bp). Universe (`UNIVERSE_V5`): the union of v1's thirty
tickers, the v2 asset ETFs and the v3 tickers (EFA, EEM, DBC join; SHY not needed). The §3 coverage
rule applies; the look reports exclusions.

## 3. Data, cadence, costs

Exactly v1 §3 and §4's data, weekly `W-FRI` segments, window 2016-01-04 → 2026-11-06, 10 bp per
side on the combined underlying book, 52-segment warm-up, out-of-sample = every later segment.

## 4. Two pre-stated combinations

- **Primary** (`V5_PRIMARY`): every qualified arm is held, weighted by inverse trailing-52-week
  volatility of its net returns, re-weighted every 4 segments; no Sharpe floor, no correlation
  penalty, no selection (`k = 13`, `min_shrunk_sharpe = −∞`, `corr_penalty = 0`).
- **Secondary** (`V5_SECONDARY`, descriptive): the v1 selector unchanged (trailing-52 shrunk
  Sharpe, top-3, 0.5 correlation penalty, inverse vol).

Controls: SPY buy-and-hold, 1/N over the thirteen arms, best arm in hindsight.

## 5. One look, the watch criterion

`python -m scripts.high_sharpe_ensemble_backtest --set v5 --start 2016-01-04 --end 2026-11-06
--source raw-adj-close`, once, on grid-svr from a read-only checkout of this branch at this commit
or later, pin matching. The verdict is the **primary** combination's: PASS iff all four parts of
the v2 §5 watch criterion hold (Sharpe > SPY on the same segments; one-sided 95 %
block-bootstrap lower bound of mean weekly net excess over SPY > 0; deflated Sharpe probability
> 0.5 at **280** trials with the cross-trial variance over the thirteen arms; drawdown no worse
than SPY's). The secondary is attached and decides nothing.

## 6. What a result means

PASS → the primary combination gets a SHADOW paper book and a `model_registry` row no higher
than SHADOW in a separate reviewed change; its forward record starts. FAIL → the programme Anik
named on 2026-11-08 stops here and is reported to him with all five records; any further set is a
new decision of his. The look is spent either way.

## 7. Integrity

As v1 §7 with `REGISTRATIONS["v5"]`; the tests assert `candidate_set("v5").n_trials_cumulative
== 280`, that `V5_QUALIFIED` equals the set of arms the §2 rule selects from the committed v1–v4
look JSONs, and that the primary configuration selects all arms.

## 8. Deviations (declared)

- The qualification uses Sharpe, not excess return (no arm had a positive excess bound), so v5 is
  a test of whether diversification across Sharpe-positive, lower-return rules can produce excess
  return; the honest prior from v1–v4 is that it cannot, because every component returns less
  than SPY and the combination cannot lever.
- Thirteen arms share three return sources (SPY beta with a brake, trend, and rotation), so the
  effective number of independent bets is small; the deflated Sharpe at 280 trials is unaffected
  by that but the correlation table in the record shows it.
