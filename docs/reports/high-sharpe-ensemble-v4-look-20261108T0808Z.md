# High-Sharpe ensemble v4 — registered look 20261108T0808Z

Verdict: **FAIL** — ensemble OOS Sharpe > SPY buy-and-hold on the same segments AND one-sided 95% block-bootstrap lower bound of mean weekly net excess over SPY > 0 after costs AND deflated Sharpe > 0.5 (observed per-period Sharpe above the expected maximum at the cumulative trial count) AND ensemble max drawdown no worse than SPY's.

Cumulative trials counted in the deflated Sharpe: 278. Watch criterion parts: Sharpe beats SPY False, excess LB > 0 False, DSR > 0.5 False, drawdown no worse True.

Out-of-sample segments: 2475 weekly, 2017-01-03 → 2026-11-05. Prices: raw_series YF:{TICKER}:adj_close via store.observations.read_window (SUCCESS, latest pull); vintages as of 2026-11-08; missing none.

| series | Sharpe (ann.) | ann. return | max DD | total | n |
|---|---|---|---|---|---|
| ensemble (net) | 0.215 | 0.014 | -0.134 | 0.149 | 2475 |
| SPY buy-and-hold | 0.837 | 0.145 | -0.337 | 2.776 | 2475 |
| 1/N all candidates | 0.109 | 0.005 | -0.117 | 0.049 | 2475 |
| best single in hindsight (three_down_qqq) | 0.349 | 0.017 | -0.097 | 0.185 | 2475 |

Mean weekly net excess vs SPY: -0.00053; one-sided 95 % lower bound -0.00085 (block 3, 10000 resamples, seed 20261108); P(mean ≤ 0) = 0.997.
Deflated Sharpe: 0.012 at n_trials = 278 (SR0 per period 0.0590, observed per period 0.0136) → fail.
PBO over the candidate matrix: 0.428 (12870 CSCV splits); P(OOS loss of the in-sample best) = 0.357.
Ensemble turnover 27.21×/yr; in cash 0.000 of segments.

## Candidates (same out-of-sample segments, net of 10 bp/side)

| candidate | Sharpe | ann. return | max DD | turnover/yr | excess-vs-SPY LB (Bonferroni) | times selected |
|---|---|---|---|---|---|---|
| rsi2_spy | -0.548 | -0.048 | -0.411 | 48.77 | -0.00121 | 17 |
| rsi2_qqq | -0.125 | -0.024 | -0.317 | 44.49 | -0.00113 | 26 |
| three_down_spy | 0.141 | 0.007 | -0.186 | 22.91 | -0.00106 | 56 |
| three_down_qqq | 0.349 | 0.017 | -0.097 | 20.06 | -0.00103 | 66 |
| turn_of_month_spy | 0.134 | 0.009 | -0.276 | 24.13 | -0.00095 | 41 |
| turn_of_month_qqq | 0.252 | 0.025 | -0.317 | 24.13 | -0.00092 | 62 |

Faults: {'candidate_errors': {'rsi2_spy': 0, 'rsi2_qqq': 0, 'three_down_spy': 0, 'three_down_qqq': 0, 'turn_of_month_spy': 0, 'turn_of_month_qqq': 0}, 'segment_returns_nan': 0, 'segment_returns_total': 5454, 'segments': 2727}
