# High-Sharpe ensemble v3 — registered look 20261108T0758Z

Verdict: **FAIL** — ensemble OOS Sharpe > SPY buy-and-hold on the same segments AND one-sided 95% block-bootstrap lower bound of mean weekly net excess over SPY > 0 after costs AND deflated Sharpe > 0.5 (observed per-period Sharpe above the expected maximum at the cumulative trial count) AND ensemble max drawdown no worse than SPY's.

Cumulative trials counted in the deflated Sharpe: 270. Watch criterion parts: Sharpe beats SPY False, excess LB > 0 False, DSR > 0.5 True, drawdown no worse True.

Out-of-sample segments: 513 weekly, 2017-01-06 → 2026-10-30. Prices: raw_series YF:{TICKER}:adj_close via store.observations.read_window (SUCCESS, latest pull); vintages as of 2026-11-08; missing none.

| series | Sharpe (ann.) | ann. return | max DD | total | n |
|---|---|---|---|---|---|
| ensemble (net) | 0.820 | 0.071 | -0.165 | 0.974 | 513 |
| SPY buy-and-hold | 0.867 | 0.143 | -0.318 | 2.747 | 513 |
| 1/N all candidates | 1.067 | 0.092 | -0.149 | 1.381 | 513 |
| best single in hindsight (vol_target_qqq_W) | 1.074 | 0.112 | -0.159 | 1.849 | 513 |

Mean weekly net excess vs SPY: -0.00146; one-sided 95 % lower bound -0.00260 (block 3, 10000 resamples, seed 20261108); P(mean ≤ 0) = 0.982.
Deflated Sharpe: 0.970 at n_trials = 270 (SR0 per period 0.0267, observed per period 0.1137) → pass.
PBO over the candidate matrix: 0.654 (12870 CSCV splits); P(OOS loss of the in-sample best) = 0.000.
Ensemble turnover 5.10×/yr; in cash 0.070 of segments.

## Candidates (same out-of-sample segments, net of 10 bp/side)

| candidate | Sharpe | ann. return | max DD | turnover/yr | excess-vs-SPY LB (Bonferroni) | times selected |
|---|---|---|---|---|---|---|
| vol_target_spy_M | 0.922 | 0.094 | -0.149 | 1.83 | -0.00239 | 40 |
| vol_target_spy_W | 0.933 | 0.093 | -0.135 | 2.93 | -0.00241 | 23 |
| vol_target_qqq_M | 1.033 | 0.112 | -0.165 | 2.01 | -0.00240 | 47 |
| vol_target_qqq_W | 1.074 | 0.112 | -0.159 | 3.17 | -0.00229 | 43 |
| risk_parity_4_M | 0.920 | 0.084 | -0.187 | 1.02 | -0.00311 | 42 |
| risk_parity_4_W | 0.930 | 0.085 | -0.187 | 1.66 | -0.00312 | 68 |
| sixty_forty_vol_scaled_M | 0.883 | 0.073 | -0.173 | 0.93 | -0.00292 | 39 |
| sixty_forty_vol_scaled_W | 0.898 | 0.074 | -0.167 | 1.48 | -0.00290 | 21 |

Faults: {'candidate_errors': {'vol_target_spy_M': 0, 'vol_target_spy_W': 0, 'vol_target_qqq_M': 0, 'vol_target_qqq_W': 0, 'risk_parity_4_M': 0, 'risk_parity_4_W': 0, 'sixty_forty_vol_scaled_M': 0, 'sixty_forty_vol_scaled_W': 0}, 'segment_returns_nan': 0, 'segment_returns_total': 3390, 'segments': 565}
