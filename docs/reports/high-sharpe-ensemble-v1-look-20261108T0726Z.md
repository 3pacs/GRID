# High-Sharpe ensemble v1 — registered look 20261108T0726Z

Verdict: **FAIL** — block-bootstrap one-sided 95% lower bound of mean weekly net excess return vs SPY > 0 AND deflated Sharpe > 0.95 at the declared trial count.

Out-of-sample segments: 513 weekly, 2017-01-06 → 2026-10-30. Prices: raw_series YF:{TICKER}:adj_close via store.observations.read_window (SUCCESS, latest pull); vintages as of 2026-11-08; missing none.

| series | Sharpe (ann.) | ann. return | max DD | total | n |
|---|---|---|---|---|---|
| ensemble (net) | 0.779 | 0.091 | -0.231 | 1.371 | 513 |
| SPY buy-and-hold | 0.867 | 0.143 | -0.318 | 2.747 | 513 |
| 1/N all candidates | 0.134 | 0.006 | -0.102 | 0.060 | 513 |
| best single in hindsight (vix_ma_exposure_spy) | 0.944 | 0.140 | -0.228 | 2.642 | 513 |

Mean weekly net excess vs SPY: -0.00104; one-sided 95 % lower bound -0.00249 (block 1, 10000 resamples, seed 20261108); P(mean ≤ 0) = 0.884.
Deflated Sharpe: 0.000 at n_trials = 248 (SR0 per period 0.3927, observed per period 0.1080) → fail.
PBO over the candidate matrix: 0.000 (12870 CSCV splits); P(OOS loss of the in-sample best) = 0.035.
Ensemble turnover 20.64×/yr; in cash 0.000 of segments.

## Candidates (same out-of-sample segments, net of 10 bp/side)

| candidate | Sharpe | ann. return | max DD | turnover/yr | excess-vs-SPY LB (Bonferroni) | times selected |
|---|---|---|---|---|---|---|
| adaptive_rotation | 0.939 | 0.163 | -0.247 | 31.89 | -0.00195 | 90 |
| s151_xs_momentum | 0.387 | 0.045 | -0.265 | 13.83 | -0.00495 | 72 |
| s151_mean_reversion | -0.687 | -0.101 | -0.700 | 50.82 | -0.00767 | 19 |
| s151_sector_rotation | -0.312 | -0.025 | -0.293 | 16.79 | -0.00627 | 8 |
| qa_dual_horizon_equity | -1.401 | -0.130 | -0.747 | 82.81 | -0.00863 | 2 |
| qa_vol_regime_equity | -1.359 | -0.126 | -0.736 | 82.70 | -0.00852 | 1 |
| vix_ma_exposure_spy | 0.944 | 0.140 | -0.228 | 3.38 | -0.00080 | 108 |

Faults: {'candidate_errors': {'adaptive_rotation': 0, 's151_xs_momentum': 0, 's151_mean_reversion': 0, 's151_sector_rotation': 0, 'qa_dual_horizon_equity': 0, 'qa_vol_regime_equity': 0, 'vix_ma_exposure_spy': 0}, 'segment_returns_nan': 0, 'segment_returns_total': 16950, 'segments': 565}
