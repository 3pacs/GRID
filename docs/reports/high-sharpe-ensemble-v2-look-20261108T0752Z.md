# High-Sharpe ensemble v2 — registered look 20261108T0752Z

Verdict: **FAIL** — ensemble OOS Sharpe > SPY buy-and-hold on the same segments AND one-sided 95% block-bootstrap lower bound of mean weekly net excess over SPY > 0 after costs AND deflated Sharpe > 0.5 (observed per-period Sharpe above the expected maximum at the cumulative trial count) AND ensemble max drawdown no worse than SPY's.

Cumulative trials counted in the deflated Sharpe: 261. Watch criterion parts: Sharpe beats SPY False, excess LB > 0 False, DSR > 0.5 True, drawdown no worse True.

Out-of-sample segments: 513 weekly, 2017-01-06 → 2026-10-30. Prices: raw_series YF:{TICKER}:adj_close via store.observations.read_window (SUCCESS, latest pull); vintages as of 2026-11-08; missing ['XLC', 'XLRE'].

| series | Sharpe (ann.) | ann. return | max DD | total | n |
|---|---|---|---|---|---|
| ensemble (net) | 0.615 | 0.071 | -0.225 | 0.966 | 513 |
| SPY buy-and-hold | 0.867 | 0.143 | -0.318 | 2.747 | 513 |
| 1/N all candidates | 0.771 | 0.084 | -0.200 | 1.212 | 513 |
| best single in hindsight (tsmom_12_1_assets_ma10_W) | 1.055 | 0.072 | -0.062 | 0.981 | 513 |

Mean weekly net excess vs SPY: -0.00140; one-sided 95 % lower bound -0.00242 (block 3, 10000 resamples, seed 20261108); P(mean ≤ 0) = 0.990.
Deflated Sharpe: 0.531 at n_trials = 261 (SR0 per period 0.0818, observed per period 0.0853) → fail.
PBO over the candidate matrix: 0.249 (12870 CSCV splits); P(OOS loss of the in-sample best) = 0.009.
Ensemble turnover 7.03×/yr; in cash 0.008 of segments.

## Candidates (same out-of-sample segments, net of 10 bp/side)

| candidate | Sharpe | ann. return | max DD | turnover/yr | excess-vs-SPY LB (Bonferroni) | times selected |
|---|---|---|---|---|---|---|
| tsmom_12_1_assets_M | 0.704 | 0.062 | -0.189 | 1.25 | -0.00339 | 15 |
| tsmom_12_1_assets_W | 0.796 | 0.069 | -0.180 | 2.51 | -0.00328 | 31 |
| tsmom_12_1_assets_ma10_M | 0.882 | 0.062 | -0.076 | 1.48 | -0.00375 | 7 |
| tsmom_12_1_assets_ma10_W | 1.055 | 0.072 | -0.062 | 2.75 | -0.00357 | 39 |
| tsmom_12_1_sectors_top3_M | 0.778 | 0.129 | -0.316 | 5.14 | -0.00174 | 30 |
| tsmom_12_1_sectors_top3_W | 0.794 | 0.134 | -0.291 | 12.16 | -0.00161 | 23 |
| tsmom_12_1_sectors_top3_ma10_M | 0.599 | 0.087 | -0.316 | 5.27 | -0.00287 | 7 |
| tsmom_12_1_sectors_top3_ma10_W | 0.652 | 0.088 | -0.200 | 12.64 | -0.00288 | 3 |
| gem_dual_momentum_M | 0.338 | 0.042 | -0.318 | 4.87 | -0.00340 | 29 |
| gem_dual_momentum_W | 0.362 | 0.042 | -0.383 | 12.37 | -0.00330 | 13 |
| ma10_spy_M | 0.728 | 0.087 | -0.263 | 1.42 | -0.00307 | 50 |
| ma10_spy_W | 0.876 | 0.103 | -0.158 | 2.43 | -0.00265 | 51 |

Faults: {'candidate_errors': {'tsmom_12_1_assets_M': 0, 'tsmom_12_1_assets_W': 0, 'tsmom_12_1_assets_ma10_M': 0, 'tsmom_12_1_assets_ma10_W': 0, 'tsmom_12_1_sectors_top3_M': 0, 'tsmom_12_1_sectors_top3_W': 0, 'tsmom_12_1_sectors_top3_ma10_M': 0, 'tsmom_12_1_sectors_top3_ma10_W': 0, 'gem_dual_momentum_M': 0, 'gem_dual_momentum_W': 0, 'ma10_spy_M': 0, 'ma10_spy_W': 0}, 'segment_returns_nan': 0, 'segment_returns_total': 10735, 'segments': 565}
