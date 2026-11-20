# High-Sharpe ensemble v6 — regime-conditional, registered look (2026-11-08T08:53Z)

Verdict: **FAIL** (cumulative trials 492). Today's cell: **expand_reflate** (observation month 2026-09-01, as known 2026-11-08): CPI y/y 0.037 vs 0.030 a year earlier, INDPRO y/y 0.022, Baa−Aaa 0.44 (stress threshold 1.04), WTI 6-month log change 0.063, GPR 137 (10-yr 90th pct 128).

Verdict parts: etf_cell_n_segments=145, sharpe_beats_spy_in_cell=False, excess_lb_positive_in_cell=False, dsr_above_half_overall=False, drawdown_no_worse_in_cell=True, long_sample_cell_n_months=307, long_sample_cell_excess_mean_nonneg=True, analog_1946_48_excess_nonneg=True, etf_cell_pass=False, long_sample_not_contradicting=True

## ETF era (weekly, 27 arms, regime-conditional top-3)

| series | Sharpe | ann. return | max DD | excess LB |
|---|---|---|---|---|
| regime-conditional ensemble | 0.780 | 0.074 | -0.161 | -0.00261 |
| unconditional selector (control) | 0.670 | 0.069 | -0.195 | -0.00287 |
| SPY buy-and-hold | 0.867 | 0.143 | -0.318 | — |
| 1/N over 27 arms | 0.815 | 0.067 | -0.137 | — |
| best arm in hindsight (vol_target_qqq_W) | 1.074 | 0.112 | -0.159 | — |

DSR 0.000 at n_trials 492; PBO 0.483; selection fallbacks to unconditional: 0.225 of selections.

### ETF era by regime cell (ensemble vs SPY; cells under 52 segments are too thin to read)

| cell | n weeks | share | ens Sharpe | SPY Sharpe | ens ann | SPY ann | ens DD | SPY DD | excess LB |
|---|---|---|---|---|---|---|---|---|---|
| contract_disinfl | 144 | 0.28 | 1.232 | 1.673 | 0.118 | 0.246 | -0.095 | -0.101 | -0.00352 |
| contract_reflate | 28 | 0.05 | -0.016 | -0.352 | -0.010 | -0.183 | -0.073 | -0.318 | -0.01122 |
| expand_disinflate | 87 | 0.17 | 1.396 | 1.355 | 0.101 | 0.229 | -0.069 | -0.155 | -0.00531 |
| expand_reflate | 145 | 0.28 | 0.059 | 0.137 | 0.001 | 0.009 | -0.186 | -0.288 | -0.00209 |
| oil_shock | 53 | 0.10 | 1.505 | 1.553 | 0.180 | 0.234 | -0.052 | -0.091 | -0.00228 |
| stress | 56 | 0.11 | 0.665 | 1.272 | 0.063 | 0.246 | -0.103 | -0.118 | -0.00814 |

### Arms inside today's cell `expand_reflate` (ETF era) vs overall

| arm | Sharpe in cell | ann in cell | Sharpe overall | ann overall | times selected |
|---|---|---|---|---|---|
| adaptive_rotation | 0.584 | 0.080 | 0.939 | 0.163 | 27 |
| s151_xs_momentum | 0.755 | 0.090 | 0.461 | 0.056 | 25 |
| s151_mean_reversion | -0.755 | -0.094 | -0.626 | -0.092 | 13 |
| s151_sector_rotation | -0.211 | -0.016 | -0.312 | -0.025 | 6 |
| qa_dual_horizon_equity | -1.398 | -0.103 | -1.442 | -0.132 | 0 |
| qa_vol_regime_equity | -1.239 | -0.093 | -1.419 | -0.129 | 0 |
| vix_ma_exposure_spy | 0.055 | -0.002 | 0.944 | 0.140 | 29 |
| tsmom_12_1_assets_M | 0.076 | 0.003 | 0.704 | 0.062 | 0 |
| tsmom_12_1_assets_W | 0.174 | 0.011 | 0.796 | 0.069 | 15 |
| tsmom_12_1_assets_ma10_M | 0.724 | 0.049 | 0.882 | 0.062 | 3 |
| tsmom_12_1_assets_ma10_W | 0.828 | 0.055 | 1.055 | 0.072 | 17 |
| tsmom_12_1_sectors_top3_M | 0.375 | 0.051 | 0.778 | 0.129 | 12 |
| tsmom_12_1_sectors_top3_W | 0.301 | 0.038 | 0.794 | 0.134 | 10 |
| tsmom_12_1_sectors_top3_ma10_M | 0.560 | 0.078 | 0.599 | 0.087 | 5 |
| tsmom_12_1_sectors_top3_ma10_W | 0.300 | 0.034 | 0.652 | 0.088 | 0 |
| gem_dual_momentum_M | -0.103 | -0.027 | 0.338 | 0.042 | 5 |
| gem_dual_momentum_W | 0.128 | 0.008 | 0.362 | 0.042 | 2 |
| ma10_spy_M | 0.488 | 0.049 | 0.728 | 0.087 | 23 |
| ma10_spy_W | 0.408 | 0.040 | 0.876 | 0.103 | 13 |
| vol_target_spy_M | 0.398 | 0.036 | 0.922 | 0.094 | 16 |
| vol_target_spy_W | 0.301 | 0.025 | 0.933 | 0.093 | 5 |
| vol_target_qqq_M | 0.359 | 0.036 | 1.033 | 0.112 | 26 |
| vol_target_qqq_W | 0.344 | 0.032 | 1.074 | 0.112 | 22 |
| risk_parity_4_M | 0.280 | 0.019 | 0.920 | 0.084 | 16 |
| risk_parity_4_W | 0.255 | 0.017 | 0.930 | 0.085 | 36 |
| sixty_forty_vol_scaled_M | 0.151 | 0.009 | 0.883 | 0.073 | 25 |
| sixty_forty_vol_scaled_W | 0.028 | -0.001 | 0.898 | 0.074 | 10 |

## Long sample (monthly, French portfolios + bonds + oil, 8 arms, regime-conditional)

OOS 1936-01-31 → 2026-08-31 (1088 months). Ensemble Sharpe 1.325 vs market 0.000; excess LB 0.00598; DSR 1.000; PBO 0.599.

| cell | n months | ens Sharpe | market Sharpe | ens ann | market ann | excess mean |
|---|---|---|---|---|---|---|
| contract_disinfl | 82 | 1.258 | 0.000 | 0.112 | 0.000 | 0.00917 |
| contract_reflate | 36 | 0.029 | 0.000 | -0.001 | 0.000 | 0.00019 |
| expand_disinflate | 279 | 1.622 | 0.000 | 0.090 | 0.000 | 0.00732 |
| expand_reflate | 307 | 1.514 | 0.000 | 0.096 | 0.000 | 0.00785 |
| oil_shock | 40 | 2.027 | 0.000 | 0.159 | 0.000 | 0.01259 |
| stress | 217 | 1.300 | 0.000 | 0.080 | 0.000 | 0.00662 |

## Analog windows (long sample; annualised returns of the panel columns, cells in force)

**1946-48 post-war disinflation under heavy debt** — lead analog for today; 35 months; cells {'expand_disinflate': 16, 'expand_reflate': 12, 'contract_reflate': 2}. Market -0.025, bills 0.006, bonds 0.015, energy 0.110 (energy − market 0.135), momentum factor 0.108, value factor 0.060, oil 0.310; regime-conditional ensemble 0.019 vs market 0.000 (mean monthly excess 0.00161).

**1973-82 great inflation (failure mode)** — the path to rule out; 119 months; cells {'stress': 53, 'expand_reflate': 41, 'expand_disinflate': 14, 'oil_shock': 11, 'contract_reflate': 1}. Market 0.079, bills 0.085, bonds 0.061, energy 0.100 (energy − market 0.021), momentum factor 0.228, value factor 0.163, oil 0.247; regime-conditional ensemble 0.136 vs market 0.000 (mean monthly excess 0.01086).

**1973-74 embargo, Fed accommodated** — oil shock cell; 14 months; cells {'oil_shock': 6, 'expand_reflate': 5, 'stress': 3, 'contract_reflate': 1}. Market -0.316, bills 0.079, bonds 0.035, energy -0.213 (energy − market 0.103), momentum factor 0.357, value factor 0.258, oil 1.260; regime-conditional ensemble 0.106 vs market 0.000 (mean monthly excess 0.00859).

**1979-81 Iranian revolution, Volcker tightened** — oil shock cell; 35 months; cells {'stress': 21, 'expand_reflate': 7, 'oil_shock': 5, 'expand_disinflate': 3}. Market 0.153, bills 0.122, bonds -0.008, energy 0.280 (energy − market 0.126), momentum factor 0.284, value factor 0.115, oil 0.342; regime-conditional ensemble 0.163 vs market 0.000 (mean monthly excess 0.01292).

**1990-91 Gulf War spike** — oil shock cell; 7 months; cells {'oil_shock': 4, 'contract_reflate': 2, 'expand_reflate': 1, 'contract_disinfl': 1}. Market 0.369, bills 0.069, bonds 0.172, energy 0.023 (energy − market -0.346), momentum factor 0.028, value factor -0.057, oil -0.416; regime-conditional ensemble 0.060 vs market 0.000 (mean monthly excess 0.00497).

**2021-23 post-shock inflation** — current-era inflation; 33 months; cells {'expand_reflate': 10, 'stress': 9, 'contract_disinfl': 8, 'oil_shock': 6, 'expand_disinflate': 1}. Market 0.064, bills 0.023, bonds -0.034, energy 0.282 (energy − market 0.218), momentum factor 0.034, value factor 0.095, oil 0.053; regime-conditional ensemble 0.058 vs market 0.000 (mean monthly excess 0.00501).

**2022 Russia** — oil shock cell; 10 months; cells {'stress': 6, 'expand_reflate': 3, 'oil_shock': 2}. Market -0.150, bills 0.017, bonds -0.109, energy 0.362 (energy − market 0.512), momentum factor 0.281, value factor 0.181, oil -0.196; regime-conditional ensemble 0.001 vs market 0.000 (mean monthly excess 0.00016).

**2025-06 Israel–Iran spike** — current-era event; 1 months; cells {'expand_disinflate': 2}. Market —, bills —, bonds —, energy — (energy − market —), momentum factor —, value factor —, oil —; regime-conditional ensemble 0.264 vs market 0.000 (mean monthly excess 0.01973).

Provenance: {"as_of": "2026-11-08", "panel_shape": [3002, 33], "long_sample_panel_shape": [1203, 17], "db_cross_check_1990_plus": {"INDPRO": {"n_db": 441, "n_compared": 441, "max_abs_diff": 1.3575000000000017, "share_within_0_1pct": 0.9841269841269841}, "FEDFUNDS": {"n_db": 442, "n_compared": 442, "max_abs_diff": 0.0, "share_within_0_1pct": 1.0}}, "excluded": {}}
