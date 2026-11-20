# High-Sharpe ensemble v6 — regime-conditional, registered look (2026-11-08T08:57Z)

Verdict: **CORRECTION of 20261108T0853Z: long-sample part only; the verdict stays with the original record** (cumulative trials 492). Today's cell: **expand_reflate** (observation month 2026-09-01, as known 2026-11-08): CPI y/y 0.037 vs 0.030 a year earlier, INDPRO y/y 0.022, Baa−Aaa 0.44 (stress threshold 1.04), WTI 6-month log change 0.063, GPR 137 (10-yr 90th pct 128).

Verdict parts: etf_cell_n_segments=None, sharpe_beats_spy_in_cell=False, excess_lb_positive_in_cell=False, dsr_above_half_overall=False, drawdown_no_worse_in_cell=False, long_sample_cell_n_months=307, long_sample_cell_excess_mean_nonneg=False, analog_1946_48_excess_nonneg=True, etf_cell_pass=False, long_sample_not_contradicting=False

## ETF era (weekly, 27 arms, regime-conditional top-3)

| series | Sharpe | ann. return | max DD | excess LB |
|---|---|---|---|---|
| regime-conditional ensemble | — | — | — | — |
| unconditional selector (control) | — | — | — | — |
| SPY buy-and-hold | — | — | — | — |
| 1/N over 27 arms | — | — | — | — |
| best arm in hindsight (None) | — | — | — | — |

DSR — at n_trials None; PBO —; selection fallbacks to unconditional: — of selections.

### ETF era by regime cell (ensemble vs SPY; cells under 52 segments are too thin to read)

| cell | n weeks | share | ens Sharpe | SPY Sharpe | ens ann | SPY ann | ens DD | SPY DD | excess LB |
|---|---|---|---|---|---|---|---|---|---|

## Long sample (monthly, French portfolios + bonds + oil, 8 arms, regime-conditional)

OOS 1936-01-31 → 2026-08-31 (1088 months). Ensemble Sharpe 1.325 vs market 0.743; excess LB -0.00503; DSR 1.000; PBO 0.599.

| cell | n months | ens Sharpe | market Sharpe | ens ann | market ann | excess mean |
|---|---|---|---|---|---|---|
| contract_disinfl | 82 | 1.258 | 1.620 | 0.112 | 0.225 | -0.00856 |
| contract_reflate | 36 | 0.029 | 0.267 | -0.001 | 0.032 | -0.00376 |
| expand_disinflate | 279 | 1.622 | 1.055 | 0.090 | 0.135 | -0.00396 |
| expand_reflate | 307 | 1.514 | 0.640 | 0.096 | 0.087 | -0.00010 |
| oil_shock | 40 | 2.027 | 0.791 | 0.159 | 0.114 | 0.00264 |
| stress | 217 | 1.300 | 0.636 | 0.080 | 0.098 | -0.00246 |

## Analog windows (long sample; annualised returns of the panel columns, cells in force)

**1946-48 post-war disinflation under heavy debt** — lead analog for today; 35 months; cells {'expand_disinflate': 16, 'expand_reflate': 12, 'contract_reflate': 2}. Market -0.025, bills 0.006, bonds 0.015, energy 0.110 (energy − market 0.135), momentum factor 0.108, value factor 0.060, oil 0.310; regime-conditional ensemble 0.019 vs market -0.024 (mean monthly excess 0.00257).

**1973-82 great inflation (failure mode)** — the path to rule out; 119 months; cells {'stress': 53, 'expand_reflate': 41, 'expand_disinflate': 14, 'oil_shock': 11, 'contract_reflate': 1}. Market 0.079, bills 0.085, bonds 0.061, energy 0.100 (energy − market 0.021), momentum factor 0.228, value factor 0.163, oil 0.247; regime-conditional ensemble 0.136 vs market 0.083 (mean monthly excess 0.00287).

**1973-74 embargo, Fed accommodated** — oil shock cell; 14 months; cells {'oil_shock': 6, 'expand_reflate': 5, 'stress': 3, 'contract_reflate': 1}. Market -0.316, bills 0.079, bonds 0.035, energy -0.213 (energy − market 0.103), momentum factor 0.357, value factor 0.258, oil 1.260; regime-conditional ensemble 0.106 vs market -0.220 (mean monthly excess 0.02617).

**1979-81 Iranian revolution, Volcker tightened** — oil shock cell; 35 months; cells {'stress': 21, 'expand_reflate': 7, 'oil_shock': 5, 'expand_disinflate': 3}. Market 0.153, bills 0.122, bonds -0.008, energy 0.280 (energy − market 0.126), momentum factor 0.284, value factor 0.115, oil 0.342; regime-conditional ensemble 0.163 vs market 0.139 (mean monthly excess 0.00088).

**1990-91 Gulf War spike** — oil shock cell; 7 months; cells {'oil_shock': 4, 'contract_reflate': 2, 'expand_reflate': 1, 'contract_disinfl': 1}. Market 0.369, bills 0.069, bonds 0.172, energy 0.023 (energy − market -0.346), momentum factor 0.028, value factor -0.057, oil -0.416; regime-conditional ensemble 0.060 vs market 0.321 (mean monthly excess -0.01936).

**2021-23 post-shock inflation** — current-era inflation; 33 months; cells {'expand_reflate': 10, 'stress': 9, 'contract_disinfl': 8, 'oil_shock': 6, 'expand_disinflate': 1}. Market 0.064, bills 0.023, bonds -0.034, energy 0.282 (energy − market 0.218), momentum factor 0.034, value factor 0.095, oil 0.053; regime-conditional ensemble 0.058 vs market 0.066 (mean monthly excess -0.00170).

**2022 Russia** — oil shock cell; 10 months; cells {'stress': 6, 'expand_reflate': 3, 'oil_shock': 2}. Market -0.150, bills 0.017, bonds -0.109, energy 0.362 (energy − market 0.512), momentum factor 0.281, value factor 0.181, oil -0.196; regime-conditional ensemble 0.001 vs market -0.072 (mean monthly excess 0.00391).

**2025-06 Israel–Iran spike** — current-era event; 1 months; cells {'expand_disinflate': 2}. Market —, bills —, bonds —, energy — (energy − market —), momentum factor —, value factor —, oil —; regime-conditional ensemble 0.264 vs market 0.309 (mean monthly excess -0.00297).

Provenance: {"as_of": "2026-11-08", "panel_shape": [0, 0], "long_sample_panel_shape": [1203, 17], "long_sample_benchmark": "MKT", "db_cross_check_1990_plus": {"INDPRO": {"n_db": 441, "n_compared": 441, "max_abs_diff": 1.3575000000000017, "share_within_0_1pct": 0.9841269841269841}, "FEDFUNDS": {"n_db": 442, "n_compared": 442, "max_abs_diff": 0.0, "share_within_0_1pct": 1.0}}, "excluded": {}, "registration_sha256_observed": "37bedb1a862f782d8efd99abc336492607ecd03ac3d0a2916a23187c5ff8cc1a"}
