"""
False Discovery Gauntlet — 5 statistical tests to prevent self-deception.

From QuantaAlpha v2 research:
  - ROBUST: CV >= 75%, permutation p < 0.05, subsample > 50%, val net Sharpe > 0.3
  - MARGINAL: CV >= 50%, permutation p < 0.10
  - UNSTABLE: everything else

Key finding: 0/7 runs achieved ROBUST in 2024 OOS. CV consistency is the real blocker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from alpha_research.validation.metrics import (
    compute_signal_metrics,
    long_short_returns,
    rank_ic,
    sharpe_ratio,
)


@dataclass(frozen=True)
class GauntletResult:
    verdict: str  # ROBUST, MARGINAL, UNSTABLE
    permutation_p: float
    permutation_passed: bool
    deflated_sharpe_passed: bool
    observed_sharpe: float
    deflated_threshold: float
    subsample_stability: float
    subsample_passed: bool
    decay_monotonic: bool
    cv_consistency: float
    cv_passed: bool
    details: dict[str, Any]


def permutation_test(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_shuffles: int = 1000,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> tuple[float, float]:
    """
    Shuffle date labels on signal, recompute Sharpe. If real model's Sharpe
    not in extreme tail of null distribution, signal is noise.

    Returns (p_value, observed_sharpe).
    """
    real_ls = long_short_returns(signal, forward_returns, top_n, cost_bps)
    observed = sharpe_ratio(real_ls)

    null_sharpes = []
    dates = signal.index.tolist()

    for _ in range(n_shuffles):
        shuffled_idx = np.random.permutation(dates)
        shuffled_signal = signal.copy()
        shuffled_signal.index = shuffled_idx
        shuffled_signal.sort_index(inplace=True)

        shuffled_ls = long_short_returns(
            shuffled_signal, forward_returns, top_n, cost_bps
        )
        null_sharpes.append(sharpe_ratio(shuffled_ls))

    null_arr = np.array(null_sharpes)
    p_value = float(np.mean(null_arr >= observed))
    return p_value, observed


def deflated_sharpe_ratio(
    observed_sharpe: float,
    n_models_tested: int,
    n_observations: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> tuple[bool, float]:
    """
    Bailey & Lopez de Prado (2014) — adjusts Sharpe for multiple testing.

    Returns (passed, threshold_sharpe).
    Expected max Sharpe from noise with N models:
      E[max(SR)] ≈ (1 - gamma) * Phi^-1(1 - 1/N) + gamma * Phi^-1(1 - 1/(N*e))
    where gamma ≈ 0.5772 (Euler-Mascheroni).
    """
    if n_models_tested <= 1:
        return True, 0.0

    gamma = 0.5772156649
    n = n_models_tested

    z1 = stats.norm.ppf(1 - 1 / n) if n > 1 else 0
    z2 = stats.norm.ppf(1 - 1 / (n * np.e)) if n > 1 else 0

    expected_max_sr = (1 - gamma) * z1 + gamma * z2

    # Adjust for non-normal returns
    sr_std = np.sqrt(
        (1 + 0.5 * observed_sharpe**2 - skewness * observed_sharpe
         + ((kurtosis - 3) / 4) * observed_sharpe**2)
        / n_observations
    ) if n_observations > 0 else 1.0

    threshold = expected_max_sr * sr_std
    return observed_sharpe > threshold, float(threshold)


def subsample_stability(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_splits: int = 20,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> float:
    """
    Split tickers into random halves N times. Compute RankIC on each half.
    Signal should be broad-based — stability > 50% means not concentrated.

    Returns fraction of splits where both halves have positive mean IC.
    """
    tickers = signal.columns.tolist()
    if len(tickers) < 4:
        return 0.0

    stable_count = 0

    for _ in range(n_splits):
        np.random.shuffle(tickers)
        mid = len(tickers) // 2
        half_a = tickers[:mid]
        half_b = tickers[mid:]

        ic_a = rank_ic(signal[half_a], forward_returns.reindex(columns=half_a))
        ic_b = rank_ic(signal[half_b], forward_returns.reindex(columns=half_b))

        if len(ic_a) > 5 and len(ic_b) > 5:
            if ic_a.mean() > 0 and ic_b.mean() > 0:
                stable_count += 1

    return stable_count / n_splits


def decay_analysis(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    horizons: tuple[int, ...] = (1, 2, 5, 10, 20),
) -> tuple[bool, dict[int, float]]:
    """
    Compute RankIC at multiple forward horizons. Real signals decay smoothly;
    noise shows erratic patterns.

    Returns (is_monotonically_decaying, {horizon: mean_ic}).
    """
    results = {}
    for h in horizons:
        ic = rank_ic(signal, forward_returns, horizon=h)
        results[h] = float(ic.mean()) if len(ic) > 0 else 0.0

    # Check monotonic decay (allowing small violations)
    values = list(results.values())
    if len(values) < 2:
        return True, results

    violations = sum(
        1 for i in range(1, len(values)) if values[i] > values[i - 1] + 0.002
    )
    is_monotonic = violations <= 1  # Allow 1 violation
    return is_monotonic, results


def cv_consistency(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_folds: int = 3,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> float:
    """
    Expanding-window cross-validation. Fraction of folds with positive
    validation net Sharpe.

    This is the hardest bar — QuantaAlpha never achieved > 67% across
    COVID/post-COVID/Ukraine regimes.
    """
    n_dates = len(signal)
    if n_dates < n_folds * 60:
        return 0.0

    fold_size = n_dates // (n_folds + 1)
    positive_folds = 0

    for fold in range(n_folds):
        val_start = (fold + 1) * fold_size
        val_end = min(val_start + fold_size, n_dates)

        val_signal = signal.iloc[val_start:val_end]
        val_returns = forward_returns.iloc[val_start:val_end]

        ls_ret = long_short_returns(val_signal, val_returns, top_n, cost_bps)
        if len(ls_ret) > 10 and sharpe_ratio(ls_ret) > 0:
            positive_folds += 1

    return positive_folds / n_folds


def run_gauntlet(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_models_tested: int = 1,
    top_n: int = 5,
    cost_bps: float = 10.0,
    n_permutations: int = 1000,
    n_subsample_splits: int = 20,
) -> GauntletResult:
    """
    Run the full 5-test False Discovery Gauntlet.

    Returns GauntletResult with verdict: ROBUST, MARGINAL, or UNSTABLE.
    """
    # 1. Permutation test
    perm_p, observed_sr = permutation_test(
        signal, forward_returns, n_permutations, top_n, cost_bps
    )

    # 2. Deflated Sharpe
    ls_ret = long_short_returns(signal, forward_returns, top_n, cost_bps)
    returns_arr = ls_ret.dropna().values
    skew = float(stats.skew(returns_arr)) if len(returns_arr) > 10 else 0.0
    kurt = float(stats.kurtosis(returns_arr, fisher=False)) if len(returns_arr) > 10 else 3.0
    deflated_passed, deflated_threshold = deflated_sharpe_ratio(
        observed_sr, n_models_tested, len(returns_arr), skew, kurt
    )

    # 3. Subsample stability
    sub_stab = subsample_stability(
        signal, forward_returns, n_subsample_splits, top_n, cost_bps
    )

    # 4. Decay analysis
    decay_mono, decay_ics = decay_analysis(signal, forward_returns)

    # 5. CV consistency
    cv_con = cv_consistency(signal, forward_returns, top_n=top_n, cost_bps=cost_bps)

    # Compute net Sharpe for threshold check
    net_sharpe = sharpe_ratio(ls_ret)

    # Verdict
    if (cv_con >= 0.75 and perm_p < 0.05 and sub_stab > 0.50 and net_sharpe > 0.3):
        verdict = "ROBUST"
    elif cv_con >= 0.50 and perm_p < 0.10:
        verdict = "MARGINAL"
    else:
        verdict = "UNSTABLE"

    return GauntletResult(
        verdict=verdict,
        permutation_p=perm_p,
        permutation_passed=perm_p < 0.05,
        deflated_sharpe_passed=deflated_passed,
        observed_sharpe=observed_sr,
        deflated_threshold=deflated_threshold,
        subsample_stability=sub_stab,
        subsample_passed=sub_stab > 0.50,
        decay_monotonic=decay_mono,
        cv_consistency=cv_con,
        cv_passed=cv_con >= 0.75,
        details={
            "net_sharpe": net_sharpe,
            "decay_ics": decay_ics,
            "n_days": len(ls_ret),
            "passes_net_sharpe_threshold": net_sharpe > 1.4,
        },
    )
