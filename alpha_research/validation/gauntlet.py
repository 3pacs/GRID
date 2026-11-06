"""
False Discovery Gauntlet — 5 statistical tests to prevent self-deception.

From QuantaAlpha v2 research:
  - ROBUST: CV >= 75%, permutation p < 0.05, subsample > 50%, val net Sharpe > 0.3
  - MARGINAL: CV >= 50%, permutation p < 0.10
  - UNSTABLE: everything else

Key finding: 0/7 runs achieved ROBUST in 2024 OOS. CV consistency is the real blocker.

S6 (2026-11-06) corrections:

* Deflated Sharpe ratio follows Bailey & López de Prado (2014): per-period
  Sharpe, skewness/kurtosis-adjusted PSR, and a threshold ``SR0`` built from
  the *cross-trial* variance of the trials' Sharpe ratios and the caller's
  trial count. The old code compared an annualized Sharpe against a threshold
  built from per-period standard errors (about sqrt(252) ~ 16x too lenient).
* The permutation null reorders contiguous *blocks* of signal dates
  (:func:`analysis.offline_research_proof.block_permutations`), so the
  signal's serial correlation survives in the null. The block is declared or
  sized from the lag-1 autocorrelation of the real long-short returns
  (:func:`analysis.offline_research_proof.autocorrelation_block`), never
  below overlap depth + 1. The old per-date shuffle was anti-conservative
  (30% rejections at nominal 5% on a persistent null panel).
* p-values are ``(1 + #null >= observed) / (perms + 1)`` (never 0), the
  default permutation count is derived from the smallest threshold the
  verdict uses (:func:`analysis.ledger_steered_exploration.perms_for_threshold`),
  and every result reports ``min_attainable_p``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, NamedTuple

import numpy as np
import pandas as pd
from scipy import stats

from alpha_research.validation.metrics import (
    long_short_returns,
    rank_ic,
    sharpe_ratio,
)
from analysis.ledger_steered_exploration import (
    PERMS_CAP,
    PERMS_MARGIN,
    min_attainable_p,
    perms_for_threshold,
)
from analysis.offline_research_proof import autocorrelation_block, block_permutations

EULER_GAMMA = 0.5772156649015329
# The smallest p threshold the verdict uses (ROBUST needs permutation p < 0.05).
PERMUTATION_ALPHA = 0.05
PERMUTATION_FLOOR = 999
DSR_CONFIDENCE = 0.95


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
    # Bailey & López de Prado DSR = P[true SR > SR0]; None when it cannot be
    # computed (several trials but no cross-trial Sharpe variance supplied).
    deflated_sharpe: float | None = None
    min_attainable_p: float = math.nan
    n_permutations: int = 0
    permutation_block: int = 1


class PermutationResult(NamedTuple):
    p_value: float
    observed_sharpe: float  # annualized, as reported by sharpe_ratio()
    min_attainable_p: float  # 1 / (n_permutations + 1)
    n_permutations: int
    block: int  # permutation block length, in signal dates
    block_basis: dict[str, Any]


@dataclass(frozen=True)
class DeflatedSharpe:
    """Bailey & López de Prado (2014) deflated Sharpe ratio. All Sharpe values per period."""

    dsr: float  # Phi((SR - SR0) sqrt(T - 1) / sqrt(1 - g3 SR + (g4 - 1) / 4 SR^2))
    passed: bool  # dsr > confidence
    sharpe: float  # observed per-period Sharpe ratio
    sr0: float  # sqrt(V[SR_n]) * E[max of n_trials standard normals]
    expected_max_z: float
    n_trials: int
    n_observations: int
    trial_sharpe_variance: float
    skewness: float
    kurtosis: float  # non-excess (normal = 3)
    confidence: float


def permutation_block_length(
    ls_returns: pd.Series | np.ndarray,
    block: int = 0,
    overlap_depth: int = 0,
) -> tuple[int, dict[str, Any]]:
    """Permutation block (in dates) for the gauntlet's null.

    A declared ``block`` is used as is, but never below ``overlap_depth + 1``
    (refused). ``block=0`` sizes it from the lag-1 autocorrelation of the real
    long-short returns with :func:`autocorrelation_block` (floor
    ``overlap_depth + 1``, at least ``MIN_BLOCKS`` blocks).
    """
    if overlap_depth < 0 or block < 0:
        raise ValueError("block and overlap_depth must be >= 0")
    floor = overlap_depth + 1
    if block:
        if block < floor:
            raise ValueError(
                f"permutation block {block} is shorter than overlap depth + 1 = {floor}"
            )
        return int(block), {"rule": "declared", "overlap_floor": floor}
    size, basis = autocorrelation_block(np.asarray(ls_returns, dtype=float), overlap_depth)
    return int(size), basis


def permutation_test(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_shuffles: int | None = None,
    top_n: int = 5,
    cost_bps: float = 10.0,
    seed: int = 42,
    *,
    block: int = 0,
    overlap_depth: int = 0,
    alpha: float = PERMUTATION_ALPHA,
    perms_cap: int = PERMS_CAP,
) -> PermutationResult:
    """
    Block-permute the signal's dates, recompute Sharpe. If the real model's
    Sharpe is not in the extreme upper tail of that null, the signal is noise.

    Contiguous blocks of dates keep their internal order (the signal's serial
    correlation is kept); see :func:`permutation_block_length` for the block.
    ``n_shuffles=None`` derives the count so the minimum attainable p is at
    least ``PERMS_MARGIN`` x below ``alpha`` (floor 999); it raises
    ``PermutationBudgetError`` if ``perms_cap`` cannot reach that.

    Returns a :class:`PermutationResult` (one-sided upper-tail p).
    """
    if n_shuffles is None:
        n_shuffles = perms_for_threshold(alpha, floor=PERMUTATION_FLOOR, cap=perms_cap)
    if n_shuffles < 1:
        raise ValueError("n_shuffles must be >= 1")
    real_ls = long_short_returns(signal, forward_returns, top_n, cost_bps)
    observed = sharpe_ratio(real_ls)
    size, basis = permutation_block_length(real_ls.dropna(), block, overlap_depth)

    dates = signal.index
    null_sharpes = np.empty(n_shuffles)
    for i, order in enumerate(block_permutations(len(dates), size, int(n_shuffles), int(seed))):
        shuffled_signal = signal.iloc[order]
        shuffled_signal.index = dates
        shuffled_ls = long_short_returns(
            shuffled_signal, forward_returns, top_n, cost_bps
        )
        null_sharpes[i] = sharpe_ratio(shuffled_ls)

    extreme = int(np.sum(null_sharpes >= observed - 1e-12))
    return PermutationResult(
        p_value=(1 + extreme) / (n_shuffles + 1),
        observed_sharpe=observed,
        min_attainable_p=min_attainable_p(n_shuffles),
        n_permutations=int(n_shuffles),
        block=size,
        block_basis=basis,
    )


def expected_max_sharpe(n_trials: int, trial_sharpe_variance: float) -> tuple[float, float]:
    """Expected maximum Sharpe of ``n_trials`` null trials (Bailey & López de Prado 2014).

    SR0 = sqrt(V[SR_n]) * ((1 - g) Phi^-1(1 - 1/N) + g Phi^-1(1 - 1/(N e))),
    g the Euler-Mascheroni constant, V[SR_n] the variance of the trials'
    Sharpe ratios across trials. Returns ``(SR0, E[max z])``; one trial
    deflates nothing (SR0 = 0).
    """
    if int(n_trials) != n_trials or n_trials < 1:
        raise ValueError("n_trials must be a positive integer")
    if not math.isfinite(trial_sharpe_variance) or trial_sharpe_variance < 0:
        raise ValueError("trial_sharpe_variance must be finite and >= 0")
    if n_trials == 1:
        return 0.0, 0.0
    z = (1 - EULER_GAMMA) * stats.norm.ppf(1 - 1 / n_trials) + EULER_GAMMA * stats.norm.ppf(
        1 - 1 / (n_trials * math.e)
    )
    return float(math.sqrt(trial_sharpe_variance) * z), float(z)


def probabilistic_sharpe_ratio(
    sharpe: float,
    benchmark_sharpe: float,
    n_observations: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """PSR: P[true SR > benchmark] for a per-period Sharpe over ``n_observations``.

    Phi((SR - SR*) sqrt(T - 1) / sqrt(1 - g3 SR + (g4 - 1) / 4 SR^2)), with
    g3 the skewness and g4 the (non-excess) kurtosis of the returns.
    """
    values = (sharpe, benchmark_sharpe, skewness, kurtosis)
    if not all(math.isfinite(v) for v in values):
        raise ValueError("Sharpe, skewness and kurtosis must be finite")
    if n_observations < 2:
        raise ValueError("PSR needs at least 2 observations")
    spread = 1 - skewness * sharpe + (kurtosis - 1) / 4 * sharpe**2
    if spread <= 0:
        raise ValueError("non-positive Sharpe-ratio variance (check skewness/kurtosis)")
    z = (sharpe - benchmark_sharpe) * math.sqrt(n_observations - 1) / math.sqrt(spread)
    return float(stats.norm.cdf(z))


def trial_sharpe_variance(trial_sharpes: Sequence[float]) -> float:
    """Sample variance (ddof=1) of the trials' per-period Sharpe ratios."""
    arr = np.asarray(trial_sharpes, dtype=float)
    if arr.ndim != 1 or len(arr) < 2 or not np.all(np.isfinite(arr)):
        raise ValueError("need >= 2 finite per-period trial Sharpe ratios")
    return float(np.var(arr, ddof=1))


def deflated_sharpe_ratio(
    sharpe: float,
    n_trials: int,
    n_observations: int,
    trial_sharpe_variance: float,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
    confidence: float = DSR_CONFIDENCE,
) -> DeflatedSharpe:
    """
    Bailey & López de Prado (2014), "The Deflated Sharpe Ratio".

    DSR = PSR(SR0): the probability that the true Sharpe exceeds the expected
    maximum Sharpe of ``n_trials`` null trials. Every Sharpe input is **per
    period** (divide an annualized Sharpe by sqrt(periods per year) and an
    annualized variance by periods per year). ``trial_sharpe_variance`` is the
    variance of the Sharpe ratios *across* the trials (not the observed
    Sharpe's own standard error); ``n_trials`` comes from the caller's ledger.
    ``passed`` is ``dsr > confidence``.
    """
    if not 0 < confidence < 1:
        raise ValueError("confidence must be in (0, 1)")
    sr0, z = expected_max_sharpe(n_trials, trial_sharpe_variance)
    dsr = probabilistic_sharpe_ratio(sharpe, sr0, n_observations, skewness, kurtosis)
    return DeflatedSharpe(
        dsr=dsr,
        passed=bool(dsr > confidence),
        sharpe=float(sharpe),
        sr0=sr0,
        expected_max_z=z,
        n_trials=int(n_trials),
        n_observations=int(n_observations),
        trial_sharpe_variance=float(trial_sharpe_variance),
        skewness=float(skewness),
        kurtosis=float(kurtosis),
        confidence=float(confidence),
    )


def subsample_stability(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_splits: int = 20,
    top_n: int = 5,
    cost_bps: float = 10.0,
    seed: int = 42,
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
    rng = np.random.default_rng(seed)

    for _ in range(n_splits):
        rng.shuffle(tickers)
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
    n_permutations: int | None = None,
    n_subsample_splits: int = 20,
    seed: int = 42,
    *,
    trial_sharpes: Sequence[float] | None = None,
    periods_per_year: int = 252,
    block: int = 0,
    overlap_depth: int = 0,
    permutation_alpha: float = PERMUTATION_ALPHA,
) -> GauntletResult:
    """
    Run the full 5-test False Discovery Gauntlet.

    ``n_models_tested`` is the number of trials behind this signal and
    ``trial_sharpes`` their per-period Sharpe ratios (all of them, this one
    included); the deflated Sharpe needs their cross-trial variance. With
    several trials and no ``trial_sharpes`` the DSR is reported unavailable
    and ``deflated_sharpe_passed`` is False. ``n_permutations=None`` derives
    the count from ``permutation_alpha`` (see :func:`permutation_test`).

    Returns GauntletResult with verdict: ROBUST, MARGINAL, or UNSTABLE.
    """
    if int(n_models_tested) != n_models_tested or n_models_tested < 1:
        raise ValueError("n_models_tested must be a positive integer")
    if trial_sharpes is not None and len(trial_sharpes) != n_models_tested:
        raise ValueError("trial_sharpes must hold one per-period Sharpe per tested model")

    # 1. Permutation test (block null)
    perm = permutation_test(
        signal,
        forward_returns,
        n_permutations,
        top_n,
        cost_bps,
        seed=seed,
        block=block,
        overlap_depth=overlap_depth,
        alpha=permutation_alpha,
    )
    perm_p, observed_sr = perm.p_value, perm.observed_sharpe

    # 2. Deflated Sharpe (per-period Sharpe, cross-trial variance)
    ls_ret = long_short_returns(signal, forward_returns, top_n, cost_bps)
    returns_arr = ls_ret.dropna().values
    skew = float(stats.skew(returns_arr)) if len(returns_arr) > 10 else 0.0
    kurt = float(stats.kurtosis(returns_arr, fisher=False)) if len(returns_arr) > 10 else 3.0
    per_period_sr = sharpe_ratio(ls_ret, annualize=1)
    dsr: DeflatedSharpe | None = None
    dsr_note = None
    if len(returns_arr) < 2:
        dsr_note = "fewer than 2 long-short returns"
    elif n_models_tested == 1:
        dsr = deflated_sharpe_ratio(per_period_sr, 1, len(returns_arr), 0.0, skew, kurt)
    elif trial_sharpes is None:
        dsr_note = (
            f"{n_models_tested} trials but no trial_sharpes: the cross-trial Sharpe "
            "variance is unknown, so SR0 cannot be computed"
        )
    else:
        dsr = deflated_sharpe_ratio(
            per_period_sr,
            n_models_tested,
            len(returns_arr),
            trial_sharpe_variance(trial_sharpes),
            skew,
            kurt,
        )

    # 3. Subsample stability
    sub_stab = subsample_stability(
        signal, forward_returns, n_subsample_splits, top_n, cost_bps, seed=seed
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

    details: dict[str, Any] = {
        "net_sharpe": net_sharpe,
        "decay_ics": decay_ics,
        "n_days": len(ls_ret),
        "passes_net_sharpe_threshold": net_sharpe > 1.4,
        "permutation_block_basis": perm.block_basis,
        "permutation_resolution_ok": perm.min_attainable_p * PERMS_MARGIN
        <= permutation_alpha,
        "per_period_sharpe": per_period_sr,
    }
    if dsr is not None:
        details["deflated_sharpe"] = {
            "dsr": dsr.dsr,
            "sr0_per_period": dsr.sr0,
            "expected_max_z": dsr.expected_max_z,
            "n_trials": dsr.n_trials,
            "trial_sharpe_variance": dsr.trial_sharpe_variance,
            "skewness": dsr.skewness,
            "kurtosis": dsr.kurtosis,
        }
    else:
        details["deflated_sharpe_unavailable"] = dsr_note

    return GauntletResult(
        verdict=verdict,
        permutation_p=perm_p,
        permutation_passed=perm_p < 0.05,
        deflated_sharpe_passed=bool(dsr is not None and dsr.passed),
        observed_sharpe=observed_sr,
        # SR0 annualized, in the same units as observed_sharpe
        deflated_threshold=(
            dsr.sr0 * math.sqrt(periods_per_year) if dsr is not None else math.nan
        ),
        subsample_stability=sub_stab,
        subsample_passed=sub_stab > 0.50,
        decay_monotonic=decay_mono,
        cv_consistency=cv_con,
        cv_passed=cv_con >= 0.75,
        details=details,
        deflated_sharpe=None if dsr is None else dsr.dsr,
        min_attainable_p=perm.min_attainable_p,
        n_permutations=perm.n_permutations,
        permutation_block=perm.block,
    )
