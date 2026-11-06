"""S6 regression tests for the False Discovery Gauntlet's statistics.

1. Deflated Sharpe ratio (Bailey & López de Prado 2014): per-period Sharpe,
   threshold from the cross-trial Sharpe variance and the caller's trial count.
   The old code compared an annualized Sharpe with a per-period threshold and
   passed the paper's own worked example (threshold 0.34 vs SR 2.5).
2. Block permutation null: the old per-date shuffle rejected 21.7% (T=250) and
   30% (T=160) of persistent null panels at nominal 5%.
3. Permutation p-values are (1 + extreme) / (perms + 1) with ``min_attainable_p``
   reported, and the default count follows the smallest threshold.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from alpha_research.validation import gauntlet
from alpha_research.validation.gauntlet import (
    deflated_sharpe_ratio,
    expected_max_sharpe,
    permutation_block_length,
    permutation_test,
    probabilistic_sharpe_ratio,
    run_gauntlet,
)
from analysis.ledger_steered_exploration import PermutationBudgetError


def ar1(rng: np.random.Generator, n: int, k: int, phi: float) -> np.ndarray:
    e = rng.standard_normal((n, k))
    x = np.empty((n, k))
    x[0] = e[0] / math.sqrt(1 - phi**2)
    for t in range(1, n):
        x[t] = phi * x[t - 1] + e[t]
    return x


def panel(values: np.ndarray) -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-01", periods=values.shape[0])
    return pd.DataFrame(values, index=dates, columns=[f"T{i}" for i in range(values.shape[1])])


def fast_long_short(signal, forward_returns, top_n=5, cost_bps=10.0):
    """Vectorized long_short_returns for complete, cost-free panels (tests only)."""
    fwd = forward_returns.shift(-1).reindex(signal.index)
    s, f = signal.to_numpy(), fwd.to_numpy()
    ok = ~np.isnan(f).any(axis=1)
    order = np.argsort(s, axis=1)
    rows = np.arange(len(s))[:, None]
    spread = f[rows, order[:, -top_n:]].mean(axis=1) - f[rows, order[:, :top_n]].mean(axis=1)
    return pd.Series(spread[ok], index=signal.index[ok])


# --- 1. deflated Sharpe ratio ---------------------------------------------------------


def test_dsr_matches_bailey_lopez_de_prado_2014_numerical_example():
    """Bailey, D. H. & López de Prado, M. (2014), "The Deflated Sharpe Ratio:
    Correcting for Selection Bias, Backtest Overfitting and Non-Normality",
    Journal of Portfolio Management 40(5), numerical example: N = 100 trials,
    V[SR_n] = 1/2 (annualized), T = 1250 daily observations (250 a year),
    skewness -3, kurtosis 10, best annualized SR 2.5. The paper reports
    SR0 = 0.1132 (per period) and DSR = 0.9004: not significant at 95%.
    """
    result = deflated_sharpe_ratio(
        sharpe=2.5 / math.sqrt(250),
        n_trials=100,
        n_observations=1250,
        trial_sharpe_variance=0.5 / 250,
        skewness=-3.0,
        kurtosis=10.0,
    )
    assert result.sr0 == pytest.approx(0.1132, abs=5e-4)
    assert result.dsr == pytest.approx(0.9004, abs=5e-4)
    assert result.passed is False


def test_dsr_threshold_comes_from_cross_trial_variance_not_the_observed_se():
    a = expected_max_sharpe(96, 1 / 500)
    b = expected_max_sharpe(96, 4 / 500)
    assert b[0] == pytest.approx(2 * a[0])  # scales with sqrt(V[SR_n])
    low = deflated_sharpe_ratio(0.01, 96, 500, 1 / 500)
    high = deflated_sharpe_ratio(0.20, 96, 500, 1 / 500)
    assert low.sr0 == high.sr0  # independent of the observed Sharpe
    assert expected_max_sharpe(1, 1.0) == (0.0, 0.0)


def test_dsr_rejects_a_noise_level_best_of_96_trials():
    """Annualized SR 1.0 over 500 days as the best of 96 trials whose Sharpe
    variance is the null 1/T: the old function passed it (threshold 0.138
    against an annualized 1.0); the expected best-of-96 is ~1.8 annualized."""
    result = deflated_sharpe_ratio(1.0 / math.sqrt(252), 96, 500, 1 / 500)
    assert result.sr0 * math.sqrt(252) == pytest.approx(1.79, abs=0.02)
    assert result.passed is False
    assert result.dsr < 0.2


def test_one_trial_is_psr_against_zero_and_inputs_are_validated():
    one = deflated_sharpe_ratio(0.1, 1, 250, 0.0, skewness=-0.5, kurtosis=5.0)
    assert one.sr0 == 0.0
    assert one.dsr == pytest.approx(probabilistic_sharpe_ratio(0.1, 0.0, 250, -0.5, 5.0))
    with pytest.raises(ValueError):
        deflated_sharpe_ratio(0.1, 0, 250, 0.0)
    with pytest.raises(ValueError):
        deflated_sharpe_ratio(0.1, 10, 250, -1.0)
    with pytest.raises(ValueError):
        deflated_sharpe_ratio(float("nan"), 10, 250, 0.01)
    with pytest.raises(ValueError):
        deflated_sharpe_ratio(0.1, 10, 1, 0.01)


@pytest.fixture
def small_panels():
    rng = np.random.default_rng(7)
    signal = panel(rng.normal(size=(200, 12)))
    returns = panel(0.001 * signal.to_numpy() + rng.normal(0, 0.02, size=(200, 12)))
    return signal, returns


def test_run_gauntlet_reports_dsr_in_consistent_units(monkeypatch, small_panels):
    monkeypatch.setattr(gauntlet, "long_short_returns", fast_long_short)
    signal, returns = small_panels
    trial_sharpes = np.random.default_rng(3).normal(0, math.sqrt(1 / 200), 96)
    result = run_gauntlet(
        signal, returns, n_models_tested=96, top_n=3, n_permutations=20,
        n_subsample_splits=4, trial_sharpes=trial_sharpes,
    )
    dsr = result.details["deflated_sharpe"]
    # the threshold is annualized like observed_sharpe (old code: ~0.14, 16x too low)
    assert result.deflated_threshold == pytest.approx(dsr["sr0_per_period"] * math.sqrt(252))
    assert result.deflated_threshold > 1.0
    assert result.deflated_sharpe == dsr["dsr"]
    assert result.deflated_sharpe_passed == (dsr["dsr"] > 0.95)
    assert result.min_attainable_p == pytest.approx(1 / 21)


def test_run_gauntlet_dsr_unavailable_without_trial_sharpes(monkeypatch, small_panels):
    monkeypatch.setattr(gauntlet, "long_short_returns", fast_long_short)
    signal, returns = small_panels
    result = run_gauntlet(
        signal, returns, n_models_tested=4, top_n=3, n_permutations=20, n_subsample_splits=4
    )
    assert result.deflated_sharpe is None
    assert result.deflated_sharpe_passed is False
    assert math.isnan(result.deflated_threshold)
    assert "cross-trial" in result.details["deflated_sharpe_unavailable"]
    with pytest.raises(ValueError, match="one per-period Sharpe"):
        run_gauntlet(signal, returns, n_models_tested=4, trial_sharpes=[0.1, 0.2])


# --- 2. block permutation null ----------------------------------------------------------


def test_null_keeps_contiguous_blocks_of_signal_dates(monkeypatch, small_panels):
    """The old per-date shuffle broke every block (it is not a block permutation)."""
    signal, returns = small_panels
    marker = panel(np.repeat(np.arange(len(signal))[:, None], 12, axis=1).astype(float))
    seen = []

    def recording(sig, fwd, top_n=5, cost_bps=10.0):
        seen.append(sig.iloc[:, 0].to_numpy().astype(int))
        return fast_long_short(sig, fwd, top_n, cost_bps)

    monkeypatch.setattr(gauntlet, "long_short_returns", recording)
    out = permutation_test(marker, returns, n_shuffles=15, top_n=3, block=10)
    assert out.block == 10
    for order in seen[1:]:  # seen[0] is the real alignment
        assert sorted(order) == list(range(len(signal)))
        blocks = order.reshape(-1, 10)
        assert (np.diff(blocks, axis=1) == 1).all()
        assert (blocks[:, 0] % 10 == 0).all()
    assert any((order != np.arange(len(order))).any() for order in seen[1:])


def test_block_permutation_holds_size_on_a_persistent_null(monkeypatch):
    """Independent but persistent signal and returns (AR(1), phi = 0.9), T = 250.

    At nominal 5% the old per-date shuffle rejected 13/60 = 21.7% of these
    worlds (30% at T = 160); the block null rejects 2/60.
    """
    monkeypatch.setattr(gauntlet, "long_short_returns", fast_long_short)
    rng = np.random.default_rng(5)
    rejections = 0
    worlds = 60
    for w in range(worlds):
        signal = panel(ar1(rng, 250, 8, 0.9))
        returns = panel(0.01 * ar1(rng, 250, 8, 0.9))
        out = permutation_test(signal, returns, n_shuffles=99, top_n=2, cost_bps=0.0, seed=w)
        assert out.block > 1  # autocorrelated long-short returns lengthen the block
        rejections += out.p_value <= 0.05
    assert rejections / worlds <= 0.10


def test_block_never_shorter_than_overlap_depth_plus_one():
    iid = np.tile([1.0, -1.0, -1.0, 1.0], 75)  # lag-1 autocorrelation ~0
    assert permutation_block_length(iid, 0, 0)[0] == 1
    assert permutation_block_length(iid, 0, 4)[0] == 5
    assert permutation_block_length(iid, 7, 4)[0] == 7
    with pytest.raises(ValueError, match="overlap depth"):
        permutation_block_length(iid, 3, 4)


# --- 3. p-value resolution ---------------------------------------------------------------


def test_p_value_is_never_zero_and_reports_min_attainable_p(monkeypatch):
    """A perfect-foresight signal beats every null draw: the old p was 0.0."""
    monkeypatch.setattr(gauntlet, "long_short_returns", fast_long_short)
    rng = np.random.default_rng(2)
    returns = panel(rng.normal(0, 0.01, size=(200, 10)))
    signal = returns.shift(-1).fillna(0.0)
    out = permutation_test(signal, returns, n_shuffles=19, top_n=2, cost_bps=0.0)
    assert out.min_attainable_p == pytest.approx(0.05)
    assert out.p_value == pytest.approx(0.05)
    assert out.n_permutations == 19


def test_default_count_follows_alpha_and_cap_errors_clearly(monkeypatch, small_panels):
    monkeypatch.setattr(gauntlet, "long_short_returns", fast_long_short)
    signal, returns = small_panels
    out = permutation_test(signal, returns, top_n=3, cost_bps=0.0)
    assert out.n_permutations == 999  # 0.05 / 10 needs 199; floor 999
    assert out.min_attainable_p <= 0.05 / 10
    with pytest.raises(PermutationBudgetError, match="above the cap"):
        permutation_test(signal, returns, top_n=3, alpha=1e-4)
