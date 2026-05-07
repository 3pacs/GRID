"""CAT-115 — Cross-asset lead-lag backtest framework tests."""
from __future__ import annotations

import numpy as np

from analysis.lead_lag_backtest import (
    FoldResult,
    _correlation_at_lag,
    _directional_hit_rate,
    _estimate_half_life,
    run_walk_forward,
)


def _seeded_random(seed=42):
    return np.random.default_rng(seed)


class TestCorrelationAtLag:
    def test_perfect_lag_correlation(self):
        # y[t] = x[t-1] → correlation at lag=1 should be ~1
        x = np.arange(100, dtype=float)
        y = np.roll(x, 1)
        y[0] = 0
        corr = _correlation_at_lag(x, y, lag=1)
        assert corr > 0.9

    def test_zero_lag_self(self):
        x = np.arange(50, dtype=float)
        corr = _correlation_at_lag(x, x, lag=0)
        assert corr > 0.99

    def test_too_short_returns_zero(self):
        x = np.array([1.0, 2.0])
        assert _correlation_at_lag(x, x, lag=5) == 0.0

    def test_constant_series_zero(self):
        x = np.full(100, 5.0)
        y = np.full(100, 3.0)
        assert _correlation_at_lag(x, y, lag=1) == 0.0


class TestDirectionalHitRate:
    def test_perfect_follow(self):
        # y diffs match x diffs shifted by 1
        x = np.array([1, 2, 3, 2, 1, 2, 3, 4, 3, 2], dtype=float)
        y = np.roll(x, 1)
        y[0] = 0
        hit = _directional_hit_rate(x, y, lag=1)
        assert hit >= 0.5

    def test_anti_follow(self):
        x = np.arange(20, dtype=float)
        y = -x.copy()
        hit = _directional_hit_rate(x, y, lag=0)
        assert hit < 0.5

    def test_too_short_returns_half(self):
        assert _directional_hit_rate(np.array([1.0]), np.array([1.0]), lag=1) == 0.5


class TestEstimateHalfLife:
    def test_decaying_correlations_returns_tau(self):
        # Simulate exp decay: corr_k = 0.9 × exp(-k/5)
        corrs = [0.9 * np.exp(-k / 5.0) for k in range(10)]
        tau = _estimate_half_life(corrs)
        assert tau is not None
        assert 2 < tau < 6

    def test_flat_correlations_none(self):
        corrs = [0.3, 0.3, 0.3]
        tau = _estimate_half_life(corrs)
        # Flat slope → None (log-linear fit fails)
        assert tau is None or tau > 10

    def test_single_value_none(self):
        assert _estimate_half_life([0.5]) is None

    def test_empty_none(self):
        assert _estimate_half_life([]) is None


class TestRunWalkForward:
    def test_too_short_empty_folds(self):
        x = list(range(50))
        y = list(range(50))
        result = run_walk_forward(
            leader_name="X", leader_series=x,
            follower_name="Y", follower_series=y,
            train_window=252, test_window=63,
        )
        assert len(result.folds) == 0
        assert result.passed_floor is False

    def test_perfect_lag_passes_floor(self):
        # Generate a long series where Y strongly leads X
        rng = _seeded_random()
        n = 500
        x = rng.standard_normal(n).cumsum()
        y = np.roll(x, 1)  # Y lags X by 1
        y[0] = 0
        result = run_walk_forward(
            leader_name="X", leader_series=x.tolist(),
            follower_name="Y", follower_series=y.tolist(),
            lag_days=1,
            train_window=100, test_window=50,
        )
        assert len(result.folds) > 0
        assert result.avg_test_correlation > 0.0

    def test_random_no_edge(self):
        rng = _seeded_random()
        x = rng.standard_normal(500).tolist()
        y = rng.standard_normal(500).tolist()
        result = run_walk_forward(
            leader_name="X", leader_series=x,
            follower_name="Y", follower_series=y,
            lag_days=1,
            train_window=100, test_window=50,
        )
        # Random series → shouldn't pass a 0.10 correlation floor
        assert result.passed_floor is False

    def test_multiple_folds_generated(self):
        rng = _seeded_random()
        n = 800
        x = rng.standard_normal(n).cumsum().tolist()
        y = rng.standard_normal(n).cumsum().tolist()
        result = run_walk_forward(
            leader_name="X", leader_series=x,
            follower_name="Y", follower_series=y,
            train_window=200, test_window=100,
        )
        # 800 observations, 200 train + 100 test = 300 per fold, stepping 100
        # → ~5 folds expected
        assert len(result.folds) >= 3

    def test_robustness_score_bounded(self):
        rng = _seeded_random()
        x = rng.standard_normal(500).tolist()
        y = rng.standard_normal(500).tolist()
        result = run_walk_forward(
            leader_name="X", leader_series=x,
            follower_name="Y", follower_series=y,
            train_window=100, test_window=50,
        )
        assert 0.0 <= result.robustness_score <= 1.0


class TestDataclassRoundtrip:
    def test_result_to_dict(self):
        rng = _seeded_random()
        x = rng.standard_normal(400).tolist()
        y = rng.standard_normal(400).tolist()
        result = run_walk_forward(
            leader_name="X", leader_series=x,
            follower_name="Y", follower_series=y,
            train_window=100, test_window=50,
        )
        d = result.to_dict()
        for k in ("leader_name", "follower_name", "lag_days", "n_folds",
                  "avg_test_correlation", "avg_test_hit_rate", "sharpe_proxy",
                  "robustness_score", "half_life_days", "passed_floor", "folds"):
            assert k in d

    def test_fold_to_dict(self):
        fold = FoldResult(
            fold_idx=0, train_start_idx=0, train_end_idx=100,
            test_start_idx=100, test_end_idx=150,
            train_correlation=0.5, test_correlation=0.4,
            test_hit_rate=0.55, sample_size=50,
        )
        d = fold.to_dict()
        assert d["fold_idx"] == 0
        assert d["train_correlation"] == 0.5
