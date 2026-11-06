"""S10 — split-conformal and adaptive conformal primitives (oracle/uncertainty.py).

Pure functions: finite-sample quantile rule, the interval/label-set identity
behind the abstain signal, ACI update direction, and empirical coverage on
synthetic data (exchangeable and shifted).
"""
from __future__ import annotations

import bisect
import math

import numpy as np
import pytest

from oracle.uncertainty import (
    ACI_GAMMA,
    CONFORMAL_ALPHA,
    AdaptiveConformal,
    binary_residual_score,
    compute_confidence_interval,
    conformal_interval_from_quantile,
    conformal_quantile,
    split_conformal_interval,
)


class TestConformalQuantile:
    def test_rank_is_ceil_n_plus_one(self):
        scores = [i / 10 for i in range(10)]  # 0.0 .. 0.9
        # n=10, alpha=0.2 -> ceil(11*0.8)=9 -> 9th smallest = 0.8
        assert conformal_quantile(scores, 0.2) == pytest.approx(0.8)

    def test_too_few_scores_is_infinite(self):
        # n=5, alpha=0.1 -> ceil(6*0.9)=6 > 5
        assert math.isinf(conformal_quantile([0.1] * 5, 0.1))

    def test_empty_and_nonpositive_alpha_infinite(self):
        assert math.isinf(conformal_quantile([], 0.1))
        assert math.isinf(conformal_quantile([0.2, 0.3], -0.4))

    def test_alpha_at_least_one_collapses(self):
        assert conformal_quantile([0.2, 0.3], 1.0) == 0.0
        assert conformal_quantile([0.2, 0.3], 1.7) == 0.0


class TestIntervalAndAbstain:
    def test_infinite_q_is_full_range_and_abstains(self):
        iv = conformal_interval_from_quantile(0.8, math.inf, alpha=0.1, n_cal=3)
        assert (iv.lower, iv.upper) == (0.0, 1.0)
        assert iv.abstain and iv.stance == "abstain"
        assert iv.to_dict()["q"] is None

    def test_take_and_fade(self):
        take = conformal_interval_from_quantile(0.9, 0.2, alpha=0.1, n_cal=100)
        fade = conformal_interval_from_quantile(0.1, 0.2, alpha=0.1, n_cal=100)
        assert take.stance == "take" and not take.abstain
        assert fade.stance == "fade" and not fade.abstain

    def test_interval_equals_label_set(self):
        """[p-q, p+q] covers y in {0,1} exactly when |y - p| <= q."""
        rng = np.random.default_rng(0)
        for _ in range(500):
            p, q = rng.uniform(), rng.uniform(0, 1.2)
            iv = conformal_interval_from_quantile(p, q, alpha=0.1, n_cal=10)
            for y in (0, 1):
                assert iv.covers(y) == (binary_residual_score(p, y) <= q + 1e-12)

    def test_never_official(self):
        iv = split_conformal_interval(0.6, [0.1, 0.2, 0.3], alpha=0.5)
        assert iv.official is False and iv.to_dict()["official"] is False

    def test_legacy_disagreement_interval_unchanged(self):
        """The live Kelly path keeps its ALPHA-11 interval byte-for-byte."""
        votes = [{"confidence": 0.7}] * 4 + [{"confidence": 0.3}] * 2
        ci = compute_confidence_interval(votes, 0.57, alpha=0.10)
        assert ci.n == 6 and ci.lower < 0.57 < ci.upper


class TestAdaptiveConformal:
    def test_pre_declared_constants(self):
        assert ACI_GAMMA == 0.005
        assert CONFORMAL_ALPHA == 0.10

    def test_update_direction(self):
        aci = AdaptiveConformal(alpha=0.1, gamma=0.01)
        aci.update(covered=False)
        assert aci.alpha_t == pytest.approx(0.1 + 0.01 * (0.1 - 1.0))
        a = aci.alpha_t
        aci.update(covered=True)
        assert aci.alpha_t == pytest.approx(a + 0.01 * 0.1)
        assert aci.n_updates == 2 and aci.n_errors == 1

    def test_alpha_t_not_clamped(self):
        aci = AdaptiveConformal(alpha=0.1, gamma=0.5)
        for _ in range(5):
            aci.update(covered=False)
        assert aci.alpha_t < 0
        assert math.isinf(aci.interval(0.5, [0.1, 0.2]).q)

    def test_bound(self):
        aci = AdaptiveConformal(alpha=0.1, gamma=0.005)
        assert aci.miscoverage_bound() is None
        for _ in range(1000):
            aci.update(covered=True)
        assert aci.miscoverage_bound() == pytest.approx((0.9 + 0.005) / (0.005 * 1000))

    @pytest.mark.parametrize("alpha,gamma", [(0.0, 0.01), (1.0, 0.01), (0.1, 0.0)])
    def test_rejects_bad_params(self, alpha, gamma):
        with pytest.raises(ValueError):
            AdaptiveConformal(alpha=alpha, gamma=gamma)


def _draw(rng, n, skill):
    p = rng.uniform(0.05, 0.95, size=n)
    truth = np.clip(0.5 + skill * (p - 0.5), 0, 1)
    y = (rng.uniform(size=n) < truth).astype(int)
    return p, y


def test_split_coverage_near_nominal_on_exchangeable_data():
    rng = np.random.default_rng(42)
    p_cal, y_cal = _draw(rng, 4000, 1.0)
    scores = sorted(binary_residual_score(p, y) for p, y in zip(p_cal, y_cal))
    p_te, y_te = _draw(rng, 4000, 1.0)
    cov = np.mean([split_conformal_interval(p, scores, alpha=0.2).covers(y) for p, y in zip(p_te, y_te)])
    assert 0.78 <= cov <= 0.84


def test_aci_recovers_coverage_under_shift_where_split_fails():
    """Calibrated scores from a skilled period, then the skill disappears."""
    rng = np.random.default_rng(7)
    p_cal, y_cal = _draw(rng, 3000, 1.0)
    cal_scores = sorted(binary_residual_score(p, y) for p, y in zip(p_cal, y_cal))
    # Shift: outcomes become anti-correlated with p.
    p_te, y_te = _draw(rng, 6000, -1.0)
    split_cov = np.mean([split_conformal_interval(p, cal_scores, alpha=0.2).covers(y)
                         for p, y in zip(p_te, y_te)])
    aci = AdaptiveConformal(alpha=0.2, gamma=0.005)
    online = list(cal_scores)
    covered = []
    for p, y in zip(p_te, y_te):
        iv = aci.interval(p, online)
        covered.append(iv.covers(y))
        aci.update(iv.covers(y))
        bisect.insort(online, binary_residual_score(p, y))
    aci_cov = float(np.mean(covered))
    assert split_cov < 0.75
    assert abs(aci_cov - 0.8) < abs(split_cov - 0.8)
    assert abs(np.mean(covered[-3000:]) - 0.8) < 0.03
