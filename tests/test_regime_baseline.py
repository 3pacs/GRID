"""intelligence.regime.baseline: the trailing-robust/1 standardization every market-state variable uses."""

from __future__ import annotations

import math

import pytest

from intelligence.regime import baseline as B


def test_value_is_never_in_its_own_baseline():
    hist = [1.0, 2.0, 3.0, 4.0, 5.0]
    r = B.standardize(100.0, hist, window=10, min_obs=3)
    assert r.median == 3.0 and r.n == 5
    last = B.standardize_last(hist + [100.0], window=10, min_obs=3)
    assert last.median == 3.0 and last.n == 5 and last.value == 100.0


def test_robust_z_uses_median_and_scaled_mad_and_caps():
    hist = [10.0, 11.0, 9.0, 10.0, 12.0, 8.0, 10.0]
    r = B.standardize(11.0, hist, window=0, min_obs=3)
    med, mad = 10.0, 1.0
    assert r.robust_z == pytest.approx((11.0 - med) / (B.MAD_SCALE * mad))
    assert r.scale == "mad"
    assert B.standardize(1e9, hist, window=0, min_obs=3).robust_z == B.Z_CAP
    assert B.standardize(-1e9, hist, window=0, min_obs=3).robust_z == -B.Z_CAP


def test_window_keeps_only_the_most_recent_observations():
    hist = [100.0] * 50 + [1.0, 2.0, 3.0, 4.0, 5.0]
    assert B.standardize(3.0, hist, window=5, min_obs=3).median == 3.0


def test_zero_mad_falls_back_to_iqr_then_to_percentile_only():
    step = [0.25] * 8 + [0.5, 0.5, 0.75, 0.75]       # MAD 0, IQR > 0
    r = B.standardize(1.0, step, window=0, min_obs=5)
    assert r.scale == "iqr" and r.robust_z is not None and r.robust_z > 0
    flat = [2.0] * 20
    r2 = B.standardize(3.0, flat, window=0, min_obs=5)
    assert r2.robust_z is None and r2.scale == "none" and r2.percentile == 1.0 and r2.label == "high"


def test_short_history_is_none_not_a_guess():
    assert B.standardize(1.0, [1.0, 2.0], window=10, min_obs=3) is None
    assert B.standardize_last([], window=10, min_obs=1) is None
    assert B.standardize(float("nan"), [1.0] * 10, window=10, min_obs=3) is None


def test_non_finite_history_is_dropped():
    r = B.standardize(2.0, [1.0, float("nan"), 2.0, math.inf, 3.0], window=0, min_obs=3)
    assert r.n == 3


def test_percentile_is_mid_rank_with_ties():
    r = B.standardize(2.0, [1.0, 2.0, 3.0, 4.0], window=0, min_obs=2)
    assert r.percentile == pytest.approx((1 + 0.5) / 4)


@pytest.mark.parametrize("z,label", [(0.0, "normal"), (0.99, "normal"), (1.0, "above"), (-1.5, "below"),
                                     (2.0, "high"), (-3.0, "low")])
def test_labels_from_z(z, label):
    assert B._label(z, 0.5) == label


@pytest.mark.parametrize("pct,label", [(0.95, "high"), (0.8, "above"), (0.5, "normal"), (0.2, "below"), (0.05, "low")])
def test_labels_from_percentile_when_no_z(pct, label):
    assert B._label(None, pct) == label


def test_window_classes():
    assert B.window_for("daily_fast") == (252, 120)
    assert B.window_for("monthly") == (60, 36)
    with pytest.raises(KeyError):
        B.window_for("hourly")
