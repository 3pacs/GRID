"""ALPHA-12 — Kelly-with-error-bars + tail adjustment tests.

Pure-function tests on trading/options_recommender.py::compute_kelly_with_bounds.
Covers: lower-bound sizing, tail shrinkage near break-even, width penalty,
edge cases (NaN/inf, inverted bounds, below break-even skip).
"""
from __future__ import annotations

import math

import pytest

from trading.options_recommender import (
    DEFAULT_PAYOUT_RATIO,
    MAX_KELLY_PER_TICKET,
    compute_kelly_fraction,
    compute_kelly_with_bounds,
)


class TestComputeKellyWithBounds:
    def test_below_break_even_returns_zero(self):
        # With default payout=3, break-even = 0.25. Lower bound at 0.20 → skip.
        assert compute_kelly_with_bounds(0.20, 0.60) == 0.0

    def test_at_break_even_returns_zero(self):
        be = 1.0 / (1.0 + DEFAULT_PAYOUT_RATIO)
        assert compute_kelly_with_bounds(be, be + 0.2) == 0.0

    def test_strong_lower_bound_returns_positive_kelly(self):
        # Lower bound well above break-even → positive Kelly
        k = compute_kelly_with_bounds(0.50, 0.70)
        assert k > 0

    def test_lower_bound_drives_sizing(self):
        # Same upper, different lower → different Kelly
        k_conservative = compute_kelly_with_bounds(0.35, 0.80)
        k_confident = compute_kelly_with_bounds(0.60, 0.80)
        assert k_confident > k_conservative

    def test_width_penalty_shrinks_wide_intervals(self):
        # Same lower, different width → wider = smaller Kelly
        k_narrow = compute_kelly_with_bounds(0.60, 0.65)
        k_wide = compute_kelly_with_bounds(0.60, 0.95)
        assert k_narrow > k_wide

    def test_kelly_capped(self):
        k = compute_kelly_with_bounds(0.99, 1.00)
        assert k <= MAX_KELLY_PER_TICKET

    def test_tail_shrinks_near_break_even(self):
        # Break-even ~0.25; lower bound just above → heavy shrink
        k_tail = compute_kelly_with_bounds(0.26, 0.40)
        k_far = compute_kelly_with_bounds(0.40, 0.50)
        assert k_tail < k_far

    def test_inverted_bounds_are_swapped(self):
        k1 = compute_kelly_with_bounds(0.40, 0.60)
        k2 = compute_kelly_with_bounds(0.60, 0.40)
        assert k1 == k2

    def test_out_of_range_clamped(self):
        k = compute_kelly_with_bounds(-0.1, 1.5)
        # -0.1 clamps to 0 which is below BE → 0
        assert k == 0.0

    def test_nan_returns_zero(self):
        assert compute_kelly_with_bounds(float("nan"), 0.6) == 0.0
        assert compute_kelly_with_bounds(0.4, float("nan")) == 0.0

    def test_inf_payout_returns_zero(self):
        assert compute_kelly_with_bounds(0.5, 0.7, payout_ratio=float("inf")) == 0.0

    def test_negative_payout_returns_zero(self):
        assert compute_kelly_with_bounds(0.5, 0.7, payout_ratio=-1.0) == 0.0

    def test_tight_interval_near_max_kelly(self):
        # Tight CI well above BE → approaches the hard cap (0.05).
        # Width penalty (~0.98x) means we land just under the cap.
        k = compute_kelly_with_bounds(0.80, 0.82)
        assert k > 0.04
        assert k <= MAX_KELLY_PER_TICKET

    def test_scalar_kelly_unchanged(self):
        # Legacy scalar path still works independently
        k = compute_kelly_fraction(0.5)
        assert k > 0


class TestKellyWithBoundsVsScalar:
    def test_bounds_are_more_conservative(self):
        """Given the same point accuracy, bounds-based Kelly ≤ scalar Kelly."""
        point = 0.60
        scalar = compute_kelly_fraction(point)
        # Width 0.10 with lower=0.55
        bounds = compute_kelly_with_bounds(0.55, 0.65)
        assert bounds <= scalar

    def test_point_equivalent_when_bounds_collapse(self):
        """When upper == lower, bounds Kelly still applies tail shrink but
        should be within reasonable range of the scalar."""
        k_bounds = compute_kelly_with_bounds(0.60, 0.60)
        k_scalar = compute_kelly_fraction(0.60)
        # bounds will be smaller due to tail factor, but both positive
        assert k_bounds > 0
        assert k_scalar > 0
        assert k_bounds <= k_scalar
