"""
Unit tests for normalization/resolver.py conflict detection logic.

Tests the conflict threshold calculations directly without hitting a real
database. We extract and test the core arithmetic used by Resolver.resolve_pending.
"""

from __future__ import annotations

import pytest

from normalization.resolver import CONFLICT_THRESHOLD, FAMILY_CONFLICT_THRESHOLDS


# ---------------------------------------------------------------------------
# Helper: replicate the conflict-detection arithmetic from Resolver
# ---------------------------------------------------------------------------

def _is_conflict(ref_val: float, other_val: float, family: str | None = None) -> bool:
    """Return True if ref_val and other_val are in conflict.

    Mirrors the logic in Resolver.resolve_pending lines 156-163.
    """
    threshold = FAMILY_CONFLICT_THRESHOLDS.get(family, CONFLICT_THRESHOLD) if family else CONFLICT_THRESHOLD

    if ref_val != 0 and not (ref_val != ref_val):  # NaN check
        pct_diff = abs(other_val - ref_val) / abs(ref_val)
    elif other_val != 0:
        pct_diff = float("inf")
    else:
        pct_diff = 0.0

    return pct_diff > threshold


class TestConflictWithinThreshold:
    """Values that differ by less than the threshold should NOT be flagged."""

    def test_identical_values_no_conflict(self):
        assert _is_conflict(100.0, 100.0) is False

    def test_tiny_difference_no_conflict(self):
        # 0.1% difference, well below default 0.5% threshold
        assert _is_conflict(100.0, 100.1) is False

    def test_exactly_at_threshold_no_conflict(self):
        # Default threshold is 0.005 (0.5%).  A difference of exactly 0.5 on
        # a ref_val of 100.0 gives pct_diff == 0.005, which is NOT > threshold.
        assert _is_conflict(100.0, 100.5) is False

    def test_negative_values_within_threshold(self):
        # -100.0 vs -100.1 => pct_diff = 0.1/100 = 0.001
        assert _is_conflict(-100.0, -100.1) is False


class TestConflictExceedingThreshold:
    """Values that differ by more than the threshold should be flagged."""

    def test_one_percent_difference_is_conflict(self):
        # 1% difference > 0.5% default threshold
        assert _is_conflict(100.0, 101.0) is True

    def test_large_difference_is_conflict(self):
        assert _is_conflict(100.0, 110.0) is True

    def test_small_ref_val_magnifies_pct_diff(self):
        # ref=1.0, other=1.01 => pct_diff = 0.01 (1%) > 0.005
        assert _is_conflict(1.0, 1.01) is True

    def test_negative_ref_large_diff(self):
        # ref=-100, other=-102 => pct_diff = 2/100 = 0.02
        assert _is_conflict(-100.0, -102.0) is True


class TestFamilyThresholdOverrides:
    """Per-family thresholds should override the default."""

    def test_vol_family_has_higher_threshold(self):
        # vol threshold is 0.02 (2%).  A 1.5% diff should NOT conflict.
        assert _is_conflict(100.0, 101.5, family="vol") is False

    def test_vol_family_exceeds_threshold(self):
        # 3% diff > 2% vol threshold
        assert _is_conflict(100.0, 103.0, family="vol") is True

    def test_commodity_family_within_threshold(self):
        # commodity threshold is 0.015.  1% diff should not conflict.
        assert _is_conflict(100.0, 101.0, family="commodity") is False

    def test_commodity_family_exceeds_threshold(self):
        # 2% diff > 1.5% commodity threshold
        assert _is_conflict(100.0, 102.0, family="commodity") is True

    def test_crypto_family_within_threshold(self):
        # crypto threshold is 0.03.  2.5% diff should not conflict.
        assert _is_conflict(100.0, 102.5, family="crypto") is False

    def test_crypto_family_exceeds_threshold(self):
        # 4% diff > 3% crypto threshold
        assert _is_conflict(100.0, 104.0, family="crypto") is True

    def test_unknown_family_uses_default(self):
        # An unknown family should fall back to the default threshold
        assert _is_conflict(100.0, 101.0, family="equities") is True


class TestDivisionByZeroHandling:
    """When ref_val is 0 the resolver must not raise ZeroDivisionError."""

    def test_ref_zero_other_nonzero_is_conflict(self):
        # ref=0, other=1 => pct_diff = inf => always a conflict
        assert _is_conflict(0.0, 1.0) is True

    def test_ref_zero_other_zero_no_conflict(self):
        # Both are zero => pct_diff = 0 => no conflict
        assert _is_conflict(0.0, 0.0) is False

    def test_ref_zero_other_tiny_is_conflict(self):
        # Any nonzero value vs zero reference is inf difference
        assert _is_conflict(0.0, 0.0001) is True

    def test_ref_zero_other_negative_is_conflict(self):
        assert _is_conflict(0.0, -0.01) is True

    def test_nan_ref_val_no_crash(self):
        # NaN != NaN, so (ref_val != ref_val) is True => branch to other_val check
        nan = float("nan")
        # other is nonzero, so pct_diff = inf
        assert _is_conflict(nan, 1.0) is True

    def test_nan_ref_and_other_zero(self):
        nan = float("nan")
        # ref is NaN, other is 0 => pct_diff = 0.0
        assert _is_conflict(nan, 0.0) is False
