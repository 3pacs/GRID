"""
Unit tests for normalization/resolver.py conflict detection logic.

Tests the conflict threshold calculations in isolation by extracting
the core logic and testing it directly, without requiring a database.
"""

from __future__ import annotations

import pytest

from normalization.resolver import (
    CONFLICT_THRESHOLD,
    FAMILY_CONFLICT_THRESHOLDS,
)


# ---------------------------------------------------------------------------
# Helper: replicate the resolver's conflict-detection arithmetic
# ---------------------------------------------------------------------------

def _compute_pct_diff(ref_val: float, other_val: float) -> float:
    """Replicate the Resolver's pct_diff calculation."""
    if ref_val != 0 and not (ref_val != ref_val):  # NaN check
        return abs(other_val - ref_val) / abs(ref_val)
    elif other_val != 0:
        return float("inf")
    else:
        return 0.0


def _is_conflict(ref_val: float, other_val: float, threshold: float) -> bool:
    """Return True if two values conflict per the given threshold."""
    return _compute_pct_diff(ref_val, other_val) > threshold


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestConflictDetectionWithinThreshold:
    """Values within the threshold should NOT be flagged as conflicts."""

    def test_identical_values_no_conflict(self):
        assert not _is_conflict(100.0, 100.0, CONFLICT_THRESHOLD)

    def test_within_default_threshold(self):
        # Default threshold is 0.005 (0.5%).  A 0.3% difference is below.
        ref = 100.0
        other = 100.3  # 0.3% above
        assert not _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_exactly_at_threshold_boundary(self):
        # Exactly at threshold is NOT a conflict (> vs >=).
        ref = 100.0
        other = ref * (1 + CONFLICT_THRESHOLD)  # exactly 0.5%
        assert not _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_negative_values_within_threshold(self):
        ref = -200.0
        other = -200.5  # 0.25%
        assert not _is_conflict(ref, other, CONFLICT_THRESHOLD)


class TestConflictDetectionExceedingThreshold:
    """Values exceeding the threshold SHOULD be flagged as conflicts."""

    def test_exceeds_default_threshold(self):
        ref = 100.0
        other = 101.0  # 1% above -- exceeds 0.5%
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_large_deviation_is_conflict(self):
        ref = 50.0
        other = 60.0  # 20%
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_negative_values_exceeding_threshold(self):
        ref = -100.0
        other = -102.0  # 2%
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_sign_mismatch_is_conflict(self):
        ref = 100.0
        other = -100.0  # 200%
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)


class TestFamilyThresholdOverrides:
    """Per-family thresholds should widen the acceptable range."""

    def test_vol_family_uses_2pct_threshold(self):
        threshold = FAMILY_CONFLICT_THRESHOLDS["vol"]
        assert threshold == 0.02
        # 1.5% diff should pass vol threshold but fail default
        ref = 100.0
        other = 101.5
        assert not _is_conflict(ref, other, threshold)
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_commodity_family_uses_1_5pct_threshold(self):
        threshold = FAMILY_CONFLICT_THRESHOLDS["commodity"]
        assert threshold == 0.015
        # 1.2% diff -- within commodity, outside default
        ref = 100.0
        other = 101.2
        assert not _is_conflict(ref, other, threshold)
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_crypto_family_uses_3pct_threshold(self):
        threshold = FAMILY_CONFLICT_THRESHOLDS["crypto"]
        assert threshold == 0.03
        # 2.5% diff -- within crypto, outside default and vol
        ref = 100.0
        other = 102.5
        assert not _is_conflict(ref, other, threshold)
        assert _is_conflict(ref, other, CONFLICT_THRESHOLD)

    def test_crypto_exceeds_family_threshold(self):
        threshold = FAMILY_CONFLICT_THRESHOLDS["crypto"]
        ref = 100.0
        other = 104.0  # 4% -- exceeds even crypto's 3%
        assert _is_conflict(ref, other, threshold)

    def test_unknown_family_falls_back_to_default(self):
        # Families not in FAMILY_CONFLICT_THRESHOLDS should use CONFLICT_THRESHOLD.
        threshold = FAMILY_CONFLICT_THRESHOLDS.get("unknown_family", CONFLICT_THRESHOLD)
        assert threshold == CONFLICT_THRESHOLD


class TestDivisionByZeroHandling:
    """When ref_val is 0, the resolver should not crash."""

    def test_ref_zero_other_nonzero_is_infinite_diff(self):
        pct = _compute_pct_diff(0.0, 5.0)
        assert pct == float("inf")

    def test_ref_zero_other_zero_is_no_conflict(self):
        pct = _compute_pct_diff(0.0, 0.0)
        assert pct == 0.0
        assert not _is_conflict(0.0, 0.0, CONFLICT_THRESHOLD)

    def test_ref_zero_other_nonzero_is_conflict(self):
        # inf > any threshold
        assert _is_conflict(0.0, 1.0, CONFLICT_THRESHOLD)

    def test_ref_nan_other_nonzero(self):
        # NaN != NaN evaluates True, so the code falls to the elif branch.
        pct = _compute_pct_diff(float("nan"), 5.0)
        assert pct == float("inf")

    def test_ref_nan_other_zero(self):
        pct = _compute_pct_diff(float("nan"), 0.0)
        assert pct == 0.0
