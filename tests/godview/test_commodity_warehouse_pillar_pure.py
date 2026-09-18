"""Pure-Python tests for godview/commodity_warehouse_pillar.py — no database, no network."""

from __future__ import annotations

import pytest

from godview.commodity_warehouse_pillar import (
    CUSHING_SERIES_ID,
    CUSHING_UNAVAILABLE_REASON,
    PHYSICAL_TIGHTNESS_THRESHOLD,
    classify_physical_tightness,
    compute_net_change,
)


def test_cushing_series_id_is_deliberately_none():
    """Grepped 2026-09-18: no real Cushing series id exists anywhere in this codebase."""
    assert CUSHING_SERIES_ID is None
    assert "never_configured" in CUSHING_UNAVAILABLE_REASON
    assert "WCESTUS1" in CUSHING_UNAVAILABLE_REASON  # names the near-miss it must not use


@pytest.mark.parametrize(
    "ratio,expected",
    [
        (None, False),
        (0.0, False),
        (0.10, False),
        (PHYSICAL_TIGHTNESS_THRESHOLD, True),
        (0.99, True),
    ],
)
def test_classify_physical_tightness(ratio, expected):
    assert classify_physical_tightness(ratio) is expected


def test_compute_net_change_none_without_a_prior_observation():
    assert compute_net_change(1000.0, None) is None


def test_compute_net_change_is_the_simple_difference():
    assert compute_net_change(1050.0, 1000.0) == pytest.approx(50.0)
    assert compute_net_change(950.0, 1000.0) == pytest.approx(-50.0)
