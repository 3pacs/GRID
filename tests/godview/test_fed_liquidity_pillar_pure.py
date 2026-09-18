"""Pure-Python tests for godview/fed_liquidity_pillar.py — no database, no network."""

from __future__ import annotations

from datetime import date

import pytest

from godview.fed_liquidity_pillar import (
    RELEASE_RULE_ID,
    RRP_BILLIONS_TO_MILLIONS,
    classify_liquidity_regime,
    compute_net_liquidity_millions,
    compute_release_date,
    compute_rrp_pct_of_peak,
    coverage_fraction_for_window,
    find_nearest_prior,
)


def test_compute_release_date_sets_thursday_for_a_wednesday_obs():
    wednesday = date(2026, 9, 16)
    assert wednesday.weekday() == 2
    release_date, source_ref = compute_release_date(wednesday)
    assert release_date == date(2026, 9, 17)  # the following Thursday
    assert release_date.weekday() == 3
    assert RELEASE_RULE_ID in source_ref
    assert "Wednesday" in source_ref


def test_compute_release_date_withholds_for_a_non_wednesday_obs():
    thursday = date(2026, 9, 17)
    release_date, source_ref = compute_release_date(thursday)
    assert release_date is None
    assert "not Wednesday" in source_ref
    assert "withheld" in source_ref


def test_compute_net_liquidity_converts_rrp_billions_to_millions():
    """WALCL/WTREGEN are millions; RRPONTSYD is billions — confirmed via FRED (module docstring)."""
    assert RRP_BILLIONS_TO_MILLIONS == 1000.0
    # 7,500,000M WALCL - 700,000M WTREGEN - 300B RRP (=300,000M) = 6,500,000M
    result = compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    assert result == pytest.approx(6_500_000.0)


def test_compute_net_liquidity_without_conversion_would_be_wrong():
    """Sanity check: skipping the *1000 scale materially changes the result
    (this is exactly the bug flagged in ingestion/altdata/fed_liquidity.py)."""
    converted = compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    unconverted = 7_500_000.0 - 700_000.0 - 300.0  # what the buggy formula would give
    assert abs(converted - unconverted) == pytest.approx(300.0 * (RRP_BILLIONS_TO_MILLIONS - 1))


def test_compute_rrp_pct_of_peak_is_100_at_a_new_high():
    history = [100.0, 200.0, 150.0, 300.0]  # last value is the peak
    pct = compute_rrp_pct_of_peak(history, window=156)
    assert pct == pytest.approx(100.0)


def test_compute_rrp_pct_of_peak_none_below_min_history():
    assert compute_rrp_pct_of_peak([100.0, 200.0], window=156) is None


def test_coverage_fraction_for_window_caps_at_one():
    assert coverage_fraction_for_window(10, window=156) == pytest.approx(10 / 156)
    assert coverage_fraction_for_window(500, window=156) == 1.0


def test_find_nearest_prior_within_tolerance():
    history = [(date(2026, 9, 2), 100.0), (date(2026, 9, 9), 110.0)]
    # target 5 days before 2026-09-16 (2026-09-11); 2026-09-09 is 7 days
    # before, 2 days off target — within tolerance (3).
    value = find_nearest_prior(history, date(2026, 9, 16), target_days=5, tolerance_days=3)
    assert value == 110.0


def test_find_nearest_prior_none_when_nothing_within_tolerance():
    history = [(date(2026, 1, 1), 100.0)]
    value = find_nearest_prior(history, date(2026, 9, 16), target_days=5, tolerance_days=3)
    assert value is None


@pytest.mark.parametrize(
    "delta_30d,expected",
    [
        (None, "insufficient_history"),
        (60_000.0, "expanding"),
        (-60_000.0, "contracting"),
        (0.0, "neutral"),
    ],
)
def test_classify_liquidity_regime(delta_30d, expected):
    assert classify_liquidity_regime(delta_30d) == expected
