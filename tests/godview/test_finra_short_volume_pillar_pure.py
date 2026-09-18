"""Pure-Python tests for godview/finra_short_volume_pillar.py — no database, no network."""

from __future__ import annotations

from datetime import date

import pytest

from godview.finra_short_volume_pillar import (
    NOT_SHORT_INTEREST_NOTE,
    RELEASE_RULE_ID,
    SPIKE_RATIO_THRESHOLD,
    classify_spike,
    compute_moving_average,
    compute_release_date,
    compute_short_ratio,
)


def test_not_short_interest_note_is_explicit():
    assert "NOT short INTEREST" in NOT_SHORT_INTEREST_NOTE
    assert "squeeze" in NOT_SHORT_INTEREST_NOTE


def test_compute_release_date_is_same_day_always():
    trade_date = date(2026, 9, 16)  # any day, including non-Tuesday/Wednesday
    release_date, source_ref = compute_release_date(trade_date)
    assert release_date == trade_date
    assert RELEASE_RULE_ID in source_ref


def test_compute_short_ratio_is_the_simple_division():
    assert compute_short_ratio(600.0, 1000.0) == pytest.approx(0.6)


def test_compute_short_ratio_none_for_non_positive_total():
    assert compute_short_ratio(100.0, 0.0) is None
    assert compute_short_ratio(100.0, -5.0) is None


def test_compute_moving_average_none_when_empty():
    assert compute_moving_average([]) is None


def test_compute_moving_average_is_the_trailing_mean():
    assert compute_moving_average([0.1, 0.2, 0.3]) == pytest.approx(0.2)
    assert compute_moving_average([0.1, 0.2, 0.3], window=2) == pytest.approx(0.25)


@pytest.mark.parametrize(
    "ratio,expected",
    [
        (None, False),
        (0.1, False),
        (SPIKE_RATIO_THRESHOLD, True),
        (0.99, True),
    ],
)
def test_classify_spike(ratio, expected):
    assert classify_spike(ratio, None) is expected
