"""Pure-Python tests for godview/sec_ftd_pillar.py — no database, no network."""

from __future__ import annotations

from datetime import date

import pytest

from godview.sec_ftd_pillar import (
    AGE_NOTE,
    NOT_A_TIMELINE_NOTE,
    RELEASE_RULE_ID,
    compute_age_days,
    compute_release_date,
    compute_total_failed_usd,
    resolve_display_symbol,
)


def test_not_a_timeline_note_forbids_buyin_and_squeeze():
    assert "T+35" in NOT_A_TIMELINE_NOTE
    assert "squeeze" in NOT_A_TIMELINE_NOTE
    assert "never summed across dates" in NOT_A_TIMELINE_NOTE


def test_age_note_distinguishes_observation_age_from_fail_age():
    assert "NOT the age of the underlying fails" in AGE_NOTE


@pytest.mark.parametrize(
    "settlement_date,expected_release",
    [
        (date(2026, 8, 1), date(2026, 8, 31)),   # first half -> month-end
        (date(2026, 8, 15), date(2026, 8, 31)),  # boundary day, still first half
        (date(2026, 8, 16), date(2026, 9, 15)),  # second half -> 15th of next month
        (date(2026, 8, 31), date(2026, 9, 15)),  # last day of month, second half
        (date(2026, 12, 20), date(2027, 1, 15)), # December wraps to January
    ],
)
def test_compute_release_date_half_month_rule(settlement_date, expected_release):
    release_date, source_ref = compute_release_date(settlement_date)
    assert release_date == expected_release
    assert RELEASE_RULE_ID in source_ref


def test_compute_total_failed_usd_is_shares_times_price():
    assert compute_total_failed_usd(1000.0, 16.99) == pytest.approx(16990.0)


def test_compute_total_failed_usd_none_without_a_price():
    assert compute_total_failed_usd(1000.0, None) is None


def test_resolve_display_symbol_prefers_the_ftd_files_own_symbol():
    symbol, source = resolve_display_symbol("Y4000A102", "HQ")
    assert symbol == "HQ"
    assert source == "ftd_file_symbol"


def test_resolve_display_symbol_falls_back_to_cusip_and_says_so():
    symbol, source = resolve_display_symbol("Y4000A102", None)
    assert symbol == "Y4000A102"
    assert source == "cusip_fallback"

    symbol2, source2 = resolve_display_symbol("Y4000A102", "   ")
    assert symbol2 == "Y4000A102"
    assert source2 == "cusip_fallback"


def test_compute_age_days_is_calendar_days_since_settlement():
    assert compute_age_days(date(2026, 8, 17), date(2026, 9, 18)) == 32
    assert compute_age_days(date(2026, 9, 18), date(2026, 9, 18)) == 0
