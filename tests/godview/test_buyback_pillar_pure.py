"""Pure-Python tests for godview/buyback_pillar.py — no database, no network."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from godview.buyback_pillar import (
    MISSING_INPUT,
    MODELING_ASSUMPTION_NOTE,
    WINDOW_AFTER_DAYS,
    WINDOW_BEFORE_DAYS,
    compute_window,
    window_dates,
)


def test_modeling_assumption_note_is_explicit_about_no_issuer_mandate():
    assert "not adopting a cooling-off period for issuers" in MODELING_ASSUMPTION_NOTE
    assert "not an SEC-mandated period" in MODELING_ASSUMPTION_NOTE


def test_missing_input_names_edgar_repurchase_data_and_what_it_blocks():
    assert "10-Q/10-K" in MISSING_INPUT
    assert "EDGAR" in MISSING_INPUT
    assert "no dollar or share buyback figure" in MISSING_INPUT


def test_compute_window_brackets_the_earnings_date():
    earnings_date = date(2026, 10, 20)
    window_start, window_end = compute_window(earnings_date)
    assert window_start == earnings_date - timedelta(days=WINDOW_BEFORE_DAYS)
    assert window_end == earnings_date + timedelta(days=WINDOW_AFTER_DAYS)
    assert window_start < earnings_date < window_end


def test_window_dates_is_inclusive_of_both_endpoints():
    days = window_dates(date(2026, 1, 1), date(2026, 1, 3))
    assert days == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]


def test_window_dates_single_day_window():
    assert window_dates(date(2026, 1, 1), date(2026, 1, 1)) == [date(2026, 1, 1)]
