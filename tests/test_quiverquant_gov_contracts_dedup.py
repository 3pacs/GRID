"""Tests for GD-FIX: QuiverQuant gov_contracts dedup-on-ingest.

Before this fix, gov_contracts records (which carry a fiscal Year/Qtr pair,
not a per-event date) fell through to `signal_date = today`, so the same
underlying quarterly aggregate was re-inserted as a brand-new row every
daily pull (the plan's evidence: ~821 rows re-inserted daily, multiplying
the row count ~30x/month). _resolve_signal_date() now anchors gov_contracts
to a stable quarter-end date so repeat pulls of unchanged data hit the same
(source_type, source_id, ticker, signal_date, signal_type) key.

QuiverQuant's (Year, Qtr) is the US federal FISCAL quarter, so the anchor is the
fiscal quarter end (FY Y Q1 = Oct-Dec of Y-1 ... Q4 = Jul-Sep of Y); GD-FIX first
read it as a calendar quarter (see tests/test_quiverquant_act_keys.py for the
full four-quarter mapping and the re-date transition).
"""

from __future__ import annotations

from datetime import date

from ingestion.altdata.quiverquant import (
    _gov_contract_period_date,
    _resolve_signal_date,
)


def test_gov_contract_period_date_from_year_qtr():
    assert _gov_contract_period_date({"Year": 2026, "Qtr": 1}) == date(2025, 12, 31)
    assert _gov_contract_period_date({"Year": 2026, "Qtr": 4}) == date(2026, 9, 30)


def test_gov_contract_period_date_accepts_lowercase_and_string_keys():
    assert _gov_contract_period_date({"year": "2025", "qtr": "3"}) == date(2025, 6, 30)


def test_gov_contract_period_date_none_when_missing():
    assert _gov_contract_period_date({}) is None
    assert _gov_contract_period_date({"Year": 2026}) is None


def test_gov_contract_period_date_none_on_bad_quarter():
    assert _gov_contract_period_date({"Year": 2026, "Qtr": 5}) is None


def test_resolve_signal_date_gov_contracts_is_stable_across_repeat_pulls():
    """The core regression: two pulls of the identical record on two
    different calendar days must resolve to the SAME signal_date."""
    rec = {"Ticker": "LMT", "Year": 2026, "Qtr": 2, "Amount": 5_000_000}
    day1 = _resolve_signal_date(rec, "gov_contracts", today=date(2026, 6, 1))
    day2 = _resolve_signal_date(rec, "gov_contracts", today=date(2026, 6, 2))
    assert day1 == day2 == date(2026, 3, 31)


def test_resolve_signal_date_gov_contracts_falls_back_to_today_without_year_qtr():
    rec = {"Ticker": "LMT", "Amount": 5_000_000}
    today = date(2026, 6, 1)
    assert _resolve_signal_date(rec, "gov_contracts", today) == today


def test_resolve_signal_date_other_endpoints_unaffected():
    """Non-gov_contracts endpoints keep using an explicit Date field when present."""
    rec = {"Ticker": "AAPL", "Date": "2026-05-01"}
    assert _resolve_signal_date(rec, "senate_trading", today=date(2026, 6, 1)) == date(2026, 5, 1)


def test_resolve_signal_date_other_endpoints_default_today_without_date():
    rec = {"Ticker": "AAPL"}
    today = date(2026, 6, 1)
    assert _resolve_signal_date(rec, "wsb", today) == today
