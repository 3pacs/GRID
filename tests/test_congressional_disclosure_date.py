"""Tests for GD-FIX: congressional.py's honest disclosure-date resolution.

Before this fix, a missing DisclosureDate/FilingDate silently fell back to
the transaction date, asserting a same-day disclosure the source never
claimed (the plan's evidence: disclosure_date == transaction_date on all
636 April-May rows). resolve_disclosure_date() now falls back to the
45-day STOCK Act statutory bound instead, and reports which basis it used.
"""

from __future__ import annotations

from datetime import date, timedelta

from ingestion.altdata.congressional import (
    DISCLOSURE_STATUTORY_LAG_DAYS,
    resolve_disclosure_date,
)


def test_reported_disclosure_date_is_used_as_is():
    txn_date = date(2026, 3, 1)
    disc_date, basis = resolve_disclosure_date(txn_date, "2026-03-10")
    assert disc_date == date(2026, 3, 10)
    assert basis == "reported"


def test_missing_disclosure_date_uses_statutory_bound_not_transaction_date():
    txn_date = date(2026, 3, 1)
    disc_date, basis = resolve_disclosure_date(txn_date, "")
    assert disc_date == txn_date + timedelta(days=DISCLOSURE_STATUTORY_LAG_DAYS)
    assert disc_date != txn_date
    assert basis == "statutory_bound"


def test_missing_disclosure_date_none_uses_statutory_bound():
    txn_date = date(2026, 3, 1)
    disc_date, basis = resolve_disclosure_date(txn_date, None)
    assert disc_date == txn_date + timedelta(days=DISCLOSURE_STATUTORY_LAG_DAYS)
    assert basis == "statutory_bound"


def test_unparseable_disclosure_date_uses_statutory_bound():
    txn_date = date(2026, 3, 1)
    disc_date, basis = resolve_disclosure_date(txn_date, "not-a-date")
    assert disc_date == txn_date + timedelta(days=DISCLOSURE_STATUTORY_LAG_DAYS)
    assert basis == "statutory_bound"


def test_disclosure_date_before_transaction_date_is_rejected():
    """A reported disclosure date earlier than the trade itself is a data
    error, not a real disclosure — fall back to the statutory bound rather
    than trusting a value that implies the member disclosed before trading.
    """
    txn_date = date(2026, 3, 10)
    disc_date, basis = resolve_disclosure_date(txn_date, "2026-03-01")
    assert disc_date == txn_date + timedelta(days=DISCLOSURE_STATUTORY_LAG_DAYS)
    assert basis == "statutory_bound"


def test_same_day_disclosure_is_accepted_as_reported():
    txn_date = date(2026, 3, 1)
    disc_date, basis = resolve_disclosure_date(txn_date, "2026-03-01")
    assert disc_date == txn_date
    assert basis == "reported"
