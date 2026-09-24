"""Dry-run outcome boundary tests; no provider, database, or persistence."""
from datetime import date, datetime, timedelta, timezone

import pytest

from evaluation.signal_outcomes import PricePoint, SignalRecord, evaluate_signal


def rec(**kw):
    defaults = dict(source_type="news", instrument="AAA", signal_date=date(2026, 9, 1),
                    direction="BUY", horizon_days=5,
                    created_at=datetime(2026, 9, 1, 18, tzinfo=timezone.utc))
    defaults.update(kw)
    return SignalRecord(**defaults)


def accessor(prices, calls):
    def read(_instrument, as_of):
        calls.append(as_of)
        return prices.get(as_of)
    return read


def point(day, value=100, basis="raw_close"):
    return PricePoint(value, day, basis)


def test_immature_horizon_never_fetches_exit_even_if_bar_exists():
    calls = []
    prices = {date(2026, 9, 1): point(date(2026, 9, 1)),
              date(2026, 9, 6): point(date(2026, 9, 6), 110)}
    result = evaluate_signal(rec(), accessor(prices, calls), today=date(2026, 9, 4))
    assert result.outcome == "UNRESOLVED"
    assert calls == [date(2026, 9, 1)]


@pytest.mark.parametrize("old,reason", [
    (date(2026, 8, 27), "stale_entry_bar"),
    (date(2026, 9, 1) - timedelta(days=200), "stale_entry_bar"),
    (date(2026, 9, 1), "stale_exit_bar"),
])
def test_stopped_or_stale_history_cannot_be_no_move(old, reason):
    calls = []
    prices = {date(2026, 9, 1): point(old), date(2026, 9, 6): point(old)}
    result = evaluate_signal(rec(), accessor(prices, calls), today=date(2026, 9, 8))
    assert result.outcome == "INELIGIBLE"
    assert result.eligibility_reason == reason


def test_valid_outcome_after_maturity():
    calls = []
    prices = {date(2026, 9, 1): point(date(2026, 9, 1)),
              date(2026, 9, 6): point(date(2026, 9, 6), 110)}
    result = evaluate_signal(rec(), accessor(prices, calls), today=date(2026, 9, 7))
    assert result.outcome == "CORRECT"
    assert result.entry_price_basis == result.exit_price_basis == "raw_close"


def test_after_16et_delays_entry_and_horizon():
    calls = []
    signal = rec(created_at=datetime(2026, 9, 1, 21, tzinfo=timezone.utc))
    prices = {date(2026, 9, 2): point(date(2026, 9, 2))}
    result = evaluate_signal(signal, accessor(prices, calls), today=date(2026, 9, 3))
    assert result.outcome == "UNRESOLVED"
    assert calls == [date(2026, 9, 2)]


def test_congressional_transaction_date_requires_public_known_at():
    calls = []
    result = evaluate_signal(rec(source_type="congressional"), accessor({}, calls), today=date(2026, 9, 8))
    assert result.eligibility_reason == "known_at_unverified"
    assert calls == []


def test_explicit_congressional_publication_can_anchor_entry():
    calls = []
    signal = rec(source_type="congressional", known_at=datetime(2026, 9, 3, 12, tzinfo=timezone.utc))
    result = evaluate_signal(signal, accessor({}, calls), today=date(2026, 9, 4))
    assert result.eligibility_reason == "missing_entry_price"
    assert calls == [date(2026, 9, 3)]


def test_entry_waits_for_later_ingestion_even_with_earlier_publication():
    calls = []
    signal = rec(source_type="congressional",
                 known_at=datetime(2026, 9, 2, 12, tzinfo=timezone.utc),
                 created_at=datetime(2026, 9, 4, 12, tzinfo=timezone.utc))
    result = evaluate_signal(signal, accessor({}, calls), today=date(2026, 9, 5))
    assert result.eligibility_reason == "missing_entry_price"
    assert calls == [date(2026, 9, 4)]


@pytest.mark.parametrize("stamp", [None, datetime(2026, 9, 1, 12)])
def test_missing_or_naive_known_at_refuses(stamp):
    calls = []
    result = evaluate_signal(rec(created_at=stamp), accessor({}, calls), today=date(2026, 9, 8))
    assert result.eligibility_reason == "known_at_unverified"
    assert calls == []


def test_adjusted_close_basis_is_refused():
    calls = []
    result = evaluate_signal(rec(), accessor({date(2026, 9, 1): point(date(2026, 9, 1), basis="adj_close")}, calls), today=date(2026, 9, 8))
    assert result.eligibility_reason == "unsupported_instrument_history"


def test_horizon_must_be_positive():
    with pytest.raises(ValueError, match="positive"):
        evaluate_signal(rec(horizon_days=0), accessor({}, []))
