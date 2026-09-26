"""Dry-run outcome boundary tests; no provider, database, or persistence."""
from datetime import date, datetime, timedelta, timezone

import pytest

from evaluation.signal_outcomes import PricePoint, SignalRecord, evaluate_signal
from evaluation.prices import PITPriceAccessor


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


def test_direct_record_cannot_assert_unverified_live_origin():
    with pytest.raises(TypeError):
        rec(origin_tag="live")


class _RawResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class _RawEngine:
    """Fake SQL transport exercising the real PITPriceAccessor selection path."""

    def __init__(self, bars):
        self.bars = bars
        self.requests = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params):
        self.requests.append((str(query), params))
        candidates = [bar for bar in self.bars if bar[0] <= params["as_of"] and
                      params["verified_since"] <= bar[3] <= params["cutoff"]]
        if not candidates:
            return _RawResult(None)
        winner = max(candidates, key=lambda bar: (bar[0], bar[3]))
        same_date = [bar for bar in candidates if bar[0] == winner[0]]
        distinct_value_count = len({bar[1] for bar in same_date})
        return _RawResult((*winner, distinct_value_count))


def _raw_bar(day, value):
    return (day, value, "YF:AAA:close", datetime(day.year, day.month, day.day, 22, tzinfo=timezone.utc))


def _real_accessor(bars):
    engine = _RawEngine(bars)
    verified = datetime(2026, 8, 1, tzinfo=timezone.utc)  # test fixture, not live cutover evidence
    return PITPriceAccessor(engine, verified_raw_close_since=verified), engine


def test_real_accessor_friday_close_cannot_be_post_known_weekend_entry():
    prices, engine = _real_accessor([_raw_bar(date(2026, 9, 4), 100)])
    signal = rec(signal_date=date(2026, 9, 4),
                 created_at=datetime(2026, 9, 4, 21, 30, tzinfo=timezone.utc))
    result = evaluate_signal(signal, prices, today=date(2026, 9, 15))
    assert result.outcome == "INELIGIBLE"
    assert result.eligibility_reason == "stale_entry_bar"
    assert engine.requests[0][1]["as_of"] == date(2026, 9, 5)


def test_real_accessor_early_exit_cannot_score_nominal_horizon():
    prices, engine = _real_accessor([_raw_bar(date(2026, 9, 1), 100),
                                    _raw_bar(date(2026, 9, 3), 110)])
    result = evaluate_signal(rec(), prices, today=date(2026, 9, 15))
    assert result.outcome == "INELIGIBLE"
    assert result.eligibility_reason == "stale_exit_bar"
    assert engine.requests[-1][1]["as_of"] == date(2026, 9, 6)


@pytest.mark.parametrize("band,cost", [
    (float("nan"), 0), (float("inf"), 0), (-1, 0), (101, 0),
    (1, float("nan")), (1, float("inf")), (1, -1), (1, 10001),
])
def test_invalid_scoring_parameters_refused_before_price_lookup(band, cost):
    calls = []
    with pytest.raises(ValueError):
        evaluate_signal(rec(), accessor({}, calls), dead_band_pct=band, cost_bps=cost)
    assert calls == []


def test_real_accessor_multi_valued_date_refused_not_averaged_or_latest_wins():
    """A second writer (fill_missing_features.py, pre-#656) inserted a second,
    differently-valued row for the same obs_date under the same series/source
    identity. evaluate_signal must come back INELIGIBLE(ambiguous_raw_close_
    multiple_values), never CORRECT/WRONG/NO_MOVE from a silently-picked value."""
    contaminated_entry_day = date(2026, 9, 1)
    prices, engine = _real_accessor([
        _raw_bar(contaminated_entry_day, 100),
        (contaminated_entry_day, 130, "YF:AAA:close",
         datetime(contaminated_entry_day.year, contaminated_entry_day.month, contaminated_entry_day.day, 23, tzinfo=timezone.utc)),
        _raw_bar(date(2026, 9, 6), 110),
    ])
    result = evaluate_signal(rec(), prices, today=date(2026, 9, 7))
    assert result.outcome == "INELIGIBLE"
    assert result.eligibility_reason == "ambiguous_raw_close_multiple_values"


def test_real_accessor_refuses_crypto_instrument_before_any_price_lookup():
    prices, engine = _real_accessor([_raw_bar(date(2026, 9, 1), 100)])
    signal = rec(instrument="BTC-USD")
    result = evaluate_signal(signal, prices, today=date(2026, 9, 8))
    assert result.outcome == "INELIGIBLE"
    assert result.eligibility_reason == "unsupported_instrument_history"
    assert engine.requests == []
