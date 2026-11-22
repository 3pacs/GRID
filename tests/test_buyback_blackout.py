"""Buyback blackout share: exact known-at handling, conflicts, coverage and UNKNOWN.

Pure computation; sockets are refused, so nothing reaches the network.
"""

from __future__ import annotations

import itertools
import math
import socket
from datetime import date

import pytest

from features.buyback_blackout import (
    MIN_WEIGHT_COVERAGE,
    SERIES_COLUMNS,
    STATUS_OK,
    STATUS_UNKNOWN,
    ScheduledEarnings,
    compute_blackout_reading,
    compute_blackout_series,
    compute_blackout_share_on_date,
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    calls: list[object] = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    yield
    assert calls == []


KNOWN = date(2026, 10, 1)
AS_OF = date(2026, 11, 20)
WEIGHTS = {"AAPL": 0.40, "MSFT": 0.35, "NVDA": 0.25}


def _full_schedule() -> list[ScheduledEarnings]:
    return [
        ScheduledEarnings("AAPL", date(2026, 11, 25), KNOWN, "2026Q3"),  # 5d ahead -> IN blackout
        ScheduledEarnings("MSFT", date(2026, 11, 19), KNOWN, "2026Q3"),  # 1d ago -> IN (+2d)
        ScheduledEarnings("NVDA", date(2027, 2, 20), KNOWN, "2026Q4"),   # 92d ahead -> known, open
        ScheduledEarnings("AAPL", date(2027, 1, 29), KNOWN, "2026Q4"),
        ScheduledEarnings("MSFT", date(2027, 1, 27), KNOWN, "2026Q4"),
    ]


def test_buyback_blackout_share_exact() -> None:
    r = compute_blackout_reading(AS_OF, _full_schedule(), WEIGHTS)
    assert r.status == STATUS_OK and r.coverage == pytest.approx(1.0)
    assert r.share == pytest.approx(0.75)
    assert (r.share_lower, r.share_upper) == (pytest.approx(0.75), pytest.approx(0.75))
    assert compute_blackout_share_on_date(AS_OF, _full_schedule(), WEIGHTS) == pytest.approx(0.75)

    ser = compute_blackout_series([AS_OF, date(2026, 12, 5)], _full_schedule(), lambda _d: WEIGHTS)
    assert list(ser.columns) == SERIES_COLUMNS
    assert ser.loc[AS_OF, "share"] == pytest.approx(0.75)
    # 12-05: every next report is known and more than 35 days out.
    assert ser.loc[date(2026, 12, 5), "share"] == pytest.approx(0.0)
    assert ser.loc[date(2026, 12, 5), "status"] == STATUS_OK


def test_reversed_order_same_day_tie_is_a_conflict_not_a_pick() -> None:
    # Two AAPL 2026Q3 schedules first seen on the same day disagree: 11-25 is
    # inside the window on 11-20, 12-30 is not. Picking either would change
    # the answer with the input order.
    tie = [
        ScheduledEarnings("AAPL", date(2026, 11, 25), date(2026, 11, 1), "2026Q3"),
        ScheduledEarnings("AAPL", date(2026, 12, 30), date(2026, 11, 1), "2026Q3"),
    ]
    others = [e for e in _full_schedule() if e.ticker != "AAPL"]
    forward = compute_blackout_reading(AS_OF, others + tie, WEIGHTS, min_coverage=0.5)
    reverse = compute_blackout_reading(AS_OF, list(reversed(others + tie)), WEIGHTS, min_coverage=0.5)
    assert forward == reverse
    assert forward.conflicted_tickers == ("AAPL",)
    assert forward.coverage == pytest.approx(0.60)
    assert forward.share == pytest.approx(0.35 / 0.60)       # MSFT of the covered weight
    assert forward.share_lower == pytest.approx(0.35)
    assert forward.share_upper == pytest.approx(0.75)        # AAPL could be either way
    # At the default threshold the 60% coverage is UNKNOWN.
    assert compute_blackout_reading(AS_OF, others + tie, WEIGHTS).share is None


def test_result_is_independent_of_input_order() -> None:
    events = _full_schedule() + [
        ScheduledEarnings("AAPL", date(2026, 12, 30), date(2026, 9, 1), "2026Q3"),  # older vintage
        ScheduledEarnings("AAPL", date(2026, 11, 25), KNOWN, "2026Q3"),             # exact duplicate
    ]
    readings = {compute_blackout_reading(AS_OF, list(p), WEIGHTS) for p in itertools.permutations(events)}
    assert len(readings) == 1
    (only,) = readings
    assert only.conflicted_tickers == () and only.share == pytest.approx(0.75)


def test_no_schedule_is_unknown_not_open() -> None:
    weights = {"AAPL": 1.0}
    none_known = compute_blackout_reading(AS_OF, [], weights)
    assert none_known.status == STATUS_UNKNOWN and none_known.share is None
    assert none_known.unknown_tickers == ("AAPL",) and none_known.coverage == 0.0

    # A known next report outside the window: a real "not in blackout".
    open_ = compute_blackout_reading(AS_OF, [ScheduledEarnings("AAPL", date(2027, 1, 29), KNOWN)], weights)
    assert open_.status == STATUS_OK and open_.share == 0.0

    # Only a past report known, or the next one beyond the lookahead: UNKNOWN.
    past_only = [ScheduledEarnings("AAPL", date(2026, 10, 30), KNOWN)]
    too_far = [ScheduledEarnings("AAPL", date(2027, 4, 30), KNOWN)]
    assert compute_blackout_reading(AS_OF, past_only, weights).share is None
    assert compute_blackout_reading(AS_OF, too_far, weights).share is None


def test_a_schedule_not_yet_known_does_not_count() -> None:
    # AAPL's date was first seen on 11-22: on 11-20 GRID could not know it.
    earnings = [ScheduledEarnings("AAPL", date(2026, 11, 25), known_at=date(2026, 11, 22))]
    assert compute_blackout_share_on_date(AS_OF, earnings, {"AAPL": 1.0}) is None
    assert compute_blackout_share_on_date(date(2026, 11, 23), earnings, {"AAPL": 1.0}) == pytest.approx(1.0)


def test_latest_known_vintage_of_a_fiscal_period_wins() -> None:
    w = {"AAPL": 1.0}
    earnings = [
        # First schedule put Q3 results on 12-30; on 11-18 it moved to 11-25.
        ScheduledEarnings("AAPL", date(2026, 12, 30), date(2026, 10, 1), "2026Q3"),
        ScheduledEarnings("AAPL", date(2026, 11, 25), date(2026, 11, 18), "2026Q3"),
    ]
    # 11-10: only the first vintage is known; its window opens 11-25.
    assert compute_blackout_share_on_date(date(2026, 11, 10), earnings, w) == pytest.approx(0.0)
    # 11-20: the moved date replaces the old one.
    assert compute_blackout_share_on_date(date(2026, 11, 20), earnings, w) == pytest.approx(1.0)
    # 11-29: the moved report is past; the stale 12-30 vintage does not count,
    # and no next report is known, so the date is UNKNOWN, not "open".
    assert compute_blackout_share_on_date(date(2026, 11, 29), earnings, w) is None


def test_coverage_threshold_and_bounds() -> None:
    events = [
        ScheduledEarnings("A", date(2026, 11, 25), KNOWN),  # blackout
        ScheduledEarnings("B", date(2027, 1, 29), KNOWN),   # open
        ScheduledEarnings("C", date(2027, 1, 29), KNOWN),   # open
    ]
    at_threshold = compute_blackout_reading(AS_OF, events, {"A": 0.45, "B": 0.30, "C": 0.15, "D": 0.10})
    assert MIN_WEIGHT_COVERAGE == 0.90
    assert at_threshold.status == STATUS_OK and at_threshold.coverage == pytest.approx(0.90)
    assert at_threshold.share == pytest.approx(0.45 / 0.90)
    assert at_threshold.share_upper - at_threshold.share_lower == pytest.approx(0.10)
    assert at_threshold.unknown_tickers == ("D",)

    below = compute_blackout_reading(AS_OF, events, {"A": 0.45, "B": 0.30, "C": 0.14, "D": 0.11})
    assert below.status == STATUS_UNKNOWN and below.share is None
    assert "below 0.90" in (below.reason or "")
    assert below.share_lower == pytest.approx(0.45) and below.share_upper == pytest.approx(0.56)


def test_weights_are_taken_as_of_each_date() -> None:
    earnings = [ScheduledEarnings("AAPL", date(2026, 11, 25), KNOWN),
                ScheduledEarnings("MSFT", date(2027, 1, 27), KNOWN)]
    caps = {date(2026, 11, 20): {"AAPL": 1.0, "MSFT": 1.0}, date(2026, 11, 21): {"AAPL": 3.0, "MSFT": 1.0}}
    ser = compute_blackout_series(list(caps), earnings, caps.__getitem__)
    assert ser["share"].tolist() == pytest.approx([0.5, 0.75])


def test_no_positive_weight_is_unknown() -> None:
    r = compute_blackout_reading(AS_OF, [], {"AAPL": 0.0})
    assert r.share is None and r.reason == "no_positive_weight"
    ser = compute_blackout_series([AS_OF], [], lambda _d: {"AAPL": 0.0})
    assert math.isnan(ser.loc[AS_OF, "share"]) and ser.loc[AS_OF, "status"] == STATUS_UNKNOWN
