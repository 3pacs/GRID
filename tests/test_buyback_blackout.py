"""Buyback blackout share uses only earnings schedules known by the as-of date.

Pure computation; sockets are refused, so nothing reaches the network.
"""

from __future__ import annotations

import socket
from datetime import date

import pytest

from features.buyback_blackout import (
    ScheduledEarnings,
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
WEIGHTS = {"AAPL": 0.40, "MSFT": 0.35, "NVDA": 0.25}


def test_buyback_blackout_share_exact() -> None:
    earnings = [
        ScheduledEarnings("AAPL", date(2026, 11, 25), KNOWN),  # 5d ahead -> IN blackout
        ScheduledEarnings("MSFT", date(2026, 11, 19), KNOWN),  # 1d ago -> IN blackout (+2d)
        ScheduledEarnings("NVDA", date(2027, 2, 20), KNOWN),   # >35d ahead -> NOT in blackout
    ]
    share = compute_blackout_share_on_date(date(2026, 11, 20), earnings, WEIGHTS)
    assert share == pytest.approx(0.75)
    ser = compute_blackout_series([date(2026, 11, 20), date(2026, 12, 5)], earnings, lambda _d: WEIGHTS)
    assert ser.loc[date(2026, 11, 20)] == pytest.approx(0.75)
    assert ser.loc[date(2026, 12, 5)] == pytest.approx(0.0)


def test_a_schedule_not_yet_known_does_not_count() -> None:
    # AAPL's date was first seen on 11-22: on 11-20 GRID could not know it.
    earnings = [ScheduledEarnings("AAPL", date(2026, 11, 25), known_at=date(2026, 11, 22))]
    assert compute_blackout_share_on_date(date(2026, 11, 20), earnings, WEIGHTS) == pytest.approx(0.0)
    assert compute_blackout_share_on_date(date(2026, 11, 23), earnings, WEIGHTS) == pytest.approx(0.40)


def test_latest_known_vintage_of_a_fiscal_period_wins() -> None:
    earnings = [
        # First schedule put Q3 results on 12-30; on 11-18 it moved to 11-25.
        ScheduledEarnings("AAPL", date(2026, 12, 30), date(2026, 10, 1), "2026Q3"),
        ScheduledEarnings("AAPL", date(2026, 11, 25), date(2026, 11, 18), "2026Q3"),
    ]
    # 11-10: only the first vintage is known; 12-30 - 35d = 11-25, not yet in blackout.
    assert compute_blackout_share_on_date(date(2026, 11, 10), earnings, WEIGHTS) == pytest.approx(0.0)
    # 11-20: the moved date is known and replaces the old one.
    assert compute_blackout_share_on_date(date(2026, 11, 20), earnings, WEIGHTS) == pytest.approx(0.40)
    # 11-29: past the moved date + 2d; the stale 12-30 vintage no longer counts.
    assert compute_blackout_share_on_date(date(2026, 11, 29), earnings, WEIGHTS) == pytest.approx(0.0)


def test_weights_are_taken_as_of_each_date() -> None:
    earnings = [ScheduledEarnings("AAPL", date(2026, 11, 25), KNOWN)]
    caps = {date(2026, 11, 20): {"AAPL": 1.0, "MSFT": 1.0}, date(2026, 11, 21): {"AAPL": 3.0, "MSFT": 1.0}}
    ser = compute_blackout_series(list(caps), earnings, caps.__getitem__)
    assert ser.tolist() == pytest.approx([0.5, 0.75])


def test_no_positive_weight_returns_none() -> None:
    assert compute_blackout_share_on_date(date(2026, 11, 20), [], {"AAPL": 0.0}) is None
