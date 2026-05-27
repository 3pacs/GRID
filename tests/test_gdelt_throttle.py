"""Tests for the GDELT DOC-API token-bucket throttle.

Regression: the GDELT puller hit HTTP 429 because it fired up to ~24 DOC-API
requests with only a 1s gap (and retries could burst harder), exceeding the
~12 req/60s ceiling. The shared _TokenBucket enforces sustained spacing.

The throttle takes injectable clock + sleep callables so we can assert its
pacing deterministically without sleeping in real time.
"""

from __future__ import annotations

import pytest

from ingestion.altdata.gdelt import (
    _GDELT_DOC_MAX_PER_WINDOW,
    _GDELT_DOC_WINDOW_SECONDS,
    _TokenBucket,
)


class _FakeClock:
    """Manually-advanced monotonic clock; sleep advances the clock."""

    def __init__(self) -> None:
        self.now = 0.0
        self.total_slept = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.now += seconds
        self.total_slept += seconds


def _make(clock: _FakeClock, **kw) -> _TokenBucket:
    return _TokenBucket(clock=clock.time, sleep=clock.sleep, **kw)


def test_first_request_is_immediate():
    clock = _FakeClock()
    bucket = _make(clock)
    assert bucket.acquire() == 0.0
    assert clock.total_slept == 0.0


def test_sustained_rate_never_exceeds_limit():
    # Fire 3x the window's worth of requests back-to-back. With no external
    # delay, the bucket must space them so the rate stays <= the ceiling.
    clock = _FakeClock()
    bucket = _make(clock)
    n = _GDELT_DOC_MAX_PER_WINDOW * 3
    for _ in range(n):
        bucket.acquire()

    elapsed = clock.now
    # n requests should take at least (n-1)/rate seconds (first is free).
    rate = _GDELT_DOC_MAX_PER_WINDOW / _GDELT_DOC_WINDOW_SECONDS
    min_expected = (n - 1) / rate - 1e-6
    assert elapsed >= min_expected

    # Any sliding 60s window must contain no more than the allowed count.
    per_window = elapsed and (n / elapsed) * _GDELT_DOC_WINDOW_SECONDS
    assert per_window <= _GDELT_DOC_MAX_PER_WINDOW + 1  # +1 burst tolerance


def test_spacing_matches_configured_rate():
    clock = _FakeClock()
    bucket = _make(clock)
    bucket.acquire()  # free
    slept = bucket.acquire()  # must wait ~5s for the 12/60s rate
    expected_gap = _GDELT_DOC_WINDOW_SECONDS / _GDELT_DOC_MAX_PER_WINDOW
    assert slept == pytest.approx(expected_gap, rel=0.05)


def test_idle_time_refills_no_sleep_needed():
    clock = _FakeClock()
    bucket = _make(clock)
    bucket.acquire()
    # Advance the clock past a full refill interval, then the next call is free.
    clock.now += _GDELT_DOC_WINDOW_SECONDS
    assert bucket.acquire() == 0.0


def test_burst_capacity_then_throttle():
    clock = _FakeClock()
    # Allow a burst of 3, then sustained spacing.
    bucket = _make(clock, capacity=3.0)
    assert bucket.acquire() == 0.0
    assert bucket.acquire() == 0.0
    assert bucket.acquire() == 0.0
    # 4th exceeds the burst, must sleep.
    assert bucket.acquire() > 0.0


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        _TokenBucket(max_per_window=0)
    with pytest.raises(ValueError):
        _TokenBucket(window_seconds=0)
