"""Tests for db.get_pool_stats() — the shared pool-instrumentation helper.

Uses a local SQLite engine so these run deterministically without a
production database, per this repo's testing rules (mock external
dependencies, no live endpoints). No SQL text or credentials are ever
logged or asserted on here — only the numeric pool counters.
"""

from __future__ import annotations

import threading

from sqlalchemy import create_engine

import db as db_module
from db import get_pool_peak_checked_out, get_pool_stats, reset_pool_peak


def _sqlite_engine(pool_size: int = 5, max_overflow: int = 2):
    # SQLite's default pool doesn't take pool_size/max_overflow the same
    # way as a real QueuePool-backed engine, so we build a QueuePool
    # explicitly to exercise the same code path get_engine() uses.
    from sqlalchemy.pool import QueuePool

    return create_engine(
        "sqlite:///:memory:",
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
    )


def test_get_pool_stats_reports_configured_capacity():
    engine = _sqlite_engine(pool_size=5, max_overflow=2)
    stats = get_pool_stats(engine)

    assert stats["pool_size"] == 5
    assert stats["max_overflow"] == 2
    assert stats["capacity"] == 7
    # Nothing has borrowed a connection yet.
    assert stats["checked_out"] == 0


def test_get_pool_stats_distinguishes_checked_out_from_checked_in():
    engine = _sqlite_engine(pool_size=3, max_overflow=1)

    conn1 = engine.connect()
    conn2 = engine.connect()
    try:
        stats = get_pool_stats(engine)
        assert stats["checked_out"] == 2
        assert stats["capacity"] == 4
    finally:
        conn1.close()
        conn2.close()

    # Returned to the pool — checked_out drops back down, capacity is
    # unchanged (capacity is configuration, not current usage).
    stats_after = get_pool_stats(engine)
    assert stats_after["checked_out"] == 0
    assert stats_after["capacity"] == 4


def test_get_pool_stats_defaults_to_shared_engine(monkeypatch):
    """Calling with no argument falls back to the process's shared engine."""
    fake_engine = _sqlite_engine(pool_size=4, max_overflow=1)
    monkeypatch.setattr(db_module, "get_engine", lambda: fake_engine)

    stats = get_pool_stats()

    assert stats["pool_size"] == 4
    assert stats["capacity"] == 5


# --- Peak (high-water-mark) tracking -----------------------------------

def setup_function(_fn):
    # Every test starts from a clean high-water-mark, independent of
    # whatever earlier tests (or an already-created shared engine) recorded.
    reset_pool_peak()


def test_reset_pool_peak_starts_at_zero():
    assert get_pool_peak_checked_out() == 0


def test_record_checkout_peak_only_grows():
    db_module._record_checkout_peak(3)
    db_module._record_checkout_peak(7)
    db_module._record_checkout_peak(2)  # lower than the current peak

    assert get_pool_peak_checked_out() == 7


def test_get_pool_stats_reports_current_peak(monkeypatch):
    """A burst that has already ended is still visible via peak_checked_out,
    even though checked_out itself has dropped back down — this is the
    whole point: a point-in-time sample of checked_out alone would miss it.
    """
    engine = _sqlite_engine(pool_size=5, max_overflow=5)

    conns = [engine.connect() for _ in range(4)]
    db_module._record_checkout_peak(engine.pool.checkedout())
    for c in conns:
        c.close()

    stats = get_pool_stats(engine)
    assert stats["checked_out"] == 0  # the burst is over
    assert stats["peak_checked_out"] == 4  # but it happened


def test_record_checkout_peak_is_thread_safe():
    """Many threads racing to record peaks must never lose the true max
    to a lost update — this is exactly what the lock in
    _record_checkout_peak exists to prevent.
    """
    values = list(range(200))  # 0..199, true max is 199
    threads = [
        threading.Thread(target=db_module._record_checkout_peak, args=(v,))
        for v in values
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert get_pool_peak_checked_out() == 199
