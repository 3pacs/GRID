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


def test_reset_pool_peak_retains_current_checked_out_as_baseline():
    """Connections already checked out at reset time won't fire a new
    'checkout' event — they were borrowed before the reset and stay
    borrowed — so resetting to 0 would silently hide them from the next
    interval's peak until some *other* checkout happens to occur. The
    reset must use the current checked-out count as its floor, not 0.
    """
    # Simulate: 5 connections already checked out when the reset happens.
    reset_pool_peak(baseline=5)
    assert get_pool_peak_checked_out() == 5

    # No new checkout occurs, but the peak must still reflect what was
    # already outstanding at reset time.
    assert get_pool_peak_checked_out() == 5

    # A later, lower reading during the interval still doesn't erase it.
    db_module._record_checkout_peak(3)
    assert get_pool_peak_checked_out() == 5

    # A genuine new high replaces it correctly.
    db_module._record_checkout_peak(9)
    assert get_pool_peak_checked_out() == 9


def test_real_queuepool_events_drive_peak_not_manual_calls():
    """Exercises actual SQLAlchemy QueuePool checkout/checkin events end to
    end — not by calling the private recording helper directly, but by
    attaching the same kind of listener get_engine() attaches and then
    performing real .connect()/.close() calls, the way application code
    actually acquires connections.
    """
    from sqlalchemy import event

    reset_pool_peak()
    engine = _sqlite_engine(pool_size=6, max_overflow=2)

    @event.listens_for(engine, "checkout")
    def _on_checkout(dbapi_conn, connection_rec, connection_proxy):  # noqa: ARG001
        db_module._record_checkout_peak(engine.pool.checkedout())

    conns = [engine.connect() for _ in range(5)]  # real checkout events fire
    for c in conns[:3]:
        c.close()  # real checkin events fire; peak must not shrink

    assert engine.pool.checkedout() == 2  # 5 borrowed, 3 returned
    assert get_pool_peak_checked_out() == 5  # but the real peak was 5

    for c in conns[3:]:
        c.close()


def test_get_engine_attaches_peak_listener_before_first_checkout(monkeypatch):
    """Exercises the real get_engine() construction path — not a hand-rolled
    copy of it — to verify the checkout listener is registered as part of
    engine creation itself, before the engine object is ever handed back to
    a caller. If registration happened after get_engine() returned instead,
    a caller that checks out a connection immediately could race ahead of
    it and go unrecorded.
    """
    from sqlalchemy.pool import QueuePool

    monkeypatch.setattr(db_module, "_engine", None)
    monkeypatch.setenv("GRID_DB_POOL_SIZE", "6")
    monkeypatch.setenv("GRID_DB_MAX_OVERFLOW", "2")
    monkeypatch.setattr(
        db_module, "settings",
        type("FakeSettings", (), {"DB_URL": "sqlite:///:memory:", "DB_PASSWORD": "x"})(),
    )

    def _fake_create_engine(url, **kwargs):
        # get_engine() passes postgres-only connect_args (a `-c
        # statement_timeout=` option) sqlite doesn't understand; strip
        # them but keep pool_size/max_overflow so the real listener sees
        # a real QueuePool built with the real configured limits.
        return create_engine(
            "sqlite:///:memory:",
            poolclass=QueuePool,
            pool_size=kwargs["pool_size"],
            max_overflow=kwargs["max_overflow"],
        )

    monkeypatch.setattr(db_module, "create_engine", _fake_create_engine)
    reset_pool_peak()

    engine = db_module.get_engine()  # the real function — real registration

    # The very first checkout this engine ever sees. If the listener
    # weren't already attached by the time get_engine() returned this
    # value, it would still read 0.
    conn = engine.connect()
    try:
        assert get_pool_peak_checked_out() >= 1
    finally:
        conn.close()
        monkeypatch.setattr(db_module, "_engine", None)  # don't leak into other tests


def test_concurrent_peak_read_and_reset_is_safe():
    """Not just concurrent writers (test_record_checkout_peak_is_thread_safe)
    — readers and resetters racing each other must never crash or observe
    a torn value, since both paths take the same lock.
    """
    reset_pool_peak()
    errors: list[Exception] = []
    observed: list[int] = []
    observed_lock = threading.Lock()

    def _resetter(n: int) -> None:
        try:
            for i in range(50):
                reset_pool_peak(baseline=(n * 50 + i) % 17)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    def _reader() -> None:
        try:
            for _ in range(50):
                v = get_pool_peak_checked_out()
                with observed_lock:
                    observed.append(v)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=_resetter, args=(n,)) for n in range(4)]
    threads += [threading.Thread(target=_reader) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # Every observed value must be a value some resetter actually set
    # (0..16) — never a torn/corrupted read.
    assert all(0 <= v <= 16 for v in observed)
