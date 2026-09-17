"""Tests for db.py's per-thread checkout attribution.

Added after a 2026-09-17 production burst (grid-hermes: checked_out
climbed 17->48 during a cycle) could not be attributed to a specific
thread from existing evidence alone -- only that it was temporally
coincident with an options scan running in the cycle's own worker thread,
while a separate always-on llm-taskqueue background thread was
independently and concurrently doing real DB work (a confirmed
`store.snapshots:save_snapshot` write) in the same window. This closes
that gap for future bursts: every checkout/checkin is now tied to the
thread that made it, with lifetime tracking, so the next burst is
attributable without a fresh investigation.

Uses a local SQLite QueuePool; no production database needed. Never
asserts on or logs SQL text, parameters, or credentials -- only thread
names, counts, and durations.
"""

from __future__ import annotations

import threading
import time

from loguru import logger as log
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool

import db as db_module
from db import get_checkout_attribution


def _sqlite_engine(pool_size: int = 8, max_overflow: int = 4):
    return create_engine(
        "sqlite:///:memory:",
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
    )


def _wire_attribution(engine):
    """Attach the same checkout/checkin listeners get_engine() attaches,
    against a throwaway test engine instead of the process's real one.
    """
    @event.listens_for(engine, "checkout")
    def _on_checkout(dbapi_conn, connection_rec, connection_proxy):  # noqa: ARG001
        db_module._record_checkout_open(connection_rec)

    @event.listens_for(engine, "checkin")
    def _on_checkin(dbapi_conn, connection_rec):  # noqa: ARG001
        db_module._record_checkout_closed(connection_rec)


def setup_function(_fn):
    # Every test starts with a clean attribution table, independent of
    # whatever an earlier test (or an already-created shared engine) left
    # open.
    with db_module._checkout_lock:
        db_module._open_checkouts.clear()


class TestCheckoutAttributionLifecycle:
    def test_no_open_checkouts_reports_empty(self):
        attribution = get_checkout_attribution()
        assert attribution["by_thread"] == {}
        assert attribution["oldest_open_seconds"] is None

    def test_real_checkout_event_records_current_thread(self):
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn = engine.connect()
        try:
            attribution = get_checkout_attribution()
            assert attribution["by_thread"] == {threading.current_thread().name: 1}
            assert attribution["oldest_open_seconds"] is not None
        finally:
            conn.close()

    def test_real_checkin_event_clears_attribution(self):
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn = engine.connect()
        conn.close()

        attribution = get_checkout_attribution()
        assert attribution["by_thread"] == {}
        assert attribution["oldest_open_seconds"] is None

    def test_multiple_connections_same_thread_count_correctly(self):
        engine = _sqlite_engine(pool_size=5, max_overflow=2)
        _wire_attribution(engine)

        conns = [engine.connect() for _ in range(3)]
        try:
            attribution = get_checkout_attribution()
            assert attribution["by_thread"] == {threading.current_thread().name: 3}
        finally:
            for c in conns:
                c.close()


class TestCheckoutAttributionAcrossThreads:
    def test_two_named_threads_are_distinguished(self):
        """The actual scenario this exists for: two concurrently-running
        threads (e.g. a hermes cycle worker and the llm-taskqueue
        background thread) each holding their own connections must show
        up as separate owners, not merged into one count.
        """
        engine = _sqlite_engine(pool_size=8, max_overflow=4)
        _wire_attribution(engine)

        held_a: list = []
        held_b: list = []
        barrier = threading.Barrier(2)

        def _worker(name: str, held: list, n: int):
            threading.current_thread().name = name
            barrier.wait(timeout=5)
            for _ in range(n):
                held.append(engine.connect())

        ta = threading.Thread(target=_worker, args=("hermes-cycle-6196", held_a, 2))
        tb = threading.Thread(target=_worker, args=("llm-taskqueue", held_b, 1))
        ta.start()
        tb.start()
        ta.join(timeout=5)
        tb.join(timeout=5)

        try:
            attribution = get_checkout_attribution()
            assert attribution["by_thread"] == {"hermes-cycle-6196": 2, "llm-taskqueue": 1}
        finally:
            for c in held_a + held_b:
                c.close()

    def test_concurrent_checkout_and_checkin_is_thread_safe(self):
        """Many threads racing to open and close connections must never
        corrupt the shared attribution table (lost entries, exceptions,
        or a count that doesn't return to zero once everything closes).
        """
        engine = _sqlite_engine(pool_size=10, max_overflow=10)
        _wire_attribution(engine)
        errors: list[Exception] = []

        def _churn(n: int):
            try:
                for _ in range(n):
                    conn = engine.connect()
                    conn.close()
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=_churn, args=(20,)) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        # Every connection was closed -- nothing should still be tracked.
        assert get_checkout_attribution()["by_thread"] == {}


class _LoguruCapture:
    """Minimal loguru sink capture -- pytest's caplog only sees records
    routed through the standard `logging` module, which loguru does not
    use by default in this codebase (confirmed: no propagate/bridge
    configured in tests/conftest.py), so asserting on loguru output needs
    loguru's own logger.add(sink) instead.
    """

    def __init__(self):
        self.lines: list[str] = []
        self._sink_id = None

    def __enter__(self):
        self._sink_id = log.add(lambda msg: self.lines.append(str(msg)), level="WARNING")
        return self

    def __exit__(self, *exc_info):
        log.remove(self._sink_id)

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


class TestLifetimeWarning:
    def test_short_lived_connection_does_not_warn(self):
        engine = _sqlite_engine()
        _wire_attribution(engine)

        with _LoguruCapture() as cap:
            conn = engine.connect()
            conn.close()

        assert "held" not in cap.text.lower()

    def test_long_lived_connection_warns_with_thread_and_duration_only(self, monkeypatch):
        """Exercises the actual warning path without a real multi-second
        sleep: lowers the lifetime threshold so a brief real sleep exceeds
        it, then confirms the warning names the thread and a duration and
        nothing else (no SQL, no connection details).
        """
        monkeypatch.setattr(db_module, "_LIFETIME_WARN_SECONDS", 0.01)
        engine = _sqlite_engine()
        _wire_attribution(engine)

        with _LoguruCapture() as cap:
            conn = engine.connect()
            time.sleep(0.05)
            conn.close()

        assert "held" in cap.text.lower()
        assert threading.current_thread().name in cap.text
        # Nothing resembling SQL or a connection string leaked into the log.
        assert "select" not in cap.text.lower()
        assert "sqlite://" not in cap.text


class TestGetPoolStatsIncludesAttribution:
    def test_checkout_owners_key_present_and_shaped(self):
        from db import get_pool_stats

        engine = _sqlite_engine(pool_size=4, max_overflow=1)
        _wire_attribution(engine)

        conn = engine.connect()
        try:
            stats = get_pool_stats(engine)
            assert "checkout_owners" in stats
            assert stats["checkout_owners"]["by_thread"] == {threading.current_thread().name: 1}
        finally:
            conn.close()
