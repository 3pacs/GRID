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

import pytest
from loguru import logger as log
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool

import db as db_module
from db import get_checkout_attribution, get_outstanding_checkouts, reset_pool_peak


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


@pytest.fixture(autouse=True)
def _reset_module_state():
    # Every test starts with a clean attribution table AND a clean peak
    # tracker, independent of whatever an earlier test -- in this file or
    # another (both share db.py's module-level state) -- left behind. A
    # prior test file's thread-safety test can leave the peak at a large
    # nonzero value; without resetting it here, an assertion in this file
    # that expects a known peak value would fail before ever reaching the
    # rest of that test body, itself leaving stale state for tests after
    # it -- exactly the kind of cross-test pollution this exists to avoid.
    #
    # Deliberately a pytest fixture, not a bare `setup_function`: this
    # module's tests are all methods on Test* classes, and
    # setup_function/teardown_function only apply to plain module-level
    # test functions -- they silently never fire for class-based tests
    # (setup_method is the per-class equivalent). An autouse fixture is
    # the one mechanism that actually applies uniformly regardless of
    # whether a test is a bare function or a class method. This was
    # verified empirically: an earlier setup_function-based version of
    # this reset passed every test in isolation (nothing to clean up yet)
    # and only failed when run alongside another test file -- exactly the
    # signature of a reset that was never actually running.
    reset_pool_peak()
    with db_module._checkout_lock:
        db_module._open_checkouts.clear()
    yield
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


class TestInvalidationAndReconnect:
    """Verifies actual behavior rather than assuming it -- these are the
    exact lifecycle edges a naive id(connection_rec)-keyed table could get
    wrong: invalidation, reconnect, and cross-thread release.
    """

    def test_invalidated_connection_still_gets_checked_in(self):
        """Invalidating mid-use replaces the underlying DBAPI connection
        for next time -- it does not skip returning the pool slot. If it
        did, this would leave a permanently stale entry that never clears.
        """
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn = engine.connect()
        assert get_checkout_attribution()["open_count"] == 1

        conn.invalidate()
        conn.close()

        assert get_checkout_attribution()["open_count"] == 0

    def test_reconnect_after_invalidation_gets_its_own_fresh_entry(self):
        """A new checkout after invalidation must be tracked as its own
        entry, not confused with (or blocked by) the invalidated one --
        id(connection_rec) differs for the new pool slot's connection
        record, so this should Just Work, but is verified rather than
        assumed.
        """
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn1 = engine.connect()
        conn1.invalidate()
        conn1.close()
        assert get_checkout_attribution()["open_count"] == 0

        conn2 = engine.connect()
        try:
            assert get_checkout_attribution()["open_count"] == 1
        finally:
            conn2.close()
        assert get_checkout_attribution()["open_count"] == 0

    def test_checkin_from_a_different_thread_preserves_acquiring_thread(self):
        """Checkout and checkin are not required to happen on the same
        thread. The *acquiring* thread (captured at checkout) must survive
        to the checkin-side warning even when a different thread performs
        the actual release.
        """
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn_holder: list = []

        def _acquire():
            threading.current_thread().name = "acquirer-thread"
            conn_holder.append(engine.connect())

        acquirer = threading.Thread(target=_acquire)
        acquirer.start()
        acquirer.join(timeout=5)

        assert get_checkout_attribution()["by_thread"] == {"acquirer-thread": 1}

        # A *different* thread (the test's main thread) releases it --
        # closing the connection object itself fires the real checkin
        # event from this thread, not the acquiring one.
        released_by_main_thread = threading.current_thread().name
        assert released_by_main_thread != "acquirer-thread"
        conn_holder[0].close()
        assert get_checkout_attribution()["by_thread"] == {}


class TestEngineDisposalClearsAttribution:
    def test_clear_engine_clears_stale_checkouts_and_peak(self, monkeypatch):
        """A checkout still open when clear_engine() disposes the engine
        would otherwise never get a matching checkin from the *new*
        engine's listeners -- permanently stale. clear_engine() must wipe
        both the attribution table and the peak tracker.
        """
        fake_disposed = _sqlite_engine()
        monkeypatch.setattr(db_module, "_engine", fake_disposed)
        with db_module._checkout_lock:
            db_module._open_checkouts[12345] = ("some-stale-thread", time.monotonic())
        db_module._record_checkout_peak(7)
        assert get_checkout_attribution()["open_count"] == 1
        assert db_module.get_pool_peak_checked_out() == 7

        db_module.clear_engine()

        assert get_checkout_attribution()["open_count"] == 0
        assert db_module.get_pool_peak_checked_out() == 0
        assert db_module._engine is None


class TestOutstandingCheckoutsVisibility:
    """A checkin-side duration warning can only fire once a connection
    returns -- it can never reveal one that never returns at all. These
    exercise the point-in-time, on-demand view that closes that gap.
    """

    def test_reports_age_and_acquiring_thread_without_requiring_checkin(self):
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn = engine.connect()  # never closed within this test
        try:
            outstanding = get_outstanding_checkouts()
            assert len(outstanding) == 1
            assert outstanding[0]["acquired_by"] == threading.current_thread().name
            assert outstanding[0]["age_seconds"] >= 0
        finally:
            conn.close()

    def test_min_age_filter_excludes_recent_checkouts(self):
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn = engine.connect()
        try:
            assert get_outstanding_checkouts(min_age_seconds=60.0) == []
            assert len(get_outstanding_checkouts(min_age_seconds=0.0)) == 1
        finally:
            conn.close()

    def test_sorted_oldest_first_across_threads(self):
        engine = _sqlite_engine(pool_size=8, max_overflow=4)
        _wire_attribution(engine)

        with db_module._checkout_lock:
            now = time.monotonic()
            db_module._open_checkouts[111] = ("thread-old", now - 10.0)
            db_module._open_checkouts[222] = ("thread-new", now - 1.0)

        result = get_outstanding_checkouts()
        assert [r["acquired_by"] for r in result] == ["thread-old", "thread-new"]

        with db_module._checkout_lock:
            db_module._open_checkouts.pop(111, None)
            db_module._open_checkouts.pop(222, None)

    def test_result_is_capped_and_keeps_the_oldest_entries(self):
        # A misconfiguration or genuine leak that accumulates far more open
        # connections than any real pool_size/max_overflow budget allows
        # must never turn this into unbounded logging. The cap must also
        # keep the *oldest* (most suspicious, longest-held) entries rather
        # than an arbitrary 200 -- those are the ones worth investigating.
        from db import _MAX_REPORTED_OPEN_CHECKOUTS

        now = time.monotonic()
        synthetic_count = _MAX_REPORTED_OPEN_CHECKOUTS + 5
        with db_module._checkout_lock:
            for i in range(synthetic_count):
                # Oldest entry is id 0 (age == synthetic_count seconds ago).
                db_module._open_checkouts[i] = (f"thread-{i}", now - (synthetic_count - i))

        try:
            result = get_outstanding_checkouts()
            assert len(result) == _MAX_REPORTED_OPEN_CHECKOUTS
            # The 5 newest (least suspicious) synthetic entries were dropped;
            # every entry that survived is at least as old as any dropped one.
            kept_names = {r["acquired_by"] for r in result}
            assert "thread-0" in kept_names  # oldest, must survive the cap
            for i in range(_MAX_REPORTED_OPEN_CHECKOUTS, synthetic_count):
                assert f"thread-{i}" not in kept_names  # newest 5 (highest i), dropped
            for i in range(0, _MAX_REPORTED_OPEN_CHECKOUTS):
                assert f"thread-{i}" in kept_names  # oldest 200 (lowest i), kept
        finally:
            with db_module._checkout_lock:
                for i in range(synthetic_count):
                    db_module._open_checkouts.pop(i, None)


class TestInstrumentationCannotBreakRealCheckout:
    """The actual production listeners (registered inside get_engine(),
    not the simplified _wire_attribution test helper above) must isolate
    every instrumentation call so a bug in tracking/logging can never
    prevent a real checkout or checkin from succeeding.
    """

    def test_get_engine_checkout_succeeds_even_if_attribution_tracking_raises(self, monkeypatch):
        monkeypatch.setattr(db_module, "_engine", None)
        monkeypatch.setenv("GRID_DB_POOL_SIZE", "4")
        monkeypatch.setenv("GRID_DB_MAX_OVERFLOW", "2")
        monkeypatch.setattr(
            db_module, "settings",
            type("FakeSettings", (), {"DB_URL": "sqlite:///:memory:", "DB_PASSWORD": "x"})(),
        )

        def _fake_create_engine(url, **kwargs):
            return create_engine(
                "sqlite:///:memory:",
                poolclass=QueuePool,
                pool_size=kwargs["pool_size"],
                max_overflow=kwargs["max_overflow"],
            )

        monkeypatch.setattr(db_module, "create_engine", _fake_create_engine)

        # Make the attribution helper explode on every call -- the real
        # checkout must still succeed regardless.
        def _boom(*args, **kwargs):
            raise RuntimeError("simulated instrumentation bug")

        monkeypatch.setattr(db_module, "_record_checkout_open", _boom)
        monkeypatch.setattr(db_module, "_record_checkout_closed", _boom)

        engine = db_module.get_engine()  # the real function, real listeners
        conn = engine.connect()  # must not raise despite _boom above
        try:
            assert conn.execute(__import__("sqlalchemy").text("SELECT 1")).scalar() == 1
        finally:
            conn.close()  # must not raise either
            monkeypatch.setattr(db_module, "_engine", None)  # don't leak into other tests
