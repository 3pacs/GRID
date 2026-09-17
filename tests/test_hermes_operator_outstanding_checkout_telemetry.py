"""Tests for hermes_operator.py's outstanding-checkout telemetry.

A 30-minute post-merge observation window (proposed for PR #533) needs a
way to see outstanding checkouts *inside the running Hermes process* --
db.py's checkout tracking is a process-local module dict, so a separate
Python invocation only ever sees its own empty tracking state, never
Hermes's. This wires get_outstanding_checkouts() into a periodic, bounded
poll that runs independently of cycle completion (the existing per-cycle
pool_stats log only fires when a cycle finishes, and a cycle can run for
up to CYCLE_TIMEOUT_SECONDS or hang past it), and confirms a connection
that has not been returned still shows up without needing to check in
first.

Uses a local SQLite QueuePool; no production database needed. Never
asserts on or logs SQL text, parameters, or credentials -- only thread
names, counts, and durations.
"""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool

import db as db_module
import scripts.hermes_operator as hermes


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
    # db.py's checkout table is process-global module state shared across
    # every test file in the suite (see test_db_checkout_attribution.py
    # for why this must be an autouse fixture and not setup_function: this
    # module's tests are class methods, and setup_function silently never
    # fires for those).
    with db_module._checkout_lock:
        db_module._open_checkouts.clear()
    yield
    with db_module._checkout_lock:
        db_module._open_checkouts.clear()


class TestLogOutstandingCheckoutsOnce:
    def test_no_open_checkouts_returns_empty_list(self):
        assert hermes._log_outstanding_checkouts_once() == []

    def test_open_but_unreturned_connection_appears(self):
        # The crux of the fix: a connection that is still checked out --
        # never closed -- must be visible without waiting for checkin.
        # This closes the previously-recorded gap that a duration warning
        # emitted only on checkin cannot reveal a connection that never
        # returns.
        engine = _sqlite_engine()
        _wire_attribution(engine)

        conn = engine.connect()
        try:
            result = hermes._log_outstanding_checkouts_once()
            assert len(result) == 1
            assert result[0]["acquired_by"] == threading.current_thread().name
            assert result[0]["age_seconds"] >= 0
        finally:
            conn.close()

        # And it disappears once actually returned.
        assert hermes._log_outstanding_checkouts_once() == []

    def test_multiple_outstanding_connections_all_appear(self):
        engine = _sqlite_engine(pool_size=5, max_overflow=2)
        _wire_attribution(engine)

        conns = [engine.connect() for _ in range(3)]
        try:
            result = hermes._log_outstanding_checkouts_once()
            assert len(result) == 3
        finally:
            for conn in conns:
                conn.close()

    def test_instrumentation_failure_is_isolated(self, monkeypatch):
        # This poll runs on its own daemon thread inside the live Hermes
        # process. An unhandled exception here must never propagate --
        # it would silently kill the telemetry thread (or, if it were
        # ever called inline, could take down real cycle work).
        import db as real_db

        def _raise():
            raise RuntimeError("boom")

        monkeypatch.setattr(real_db, "get_outstanding_checkouts", _raise)

        assert hermes._log_outstanding_checkouts_once() is None

    def test_does_not_log_when_nothing_outstanding(self, monkeypatch):
        # Quiet at rest -- only worth a log line when there's something to
        # see, unlike the per-cycle pool_stats log which always reports
        # capacity regardless of pressure.
        logged = []
        monkeypatch.setattr(hermes.log, "info", lambda *a, **k: logged.append((a, k)))

        hermes._log_outstanding_checkouts_once()

        assert logged == []

    def test_logs_when_something_outstanding(self, monkeypatch):
        logged = []
        monkeypatch.setattr(hermes.log, "info", lambda *a, **k: logged.append((a, k)))

        engine = _sqlite_engine()
        _wire_attribution(engine)
        conn = engine.connect()
        try:
            hermes._log_outstanding_checkouts_once()
        finally:
            conn.close()

        assert len(logged) == 1


class TestOutstandingCheckoutTelemetryLoop:
    def test_loop_sleeps_then_polls_on_the_injected_interval(self, monkeypatch):
        calls: list[str] = []

        def _fake_sleep(seconds):
            calls.append(f"sleep:{seconds}")
            if len(calls) >= 3:
                raise SystemExit("stop test loop")

        def _fake_poll():
            calls.append("poll")
            return []

        monkeypatch.setattr(hermes, "_log_outstanding_checkouts_once", _fake_poll)

        with pytest.raises(SystemExit):
            hermes._outstanding_checkout_telemetry_loop(
                interval_seconds=42, sleep_fn=_fake_sleep,
            )

        # Sleep happens before each poll (not after), and the loop polls
        # once per completed sleep -- two full iterations here before the
        # third sleep call raises and cuts the loop short.
        assert calls == ["sleep:42", "poll", "sleep:42"]

    def test_default_sleep_fn_is_real_time_sleep(self):
        # No sleep_fn override in production use -- confirms the injectable
        # parameter defaults to the real time.sleep rather than silently
        # becoming a no-op loop when called the normal way.
        import inspect

        sig = inspect.signature(hermes._outstanding_checkout_telemetry_loop)
        assert sig.parameters["sleep_fn"].default is hermes.time.sleep

    def test_poll_interval_fits_a_30_minute_observation_window(self):
        # A fixed 30-minute window (the size proposed for the post-merge
        # observation) should yield multiple samples, not just one or two.
        assert 0 < hermes.OUTSTANDING_CHECKOUT_POLL_SECONDS <= 300
        samples_in_30_min = (30 * 60) / hermes.OUTSTANDING_CHECKOUT_POLL_SECONDS
        assert samples_in_30_min >= 6
