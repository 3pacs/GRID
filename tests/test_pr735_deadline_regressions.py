"""Offline behavior regressions for the two independently reviewed #735 defects."""

import ast
import inspect
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ingestion.options as options
import ingestion.smart_scheduler as ss
from scripts import hermes_operator as ho
from tests.test_options_scheduler_budget import _puller


def test_expiration_after_final_capture_is_partial_without_catalog_write(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS"})
    live = [True]
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: live.__setitem__(0, False))
    result = obj.pull_all(should_continue=lambda: live[0])
    assert result.summary["status"] == "PARTIAL"
    assert result.summary["rows_inserted"] == 12
    assert result[0]["status"] == "SUCCESS"  # completed capture is still real
    obj._mark_catalog_pulled.assert_not_called()


def _catalog_puller(monkeypatch):
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = MagicMock()
    obj.engine.url = "postgresql://offline.invalid/unused"
    obj.source_id = 185
    catalog_engine = MagicMock()
    conn = catalog_engine.begin.return_value.__enter__.return_value
    # Baseline uses the shared engine; repaired code must use bounded connection setup.
    obj.engine.begin.return_value.__enter__.return_value = conn
    factory = MagicMock(return_value=catalog_engine)
    monkeypatch.setattr(options, "create_engine", factory, raising=False)
    clock = [100.0]
    monkeypatch.setattr(options.time, "monotonic", lambda: clock[0])
    return obj, catalog_engine, conn, factory, clock


def test_catalog_cancelled_before_connection_does_not_begin(monkeypatch):
    obj, engine, conn, factory, _clock = _catalog_puller(monkeypatch)
    assert obj._mark_catalog_pulled(should_continue=lambda: False) is False
    factory.assert_not_called()
    engine.begin.assert_not_called()
    conn.execute.assert_not_called()


def test_catalog_complete_transaction_commits_once_and_disposes(monkeypatch):
    obj, engine, conn, factory, _clock = _catalog_puller(monkeypatch)
    assert obj._mark_catalog_pulled(should_continue=lambda: True) is True
    factory.assert_called_once()
    assert factory.call_args.args[0] == obj.engine.url
    obj.engine.begin.assert_not_called()
    updates = [call for call in conn.execute.call_args_list
               if str(call.args[0]).startswith("UPDATE source_catalog")]
    assert len(updates) == 1
    assert updates[0].args[1] == {"sid": 185}
    assert engine.begin.return_value.__exit__.call_args.args == (None, None, None)
    engine.dispose.assert_called_once_with()
    # The cooperative local deadline and configured statement limit are sized
    # below the options cleanup margin (900s hard / 840s cooperative). This
    # configuration check does not bound an in-progress durable COMMIT/WAL wait.
    assert options.CATALOG_PUBLICATION_SECONDS + 5 < 60


@pytest.mark.parametrize("cancel_at", ["connection", "limits", "update"])
def test_catalog_cancellation_rolls_back_at_each_publication_boundary(monkeypatch, cancel_at):
    obj, engine, conn, _factory, _clock = _catalog_puller(monkeypatch)
    live = [True]
    if cancel_at == "connection":
        engine.begin.return_value.__enter__.side_effect = lambda: (live.__setitem__(0, False), conn)[1]

    def execute(statement, *_args):
        sql = str(statement)
        if cancel_at == "limits" and "idle_in_transaction_session_timeout" in sql:
            live[0] = False
        if cancel_at == "update" and sql.startswith("UPDATE source_catalog"):
            live[0] = False
        return MagicMock(rowcount=1)

    conn.execute.side_effect = execute
    assert obj._mark_catalog_pulled(should_continue=lambda: live[0]) is False
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert sum(sql.startswith("UPDATE") for sql in statements) == (cancel_at == "update")
    # Engine.begin's context receives the cancellation exception, so it rolls back.
    assert engine.begin.return_value.__exit__.call_args.args[0] is options._OptionsBudgetExpired
    engine.dispose.assert_called_once_with()


def test_catalog_lock_timeout_is_bounded_and_not_success(monkeypatch):
    obj, engine, conn, factory, clock = _catalog_puller(monkeypatch)

    def execute(statement, *_args):
        if str(statement).startswith("UPDATE source_catalog"):
            clock[0] += 3  # simulated PostgreSQL lock_timeout, no real waiting/DB
            raise TimeoutError("offline simulated catalog lock timeout")
        return MagicMock(rowcount=1)

    conn.execute.side_effect = execute
    assert obj._mark_catalog_pulled() is False
    kwargs = factory.call_args.kwargs
    assert kwargs["connect_args"]["connect_timeout"] == 5
    assert kwargs["poolclass"].__name__ == "NullPool"
    assert "statement_timeout=5000" in kwargs["connect_args"]["options"]
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert "SET LOCAL lock_timeout = '3s'" in statements
    assert "SET LOCAL statement_timeout = '5s'" in statements
    assert "SET LOCAL idle_in_transaction_session_timeout = '5s'" in statements
    assert engine.begin.return_value.__exit__.call_args.args[0] is TimeoutError
    engine.dispose.assert_called_once_with()


def test_catalog_own_transaction_deadline_blocks_update(monkeypatch):
    obj, engine, conn, _factory, clock = _catalog_puller(monkeypatch)

    def execute(statement, *_args):
        if "idle_in_transaction_session_timeout" in str(statement):
            clock[0] += 16
        return MagicMock(rowcount=1)

    conn.execute.side_effect = execute
    assert obj._mark_catalog_pulled() is False
    assert not any(str(call.args[0]).startswith("UPDATE") for call in conn.execute.call_args_list)
    assert engine.begin.return_value.__exit__.call_args.args[0] is options._OptionsBudgetExpired


def test_failed_catalog_publication_preserves_writes_without_fresh_success(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS"})
    obj._mark_catalog_pulled.return_value = False
    result = obj.pull_all()
    assert result.summary["status"] == "PARTIAL"
    assert result.summary["rows_inserted"] == 12


def test_options_forwards_same_cooperative_callback_to_catalog(monkeypatch):
    obj = _puller(monkeypatch, {"SPY": "SUCCESS"})

    def deadline():
        return True

    assert obj.pull_all(should_continue=deadline).summary["status"] == "SUCCESS"
    obj._mark_catalog_pulled.assert_called_once_with(should_continue=deadline)


def test_options_success_does_not_repeat_unbounded_scheduler_catalog_write(monkeypatch):
    import threading

    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched.engine = MagicMock()
    sched._thread_semaphore = threading.Semaphore(1)
    sched._threads_lock = threading.Lock()
    sched._active_threads = set()
    sched._orphan_thread_count = 0
    sched._update_last_pull = MagicMock()
    runner = MagicMock()
    runner.pull.return_value = {"status": "SUCCESS", "rows_inserted": 12}
    monkeypatch.setattr(sched, "_build_puller_instance", lambda *_args: runner)
    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "options")
    assert sched._run_puller(entry)["status"] == "SUCCESS"
    sched._update_last_pull.assert_not_called()


_SESSION_NOW = datetime(2026, 11, 10, 15, tzinfo=timezone.utc)


def _actual_oracle_dispatch(monkeypatch, seconds_left, *, last=None, retry=True, dry_run=False):
    """Run the production run_cycle call site and hourly admission, with no external work."""
    cycle = ast.parse(inspect.getsource(ho.run_cycle))
    calls = [node for node in ast.walk(cycle) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "_run_oracle_forward_step"]
    assert len(calls) == 1

    class SessionClock(datetime):
        @classmethod
        def now(cls, tz):
            return _SESSION_NOW.astimezone(tz)

    monkeypatch.setattr(ho, "datetime", SessionClock)
    monkeypatch.setattr(ho, "time", types.SimpleNamespace(monotonic=lambda: 1000.0))
    engine = MagicMock()
    state = types.SimpleNamespace(last_oracle_cycle=last, current_step="previous", cooldowns=MagicMock())
    state.cooldowns.can_retry.return_value = retry
    report_module = types.ModuleType("oracle.report")
    report_module.send_oracle_report = MagicMock()
    monkeypatch.setitem(sys.modules, "oracle.report", report_module)
    worker = MagicMock(return_value={"status": "completed", "new_predictions": 1})
    monkeypatch.setattr(ho, "_run_oracle_forward_pass", worker)
    dispatch = MagicMock(side_effect=lambda name, work, timeout, state: (work(), True))
    monkeypatch.setattr(ho, "_run_with_timeout", dispatch)
    result = {}
    namespace = dict(vars(ho), state=state, engine=engine, dry_run=dry_run,
                     cycle_deadline=1000.0 + seconds_left, cycle_result=result)
    eval(compile(ast.Expression(calls[0]), "actual_oracle_dispatch", "eval"), namespace)
    return state, result["oracle"], dispatch, worker


@pytest.mark.parametrize("seconds_left", [-1, 0, 1, 300, 599])
def test_actual_oracle_never_starts_without_full_budget_and_cleanup(monkeypatch, seconds_left):
    state, result, dispatch, worker = _actual_oracle_dispatch(monkeypatch, seconds_left)
    dispatch.assert_not_called()
    worker.assert_not_called()
    assert state.last_oracle_cycle is None
    assert state.current_step == "previous"
    state.cooldowns.record_attempt.assert_not_called()
    assert result == {"status": "deferred", "reason": "insufficient_cycle_budget",
                      "budget_left_s": seconds_left, "required_s": 600}


@pytest.mark.parametrize("seconds_left", [600, 601, 4500])
def test_actual_oracle_runs_when_forward_budget_and_cleanup_fit(monkeypatch, seconds_left):
    state, result, dispatch, worker = _actual_oracle_dispatch(
        monkeypatch, seconds_left, last=_SESSION_NOW - timedelta(hours=1),
    )
    assert ho.CYCLE_TIMEOUT_SECONDS == 4500
    assert dispatch.call_count == 1
    assert dispatch.call_args.args[0] == "oracle_cycle"
    assert dispatch.call_args.args[2:] == (300, state)
    worker.assert_called_once()
    assert state.last_oracle_cycle == _SESSION_NOW
    state.cooldowns.record_attempt.assert_called_once_with("oracle_cycle", success=True)
    assert result == {"status": "completed", "new_predictions": 1}


@pytest.mark.parametrize(("last", "retry", "reason"), [
    (_SESSION_NOW - timedelta(seconds=3599), True, "hourly_cadence"),
    (None, False, "timeout_or_failure_cooldown"),
])
def test_oracle_due_and_blacklist_rules_survive_budget_gate(monkeypatch, last, retry, reason):
    state, result, dispatch, worker = _actual_oracle_dispatch(monkeypatch, 4500, last=last, retry=retry)
    dispatch.assert_not_called()
    worker.assert_not_called()
    state.cooldowns.record_attempt.assert_not_called()
    assert state.current_step == "previous"
    assert state.last_oracle_cycle == last
    assert result == {"status": "deferred", "reason": reason}


@pytest.mark.parametrize(("seconds_left", "reason"), [(599, "insufficient_cycle_budget"), (600, "dry_run")])
def test_dry_run_does_not_advance_marker(monkeypatch, seconds_left, reason):
    state, result, dispatch, worker = _actual_oracle_dispatch(monkeypatch, seconds_left, dry_run=True)
    dispatch.assert_not_called()
    worker.assert_not_called()
    state.cooldowns.record_attempt.assert_not_called()
    assert state.current_step == "previous"
    assert state.last_oracle_cycle is None
    assert result["reason"] == reason
