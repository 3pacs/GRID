"""Offline behavior regressions for the two independently reviewed #735 defects."""

import ast
import inspect
import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

import ingestion.options as options
import ingestion.smart_scheduler as ss
from scripts import hermes_operator as ho
from tests.test_options_scheduler_budget import _puller
from tests.options_publication_protocol import reviewed_function_rows


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
    conn = catalog_engine.connect.return_value
    conn.info = {}
    conn.execute.return_value.scalar_one.return_value = 0
    conn.execute.return_value.all.return_value = reviewed_function_rows()
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
    conn.begin.return_value.commit.assert_called_once_with()
    conn.begin.return_value.rollback.assert_not_called()
    conn.close.assert_called_once_with()
    assert obj._catalog_receipt.commit_ack == "ACKNOWLEDGED"
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
        engine.connect.side_effect = lambda: (live.__setitem__(0, False), conn)[1]

    def execute(statement, *_args):
        sql = str(statement)
        if cancel_at == "limits" and "idle_in_transaction_session_timeout" in sql:
            live[0] = False
        if cancel_at == "update" and sql.startswith("UPDATE source_catalog"):
            live[0] = False
        result = MagicMock(rowcount=1)
        result.scalar_one.return_value = 0
        result.all.return_value = reviewed_function_rows()
        return result

    conn.execute.side_effect = execute
    assert obj._mark_catalog_pulled(should_continue=lambda: live[0]) is False
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert sum(sql.startswith("UPDATE") for sql in statements) == (cancel_at == "update")
    conn.begin.return_value.rollback.assert_called_once_with()
    conn.begin.return_value.commit.assert_not_called()
    assert obj._catalog_receipt.commit_ack == "NOT_COMMITTED"
    assert obj._catalog_receipt.error == "_OptionsBudgetExpired"
    engine.dispose.assert_called_once_with()


def test_catalog_lock_timeout_is_bounded_and_not_success(monkeypatch):
    obj, engine, conn, factory, clock = _catalog_puller(monkeypatch)

    def execute(statement, *_args):
        if str(statement).startswith("UPDATE source_catalog"):
            clock[0] += 3  # simulated PostgreSQL lock_timeout, no real waiting/DB
            raise TimeoutError("offline simulated catalog lock timeout")
        result = MagicMock(rowcount=1)
        result.scalar_one.return_value = 0
        result.all.return_value = reviewed_function_rows()
        return result

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
    conn.begin.return_value.rollback.assert_called_once_with()
    conn.begin.return_value.commit.assert_not_called()
    assert obj._catalog_receipt.error == "TimeoutError"
    engine.dispose.assert_called_once_with()


def test_catalog_own_transaction_deadline_blocks_update(monkeypatch):
    obj, engine, conn, _factory, clock = _catalog_puller(monkeypatch)

    def execute(statement, *_args):
        if "idle_in_transaction_session_timeout" in str(statement):
            clock[0] += 16
        result = MagicMock(rowcount=1)
        result.scalar_one.return_value = 0
        result.all.return_value = reviewed_function_rows()
        return result

    conn.execute.side_effect = execute
    assert obj._mark_catalog_pulled() is False
    assert not any(str(call.args[0]).startswith("UPDATE") for call in conn.execute.call_args_list)
    conn.begin.return_value.rollback.assert_called_once_with()
    conn.begin.return_value.commit.assert_not_called()
    # The publisher now includes lock/closure setup in its 15s deadline and
    # expires before invoking the catalog callback's separate budget check.
    assert obj._catalog_receipt.error == "PublicationBudgetExpired"


def test_catalog_unknown_commit_ack_stops_without_replay(monkeypatch):
    obj, engine, conn, factory, _clock = _catalog_puller(monkeypatch)
    conn.begin.return_value.commit.side_effect = ConnectionError("synthetic lost COMMIT ACK")
    assert obj._mark_catalog_pulled() is False
    assert obj._catalog_receipt.commit_ack == "UNKNOWN"
    conn.begin.return_value.commit.assert_called_once_with()
    conn.begin.return_value.rollback.assert_not_called()
    factory.assert_called_once()
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize("boundary", ["close", "dispose"])
def test_catalog_post_ack_cleanup_preserves_receipt(monkeypatch, boundary):
    obj, engine, conn, _factory, _clock = _catalog_puller(monkeypatch)
    getattr(conn if boundary == "close" else engine, boundary).side_effect = RuntimeError("synthetic cleanup failure")
    acknowledged_return = obj._mark_catalog_pulled()
    assert acknowledged_return is (boundary == "dispose")
    assert obj._catalog_receipt.commit_ack == "ACKNOWLEDGED"
    assert obj._catalog_receipt.cleanup_failed is True
    assert obj._catalog_receipt.stop is True
    conn.begin.return_value.commit.assert_called_once_with()
    conn.begin.return_value.rollback.assert_not_called()


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


def _actual_oracle_dispatch(monkeypatch, elapsed, *, last=None, retry=True, dry_run=False):
    """Execute the real run_cycle oracle block with every external action mocked."""
    tree = ast.parse(inspect.getsource(ho.run_cycle))
    oracle_block = next(
        node for node in tree.body[0].body
        if isinstance(node, ast.Try)
        and any(isinstance(n, ast.Constant) and n.value == "Running Oracle prediction cycle..."
                for n in ast.walk(node))
    )
    state = types.SimpleNamespace(last_oracle_cycle=last, current_step="previous", cooldowns=MagicMock())
    state.cooldowns.can_retry.return_value = retry
    engine = MagicMock()
    oracle_engine = types.ModuleType("oracle.engine")
    oracle_engine.OracleEngine = MagicMock()
    oracle_report = types.ModuleType("oracle.report")
    oracle_report.send_oracle_report = MagicMock()
    monkeypatch.setitem(sys.modules, "oracle.engine", oracle_engine)
    monkeypatch.setitem(sys.modules, "oracle.report", oracle_report)
    dispatch = MagicMock(return_value=({"new_predictions": 1, "scoring": {}}, True))
    monkeypatch.setattr(ho.time, "monotonic", lambda: 1000 + elapsed)
    namespace = dict(vars(ho), state=state, engine=engine, dry_run=dry_run,
                     cycle_start=1000, cycle_deadline=1000 + ho.CYCLE_TIMEOUT_SECONDS,
                     cycle_result={}, _run_with_timeout=dispatch)
    exec(compile(ast.Module(body=[oracle_block], type_ignores=[]), "actual_oracle_dispatch", "exec"), namespace)
    return state, namespace["cycle_result"], dispatch, oracle_engine.OracleEngine


@pytest.mark.parametrize("elapsed", [201, 900, 1260, 4499, 4501])
def test_actual_oracle_never_starts_without_full_budget_and_cleanup(monkeypatch, elapsed):
    state, result, dispatch, constructor = _actual_oracle_dispatch(monkeypatch, elapsed)
    dispatch.assert_not_called()
    constructor.assert_not_called()
    assert state.last_oracle_cycle is None
    assert state.current_step == "previous"
    state.cooldowns.record_attempt.assert_not_called()
    assert result["oracle"]["deferred"] == "insufficient_cycle_budget"


@pytest.mark.parametrize("elapsed", [0, 100, 200])
def test_actual_oracle_runs_with_original_contract_when_budget_fits(monkeypatch, elapsed):
    state, result, dispatch, _constructor = _actual_oracle_dispatch(monkeypatch, elapsed)
    assert dispatch.call_args.args[2] == 4000
    assert ho.CYCLE_TIMEOUT_SECONDS == 4500
    assert state.last_oracle_cycle is not None
    assert result["oracle"]["predictions"] == 1


def test_oracle_due_and_blacklist_rules_survive_budget_gate(monkeypatch):
    recent = datetime.now(timezone.utc)
    for last, retry in [(recent, True), (None, False)]:
        state, result, dispatch, _constructor = _actual_oracle_dispatch(monkeypatch, 0, last=last, retry=retry)
        dispatch.assert_not_called()
        assert state.last_oracle_cycle == last
        assert "oracle" not in result


def test_dry_run_late_oracle_does_not_advance_marker(monkeypatch):
    state, result, dispatch, _constructor = _actual_oracle_dispatch(monkeypatch, 1260, dry_run=True)
    dispatch.assert_not_called()
    assert state.last_oracle_cycle is None
    assert result["oracle"]["deferred"] == "insufficient_cycle_budget"
