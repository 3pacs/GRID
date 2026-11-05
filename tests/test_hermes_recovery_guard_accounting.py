"""Actual retry guards and recovery readers must preserve attempt ownership."""
import ast
import inspect
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts import hermes_fixers as hf, hermes_operator as ho


def _setup(monkeypatch):
    source = "guard_accounting"
    state = SimpleNamespace(cooldowns=MagicMock(), current_step="prior",
                            repair_backlog={}, repair_last_check={}, repair_uncovered={})
    state.cooldowns.can_retry.return_value = True
    active = {}
    monkeypatch.setattr(hf, "_REPAIRS_IN_FLIGHT", active)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
        ("GUARD:A", 2, None, None, source), ("GUARD:B", 2, None, None, source)]
    return source, state, engine, active


def _read(reader, source, state, engine, *, stale_sources=None, before_stale=None):
    if reader == "gap":
        result = hf.fill_data_gaps(engine, state)
        assert result["gaps_found"] == 2
        assert result["sources_repulled"] == ([source] if result["gaps_filled"] else [])
        return result["gaps_filled"]
    # Execute the complete, unmodified stale-source block from the real cycle.
    nodes = ast.parse(inspect.getsource(ho.run_cycle)).body[0].body
    start = next(i for i, node in enumerate(nodes) if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id == "stale_sources"
                         for target in node.targets))
    # Include the real run_cycle prologue: the stale block now consumes this
    # existing local budget. Do not invent a large/sentinel deadline.
    context = [
        node for node in nodes if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id in {"cycle_start", "cycle_deadline"}
                for target in node.targets)
    ]
    assert [node.targets[0].id for node in context] == ["cycle_start", "cycle_deadline"]
    namespace = dict(vars(ho), health={"db": {"stale_sources": (
                         [{"source": source}] if stale_sources is None else stale_sources)}},
                     dry_run=False, state=state, engine=engine, cycle_result={}, _retry_source=hf._retry_source)
    exec(compile(ast.Module(body=context, type_ignores=[]),
                 "actual-cycle-deadline-context", "exec"), namespace)
    if before_stale is not None:
        before_stale(namespace)
    exec(compile(ast.Module(body=nodes[start:start + 2], type_ignores=[]),
                 "actual-stale-recovery-block", "exec"), namespace)
    return namespace["cycle_result"]["stale_refreshed"]


@pytest.mark.parametrize("reader", ["gap", "stale"])
@pytest.mark.parametrize("mode", ["in_flight", "superseded"])
def test_real_guard_does_not_change_cooldowns_or_claim_recovery(monkeypatch, reader, mode):
    source, state, engine, active = _setup(monkeypatch)
    owner = {"started": hf.time.monotonic(), "token": -101, "thread": threading.get_ident()}
    if mode == "in_flight":
        active[source] = owner
        monkeypatch.setattr(hf, "_resolve_puller", lambda *_a: pytest.fail("live retry constructed a puller"))
    else:
        class Superseded:
            SOURCE_NAME = source
            def pull(self):
                active[source] = owner
                return {"status": "SUCCESS", "rows_inserted": 6}
        monkeypatch.setattr(hf, "_resolve_puller", lambda *_a: (Superseded(), "pull", {}))

    assert _read(reader, source, state, engine) == 0
    state.cooldowns.record_attempt.assert_not_called()
    assert active[source] is owner  # Neither caller may clear the live owner's entry.
    engine.begin.assert_not_called()  # The actual guard also withholds catalogue publication.


@pytest.mark.parametrize("reader", ["gap", "stale"])
@pytest.mark.parametrize("outcome, rows, recovered, success", [
    ("SUCCESS", 6, True, True),
    ("SUCCESS", 0, False, True),
    ("SUCCESS", None, False, False),
    ("PARTIAL", None, False, False),
    ("FAILED", 0, False, False),
])
def test_completed_retry_keeps_true_recovery_cadence_and_failure_accounting(
        monkeypatch, reader, outcome, rows, recovered, success):
    source, state, engine, active = _setup(monkeypatch)

    class Completed:
        SOURCE_NAME = source
        def pull(self):
            return {"status": outcome, "rows_inserted": rows}
    monkeypatch.setattr(hf, "_resolve_puller", lambda *_a: (Completed(), "pull", {}))

    assert _read(reader, source, state, engine) == ((2 if reader == "gap" else 1) if recovered else 0)
    state.cooldowns.record_attempt.assert_called_once()
    recorded = state.cooldowns.record_attempt.call_args
    assert recorded.args == (source,)
    assert recorded.kwargs["success"] is success
    assert ("error" in recorded.kwargs) is (not success)
    assert source not in active
    assert engine.begin.call_count == int(recovered)


def test_stale_reader_admits_nothing_at_original_cycle_deadline(monkeypatch):
    source, state, engine, _active = _setup(monkeypatch)
    clock = [100.0]
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    retry = MagicMock(side_effect=AssertionError("retry admitted after original deadline"))
    monkeypatch.setattr(hf, "_retry_source", retry)
    def consume_budget(namespace):
        assert namespace["cycle_start"] == 100.0
        assert namespace["cycle_deadline"] == 100.0 + ho.CYCLE_TIMEOUT_SECONDS
        clock[0] = namespace["cycle_deadline"]
    assert _read("stale", source, state, engine, before_stale=consume_budget) == 0
    retry.assert_not_called()
    state.cooldowns.record_attempt.assert_not_called()
    engine.begin.assert_not_called()


def test_stale_reader_forwards_original_deadline_and_withholds_late_recovery(monkeypatch):
    source, state, engine, active = _setup(monkeypatch)
    clock = [100.0]
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    state.repair_backlog[source] = ["SYNTHETIC_PENDING"]
    callbacks = []
    class Late:
        SOURCE_NAME = source
        def pull(self, should_continue):
            callbacks.append(should_continue())
            clock[0] = 100.0 + ho.CYCLE_TIMEOUT_SECONDS
            callbacks.append(should_continue())
            return {"status": "SUCCESS", "rows_inserted": 6}
    resolve = MagicMock(return_value=(Late(), "pull", {}))
    monkeypatch.setattr(hf, "_resolve_puller", resolve)
    def elapsed_before_block(namespace):
        # The stale block must retain the cycle's origin, not reset its budget.
        clock[0] += 50.0
        assert namespace["cycle_deadline"] == 100.0 + ho.CYCLE_TIMEOUT_SECONDS
    assert _read("stale", source, state, engine,
                 stale_sources=[{"source": source}, {"source": "later_source"}],
                 before_stale=elapsed_before_block) == 0
    assert callbacks == [True, False]
    resolve.assert_called_once()
    state.cooldowns.record_attempt.assert_not_called()
    assert state.repair_backlog[source] == ["SYNTHETIC_PENDING"]
    assert source not in active
    engine.begin.assert_not_called()


def test_stale_reader_keeps_fifteen_invocation_limit_for_nonrecovery(monkeypatch):
    source, state, engine, _active = _setup(monkeypatch)
    retry = MagicMock(return_value={"status": "skipped", "reason": "in_flight"})
    monkeypatch.setattr(hf, "_retry_source", retry)
    sources = [{"source": f"{source}_{index}"} for index in range(20)]
    assert _read("stale", source, state, engine, stale_sources=sources) == 0
    assert retry.call_count == 15
    assert [call.args[0] for call in retry.call_args_list] == [row["source"] for row in sources[:15]]
    assert all(call.kwargs["attempt"] == 1 and callable(call.kwargs["should_continue"])
               for call in retry.call_args_list)
    state.cooldowns.record_attempt.assert_not_called()
    engine.begin.assert_not_called()


def test_stale_reader_cooling_prefix_does_not_consume_admitted_slots(monkeypatch):
    source, state, engine, _active = _setup(monkeypatch)
    state.cooldowns.can_retry.side_effect = lambda name: name == source
    retry = MagicMock(return_value={"status": "skipped", "reason": "in_flight"})
    monkeypatch.setattr(hf, "_retry_source", retry)
    sources = [{"source": f"cooling_{index}"} for index in range(15)] + [{"source": source}]
    assert _read("stale", source, state, engine, stale_sources=sources) == 0
    retry.assert_called_once()
    assert retry.call_args.args[0] == source
    state.cooldowns.record_attempt.assert_not_called()
    engine.begin.assert_not_called()
