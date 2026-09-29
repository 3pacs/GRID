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


def _read(reader, source, state, engine):
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
    namespace = dict(vars(ho), health={"db": {"stale_sources": [{"source": source}]}},
                     dry_run=False, state=state, engine=engine, cycle_result={}, _retry_source=hf._retry_source)
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
