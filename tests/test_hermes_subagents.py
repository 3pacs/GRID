from __future__ import annotations

from types import SimpleNamespace

from scripts import goal_worker


def test_goal_worker_installs_hermes_subagent_handlers() -> None:
    assert "hermes_diagnose_source" in goal_worker.HANDLERS
    assert "hermes_scout_free_data" in goal_worker.HANDLERS
    assert "hermes_wiring_audit" in goal_worker.HANDLERS
    assert "hermes_storage_maintenance" in goal_worker.HANDLERS


def test_hermes_source_doctor_handler_uses_source_inspector(monkeypatch) -> None:
    calls: list[tuple[object, str]] = []

    def fake_inspect_source(engine, source):
        calls.append((engine, source))
        return {"status": "ok", "source": source}

    from scripts import hermes_fixers

    monkeypatch.setattr(hermes_fixers, "_inspect_source", fake_inspect_source)

    engine = object()
    goal = SimpleNamespace(goal_type="hermes_diagnose_source", target_id="Baltic_Exchange")
    outcome, payload, duration_ms = goal_worker.execute_goal(engine, goal, node_id="test-node")

    assert outcome == "done"
    assert payload == {"status": "ok", "source": "Baltic_Exchange"}
    assert duration_ms >= 0
    assert calls == [(engine, "Baltic_Exchange")]


def test_hermes_free_data_scout_handler_uses_scout(monkeypatch) -> None:
    calls: list[tuple[object, str, int]] = []

    def fake_scout(engine, source, state):
        calls.append((engine, source, state.cycle_count))
        return {"status": "ok", "source": source, "candidate_count": 2}

    from scripts import hermes_fixers

    monkeypatch.setattr(hermes_fixers, "_scout_free_data_sources", fake_scout)

    engine = object()
    goal = SimpleNamespace(
        goal_type="hermes_scout_free_data",
        target_id="Tiingo",
        payload={"requested_cycle": 12},
    )
    outcome, payload, duration_ms = goal_worker.execute_goal(engine, goal, node_id="test-node")

    assert outcome == "done"
    assert payload == {"status": "ok", "source": "Tiingo", "candidate_count": 2}
    assert duration_ms >= 0
    assert calls == [(engine, "Tiingo", 12)]


def test_hermes_storage_maintenance_handler_uses_storage_inspector(monkeypatch) -> None:
    calls: list[tuple[object, str, int]] = []

    def fake_inspect_storage(engine, target_id, state):
        calls.append((engine, target_id, state.cycle_count))
        return {"status": "ok", "target_id": target_id, "cleanup_candidates": 3}

    from scripts import hermes_fixers

    monkeypatch.setattr(hermes_fixers, "_inspect_storage_maintenance", fake_inspect_storage)

    engine = object()
    goal = SimpleNamespace(
        goal_type="hermes_storage_maintenance",
        target_id="grid-svr-data",
        payload={"requested_cycle": 44},
    )
    outcome, payload, duration_ms = goal_worker.execute_goal(engine, goal, node_id="test-node")

    assert outcome == "done"
    assert payload == {"status": "ok", "target_id": "grid-svr-data", "cleanup_candidates": 3}
    assert duration_ms >= 0
    assert calls == [(engine, "grid-svr-data", 44)]


def test_hermes_operator_daily_dispatches_storage_maintainer(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_execute(cmd, *, engine, health, state):
        calls.append({
            "cmd": cmd,
            "engine": engine,
            "health": health,
            "cycle": state.cycle_count,
        })
        return {"status": "queued", "goal_id": 321}

    from scripts import hermes_operator

    monkeypatch.setattr(hermes_operator, "_execute_hermes_repair_command", fake_execute)

    engine = object()
    state = SimpleNamespace(cycle_count=55)
    result = hermes_operator._dispatch_daily_storage_maintenance(engine, state)

    assert result == {"status": "queued", "goal_id": 321}
    assert calls == [{
        "cmd": "DISPATCH_SUBAGENT:storage_maintainer:grid-svr-data:130",
        "engine": engine,
        "health": {},
        "cycle": 55,
    }]
