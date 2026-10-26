"""Regression test for the ``grid_cron.sh agents`` job's inline Python.

Root cause (diagnosed 2026-09-26): ``scripts/grid_cron.sh``'s ``agents)``
case ran::

    from agents.runner import run_agent
    result = run_agent()

but ``agents/runner.py`` has never defined a module-level ``run_agent``
function — only the ``AgentRunner`` class with an instance ``.run()``
method (see ``agents/scheduler.py::_run_scheduled_agents`` and
``api/routers/agents.py`` for the correct, already-used pattern). Every
weekday 17:00Z cron invocation of ``grid_cron.sh agents`` therefore raised
``ImportError: cannot import name 'run_agent' from 'agents.runner'`` and
``agent_runs`` stayed empty.

This test extracts the exact inline ``python -c "..."`` snippet for the
``agents)`` case out of the shell script (so it breaks if anyone
reintroduces the bad import) and executes it against stub ``db`` /
``agents.runner`` modules, asserting it constructs ``AgentRunner`` and
calls ``.run()`` — never referencing a bare ``run_agent`` name.
"""

from __future__ import annotations

import re
import sys
import types
from pathlib import Path

GRID_CRON_SH = Path(__file__).resolve().parent.parent / "scripts" / "grid_cron.sh"


def _extract_agents_snippet() -> str:
    """Pull the inline ``python -c "..."`` body out of the ``agents)`` case."""
    text = GRID_CRON_SH.read_text(encoding="utf-8")
    match = re.search(
        r'agents\)\n.*?python -c "\n(.*?)\n"',
        text,
        re.DOTALL,
    )
    assert match, "Could not locate the agents) case's inline python -c snippet"
    return match.group(1)


class _FakeAgentRunner:
    instances: list["_FakeAgentRunner"] = []

    def __init__(self, engine):
        self.engine = engine
        self.run_called = False
        _FakeAgentRunner.instances.append(self)

    def run(self):
        self.run_called = True
        return {"run_id": 1, "final_decision": "HOLD"}


def test_agents_case_does_not_import_bare_run_agent() -> None:
    """The bad import must not be reintroduced."""
    snippet = _extract_agents_snippet()
    assert "import run_agent" not in snippet
    assert "run_agent()" not in snippet


def test_agents_case_uses_agentrunner_pattern(monkeypatch) -> None:
    """Executing the extracted snippet must build AgentRunner and call .run()."""
    snippet = _extract_agents_snippet()
    _FakeAgentRunner.instances.clear()

    fake_engine = object()

    fake_db_module = types.ModuleType("db")
    fake_db_module.get_engine = lambda: fake_engine

    fake_agents_pkg = types.ModuleType("agents")
    fake_runner_module = types.ModuleType("agents.runner")
    fake_runner_module.AgentRunner = _FakeAgentRunner

    monkeypatch.setitem(sys.modules, "db", fake_db_module)
    monkeypatch.setitem(sys.modules, "agents", fake_agents_pkg)
    monkeypatch.setitem(sys.modules, "agents.runner", fake_runner_module)

    exec_globals: dict = {}
    exec(compile(snippet, str(GRID_CRON_SH), "exec"), exec_globals)

    assert len(_FakeAgentRunner.instances) == 1
    instance = _FakeAgentRunner.instances[0]
    assert instance.engine is fake_engine
    assert instance.run_called is True
