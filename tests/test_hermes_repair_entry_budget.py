"""Call-entry deadline/ownership regression with synthetic SQL/LLM/pullers.

The actual diagnostics function and token helpers are compiled from source.
No application module, live connection, provider, admission or pipeline runs.
"""
from __future__ import annotations

import ast
import __future__
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import threading
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest


def _load(source: str | None = None):
    path = Path(__file__).resolve().parents[1] / "scripts/hermes_fixers.py"
    tree = ast.parse(path.read_text(encoding="utf8") if source is None else source)
    names = {"diagnose_and_fix_pulls", "_next_diagnostics_token", "_diagnostics_abandoned",
             "_normalize_source_key", "_classify_pull_failure"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    clock = SimpleNamespace(now=0.0)
    namespace = {"Any": Any, "OperatorState": object, "date": date,
                 "datetime": datetime, "timezone": timezone, "json": json, "re": re,
                 "time": SimpleNamespace(monotonic=lambda: clock.now),
                 "_DIAGNOSTICS_LOCK": threading.Lock(), "_DIAGNOSTICS_TOKEN": 0,
                 "REPAIR_BUDGET_SECONDS": 180, "MAX_PULL_RETRIES": 3,
                 "HERMES_TEMPERATURE": 0.3, "log": MagicMock(),
                 "_relation_exists": lambda *a: True,
                 "_format_pull_action_catalog": lambda: "RETRY",
                 "_parse_pull_diagnosis_actions": lambda response: {},
                 "log_issue": MagicMock(), "resolve_source_issues": MagicMock()}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace, clock


def _exercise(monkeypatch, *, query_seconds=0, diagnosis_seconds=0,
              supersede_in_diagnosis=False, dry_run=False, source=None,
              expire_in_first_pull=False, supersede_in_query=False,
              diagnosis_raises=False):
    namespace, clock = _load(source)
    rows = [(name, 1, datetime(2026, 10, 28, tzinfo=timezone.utc), "synthetic timeout")
            for name in ("Synthetic_A", "Synthetic_B")]
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value

    def execute(*a, **k):
        clock.now += query_seconds
        if supersede_in_query:
            namespace["_next_diagnostics_token"]()
        return SimpleNamespace(fetchall=lambda: rows)

    conn.execute.side_effect = execute
    cooldowns = MagicMock()
    cooldowns.can_retry.return_value = True
    cooldowns.get_status.return_value = None
    state = SimpleNamespace(cooldowns=cooldowns, cycle_count=1)
    client = MagicMock()

    def chat(**kwargs):
        clock.now += diagnosis_seconds
        if diagnosis_raises:
            raise RuntimeError("synthetic diagnosis failure")
        if supersede_in_diagnosis:
            namespace["_next_diagnostics_token"]()
        return "Synthetic_A: RETRY - synthetic"

    client.chat.side_effect = chat
    router = ModuleType("llm.router")
    router.get_llm = MagicMock(return_value=client)
    router.Tier = SimpleNamespace(REASON="synthetic")
    monkeypatch.setitem(__import__("sys").modules, "llm.router", router)
    sql = ModuleType("sqlalchemy")
    sql.text = lambda value: value
    monkeypatch.setitem(__import__("sys").modules, "sqlalchemy", sql)
    pulls = []

    def retry(name, engine, *, attempt, state, should_continue):
        pulls.append((name, should_continue()))
        if expire_in_first_pull:
            clock.now = 181
            pulls.append(("AFTER_EXPIRY_CALLBACK", should_continue()))
        return {"outcome": "NO_NEW_DATA"}

    namespace["_retry_source"] = retry
    result = namespace["diagnose_and_fix_pulls"](
        engine, hermes_available=True, state=state, dry_run=dry_run)
    return result, pulls, client, state, namespace


@pytest.mark.parametrize("query_seconds,diagnosis_seconds", [(181, 0), (0, 241), (120, 61)])
def test_expired_call_never_starts_a_source_repair(monkeypatch, query_seconds, diagnosis_seconds):
    result, pulls, client, state, ns = _exercise(
        monkeypatch, query_seconds=query_seconds, diagnosis_seconds=diagnosis_seconds)
    assert pulls == []
    assert result["skipped_actions"] == ["Synthetic_A", "Synthetic_B"]
    state.cooldowns.record_attempt.assert_not_called()
    ns["log_issue"].assert_not_called()
    ns["resolve_source_issues"].assert_not_called()
    if query_seconds >= 180:
        client.chat.assert_not_called()


@pytest.mark.parametrize("where", ["query", "diagnosis"])
def test_older_slow_call_cannot_reclaim_newer_diagnostics_ownership(monkeypatch, where):
    result, pulls, client, state, ns = _exercise(
        monkeypatch, supersede_in_query=where == "query",
        supersede_in_diagnosis=where == "diagnosis")
    assert pulls == []
    assert result["skipped_actions"] == ["Synthetic_A", "Synthetic_B"]
    state.cooldowns.record_attempt.assert_not_called()
    assert ns["_DIAGNOSTICS_TOKEN"] == 2


def test_timely_query_and_diagnosis_keep_normal_repair_checks(monkeypatch):
    result, pulls, client, state, ns = _exercise(
        monkeypatch, query_seconds=20, diagnosis_seconds=30)
    assert pulls == [("Synthetic_A", True), ("Synthetic_B", True)]
    assert result["retried"] == 2
    assert result["fixed"] == 0  # unchanged checks are not fresh materialization
    assert state.cooldowns.record_attempt.call_count == 2


def test_inflight_callback_and_next_source_use_original_deadline(monkeypatch):
    result, pulls, client, state, ns = _exercise(
        monkeypatch, diagnosis_seconds=30, expire_in_first_pull=True)
    assert pulls == [("Synthetic_A", True), ("AFTER_EXPIRY_CALLBACK", False)]
    assert result["skipped_actions"] == ["Synthetic_B"]


def test_dry_run_does_not_take_diagnostics_ownership_or_dispatch(monkeypatch):
    result, pulls, client, state, ns = _exercise(monkeypatch, dry_run=True)
    assert ns["_DIAGNOSTICS_TOKEN"] == 0
    assert pulls == []
    state.cooldowns.record_attempt.assert_not_called()


def test_failed_slow_diagnosis_does_not_restart_repair_budget(monkeypatch):
    result, pulls, client, state, ns = _exercise(
        monkeypatch, diagnosis_seconds=241, diagnosis_raises=True)
    assert pulls == []
    assert result["skipped_actions"] == ["Synthetic_A", "Synthetic_B"]
    state.cooldowns.record_attempt.assert_not_called()
