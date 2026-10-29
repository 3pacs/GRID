"""Offline scheduler reporting regression; no handler imports or constructors."""
from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest


def _load_tick(source: str | None = None):
    """Compile the actual tick method and literal policy constants only.

    Importing the full scheduler is unnecessary for this reporting contract.
    No SEC/FINRA admissions, application setup, provider, or database is run.
    """
    path = Path(__file__).resolve().parents[1] / "ingestion/smart_scheduler.py"
    tree = ast.parse(path.read_text(encoding="utf8") if source is None else source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "SmartScheduler")
    tick = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "tick")
    names = {"MAX_PULLERS_PER_TICK", "TICK_TIME_BUDGET_S", "OUTCOME_SUCCESS",
             "OUTCOME_NO_NEW_DATA", "OUTCOME_SKIPPED", "OUTCOME_FAILED", "OUTCOME_PARTIAL"}
    namespace = {"Any": Any, "datetime": datetime, "timezone": timezone, "log": MagicMock()}
    for n in tree.body:
        if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and n.targets[0].id in names:
            namespace[n.targets[0].id] = ast.literal_eval(n.value)
    exec(compile(ast.Module(body=[tick], type_ignores=[]), str(path), "exec"), namespace)
    return namespace


def _exercise(namespace, total: int, run_limit: int):
    due = [{"name": f"synthetic-{i}"} for i in range(total)]
    clock_calls = 0

    def monotonic():
        nonlocal clock_calls
        call = clock_calls
        clock_calls += 1
        return 0.0 if call <= run_limit else namespace["TICK_TIME_BUDGET_S"] + 1.0

    namespace["time"] = SimpleNamespace(monotonic=monotonic)
    scheduler = SimpleNamespace(
        _get_due_pullers=lambda: due,
        _run_puller=MagicMock(return_value={"status": "SUCCESS", "rows_inserted": 1}),
        _record_result=MagicMock(),
        _log_run=MagicMock(),
    )
    result = namespace["tick"](scheduler)
    ran_names = [call.args[0]["name"] for call in scheduler._run_puller.call_args_list]
    return result, ran_names


@pytest.mark.parametrize("total,run_limit", [(77, 5), (12, 0), (12, 3), (12, 8), (4, 8), (0, 0)])
def test_tick_backlog_partitions_every_due_job_after_budget_cutoff(total, run_limit):
    namespace = _load_tick()
    result, ran_names = _exercise(namespace, total, run_limit)
    expected_ran = min(total, run_limit, namespace["MAX_PULLERS_PER_TICK"])
    assert result["ran"] == len(ran_names) == expected_ran
    assert result["still_due"] == [f"synthetic-{i}" for i in range(expected_ran, total)]
    assert result["total_due"] == result["ran"] + len(result["still_due"])
    assert not set(ran_names).intersection(result["still_due"])
