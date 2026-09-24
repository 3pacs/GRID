"""Regression: full-pipeline runs must respect the held retention policy.

Hermes' insight_cleanup and briefing_cleanup tasks are excluded from
DAILY_INTEL_INITIAL_ALLOWLIST pending a deletion policy. RUN_PIPELINE can
invoke this runner, so neither file deletion path may run here.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_REPO = Path(__file__).resolve().parent.parent
_PIPELINE = _REPO / "scripts" / "run_full_pipeline.py"


def _module_file(module: str) -> Path | None:
    """Source file for a first-party module, or None for stdlib/third-party."""
    base = _REPO.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _top_level_names(module_file: Path) -> set[str]:
    """Names a module binds at import time, read from source (never imported)."""
    names: set[str] = set()
    pending = list(ast.parse(module_file.read_text(encoding="utf-8")).body)
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update((a.asname or a.name).split(".")[0] for a in node.names)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(
                n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)
            )
        elif isinstance(node, (ast.If, ast.Try)):
            pending += node.body + node.orelse + getattr(node, "finalbody", [])
            for handler in getattr(node, "handlers", []):
                pending += handler.body
    return names


def test_every_first_party_lazy_import_resolves() -> None:
    """Each step imports lazily inside a broad ``except Exception`` that logs
    at debug, so a wrong name never surfaces at runtime — check them all
    statically instead of importing the heavy step modules."""
    unresolved: list[str] = []
    for node in ast.walk(ast.parse(_PIPELINE.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.ImportFrom) or node.level or not node.module:
            continue
        module_file = _module_file(node.module)
        if module_file is None:
            continue
        defined = _top_level_names(module_file)
        unresolved += [
            f"line {node.lineno}: from {node.module} import {alias.name}"
            for alias in node.names
            if alias.name not in defined
            and _module_file(f"{node.module}.{alias.name}") is None
        ]
    assert unresolved == []


@pytest.fixture
def rfp(monkeypatch: pytest.MonkeyPatch) -> Any:
    # The script chdirs to the repo root at import time; restore cwd after.
    monkeypatch.chdir(Path.cwd())
    import scripts.run_full_pipeline as module

    return module


def test_pipeline_does_not_bypass_held_retention_tasks(
    rfp: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # run_pipeline imports db once before invoking steps. Stub that import so
    # no database settings, connection or other pipeline step is exercised.
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: object()
    monkeypatch.setitem(sys.modules, "db", fake_db)

    ran: list[str] = []

    def skip_step(label: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        ran.append(label)

    monkeypatch.setattr(rfp, "_safe_run", skip_step)
    summary = rfp.run_pipeline(historical=False)

    assert "File Rotation Cleanup" not in ran
    assert "cleanup" not in summary["steps"]

    # A direct call outside _safe_run would bypass the preceding assertion.
    pipeline = next(
        node for node in ast.parse(_PIPELINE.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name == "run_pipeline"
    )
    held_names = {"cleanup_old_insights", "cleanup_old_briefings"}
    assert not any(
        (isinstance(node, ast.Name) and node.id in held_names)
        or (isinstance(node, ast.Attribute) and node.attr in held_names)
        or (isinstance(node, ast.ImportFrom) and any(a.name in held_names for a in node.names))
        for node in ast.walk(pipeline)
    )
