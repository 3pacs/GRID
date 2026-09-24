"""Regression: ``scripts/run_full_pipeline.py`` Step 12 ("File Rotation
Cleanup") must only import names that exist, and must not own
market-briefing retention.

Step 12 used to run::

    try:
        from ollama.market_briefing import MarketBriefingGenerator
        cleaned["briefings"] = MarketBriefingGenerator.cleanup_old_briefings(max_age_days=90)
    except Exception as exc:
        log.debug("Briefing cleanup skipped: {e}", e=str(exc))

``ollama/market_briefing.py`` has only ever defined ``MarketBriefingEngine``
(the wrong name dates from the commit that added the block, 1c2c7022), so
every run raised ImportError, logged it at debug, reported the step "OK",
and deleted nothing.

The block is removed rather than renamed. Briefing retention already has a
single owner, ``scripts/hermes_operator.py::_daily_intel_briefing_cleanup``
(same 90-day window, correct class), gated by
``DAILY_INTEL_INITIAL_ALLOWLIST`` and held out of it by the controller until
a file-deletion policy is accepted (2026-09-20). ``run_pipeline`` is reachable
from Hermes' ``RUN_PIPELINE`` repair skill (``scripts/hermes_fixers.py``), so
correcting the name here would have turned a no-op into a second, ungated
deletion path around that hold.

No DB, network or filesystem side effects: every step except Step 12 is
skipped, and both cleanup functions are stubs.
"""
from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path
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


def test_cleanup_step_rotates_insights_and_leaves_briefings_to_hermes(
    rfp: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import db
    import outputs.llm_logger
    from ollama.market_briefing import MarketBriefingEngine

    insight_calls: list[int] = []
    briefing_calls: list[int] = []

    def fake_insight_cleanup(max_age_days: int = 90) -> int:
        insight_calls.append(max_age_days)
        return 4

    def fake_briefing_cleanup(max_age_days: int = 90) -> int:
        briefing_calls.append(max_age_days)
        return 0

    monkeypatch.setattr(db, "get_engine", lambda: object())
    monkeypatch.setattr(outputs.llm_logger, "cleanup_old_insights", fake_insight_cleanup)
    monkeypatch.setattr(
        MarketBriefingEngine, "cleanup_old_briefings", staticmethod(fake_briefing_cleanup),
    )

    ran: list[str] = []

    def only_cleanup_step(label: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        ran.append(label)
        return fn(*args, **kwargs) if label == "File Rotation Cleanup" else None

    monkeypatch.setattr(rfp, "_safe_run", only_cleanup_step)

    summary = rfp.run_pipeline(historical=False)

    assert "File Rotation Cleanup" in ran
    assert summary["steps"]["cleanup"]["insights"] == 4
    assert insight_calls == [90]
    assert "briefings" not in summary["steps"]["cleanup"]
    assert briefing_calls == [], (
        "briefing retention belongs to hermes_operator's allow-list-gated "
        "briefing_cleanup task; run_pipeline must not delete briefings"
    )
