"""GRID W4b — scripts/research_status.py::latest_research_run().

This is the read-only status surface W4c wires into an API/UI. It reads the
same analytical_snapshots rows scripts/autoresearch.py writes (category=
"research_run", subcategory="autoresearch") — no new table.

Uses a real SQLite-backed... no: analytical_snapshots is Postgres/JSONB-
specific (payload column is JSONB), so this test uses a fake SQLAlchemy
engine/connection instead of a real database, per this task's hard
boundary (no local Postgres). Run with:

    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_research_status.py -q
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.research_status import latest_research_run  # noqa: E402


class _FakeResult:
    def __init__(self, row: tuple | None):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConnection:
    def __init__(self, row: tuple | None, captured_params: dict[str, Any]):
        self._row = row
        self._captured_params = captured_params

    def execute(self, stmt, params=None):
        self._captured_params.update(params or {})
        return _FakeResult(self._row)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, row: tuple | None):
        self._row = row
        self.captured_params: dict[str, Any] = {}

    def connect(self):
        return _FakeConnection(self._row, self.captured_params)


def test_latest_research_run_returns_none_when_no_rows():
    engine = _FakeEngine(row=None)
    assert latest_research_run(engine) is None


def test_latest_research_run_merges_payload_and_filters_category_subcategory():
    payload = {
        "run_id": "abc-123",
        "status": "running",
        "phase": "iteration",
        "iteration": 2,
        "generation": 4,
    }
    created_at = datetime(2026, 9, 18, 12, 0, 0, tzinfo=timezone.utc)
    engine = _FakeEngine(row=(99, created_at, payload))

    result = latest_research_run(engine)

    assert result["id"] == 99
    assert result["created_at"] == created_at.isoformat()
    assert result["run_id"] == "abc-123"
    assert result["status"] == "running"
    assert result["phase"] == "iteration"
    assert result["iteration"] == 2
    assert result["generation"] == 4

    # Must filter on the exact category/subcategory scripts/autoresearch.py
    # writes — a drift here would silently make W4c's API surface always
    # return None or the wrong subsystem's latest row.
    assert engine.captured_params["cat"] == "research_run"
    assert engine.captured_params["sub"] == "autoresearch"


def test_latest_research_run_is_fail_soft_on_engine_error():
    class _RaisingEngine:
        def connect(self):
            raise RuntimeError("database unreachable (simulated)")

    assert latest_research_run(_RaisingEngine()) is None
