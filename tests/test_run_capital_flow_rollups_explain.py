"""Unit test for ``scripts/run_capital_flow_rollups.py --explain-ttm``.

(fable-daily-intel-sql-tasks-20260920, PR #587 follow-up.)

``--explain-ttm`` is a read-only pre-deployment plan check intended to run
against PRODUCTION, so this test never touches a real database: it swaps
in a fake engine/connection that just records what was executed, and
asserts (a) exactly one statement was issued, (b) that statement's text
starts with ``EXPLAIN`` (not ``EXPLAIN ANALYZE`` — plans only, never
executes), and (c) none of the actual task functions (``compute_ttm``,
``fold_announcements``, ``run_all``) were called.
"""

from __future__ import annotations

import sys

import pytest

import scripts.run_capital_flow_rollups as runner


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeConnection:
    def __init__(self, executed: list):
        self._executed = executed

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, stmt, params=None):
        self._executed.append((stmt, params))
        return _FakeResult([("Fake Plan Line 1",), ("Fake Plan Line 2",)])


class _FakeEngine:
    def __init__(self):
        self.executed: list = []

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self.executed)


def _forbidden(*_args, **_kwargs):
    raise AssertionError(
        "--explain-ttm must not run any actual rollup task, but a task "
        "function was called"
    )


def test_explain_ttm_issues_one_explain_statement_and_no_task_run(monkeypatch):
    fake_engine = _FakeEngine()

    monkeypatch.setattr(runner, "get_engine", lambda: fake_engine)
    monkeypatch.setattr(runner, "compute_ttm", _forbidden)
    monkeypatch.setattr(runner, "fold_announcements", _forbidden)
    monkeypatch.setattr(runner, "run_all", _forbidden)
    monkeypatch.setattr(sys, "argv", ["run_capital_flow_rollups.py", "--explain-ttm"])

    exit_code = runner.main()

    assert exit_code == 0
    assert len(fake_engine.executed) == 1, (
        f"expected exactly one statement, got {len(fake_engine.executed)}: "
        f"{fake_engine.executed!r}"
    )
    stmt, params = fake_engine.executed[0]
    sql_text = str(stmt)
    assert sql_text.strip().startswith("EXPLAIN "), (
        f"--explain-ttm statement must begin with EXPLAIN (plain, no "
        f"ANALYZE — plans only, never executes), got: {sql_text[:80]!r}"
    )
    assert "EXPLAIN (ANALYZE" not in sql_text.upper()
    assert params == {
        "window": runner.TTM_WINDOW_QUARTERS,
        "source_filing": runner.TTM_SOURCE_FILING,
        "confidence": runner.TTM_CONFIDENCE,
    }


def test_explain_ttm_sql_matches_compute_ttm_source(monkeypatch):
    """The EXPLAINed text is exactly ``ttm_statement_sql()`` — same
    source as what ``compute_ttm`` executes, not a hand-copied string."""
    fake_engine = _FakeEngine()
    monkeypatch.setattr(runner, "get_engine", lambda: fake_engine)
    monkeypatch.setattr(sys, "argv", ["run_capital_flow_rollups.py", "--explain-ttm"])

    runner.main()

    stmt, _params = fake_engine.executed[0]
    assert str(stmt) == f"EXPLAIN {runner.ttm_statement_sql()}"
