"""A DB-level failure in the autoresearch query path must surface as an
explicit, structured run-state outcome — never a bare exception that only
a generic ``except Exception: log.warning(...)`` one level up notices.

Before this slice's fix:
  - ``run_autoresearch`` called ``get_feature_list(cur)`` /
    ``get_feature_name_map(cur)`` / ``get_market_snapshot(cur)`` directly,
    with no try/except around any of them. A DB error there (bad column,
    dropped connection, permissions) propagated as a bare, unlabeled
    exception all the way out of ``run_autoresearch``.
  - That exception was only ever caught one level up, generically, in
    ``scripts/hermes_fixers.py::maybe_run_autoresearch``:
    ``except Exception as exc: log.warning(...); return {"error": str(exc)}``.
  - Separately (see test_autoresearch_schema_contract.py for the full
    writeup), ``run_autoresearch``'s successful-path return dict used the
    key ``"iterations_run"``, but ``maybe_run_autoresearch`` reads
    ``result.get("iterations", 0)`` into ``state.hypotheses_tested`` — a
    key-name mismatch, so ``hypotheses_tested`` stayed at 0 regardless of
    whether the run succeeded or failed. That is the concrete, reproducible
    mechanism behind "hypotheses_tested stays 0 with no visible failure."

After the fix:
  - ``_load_research_context(cur)`` wraps each of those three DB calls and
    raises a typed ``AutoresearchDataError(phase, original)`` naming which
    phase failed.
  - ``run_autoresearch`` catches exactly that typed exception and returns a
    structured result via ``_failed_result(phase, error)``:
    ``{"status": "failed", "phase": ..., "error": ..., "iterations": 0, ...}``.
  - The successful-path return now also sets ``"iterations"`` (in addition
    to the pre-existing ``"iterations_run"``, still read by
    scripts/notify.py), so ``state.hypotheses_tested`` advances correctly.

These tests use a fake connection/cursor that raises on ``execute`` — no
real DB, per the task's hard boundary. Run with:
    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_autoresearch_failure_visibility.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.autoresearch as autoresearch  # noqa: E402


class _RaisingCursor:
    """Cursor stand-in whose .execute() always raises, no real DB involved."""

    def __init__(self, exc: Exception):
        self._exc = exc
        self.calls = 0

    def execute(self, *args: Any, **kwargs: Any) -> None:
        self.calls += 1
        raise self._exc

    def fetchall(self):  # pragma: no cover - never reached, execute raises first
        return []

    def fetchone(self):  # pragma: no cover
        return None

    def close(self) -> None:
        pass


class _FakeConnection:
    """Connection stand-in that hands back a cursor which always raises."""

    def __init__(self, exc: Exception):
        self._exc = exc
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return _RaisingCursor(self._exc)

    def close(self) -> None:
        self.closed = True


class _FakeOllama:
    is_available = True

    def chat(self, *args: Any, **kwargs: Any):  # pragma: no cover - not reached
        raise AssertionError("chat() should not be called: failure happens before it")


def _simulated_db_error() -> Exception:
    """A stand-in for a real driver error (e.g. psycopg2.errors.UndefinedColumn)."""
    return RuntimeError('column "f.does_not_exist" does not exist (simulated)')


# ── Unit-level: _load_research_context wraps a raising cursor ──────────

def test_load_research_context_raises_typed_error_on_db_failure():
    """_load_research_context must not let a raw DB exception pass through
    unlabeled — it must raise AutoresearchDataError naming the phase.
    """
    original = _simulated_db_error()
    cur = _RaisingCursor(original)

    with pytest.raises(autoresearch.AutoresearchDataError) as excinfo:
        autoresearch._load_research_context(cur)

    err = excinfo.value
    assert err.phase == "feature_list"
    assert err.original is original
    # The original exception must be reachable, not swallowed into a string.
    assert isinstance(err.original, RuntimeError)


def test_load_research_context_names_later_phase_when_feature_list_succeeds(monkeypatch):
    """If feature_list succeeds but a later query fails, the phase name
    must reflect *that* query, not just "something failed".
    """
    original = _simulated_db_error()

    monkeypatch.setattr(autoresearch, "get_feature_list", lambda cur: "(ok)")
    monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: {})

    def _raise_market_snapshot(cur):
        raise original

    monkeypatch.setattr(autoresearch, "get_market_snapshot", _raise_market_snapshot)

    with pytest.raises(autoresearch.AutoresearchDataError) as excinfo:
        autoresearch._load_research_context(cur=object())

    assert excinfo.value.phase == "market_snapshot"
    assert excinfo.value.original is original


# ── End-to-end: run_autoresearch turns the failure into a structured result ─

@pytest.fixture
def _isolated_ortho_cache(monkeypatch):
    """_select_orthogonal_features memoizes into a module global; make sure
    an earlier test/run in this process can't hide the simulated failure.
    """
    monkeypatch.setattr(autoresearch, "_ortho_cache", None)


def test_run_autoresearch_returns_structured_failure_on_db_error(monkeypatch, _isolated_ortho_cache):
    """The behavior change this slice makes: a DB failure while loading the
    research context must come back from run_autoresearch() as an explicit
    structured result, not propagate as a bare exception and not silently
    report success.
    """
    original = _simulated_db_error()
    fake_connection = _FakeConnection(original)

    monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
    monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
    monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit: object())
    monkeypatch.setattr(autoresearch, "get_ollama", lambda: _FakeOllama())
    monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda ollama: object())

    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: fake_connection)

    # Must not raise — the point of this fix is that the caller gets a
    # result object back, never an uncaught exception.
    result = autoresearch.run_autoresearch(max_iterations=1)

    assert result["status"] == "failed"
    assert result["phase"] == "feature_list"
    assert "does_not_exist" in result["error"]
    # Both keys populated and both zero — no phantom progress on failure.
    assert result["iterations"] == 0
    assert result["iterations_run"] == 0
    assert result["passed"] is False
    # The connection must be cleaned up even on this early failure path.
    assert fake_connection.closed is True


def test_run_autoresearch_success_path_sets_both_iteration_keys(monkeypatch, _isolated_ortho_cache):
    """Regression guard for the key-mismatch bug: scripts/hermes_fixers.py
    reads result.get("iterations", 0) into state.hypotheses_tested, while
    scripts/notify.py reads result.get("iterations_run", 0). Both must be
    populated on a normal (even trivially empty) successful run.
    """
    monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
    monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
    monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit: object())
    monkeypatch.setattr(autoresearch, "get_ollama", lambda: _FakeOllama())
    monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda ollama: object())
    monkeypatch.setattr(autoresearch, "get_feature_list", lambda cur: "(no features)")
    monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: {})
    monkeypatch.setattr(autoresearch, "get_market_snapshot", lambda cur: "(no data)")

    class _NullCursor:
        def execute(self, *a, **k):
            pass

        def fetchall(self):
            return []

        def fetchone(self):
            return None

        def close(self):
            pass

    class _NullConnection:
        autocommit = False

        def cursor(self):
            return _NullCursor()

        def close(self):
            pass

    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _NullConnection())

    # Ollama "available" but chat() returns None -> the loop records a
    # single "no response" attempt and ends; with max_iterations=1 that
    # means zero real hypotheses were tested, which is exactly the case
    # the original iterations/iterations_run mismatch hid.
    def _fake_chat(self, *a, **k):
        return None

    _FakeOllama.chat = _fake_chat  # type: ignore[assignment]

    result = autoresearch.run_autoresearch(max_iterations=1)

    assert result["status"] == "ok"
    assert result["iterations"] == result["iterations_run"]
