"""M1 honesty: a NULL the database holds is reported as null, never as 0.

#959 sweep rows 13c and 17b. `/regime/all-active` turned a NULL confidence or
transition probability into 0.0, and the Dad workbook evidence rows turned a
NULL `context_score` into 0.0, so the PWA drew "0%" and "0.0" for values nobody
computed. A real 0.0 must still come through as 0.0.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from api.routers import dad
from api.routers import regime as regime_router

AS_OF = datetime(2026, 11, 24, 21, 0, tzinfo=timezone.utc)


def _regime_conn(rows):
    conn = MagicMock()
    first = MagicMock()
    first.fetchall.return_value = rows
    # Every later query (top movers etc.) fails, which that route already tolerates.
    conn.execute.side_effect = [first] + [RuntimeError("no more queries in this test")] * 20
    engine = MagicMock()

    @contextmanager
    def _connect():
        yield conn

    engine.connect = _connect
    return engine


def _all_active(monkeypatch, rows):
    monkeypatch.setattr(regime_router, "get_db_engine", lambda: _regime_conn(rows))
    return regime_router.get_all_active(_token="t")


def test_all_active_reports_null_confidence_as_null(monkeypatch):
    rows = [
        ("GROWTH", None, None, {}, "HOLD", AS_OF),
        ("FRAGILE", 0.4, 0.1, {}, "DEFENSIVE", AS_OF),
    ]
    out = _all_active(monkeypatch, rows)
    by_state = {r["state"]: r for r in out["macro"]}
    assert by_state["GROWTH"]["confidence"] is None
    assert by_state["GROWTH"]["transition_probability"] is None
    assert by_state["FRAGILE"]["confidence"] == 0.4
    # A state with no confidence sorts after one that has it, and does not raise.
    assert [r["state"] for r in out["macro"]] == ["FRAGILE", "GROWTH"]


def test_all_active_keeps_a_real_zero(monkeypatch):
    out = _all_active(monkeypatch, [("NEUTRAL", 0.0, 0.0, {}, "HOLD", AS_OF)])
    entry = out["macro"][0]
    assert entry["confidence"] == 0.0
    assert entry["transition_probability"] == 0.0


class _Result:
    def __init__(self, one=None, many=None):
        self._one, self._many = one, many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _FakeDuck:
    def __init__(self, score):
        self.score = score

    def execute(self, sql, params=None):
        if "FROM ticker_summary" in sql:
            return _Result(one=("AAA", 3, 1, 1, 2.0, "xlsx"))
        if "FROM ticker_best_evidence" in sql:
            return _Result(many=[("sheet", "AAA cell", self.score, "f.xlsx", "S1", "B2", "row", "hdr")])
        return _Result(many=[])

    def close(self):
        pass


def _workbook(monkeypatch, tmp_path, score):
    db = tmp_path / "research.duckdb"
    db.write_bytes(b"")
    monkeypatch.setattr(dad, "_research_db_path", lambda: db)
    monkeypatch.setattr(dad, "_connect_duckdb", lambda path: _FakeDuck(score))
    return dad._load_workbook_context("AAA")


def test_workbook_evidence_reports_null_context_score_as_null(monkeypatch, tmp_path):
    out = _workbook(monkeypatch, tmp_path, None)
    assert out["workbook"]["evidence"][0]["context_score"] is None


def test_workbook_evidence_keeps_a_real_zero_context_score(monkeypatch, tmp_path):
    out = _workbook(monkeypatch, tmp_path, 0)
    assert out["workbook"]["evidence"][0]["context_score"] == 0.0
