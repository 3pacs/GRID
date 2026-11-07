"""Honest ReasoningBank lessons (2026-11-07).

The rules (intelligence/reasoning_bank.py):

1. Old lessons don't count: only lessons since HONEST_LESSONS_SINCE from the
   verdict scorer or the oracle's contrast distillation move conviction.
2. No two-sided record, no adjustment (covered in test_reasoning_bank.py).
3. One lesson per official verdict, written when it is scored, both sides;
   postmortems explain misses and write no lessons.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from intelligence import postmortem
from intelligence import reasoning_bank as rb
from oracle.engine import OracleEngine

# ── rule 1: retrieval scope ─────────────────────────────────────────────────


def test_retrieve_scopes_by_time_and_writer():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    rb.retrieve_lessons(engine, fingerprint={"regime": "GROWTH"}, top_k=20,
                        since=rb.HONEST_LESSONS_SINCE, source_types=rb.HONEST_LESSON_SOURCES)
    sql, params = str(conn.execute.call_args.args[0]), conn.execute.call_args.args[1]
    assert "created_at >= :since" in sql and "source_type IN (:st_0, :st_1)" in sql
    assert params["since"] == rb.HONEST_LESSONS_SINCE
    assert (params["st_0"], params["st_1"]) == ("oracle_verdict", "oracle_contrast")


def test_retrieve_without_scope_is_unchanged():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    rb.retrieve_lessons(engine, fingerprint={"regime": "GROWTH"})
    sql = str(conn.execute.call_args.args[0])
    assert ":since" not in sql and ":st_0" not in sql


def test_honest_scope_on_sqlite_drops_legacy_rows():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE reasoning_lessons (
                id INTEGER PRIMARY KEY, title TEXT, description TEXT, content TEXT,
                outcome_class TEXT, condition_fingerprint TEXT, source_type TEXT,
                source_id TEXT, embedding TEXT, created_at TIMESTAMP)""")
        rows = [
            ("failure", "postmortem_failure", "2026-09-18 00:00:00"),   # legacy
            ("failure", "oracle_verdict", "2026-11-01 00:00:00"),       # before the cutoff
            ("success", "postmortem_success", "2026-11-08 00:00:00"),   # wrong writer
            ("success", "oracle_verdict", "2026-11-08 00:00:00"),       # counts
        ]
        for i, (oc, st, ts) in enumerate(rows):
            conn.exec_driver_sql(
                "INSERT INTO reasoning_lessons VALUES (?, 't', 'd', 'c', ?, '{}', ?, ?, NULL, ?)",
                (i, oc, st, str(i), ts))
    out = rb.retrieve_lessons(engine, fingerprint={}, top_k=10,
                              since=datetime(2026, 11, 7, tzinfo=timezone.utc),
                              source_types=rb.HONEST_LESSON_SOURCES)
    assert [(l.outcome_class, l.source_type) for l in out] == [("success", "oracle_verdict")]


# ── rule 3: one lesson per official verdict, after the commit ────────────────


class _Begin:
    def __init__(self, conn, events):
        self.conn, self.events = conn, events

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc: object):
        self.events.append("commit")
        return False


def _scoring_engine(rows, events):
    conn = MagicMock()
    first = MagicMock()
    first.fetchall.return_value = rows
    conn.execute.side_effect = [first] + [MagicMock() for _ in range(20)]
    engine = MagicMock()
    engine.begin.side_effect = lambda: _Begin(conn, events)
    return engine


def test_scorer_writes_one_lesson_per_verdict_after_commit(monkeypatch):
    created = datetime(2026, 11, 1, 15, tzinfo=timezone.utc)
    expiry = date(2026, 11, 8)
    rows = [
        # hit: CALL, price 100 -> 110, expected 5%
        ("p-hit", "NVDA", "CALL", 120.0, 100.0, expiry, 0.6, 5.0, "m1", "oracle_pred_nullable_0918", created),
        # miss: PUT, price went up
        ("p-miss", "AAPL", "PUT", 90.0, 100.0, expiry, 0.6, 5.0, "m2", "oracle_pred_nullable_0918", created),
        # partial: CALL, +1% against an expected 5%
        ("p-part", "MSFT", "CALL", 110.0, 100.0, expiry, 0.6, 5.0, "m3", "oracle_pred_nullable_0918", created),
    ]
    prices = {"NVDA": 110.0, "AAPL": 104.0, "MSFT": 101.0}
    events: list[str] = []
    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = _scoring_engine(rows, events)
    monkeypatch.setattr(eng, "_load_split_book", lambda tickers: {}, raising=False)
    monkeypatch.setattr(eng, "_get_price_at_date", lambda t, d: prices[t], raising=False)
    monkeypatch.setattr(eng, "_split_adjust_exit", lambda book, t, a, e, c: a, raising=False)
    written: list[Any] = []

    def _capture(engine, lesson, **kw):
        events.append("lesson")
        written.append(lesson)
        return len(written)

    monkeypatch.setattr(rb, "write_reasoning_lesson", _capture)
    results = eng.score_expired_predictions()

    assert (results["hits"], results["misses"], results["partials"]) == (1, 1, 1)
    assert results["lessons_written"] == 3
    assert events == ["commit", "lesson", "lesson", "lesson"]
    by_id = {l.source_id: l for l in written}
    assert by_id["p-hit"].outcome_class == "success"
    assert by_id["p-part"].outcome_class == "success"  # the direction was right
    assert by_id["p-miss"].outcome_class == "failure"
    assert {l.source_type for l in written} == {rb.VERDICT_LESSON_SOURCE}
    fp = by_id["p-miss"].condition_fingerprint
    assert (fp["ticker"], fp["direction"], fp["horizon_bucket"]) == ("AAPL", "put", "1w")


def test_no_verdicts_no_lessons():
    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = MagicMock()
    assert eng._write_verdict_lessons([]) == 0


def test_a_failed_lesson_write_never_breaks_scoring(monkeypatch):
    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = MagicMock()

    def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(rb, "write_reasoning_lesson", _boom)
    v = {"prediction_id": "p1", "ticker": "NVDA", "direction": "CALL", "verdict": "hit",
         "model": "m", "created_at": datetime(2026, 11, 1, tzinfo=timezone.utc),
         "expiry": date(2026, 11, 8), "actual_move": 3.0, "pnl": 3.0}
    assert eng._write_verdict_lessons([v]) == 0


# ── postmortems explain misses; they do not write lessons ───────────────────


def test_postmortems_write_no_lessons(monkeypatch):
    assert postmortem.POSTMORTEM_WRITES_LESSONS is False
    called = []
    monkeypatch.setattr(postmortem, "_emit_lesson_from_postmortem",
                        lambda *a, **k: called.append(1))
    engine = MagicMock()
    pm = MagicMock()
    pm.which_signals_were_wrong = []
    pm.which_signals_were_right = []
    pm.to_dict.return_value = {}
    postmortem._store_postmortem(engine, pm, trade_id=None, prediction_id="p1")
    assert called == []
    engine.begin.assert_called()  # the postmortem row itself is still written


@pytest.mark.parametrize("name", ["postmortem_batch", "sleuth_dispatch"])
def test_hermes_runs_the_released_tasks(name):
    from scripts import hermes_operator as ho

    assert name in ho.DAILY_INTEL_INITIAL_ALLOWLIST
    assert name not in ho.DAILY_INTEL_HOLD_REASONS
