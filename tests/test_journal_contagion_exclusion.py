"""Offline routing tests: contagion trade tickets are never served as the regime (#959, Codex hold on #1495).

The PostgreSQL semantics are proven in tests/test_journal_contagion_exclusion_pg.py. These check that the
regime readers include the exclusion, and that the sealed qualification guard is untouched.
"""
from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from store.journal_contagion_exclusion import (
    CONTAGION_TICKET_JOURNAL_MARKER,
    NOT_CONTAGION_TICKET_JOURNAL_PREDICATE,
    excluding_contagion_tickets,
)
from store.journal_qualification import NON_PROVISIONAL_JOURNAL_PREDICATE, non_provisional_journal_select

PG = postgresql.dialect()
PG_PRED = str(text(NON_PROVISIONAL_JOURNAL_PREDICATE).compile(dialect=PG))
PG_NOT_TICKET = str(text(NOT_CONTAGION_TICKET_JOURNAL_PREDICATE).compile(dialect=PG))


def _engine(rows=()):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    queries: list[str] = []

    def execute(statement, *_a, **_k):
        queries.append(str(statement.compile(dialect=PG)))
        result = MagicMock()
        result.fetchone.return_value = rows[0] if rows else None
        result.fetchall.return_value = list(rows)
        return result

    conn.execute.side_effect = execute
    return engine, queries


def test_the_marker_is_the_tag_not_the_state_prefix():
    assert "'contagion_derived'" in CONTAGION_TICKET_JOURNAL_MARKER
    assert "flow_thesis" in CONTAGION_TICKET_JOURNAL_MARKER
    assert "CONTAGION_" not in CONTAGION_TICKET_JOURNAL_MARKER and "LIKE" not in CONTAGION_TICKET_JOURNAL_MARKER


def test_a_select_gets_the_exclusion_with_its_own_bind_that_cannot_collide_with_the_guard():
    stmt = excluding_contagion_tickets(non_provisional_journal_select("inferred_state"))
    compiled = stmt.compile(dialect=PG)
    sql = str(compiled)
    assert PG_PRED in sql and PG_NOT_TICKET in sql
    assert {"journal_marker_whitespace", "contagion_marker_whitespace"} <= set(compiled.params)


def test_regime_current_excludes_tickets_before_order_and_limit(monkeypatch):
    from api.routers import regime

    engine, queries = _engine()
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    try:
        regime.get_current(_token="fixture")
    except Exception:
        pass  # no journal row: the response path is not under test
    reads = [q for q in queries if "FROM decision_journal" in q]
    assert reads
    for sql in reads:
        assert PG_PRED in sql and PG_NOT_TICKET in sql
        assert sql.index(PG_NOT_TICKET) < sql.index("ORDER BY") < sql.index("LIMIT")


def test_mcp_regime_current_uses_the_guard_and_history_excludes_tickets(monkeypatch):
    from api.routers import mcp_export

    engine, queries = _engine([(date(2026, 11, 7), "GROWTH", 0.8)])
    monkeypatch.setattr(mcp_export, "get_db_engine", lambda: engine)
    mcp_export.mcp_regime(_token="fixture")
    current, history = [q for q in queries if "FROM decision_journal" in q]
    assert PG_PRED in current and PG_NOT_TICKET in current
    assert current.index(PG_PRED) < current.index("ORDER BY") < current.index("LIMIT")
    assert PG_NOT_TICKET in history and PG_PRED not in history   # raw history, but never a ticket


def test_regime_history_and_transitions_exclude_tickets_but_not_provisional(monkeypatch):
    from api.routers import regime

    engine, queries = _engine([(date(2026, 11, 7), "GROWTH", 0.8)])
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    regime.get_history(days=30, _token="fixture")
    regime.get_transitions(_token="fixture")
    assert len(queries) == 2
    assert all(PG_NOT_TICKET in q and PG_PRED not in q for q in queries)


def test_the_sealed_qualification_guard_is_untouched():
    # .github/workflows/oct25-native-compatibility.yml pins these exact bytes; extending the shared
    # predicate is a decision for that seal's owner, not part of this change.
    path = Path(__file__).resolve().parents[1] / "store" / "journal_qualification.py"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == "5ec7e6b2dd01446a2cfc6c313d6b95d1e688956f2fdd2ed4d1700e170b45d65b"
