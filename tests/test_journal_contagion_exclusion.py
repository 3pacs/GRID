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
        sql = str(statement.compile(dialect=PG))
        queries.append(sql)
        result = MagicMock()
        # Only journal reads return the fixture rows; other tables (e.g. feature movers) are empty.
        journal = "FROM decision_journal" in sql
        result.fetchone.return_value = rows[0] if (rows and journal) else None
        result.fetchall.return_value = list(rows) if journal else []
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


# ── the three selectors Codex named at 853e39e68 (get_weights, get_all_active, get_synthesis) ──────────────

def _assert_excluded_before_ordering(queries, *, distinct):
    reads = [q for q in queries if "FROM decision_journal" in q]
    assert len(reads) == 1
    sql = reads[0]
    assert PG_PRED in sql and PG_NOT_TICKET in sql
    assert sql.index(PG_NOT_TICKET) < sql.index("ORDER BY")
    assert ("SELECT DISTINCT ON (decision_journal.inferred_state)" in sql) is distinct


def test_regime_weights_excludes_tickets(monkeypatch):
    import sys
    from types import SimpleNamespace
    from api.routers import regime

    path = MagicMock()
    path.exists.return_value = False
    monkeypatch.setitem(sys.modules, "scripts.auto_regime", SimpleNamespace(
        DEFAULT_FEATURE_WEIGHTS={}, FEATURE_WEIGHTS={}, WEIGHTS_OVERRIDE_PATH=path))
    engine, queries = _engine([("GROWTH", 0.8, "S=0.125")])
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    regime.get_weights(_token="fixture")
    _assert_excluded_before_ordering(queries, distinct=False)
    assert "LIMIT" in queries[-1] and queries[-1].index(PG_NOT_TICKET) < queries[-1].index("LIMIT")


def test_regime_all_active_excludes_tickets_before_distinct_selection(monkeypatch):
    import sys
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from api.routers import regime

    now = datetime(2026, 11, 8, 12, tzinfo=timezone.utc)
    engine, queries = _engine([("GROWTH", 0.8, 0.1, {}, "HOLD", now)])
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    monkeypatch.setitem(sys.modules, "discovery.clustering", SimpleNamespace(
        ClusterDiscovery=lambda *_: SimpleNamespace(identify_transition_leaders=lambda: {})))
    regime.get_all_active(_token="fixture")
    _assert_excluded_before_ordering(queries, distinct=True)


def test_regime_synthesis_excludes_tickets_before_distinct_selection(monkeypatch):
    import sys
    from types import SimpleNamespace
    from api.routers import regime

    engine, queries = _engine([("GROWTH", 0.8, "HOLD")])
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    client = MagicMock(is_available=True)
    client.chat.return_value = "Fixture synthesis"
    monkeypatch.setitem(sys.modules, "llm.router", SimpleNamespace(
        Tier=SimpleNamespace(REASON="fixture"), get_llm=MagicMock(return_value=client)))
    regime.get_synthesis(_token="fixture")
    _assert_excluded_before_ordering(queries, distinct=True)


def test_every_guarded_regime_and_mcp_selector_is_wrapped():
    # Regression guard for the next selector added to these files: no bare non_provisional_journal_select(.
    import re
    root = Path(__file__).resolve().parents[1] / "api" / "routers"
    for name in ("regime.py", "mcp_export.py"):
        source = (root / name).read_text()
        bare = re.findall(r"(?<!excluding_contagion_tickets\()non_provisional_journal_select\(", source)
        assert bare == [], f"{name}: unwrapped non_provisional_journal_select( call"
        assert source.count("excluding_contagion_tickets(non_provisional_journal_select(") >= 1, name
