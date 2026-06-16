"""Tests for ``scripts.enrich_connections``.

Focus: the heavy actor-enrichment passes must lift the per-statement
timeout (``SET LOCAL statement_timeout = 0``) as the FIRST statement
inside each ``engine.begin()`` transaction, so the 152K+ row DISTINCT
scans / self-JOINs are not killed by the global 120s default. The
override is per-transaction (SET LOCAL), so the global default is
never mutated.

Uses a MagicMock engine that records every executed SQL string. No
live database is touched — each enrichment returns zero rows so the
pass short-circuits right after the lift + first query.
"""

from __future__ import annotations

import pytest

from scripts import enrich_connections as ec


# ─────────────────────────────────────────────────────────────────
# Recording mock engine
# ─────────────────────────────────────────────────────────────────


class _RecordingConn:
    """Connection stub that records executed SQL and returns no rows."""

    def __init__(self, log: list[str]) -> None:
        self._log = log

    def execute(self, stmt, *args, **kwargs):
        self._log.append(str(getattr(stmt, "text", stmt)))
        result = MagicResult()
        return result


class MagicResult:
    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _RecordingEngine:
    """Engine stub whose ``begin()`` yields a recording connection."""

    def __init__(self) -> None:
        self.sql_log: list[str] = []

    def begin(self):
        conn = _RecordingConn(self.sql_log)
        return _CtxMgr(conn)

    # enrich passes only ever use begin(); connect() unused here.
    def connect(self):  # pragma: no cover - defensive
        conn = _RecordingConn(self.sql_log)
        return _CtxMgr(conn)


class _CtxMgr:
    def __init__(self, conn) -> None:
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False


# All enrichment passes that open a heavy transaction. Each must lift
# the statement timeout before issuing its scan.
_ENRICH_FUNCS = [
    ec.enrich_insider_connections,
    ec.enrich_congressional_connections,
    ec.enrich_gov_contracts,
    ec.enrich_lobbying,
    ec.enrich_insider_clusters,
    ec.enrich_fara_foreign_lobbying,
    ec.enrich_darkpool_signals,
    ec.enrich_institutional_flows,
    ec.enrich_congress_insider_overlap,
]


# ─────────────────────────────────────────────────────────────────
# 1. The override constant is "unlimited" (0) and used in the helper
# ─────────────────────────────────────────────────────────────────


def test_statement_timeout_override_is_unlimited():
    assert ec._ENRICH_STATEMENT_TIMEOUT_MS == 0


def test_lift_helper_issues_set_local_first():
    engine = _RecordingEngine()
    with engine.begin() as conn:
        ec._lift_statement_timeout(conn)
    assert engine.sql_log == ["SET LOCAL statement_timeout = 0"]


# ─────────────────────────────────────────────────────────────────
# 2. Every enrichment pass lifts the timeout as its FIRST statement
# ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("enrich_fn", _ENRICH_FUNCS, ids=lambda f: f.__name__)
def test_enrichment_lifts_statement_timeout_first(enrich_fn):
    engine = _RecordingEngine()
    enrich_fn(engine)

    assert engine.sql_log, f"{enrich_fn.__name__} issued no SQL"
    # The override MUST be the first statement so it applies to the
    # heavy scan that follows (SET LOCAL only affects the open txn).
    first = engine.sql_log[0].strip()
    assert first == "SET LOCAL statement_timeout = 0", (
        f"{enrich_fn.__name__} first statement was {first!r}, "
        "expected the statement-timeout override"
    )
    # Exactly one override per transaction (not sprinkled per-query).
    overrides = [s for s in engine.sql_log if "statement_timeout" in s]
    assert len(overrides) == 1


# ─────────────────────────────────────────────────────────────────
# 3. The override is SET LOCAL (per-txn), never a global SET
# ─────────────────────────────────────────────────────────────────


def test_override_is_set_local_not_global():
    engine = _RecordingEngine()
    ec.enrich_insider_connections(engine)
    timeout_stmts = [s for s in engine.sql_log if "statement_timeout" in s]
    assert timeout_stmts
    for s in timeout_stmts:
        # Must be SET LOCAL (transaction-scoped), never a bare global SET
        # that would change the default for every other caller on the conn.
        assert "SET LOCAL" in s
