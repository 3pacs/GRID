"""Oracle writers share the complete v2 identity and immutable DO NOTHING.

The runtime guard checks each target database, refuses legacy/invalid arbiters,
and surfaces DDL errors. Real migration/retry/race proofs are in the PostgreSQL
identity tests; these fast checks cover all three writer SQL contracts.
"""

from __future__ import annotations

from typing import Any

import pytest

from schema_guard import reset_for_tests


@pytest.fixture(autouse=True)
def _reset_schema_guard():
    reset_for_tests()
    yield
    reset_for_tests()


class _RecordingConn:
    """Context-manager connection that records every executed SQL string."""

    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def __enter__(self) -> "_RecordingConn":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, clause: Any, params: dict[str, Any] | None = None) -> "_Result":
        self._sink.append(str(clause))
        result = _Result()
        if "INSERT INTO oracle_predictions" in str(clause):
            result.fetchone = lambda: (params["id"],)
        return result


class _Result:
    def fetchall(self) -> list[Any]:
        return []

    def fetchone(self) -> Any:
        return None

    def scalar(self) -> Any:
        return None


class _RecordingEngine:
    """Fake Engine: begin()/connect() both yield a recording connection."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def begin(self) -> _RecordingConn:
        return _RecordingConn(self.executed)

    def connect(self) -> _RecordingConn:
        return _RecordingConn(self.executed)


_ON_CONFLICT_NEEDLES = (
    "ON CONFLICT (",
    "ticker, direction, expiry, prediction_type",
    "(COALESCE(model_version, ''))",
    "created_at AT TIME ZONE 'UTC'",
    "WHERE dedup_keep = TRUE",
    "DO NOTHING",
    "model_name",
    "recording_policy",
)

_INDEX_NEEDLES = (
    "CREATE UNIQUE INDEX",
    "oracle_predictions_dedup_unique",
    "WHERE dedup_keep = TRUE",
)


def _find_insert(sqls: list[str]) -> str:
    inserts = [s for s in sqls if "INSERT INTO oracle_predictions" in s]
    assert inserts, f"no oracle_predictions insert was executed; saw: {sqls}"
    return inserts[0]


def _find_index_ddl(sqls: list[str]) -> str:
    ddls = [
        s
        for s in sqls
        if "CREATE UNIQUE INDEX" in s and "oracle_predictions_dedup_unique" in s
    ]
    assert ddls, f"dedup index was not ensured before insert; saw: {sqls}"
    return ddls[0]


# ---------------------------------------------------------------------------
# oracle/publish.py
# ---------------------------------------------------------------------------

def test_publish_ensures_index_then_upserts_on_natural_key():
    from oracle.publish import publish_astrogrid_prediction

    engine = _RecordingEngine()
    result = publish_astrogrid_prediction(
        engine,  # type: ignore[arg-type]
        {
            "prediction_id": "p1",
            "target_symbols": ["NVDA"],
            "confidence": 0.7,
            "model_version": "astrogrid-oracle-v1",
        },
    )

    assert result["status"] == "published"

    index_ddl = _find_index_ddl(engine.executed)
    for needle in _INDEX_NEEDLES:
        assert needle in index_ddl

    insert = _find_insert(engine.executed)
    for needle in _ON_CONFLICT_NEEDLES:
        assert needle in insert, f"missing {needle!r} in publish insert"

    # Index DDL must be emitted BEFORE the insert (pre-migration safety).
    assert engine.executed.index(index_ddl) < engine.executed.index(insert)


# ---------------------------------------------------------------------------
# intelligence/obsidian_agent.py
# ---------------------------------------------------------------------------

def test_obsidian_cycle_ensures_index_then_upserts_on_natural_key(monkeypatch):
    import intelligence.obsidian_agent as oa

    engine = _RecordingEngine()

    class _Note:
        id = 1
        vault_path = "x.md"
        domain = "alpha"
        status = "approved"
        title = "NVDA setup"
        body = "Watching $NVDA for earnings momentum."
        agent_flags: dict = {}
        frontmatter: dict = {}

    # The cycle SELECTs recent notes first; make that return our approved note,
    # and short-circuit the preferences update (separate path, separate table).
    def _fake_begin():
        conn = _RecordingConn(engine.executed)
        original_execute = conn.execute

        def execute(clause, params=None):
            original_execute(clause, params)
            res = _Result()
            if "FROM obsidian_notes" in str(clause):
                res.fetchall = lambda: [_Note()]  # type: ignore[assignment]
            return res

        conn.execute = execute  # type: ignore[assignment]
        return conn

    monkeypatch.setattr(engine, "begin", _fake_begin)
    monkeypatch.setattr(oa, "enrich_note", lambda conn, nid, body: body)
    monkeypatch.setattr(oa, "_update_preferences", lambda eng: None)

    oa.run_agent_cycle(engine)  # type: ignore[arg-type]

    index_ddl = _find_index_ddl(engine.executed)
    for needle in _INDEX_NEEDLES:
        assert needle in index_ddl

    insert = _find_insert(engine.executed)
    for needle in _ON_CONFLICT_NEEDLES:
        assert needle in insert, f"missing {needle!r} in obsidian insert"

    assert engine.executed.index(index_ddl) < engine.executed.index(insert)


# ---------------------------------------------------------------------------
# oracle/dedup_index.py helper semantics
# ---------------------------------------------------------------------------

def test_ensure_dedup_index_rechecks_each_database():
    from oracle.dedup_index import ensure_dedup_index
    engines = [_RecordingEngine(), _RecordingEngine()]
    for engine in engines:
        ensure_dedup_index(engine)
        assert any("CREATE UNIQUE INDEX" in sql for sql in engine.executed)


def test_ensure_dedup_index_propagates_ddl_failure():
    from oracle.dedup_index import ensure_dedup_index
    class _BoomEngine:
        def begin(self):
            raise RuntimeError("ddl lock timeout")
    with pytest.raises(RuntimeError, match="ddl lock timeout"):
        ensure_dedup_index(_BoomEngine())


def test_helper_index_ddl_matches_engine_and_publish_predicate():
    """The arbiter predicate must be byte-identical across all definitions."""
    from pathlib import Path

    from oracle.dedup_index import _DEDUP_INDEX_DDL

    root = Path(__file__).resolve().parents[1]
    engine_src = (root / "oracle/engine.py").read_text()
    migration_src = (
        root / "migrations/versions/oracle_forward_v2_20261110.py"
    ).read_text()

    predicate_lines = (
        "ticker,",
        "direction,",
        "expiry,",
        "prediction_type,",
        "(COALESCE(model_version, '')),",
        "((created_at AT TIME ZONE 'UTC')::date),",
        "(COALESCE(recording_policy, 'legacy'))",
    )
    for line in predicate_lines:
        assert line in _DEDUP_INDEX_DDL
        assert line in migration_src
    assert "ensure_dedup_index_on_connection" in engine_src

    assert "WHERE dedup_keep = TRUE" in _DEDUP_INDEX_DDL
    assert "oracle_predictions_dedup_unique" in migration_src
