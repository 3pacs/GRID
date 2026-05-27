"""Write-path guard tests for oracle_predictions natural-key dedup.

Companion to ``tests/test_oracle_dedup_consumers.py`` (which pins the consumer
read path and the writers' ON CONFLICT clause at the *source* level). This file
exercises the *runtime* behaviour of the two non-engine writers
(``oracle/publish.py`` and ``intelligence/obsidian_agent.py``) and proves:

1. Each writer is idempotent on the natural key: the emitted INSERT carries
   ``ON CONFLICT (... COALESCE(model_version,'') ... created_at AT TIME ZONE
   'UTC' ...) WHERE dedup_keep = TRUE DO UPDATE SET ...``.
2. Pre-migration safety: before that insert runs, the writer ensures the
   partial unique index ``oracle_predictions_dedup_unique`` exists (so the
   ON CONFLICT can't raise 42P10 on a not-yet-migrated DB).
3. ``ensure_dedup_index`` runs at most once per process and never raises into
   the caller's hot path even if the DDL fails.
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
        return _Result()


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
    "DO UPDATE SET",
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

def test_ensure_dedup_index_runs_once_per_process():
    from oracle.dedup_index import ensure_dedup_index

    engine = _RecordingEngine()
    ensure_dedup_index(engine)  # type: ignore[arg-type]
    ensure_dedup_index(engine)  # type: ignore[arg-type]

    ddls = [s for s in engine.executed if "oracle_predictions_dedup_unique" in s]
    assert len(ddls) == 1, "index ensure should fire exactly once per process"


def test_ensure_dedup_index_swallows_ddl_failure():
    from oracle.dedup_index import ensure_dedup_index

    class _BoomEngine:
        def begin(self):
            raise RuntimeError("ddl lock timeout")

    # Must not raise — pre-migration safety is best-effort, the writer's own
    # insert surfaces the real error if the index truly can't be created.
    ensure_dedup_index(_BoomEngine())  # type: ignore[arg-type]


def test_helper_index_ddl_matches_engine_and_publish_predicate():
    """The arbiter predicate must be byte-identical across all definitions."""
    from pathlib import Path

    from oracle.dedup_index import _DEDUP_INDEX_DDL

    root = Path(__file__).resolve().parents[1]
    engine_src = (root / "oracle/engine.py").read_text()
    migration_src = (
        root / "migrations/0055_oracle_predictions_dedup_guard_index.sql"
    ).read_text()

    predicate_lines = (
        "ticker,",
        "direction,",
        "expiry,",
        "prediction_type,",
        "(COALESCE(model_version, '')),",
        "((created_at AT TIME ZONE 'UTC')::date)",
    )
    for line in predicate_lines:
        assert line in _DEDUP_INDEX_DDL
        assert line in engine_src
        assert line in migration_src

    assert "WHERE dedup_keep = TRUE" in _DEDUP_INDEX_DDL
    assert "oracle_predictions_dedup_unique" in migration_src
