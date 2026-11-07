"""Real-PostgreSQL proof for migrations/versions/hyp_registry_labels_20261107.py
and governance.hypothesis_labels.

Runs in a throwaway schema (random name, dropped afterwards) with minimal
``hypothesis_registry`` / ``discovered_hypotheses`` stand-ins. Proves:

* labels are appended once per row (a re-run inserts nothing) and the registry
  rows are byte-for-byte unchanged;
* the label table refuses UPDATE, DELETE and TRUNCATE;
* the PASSED consumers' predicate keeps only post-cutoff, unlabelled rows;
* ``discovered_hypotheses`` gains the nullable v2 verdict columns.

Uses the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``); skips when no
PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from governance import hypothesis_labels as hl

_MIGRATION_MODULE = "migrations.versions.hyp_registry_labels_20261107"
OLD = hl.LEGACY_CUTOFF - timedelta(days=60)
NEW = hl.LEGACY_CUTOFF + timedelta(hours=1)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"hyp_labels_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE hypothesis_registry (
                id SERIAL PRIMARY KEY, statement TEXT NOT NULL, layer TEXT NOT NULL,
                state TEXT NOT NULL, kill_reason TEXT,
                created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
            )
        """))
        conn.execute(text("CREATE TABLE discovered_hypotheses (id TEXT PRIMARY KEY, status TEXT)"))
        rows = [
            ("old passed", "PASSED", None, OLD),
            ("old passed dup", "PASSED", None, OLD),
            ("flagged", "TESTING", "LLM review flag: Unrealistic assumptions (Sharpe 9.6)", OLD),
            ("inconclusive", "TESTING", None, OLD),
            ("failed", "FAILED", "no edge", OLD),
            ("new passed", "PASSED", None, NEW),
        ]
        for statement, state, kill, created in rows:
            conn.execute(text(
                "INSERT INTO hypothesis_registry (statement, layer, state, kill_reason, created_at, updated_at) "
                "VALUES (:s, 'TACTICAL', :st, :k, :c, :c)"
            ), {"s": statement, "st": state, "k": kill, "c": created})
    try:
        yield engine, schema
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, step: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            getattr(migration, step)()
        finally:
            migration.op = real_op
        trans.commit()


def _registry_snapshot(engine: Engine) -> list[tuple]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text("SELECT * FROM hypothesis_registry ORDER BY id"))]


def test_labels_append_once_and_registry_is_untouched(scratch):
    engine, _ = scratch
    _run(engine, "upgrade")
    before = _registry_snapshot(engine)

    dry = hl.label_legacy_registry(engine)
    assert dry["planned"] == {"LEGACY_UNVALIDATED": 2, "LEGACY_LLM_FLAGGED": 1, "LEGACY_INCONCLUSIVE": 1}
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM hypothesis_registry_labels")).scalar() == 0

    applied = hl.label_legacy_registry(engine, dry_run=False, decided_by="test-owner")
    assert applied["inserted"] == 4
    again = hl.label_legacy_registry(engine, dry_run=False, decided_by="test-owner")
    assert again["inserted"] == 0
    assert again["already_labelled"] == {"LEGACY_UNVALIDATED": 2, "LEGACY_LLM_FLAGGED": 1,
                                         "LEGACY_INCONCLUSIVE": 1}
    assert _registry_snapshot(engine) == before

    with engine.connect() as conn:
        labels = dict(conn.execute(text(
            "SELECT h.statement, l.label FROM hypothesis_registry_labels l "
            "JOIN hypothesis_registry h ON h.id = l.registry_id"
        )).fetchall())
    assert labels == {"old passed": "LEGACY_UNVALIDATED", "old passed dup": "LEGACY_UNVALIDATED",
                      "flagged": "LEGACY_LLM_FLAGGED", "inconclusive": "LEGACY_INCONCLUSIVE"}


def test_label_table_is_append_only(scratch):
    engine, _ = scratch
    _run(engine, "upgrade")
    hl.label_legacy_registry(engine, dry_run=False, decided_by="test-owner")
    for stmt in ("UPDATE hypothesis_registry_labels SET label = 'LEGACY_INCONCLUSIVE'",
                 "DELETE FROM hypothesis_registry_labels",
                 "TRUNCATE hypothesis_registry_labels"):
        with pytest.raises(DBAPIError, match="append-only"), engine.begin() as conn:
            conn.execute(text(stmt))


def test_consumer_predicate_keeps_only_validated_passed(scratch):
    engine, _ = scratch
    _run(engine, "upgrade")
    with engine.connect() as conn:
        ids = dict(conn.execute(text("SELECT statement, id FROM hypothesis_registry")).fetchall())
        # Fail closed before any label row exists: the cutoff alone excludes legacy rows.
        kept = [r[0] for r in conn.execute(text(
            "SELECT h.statement FROM hypothesis_registry h WHERE h.state = 'PASSED' "
            "AND h.created_at >= :legacy_cutoff AND NOT EXISTS (SELECT 1 FROM "
            "hypothesis_registry_labels l WHERE l.registry_id = h.id)"
        ), {"legacy_cutoff": hl.LEGACY_CUTOFF})]
        assert kept == ["new passed"]
        assert hl.passed_validation(conn, ids["new passed"])["validated"] is True
        assert hl.passed_validation(conn, ids["old passed"])["validated"] is False
        assert hl.passed_validation(conn, ids["flagged"])["validated"] is False


def test_v2_columns_added_and_downgrade_keeps_them(scratch):
    engine, _ = scratch
    _run(engine, "upgrade")
    _run(engine, "upgrade")  # re-applyable
    with engine.connect() as conn:
        cols = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'discovered_hypotheses'"
        ))}
    assert {"scoring_version", "trial_p_value", "trial_q_value", "trial_ledger_sha256"} <= cols
    _run(engine, "downgrade")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('hypothesis_registry_labels')")).scalar() is None
        assert conn.execute(text(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() "
            "AND table_name = 'discovered_hypotheses' AND column_name = 'scoring_version'"
        )).scalar() == 1
