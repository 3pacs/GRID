"""Real-PostgreSQL proof for migrations/versions/actors_provenance_columns_0922.py --
the SCHEMA-ONLY half of the #596 remediation split (see
00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

Unlike the original combined migration (actors_provenance_20260917.py, reverted by
#597), this one does nothing but add two columns -- no backfill UPDATE, no dependency on
`intelligence.actors.seed_data`. Proves: both columns land with the right type/default/
nullability, the migration's SET LOCAL timeouts are scoped to its own transaction only
(same precedent as oracle_pred_nullable_0918 and the original actors_provenance_20260917),
downgrade removes both columns cleanly, and upgrade is fast (no row-count-dependent work
to be slow in the first place -- this is the whole point of the split).

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
import time

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

_MIGRATION_MODULE = "migrations.versions.actors_provenance_columns_0922"

_MINIMAL_ACTORS_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    category TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@pytest.fixture(autouse=True)
def _actors_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_ACTORS_DDL))
    yield


def _reset_provenance_columns(pg_engine: Engine) -> None:
    with pg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance_as_of"))
        conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance"))


def _run_migration_fn(pg_engine: Engine, migration, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            getattr(migration, fn_name)()
        finally:
            migration.op = real_op
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


def test_upgrade_adds_both_columns_with_correct_shape(pg_engine: Engine):
    migration = importlib.import_module(_MIGRATION_MODULE)
    _reset_provenance_columns(pg_engine)

    _run_migration_fn(pg_engine, migration, "upgrade")

    with pg_engine.connect() as conn:
        cols = {
            r[0]: (r[1], r[2], r[3])
            for r in conn.execute(text(
                "SELECT column_name, is_nullable, data_type, column_default "
                "FROM information_schema.columns "
                "WHERE table_name = 'actors' "
                "AND column_name IN ('provenance', 'provenance_as_of')"
            )).fetchall()
        }
    assert cols["provenance"][0] == "NO", "provenance must be NOT NULL"
    assert "'observed'" in (cols["provenance"][2] or ""), "provenance default must be 'observed'"
    assert cols["provenance_as_of"][0] == "YES", "provenance_as_of must be nullable"


def test_upgrade_is_idempotent(pg_engine: Engine):
    migration = importlib.import_module(_MIGRATION_MODULE)
    _reset_provenance_columns(pg_engine)

    _run_migration_fn(pg_engine, migration, "upgrade")
    _run_migration_fn(pg_engine, migration, "upgrade")  # must not raise (IF NOT EXISTS)

    with pg_engine.connect() as conn:
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'actors' "
            "AND column_name IN ('provenance', 'provenance_as_of')"
        )).fetchall()
    assert {c[0] for c in cols} == {"provenance", "provenance_as_of"}


def test_upgrade_does_not_touch_existing_row_values(pg_engine: Engine):
    """The whole point of the split: schema alone never classifies anything.
    Every pre-existing row -- including a real seed-list id -- reads the
    column DEFAULT until the separate backfill script runs."""
    migration = importlib.import_module(_MIGRATION_MODULE)
    _reset_provenance_columns(pg_engine)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category) VALUES "
            "('sch0922_test_actor', 'Test Actor', 'test', 'test') "
            "ON CONFLICT (id) DO NOTHING"
        ))

    _run_migration_fn(pg_engine, migration, "upgrade")

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, provenance_as_of FROM actors WHERE id = 'sch0922_test_actor'"
        )).fetchone()
    assert row.provenance == "observed", "schema-only migration must not classify any row"
    assert row.provenance_as_of is None

    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM actors WHERE id = 'sch0922_test_actor'"))


def test_upgrade_runs_with_finite_timeouts_scoped_to_its_own_transaction(pg_engine: Engine):
    migration = importlib.import_module(_MIGRATION_MODULE)
    _reset_provenance_columns(pg_engine)

    with pg_engine.connect() as baseline_conn:
        baseline_lock_timeout = baseline_conn.execute(text("SHOW lock_timeout")).scalar()
        baseline_statement_timeout = baseline_conn.execute(text("SHOW statement_timeout")).scalar()

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            migration.upgrade()
        finally:
            migration.op = real_op

        assert conn.execute(text("SHOW lock_timeout")).scalar() == migration._LOCK_TIMEOUT
        assert conn.execute(text("SHOW statement_timeout")).scalar() == migration._STATEMENT_TIMEOUT
        trans.commit()

        # Decisive: same connection, right after COMMIT, must have reset.
        assert conn.execute(text("SHOW lock_timeout")).scalar() == baseline_lock_timeout
        assert conn.execute(text("SHOW statement_timeout")).scalar() == baseline_statement_timeout
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


def test_upgrade_is_fast_no_row_scan(pg_engine: Engine):
    """Nothing here should scale with table size -- both ALTERs are
    metadata-only and there is no backfill UPDATE at all. A generous bound
    (2s) proves this is not accidentally doing row-by-row work, not a tight
    performance assertion."""
    migration = importlib.import_module(_MIGRATION_MODULE)
    _reset_provenance_columns(pg_engine)

    start = time.monotonic()
    _run_migration_fn(pg_engine, migration, "upgrade")
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, f"schema-only migration took {elapsed:.2f}s -- expected near-instant"


def test_downgrade_drops_both_columns(pg_engine: Engine):
    """Isolated in a rolled-back transaction -- never touches the shared
    table's real schema."""
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        conn.execute(text(
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance TEXT "
            "NOT NULL DEFAULT 'observed'"
        ))
        conn.execute(text(
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE"
        ))

        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            migration.downgrade()
        finally:
            migration.op = real_op

        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'actors' "
            "AND column_name IN ('provenance', 'provenance_as_of')"
        )).fetchall()
        assert cols == []
    finally:
        trans.rollback()
        conn.close()
