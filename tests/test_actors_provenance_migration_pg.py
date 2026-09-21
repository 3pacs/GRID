"""Real-PostgreSQL proof for migrations/versions/actors_provenance_20260917.py.

Unlike most migrations in this lane, this one does not stop at adding
columns -- it also runs a real, scoped ``UPDATE`` backfilling every existing
``actors`` row the hand-curated seed list owns. This file proves that write
against a real database: exactly the seed ids get stamped, everything else
keeps the column default, a second run is a no-op, and downgrade removes
both columns cleanly. ``_KNOWN_ACTORS`` (500+ entries) is monkeypatched to a
handful of ids for a fast, deterministic proof -- the migration's own
``_seed_actor_ids()`` reads it by name at call time, so patching the module
attribute is sufficient.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if
no PostgreSQL is reachable. Every row this file writes uses a unique
``pkt2prov_pg_<uuid>`` id and the autouse fixture deletes them afterward.
"""

from __future__ import annotations

import importlib
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

_MIGRATION_MODULE = "migrations.versions.actors_provenance_20260917"

_MINIMAL_ACTORS_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    category TEXT NOT NULL
)
"""


@pytest.fixture(autouse=True)
def _actors_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_ACTORS_DDL))
    yield


@pytest.fixture
def test_ids() -> list[str]:
    return []


@pytest.fixture(autouse=True)
def cleanup_test_rows(pg_engine: Engine, test_ids: list[str]):
    yield
    if not test_ids:
        return
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM actors WHERE id = ANY(:ids)").bindparams(ids=test_ids),
        )


def _insert_actor(conn, *, actor_id: str, name: str) -> None:
    conn.execute(
        text(
            "INSERT INTO actors (id, name, tier, category) "
            "VALUES (:id, :name, 'test', 'test')",
        ).bindparams(id=actor_id, name=name),
    )


def test_upgrade_backfills_exactly_the_seed_ids_and_defaults_everyone_else(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    seeded_id = f"pkt2prov_pg_{uuid.uuid4().hex[:16]}"
    observed_id = f"pkt2prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.extend([seeded_id, observed_id])

    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=seeded_id, name="Seeded Test Actor")
        _insert_actor(conn, actor_id=observed_id, name="Observed Test Actor")

    # Drop any columns a prior run of this test file left behind, so upgrade()
    # starts from the same pre-migration shape every time.
    with pg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance_as_of"))
        conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance"))

    import intelligence.actors.seed_data as seed_data

    monkeypatch.setattr(seed_data, "_KNOWN_ACTORS", {seeded_id: {}})

    migration = importlib.import_module(_MIGRATION_MODULE)

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    with pg_engine.connect() as verify_conn:
        rows = {
            r[0]: dict(r._mapping)
            for r in verify_conn.execute(
                text(
                    "SELECT id, provenance, provenance_as_of FROM actors "
                    "WHERE id = ANY(:ids)",
                ).bindparams(ids=[seeded_id, observed_id]),
            ).fetchall()
        }

    assert rows[seeded_id]["provenance"] == migration.PROVENANCE_SEED
    assert str(rows[seeded_id]["provenance_as_of"]) == migration.SEED_VINTAGE
    assert rows[observed_id]["provenance"] == migration.PROVENANCE_OBSERVED
    assert rows[observed_id]["provenance_as_of"] is None

    # Idempotent: running upgrade() again must not raise and must not change
    # either row (the backfill's own WHERE excludes rows already matching).
    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    with pg_engine.connect() as verify_conn:
        rows_again = {
            r[0]: dict(r._mapping)
            for r in verify_conn.execute(
                text(
                    "SELECT id, provenance, provenance_as_of FROM actors "
                    "WHERE id = ANY(:ids)",
                ).bindparams(ids=[seeded_id, observed_id]),
            ).fetchall()
        }
    assert rows_again == rows, "a second upgrade() run must be a no-op"


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
            "NOT NULL DEFAULT 'observed'",
        ))
        conn.execute(text(
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE",
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
            "AND column_name IN ('provenance', 'provenance_as_of')",
        )).fetchall()
        assert cols == []
    finally:
        trans.rollback()
        conn.close()
