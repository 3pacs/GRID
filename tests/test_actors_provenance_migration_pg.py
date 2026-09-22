"""Real-PostgreSQL proof for migrations/versions/actors_provenance_20260917.py.

Unlike most migrations in this lane, this one does not stop at adding
columns -- it also runs two real, scoped ``UPDATE`` statements backfilling
every existing ``actors`` row the hand-curated seed list owns. This file
proves that write against a real database: exactly the seed ids that are
still pristine (untouched since ``SEED_VINTAGE_TS``) get stamped ``'seed'``,
a seed id touched by *something* since seeding -- a maintenance-only
timestamp bump, not a confirmed observation -- is explicitly stamped
``'unconfirmed'`` rather than silently inheriting the column's ``'observed'``
default, every non-seed id keeps that default untouched, a second run is a
no-op, downgrade removes both columns cleanly, and the migration's
``SET LOCAL`` timeouts are proven to reset on the *same* connection once its
own transaction ends, not just observed as already-baseline on a fresh one.
``_KNOWN_ACTORS`` (500+ entries) is monkeypatched to a handful of ids for a
fast, deterministic proof -- the migration's own ``_seed_actor_ids()`` reads
it by name at call time, so patching the module attribute is sufficient.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if
no PostgreSQL is reachable. Every row this file writes uses a unique
``pkt2prov_pg_<uuid>`` id and the autouse fixture deletes them afterward.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

_MIGRATION_MODULE = "migrations.versions.actors_provenance_20260917"

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
        # A table left behind by a version of this file predating the
        # updated_at column still needs it added.
        conn.execute(text(
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS updated_at "
            "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        ))
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


def _insert_actor(
    conn, *, actor_id: str, name: str, updated_at: datetime | None = None,
) -> None:
    if updated_at is None:
        conn.execute(
            text(
                "INSERT INTO actors (id, name, tier, category) "
                "VALUES (:id, :name, 'test', 'test')",
            ).bindparams(id=actor_id, name=name),
        )
    else:
        conn.execute(
            text(
                "INSERT INTO actors (id, name, tier, category, updated_at) "
                "VALUES (:id, :name, 'test', 'test', :updated_at)",
            ).bindparams(id=actor_id, name=name, updated_at=updated_at),
        )


def _run_migration_fn(pg_engine: Engine, migration, fn_name: str) -> None:
    """Run the real ``upgrade``/``downgrade`` function against a live connection."""
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


def _reset_provenance_columns(pg_engine: Engine) -> None:
    """Drop columns a prior run of this test file left behind, so upgrade()
    starts from the same pre-migration shape every time."""
    with pg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance_as_of"))
        conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance"))


def test_upgrade_backfills_exactly_the_seed_ids_and_defaults_everyone_else(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    migration = importlib.import_module(_MIGRATION_MODULE)

    seeded_id = f"pkt2prov_pg_{uuid.uuid4().hex[:16]}"
    observed_id = f"pkt2prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.extend([seeded_id, observed_id])

    with pg_engine.begin() as conn:
        # Pristine seed row: updated_at pinned to the exact seed vintage,
        # exactly what the real _seed_known_actors stamps and nothing since.
        _insert_actor(
            conn, actor_id=seeded_id, name="Seeded Test Actor",
            updated_at=migration.SEED_VINTAGE_TS,
        )
        _insert_actor(conn, actor_id=observed_id, name="Observed Test Actor")

    _reset_provenance_columns(pg_engine)

    import intelligence.actors.seed_data as seed_data

    monkeypatch.setattr(seed_data, "_KNOWN_ACTORS", {seeded_id: {}})

    _run_migration_fn(pg_engine, migration, "upgrade")

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
    _run_migration_fn(pg_engine, migration, "upgrade")

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


def test_upgrade_marks_a_maintenance_touched_seed_id_unconfirmed(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    """Seed-list membership alone is not sufficient for the backfill, and a
    moved ``updated_at`` alone is not proof of a real observation either.

    Simulates the realistic case a repo-wide writer survey actually found:
    something like ``intelligence/actors/trial_bridge.py``'s connection-merge
    path touches a row's ``updated_at`` (and an unrelated field, here
    ``connections``) with no confirmed measurement attached at all -- not
    ``save_actor``'s contract, which always writes real evidence alongside
    the timestamp. The backfill must not call this ``'seed'`` (it is not
    pristine) and must not call it ``'observed'`` either (nothing confirms
    it) -- it must be explicitly ``'unconfirmed'``.
    """
    migration = importlib.import_module(_MIGRATION_MODULE)

    touched_id = f"pkt2prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(touched_id)

    maintenance_touch_time = migration.SEED_VINTAGE_TS + timedelta(days=30)
    with pg_engine.begin() as conn:
        _insert_actor(
            conn, actor_id=touched_id, name="Maintenance Touched Actor",
            updated_at=maintenance_touch_time,
        )

    _reset_provenance_columns(pg_engine)

    import intelligence.actors.seed_data as seed_data

    monkeypatch.setattr(seed_data, "_KNOWN_ACTORS", {touched_id: {}})

    _run_migration_fn(pg_engine, migration, "upgrade")

    with pg_engine.connect() as verify_conn:
        row = dict(verify_conn.execute(
            text(
                "SELECT provenance, provenance_as_of FROM actors WHERE id = :id",
            ).bindparams(id=touched_id),
        ).fetchone()._mapping)

    assert row["provenance"] == migration.PROVENANCE_UNCONFIRMED, (
        "a seed id touched by something other than a confirmed observation "
        "must be explicitly 'unconfirmed', not silently 'observed' and not "
        "relabeled 'seed' just because it is in the static seed list"
    )
    assert row["provenance_as_of"] is None


def test_upgrade_runs_for_real_with_finite_timeouts_scoped_to_its_own_transaction(
    pg_engine: Engine, monkeypatch,
):
    """The real ``upgrade()`` must set its SET LOCAL guards, and those guards
    must never outlive the migration's own transaction -- see
    oracle_pred_nullable_0918 for the precedent this follows.

    The decisive proof is on the *same* connection: SET LOCAL is scoped to
    the transaction, so it must reset the instant that transaction ends,
    before the connection is ever returned or reused. Checking only a fresh
    connection afterward would not actually prove this -- a fresh connection
    always starts at the server/role default regardless of whether SET LOCAL
    ever leaked, so that alone cannot distinguish "scoped correctly" from
    "happened to match by coincidence". Both checks are kept: same-connection
    (decisive) and fresh-connection (confirms no session-wide side effect).
    """
    migration = importlib.import_module(_MIGRATION_MODULE)

    # No seed ids to backfill -- isolates this test to the timeout/DDL
    # behavior, independent of the backfill scenarios covered above.
    import intelligence.actors.seed_data as seed_data

    monkeypatch.setattr(seed_data, "_KNOWN_ACTORS", {})

    _reset_provenance_columns(pg_engine)

    with pg_engine.connect() as baseline_conn:
        baseline_lock_timeout = baseline_conn.execute(
            text("SHOW lock_timeout"),
        ).scalar()
        baseline_statement_timeout = baseline_conn.execute(
            text("SHOW statement_timeout"),
        ).scalar()

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

        # Still in effect INSIDE the transaction that ran the migration.
        assert conn.execute(text("SHOW lock_timeout")).scalar() == (
            migration._LOCK_TIMEOUT
        )
        assert conn.execute(text("SHOW statement_timeout")).scalar() == (
            migration._STATEMENT_TIMEOUT
        )
        trans.commit()

        # Decisive: the SAME connection, immediately after COMMIT, must have
        # reset to whatever it saw before the transaction started -- this is
        # what actually proves SET LOCAL's transaction scoping, not just that
        # some other, unrelated connection happens to look unaffected.
        assert conn.execute(text("SHOW lock_timeout")).scalar() == (
            baseline_lock_timeout
        )
        assert conn.execute(text("SHOW statement_timeout")).scalar() == (
            baseline_statement_timeout
        )
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    # Corroborating, not decisive on its own: a fresh connection/session
    # must also see the ordinary baseline, confirming SET LOCAL had no
    # session-wide or server-wide side effect beyond the one transaction.
    with pg_engine.connect() as after_conn:
        assert after_conn.execute(text("SHOW lock_timeout")).scalar() == (
            baseline_lock_timeout
        )
        assert after_conn.execute(text("SHOW statement_timeout")).scalar() == (
            baseline_statement_timeout
        )

    with pg_engine.connect() as verify_conn:
        cols = verify_conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'actors' "
            "AND column_name IN ('provenance', 'provenance_as_of')",
        )).fetchall()
    assert {c[0] for c in cols} == {"provenance", "provenance_as_of"}


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
