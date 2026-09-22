"""Real-PostgreSQL proof for migrations/versions/earnings_pred_move_basis_0918.py.

Unlike test_earnings_predictions_schema_parity.py (which only compares column
lists, no live database needed since this migration never touches a row),
this file proves the real ``upgrade()``/``downgrade()`` functions run
cleanly against a real database, and that the finite ``SET LOCAL
lock_timeout``/``statement_timeout`` guards added alongside
``oracle_pred_nullable_0918``'s precedent never leak past this migration's
own transaction.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if
no PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

_MIGRATION_MODULE = "migrations.versions.earnings_pred_move_basis_0918"

_MINIMAL_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS earnings_predictions (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL
)
"""


@pytest.fixture(autouse=True)
def _earnings_predictions_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_TABLE_DDL))
        # A table left behind by upgrade() in a prior test run already has
        # both columns; drop them so every test starts from the same shape.
        conn.execute(text(
            "ALTER TABLE earnings_predictions DROP COLUMN IF EXISTS expected_move_options",
        ))
        conn.execute(text(
            "ALTER TABLE earnings_predictions DROP COLUMN IF EXISTS predicted_move_basis",
        ))
    yield


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


def test_upgrade_runs_for_real_with_finite_timeouts_scoped_to_its_own_transaction(
    pg_engine: Engine,
):
    """The real ``upgrade()`` -- not a hand-replicated copy of its SQL --
    must run cleanly with its ``SET LOCAL lock_timeout``/``statement_timeout``
    guards, and those guards must never outlive the migration's own
    transaction. Idempotent (``ADD COLUMN IF NOT EXISTS``), so safe to run
    for real against the shared disposable database.

    The decisive proof is on the *same* connection: SET LOCAL is scoped to
    the transaction, so it must reset the instant that transaction ends. A
    fresh connection afterward cannot distinguish "scoped correctly" from
    "happened to match the default regardless" -- it always starts at the
    server/role default either way. Both checks are kept: same-connection
    (decisive) and fresh-connection (confirms no session-wide side effect).
    """
    migration = importlib.import_module(_MIGRATION_MODULE)

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

        # Decisive: the SAME connection, immediately after COMMIT, must be
        # back to what it saw before the transaction started.
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
    # must also see the ordinary baseline.
    with pg_engine.connect() as after_conn:
        assert after_conn.execute(text("SHOW lock_timeout")).scalar() == (
            baseline_lock_timeout
        )
        assert after_conn.execute(text("SHOW statement_timeout")).scalar() == (
            baseline_statement_timeout
        )

    # The migration's real effect still happened.
    with pg_engine.connect() as verify_conn:
        cols = verify_conn.execute(
            text(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name = 'earnings_predictions' "
                "AND column_name IN ('predicted_move_basis', 'expected_move_options')",
            ),
        ).fetchall()
    by_col = {r[0]: r[1] for r in cols}
    assert by_col["predicted_move_basis"] == "YES"
    assert by_col["expected_move_options"] == "YES"

    # Idempotent: running upgrade() again must not raise.
    _run_migration_fn(pg_engine, migration, "upgrade")


def test_downgrade_drops_both_columns_scoped_to_its_own_transaction(pg_engine: Engine):
    """Isolated in a rolled-back transaction -- never touches the shared
    table's real schema."""
    migration = importlib.import_module(_MIGRATION_MODULE)

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        conn.execute(text(
            "ALTER TABLE earnings_predictions ADD COLUMN IF NOT EXISTS "
            "predicted_move_basis TEXT",
        ))
        conn.execute(text(
            "ALTER TABLE earnings_predictions ADD COLUMN IF NOT EXISTS "
            "expected_move_options DOUBLE PRECISION",
        ))

        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            migration.downgrade()
        finally:
            migration.op = real_op

        assert conn.execute(text("SHOW lock_timeout")).scalar() == (
            migration._LOCK_TIMEOUT
        )

        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'earnings_predictions' "
            "AND column_name IN ('predicted_move_basis', 'expected_move_options')",
        )).fetchall()
        assert cols == []
    finally:
        trans.rollback()
        conn.close()
