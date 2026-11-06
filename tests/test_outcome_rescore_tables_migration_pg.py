"""Real-PostgreSQL proof for migrations/versions/outcome_rescore_tables_20261106.py.

Runs in a throwaway schema (random name, dropped afterwards) with minimal
``signal_sources`` / ``decision_journal`` stand-ins for the foreign keys.
Proves both tables accept INSERT and refuse UPDATE, DELETE and TRUNCATE, and
that the ``grid`` role (when present) holds SELECT and INSERT only.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips when
no PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

_MIGRATION_MODULE = "migrations.versions.outcome_rescore_tables_20261106"
_NOW = datetime(2026, 11, 6, tzinfo=timezone.utc)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"outcome_rescore_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE signal_sources (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE decision_journal (id BIGINT PRIMARY KEY)"))
        conn.execute(text("INSERT INTO signal_sources (id) VALUES (1)"))
        conn.execute(text("INSERT INTO decision_journal (id) VALUES (1)"))
    try:
        yield engine, schema
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _upgrade(engine: Engine) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        trans.commit()


_INSERTS = {
    "signal_source_outcomes": (
        "INSERT INTO signal_source_outcomes (signal_source_id, evaluation_version, source_type, "
        "ticker, signal_date, direction, horizon_days, outcome, dead_band_pct, cost_bps, evaluated_at) "
        "VALUES (1, 'sig-eval-4-sessions', 'scanner', 'AAA', :d, 'BUY', 7, 'NO_MOVE', 1.0, 0.0, :ts)"
    ),
    "decision_journal_verdicts": (
        "INSERT INTO decision_journal_verdicts (decision_id, scorer_version, verdict, outcome_value, "
        "annotation, evaluated_at) VALUES (1, 'journal-verdict-v2-anchor-close', 'NEUTRAL', 0.0, 'x', :ts)"
    ),
}


@pytest.mark.parametrize("table", sorted(_INSERTS))
def test_insert_ok_and_update_delete_truncate_refused(scratch, table):
    engine, _schema = scratch
    _upgrade(engine)
    with engine.begin() as conn:
        conn.execute(text(_INSERTS[table]), {"d": date(2026, 9, 15), "ts": _NOW})
    for stmt in (f"UPDATE {table} SET evaluated_at = now()", f"DELETE FROM {table}", f"TRUNCATE {table}"):
        with pytest.raises(DBAPIError, match="append-only"):
            with engine.begin() as conn:
                conn.execute(text(stmt))
    with engine.connect() as conn:
        assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar() == 1


def test_grid_role_holds_select_insert_only(scratch):
    engine, schema = scratch
    _upgrade(engine)
    with engine.connect() as conn:
        if conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = 'grid'")).scalar() is None:
            pytest.skip("no grid role on this PostgreSQL")
        for table in _INSERTS:
            privs = {r[0] for r in conn.execute(text(
                "SELECT privilege_type FROM information_schema.role_table_grants "
                "WHERE grantee = 'grid' AND table_schema = :s AND table_name = :t"
            ), {"s": schema, "t": table})}
            assert privs == {"SELECT", "INSERT"}, (table, privs)
