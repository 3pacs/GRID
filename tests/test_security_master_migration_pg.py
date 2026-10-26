"""Real-PostgreSQL proof for migrations/versions/security_master_20260927.py (GD1).

Runs the real migration upgrade()/downgrade() in a throwaway schema (random
name, dropped afterwards) against a live PostgreSQL, following the same
pattern as tests/test_raw_series_quarantined_migration_pg.py and
tests/test_godview_schema_migration_pg.py. Proves:

* all three tables (security_master, security_identifiers,
  security_sector_membership) exist after upgrade with their expected
  columns;
* security_identifiers.entity_id and security_sector_membership.entity_id
  are FKs onto security_master.entity_id with ON DELETE CASCADE -- deleting
  an entity actually cascades, it isn't just declared;
* the JSONB columns (provenance, conflict_detail x2) round-trip real JSON;
* the three partial indexes exist with their documented predicates
  (idx_security_master_cik WHERE cik IS NOT NULL,
  idx_security_identifiers_conflict WHERE conflict_flag,
  idx_sector_membership_primary WHERE is_primary);
* downgrade cleanly drops all three tables (reverse dependency order), and
  upgrade is safe to run again afterwards.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips
when no PostgreSQL is reachable (the dedicated CI step fails on a skip).
"""

from __future__ import annotations

import importlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_MIGRATION_MODULE = "migrations.versions.security_master_20260927"
_TABLES = ("security_master", "security_identifiers", "security_sector_membership")

_COLUMNS_SQL = text(
    "SELECT table_name, column_name, data_type, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = ANY(:tables) "
    "ORDER BY table_name, ordinal_position"
)

_INDEXES_SQL = text(
    "SELECT indexname, indexdef FROM pg_indexes "
    "WHERE schemaname = current_schema() AND tablename = ANY(:tables) "
    "ORDER BY indexname"
)

_FK_SQL = text(
    "SELECT conname, confdeltype, "
    "       (SELECT relname FROM pg_class WHERE oid = conrelid) AS child_table, "
    "       (SELECT relname FROM pg_class WHERE oid = confrelid) AS parent_table "
    "FROM pg_constraint "
    "WHERE contype = 'f' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) "
    "ORDER BY conname"
)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"security_master_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        pg_engine.url, connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            real_op = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                getattr(migration, fn_name)()
            finally:
                migration.op = real_op
            trans.commit()
        except Exception:
            trans.rollback()
            raise


def _tables(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        return {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = current_schema() AND table_name = ANY(:tables)"
                ),
                {"tables": list(_TABLES)},
            )
        }


def _columns(engine: Engine) -> list[tuple]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(_COLUMNS_SQL, {"tables": list(_TABLES)})]


def _indexdefs(engine: Engine) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(_INDEXES_SQL, {"tables": list(_TABLES)}).fetchall()
    return {r[0]: r[1] for r in rows}


def _insert_entity(conn, entity_id: str, name: str = "Test Corp", source: str = "test") -> None:
    conn.execute(
        text(
            "INSERT INTO security_master (entity_id, name, source) VALUES (:e, :n, :s)"
        ),
        {"e": entity_id, "n": name, "s": source},
    )


# --------------------------------------------------------------------- upgrade


def test_upgrade_creates_all_three_tables(scratch):
    assert _tables(scratch) == set()
    _run(scratch, "upgrade")
    assert _tables(scratch) == set(_TABLES)


def test_upgrade_leaves_the_expected_columns(scratch):
    _run(scratch, "upgrade")
    columns = {(t, c) for t, c, _, _ in _columns(scratch)}

    for col in (
        "entity_id", "cik", "name", "security_type", "is_active", "delisted_at",
        "delisted_reason", "delisted_basis", "sic", "source", "provenance",
        "created_at", "updated_at",
    ):
        assert ("security_master", col) in columns, col

    for col in (
        "id", "entity_id", "id_scheme", "id_value", "valid_from", "valid_to",
        "is_primary", "source", "conflict_flag", "conflict_detail", "created_at",
    ):
        assert ("security_identifiers", col) in columns, col

    for col in (
        "id", "entity_id", "taxonomy", "sector", "subsector", "is_primary",
        "tie_break_method", "weight", "source", "conflict_flag", "conflict_detail",
        "valid_from", "valid_to", "created_at",
    ):
        assert ("security_sector_membership", col) in columns, col


def test_upgrade_is_idempotent(scratch):
    _run(scratch, "upgrade")
    first = _columns(scratch)
    _run(scratch, "upgrade")
    assert _columns(scratch) == first


# ---------------------------------------------------------------- foreign keys


def test_child_tables_fk_onto_security_master_with_cascade_delete(scratch):
    _run(scratch, "upgrade")

    with scratch.connect() as conn:
        fks = conn.execute(_FK_SQL).fetchall()
    by_child = {row.child_table: row for row in fks}

    assert by_child["security_identifiers"].parent_table == "security_master"
    assert by_child["security_identifiers"].confdeltype == "c"  # 'c' == CASCADE
    assert by_child["security_sector_membership"].parent_table == "security_master"
    assert by_child["security_sector_membership"].confdeltype == "c"

    entity_id = "corp_TEST"
    with scratch.begin() as conn:
        _insert_entity(conn, entity_id)
        conn.execute(
            text(
                "INSERT INTO security_identifiers "
                "(entity_id, id_scheme, id_value, valid_from, source) "
                "VALUES (:e, 'ticker', 'TEST', '2026-01-01', 'test')"
            ),
            {"e": entity_id},
        )
        conn.execute(
            text(
                "INSERT INTO security_sector_membership "
                "(entity_id, sector, valid_from, source) "
                "VALUES (:e, 'Technology', '2026-01-01', 'test')"
            ),
            {"e": entity_id},
        )

    with scratch.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM security_identifiers")).scalar() == 1
        assert conn.execute(text("SELECT count(*) FROM security_sector_membership")).scalar() == 1

    # Deleting the parent entity must cascade to both children -- not just
    # be declared, but actually enforced.
    with scratch.begin() as conn:
        conn.execute(text("DELETE FROM security_master WHERE entity_id = :e"), {"e": entity_id})

    with scratch.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM security_identifiers")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM security_sector_membership")).scalar() == 0


# --------------------------------------------------------------------- JSONB


def test_jsonb_columns_round_trip_real_json(scratch):
    _run(scratch, "upgrade")
    entity_id = "corp_JSON"
    provenance = {"seed": "sec_company_tickers", "as_of": "2026-09-27"}
    conflict_detail = {"weights": {"Technology": 0.7, "Industrials": 0.3}}

    with scratch.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO security_master (entity_id, name, source, provenance) "
                "VALUES (:e, 'JSON Corp', 'test', CAST(:p AS JSONB))"
            ),
            {"e": entity_id, "p": json.dumps(provenance)},
        )
        conn.execute(
            text(
                "INSERT INTO security_identifiers "
                "(entity_id, id_scheme, id_value, valid_from, source, conflict_flag, conflict_detail) "
                "VALUES (:e, 'ticker', 'JSON', '2026-01-01', 'test', TRUE, CAST(:d AS JSONB))"
            ),
            {"e": entity_id, "d": json.dumps(conflict_detail)},
        )
        conn.execute(
            text(
                "INSERT INTO security_sector_membership "
                "(entity_id, sector, valid_from, source, conflict_flag, conflict_detail) "
                "VALUES (:e, 'Technology', '2026-01-01', 'test', TRUE, CAST(:d AS JSONB))"
            ),
            {"e": entity_id, "d": json.dumps(conflict_detail)},
        )

    with scratch.connect() as conn:
        got_provenance = conn.execute(
            text("SELECT provenance FROM security_master WHERE entity_id = :e"), {"e": entity_id}
        ).scalar()
        got_identifier_detail = conn.execute(
            text("SELECT conflict_detail FROM security_identifiers WHERE entity_id = :e"), {"e": entity_id}
        ).scalar()
        got_sector_detail = conn.execute(
            text("SELECT conflict_detail FROM security_sector_membership WHERE entity_id = :e"), {"e": entity_id}
        ).scalar()

    assert got_provenance == provenance
    assert got_identifier_detail == conflict_detail
    assert got_sector_detail == conflict_detail


def test_security_master_provenance_defaults_to_empty_object(scratch):
    _run(scratch, "upgrade")
    entity_id = "corp_DEFAULT"
    with scratch.begin() as conn:
        _insert_entity(conn, entity_id)
    with scratch.connect() as conn:
        provenance = conn.execute(
            text("SELECT provenance FROM security_master WHERE entity_id = :e"), {"e": entity_id}
        ).scalar()
    assert provenance == {}


# ------------------------------------------------------------------ indexes


def test_partial_indexes_exist_with_the_documented_predicates(scratch):
    _run(scratch, "upgrade")
    indexdefs = _indexdefs(scratch)

    assert "idx_security_master_cik" in indexdefs
    assert "cik IS NOT NULL" in indexdefs["idx_security_master_cik"]

    assert "idx_security_identifiers_conflict" in indexdefs
    assert "conflict_flag" in indexdefs["idx_security_identifiers_conflict"]
    assert "WHERE" in indexdefs["idx_security_identifiers_conflict"]

    assert "idx_sector_membership_primary" in indexdefs
    assert "is_primary" in indexdefs["idx_sector_membership_primary"]
    assert "WHERE" in indexdefs["idx_sector_membership_primary"]


def test_cik_partial_index_still_allows_multiple_null_ciks(scratch):
    """The unique index is WHERE cik IS NOT NULL -- multiple NULLs (companies
    without a resolved CIK yet) must not collide."""
    _run(scratch, "upgrade")
    with scratch.begin() as conn:
        _insert_entity(conn, "corp_NOCIK1")
        _insert_entity(conn, "corp_NOCIK2")
    with scratch.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM security_master WHERE cik IS NULL")).scalar()
    assert count == 2


# ---------------------------------------------------------------- downgrade


def test_downgrade_drops_all_three_tables(scratch):
    _run(scratch, "upgrade")
    entity_id = "corp_DOWNGRADE"
    with scratch.begin() as conn:
        _insert_entity(conn, entity_id)
        conn.execute(
            text(
                "INSERT INTO security_identifiers "
                "(entity_id, id_scheme, id_value, valid_from, source) "
                "VALUES (:e, 'ticker', 'DG', '2026-01-01', 'test')"
            ),
            {"e": entity_id},
        )
        conn.execute(
            text(
                "INSERT INTO security_sector_membership "
                "(entity_id, sector, valid_from, source) "
                "VALUES (:e, 'Technology', '2026-01-01', 'test')"
            ),
            {"e": entity_id},
        )

    _run(scratch, "downgrade")

    assert _tables(scratch) == set()


def test_upgrade_after_downgrade_is_clean(scratch):
    _run(scratch, "upgrade")
    _run(scratch, "downgrade")
    _run(scratch, "upgrade")
    assert _tables(scratch) == set(_TABLES)
