"""Real-PostgreSQL proof for migrations/versions/people_events_20260927.py.

Runs in a throwaway schema (random name, dropped afterwards), following
tests/test_raw_series_quarantined_migration_pg.py's pattern. Proves:

* upgrade creates `people_events` with every column the plan's canonical
  event tuple (section 2.1) and this program's requirements ask for, plus
  its indexes;
* the CHECK constraints actually reject a fabricated/unknown `known_at_basis`,
  `channel`, `actor_id_basis` and `direction`, and NULL `known_at`;
* `UNIQUE (channel, dedup_key)` is a real database constraint, not just
  application logic;
* `echo_of` is a working self-reference;
* upgrade is idempotent (CREATE TABLE/INDEX IF NOT EXISTS) and downgrade
  cleanly drops the table.

Uses the shared `pg_engine` fixture (`GRID_TEST_DB_URL`); skips when no
PostgreSQL is reachable, per this repo's disposable-test-DB convention --
every schema here is uniquely named and dropped in a `finally`, never the
shared `griddb_test` database itself.
"""

from __future__ import annotations

import importlib
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

_MIGRATION_MODULE = "migrations.versions.people_events_20260927"

_EXPECTED_COLUMNS = {
    "id", "channel", "dedup_key", "event_time", "known_at", "known_at_basis",
    "actor_id", "actor_id_basis", "actor_type", "co_actor_ids",
    "entity_ticker", "entity_cik", "security_id", "direction",
    "transaction_code", "size_usd", "source", "source_record_id",
    "source_refs", "n_sources", "echo_of", "provenance", "ingested_at",
}


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"people_events_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
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


def _insert_minimal(engine: Engine, **overrides) -> None:
    values = {
        "channel": "form4",
        "dedup_key": "AAPL|X|2026-09-01|P|100",
        "known_at_basis": "filing",
        "actor_id": "X",
        "actor_id_basis": "normalized_name",
        "actor_type": "insider",
        "source": "quiverquant",
    }
    values.update(overrides)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO people_events "
                "(channel, dedup_key, event_time, known_at, known_at_basis, "
                " actor_id, actor_id_basis, actor_type, source) "
                "VALUES (:channel, :dedup_key, NOW(), NOW(), :known_at_basis, "
                " :actor_id, :actor_id_basis, :actor_type, :source)"
            ),
            values,
        )


def test_upgrade_creates_every_required_column(scratch):
    _run(scratch, "upgrade")
    with scratch.connect() as conn:
        cols = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'people_events'"
                )
            ).fetchall()
        }
    assert cols == _EXPECTED_COLUMNS


def test_upgrade_creates_the_expected_indexes(scratch):
    _run(scratch, "upgrade")
    with scratch.connect() as conn:
        idx = {
            r[0]
            for r in conn.execute(
                text("SELECT indexname FROM pg_indexes WHERE tablename = 'people_events'")
            ).fetchall()
        }
    assert "people_events_pkey" in idx
    assert "idx_people_events_entity_known_at" in idx
    assert "idx_people_events_actor_known_at" in idx
    assert "idx_people_events_channel_known_at" in idx
    assert "idx_people_events_echo_of" in idx
    assert "idx_people_events_security_id" in idx


def test_upgrade_is_idempotent(scratch):
    _run(scratch, "upgrade")
    _run(scratch, "upgrade")  # CREATE TABLE/INDEX IF NOT EXISTS must not error
    with scratch.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM people_events")).scalar()
    assert count == 0


def test_downgrade_drops_the_table(scratch):
    _run(scratch, "upgrade")
    _insert_minimal(scratch)
    _run(scratch, "downgrade")
    with scratch.connect() as conn:
        exists = conn.execute(
            text("SELECT to_regclass('people_events') IS NOT NULL")
        ).scalar()
    assert exists is False


def test_known_at_cannot_be_null(scratch):
    _run(scratch, "upgrade")
    with pytest.raises(DBAPIError):
        with scratch.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO people_events "
                    "(channel, dedup_key, event_time, known_at, known_at_basis, "
                    " actor_id, actor_id_basis, actor_type, source) "
                    "VALUES ('form4', 'x', NOW(), NULL, 'filing', 'x', 'normalized_name', 'insider', 'quiverquant')"
                )
            )


@pytest.mark.parametrize(
    "column,value",
    [
        ("known_at_basis", "made_up"),
        ("channel", "not_a_real_channel"),
        ("actor_id_basis", "made_up"),
    ],
)
def test_check_constraints_reject_unknown_values(scratch, column, value):
    _run(scratch, "upgrade")
    with pytest.raises(DBAPIError):
        _insert_minimal(scratch, **{column: value})


def test_direction_check_rejects_unknown_values_but_allows_null(scratch):
    _run(scratch, "upgrade")
    with scratch.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO people_events "
                "(channel, dedup_key, event_time, known_at, known_at_basis, "
                " actor_id, actor_id_basis, actor_type, source, direction) "
                "VALUES ('form4', 'null-direction', NOW(), NOW(), 'filing', 'x', "
                " 'normalized_name', 'insider', 'quiverquant', NULL)"
            )
        )
    with pytest.raises(DBAPIError):
        with scratch.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO people_events "
                    "(channel, dedup_key, event_time, known_at, known_at_basis, "
                    " actor_id, actor_id_basis, actor_type, source, direction) "
                    "VALUES ('form4', 'bad-direction', NOW(), NOW(), 'filing', 'x', "
                    " 'normalized_name', 'insider', 'quiverquant', 'sideways')"
                )
            )


def test_unique_channel_dedup_key_is_a_real_constraint(scratch):
    _run(scratch, "upgrade")
    _insert_minimal(scratch, dedup_key="dupe")
    with pytest.raises(DBAPIError):
        _insert_minimal(scratch, dedup_key="dupe")


def test_same_dedup_key_on_a_different_channel_does_not_collide(scratch):
    _run(scratch, "upgrade")
    _insert_minimal(scratch, channel="form4", dedup_key="shared-key")
    _insert_minimal(scratch, channel="news", dedup_key="shared-key")
    with scratch.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM people_events")).scalar()
    assert count == 2


def test_echo_of_self_references_and_rejects_a_dangling_id(scratch):
    _run(scratch, "upgrade")
    _insert_minimal(scratch, dedup_key="original")
    with scratch.connect() as conn:
        original_id = conn.execute(text("SELECT id FROM people_events WHERE dedup_key = 'original'")).scalar()

    with scratch.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO people_events "
                "(channel, dedup_key, event_time, known_at, known_at_basis, "
                " actor_id, actor_id_basis, actor_type, source, echo_of) "
                "VALUES ('news', 'echo', NOW(), NOW(), 'publish', 'x', "
                " 'normalized_name', 'news_actor', 'news', :echo_of)"
            ),
            {"echo_of": original_id},
        )

    with pytest.raises(DBAPIError):
        with scratch.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO people_events "
                    "(channel, dedup_key, event_time, known_at, known_at_basis, "
                    " actor_id, actor_id_basis, actor_type, source, echo_of) "
                    "VALUES ('news', 'dangling-echo', NOW(), NOW(), 'publish', 'x', "
                    " 'normalized_name', 'news_actor', 'news', 999999999)"
                )
            )

