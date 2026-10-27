"""Real-PostgreSQL proof for migrations/versions/raw_series_quarantined_20260926.py.

Runs in a throwaway schema (random name, dropped afterwards) holding a
minimal ``raw_series`` created with schema.sql's original inline CHECK, so the
constraint gets the same generated name production has
(``raw_series_pull_status_check``). Proves:

* upgrade leaves exactly one pull_status CHECK, under the original name,
  allowing QUARANTINED, NOT VALID (no scan), and still enforced on writes;
* the SET LOCAL timeouts are scoped to the migration's own transaction;
* ``scripts.hermes_health.check_db_health`` does not count QUARANTINED rows as
  failed pulls or as the latest successful pull, and reports them separately;
* downgrade refuses while QUARANTINED rows exist, and otherwise restores the
  three-value constraint.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips when
no PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

_MIGRATION_MODULE = "migrations.versions.raw_series_quarantined_20260926"

# schema.sql's raw_series before this change (FK to source_catalog omitted: it
# is irrelevant to the status constraint and would need a second table).
_OLD_RAW_SERIES_DDL = """
CREATE TABLE raw_series (
    id                BIGSERIAL PRIMARY KEY,
    series_id         TEXT NOT NULL,
    source_id         INTEGER NOT NULL,
    obs_date          DATE NOT NULL,
    pull_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    value             DOUBLE PRECISION NOT NULL,
    raw_payload       JSONB,
    pull_status       TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED'))
)
"""

# check_db_health also reads source freshness; only these columns are
# touched. `id` and `latency_class` were added for the cadence-aware
# staleness fix (GRID-STALE-SOURCES-AUDIT-20260929.md): check_db_health now
# LATERAL-joins raw_series on source_catalog.id and reads latency_class as
# a cadence fallback signal. This table stays empty in every test below, so
# the LATERAL join never matches a row either way -- these columns just
# need to exist for the query to parse.
_SOURCE_CATALOG_DDL = """
CREATE TABLE source_catalog (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    last_pull_at  TIMESTAMPTZ,
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    latency_class TEXT NOT NULL DEFAULT 'EOD'
)
"""

_CONSTRAINTS_SQL = text(
    "SELECT conname, convalidated, pg_get_constraintdef(oid) "
    "FROM pg_constraint "
    "WHERE conrelid = 'raw_series'::regclass AND contype = 'c' "
    "ORDER BY conname"
)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"quarantine_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        pg_engine.url, connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(_OLD_RAW_SERIES_DDL))
            conn.execute(text(_SOURCE_CATALOG_DDL))
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


def _insert(engine: Engine, status: str, *, hours_ago: float = 0.0, obs_date: str = "2026-09-25") -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
                "VALUES ('YF:SPY:close', 1, :d, NOW() - make_interval(secs => :s), 600.0, :st)"
            ),
            {"d": obs_date, "s": hours_ago * 3600.0, "st": status},
        )


def test_upgrade_swaps_in_a_not_valid_constraint_under_the_original_name(scratch):
    with scratch.connect() as conn:
        before = conn.execute(_CONSTRAINTS_SQL).fetchall()
    assert [r[0] for r in before] == ["raw_series_pull_status_check"]

    _run(scratch, "upgrade")

    with scratch.connect() as conn:
        after = conn.execute(_CONSTRAINTS_SQL).fetchall()
    assert len(after) == 1
    name, validated, definition = after[0]
    assert name == "raw_series_pull_status_check"
    assert validated is False, "must be added NOT VALID: no scan of existing rows"
    assert "QUARANTINED" in definition and "NOT VALID" in definition

    _insert(scratch, "QUARANTINED")
    with pytest.raises(DBAPIError):
        _insert(scratch, "BOGUS")  # still enforced for new writes


def test_upgrade_timeouts_are_scoped_to_its_own_transaction(scratch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
    with scratch.connect() as conn:
        baseline = (
            conn.execute(text("SHOW lock_timeout")).scalar(),
            conn.execute(text("SHOW statement_timeout")).scalar(),
        )
        conn.rollback()
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        assert conn.execute(text("SHOW lock_timeout")).scalar() == migration._LOCK_TIMEOUT
        assert conn.execute(text("SHOW statement_timeout")).scalar() == migration._STATEMENT_TIMEOUT
        trans.commit()
        assert (
            conn.execute(text("SHOW lock_timeout")).scalar(),
            conn.execute(text("SHOW statement_timeout")).scalar(),
        ) == baseline


def test_quarantined_rows_are_not_failures_and_are_reported_separately(scratch, monkeypatch):
    from scripts import hermes_health

    _run(scratch, "upgrade")
    _insert(scratch, "SUCCESS", hours_ago=5, obs_date="2026-09-24")
    _insert(scratch, "FAILED", hours_ago=2)
    for i in range(3):
        _insert(scratch, "QUARANTINED", hours_ago=0.1 * (i + 1), obs_date=f"2026-09-2{i + 1}")

    health = hermes_health.check_db_health(scratch)

    assert health.get("error") is None, health.get("error")
    assert health["failed_pulls_1h"] == 0
    assert health["failed_pulls_24h"] == 1
    assert health["quarantined_rows"] == 3
    assert health["quarantined_rows_capped"] is False
    # latest_pull is the SUCCESS row (5h ago), not the newer quarantined ones.
    with scratch.connect() as conn:
        success_ts = conn.execute(text(
            "SELECT pull_timestamp FROM raw_series WHERE pull_status = 'SUCCESS'"
        )).scalar()
    assert health["latest_pull"] == success_ts.isoformat()

    monkeypatch.setattr(hermes_health, "QUARANTINED_COUNT_CAP", 2)
    capped = hermes_health.check_db_health(scratch)
    assert capped["quarantined_rows"] == 2 and capped["quarantined_rows_capped"] is True


def test_downgrade_refuses_while_quarantined_rows_exist_then_restores(scratch):
    _run(scratch, "upgrade")
    _insert(scratch, "QUARANTINED")

    with pytest.raises(DBAPIError, match="QUARANTINED rows"):
        _run(scratch, "downgrade")
    with scratch.connect() as conn:  # rolled back: still the upgraded constraint
        (_, _, definition), = conn.execute(_CONSTRAINTS_SQL).fetchall()
    assert "QUARANTINED" in definition

    with scratch.begin() as conn:  # stands in for restoring from the backup
        conn.execute(text(
            "UPDATE raw_series SET pull_status = 'SUCCESS' WHERE pull_status = 'QUARANTINED'"
        ))
    _run(scratch, "downgrade")

    with scratch.connect() as conn:
        rows = conn.execute(_CONSTRAINTS_SQL).fetchall()
    assert len(rows) == 1
    name, validated, definition = rows[0]
    assert name == "raw_series_pull_status_check"
    assert validated is False
    assert "QUARANTINED" not in definition
    with pytest.raises(DBAPIError):
        _insert(scratch, "QUARANTINED")


def test_upgrade_is_safe_on_a_database_built_from_the_new_schema_sql(scratch):
    """A fresh DB created from the updated schema.sql already allows
    QUARANTINED; running the migration there must still leave one
    constraint under the canonical name."""
    _run(scratch, "upgrade")
    _run(scratch, "upgrade")
    with scratch.connect() as conn:
        rows = conn.execute(_CONSTRAINTS_SQL).fetchall()
    assert [r[0] for r in rows] == ["raw_series_pull_status_check"]
    assert "QUARANTINED" in rows[0][2]
