"""Real-PostgreSQL proof for migrations/versions/eligibility_core_20261122.py (#1301).

Runs in a throwaway schema on the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``); CI runs it in its own
step that fails on any skip. Proves that the upgrade records a pre-image for exactly the rows it changes,
touches no deprecated or unrelated row, and is a no-op when re-run; that downgrade restores each row's old
flag from that pre-image; and that the change log is append-only.
"""

from __future__ import annotations

import importlib
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

_MIGRATION = "migrations.versions.eligibility_core_20261122"

_REGISTRY = """
CREATE TABLE feature_registry (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    model_eligible  BOOLEAN NOT NULL DEFAULT FALSE,
    deprecated_at   TIMESTAMPTZ
);
INSERT INTO feature_registry (id, name, model_eligible, deprecated_at) VALUES
    (1,  'yld_curve_2s10s',  FALSE, NULL),
    (11, 'vix_spot',         FALSE, NULL),
    (12, 'vvix',             TRUE,  NULL),
    (13, 'vix9d_spot',       FALSE, now()),
    (40, 'lunar_phase',      TRUE,  NULL),
    (41, 'ephemeris_sun_lon', TRUE, NULL),
    (42, 'tithi',            TRUE,  now()),
    (50, 'hy_oas_spread',    TRUE,  NULL),
    (51, 'copper',           FALSE, NULL);
"""


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"elig_core_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url.update_query_dict({"options": f"-csearch_path={schema}"}))
    with engine.begin() as conn:
        conn.exec_driver_sql(_REGISTRY)
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION)
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


def _flags(engine: Engine) -> dict[str, bool]:
    with engine.connect() as conn:
        return dict(conn.execute(text("SELECT name, model_eligible FROM feature_registry")).all())


def _log(engine: Engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT feature_id, name, old_eligible, new_eligible, direction "
                                 "FROM feature_eligibility_changes ORDER BY id")).all()


def test_upgrade_records_a_pre_image_for_exactly_what_it_changes(scratch):
    before = _flags(scratch)
    _run(scratch, "upgrade")
    after = _flags(scratch)
    changed = {k for k in before if before[k] != after[k]}
    assert changed == {"yld_curve_2s10s", "vix_spot", "lunar_phase", "ephemeris_sun_lon"}
    assert after["vix_spot"] and after["yld_curve_2s10s"] and not after["lunar_phase"]
    assert not after["vix9d_spot"] and after["tithi"]          # deprecated rows untouched
    assert after["hy_oas_spread"] and not after["copper"]      # unrelated rows untouched
    assert sorted(_log(scratch)) == sorted([
        (1, "yld_curve_2s10s", False, True, "upgrade"), (11, "vix_spot", False, True, "upgrade"),
        (40, "lunar_phase", True, False, "upgrade"), (41, "ephemeris_sun_lon", True, False, "upgrade")])

    _run(scratch, "upgrade")                                   # a re-run changes and records nothing
    assert _flags(scratch) == after and len(_log(scratch)) == 4


def test_downgrade_restores_each_old_flag_from_the_pre_image(scratch):
    before = _flags(scratch)
    _run(scratch, "upgrade")
    with scratch.begin() as conn:                              # someone already put one back by hand
        conn.execute(text("UPDATE feature_registry SET model_eligible = TRUE WHERE name = 'lunar_phase'"))
    _run(scratch, "downgrade")
    assert _flags(scratch) == before
    downgrades = [r for r in _log(scratch) if r.direction == "downgrade"]
    assert sorted((r.name, r.old_eligible, r.new_eligible) for r in downgrades) == [
        ("ephemeris_sun_lon", False, True), ("vix_spot", True, False), ("yld_curve_2s10s", True, False)]


@pytest.mark.parametrize("statement", [
    "UPDATE feature_eligibility_changes SET new_eligible = NOT new_eligible",
    "DELETE FROM feature_eligibility_changes",
    "TRUNCATE feature_eligibility_changes",
])
def test_the_change_log_is_append_only(scratch, statement):
    _run(scratch, "upgrade")
    with pytest.raises(DBAPIError, match="append-only"):
        with scratch.begin() as conn:
            conn.execute(text(statement))
    assert len(_log(scratch)) == 4
