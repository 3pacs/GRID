"""Real-PostgreSQL proof for the eligibility change log migration (#1301).

Covers ``migrations/versions/eligibility_core_20261122.py``. Nothing in this release writes to the log; the
tool that changes flags is a separate PR. Each test gets a throwaway schema and two throwaway roles on the
shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``): ``owner`` runs the migration (migration authority) and
``app`` stands in for the application role ``grid`` and owns ``feature_registry`` as ``grid`` does on griddb.
CI runs this file in its own step that fails on any skip. It proves:

* the migration creates only the log, its triggers and grants, changes no flag, and leaves ``app`` SELECT only,
  even under default privileges that would grant it everything; a re-run is harmless;
* the log is append-only, even for its owner, and its constraints refuse a row that is not a change, a
  rollback that names no set, an apply that names one, a blank plan, and a feature twice in one set;
* downgrade drops an empty log and its function, and refuses once a change is recorded;
* when ``app`` itself runs the migration (griddb's deploy), the upgrade still succeeds, ``app`` owns the log,
  and the deploy log carries a warning with the ownership statements.
"""

from __future__ import annotations

import importlib
import json
import logging
import uuid
from dataclasses import dataclass

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, ProgrammingError

_MIGRATION = "migrations.versions.eligibility_core_20261122"

_REGISTRY = """
CREATE TABLE feature_registry (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    model_eligible  BOOLEAN NOT NULL DEFAULT FALSE,
    deprecated_at   TIMESTAMPTZ
);
INSERT INTO feature_registry (id, name, model_eligible, deprecated_at) VALUES
    (1,  'yld_curve_2s10s',   FALSE, NULL),
    (11, 'vix_spot',          FALSE, NULL),
    (12, 'vvix',              TRUE,  NULL),
    (40, 'lunar_phase',       TRUE,  NULL);
"""

_SET_TEMPLATE = text("SELECT set_config('elig_test.template', :template, true), "
                     "set_config('elig_test.args', :args, true)")
_EXECUTE_TEMPLATE = text("""
DO $$
BEGIN
    EXECUTE format(current_setting('elig_test.template'),
                   VARIADIC ARRAY(SELECT json_array_elements_text(current_setting('elig_test.args')::json)));
END
$$
""")

_RECORD = text(
    "INSERT INTO feature_eligibility_changes (change_set, action, rolls_back, feature_id, name, old_eligible, "
    "new_eligible, plan) VALUES (:change_set, :action, :rolls_back, :feature_id, :name, :old, :new, :plan)")


def _ddl(conn: Connection, template: str, *identifiers: str) -> None:
    """Run a literal template whose %I placeholders PostgreSQL fills (quoted) from bound identifiers."""
    conn.execute(_SET_TEMPLATE, {"template": template, "args": json.dumps(identifiers)})
    conn.execute(_EXECUTE_TEMPLATE)


@dataclass
class Db:
    schema: str
    owner_role: str
    app_role: str
    admin: Engine  # the test server's superuser
    owner: Engine
    app: Engine


@pytest.fixture()
def db(pg_engine: Engine, monkeypatch):
    tag = uuid.uuid4().hex[:10]
    schema, owner, app = f"elig_{tag}", f"elig_owner_{tag}", f"elig_app_{tag}"
    with pg_engine.begin() as conn:
        _ddl(conn, "CREATE SCHEMA %I", schema)
        _ddl(conn, "CREATE ROLE %I NOLOGIN NOSUPERUSER NOINHERIT", owner)
        _ddl(conn, "CREATE ROLE %I NOLOGIN NOSUPERUSER NOINHERIT", app)
        _ddl(conn, "GRANT USAGE, CREATE ON SCHEMA %I TO %I, %I", schema, owner, app)

    def engine(role: str | None) -> Engine:
        options = "-csearch_path=" + schema + ("" if role is None else " -crole=" + role)
        return create_engine(pg_engine.url.update_query_dict({"options": options}))

    handle = Db(schema, owner, app, engine(None), engine(owner), engine(app))
    with handle.app.begin() as conn:  # feature_registry belongs to the application role, as on griddb
        conn.exec_driver_sql(_REGISTRY)
    monkeypatch.setattr(importlib.import_module(_MIGRATION), "APP_ROLE", app)
    try:
        yield handle
    finally:
        for e in (handle.admin, handle.owner, handle.app):
            e.dispose()
        with pg_engine.begin() as conn:
            _ddl(conn, "DROP SCHEMA IF EXISTS %I CASCADE", schema)
            for role in (app, owner):
                _ddl(conn, "DROP OWNED BY %I", role)
                _ddl(conn, "DROP ROLE %I", role)


def _migrate(engine: Engine, fn_name: str = "upgrade") -> None:
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
        return conn.execute(text(
            "SELECT change_set::text AS change_set, action, rolls_back::text AS rolls_back, feature_id, name, "
            "old_eligible, new_eligible, plan, recorded_by FROM feature_eligibility_changes ORDER BY id")).all()


def _row(**overrides) -> dict:
    row = {"change_set": str(uuid.uuid4()), "action": "apply", "rolls_back": None, "feature_id": 11,
           "name": "vix_spot", "old": False, "new": True, "plan": "test plan"}
    return {**row, **overrides}


def _record(engine: Engine, *rows: dict) -> None:
    """Write log rows directly, as the owner: no writer ships in this release."""
    with engine.begin() as conn:
        for row in rows:
            conn.execute(_RECORD, row)


def _as_app(db: Db, sql: str) -> None:
    with db.app.begin() as conn:
        conn.execute(text(sql))


# ── the migration ───────────────────────────────────────────────────────────────────────────────────────────


def test_upgrade_creates_only_the_log_and_changes_no_flag(db):
    before = _flags(db.admin)
    _migrate(db.owner)
    _migrate(db.owner)  # a re-run is harmless
    assert _flags(db.admin) == before
    assert _log(db.admin) == []
    with db.admin.connect() as conn:
        objects = conn.execute(text(
            "SELECT c.relname::text, c.relkind::text, pg_get_userbyid(c.relowner) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = :s ORDER BY 1"), {"s": db.schema}).all()
        triggers = conn.execute(text(
            "SELECT tgname::text FROM pg_trigger WHERE tgrelid = 'feature_eligibility_changes'::regclass "
            "AND NOT tgisinternal ORDER BY 1")).scalars().all()
        change_set_type = conn.execute(text(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = :s "
            "AND table_name = 'feature_eligibility_changes' AND column_name = 'change_set'"), {"s": db.schema}).scalar()
    log_objects = {tuple(o) for o in objects if o[0].startswith("feature_eligibility_changes")}
    assert log_objects == {
        ("feature_eligibility_changes", "r", db.owner_role),
        ("feature_eligibility_changes_id_seq", "S", db.owner_role),
        ("feature_eligibility_changes_pkey", "i", db.owner_role),
        ("feature_eligibility_changes_one_row_per_feature", "i", db.owner_role),
    }
    assert triggers == sorted(importlib.import_module(_MIGRATION).TRIGGER_NAMES)
    assert change_set_type == "uuid"


def test_the_app_role_can_read_the_log_but_never_write_it(db):
    with db.owner.begin() as conn:  # default privileges that would hand the app role everything
        _ddl(conn, "ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO %I", db.schema, db.app_role)
        _ddl(conn, "ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON SEQUENCES TO %I", db.schema, db.app_role)
    _migrate(db.owner)
    with db.app.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM feature_eligibility_changes")).scalar() == 0
    forged = ("INSERT INTO feature_eligibility_changes (change_set, action, feature_id, name, old_eligible, "
              "new_eligible, plan) VALUES (gen_random_uuid(), 'apply', 11, 'vix_spot', TRUE, FALSE, 'forged')")
    for statement in (forged, "SELECT nextval('feature_eligibility_changes_id_seq')",
                      "UPDATE feature_eligibility_changes SET plan = 'x'", "TRUNCATE feature_eligibility_changes"):
        with pytest.raises(ProgrammingError, match="permission denied"):
            _as_app(db, statement)
    with db.admin.connect() as conn:
        grants = conn.execute(text(
            "SELECT CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(g.grantee) END, g.privilege_type "
            "FROM pg_class c, aclexplode(c.relacl) g WHERE c.oid = 'feature_eligibility_changes'::regclass "
            "AND g.grantee <> c.relowner")).all()
        seq_grants = conn.execute(text(
            "SELECT count(*) FROM pg_class c, aclexplode(c.relacl) g "
            "WHERE c.oid = 'feature_eligibility_changes_id_seq'::regclass AND g.grantee <> c.relowner")).scalar()
    assert [tuple(g) for g in grants] == [(db.app_role, "SELECT")]
    assert seq_grants == 0


@pytest.mark.parametrize("statement", [
    "UPDATE feature_eligibility_changes SET new_eligible = NOT new_eligible",
    "DELETE FROM feature_eligibility_changes",
    "TRUNCATE feature_eligibility_changes",
])
def test_the_log_is_append_only_even_for_its_owner(db, statement):
    _migrate(db.owner)
    _record(db.owner, _row(), _row(feature_id=1, name="yld_curve_2s10s"))
    with pytest.raises(DBAPIError, match="append-only"):
        with db.owner.begin() as conn:
            conn.execute(text(statement))
    assert len(_log(db.admin)) == 2


def test_a_recorded_row_carries_its_writer(db):
    _migrate(db.owner)
    _record(db.owner, _row())
    (row,) = _log(db.admin)
    assert (row.action, row.feature_id, row.name, row.old_eligible, row.new_eligible, row.recorded_by) == (
        "apply", 11, "vix_spot", False, True, db.owner_role)


_SET = str(uuid.uuid4())


@pytest.mark.parametrize("rows, constraint", [
    ([_row(old=True, new=True)], "feature_eligibility_changes_is_a_change"),
    ([_row(action="rollback")], "feature_eligibility_changes_rollback_names_its_set"),
    ([_row(rolls_back=str(uuid.uuid4()))], "feature_eligibility_changes_rollback_names_its_set"),
    ([_row(plan="  ")], "feature_eligibility_changes_plan_check"),
    ([_row(action="flip")], "feature_eligibility_changes_action_check"),
    ([_row(change_set=_SET), _row(change_set=_SET, old=True, new=False)],
     "feature_eligibility_changes_one_row_per_feature"),
])
def test_the_log_refuses_a_row_that_is_not_a_well_formed_change(db, rows, constraint):
    _migrate(db.owner)
    with pytest.raises(DBAPIError, match=constraint):
        _record(db.owner, *rows)
    assert _log(db.admin) == []


def test_downgrade_drops_an_empty_log_and_refuses_once_a_change_is_recorded(db):
    _migrate(db.owner)
    _migrate(db.owner, "downgrade")
    with db.admin.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('feature_eligibility_changes')")).scalar() is None
        assert conn.execute(text("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                                 "WHERE p.proname = :f AND n.nspname = :s"),
                            {"f": "feature_eligibility_changes_append_only", "s": db.schema}).scalar() == 0
    _migrate(db.owner)
    _record(db.owner, _row())
    with pytest.raises(DBAPIError, match="downgrade refused"):
        _migrate(db.owner, "downgrade")
    assert len(_log(db.admin)) == 1


def test_an_app_run_migration_succeeds_and_warns_with_the_ownership_statements(db, caplog):
    """griddb's deploy runs alembic as the application role, which then owns the log."""
    before = _flags(db.admin)
    with caplog.at_level(logging.WARNING, logger="alembic.runtime.migration"):
        _migrate(db.app)
    assert _flags(db.admin) == before
    migration = importlib.import_module(_MIGRATION)
    (warning,) = [r.getMessage() for r in caplog.records if "could write pre-images" in r.getMessage()]
    assert all(statement in warning for statement in migration.OWNERSHIP_FIX_SQL)
    with db.admin.connect() as conn:
        owners = conn.execute(text(
            "SELECT pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.oid = 'feature_eligibility_changes'::regclass "
            "UNION ALL SELECT pg_get_userbyid(p.proowner) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE p.proname = 'feature_eligibility_changes_append_only' AND n.nspname = :s"), {"s": db.schema}
        ).scalars().all()
    assert owners == [db.app_role, db.app_role]


def test_an_owner_run_migration_does_not_warn(db, caplog):
    with caplog.at_level(logging.WARNING, logger="alembic.runtime.migration"):
        _migrate(db.owner)
    assert not [r for r in caplog.records if "could write pre-images" in r.getMessage()]
