"""Real-PostgreSQL proof for ``scripts/apply_eligibility_change.py`` (#1301).

The migration's own contract (only the log, triggers and grants; append-only; constraints; downgrade) is
``tests/test_eligibility_core_migration_pg.py``. This file proves the apply tool on the same schema. Each test
gets a throwaway schema and three throwaway roles on the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``):
``owner`` runs the migration (migration authority), ``app`` stands in for the application role ``grid`` and owns
``feature_registry`` as ``grid`` does on griddb, and ``op`` is a member of ``owner``. CI runs this file in its own
step that fails on any skip. It proves:

* when ``app`` itself runs the migration (griddb's deploy), every write is refused until the listed ownership
  statements are run;
* ``--apply`` refuses while the prerequisites are missing (as today), and every write refuses without migration
  authority, when ``app`` holds a write privilege, or when a trigger runs a function ``app`` controls;
* an apply records one change set and flips only rows whose flag differs; a rollback restores only its own set,
  is recorded, happens once, and is refused whole on any divergent row; the coordinator's case: rolling back a
  second set that left vix_spot alone leaves vix_spot enabled;
* the log is append-only after a real apply, and downgrade refuses once an apply recorded a set;
* blockers and bounded lock waits refuse before any write;
* the write boundary (#1383): an apply holds the deploy lock from before the change until after the commit and
  re-checks the evidence right before the commit, so a repin or restart that lands after the first probe refuses
  with nothing written; a busy deploy lock refuses; a different backend refuses; a receipt writes once;
* rollback authorization (#1383): a rollback writes only an approved rollback receipt (its rows, its backend, its
  change set), a changed backend or preview refuses, and it works while the forward prerequisites fail.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, ProgrammingError

from scripts import apply_eligibility_change as elig
from tests.eligibility_fakes import FakeHost

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
    (13, 'vix9d_spot',        FALSE, now()),
    (40, 'lunar_phase',       TRUE,  NULL),
    (41, 'ephemeris_sun_lon', TRUE,  NULL),
    (42, 'tithi',             TRUE,  now()),
    (50, 'hy_oas_spread',     TRUE,  NULL),
    (51, 'copper',            FALSE, NULL);
"""

# What the plan changes on that registry (deprecated vix9d_spot/tithi and unrelated rows stay as they are).
FIRST_SET = [(1, "yld_curve_2s10s", False, True), (11, "vix_spot", False, True),
             (40, "lunar_phase", True, False), (41, "ephemeris_sun_lon", True, False)]

PASSING_GATE = {"ok": True, "refusals": [], "checks": [], "notes": ["test: prerequisites treated as deployed"]}

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


def _ddl(conn: Connection, template: str, *identifiers: str) -> None:
    """Run a literal template whose %I placeholders PostgreSQL fills (quoted) from bound identifiers."""
    conn.execute(_SET_TEMPLATE, {"template": template, "args": json.dumps(identifiers)})
    conn.execute(_EXECUTE_TEMPLATE)


@dataclass
class Db:
    schema: str
    owner_role: str
    app_role: str
    op_role: str
    admin: Engine  # the test server's superuser
    owner: Engine
    app: Engine
    op: Engine

    def url(self, role: str) -> str:
        engine = {self.owner_role: self.owner, self.app_role: self.app, self.op_role: self.op}[role]
        return engine.url.render_as_string(hide_password=False)


@pytest.fixture()
def db(pg_engine: Engine, monkeypatch):
    tag = uuid.uuid4().hex[:10]
    schema, owner, app, op = f"elig_{tag}", f"elig_owner_{tag}", f"elig_app_{tag}", f"elig_op_{tag}"
    with pg_engine.begin() as conn:
        _ddl(conn, "CREATE SCHEMA %I", schema)
        _ddl(conn, "CREATE ROLE %I NOLOGIN NOSUPERUSER NOINHERIT", owner)
        _ddl(conn, "CREATE ROLE %I NOLOGIN NOSUPERUSER NOINHERIT", app)
        _ddl(conn, "CREATE ROLE %I NOLOGIN NOSUPERUSER INHERIT", op)
        _ddl(conn, "GRANT %I TO %I", owner, op)
        _ddl(conn, "GRANT USAGE, CREATE ON SCHEMA %I TO %I, %I", schema, owner, app)

    def engine(role: str | None) -> Engine:
        options = "-csearch_path=" + schema + ("" if role is None else " -crole=" + role)
        return create_engine(pg_engine.url.update_query_dict({"options": options}))

    handle = Db(schema, owner, app, op, engine(None), engine(owner), engine(app), engine(op))
    with handle.app.begin() as conn:  # feature_registry belongs to the application role, as on griddb
        conn.exec_driver_sql(_REGISTRY)
        _ddl(conn, "GRANT SELECT, UPDATE ON feature_registry TO %I", owner)
    monkeypatch.setattr(importlib.import_module(_MIGRATION), "APP_ROLE", app)
    monkeypatch.setattr(elig, "APP_ROLE", app)
    try:
        yield handle
    finally:
        for e in (handle.admin, handle.owner, handle.app, handle.op):
            e.dispose()
        with pg_engine.begin() as conn:
            _ddl(conn, "DROP SCHEMA IF EXISTS %I CASCADE", schema)
            for role in (op, app, owner):
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


def _set(engine: Engine, change_set: str):
    return sorted((r.feature_id, r.name, r.old_eligible, r.new_eligible)
                  for r in _log(engine) if r.change_set == change_set)


def _planned(engine: Engine):
    with engine.connect() as conn:
        return elig.planned_changes(conn)


# A write boundary with no deploy lock and nothing to re-check, for run() tests that are not about it.
OPEN_BOUNDARY = elig.WriteBoundary(hold=contextlib.nullcontext, recheck=lambda: [])


def _backend(engine: Engine) -> dict:
    with engine.connect() as conn:
        return elig.backend_identity(conn)


def _approval(engine: Engine, changes, change_set: uuid.UUID | None = None) -> elig.Approval:
    """What a dry run's receipt approves: these rows, on the backend the dry run read, under a new change set."""
    return elig.Approval(changes=tuple(changes), backend=_backend(engine), change_set=change_set or uuid.uuid4())


def _apply(engine: Engine, approved=None, boundary: elig.WriteBoundary = OPEN_BOUNDARY, **kw):
    """The owner's flow: the rows the plan would change are read first (what the dry run's receipt records), then
    the apply is given exactly those."""
    changes = _planned(engine) if approved is None else approved
    return elig.run(engine, write=True, prerequisites=PASSING_GATE, approval=_approval(engine, changes),
                    boundary=boundary, **kw)


def _rollback(engine: Engine, change_set: str, *, write: bool = True):
    """A rollback dry run's rows, then the write given exactly those (empty when the dry run refused)."""
    target = uuid.UUID(change_set)
    if not write:
        return elig.run(engine, write=False, rollback_of=target)
    with engine.connect() as conn:
        try:
            changes = elig.plan_rollback(conn, target, lock=False)
        except elig.Refused:
            changes = []
    return elig.run(engine, write=True, rollback_of=target, approval=_approval(engine, changes))


def _changes(report) -> list[tuple]:
    return [(c["feature_id"], c["name"], c["old_eligible"], c["new_eligible"]) for c in report["changes"]]


def _swapped(rows) -> list[tuple]:
    return [(i, n, new, old) for i, n, old, new in rows]


def _as_app(db: Db, sql: str) -> None:
    with db.app.begin() as conn:
        conn.execute(text(sql))


# ── migration authority on an app-owned log ─────────────────────────────────────────────────────────────────


def test_an_app_owned_log_refuses_every_write_until_ownership_moves(db, caplog):
    """griddb's deploy runs alembic as the application role, which then owns the log."""
    with caplog.at_level(logging.WARNING, logger="alembic.runtime.migration"):
        _migrate(db.app)
    assert any("could write pre-images" in r.getMessage() for r in caplog.records)
    for engine in (db.app, db.owner):
        report = _apply(engine)
        assert report["status"] == "refused" and report["authority"]["ownership_fix_sql"]
        assert any("owns feature_eligibility_changes or is a member" in r for r in report["refusals"])
    assert _log(db.admin) == []

    # elig.OWNERSHIP_FIX_SQL, with this test's roles (owner for grid_eligibility_owner, app for grid).
    fix = [
        ("ALTER TABLE feature_eligibility_changes OWNER TO %I", db.owner_role),
        ("REVOKE ALL ON feature_eligibility_changes FROM %I", db.app_role),
        ("REVOKE ALL ON SEQUENCE feature_eligibility_changes_id_seq FROM %I", db.app_role),
        ("GRANT SELECT ON feature_eligibility_changes TO %I", db.app_role),
    ]
    assert [t.replace("%I", "grid") for t, _ in fix[1:]] == [s.rstrip(";") for s in elig.OWNERSHIP_FIX_SQL[4:7]]
    with db.admin.begin() as conn:
        for template, role in fix:
            _ddl(conn, template, role)
    report = _apply(db.owner)  # the trigger function still belongs to the app role
    assert report["status"] == "refused"
    assert report["authority"]["app_controlled_triggers"] and any("run a function" in r for r in report["refusals"])

    with db.admin.begin() as conn:
        _ddl(conn, "ALTER FUNCTION feature_eligibility_changes_append_only() OWNER TO %I", db.owner_role)
    report = _apply(db.owner)
    assert report["status"] == "applied" and report["authority"]["ok"]
    assert _set(db.admin, report["change_set"]) == FIRST_SET
    with pytest.raises(ProgrammingError, match="permission denied"):
        _as_app(db, "SELECT nextval('feature_eligibility_changes_id_seq')")


# ── the prerequisite gate and migration authority ──────────────────────────────────────────────────────────


def _no_systemd(tmp_path):
    fake = FakeHost(tmp_path)
    probes = fake.probes()
    return lambda: elig.Probes(**{**probes.__dict__, "show": lambda unit: None})


def test_apply_refuses_today_because_1307_is_not_deployed(db, monkeypatch, capsys, tmp_path):
    _migrate(db.owner)
    before = _flags(db.admin)
    monkeypatch.setenv("ELIG_TEST_DB_URL", db.url(db.owner_role))
    monkeypatch.setattr(elig, "default_probes", _no_systemd(tmp_path))   # this repo's own tree, no GRID units
    assert elig.main(["--apply", "--db-url-env", "ELIG_TEST_DB_URL"]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "refused" and not report["writes_to_database"]
    assert len(report["refusals"]) == 3         # no service inventory, no #1307 constant, no approved receipt
    assert any("deployed service inventory cannot be checked" in r for r in report["refusals"])
    assert any("REGIME_HYSTERESIS_RULE" in r for r in report["refusals"])
    assert any("--apply needs --approved-receipt" in r for r in report["refusals"])
    assert "testpass" not in report["database"]
    assert _flags(db.admin) == before and _log(db.admin) == []

    assert elig.main(["--db-url-env", "ELIG_TEST_DB_URL"]) == 0  # the dry run shows the plan and why it would refuse
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "dry_run" and report["authority"]["ok"]
    assert _changes(report) == FIRST_SET
    assert len(report["would_refuse"]) == 2
    assert len(report["receipt"]["refusals"]) == 2          # a receipt is printed for review, but cannot approve
    assert report["receipt_sha256"] == hashlib.sha256(elig.canonical(report["receipt"])).hexdigest()
    assert _flags(db.admin) == before and _log(db.admin) == []


@pytest.fixture()
def owner_flow(db, monkeypatch, capsys, tmp_path):
    """main() as the owner runs it, on a fake grid-svr whose every tree carries the prerequisites."""
    _migrate(db.owner)
    fake = FakeHost(tmp_path)
    monkeypatch.setattr(elig, "ROOT", fake.current)
    monkeypatch.setattr(elig, "default_probes", fake.probes)
    monkeypatch.setenv("ELIG_TEST_DB_URL", db.url(db.owner_role))
    receipt_file = tmp_path / "receipt.json"

    def dry_run():
        assert elig.main(["--db-url-env", "ELIG_TEST_DB_URL", "--receipt-out", str(receipt_file)]) == 0
        return json.loads(capsys.readouterr().out)

    def apply(sha: str):
        code = elig.main(["--apply", "--approved-receipt", str(receipt_file), "--approved-receipt-sha256", sha,
                          "--db-url-env", "ELIG_TEST_DB_URL"])
        return code, json.loads(capsys.readouterr().out)

    return fake, receipt_file, dry_run, apply


@pytest.fixture()
def rollback_flow(db, owner_flow, monkeypatch, capsys, tmp_path):
    """--rollback as the owner runs it: a dry run saves a rollback receipt; --apply writes exactly that receipt."""
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    code, applied = apply(dry["receipt_sha256"])
    assert code == 0 and applied["status"] == "applied"
    counter = {"n": 0}

    def rollback_dry_run(change_set: str):
        counter["n"] += 1
        out = tmp_path / f"rollback-{counter['n']}.json"
        code = elig.main(["--rollback", change_set, "--receipt-out", str(out), "--db-url-env", "ELIG_TEST_DB_URL"])
        return code, json.loads(capsys.readouterr().out), out

    def rollback_apply(change_set: str, receipt_file, sha: str):
        code = elig.main(["--rollback", change_set, "--apply", "--approved-receipt", str(receipt_file),
                          "--approved-receipt-sha256", sha, "--db-url-env", "ELIG_TEST_DB_URL"])
        return code, json.loads(capsys.readouterr().out)

    return fake, applied["change_set"], rollback_dry_run, rollback_apply


def test_the_owner_flow_dry_run_review_then_apply(db, owner_flow):
    fake, receipt_file, dry_run, apply = owner_flow
    before = _flags(db.admin)
    dry = dry_run()
    receipt = dry["receipt"]
    assert receipt["refusals"] == [] and dry["would_refuse"] == []
    assert _changes({"changes": receipt["planned_changes"]}) == FIRST_SET
    assert receipt_file.read_bytes() == elig.canonical(receipt)
    assert hashlib.sha256(receipt_file.read_bytes()).hexdigest() == dry["receipt_sha256"]
    assert set(receipt["units"]) == set(elig.SERVICE_UNITS) and len(receipt["trees"]) == 2
    assert _flags(db.admin) == before and _log(db.admin) == []     # the dry run wrote nothing

    fake.now += timedelta(minutes=10)                               # the owner reviews it
    code, report = apply(dry["receipt_sha256"])
    assert code == 0 and report["status"] == "applied"
    assert report["approved_receipt_sha256"] == dry["receipt_sha256"]
    assert _changes(report) == FIRST_SET and _set(db.admin, report["change_set"]) == FIRST_SET


def test_a_restart_between_the_dry_run_and_the_apply_refuses(db, owner_flow):
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    fake.run("grid-scheduler.service", 7777, fake.pinned, fake.pinned)
    before = _flags(db.admin)
    code, report = apply(dry["receipt_sha256"])
    assert code == 2 and report["status"] == "refused"
    assert any("['grid-scheduler.service']['main_pid']: approved 2003, now 7777" in r for r in report["refusals"])
    assert _flags(db.admin) == before and _log(db.admin) == []


def test_the_0803z_repin_refuses_the_apply(db, owner_flow):
    """The scheduler's configuration repinned after the receipt; its process still runs the approved tree."""
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    fake.units["grid-scheduler.service"]["WorkingDirectory"] = str(fake.current)
    code, report = apply(dry["receipt_sha256"])
    assert code == 2
    assert any("runs from" in r and "the configuration is not what runs" in r for r in report["refusals"])
    assert any("['grid-scheduler.service']['working_directory']" in r for r in report["refusals"])
    assert _log(db.admin) == []


def test_the_apply_holds_the_deploy_lock_from_before_the_change_until_after_the_commit(db, owner_flow):
    from sqlalchemy import event

    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    assert dry["receipt"]["deploy_locks"] == [fake.deploy_lock_path]
    fake.lock_events.clear()
    record = lambda conn: fake.lock_events.append(("commit", ""))   # noqa: E731
    event.listen(Engine, "commit", record)
    try:
        code, report = apply(dry["receipt_sha256"])
    finally:
        event.remove(Engine, "commit", record)
    assert code == 0 and report["status"] == "applied"
    assert report["change_set"] == dry["receipt"]["change_set"]          # the receipt named the set it writes
    assert report["write_boundary"] == {"deploy_locks_held": [fake.deploy_lock_path], "rechecked": True}
    order = [kind for kind, _ in fake.lock_events]
    assert order.index("lock") < order.index("commit") < order.index("unlock")


@pytest.mark.parametrize("change, named", [
    ("repin", "['grid-scheduler.service']['working_directory']"),
    ("restart", "['grid-scheduler.service']['main_pid']"),
    ("checkout", "['restart_receipt']['started_after_code']"),
])
def test_a_change_that_lands_after_the_first_probe_refuses_at_the_write_boundary(db, owner_flow, change, named):
    """main() verified the receipt before connecting; this lands while the apply holds the deploy lock (a restart
    or an in-place checkout needs no deploy), after the rows changed in the transaction and before the commit."""
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    before = _flags(db.admin)

    def lands():
        if change == "repin":
            fake.units["grid-scheduler.service"]["WorkingDirectory"] = str(fake.current)
        elif change == "restart":
            fake.run("grid-scheduler.service", 7777, fake.pinned, fake.pinned)
        else:
            fake.code_at[str(fake.pinned)] = fake.now
    fake.on_lock = lands
    code, report = apply(dry["receipt_sha256"])
    assert code == 2 and report["status"] == "refused" and not report["writes_to_database"]
    assert report["refusals"][0] == ("the write-boundary re-check found the deployed code changed after the receipt "
                                     "was verified; nothing was written")
    assert any(named in r for r in report["refusals"][1:]), report["refusals"]
    assert _flags(db.admin) == before and _log(db.admin) == []
    assert [kind for kind, _ in fake.lock_events][-1] == "unlock"


def test_a_deploy_holding_its_lock_refuses_the_apply(db, owner_flow):
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    fake.busy_locks.add(fake.deploy_lock_path)
    before = _flags(db.admin)
    code, report = apply(dry["receipt_sha256"])
    assert code == 2 and report["refusals"] == [f"a deploy holds {fake.deploy_lock_path}; nothing was written; "
                                                "retry when it finishes"]
    assert _flags(db.admin) == before and _log(db.admin) == []


def test_a_different_backend_behind_the_same_url_refuses_the_apply(db, owner_flow, monkeypatch):
    """A failover or DNS change: the redacted DSN is the same, the backend the write is connected to is not."""
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    real = elig.backend_identity
    monkeypatch.setattr(elig, "backend_identity", lambda conn: {**real(conn), "system_identifier": "1",
                                                                "timeline_id": 2})
    before = _flags(db.admin)
    code, report = apply(dry["receipt_sha256"])
    assert code == 2 and report["refusals"][0].startswith("the database backend is not the one the approved receipt")
    assert any("backend['system_identifier']" in r for r in report["refusals"])
    assert any("backend['timeline_id']: approved 1, now 2" in r for r in report["refusals"])
    assert _flags(db.admin) == before and _log(db.admin) == []


def test_a_receipt_writes_once(db, owner_flow):
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    assert apply(dry["receipt_sha256"])[0] == 0
    after = _flags(db.admin)
    code, report = apply(dry["receipt_sha256"])                # the same approved receipt, within its 30 minutes
    assert code == 2 and report["refusals"] == [f"change set {dry['receipt']['change_set']} is already recorded: an "
                                                "approved receipt writes once (run a new dry run)"]
    assert _flags(db.admin) == after and len(_log(db.admin)) == 4


def test_the_backend_identity_needs_no_superuser(db):
    """pg_control_system() and pg_control_checkpoint() as the non-superuser migration authority."""
    identity = _backend(db.owner)
    assert identity["system_identifier"].isdigit() and identity["timeline_id"] >= 1
    assert identity["database_oid"] > 0 and identity["in_recovery"] is False
    assert identity["postmaster_started_at"].endswith("Z")
    with db.owner.connect() as conn:
        assert conn.execute(text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")).scalar_one() is False


# ── rollback authorization (#1383) ──────────────────────────────────────────────────────────────────────────


def test_a_rollback_writes_exactly_its_approved_receipt(db, rollback_flow):
    fake, set_1, rollback_dry_run, rollback_apply = rollback_flow
    applied = _flags(db.admin)
    code, dry, receipt_file = rollback_dry_run(set_1)
    assert code == 0 and dry["status"] == "dry_run"
    receipt = dry["receipt"]
    assert receipt["kind"] == elig.ROLLBACK_RECEIPT_KIND and receipt["rollback_of"] == set_1
    assert receipt["refusals"] == [] and _changes({"changes": receipt["planned_changes"]}) == _swapped(FIRST_SET)
    assert receipt["backend"] == _backend(db.owner) and "units" not in receipt and "trees" not in receipt
    assert receipt_file.read_bytes() == elig.canonical(receipt) and _flags(db.admin) == applied

    fake.now += timedelta(minutes=5)
    code, report = rollback_apply(set_1, receipt_file, dry["receipt_sha256"])
    assert code == 0 and report["status"] == "rolled_back"
    assert report["change_set"] == receipt["change_set"]
    assert _set(db.admin, receipt["change_set"]) == _swapped(FIRST_SET)
    assert _flags(db.admin) != applied and len(_log(db.admin)) == 8


def test_a_rollback_without_an_approved_receipt_refuses_before_connecting(db, rollback_flow, capsys):
    fake, set_1, rollback_dry_run, rollback_apply = rollback_flow
    before = _flags(db.admin)
    assert elig.main(["--rollback", set_1, "--apply", "--db-url-env", "ELIG_TEST_DB_URL"]) == 2
    bare = json.loads(capsys.readouterr().out)
    assert bare["mode"] == "rollback" and not bare["writes_to_database"]
    assert bare["refusals"][0].startswith("--rollback with --apply needs --approved-receipt and "
                                          "--approved-receipt-sha256")
    code, dry, receipt_file = rollback_dry_run(set_1)
    other = str(uuid.uuid4())
    code, report = rollback_apply(other, receipt_file, dry["receipt_sha256"])   # a receipt for another set
    assert code == 2 and f"the approved receipt rolls back {set_1}, not {other}" in report["refusals"]
    fake.host = "laptop"
    code, report = rollback_apply(set_1, receipt_file, dry["receipt_sha256"])
    assert code == 2 and any("receipt['host']: approved 'grid-svr', now 'laptop'" in r for r in report["refusals"])
    fake.host, fake.now = "grid-svr", fake.now + timedelta(seconds=elig.RECEIPT_MAX_AGE_S + 1)
    code, report = rollback_apply(set_1, receipt_file, dry["receipt_sha256"])
    assert code == 2 and "s old; an apply takes one at most" in report["refusals"][0]
    assert _flags(db.admin) == before and len(_log(db.admin)) == 4


def test_a_changed_backend_after_an_approved_rollback_plan_refuses(db, rollback_flow, monkeypatch):
    fake, set_1, rollback_dry_run, rollback_apply = rollback_flow
    code, dry, receipt_file = rollback_dry_run(set_1)
    real = elig.backend_identity
    monkeypatch.setattr(elig, "backend_identity", lambda conn: {**real(conn),
                                                                "postmaster_started_at": "2099-01-01T00:00:00Z"})
    before = _flags(db.admin)
    code, report = rollback_apply(set_1, receipt_file, dry["receipt_sha256"])
    assert code == 2 and report["refusals"][0].startswith("the database backend is not the one the approved receipt")
    assert _flags(db.admin) == before and len(_log(db.admin)) == 4


def test_a_changed_preview_after_an_approved_rollback_plan_refuses(db, rollback_flow):
    fake, set_1, rollback_dry_run, rollback_apply = rollback_flow
    code, dry, receipt_file = rollback_dry_run(set_1)
    _as_app(db, "UPDATE feature_registry SET model_eligible = FALSE WHERE name = 'vix_spot'")
    before = _flags(db.admin)
    code, report = rollback_apply(set_1, receipt_file, dry["receipt_sha256"])
    assert code == 2 and report["status"] == "refused" and [d["feature_id"] for d in report["divergent"]] == [11]
    assert _flags(db.admin) == before and len(_log(db.admin)) == 4


def test_a_rollback_whose_rows_differ_from_its_approval_refuses(db):
    _migrate(db.owner)
    set_1 = _apply(db.owner)["change_set"]
    before = _flags(db.admin)
    wrong = [{"feature_id": 11, "name": "vix_spot", "old_eligible": True, "new_eligible": False}]
    report = elig.run(db.owner, write=True, rollback_of=uuid.UUID(set_1), approval=_approval(db.owner, wrong))
    assert report["status"] == "refused"
    assert report["refusals"][0] == ("the rows the rollback would restore differ from the approved receipt's; "
                                     "nothing was written (run a new rollback dry run)")
    assert _flags(db.admin) == before and len(_log(db.admin)) == 4


def test_an_emergency_rollback_works_while_the_forward_prerequisites_fail(db, rollback_flow, capsys):
    """The forward gate refuses on every count; the rollback consults none of it."""
    fake, set_1, rollback_dry_run, rollback_apply = rollback_flow
    for tree in (fake.current, fake.pinned):
        (tree / elig.PREREQUISITES[0].path).write_text("import os\n")      # #1307's literal is gone
        fake.ancestors[str(tree)] = False                                   # and #1350 is not deployed
    fake.run("grid-scheduler.service", 7777, fake.current, fake.pinned)     # a unit runs another tree
    assert elig.main(["--db-url-env", "ELIG_TEST_DB_URL"]) == 0
    forward = json.loads(capsys.readouterr().out)
    assert len(forward["would_refuse"]) >= 5
    code, dry, receipt_file = rollback_dry_run(set_1)
    assert code == 0 and dry["receipt"]["refusals"] == []
    code, report = rollback_apply(set_1, receipt_file, dry["receipt_sha256"])
    assert code == 0 and report["status"] == "rolled_back"
    assert _set(db.admin, report["change_set"]) == _swapped(FIRST_SET)


def test_rows_that_changed_since_the_receipt_refuse_the_apply(db, owner_flow):
    fake, _, dry_run, apply = owner_flow
    dry = dry_run()
    _as_app(db, "UPDATE feature_registry SET model_eligible = TRUE WHERE name = 'vix_spot'")
    before = _flags(db.admin)
    code, report = apply(dry["receipt_sha256"])
    assert code == 2 and report["status"] == "refused" and not report["writes_to_database"]
    assert report["refusals"][0].startswith("the rows the plan would change differ from the approved receipt's")
    assert "planned_changes['11']: approved {'name': 'vix_spot', 'old_eligible': False, 'new_eligible': True}, " \
           "absent now" in report["refusals"]
    assert _flags(db.admin) == before and _log(db.admin) == []


def test_a_row_that_changes_during_the_apply_rolls_the_set_back(db, monkeypatch):
    """The pre-check saw the approved rows; the locked update found another set: nothing is kept."""
    _migrate(db.owner)
    approved = _planned(db.owner)
    _as_app(db, "UPDATE feature_registry SET model_eligible = TRUE WHERE name = 'vix_spot'")
    before = _flags(db.admin)
    monkeypatch.setattr(elig, "planned_changes", lambda conn, plan=elig.PLAN: approved)
    report = _apply(db.owner, approved=approved)
    assert report["status"] == "refused" and not report["writes_to_database"]
    assert report["refusals"][0] == "the rows changed while the apply ran; nothing was written"
    assert _flags(db.admin) == before and _log(db.admin) == []


def test_writes_need_migration_authority(db):
    _migrate(db.owner)
    applied = _apply(db.owner)
    before = _flags(db.admin)
    for report in (_apply(db.app), _rollback(db.app, applied["change_set"])):
        assert report["status"] == "refused"
        assert any("neither owns feature_eligibility_changes" in r for r in report["refusals"])
    assert _flags(db.admin) == before and len(_log(db.admin)) == 4

    rolled_back = _rollback(db.op, applied["change_set"])  # a member of the owner role has authority
    assert rolled_back["status"] == "rolled_back"
    assert {r.recorded_by for r in _log(db.admin)} == {db.owner_role, db.op_role}


@pytest.mark.parametrize("grant, refusal", [
    ("GRANT INSERT ON feature_eligibility_changes TO %I", "holds INSERT"),
    ("GRANT TRIGGER ON feature_eligibility_changes TO %I", "holds TRIGGER"),
    ("GRANT USAGE ON SEQUENCE feature_eligibility_changes_id_seq TO %I", "holds SEQUENCE USAGE"),
    ("GRANT %I TO %I", "is a member of its owner"),
])
def test_writes_refuse_while_the_app_role_could_write_a_pre_image(db, grant, refusal):
    _migrate(db.owner)
    with db.admin.begin() as conn:
        _ddl(conn, grant, *((db.owner_role, db.app_role) if grant.count("%I") == 2 else (db.app_role,)))
    report = _apply(db.owner)
    assert report["status"] == "refused" and any(refusal in r for r in report["refusals"])
    assert report["authority"]["ownership_fix_sql"]
    assert _log(db.admin) == []


def test_writes_refuse_while_a_trigger_runs_a_function_the_app_role_controls(db):
    """A trigger runs with the privileges of the role whose write fired it: the operator's, here."""
    _migrate(db.owner)
    with db.app.begin() as conn:
        conn.execute(text("CREATE FUNCTION forge() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql"))
        conn.execute(text("CREATE TRIGGER forge BEFORE UPDATE ON feature_registry "
                          "FOR EACH ROW EXECUTE FUNCTION forge()"))
    report = _apply(db.owner)
    assert report["status"] == "refused"
    assert report["authority"]["app_controlled_triggers"] == [
        {"table_name": "feature_registry", "trigger_name": "forge", "function_name": "forge",
         "function_owner": db.app_role}]
    assert _log(db.admin) == []


def test_writes_refuse_when_the_app_role_does_not_exist(db):
    _migrate(db.owner)
    report = _apply(db.owner, app_role="elig_no_such_role")
    assert report["status"] == "refused" and any("does not exist" in r for r in report["refusals"])


# ── apply and rollback ─────────────────────────────────────────────────────────────────────────────────────


def test_apply_records_one_change_set_and_flips_only_differing_rows(db):
    _migrate(db.owner)
    before = _flags(db.admin)
    report = _apply(db.owner)
    assert report["status"] == "applied" and report["writes_to_database"]
    assert _changes(report) == FIRST_SET
    after = _flags(db.admin)
    assert {k for k in before if before[k] != after[k]} == {n for _, n, _, _ in FIRST_SET}
    assert not after["vix9d_spot"] and after["tithi"]          # deprecated rows untouched
    assert after["hy_oas_spread"] and not after["copper"]      # unrelated rows untouched
    log = _log(db.admin)
    assert {(r.change_set, r.action, r.rolls_back, r.plan, r.recorded_by) for r in log} == {
        (report["change_set"], "apply", None, elig.PLAN.name, db.owner_role)}
    assert _set(db.admin, report["change_set"]) == FIRST_SET

    again = _apply(db.owner)  # nothing differs any more: no change set is recorded
    assert again["status"] == "nothing_to_change" and not again["writes_to_database"]
    assert _flags(db.admin) == after and len(_log(db.admin)) == 4


def test_coordinators_case_rolling_back_set_2_leaves_vix_spot_enabled(db):
    """Set 1 enables vix_spot; set 2 leaves it alone and changes another row; rolling back set 2 keeps vix_spot on."""
    _migrate(db.owner)
    set_1 = _apply(db.owner)["change_set"]
    _as_app(db, "UPDATE feature_registry SET model_eligible = TRUE WHERE name = 'lunar_phase'")
    second = _apply(db.owner)
    set_2 = second["change_set"]
    assert _changes(second) == [(40, "lunar_phase", True, False)]

    report = _rollback(db.owner, set_2)
    assert report["status"] == "rolled_back"
    assert _changes(report) == [(40, "lunar_phase", False, True)]
    flags = _flags(db.admin)
    assert flags["vix_spot"] and flags["yld_curve_2s10s"] and not flags["ephemeris_sun_lon"]
    assert flags["lunar_phase"]
    rollback_rows = [r for r in _log(db.admin) if r.action == "rollback"]
    assert [(r.change_set, r.rolls_back, r.feature_id, r.old_eligible, r.new_eligible) for r in rollback_rows] == [
        (report["change_set"], set_2, 40, False, True)]
    assert _set(db.admin, set_1) == FIRST_SET  # set 1's record is untouched


def test_rollback_restores_its_set_once_and_is_recorded(db):
    _migrate(db.owner)
    before = _flags(db.admin)
    set_1 = _apply(db.owner)["change_set"]
    preview = _rollback(db.owner, set_1, write=False)
    assert preview["status"] == "dry_run" and len(_log(db.admin)) == 4
    assert _changes(preview) == _swapped(FIRST_SET)

    report = _rollback(db.owner, set_1)
    assert report["status"] == "rolled_back" and _flags(db.admin) == before
    assert _set(db.admin, report["change_set"]) == _swapped(FIRST_SET)
    assert {r.rolls_back for r in _log(db.admin) if r.action == "rollback"} == {set_1}

    again = _rollback(db.owner, set_1)
    assert again["status"] == "refused" and "already rolled back" in again["refusals"][0]
    of_rollback = _rollback(db.owner, report["change_set"])
    assert of_rollback["status"] == "refused" and "is a rollback" in of_rollback["refusals"][0]
    unknown = _rollback(db.owner, str(uuid.uuid4()))
    assert unknown["status"] == "refused" and "is recorded" in unknown["refusals"][0]
    assert len(_log(db.admin)) == 8 and _flags(db.admin) == before


@pytest.mark.parametrize("drift, feature_id, reason", [
    ("UPDATE feature_registry SET model_eligible = FALSE WHERE name = 'vix_spot'", 11, "its flag is False"),
    ("UPDATE feature_registry SET name = 'lunar_phase_v2' WHERE id = 40", 40, "its name is now 'lunar_phase_v2'"),
    ("DELETE FROM feature_registry WHERE id = 41", 41, "no longer exists"),
])
def test_a_divergent_rollback_is_refused_whole(db, drift, feature_id, reason):
    _migrate(db.owner)
    set_1 = _apply(db.owner)["change_set"]
    _as_app(db, drift)
    before = _flags(db.admin)
    for write in (False, True):
        report = _rollback(db.owner, set_1, write=write)
        assert report["status"] == "refused" and not report["writes_to_database"]
        assert [d["feature_id"] for d in report["divergent"]] == [feature_id]
        assert any(reason in r for r in report["divergent"][0]["reasons"])
    assert _flags(db.admin) == before  # nothing partly restored
    assert len(_log(db.admin)) == 4


def test_rollback_refuses_while_a_later_set_on_the_same_feature_is_in_effect(db):
    _migrate(db.owner)
    set_1 = _apply(db.owner)["change_set"]
    _as_app(db, "UPDATE feature_registry SET model_eligible = TRUE WHERE name = 'lunar_phase'")
    set_2 = _apply(db.owner)["change_set"]
    report = _rollback(db.owner, set_1)  # lunar_phase's flag is what set 1 left, but set 2 set it
    assert report["status"] == "refused"
    assert [d["feature_id"] for d in report["divergent"]] == [40]
    assert report["divergent"][0]["reasons"] == [
        f"later change set(s) {set_2} changed it and are still in effect (roll those back first)"]

    assert _rollback(db.owner, set_2)["status"] == "rolled_back"
    report = _rollback(db.owner, set_1)  # lunar_phase was turned back on by hand before set 2: still divergent
    assert report["status"] == "refused"
    assert report["divergent"][0]["reasons"] == ["its flag is True, not the False this set left"]


# ── bounded waits and blockers (Codex's release-owner note on #1346) ────────────────────────────────────────


class _Held:
    """Another session holding locks in an open transaction until closed."""

    def __init__(self, engine: Engine, *statements: str, params: dict | None = None):
        self.conn = engine.connect()
        self.trans = self.conn.begin()
        for statement in statements:
            self.conn.execute(text(statement), params or {})
        self.pid = self.conn.execute(text("SELECT pg_backend_pid()")).scalar_one()

    def close(self) -> None:
        self.trans.rollback()
        self.conn.close()


@pytest.mark.parametrize("statements, mode", [
    (("LOCK TABLE feature_registry IN SHARE MODE",), "ShareLock"),
    (("SELECT id FROM feature_registry WHERE name = 'vix_spot' FOR UPDATE",), "RowShareLock"),
    (("UPDATE feature_registry SET deprecated_at = deprecated_at WHERE name = 'copper'",), "RowExclusiveLock"),
])
def test_a_write_refuses_before_any_lock_while_another_session_holds_feature_registry(db, statements, mode):
    _migrate(db.owner)
    before = _flags(db.admin)
    held = _Held(db.admin, *statements)
    try:
        dry = elig.run(db.owner, write=False, prerequisites=PASSING_GATE)
        assert any(b["pid"] == held.pid and b["mode"] == mode for b in dry["blockers"])
        assert any("retry when they finish" in r for r in dry["would_refuse"])
        report = _apply(db.owner)
        assert report["status"] == "refused" and not report["writes_to_database"]
        assert any(b["pid"] == held.pid and b["relation"] == "feature_registry" for b in report["blockers"])
    finally:
        held.close()
    assert _flags(db.admin) == before and _log(db.admin) == []
    assert _apply(db.owner)["status"] == "applied"           # once the session is gone the same run goes through


def test_a_plain_read_never_blocks_an_apply(db):
    _migrate(db.owner)
    held = _Held(db.admin, "SELECT count(*) FROM feature_registry")
    try:
        report = _apply(db.owner)
    finally:
        held.close()
    assert report["status"] == "applied" and report["blockers"] == []


def test_another_run_holding_the_advisory_lock_is_a_blocker(db):
    _migrate(db.owner)
    held = _Held(db.admin, "SELECT pg_advisory_xact_lock(:key)", params={"key": elig.ADVISORY_LOCK_KEY})
    try:
        report = _apply(db.owner)
    finally:
        held.close()
    assert report["status"] == "refused"
    assert [b["locktype"] for b in report["blockers"] if b["pid"] == held.pid] == ["advisory"]


def test_a_lock_wait_that_runs_out_is_a_bounded_refusal_not_a_traceback(db, monkeypatch):
    """A blocker that arrives after the preflight: the wait is bounded by lock_timeout, nothing is written, and the
    report names the blocker."""
    import time

    _migrate(db.owner)
    before = _flags(db.admin)
    held = _Held(db.admin, "LOCK TABLE feature_registry IN SHARE MODE")
    real_find = elig.find_blockers
    calls = {"n": 0}

    def _miss_the_first_look(conn):
        calls["n"] += 1
        return [] if calls["n"] == 1 else real_find(conn)

    monkeypatch.setattr(elig, "find_blockers", _miss_the_first_look)
    try:
        started = time.monotonic()
        report = _apply(db.owner, lock_timeout="300ms")
        elapsed = time.monotonic() - started
    finally:
        held.close()
    assert report["status"] == "refused" and not report["writes_to_database"]
    assert "lock_timeout 300ms" in report["refusals"][0]
    assert any(b["pid"] == held.pid for b in report["blockers"])
    assert elapsed < 5
    assert _flags(db.admin) == before and _log(db.admin) == []


# ── the migration after a real apply ───────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("statement", [
    "UPDATE feature_eligibility_changes SET new_eligible = NOT new_eligible",
    "DELETE FROM feature_eligibility_changes",
    "TRUNCATE feature_eligibility_changes",
])
def test_an_applied_set_is_append_only_even_for_its_owner(db, statement):
    _migrate(db.owner)
    assert _apply(db.owner)["status"] == "applied"
    with pytest.raises(DBAPIError, match="append-only"):
        with db.owner.begin() as conn:
            conn.execute(text(statement))
    assert len(_log(db.admin)) == 4


def test_downgrade_refuses_once_an_apply_recorded_a_set(db):
    _migrate(db.owner)
    applied = _apply(db.owner)
    with pytest.raises(DBAPIError, match="downgrade refused"):
        _migrate(db.owner, "downgrade")
    assert _set(db.admin, applied["change_set"]) == FIRST_SET
