"""PostgreSQL contract: dollar_flows stored-row contract, its constraint and its repair.

Covers what SQLite cannot: alembic revision ``df_13f_observed_20261127``
(NOT VALID on a table that still holds superseded rows, validated once clean,
idempotent, a no-op without the table, downgrade); the writer's DDL; the
writer raising (and rolling back its window) when PostgreSQL rejects a row;
the run-end audit agreeing with the CHECK; and the repair script end to end
on rows shaped like production's 49 (export, dry run, apply, refusals, lock,
restore).

Runs against a fresh schema on ``GRID_TEST_DB_URL`` (local, disposable
database only). Skips without it unless ``REQUIRE_DOLLAR_FLOWS_CONTRACT_PG=1``,
which the dedicated CI step sets so the contract cannot pass by skipping.
"""

from __future__ import annotations

import importlib
import json
import os
import threading
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError, OperationalError

from intelligence import dollar_flows
from scripts import repair_dollar_flows_stored_contract as repair
from scripts import run_dollar_flows

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_DOLLAR_FLOWS_CONTRACT_PG"
_MIGRATION = "migrations.versions.df_13f_observed_20261127"
TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)

# Production's table before the revision (no CHECK).
_PRE_REVISION_DDL = """
CREATE TABLE dollar_flows (
    id SERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,
    actor_name TEXT,
    ticker TEXT,
    amount_usd NUMERIC NOT NULL,
    direction TEXT NOT NULL,
    confidence TEXT DEFAULT 'estimated',
    evidence JSONB,
    flow_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
)
"""

_LEGACY_EVIDENCE = {"cusip": None, "action": "UNKNOWN", "shares": None, "pct_change": None, "filing_accession": None}


@pytest.fixture()
def pg_url():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required for the dollar_flows contract PG proof ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("dollar_flows contract PG proof requires a local disposable test database")
    return url


@pytest.fixture()
def pg(pg_url):
    schema = "dfc_" + uuid4().hex[:12]
    admin = create_engine(pg_url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(pg_url, connect_args={"options": f"-csearch_path={schema}"})
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _migrate(engine, fn: str = "upgrade") -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        try:
            migration.op = Operations(MigrationContext.configure(conn))
            getattr(migration, fn)()
            trans.commit()
        except BaseException:
            trans.rollback()
            raise
        finally:
            migration.op = real_op


def _constraint(engine):
    with engine.connect() as conn:
        return repair._constraint_state(conn)


def _insert(conn, *, source_type, direction, flow_date, ticker="GRDX", actor="A", amount="10",
            confidence="confirmed", evidence=None):
    return conn.execute(
        text(
            "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, confidence, "
            "evidence, flow_date) VALUES (:st, :a, :t, CAST(:amt AS NUMERIC), :d, :c, CAST(:ev AS JSONB), :fd) "
            "RETURNING id"
        ),
        {"st": source_type, "a": actor, "t": ticker, "amt": amount, "d": direction, "c": confidence,
         "ev": json.dumps(evidence if evidence is not None else {}), "fd": flow_date},
    ).scalar()


def _seed_production_shape(engine) -> dict[str, list[int]]:
    """Legacy 13F triples (total, count, ratio) like production's 49, plus rows that must survive."""
    ids: dict[str, list[int]] = {"legacy": [], "keep": []}
    with engine.begin() as conn:
        for cik, fd, amounts in [
            ("0002012383", date(2026, 11, 13), ["6791591104981.0", "50911.0", "0.093589"]),
            ("0000093751", date(2026, 11, 12), ["3435560457924.0", "4310.0", "0.229743"]),
        ]:
            for amt in amounts:
                ids["legacy"].append(_insert(conn, source_type="13f", direction="inflow", flow_date=fd, ticker="",
                                             actor=cik, amount=amt, evidence=_LEGACY_EVIDENCE))
        ids["keep"].append(_insert(conn, source_type="13f", direction="observed", confidence="derived",
                                   flow_date=date(2026, 11, 25), ticker=None, actor="Wellington", amount="237772.0"))
        ids["keep"].append(_insert(conn, source_type="13f", direction="observed", confidence="derived",
                                   flow_date=date(2026, 11, 25), actor="Wellington", amount="41562.0"))
        ids["keep"].append(_insert(conn, source_type="etf_flow", direction="inflow", flow_date=date(2026, 11, 13)))
        ids["keep"].append(_insert(conn, source_type="insider", direction="outflow", flow_date=date(2026, 11, 13)))
        ids["keep"].append(_insert(conn, source_type="options_flow", direction="inflow", flow_date=YESTERDAY))
    return ids


def _ids(engine) -> list[int]:
    with engine.connect() as conn:
        return conn.execute(text("SELECT id FROM dollar_flows ORDER BY id")).scalars().all()


# ── alembic revision ────────────────────────────────────────────────────────

def test_revision_on_a_table_with_superseded_rows_is_not_valid_and_enforced(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)

    _migrate(pg)

    assert _constraint(pg) is False  # present, NOT VALID
    assert _ids(pg) == sorted(ids["legacy"] + ids["keep"])  # nothing touched
    with pytest.raises(IntegrityError, match=dollar_flows.DOLLAR_FLOWS_CONTRACT_CONSTRAINT):
        with pg.begin() as conn:
            _insert(conn, source_type="13f", direction="inflow", flow_date=YESTERDAY, ticker="")
    with pytest.raises(IntegrityError):
        with pg.begin() as conn:
            conn.execute(text("UPDATE dollar_flows SET direction = 'inflow' WHERE id = :i"), {"i": ids["keep"][0]})
    with pg.begin() as conn:
        _insert(conn, source_type="13f", direction="observed", flow_date=YESTERDAY, confidence="derived")
        _insert(conn, source_type="insider", direction="inflow", flow_date=YESTERDAY)

    _migrate(pg)  # idempotent while rows remain
    assert _constraint(pg) is False

    with pg.begin() as conn:
        conn.execute(text("DELETE FROM dollar_flows WHERE id = ANY(:ids)"), {"ids": ids["legacy"]})
    _migrate(pg)  # a re-run validates once clean
    assert _constraint(pg) is True


def test_revision_on_a_clean_table_validates_at_once(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
        _insert(conn, source_type="13f", direction="observed", flow_date=YESTERDAY)
    _migrate(pg)
    assert _constraint(pg) is True
    _migrate(pg)
    assert _constraint(pg) is True


def test_revision_without_the_table_is_a_no_op(pg):
    _migrate(pg)
    with pg.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('dollar_flows')")).scalar() is None


def test_downgrade_drops_only_the_constraint(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
        _insert(conn, source_type="insider", direction="inflow", flow_date=YESTERDAY)
    _migrate(pg)
    _migrate(pg, "downgrade")
    assert _constraint(pg) is None
    assert len(_ids(pg)) == 1


def test_writer_ddl_creates_the_validated_constraint(pg):
    dollar_flows._ensure_table(pg)
    assert _constraint(pg) is True
    with pytest.raises(IntegrityError):
        with pg.begin() as conn:
            _insert(conn, source_type="13f", direction="inflow", flow_date=YESTERDAY, ticker="")


# ── writer and audit ────────────────────────────────────────────────────────

def test_audit_agrees_with_the_check_on_production_shaped_rows(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    with pg.connect() as conn:
        audit = dollar_flows.count_stored_contract_violations(conn)
        by_check = conn.execute(
            text(f"SELECT count(*) FROM dollar_flows WHERE NOT ({dollar_flows.DOLLAR_FLOWS_CONTRACT_CHECK_SQL})")
        ).scalar()
        rows = conn.execute(text("SELECT source_type, direction FROM dollar_flows")).mappings().all()
    by_python = sum(1 for r in rows if dollar_flows.flow_contract_violation(dict(r)))
    assert audit == {"13f": len(ids["legacy"])}
    assert by_check == by_python == len(ids["legacy"])


def test_writer_raises_when_postgres_rejects_a_row_and_keeps_the_window(pg, monkeypatch):
    """The old ``except: log.debug`` would have hidden this: PostgreSQL aborts
    the transaction, so the run's DELETE and every insert are lost."""
    dollar_flows._ensure_table(pg)
    with pg.begin() as conn:
        kept = _insert(conn, source_type="insider", direction="inflow", flow_date=YESTERDAY)
    monkeypatch.setattr(dollar_flows, "flow_contract_violation", lambda f: None)  # simulate a writer regression
    flows = [
        {"source_type": "insider", "actor_name": "B", "ticker": "GRDY", "amount_usd": 5.0, "direction": "inflow",
         "confidence": "confirmed", "evidence": {}, "flow_date": YESTERDAY},
        {"source_type": "13f", "actor_name": "C", "ticker": "", "amount_usd": 7.0, "direction": "inflow",
         "confidence": "confirmed", "evidence": {}, "flow_date": YESTERDAY},
    ]
    with pytest.raises(IntegrityError):
        dollar_flows._persist_flows(pg, flows)
    assert _ids(pg) == [kept]


def test_runner_on_postgres_exits_3_then_0_after_the_repair(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    _migrate(pg)

    assert run_dollar_flows.main(["--days", "7", "--json"], engine=pg) == 3
    assert json.loads(capsys.readouterr().out)["stored_contract_violations"] == {"13f": len(ids["legacy"])}

    man_path = tmp_path / "m.json"
    assert repair.main(["export", "--out", str(man_path)], engine=pg) == 0
    sha = json.loads(capsys.readouterr().out)["rows_sha256"]
    assert repair.main(["apply", "--manifest", str(man_path), "--expect-count", str(len(ids["legacy"])),
                        "--expect-sha256", sha], engine=pg) == 0
    capsys.readouterr()
    assert run_dollar_flows.main(["--days", "7"], engine=pg) == 0


# ── repair script ───────────────────────────────────────────────────────────

def _export(pg, tmp_path, capsys) -> tuple[Path, dict]:
    path = tmp_path / "manifest.json"
    assert repair.main(["export", "--out", str(path)], engine=pg) == 0
    return path, json.loads(capsys.readouterr().out)


def test_repair_export_dry_run_apply_restore_round_trip(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    _migrate(pg)
    n = len(ids["legacy"])
    before = _ids(pg)

    path, rep = _export(pg, tmp_path, capsys)
    assert rep["count"] == n and rep["ids"] == ids["legacy"]
    assert rep["amount_usd_sum"] == "10227151618126.323332"
    assert rep["constraint"] == "not_valid"
    man = json.loads(path.read_text())
    assert man["rows"][0]["ticker"] == "" and man["rows"][0]["evidence"] == _LEGACY_EVIDENCE
    # Export never overwrites a manifest.
    assert pytest.raises(FileExistsError, repair.main, ["export", "--out", str(path)], engine=pg)

    args = ["apply", "--manifest", str(path), "--expect-count", str(n), "--expect-sha256", rep["rows_sha256"]]
    assert repair.main(args + ["--dry-run"], engine=pg) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["deleted"] == n and dry["dry_run"] is True
    assert _ids(pg) == before

    assert repair.main(args, engine=pg) == 0
    done = json.loads(capsys.readouterr().out)
    assert done["deleted"] == n and done["remaining_violations"] == 0
    assert done["constraint"] == "validated"
    assert _ids(pg) == ids["keep"]

    # Restore refuses while the constraint exists ...
    restore = ["restore", "--manifest", str(path), "--expect-sha256", rep["rows_sha256"]]
    assert repair.main(restore, engine=pg) == 2
    assert "DROP CONSTRAINT" in json.loads(capsys.readouterr().out)["refused"]
    # ... and after the operator drops it, puts back exactly the exported rows.
    _migrate(pg, "downgrade")
    assert repair.main(restore + ["--dry-run"], engine=pg) == 0
    capsys.readouterr()
    assert _ids(pg) == ids["keep"]
    assert repair.main(restore, engine=pg) == 0
    assert json.loads(capsys.readouterr().out)["rows_sha256"] == rep["rows_sha256"]
    assert _ids(pg) == before
    with pg.connect() as conn:
        assert repair._read_violations(conn) == man["rows"]
    # A second restore refuses: the ids exist.
    assert repair.main(restore, engine=pg) == 2


@pytest.mark.parametrize("change", ["new_violation", "row_changed", "row_gone"])
def test_repair_apply_refuses_a_stale_manifest(pg, tmp_path, capsys, change):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    path, rep = _export(pg, tmp_path, capsys)
    with pg.begin() as conn:
        if change == "new_violation":
            _insert(conn, source_type="13f", direction="outflow", flow_date=YESTERDAY, ticker="")
        elif change == "row_changed":
            conn.execute(text("UPDATE dollar_flows SET amount_usd = 1 WHERE id = :i"), {"i": ids["legacy"][0]})
        else:
            conn.execute(text("DELETE FROM dollar_flows WHERE id = :i"), {"i": ids["legacy"][0]})
    before = _ids(pg)
    rc = repair.main(["apply", "--manifest", str(path), "--expect-count", str(len(ids["legacy"])),
                      "--expect-sha256", rep["rows_sha256"]], engine=pg)
    assert rc == 2
    assert "re-export" in json.loads(capsys.readouterr().out)["refused"]
    assert _ids(pg) == before


def test_repair_apply_refuses_wrong_count_digest_or_edited_manifest(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    path, rep = _export(pg, tmp_path, capsys)
    n, sha = len(ids["legacy"]), rep["rows_sha256"]
    before = _ids(pg)
    base = ["apply", "--manifest", str(path)]
    assert repair.main(base + ["--expect-count", str(n + 1), "--expect-sha256", sha], engine=pg) == 2
    assert repair.main(base + ["--expect-count", str(n), "--expect-sha256", "0" * 64], engine=pg) == 2
    man = json.loads(path.read_text())
    man["ids"] = man["ids"] + [ids["keep"][0]]
    man["count"] += 1
    edited = tmp_path / "edited.json"
    edited.write_text(json.dumps(man))
    assert repair.main(["apply", "--manifest", str(edited), "--expect-count", str(n + 1),
                        "--expect-sha256", sha], engine=pg) == 2
    capsys.readouterr()
    assert _ids(pg) == before


def test_repair_apply_waits_for_a_concurrent_writer_and_gives_up_cleanly(pg, tmp_path, capsys):
    """The daily writer holds ROW EXCLUSIVE while it rewrites its window; the
    repair's SHARE ROW EXCLUSIVE waits for it and gives up after its
    lock_timeout without deleting anything."""
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    path, rep = _export(pg, tmp_path, capsys)
    before = _ids(pg)

    writer = pg.connect()
    wtx = writer.begin()
    _insert(writer, source_type="insider", direction="inflow", flow_date=YESTERDAY)
    try:
        with pytest.raises(OperationalError, match="lock timeout"):
            repair.main(["apply", "--manifest", str(path), "--expect-count", str(len(ids["legacy"])),
                         "--expect-sha256", rep["rows_sha256"]], engine=pg)
    finally:
        wtx.rollback()
        writer.close()
    assert _ids(pg) == before


def test_repair_blocks_the_writer_until_it_commits(pg, tmp_path, capsys):
    """A writer that starts while the repair holds its lock waits, then runs."""
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed_production_shape(pg)
    path, rep = _export(pg, tmp_path, capsys)
    man = repair.load_manifest(path, rep["rows_sha256"])

    order: list[str] = []
    lock_held = threading.Event()
    real_read = repair._read_violations

    def slow_read(conn):
        rows = real_read(conn)
        if not lock_held.is_set():
            lock_held.set()
            threading.Event().wait(1.0)
            order.append("repair_read_done")
        return rows

    def writer():
        lock_held.wait(5)
        with pg.begin() as conn:
            _insert(conn, source_type="insider", direction="inflow", flow_date=YESTERDAY)
        order.append("writer_committed")

    t = threading.Thread(target=writer)
    t.start()
    repair._read_violations = slow_read
    try:
        out = repair.apply(pg, man, expect_count=len(ids["legacy"]), dry_run=False)
    finally:
        repair._read_violations = real_read
    order.append("repair_committed")
    t.join(10)
    assert out["deleted"] == len(ids["legacy"])
    assert order.index("repair_read_done") < order.index("writer_committed")
    assert len(_ids(pg)) == len(ids["keep"]) + 1
