"""PostgreSQL contract: the etf_flow stored-row contract, its CHECK and its re-derive.

Covers alembic revision ``df_etf_flow_basis_20261128`` (NOT VALID on a table
that still holds old-rule rows, enforced on new rows, NULL-safe, idempotent,
a no-op without the table, downgrade drops only it); the writer's DDL; the
run-end audit agreeing with the CHECK and the Python check; and
``scripts/rederive_dollar_flows_etf_legacy.py`` end to end on rows shaped like
production's 693 (export, refusals, dry run, apply, validate, restore, lock),
next to the 13F contract and its repair, which neither touches the other's rows.

Fresh schema on ``GRID_TEST_DB_URL`` (local, disposable). Fails instead of
skipping when ``REQUIRE_DOLLAR_FLOWS_CONTRACT_PG=1`` (the CI step).
"""

from __future__ import annotations

import importlib
import json
import threading
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError

from intelligence import dollar_flows
from scripts import rederive_dollar_flows_etf_legacy as rederive
from scripts import repair_dollar_flows_stored_contract as repair13f
from scripts import run_dollar_flows
from tests import test_dollar_flows_stored_contract_pg as shared
from tests.test_dollar_flows_stored_contract_pg import _LEGACY_EVIDENCE, _PRE_REVISION_DDL, _insert

# Fixtures shared with #1508's proof (disposable schema, frozen writer clock), bound here for pytest.
pg_url = shared.pg_url
pg = shared.pg
_frozen_clock = shared._frozen_clock

pytestmark = pytest.mark.xdist_group("postgres")

_ETF_MIGRATION = "migrations.versions.df_etf_flow_basis_20261128"
_13F_MIGRATION = "migrations.versions.df_13f_observed_20261127"


def _migrate(engine, module: str, fn: str = "upgrade") -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(module)
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


def _both(engine) -> None:
    _migrate(engine, _13F_MIGRATION)
    _migrate(engine, _ETF_MIGRATION)


def _state(engine):
    with engine.connect() as conn:
        return rederive._constraint_state(conn)


OLD_RULE_CREATED = "2026-11-20 11:45:18+00"                              # inside the old rule's period


def _legacy_etf(conn, ticker: str, d: date, raw: str, created: str = OLD_RULE_CREATED) -> int:
    """A row as the old rule stored it: inflow, {series_id, raw_value}, amount 0.3% of raw."""
    rid = _insert(conn, source_type="etf_flow", direction="inflow", flow_date=d, ticker=ticker,
                  actor=f"ETF:{ticker}", amount=str(Decimal(raw) * Decimal("0.003")), confidence="estimated",
                  evidence={"series_id": f"ETF_FLOW:{ticker}:5d", "raw_value": float(raw)})
    conn.execute(text("UPDATE dollar_flows SET created_at = CAST(:c AS TIMESTAMPTZ) WHERE id = :i"),
                 {"c": created, "i": rid})
    return rid


def _seed(engine) -> dict[str, list[int]]:
    ids: dict[str, list[int]] = {"legacy_etf": [], "legacy_13f": [], "keep": []}
    with engine.begin() as conn:
        for ticker, d, raw in [("SPY", date(2026, 3, 2), "300862597809.906"), ("QQQ", date(2026, 7, 1), "101.5"),
                               ("TLT", date(2026, 11, 13), "20290501464.373016")]:
            ids["legacy_etf"].append(_legacy_etf(conn, ticker, d, raw))
        ids["legacy_13f"].append(_insert(conn, source_type="13f", direction="inflow", flow_date=date(2026, 11, 13),
                                         ticker="", actor="0002012383", amount="6791591104981.0",
                                         evidence=_LEGACY_EVIDENCE))
        ids["keep"].append(_insert(conn, source_type="etf_flow", direction="neutral", flow_date=date(2026, 11, 20),
                                   ticker="SPY", evidence={"series_id": "ETF_FLOW:SPY:5d", "raw_value": 1.0,
                                                           "direction_basis": "unsigned_volume"}))
        ids["keep"].append(_insert(conn, source_type="insider", direction="inflow", flow_date=date(2026, 11, 13)))
    return ids


def _rows(engine, ids):
    with engine.connect() as conn:
        return rederive._read_ids(conn, ids)


def _export(engine, tmp_path, capsys, name="m.json") -> tuple[Path, dict]:
    path = tmp_path / name
    assert rederive.main(["export", "--out", str(path)], engine=engine) == 0
    return path, json.loads(capsys.readouterr().out)


def _apply(engine, path, rep, *extra) -> int:
    return rederive.main(["apply", "--manifest", str(path), "--expect-count", str(rep["count"]),
                          "--expect-sha256", rep["rows_sha256"], *extra], engine=engine)


# ── migration, DDL, audit ───────────────────────────────────────────────────


def test_revision_on_a_table_with_old_rule_rows_is_not_valid_and_enforced(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    _migrate(pg, _ETF_MIGRATION)                                          # idempotent
    assert _state(pg) is False
    assert repair13f._constraint_state(pg.connect()) is False             # 13F's own, unchanged
    for evidence in ({"series_id": "ETF_FLOW:SPY:5d", "raw_value": 1.0}, {"direction_basis": None}, {}, None,
                     {"direction_basis": 5}, [1]):
        with pytest.raises(IntegrityError, match="dollar_flows_etf_flow_direction_basis"):
            with pg.begin() as conn:
                conn.execute(text(
                    "INSERT INTO dollar_flows (source_type, amount_usd, direction, evidence, flow_date) "
                    "VALUES ('etf_flow', 1, 'inflow', CAST(:e AS JSONB), DATE '2026-11-27')"),
                    {"e": None if evidence is None else json.dumps(evidence)})
    with pg.begin() as conn:
        _insert(conn, source_type="etf_flow", direction="neutral", flow_date=date(2026, 11, 27),
                evidence={"direction_basis": "unsigned_volume"})
        _insert(conn, source_type="etf_flow", direction="inflow", flow_date=date(2026, 11, 27),
                evidence={"direction_basis": "net_flow"})
    assert len(_rows(pg, ids["legacy_etf"])) == 3                        # no stored row changed


def test_revision_without_the_table_is_a_no_op_and_downgrade_drops_only_it(pg):
    _migrate(pg, _ETF_MIGRATION)
    with pg.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('dollar_flows')")).scalar() is None
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    _both(pg)
    _migrate(pg, _ETF_MIGRATION, "downgrade")
    assert _state(pg) is None
    assert repair13f._constraint_state(pg.connect()) is False


def test_writer_ddl_creates_both_constraints_validated(pg):
    with pg.begin() as conn:
        conn.execute(text(dollar_flows.CREATE_TABLE_SQL))
    assert _state(pg) is True and repair13f._constraint_state(pg.connect()) is True
    with pytest.raises(IntegrityError):
        with pg.begin() as conn:
            _legacy_etf(conn, "SPY", date(2026, 11, 27), "1")
    with pg.connect() as conn:
        assert rederive._require_constraint(conn) is True                # the DDL's CHECK passes the probes


def test_one_contract_text_for_writer_audit_migration_and_schema():
    migration = importlib.import_module(_ETF_MIGRATION)
    check = dollar_flows.ETF_FLOW_CONTRACT_CHECK_SQL
    assert migration.CHECK_SQL == check and migration.CONSTRAINT == dollar_flows.ETF_FLOW_CONTRACT_CONSTRAINT
    line = f"CONSTRAINT {dollar_flows.ETF_FLOW_CONTRACT_CONSTRAINT} CHECK {check}"
    assert line in dollar_flows.CREATE_TABLE_SQL
    assert line in (Path(__file__).resolve().parents[1] / "schema.sql").read_text()
    assert migration.down_revision == "df_13f_observed_20261127"


def test_audit_agrees_with_the_check_and_python(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    with pg.begin() as conn:
        conn.execute(text("INSERT INTO dollar_flows (source_type, amount_usd, direction, evidence, flow_date) "
                          "VALUES ('etf_flow', 1, 'neutral', NULL, DATE '2026-11-01')"))
    with pg.connect() as conn:
        audit = dollar_flows.count_stored_contract_violations(conn)
        by_check = conn.execute(text(
            f"SELECT count(*) FROM dollar_flows WHERE NOT {dollar_flows.ETF_FLOW_CONTRACT_CHECK_SQL}")).scalar()
        rows = conn.execute(text("SELECT source_type, direction, evidence FROM dollar_flows")).mappings().all()
    by_python = sum(1 for r in rows if dollar_flows.flow_contract_violation(dict(r)) == "etf_flow")
    assert audit == {"13f": 1, "etf_flow": len(ids["legacy_etf"]) + 1}
    assert by_check == by_python == len(ids["legacy_etf"]) + 1


@pytest.mark.parametrize("encoded", [False, True], ids=["json_object", "json_string_scalar"])
def test_the_writer_refuses_evidence_it_would_store_as_a_json_string_before_any_delete(pg, monkeypatch, capsys,
                                                                                         encoded):
    """Evidence already JSON-encoded would be encoded again and reach PostgreSQL as a JSON
    string, which the CHECK rejects. The preflight judges it as stored: refused before the
    window DELETE (exit 4, the last good window intact), not a database error mid-write."""
    from tests.test_dollar_flows_stored_contract_pg import TODAY, YESTERDAY
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
        _insert(conn, source_type="insider", direction="inflow", flow_date=YESTERDAY, actor="Old")
        _insert(conn, source_type="etf_flow", direction="neutral", flow_date=YESTERDAY, ticker="SPY",
                evidence={"direction_basis": "unsigned_volume"})
    _both(pg)
    with pg.connect() as conn:
        before = conn.execute(text("SELECT * FROM dollar_flows ORDER BY id")).fetchall()
    evidence = {"series_id": "ETF_FLOW:SPY:5d", "raw_value": 1000.0, "direction_basis": "unsigned_volume"}
    flow = {"source_type": "etf_flow", "actor_name": "ETF:SPY", "ticker": "SPY", "amount_usd": 3.0,
            "direction": "neutral", "confidence": "estimated", "flow_date": TODAY,
            "evidence": json.dumps(evidence) if encoded else evidence}
    monkeypatch.setattr(dollar_flows, "_normalize_etf_flows", lambda *a, **k: [flow])
    code = run_dollar_flows.main(["--days", "7", "--json"], engine=pg)
    out = json.loads(capsys.readouterr().out)
    with pg.connect() as conn:
        after = conn.execute(text("SELECT * FROM dollar_flows ORDER BY id")).fetchall()
        stored = conn.execute(text("SELECT jsonb_typeof(evidence) FROM dollar_flows WHERE flow_date = :d "
                                   "AND source_type = 'etf_flow'"), {"d": TODAY}).scalars().all()
    if encoded:
        assert code == 4 and out["rejected_by_contract"] == {"etf_flow": 1} and out["persisted"] == 0
        assert after == before                                              # no DELETE, no INSERT
    else:
        assert code == 0 and out["persisted"] == 1 and stored == ["object"]


# ── export ──────────────────────────────────────────────────────────────────


def test_export_plans_each_old_rule_row_and_writes_nothing(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    before = _rows(pg, ids["legacy_etf"])
    path, rep = _export(pg, tmp_path, capsys)
    man = rederive.load_manifest(path, rep["rows_sha256"])
    assert rep["count"] == 3 and man["ids"] == ids["legacy_etf"] and rep["constraint"] == "not_valid"
    for old, new in zip(man["rows"], man["after"]):
        assert new["direction"] == "neutral" and old["direction"] == "inflow"
        assert new["evidence"]["direction_basis"] == "unsigned_volume"
        assert new["evidence"]["rederived"]["manifest_rows_sha256"] == rep["rows_sha256"]
        assert {k: v for k, v in new["evidence"].items() if k not in ("direction_basis", "rederived")} == old["evidence"]
        assert {k: new[k] for k in new if k not in ("direction", "evidence")} == \
               {k: old[k] for k in old if k not in ("direction", "evidence")}       # amount, dates, ids kept
    assert _rows(pg, ids["legacy_etf"]) == before
    with pytest.raises(FileExistsError):
        rederive.export(pg, path)                                         # never overwritten


@pytest.mark.parametrize("odd", ["outflow", "extra_key", "series_mismatch", "amount_off",
                                 "created_after_the_old_rule", "dated_after_the_old_rule"])
def test_export_refuses_a_breaking_row_that_is_not_the_old_rules_shape(pg, tmp_path, capsys, odd):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
        ev = {"series_id": "ETF_FLOW:SPY:5d", "raw_value": 100.0}
        kw = dict(source_type="etf_flow", direction="inflow", flow_date=date(2026, 5, 1), ticker="SPY",
                  amount="0.3", evidence=ev)
        created = OLD_RULE_CREATED
        if odd == "outflow":
            kw["direction"] = "outflow"
        elif odd == "extra_key":
            kw["evidence"] = {**ev, "note": "hand-written"}
        elif odd == "series_mismatch":
            kw["ticker"] = "QQQ"
        elif odd == "amount_off":
            kw["amount"] = "0.31"
        elif odd == "created_after_the_old_rule":
            created = "2026-11-27 09:00:00+00"                            # same shape, written later by hand
        else:
            kw["flow_date"] = date(2026, 11, 20)
        rid = _insert(conn, **kw)
        conn.execute(text("UPDATE dollar_flows SET created_at = CAST(:c AS TIMESTAMPTZ) WHERE id = :i"),
                     {"c": created, "i": rid})
    assert rederive.main(["export", "--out", str(tmp_path / "m.json")], engine=pg) == 2
    assert "need a person" in capsys.readouterr().err
    assert not (tmp_path / "m.json").exists()


def test_an_edited_plan_is_refused(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    man = json.loads(path.read_text())
    man["after"][0]["amount_usd"] = "1"
    path.write_text(json.dumps(man))
    with pytest.raises(rederive.Refused, match="not the re-derive"):
        rederive.load_manifest(path, rep["rows_sha256"])
    assert rederive.main(["apply", "--manifest", str(path), "--expect-count", "3", "--expect-sha256", "0" * 64],
                         engine=pg) == 2


# ── apply, validate, restore ────────────────────────────────────────────────


def test_dry_run_then_apply_rederives_validates_and_leaves_13f_to_its_repair(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    man = rederive.load_manifest(path, rep["rows_sha256"])
    before_all = _rows(pg, ids["legacy_etf"] + ids["legacy_13f"] + ids["keep"])

    assert _apply(pg, path, rep, "--dry-run") == 0
    assert json.loads(capsys.readouterr().out)["dry_run"] is True
    assert _rows(pg, ids["legacy_etf"] + ids["legacy_13f"] + ids["keep"]) == before_all and _state(pg) is False

    assert _apply(pg, path, rep) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["rederived"] == 3 and out["remaining_violations"] == 0 and out["after_sha256"] == man["after_sha256"]
    assert _rows(pg, ids["legacy_etf"]) == man["after"] and _state(pg) is True
    assert _rows(pg, ids["legacy_13f"] + ids["keep"]) == before_all[3:]   # nothing else touched
    with pg.connect() as conn:
        assert dollar_flows.count_stored_contract_violations(conn) == {"13f": 1}
    assert rederive.main(["status"], engine=pg) == 0
    assert json.loads(capsys.readouterr().out) == {"action": "status", "breaking_contract": 0, "rederived": 3,
                                                    "constraint": "validated"}
    assert _apply(pg, path, rep) == 2                                    # a second apply finds nothing to do


def test_the_13f_repair_keeps_old_rule_etf_rows_and_the_rederive_keeps_13f_rows(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    p13 = tmp_path / "13f.json"
    assert repair13f.main(["export", "--out", str(p13)], engine=pg) == 0
    r13 = json.loads(capsys.readouterr().out)
    assert r13["ids"] == ids["legacy_13f"]                                # the 13F repair sees only its rows
    assert repair13f.main(["apply", "--manifest", str(p13), "--expect-count", "1",
                           "--expect-sha256", r13["rows_sha256"]], engine=pg) == 0
    capsys.readouterr()
    assert [r["id"] for r in _rows(pg, ids["legacy_etf"])] == ids["legacy_etf"]
    path, rep = _export(pg, tmp_path, capsys)
    assert _apply(pg, path, rep) == 0
    capsys.readouterr()
    assert run_dollar_flows.main(["--days", "7"], engine=pg) == 0        # both contracts hold: the alarm clears


def test_runner_exits_3_while_old_rule_etf_rows_remain_and_names_the_rederive(pg, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
        for t, d in (("SPY", date(2026, 4, 1)), ("QQQ", date(2026, 4, 2))):
            _legacy_etf(conn, t, d, "1000")
    _both(pg)
    assert run_dollar_flows.main(["--days", "7", "--json"], engine=pg) == 3
    captured = capsys.readouterr()
    assert json.loads(captured.out)["stored_contract_violations"] == {"etf_flow": 2}
    assert "rederive_dollar_flows_etf_legacy.py" in captured.err


@pytest.mark.parametrize("change", ["new_violation", "row_changed", "row_gone"])
def test_apply_refuses_a_stale_manifest(pg, tmp_path, capsys, change):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    with pg.begin() as conn:
        conn.execute(text(f"ALTER TABLE dollar_flows DROP CONSTRAINT {rederive.ETF_FLOW_CONTRACT_CONSTRAINT}"))
        if change == "new_violation":
            _legacy_etf(conn, "IWM", date(2026, 6, 1), "5")
        elif change == "row_changed":
            conn.execute(text("UPDATE dollar_flows SET amount_usd = amount_usd + 1 WHERE id = :i"),
                         {"i": ids["legacy_etf"][0]})
        else:
            conn.execute(text("DELETE FROM dollar_flows WHERE id = :i"), {"i": ids["legacy_etf"][0]})
    _migrate(pg, _ETF_MIGRATION)
    before = _rows(pg, ids["legacy_etf"])
    assert _apply(pg, path, rep) == 2
    assert "re-export" in capsys.readouterr().err
    assert _rows(pg, ids["legacy_etf"]) == before


def test_apply_refuses_without_the_constraint_or_with_one_grouped_otherwise(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    path, rep = _export(pg, tmp_path, capsys)
    before = _rows(pg, ids["legacy_etf"])
    assert _apply(pg, path, rep) == 2
    assert "is absent" in capsys.readouterr().err
    # Same operators and operands, so the flat text comparison passes, grouped differently:
    # only the probe rows see it. The second is the partner review's regrouping, which
    # rejects an insider row carrying a basis.
    typed = "COALESCE(jsonb_typeof(evidence -> 'direction_basis'), '') = 'string'"
    nonempty = "evidence ->> 'direction_basis' <> ''"
    for regrouped in (
        f"(source_type <> 'etf_flow' OR {typed} AND {nonempty} AND direction = 'neutral' "
        "OR evidence ->> 'direction_basis' <> 'unsigned_volume')",
        f"((source_type <> 'etf_flow' OR {typed} AND {nonempty}) AND (direction = 'neutral' "
        "OR evidence ->> 'direction_basis' <> 'unsigned_volume'))",
    ):
        assert rederive._flat(f"CHECK {regrouped}") == rederive.EXPECTED_FLAT_DEF
        with pg.begin() as conn:
            conn.execute(text(f"ALTER TABLE dollar_flows DROP CONSTRAINT IF EXISTS "
                              f"{rederive.ETF_FLOW_CONTRACT_CONSTRAINT}"))
            conn.execute(text(f"ALTER TABLE dollar_flows ADD CONSTRAINT {rederive.ETF_FLOW_CONTRACT_CONSTRAINT} "
                              f"CHECK {regrouped} NOT VALID"))
        assert _apply(pg, path, rep) == 2
        assert "does not behave as the contract" in capsys.readouterr().err
        assert _rows(pg, ids["legacy_etf"]) == before


def test_the_probes_leave_no_row_and_the_contract_check_passes_on_every_server(pg):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    _both(pg)
    with pg.connect() as conn:
        with conn.begin():
            assert rederive._require_constraint(conn) is False
            assert conn.execute(text("SELECT count(*) FROM dollar_flows")).scalar() == 0


def test_validate_refuses_while_old_rule_rows_remain_and_validates_a_clean_table(pg, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
        _legacy_etf(conn, "SPY", date(2026, 4, 1), "1000")
    _both(pg)
    assert rederive.main(["validate"], engine=pg) == 2
    with pg.begin() as conn:
        conn.execute(text("DELETE FROM dollar_flows"))
    assert rederive.main(["validate", "--dry-run"], engine=pg) == 0 and _state(pg) is False
    assert rederive.main(["validate"], engine=pg) == 0 and _state(pg) is True


def test_restore_refuses_while_the_constraint_exists_then_writes_the_originals_back(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    man = rederive.load_manifest(path, rep["rows_sha256"])
    assert _apply(pg, path, rep) == 0
    capsys.readouterr()
    restore = ["restore", "--manifest", str(path), "--expect-sha256", rep["rows_sha256"]]
    assert rederive.main(restore, engine=pg) == 2
    assert rederive.DROP_SQL in capsys.readouterr().err
    with pg.begin() as conn:
        conn.execute(text(rederive.DROP_SQL))
    assert rederive.main([*restore, "--dry-run"], engine=pg) == 0
    capsys.readouterr()
    assert _rows(pg, ids["legacy_etf"]) == man["after"]
    assert rederive.main(restore, engine=pg) == 0
    out = json.loads(capsys.readouterr().out)
    assert _rows(pg, ids["legacy_etf"]) == man["rows"] and out["rows_sha256"] == rep["rows_sha256"]
    assert out["re_add_constraint"] == rederive.READD_SQL
    with pg.begin() as conn:
        conn.execute(text(rederive.READD_SQL))
    assert _state(pg) is False


def test_restore_refuses_a_row_changed_since_apply(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    assert _apply(pg, path, rep) == 0
    with pg.begin() as conn:
        conn.execute(text(rederive.DROP_SQL))
        conn.execute(text("UPDATE dollar_flows SET confidence = 'confirmed' WHERE id = :i"), {"i": ids["legacy_etf"][1]})
    capsys.readouterr()
    before = _rows(pg, ids["legacy_etf"])
    assert rederive.main(["restore", "--manifest", str(path), "--expect-sha256", rep["rows_sha256"]], engine=pg) == 2
    assert "no longer as apply left them" in capsys.readouterr().err
    assert _rows(pg, ids["legacy_etf"]) == before


# ── concurrency ─────────────────────────────────────────────────────────────


def test_apply_waits_for_a_concurrent_writer_and_gives_up_cleanly(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    ids = _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    before = _rows(pg, ids["legacy_etf"])
    writer = pg.connect()
    wtx = writer.begin()
    _insert(writer, source_type="insider", direction="inflow", flow_date=date(2026, 11, 26))
    try:
        with pytest.raises(OperationalError, match="lock timeout"):
            _apply(pg, path, rep)
    finally:
        wtx.rollback()
        writer.close()
    assert _rows(pg, ids["legacy_etf"]) == before


def test_a_writer_that_starts_during_apply_waits_until_it_commits(pg, tmp_path, capsys):
    with pg.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))
    _seed(pg)
    _both(pg)
    path, rep = _export(pg, tmp_path, capsys)
    man = rederive.load_manifest(path, rep["rows_sha256"])
    order: list[str] = []
    lock_held = threading.Event()
    real_read = rederive._read_violations

    def slow_read(conn):
        rows = real_read(conn)
        if not lock_held.is_set():
            lock_held.set()
            threading.Event().wait(1.0)
            order.append("rederive_read_done")
        return rows

    def writer():
        lock_held.wait(5)
        with pg.begin() as conn:
            _insert(conn, source_type="insider", direction="inflow", flow_date=date(2026, 11, 26))
        order.append("writer_committed")

    t = threading.Thread(target=writer)
    t.start()
    rederive._read_violations = slow_read
    try:
        out = rederive.apply(pg, man, expect_count=3, dry_run=False)
    finally:
        rederive._read_violations = real_read
    t.join(10)
    assert out["rederived"] == 3
    assert order.index("rederive_read_done") < order.index("writer_committed")
