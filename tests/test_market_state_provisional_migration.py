"""The reviewed migration for ``market_state_provisional`` (#1312, Codex HOLD on #1360).

Production never creates the table at runtime; the migration does, with the GRANT footer. ``schema.sql`` declares
the same statement (the replay applies schema.sql, then ``alembic upgrade head``), and the SQLite-only copy in
``intelligence/regime/market_state.py`` keeps the same columns. Static checks plus a recorded ``upgrade()``; the
live replay is ``.github/workflows/migration-replay.yml``.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from intelligence.regime import market_state as M

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "migrations/versions/market_state_prov_20261122.py"
SPEC = importlib.util.spec_from_file_location("market_state_provisional_migration", PATH)
MIG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIG)


def _squash(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _columns(ddl: str) -> list[tuple[str, str]]:
    body = ddl[ddl.index("(") + 1: ddl.rindex(")")]
    out = []
    for part in re.split(r",(?![^(]*\))", body):
        words = part.split()
        if words:
            out.append((words[0], _squash(" ".join(words[1:]))))
    return out


def test_revision_identity_and_the_constant_the_store_names():
    assert MIG.revision == M.PROVISIONAL_MIGRATION == PATH.stem
    assert MIG.down_revision and MIG.down_revision != MIG.revision


def test_schema_sql_declares_exactly_the_migrations_table():
    assert _squash(MIG.TABLE) + ";" in _squash((ROOT / "schema.sql").read_text())


def test_grant_footer_grants_the_application_role():
    assert re.search(r"GRANT ALL ON market_state_provisional TO grid", MIG.GRANTS)
    assert "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid')" in MIG.GRANTS


def test_upgrade_creates_the_table_then_grants_and_downgrade_refuses_with_rows():
    ran: list[str] = []

    class Op:
        @staticmethod
        def execute(sql: str) -> None:
            ran.append(sql)

    MIG.op = Op
    MIG.upgrade()
    assert ran[-2:] == [MIG.TABLE, MIG.GRANTS] and all("lock_timeout" in s or "statement_timeout" in s
                                                       for s in ran[:-2])
    ran.clear()
    MIG.downgrade()
    assert "RAISE EXCEPTION" in ran[0] and "SELECT 1 FROM market_state_provisional" in ran[0]
    assert ran[1] == "DROP TABLE IF EXISTS market_state_provisional"


def test_the_sqlite_copy_has_the_migrations_columns():
    pg = _columns(MIG.TABLE)
    lite = _columns(str(M._PROVISIONAL_TABLE_SQL))
    assert [c for c, _ in pg] == [c for c, _ in lite] == ["as_of_date", "rule", "record", "computed_at",
                                                          "provisional"]
    pg_types, lite_types = dict(pg), dict(lite)
    assert pg_types["provisional"] == lite_types["provisional"] == "BOOLEAN NOT NULL DEFAULT TRUE CHECK (provisional)"
    assert pg_types["as_of_date"] == lite_types["as_of_date"] == "DATE PRIMARY KEY"
    assert pg_types["record"] == "JSONB NOT NULL" and lite_types["record"] == "TEXT NOT NULL"
    assert pg_types["computed_at"] == "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
