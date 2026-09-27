"""Offline guards for migrations/versions/godview_writers_20260926.py (god view G2).

Runs the revision's upgrade()/downgrade() against a recording stand-in for
``alembic.op`` and checks the SQL it would emit. No database is needed; the
real-PostgreSQL behaviour is proven in tests/test_godview_schema_migration_pg.py.

What is pinned here:
* the revision is on the single alembic chain and its id fits alembic_version;
* the upgrade is schema-only: it never touches market_god_view_daily (G6 owns
  that) and never UPDATEs, DELETEs, TRUNCATEs or DROPs anything;
* every added column is nullable with no default, so legacy rows stay valid
  and the ADD COLUMNs are catalog-only;
* both directions set lock_timeout / statement_timeout before any DDL;
* the downgrade removes exactly the columns and constraints the upgrade added
  and restores exactly the NOT NULLs it dropped;
* the grant footer covers the one new table.
"""

from __future__ import annotations

import importlib
import os
import re

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULE = "migrations.versions.godview_writers_20260926"
REVISION = "godview_writers_20260926"
PILLAR_TABLES = ("cftc_positioning_daily", "fed_net_liquidity_daily", "dealer_gex_daily")

# Columns the plan (section 3) requires on every pillar row.
COMMON_COLUMNS = {
    "release_at", "available_at", "availability_basis", "provenance", "source_ref",
    "run_id", "code_sha", "updated_at", "coverage_fraction",
}

# NOT NULLs the plan says would force fabricated values.
EXPECTED_DROPPED_NOT_NULL = {
    "cftc_positioning_daily": {"crowding_regime"},
    "fed_net_liquidity_daily": {"liquidity_regime"},
    "dealer_gex_daily": {
        "spot_price", "net_gex_usd_m", "call_gex_usd_m", "put_gex_usd_m",
        "gamma_flip_strike", "spot_to_flip_pct", "gex_regime", "max_pain_strike",
        "put_call_oi_ratio", "atm_iv",
    },
}


class _RecordingOp:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql) -> None:
        self.statements.append(str(sql))


def _emit(fn_name: str) -> list[str]:
    migration = importlib.import_module(MODULE)
    recorder = _RecordingOp()
    real_op = migration.op
    migration.op = recorder
    try:
        getattr(migration, fn_name)()
    finally:
        migration.op = real_op
    return [re.sub(r"\s+", " ", s).strip() for s in recorder.statements]


def _alter_body(statements: list[str], table: str) -> str:
    matches = [s for s in statements if s.startswith(f"ALTER TABLE {table} ")]
    assert len(matches) == 1, f"expected one ALTER TABLE {table}, got {len(matches)}"
    return matches[0]


def _added_columns(body: str) -> dict[str, str]:
    return {
        m.group(1): m.group(2).strip()
        for m in re.finditer(r"ADD COLUMN IF NOT EXISTS (\w+) ([^,]+?)(?=,|$)", body)
    }


def _script() -> ScriptDirectory:
    config = Config(os.path.join(REPO_ROOT, "alembic.ini"))
    config.set_main_option("script_location", os.path.join(REPO_ROOT, "migrations"))
    return ScriptDirectory.from_config(config)


@pytest.mark.unit
def test_revision_is_on_the_single_chain():
    script = _script()
    rev = script.get_revision(REVISION)
    assert rev is not None
    assert len(REVISION) <= 32
    parent = script.get_revision(rev.down_revision)
    assert parent is not None, f"parent {rev.down_revision} missing"
    # Whatever the head is (a later revision may be stacked on this one), this
    # revision must be its ancestor, not a sibling branch.
    (head,) = script.get_heads()
    assert REVISION in {r.revision for r in script.walk_revisions("base", head)}


@pytest.mark.unit
@pytest.mark.parametrize("fn_name", ["upgrade", "downgrade"])
def test_timeouts_are_set_before_any_ddl(fn_name):
    statements = _emit(fn_name)
    assert statements[0] == "SET LOCAL lock_timeout = '5s'"
    assert statements[1] == "SET LOCAL statement_timeout = '30s'"
    migration = importlib.import_module(MODULE)
    assert migration._LOCK_TIMEOUT == "5s"
    assert migration._STATEMENT_TIMEOUT == "30s"


@pytest.mark.unit
def test_upgrade_is_schema_only_and_leaves_the_matview_alone():
    statements = _emit("upgrade")
    for sql in statements:
        upper = sql.upper()
        assert "MARKET_GOD_VIEW_DAILY" not in upper, "G2 must not touch the matview (G6 decides)"
        for verb in ("UPDATE ", "DELETE ", "TRUNCATE", "DROP TABLE", "DROP COLUMN", "INSERT "):
            assert verb not in upper, f"upgrade must not {verb.strip()}: {sql[:120]}"
        assert "SET NOT NULL" not in upper
    altered = {m.group(1) for s in statements for m in re.finditer(r"^ALTER TABLE (\w+)", s)}
    assert altered == set(PILLAR_TABLES)
    created = {m.group(1) for s in statements
               for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+)", s)}
    assert created == {"godview_runs"}


@pytest.mark.unit
@pytest.mark.parametrize("table", PILLAR_TABLES)
def test_added_columns_are_nullable_without_defaults(table):
    added = _added_columns(_alter_body(_emit("upgrade"), table))
    assert COMMON_COLUMNS <= set(added), COMMON_COLUMNS - set(added)
    for column, decl in added.items():
        assert "NOT NULL" not in decl.upper(), f"{table}.{column} must be nullable"
        assert "DEFAULT" not in decl.upper(), f"{table}.{column} must not carry a default"


@pytest.mark.unit
@pytest.mark.parametrize("table", PILLAR_TABLES)
def test_not_null_drops_match_the_plan(table):
    body = _alter_body(_emit("upgrade"), table)
    dropped = set(re.findall(r"ALTER COLUMN (\w+) DROP NOT NULL", body))
    assert dropped == EXPECTED_DROPPED_NOT_NULL[table]
    migration = importlib.import_module(MODULE)
    if table == "dealer_gex_daily":
        assert set(migration.GEX_LEGACY_VALUE_COLUMNS) == dropped


@pytest.mark.unit
@pytest.mark.parametrize("table", PILLAR_TABLES)
def test_downgrade_reverses_the_upgrade_exactly(table):
    up = _alter_body(_emit("upgrade"), table)
    down = _alter_body(_emit("downgrade"), table)

    assert set(re.findall(r"DROP COLUMN IF EXISTS (\w+)", down)) == set(_added_columns(up))
    assert set(re.findall(r"ALTER COLUMN (\w+) SET NOT NULL", down)) == set(
        re.findall(r"ALTER COLUMN (\w+) DROP NOT NULL", up)
    )
    assert set(re.findall(r"DROP CONSTRAINT IF EXISTS (\w+)", down)) == set(
        re.findall(r"ADD CONSTRAINT (\w+)", up)
    )


@pytest.mark.unit
def test_downgrade_refuses_before_dropping_anything():
    statements = _emit("downgrade")
    guard = statements[2]
    assert "RAISE EXCEPTION" in guard
    for table in PILLAR_TABLES:
        assert f"FROM {table} WHERE provenance IS NOT NULL" in guard
    assert "FROM godview_runs" in guard
    assert statements[-1] == "DROP TABLE IF EXISTS godview_runs"


@pytest.mark.unit
def test_gex_rows_can_only_be_modeled():
    body = _alter_body(_emit("upgrade"), "dealer_gex_daily")
    assert "provenance IS NULL OR provenance = 'modeled'" in body


@pytest.mark.unit
def test_grant_footer_covers_the_new_table():
    joined = "\n".join(_emit("upgrade"))
    assert "GRANT ALL ON godview_runs TO grid" in joined
