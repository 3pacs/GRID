"""``options_mispricing_scans`` has two authors; they must agree.

The table is created lazily by ``discovery/options_scanner.py`` with
``CREATE TABLE IF NOT EXISTS`` and reshaped on existing databases by alembic
revision ``options_rec_scanner_score_0917`` with ``ALTER TABLE IF EXISTS``.
Whichever runs first wins, and neither can see what the other did:

* On a database where the scanner has run, the table exists in the OLD shape
  (``is_100x NOT NULL DEFAULT FALSE``, ``payoff_multiple NOT NULL``, no
  ``payoff_inputs``) and only the migration can fix it.
* On a database where it has not, ``ALTER TABLE IF EXISTS`` silently skips
  every statement and the scanner's own ``CREATE`` must already carry the new
  shape.

That second case is the dangerous one: the migration reports success having
done nothing, and the only thing standing between that and a wrong table is
the module's DDL being correct. This test compares the two definitions
directly, with no database, so drift is caught in CI rather than on a host
where the scanner happened not to have run yet.

The live equivalent (apply the migration to a real PostgreSQL 14 and diff
``information_schema``) is recorded in the #539 migration-verification
write-up; this is its offline guard.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCANNER = REPO_ROOT / "discovery" / "options_scanner.py"
MIGRATION = (
    REPO_ROOT / "migrations" / "versions" / "options_rec_scanner_score_0917.py"
)
TABLE = "options_mispricing_scans"


def _create_table_body(source: str) -> str:
    """Return the column list inside the module's CREATE TABLE for the table."""
    start = source.index(f"CREATE TABLE IF NOT EXISTS {TABLE} (")
    depth = 0
    for i in range(source.index("(", start), len(source)):
        if source[i] == "(":
            depth += 1
        elif source[i] == ")":
            depth -= 1
            if depth == 0:
                return source[source.index("(", start) + 1:i]
    raise AssertionError(f"unbalanced parentheses in {TABLE} DDL")


def _columns(body: str) -> dict[str, str]:
    """Map column name -> its definition, ignoring comments and table constraints."""
    cleaned = "\n".join(
        line for line in body.splitlines() if not line.strip().startswith("--")
    )
    # Split on commas that are not inside parentheses.
    parts, depth, buf = [], 0, ""
    for ch in cleaned:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    parts.append(buf)

    cols: dict[str, str] = {}
    for part in parts:
        frag = " ".join(part.split())
        if not frag:
            continue
        name = frag.split(" ", 1)[0]
        if name.upper() in {"UNIQUE", "PRIMARY", "CONSTRAINT", "CHECK", "FOREIGN"}:
            continue
        cols[name] = frag
    return cols


@pytest.fixture(scope="module")
def module_columns() -> dict[str, str]:
    return _columns(_create_table_body(SCANNER.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def migration_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


# ── The shape the migration produces, asserted on the module's DDL ────────


@pytest.mark.unit
def test_is_100x_is_nullable_with_no_default(module_columns):
    """An unmodelled payoff is NULL, not a confident FALSE (audit C-M20)."""
    col = module_columns["is_100x"]
    assert "BOOLEAN" in col.upper()
    assert "NOT NULL" not in col.upper(), (
        "discovery/options_scanner.py still creates is_100x NOT NULL. On a "
        "database where the scanner runs before alembic, the migration's "
        "ALTER TABLE IF EXISTS skips and this DDL is the final word — the "
        "table would keep the old shape with the migration reporting success."
    )
    assert "DEFAULT" not in col.upper(), (
        "is_100x still carries a DEFAULT. A default FALSE is exactly the "
        "fabricated value the reshaping exists to remove."
    )


@pytest.mark.unit
def test_payoff_multiple_is_nullable(module_columns):
    col = module_columns["payoff_multiple"]
    assert "DOUBLE PRECISION" in col.upper()
    assert "NOT NULL" not in col.upper(), (
        "payoff_multiple must be nullable: a payoff that could not be "
        "modelled is NULL, not a number."
    )


@pytest.mark.unit
def test_payoff_inputs_exists_and_is_jsonb(module_columns):
    assert "payoff_inputs" in module_columns, (
        "discovery/options_scanner.py does not create payoff_inputs, so a "
        "fresh database would never get the column the migration adds."
    )
    assert "JSONB" in module_columns["payoff_inputs"].upper()


# ── Drift: every column the migration touches must exist in the module ────


@pytest.mark.unit
def test_migration_touches_only_columns_the_module_declares(
    module_columns, migration_sql
):
    """Guards the other direction: a migration column the DDL never creates."""
    # The migration builds each statement from adjacent string literals, so
    # work per op.execute(...) call and only on the ones naming this table.
    relevant: set[str] = set()
    for call in re.findall(r"op\.execute\((.*?)\)\n", migration_sql, re.S):
        if TABLE not in call:
            continue
        flat = " ".join(re.findall(r'"([^"]*)"', call))
        relevant.update(
            re.findall(
                r"(?:ADD COLUMN IF NOT EXISTS|ALTER COLUMN)\s+(\w+)", flat
            )
        )
    assert relevant, "no options_mispricing_scans column changes found"
    missing = sorted(relevant - set(module_columns))
    assert not missing, (
        f"migration alters {missing} on {TABLE}, but "
        "discovery/options_scanner.py's CREATE TABLE never declares those "
        "columns. On a fresh database the ALTERs skip and the columns never "
        "appear at all."
    )


@pytest.mark.unit
def test_migration_still_guards_the_table_with_if_exists(migration_sql):
    """The IF EXISTS is load-bearing; without it a fresh DB fails to migrate."""
    statements = [
        s for s in re.findall(r'"[^"]*ALTER TABLE[^"]*"', migration_sql)
        if TABLE in s
    ]
    assert statements
    for stmt in statements:
        assert "ALTER TABLE IF EXISTS" in stmt, (
            f"{stmt} drops the IF EXISTS guard — alembic would fail on any "
            "database where the options scanner has never run."
        )


@pytest.mark.unit
def test_module_documents_that_alembic_owns_the_reshaping(module_columns):
    """The lazy CREATE must not silently re-add the old shape later."""
    source = SCANNER.read_text(encoding="utf-8")
    assert "options_rec_scanner_score_0917" in source, (
        "discovery/options_scanner.py no longer points at the alembic "
        "revision that reshapes pre-existing deployments; the next reader "
        "will not know why the CREATE and the live table can differ."
    )
