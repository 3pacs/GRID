"""Keep ``earnings_predictions`` in step across its two declarations.

The table has no single owner. ``intelligence/earnings_intel.py`` creates it
with ``CREATE TABLE IF NOT EXISTS`` (it is not in ``schema.sql``), and
``migrations/versions/earnings_pred_move_basis_0918.py`` adds
``predicted_move_basis`` / ``expected_move_options`` to databases that already
have it.

Both halves are written to be no-ops when they are not needed --
``CREATE TABLE IF NOT EXISTS``, ``ALTER TABLE IF EXISTS``, ``ADD COLUMN IF NOT
EXISTS``. That is what makes them safe to run repeatedly, and also what makes
drift invisible: rename a column on one side, spell a type differently, add a
third column to only one of them, and nothing raises. A fresh database gets one
shape, a migrated database gets the other, and the first symptom is an
``UndefinedColumn`` from the INSERT on whichever deployment lost the coin toss.

So this compares the two column-for-column, parsed from the real source on both
sides rather than from a hand-copied list. No database required.

It also pins the INSERT in ``_store_prediction`` to columns the CREATE actually
declares -- the write path is the thing the drift would break.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "intelligence" / "earnings_intel.py"
MIGRATION = ROOT / "migrations" / "versions" / "earnings_pred_move_basis_0918.py"

# The columns this migration exists to add. Both declarations must agree on
# every one of them, name and type.
MIGRATED_COLUMNS = {
    "predicted_move_basis": "TEXT",
    "expected_move_options": "DOUBLE PRECISION",
}

# Column-level constraint keywords that are part of the type in the CREATE but
# not repeated in an ADD COLUMN, so they are stripped before comparing.
_TYPE_NOISE = re.compile(
    r"\s+(PRIMARY\s+KEY|NOT\s+NULL|DEFAULT\s+.*|UNIQUE|REFERENCES\s+.*)$",
    re.IGNORECASE,
)


def _create_table_body(source: str, table: str) -> str:
    """Return the text between the parens of ``CREATE TABLE IF NOT EXISTS <table> (...)``."""
    start = re.search(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(table)}\s*\(",
        source,
        re.IGNORECASE,
    )
    assert start, f"no CREATE TABLE IF NOT EXISTS {table} in {MODULE.name}"

    depth = 0
    for i in range(start.end() - 1, len(source)):
        if source[i] == "(":
            depth += 1
        elif source[i] == ")":
            depth -= 1
            if depth == 0:
                return source[start.end() : i]
    raise AssertionError(f"unbalanced parens in CREATE TABLE {table}")


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are not inside parentheses."""
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [" ".join(p.split()) for p in parts if p.strip()]


def module_create_columns() -> dict[str, str]:
    """{column name: normalised type} from the module's CREATE TABLE."""
    body = _create_table_body(MODULE.read_text(encoding="utf-8"), "earnings_predictions")
    columns: dict[str, str] = {}
    for item in _split_top_level(body):
        head = item.split(None, 1)[0].upper()
        # Table-level constraints, not columns.
        if head in {"UNIQUE", "PRIMARY", "FOREIGN", "CHECK", "CONSTRAINT", "EXCLUDE"}:
            continue
        name, _, rest = item.partition(" ")
        columns[name] = _TYPE_NOISE.sub("", rest).strip().upper()
    return columns


def migration_added_columns() -> dict[str, str]:
    """{column name: normalised type} from the revision's ADD COLUMN statements."""
    source = MIGRATION.read_text(encoding="utf-8")
    # The statements are split across adjacent string literals; join them back
    # into one blob of SQL before matching so a reflow cannot hide a column.
    sql = " ".join(re.findall(r'"([^"]*)"', source))
    return {
        m.group(1): " ".join(m.group(2).split()).upper()
        for m in re.finditer(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(\w+)\s+([A-Za-z ]+?)(?=\s*(?:,|$|ALTER|ADD))",
            sql,
            re.IGNORECASE,
        )
    }


@pytest.mark.unit
def test_migration_adds_exactly_the_columns_it_documents():
    assert migration_added_columns() == MIGRATED_COLUMNS


@pytest.mark.unit
def test_module_create_table_carries_the_migrated_columns():
    """A fresh database must get the post-migration shape from the CREATE alone.

    Without this, ``ALTER TABLE IF EXISTS`` on a database where the table does
    not exist yet is a silent no-op, the module creates the old shape
    afterwards, and the columns never appear at all.
    """
    created = module_create_columns()
    missing = [c for c in MIGRATED_COLUMNS if c not in created]
    assert not missing, (
        f"{MODULE.name}'s CREATE TABLE is missing {missing}, which "
        f"{MIGRATION.name} adds. A database created after this deploy would "
        "never get them: the migration's ALTER TABLE IF EXISTS is a no-op "
        "while the table is absent."
    )
    for column, expected_type in MIGRATED_COLUMNS.items():
        assert created[column] == expected_type, (
            f"{column} is {created[column]!r} in {MODULE.name} but "
            f"{expected_type!r} in {MIGRATION.name}"
        )


@pytest.mark.unit
def test_module_no_longer_alters_the_table_at_runtime():
    """The DDL belongs in the revision, not in a per-request code path.

    ``_ensure_tables()`` is called at the top of every public function in the
    module -- every ``/api/v1/earnings/*`` handler -- so a runtime ``ADD
    COLUMN`` took ``ACCESS EXCLUSIVE`` on ``earnings_predictions`` on every
    request, queued behind any open transaction touching the table and blocking
    readers behind it while it waited.
    """
    source = MODULE.read_text(encoding="utf-8")
    assert "ALTER TABLE" not in source.upper(), (
        f"{MODULE.name} runs DDL at request time again. Add the column in "
        f"a new alembic revision and to the CREATE TABLE instead."
    )
    assert "earnings_pred_move_basis_0918" in source, (
        "the CREATE TABLE should name the revision that migrates existing "
        "databases, so the two are findable from each other"
    )


@pytest.mark.unit
def test_insert_only_names_declared_columns():
    """The write path must not reference a column neither declaration creates."""
    source = MODULE.read_text(encoding="utf-8")
    insert = re.search(
        r"INSERT\s+INTO\s+earnings_predictions\s*\((.*?)\)\s*VALUES",
        source,
        re.IGNORECASE | re.DOTALL,
    )
    assert insert, "no INSERT INTO earnings_predictions found"

    inserted = {c.strip() for c in insert.group(1).split(",") if c.strip()}
    declared = set(module_create_columns())
    undeclared = sorted(inserted - declared)
    assert not undeclared, (
        f"_store_prediction writes {undeclared}, which the CREATE TABLE does "
        "not declare"
    )
    # The two columns this revision is about are actually written, or the
    # migration would be adding dead columns.
    assert set(MIGRATED_COLUMNS) <= inserted
