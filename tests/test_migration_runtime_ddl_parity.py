"""Keep migration-created tables in step with the runtime DDL they copy.

Three tables that alembic revisions ALTER, index or select from are not
created by ``schema.sql`` or by any revision's own history -- the application
creates them at runtime with ``CREATE TABLE IF NOT EXISTS``:

* ``discovered_hypotheses`` -- ``intelligence/hypothesis_engine.py::_SCHEMA_SQL``
* ``analytical_snapshots``  -- ``store/snapshots.py::ANALYTICAL_SNAPSHOTS_DDL``
* ``news_articles``         -- ``ingestion/altdata/news_scraper.py::_ensure_news_table``

On a genuinely fresh database (``schema.sql`` -> ``alembic stamp
7e4dfecce247`` -> ``alembic upgrade head``) nothing has run that runtime code
yet, so ``phase4_fts_001`` / ``phase4_fts_002`` failed with UndefinedTable
(issue #523). Those revisions now bootstrap the tables with frozen copies of
the runtime DDL. A frozen copy can drift; this module makes the drift loud:

1. every runtime column is created by the migration chain (the bootstrap
   ``CREATE TABLE`` plus any later ``ADD COLUMN``), so a fresh database never
   lacks a column the application writes; and
2. no bootstrap copy declares a column the runtime DDL does not have, which
   is the "two rival DDLs for one table" failure ``test_migration_fts_columns``
   documents (an FTS trigger written against a column the real table lacks).

Static source checks -- no database. The live proof is the
``migration-replay`` CI job (``.github/workflows/migration-replay.yml``).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "migrations" / "versions"

# table -> file holding the runtime CREATE TABLE IF NOT EXISTS for it.
RUNTIME_DDL_SOURCES: dict[str, Path] = {
    "discovered_hypotheses": ROOT / "intelligence" / "hypothesis_engine.py",
    "analytical_snapshots": ROOT / "store" / "snapshots.py",
    "news_articles": ROOT / "ingestion" / "altdata" / "news_scraper.py",
}

# table -> revision file that bootstraps it for a fresh database.
BOOTSTRAP_REVISIONS: dict[str, Path] = {
    "discovered_hypotheses": VERSIONS / "phase4_fts_intelligence_search.py",
    "analytical_snapshots": VERSIONS / "phase4_fts_intelligence_search.py",
    "news_articles": VERSIONS / "phase4_fts_news.py",
}

# Columns a migration owns on purpose and the runtime DDL deliberately omits
# (see the note above ANALYTICAL_SNAPSHOTS_DDL: search_vector belongs to the
# FTS migration's lifecycle, with its trigger).
MIGRATION_OWNED_COLUMNS: dict[str, set[str]] = {
    "discovered_hypotheses": {"search_vector"},
    "analytical_snapshots": {"search_vector"},
    "news_articles": set(),
}

_CONSTRAINT_WORDS = {"PRIMARY", "UNIQUE", "CONSTRAINT", "FOREIGN", "CHECK", "EXCLUDE"}


def _strip_sql_comments(sql: str) -> str:
    """Remove ``-- ...`` line comments so they can't be read as columns."""
    return re.sub(r"--[^\n]*", "", sql)


def _balanced_body(source: str, open_paren: int) -> str:
    """Text between ``source[open_paren]`` (a ``(``) and its matching ``)``."""
    depth = 0
    for i in range(open_paren, len(source)):
        ch = source[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return source[open_paren + 1 : i]
    raise AssertionError("unbalanced parentheses in CREATE TABLE")


def _split_top_level(body: str) -> list[str]:
    """Split a column list on commas that are not inside parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
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
    return [p.strip() for p in parts if p.strip()]


def create_table_columns(source: str, table: str) -> list[set[str]]:
    """Column sets of every ``CREATE TABLE [IF NOT EXISTS] <table> (...)`` in ``source``.

    Parameters:
        source: File text to scan.
        table: Unqualified table name.

    Returns:
        list[set[str]]: One column set per CREATE TABLE found, in file order.
    """
    found: list[set[str]] = []
    pattern = re.compile(
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?{table}\s*\(",
        re.IGNORECASE,
    )
    for m in pattern.finditer(source):
        body = _strip_sql_comments(_balanced_body(source, m.end() - 1))
        columns = set()
        for part in _split_top_level(body):
            name = part.split()[0].strip('"')
            if name.upper() in _CONSTRAINT_WORDS:
                continue
            columns.add(name.lower())
        found.append(columns)
    return found


def added_columns(source: str, table: str) -> set[str]:
    """Columns ``source`` adds to ``table`` via ``ALTER TABLE ... ADD COLUMN``.

    Parameters:
        source: File text to scan.
        table: Unqualified table name.

    Returns:
        set[str]: Every column name added to ``table``.
    """
    columns: set[str] = set()
    for m in re.finditer(
        rf"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?:public\.)?{table}\b",
        source,
        re.IGNORECASE,
    ):
        # One statement: up to the end of the SQL string or the semicolon.
        rest = source[m.end():]
        end = min(
            (i for i in (rest.find(";"), rest.find('"""'), rest.find("'''")) if i >= 0),
            default=len(rest),
        )
        columns.update(
            c.lower()
            for c in re.findall(
                r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?\"?([a-zA-Z_][a-zA-Z0-9_]*)",
                rest[:end],
                re.IGNORECASE,
            )
        )
    return columns


def runtime_columns(table: str) -> set[str]:
    """Columns the application's own DDL gives ``table`` (CREATE + ADD COLUMN)."""
    source = RUNTIME_DDL_SOURCES[table].read_text()
    creates = create_table_columns(source, table)
    assert len(creates) == 1, (
        f"expected exactly one runtime CREATE TABLE {table} in "
        f"{RUNTIME_DDL_SOURCES[table].relative_to(ROOT)}, found {len(creates)}"
    )
    return creates[0] | added_columns(source, table)


def migration_chain_columns(table: str) -> set[str]:
    """Columns the alembic chain (plus ``schema.sql``) creates on ``table``."""
    columns: set[str] = set()
    sources = [p.read_text() for p in sorted(VERSIONS.glob("*.py"))]
    sources.append((ROOT / "schema.sql").read_text())
    for source in sources:
        for created in create_table_columns(source, table):
            columns |= created
        columns |= added_columns(source, table)
    return columns


TABLES = sorted(RUNTIME_DDL_SOURCES)


@pytest.mark.parametrize("table", TABLES)
def test_runtime_ddl_parses(table: str) -> None:
    """The parser finds a non-trivial runtime column list (guards the guard)."""
    columns = runtime_columns(table)
    assert {"id"} < columns, f"{table}: runtime DDL parsed to {sorted(columns)}"


@pytest.mark.parametrize("table", TABLES)
def test_bootstrap_revision_creates_table(table: str) -> None:
    """Each runtime-owned table has a fresh-database bootstrap in its revision."""
    path = BOOTSTRAP_REVISIONS[table]
    creates = create_table_columns(path.read_text(), table)
    assert creates, (
        f"{path.name} no longer creates {table}. On a fresh database "
        f"(schema.sql -> stamp 7e4dfecce247 -> upgrade head) nothing else does, "
        f"and the revision's ALTER/SELECT fails with UndefinedTable (issue #523)."
    )
    assert re.search(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{table}\b", path.read_text()
    ), f"{path.name}: the {table} bootstrap must be IF NOT EXISTS (no-op on production)"


@pytest.mark.parametrize("table", TABLES)
def test_migration_columns_superset_of_runtime(table: str) -> None:
    """A fresh database built by migrations has every column the app uses."""
    missing = runtime_columns(table) - migration_chain_columns(table)
    assert not missing, (
        f"{table}: runtime DDL in "
        f"{RUNTIME_DDL_SOURCES[table].relative_to(ROOT)} declares {sorted(missing)}, "
        f"which no migration creates. On a database built by schema.sql + alembic "
        f"the runtime CREATE TABLE IF NOT EXISTS is a no-op, so the column would "
        f"never exist. Add it in a new revision (ADD COLUMN IF NOT EXISTS)."
    )


@pytest.mark.parametrize("table", TABLES)
def test_bootstrap_declares_no_rival_columns(table: str) -> None:
    """The frozen copy may lag the runtime DDL but never invent columns."""
    path = BOOTSTRAP_REVISIONS[table]
    allowed = runtime_columns(table) | MIGRATION_OWNED_COLUMNS[table]
    for created in create_table_columns(path.read_text(), table):
        extra = created - allowed
        assert not extra, (
            f"{path.name}: bootstrap CREATE TABLE {table} declares {sorted(extra)}, "
            f"which the runtime DDL does not have -- a rival definition of the "
            f"same table (see test_migration_fts_columns)."
        )


def test_parser_handles_nested_parens_and_constraints() -> None:
    """Defaults with parentheses, array types and table constraints parse right."""
    sql = """
        CREATE TABLE IF NOT EXISTS t (
            id SERIAL PRIMARY KEY,
            tags TEXT[],
            n NUMERIC(10, 2) DEFAULT 0,  -- comment, with a comma
            created_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT t_key UNIQUE (id, n)
        )
        ALTER TABLE t ADD COLUMN IF NOT EXISTS extra TEXT, ADD COLUMN more INT;
    """
    assert create_table_columns(sql, "t") == [{"id", "tags", "n", "created_at"}]
    assert added_columns(sql, "t") == {"extra", "more"}
