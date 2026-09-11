"""
Static schema-conformance guard for the resolution path.

The frozen-price incident (resolved_series stopped advancing on 2026-04-04)
was a schema mismatch that no test could see: the Hermes cycle's "fast SQL
resolution" step joined a table that does not exist (``entity_map``), wrote
to columns ``resolved_series`` does not have (``source_id``,
``resolved_at``), and targeted an ON CONFLICT key one column short of the
real unique index. Every cycle raised, the exception was swallowed by
``log.debug``, and the PIT table every analytical surface reads silently
stopped receiving rows for seven months.

Nothing about that needed a database to catch. This test parses the
declared schema and every SQL string literal in the modules on the
resolution path, and asserts that each literal naming ``raw_series`` or
``resolved_series``:

  1. references only tables that are actually declared somewhere
     (schema.sql or a migration), and
  2. uses only columns those two tables actually declare.

Purely static — no DB connection, no fixtures.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_SQL = REPO_ROOT / "schema.sql"
MIGRATIONS_DIR = REPO_ROOT / "migrations"

# The tables this guard checks column-by-column, and the modules that read
# or write them on the resolution path.
GUARDED_TABLES = ("raw_series", "resolved_series")
RESOLUTION_MODULES = (
    "normalization/resolver.py",
    "scripts/hermes_operator.py",
    "scripts/hermes_fixers.py",
    "ingestion/scheduler.py",
)

# Words that follow FROM/JOIN/INTO/UPDATE but are not table names.
_NON_TABLE_TOKENS = {
    "select", "lateral", "only", "unnest", "generate_series", "values",
}

# Leading keywords of a table constraint rather than a column definition.
_CONSTRAINT_KEYWORDS = {
    "primary", "foreign", "unique", "check", "constraint", "exclude", "like",
}


# ---------------------------------------------------------------------------
# Schema parsing
# ---------------------------------------------------------------------------

def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are not inside parentheses."""
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        items.append("".join(current))
    return items


def _create_table_body(sql: str, start: int) -> str | None:
    """Return the parenthesised body of a CREATE TABLE starting at `start`."""
    open_paren = sql.find("(", start)
    if open_paren == -1:
        return None
    depth = 0
    for index in range(open_paren, len(sql)):
        if sql[index] == "(":
            depth += 1
        elif sql[index] == ")":
            depth -= 1
            if depth == 0:
                return sql[open_paren + 1:index]
    return None


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
_ADD_COLUMN_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:[a-z_][a-z0-9_]*\.)?"
    r"([a-z_][a-z0-9_]*)\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
_OP_CREATE_TABLE_RE = re.compile(r"""op\.create_table\(\s*["']([a-z_][a-z0-9_]*)["']""")


def parse_table_columns(sql: str) -> dict[str, set[str]]:
    """Map table name -> declared column names for every CREATE TABLE in `sql`."""
    tables: dict[str, set[str]] = {}
    for match in _CREATE_TABLE_RE.finditer(sql):
        table = match.group(1).lower()
        body = _create_table_body(sql, match.end())
        if body is None:
            continue
        columns: set[str] = set()
        for item in _split_top_level(body):
            item = item.strip()
            if not item:
                continue
            first = item.split()[0].strip('"').lower()
            if first in _CONSTRAINT_KEYWORDS:
                continue
            if re.fullmatch(r"[a-z_][a-z0-9_]*", first):
                columns.add(first)
        tables.setdefault(table, set()).update(columns)
    return tables


def _migration_sql() -> str:
    """Concatenated SQL of every migration file (plain .sql only)."""
    if not MIGRATIONS_DIR.exists():
        return ""
    parts = [
        path.read_text(encoding="utf-8", errors="replace")
        for path in sorted(MIGRATIONS_DIR.rglob("*.sql"))
    ]
    return "\n".join(parts)


def declared_schema() -> tuple[dict[str, set[str]], set[str]]:
    """Return (columns of the guarded tables, every table name that exists).

    Columns come from schema.sql — the authoritative declaration for
    raw_series and resolved_series — widened by any migration that adds a
    column to them, so a legitimately migrated column is not a failure.

    The set of existing table names spans schema.sql, the plain-SQL
    migrations and alembic's ``op.create_table`` calls, because a query may
    legitimately join a table introduced by a migration. ``entity_map``
    appears in none of them, which is the point.
    """
    schema_text = SCHEMA_SQL.read_text(encoding="utf-8")
    migration_text = _migration_sql()

    schema_tables = parse_table_columns(schema_text)
    guarded = {
        table: set(schema_tables.get(table, set())) for table in GUARDED_TABLES
    }
    for table, column in _ADD_COLUMN_RE.findall(migration_text):
        table = table.lower()
        if table in guarded:
            guarded[table].add(column.lower())

    existing = set(schema_tables)
    existing.update(parse_table_columns(migration_text))
    for path in sorted((MIGRATIONS_DIR / "versions").glob("*.py")):
        source = path.read_text(encoding="utf-8", errors="replace")
        existing.update(_OP_CREATE_TABLE_RE.findall(source))

    return guarded, existing


# ---------------------------------------------------------------------------
# SQL literal extraction
# ---------------------------------------------------------------------------

def _resolve_str(node: ast.AST, consts: dict[str, str]) -> str | None:
    """Best-effort static evaluation of a string expression.

    Handles plain literals, module-level string constants referenced by
    name, ``+`` concatenation of either, and f-strings (interpolated parts
    become a placeholder so the surrounding SQL is still checkable). This
    is what lets the guard see a query assembled from a fixed prefix plus
    a fixed predicate constant, as normalization/resolver.py does.
    """
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.Name):
        return consts.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolve_str(node.left, consts)
        right = _resolve_str(node.right, consts)
        if left is not None and right is not None:
            return left + right
        return None
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            else:
                parts.append(" __expr__ ")
        return "".join(parts)
    return None


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level NAME = "..." string constants, for name resolution."""
    consts: dict[str, str] = {}
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue
        text_value = _resolve_str(value, consts)
        if text_value is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                consts[target.id] = text_value
    return consts


_SQL_STATEMENT_RE = re.compile(
    r"\b(select|insert|update|delete|from|join)\b", re.IGNORECASE
)
# A predicate fragment reads as SQL without naming a table.
_SQL_FRAGMENT_RE = re.compile(
    r"(>=|<=|<>|!=|=|\bAND\b|\bOR\b|\bWHERE\b|\bORDER\s+BY\b|\bIS\b|\bIN\b)",
    re.IGNORECASE,
)


def _resolvable_strings(source: str) -> list[str]:
    """Every statically resolvable string expression in `source`, deduped."""
    tree = ast.parse(source)
    consts = _module_string_constants(tree)
    found: list[str] = []
    seen: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Constant, ast.BinOp, ast.JoinedStr)):
            continue
        value = _resolve_str(node, consts)
        if not value or value in seen:
            continue
        seen.add(value)
        found.append(value)
    return found


def sql_literals(source: str) -> list[str]:
    """Every statically resolvable string in `source` that names a guarded table."""
    found: list[str] = []
    for value in _resolvable_strings(source):
        lowered = value.lower()
        if not any(table in lowered for table in GUARDED_TABLES):
            continue
        # Only strings that actually read as SQL, not prose mentioning the
        # table (docstrings, log messages, dict keys).
        if not _SQL_STATEMENT_RE.search(lowered):
            continue
        found.append(value)
    return found


def module_alias_bindings(source: str) -> dict[str, set[str]]:
    """Alias -> guarded table(s), pooled across every SQL literal in the module.

    Queries are not always one literal: normalization/resolver.py assembles
    its raw_series reads from a fixed prefix (which names the table and
    binds the alias) plus one of three fixed predicate constants (which
    only use the alias). Pooling the bindings per module is what lets the
    column check see those predicates at all.

    Pooling is deliberately lenient where a module binds the same alias to
    both guarded tables — `rs` is `resolved_series` in one hermes_fixers
    query and `raw_series` in another — so such an alias is checked against
    the union of both column sets. That still catches a column that exists
    on neither, which is the failure mode this guard exists for.
    """
    bindings: dict[str, set[str]] = {}
    for sql in sql_literals(source):
        for alias, tables in alias_bindings(sql).items():
            bindings.setdefault(alias, set()).update(tables)
    return bindings


def sql_units(source: str) -> list[str]:
    """SQL strings to column-check: full statements plus predicate fragments.

    A fragment qualifies when it reads as SQL and references an alias this
    module binds to a guarded table.
    """
    bindings = module_alias_bindings(source)
    units = sql_literals(source)
    seen = set(units)
    if not bindings:
        return units
    alias_re = re.compile(
        r"\b(" + "|".join(sorted(re.escape(a) for a in bindings)) + r")\.",
        re.IGNORECASE,
    )
    for value in _resolvable_strings(source):
        if value in seen:
            continue
        if not alias_re.search(value):
            continue
        if not _SQL_FRAGMENT_RE.search(value):
            continue
        seen.add(value)
        units.append(value)
    return units


# ---------------------------------------------------------------------------
# Reference checks over one SQL literal
# ---------------------------------------------------------------------------

_REF_RE = re.compile(
    r"\b(FROM|JOIN|INTO|UPDATE)\s+(?:ONLY\s+)?(?:[a-z_][a-z0-9_]*\.)?"
    r"([a-z_][a-z0-9_]*)(?:\s+(?:AS\s+)?([a-z_][a-z0-9_]*))?",
    re.IGNORECASE,
)
_CTE_RE = re.compile(r"([a-z_][a-z0-9_]*)\s+AS\s*\(", re.IGNORECASE)
_INSERT_COLUMNS_RE = re.compile(
    r"INSERT\s+INTO\s+(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)\s*\(([^)]*)\)",
    re.IGNORECASE,
)
# Words that can follow an alias slot without being an alias.
_ALIAS_STOPWORDS = {
    "on", "where", "set", "using", "group", "order", "limit", "having",
    "join", "inner", "left", "right", "full", "cross", "lateral", "and",
    "or", "as", "values", "select", "returning", "union", "window", "for",
    "offset", "natural", "from",
}


def referenced_tables(sql: str) -> set[str]:
    """Table names referenced after FROM/JOIN/INTO/UPDATE, excluding CTEs."""
    ctes = {name.lower() for name in _CTE_RE.findall(sql)}
    tables: set[str] = set()
    for _keyword, table, _alias in _REF_RE.findall(sql):
        table = table.lower()
        if table in _NON_TABLE_TOKENS or table in ctes:
            continue
        tables.add(table)
    return tables


def alias_bindings(sql: str) -> dict[str, set[str]]:
    """Map each alias (and each bare table name) to the guarded tables it binds."""
    bindings: dict[str, set[str]] = {}
    for _keyword, table, alias in _REF_RE.findall(sql):
        table = table.lower()
        if table not in GUARDED_TABLES:
            continue
        bindings.setdefault(table, set()).add(table)
        alias = (alias or "").lower()
        if alias and alias not in _ALIAS_STOPWORDS:
            bindings.setdefault(alias, set()).add(table)
    return bindings


def qualified_column_violations(
    sql: str,
    columns: dict[str, set[str]],
    bindings: dict[str, set[str]] | None = None,
) -> list[str]:
    """``alias.column`` references that the bound guarded table doesn't declare.

    Args:
        sql: The SQL string (statement or predicate fragment) to check.
        columns: Declared columns per guarded table.
        bindings: Alias -> guarded table(s). Defaults to the bindings
            visible in `sql` itself; pass module-wide bindings to also
            check fragments that use an alias bound elsewhere.
    """
    if bindings is None:
        bindings = alias_bindings(sql)
    if not bindings:
        return []
    violations: list[str] = []
    for alias, column in re.findall(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)", sql):
        tables = bindings.get(alias.lower())
        if not tables:
            continue
        allowed: set[str] = set()
        for table in tables:
            allowed |= columns.get(table, set())
        if column.lower() not in allowed:
            violations.append(f"{alias}.{column} (bound to {sorted(tables)})")
    return violations


def insert_column_violations(sql: str, columns: dict[str, set[str]]) -> list[str]:
    """Column names in an ``INSERT INTO <guarded table> (...)`` list that don't exist."""
    violations: list[str] = []
    for table, column_list in _INSERT_COLUMNS_RE.findall(sql):
        table = table.lower()
        if table not in GUARDED_TABLES:
            continue
        for raw in column_list.split(","):
            column = raw.strip().strip('"').lower()
            if not re.fullmatch(r"[a-z_][a-z0-9_]*", column):
                continue
            if column not in columns.get(table, set()):
                violations.append(f"INSERT INTO {table} ({column})")
    return violations


# ---------------------------------------------------------------------------
# Tests — the parser itself
# ---------------------------------------------------------------------------

class TestSchemaParsing:
    """The guard is only as good as its schema parse, so pin that first."""

    def test_resolved_series_columns_match_schema(self):
        columns, _ = declared_schema()
        assert columns["resolved_series"] == {
            "id", "feature_id", "obs_date", "release_date", "vintage_date",
            "value", "source_priority_used", "conflict_flag",
            "conflict_detail", "resolution_version",
        }

    def test_raw_series_columns_match_schema(self):
        columns, _ = declared_schema()
        assert columns["raw_series"] == {
            "id", "series_id", "source_id", "obs_date", "pull_timestamp",
            "value", "raw_payload", "pull_status",
        }

    def test_resolved_series_has_no_source_id_or_resolved_at(self):
        """The two columns the broken INSERT wrote to never existed."""
        columns, _ = declared_schema()
        assert "source_id" not in columns["resolved_series"]
        assert "resolved_at" not in columns["resolved_series"]

    def test_entity_map_is_not_a_table(self):
        """`entity_map` is a Python module, not a relation — the broken
        INSERT ... SELECT joined it as if it were one."""
        _, existing = declared_schema()
        assert "entity_map" not in existing

    def test_core_tables_are_declared(self):
        _, existing = declared_schema()
        for table in ("raw_series", "resolved_series", "source_catalog",
                      "feature_registry"):
            assert table in existing


# ---------------------------------------------------------------------------
# Tests — the conformance guard over real modules
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("module_path", RESOLUTION_MODULES)
class TestResolutionModuleConformance:

    def test_referenced_tables_exist(self, module_path):
        """Every table joined by a raw_series/resolved_series query exists."""
        columns, existing = declared_schema()
        source = (REPO_ROOT / module_path).read_text(encoding="utf-8")
        offenders: list[str] = []
        for sql in sql_literals(source):
            for table in sorted(referenced_tables(sql) - existing):
                offenders.append(f"{table} in: {' '.join(sql.split())[:180]}")
        assert not offenders, (
            f"{module_path} references undeclared table(s):\n  "
            + "\n  ".join(offenders)
        )

    def test_qualified_columns_are_declared(self, module_path):
        """No query reads a raw_series/resolved_series column that doesn't exist."""
        columns, _ = declared_schema()
        source = (REPO_ROOT / module_path).read_text(encoding="utf-8")
        bindings = module_alias_bindings(source)
        offenders: list[str] = []
        for sql in sql_units(source):
            for violation in qualified_column_violations(sql, columns, bindings):
                offenders.append(f"{violation} in: {' '.join(sql.split())[:180]}")
        assert not offenders, (
            f"{module_path} uses undeclared column(s):\n  " + "\n  ".join(offenders)
        )

    def test_insert_column_lists_are_declared(self, module_path):
        """No INSERT names a raw_series/resolved_series column that doesn't exist."""
        columns, _ = declared_schema()
        source = (REPO_ROOT / module_path).read_text(encoding="utf-8")
        offenders: list[str] = []
        for sql in sql_literals(source):
            for violation in insert_column_violations(sql, columns):
                offenders.append(f"{violation} in: {' '.join(sql.split())[:180]}")
        assert not offenders, (
            f"{module_path} inserts undeclared column(s):\n  "
            + "\n  ".join(offenders)
        )

    def test_guard_actually_sees_sql_in_the_module(self, module_path):
        """Guard against the guard silently matching nothing — if a refactor
        moves these queries somewhere the extractor can't follow, the tests
        above would pass vacuously."""
        source = (REPO_ROOT / module_path).read_text(encoding="utf-8")
        assert sql_literals(source), (
            f"no raw_series/resolved_series SQL found in {module_path} — "
            "either the queries moved or the extractor regressed"
        )


# ---------------------------------------------------------------------------
# Tests — the guard catches the exact historical regression
# ---------------------------------------------------------------------------

BROKEN_INSERT = """
    INSERT INTO resolved_series (feature_id, obs_date, value, source_id, resolved_at)
    SELECT em.feature_id, rs.obs_date, rs.value, rs.source_id, NOW()
    FROM raw_series rs
    JOIN entity_map em ON em.series_id = rs.series_id
    WHERE rs.pull_status = 'SUCCESS'
    ON CONFLICT (feature_id, obs_date) DO NOTHING
"""


class TestGuardCatchesTheRegression:
    """Feed the guard the shape of commit b0a02b4's INSERT and prove each
    check fires. Without this the tests above could pass by never looking."""

    def test_nonexistent_table_is_caught(self):
        _, existing = declared_schema()
        assert "entity_map" in referenced_tables(BROKEN_INSERT) - existing

    def test_nonexistent_insert_columns_are_caught(self):
        columns, _ = declared_schema()
        violations = insert_column_violations(BROKEN_INSERT, columns)
        assert any("source_id" in v for v in violations)
        assert any("resolved_at" in v for v in violations)

    def test_valid_sql_passes_every_check(self):
        """The resolver's real INSERT must be clean under all three checks."""
        columns, existing = declared_schema()
        good = (
            "INSERT INTO resolved_series "
            "(feature_id, obs_date, release_date, vintage_date, "
            "value, source_priority_used, conflict_flag, conflict_detail) "
            "SELECT rs.series_id, rs.obs_date, rs.pull_timestamp, rs.value "
            "FROM raw_series rs "
            "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING"
        )
        assert not referenced_tables(good) - existing
        assert not insert_column_violations(good, columns)
        assert not qualified_column_violations(good, columns)

    def test_broken_insert_is_caught_through_module_extraction(self):
        """End-to-end: put the broken INSERT inside a module the way commit
        b0a02b4 did and prove the extractor finds it and every check fires.
        Checking the functions in isolation is not enough — the bug shipped
        because nothing looked at the module at all."""
        columns, existing = declared_schema()
        module_source = (
            "from sqlalchemy import text\n"
            "def step(conn):\n"
            '    conn.execute(text("""' + BROKEN_INSERT + '"""))\n'
        )

        literals = sql_literals(module_source)
        assert literals, "extractor missed the INSERT inside the module"

        bindings = module_alias_bindings(module_source)
        undeclared_tables: set[str] = set()
        insert_violations: list[str] = []
        for sql in sql_units(module_source):
            undeclared_tables |= referenced_tables(sql) - existing
            insert_violations += insert_column_violations(sql, columns)
            qualified_column_violations(sql, columns, bindings)

        assert "entity_map" in undeclared_tables
        assert any("source_id" in v for v in insert_violations)
        assert any("resolved_at" in v for v in insert_violations)

    def test_window_predicate_fragments_are_checked(self):
        """normalization/resolver.py assembles its raw_series reads from a
        fixed prefix plus one of three fixed window predicates chosen at
        run time. The prefix names the table; the predicates only use the
        alias. Both halves must end up in the checked set, or a typo like
        `rs.pulled_at` in a predicate would sail through."""
        source = (REPO_ROOT / "normalization/resolver.py").read_text(encoding="utf-8")
        units = sql_units(source)
        predicates = [u for u in units if "pull_timestamp >=" in u and "FROM" not in u]
        assert len(predicates) >= 3, (
            f"expected the 3 window predicates among the checked units, "
            f"got {predicates}"
        )

    def test_a_typo_in_a_window_predicate_would_be_caught(self):
        """Prove the fragment path actually validates columns."""
        columns, _ = declared_schema()
        bindings = {"rs": {"raw_series"}}
        assert qualified_column_violations(
            "rs.pull_timestamp >= :since", columns, bindings
        ) == []
        assert qualified_column_violations(
            "rs.pulled_at >= :since", columns, bindings
        ) != []
