"""Static schema-contract check for scripts/autoresearch.py.

GRID W4 slice (2026-09-18/19): the working hypothesis going into this task
was that ``scripts/autoresearch.py`` queries a column
(``feature_registry.subfamily``) that no longer exists on the live schema,
that the query therefore raises, and that the raised exception is what
silently keeps ``OperatorState.hypotheses_tested`` at 0 (swallowed by a bare
``log.warning`` in ``scripts/hermes_fixers.py::maybe_run_autoresearch``).

This test is a *static* verifier — it never connects to any database. It
parses every ``CREATE TABLE`` in schema.sql and every SQL string passed to
``cursor.execute(...)`` in scripts/autoresearch.py, and asserts that every
``alias.column`` / bare-column reference actually names a column schema.sql
defines for that table.

The original known-finding bundled two separate claims:

1. **The counter-key bug** (``iterations_run`` vs ``iterations``) —
   **REPRODUCED**, and fixed at commit 8a20c84a (see
   test_autoresearch_failure_visibility.py). This is a plain Python
   dict-key mismatch between what ``run_autoresearch()`` returns and what
   ``scripts/hermes_fixers.py:1698`` (``maybe_run_autoresearch``) reads via
   ``result.get("iterations", 0)`` — fully verifiable from source, no DB
   involved.

2. **``feature_registry.subfamily`` vs ``signal_subtype``** — **SETTLED,
   2026-09-19**. A read-only catalog query was run directly against griddb
   on 2026-09-19:

       SELECT column_name FROM information_schema.columns
       WHERE table_name = 'feature_registry'
       ORDER BY ordinal_position;

   Production's actual column list is: id, name, family, description,
   transformation, transformation_version, lag_days, normalization,
   missing_data_policy, eligible_from_date, model_eligible, created_at,
   deprecated_at, signal_domain, signal_subtype. There is **no**
   ``subfamily`` column in production. ``signal_domain``/``signal_subtype``
   were added by the untracked runtime DDL in
   scripts/signal_taxonomy.py (an ``ALTER TABLE feature_registry ADD
   COLUMN ... TEXT`` inside a ``DO`` block, plus
   ``CREATE INDEX IF NOT EXISTS idx_feature_domain ON feature_registry
   (signal_domain)`` / ``idx_feature_subtype ON feature_registry
   (signal_subtype)``), never through the tracked migrations/ chain or
   schema.sql — which is exactly the gap this slice closes.

   The fix reuses, verbatim, a fix that was already written and correct but
   never pushed: commit 91e1e6750f40ae6eb461aa5455f72ebf01ea2fbb (2026-07-15,
   author Anik), which existed only in grid-svr's old checkout
   (``/home/grid/grid_v4/grid_repo``). ``scripts/autoresearch.py::get_feature_list``
   now selects ``COALESCE(f.signal_subtype, '')`` (was ``f.subfamily``) and
   groups by ``f.signal_subtype`` (was ``f.subfamily``) — see that
   function's docstring for the full history.

   ``schema.sql`` and a new alembic revision
   (``migrations/versions/feature_signal_taxonomy_20260919.py``) now declare
   ``signal_domain``/``signal_subtype`` (and their indexes) on
   ``feature_registry``, reconciling the tracked schema with what production
   actually has. ``subfamily`` stays declared in schema.sql — removing it is
   out of scope (taxonomy_fix.sql still references it) — but production does
   not have it, and scripts/autoresearch.py must not (and, after this
   change, does not) query it.

This test is kept as a regression guard against tracked source (it WOULD
fail if a future edit introduced a genuine column typo against schema.sql),
and to make the tracked-source portion of this verification reproducible
rather than just asserted in a report.

--------------------------------------------------------------------------
Extractor limits (documented, not fixed here — this is intentionally a
lightweight regex/AST-based check, not a real SQL parser):

  - Table column definitions are extracted from schema.sql by locating each
    ``CREATE TABLE IF NOT EXISTS <name> ( ... );`` block and splitting its
    body on top-level commas (paren-depth aware, so a CHECK(...) with
    embedded commas doesn't fracture the split). The first whitespace-
    delimited token of each resulting fragment is taken as the column name
    unless it is a table-level keyword (CONSTRAINT/PRIMARY/FOREIGN/UNIQUE/
    CHECK). This does not understand ALTER TABLE ... ADD COLUMN anywhere
    (schema.sql declares signal_domain/signal_subtype as ordinary columns
    directly, so no ALTER TABLE parsing is needed for those; taxonomy_fix.sql
    and scripts/signal_taxonomy.py's runtime ALTER TABLEs remain out of
    scope by design, per the task's instruction to check schema.sql/migrations
    only).
  - Query strings are extracted from scripts/autoresearch.py via `ast`,
    matched on any ``<expr>.execute(<string literal>, ...)`` call. Adjacent
    string-literal concatenation (``"a" "b"``) is already folded into one
    Constant by Python's own parser, so multi-line INSERT/UPDATE statements
    built that way are handled for free.
  - Table/alias bindings are read from ``FROM <table> [AS] <alias>`` and
    ``JOIN <table> [AS] <alias>`` via regex. CTEs, derived tables (a
    subquery used as a FROM source), and UNIONs are not resolved — none
    appear in scripts/autoresearch.py today.
  - ``alias.column`` references are matched via a plain ``\\w+\\.\\w+``
    regex applied to the whole query text. It does not know about string
    literals or comments, so a literal like ``'f.oo'`` inside a query would
    be (mis)treated as a reference; none exist in this file today.
  - Bare (unqualified) columns are only checked for the simple forms this
    file actually uses: ``INSERT INTO <table> (<cols>) VALUES (...)``,
    ``UPDATE <table> SET <col>=... [, ...] WHERE ...``, and a plain
    ``SELECT <col, col, ...> FROM <table> WHERE ...`` with no aliases,
    functions, or ``*``. Anything fancier (subselects, CASE expressions,
    computed columns) is skipped rather than mis-flagged.
  - CHECK constraints, defaults, and column types are ignored; only column
    *names* are validated, not value domains.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
AUTORESEARCH_PY = REPO_ROOT / "scripts" / "autoresearch.py"
SCHEMA_SQL = REPO_ROOT / "schema.sql"

_TABLE_LEVEL_KEYWORDS = {"CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"}


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are not inside parens."""
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
    if current:
        parts.append("".join(current))
    return parts


def parse_schema_columns(schema_sql_text: str) -> dict[str, set[str]]:
    """Map table name -> set of column names, from CREATE TABLE blocks."""
    tables: dict[str, set[str]] = {}
    for match in re.finditer(
        r"CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\((.*?)\n\);",
        schema_sql_text,
        re.DOTALL | re.IGNORECASE,
    ):
        table_name = match.group(1)
        body = match.group(2)
        columns: set[str] = set()
        for fragment in _split_top_level(body):
            fragment = fragment.strip()
            if not fragment:
                continue
            first_word = fragment.split()[0] if fragment.split() else ""
            if not re.match(r"^\w+$", first_word):
                continue
            if first_word.upper() in _TABLE_LEVEL_KEYWORDS:
                continue
            columns.add(first_word)
        tables[table_name] = columns
    return tables


def extract_query_strings(py_path: Path) -> list[str]:
    """Pull every string literal passed as the first arg to `.execute(...)`."""
    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    queries: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "execute"):
            continue
        if not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            queries.append(first.value)
    return queries


_FROM_JOIN_RE = re.compile(r"\b(?:FROM|JOIN)\s+(\w+)\s+(?:AS\s+)?(\w+)\b", re.IGNORECASE)
_ALIAS_COLUMN_RE = re.compile(r"\b(\w+)\.(\w+)\b")
_INSERT_RE = re.compile(r"INSERT\s+INTO\s+(\w+)\s*\(([^)]*)\)", re.IGNORECASE | re.DOTALL)
_UPDATE_RE = re.compile(r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\bWHERE\b|$)", re.IGNORECASE | re.DOTALL)
_SIMPLE_SELECT_RE = re.compile(
    r"SELECT\s+(?!\*)([\w\s,]+?)\s+FROM\s+(\w+)\s+WHERE",
    re.IGNORECASE | re.DOTALL,
)


def check_query(query: str, schema: dict[str, set[str]]) -> list[str]:
    """Return a list of human-readable violation strings for one query."""
    violations: list[str] = []

    # 1. alias.column references, resolved via this query's FROM/JOIN aliases.
    alias_to_table = {alias: table for table, alias in _FROM_JOIN_RE.findall(query)}
    for alias, column in _ALIAS_COLUMN_RE.findall(query):
        table = alias_to_table.get(alias)
        if table is None or table not in schema:
            continue  # unresolved alias / unmodeled table — documented limit
        if column not in schema[table]:
            violations.append(
                f"{table}.{column} (referenced as {alias}.{column}) is not a "
                f"column of {table} in schema.sql"
            )

    # 2. INSERT INTO table (col, col, ...)
    for table, col_list in _INSERT_RE.findall(query):
        if table not in schema:
            continue
        for col in col_list.split(","):
            col = col.strip()
            if not col or not re.match(r"^\w+$", col):
                continue
            if col not in schema[table]:
                violations.append(f"{table}.{col} (INSERT column list) is not a column of {table} in schema.sql")

    # 3. UPDATE table SET col=..., col=... WHERE ...
    for table, set_clause in _UPDATE_RE.findall(query):
        if table not in schema:
            continue
        for assignment in _split_top_level(set_clause):
            assignment = assignment.strip()
            if not assignment or "=" not in assignment:
                continue
            col = assignment.split("=", 1)[0].strip()
            if not re.match(r"^\w+$", col):
                continue
            if col not in schema[table]:
                violations.append(f"{table}.{col} (UPDATE SET) is not a column of {table} in schema.sql")

    # 4. Plain "SELECT col, col FROM table WHERE ..." with no alias/functions/*.
    for col_list, table in _SIMPLE_SELECT_RE.findall(query):
        if table not in schema:
            continue
        if "(" in col_list or "." in col_list:
            continue  # has a function call or an alias-qualified column — skip
        for col in col_list.split(","):
            col = col.strip()
            if not col or not re.match(r"^\w+$", col):
                continue
            if col not in schema[table]:
                violations.append(f"{table}.{col} (bare SELECT) is not a column of {table} in schema.sql")

    return violations


@pytest.fixture(scope="module")
def schema_columns() -> dict[str, set[str]]:
    return parse_schema_columns(SCHEMA_SQL.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def autoresearch_queries() -> list[str]:
    return extract_query_strings(AUTORESEARCH_PY)


def test_extractor_finds_feature_registry_with_subfamily(schema_columns):
    """Sanity check on the extractor itself, independent of autoresearch.py.

    schema.sql declares `signal_domain`/`signal_subtype` on feature_registry
    (added 2026-09-19 to reconcile with what scripts/signal_taxonomy.py's
    untracked runtime DDL already put on production — see the module
    docstring). `subfamily` is also still declared: removing it is out of
    scope for this change (taxonomy_fix.sql still references it), even
    though the 2026-09-19 production catalog query found no `subfamily`
    column live. If this fails, the regex extractor above is broken, not
    the schema.
    """
    assert "feature_registry" in schema_columns
    assert "subfamily" in schema_columns["feature_registry"]
    assert "signal_domain" in schema_columns["feature_registry"]
    assert "signal_subtype" in schema_columns["feature_registry"]
    assert "family" in schema_columns["feature_registry"]
    assert "model_eligible" in schema_columns["feature_registry"]


def test_extractor_finds_expected_tables(schema_columns):
    for table in (
        "feature_registry",
        "resolved_series",
        "hypothesis_registry",
        "model_registry",
        "validation_results",
    ):
        assert table in schema_columns, f"extractor did not find CREATE TABLE for {table}"


def test_autoresearch_queries_found_at_least_one():
    queries = extract_query_strings(AUTORESEARCH_PY)
    assert len(queries) >= 8, (
        "expected to find the known cur.execute(...) call sites in "
        "scripts/autoresearch.py; extractor may have broken on a refactor"
    )


def test_every_autoresearch_column_reference_exists_in_schema(schema_columns, autoresearch_queries):
    """The actual contract check.

    Every column scripts/autoresearch.py references on feature_registry,
    resolved_series, hypothesis_registry, model_registry, and
    validation_results must be a column schema.sql defines for that table.

    As documented in this module's docstring, this finds ZERO violations
    against tracked source: schema.sql now declares every column
    scripts/autoresearch.py references (including signal_subtype, added
    2026-09-19), and the query fix in get_feature_list() means autoresearch.py
    no longer references subfamily at all.
    """
    all_violations: list[str] = []
    for query in autoresearch_queries:
        all_violations.extend(check_query(query, schema_columns))

    assert not all_violations, "column/schema mismatches found:\n" + "\n".join(all_violations)


def test_autoresearch_does_not_query_subfamily(autoresearch_queries):
    """Regression guard for the fix: no cur.execute(...) string in
    scripts/autoresearch.py may reference `subfamily` — production has no
    such column (2026-09-19 catalog query) — and at least one must
    reference `signal_subtype`, the column get_feature_list() now uses.

    This directly guards against re-introducing the bug this slice fixes
    (grid-svr commit 91e1e6750f40ae6eb461aa5455f72ebf01ea2fbb, reused here):
    scripts/autoresearch.py:444-450 (get_feature_list) used to select
    `COALESCE(f.subfamily, '')` and group by `f.subfamily`; it now selects
    and groups by `f.signal_subtype`.
    """
    assert not any("subfamily" in q for q in autoresearch_queries), (
        "scripts/autoresearch.py queries `subfamily`, which does not exist "
        "on production feature_registry (2026-09-19 catalog query) — this "
        "is the bug fixed by reusing grid-svr commit 91e1e675"
    )
    assert any("signal_subtype" in q for q in autoresearch_queries), (
        "expected at least one cur.execute(...) string in "
        "scripts/autoresearch.py to reference signal_subtype "
        "(get_feature_list's fixed query) — extractor may have broken, or "
        "the fix was reverted"
    )


def test_taxonomy_migration_is_idempotent_and_non_destructive():
    """Static check on the new alembic revision's DDL and downgrade.

    migrations/versions/feature_signal_taxonomy_20260919.py's TAXONOMY_DDL
    must only ever use ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT
    EXISTS (so re-running it, e.g. against griddb where
    scripts/signal_taxonomy.py already applied the same DDL by hand, is
    always safe), and its downgrade() must never DROP the columns it added
    (production data written by scripts/signal_taxonomy.py would be
    destroyed — see that function's docstring).
    """
    import ast

    migration_path = (
        REPO_ROOT / "migrations" / "versions" / "feature_signal_taxonomy_20260919.py"
    )
    source = migration_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    taxonomy_ddl: list[str] = []
    for node in ast.walk(tree):
        is_plain_assign = isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "TAXONOMY_DDL" for t in node.targets
        )
        is_annotated_assign = (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "TAXONOMY_DDL"
        )
        if is_plain_assign or is_annotated_assign:
            for elt in node.value.elts:
                assert isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                taxonomy_ddl.append(elt.value)

    assert taxonomy_ddl, "TAXONOMY_DDL not found or not a literal tuple of strings"
    for stmt in taxonomy_ddl:
        upper = stmt.upper()
        is_safe_add_column = "ADD COLUMN IF NOT EXISTS" in upper
        is_safe_create_index = "CREATE INDEX IF NOT EXISTS" in upper
        assert is_safe_add_column or is_safe_create_index, (
            f"TAXONOMY_DDL statement is not an idempotent ADD COLUMN/CREATE "
            f"INDEX: {stmt!r}"
        )
        assert "DROP" not in upper, f"TAXONOMY_DDL statement contains DROP: {stmt!r}"

    # downgrade() source must contain no DROP statement.
    downgrade_source = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "downgrade":
            downgrade_source = ast.get_source_segment(source, node)
            break
    assert downgrade_source is not None, "downgrade() function not found"
    # Look for actual DDL/alembic-op DROP calls, not the English word "drop"
    # in the docstring's prose (which explains *why* it must not drop).
    drop_ddl = re.search(
        r"\bDROP\s+(COLUMN|TABLE|INDEX)\b|op\.drop_(column|index|table)\(",
        downgrade_source,
        re.IGNORECASE,
    )
    assert drop_ddl is None, (
        f"downgrade() must not drop signal_domain/signal_subtype — production "
        f"data written by scripts/signal_taxonomy.py would be destroyed "
        f"(found: {drop_ddl.group(0) if drop_ddl else None!r})"
    )
