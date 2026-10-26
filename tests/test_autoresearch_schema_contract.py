"""Static schema-contract check for scripts/autoresearch.py.

GRID W4 slice (2026-09-18): the working hypothesis going into this task was
that ``scripts/autoresearch.py`` queries a column (``feature_registry.subfamily``)
that no longer exists on the live schema, that the query therefore raises,
and that the raised exception is what silently keeps
``OperatorState.hypotheses_tested`` at 0 (swallowed by a bare
``log.warning`` in ``scripts/hermes_fixers.py::maybe_run_autoresearch``).

This test is a *static* verifier, and it only ever looks at tracked source in
this worktree (schema.sql + migrations/versions/ + scripts/autoresearch.py at
base origin/main 3fe3f5ef) — it never connects to any database, per this
lane's hard boundary (no local Postgres exists; this lane must not connect to
one). It parses every ``CREATE TABLE`` in schema.sql and every SQL string
passed to ``cursor.execute(...)`` in scripts/autoresearch.py, and asserts that
every ``alias.column`` / bare-column reference actually names a column
schema.sql defines for that table.

The original known-finding bundled two separate claims. They resolve
differently against tracked source:

1. **The counter-key bug** (``iterations_run`` vs ``iterations``) —
   **REPRODUCED**, and fixed at commit 8a20c84a (see
   test_autoresearch_failure_visibility.py). This is a plain Python
   dict-key mismatch between what ``run_autoresearch()`` returns and what
   ``scripts/hermes_fixers.py:1698`` (``maybe_run_autoresearch``) reads via
   ``result.get("iterations", 0)`` — fully verifiable from source, no DB
   involved.

2. **``feature_registry.subfamily`` vs ``signal_subtype``** — **not
   reproducible from tracked source on 3fe3f5ef**. ``schema.sql:89-110``
   and the alembic baseline migration
   (migrations/versions/7e4dfecce247_baseline_schema_from_schema_sql.py)
   both define ``feature_registry.subfamily`` as a live column, and no
   migration under migrations/versions/ renames or drops it.
   ``signal_subtype`` is a *different* column, addable to
   ``feature_registry`` only by the standalone, untracked script
   scripts/signal_taxonomy.py (its own "ALTER TABLE feature_registry ADD
   COLUMN signal_domain/signal_subtype", run outside the migrations/ chain
   and outside schema.sql), plus a genuine, unrelated
   ``signal_data.signal_subtype`` column added by
   migrations/0053_signal_subtype.sql.

   Local source cannot disprove the earlier production schema mismatch.
   This result is UNRESOLVED ACROSS VERSIONS, not a refutation of the
   2026-09-17 audit: that audit measured `feature_registry` live, and
   production may have since been (or already was) altered by
   scripts/signal_taxonomy.py's untracked runtime DDL, or by other
   out-of-band changes made under the open incident — none of which show
   up in this worktree's tracked source. This test only proves what
   schema.sql and migrations/versions/ say on 3fe3f5ef; it says nothing
   about what is actually deployed.

   The read-only catalog query that would settle it, for whoever has
   production access to run it:

       SELECT column_name FROM information_schema.columns
       WHERE table_name = 'feature_registry'
       ORDER BY ordinal_position;

   This lane does **not** run that query, or any query, against any
   database — no local Postgres exists, and this task's hard boundary
   forbids connecting to one.

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
    (schema.sql has none for the tables this file touches; taxonomy_migration.sql
    and scripts/signal_taxonomy.py's runtime ALTER TABLEs are out of scope by
    design, per the task's instruction to check schema.sql/migrations only).
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

    schema.sql:89-110 defines feature_registry with a `subfamily` column.
    If this fails, the regex extractor above is broken, not the schema.
    """
    assert "feature_registry" in schema_columns
    assert "subfamily" in schema_columns["feature_registry"]
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

    As documented in this module's docstring, this currently finds ZERO
    violations against tracked source: the originally-hypothesized
    `f.subfamily` vs `signal_subtype` mismatch is not reproducible from
    tracked source on 3fe3f5ef (schema.sql / migrations/versions/). That is
    not the same as settled against production — see the module docstring
    for the read-only catalog query that would settle it, and why this lane
    does not run it.
    """
    all_violations: list[str] = []
    for query in autoresearch_queries:
        all_violations.extend(check_query(query, schema_columns))

    assert not all_violations, "column/schema mismatches found:\n" + "\n".join(all_violations)


def test_subfamily_is_a_real_column_not_a_typo_for_signal_subtype(schema_columns):
    """Confirms the known-finding hypothesis is not reproducible from
    tracked source on 3fe3f5ef. It does NOT settle the question against
    production — see the module docstring: local source cannot disprove
    the earlier production schema mismatch, which the 2026-09-17 audit
    measured live and which untracked runtime DDL (scripts/signal_taxonomy.py)
    or other out-of-band incident changes could have altered since. This
    result is UNRESOLVED ACROSS VERSIONS, not "false".

    scripts/autoresearch.py:256-269 (get_feature_list) selects
    `COALESCE(f.subfamily, '')` from feature_registry. The hypothesis was
    that this column had been renamed/replaced by `signal_subtype`. On
    tracked source only: both schema.sql and the alembic baseline
    migration (migrations/versions/7e4dfecce247_baseline_schema_from_schema_sql.py)
    define `feature_registry.subfamily` as a real column, and no migration
    under migrations/versions/ touches it. `signal_subtype` is a distinct
    column addable out-of-band by scripts/signal_taxonomy.py's own ALTER
    TABLE (outside the tracked migration chain) — on tracked source it
    coexists with `subfamily` rather than replacing it, but whether that
    script (or something else) has since altered production is exactly
    what `information_schema.columns` on the live DB would show, and this
    lane does not query it.
    """
    feature_registry_cols = schema_columns["feature_registry"]
    assert "subfamily" in feature_registry_cols
    # signal_subtype is not part of the tracked schema.sql definition for
    # feature_registry at all (it's bolted on by a separate, untracked
    # script) — confirming it is not "the" schema column for this table.
    assert "signal_subtype" not in feature_registry_cols
