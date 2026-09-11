"""Schema conformance guard for SQL that writes or reads resolved_series.

The 2026-03-29 Hermes regression (commit b0a02b4) shipped a statement that
named four things which do not exist: the table ``entity_map``, the columns
``source_id`` and ``resolved_at``, and a unique constraint on
``(feature_id, obs_date)``. It parsed fine in Python, failed on every
execution, and was logged at debug — so nothing in CI or in the logs said a
word for five and a half months.

This module re-reads ``schema.sql`` and checks every SQL literal that names
``resolved_series`` in the four modules that write or read it outside the API
layer — ``scripts/hermes_operator.py``, ``normalization/resolver.py``,
``scripts/hermes_fixers.py`` and ``ingestion/scheduler.py``:

  * every column named in an INSERT column list is a real column;
  * every ``alias.column`` reference bound to resolved_series is real;
  * every ``ON CONFLICT (...)`` target matches a UNIQUE index that exists;
  * every table joined in such a statement is declared somewhere in the
    schema (``schema.sql`` or a migration).

The checker is exercised against the historical bad statement so the guard
itself cannot rot into a no-op, and ``test_every_guarded_module_yields_sql``
fails if a module in the list stops producing literals at all — a rename or a
refactor to f-strings would otherwise turn its coverage into a silent pass,
which is the same failure mode the regression exploited.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "schema.sql"
# The pipeline modules whose resolved_series SQL is schema-checked: the
# resolver (the only writer), the two Hermes-side readers, and the operator
# itself.
GUARDED_MODULES = (
    "scripts/hermes_operator.py",
    "normalization/resolver.py",
    "scripts/hermes_fixers.py",
    "ingestion/scheduler.py",
)

# Of those, the ones that must actually yield SQL literals. A module here
# that produces nothing is a hole, not a pass: the extractor only sees
# ``ast.Constant`` strings, so SQL moved into an f-string or a template
# would silently drop out of coverage. See
# TestGuardedModules.test_modules_that_must_yield_sql_still_do.
MODULES_REQUIRING_SQL = (
    "normalization/resolver.py",
    "scripts/hermes_fixers.py",
    "ingestion/scheduler.py",
)

# scripts/hermes_operator.py is the exception, and deliberately so: since the
# b0a02b4 statement was removed it carries no resolved_series SQL at all, and
# must not regain any. The resolver is the single writer; a hand-rolled
# INSERT reappearing in the cycle is the exact shape of the regression. It
# stays in GUARDED_MODULES so anything added there is checked on arrival.
MODULE_REQUIRING_NO_SQL = "scripts/hermes_operator.py"

# Words that can follow a table name but are not an alias.
_NOT_AN_ALIAS = {
    "where", "group", "order", "limit", "offset", "on", "join", "left",
    "right", "inner", "outer", "full", "cross", "set", "values", "returning",
    "select", "union", "having", "using", "as", "into", "for", "window",
    "fetch", "lateral", "natural", "and", "or", "not", "with",
}


# ---------------------------------------------------------------------------
# schema.sql introspection
# ---------------------------------------------------------------------------

def _table_columns(table: str) -> set[str]:
    """Column names declared in schema.sql's CREATE TABLE for ``table``."""
    schema = SCHEMA_PATH.read_text()
    match = re.search(
        r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+" + table + r"\s*\((.*?)\n\);",
        schema,
        re.S | re.I,
    )
    assert match, f"no CREATE TABLE for {table} in schema.sql"
    columns: set[str] = set()
    for line in match.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        first = line.split()[0]
        if first.upper() in {
            "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "CONSTRAINT", "EXCLUDE",
        }:
            continue
        columns.add(first.strip(",").lower())
    return columns


def _unique_index_columns(table: str) -> set[tuple[str, ...]]:
    """Column tuples covered by a UNIQUE index/constraint on ``table``."""
    schema = SCHEMA_PATH.read_text()
    found: set[tuple[str, ...]] = set()
    pattern = re.compile(
        r"CREATE UNIQUE INDEX(?:\s+IF NOT EXISTS)?\s+\w+\s+ON\s+" + table
        + r"\s*\(([^)]*)\)",
        re.S | re.I,
    )
    for match in pattern.finditer(schema):
        cols = tuple(
            c.strip().split()[0].lower()
            for c in match.group(1).split(",")
            if c.strip()
        )
        found.add(cols)
    # A single-column PRIMARY KEY is also a unique target.
    create = re.search(
        r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+" + table + r"\s*\((.*?)\n\);",
        schema, re.S | re.I,
    )
    if create:
        for line in create.group(1).splitlines():
            if "PRIMARY KEY" in line.upper():
                found.add((line.strip().split()[0].lower(),))
    return found


def _declared_tables() -> set[str]:
    """Every table name declared in schema.sql or in a .sql migration."""
    sources = [SCHEMA_PATH.read_text()]
    migrations = REPO_ROOT / "migrations"
    if migrations.is_dir():
        for path in migrations.rglob("*.sql"):
            sources.append(path.read_text(errors="replace"))
    names: set[str] = set()
    for text in sources:
        for match in re.finditer(
            r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+([\w.]+)", text, re.I,
        ):
            names.add(match.group(1).split(".")[-1].lower())
    return names


# ---------------------------------------------------------------------------
# SQL literal checking
# ---------------------------------------------------------------------------

def _sql_literals(path: Path) -> list[str]:
    """Every string constant in a module that mentions resolved_series."""
    tree = ast.parse(path.read_text())
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if "resolved_series" in node.value.lower():
                out.append(node.value)
    return out


def check_sql(sql: str, columns: set[str], uniques: set[tuple[str, ...]],
              tables: set[str]) -> list[str]:
    """Return a list of schema violations found in one SQL literal."""
    problems: list[str] = []
    lowered = sql.lower()

    # 1. INSERT INTO resolved_series (col, col, ...)
    for match in re.finditer(
        r"insert\s+into\s+resolved_series\s*\(([^)]*)\)", lowered, re.S,
    ):
        for raw in match.group(1).split(","):
            col = raw.strip()
            if col and col not in columns:
                problems.append(f"INSERT names unknown column '{col}'")

    # 2. ON CONFLICT (...) must match a real unique index.
    if "resolved_series" in lowered:
        for match in re.finditer(r"on\s+conflict\s*\(([^)]*)\)", lowered, re.S):
            target = tuple(
                c.strip() for c in match.group(1).split(",") if c.strip()
            )
            for col in target:
                if col not in columns:
                    problems.append(f"ON CONFLICT names unknown column '{col}'")
            if target and all(c in columns for c in target) and target not in uniques:
                problems.append(
                    f"ON CONFLICT {target} has no matching UNIQUE index"
                )

    # 3. alias.column references bound to resolved_series.
    aliases = {"resolved_series"}
    for match in re.finditer(
        r"(?:from|join|into|update)\s+resolved_series\s+(?:as\s+)?(\w+)",
        lowered,
    ):
        alias = match.group(1)
        if alias not in _NOT_AN_ALIAS:
            aliases.add(alias)
    for alias in aliases:
        for match in re.finditer(rf"\b{alias}\.(\w+)", lowered):
            col = match.group(1)
            if col not in columns:
                problems.append(f"'{alias}.{col}' is not a resolved_series column")

    # 4. Tables referenced in a resolved_series statement must exist.
    for match in re.finditer(r"(?:from|join)\s+([a-z_][\w.]*)", lowered):
        table = match.group(1).split(".")[-1]
        if table in {"resolved_series"} or table in tables:
            continue
        if table in _NOT_AN_ALIAS:
            continue
        problems.append(f"unknown table '{table}'")

    return problems


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def schema():
    return (
        _table_columns("resolved_series"),
        _unique_index_columns("resolved_series"),
        _declared_tables(),
    )


# ---------------------------------------------------------------------------
# Tests — the schema itself
# ---------------------------------------------------------------------------

class TestSchemaIntrospection:

    def test_expected_columns(self, schema):
        columns, _, _ = schema
        assert columns == {
            "id", "feature_id", "obs_date", "release_date", "vintage_date",
            "value", "source_priority_used", "conflict_flag",
            "conflict_detail", "resolution_version",
        }

    def test_columns_the_2026_03_regression_invented_do_not_exist(self, schema):
        columns, _, _ = schema
        assert "source_id" not in columns
        assert "resolved_at" not in columns

    def test_unique_key_includes_vintage_date(self, schema):
        _, uniques, _ = schema
        assert ("feature_id", "obs_date", "vintage_date") in uniques
        assert ("feature_id", "obs_date") not in uniques

    def test_entity_map_is_not_a_table(self, schema):
        _, _, tables = schema
        assert "entity_map" not in tables, (
            "entity_map is the Python SEED_MAPPINGS dict, not a table"
        )


# ---------------------------------------------------------------------------
# Tests — the guarded modules
# ---------------------------------------------------------------------------

class TestGuardedModules:

    @pytest.mark.parametrize("rel_path", GUARDED_MODULES)
    def test_every_sql_literal_conforms_to_schema(self, rel_path, schema):
        columns, uniques, tables = schema
        path = REPO_ROOT / rel_path
        failures: list[str] = []
        for sql in _sql_literals(path):
            for problem in check_sql(sql, columns, uniques, tables):
                failures.append(f"{rel_path}: {problem}\n    in: {sql.strip()[:160]}")
        assert not failures, "\n".join(failures)

    def test_resolver_insert_is_still_covered(self, schema):
        """Guard the guard: the resolver's INSERT must be one of the literals."""
        literals = _sql_literals(REPO_ROOT / "normalization/resolver.py")
        assert any("INSERT INTO resolved_series" in s for s in literals)

    @pytest.mark.parametrize("rel_path", GUARDED_MODULES)
    def test_guarded_module_exists(self, rel_path):
        """A renamed or deleted module must fail loudly, not drop coverage."""
        assert (REPO_ROOT / rel_path).exists(), (
            f"{rel_path} is in GUARDED_MODULES but does not exist"
        )

    @pytest.mark.parametrize("rel_path", MODULES_REQUIRING_SQL)
    def test_modules_that_must_yield_sql_still_do(self, rel_path):
        """Guard the guard: a covered module must still expose SQL to check.

        ``_sql_literals`` only sees ``ast.Constant`` strings. If a module is
        refactored so its resolved_series SQL is built by an f-string, a
        helper or a template, the extractor returns nothing and
        ``test_every_sql_literal_conforms_to_schema`` passes vacuously —
        exactly the shape of the 2026-03 regression, where nothing failed
        because nothing was looking.
        """
        literals = _sql_literals(REPO_ROOT / rel_path)
        assert literals, (
            f"{rel_path} yielded no resolved_series SQL literals — either it "
            f"no longer touches the table (move it out of "
            f"MODULES_REQUIRING_SQL) or its SQL is no longer a plain string "
            f"constant, in which case the checker is blind to it and must be "
            f"taught the new shape"
        )

    def test_hermes_operator_has_no_resolved_series_sql(self):
        """The cycle must not hand-roll resolved_series SQL again.

        b0a02b4 put an INSERT ... SELECT into the cycle and wrapped it in
        ``except Exception: log.debug(...)``; it named a table and columns
        that do not exist and failed silently for 5.5 months. The resolver
        is the single writer, reached through
        ``_run_resolution_step`` — the operator itself should name
        resolved_series nowhere in SQL.
        """
        literals = _sql_literals(REPO_ROOT / MODULE_REQUIRING_NO_SQL)
        assert literals == [], (
            f"{MODULE_REQUIRING_NO_SQL} has regained resolved_series SQL. "
            f"Route writes through normalization.resolver.Resolver instead: "
            f"{literals}"
        )


# ---------------------------------------------------------------------------
# Tests — the checker detects the historical regression
# ---------------------------------------------------------------------------

_REGRESSION_SQL = """
    INSERT INTO resolved_series (feature_id, obs_date, value, source_id, resolved_at)
    SELECT em.feature_id, rs.obs_date, rs.value, rs.source_id, NOW()
    FROM raw_series rs
    JOIN entity_map em ON em.series_id = rs.series_id
    WHERE rs.pull_timestamp > NOW() - INTERVAL '1 hour'
    AND rs.pull_status = 'SUCCESS'
    AND NOT EXISTS (
        SELECT 1 FROM resolved_series res
        WHERE res.feature_id = em.feature_id
        AND res.obs_date = rs.obs_date
    )
    ON CONFLICT (feature_id, obs_date) DO NOTHING
"""


class TestCheckerCatchesTheRegression:

    def test_regression_sql_is_rejected(self, schema):
        problems = check_sql(_REGRESSION_SQL, *schema)
        joined = " | ".join(problems)
        assert "source_id" in joined
        assert "resolved_at" in joined
        assert "entity_map" in joined
        assert "no matching UNIQUE index" in joined

    def test_current_resolver_insert_is_accepted(self, schema):
        good = (
            "INSERT INTO resolved_series "
            "(feature_id, obs_date, release_date, vintage_date, "
            "value, source_priority_used, conflict_flag, conflict_detail) "
            "VALUES (:fid, :od, :rd, :vd, :val, :src, :cf, :cd) "
            "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING"
        )
        assert check_sql(good, *schema) == []


# ---------------------------------------------------------------------------
# Tests — the health surface must not report a future resolver run
# ---------------------------------------------------------------------------

class TestPipelineHealthResolverStatus:
    """`/pipeline-health` reports resolver.last_run = MAX(vintage_date).

    Sources that publish forward-dated releases (FRED calendar rows on
    griddb reach 2026-12-31) made that read report a run five months in the
    future while resolution was dead. The query must clamp to today.
    """

    def _resolver_status_sql(self) -> list[str]:
        source = (REPO_ROOT / "api/routers/system.py").read_text()
        tree = ast.parse(source)
        return [
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and "vintage_date" in node.value
            and "resolved_series" in node.value
        ]

    def test_vintage_reads_are_clamped_to_today(self):
        statements = self._resolver_status_sql()
        assert statements, "no resolver vintage query found in system router"
        for sql in statements:
            assert "vintage_date <= CURRENT_DATE" in sql, sql

    def test_last_run_uses_max_vintage(self):
        assert any(
            "MAX(vintage_date)" in sql for sql in self._resolver_status_sql()
        )
