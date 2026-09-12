"""Guard the FTS migrations against columns the target table does not have.

``migrations/versions/phase4_fts_intelligence_search.py`` shipped an
``analytical_snapshots`` tsvector built from ``title`` and ``summary``. That
table has neither: its text columns are ``category`` and ``subcategory``, with
the detail in jsonb ``payload`` / ``metrics``.

The failure mode is worse than a no-op. The backfill ``UPDATE`` errors, and the
``BEFORE INSERT OR UPDATE`` trigger it installs raises

    record "new" has no field "title"

on *every* write to the table — so a mis-named column here does not degrade
search, it takes the table offline for writes. Observed live on grid-svr
2026-09-11: the trigger was installed at 05:55 UTC and every write to
``analytical_snapshots`` failed until 15:26 UTC, when it was dropped by hand.

Where ``title`` and ``summary`` came from
-----------------------------------------
They were real — in a *different* declaration of the same table name.
``scripts/parse_datasets.py`` used to carry its own ``CREATE TABLE IF NOT
EXISTS analytical_snapshots`` with ``actor / ticker / title / summary / data``
columns, unrelated to the table ``store/snapshots.py`` actually creates and
every writer actually uses. Two rival DDLs for one table name is what made the
migration look correct to its author. So this module checks three things:

1. the FTS migrations only name columns the canonical DDL declares;
2. the canonical DDL is the only one in the repository; and
3. the ``intelligence_search`` view keeps every arm it has been given.

(1) is derived from ``store/snapshots.py``'s ``ANALYTICAL_SNAPSHOTS_DDL``
rather than from a hand-copied column list, so changing the table
automatically re-checks the migrations against it.

Static source checks, in the style of
``test_prediction_backtest_no_fstring_sql`` — no database required, so they run
anywhere the suite runs.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "migrations" / "versions"
SNAPSHOT_STORE = ROOT / "store" / "snapshots.py"

# Every migration that builds FTS objects over these tables. The original bug
# lived in the first of these; the guard missing the other two is how a
# corrected migration could still be undone by a later one.
FTS_MIGRATIONS: tuple[Path, ...] = (
    VERSIONS / "phase4_fts_intelligence_search.py",
    VERSIONS / "reapply_snapshot_fts_20260911.py",
    VERSIONS / "restore_news_search_arm_20260912.py",
)

# ``search_vector`` is added by the FTS migration itself, not by the table's
# own bootstrap DDL, so it is legal to reference but will not be found there.
FTS_ADDED_COLUMNS = {"search_vector"}


def _canonical_snapshot_columns() -> set[str]:
    """Columns declared by ``store.snapshots.ANALYTICAL_SNAPSHOTS_DDL``.

    Read as text rather than imported: this module must stay free of
    numpy/pandas/sqlalchemy so it runs in a bare CI environment.

    Returns:
        set[str]: Column names in the canonical ``analytical_snapshots`` DDL.
    """
    source = SNAPSHOT_STORE.read_text()
    m = re.search(
        r"ANALYTICAL_SNAPSHOTS_DDL\s*=\s*\"\"\"(.*?)\"\"\"",
        source,
        re.DOTALL,
    )
    assert m, (
        "store/snapshots.py no longer exposes ANALYTICAL_SNAPSHOTS_DDL — the "
        "canonical analytical_snapshots DDL must stay a named constant so "
        "this guard can check the migrations against it"
    )
    body = m.group(1)
    inner = re.search(r"analytical_snapshots\s*\((.*)\)", body, re.DOTALL)
    assert inner, "could not parse the column list out of ANALYTICAL_SNAPSHOTS_DDL"
    columns = set()
    for line in inner.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        name = line.split()[0]
        if name.upper() in {"PRIMARY", "UNIQUE", "CONSTRAINT", "FOREIGN", "CHECK"}:
            continue
        columns.add(name)
    assert columns, "ANALYTICAL_SNAPSHOTS_DDL parsed to zero columns"
    return columns


# Columns each table is allowed to be referenced by. analytical_snapshots is
# derived from the canonical DDL; the other three are pinned from the live
# griddb schema on grid-svr (information_schema.columns, 2026-09-12) because
# they have no single in-repo DDL to derive from.
ALLOWED_COLUMNS: dict[str, set[str]] = {
    "analytical_snapshots": _canonical_snapshot_columns() | FTS_ADDED_COLUMNS,
    "actors": {"id", "name", "category", "title", "search_vector"},
    "signal_data": {"id", "description", "ticker", "actor", "search_vector"},
    "discovered_hypotheses": {"id", "thesis", "pattern_type", "search_vector"},
}


def _read(path: Path) -> str:
    """Source of ``path``, or ``""`` if it is missing.

    Collection must not explode when a migration named by ``FTS_MIGRATIONS``
    has been deleted — ``test_fts_migrations_all_exist`` reports that.

    Parameters:
        path: File to read.

    Returns:
        str: File contents, or an empty string.
    """
    return path.read_text() if path.exists() else ""


# (migration path, table) pairs that actually declare a trigger / backfill.
_TRIGGER_CASES = [
    (path, table)
    for path in FTS_MIGRATIONS
    for table in sorted(ALLOWED_COLUMNS)
    if re.search(
        rf"CREATE OR REPLACE FUNCTION\s+{table}_search_vector_update\(\)",
        _read(path),
    )
]

_BACKFILL_CASES = [
    (path, table)
    for path in FTS_MIGRATIONS
    for table in sorted(ALLOWED_COLUMNS)
    if re.search(rf"UPDATE {table}\s+SET search_vector", _read(path))
]


def _trigger_body(source: str, table: str) -> str:
    """Return the body of ``<table>_search_vector_update()``."""
    # Run to the CLOSING dollar-quote: the opening one sits in "AS $$" on the
    # first line, so a non-greedy match to the first "$$" stops before the body.
    m = re.search(
        rf"CREATE OR REPLACE FUNCTION\s+{table}_search_vector_update\(\)"
        rf".*?\$\$\s*LANGUAGE",
        source,
        re.DOTALL,
    )
    assert m, f"no trigger function found for {table}"
    return m.group(0)


def _backfill_body(source: str, table: str) -> str:
    """Return the ``UPDATE <table> SET search_vector ...`` statement."""
    m = re.search(
        rf"UPDATE {table}\s+SET search_vector = to_tsvector\(.*?\)\s*WHERE",
        source,
        re.DOTALL,
    )
    assert m, f"no search_vector backfill found for {table}"
    return m.group(0)


def test_fts_migrations_all_exist() -> None:
    """Every migration this guard claims to cover is actually present."""
    missing = [p.name for p in FTS_MIGRATIONS if not p.exists()]
    assert not missing, f"FTS migrations named by the guard are missing: {missing}"


def test_every_fts_migration_is_covered() -> None:
    """No migration touches search_vector without this guard knowing about it."""
    covered = {p.name for p in FTS_MIGRATIONS}
    uncovered = sorted(
        p.name
        for p in VERSIONS.glob("*.py")
        if p.name not in covered
        and re.search(r"search_vector\s*:?=|SET search_vector", p.read_text())
    )
    assert not uncovered, (
        f"migration(s) {uncovered} build search_vector objects but are not in "
        f"FTS_MIGRATIONS, so nothing checks their column references. Add them."
    )


@pytest.mark.parametrize(
    ("path", "table"), _TRIGGER_CASES, ids=lambda v: getattr(v, "name", v)
)
def test_trigger_references_only_real_columns(path: Path, table: str) -> None:
    body = _trigger_body(_read(path), table)
    refs = set(re.findall(r"NEW\.([a-z_][a-z0-9_]*)", body))
    assert refs, f"{path.name}/{table}: trigger references no columns at all"
    unknown = refs - ALLOWED_COLUMNS[table]
    assert not unknown, (
        f"{path.name}: {table} trigger references column(s) the table does not "
        f"have: {sorted(unknown)}. A BEFORE INSERT OR UPDATE trigger naming a "
        f"missing field raises 'record \"new\" has no field ...' and blocks "
        f"every write to the table."
    )


@pytest.mark.parametrize(
    ("path", "table"), _BACKFILL_CASES, ids=lambda v: getattr(v, "name", v)
)
def test_backfill_references_only_real_columns(path: Path, table: str) -> None:
    body = _backfill_body(_read(path), table)
    refs = set(re.findall(r"COALESCE\(([a-z_][a-z0-9_]*)", body))
    assert refs, f"{path.name}/{table}: backfill references no columns at all"
    unknown = refs - ALLOWED_COLUMNS[table]
    assert not unknown, (
        f"{path.name}: {table} search_vector backfill references column(s) the "
        f"table does not have: {sorted(unknown)}"
    )


def test_analytical_snapshots_never_names_title_or_summary() -> None:
    """Pin the specific regression: the table has neither column."""
    for path, table in _TRIGGER_CASES + _BACKFILL_CASES:
        if table != "analytical_snapshots":
            continue
        source = path.read_text()
        bodies = []
        if (path, table) in _TRIGGER_CASES:
            bodies.append(_trigger_body(source, table))
        if (path, table) in _BACKFILL_CASES:
            bodies.append(_backfill_body(source, table))
        for body in bodies:
            for bad in ("title", "summary"):
                assert not re.search(rf"\b(NEW\.)?{bad}\b", body), (
                    f"{path.name}: analytical_snapshots FTS names '{bad}', "
                    f"which it does not have; use category / subcategory"
                )


@pytest.mark.parametrize("path", FTS_MIGRATIONS, ids=lambda p: p.name)
def test_intelligence_search_view_snapshot_arm_uses_real_columns(path: Path) -> None:
    """The materialized view's snapshot arm selects from the same table."""
    source = _read(path)
    for m in re.finditer(
        r"SELECT 'snapshot'.*?FROM analytical_snapshots[^\n]*", source, re.DOTALL
    ):
        refs = set(re.findall(r"COALESCE\(([a-z_][a-z0-9_]*)", m.group(0)))
        unknown = refs - ALLOWED_COLUMNS["analytical_snapshots"]
        assert not unknown, (
            f"{path.name}: intelligence_search snapshot arm references "
            f"{sorted(unknown)}, which analytical_snapshots does not have"
        )


# ---------------------------------------------------------------------------
# Root cause: one table name, two DDLs
# ---------------------------------------------------------------------------

def test_analytical_snapshots_has_exactly_one_ddl() -> None:
    """Only ``store/snapshots.py`` may declare ``analytical_snapshots``.

    This is the test that would have caught the original bug before it
    shipped. ``scripts/parse_datasets.py`` carried a rival
    ``CREATE TABLE IF NOT EXISTS analytical_snapshots`` with ``title`` and
    ``summary`` columns; the FTS migration was written against that shape and
    took the real table offline for writes. Tests are exempt — they stand up
    throwaway fixtures.
    """
    pattern = re.compile(
        r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?analytical_snapshots\b",
        re.IGNORECASE,
    )
    offenders = []
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT)
        # Skip dotted dirs (.git, .venv, .claude worktrees — which are whole
        # copies of this repo and would report their own copy of the file),
        # vendored trees, and test fixtures, which stand up throwaway tables.
        if any(part.startswith(".") for part in rel.parts):
            continue
        if rel.parts[0] in {"tests", "node_modules", "pwa", "venv", "site-packages"}:
            continue
        if rel == Path("store/snapshots.py"):
            continue
        try:
            if pattern.search(path.read_text(errors="ignore")):
                offenders.append(str(rel))
        except OSError:  # pragma: no cover - unreadable file
            continue
    assert not offenders, (
        f"{sorted(offenders)} declare their own analytical_snapshots table. "
        f"store/snapshots.py::ANALYTICAL_SNAPSHOTS_DDL is the only definition; "
        f"a second one is what let the FTS migration name title/summary and "
        f"block every write to the real table."
    )


def test_canonical_ddl_has_no_title_or_summary() -> None:
    """The columns the broken trigger wanted must not quietly reappear."""
    columns = _canonical_snapshot_columns()
    assert "title" not in columns and "summary" not in columns, (
        "analytical_snapshots gained a title/summary column. If that is "
        "deliberate, the FTS trigger and the intelligence_search snapshot arm "
        "both need updating in the same change."
    )


# ---------------------------------------------------------------------------
# intelligence_search arms
# ---------------------------------------------------------------------------

def test_latest_view_rebuild_keeps_every_arm() -> None:
    """A view rebuild must not silently drop a source type.

    ``reapply_snapshot_fts_20260911`` rebuilt ``intelligence_search`` from
    ``phase4_fts_001``'s four-arm definition to fix the snapshot arm, which
    dropped the ``news`` arm ``phase4_fts_002`` had added — 75,230 news rows
    fell out of the search corpus with nothing erroring.
    """
    source = _read(VERSIONS / "restore_news_search_arm_20260912.py")
    m = re.search(
        r"CREATE MATERIALIZED VIEW intelligence_search AS(.*?)\"\"\"",
        source,
        re.DOTALL,
    )
    assert m, (
        "no five-arm intelligence_search definition found. The latest view "
        "rebuild must carry every source type; on griddb the reapply revision "
        "left a four-arm view with news_articles indexed by nothing."
    )
    arms = set(re.findall(r"SELECT '([a-z_]+)' AS source_type", m.group(1)))
    expected = {"actor", "signal", "hypothesis", "snapshot", "news"}
    assert arms == expected, (
        f"intelligence_search rebuild has arms {sorted(arms)}, expected "
        f"{sorted(expected)}. A rebuild that omits an arm removes that whole "
        f"corpus from search without raising anything."
    )
