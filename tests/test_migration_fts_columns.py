"""Guard the FTS migration against columns the target table does not have.

``migrations/versions/phase4_fts_intelligence_search.py`` shipped an
``analytical_snapshots`` tsvector built from ``title`` and ``summary``. That
table has neither: its text columns are ``category`` and ``subcategory``, with
the detail in jsonb ``payload`` / ``metrics``.

The failure mode is worse than a no-op. The backfill ``UPDATE`` errors, and the
``BEFORE INSERT OR UPDATE`` trigger it installs raises

    record "new" has no field "title"

on *every* write to the table — so a mis-named column here does not degrade
search, it takes the table offline for writes. Observed live on grid-svr
2026-09-11: the trigger was installed at 05:55 UTC and every subsequent write
to ``analytical_snapshots`` failed until it was dropped.

Static source check, in the style of ``test_prediction_backtest_no_fstring_sql``
— no database required, so it runs anywhere the suite runs.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "migrations" / "versions" / "phase4_fts_intelligence_search.py"
)

# Columns each table actually has, as far as this migration is allowed to
# reference them. Sourced from the live griddb schema on grid-svr
# (information_schema.columns, 2026-09-11).
ALLOWED_COLUMNS: dict[str, set[str]] = {
    "analytical_snapshots": {
        "id", "snapshot_date", "category", "subcategory",
        "as_of_date", "payload", "metrics", "created_at", "search_vector",
    },
    "actors": {"id", "name", "category", "title", "search_vector"},
    "signal_data": {"id", "description", "ticker", "actor", "search_vector"},
    "discovered_hypotheses": {"id", "thesis", "pattern_type", "search_vector"},
}

SOURCE = MIGRATION.read_text()


def _trigger_body(table: str) -> str:
    """Return the body of ``<table>_search_vector_update()``."""
    # Run to the CLOSING dollar-quote: the opening one sits in "AS $$" on the
    # first line, so a non-greedy match to the first "$$" stops before the body.
    m = re.search(
        rf"CREATE OR REPLACE FUNCTION {table}_search_vector_update\(\)"
        rf".*?\$\$\s*LANGUAGE",
        SOURCE,
        re.DOTALL,
    )
    assert m, f"no trigger function found for {table}"
    return m.group(0)


def _backfill_body(table: str) -> str:
    """Return the ``UPDATE <table> SET search_vector ...`` statement."""
    m = re.search(
        rf"UPDATE {table}\s+SET search_vector = to_tsvector\(.*?\)\s*WHERE",
        SOURCE,
        re.DOTALL,
    )
    assert m, f"no search_vector backfill found for {table}"
    return m.group(0)


@pytest.mark.parametrize("table", sorted(ALLOWED_COLUMNS))
def test_trigger_references_only_real_columns(table: str) -> None:
    refs = set(re.findall(r"NEW\.([a-z_][a-z0-9_]*)", _trigger_body(table)))
    assert refs, f"{table}: trigger references no columns at all"
    unknown = refs - ALLOWED_COLUMNS[table]
    assert not unknown, (
        f"{table} trigger references column(s) the table does not have: "
        f"{sorted(unknown)}. A BEFORE INSERT OR UPDATE trigger naming a "
        f"missing field raises 'record \"new\" has no field ...' and blocks "
        f"every write to the table."
    )


@pytest.mark.parametrize("table", sorted(ALLOWED_COLUMNS))
def test_backfill_references_only_real_columns(table: str) -> None:
    refs = set(re.findall(r"COALESCE\(([a-z_][a-z0-9_]*)", _backfill_body(table)))
    assert refs, f"{table}: backfill references no columns at all"
    unknown = refs - ALLOWED_COLUMNS[table]
    assert not unknown, (
        f"{table} search_vector backfill references column(s) the table does "
        f"not have: {sorted(unknown)}"
    )


def test_analytical_snapshots_never_names_title_or_summary() -> None:
    """Pin the specific regression: the table has neither column."""
    for body in (
        _trigger_body("analytical_snapshots"),
        _backfill_body("analytical_snapshots"),
    ):
        for bad in ("title", "summary"):
            assert not re.search(rf"\b(NEW\.)?{bad}\b", body), (
                f"analytical_snapshots FTS names '{bad}', which it does not "
                f"have; use category / subcategory"
            )


def test_intelligence_search_view_snapshot_arm_uses_real_columns() -> None:
    """The materialized view's snapshot arm selects from the same table."""
    m = re.search(
        r"SELECT 'snapshot' AS source_type.*?FROM analytical_snapshots[^\n]*",
        SOURCE,
        re.DOTALL,
    )
    assert m, "no snapshot arm found in the intelligence_search view"
    refs = set(re.findall(r"COALESCE\(([a-z_][a-z0-9_]*)", m.group(0)))
    unknown = refs - ALLOWED_COLUMNS["analytical_snapshots"]
    assert not unknown, (
        f"intelligence_search snapshot arm references {sorted(unknown)}, "
        f"which analytical_snapshots does not have"
    )
