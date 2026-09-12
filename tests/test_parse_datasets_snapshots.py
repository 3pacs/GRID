"""``scripts/parse_datasets.py`` must write the real analytical_snapshots.

The script used to declare its own ``analytical_snapshots`` — ``actor /
ticker / title / summary / data`` — and INSERT into those columns. The real
table, owned by ``store/snapshots.py``, is ``snapshot_date / category /
subcategory / as_of_date / payload / metrics`` and has never had any of them.

Two consequences, both live:

* every ``_insert_snapshots_batch`` call against griddb failed outright
  (``column "actor" of relation "analytical_snapshots" does not exist``), and
* the phantom shape is what the FTS migration was written against, so its
  trigger assigned ``NEW.title`` and PostgreSQL rejected *every* write to the
  table with ``record "new" has no field "title"`` (2026-09-11, 05:55–15:26
  UTC).

These tests pin the mapping onto the canonical columns. The helper under test
is pure, so ``db`` is stubbed when psycopg2 is not installed rather than
skipping — this check must run in a bare environment too.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "parse_datasets.py"

CANONICAL_COLUMNS = {
    "snapshot_date", "category", "subcategory", "as_of_date", "payload",
}


def _load_module():
    """Import scripts/parse_datasets.py as a module.

    Prefers the real ``db``; falls back to a stub when psycopg2 is absent, so
    the pure row-mapping helper stays testable without a database driver.
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    try:
        import db  # noqa: F401
    except Exception:
        stub = ModuleType("db")
        stub.get_engine = lambda *a, **k: None  # type: ignore[attr-defined]
        sys.modules["db"] = stub

    spec = importlib.util.spec_from_file_location("parse_datasets", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ImportError as exc:  # pragma: no cover - bare env without sqlalchemy
        pytest.skip(f"parse_datasets deps unavailable: {exc}")
    return module


@pytest.fixture(scope="module")
def parse_datasets():
    return _load_module()


@pytest.fixture(scope="module")
def snapshot_row(parse_datasets):
    return parse_datasets.DatasetParser._snapshot_row


def test_row_uses_only_canonical_columns(snapshot_row) -> None:
    row = snapshot_row({
        "category": "congressional_trade",
        "snapshot_date": date(2026, 3, 4),
        "actor": "A Senator",
        "ticker": "NVDA",
        "title": "A Senator — purchase",
        "summary": "purchase NVDA ($1,001 - $15,000)",
        "data": json.dumps({"chamber": "senate"}),
        "confidence": "confirmed",
        "source_id": "senate_efds",
    })
    assert set(row) == CANONICAL_COLUMNS, (
        "the insert must bind only columns analytical_snapshots actually has"
    )


def test_row_never_binds_title_or_summary_as_columns(snapshot_row) -> None:
    """The exact columns the broken FTS trigger believed in."""
    row = snapshot_row({"category": "fed_speech", "title": "t", "summary": "s"})
    for phantom in ("title", "summary", "actor", "ticker", "data", "confidence"):
        assert phantom not in row, (
            f"'{phantom}' is not a column of analytical_snapshots; it belongs "
            f"in the jsonb payload"
        )


def test_descriptive_fields_survive_in_the_payload(snapshot_row) -> None:
    row = snapshot_row({
        "category": "fed_speech",
        "snapshot_date": date(2026, 5, 1),
        "actor": "Jerome Powell",
        "ticker": None,
        "title": "Economic Outlook",
        "summary": "Powell at Jackson Hole",
        "data": json.dumps({"location": "Jackson Hole"}),
        "confidence": "confirmed",
        "source_id": "fed_speeches",
    })
    payload = json.loads(row["payload"])
    assert payload["actor"] == "Jerome Powell"
    assert payload["title"] == "Economic Outlook"
    assert payload["summary"] == "Powell at Jackson Hole"
    assert payload["source_id"] == "fed_speeches"
    assert payload["data"] == {"location": "Jackson Hole"}


def test_source_id_becomes_subcategory(snapshot_row) -> None:
    row = snapshot_row({"category": "congressional_trade", "source_id": "house_disclosures"})
    assert row["subcategory"] == "house_disclosures"


def test_missing_date_is_filled_because_the_columns_are_not_null(snapshot_row) -> None:
    """``snapshot_date`` and ``as_of_date`` are both NOT NULL on the real table."""
    row = snapshot_row({"category": "fed_speech", "snapshot_date": None})
    assert isinstance(row["snapshot_date"], date)
    assert row["as_of_date"] == row["snapshot_date"]


def test_non_json_data_is_kept_rather_than_crashing(snapshot_row) -> None:
    row = snapshot_row({"category": "fed_speech", "data": "not json at all"})
    assert json.loads(row["payload"])["data"] == {"raw": "not json at all"}


def test_insert_statement_names_only_canonical_columns(parse_datasets) -> None:
    """Static check on the SQL itself, independent of the row mapping."""
    source = SCRIPT.read_text()
    start = source.index("INSERT INTO analytical_snapshots")
    stmt = source[start:start + 400]
    for phantom in ("actor", "ticker", "title", "summary", "confidence"):
        assert f":{phantom}" not in stmt, (
            f"the analytical_snapshots INSERT still binds :{phantom}, which "
            f"is not a column of that table"
        )
