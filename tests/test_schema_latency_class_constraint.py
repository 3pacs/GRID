"""Pin schema.sql's source_catalog latency_class CHECK to the live set.

Closes C4 of CLAUDE-REVIEW-ASSET-REGISTRIES-LATENCY-c3fdffb4-20261020: the
live griddb constraint ``source_catalog_latency_class_check`` was widened
out-of-band to seven values (re-read 2026-10-20, with live rows in every one
of the seven classes) while ``schema.sql`` still declared four. A fresh
database gets this constraint from ``schema.sql`` alone (``python db.py`` ->
``db.apply_schema``; the alembic baseline ``7e4dfecce247`` is a no-op), so
``schema.sql`` is the only in-repo artefact that has to match production.

Parses the constraint out of the ``CREATE TABLE source_catalog`` block --
no database -- and checks it against:

  * ``ingestion.base.VALID_LATENCY_CLASSES`` when that constant exists
    (added by PR #880, ``claude/asset-registries-latency-class-20261020``);
    until it merges, a local literal mirroring the live constraint stands in.
    Follow-up once #880 is on main: drop ``_LIVE_CONSTRAINT_SET`` here and
    assert against the constant unconditionally.
  * the seed rows ``schema.sql`` itself inserts into ``source_catalog``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = REPO_ROOT / "schema.sql"

# Verbatim from griddb on 2026-10-20 (pg_get_constraintdef):
#   CHECK ((latency_class = ANY (ARRAY['REALTIME','INTRADAY','EOD','WEEKLY',
#                                      'MONTHLY','QUARTERLY','ANNUAL'])))
_LIVE_CONSTRAINT_SET = frozenset(
    {"REALTIME", "INTRADAY", "EOD", "WEEKLY", "MONTHLY", "QUARTERLY", "ANNUAL"}
)

_SOURCE_CATALOG_BLOCK = re.compile(
    r"CREATE TABLE IF NOT EXISTS source_catalog\s*\((?P<body>.*?)\n\);",
    re.DOTALL,
)
_LATENCY_CHECK = re.compile(
    r"latency_class\s+TEXT\s+NOT NULL\s+CHECK\s*\(\s*latency_class\s+IN\s*\((?P<values>[^)]*)\)\s*\)",
)


def _schema_latency_class_values() -> frozenset[str]:
    text = SCHEMA_SQL.read_text(encoding="utf-8")
    block = _SOURCE_CATALOG_BLOCK.search(text)
    assert block is not None, "CREATE TABLE source_catalog not found in schema.sql"
    body = "\n".join(
        line for line in block.group("body").splitlines() if not line.lstrip().startswith("--")
    )
    check = _LATENCY_CHECK.search(body)
    assert check is not None, "latency_class CHECK (... IN (...)) not found in source_catalog block"
    return frozenset(re.findall(r"'([A-Z]+)'", check.group("values")))


def _expected_set() -> frozenset[str]:
    try:
        from ingestion.base import VALID_LATENCY_CLASSES  # PR #880
    except ImportError:
        return _LIVE_CONSTRAINT_SET
    return frozenset(VALID_LATENCY_CLASSES)


@pytest.mark.unit
def test_schema_sql_latency_class_check_matches_live_constraint():
    assert _schema_latency_class_values() == _expected_set()


@pytest.mark.unit
def test_local_literal_matches_live_constraint_until_880_merges():
    """Guards the stand-in literal itself; once #880 merges this also proves
    the two in-repo sets agree and the literal can be removed."""
    assert _expected_set() == _LIVE_CONSTRAINT_SET


@pytest.mark.unit
def test_schema_sql_seed_rows_use_allowed_latency_classes():
    text = SCHEMA_SQL.read_text(encoding="utf-8")
    insert = re.search(
        r"INSERT INTO source_catalog\s*\((?P<cols>[^)]*)\)\s*VALUES(?P<rows>.*?)(?:\nON CONFLICT|;)",
        text,
        re.DOTALL,
    )
    assert insert is not None, "schema.sql source_catalog seed INSERT not found"
    cols = [c.strip() for c in insert.group("cols").split(",")]
    idx = cols.index("latency_class")
    rows = re.findall(r"\((.*?)\)", insert.group("rows"), re.DOTALL)
    assert rows, "no seed rows parsed"
    allowed = _schema_latency_class_values()
    bad = []
    for row in rows:
        values = [v.strip().strip("'") for v in row.split(",")]
        if values[idx] not in allowed:
            bad.append((values[0], values[idx]))
    assert not bad, f"schema.sql seeds latency_class values outside its own CHECK: {bad}"
