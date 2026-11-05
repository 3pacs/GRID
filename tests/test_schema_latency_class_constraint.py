"""Pin the composed schema.sql source_catalog CHECK to its shared constant.

This composition requires PR #880's VALID_LATENCY_CLASSES unconditionally;
missing imports or dependency errors must fail collection rather than use a
stand-in set. Schema and seed checks are source-only and do not establish
an installed database's constraint state.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ingestion.base import VALID_LATENCY_CLASSES

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = REPO_ROOT / "schema.sql"

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


@pytest.mark.unit
def test_schema_sql_latency_class_check_matches_shared_constant():
    assert _schema_latency_class_values() == frozenset(VALID_LATENCY_CLASSES)


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
