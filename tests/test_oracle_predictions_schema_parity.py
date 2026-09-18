"""The oracle engine's CREATE TABLE and alembic revision oracle_pred_nullable_0918
must agree: ``entry_price`` and ``confidence`` are nullable on both paths.

The revision guards with ``ALTER TABLE IF EXISTS`` (the engine creates the
table, alembic never does), so on a database where the oracle has not run the
revision is a silent no-op and the engine's CREATE is the only thing that
shapes the table. If the CREATE drifted back to NOT NULL nothing would raise
until the first honest NULL insert failed in production. This pins both halves
without a database, and pins that the engine bootstrap issues no ALTER COLUMN
of its own (that ran ACCESS EXCLUSIVE on every bootstrap).
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "oracle" / "engine.py"
REVISION = ROOT / "migrations" / "versions" / "oracle_pred_nullable_0918.py"


def _create_block() -> str:
    src = ENGINE.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS oracle_predictions \((.*?)\n\s*\)", src, re.S
    )
    assert m, "oracle_predictions CREATE TABLE not found in oracle/engine.py"
    return m.group(1)


def _column_decl(block: str, col: str) -> str:
    for line in block.splitlines():
        if line.strip().startswith(col + " "):
            return line.strip()
    raise AssertionError(f"{col} not declared in the oracle_predictions CREATE")


def test_engine_create_declares_both_columns_nullable():
    block = _create_block()
    for col in ("entry_price", "confidence"):
        decl = _column_decl(block, col)
        assert "NOT NULL" not in decl.upper(), decl


def test_revision_relaxes_exactly_those_columns():
    src = REVISION.read_text(encoding="utf-8")
    assert 'revision: str = "oracle_pred_nullable_0918"' in src
    assert len("oracle_pred_nullable_0918") <= 32
    assert '_COLUMNS = ("entry_price", "confidence")' in src
    assert "ALTER TABLE IF EXISTS oracle_predictions ALTER COLUMN" in src
    assert "DROP NOT NULL" in src


def test_engine_bootstrap_issues_no_alter_column():
    # Comments may mention the statement; executed SQL may not contain it.
    code = "\n".join(
        line for line in ENGINE.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("#")
    )
    assert re.search(r"ALTER\s+COLUMN", code, re.I) is None, (
        "oracle/engine.py must not ALTER COLUMN at runtime; that belongs in alembic"
    )


def test_revision_never_updates_or_deletes_rows():
    src = REVISION.read_text(encoding="utf-8")
    body = src[src.index("def upgrade"):]
    assert "UPDATE oracle_predictions" not in body
    assert "DELETE FROM oracle_predictions" not in body
