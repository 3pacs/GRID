"""The god-view revision is not self-contained: its materialized view reads two tables that the
revision itself never creates, so ``alembic upgrade`` through it fails with
``relation ... does not exist`` unless both already exist (observed on isolated databases,
2026-09-18).

* ``insider_trades`` is created by the earlier revision ``f1a2b3c4d5e6_capital_flow_tables`` —
  but a database built from ``schema.sql`` and *stamped* at a later revision skips that revision
  (and the full history cannot be replayed from nothing on a schema.sql database today: three
  older revisions reference ``discovered_hypotheses``, which schema.sql never creates).
* ``market_briefings`` originally used lazy runtime DDL; explicit preparation now owns it
  in ``intelligence/runtime_schema.py``. Ordinary publication persistence must stay DDL-free.

Both are therefore declared in ``schema.sql`` with the same DDL as their existing creators.
This guard reads the files only; it needs no database.
"""

from __future__ import annotations

import ast
import re

import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GOD_VIEW = ROOT / "migrations" / "versions" / "god_view_market_tables_20260918.py"
CAPITAL_FLOW = ROOT / "migrations" / "versions" / "f1a2b3c4d5e6_capital_flow_tables.py"
SCHEMA = ROOT / "schema.sql"
BRIEFING_MODULE = ROOT / "ollama" / "market_briefing.py"
BRIEFING_PREPARATION = ROOT / "intelligence" / "runtime_schema.py"

VIEW_DEPENDENCIES = ("market_briefings", "insider_trades")


def _creates(sql_text: str, table: str) -> bool:
    return re.search(
        rf"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?{table}\b", sql_text, re.IGNORECASE
    ) is not None


def _columns(sql_text: str, table: str) -> list[str]:
    """Column names of the CREATE TABLE for ``table`` in ``sql_text`` (first definition wins)."""
    m = re.search(
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{table}\s*\((.*?)\);",
        sql_text,
        re.IGNORECASE | re.DOTALL,
    )
    assert m, f"no CREATE TABLE {table} found"
    cols = []
    for line in m.group(1).splitlines():
        line = line.strip().rstrip(",")
        if not line or line.upper().startswith(("PRIMARY KEY", "UNIQUE", "CONSTRAINT", "FOREIGN KEY")):
            continue
        cols.append(line.split()[0].lower())
    return cols


def test_god_view_revision_reads_the_two_external_tables():
    text = GOD_VIEW.read_text(encoding="utf-8")
    for table in VIEW_DEPENDENCIES:
        assert re.search(rf"\b{table}\b", text), f"{table} no longer referenced; update this guard"
        assert not _creates(text, table), f"{table} is created by the revision itself now; update this guard"


def test_insider_trades_is_created_by_an_earlier_revision():
    assert _creates(CAPITAL_FLOW.read_text(encoding="utf-8"), "insider_trades")


def test_insider_trades_is_declared_in_schema_sql_with_the_revision_ddl():
    schema = SCHEMA.read_text(encoding="utf-8")
    assert _creates(schema, "insider_trades")
    assert "IF NOT EXISTS insider_trades" in schema
    assert _columns(schema, "insider_trades") == _columns(
        CAPITAL_FLOW.read_text(encoding="utf-8"), "insider_trades"
    )
    for col in ("trade_date", "trade_type"):  # the columns the god-view matview reads
        assert col in _columns(schema, "insider_trades")


def test_market_briefings_is_declared_in_schema_sql():
    schema = SCHEMA.read_text(encoding="utf-8")
    assert _creates(schema, "market_briefings"), (
        "schema.sql must declare market_briefings: the god-view materialized view depends on it "
        "and a fresh install runs alembic upgrade head right after schema.sql"
    )
    assert "IF NOT EXISTS market_briefings" in schema, "must stay idempotent (explicit preparation also creates it)"



def _publication_preparation_ddl(source: str) -> str:
    """Read the explicit preparation literal without importing/executing it."""
    tree = ast.parse(source)
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "_PUBLICATION_DDL"
                           for target in node.targets)]
    assert len(assignments) == 1, "explicit publication preparation literal missing or ambiguous"
    statements = ast.literal_eval(assignments[0].value)
    assert isinstance(statements, tuple) and statements and isinstance(statements[0], str)
    ddl = statements[0].replace("{schema}.", "")
    assert "{schema}" not in ddl, "unresolved explicit schema template"
    # Keep the existing column selector valid when several fields share a line;
    # do not split commas inside type/default expressions.
    depth = 0
    normalized = []
    for char in ddl:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        normalized.append(char)
        if char == "," and depth == 1:
            normalized.append("\n")
    return "".join(normalized).rstrip("; \n") + ";"


def test_schema_sql_market_briefings_matches_the_runtime_ddl():
    schema_cols = _columns(SCHEMA.read_text(encoding="utf-8"), "market_briefings")
    runtime_cols = _columns(_publication_preparation_ddl(
        BRIEFING_PREPARATION.read_text(encoding="utf-8")), "market_briefings")
    assert schema_cols == runtime_cols, (schema_cols, runtime_cols)
    # the columns the god-view matview selects must exist
    for col in ("id", "briefing_type", "briefing_date", "snapshot_data"):
        assert col in schema_cols


def test_explicit_publication_selector_refuses_missing_table_definition():
    source = "_PUBLICATION_DDL = ('CREATE TABLE IF NOT EXISTS {schema}.other_table (id SERIAL)',)"
    with pytest.raises(AssertionError, match="no CREATE TABLE market_briefings found"):
        _columns(_publication_preparation_ddl(source), "market_briefings")


def test_ordinary_publication_writer_does_not_reintroduce_schema_ddl():
    tree = ast.parse(BRIEFING_MODULE.read_text(encoding="utf-8"))
    method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                  and node.name == "_persist_to_db")
    for node in ast.walk(method):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert not re.search(r"\b(?:CREATE|ALTER|DROP)\s+(?:TABLE|INDEX)\b", node.value, re.I)
