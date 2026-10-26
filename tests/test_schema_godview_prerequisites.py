"""The god-view revision is not self-contained: its materialized view reads two tables that the
revision itself never creates, so ``alembic upgrade`` through it fails with
``relation ... does not exist`` unless both already exist (observed on isolated databases,
2026-09-18).

* ``insider_trades`` is created by the earlier revision ``f1a2b3c4d5e6_capital_flow_tables`` —
  but a database built from ``schema.sql`` and *stamped* at a later revision skips that revision
  (and the full history cannot be replayed from nothing on a schema.sql database today: three
  older revisions reference ``discovered_hypotheses``, which schema.sql never creates).
* ``market_briefings`` was only ever created lazily at runtime by ``ollama/market_briefing.py``.

Both are therefore declared in ``schema.sql`` with the same DDL as their existing creators.
This guard reads the files only; it needs no database.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GOD_VIEW = ROOT / "migrations" / "versions" / "god_view_market_tables_20260918.py"
CAPITAL_FLOW = ROOT / "migrations" / "versions" / "f1a2b3c4d5e6_capital_flow_tables.py"
SCHEMA = ROOT / "schema.sql"
BRIEFING_MODULE = ROOT / "ollama" / "market_briefing.py"

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
    assert "IF NOT EXISTS market_briefings" in schema, "must stay idempotent (runtime module also creates it)"


def test_schema_sql_market_briefings_matches_the_runtime_ddl():
    schema_cols = _columns(SCHEMA.read_text(encoding="utf-8"), "market_briefings")
    runtime_cols = _columns(BRIEFING_MODULE.read_text(encoding="utf-8"), "market_briefings")
    assert schema_cols == runtime_cols, (schema_cols, runtime_cols)
    # the columns the god-view matview selects must exist
    for col in ("id", "briefing_type", "briefing_date", "snapshot_data"):
        assert col in schema_cols
