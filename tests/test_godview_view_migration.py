"""Static checks on the G6 view migration (no database)."""

from __future__ import annotations

import importlib
import pathlib
import re

MIGRATION = "migrations.versions.godview_view_v2_20260927"
SOURCE = pathlib.Path(__file__).resolve().parent.parent / "migrations" / "versions" / "godview_view_v2_20260927.py"


def test_revision_ids():
    m = importlib.import_module(MIGRATION)
    assert m.revision == "godview_view_v2_20260927"
    assert len(m.revision) <= 32
    # Stacked after #683's resolved_retractions_20260927 (see the PR body for
    # the merge order that keeps a single head).
    assert m.down_revision == "resolved_retractions_20260927"


def test_timeouts_are_literals_matching_the_constants():
    text = SOURCE.read_text(encoding="utf-8")
    m = importlib.import_module(MIGRATION)
    assert f"SET LOCAL lock_timeout = '{m._LOCK_TIMEOUT}'" in text
    assert f"SET LOCAL statement_timeout = '{m._STATEMENT_TIMEOUT}'" in text


def test_no_formatted_sql_and_no_cascade():
    text = SOURCE.read_text(encoding="utf-8")
    assert not re.search(r"\bf\"\"\"|\bf'''|\bf\"|\bf'", text), "f-string in a migration"
    assert ".format(" not in text
    assert "CASCADE" not in text.upper().replace("NOT CASCADE", "")


def test_view_reads_only_provenance_rows_with_a_known_at_bound():
    m = importlib.import_module(MIGRATION)
    sql = m._CREATE_VIEW_SQL
    assert sql.count("provenance IS NOT NULL") == 6  # fed + ES/ZN/GC/CL + GEX
    assert sql.count("release_at < s.known_before AND") == 6
    assert sql.count("available_at < s.known_before") == 6
    # Nothing is joined on the observation date alone, and the fabricated /
    # unsourced legacy pillars are gone.
    for gone in ("market_briefings", "commodity_warehouse_inventories", "corporate_buyback_blackouts",
                 "insider_trades", "obs_date <= ", "report_date <= "):
        assert gone not in sql, gone
    for root in m.CFTC_VIEW_ROOTS:
        assert f"c.contract_code = '{root}'" in sql


def test_staleness_thresholds_are_the_plan_cadences():
    # Plan section 5: fed > 9 days, CFTC > 10 days after release (the G8 API
    # uses the same numbers in godview/read_model.py).
    m = importlib.import_module(MIGRATION)
    assert (m.FED_STALE_AFTER_DAYS, m.CFTC_STALE_AFTER_DAYS) == (9, 10)
    assert f"fed.obs_date) > {m.FED_STALE_AFTER_DAYS} END" in m._CREATE_VIEW_SQL
    assert m._CREATE_VIEW_SQL.count(f"> interval '{m.CFTC_STALE_AFTER_DAYS} days'") == 4


def test_downgrade_restores_the_legacy_definition_verbatim():
    m = importlib.import_module(MIGRATION)
    base = (SOURCE.parent / "god_view_market_tables_20260918.py").read_text(encoding="utf-8")

    def norm(s: str) -> str:
        return " ".join(s.split())

    start = base.index("CREATE MATERIALIZED VIEW IF NOT EXISTS market_god_view_daily")
    end = base.index("(as_of_date DESC);", start) + len("(as_of_date DESC);")
    assert norm(base[start:end]) == norm(m._LEGACY_MATVIEW_SQL)
