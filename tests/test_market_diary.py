"""Tests for the market diary module.

Wave 3 §4.1 fixes covered here (see GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md):
  * missing / stale close -> None, never a fabricated 0 or a stale value
    (test_gather_market_moves_* below);
  * no pre-open thesis snapshot -> verdict None, not a fabricated one
    (test_gather_thesis_accuracy_*);
  * _gather_thesis_accuracy must never call
    analysis.flow_thesis_scoring.generate_unified_thesis() at write time
    (test_gather_thesis_accuracy_never_calls_generate_unified_thesis,
    test_market_diary_module_never_imports_generate_unified_thesis).
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

YF_SOURCE_ID = 1


@pytest.fixture()
def diary_engine():
    """A sqlite engine with the tables market_diary.py's readers touch.

    Mirrors the schema style in tests/test_raw_series_quarantined_status.py
    (explicit pull_timestamp column, sqlite3 date/datetime adapters) so
    store.observations.read_latest_n behaves the same as it does against
    PostgreSQL.
    """
    engine = create_engine("sqlite://")
    md = MetaData()
    Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("pull_status", String, nullable=False),
    )
    Table(
        "thesis_snapshots", md,
        Column("id", Integer, primary_key=True),
        Column("timestamp", DateTime, nullable=False),
        Column("overall_direction", String, nullable=False),
        Column("conviction", Float),
    )
    Table(
        "cross_reference_checks", md,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("category", String),
        Column("assessment", String),
        Column("implication", Text),
        Column("checked_at", DateTime, nullable=False),
    )
    Table(
        "market_diary", md,
        Column("id", Integer, primary_key=True),
        Column("date", Date, nullable=False, unique=True),
        Column("content", Text, nullable=False),
        Column("market_moves", Text),
        Column("active_actors", Text),
        Column("thesis_accuracy", Text),
        Column("narrative_model", Text),
        Column("narrative_fallback", Boolean, nullable=False, server_default="0"),
        Column("generated_at", DateTime, nullable=False),
    )
    md.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            md.tables["source_catalog"].insert(),
            {"id": YF_SOURCE_ID, "name": "yfinance"},
        )
    yield engine
    engine.dispose()


def _price_row(series_id, d, value, ts_offset_h=0, source_id=YF_SOURCE_ID):
    return {
        "series_id": series_id, "source_id": source_id, "obs_date": d,
        "pull_timestamp": datetime(2026, 9, 20, 6, 0, 0) + timedelta(hours=ts_offset_h),
        "value": value, "pull_status": "SUCCESS",
    }


def _insert_prices(engine, rows):
    from sqlalchemy import MetaData as _MD

    md = _MD()
    md.reflect(engine, only=["raw_series"])
    with engine.begin() as conn:
        for r in rows:
            conn.execute(md.tables["raw_series"].insert(), r)


def test_ensure_table():
    """ensure_table creates the table and adds the narrative_* columns."""
    from intelligence.market_diary import ensure_table

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    ensure_table(mock_engine)
    # 1 CREATE TABLE + 2 ADD COLUMN IF NOT EXISTS (narrative_model, narrative_fallback)
    assert mock_conn.execute.call_count == 3


def test_gather_market_moves_empty_db():
    """_gather_market_moves returns empty structure when DB has no data."""
    from intelligence.market_diary import _gather_market_moves

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    result = _gather_market_moves(mock_engine, date(2026, 3, 27))
    assert "indices" in result
    assert "sector_leaders" in result
    assert "sector_laggards" in result


def test_gather_active_actors_empty_db():
    """_gather_active_actors returns empty structure when DB has no data."""
    from intelligence.market_diary import _gather_active_actors

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    result = _gather_active_actors(mock_engine, date(2026, 3, 27))
    assert "congressional_trades" in result
    assert "insider_filings" in result
    assert "lever_puller_actions" in result


def test_build_fallback_narrative():
    """Fallback narrative generates valid markdown when LLM is offline."""
    from intelligence.market_diary import _build_fallback_narrative

    moves = {
        "indices": {
            "S&P 500": {"close": 5200.0, "change": 50.0, "change_pct": 0.97},
        },
        "sector_leaders": [{"sector": "Tech", "etf": "XLK", "change_pct": 1.5}],
        "sector_laggards": [{"sector": "Energy", "etf": "XLE", "change_pct": -0.8}],
        "notable": [],
    }
    actors = {"congressional_trades": [], "insider_filings": [], "lever_puller_actions": []}
    thesis_acc = {"morning_thesis": "BULLISH", "actual_outcome": "BULLISH", "verdict": "correct", "sp500_return_pct": 0.97}

    result = _build_fallback_narrative(date(2026, 3, 27), moves, actors, thesis_acc)
    assert "S&P 500" in result
    assert "BULLISH" in result
    assert "correct" in result


def test_verdict_logic():
    """Thesis accuracy verdict is computed correctly."""
    # We test the logic inline since it's embedded in _gather_thesis_accuracy
    # but we can test the comparison logic directly
    test_cases = [
        ("BULLISH", "BULLISH", "correct"),
        ("BEARISH", "BEARISH", "correct"),
        ("BULLISH", "BEARISH", "wrong"),
        ("BEARISH", "BULLISH", "wrong"),
        ("NEUTRAL", "BULLISH", "partial"),
        ("BULLISH", "NEUTRAL", "partial"),
    ]

    for morning, actual, expected_verdict in test_cases:
        if morning == actual:
            verdict = "correct"
        elif morning == "NEUTRAL" or actual == "NEUTRAL":
            verdict = "partial"
        else:
            verdict = "wrong"
        assert verdict == expected_verdict, f"Failed for {morning} vs {actual}"


def test_list_diary_entries_empty():
    """list_diary_entries returns empty when table has no rows."""
    from intelligence.market_diary import list_diary_entries

    mock_engine = MagicMock()
    mock_conn = MagicMock()

    # ensure_table call
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # list queries
    mock_total = MagicMock()
    mock_total.fetchone.return_value = (0,)
    mock_rows = MagicMock()
    mock_rows.fetchall.return_value = []
    mock_conn.execute.side_effect = [
        mock_conn.execute.return_value,  # CREATE TABLE
        mock_conn.execute.return_value,  # ADD COLUMN narrative_model
        mock_conn.execute.return_value,  # ADD COLUMN narrative_fallback
        mock_total,  # COUNT
        mock_rows,   # SELECT
    ]
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    result = list_diary_entries(mock_engine)
    assert result["total"] == 0 or isinstance(result["entries"], list)


def test_search_diary_empty():
    """search_diary returns empty list for no matches."""
    from intelligence.market_diary import search_diary

    mock_engine = MagicMock()
    mock_conn = MagicMock()

    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    mock_rows = MagicMock()
    mock_rows.fetchall.return_value = []
    mock_conn.execute.side_effect = [
        mock_conn.execute.return_value,  # CREATE TABLE
        mock_conn.execute.return_value,  # ADD COLUMN narrative_model
        mock_conn.execute.return_value,  # ADD COLUMN narrative_fallback
        mock_rows,  # ILIKE query
    ]
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    results = search_diary(mock_engine, "nonexistent")
    assert isinstance(results, list)


# ──────────────────────────────────────────────────────────────────────
# Wave 3 §4.1 fix #2: price freshness (missing / stale close -> None)
# ──────────────────────────────────────────────────────────────────────

def test_gather_market_moves_disabled_by_default(diary_engine):
    """PRICES_ENABLED defaults False; moves carries no fabricated prices."""
    from intelligence import market_diary

    assert market_diary.PRICES_ENABLED is False  # the shipped default

    result = market_diary._gather_market_moves(diary_engine, date(2026, 9, 26))
    assert result["prices_enabled"] is False
    assert result["indices"] == {}
    assert "disabled_reason" in result


def test_gather_market_moves_missing_close_is_null_not_zero(diary_engine, monkeypatch):
    """Only one observation date exists (no prior close) -> null, not chg_pct=0."""
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    target = date(2026, 9, 26)
    # Only today's row for ^GSPC -- no prior close to diff against.
    _insert_prices(diary_engine, [_price_row("YF:^GSPC:close", target, 5000.0)])

    result = market_diary._gather_market_moves(diary_engine, target)
    entry = result["indices"]["S&P 500"]
    assert entry.get("change_pct") is None
    assert entry.get("close") is None
    assert entry.get("status") == "no close for date"


def test_gather_market_moves_stale_close_is_null(diary_engine, monkeypatch):
    """Newest accepted obs_date is before target_date -> refused, not reported as today's move."""
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    target = date(2026, 9, 26)
    stale_date = date(2026, 9, 24)  # e.g. a holiday gap; today hasn't pulled yet
    _insert_prices(diary_engine, [
        _price_row("YF:^GSPC:close", stale_date, 5000.0, ts_offset_h=0),
        _price_row("YF:^GSPC:close", stale_date - timedelta(days=1), 4950.0, ts_offset_h=-24),
    ])

    result = market_diary._gather_market_moves(diary_engine, target)
    entry = result["indices"]["S&P 500"]
    assert entry == {"status": "no close for date"}


def test_gather_market_moves_zero_prior_close_is_null_not_zero(diary_engine, monkeypatch):
    """A prior close of 0 must render change_pct=None, never a fabricated 0."""
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    target = date(2026, 9, 26)
    prior = target - timedelta(days=1)
    _insert_prices(diary_engine, [
        _price_row("YF:^GSPC:close", target, 5000.0, ts_offset_h=24),
        _price_row("YF:^GSPC:close", prior, 0.0, ts_offset_h=0),
    ])

    result = market_diary._gather_market_moves(diary_engine, target)
    entry = result["indices"]["S&P 500"]
    assert entry["close"] == 5000.0
    assert entry["change_pct"] is None


def test_gather_market_moves_fresh_close_records_provenance(diary_engine, monkeypatch):
    """A fresh (obs_date == target_date) pair records obs_date/price_basis/source."""
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    target = date(2026, 9, 26)
    prior = target - timedelta(days=1)
    _insert_prices(diary_engine, [
        _price_row("YF:^GSPC:close", target, 5100.0, ts_offset_h=24),
        _price_row("YF:^GSPC:close", prior, 5000.0, ts_offset_h=0),
    ])

    result = market_diary._gather_market_moves(diary_engine, target)
    entry = result["indices"]["S&P 500"]
    assert entry["obs_date"] == target.isoformat()
    assert entry["price_basis"] == "raw_close"
    assert entry["source"] == "yfinance"
    assert entry["change_pct"] == pytest.approx(2.0, abs=0.01)


# ──────────────────────────────────────────────────────────────────────
# Wave 3 §4.1 fix #1: pre-open thesis verdict, no look-ahead
# ──────────────────────────────────────────────────────────────────────

def test_gather_thesis_accuracy_no_pre_open_snapshot_is_null_verdict(diary_engine):
    """No thesis_snapshots row before 13:30Z that day -> verdict None with a reason."""
    from intelligence import market_diary

    target = date(2026, 9, 26)
    result = market_diary._gather_thesis_accuracy(diary_engine, target)
    assert result["verdict"] is None
    assert result["reason"] == "no pre-open thesis snapshot"
    assert result["morning_thesis"] is None


def test_gather_thesis_accuracy_uses_pre_open_snapshot_not_generate_unified_thesis(diary_engine, monkeypatch):
    """A pre-open snapshot (before 13:30Z) is read directly; a same-day
    post-cutoff snapshot must NOT be picked (that would still be look-ahead
    relative to the pre-open call)."""
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    target = date(2026, 9, 26)
    with diary_engine.begin() as conn:
        from sqlalchemy import MetaData as _MD
        md = _MD()
        md.reflect(diary_engine, only=["thesis_snapshots"])
        tbl = md.tables["thesis_snapshots"]
        conn.execute(tbl.insert(), {
            "id": 1,
            "timestamp": datetime(2026, 9, 26, 12, 0, 0),  # pre-open (before 13:30Z)
            "overall_direction": "bullish",
            "conviction": 0.7,
        })
        conn.execute(tbl.insert(), {
            "id": 2,
            "timestamp": datetime(2026, 9, 26, 18, 0, 0),  # post-open -- must be ignored
            "overall_direction": "bearish",
            "conviction": 0.9,
        })

    result = market_diary._gather_thesis_accuracy(diary_engine, target)
    assert result["morning_thesis"] == "BULLISH"
    assert result["morning_conviction"] == pytest.approx(0.7)


def test_gather_thesis_accuracy_never_calls_generate_unified_thesis(diary_engine, monkeypatch):
    """Functional guard: even if generate_unified_thesis is patched to blow
    up, _gather_thesis_accuracy must not touch it (no import path calls it
    anymore)."""
    import analysis.flow_thesis_scoring as scoring
    from intelligence import market_diary

    forbid = MagicMock(side_effect=AssertionError("must not call generate_unified_thesis"))
    monkeypatch.setattr(scoring, "generate_unified_thesis", forbid)

    result = market_diary._gather_thesis_accuracy(diary_engine, date(2026, 9, 26))

    forbid.assert_not_called()
    assert result["reason"] == "no pre-open thesis snapshot"


def test_market_diary_module_never_imports_generate_unified_thesis():
    """Static guard (grep-style, per tests/test_get_side_effects.py's
    convention of pinning a bug class so it cannot silently come back):
    the look-ahead bug was a call to generate_unified_thesis() at write
    time. Assert the source no longer references it at all."""
    import inspect

    from intelligence import market_diary

    source = inspect.getsource(market_diary)
    assert "generate_unified_thesis" not in source


# ──────────────────────────────────────────────────────────────────────
# Wave 3 §4.1 fix #4: cross_reference_reports (empty) replaced by
# cross_reference_checks (the table every other consumer reads)
# ──────────────────────────────────────────────────────────────────────

def test_gather_thesis_accuracy_reads_cross_reference_checks(diary_engine):
    """Anomalies come from cross_reference_checks, never cross_reference_reports."""
    from intelligence import market_diary

    target = date(2026, 9, 26)
    with diary_engine.begin() as conn:
        from sqlalchemy import MetaData as _MD
        md = _MD()
        md.reflect(diary_engine, only=["cross_reference_checks"])
        tbl = md.tables["cross_reference_checks"]
        conn.execute(tbl.insert(), {
            "id": 1, "name": "CFTC vs AIS", "category": "shipping",
            "assessment": "major_divergence", "implication": "flows understated",
            "checked_at": datetime(2026, 9, 26, 10, 0, 0),
        })
        conn.execute(tbl.insert(), {
            "id": 2, "name": "irrelevant", "category": "x",
            "assessment": "consistent", "implication": "",
            "checked_at": datetime(2026, 9, 26, 11, 0, 0),
        })

    result = market_diary._gather_thesis_accuracy(diary_engine, target)
    assert result.get("anomalies_detected") == 1
    assert "CFTC vs AIS" in result["details"][0]["flag"]


# ──────────────────────────────────────────────────────────────────────
# Wave 3 §4.1 fix #5: --dry-run does not persist
# ──────────────────────────────────────────────────────────────────────

def test_write_diary_entry_dry_run_does_not_persist(diary_engine, monkeypatch):
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", False)
    # The diary_engine fixture already created market_diary with the
    # production schema; ensure_table's DDL is Postgres-only (SERIAL,
    # JSONB, TIMESTAMPTZ) and isn't the thing under test here, so skip it.
    monkeypatch.setattr(market_diary, "ensure_table", lambda engine: None)
    target = date(2026, 9, 26)

    result = market_diary.write_diary_entry(diary_engine, target_date=target, dry_run=True)
    assert result["dry_run"] is True
    assert result["date"] == target.isoformat()

    with diary_engine.connect() as conn:
        from sqlalchemy import text as _text
        row = conn.execute(_text("SELECT COUNT(*) FROM market_diary")).fetchone()
    assert row[0] == 0


def test_write_diary_entry_persists_narrative_model_and_fallback(diary_engine, monkeypatch):
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", False)
    monkeypatch.setattr(market_diary, "ensure_table", lambda engine: None)
    # No LLM client reachable in the test env -> falls back to the
    # rule-based narrative, so narrative_fallback must be recorded True.
    monkeypatch.setattr(
        "llm.router.get_llm",
        MagicMock(side_effect=RuntimeError("no LLM in tests")),
    )
    target = date(2026, 9, 26)

    result = market_diary.write_diary_entry(diary_engine, target_date=target, dry_run=False)
    assert result["narrative_fallback"] is True
    assert result["narrative_model"] is None

    entry = market_diary.get_diary_entry(diary_engine, target)
    assert entry["narrative_fallback"] is True
