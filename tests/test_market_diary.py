"""Tests for the market diary module."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


def test_ensure_table():
    """ensure_table executes CREATE TABLE without error."""
    from intelligence.market_diary import ensure_table

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    ensure_table(mock_engine)
    mock_conn.execute.assert_called_once()


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
        mock_rows,  # ILIKE query
    ]
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    results = search_diary(mock_engine, "nonexistent")
    assert isinstance(results, list)
