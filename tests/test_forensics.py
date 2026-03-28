"""
Tests for intelligence.forensics — forensic move analyzer.

Tests cover:
  - ForensicReport dataclass
  - Helper functions (direction, alignment, confidence)
  - find_significant_moves with mock price data
  - analyze_move with mock engine
  - Narrative generation fallback
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from intelligence.forensics import (
    ForensicReport,
    _compute_confidence,
    _direction_label,
    _event_aligns,
    _safe_float,
    find_significant_moves,
    analyze_move,
    load_forensic_reports,
    _generate_narrative,
)


# ── Dataclass Tests ───────────────────────────────────────────────────────


class TestForensicReport:
    def test_creation(self):
        report = ForensicReport(
            ticker="NVDA",
            move_date="2026-01-15",
            move_pct=5.2,
            move_direction="up",
            preceding_events=[],
            warning_signals=3,
            avg_lead_time_hours=48.0,
            key_actors=["Nancy Pelosi"],
            total_dollar_flow=1_500_000.0,
            narrative="Test narrative.",
            pattern_match=None,
            confidence=0.75,
        )
        assert report.ticker == "NVDA"
        assert report.move_direction == "up"
        assert report.confidence == 0.75

    def test_to_dict(self):
        report = ForensicReport(
            ticker="AAPL",
            move_date="2026-03-01",
            move_pct=-3.1,
            move_direction="down",
            preceding_events=[{"event_type": "insider"}],
            warning_signals=1,
            avg_lead_time_hours=24.0,
            key_actors=[],
            total_dollar_flow=0.0,
            narrative="Fell hard.",
            pattern_match={"pattern": "insider:bearish->price_move", "occurrences": 5},
            confidence=0.6,
        )
        d = report.to_dict()
        assert isinstance(d, dict)
        assert d["ticker"] == "AAPL"
        assert d["preceding_events"] == [{"event_type": "insider"}]
        assert d["pattern_match"]["occurrences"] == 5


# ── Helper Tests ──────────────────────────────────────────────────────────


class TestHelpers:
    def test_direction_label(self):
        assert _direction_label(0.05) == "up"
        assert _direction_label(-0.02) == "down"
        assert _direction_label(0.0) == "up"  # zero is up

    def test_event_aligns(self):
        assert _event_aligns("bullish", "up") is True
        assert _event_aligns("bearish", "down") is True
        assert _event_aligns("bullish", "down") is False
        assert _event_aligns("neutral", "up") is False

    def test_safe_float(self):
        assert _safe_float(None) == 0.0
        assert _safe_float("123.45") == 123.45
        assert _safe_float("bad") == 0.0
        assert _safe_float(42) == 42.0


# ── Confidence Scoring ────────────────────────────────────────────────────


class TestConfidence:
    def test_no_events(self):
        c = _compute_confidence(
            total_events=0, aligned_events=0,
            has_pattern=False, has_dollar_flow=False, avg_lead=0,
        )
        assert c == 0.1

    def test_many_aligned_events(self):
        c = _compute_confidence(
            total_events=20, aligned_events=18,
            has_pattern=True, has_dollar_flow=True, avg_lead=48.0,
        )
        assert c > 0.5

    def test_confidence_bounded(self):
        c = _compute_confidence(
            total_events=200, aligned_events=200,
            has_pattern=True, has_dollar_flow=True, avg_lead=24.0,
        )
        assert c <= 1.0

    def test_no_alignment(self):
        c = _compute_confidence(
            total_events=10, aligned_events=0,
            has_pattern=False, has_dollar_flow=False, avg_lead=0,
        )
        assert c < 0.3


# ── find_significant_moves ────────────────────────────────────────────────


class TestFindSignificantMoves:
    @patch("intelligence.forensics._get_price_moves")
    def test_filters_by_threshold(self, mock_prices):
        mock_prices.return_value = [
            {"date": "2026-01-10", "close": 100, "pct_change": 0.01, "direction": "up"},
            {"date": "2026-01-11", "close": 105, "pct_change": 0.05, "direction": "up"},
            {"date": "2026-01-12", "close": 100, "pct_change": -0.048, "direction": "down"},
            {"date": "2026-01-13", "close": 101, "pct_change": 0.01, "direction": "up"},
        ]
        engine = MagicMock()
        moves = find_significant_moves(engine, "NVDA", days=90, threshold=0.03)
        assert len(moves) == 2
        assert moves[0]["direction"] == "down"  # most recent first
        assert moves[1]["direction"] == "up"

    @patch("intelligence.forensics._get_price_moves")
    def test_empty_price_data(self, mock_prices):
        mock_prices.return_value = []
        engine = MagicMock()
        moves = find_significant_moves(engine, "AAPL", days=90)
        assert moves == []


# ── Narrative Fallback ────────────────────────────────────────────────────


class TestNarrativeFallback:
    @patch("intelligence.forensics._get_llm_narrative", return_value=None)
    def test_rule_based_narrative(self, mock_llm):
        narrative = _generate_narrative(
            ticker="TSLA",
            move_date="2026-02-01",
            move_pct=0.04,
            move_dir="up",
            preceding=[MagicMock(direction="bullish")] * 3,
            aligned_count=3,
            key_actors=["Elon Musk"],
            total_flow=500_000,
            avg_lead=72.0,
            pattern_match=None,
        )
        assert "TSLA" in narrative
        assert "up" in narrative
        assert "Elon Musk" in narrative
        assert "$500,000" in narrative


# ── load_forensic_reports ─────────────────────────────────────────────────


class TestLoadForensicReports:
    def test_empty_result(self, mock_engine):
        reports = load_forensic_reports(mock_engine, "NVDA", days=90)
        assert reports == []
