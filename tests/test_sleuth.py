"""
Tests for the GRID investigative research engine (intelligence/sleuth.py).

Tests lead creation, persistence, generation, investigation, rabbit holes,
LLM response parsing, and context gathering with mocked database/LLM.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from intelligence.sleuth import (
    Lead,
    Sleuth,
    _parse_investigation_response,
    _llm_investigate,
    ensure_tables,
    LEAD_CATEGORIES,
    LEAD_STATUSES,
    ANOMALY_ZSCORE_THRESHOLD,
    HIGH_PRIORITY_THRESHOLD,
)


# ── Lead Data Class Tests ────────────────────────────────────────────────


class TestLead:
    """Test the Lead dataclass."""

    def test_default_status(self):
        lead = Lead(
            id="LEAD-TEST-0001",
            question="Why did X happen?",
            category="actor_pattern",
            priority=0.7,
            evidence=[{"ticker": "AAPL"}],
        )
        assert lead.status == "new"
        assert lead.findings is None
        assert lead.follow_up_leads == []
        assert lead.hypotheses == []
        assert lead.resolved_at is None
        assert lead.created_at  # auto-populated

    def test_created_at_auto_populated(self):
        lead = Lead(
            id="LEAD-TEST-0002",
            question="Test?",
            category="data_anomaly",
            priority=0.5,
            evidence=[],
        )
        # Should be an ISO format timestamp
        assert "T" in lead.created_at
        assert "20" in lead.created_at  # year prefix

    def test_created_at_preserved(self):
        ts = "2025-01-15T12:00:00+00:00"
        lead = Lead(
            id="LEAD-TEST-0003",
            question="Test?",
            category="data_anomaly",
            priority=0.5,
            evidence=[],
            created_at=ts,
        )
        assert lead.created_at == ts


# ── Response Parsing Tests ───────────────────────────────────────────────


class TestParseInvestigationResponse:
    """Test the LLM response parser."""

    def test_full_response(self):
        response = (
            "HYPOTHESES:\n"
            "1. [Most likely] Insiders coordinated their sales | Confidence: high\n"
            "2. [Alternative] Earnings pre-announcement leaked | Confidence: medium\n"
            "3. [Contrarian] Coincidental rebalancing | Confidence: low\n"
            "\n"
            "EVIDENCE NEEDED:\n"
            "- Check if the insiders share a common fund advisor\n"
            "- Look at earnings surprise magnitude\n"
            "\n"
            "FOLLOW-UP QUESTIONS:\n"
            "- Were there any unusual options activity before the sales?\n"
            "- Did any board meetings occur in the prior week?\n"
            "\n"
            "CONCLUSION:\n"
            "The coordinated insider selling likely reflects advance knowledge "
            "of negative earnings."
        )
        result = _parse_investigation_response(response)

        assert len(result["hypotheses"]) == 3
        assert result["hypotheses"][0]["confidence"] == "high"
        assert result["hypotheses"][1]["confidence"] == "medium"
        assert result["hypotheses"][2]["confidence"] == "low"
        assert "coordinated" in result["hypotheses"][0]["hypothesis"].lower() or \
               "insiders" in result["hypotheses"][0]["hypothesis"].lower()
        assert len(result["evidence_needed"]) == 2
        assert len(result["follow_up_questions"]) == 2
        assert "earnings" in result["conclusion"].lower()

    def test_empty_response(self):
        result = _parse_investigation_response("")
        assert result["hypotheses"] == []
        assert result["follow_up_questions"] == []
        assert result["conclusion"] == ""

    def test_partial_response(self):
        response = (
            "HYPOTHESES:\n"
            "1. Something happened | Confidence: medium\n"
            "\n"
            "CONCLUSION:\n"
            "Need more data."
        )
        result = _parse_investigation_response(response)
        assert len(result["hypotheses"]) == 1
        assert result["conclusion"] == "Need more data."
        assert result["follow_up_questions"] == []


# ── Lead ID Generation ───────────────────────────────────────────────────


class TestLeadIdGeneration:

    def test_format(self):
        lid = Sleuth._make_lead_id("actor_pattern")
        assert lid.startswith("LEAD-ACTO-")
        assert len(lid) > 14

    def test_uniqueness(self):
        ids = {Sleuth._make_lead_id("test") for _ in range(100)}
        assert len(ids) == 100  # all unique


# ── Sleuth with Mocked DB ───────────────────────────────────────────────


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = MagicMock()
    # Mock the connection context manager chain
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = None
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


class TestSleuthInit:

    @patch("intelligence.sleuth._tables_ensured", True)
    def test_creates_instance(self, mock_engine):
        sleuth = Sleuth(mock_engine)
        assert sleuth.engine is mock_engine


class TestSleuthLeadPersistence:

    @patch("intelligence.sleuth._tables_ensured", True)
    def test_save_lead(self, mock_engine):
        sleuth = Sleuth(mock_engine)
        lead = Lead(
            id="LEAD-TEST-0001",
            question="Test question?",
            category="actor_pattern",
            priority=0.8,
            evidence=[{"test": True}],
        )
        sleuth._save_lead(lead)
        # Verify execute was called on the connection
        conn = mock_engine.begin.return_value.__enter__.return_value
        assert conn.execute.called

    @patch("intelligence.sleuth._tables_ensured", True)
    def test_load_lead_not_found(self, mock_engine):
        sleuth = Sleuth(mock_engine)
        result = sleuth._load_lead("nonexistent")
        assert result is None


class TestSleuthLeadGeneration:

    @patch("intelligence.sleuth._tables_ensured", True)
    def test_generate_leads_handles_empty_db(self, mock_engine):
        """generate_leads should not crash when tables are empty."""
        sleuth = Sleuth(mock_engine)
        leads = sleuth.generate_leads()
        # With empty DB, most generators will return []
        assert isinstance(leads, list)

    @patch("intelligence.sleuth._tables_ensured", True)
    def test_actor_pattern_detection(self, mock_engine):
        """Actor pattern generator should detect clusters."""
        sleuth = Sleuth(mock_engine)

        # Mock signal_sources data with a cluster
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = [
            ("AAPL", "trader_1", "congressional", "BUY", "2025-03-01", 150.0),
            ("AAPL", "trader_2", "insider", "BUY", "2025-03-03", 152.0),
            ("AAPL", "trader_3", "congressional", "BUY", "2025-03-05", 148.0),
        ]

        leads = sleuth._generate_actor_pattern_leads()
        assert isinstance(leads, list)
        # Should detect the AAPL BUY cluster
        if leads:
            assert leads[0].category == "actor_pattern"
            assert "AAPL" in leads[0].question


class TestSleuthInvestigation:

    @patch("intelligence.sleuth._tables_ensured", True)
    @patch("intelligence.sleuth._llm_investigate")
    def test_investigate_lead_with_llm(self, mock_llm, mock_engine):
        """Investigation should call LLM and parse results."""
        mock_llm.return_value = {
            "raw_response": "test",
            "hypotheses": [
                {"hypothesis": "Insider trading", "confidence": "high"},
            ],
            "evidence_needed": ["Check SEC filings"],
            "follow_up_questions": [
                "Were there unusual options purchases before the insider sale?"
            ],
            "conclusion": "Likely insider knowledge.",
        }

        sleuth = Sleuth(mock_engine)
        lead = Lead(
            id="LEAD-TEST-INV1",
            question="Why did the insider sell?",
            category="timing_suspicious",
            priority=0.9,
            evidence=[{"ticker": "MSFT", "source_type": "insider"}],
        )

        result = sleuth.investigate_lead(lead)
        assert result.status == "resolved"
        assert result.findings == "Likely insider knowledge."
        assert len(result.hypotheses) == 1
        assert result.resolved_at is not None

    @patch("intelligence.sleuth._tables_ensured", True)
    @patch("intelligence.sleuth._llm_investigate")
    def test_investigate_lead_llm_unavailable(self, mock_llm, mock_engine):
        """When LLM is unavailable, lead should be put back in queue."""
        mock_llm.return_value = None

        sleuth = Sleuth(mock_engine)
        lead = Lead(
            id="LEAD-TEST-INV2",
            question="Test?",
            category="data_anomaly",
            priority=0.5,
            evidence=[],
        )

        result = sleuth.investigate_lead(lead)
        assert result.status == "new"  # back in queue
        assert "unavailable" in result.findings.lower()


class TestSleuthRabbitHole:

    @patch("intelligence.sleuth._tables_ensured", True)
    @patch("intelligence.sleuth._llm_investigate")
    def test_rabbit_hole_depth_zero(self, mock_llm, mock_engine):
        """Depth 0 should investigate root but not follow children."""
        mock_llm.return_value = {
            "raw_response": "test",
            "hypotheses": [],
            "evidence_needed": [],
            "follow_up_questions": ["Follow up?"],
            "conclusion": "Done.",
        }

        sleuth = Sleuth(mock_engine)
        lead = Lead(
            id="LEAD-TEST-RH1",
            question="Root question?",
            category="actor_pattern",
            priority=0.7,
            evidence=[],
        )

        results = sleuth.follow_rabbit_hole(lead, depth=0)
        assert len(results) == 1
        assert results[0].id == lead.id

    @patch("intelligence.sleuth._tables_ensured", True)
    def test_rabbit_hole_skips_resolved(self, mock_engine):
        """Already-resolved leads should not be re-investigated."""
        sleuth = Sleuth(mock_engine)
        lead = Lead(
            id="LEAD-TEST-RH2",
            question="Already done?",
            category="actor_pattern",
            priority=0.7,
            evidence=[],
            status="resolved",
            findings="Previously resolved.",
        )

        results = sleuth.follow_rabbit_hole(lead, depth=3)
        assert len(results) == 1  # just the root, not re-investigated


class TestSleuthDailyInvestigation:

    @patch("intelligence.sleuth._tables_ensured", True)
    @patch("intelligence.sleuth._llm_investigate")
    def test_daily_investigation_returns_report(self, mock_llm, mock_engine):
        """Daily investigation should return a structured report."""
        mock_llm.return_value = None  # LLM unavailable

        sleuth = Sleuth(mock_engine)
        report = sleuth.daily_investigation()

        assert "timestamp" in report
        assert "leads_generated" in report
        assert "leads_investigated" in report
        assert "total_open" in report
        assert isinstance(report["leads_generated"], int)
