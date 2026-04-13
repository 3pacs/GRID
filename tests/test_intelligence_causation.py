"""
Tests for the causal-links intelligence endpoint.

Tests the GET /api/v1/intelligence/causal-links API that powers
the Timeline forensic visualization causal arrow overlay.

Uses unittest.mock to avoid real API calls and database writes.
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest


# ── Test fixtures ────────────────────────────────────────────────────────


def _mock_engine():
    """Create a mock SQLAlchemy engine with connection context managers."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


_SENTINEL = object()


def _mock_causal_row(
    id_val=1,
    signal_id=42,
    cause_type="congressional",
    cause_date=_SENTINEL,
    cause_desc="Nancy Pelosi bought AAPL calls",
    ticker="AAPL",
    actor="Nancy Pelosi",
    probability=0.82,
    evidence=_SENTINEL,
    effect_date=_SENTINEL,
    effect_desc="AAPL rallied 3.2% in 2 sessions",
    lead_time=2.0,
):
    """Create a mock database row matching the causal-links query output."""
    if cause_date is _SENTINEL:
        cause_date = date.today() - timedelta(days=5)
    if effect_date is _SENTINEL:
        effect_date = date.today() - timedelta(days=3)
    if evidence is _SENTINEL:
        evidence = {"lead_time_days": 2, "sources": ["House disclosure"]}

    return (
        id_val,         # cl.id
        signal_id,      # cause_signal_id
        cause_type,     # cause_type
        cause_date,     # cause_date
        cause_desc,     # cause_description
        ticker,         # effect_ticker
        actor,          # lever_actor
        probability,    # probability
        evidence,       # evidence
        effect_date,    # effect_date
        effect_desc,    # effect_description
        lead_time,      # lead_time_days
    )


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.new_event_loop().run_until_complete(coro)


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINT TESTS (unit tests via direct function import)
# ══════════════════════════════════════════════════════════════════════════


class TestCausalLinksEndpoint:
    """Tests for the causal-links API endpoint."""

    def test_valid_ticker_returns_links(self):
        """Should return causal links for a valid ticker."""
        engine, conn = _mock_engine()
        rows = [
            _mock_causal_row(id_val=1, ticker="AAPL", cause_type="congressional"),
            _mock_causal_row(id_val=2, ticker="AAPL", cause_type="insider", actor="Tim Cook", probability=0.65),
        ]
        conn.execute.return_value.fetchall.return_value = rows

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="AAPL", days=90, _token="test"))

        assert result["ticker"] == "AAPL"
        assert result["days"] == 90
        assert len(result["links"]) == 2
        assert result["links"][0]["cause_type"] == "congressional"
        assert result["links"][1]["lever_actor"] == "Tim Cook"
        assert "error" not in result

    def test_days_parameter(self):
        """Should pass days parameter correctly to the query."""
        engine, conn = _mock_engine()
        conn.execute.return_value.fetchall.return_value = [
            _mock_causal_row(id_val=1, ticker="MSFT"),
        ]

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="MSFT", days=30, _token="test"))

        assert result["days"] == 30
        assert len(result["links"]) == 1
        assert conn.execute.called

    def test_empty_results(self):
        """Should return empty links list for a ticker with no causal data."""
        engine, conn = _mock_engine()
        conn.execute.return_value.fetchall.return_value = []

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="ZZZZ", days=90, _token="test"))

        assert result["ticker"] == "ZZZZ"
        assert result["links"] == []
        assert "error" not in result

    def test_ticker_uppercased(self):
        """Should uppercase the ticker regardless of input case."""
        engine, conn = _mock_engine()
        conn.execute.return_value.fetchall.return_value = []

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="aapl", days=90, _token="test"))

        assert result["ticker"] == "AAPL"

    def test_db_error_returns_graceful_error(self):
        """Should return an error message if the DB query fails."""
        engine, conn = _mock_engine()
        conn.execute.side_effect = Exception("Connection refused")

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="AAPL", days=90, _token="test"))

        assert result["links"] == []
        assert "error" in result
        assert "Connection refused" in result["error"]

    def test_null_fields_handled(self):
        """Should handle None/null fields gracefully in the response."""
        engine, conn = _mock_engine()
        row = _mock_causal_row(
            signal_id=None,
            actor=None,
            probability=None,
            evidence=None,
            cause_desc=None,
            effect_desc=None,
        )
        conn.execute.return_value.fetchall.return_value = [row]

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="AAPL", days=90, _token="test"))

        link = result["links"][0]
        assert link["cause_signal_id"] is None
        assert link["lever_actor"] == "Unknown"
        assert link["probability"] == 0.5
        assert link["evidence"] == {}
        assert link["cause_description"] == ""
        assert link["effect_description"] == ""

    def test_evidence_json_string_parsed(self):
        """Should parse evidence when it comes as a JSON string."""
        engine, conn = _mock_engine()
        row = _mock_causal_row(
            evidence='{"lead_time_days": 5, "sources": ["SEC"]}'
        )
        conn.execute.return_value.fetchall.return_value = [row]

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="AAPL", days=90, _token="test"))

        link = result["links"][0]
        assert link["evidence"]["lead_time_days"] == 5
        assert "SEC" in link["evidence"]["sources"]

    def test_signal_data_join_fills_descriptions(self):
        """Should use signal_data descriptions when available via LEFT JOIN."""
        engine, conn = _mock_engine()
        row = _mock_causal_row(
            effect_desc="Insider cluster buy detected - 5 directors within 3 days",
        )
        conn.execute.return_value.fetchall.return_value = [row]

        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            from api.routers.intelligence_causation import get_causal_links

            result = _run(get_causal_links(ticker="AAPL", days=90, _token="test"))

        link = result["links"][0]
        assert "cluster buy" in link["effect_description"]


# ══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTION TESTS
# ══════════════════════════════════════════════════════════════════════════


class TestParseEvidence:
    """Tests for the _parse_evidence helper."""

    def test_none_returns_empty_dict(self):
        from api.routers.intelligence_causation import _parse_evidence

        assert _parse_evidence(None) == {}

    def test_dict_passthrough(self):
        from api.routers.intelligence_causation import _parse_evidence

        data = {"key": "value"}
        assert _parse_evidence(data) == data

    def test_json_string_parsed(self):
        from api.routers.intelligence_causation import _parse_evidence

        assert _parse_evidence('{"a": 1}') == {"a": 1}

    def test_invalid_json_returns_empty(self):
        from api.routers.intelligence_causation import _parse_evidence

        assert _parse_evidence("not json") == {}

    def test_other_types_return_empty(self):
        from api.routers.intelligence_causation import _parse_evidence

        assert _parse_evidence(12345) == {}
        assert _parse_evidence([1, 2]) == {}
