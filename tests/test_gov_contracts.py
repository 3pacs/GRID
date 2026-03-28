"""
Tests for GRID government contract ingestion and intelligence modules.

Tests the contractor-to-ticker mapping, award normalization, series_id
formatting, intelligence query functions, and overlap detection with
mocked database results.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch, PropertyMock
import json

import pytest

from ingestion.altdata.gov_contracts import (
    CONTRACTOR_TICKER_MAP,
    _match_contractor_to_ticker,
    _normalize_agency,
    _format_amount_tag,
    GovContractsPuller,
)
from intelligence.gov_intel import (
    ContractRecord,
    InsiderContractOverlap,
    get_recent_contracts,
    get_contracts_for_ticker,
    detect_contract_insider_overlap,
    _parse_payload,
)


# ── Contractor Mapping Tests ─────────────────────────────────────────────


class TestContractorTickerMap:
    """Test the contractor name to ticker matching."""

    def test_exact_match(self):
        assert _match_contractor_to_ticker("Lockheed Martin Corp") == "LMT"

    def test_case_insensitive(self):
        assert _match_contractor_to_ticker("BOEING COMPANY") == "BA"
        assert _match_contractor_to_ticker("boeing") == "BA"

    def test_partial_match(self):
        assert _match_contractor_to_ticker("Raytheon Technologies Corporation") == "RTX"

    def test_aws_match(self):
        assert _match_contractor_to_ticker("Amazon Web Services Inc") == "AMZN"

    def test_no_match(self):
        assert _match_contractor_to_ticker("Small Unknown Contractor LLC") is None

    def test_empty_input(self):
        assert _match_contractor_to_ticker("") is None
        assert _match_contractor_to_ticker(None) is None

    def test_specificity_ordering(self):
        """Longer keys should match before shorter ones."""
        # "raytheon technologies" should match before "raytheon"
        assert _match_contractor_to_ticker("Raytheon Technologies") == "RTX"

    def test_map_has_minimum_entries(self):
        """Map should have 30+ entries as specified."""
        unique_tickers = set(CONTRACTOR_TICKER_MAP.values())
        assert len(unique_tickers) >= 30


# ── Normalization Tests ──────────────────────────────────────────────────


class TestNormalization:
    """Test agency name normalization and amount formatting."""

    def test_normalize_agency_basic(self):
        result = _normalize_agency("Department of Defense")
        assert result == "DEPARTMENT_OF_DEFENSE"

    def test_normalize_agency_special_chars(self):
        result = _normalize_agency("Dept. of Health & Human Services")
        assert "HEALTH" in result
        assert "." not in result
        assert "&" not in result

    def test_normalize_agency_empty(self):
        assert _normalize_agency("") == "UNKNOWN"
        assert _normalize_agency(None) == "UNKNOWN"

    def test_normalize_agency_truncation(self):
        long_name = "A" * 100
        result = _normalize_agency(long_name)
        assert len(result) <= 40

    def test_format_amount_millions(self):
        assert _format_amount_tag(15_000_000) == "15M"
        assert _format_amount_tag(100_000_000) == "100M"

    def test_format_amount_billions(self):
        assert _format_amount_tag(1_200_000_000) == "1.2B"
        assert _format_amount_tag(5_000_000_000) == "5.0B"


# ── Award Normalization Tests ────────────────────────────────────────────


class TestAwardNormalization:
    """Test the raw API response normalization."""

    @patch.object(GovContractsPuller, "_resolve_source_id", return_value=1)
    def test_normalize_valid_award(self, mock_src):
        puller = GovContractsPuller.__new__(GovContractsPuller)
        puller.engine = MagicMock()
        puller.source_id = 1

        raw = {
            "Award ID": "W31P4Q-20-C-0123",
            "Recipient Name": "Lockheed Martin Corp",
            "Award Amount": 50_000_000,
            "Total Outlays": 10_000_000,
            "Description": "Aircraft maintenance contract",
            "Start Date": "2025-01-15",
            "Awarding Agency": "Department of Defense",
            "Awarding Sub Agency": "Army",
            "Contract Award Type": "Definitive Contract",
            "NAICS Code": "336411",
            "NAICS Description": "Aircraft Manufacturing",
        }

        result = puller._normalize_award(raw)
        assert result is not None
        assert result["award_id"] == "W31P4Q-20-C-0123"
        assert result["amount"] == 50_000_000
        assert result["recipient_name"] == "Lockheed Martin Corp"
        assert result["award_date"] == date(2025, 1, 15)

    @patch.object(GovContractsPuller, "_resolve_source_id", return_value=1)
    def test_normalize_below_threshold(self, mock_src):
        puller = GovContractsPuller.__new__(GovContractsPuller)
        puller.engine = MagicMock()
        puller.source_id = 1

        raw = {"Award ID": "X123", "Award Amount": 5_000_000}
        assert puller._normalize_award(raw) is None

    @patch.object(GovContractsPuller, "_resolve_source_id", return_value=1)
    def test_normalize_missing_id(self, mock_src):
        puller = GovContractsPuller.__new__(GovContractsPuller)
        puller.engine = MagicMock()
        puller.source_id = 1

        raw = {"Award Amount": 50_000_000}
        assert puller._normalize_award(raw) is None


# ── Payload Parsing Tests ────────────────────────────────────────────────


class TestParsePayload:
    """Test JSON payload parsing helper."""

    def test_parse_dict(self):
        assert _parse_payload({"key": "val"}) == {"key": "val"}

    def test_parse_json_string(self):
        assert _parse_payload('{"key": "val"}') == {"key": "val"}

    def test_parse_none(self):
        assert _parse_payload(None) is None

    def test_parse_invalid_string(self):
        assert _parse_payload("not json") is None

    def test_parse_number(self):
        assert _parse_payload(42) is None


# ── Intelligence Query Tests (mocked DB) ─────────────────────────────────


class TestGetRecentContracts:
    """Test get_recent_contracts with mocked database."""

    def test_returns_contracts(self):
        payload = json.dumps({
            "award_id": "W123",
            "recipient_name": "Boeing",
            "ticker": "BA",
            "awarding_agency": "DoD",
            "description": "Test contract",
            "naics_code": "336411",
            "contract_type": "Definitive Contract",
        })

        mock_row = (
            "GOV_CONTRACT:DOD:BA:50M",
            date(2025, 3, 1),
            50_000_000.0,
            payload,
        )

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [mock_row]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = get_recent_contracts(mock_engine, days=30)
        assert len(result) == 1
        assert result[0].ticker == "BA"
        assert result[0].amount == 50_000_000.0

    def test_empty_results(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = get_recent_contracts(mock_engine, days=7)
        assert result == []


class TestInsiderOverlap:
    """Test the overlap detection data class."""

    def test_overlap_to_dict(self):
        overlap = InsiderContractOverlap(
            ticker="LMT",
            contract_award_id="W123",
            contract_amount=100_000_000,
            contract_date="2025-03-01",
            contract_agency="DoD",
            trade_type="congressional",
            trade_source_id="member_x",
            trade_signal_type="BUY",
            trade_date="2025-02-20",
            days_before_contract=9,
            suspicion_score=0.75,
        )
        d = overlap.to_dict()
        assert d["ticker"] == "LMT"
        assert d["suspicion_score"] == 0.75
        assert d["days_before_contract"] == 9

    def test_contract_record_to_dict(self):
        record = ContractRecord(
            award_id="X456",
            recipient_name="Raytheon",
            ticker="RTX",
            amount=25_000_000,
            awarding_agency="Navy",
            description="Missile defense",
            award_date="2025-02-15",
            naics_code="332993",
            contract_type="Delivery Order",
        )
        d = record.to_dict()
        assert d["ticker"] == "RTX"
        assert d["amount"] == 25_000_000
