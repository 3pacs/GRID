"""
Tests for intelligence.company_analyzer — company influence profiling pipeline.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from intelligence.company_analyzer import (
    ANALYSIS_QUEUE,
    CompanyProfile,
    _compute_company_suspicion,
    _compute_confidence,
    _compute_lobbying_trend,
    _extract_top_issues,
    _parse_amount_range,
    _TICKER_NAMES,
    _TICKER_SECTORS,
)


# ── Unit tests for helpers ──────────────────────────────────────────────


class TestParseAmountRange:
    def test_numeric_passthrough(self):
        assert _parse_amount_range(5000.0) == 5000.0

    def test_dollar_range(self):
        result = _parse_amount_range("$1,001 - $15,000")
        assert result == pytest.approx(8000.5)

    def test_plain_number(self):
        assert _parse_amount_range("10000") == 10000.0

    def test_none(self):
        assert _parse_amount_range(None) == 0.0

    def test_empty_string(self):
        assert _parse_amount_range("") == 0.0

    def test_integer(self):
        assert _parse_amount_range(42) == 42.0


class TestComputeLobbyingTrend:
    def test_increasing(self):
        filings = [
            {"date": "2025-01-01", "amount": 100},
            {"date": "2025-02-01", "amount": 100},
            {"date": "2025-07-01", "amount": 200},
            {"date": "2025-08-01", "amount": 200},
        ]
        assert _compute_lobbying_trend(filings) == "increasing"

    def test_decreasing(self):
        filings = [
            {"date": "2025-01-01", "amount": 200},
            {"date": "2025-02-01", "amount": 200},
            {"date": "2025-07-01", "amount": 50},
            {"date": "2025-08-01", "amount": 50},
        ]
        assert _compute_lobbying_trend(filings) == "decreasing"

    def test_stable(self):
        filings = [
            {"date": "2025-01-01", "amount": 100},
            {"date": "2025-07-01", "amount": 100},
        ]
        assert _compute_lobbying_trend(filings) == "stable"

    def test_empty(self):
        assert _compute_lobbying_trend([]) == "stable"

    def test_single(self):
        assert _compute_lobbying_trend([{"date": "2025-01-01", "amount": 100}]) == "stable"


class TestExtractTopIssues:
    def test_basic(self):
        filings = [
            {"issue_codes": ["TAX", "DEF", "TAX"]},
            {"issue_codes": ["DEF"]},
        ]
        result = _extract_top_issues(filings)
        assert result[0] == "TAX" or result[0] == "DEF"
        assert "TAX" in result
        assert "DEF" in result

    def test_empty(self):
        assert _extract_top_issues([]) == []


class TestComputeSuspicion:
    def test_zero_data(self):
        score = _compute_company_suspicion(
            gov_total=0, lobbying_total=0, pac_total=0,
            circular_detected=False, hypocrisy_count=0,
            committee_overlap=[], insider_direction="neutral",
            total_money=0,
        )
        assert score == 0.0

    def test_max_flags(self):
        score = _compute_company_suspicion(
            gov_total=500_000_000, lobbying_total=10_000_000, pac_total=5_000_000,
            circular_detected=True, hypocrisy_count=5,
            committee_overlap=[{"committee": "Armed Services"}, {"committee": "Finance"}],
            insider_direction="net_selling",
            total_money=200_000_000,
        )
        assert score > 0.5

    def test_circular_flow_adds_suspicion(self):
        without = _compute_company_suspicion(
            gov_total=0, lobbying_total=0, pac_total=0,
            circular_detected=False, hypocrisy_count=0,
            committee_overlap=[], insider_direction="neutral",
            total_money=0,
        )
        with_flow = _compute_company_suspicion(
            gov_total=0, lobbying_total=0, pac_total=0,
            circular_detected=True, hypocrisy_count=0,
            committee_overlap=[], insider_direction="neutral",
            total_money=0,
        )
        assert with_flow > without

    def test_score_capped_at_one(self):
        score = _compute_company_suspicion(
            gov_total=1_000_000_000, lobbying_total=100_000_000, pac_total=50_000_000,
            circular_detected=True, hypocrisy_count=10,
            committee_overlap=[{"committee": f"C{i}"} for i in range(10)],
            insider_direction="net_selling",
            total_money=1_000_000_000,
        )
        assert score <= 1.0


class TestComputeConfidence:
    def test_no_data(self):
        conf = _compute_confidence(
            gov={"count": 0},
            influence={"lobbying_total": 0, "pac_total": 0, "member_trades": []},
            insider={"direction": "neutral", "holders": []},
            export={"risk_level": "UNKNOWN"},
        )
        assert conf == 0.0

    def test_full_data(self):
        conf = _compute_confidence(
            gov={"count": 5},
            influence={"lobbying_total": 1000, "pac_total": 500, "member_trades": [{"a": 1}]},
            insider={"direction": "net_buying", "holders": [{"member": "X"}]},
            export={"risk_level": "HIGH"},
        )
        assert conf == 1.0


class TestDataCompleteness:
    def test_analysis_queue_has_100_items(self):
        assert len(ANALYSIS_QUEUE) == 100

    def test_all_queue_tickers_have_names(self):
        missing = [t for t in ANALYSIS_QUEUE if t not in _TICKER_NAMES]
        assert missing == [], f"Missing names for: {missing}"

    def test_all_queue_tickers_have_sectors(self):
        missing = [t for t in ANALYSIS_QUEUE if t not in _TICKER_SECTORS]
        assert missing == [], f"Missing sectors for: {missing}"


class TestCompanyProfileDataclass:
    def test_to_dict(self):
        profile = CompanyProfile(
            ticker="NVDA", name="NVIDIA", sector="Semiconductors",
            market_cap=1e12,
            gov_contracts_total=5e9, gov_contracts_count=10,
            top_agencies=["DOD"],
            congress_holders=[], committee_overlap_count=0,
            insider_net_direction="net_buying",
            insider_total_value_90d=1e6, cluster_signals=2,
            lobbying_spend_annual=5e6, lobbying_trend="increasing",
            top_issues=["DEF", "SCI"],
            influence_loops=1, suspicion_score=0.45,
            hypocrisy_flags=1,
            export_control_risk="HIGH", regulatory_actions=3,
            analysis_narrative="Test narrative.",
            last_analyzed="2026-01-01T00:00:00+00:00",
            confidence=0.8,
        )
        d = profile.to_dict()
        assert d["ticker"] == "NVDA"
        assert d["suspicion_score"] == 0.45
        assert isinstance(d["top_agencies"], list)
