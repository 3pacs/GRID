"""Unit tests for intelligence/llm_red_team.py (CAT-181).

Zero network calls — the ``llm_client`` parameter is mocked in every test
that exercises ``red_team_prediction``.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence.llm_red_team import (
    CounterArgument,
    NUM_COUNTERS,
    RedTeamReport,
    TOP_K_FOR_RISK,
    build_red_team_prompt,
    compute_epistemic_risk,
    parse_red_team_response,
    red_team_prediction,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_signals() -> list[str]:
    return [
        "dealer_gamma: -$2.1B (bearish, z=-1.8)",
        "insider_cluster_buy: 4 buys in 5 days",
        "dark_pool_ratio: 0.62 (above 0.55 threshold)",
    ]


def _stub_client(return_value: Any) -> MagicMock:
    """Build a mock LLM client whose .generate() returns ``return_value``."""
    client = MagicMock()
    client.generate = MagicMock(return_value=return_value)
    return client


def _raising_client() -> MagicMock:
    client = MagicMock()
    client.generate = MagicMock(side_effect=RuntimeError("LLM offline"))
    return client


# ── build_red_team_prompt ───────────────────────────────────────────────────


class TestBuildRedTeamPrompt:
    def test_non_empty_with_core_fields(self, sample_signals: list[str]) -> None:
        prompt = build_red_team_prompt(
            ticker="NVDA",
            direction="CALL",
            horizon_days=7,
            score=0.7321,
            signal_summaries=sample_signals,
        )
        assert isinstance(prompt, str)
        assert prompt.strip()
        assert "NVDA" in prompt
        assert "7" in prompt  # horizon_days
        assert "0.7321" in prompt  # score formatted
        assert "CALL" in prompt
        assert str(NUM_COUNTERS) in prompt

    def test_includes_all_signal_summaries(self, sample_signals: list[str]) -> None:
        prompt = build_red_team_prompt(
            ticker="TSLA",
            direction="PUT",
            horizon_days=14,
            score=0.5,
            signal_summaries=sample_signals,
        )
        for sig in sample_signals:
            assert sig in prompt

    def test_empty_signal_list_is_handled(self) -> None:
        prompt = build_red_team_prompt(
            ticker="AAPL",
            direction="LONG",
            horizon_days=30,
            score=0.42,
            signal_summaries=[],
        )
        assert "AAPL" in prompt
        assert "no contributing signals" in prompt.lower()


# ── parse_red_team_response ─────────────────────────────────────────────────


def _make_json_payload(entries: list[dict[str, Any]]) -> str:
    return json.dumps({"counters": entries})


class TestParseRedTeamResponse:
    def test_clean_json(self) -> None:
        raw = _make_json_payload(
            [
                {"text": "Fed hikes unexpectedly", "severity": 0.9, "plausibility": 0.6},
                {"text": "Earnings miss priced in", "severity": 0.7, "plausibility": 0.5},
                {"text": "Liquidity freeze", "severity": 0.8, "plausibility": 0.4},
            ]
        )
        counters = parse_red_team_response(raw)
        assert len(counters) == 3
        assert all(isinstance(c, CounterArgument) for c in counters)
        assert counters[0].text == "Fed hikes unexpectedly"
        assert counters[0].severity == pytest.approx(0.9)
        assert counters[0].plausibility == pytest.approx(0.6)
        # composite grade = 0.5*0.9 + 0.5*0.6 = 0.75
        assert counters[0].grade == pytest.approx(0.75)

    def test_json_in_json_code_fence(self) -> None:
        payload = _make_json_payload(
            [{"text": "Thesis too crowded", "severity": 0.5, "plausibility": 0.5}]
        )
        raw = f"```json\n{payload}\n```"
        counters = parse_red_team_response(raw)
        assert len(counters) == 1
        assert counters[0].text == "Thesis too crowded"

    def test_json_in_plain_code_fence(self) -> None:
        payload = _make_json_payload(
            [{"text": "Regime flip", "severity": 0.6, "plausibility": 0.4}]
        )
        raw = f"```\n{payload}\n```"
        counters = parse_red_team_response(raw)
        assert len(counters) == 1
        assert counters[0].text == "Regime flip"

    def test_json_with_prose_preamble(self) -> None:
        payload = _make_json_payload(
            [{"text": "Macro shock", "severity": 1.0, "plausibility": 0.3}]
        )
        raw = f"Sure, here is my analysis: {payload}\nLet me know if you need more."
        counters = parse_red_team_response(raw)
        assert len(counters) == 1
        assert counters[0].text == "Macro shock"
        # severity=1.0, plausibility=0.3 → grade = 0.65
        assert counters[0].grade == pytest.approx(0.65)

    def test_empty_string(self) -> None:
        assert parse_red_team_response("") == []

    def test_whitespace_only(self) -> None:
        assert parse_red_team_response("   \n  ") == []

    def test_malformed_json(self) -> None:
        raw = "{ counters: [this is not json "
        # Should not raise
        result = parse_red_team_response(raw)
        assert result == []

    def test_missing_counters_key(self) -> None:
        raw = json.dumps({"hypotheses": []})
        assert parse_red_team_response(raw) == []

    def test_out_of_range_values_are_clamped(self) -> None:
        raw = _make_json_payload(
            [{"text": "X", "severity": 1.5, "plausibility": -0.2}]
        )
        counters = parse_red_team_response(raw)
        assert len(counters) == 1
        assert counters[0].severity == 1.0
        assert counters[0].plausibility == 0.0
        assert counters[0].grade == pytest.approx(0.5)

    def test_item_without_text_is_skipped(self) -> None:
        raw = _make_json_payload(
            [
                {"severity": 0.9, "plausibility": 0.9},  # missing text
                {"text": "", "severity": 0.5, "plausibility": 0.5},  # empty text
                {"text": "valid", "severity": 0.6, "plausibility": 0.4},
            ]
        )
        counters = parse_red_team_response(raw)
        assert len(counters) == 1
        assert counters[0].text == "valid"


# ── compute_epistemic_risk ──────────────────────────────────────────────────


def _counter(grade: float) -> CounterArgument:
    return CounterArgument(text="t", severity=grade, plausibility=grade, grade=grade)


class TestComputeEpistemicRisk:
    def test_empty_returns_zero(self) -> None:
        assert compute_epistemic_risk([]) == 0.0

    def test_single_counter_returns_its_grade(self) -> None:
        assert compute_epistemic_risk([_counter(0.42)]) == pytest.approx(0.42)

    def test_two_counters_returns_mean(self) -> None:
        risk = compute_epistemic_risk([_counter(0.6), _counter(0.8)])
        assert risk == pytest.approx(0.7)

    def test_three_counters_uses_top_two(self) -> None:
        # Grades: 0.9, 0.4, 0.7  → sorted desc: 0.9, 0.7, 0.4 → mean of top 2 = 0.8
        risk = compute_epistemic_risk(
            [_counter(0.9), _counter(0.4), _counter(0.7)]
        )
        assert risk == pytest.approx(0.8)
        # Sanity: constant says we take exactly 2
        assert TOP_K_FOR_RISK == 2

    def test_many_counters_still_top_two(self) -> None:
        counters = [_counter(g) for g in (0.1, 0.2, 0.95, 0.3, 0.85)]
        # top 2 = 0.95 and 0.85 → mean = 0.90
        assert compute_epistemic_risk(counters) == pytest.approx(0.90)


# ── red_team_prediction ─────────────────────────────────────────────────────


class TestRedTeamPrediction:
    def test_happy_path_three_counters(self, sample_signals: list[str]) -> None:
        raw = _make_json_payload(
            [
                {"text": "Rates up 50bp", "severity": 0.95, "plausibility": 0.6},
                {"text": "Guide cut", "severity": 0.7, "plausibility": 0.55},
                {"text": "China tariff", "severity": 0.8, "plausibility": 0.4},
            ]
        )
        client = _stub_client(raw)

        report = red_team_prediction(
            ticker="NVDA",
            direction="CALL",
            horizon_days=7,
            score=0.81,
            signal_summaries=sample_signals,
            llm_client=client,
        )

        assert isinstance(report, RedTeamReport)
        assert report.ticker == "NVDA"
        assert report.prediction_score == 0.81
        assert len(report.counters) == 3
        assert report.epistemic_risk_score > 0.0
        # Composite grades:
        #   Rates up 50bp : 0.5*0.95 + 0.5*0.6  = 0.775
        #   Guide cut     : 0.5*0.7  + 0.5*0.55 = 0.625
        #   China tariff  : 0.5*0.8  + 0.5*0.4  = 0.600
        # Top 2 = 0.775 and 0.625 → mean = 0.700
        assert report.epistemic_risk_score == pytest.approx(0.7)
        client.generate.assert_called_once()

    def test_client_raises_returns_empty_report(
        self, sample_signals: list[str]
    ) -> None:
        client = _raising_client()
        report = red_team_prediction(
            ticker="AAPL",
            direction="PUT",
            horizon_days=14,
            score=0.55,
            signal_summaries=sample_signals,
            llm_client=client,
        )
        assert isinstance(report, RedTeamReport)
        assert report.ticker == "AAPL"
        assert report.prediction_score == 0.55
        assert report.counters == tuple()
        assert report.epistemic_risk_score == 0.0

    def test_client_returns_garbage(self, sample_signals: list[str]) -> None:
        client = _stub_client("this is not json at all, just free prose")
        report = red_team_prediction(
            ticker="MSFT",
            direction="LONG",
            horizon_days=21,
            score=0.44,
            signal_summaries=sample_signals,
            llm_client=client,
        )
        assert report.counters == tuple()
        assert report.epistemic_risk_score == 0.0
        assert report.ticker == "MSFT"

    def test_client_returns_none(self, sample_signals: list[str]) -> None:
        client = _stub_client(None)
        report = red_team_prediction(
            ticker="TSLA",
            direction="CALL",
            horizon_days=3,
            score=0.9,
            signal_summaries=sample_signals,
            llm_client=client,
        )
        assert report.counters == tuple()
        assert report.epistemic_risk_score == 0.0

    def test_client_with_only_chat_method(
        self, sample_signals: list[str]
    ) -> None:
        """Client that exposes .chat() but not .generate() should still work."""
        raw = _make_json_payload(
            [{"text": "A", "severity": 0.5, "plausibility": 0.5}]
        )
        client = MagicMock(spec=["chat"])  # no .generate
        client.chat = MagicMock(return_value=raw)

        report = red_team_prediction(
            ticker="AMD",
            direction="LONG",
            horizon_days=10,
            score=0.5,
            signal_summaries=sample_signals,
            llm_client=client,
        )
        assert len(report.counters) == 1
        client.chat.assert_called_once()


# ── RedTeamReport.to_dict round-trip ────────────────────────────────────────


class TestRedTeamReportToDict:
    def test_round_trip_all_fields(self) -> None:
        counters = (
            CounterArgument(text="A", severity=0.9, plausibility=0.6, grade=0.75),
            CounterArgument(text="B", severity=0.4, plausibility=0.8, grade=0.60),
        )
        report = RedTeamReport(
            ticker="META",
            prediction_score=0.33,
            counters=counters,
            epistemic_risk_score=0.675,
        )
        d = report.to_dict()
        assert d["ticker"] == "META"
        assert d["prediction_score"] == 0.33
        assert d["epistemic_risk_score"] == 0.675
        assert isinstance(d["counters"], list)
        assert len(d["counters"]) == 2
        assert d["counters"][0] == {
            "text": "A",
            "severity": 0.9,
            "plausibility": 0.6,
            "grade": 0.75,
        }
        assert d["counters"][1]["text"] == "B"
        # Ensure everything is JSON serializable.
        assert json.dumps(d)

    def test_empty_report_to_dict(self) -> None:
        report = RedTeamReport(
            ticker="SPY",
            prediction_score=0.5,
            counters=tuple(),
            epistemic_risk_score=0.0,
        )
        d = report.to_dict()
        assert d["counters"] == []
        assert d["epistemic_risk_score"] == 0.0
        assert d["ticker"] == "SPY"
