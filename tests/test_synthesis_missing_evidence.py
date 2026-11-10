from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from intelligence.decision_gateway import _run_red_team, combine_verdict, should_i_trade
from intelligence.freshness_guard import check_freshness


@pytest.mark.parametrize(
    "risk", [None, float("nan"), float("inf"), -0.1, 1.1, True, "0.1", 10**1000]
)
def test_invalid_risk_fails_closed(risk):
    assert combine_verdict("high", 0.8, "robust", 0.9, risk)[0] == "no_trade"


@pytest.mark.parametrize(
    "label,score",
    [
        (None, None),
        ("unknown", 0.9),
        ("robust", float("nan")),
        ("robust", True),
        ("fragile", -1),
    ],
)
def test_missing_invalid_stress_fails_closed(label, score):
    assert combine_verdict("high", 0.8, label, score, 0.1)[0] == "no_trade"


@pytest.mark.parametrize("risk", [None, float("nan"), True, "0.1", 2])
def test_malformed_red_team_result(risk):
    with patch(
        "intelligence.llm_red_team.red_team_prediction",
        return_value=SimpleNamespace(
            counters=("fixture counterargument",), epistemic_risk_score=risk
        ),
    ):
        _report, value, error = _run_red_team(SimpleNamespace())
    assert value is None and error


def test_red_team_exception_is_unknown():
    with patch(
        "intelligence.llm_red_team.red_team_prediction",
        side_effect=RuntimeError("offline"),
    ):
        assert _run_red_team(SimpleNamespace())[1] is None


def test_partial_gateway_cannot_issue_ticket():
    with (
        patch(
            "intelligence.decision_gateway._run_prediction",
            return_value=(SimpleNamespace(), None),
        ),
        patch(
            "intelligence.decision_gateway._run_red_team",
            return_value=(None, None, "offline"),
        ),
        patch(
            "intelligence.decision_gateway._run_pattern_library",
            return_value=(None, None, "offline"),
        ),
        patch("trading.trade_ticket_generator.generate_ticket") as ticket,
    ):
        result = should_i_trade(
            MagicMock(), "NVDA", account_size_usd=100, current_price=20
        )
    assert result.unified_verdict == "no_trade"
    assert result.trade_ticket is None
    assert "red_team" in result.stage_errors
    ticket.assert_not_called()


@pytest.mark.parametrize(
    "offset,status", [(1, "INVALID"), (0, "FRESH"), (-1, "FRESH"), (-365, "STALE")]
)
def test_freshness_future_not_fresh(offset, status):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
        ("feature", "", date.today() + timedelta(days=offset))
    ]
    assert check_freshness(engine, ["feature"])["feature"].status == status


def test_missing_pattern_is_not_agreement():
    verdict, reasons = combine_verdict("high", None, "robust", 0.9, 0.1)
    assert verdict == "high"
    assert "unavailable" in reasons[0]
    assert "align" not in reasons[0]


@pytest.mark.parametrize(
    "field,value",
    [("pattern", True), ("pattern", "bad"), ("stress", True), ("stress", "bad")],
)
def test_invalid_adapter_value_blocks_ticket(field, value):
    with (
        patch(
            "intelligence.decision_gateway._run_prediction",
            return_value=(SimpleNamespace(), None),
        ),
        patch(
            "intelligence.decision_gateway._run_red_team",
            return_value=(SimpleNamespace(), 0.1, None),
        ),
        patch(
            "intelligence.decision_gateway._run_provenance",
            return_value=(SimpleNamespace(verdict="high"), None),
        ),
        patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=SimpleNamespace(
                confidence_signal=value if field == "pattern" else 0.8
            ),
        ),
        patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=SimpleNamespace(
                robustness_label="robust",
                robustness_score=value if field == "stress" else 0.9,
            ),
        ),
        patch("trading.trade_ticket_generator.generate_ticket") as ticket,
    ):
        result = should_i_trade(
            MagicMock(), "NVDA", account_size_usd=100, current_price=20
        )
    assert result.unified_verdict == "no_trade"
    ticket.assert_not_called()


@pytest.mark.parametrize("raw", [None, "", "not JSON"])
def test_real_red_team_fallback_is_unavailable(raw):
    client = SimpleNamespace(generate=lambda prompt: raw)
    report, risk, error = _run_red_team(SimpleNamespace(), llm_client=client)
    assert report.counters == ()
    assert risk is None
    assert error == "red-team counterarguments unavailable"


def test_real_red_team_zero_risk_with_counters_is_valid():
    import json

    client = SimpleNamespace(
        generate=lambda prompt: json.dumps(
            {
                "counters": [
                    {
                        "text": "An explicitly tested counterargument",
                        "severity": 0,
                        "plausibility": 0,
                    }
                ]
            }
        )
    )
    report, risk, error = _run_red_team(SimpleNamespace(), llm_client=client)
    assert report.counters
    assert risk == 0.0
    assert error is None


@pytest.mark.parametrize(
    "scores",
    [
        {},
        {"severity": "bad", "plausibility": None},
        {"severity": float("nan"), "plausibility": float("nan")},
        {"severity": True, "plausibility": 0.5},
        {"severity": 2.0, "plausibility": 0.5},
        {"severity": float("inf"), "plausibility": 0.5},
        {"severity": 10**1000, "plausibility": 0.5},
    ],
)
def test_real_gateway_rejects_raw_invalid_counter_scores(scores):
    import json

    raw = json.dumps({"counters": [{"text": "Revenue risk", **scores}]})
    client = SimpleNamespace(generate=lambda prompt: raw)
    report, risk, error = _run_red_team(SimpleNamespace(), llm_client=client)
    assert not report.counters
    assert risk is None and error


def test_strict_counter_grade_is_computed_not_model_override():
    import json

    client = SimpleNamespace(
        generate=lambda prompt: json.dumps(
            {
                "counters": [
                    {
                        "text": "Material risk",
                        "severity": 1,
                        "plausibility": 1,
                        "grade": 0,
                    }
                ]
            }
        )
    )
    report, risk, error = _run_red_team(SimpleNamespace(), llm_client=client)
    assert report.counters[0].grade == 1
    assert risk == 1 and error is None


def test_strict_parse_rejects_whole_partial_response():
    import json

    from intelligence.llm_red_team import parse_red_team_response

    raw = json.dumps(
        {
            "counters": [
                {"text": "Valid", "severity": 0, "plausibility": 0},
                {"text": "Invalid"},
            ]
        }
    )
    assert parse_red_team_response(raw, strict_scores=True) == []
    assert len(parse_red_team_response(raw)) == 2  # legacy contract unchanged
