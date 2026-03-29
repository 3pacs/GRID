from __future__ import annotations

from scripts.seed_astrogrid_prediction_corpus import (
    build_prediction_request,
    default_question_templates,
)


def test_default_question_templates_cover_priority_questions() -> None:
    questions = [template.question for template in default_question_templates()]
    assert any("What crypto should I buy right now" in question for question in questions)
    assert any("Google, Apple, or Microsoft" in question for question in questions)
    assert any("NVDA worth buying" in question for question in questions)
    assert any("When should I buy Meta" in question for question in questions)


def test_build_prediction_request_returns_structured_seed_answer() -> None:
    template = next(
        item for item in default_question_templates()
        if "Google, Apple, or Microsoft" in item.question
    )
    snapshot = {
        "date": "2026-03-28",
        "lunar": {"phase_name": "Full Moon"},
        "seer": {"reading": "geometry leads.", "prediction": "Continuation has the cleaner edge."},
        "events": [{"name": "Next Full Moon"}],
    }
    scorecard = {
        "items": [
            {"symbol": "AAPL", "label": "Apple", "group": "equity", "bias": "press", "trend": "uptrend", "confidence": 0.75, "momentum_score": 0.42, "change_5d_pct": 3.1, "change_20d_pct": 8.6},
            {"symbol": "MSFT", "label": "Microsoft", "group": "equity", "bias": "press", "trend": "uptrend", "confidence": 0.72, "momentum_score": 0.31, "change_5d_pct": 2.2, "change_20d_pct": 6.2},
            {"symbol": "GOOGL", "label": "Alphabet", "group": "equity", "bias": "wait", "trend": "mixed", "confidence": 0.61, "momentum_score": 0.11, "change_5d_pct": 0.6, "change_20d_pct": 1.3},
        ]
    }
    regime_payload = {"state": "GROWTH", "confidence": 0.72, "transition_probability": 0.18}
    thesis_payload = {"overall_direction": "BULLISH", "conviction": 72, "narrative": "Risk appetite favors large-cap growth."}

    req = build_prediction_request(
        template=template,
        snapshot=snapshot,
        scorecard=scorecard,
        regime_payload=regime_payload,
        thesis_payload=thesis_payload,
    )

    assert req.question == template.question
    assert req.horizon_label == "swing"
    assert req.target_symbols[0] == "AAPL"
    assert req.call.startswith(("buy", "accumulate", "wait"))
    assert "break if" in req.invalidation
    assert req.market_overlay_snapshot["regime"]["state"] == "GROWTH"
