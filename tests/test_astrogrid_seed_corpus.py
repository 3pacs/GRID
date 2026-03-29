from __future__ import annotations

from datetime import date

from oracle.astrogrid_universe import get_astrogrid_scoreable_universe
from scripts.seed_astrogrid_prediction_corpus import (
    build_prediction_request,
    default_question_templates,
)
from store.astrogrid import _effective_verdict


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
            {"symbol": "MSFT", "label": "Microsoft", "group": "equity", "bias": "press", "trend": "uptrend", "confidence": 0.72, "momentum_score": 0.31, "change_5d_pct": 2.2, "change_20d_pct": 6.2, "status": "scoreable_now", "scoreable_now": True},
            {"symbol": "GOOGL", "label": "Alphabet", "group": "equity", "bias": "wait", "trend": "mixed", "confidence": 0.61, "momentum_score": 0.11, "change_5d_pct": 0.6, "change_20d_pct": 1.3, "status": "scoreable_now", "scoreable_now": True},
        ]
    }
    scorecard["items"][0]["status"] = "scoreable_now"
    scorecard["items"][0]["scoreable_now"] = True
    regime_payload = {"state": "GROWTH", "confidence": 0.72, "transition_probability": 0.18}
    thesis_payload = {"overall_direction": "BULLISH", "conviction": 72, "narrative": "Risk appetite favors large-cap growth."}

    req = build_prediction_request(
        template=template,
        snapshot=snapshot,
        scorecard=scorecard,
        regime_payload=regime_payload,
        thesis_payload=thesis_payload,
        as_of_date=date(2026, 3, 28),
    )

    assert req.question == template.question
    assert req.horizon_label == "swing"
    assert req.scoring_class == "liquid_market"
    assert req.target_symbols[0] == "AAPL"
    assert req.call.startswith(("buy", "accumulate", "wait"))
    assert "break if" in req.invalidation
    assert req.market_overlay_snapshot["regime"]["state"] == "GROWTH"
    assert req.market_overlay_snapshot["scorecard"]["target_statuses"][0]["status"] == "scoreable_now"


def test_scoreable_universe_uses_canonical_qqq_and_cl_features() -> None:
    universe = {item["symbol"]: item for item in get_astrogrid_scoreable_universe()}
    assert universe["QQQ"]["price_feature"] == "qqq_full"
    assert universe["CL"]["price_feature"] == "cl_close"


def test_build_prediction_request_preserves_historical_as_of_date() -> None:
    template = next(
        item for item in default_question_templates()
        if "What crypto should I buy right now" in item.question
    )
    snapshot = {
        "date": "2025-01-15",
        "lunar": {"phase_name": "Full Moon"},
        "seer": {"reading": "geometry leads."},
        "events": [{"name": "Next Full Moon"}],
    }
    scorecard = {
        "items": [
            {"symbol": "BTC", "label": "Bitcoin", "group": "crypto", "bias": "press", "trend": "uptrend", "confidence": 0.82, "momentum_score": 0.41, "change_5d_pct": 3.8, "change_20d_pct": 11.2, "status": "scoreable_now", "scoreable_now": True},
            {"symbol": "ETH", "label": "Ethereum", "group": "crypto", "bias": "press", "trend": "uptrend", "confidence": 0.71, "momentum_score": 0.28, "change_5d_pct": 2.6, "change_20d_pct": 8.4, "status": "scoreable_now", "scoreable_now": True},
            {"symbol": "SOL", "label": "Solana", "group": "crypto", "bias": "wait", "trend": "mixed", "confidence": 0.55, "momentum_score": 0.12, "change_5d_pct": 1.1, "change_20d_pct": 4.2, "status": "scoreable_now", "scoreable_now": True},
        ]
    }

    req = build_prediction_request(
        template=template,
        snapshot=snapshot,
        scorecard=scorecard,
        regime_payload={"state": "RISK_ON"},
        thesis_payload={"overall_direction": "BULLISH"},
        as_of_date=date(2025, 1, 15),
    )

    assert req.as_of_ts == "2025-01-15T12:00:00+00:00"


def test_build_prediction_request_downgrades_degraded_targets() -> None:
    template = next(
        item for item in default_question_templates()
        if "SPY or QQQ" in item.question
    )
    snapshot = {
        "date": "2026-03-28",
        "lunar": {"phase_name": "Full Moon"},
        "seer": {"reading": "geometry leads."},
        "events": [{"name": "Next Full Moon"}],
    }
    scorecard = {
        "items": [
            {"symbol": "SPY", "label": "S&P 500", "group": "macro", "bias": "press", "trend": "uptrend", "confidence": 0.74, "momentum_score": 0.33, "change_5d_pct": 2.4, "change_20d_pct": 5.9, "status": "scoreable_now", "scoreable_now": True},
            {"symbol": "QQQ", "label": "Nasdaq 100", "group": "macro", "bias": "press", "trend": "uptrend", "confidence": 0.55, "momentum_score": 0.31, "change_5d_pct": 2.1, "change_20d_pct": 5.4, "status": "degraded", "scoreable_now": False, "reason_if_not": "needs longer history"},
        ]
    }
    regime_payload = {"state": "GROWTH", "confidence": 0.7, "transition_probability": 0.2}
    thesis_payload = {"overall_direction": "BULLISH", "conviction": 70}

    req = build_prediction_request(
        template=template,
        snapshot=snapshot,
        scorecard=scorecard,
        regime_payload=regime_payload,
        thesis_payload=thesis_payload,
        as_of_date=date(2026, 3, 28),
    )

    assert req.scoring_class == "unscored_experimental"
    assert any(item["status"] == "degraded" for item in req.market_overlay_snapshot["scorecard"]["target_statuses"])


def test_effective_verdict_requires_substantial_move_for_directional_calls() -> None:
    assert _effective_verdict("bullish", 0.01, horizon_label="swing") == "miss"
    assert _effective_verdict("bullish", 0.025, horizon_label="swing") == "partial"
    assert _effective_verdict("bullish", 0.05, horizon_label="swing") == "hit"
    assert _effective_verdict("bearish", -0.03, horizon_label="macro") == "miss"
    assert _effective_verdict("bearish", -0.05, horizon_label="macro") == "partial"
    assert _effective_verdict("bearish", -0.09, horizon_label="macro") == "hit"


def test_effective_verdict_allows_small_move_only_for_neutral_calls() -> None:
    assert _effective_verdict("neutral", 0.005, horizon_label="swing") == "hit"
    assert _effective_verdict("neutral", 0.015, horizon_label="swing") == "miss"
