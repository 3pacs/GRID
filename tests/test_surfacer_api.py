"""Tests for the Surfacer API helpers and route wiring."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from types import ModuleType, SimpleNamespace


try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


def _row(**kwargs):
    return SimpleNamespace(_mapping=kwargs)


def test_router_wires_candidates_endpoint():
    from api.routers.surfacer import router

    assert router.prefix == "/api/v1/surfacer"
    assert "surfacer" in router.tags
    assert any(route.path.endswith("/candidates") for route in router.routes)


def test_unscored_hypothesis_still_becomes_candidate():
    from api.routers.surfacer import _hypothesis_candidate

    candidate = _hypothesis_candidate(_row(
        id=42,
        thesis="Semis rally when export restriction panic fades",
        pattern_type="policy_reversal",
        evidence='["fresh policy headline", "relative strength"]',
        test_criteria="NVDA and SOXX confirm together",
        invalidation="SOXX loses relative strength",
        confidence=None,
        status="active",
        times_tested=0,
        times_correct=0,
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
        last_tested=None,
        role="candidate",
    ))

    assert candidate["status"] == "unscored"
    assert candidate["confidence"] == 0.35
    assert candidate["alpha_score"] > 0
    assert candidate["source_modules"] == ["discovery", "hypotheses"]


def test_hypothesis_copy_is_human_readable():
    from api.routers.surfacer import _hypothesis_candidate

    candidate = _hypothesis_candidate(_row(
        id="hyp_clean",
        thesis="When snap:llm_task_expectation_tracking activity spikes, sig:insider activity increases within 2 days",
        pattern_type="lead_lag",
        evidence=[{
            "p_value": 0.001,
            "lag_days": 2,
            "signal_a": "snap:llm_task_expectation_tracking",
            "signal_b": "sig:insider",
            "correlation": 0.306,
            "n_observations": 107,
        }],
        test_criteria={
            "lag_days": 2,
            "watch_signal": "snap:llm_task_expectation_tracking",
            "expect_signal": "sig:insider",
            "expected_direction": "increases",
        },
        invalidation="If snap:llm_task_expectation_tracking spikes and sig:insider does NOT increase",
        confidence=0.75,
        status="active",
        times_tested=5,
        times_correct=4,
        created_at=datetime.now(timezone.utc) - timedelta(days=2),
        last_tested=datetime.now(timezone.utc),
        role="candidate",
    ))

    combined = " ".join([
        candidate["title"],
        candidate["summary"],
        candidate["invalidation"],
        candidate["evidence"][0]["detail"],
    ])
    assert "snap:" not in combined
    assert "sig:" not in combined
    assert "llm_task" not in combined
    assert "insider activity" in combined


def test_oracle_candidate_extracts_trade_and_anti_signals():
    from api.routers.surfacer import _oracle_candidate

    candidate = _oracle_candidate(_row(
        id=7,
        created_at=datetime.now(timezone.utc),
        ticker="UNH",
        prediction_type="PUT",
        direction="down",
        expiry=datetime.now(timezone.utc) + timedelta(days=21),
        confidence=0.54,
        expected_move_pct=8.5,
        signal_strength=0.68,
        coherence=0.72,
        model_name="qwen",
        signals='[{"label":"earnings pressure","detail":"margin compression"}]',
        anti_signals='["defensive rotation bid"]',
        flow_context='{"options":"put demand rising"}',
        verdict="pending",
    ))

    assert candidate["direction"] == "bearish"
    assert candidate["tickers"] == ["UNH"]
    assert "UNH PUT bias" in candidate["trade_expression"]
    assert candidate["contradictions"] == ["defensive rotation bid"]


def test_speculative_zero_confidence_signals_do_not_dominate():
    from api.routers.surfacer import _signal_candidate

    candidate = _signal_candidate(_row(
        id=99,
        signal_type="dex_liquidity_spike",
        signal_date="2026-04-18",
        ticker="SOL:MEME",
        actor=None,
        direction="new_pool",
        magnitude=9000000,
        description=None,
        data={"liquidity": 9000000},
        confidence=0,
        source_id=1,
        created_at=datetime.now(timezone.utc),
    ))

    assert candidate["status"] == "needs_research"
    assert candidate["score_parts"]["tradability"] == 35
    assert candidate["score_parts"]["risk_penalty"] >= 26
    assert candidate["alpha_score"] < 45


def test_dedupe_keeps_highest_score_and_sorts_descending():
    from api.routers.surfacer import _dedupe_candidates

    deduped = _dedupe_candidates([
        {"id": "low", "title": "NVDA Call setup", "tickers": ["NVDA"], "direction": "bullish", "alpha_score": 31},
        {"id": "high", "title": "NVDA Call setup", "tickers": ["NVDA"], "direction": "bullish", "alpha_score": 72},
        {"id": "other", "title": "UNH Put setup", "tickers": ["UNH"], "direction": "bearish", "alpha_score": 55},
    ])

    assert [item["id"] for item in deduped] == ["high", "other"]


def test_freshness_labels_recent_rows():
    from api.routers.surfacer import _freshness

    fresh = _freshness(datetime.now(timezone.utc) - timedelta(hours=4))
    stale = _freshness(datetime.now(timezone.utc) - timedelta(days=8))

    assert fresh["label"] == "fresh"
    assert stale["label"] == "stale"
