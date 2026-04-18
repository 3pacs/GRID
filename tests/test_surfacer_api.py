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


def test_internal_telemetry_hypothesis_is_not_front_page():
    from api.routers.surfacer import _hypothesis_candidate

    candidate = _hypothesis_candidate(_row(
        id="hyp_internal",
        thesis="When snap:llm_task_expectation_tracking spikes, sig:insider increases within 2 days",
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
        created_at=datetime.now(timezone.utc) - timedelta(hours=5),
        last_tested=datetime.now(timezone.utc),
        role="candidate",
    ))

    assert candidate["diagnostic"] is True
    assert candidate["front_page"] is False
    assert candidate["status"] == "internal_telemetry"
    assert candidate["alpha_score"] <= 12
    assert candidate["score_parts"]["tradability"] == 0
    assert "not a trade candidate" in candidate["trade_expression"]


def test_hypothesis_without_trade_target_is_research_only():
    from api.routers.surfacer import _hypothesis_candidate

    candidate = _hypothesis_candidate(_row(
        id="hyp_research",
        thesis="Geopolitical tone leads darkpool activity within 3 days",
        pattern_type="lead_lag",
        evidence=[{
            "lag_days": 3,
            "signal_a": "sig:geopolitical_tone",
            "signal_b": "sig:darkpool",
            "correlation": 0.469,
        }],
        test_criteria={
            "lag_days": 3,
            "watch_signal": "sig:geopolitical_tone",
            "expect_signal": "sig:darkpool",
            "expected_direction": "increases",
        },
        invalidation="If geopolitical tone spikes and darkpool does NOT increase",
        confidence=0.75,
        status="active",
        times_tested=5,
        times_correct=4,
        created_at=datetime.now(timezone.utc) - timedelta(hours=5),
        last_tested=datetime.now(timezone.utc),
        role="candidate",
    ))

    assert candidate["diagnostic"] is False
    assert candidate["research_only"] is True
    assert candidate["front_page"] is False
    assert candidate["status"] == "research_only"
    assert candidate["tickers"] == []
    assert candidate["alpha_score"] <= 34


def test_candidate_selection_drops_diagnostics_by_default():
    from api.routers.surfacer import _select_candidates

    candidates = [
        {
            "id": "diagnostic",
            "title": "Internal telemetry correlation",
            "tickers": [],
            "direction": "watch",
            "alpha_score": 99,
            "freshness": {"label": "fresh"},
            "horizon": "multi_week",
            "diagnostic": True,
            "front_page": False,
        },
        {
            "id": "research",
            "title": "Generic lead-lag row",
            "tickers": [],
            "direction": "watch",
            "alpha_score": 80,
            "freshness": {"label": "fresh"},
            "horizon": "multi_week",
            "front_page": False,
            "research_only": True,
        },
        {
            "id": "blocked",
            "title": "BAC blocked setup",
            "tickers": ["BAC"],
            "direction": "bullish",
            "alpha_score": 99,
            "freshness": {"label": "fresh"},
            "horizon": "swing",
            "conviction": {"label": "blocked", "score": 99},
        },
        {
            "id": "market",
            "title": "BAC bullish setup",
            "tickers": ["BAC"],
            "direction": "bullish",
            "alpha_score": 55,
            "freshness": {"label": "fresh"},
            "horizon": "swing",
        },
    ]

    selected, filtered = _select_candidates(
        candidates,
        include_diagnostics=False,
        fresh_only=False,
        horizon="all",
        limit=10,
    )
    with_diagnostics, with_filtered_meta = _select_candidates(
        candidates,
        include_diagnostics=True,
        fresh_only=False,
        horizon="all",
        limit=10,
    )

    assert [item["id"] for item in selected] == ["market"]
    assert filtered["diagnostics_filtered"] == 1
    assert filtered["research_filtered"] == 1
    assert filtered["blocked_filtered"] == 1
    assert filtered["front_page_filtered"] == 3
    assert [item["id"] for item in with_diagnostics] == ["blocked", "diagnostic", "research", "market"]
    assert with_filtered_meta["front_page_filtered"] == 0


def test_conviction_gate_promotes_only_when_tradeable_evidence_aligns():
    from api.routers.surfacer import _build_conviction_gate

    candidate = {
        "title": "BAC Bullish setup",
        "summary": "BAC has a bullish oracle read.",
        "why_now": "Fresh model prediction with supporting signal stack.",
        "confidence": 0.72,
        "expected_move_pct": 9.0,
        "direction": "bullish",
        "horizon": "multi_week",
        "tickers": ["BAC"],
        "evidence": [{"label": "Oracle", "detail": "BAC-specific evidence"}] * 3,
        "contradictions": [],
        "source_modules": ["oracle", "signal_data"],
    }

    gate = _build_conviction_gate(
        candidate,
        options={"iv_atm": 0.20, "total_oi": 8000, "total_volume": 1500},
        track_record={"samples": 20, "hits": 14, "partials": 2, "misses": 4, "hit_rate": 0.75, "avg_pnl_pct": 4.2},
        confirmation={"samples": 5, "aligned": 3, "opposed": 0, "signals": ["Options Flow"]},
    )

    assert gate["label"] == "play"
    assert gate["score"] >= 82
    assert gate["expectation_gap"]["edge_pct"] > 0
    assert gate["missing"] == []


def test_oracle_candidate_carries_signal_calibration_context():
    from api.routers.surfacer import _oracle_candidate

    candidate = _oracle_candidate(_row(
        id=7,
        created_at=datetime.now(timezone.utc),
        ticker="BAC",
        prediction_type="CALL",
        direction="up",
        expiry=datetime.now(timezone.utc) + timedelta(days=30),
        confidence=0.62,
        expected_move_pct=9.5,
        signal_strength=0.7,
        coherence=0.8,
        model_name="fundamental",
        signals={
            "regime": "TIGHTENING",
            "fci_regime": "NEUTRAL",
            "signal_contributions": {
                "fundamental": 0.8,
                "options_flow": 0.4,
            },
        },
        anti_signals=[],
        flow_context={},
        verdict="pending",
    ))

    assert candidate["calibration"]["regime"] == "TIGHTENING"
    assert candidate["calibration"]["signal_contributions"] == {
        "fundamental": 0.8,
        "options_flow": 0.4,
    }


def test_signal_brier_history_fills_track_record_gap():
    from api.routers.surfacer import _merge_track_records

    merged = _merge_track_records(
        {"samples": 0},
        [
            {
                "signal_source": "options_flow",
                "samples": 30,
                "hit_rate": 0.7,
                "running_brier": 0.18,
                "contribution_weight": 0.7,
            },
            {
                "signal_source": "fundamental",
                "samples": 20,
                "hit_rate": 0.6,
                "running_brier": 0.21,
                "contribution_weight": 0.3,
            },
        ],
    )

    assert merged["source"] == "signal_brier"
    assert merged["samples"] == 50
    assert merged["hit_rate"] > 0.65
    assert merged["signal_brier"] > 0
    assert len(merged["signal_scorecards"]) == 2


def test_fallback_scorecards_remain_visible_but_do_not_drive_track_record():
    from api.routers.surfacer import _merge_track_records

    merged = _merge_track_records(
        {"samples": 12, "hit_rate": 0.6, "avg_pnl_pct": 1.2, "source": "oracle_predictions"},
        [
            {
                "signal_source": "options_flow",
                "samples": 24,
                "hit_rate": 0.75,
                "running_brier": 0.16,
                "contribution_weight": 0.8,
                "horizon_fallback": True,
            },
            {
                "signal_source": "oracle_aggregate",
                "samples": 1312,
                "hit_rate": 0.72,
                "running_brier": 0.15,
                "contribution_weight": 1.0,
                "aggregate_fallback": True,
                "horizon_fallback": True,
            },
        ],
    )

    assert merged["samples"] == 12
    assert merged["hit_rate"] == 0.6
    assert "signal_brier" not in merged
    assert [card["signal_source"] for card in merged["signal_scorecards"]] == ["options_flow", "oracle_aggregate"]


def test_signal_scorecards_fall_back_to_oracle_aggregate():
    from api.routers.surfacer import _fetch_signal_scorecards

    class _Result:
        def __init__(self, value=None, row=None):
            self._value = value
            self._row = row

        def scalar(self):
            return self._value

        def fetchone(self):
            return self._row

    class _Conn:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "to_regclass" in sql:
                return _Result(value=True)
            if "regime_conditional_brier_history" in sql:
                return _Result(row=None)
            if (
                "per_signal_brier_history" in sql
                and "ORDER BY ABS" in sql
                and params["source"] == "oracle_aggregate"
            ):
                return _Result(row=SimpleNamespace(_mapping={
                    "horizon_days": 7,
                    "scored_count": 1312,
                    "hit_count": 940,
                    "running_brier": 0.15,
                    "running_ece": 0.05,
                    "last_updated": datetime.now(timezone.utc),
                }))
            return _Result(row=None)

    cards = _fetch_signal_scorecards(_Conn(), {}, 30, "NEUTRAL")

    assert cards[0]["signal_source"] == "oracle_aggregate"
    assert cards[0]["aggregate_fallback"] is True
    assert cards[0]["horizon_fallback"] is True
    assert cards[0]["horizon_days"] == 7
    assert cards[0]["samples"] == 1312


def test_missing_data_request_key_dedupes_common_stock_gaps():
    from api.routers.surfacer import _missing_data_request_key

    candidate = {
        "id": "oracle-1",
        "direction": "bullish",
        "horizon": "multi_week",
        "model_name": "oracle",
        "calibration": {
            "signal_contributions": {
                "options_flow": 0.6,
                "fundamental": 0.4,
            }
        },
    }
    request = {
        "type": "historical_calibration",
        "ticker": "nvda",
        "horizon_days": 30,
    }

    key = _missing_data_request_key(candidate, request)

    assert key == "surfacer:historical_calibration:NVDA:30:bullish:oracle:fundamental,options_flow"


def test_missing_data_prompt_demands_structured_database_plan():
    from api.routers.surfacer import _build_missing_data_prompt

    prompt = _build_missing_data_prompt(
        {
            "id": "oracle-1",
            "title": "NVDA bullish setup",
            "direction": "bullish",
            "horizon": "multi_week",
            "tickers": ["NVDA"],
            "confidence": 0.72,
            "expected_move_pct": 8.0,
            "source_modules": ["oracle"],
            "calibration": {"signal_contributions": {"options_flow": 1.0}},
            "conviction": {"track_record": {"samples": 0}, "options": {}},
        },
        {
            "type": "historical_calibration",
            "ticker": "NVDA",
            "horizon_days": 30,
            "reason": "No scored analogs found.",
        },
    )

    assert "strict JSON" in prompt
    assert "database_write_plan" in prompt
    assert "Do not make up prices" in prompt
    assert "NVDA bullish setup" in prompt


def test_conviction_gate_blocks_unusable_inside_information():
    from api.routers.surfacer import _build_conviction_gate

    gate = _build_conviction_gate({
        "title": "XYZ merger leak",
        "summary": "Leaked earnings and material nonpublic information point higher.",
        "confidence": 0.99,
        "direction": "bullish",
        "horizon": "swing",
        "tickers": ["XYZ"],
        "evidence": [{"detail": "confidential deal details"}],
        "contradictions": [],
        "source_modules": ["signal_data"],
    })

    assert gate["label"] == "blocked"
    assert gate["score"] == 0
    assert "Do not trade" in gate["summary"]


def test_conviction_gate_keeps_no_target_rows_in_research():
    from api.routers.surfacer import _build_conviction_gate

    gate = _build_conviction_gate({
        "title": "Geopolitical tone leads dark-pool activity",
        "summary": "No concrete ticker.",
        "confidence": 0.75,
        "direction": "watch",
        "horizon": "multi_week",
        "tickers": [],
        "evidence": [{"detail": "generic correlation"}],
        "contradictions": [],
        "source_modules": ["discovery"],
    })

    assert gate["label"] == "research"
    assert "target" in gate["missing"]


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
