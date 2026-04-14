"""Tests for intelligence/decision_gateway.py.

Every test mocks the downstream modules via ``unittest.mock.patch`` so
the gateway is exercised in pure orchestration mode — no real oracle,
no LLM, no DB.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from intelligence.decision_gateway import (
    DecisionResponse,
    _downgrade_verdict,
    combine_verdict,
    should_i_trade,
)


# ── combine_verdict (pure function) ──────────────────────────────────────


class TestCombineVerdict:
    def test_no_trade_passthrough(self):
        v, reasons = combine_verdict("no_trade", None, None, None, 0.0)
        assert v == "no_trade"
        assert any("no_trade" in r for r in reasons)

    def test_red_team_veto(self):
        v, reasons = combine_verdict("high", 0.7, "robust", 0.95, red_team_risk=0.85)
        assert v == "no_trade"
        assert any("red-team" in r.lower() for r in reasons)

    def test_fragile_stress_downgrades_one_level(self):
        v, reasons = combine_verdict(
            "high", 0.7, "fragile", 0.3, red_team_risk=0.0
        )
        assert v == "medium"
        assert any("stress fragile" in r for r in reasons)

    def test_fragile_but_robust_score_no_downgrade(self):
        # Label fragile but score >= 0.5 → no downgrade
        v, _ = combine_verdict("high", 0.7, "fragile", 0.6, red_team_risk=0.0)
        assert v == "high"

    def test_pattern_disagreement_downgrades(self):
        # pattern_confidence 0.15 < 0.3 → downgrade
        v, reasons = combine_verdict(
            "medium", 0.15, "robust", 0.95, red_team_risk=0.0
        )
        assert v == "low"
        assert any("pattern library" in r for r in reasons)

    def test_pattern_zero_means_insufficient_no_downgrade(self):
        # pattern_confidence=0.0 means pattern library had insufficient
        # analogs — should NOT downgrade (can't use it as evidence)
        v, _ = combine_verdict("medium", 0.0, "robust", 0.95, red_team_risk=0.0)
        assert v == "medium"

    def test_pattern_none_no_downgrade(self):
        v, _ = combine_verdict("medium", None, "robust", 0.95, red_team_risk=0.0)
        assert v == "medium"

    def test_stacked_downgrades(self):
        # Fragile stress + weak pattern → 2 downgrades
        v, reasons = combine_verdict(
            "high", 0.15, "fragile", 0.3, red_team_risk=0.0
        )
        assert v == "low"  # high → medium → low
        assert len(reasons) >= 2

    def test_stacked_downgrades_below_floor(self):
        # Already low, two downgrades → no_trade
        v, _ = combine_verdict(
            "low", 0.15, "fragile", 0.3, red_team_risk=0.0
        )
        assert v == "no_trade"

    def test_all_layers_align_clean_reason(self):
        v, reasons = combine_verdict(
            "high", 0.85, "robust", 0.95, red_team_risk=0.1
        )
        assert v == "high"
        assert any("align" in r for r in reasons)

    def test_unknown_verdict_coerces_to_no_trade(self):
        v, _ = combine_verdict("alpha_beast_mode", 0.8, "robust", 0.95, 0.0)
        assert v == "no_trade"


class TestDowngradeVerdict:
    def test_high_to_medium(self):
        assert _downgrade_verdict("high") == "medium"

    def test_medium_to_low(self):
        assert _downgrade_verdict("medium") == "low"

    def test_low_to_no_trade(self):
        assert _downgrade_verdict("low") == "no_trade"

    def test_no_trade_stays(self):
        assert _downgrade_verdict("no_trade") == "no_trade"

    def test_unknown_to_no_trade(self):
        assert _downgrade_verdict("quantum") == "no_trade"


# ── Full gateway orchestration ───────────────────────────────────────────


def _mock_prediction() -> SimpleNamespace:
    return SimpleNamespace(
        ticker="NVDA",
        direction="bullish",
        score=75,
        confidence=0.82,
        confidence_lower=0.72,
        confidence_upper=0.92,
        horizon=7,
        regime="EXPANSION",
        fci_regime="EASY",
        model_votes=[
            {"model_name": "flow_momentum", "weight": 2.0},
            {"model_name": "regime_contrarian", "weight": 3.0},
        ],
        shapley_top_contributor="regime_contrarian",
        shapley_top_share=0.6,
        fragility_multiplier=1.0,
        disagreement_score=0.0,
        crowd_aligned=False,
        market_implied_prob=0.78,
        catalyst_type="earnings",
        liquidity_state="EXPANSION",
    )


def _mock_provenance(verdict: str = "high") -> SimpleNamespace:
    obj = SimpleNamespace(
        ticker="NVDA",
        verdict=verdict,
        aggregate_conviction=1.3,
        direction="bullish",
        confidence=0.82,
        confidence_lower=0.72,
        confidence_upper=0.92,
        horizon_days=7,
        regime="EXPANSION",
        fci_regime="EASY",
        top_shapley_contributor="regime_contrarian",
        top_shapley_share=0.6,
        fragility_multiplier=1.0,
        disagreement_score=0.0,
        crowd_aligned=False,
        market_implied_prob=0.78,
        red_team_epistemic_risk=0.0,
        shipping_fudge_alerts=[],
        signal_evidence=[],
        causation=SimpleNamespace(
            lever="earnings",
            flow_direction="open",
            actor="regime_contrarian",
            complete=True,
            to_dict=lambda: {
                "lever": "earnings",
                "flow_direction": "open",
                "actor": "regime_contrarian",
                "complete": True,
            },
        ),
    )
    obj.to_dict = lambda: {"ticker": "NVDA", "verdict": verdict}
    return obj


class TestShouldITrade:
    def test_happy_path_high_verdict_produces_ticket(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("high")
        pattern = SimpleNamespace(confidence_signal=0.75)
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)
        ticket = SimpleNamespace(ticker="NVDA", verdict="high")

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.1),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ), patch(
            "trading.trade_ticket_generator.generate_ticket",
            return_value=ticket,
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "nvda",
                account_size_usd=100000,
                current_price=500.0,
                vol_30d=0.35,
            )

        assert response.ticker == "NVDA"
        assert response.unified_verdict == "high"
        assert response.trade_ticket is ticket
        assert response.stage_errors == {}

    def test_red_team_veto_zeros_verdict(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("high")
        pattern = SimpleNamespace(confidence_signal=0.75)
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.9),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=500.0,
                vol_30d=0.35,
            )

        assert response.unified_verdict == "no_trade"
        assert response.trade_ticket is None
        assert any("red-team" in r.lower() for r in response.verdict_reasons)

    def test_prediction_failure_partial_response(self):
        engine = MagicMock()
        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor:
            MockPredictor.return_value.predict.side_effect = RuntimeError("db down")
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=500.0,
            )

        assert response.prediction is None
        assert "prediction" in response.stage_errors
        assert response.unified_verdict == "no_trade"

    def test_pattern_disagrees_downgrades(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("high")
        pattern = SimpleNamespace(confidence_signal=0.15)  # weak base rate
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.1),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ), patch(
            "trading.trade_ticket_generator.generate_ticket",
            return_value=None,
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=500.0,
                vol_30d=0.35,
            )

        assert response.unified_verdict == "medium"

    def test_fragile_stress_downgrades(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("high")
        pattern = SimpleNamespace(confidence_signal=0.75)
        stress = SimpleNamespace(
            robustness_label="fragile", robustness_score=0.3
        )

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.1),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ), patch(
            "trading.trade_ticket_generator.generate_ticket",
            return_value=None,
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=500.0,
                vol_30d=0.35,
            )

        assert response.unified_verdict == "medium"  # high → medium

    def test_pattern_library_failure_does_not_break_gateway(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("high")
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.1),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            side_effect=RuntimeError("pattern library oops"),
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ), patch(
            "trading.trade_ticket_generator.generate_ticket",
            return_value=SimpleNamespace(ticker="NVDA"),
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=500.0,
                vol_30d=0.35,
            )

        assert response.pattern_report is None
        assert "pattern_library" in response.stage_errors
        # High verdict preserved — no pattern confidence to downgrade on
        assert response.unified_verdict == "high"

    def test_missing_current_price_blocks_ticket(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("high")
        pattern = SimpleNamespace(confidence_signal=0.75)
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.1),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=None,
            )

        assert response.unified_verdict == "high"
        assert response.trade_ticket is None
        assert "trade_ticket" in response.stage_errors

    def test_low_verdict_blocks_ticket(self):
        engine = MagicMock()
        pred = _mock_prediction()
        prov = _mock_provenance("low")
        pattern = SimpleNamespace(confidence_signal=0.75)
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)

        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=SimpleNamespace(epistemic_risk_score=0.1),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ):
            MockPredictor.return_value.predict.return_value = pred
            response = should_i_trade(
                engine,
                "NVDA",
                account_size_usd=100000,
                current_price=500.0,
                vol_30d=0.35,
            )

        assert response.unified_verdict == "low"
        assert response.trade_ticket is None
        assert "trade_ticket" in response.stage_errors

    def test_to_dict_serializes_all_stages(self):
        response = DecisionResponse(
            ticker="NVDA",
            generated_at="2026-04-14T00:00:00+00:00",
            horizon_days=7,
            prediction=None,
            red_team_report=None,
            provenance_report=None,
            pattern_report=None,
            stress_report=None,
            trade_ticket=None,
            unified_verdict="no_trade",
            verdict_reasons=["nothing to see here"],
            stage_errors={"prediction": "db down"},
        )
        d = response.to_dict()
        for k in (
            "ticker", "generated_at", "horizon_days", "unified_verdict",
            "verdict_reasons", "stage_errors",
            "prediction", "red_team_report", "provenance_report",
            "pattern_report", "stress_report", "trade_ticket",
        ):
            assert k in d
        assert d["unified_verdict"] == "no_trade"
        assert d["stage_errors"] == {"prediction": "db down"}

    def test_ticker_uppercased(self):
        engine = MagicMock()
        with patch(
            "oracle.engine.EnsemblePredictor"
        ) as MockPredictor:
            MockPredictor.return_value.predict.side_effect = RuntimeError("stub")
            response = should_i_trade(
                engine,
                "nvda",
                account_size_usd=100000,
            )
        assert response.ticker == "NVDA"
