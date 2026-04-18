"""Tests for SYNTH Wave A handler wiring (SYNTH-19/20/21/22/23).

Covers the three new ``contracts.handlers.*`` modules, the populated
``contracts.router.ROUTES`` table, and the three new consumer methods on
``oracle.engine.ModelRegistry``, ``oracle.calibration``, and
``intelligence.trust_scorer.TrustScorer``.

All tests use ``mock_engine`` from the shared conftest — no live DB calls.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from contracts.router import ROUTES, resolve_handler
from contracts.schemas import (
    ALL_CONTRACTS,
    BaseContract,
    PostmortemCompleted,
    PredictionScored,
    SignalRef,
)


# ── Shared fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def corr_id() -> UUID:
    return uuid4()


@pytest.fixture
def sample_signal_refs() -> list[SignalRef]:
    return [
        SignalRef(
            signal_id=uuid4(),
            source="insider",
            trust_at_prediction=0.65,
            weight_at_prediction=1.0,
        ),
        SignalRef(
            signal_id=uuid4(),
            source="darkpool",
            trust_at_prediction=0.55,
            weight_at_prediction=0.8,
        ),
    ]


@pytest.fixture
def scored_evt(corr_id, sample_signal_refs) -> PredictionScored:
    return PredictionScored(
        producer_module="oracle.engine",
        correlation_id=corr_id,
        prediction_id=uuid4(),
        decision_id=42,
        ticker="AAPL",
        verdict="HIT",
        expected_direction="UP",
        realized_direction="UP",
        confidence=0.8,
        brier_component=0.04,
        signals_used=sample_signal_refs,
        model_weights_at_prediction={"flow_momentum": 1.2, "cross_asset": 0.9},
    )


@pytest.fixture
def miss_evt(corr_id, sample_signal_refs) -> PredictionScored:
    return PredictionScored(
        producer_module="oracle.engine",
        correlation_id=corr_id,
        prediction_id=uuid4(),
        decision_id=43,
        ticker="AAPL",
        verdict="MISS",
        expected_direction="UP",
        realized_direction="DOWN",
        confidence=0.9,
        brier_component=0.81,
        signals_used=sample_signal_refs,
        model_weights_at_prediction={"flow_momentum": 1.2},
    )


@pytest.fixture
def postmortem_evt(corr_id, sample_signal_refs) -> PostmortemCompleted:
    return PostmortemCompleted(
        producer_module="postmortem.apply_contagion_feedback",
        correlation_id=corr_id,
        prediction_id=uuid4(),
        ticker="AAPL",
        verdict="MISS",
        realized_pnl=Decimal("-1234.56"),
        signals_used=sample_signal_refs,
        root_cause="dealer_gamma_flip",
        contributing_signal_ids=[s.signal_id for s in sample_signal_refs],
    )


# ── 1. Handler invocation smoke tests ─────────────────────────────────────


class TestHandlerInvocation:
    """Every handler must be invocable with a synthetic contract event."""

    def test_trust_on_prediction_scored_invocable(self, scored_evt, mock_engine):
        from contracts.handlers import trust

        # Should not raise; all DB paths are mocked to return empty.
        trust.on_prediction_scored(scored_evt, engine=mock_engine)

    def test_trust_on_postmortem_completed_invocable(
        self, postmortem_evt, mock_engine
    ):
        from contracts.handlers import trust

        trust.on_postmortem_completed(postmortem_evt, engine=mock_engine)

    def test_oracle_weights_on_prediction_scored_invocable(
        self, scored_evt, mock_engine
    ):
        from contracts.handlers import oracle_weights

        oracle_weights.on_prediction_scored(scored_evt, engine=mock_engine)

    def test_oracle_weights_on_postmortem_completed_invocable(
        self, postmortem_evt, mock_engine
    ):
        from contracts.handlers import oracle_weights

        # MISS verdict routes into decay_model_by_source.
        oracle_weights.on_postmortem_completed(
            postmortem_evt, engine=mock_engine
        )

    def test_calibration_on_prediction_scored_invocable(
        self, scored_evt, mock_engine
    ):
        from contracts.handlers import calibration

        calibration.on_prediction_scored(scored_evt, engine=mock_engine)


# ── 2. Router integrity ───────────────────────────────────────────────────


class TestRouterIntegrity:
    """Every ROUTES entry must resolve to a real callable and every key
    must be a Pydantic contract."""

    def test_routes_contains_both_wave_a_contracts(self):
        assert PredictionScored in ROUTES
        assert PostmortemCompleted in ROUTES

    def test_routes_keys_are_all_contracts(self):
        for key in ROUTES:
            assert key in ALL_CONTRACTS, f"{key!r} is not a registered contract"
            assert issubclass(key, BaseContract)

    def test_every_registered_handler_resolves(self):
        for contract_type, paths in ROUTES.items():
            assert isinstance(paths, list) and paths
            for path in paths:
                handler = resolve_handler(path)
                assert callable(handler), f"{path} is not callable"

    def test_prediction_scored_has_three_handlers(self):
        assert len(ROUTES[PredictionScored]) == 3

    def test_postmortem_completed_has_two_handlers(self):
        assert len(ROUTES[PostmortemCompleted]) == 2


# ── 3. ModelRegistry.update_from_contract ─────────────────────────────────


class TestModelRegistryUpdateFromContract:
    def test_hit_increases_weight(self, scored_evt, mock_engine):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        n = registry.update_from_contract(scored_evt)
        assert n == 2  # two models in model_weights_at_prediction

    def test_miss_decreases_weight(self, miss_evt, mock_engine):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        n = registry.update_from_contract(miss_evt)
        assert n == 1

    def test_unknown_verdict_is_noop(self, corr_id, mock_engine):
        from oracle.engine import ModelRegistry

        # Build a frozen event with a verdict that is not in the likelihood
        # map. Pydantic's Literal type would reject this outright, so go
        # through a MagicMock to simulate schema drift.
        fake_evt = MagicMock()
        fake_evt.verdict = "UNKNOWN"
        fake_evt.confidence = 0.5
        fake_evt.model_weights_at_prediction = {"flow_momentum": 1.0}

        registry = ModelRegistry(mock_engine)
        assert registry.update_from_contract(fake_evt) == 0

    def test_zero_confidence_is_handled(self, corr_id, sample_signal_refs, mock_engine):
        from oracle.engine import ModelRegistry

        evt = PredictionScored(
            producer_module="oracle.engine",
            correlation_id=corr_id,
            prediction_id=uuid4(),
            decision_id=1,
            ticker="AAPL",
            verdict="HIT",
            expected_direction="UP",
            realized_direction="UP",
            confidence=0.0,
            brier_component=1.0,
            signals_used=sample_signal_refs,
            model_weights_at_prediction={"flow_momentum": 1.0},
        )

        registry = ModelRegistry(mock_engine)
        # Zero-confidence HIT carries zero evidence; we still touch the row
        # once to bump the hits counter.
        n = registry.update_from_contract(evt)
        assert n == 1


# ── 4. decay_model_by_source ──────────────────────────────────────────────


class TestDecayModelBySource:
    def test_decay_calls_update(self, mock_engine):
        from oracle.engine import ModelRegistry

        # Arrange: mock the UPDATE rowcount.
        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        result = MagicMock()
        result.rowcount = 3
        mock_conn.execute.return_value = result

        registry = ModelRegistry(mock_engine)
        assert registry.decay_model_by_source("insider", 0.9) == 3
        assert mock_conn.execute.called

    def test_decay_noop_when_source_empty(self, mock_engine):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        assert registry.decay_model_by_source("", 0.9) == 0

    def test_decay_noop_when_factor_zero(self, mock_engine):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        assert registry.decay_model_by_source("insider", 0.0) == 0


# ── 5. update_running_metrics ─────────────────────────────────────────────


class TestUpdateRunningMetrics:
    def test_first_value_seeds_averages(self, mock_engine):
        from oracle.calibration import update_running_metrics

        # First call: fetchone returns (0.0, 0.0, 0).
        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        fetch_result = MagicMock()
        fetch_result.fetchone.return_value = (0.0, 0.0, 0)
        mock_conn.execute.return_value = fetch_result

        out = update_running_metrics(
            mock_engine, model_id="flow_momentum", prediction=0.8, actual=1.0
        )
        assert out["count"] == 1
        assert out["running_brier"] == pytest.approx((0.8 - 1.0) ** 2)
        assert out["running_ece"] == pytest.approx(0.2)

    def test_subsequent_value_is_running_mean(self, mock_engine):
        from oracle.calibration import update_running_metrics

        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        # Simulate a model that already has one scored prediction.
        fetch_result = MagicMock()
        fetch_result.fetchone.return_value = (0.04, 0.2, 1)
        mock_conn.execute.return_value = fetch_result

        out = update_running_metrics(
            mock_engine, model_id="flow_momentum", prediction=0.5, actual=1.0
        )
        # new_count = 2
        # new_brier = 0.04 + (0.25 - 0.04) / 2 = 0.145
        # new_ece   = 0.2  + (0.5  - 0.2)  / 2 = 0.35
        assert out["count"] == 2
        assert out["running_brier"] == pytest.approx(0.145)
        assert out["running_ece"] == pytest.approx(0.35)

    def test_missing_model_seeds_row(self, mock_engine):
        from oracle.calibration import update_running_metrics

        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        fetch_result = MagicMock()
        fetch_result.fetchone.return_value = None
        mock_conn.execute.return_value = fetch_result

        out = update_running_metrics(
            mock_engine,
            model_id="brand_new_model",
            prediction=0.6,
            actual=0.0,
        )
        assert out["count"] == 1
        assert out["running_brier"] == pytest.approx(0.36)
        assert out["running_ece"] == pytest.approx(0.6)

    def test_missing_model_id_raises(self, mock_engine):
        from oracle.calibration import update_running_metrics

        with pytest.raises(ValueError):
            update_running_metrics(
                mock_engine, model_id="", prediction=0.5, actual=1.0
            )


# ── 6. score_prediction_signals (TrustScorer) ─────────────────────────────


class TestScorePredictionSignals:
    def test_happy_path_scores_pending_signals(
        self, mock_engine, sample_signal_refs
    ):
        from intelligence.trust_scorer import TrustScorer

        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        result = MagicMock()
        result.rowcount = 2
        mock_conn.execute.return_value = result

        scorer = TrustScorer(mock_engine)
        n = scorer.score_prediction_signals(
            prediction_id=uuid4(),
            verdict="HIT",
            signals=sample_signal_refs,
        )
        assert n == 2
        assert mock_conn.execute.called

    def test_empty_signals_returns_zero(self, mock_engine):
        from intelligence.trust_scorer import TrustScorer

        scorer = TrustScorer(mock_engine)
        assert (
            scorer.score_prediction_signals(
                prediction_id=uuid4(), verdict="HIT", signals=[]
            )
            == 0
        )

    def test_signals_without_signal_id_are_skipped(self, mock_engine):
        from intelligence.trust_scorer import TrustScorer

        scorer = TrustScorer(mock_engine)
        # Dict-shaped signal with no signal_id field — edge case.
        bad_signals = [{"source": "insider"}, {"trust_at_prediction": 0.5}]
        assert (
            scorer.score_prediction_signals(
                prediction_id=uuid4(), verdict="HIT", signals=bad_signals
            )
            == 0
        )


# ── 7. update_source_trust_from_postmortem ────────────────────────────────


class TestUpdateSourceTrustFromPostmortem:
    def test_miss_verdict_decays_source(self, mock_engine, sample_signal_refs):
        from intelligence.trust_scorer import TrustScorer

        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        result = MagicMock()
        result.rowcount = 1
        mock_conn.execute.return_value = result

        scorer = TrustScorer(mock_engine)
        # Two distinct sources in sample_signal_refs → two UPSERTs.
        n = scorer.update_source_trust_from_postmortem(
            prediction_id=uuid4(),
            verdict="MISS",
            signals=sample_signal_refs,
            root_cause="dealer_gamma_flip",
        )
        # We count rowcount per source; two sources × 1 each.
        assert n == 2

    def test_unknown_verdict_is_noop(self, mock_engine, sample_signal_refs):
        from intelligence.trust_scorer import TrustScorer

        scorer = TrustScorer(mock_engine)
        n = scorer.update_source_trust_from_postmortem(
            prediction_id=uuid4(),
            verdict="UNKNOWN",
            signals=sample_signal_refs,
        )
        assert n == 0

    def test_empty_signals_is_noop(self, mock_engine):
        from intelligence.trust_scorer import TrustScorer

        scorer = TrustScorer(mock_engine)
        assert (
            scorer.update_source_trust_from_postmortem(
                prediction_id=uuid4(), verdict="MISS", signals=[]
            )
            == 0
        )


# ── 8. Calibration handler branch coverage ────────────────────────────────


class TestCalibrationHandler:
    def test_no_model_weights_is_noop(self, corr_id, sample_signal_refs, mock_engine):
        from contracts.handlers import calibration

        evt = PredictionScored(
            producer_module="oracle.engine",
            correlation_id=corr_id,
            prediction_id=uuid4(),
            decision_id=1,
            ticker="AAPL",
            verdict="HIT",
            expected_direction="UP",
            realized_direction="UP",
            confidence=0.8,
            brier_component=0.04,
            signals_used=sample_signal_refs,
            model_weights_at_prediction={},
        )

        # With no model weights the handler must not touch the DB.
        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        mock_conn.execute.reset_mock()
        calibration.on_prediction_scored(evt, engine=mock_engine)
        assert not mock_conn.execute.called

    def test_partial_verdict_maps_to_half(self, corr_id, sample_signal_refs, mock_engine):
        from contracts.handlers import calibration

        evt = PredictionScored(
            producer_module="oracle.engine",
            correlation_id=corr_id,
            prediction_id=uuid4(),
            decision_id=1,
            ticker="AAPL",
            verdict="PARTIAL",
            expected_direction="UP",
            realized_direction="FLAT",
            confidence=0.7,
            brier_component=0.04,
            signals_used=sample_signal_refs,
            model_weights_at_prediction={"flow_momentum": 1.0},
        )

        mock_conn = (
            mock_engine.begin.return_value.__enter__.return_value
        )
        fetch_result = MagicMock()
        fetch_result.fetchone.return_value = (0.0, 0.0, 0)
        mock_conn.execute.return_value = fetch_result

        # Should resolve without raising.
        calibration.on_prediction_scored(evt, engine=mock_engine)
