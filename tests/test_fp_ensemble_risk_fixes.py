"""Regression tests for verified ensemble predictor, uncertainty, risk gate, and
signal-registry flag defects.

Covers:
  1. Direction-blind ensemble confidence & disagreement shrinking toward 0.5.
  2. Track-record triple-counting in vote_weight & Beta-prior hit-rate shrinkage.
  3. Direction-blind FCI confidence multiplier.
  4. Hardcoded +-3% target_move, neutral calling options_implied_probability,
     and missing horizon / realized-vol scaling.
  5. Unsigned ensemble CI and zero-width CI for small/uniform ensembles.
  6. CircuitBreaker exposure using confidence as price and fail-open ticket gate.
  7. GRID_SIGNAL_REGISTRY default mismatch between engine.py and model_factory.py.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from intelligence.signal_provenance import (
    CausationChain,
    SignalEvidence,
    TradeProvenanceReport,
)
from oracle.risk import (
    CircuitBreaker,
    CircuitBreakerConfig,
    get_global_circuit_breaker,
    reset_global_circuit_breaker,
)
from oracle.uncertainty import compute_confidence_interval
from store.observations import Observation
from trading.trade_ticket_generator import generate_ticket, kelly_size_from_report


@pytest.fixture(autouse=True)
def _admit_models_and_reset_breaker(monkeypatch):
    monkeypatch.setattr(
        "oracle.model_factory.ModelFactory.active_model_names",
        lambda conn, names, **kw: set(names),
    )
    # Prevent contrast_distillation from attempting LLM/DB calls on high-divergence mocks
    monkeypatch.setattr(
        "oracle.contrast_distillation.compute_divergence",
        lambda rollouts: 0.0,
    )
    reset_global_circuit_breaker()
    yield
    reset_global_circuit_breaker()


def _make_predictor(
    vote_specs: list[tuple[str, float]],
    *,
    hit_rates: dict[str, float] | None = None,
    bucket_weights: dict[str, float] | None = None,
):
    """Build an EnsemblePredictor with synthetic model heads (no DB)."""
    from oracle.engine import EnsemblePredictor

    predictor = EnsemblePredictor.__new__(EnsemblePredictor)
    predictor.engine = MagicMock()
    predictor.factory = MagicMock()
    predictor.aggregator = MagicMock()

    models = []
    aggs = []
    for i, (direction, conf) in enumerate(vote_specs):
        name = f"model_{i}"
        m = SimpleNamespace(name=name, min_signals=1, weight_config={})
        models.append(m)
        aggs.append(
            SimpleNamespace(
                direction=direction,
                strength=0.8,
                confidence=conf,
                coherence=0.9,
                signal_count=3,
            )
        )

    predictor.factory.list_active_models.return_value = models
    predictor.factory.get_signals_for_model.return_value = [object()] * 3
    predictor.aggregator.aggregate.side_effect = aggs

    hr_map = hit_rates or {}
    bw_map = bucket_weights or {}
    predictor._get_hit_rate = lambda name, *, horizon=None: hr_map.get(name, 0.6)
    predictor._get_bucket_weight = lambda name, *, horizon=None: bw_map.get(name, 1.0)
    return predictor


def _neutral_context_patches(fci_score: float = 0.0):
    """Context manager stack neutralizing unrelated SWEEP modifiers."""
    fci_stub = SimpleNamespace(score=fci_score, regime="NEUTRAL")
    shapley_stub = SimpleNamespace(
        fragility_multiplier=1.0, top_contributor="", top_share=0.0
    )
    crowd_stub = SimpleNamespace(score=0.0, crowd_direction=None)
    penalty_stub = SimpleNamespace(multiplier=1.0, aligned=False)
    return (
        patch(
            "intelligence.catalyst_aggregator.proximity_score",
            return_value={"score": 0.0, "catalyst_type": None},
        ),
        patch(
            "intelligence.liquidity_regime.classify_current_regime",
            return_value=None,
        ),
        patch(
            "oracle.regime_router.RegimeRouter.model_regime_weight",
            return_value=1.0,
        ),
        patch(
            "intelligence.financial_conditions_index.compute_fci",
            return_value=fci_stub,
        ),
        patch(
            "intelligence.shapley_attribution.attribute_votes",
            return_value=shapley_stub,
        ),
        patch(
            "intelligence.consensus_crowdedness.compute_crowdedness",
            return_value=crowd_stub,
        ),
        patch(
            "intelligence.consensus_crowdedness.compute_penalty",
            return_value=penalty_stub,
        ),
    )


def _make_report(
    *,
    direction: str = "bullish",
    confidence: float = 0.78,
    confidence_lower: float = 0.70,
    confidence_upper: float = 0.86,
) -> TradeProvenanceReport:
    return TradeProvenanceReport(
        ticker="AAPL",
        generated_at="2026-11-21T00:00:00+00:00",
        direction=direction,
        score=72,
        confidence=confidence,
        confidence_lower=confidence_lower,
        confidence_upper=confidence_upper,
        horizon_days=7,
        regime="EXPANSION",
        fci_regime="EASY",
        signal_evidence=[
            SignalEvidence(
                signal_source="sig_0",
                shapley_weight=0.5,
                scorecard=None,
                classification="strong",
            )
        ],
        top_shapley_contributor="sig_0",
        top_shapley_share=0.5,
        fragility_multiplier=1.0,
        disagreement_score=0.1,
        crowd_aligned=False,
        market_implied_prob=0.55,
        red_team_epistemic_risk=0.1,
        shipping_fudge_alerts=[],
        causation=CausationChain(
            lever="Fed-cut", flow_direction="open", actor="Fed", complete=True
        ),
        cooccurrence_lift=1.0,
        regime_calibrated_signal_count=0,
        confidence_bucket_multiplier=1.0,
        scenario_multiplier=1.0,
        null_hypothesis_penalty=1.0,
        meta_learning_multiplier=1.0,
        contra_indicator_multiplier=1.0,
        squeeze_multiplier=1.0,
        arbitrage_multiplier=1.0,
        convergence_multiplier=1.0,
        money_flow_multiplier=1.0,
        aggregate_conviction=1.30,
        verdict="high",
    )


# ── Defect 1: Direction-blind confidence & disagreement toward 0.5 ───────────


class TestDefect1SignedProbabilityConfidence:
    def test_5_bulls_4_bears_at_09_gives_low_confidence_and_zero_kelly(self):
        votes = [("bullish", 0.9)] * 5 + [("bearish", 0.9)] * 4
        predictor = _make_predictor(votes)
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches()
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            pred = predictor.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        # Signed mean p_up = (5*0.9 + 4*0.1) / 9 = 0.5444; disagreement shrinks
        # (confidence - 0.5) toward 0.5 -> ~0.537, never 0.754.
        assert pred.direction == "bullish"
        assert 0.5 <= pred.confidence < 0.55
        # Lower bound from signed p_up is well below 2:1 Kelly break-even (1/3).
        assert pred.confidence_lower < 0.34
        report = _make_report(
            confidence=pred.confidence,
            confidence_lower=pred.confidence_lower,
            confidence_upper=pred.confidence_upper,
        )
        kelly_pct, _ = kelly_size_from_report(report, 100_000.0)
        assert kelly_pct == 0.0

    def test_exact_split_is_neutral_at_half(self):
        votes = [("bullish", 0.8)] * 3 + [("bearish", 0.8)] * 3
        predictor = _make_predictor(votes)
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches()
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            pred = predictor.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        assert pred.direction == "neutral"
        assert pred.confidence == pytest.approx(0.5, abs=1e-4)

    def test_disagreement_shrinks_toward_half_not_zero(self):
        # 3 bulls at 0.7 (p_up=0.7) + 1 bear at 0.7 (p_up=0.3) -> p_up = 0.60
        votes = [("bullish", 0.7)] * 3 + [("bearish", 0.7)]
        predictor = _make_predictor(votes)
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches()
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            pred = predictor.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        expected = 0.5 + (0.6 - 0.5) * (1.0 - 0.4 * pred.disagreement_score)
        assert pred.direction == "bullish"
        assert pred.disagreement_score > 0.0
        assert pred.confidence == pytest.approx(expected, abs=1e-3)
        assert pred.confidence > 0.5


# ── Defect 2: No triple-counting in vote_weight + Beta-shrunk hit_rate ───────


class TestDefect2VoteWeightSingleCount:
    def test_vote_weight_ignores_bucket_and_regime_weight_multipliers(self):
        predictor = _make_predictor(
            [("bullish", 0.8), ("bullish", 0.8)],
            hit_rates={"model_0": 0.6, "model_1": 0.6},
            bucket_weights={"model_0": 3.0, "model_1": 0.2},
        )
        p1, p2, _, p4, p5, p6, p7 = _neutral_context_patches()
        with p1, p2, p4, p5, p6, p7, patch(
            "oracle.regime_router.RegimeRouter.model_regime_weight",
            side_effect=lambda name, regime: 2.5 if name == "model_0" else 0.3,
        ), patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            pred = predictor.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        weights = {v["model_name"]: v["vote_weight"] for v in pred.model_votes}
        assert weights["model_0"] == pytest.approx(0.6, abs=1e-4)
        assert weights["model_1"] == pytest.approx(0.6, abs=1e-4)

    def test_get_hit_rate_applies_beta_prior_shrinkage(self):
        from oracle.engine import EnsemblePredictor

        predictor = EnsemblePredictor.__new__(EnsemblePredictor)
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        predictor.engine = engine

        # Horizon bucket with 5 hits, 0 misses -> (5 + 5) / (5 + 10) = 2/3, not 1.0
        res_perfect = MagicMock()
        res_perfect.fetchone.return_value = (
            {"7d": {"hits": 5, "misses": 0, "partials": 0}},
        )
        conn.execute.return_value = res_perfect
        hr_perfect = predictor._get_hit_rate("m1", horizon=7)
        assert hr_perfect == pytest.approx(10.0 / 15.0, abs=1e-4)

        # Horizon bucket with 0 hits, 5 misses -> (0 + 5) / (5 + 10) = 1/3, not 0.0
        res_zero = MagicMock()
        res_zero.fetchone.return_value = (
            {"7d": {"hits": 0, "misses": 5, "partials": 0}},
        )
        conn.execute.return_value = res_zero
        hr_zero = predictor._get_hit_rate("m1", horizon=7)
        assert hr_zero == pytest.approx(5.0 / 15.0, abs=1e-4)


# ── Defect 3: Signed FCI multiplier ──────────────────────────────────────────


class TestDefect3SignedFCI:
    def test_loose_fci_raises_bullish_and_lowers_bearish(self):
        bull_pred = _make_predictor([("bullish", 0.7), ("bullish", 0.7)])
        bear_pred = _make_predictor([("bearish", 0.7), ("bearish", 0.7)])
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches(fci_score=2.0)
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            r_bull = bull_pred.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )
            r_bear = bear_pred.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        assert r_bull.confidence == pytest.approx(0.7 * 1.10, abs=1e-3)
        assert r_bear.confidence == pytest.approx(0.7 * 0.90, abs=1e-3)

    def test_tight_fci_lowers_bullish_and_raises_bearish(self):
        bull_pred = _make_predictor([("bullish", 0.7), ("bullish", 0.7)])
        bear_pred = _make_predictor([("bearish", 0.7), ("bearish", 0.7)])
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches(fci_score=-2.0)
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            r_bull = bull_pred.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )
            r_bear = bear_pred.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        assert r_bull.confidence == pytest.approx(0.7 * 0.90, abs=1e-3)
        assert r_bear.confidence == pytest.approx(0.7 * 1.10, abs=1e-3)

    def test_fci_does_not_distort_neutral(self):
        neut_pred = _make_predictor([("bullish", 0.8), ("bearish", 0.8)])
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches(fci_score=2.5)
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "intelligence.market_implied_prob.options_implied_probability",
            return_value=None,
        ):
            r_neut = neut_pred.predict(
                "AAPL", as_of=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

        assert r_neut.direction == "neutral"
        assert r_neut.confidence == pytest.approx(0.5, abs=1e-4)


# ── Defect 4: Horizon-scaled realized-vol target_move & neutral abstention ───


def _synthetic_price_observations(n_days: int = 25, step_log_ret: float = 0.015):
    """Alternating +-step_log_ret closes with known sample daily std."""
    start = date(2026, 10, 15)
    obs = []
    price = 100.0
    for i in range(n_days):
        if i > 0:
            r = step_log_ret if i % 2 == 1 else -step_log_ret
            price = price * math.exp(r)
        d = start + timedelta(days=i)
        obs.append(
            Observation(
                series_id="YF:AAPL:close",
                obs_date=d,
                value=price,
                pull_timestamp=datetime(
                    d.year, d.month, d.day, 21, 0, tzinfo=timezone.utc
                ),
                source="yfinance",
            )
        )
    return obs


class TestDefect4HorizonScaledTargetMove:
    def test_neutral_does_not_call_options_implied_probability(self):
        predictor = _make_predictor([("bullish", 0.8), ("bearish", 0.8)])
        obs = _synthetic_price_observations()
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches()
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "store.observations.read_window", return_value=obs
        ), patch(
            "store.observations.read_splits", return_value=[]
        ), patch(
            "intelligence.market_implied_prob.options_implied_probability"
        ) as mock_opt:
            pred = predictor.predict(
                "AAPL",
                as_of=datetime(2026, 11, 21, tzinfo=timezone.utc),
                horizon=7,
            )

        assert pred.direction == "neutral"
        mock_opt.assert_not_called()
        assert pred.market_implied_prob == 0.0
        assert pred.market_divergence_severity == ""

    def test_target_move_scales_with_realized_vol_and_sqrt_horizon(self):
        obs = _synthetic_price_observations(n_days=25, step_log_ret=0.015)
        log_rets = [
            math.log(obs[i].value / obs[i - 1].value) for i in range(1, len(obs))
        ]
        mean_r = sum(log_rets) / len(log_rets)
        expected_sigma = math.sqrt(
            sum((r - mean_r) ** 2 for r in log_rets) / (len(log_rets) - 1)
        )

        as_of = datetime(2026, 11, 21, 15, 0, tzinfo=timezone.utc)
        seen_moves: list[float] = []

        def _capture_opt(engine, ticker, *, target_move_pct, horizon_days):
            seen_moves.append(target_move_pct)
            return None

        for direction, horizon in [("bullish", 7), ("bullish", 28), ("bearish", 7)]:
            predictor = _make_predictor([(direction, 0.8), (direction, 0.8)])
            p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches()
            with p1, p2, p3, p4, p5, p6, p7, patch(
                "store.observations.read_window", return_value=obs
            ) as mock_rw, patch(
                "store.observations.read_splits", return_value=[]
            ), patch(
                "intelligence.market_implied_prob.options_implied_probability",
                side_effect=_capture_opt,
            ):
                predictor.predict("AAPL", as_of=as_of, horizon=horizon)
                # Must pass PIT bounds to store.observations.read_window
                assert mock_rw.called
                rw_kwargs = mock_rw.call_args.kwargs
                assert rw_kwargs["as_of"] == as_of.date()
                assert rw_kwargs["as_of_ts"] == as_of

        move_bull_7, move_bull_28, move_bear_7 = seen_moves
        assert move_bull_7 > 0
        assert move_bear_7 < 0
        assert move_bear_7 == pytest.approx(-move_bull_7, rel=1e-6)
        # sqrt(28) / sqrt(7) == 2.0
        assert move_bull_28 == pytest.approx(2.0 * move_bull_7, rel=1e-6)
        assert move_bull_7 == pytest.approx(
            expected_sigma * math.sqrt(7), rel=1e-3
        )

    def test_missing_realized_vol_abstains_instead_of_hardcoded_3pct(self):
        predictor = _make_predictor([("bullish", 0.8), ("bullish", 0.8)])
        p1, p2, p3, p4, p5, p6, p7 = _neutral_context_patches()
        with p1, p2, p3, p4, p5, p6, p7, patch(
            "store.observations.read_window", return_value=[]
        ), patch(
            "intelligence.market_implied_prob.options_implied_probability"
        ) as mock_opt:
            pred = predictor.predict(
                "AAPL",
                as_of=datetime(2026, 11, 21, tzinfo=timezone.utc),
                horizon=7,
            )

        mock_opt.assert_not_called()
        assert pred.market_implied_prob == 0.0
        assert pred.market_divergence_severity == ""


# ── Defect 5: Signed CI and non-zero Wilson floor in oracle/uncertainty.py ───


class TestDefect5UncertaintySignedCIAndFloor:
    def test_three_bulls_vs_three_bears_is_not_zero_width(self):
        votes = (
            [{"direction": "bullish", "confidence": 0.8}] * 3
            + [{"direction": "bearish", "confidence": 0.8}] * 3
        )
        ci = compute_confidence_interval(votes, point_confidence=0.669, alpha=0.10)
        assert ci.width > 0.3
        assert ci.lower < 0.5
        assert ci.sem > 0.0

    def test_singleton_has_wide_wilson_interval_and_nonzero_sem(self):
        votes = [{"direction": "bullish", "confidence": 0.8}]
        ci = compute_confidence_interval(votes, point_confidence=0.8, alpha=0.10)
        assert ci.width > 0.5
        assert ci.lower < 0.35
        assert ci.lower <= 0.8 <= ci.upper
        assert ci.sem > 0.0

    def test_uniform_unanimous_never_zero_width(self):
        votes = [{"direction": "bullish", "confidence": 0.8}] * 6
        ci = compute_confidence_interval(votes, point_confidence=0.8, alpha=0.10)
        assert ci.width > 0.1
        assert 0.0 <= ci.lower < 0.8 < ci.upper <= 1.0

    def test_bearish_and_bullish_intervals_are_symmetric(self):
        bulls = [{"direction": "bullish", "confidence": 0.8}] * 6
        bears = [{"direction": "bearish", "confidence": 0.8}] * 6
        ci_bull = compute_confidence_interval(bulls, point_confidence=0.8, alpha=0.10)
        ci_bear = compute_confidence_interval(bears, point_confidence=0.8, alpha=0.10)
        assert ci_bull.width > 0.1
        assert ci_bull.lower == pytest.approx(ci_bear.lower, abs=1e-6)
        assert ci_bull.upper == pytest.approx(ci_bear.upper, abs=1e-6)


# ── Defect 6: CircuitBreaker exposure price & fail-closed ticket gate ────────


class TestDefect6RiskGatePriceAndFailClosed:
    def test_check_recommendation_uses_actual_current_price_not_confidence(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(max_total_exposure=1000.0))
        # 20 shares * $100 = $2000 > $1000 -> MUST block even when confidence=0.1
        res = breaker.check_recommendation(
            regime="EXPANSION",
            confidence=0.1,
            recommended_action="BUY",
            position_size=20.0,
            current_price=100.0,
        )
        assert res.passed is False
        assert "exposure" in res.reason.lower()

    def test_check_recommendation_without_price_uses_position_size_directly(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(max_total_exposure=1000.0))
        # No current_price -> notional exposure = position_size = 2000 > 1000
        res = breaker.check_recommendation(
            regime="EXPANSION",
            confidence=0.1,
            recommended_action="BUY",
            position_size=2000.0,
        )
        assert res.passed is False
        assert "exposure" in res.reason.lower()

    def test_generate_ticket_blocks_when_notional_exceeds_exposure_limit(self):
        # Default max_total_exposure = $50,000. 5% of $1.2M = $60,000 > $50,000.
        # Previously confidence=0.70 multiplied exposure down to $42,000 and passed!
        report = _make_report(confidence=0.70, confidence_lower=0.70)
        ticket = generate_ticket(
            report,
            account_size_usd=1_200_000.0,
            current_price=100.0,
            vol_30d=0.30,
        )
        assert ticket is None

    def test_breaker_internal_error_fails_closed_with_risk_check_error(self):
        breaker = CircuitBreaker(CircuitBreakerConfig())
        with patch.object(
            breaker._risk_mgr,
            "check_order",
            side_effect=RuntimeError("corrupted risk state"),
        ):
            res = breaker.check_recommendation(
                regime="EXPANSION",
                confidence=0.8,
                recommended_action="BUY",
                position_size=10.0,
                current_price=100.0,
            )
        assert res.passed is False
        assert "risk_check_error" in res.reason

    def test_generate_ticket_fails_closed_on_risk_check_exception(self):
        report = _make_report()
        with patch.object(
            CircuitBreaker,
            "check_recommendation",
            side_effect=RuntimeError("risk module exploded"),
        ):
            ticket = generate_ticket(
                report,
                account_size_usd=100_000.0,
                current_price=250.0,
                vol_30d=0.30,
            )
        assert ticket is None

    def test_generate_ticket_fails_closed_on_risk_import_error(self):
        report = _make_report()
        with patch(
            "oracle.risk.get_global_circuit_breaker",
            side_effect=ImportError("oracle.risk unavailable"),
        ):
            ticket = generate_ticket(
                report,
                account_size_usd=100_000.0,
                current_price=250.0,
                vol_30d=0.30,
            )
        assert ticket is None


# ── Defect 7: Shared GRID_SIGNAL_REGISTRY helper defaulting to "0" ───────────


class TestDefect7SignalRegistryFlagUnified:
    def test_unset_flag_defaults_disabled_and_shared_helper(self, monkeypatch):
        import oracle.engine as oe
        import oracle.model_factory as mf

        monkeypatch.delenv("GRID_SIGNAL_REGISTRY", raising=False)
        assert hasattr(mf, "signal_registry_enabled")
        assert hasattr(oe, "_signal_registry_enabled")
        assert oe._signal_registry_enabled is mf.signal_registry_enabled
        assert mf.signal_registry_enabled() is False
        assert mf._signal_registry_enabled() is False

    def test_explicit_flag_values_match_between_modules(self, monkeypatch):
        import oracle.engine as oe
        import oracle.model_factory as mf

        monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
        assert mf.signal_registry_enabled() is True
        assert oe._signal_registry_enabled() is True

        for val in ("0", "", "true", "yes"):
            monkeypatch.setenv("GRID_SIGNAL_REGISTRY", val)
            assert mf.signal_registry_enabled() is False
            assert oe._signal_registry_enabled() is False
