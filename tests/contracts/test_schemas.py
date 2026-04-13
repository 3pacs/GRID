"""BaseContract + concrete schema contracts are frozen, typed, validated."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from contracts.schemas import (
    BaseContract,
    PostmortemCompleted,
    PredictionScored,
    BacktestGateVerdict,
    OptionsTradeOutcome,
    CrossReferenceAnomaly,
    LeverageRiskUpdate,
    RegimeTransition,
    SignalFired,
    HypothesisGenerated,
    ActorMaterialized,
    PullLifecycle,
    ForensicsTrace,
    InvestigationProgress,
    SignalRef,
    ALL_CONTRACTS,
)


CID = uuid4()


def _base_kwargs() -> dict:
    return {"producer_module": "test.producer", "correlation_id": CID}


def test_base_contract_auto_fields():
    class Dummy(BaseContract):
        pass

    c = Dummy(**_base_kwargs())
    assert isinstance(c.event_id, UUID)
    assert isinstance(c.timestamp, datetime)
    assert c.timestamp.tzinfo is not None
    assert c.correlation_id == CID
    assert c.schema_version == 1


def test_base_contract_is_frozen():
    class Dummy(BaseContract):
        pass

    c = Dummy(**_base_kwargs())
    with pytest.raises(ValidationError):
        c.producer_module = "mutated"


def test_base_contract_forbids_extra_fields():
    class Dummy(BaseContract):
        pass

    with pytest.raises(ValidationError):
        Dummy(**_base_kwargs(), unknown_field="x")


def test_postmortem_completed_roundtrip():
    sig = SignalRef(
        signal_id=uuid4(),
        source="congressional",
        trust_at_prediction=0.72,
        weight_at_prediction=0.35,
    )
    c = PostmortemCompleted(
        **_base_kwargs(),
        prediction_id=uuid4(),
        ticker="NVDA",
        verdict="MISS",
        realized_pnl=Decimal("-1250.00"),
        signals_used=[sig],
        root_cause="rate shock",
        contributing_signal_ids=[sig.signal_id],
    )
    assert c.verdict == "MISS"
    assert c.signals_used[0].source == "congressional"


def test_postmortem_verdict_literal_enforced():
    with pytest.raises(ValidationError):
        PostmortemCompleted(
            **_base_kwargs(),
            prediction_id=uuid4(),
            ticker="NVDA",
            verdict="BOGUS",          # not in Literal set
            realized_pnl=Decimal("0"),
            signals_used=[],
            root_cause="x",
            contributing_signal_ids=[],
        )


def test_prediction_scored_required_fields():
    c = PredictionScored(
        **_base_kwargs(),
        prediction_id=uuid4(),
        decision_id=42,
        ticker="SPY",
        verdict="HIT",
        expected_direction="UP",
        realized_direction="UP",
        confidence=0.81,
        brier_component=0.036,
        signals_used=[],
        model_weights_at_prediction={"flow_momentum": 0.4, "regime_contrarian": 0.2},
    )
    assert c.decision_id == 42


def test_backtest_gate_verdict_required_fields():
    c = BacktestGateVerdict(
        **_base_kwargs(),
        hypothesis_id=uuid4(),
        model_version_id=7,
        gate_name="shadow_to_staging",
        verdict="PASS",
        metrics={"sharpe": 1.2, "win_rate": 0.63},
        promotion_target_state="STAGING",
        operator_id="hermes",
    )
    assert c.verdict == "PASS"


def test_options_trade_outcome_required_fields():
    c = OptionsTradeOutcome(
        **_base_kwargs(),
        trade_id=101,
        ticker="TSLA",
        strategy="long_call",
        pnl=Decimal("450.00"),
        signal_mix={"pcr": 0.3, "skew": 0.2, "gamma_wall": 0.5},
        hit_levels={"target": True, "stop": False},
        duration_s=86400,
    )
    assert c.pnl == Decimal("450.00")


def test_cross_reference_anomaly_required_fields():
    c = CrossReferenceAnomaly(
        **_base_kwargs(),
        statistic="retail_sales_yoy",
        official_value=Decimal("3.2"),
        reality_proxy_value=Decimal("0.8"),
        confidence_delta=0.62,
        evidence_links=["https://sat.example/img1"],
        severity="HIGH",
    )
    assert c.severity == "HIGH"


def test_leverage_risk_update_required_fields():
    c = LeverageRiskUpdate(
        **_base_kwargs(),
        system_leverage_index=0.84,
        top_leveraged_actors=["actor:archegos", "actor:ltcm"],
        critical_threshold_breached=True,
        components={"margin_debt": 0.9, "repo": 0.7},
    )
    assert c.critical_threshold_breached is True


def test_regime_transition_required_fields():
    c = RegimeTransition(
        **_base_kwargs(),
        from_state="NEUTRAL",
        to_state="FRAGILE",
        confidence=0.77,
        triggering_features=["vix", "move"],
        transition_probability_matrix=[[0.8, 0.2], [0.3, 0.7]],
    )
    assert c.to_state == "FRAGILE"


def test_signal_fired_required_fields():
    c = SignalFired(
        **_base_kwargs(),
        signal_id=uuid4(),
        source="insider",
        signal_type="cluster_buy",
        strength=0.66,
        ticker="AMD",
        actor_hint="actor:ceo_lisa_su",
        raw_row_ids=[1001, 1002],
    )
    assert c.strength == 0.66


def test_hypothesis_generated_required_fields():
    c = HypothesisGenerated(
        **_base_kwargs(),
        hypothesis_id=uuid4(),
        statement="Dealer gamma flips short below 4500 SPX",
        layer="EXECUTION",
        feature_ids=["gex_spx", "spx_close"],
        lag_structure={"gex_spx": 1},
        predecessor_id=None,
    )
    assert c.layer == "EXECUTION"


def test_actor_materialized_required_fields():
    c = ActorMaterialized(
        **_base_kwargs(),
        actor_id="actor:powell_jerome",
        canonical_name="Jerome H. Powell",
        aliases=["J. Powell", "Chair Powell"],
        wealth_estimate=Decimal("50000000"),
        discovery_source="fed_bio",
        confidence_label="confirmed",
    )
    assert c.canonical_name == "Jerome H. Powell"


def test_pull_lifecycle_required_fields():
    c = PullLifecycle(
        **_base_kwargs(),
        puller_name="fred",
        state="COMPLETED",
        row_count=1240,
        duration_s=12.5,
        error=None,
    )
    assert c.state == "COMPLETED"


def test_pull_lifecycle_state_literal_enforced():
    with pytest.raises(ValidationError):
        PullLifecycle(
            **_base_kwargs(),
            puller_name="fred",
            state="BOGUS",
            row_count=0,
            duration_s=0,
            error=None,
        )


def test_forensics_trace_required_fields():
    c = ForensicsTrace(
        **_base_kwargs(),
        trace_id=uuid4(),
        ticker="SPY",
        window_start=datetime.now(timezone.utc),
        window_end=datetime.now(timezone.utc),
        reconstructed_sequence=[{"t": 0, "event": "block"}],
        suspected_levers=["dealer_gamma_flip"],
    )
    assert c.ticker == "SPY"


def test_investigation_progress_required_fields():
    c = InvestigationProgress(
        **_base_kwargs(),
        board_id=9,
        step=3,
        total_steps=10,
        description="fetching 13F holdings",
        partial_nodes=[],
    )
    assert c.step == 3


def test_all_contracts_registry_is_complete():
    # 13 original contracts + EdgeValidated (SYNTH-C wave, task #100)
    assert len(ALL_CONTRACTS) == 14
    names = {cls.__name__ for cls in ALL_CONTRACTS}
    expected = {
        "PostmortemCompleted", "PredictionScored", "BacktestGateVerdict",
        "OptionsTradeOutcome", "CrossReferenceAnomaly", "LeverageRiskUpdate",
        "RegimeTransition", "SignalFired", "HypothesisGenerated",
        "ActorMaterialized", "PullLifecycle", "ForensicsTrace",
        "InvestigationProgress",
        "EdgeValidated",
    }
    assert names == expected
