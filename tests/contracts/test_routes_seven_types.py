"""End-to-end smoke tests for the canonical §7.3 ROUTES wiring.

Each test fires a synthetic contract through the FakeBus → Dispatcher and
asserts that every handler path resolves and is invoked exactly once per
event. Handlers themselves are stubbed via monkeypatch so we test the
*routing*, not the business logic of each sink.

This is the SYNTH-45 final-seal coverage: if any handler path drifts or
gets renamed, this suite breaks at test time instead of at dispatch time.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable
from uuid import uuid4

import pytest

from contracts.channels import channel_for
from contracts.dispatcher import Dispatcher
from contracts.router import ROUTES
from contracts.schemas import (
    CrossReferenceAnomaly,
    EdgeValidated,
    OptionsTradeOutcome,
    PostmortemCompleted,
    PredictionScored,
    RegimeTransition,
    SignalFired,
)


# The 7 contract types §7.3 says must be routed (PullLifecycle is the
# 8th, retained operationally but verified separately in existing tests).
SEVEN_CONTRACT_TYPES = (
    PredictionScored,
    PostmortemCompleted,
    OptionsTradeOutcome,
    SignalFired,
    CrossReferenceAnomaly,
    EdgeValidated,
    RegimeTransition,
)


def _stub_all_handlers(monkeypatch) -> dict[str, list]:
    """Replace every handler in ROUTES with a recording stub.

    Returns a dict mapping handler dotted-path → list of received contracts,
    so the test can assert per-handler invocation count.
    """
    received: dict[str, list] = {}
    import importlib

    for contract_type, handler_paths in ROUTES.items():
        for path in handler_paths:
            module_path, _, attr = path.rpartition(".")
            module = importlib.import_module(module_path)
            received[path] = []

            def _make_stub(p: str) -> Callable:
                def _stub(evt, *, engine=None):
                    received[p].append(evt)
                return _stub

            monkeypatch.setattr(module, attr, _make_stub(path))
    return received


def _make_prediction_scored() -> PredictionScored:
    return PredictionScored(
        producer_module="test.oracle",
        correlation_id=uuid4(),
        prediction_id=uuid4(),
        decision_id=1,
        ticker="AAPL",
        verdict="HIT",
        expected_direction="UP",
        realized_direction="UP",
        confidence=0.75,
        brier_component=0.0625,
        signals_used=[],
        model_weights_at_prediction={"m1": 0.6, "m2": 0.4},
        horizon=7,
    )


def _make_postmortem_completed() -> PostmortemCompleted:
    return PostmortemCompleted(
        producer_module="test.postmortem",
        correlation_id=uuid4(),
        prediction_id=uuid4(),
        ticker="AAPL",
        verdict="MISS",
        realized_pnl=Decimal("-100.0"),
        signals_used=[],
        root_cause="trade_loss",
        contributing_signal_ids=[],
    )


def _make_options_trade_outcome() -> OptionsTradeOutcome:
    return OptionsTradeOutcome(
        producer_module="test.trading",
        correlation_id=uuid4(),
        trade_id=42,
        ticker="TSLA",
        strategy="contagion_v1",
        pnl=Decimal("150.50"),
        signal_mix={"contagion": 0.7, "options": 0.3},
        hit_levels={"entry": True, "target": True, "stop": False},
        duration_s=86400,
    )


def _make_signal_fired() -> SignalFired:
    return SignalFired(
        producer_module="test.detector",
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source="holder_overlap",
        signal_type="BUY",
        strength=0.82,
        ticker="NVDA",
        actor_hint="ICAHN",
        raw_row_ids=[101, 102],
    )


def _make_cross_reference_anomaly() -> CrossReferenceAnomaly:
    return CrossReferenceAnomaly(
        producer_module="test.cross_lens",
        correlation_id=uuid4(),
        statistic="supply_shock",
        official_value=Decimal("100.0"),
        reality_proxy_value=Decimal("125.0"),
        confidence_delta=0.25,
        evidence_links=["edgar:1234", "gdelt:5678"],
        severity="HIGH",
    )


def _make_edge_validated() -> EdgeValidated:
    return EdgeValidated(
        producer_module="test.edge_validator",
        correlation_id=uuid4(),
        edge_id=99,
        upstream_id="TSMC",
        downstream_id="AAPL",
        relationship="supplier",
        validation_correlation=0.05,
        weak_since=datetime.now(timezone.utc),
        relationship_weak=True,
        implied_pct_cogs=None,
    )


def _make_regime_transition() -> RegimeTransition:
    return RegimeTransition(
        producer_module="test.regime",
        correlation_id=uuid4(),
        from_state="NEUTRAL",
        to_state="CRISIS",
        confidence=0.91,
        triggering_features=["vix", "credit_spread"],
        transition_probability_matrix=[[0.5, 0.5], [0.5, 0.5]],
    )


CONTRACT_FACTORIES = {
    PredictionScored: _make_prediction_scored,
    PostmortemCompleted: _make_postmortem_completed,
    OptionsTradeOutcome: _make_options_trade_outcome,
    SignalFired: _make_signal_fired,
    CrossReferenceAnomaly: _make_cross_reference_anomaly,
    EdgeValidated: _make_edge_validated,
    RegimeTransition: _make_regime_transition,
}


def test_seven_contract_types_are_in_routes():
    """Every §7.3 contract type must be a key in ROUTES."""
    routed = set(ROUTES.keys())
    for cls in SEVEN_CONTRACT_TYPES:
        assert cls in routed, (
            f"{cls.__name__} missing from ROUTES (§7.3 violation)"
        )


def test_every_handler_in_routes_is_importable():
    """Re-runs the integrity test from test_router_integrity over the new
    handler paths. Lives here too because this file is the §7.3 contract."""
    from contracts.router import resolve_handler

    for contract_type, handler_paths in ROUTES.items():
        assert handler_paths, f"{contract_type.__name__} has no handlers"
        for path in handler_paths:
            handler = resolve_handler(path)
            assert callable(handler), f"{path} is not callable"


@pytest.mark.parametrize("contract_type", SEVEN_CONTRACT_TYPES,
                         ids=[c.__name__ for c in SEVEN_CONTRACT_TYPES])
def test_dispatcher_routes_each_type_to_every_handler(
    contract_type, fake_bus, monkeypatch
):
    """Fire one synthetic contract per type and assert every handler in
    ``ROUTES[contract_type]`` receives it exactly once."""
    received = _stub_all_handlers(monkeypatch)

    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    factory = CONTRACT_FACTORIES[contract_type]
    contract = factory()
    fake_bus.emit_sync(
        channel_for(contract_type),
        contract.model_dump(mode="json"),
    )
    d.wait_idle()

    expected_handlers = ROUTES[contract_type]
    for path in expected_handlers:
        assert len(received[path]) == 1, (
            f"{path} received {len(received[path])} events, expected 1 "
            f"for {contract_type.__name__}"
        )
        delivered = received[path][0]
        assert isinstance(delivered, contract_type), (
            f"{path} got {type(delivered).__name__}, expected "
            f"{contract_type.__name__}"
        )

    # Sanity — no spillover into other contracts' handlers.
    for other_type, other_handlers in ROUTES.items():
        if other_type is contract_type:
            continue
        for path in other_handlers:
            if path in expected_handlers:
                continue
            assert len(received[path]) == 0, (
                f"{path} ({other_type.__name__}) leaked an event from "
                f"{contract_type.__name__}"
            )

    assert dead_letters == [], (
        f"unexpected DLQ writes for {contract_type.__name__}: {dead_letters}"
    )


def test_all_seven_types_have_at_least_one_handler():
    """Defensive: any §7.3 contract type with zero handlers is a regression."""
    for cls in SEVEN_CONTRACT_TYPES:
        assert ROUTES.get(cls), f"{cls.__name__} has empty handler list"
