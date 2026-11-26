"""
Tests for the pre-trade circuit breaker gate wired into
``trading.trade_ticket_generator.generate_ticket``.

These tests specifically exercise the new gate added in Step 2 of the
risk-salvage work — the gate must:

  - Let a happy-path ticket through when the breaker passes.
  - Return None when the global singleton is halted.
  - Return None when record_outcome has crossed the kill threshold.
  - Still generate a ticket when the breaker raises an exception (the
    try/except in the gate must fail open on broken risk modules so
    otherwise-valid trades are not blocked by infrastructure bugs).

HOLD actions are NOT tested here because ``generate_ticket`` only ever
emits tickets for bullish/bearish directions — it does not produce a
ticket for HOLD at all. The HOLD pass-through behaviour is covered in
tests/test_oracle_risk.py at the breaker level.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from features.per_signal_brier import SignalScorecard
from intelligence import signal_provenance as sp
from intelligence.signal_provenance import (
    REPORT_BASIS_LIVE,
    CausationChain,
    SignalEvidence,
    SignedFamilyReceipt,
    TradeProvenanceReport,
    actionable_refusal_reason,
)
from oracle.risk import (
    CircuitBreaker,
    CircuitBreakerConfig,
    get_global_circuit_breaker,
    reset_global_circuit_breaker,
)
from trading.trade_ticket_generator import (
    TradeTicket,
    generate_ticket,
)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Every test starts with a clean breaker singleton."""
    reset_global_circuit_breaker()
    yield
    reset_global_circuit_breaker()


def _fenced_card(source: str, weight: float) -> SignalScorecard:
    """A calibrated scorecard stamped before the report, so each row is
    eligible directional evidence and the evidence floor passes."""
    return SignalScorecard(
        signal_source=source,
        horizon_days=7,
        scored_count=50,
        running_brier=0.08,
        running_ece=0.06,
        hit_rate=0.65,
        last_updated=datetime(2026, 4, 12, tzinfo=timezone.utc),
        is_calibrated=True,
        conviction_weight=weight,
    )


# A live report's decision instant, after the cards' stamp and before now.
_DECISION_CLOCK = datetime(2026, 4, 13, tzinfo=timezone.utc)


# Issuance happens one minute after the decision, inside
# ``sp.ISSUANCE_MAX_AGE`` (Codex HOLD 6033471347).
_ISSUED_AT = _DECISION_CLOCK + timedelta(minutes=1)


@pytest.fixture(autouse=True)
def _issue_just_after_the_decision(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sp, "_utc_now", lambda: _ISSUED_AT)


def _signed(source: str, direction: str, ticker: str) -> SignedFamilyReceipt:
    """CONTRACT FIXTURE: a complete signed per-family receipt from the
    report's own decision, in ``direction``. Eligible evidence needs one
    (Codex HOLD 6033471347); nothing in production builds it yet."""
    return SignedFamilyReceipt(
        signal_source=source,
        direction=direction,
        ticker=ticker,
        decision_clock=_DECISION_CLOCK,
        consumed_signals=(f"{source}:obs-1",),
        head_directions=(("head", direction),),
        complete_contributing_set=True,
    )


def _make_report(
    *,
    ticker: str = "TSLA",
    direction: str = "bullish",
    confidence: float = 0.78,
    confidence_lower: float = 0.70,
    confidence_upper: float = 0.86,
    horizon_days: int = 7,
    regime: str = "growth",
    fci_regime: str = "loose",
    aggregate_conviction: float = 1.30,
    verdict: str = "high",
    lever: str = "Fed-cut-25bp",
    actor: str = "Federal Reserve",
    flow_direction: str = "open",
    top_contributor: str = "fed_liquidity",
    n_evidence: int = 3,
) -> TradeProvenanceReport:
    """Build a minimal TradeProvenanceReport for ticket generation tests."""
    evidence: list[SignalEvidence] = [
        SignalEvidence(
            signal_source=f"signal_{i}",
            shapley_weight=round(0.4 / max(1, i + 1), 4),
            scorecard=_fenced_card(f"signal_{i}", 1.3 if i == 0 else 1.0),
            classification="strong" if i == 0 else "neutral",
            receipt=_signed(f"signal_{i}", direction, ticker),
        )
        for i in range(n_evidence)
    ]
    return TradeProvenanceReport(
        ticker=ticker,
        generated_at="2026-04-13T00:00:00+00:00",
        direction=direction,
        score=72,
        confidence=confidence,
        confidence_lower=confidence_lower,
        confidence_upper=confidence_upper,
        horizon_days=horizon_days,
        regime=regime,
        fci_regime=fci_regime,
        signal_evidence=evidence,
        top_shapley_contributor=top_contributor,
        top_shapley_share=0.42,
        fragility_multiplier=1.0,
        disagreement_score=0.1,
        crowd_aligned=True,
        market_implied_prob=0.55,
        red_team_epistemic_risk=0.1,
        shipping_fudge_alerts=[],
        causation=CausationChain(
            lever=lever,
            flow_direction=flow_direction,
            actor=actor,
            complete=True,
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
        aggregate_conviction=aggregate_conviction,
        verdict=verdict,
        decision_clock=_DECISION_CLOCK.isoformat(),
        actionable=True,
        basis=REPORT_BASIS_LIVE,
    )


# ── Happy path ────────────────────────────────────────────────────────


def test_happy_path_returns_ticket_when_breaker_is_clean():
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert isinstance(ticket, TradeTicket)
    assert ticket.ticker == "TSLA"
    assert ticket.direction == "bullish"


def test_happy_path_bearish_also_passes():
    report = _make_report(direction="bearish")
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert isinstance(ticket, TradeTicket)
    assert ticket.direction == "bearish"


def test_happy_path_uses_global_singleton():
    """Verify the gate actually reads from the global singleton."""
    breaker = get_global_circuit_breaker()
    # Breaker is fresh — should NOT be halted.
    assert breaker.is_halted is False
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert ticket is not None


# ── Halt blocks ticket ────────────────────────────────────────────────


def test_manually_halted_breaker_blocks_ticket():
    breaker = get_global_circuit_breaker()
    breaker.activate_kill_switch("emergency manual halt")
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert ticket is None


def test_kill_switch_via_record_outcome_blocks_subsequent_ticket():
    """
    Drive the breaker into an automatic halt by recording a loss below
    the kill-switch threshold, then verify the next ticket request is
    refused by the gate.
    """
    # Default CircuitBreakerConfig has kill_switch_threshold=-10_000.
    breaker = get_global_circuit_breaker()
    breaker.record_outcome(regime="GROWTH", pnl=-15_000.0)
    assert breaker.is_halted is True

    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert ticket is None


def test_bearish_ticket_also_blocked_when_halted():
    breaker = get_global_circuit_breaker()
    breaker.activate_kill_switch("halt")
    report = _make_report(direction="bearish")
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert ticket is None


def test_halt_via_patched_singleton_instance():
    """Inject a pre-halted breaker through monkeypatch of the singleton."""
    preloaded = CircuitBreaker(CircuitBreakerConfig())
    preloaded.activate_kill_switch("injected halt")
    with patch("oracle.risk._GLOBAL_BREAKER", preloaded):
        report = _make_report()
        ticket = generate_ticket(
            report,
            account_size_usd=100_000.0,
            current_price=250.0,
            vol_30d=0.30,
        )
    assert ticket is None


# ── Fail-closed on exception ──────────────────────────────────────────


def test_exception_in_check_recommendation_fails_closed():
    """
    A broken risk module (e.g. someone mutates the breaker into an
    inconsistent state) MUST fail closed and refuse the ticket rather than
    letting unvalidated trades through.
    """
    def _boom(*_args, **_kwargs):
        raise RuntimeError("risk module imploded")

    with patch.object(CircuitBreaker, "check_recommendation", _boom):
        report = _make_report()
        ticket = generate_ticket(
            report,
            account_size_usd=100_000.0,
            current_price=250.0,
            vol_30d=0.30,
        )
    assert ticket is None


def test_import_error_in_gate_fails_closed():
    """
    If the risk module fails to import (simulated by patching
    get_global_circuit_breaker to raise), the generator fails closed and
    refuses the ticket.
    """
    def _raise_on_import(*_a, **_kw):
        raise ImportError("oracle.risk unavailable")

    with patch(
        "oracle.risk.get_global_circuit_breaker",
        side_effect=_raise_on_import,
    ):
        report = _make_report()
        ticket = generate_ticket(
            report,
            account_size_usd=100_000.0,
            current_price=250.0,
            vol_30d=0.30,
        )
    assert ticket is None


# ── Refusal gates upstream of the risk gate still work ────────────────


def test_low_conviction_still_refused_even_with_clean_breaker():
    """
    The existing conviction gate must still fire before (or alongside)
    the new risk gate. Clean breaker should not rescue a low-conviction
    ticket.
    """
    report = _make_report(aggregate_conviction=0.1, verdict="no_trade")
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert ticket is None


def test_zero_current_price_refused_before_risk_gate():
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=0.0,
        vol_30d=0.30,
    )
    assert ticket is None


def test_reset_between_calls_clears_halt():
    breaker = get_global_circuit_breaker()
    breaker.activate_kill_switch("temporary")
    # Reset the singleton itself (fresh instance, no cooldown legacy).
    reset_global_circuit_breaker()
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=250.0,
        vol_30d=0.30,
    )
    assert isinstance(ticket, TradeTicket)


def test_fixture_report_clears_the_floor_so_refusals_here_are_the_gates() -> None:
    # Round 7 (Codex HOLD 6033471347): each row's contract receipt makes it
    # eligible and the report issues inside ISSUANCE_MAX_AGE, so a refusal
    # in this file comes from the circuit breaker, not from the floor.
    for direction in ("bullish", "bearish"):
        assert actionable_refusal_reason(_make_report(direction=direction)) is None
