"""
Tests for ``trading.trade_ticket_generator``.

Validates:
  - Pure helpers (compute_invalidation_price, compute_target_price,
    kelly_size_from_report, compose_thesis, compose_evidence_summary)
  - Refusal gates on ``generate_ticket``
  - SOP fields populated and derived from the report (not hardcoded)
  - 5% Kelly hard cap holds across conviction levels
  - to_dict() roundtrip on the frozen dataclass
"""

from __future__ import annotations


import pytest

from intelligence.signal_provenance import (
    CausationChain,
    SignalEvidence,
    TradeProvenanceReport,
)
from trading.trade_ticket_generator import (
    DEFAULT_VOL_30D,
    MAX_KELLY_PER_TICKET,
    MIN_CONVICTION_FOR_TICKET,
    TradeTicket,
    compose_evidence_summary,
    compose_thesis,
    compute_invalidation_price,
    compute_target_price,
    generate_ticket,
    kelly_size_from_report,
)


# ── Fixtures ──────────────────────────────────────────────────────────────


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
    n_evidence: int = 4,
) -> TradeProvenanceReport:
    """Build a frozen TradeProvenanceReport for tests — no DB needed."""
    evidence: list[SignalEvidence] = []
    for i in range(n_evidence):
        evidence.append(
            SignalEvidence(
                signal_source=f"signal_{i}",
                shapley_weight=round(0.4 / max(1, i + 1), 4),
                scorecard=None,
                classification="strong" if i == 0 else "neutral",
            )
        )
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
    )


# ── compute_invalidation_price ────────────────────────────────────────────


def test_invalidation_long_below_current():
    stop = compute_invalidation_price(
        current_price=100.0, direction="bullish", vol_30d=0.30
    )
    assert stop < 100.0
    # 2σ daily move at 30% vol ≈ 100 * 2 * 0.30 / sqrt(252) ≈ 3.78
    assert 95.0 < stop < 99.0


def test_invalidation_short_above_current():
    stop = compute_invalidation_price(
        current_price=100.0, direction="bearish", vol_30d=0.30
    )
    assert stop > 100.0
    assert 101.0 < stop < 105.0


def test_invalidation_vol_none_returns_current():
    stop = compute_invalidation_price(
        current_price=100.0, direction="bullish", vol_30d=None
    )
    assert stop == 100.0


def test_invalidation_wider_risk_multiple_means_wider_stop():
    narrow = compute_invalidation_price(100.0, "bullish", 0.30, risk_multiple=2.0)
    wide = compute_invalidation_price(100.0, "bullish", 0.30, risk_multiple=3.0)
    # Wider risk multiple → stop further below current
    assert wide < narrow < 100.0


def test_invalidation_zero_or_negative_vol_returns_current():
    assert compute_invalidation_price(100.0, "bullish", 0.0) == 100.0
    assert compute_invalidation_price(100.0, "bullish", -0.1) == 100.0


# ── compute_target_price ──────────────────────────────────────────────────


def test_target_bullish_high_conviction_above_current():
    tgt = compute_target_price(
        current_price=100.0, direction="bullish",
        conviction=1.3, vol_30d=0.30, horizon_days=7,
    )
    assert tgt > 100.0


def test_target_bearish_below_current():
    tgt = compute_target_price(
        current_price=100.0, direction="bearish",
        conviction=1.3, vol_30d=0.30, horizon_days=7,
    )
    assert tgt < 100.0


def test_target_zero_conviction_returns_current():
    tgt = compute_target_price(
        current_price=100.0, direction="bullish",
        conviction=0.0, vol_30d=0.30,
    )
    assert tgt == 100.0


# ── kelly_size_from_report ────────────────────────────────────────────────


def test_kelly_uses_confidence_lower_not_raw_confidence():
    """ALPHA-12: size off the credible-interval floor, not the centroid."""
    high_raw = _make_report(confidence=0.99, confidence_lower=0.55)
    low_raw = _make_report(confidence=0.55, confidence_lower=0.55)
    pct_high, _ = kelly_size_from_report(high_raw, 100_000.0)
    pct_low, _ = kelly_size_from_report(low_raw, 100_000.0)
    # Equal confidence_lower → equal Kelly, regardless of raw confidence.
    assert pct_high == pct_low


def test_kelly_capped_at_5pct():
    report = _make_report(confidence=0.99, confidence_lower=0.95)
    pct, dollars = kelly_size_from_report(report, 100_000.0)
    assert pct <= MAX_KELLY_PER_TICKET
    assert pct == MAX_KELLY_PER_TICKET  # very high edge → hits cap
    assert dollars == pytest.approx(5_000.0)


def test_kelly_zero_edge_returns_zero():
    # p=0.33, q=0.67, b=2  →  edge = (0.66 - 0.67) / 2 = negative → 0
    report = _make_report(confidence_lower=0.33)
    pct, dollars = kelly_size_from_report(report, 100_000.0)
    assert pct == 0.0
    assert dollars == 0.0


def test_kelly_dollars_are_pct_times_account():
    report = _make_report(confidence_lower=0.95)
    pct, dollars = kelly_size_from_report(report, 50_000.0)
    assert dollars == pytest.approx(pct * 50_000.0)


# ── compose_thesis ────────────────────────────────────────────────────────


def test_compose_thesis_contains_all_sop_fields():
    report = _make_report(
        lever="Fed-cut-25bp",
        actor="Federal Reserve",
        flow_direction="open",
        regime="growth",
        fci_regime="loose",
        direction="bullish",
        horizon_days=7,
    )
    text = compose_thesis(report)
    # Lever, actor, flow, regime, fci, direction, horizon all present.
    assert "Fed-cut-25bp" in text
    assert "Federal Reserve" in text
    assert "open" in text
    assert "growth" in text
    assert "loose" in text
    assert "bullish" in text
    assert "7d" in text


# ── compose_evidence_summary ──────────────────────────────────────────────


def test_evidence_summary_returns_top_n_when_more_exist():
    report = _make_report(n_evidence=5)
    text = compose_evidence_summary(report, top_n=3)
    # Top 3 by shapley weight should be signal_0, signal_1, signal_2.
    assert "signal_0" in text
    assert "signal_1" in text
    assert "signal_2" in text
    assert "signal_3" not in text
    assert "signal_4" not in text


def test_evidence_summary_handles_fewer_than_n():
    report = _make_report(n_evidence=2)
    text = compose_evidence_summary(report, top_n=5)
    # Should not raise, should include both.
    assert "signal_0" in text
    assert "signal_1" in text


def test_evidence_summary_empty_evidence():
    report = _make_report(n_evidence=0)
    text = compose_evidence_summary(report)
    assert "no contributing signals" in text


# ── generate_ticket — refusal gates ───────────────────────────────────────


def test_generate_ticket_refuses_no_trade_verdict():
    report = _make_report(verdict="no_trade", aggregate_conviction=0.2)
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
    )
    assert ticket is None


def test_generate_ticket_refuses_low_verdict():
    report = _make_report(verdict="low", aggregate_conviction=0.5)
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
    )
    assert ticket is None


def test_generate_ticket_refuses_medium_verdict_below_min_conviction():
    report = _make_report(
        verdict="medium",
        aggregate_conviction=MIN_CONVICTION_FOR_TICKET - 0.1,
    )
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
    )
    assert ticket is None


def test_generate_ticket_refuses_zero_or_negative_price():
    report = _make_report()
    assert generate_ticket(report, account_size_usd=100_000.0, current_price=0.0) is None
    assert generate_ticket(report, account_size_usd=100_000.0, current_price=-5.0) is None


# ── generate_ticket — happy paths ─────────────────────────────────────────


def test_generate_ticket_high_verdict_returns_ticket():
    report = _make_report(verdict="high", aggregate_conviction=1.30)
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=0.30,
    )
    assert ticket is not None
    assert isinstance(ticket, TradeTicket)
    assert ticket.ticker == "TSLA"
    assert ticket.verdict == "high"
    assert ticket.aggregate_conviction == pytest.approx(1.30)


def test_generate_ticket_populates_all_required_sop_fields():
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=0.30,
    )
    assert ticket is not None
    # Every SOP field must be a non-empty string.
    assert ticket.lever and "Fed-cut-25bp" in ticket.lever
    assert ticket.condition and "growth" in ticket.condition
    assert ticket.thesis and "Federal Reserve" in ticket.thesis
    assert ticket.invalidation and "$" in ticket.invalidation
    assert ticket.evidence_summary
    assert ticket.kelly_size_pct >= 0.0
    assert ticket.kelly_size_dollars >= 0.0


def test_generate_ticket_equity_branch_options_fields_none():
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=0.30,
        instrument="equity",
    )
    assert ticket is not None
    assert ticket.instrument_type == "equity"
    assert ticket.options_strike is None
    assert ticket.options_expiry is None
    assert ticket.options_premium_est is None
    assert ticket.options_iv is None


def test_generate_ticket_bullish_stop_below_target_above():
    report = _make_report(direction="bullish")
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=0.30,
    )
    assert ticket is not None
    assert ticket.stop_price < ticket.entry_price
    assert ticket.target_price > ticket.entry_price


def test_generate_ticket_bearish_stop_above_target_below():
    report = _make_report(direction="bearish")
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=0.30,
    )
    assert ticket is not None
    assert ticket.stop_price > ticket.entry_price
    assert ticket.target_price < ticket.entry_price


def test_generate_ticket_vol_none_uses_default():
    report = _make_report()
    ticket = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=None,  # fall through to DEFAULT_VOL_30D
    )
    assert ticket is not None
    # With DEFAULT_VOL_30D (0.30) the stop should still be a sensible number.
    assert 0 < ticket.stop_price < 100.0
    # Same as if we had passed DEFAULT_VOL_30D explicitly.
    explicit = generate_ticket(
        report,
        account_size_usd=100_000.0,
        current_price=100.0,
        vol_30d=DEFAULT_VOL_30D,
    )
    assert explicit is not None
    assert ticket.stop_price == pytest.approx(explicit.stop_price)


def test_generate_ticket_kelly_never_exceeds_cap():
    """Even with confidence_lower pinned to 1.0, Kelly stays at 5%."""
    for conf_lower in (0.70, 0.80, 0.90, 0.95, 0.999, 1.0):
        report = _make_report(
            confidence_lower=conf_lower,
            aggregate_conviction=1.40,
            verdict="high",
        )
        ticket = generate_ticket(
            report,
            account_size_usd=1_000_000.0,
            current_price=100.0,
            vol_30d=0.30,
        )
        assert ticket is not None
        assert ticket.kelly_size_pct <= MAX_KELLY_PER_TICKET + 1e-12, (
            f"Kelly cap breached at confidence_lower={conf_lower}: "
            f"{ticket.kelly_size_pct}"
        )


# ── TradeTicket dataclass ─────────────────────────────────────────────────


def test_trade_ticket_is_frozen():
    report = _make_report()
    ticket = generate_ticket(
        report, account_size_usd=100_000.0, current_price=100.0, vol_30d=0.30
    )
    assert ticket is not None
    with pytest.raises(Exception):
        ticket.entry_price = 999.0  # type: ignore[misc]


def test_trade_ticket_to_dict_roundtrip():
    report = _make_report()
    ticket = generate_ticket(
        report, account_size_usd=100_000.0, current_price=100.0, vol_30d=0.30
    )
    assert ticket is not None
    d = ticket.to_dict()
    # Spot-check: every public field is present.
    for key in (
        "ticker", "direction", "instrument_type", "entry_price", "stop_price",
        "target_price", "kelly_size_pct", "kelly_size_dollars", "thesis",
        "invalidation", "lever", "condition", "evidence_summary",
        "generated_at", "verdict", "aggregate_conviction",
        "options_strike", "options_expiry", "options_premium_est", "options_iv",
    ):
        assert key in d
    # Roundtrip via TradeTicket(**d) reconstructs an equal ticket.
    rebuilt = TradeTicket(**d)
    assert rebuilt == ticket


# ── Invalidation derivation: must come from the report, not hardcoded ────


def test_invalidation_text_references_top_contributor_from_report():
    report = _make_report(top_contributor="dealer_gamma_flip")
    ticket = generate_ticket(
        report, account_size_usd=100_000.0, current_price=100.0, vol_30d=0.30
    )
    assert ticket is not None
    # The invalidation string must mention the contributor pulled off
    # the report — proves the field is data-driven, not a constant.
    assert "dealer_gamma_flip" in ticket.invalidation
    # And confidence_lower must appear, proving the price floor is
    # derived from the report and not hardcoded.
    assert f"{report.confidence_lower:.3f}" in ticket.invalidation
