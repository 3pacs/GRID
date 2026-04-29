#!/usr/bin/env python3
"""call_a_trade.py — run the confidence stack end-to-end on a concrete
setup and print the structured trade ticket.

Worked example: LONG TSM, 7d horizon, April 2026.

Thesis in one sentence: rising SEMI book-to-bill (CAT-89) + rising
Taiwan export orders (CAT-9) in an EXPANSION liquidity regime with
easy FCI → TSMC revenue inflection leading the global semiconductor
capex upcycle; retail options pulse still chasing NVDA so the
contrarian edge on TSM is intact; no active Taiwan Strait OSINT
escalation so the tail-risk discount is small.

What's real vs synthesized
--------------------------

REAL (runs against the actual shipped modules):
  - intelligence.signal_provenance: compute_aggregate_conviction,
    _verdict_from_aggregate, CausationChain, SignalEvidence,
    TradeProvenanceReport dataclass
  - intelligence.counterfactual_stress: run_stress_test
  - trading.trade_ticket_generator: generate_ticket
  - features.per_signal_brier: compute_conviction_weight

SYNTHESIZED from plausible current-state values:
  - Per-signal Brier scorecards (what the bootstrap_per_signal_brier
    script would produce after replaying oracle_predictions history)
  - Shapley contribution weights (what the oracle engine's Shapley
    attribution would produce for this ticker + regime)
  - Current price + 30d realized vol (public values, April 2026)

Everything downstream of those inputs is the actual confidence stack
computing over the real code paths.
"""
from __future__ import annotations

from datetime import datetime, timezone

from features.per_signal_brier import SignalScorecard, compute_conviction_weight
from intelligence.counterfactual_stress import run_stress_test
from intelligence.signal_provenance import (
    CausationChain,
    SignalEvidence,
    TradeProvenanceReport,
    _verdict_from_aggregate,
    compute_aggregate_conviction,
)
from trading.trade_ticket_generator import generate_ticket


def _card(source: str, brier: float, count: int = 150) -> SignalScorecard:
    """Construct a SignalScorecard with a realistic conviction_weight
    computed by the actual shipped per_signal_brier.compute_conviction_weight.
    """
    return SignalScorecard(
        signal_source=source,
        horizon_days=7,
        scored_count=count,
        running_brier=brier,
        running_ece=brier * 1.1,
        hit_rate=max(0.5, 1 - (brier * 2.5)),  # rough inversion for display
        last_updated=datetime.now(timezone.utc),
        is_calibrated=True,
        conviction_weight=compute_conviction_weight(brier, count),
    )


def main() -> int:
    # ─── INPUTS ──────────────────────────────────────────────────────
    ticker = "TSM"
    current_price = 215.50   # TSM NYSE ADR, plausible April 2026
    vol_30d = 0.32           # 32% 30d realized vol — typical for TSM
    account_size_usd = 100_000
    horizon_days = 7

    # Per-signal Brier scorecards — what bootstrap_per_signal_brier
    # would produce after replaying oracle_predictions. Lower Brier =
    # better calibrated. The tight numbers here reflect the fact that
    # these 5 signals happen to be structurally predictive of TSM on
    # this horizon (SEMI b2b + Taiwan exports directly measure TSMC's
    # orderbook; flow momentum + regime contrarian are the most
    # historically-calibrated oracle models).
    scorecards = {
        "flow_momentum":     _card("flow_momentum",     0.09, 180),
        "regime_contrarian": _card("regime_contrarian", 0.11, 150),
        "semi_book_to_bill": _card("semi_book_to_bill", 0.08,  95),
        "taiwan_exports":    _card("taiwan_exports",    0.10, 105),
        "news_energy":       _card("news_energy",       0.14, 120),
    }

    # Shapley contributions from the oracle attribution — these are
    # what the predict() path would return given the SWEEP wiring
    # (flow_momentum + regime_contrarian are the two default-model
    # consumers of semi_book_to_bill, taiwan_exports, and news_energy).
    signal_evidence = [
        SignalEvidence("semi_book_to_bill", 0.28, scorecards["semi_book_to_bill"], "strong"),
        SignalEvidence("flow_momentum",     0.24, scorecards["flow_momentum"],     "strong"),
        SignalEvidence("taiwan_exports",    0.22, scorecards["taiwan_exports"],    "strong"),
        SignalEvidence("regime_contrarian", 0.16, scorecards["regime_contrarian"], "strong"),
        SignalEvidence("news_energy",       0.10, scorecards["news_energy"],       "strong"),
    ]

    # Penalty knobs — realistic for the current setup
    fragility_multiplier = 0.95    # slight Shapley concentration, not fragile
    disagreement_score = 0.15      # small disagreement between model heads
    red_team_epistemic_risk = 0.10 # LLM raised weak counters, no veto
    fudge_alert_count = 0          # no Taiwan shipping fudge alerts active

    # ─── STAGE 1: aggregate conviction ───────────────────────────────
    aggregate = compute_aggregate_conviction(
        signal_evidence,
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        red_team_epistemic_risk=red_team_epistemic_risk,
        fudge_alert_count=fudge_alert_count,
    )
    confidence = 0.78
    verdict = _verdict_from_aggregate(aggregate, confidence)

    # ─── STAGE 2: build provenance report ───────────────────────────
    causation = CausationChain(
        lever="semi_book_to_bill_rising",  # named lever per user memory SOP
        flow_direction="open",              # capex valve opening
        actor="TSMC_foundry_cycle",
        complete=True,
    )

    provenance = TradeProvenanceReport(
        ticker=ticker,
        generated_at=datetime.now(timezone.utc).isoformat(),
        direction="bullish",
        score=72,
        confidence=confidence,
        confidence_lower=0.68,
        confidence_upper=0.88,
        horizon_days=horizon_days,
        regime="EXPANSION",
        fci_regime="EASY",
        signal_evidence=signal_evidence,
        top_shapley_contributor="semi_book_to_bill",
        top_shapley_share=0.28,
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        crowd_aligned=False,  # retail chasing NVDA, not TSM — edge intact
        market_implied_prob=0.71,  # oracle is slightly ahead of options market
        red_team_epistemic_risk=red_team_epistemic_risk,
        shipping_fudge_alerts=[],
        causation=causation,
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
        memory_lesson_multiplier=1.0,
        aggregate_conviction=aggregate,
        verdict=verdict,
    )

    # ─── STAGE 3: run counterfactual stress test ────────────────────
    stress = run_stress_test(provenance)

    # ─── STAGE 4: generate the trade ticket ─────────────────────────
    ticket = generate_ticket(
        provenance,
        account_size_usd=account_size_usd,
        current_price=current_price,
        instrument="equity",
        vol_30d=vol_30d,
    )

    # ─── OUTPUT ──────────────────────────────────────────────────────
    print("=" * 72)
    print("GRID DECISION GATEWAY — should_i_trade('TSM') — 2026-04-14")
    print("=" * 72)
    print()
    print(f"Setup:           LONG TSM, {horizon_days}d horizon")
    print(f"Current price:   ${current_price:.2f}")
    print(f"30d vol:         {vol_30d:.0%}")
    print(f"Account size:    ${account_size_usd:,.0f}")
    print()

    print("─── CONVICTION STACK ──────────────────────────────────────────────")
    print(f"{'Signal':<22}{'Weight':>10}{'Brier':>10}{'n':>6}  {'Conviction':>12}")
    for ev in signal_evidence:
        sc = ev.scorecard
        print(
            f"{ev.signal_source:<22}"
            f"{ev.shapley_weight:>10.2f}"
            f"{sc.running_brier:>10.3f}"
            f"{sc.scored_count:>6d}  "
            f"{sc.conviction_weight:>12.3f}"
        )
    print()
    print(f"Raw confidence:        {confidence:.2f}  (lower={provenance.confidence_lower}, upper={provenance.confidence_upper})")
    print(f"Disagreement penalty:  × {1 - 0.4 * disagreement_score:.3f}")
    print(f"Fragility multiplier:  × {fragility_multiplier:.3f}")
    print(f"Red-team penalty:      × {1 - 0.5 * red_team_epistemic_risk:.3f}")
    print(f"Fudge penalty:         × {max(0.1, 1 - 0.15 * fudge_alert_count):.3f}")
    print(f"─────────────────────")
    print(f"Aggregate conviction:  {aggregate:.3f}")
    print(f"Provenance verdict:    {verdict.upper()}")
    print()

    print("─── COUNTERFACTUAL STRESS TEST ────────────────────────────────────")
    print(f"Robustness score:      {stress.robustness_score:.3f}")
    print(f"Robustness label:      {stress.robustness_label.upper()}")
    print(f"Perturbations tested:  {len(stress.perturbations)}")
    print(f"Verdict breaks:        {stress.break_count}")
    print(f"Advisory:              {stress.advisory}")
    if stress.fragility_flags:
        fragile = [f for f in stress.fragility_flags if f.fragile]
        if fragile:
            print("Fragile signals:")
            for f in fragile:
                sigma = f.breaking_sigma
                sigma_str = f"{sigma:+.1f}σ" if sigma is not None else "?"
                print(f"  - {f.signal_source}: breaks at {sigma_str} ({f.reason})")
    print()

    # ─── TRADE TICKET ────────────────────────────────────────────────
    print("=" * 72)
    if ticket is None:
        print("  NO TICKET — the generator refused to produce an actionable call")
        print("=" * 72)
        return 1

    print(f"  TRADE TICKET — verdict={ticket.verdict.upper()}  ({ticker})")
    print("=" * 72)
    print()
    print(f"  Direction:     {ticket.direction.upper()}")
    print(f"  Instrument:    {ticket.instrument_type}")
    print()
    print(f"  Entry:         ${ticket.entry_price:.2f}")
    print(f"  Stop:          ${ticket.stop_price:.2f}   "
          f"({((ticket.stop_price / ticket.entry_price) - 1) * 100:+.2f}%)")
    print(f"  Target:        ${ticket.target_price:.2f}   "
          f"({((ticket.target_price / ticket.entry_price) - 1) * 100:+.2f}%)")
    risk = ticket.entry_price - ticket.stop_price
    reward = ticket.target_price - ticket.entry_price
    if abs(risk) > 1e-9:
        print(f"  Reward/Risk:   {reward / risk:.2f}")
    print()
    print(f"  Kelly size:    {ticket.kelly_size_pct * 100:.2f}% of account "
          f"= ${ticket.kelly_size_dollars:,.0f}")
    shares = int(ticket.kelly_size_dollars / ticket.entry_price)
    print(f"  Shares:        ~{shares} @ ${ticket.entry_price:.2f}")
    print()
    print(f"  LEVER:         {ticket.lever}")
    print(f"  CONDITION:     {ticket.condition}")
    print()
    print("  THESIS:")
    for line in _wrap(ticket.thesis, 68):
        print(f"    {line}")
    print()
    print("  INVALIDATION:")
    for line in _wrap(ticket.invalidation, 68):
        print(f"    {line}")
    print()
    print("  EVIDENCE:")
    for line in ticket.evidence_summary.split("\n"):
        print(f"    {line}")
    print()
    print("  GENERATED:     " + ticket.generated_at)
    print("=" * 72)
    return 0


def _wrap(text: str, width: int) -> list[str]:
    """Tiny word-wrap helper — keeps output readable without importing textwrap."""
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    length = 0
    for w in words:
        if length + len(w) + (1 if current else 0) > width:
            lines.append(" ".join(current))
            current = [w]
            length = len(w)
        else:
            current.append(w)
            length += len(w) + (1 if current[:-1] else 0)
    if current:
        lines.append(" ".join(current))
    return lines


if __name__ == "__main__":
    raise SystemExit(main())
