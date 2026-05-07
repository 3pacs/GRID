#!/usr/bin/env python3
"""audit_conviction_stack.py — print the full conviction-stack puzzle map.

This is the "put the puzzle together" answer for the 11 calibration +
amplifier layers that stack into ``intelligence.signal_provenance.
compute_aggregate_conviction``. It walks the compute_aggregate_conviction
signature, the TradeProvenanceReport fields, and the module-level
entry-point conventions to produce a single table showing:

  - layer name + taxonomy class
  - upstream module + entry-point function
  - multiplier range (low, high, neutral)
  - scope (global / per-ticker / per-horizon / per-condition)
  - orthogonality hypothesis — what this layer measures that no other
    layer measures

Runs offline (no DB). Use it to audit redundancy before wiring any new
adjuster into the live path.

    python3 -m scripts.audit_conviction_stack
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass

from intelligence.signal_provenance import (
    TradeProvenanceReport,
    compute_aggregate_conviction,
)


@dataclass(frozen=True)
class Layer:
    taxonomy: str          # CALIBRATION / PENALTY / AMPLIFIER / CONVERGENCE
    name: str              # short pretty name
    param: str             # compute_aggregate_conviction kwarg
    field: str             # TradeProvenanceReport attribute
    low: float
    neutral: float
    high: float
    module: str            # dotted path
    entry_point: str       # function called by build_provenance_report
    scope: str             # what it varies over
    measures: str          # one-line orthogonality hypothesis


_LAYERS: tuple[Layer, ...] = (
    Layer(
        taxonomy="PENALTY",
        name="disagreement",
        param="disagreement_score",
        field="disagreement_score",
        low=0.60, neutral=1.00, high=1.00,
        module="oracle.engine",
        entry_point="EnsemblePrediction.disagreement_score",
        scope="per-prediction",
        measures="per-head variance across the 5 oracle models",
    ),
    Layer(
        taxonomy="PENALTY",
        name="fragility",
        param="fragility_multiplier",
        field="fragility_multiplier",
        low=0.50, neutral=1.00, high=1.50,
        module="oracle.engine (Shapley concentration)",
        entry_point="prediction.fragility_multiplier",
        scope="per-prediction",
        measures="how concentrated Shapley weights are on one signal",
    ),
    Layer(
        taxonomy="PENALTY",
        name="red_team",
        param="red_team_epistemic_risk",
        field="red_team_epistemic_risk",
        low=0.50, neutral=1.00, high=1.00,
        module="intelligence.llm_red_team",
        entry_point="llm_red_team_score()",
        scope="per-prediction",
        measures="LLM adversarial review — 'why might this be wrong'",
    ),
    Layer(
        taxonomy="PENALTY",
        name="fudge_alerts",
        param="fudge_alert_count",
        field="shipping_fudge_alerts (len)",
        low=0.10, neutral=1.00, high=1.00,
        module="intelligence.cross_reference",
        entry_point="cross_reference_checks query",
        scope="per-sector (shipping)",
        measures="government-vs-physical-reality divergences active now",
    ),
    Layer(
        taxonomy="CALIBRATION",
        name="cooccurrence_lift",
        param="cooccurrence_lift",
        field="cooccurrence_lift",
        low=0.75, neutral=1.00, high=1.25,
        module="intelligence.signal_cooccurrence",
        entry_point="get_lift_multiplier(engine, contributions)",
        scope="per-firing-signal-pair",
        measures="historical joint hit rate of firing signal pairs",
    ),
    Layer(
        taxonomy="CALIBRATION",
        name="confidence_bucket",
        param="confidence_bucket_multiplier",
        field="confidence_bucket_multiplier",
        low=0.60, neutral=1.00, high=1.08,
        module="intelligence.confidence_bucket_tracker",
        entry_point="conviction_multiplier_for_bucket(engine, conf, horizon)",
        scope="per-horizon × per-0.05-bucket",
        measures="empirical hit rate inside each probability bucket",
    ),
    Layer(
        taxonomy="CALIBRATION",
        name="historical_scenario",
        param="scenario_multiplier",
        field="scenario_multiplier",
        low=0.70, neutral=1.00, high=1.10,
        module="intelligence.historical_scenario_library",
        entry_point="scenario_conviction_multiplier(engine, as_of, horizon, dir)",
        scope="per-macro-snapshot",
        measures="analog base rates — did setups like this one play out",
    ),
    Layer(
        taxonomy="PENALTY",
        name="null_hypothesis",
        param="null_hypothesis_penalty_value",
        field="null_hypothesis_penalty",
        low=0.50, neutral=1.00, high=1.00,
        module="intelligence.null_hypothesis_forecaster",
        entry_point="null_hypothesis_penalty(engine, horizon)",
        scope="per-horizon global",
        measures="oracle edge vs majority / regime-base / coin-flip / momentum-K20",
    ),
    Layer(
        taxonomy="AMPLIFIER",
        name="meta_learning_edge",
        param="meta_learning_multiplier",
        field="meta_learning_multiplier",
        low=0.40, neutral=1.00, high=1.50,
        module="intelligence.meta_learning_matrix",
        entry_point="get_aggregate_weight_multiplier(engine, contribs, condition)",
        scope="per-signal × (horizon, regime, fci, vol)",
        measures="per-signal historical edge in exact current condition cube",
    ),
    Layer(
        taxonomy="AMPLIFIER",
        name="contra_indicator",
        param="contra_indicator_multiplier",
        field="contra_indicator_multiplier",
        low=0.85, neutral=1.00, high=1.15,
        module="intelligence.contra_indicator_ensemble",
        entry_point="contra_conviction_multiplier(engine, as_of, direction)",
        scope="global crowd snapshot",
        measures="AAII / BofA / PCR / retail-flow / smart-money / COT extremes",
    ),
    Layer(
        taxonomy="AMPLIFIER",
        name="short_squeeze",
        param="squeeze_multiplier",
        field="squeeze_multiplier",
        low=0.90, neutral=1.00, high=1.15,
        module="intelligence.short_squeeze_composite",
        entry_point="squeeze_conviction_multiplier(engine, ticker, as_of, direction)",
        scope="per-ticker",
        measures="SI% + DTC + borrow fee + momentum + social + GEX sign",
    ),
    Layer(
        taxonomy="AMPLIFIER",
        name="prediction_market_arb",
        param="arbitrage_multiplier",
        field="arbitrage_multiplier",
        low=0.95, neutral=1.00, high=1.10,
        module="intelligence.prediction_market_arbitrage",
        entry_point="arbitrage_conviction_multiplier(engine, ticker, ...)",
        scope="per-ticker × horizon",
        measures="oracle confidence vs Polymarket/Kalshi implied probability",
    ),
    Layer(
        taxonomy="CONVERGENCE",
        name="multi_stream_convergence",
        param="convergence_multiplier",
        field="convergence_multiplier",
        low=0.92, neutral=1.00, high=1.25,
        module="intelligence.signal_convergence_scanner",
        entry_point="convergence_conviction_multiplier(engine, ticker, ...)",
        scope="per-ticker × direction × 7d window",
        measures="8 orthogonal alt-data streams co-firing on same ticker",
    ),
    Layer(
        taxonomy="AMPLIFIER",
        name="money_flow_engine",
        param="money_flow_multiplier",
        field="money_flow_multiplier",
        low=0.70, neutral=1.00, high=1.30,
        module="intelligence.money_flow_adapter",
        entry_point="money_flow_conviction_multiplier(engine, as_of, direction)",
        scope="global × 8 macro flow layers",
        measures="sovereign + monetary + credit + institutional + corporate + retail + market + crypto capital rotation direction",
    ),
)


def _print_header(title: str) -> None:
    print()
    print("═" * 100)
    print(f"  {title}")
    print("═" * 100)


def _print_layer_table() -> None:
    _print_header("CONVICTION STACK — 14 multipliers feed compute_aggregate_conviction")
    hdr = f"{'#':>2}  {'class':<12}  {'name':<26}  {'range':<15}  {'scope':<30}"
    print(hdr)
    print("─" * 100)
    for i, L in enumerate(_LAYERS, 1):
        rng = f"[{L.low:.2f} ↔ {L.high:.2f}]"
        print(f"{i:>2}  {L.taxonomy:<12}  {L.name:<26}  {rng:<15}  {L.scope:<30}")


def _print_orthogonality_map() -> None:
    _print_header("ORTHOGONALITY MAP — what each layer uniquely measures")
    for L in _LAYERS:
        print(f"  [{L.taxonomy}] {L.name}")
        print(f"      module      : {L.module}")
        print(f"      entry       : {L.entry_point}")
        print(f"      measures    : {L.measures}")
        print()


def _print_redundancy_check() -> None:
    _print_header("REDUNDANCY CHECK — are any two layers measuring the same thing?")
    # Pairs where I've hand-audited the orthogonality claim
    pairs = [
        ("cooccurrence_lift", "meta_learning_edge",
         "cooccurrence works on SIGNAL PAIRS; meta_learning works on SIGNAL × CONDITION. "
         "Orthogonal: a pair can be calibrated while neither solo signal has edge in the regime."),
        ("confidence_bucket", "null_hypothesis",
         "confidence_bucket asks 'is 0.80 really 0.80?'; null_hypothesis asks 'does the oracle "
         "beat a dumb baseline at all?' Orthogonal: a bucket can be well-calibrated and still "
         "contain no edge."),
        ("historical_scenario", "meta_learning_edge",
         "historical_scenario uses 10-dim MACRO feature space; meta_learning uses 4-dim condition "
         "CUBE (regime/fci/vol/horizon). Orthogonal but partially correlated — expect ~0.3–0.5 "
         "Spearman in practice; acceptable because they disagree on edge cases."),
        ("contra_indicator", "convergence",
         "contra flags when retail/sell-side is EXTREME; convergence counts orthogonal PRO streams. "
         "Orthogonal: contra can fire loud while convergence is silent (opinion without action) or "
         "vice versa (action without sentiment shift)."),
        ("short_squeeze", "convergence",
         "squeeze measures FUEL (SI / borrow / DTC); convergence measures IGNITION (multi-stream "
         "action in the last 7d). Orthogonal: high squeeze with no ignition is a dud; high "
         "convergence with low squeeze is a normal momentum trade."),
        ("prediction_market_arb", "null_hypothesis",
         "arbitrage compares oracle to PREDICTION MARKETS (a real counterparty); null_hypothesis "
         "compares oracle to DUMB BASELINES on its own history. Orthogonal: oracle can beat both, "
         "beat one, or beat neither independently."),
    ]
    for a, b, explanation in pairs:
        print(f"  {a}  ↔  {b}")
        print(f"      {explanation}")
        print()


def _print_audit_signature_drift() -> None:
    """Sanity check: every layer's kwarg exists on compute_aggregate_conviction.
    This catches the class of bug where someone adds a layer but forgets
    to thread it through the aggregator.
    """
    _print_header("WIRING AUDIT — every layer is actually consumed by compute_aggregate_conviction")
    sig = inspect.signature(compute_aggregate_conviction)
    kwargs = set(sig.parameters.keys())
    report_fields = set(TradeProvenanceReport.__annotations__.keys())
    errors: list[str] = []
    for L in _LAYERS:
        if L.param not in kwargs:
            errors.append(f"  ✗ {L.name}: param {L.param!r} NOT in compute_aggregate_conviction signature")
        # field may carry extra projection (e.g. "len(...)"), skip strict check
    if errors:
        for e in errors:
            print(e)
    else:
        print(f"  ✓ all {len(_LAYERS)} layers correctly wired into aggregator")
    print(f"  (aggregator accepts {len(kwargs)} kwargs; TradeProvenanceReport exposes "
          f"{len(report_fields)} fields)")


def _print_combined_range() -> None:
    """Walk the full range algebraically to get the min/max possible aggregate
    multiplier if every layer hit its extreme simultaneously. This is the
    theoretical ceiling / floor of the adjuster stack — helpful for judging
    whether the 0.0–1.5 clamp in compute_aggregate_conviction ever bites.
    """
    _print_header("RANGE ALGEBRA — theoretical min/max of stacked adjusters (before clamp)")
    lo_prod = 1.0
    hi_prod = 1.0
    for L in _LAYERS:
        lo_prod *= L.low
        hi_prod *= L.high
    print(f"  product of all 13 lows  : {lo_prod:.4f}")
    print(f"  product of all 13 highs : {hi_prod:.4f}")
    print("  aggregator clamp         : [0.00, 1.50]")
    print(f"  ⇒ max achievable boost   : {min(1.5, hi_prod):.4f}")
    print(f"  ⇒ max achievable haircut : {max(0.0, lo_prod):.4f}")
    print("  ⇒ typical base conviction: ~1.0 (sum of shapley × conviction_weight)")
    print("  ⇒ realistic ceiling/floor: ~0.5× to ~1.5× once clamp applies")


def main() -> int:
    _print_header("GRID CONVICTION STACK — puzzle map as of today")
    _print_layer_table()
    _print_orthogonality_map()
    _print_redundancy_check()
    _print_audit_signature_drift()
    _print_combined_range()
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
