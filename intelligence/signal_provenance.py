"""
Signal provenance — the per-ticker "why" report.

For a given ticker, assemble every piece of evidence the platform has
seen into one structured report that a trader can eyeball in 10
seconds to decide whether to act on the oracle's prediction:

  1. The oracle ensemble prediction (direction, confidence, CI)
  2. Per-signal per-horizon Brier scorecards (features/per_signal_brier)
     for every contributing signal — the conviction dial
  3. The top Shapley contributor + fragility multiplier (ALPHA-9)
  4. The LLM red-team epistemic-risk score (CAT-181)
  5. Recent shipping fudge alerts overlapping the ticker's sector
  6. The causation chain: lever, flow direction, actor
  7. Confidence breakdown showing how each multiplier stacked

The report is deliberately read-only — it does NOT run predict() itself
(callers pass in an already-computed ``EnsemblePrediction``). That
separation keeps the provenance logic testable without mocking the
entire oracle engine.

Output shape
------------

``TradeProvenanceReport`` is a frozen dataclass with ``to_dict()`` so
the API layer can serialize it directly. Every contributing signal
gets a ``SignalEvidence`` entry with its scorecard, weight, and
classification (strong/neutral/weak/anti).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from features.per_signal_brier import (
    SignalScorecard,
)
from features.regime_conditional_brier import (
    get_scorecard_with_regime_fallback,
)
from intelligence import signal_convergence_scanner as _convergence_mod
from intelligence.confidence_bucket_tracker import (
    conviction_multiplier_for_bucket,
)
from intelligence.contra_indicator_ensemble import contra_conviction_multiplier
from intelligence.historical_scenario_library import (
    scenario_conviction_multiplier,
)
from intelligence.meta_learning_matrix import (
    build_condition_tuple,
    get_aggregate_weight_multiplier,
)
from intelligence.money_flow_adapter import money_flow_conviction_multiplier
from intelligence.null_hypothesis_forecaster import null_hypothesis_penalty
from intelligence.prediction_market_arbitrage import (
    arbitrage_conviction_multiplier,
)
from intelligence.reasoning_bank import (
    MEMORY_LESSON_MULT_MAX,
    MEMORY_LESSON_MULT_MIN,
    memory_lesson_conviction_multiplier,
)
from intelligence.short_squeeze_composite import squeeze_conviction_multiplier
from intelligence.signal_cooccurrence import get_lift_multiplier


# ── Classification thresholds ─────────────────────────────────────────────

STRONG_CONVICTION_THRESHOLD: float = 1.2    # conviction weight ≥ 1.2 → strong
WEAK_CONVICTION_THRESHOLD: float = 0.5      # < 0.5 → weak
ANTI_CONVICTION_THRESHOLD: float = 0.01     # ≤ 0.01 → anti-predictive


def _classify_evidence(scorecard: SignalScorecard | None) -> str:
    """Translate a scorecard into a classification string.

    Pure function — no engine required.
    """
    if scorecard is None:
        return "no_history"
    if not scorecard.is_calibrated:
        return "cold_start"
    w = scorecard.conviction_weight
    if w <= ANTI_CONVICTION_THRESHOLD:
        return "anti_predictive"
    if w < WEAK_CONVICTION_THRESHOLD:
        return "weak"
    if w >= STRONG_CONVICTION_THRESHOLD:
        return "strong"
    return "neutral"


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SignalEvidence:
    """One contributing signal's evidence row in the provenance report."""

    signal_source: str
    shapley_weight: float         # normalized attribution share in [0, 1]
    scorecard: SignalScorecard | None
    classification: str           # strong / neutral / weak / anti_predictive / cold_start / no_history

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_source": self.signal_source,
            "shapley_weight": round(self.shapley_weight, 4),
            "scorecard": (
                self.scorecard.to_dict() if self.scorecard is not None else None
            ),
            "classification": self.classification,
        }


@dataclass(frozen=True)
class CausationChain:
    """The lever→flow→actor chain enforced by the user memory SOP.

    Every prediction MUST name a lever (who pulled what), a flow
    (which liquidity valve opened/closed), and an actor (the named
    party responsible). When any field is empty the chain is marked
    ``complete=False`` and the provenance report flags it as a noisy
    conditions-only prediction.
    """

    lever: str
    flow_direction: str           # 'open' / 'close' / 'neutral'
    actor: str
    complete: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "lever": self.lever,
            "flow_direction": self.flow_direction,
            "actor": self.actor,
            "complete": self.complete,
        }


@dataclass(frozen=True)
class TradeProvenanceReport:
    """Full per-ticker evidence report. Serialized to the API layer.

    Keep the single defaulted field at the end so Python 3.9 can import
    this dataclass on the live audit host.
    """

    ticker: str
    generated_at: str
    direction: str
    score: int
    confidence: float
    confidence_lower: float
    confidence_upper: float
    horizon_days: int
    regime: str
    fci_regime: str
    signal_evidence: list[SignalEvidence]
    top_shapley_contributor: str
    top_shapley_share: float
    fragility_multiplier: float
    disagreement_score: float
    crowd_aligned: bool
    market_implied_prob: float
    red_team_epistemic_risk: float | None
    shipping_fudge_alerts: list[dict[str, Any]]
    causation: CausationChain
    cooccurrence_lift: float  # pair-lift multiplier ∈ [0.75, 1.25] (CAT-177 signal_cooccurrence)
    regime_calibrated_signal_count: int  # how many evidence rows used regime-conditional Brier
    confidence_bucket_multiplier: float  # ∈ [0.60, 1.08] (CAT-180 confidence_bucket_tracker)
    scenario_multiplier: float            # ∈ [0.70, 1.10] (CAT-176 historical_scenario_library)
    null_hypothesis_penalty: float        # ∈ [0.50, 1.00] (CAT-186 null_hypothesis_forecaster)
    meta_learning_multiplier: float       # ∈ [0.40, 1.50] (CAT-193 meta_learning_matrix)
    contra_indicator_multiplier: float    # ∈ [0.85, 1.15] (CAT-184 contra_indicator_ensemble)
    squeeze_multiplier: float             # ∈ [0.90, 1.15] (CAT-138 short_squeeze_composite)
    arbitrage_multiplier: float           # ∈ [0.95, 1.10] (CAT-183 prediction_market_arbitrage)
    convergence_multiplier: float         # ∈ [0.92, 1.25] (dots-connector — signal_convergence_scanner)
    money_flow_multiplier: float          # ∈ [0.70, 1.30] (14th layer — money_flow_adapter)
    aggregate_conviction: float
    verdict: str  # 'high' / 'medium' / 'low' / 'no_trade'
    memory_lesson_multiplier: float = 1.0  # ∈ [0.85, 1.15] (15th layer — reasoning_bank). Default 1.0 (neutral) so older callers remain valid.
    # Coverage (2026-09-10, GRID-4 pivot §2.2/§7): which adjuster layers
    # actually computed a value (True), failed/had no data (False), or are
    # not applicable ("not_applicable", excluded from layers_total), plus
    # how much Shapley weight sits on signals with a calibrated scorecard.
    layer_coverage: dict[str, bool | str] = field(default_factory=dict)
    layers_present: int = 0
    layers_total: int = 0
    evidence_coverage: float = 0.0
    verdict_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "generated_at": self.generated_at,
            "direction": self.direction,
            "score": self.score,
            "confidence": round(self.confidence, 4),
            "confidence_lower": round(self.confidence_lower, 4),
            "confidence_upper": round(self.confidence_upper, 4),
            "horizon_days": self.horizon_days,
            "regime": self.regime,
            "fci_regime": self.fci_regime,
            "signal_evidence": [e.to_dict() for e in self.signal_evidence],
            "top_shapley_contributor": self.top_shapley_contributor,
            "top_shapley_share": round(self.top_shapley_share, 4),
            "fragility_multiplier": round(self.fragility_multiplier, 4),
            "disagreement_score": round(self.disagreement_score, 4),
            "crowd_aligned": self.crowd_aligned,
            "market_implied_prob": round(self.market_implied_prob, 4),
            "red_team_epistemic_risk": (
                round(self.red_team_epistemic_risk, 4)
                if self.red_team_epistemic_risk is not None
                else None
            ),
            "shipping_fudge_alerts": self.shipping_fudge_alerts,
            "causation": self.causation.to_dict(),
            "cooccurrence_lift": round(self.cooccurrence_lift, 4),
            "regime_calibrated_signal_count": self.regime_calibrated_signal_count,
            "confidence_bucket_multiplier": round(self.confidence_bucket_multiplier, 4),
            "scenario_multiplier": round(self.scenario_multiplier, 4),
            "null_hypothesis_penalty": round(self.null_hypothesis_penalty, 4),
            "meta_learning_multiplier": round(self.meta_learning_multiplier, 4),
            "contra_indicator_multiplier": round(self.contra_indicator_multiplier, 4),
            "squeeze_multiplier": round(self.squeeze_multiplier, 4),
            "arbitrage_multiplier": round(self.arbitrage_multiplier, 4),
            "convergence_multiplier": round(self.convergence_multiplier, 4),
            "money_flow_multiplier": round(self.money_flow_multiplier, 4),
            "memory_lesson_multiplier": round(self.memory_lesson_multiplier, 4),
            "aggregate_conviction": round(self.aggregate_conviction, 4),
            "verdict": self.verdict,
            "layer_coverage": dict(self.layer_coverage),
            "layers_present": self.layers_present,
            "layers_total": self.layers_total,
            "evidence_coverage": round(self.evidence_coverage, 4),
            "verdict_reason": self.verdict_reason,
        }


# ── Aggregate conviction ──────────────────────────────────────────────────


# Quantitative provenance adjuster layers. A layer is *present* when its
# upstream computed a real value; ``None`` means the upstream lookup failed,
# had no data, was not applicable (e.g. shipping fudge alerts on a non-shipping
# ticker), or was never attempted (``edge_signal``). Presence is reported as
# coverage so the verdict can distinguish "checked and neutral" from "not
# checked" — the distinction whose absence produced the 11.9%-hit-rate HIGH
# bucket (docs/planning/GRID-4-PRODUCT-PIVOT.md §2.2 and §7; LEVER-PACKAGE.md
# §3D / §7 T0.3).
#
# Deliberately excluded from CONVICTION_LAYERS (and from the multiplier stack):
# - ``disagreement`` and ``fragility``: already multiplied into
#   ``EnsemblePrediction.confidence`` in ``oracle.engine.EnsemblePredictor.predict``
#   (lines ~3014 and ~3062), and carried as dataclass defaults (0.0 / 1.0) that
#   cannot distinguish a computed neutral value from a failure. Keeping them
#   solely on the ``confidence`` axis prevents double-counting while still
#   displaying both values on ``TradeProvenanceReport``.
# - ``red_team``: advisory only (no numeric multiplier, no veto). An
#   LLM-derived layer may affect conviction only after it passes the same
#   out-of-sample gate as quantitative layers.
CONVICTION_LAYERS: tuple[str, ...] = (
    "fudge_alerts",
    "cooccurrence",
    "confidence_bucket",
    "scenario",
    "null_hypothesis",
    "meta_learning",
    "contra_indicator",
    "short_squeeze",
    "prediction_market_arb",
    "convergence",
    "money_flow",
    "memory_lesson",
    "edge_signal",
)

# Sentinel stored in ``layer_coverage`` for layers that do not apply to a
# given report (e.g. ``fudge_alerts`` on non-shipping-exposed tickers, or the
# unwired ``edge_signal`` layer). Excluded from the coverage denominator.
LAYER_NOT_APPLICABLE: str = "not_applicable"

# Evidence rows whose scorecard has enough history to mean something.
_CALIBRATED_CLASSIFICATIONS = frozenset({"strong", "neutral", "weak", "anti_predictive"})


@dataclass(frozen=True)
class ConvictionAggregate:
    """Aggregate conviction plus the coverage that produced it."""

    value: float
    layer_coverage: dict[str, bool | str]
    evidence_coverage: float

    @property
    def layers_present(self) -> int:
        return sum(1 for status in self.layer_coverage.values() if status is True)

    @property
    def layers_not_applicable(self) -> int:
        return sum(
            1 for status in self.layer_coverage.values()
            if status == LAYER_NOT_APPLICABLE
        )

    @property
    def layers_total(self) -> int:
        return sum(
            1 for status in self.layer_coverage.values()
            if status != LAYER_NOT_APPLICABLE
        )

    @property
    def layer_coverage_ratio(self) -> float:
        return self.layers_present / self.layers_total if self.layers_total else 0.0


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def compute_evidence_coverage(signal_evidence: list[SignalEvidence]) -> float:
    """Share of Shapley weight resting on signals with a calibrated scorecard.

    ``no_history`` and ``cold_start`` rows contribute a neutral 1.0 to the
    base term, so a report built entirely from them has an aggregate of
    exactly 1.0 that carries no information. This ratio is what lets the
    verdict see that.
    """
    total = sum(max(0.0, float(ev.shapley_weight)) for ev in signal_evidence)
    if total <= 0.0:
        return 0.0
    covered = sum(
        max(0.0, float(ev.shapley_weight))
        for ev in signal_evidence
        if ev.classification in _CALIBRATED_CLASSIFICATIONS
    )
    return covered / total


def aggregate_conviction_with_coverage(
    signal_evidence: list[SignalEvidence],
    *,
    fragility_multiplier: float | None = None,
    disagreement_score: float | None = None,
    red_team_epistemic_risk: float | None = None,
    fudge_alert_count: int | None = None,
    cooccurrence_lift: float | None = None,
    confidence_bucket_multiplier: float | None = None,
    scenario_multiplier: float | None = None,
    null_hypothesis_penalty_value: float | None = None,
    meta_learning_multiplier: float | None = None,
    contra_indicator_multiplier: float | None = None,
    squeeze_multiplier: float | None = None,
    arbitrage_multiplier: float | None = None,
    convergence_multiplier: float | None = None,
    money_flow_multiplier: float | None = None,
    memory_lesson_multiplier: float | None = None,
    edge_signal_multiplier: float | None = None,
    not_applicable: Iterable[str] = (),
) -> ConvictionAggregate:
    """Combine per-signal conviction weights into a single scalar, and say
    which layers actually contributed.

    Formula (pure, deterministic):

        base = Σ (shapley_weight_i × conviction_weight_i)
        penalty = Π over PRESENT layers of that layer's clamped multiplier
        aggregate = clamp(base × penalty, 0.0, 1.5)

    A layer passed as ``None`` is absent: it multiplies nothing (the value is
    identical to the old neutral-1.0 behaviour) and is reported as
    ``layer_coverage[name] = False``. Layers named in ``not_applicable`` are
    recorded as ``layer_coverage[name] = "not_applicable"`` and excluded from
    the coverage denominator (``layers_total``). Callers that want the scalar
    only use :func:`compute_aggregate_conviction`.

    ``cooccurrence_lift`` comes from ``intelligence.signal_cooccurrence.
    get_lift_multiplier`` (CAT-177): pairs of firing signals that
    historically hit together get a boost; pairs that dragged each other
    down get a discount.
    """
    # Per-signal overrides from intelligence.signal_weight_overrides.
    # Default-ON, env-gated. Multiplier of 1.0 = no effect.
    try:
        from intelligence.signal_weight_overrides import get_override as _signal_override
    except Exception:  # noqa: BLE001
        _signal_override = lambda _s: 1.0  # noqa: E731

    base = 0.0
    for ev in signal_evidence:
        override = _signal_override(getattr(ev, "signal_source", ""))
        if ev.scorecard is None:
            base += ev.shapley_weight * 1.0 * override  # neutral on no-history
        else:
            base += ev.shapley_weight * ev.scorecard.conviction_weight * override

    coverage: dict[str, bool | str] = {}
    na_layers = frozenset(not_applicable)
    penalty = 1.0

    def layer(name: str, value: Any, multiplier_of: Callable[[Any], float]) -> None:
        nonlocal penalty
        if name in na_layers:
            coverage[name] = LAYER_NOT_APPLICABLE
            return
        present = value is not None
        coverage[name] = present
        if present:
            penalty *= multiplier_of(value)

    # Signature retains fragility_multiplier, disagreement_score, and
    # red_team_epistemic_risk for caller compatibility, but none of the three
    # are multiplied into aggregate conviction or counted in layer_coverage:
    # - disagreement and fragility are already applied to confidence in
    #   oracle.engine.EnsemblePredictor.predict() (~L3014, ~L3062) and act on
    #   the verdict via the confidence axis alone (no double counting).
    # - red_team is advisory only: an LLM-derived layer may affect conviction
    #   only after it passes the same out-of-sample gate as quantitative layers.
    _ = (fragility_multiplier, disagreement_score, red_team_epistemic_risk)

    layer("fudge_alerts", fudge_alert_count,
          lambda v: max(0.1, 1.0 - 0.15 * max(0, int(v))))
    layer("cooccurrence", cooccurrence_lift, lambda v: _clamp(v, 0.75, 1.25))
    # Closing-the-loop calibration layers — each clamped to its own range
    # by the upstream module so we only need a defensive clamp here.
    layer("confidence_bucket", confidence_bucket_multiplier, lambda v: _clamp(v, 0.50, 1.10))
    layer("scenario", scenario_multiplier, lambda v: _clamp(v, 0.60, 1.15))
    layer("null_hypothesis", null_hypothesis_penalty_value, lambda v: _clamp(v, 0.40, 1.00))
    # Second-wave amplifiers: meta-learning edge, contra-indicator crowd,
    # per-ticker squeeze loadedness, and oracle-vs-market arbitrage.
    layer("meta_learning", meta_learning_multiplier, lambda v: _clamp(v, 0.40, 1.50))
    layer("contra_indicator", contra_indicator_multiplier, lambda v: _clamp(v, 0.80, 1.20))
    layer("short_squeeze", squeeze_multiplier, lambda v: _clamp(v, 0.85, 1.20))
    layer("prediction_market_arb", arbitrage_multiplier, lambda v: _clamp(v, 0.90, 1.15))
    # The dots-connector: rewards orthogonal multi-stream convergence.
    layer("convergence", convergence_multiplier, lambda v: _clamp(v, 0.90, 1.30))
    # 14th layer — money-flow engine, clamped hard to [0.70, 1.30].
    layer("money_flow", money_flow_multiplier, lambda v: _clamp(v, 0.70, 1.30))
    # 15th layer — ReasoningBank memory prior; a prior, not direct evidence.
    layer("memory_lesson", memory_lesson_multiplier,
          lambda v: _clamp(v, MEMORY_LESSON_MULT_MIN, MEMORY_LESSON_MULT_MAX))
    # 16th layer — EDGE multipliers from the backtest edge_table, bounded at
    # the source ([EDGE_MULTIPLIER_MIN, EDGE_MULTIPLIER_MAX]).
    layer("edge_signal", edge_signal_multiplier, lambda v: _clamp(v, 0.40, 1.80))

    return ConvictionAggregate(
        value=max(0.0, min(1.5, base * penalty)),
        layer_coverage=coverage,
        evidence_coverage=compute_evidence_coverage(signal_evidence),
    )


def compute_aggregate_conviction(
    signal_evidence: list[SignalEvidence],
    *,
    fragility_multiplier: float | None = None,
    disagreement_score: float | None = None,
    red_team_epistemic_risk: float | None = None,
    fudge_alert_count: int | None = None,
    cooccurrence_lift: float | None = None,
    confidence_bucket_multiplier: float | None = None,
    scenario_multiplier: float | None = None,
    null_hypothesis_penalty_value: float | None = None,
    meta_learning_multiplier: float | None = None,
    contra_indicator_multiplier: float | None = None,
    squeeze_multiplier: float | None = None,
    arbitrage_multiplier: float | None = None,
    convergence_multiplier: float | None = None,
    money_flow_multiplier: float | None = None,
    memory_lesson_multiplier: float | None = None,
    edge_signal_multiplier: float | None = None,
) -> float:
    """Scalar form of :func:`aggregate_conviction_with_coverage`.

    Kept for callers that only need the number (counterfactual stress,
    walk-forward validation, ``scripts/call_a_trade.py``). Values are
    identical to the pre-coverage implementation; only the reporting of
    which layers were present is new.
    """
    return aggregate_conviction_with_coverage(
        signal_evidence,
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        red_team_epistemic_risk=red_team_epistemic_risk,
        fudge_alert_count=fudge_alert_count,
        cooccurrence_lift=cooccurrence_lift,
        confidence_bucket_multiplier=confidence_bucket_multiplier,
        scenario_multiplier=scenario_multiplier,
        null_hypothesis_penalty_value=null_hypothesis_penalty_value,
        meta_learning_multiplier=meta_learning_multiplier,
        contra_indicator_multiplier=contra_indicator_multiplier,
        squeeze_multiplier=squeeze_multiplier,
        arbitrage_multiplier=arbitrage_multiplier,
        convergence_multiplier=convergence_multiplier,
        money_flow_multiplier=money_flow_multiplier,
        memory_lesson_multiplier=memory_lesson_multiplier,
        edge_signal_multiplier=edge_signal_multiplier,
    ).value


# HIGH requires at least this share of adjuster layers to have computed a
# real value, and at least this share of Shapley weight on calibrated
# scorecards. Both are deliberately modest floors: they reject "nothing was
# checked", not "some layers were quiet".
MIN_LAYER_COVERAGE_FOR_HIGH: float = 0.6
MIN_EVIDENCE_COVERAGE_FOR_HIGH: float = 0.5


def _verdict_from_aggregate(
    conviction: float,
    confidence: float,
    *,
    layer_coverage_ratio: float = 1.0,
    evidence_coverage: float = 1.0,
) -> str:
    """Classify the final trade verdict.

    Rules (deterministic, 2026-05-17 calibration):
      - aggregate conviction < 0.3 → no_trade
      - aggregate conviction < 0.7 OR raw confidence < 0.55 → low
      - 0.55 <= confidence <= 0.85 (CALIBRATED zone) → high
      - confidence > 0.85 (SATURATED zone) → medium
      - otherwise → medium

    Two interacting issues drove this gate:

    1. Conviction dimension is dead. A 6000-trade probe on grid-svr
       (2026-05-17) showed every trade's ``aggregate_conviction`` rounds
       to exactly 1.0 — the 13-layer adjuster chain is at neutral defaults
       because the upstream calibration substrate (per_signal_brier_history,
       confidence_bucket_tracker, regime_brier, meta_learning_matrix) is
       sparse. Each adjuster falls through to its defensive 1.0 default.
       Restore the two-axis gate ``conviction >= 1.15 AND confidence >= 0.7``
       once the substrate repopulates with backdated last_updated (see
       follow-up: backdate bootstrap_per_signal_brier).

    2. Confidence has a saturated 0.95 cap. ``oracle/engine.py``,
       ``intelligence/news_impact.py``, ``oracle/contrast_distillation.py``,
       ``oracle/psi_model.py``, ``oracle/forecaster_adapter.py``, and
       ``store/astrogrid.py`` all clamp raw confidence at ``min(0.95, ...)``
       without per-model calibration. Result: 612 of 677 trades with
       confidence >= 0.90 land at exactly 0.950; they are the worst-
       performing cohort in the audit (hit = 16.8%, mean_pnl = -1.69%).
       Until those caps are tightened or replaced with per-model
       reliability curves, the HIGH gate excludes confidence > 0.85 so the
       bucket isolates the calibrated 0.55-0.85 zone where the audit was
       previously discovering sharpe = +2.01 alpha under the MEDIUM label.

    See PR #192 (HIGH on confidence alone) + this PR (tighten upper bound).

    3. Coverage gate (2026-09-10). HIGH additionally requires that enough of
       the adjuster layers actually computed a value
       (``layer_coverage_ratio >= MIN_LAYER_COVERAGE_FOR_HIGH``) and that
       enough Shapley weight sits on calibrated scorecards
       (``evidence_coverage >= MIN_EVIDENCE_COVERAGE_FOR_HIGH``). A report
       whose layers silently defaulted to neutral is demoted to LOW — the
       pivot's rule that a signal whose layers cannot compute is not
       surfaced. Callers that pass no coverage keep the pre-gate behaviour.
    """
    # Single-use axis note: ``confidence`` already incorporates engine-level
    # disagreement and Shapley fragility dampening (oracle/engine.py ~L3014,
    # ~L3062), while ``conviction`` no longer re-applies either penalty. Both
    # verdict axes therefore stay orthogonal without double counting.
    if conviction < 0.3:
        return "no_trade"
    if conviction < 0.7 or confidence < 0.55:
        return "low"
    if 0.55 <= confidence <= 0.85:
        if (
            layer_coverage_ratio < MIN_LAYER_COVERAGE_FOR_HIGH
            or evidence_coverage < MIN_EVIDENCE_COVERAGE_FOR_HIGH
        ):
            return "low"
        return "high"
    return "medium"


def _verdict_reason(
    verdict: str,
    conviction: float,
    confidence: float,
    *,
    layer_coverage_ratio: float,
    evidence_coverage: float,
) -> str:
    """One-line, operator-readable reason for the verdict."""
    if conviction < 0.3:
        return f"aggregate conviction {conviction:.2f} < 0.30"
    if conviction < 0.7:
        return f"aggregate conviction {conviction:.2f} < 0.70"
    if confidence < 0.55:
        return f"confidence {confidence:.2f} < 0.55"
    if confidence > 0.85:
        return f"confidence {confidence:.2f} in the saturated > 0.85 zone"
    if layer_coverage_ratio < MIN_LAYER_COVERAGE_FOR_HIGH:
        return (
            f"only {layer_coverage_ratio:.0%} of adjuster layers computed "
            f"(floor {MIN_LAYER_COVERAGE_FOR_HIGH:.0%})"
        )
    if evidence_coverage < MIN_EVIDENCE_COVERAGE_FOR_HIGH:
        return (
            f"only {evidence_coverage:.0%} of signal weight has a calibrated "
            f"scorecard (floor {MIN_EVIDENCE_COVERAGE_FOR_HIGH:.0%})"
        )
    return f"{verdict}: conviction {conviction:.2f}, confidence {confidence:.2f}, coverage ok"


# ── Shapley contribution extraction & model→signal key mapping ────────────


def _normalize_weight_dict(raw: Any) -> dict[str, float]:
    """Coerce a ``{key: weight}`` mapping into a normalized positive-finite dict."""
    if not isinstance(raw, dict) or not raw:
        return {}
    cleaned: dict[str, float] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.strip():
            continue
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(w) or w <= 0.0:
            continue
        key = k.strip()
        cleaned[key] = cleaned.get(key, 0.0) + w
    total = sum(cleaned.values())
    if total <= 0.0:
        return {}
    return {k: v / total for k, v in cleaned.items()}


def _parse_string_list(raw: Any) -> list[str]:
    """Parse a JSONB / list column into a clean list of non-empty strings."""
    if raw is None:
        return []
    val = raw
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8")
        except UnicodeDecodeError:
            return []
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except (ValueError, TypeError):
            return []
    if not isinstance(val, (list, tuple)):
        return []
    return [s.strip() for s in val if isinstance(s, str) and s.strip()]


def _default_model_signal_map() -> dict[str, list[str]]:
    """Fallback model_name → signal_families/signal_sources map from code defaults.

    Mirrors ``scripts/bootstrap_per_signal_brier.py`` Layer 2: prefers
    ``oracle.engine.DEFAULT_MODELS`` ``signal_families`` (which bootstrap writes
    into ``per_signal_brier_history``) and supplements with
    ``oracle.model_factory._DEFAULT_SIGNAL_SOURCES`` for models not in
    ``DEFAULT_MODELS`` (e.g. ``ai_trader_crowd``).
    """
    mapping: dict[str, list[str]] = {}
    try:
        from oracle.engine import DEFAULT_MODELS

        for model in DEFAULT_MODELS:
            fams = _parse_string_list(getattr(model, "signal_families", None))
            if fams:
                mapping[model.name] = fams
    except Exception:  # noqa: BLE001
        pass
    try:
        from oracle.model_factory import _DEFAULT_SIGNAL_SOURCES

        for name, sources in _DEFAULT_SIGNAL_SOURCES.items():
            if name not in mapping:
                parsed = _parse_string_list(sources)
                if parsed:
                    mapping[name] = parsed
    except Exception:  # noqa: BLE001
        pass
    return mapping


def _load_model_signal_map(
    engine: Engine,
    model_names: Iterable[str],
) -> dict[str, list[str]]:
    """Map oracle model names to their contributing signal families/sources.

    Queries ``oracle_models`` (parameterized SQL) for ``signal_families`` /
    ``signal_sources`` first, then fills any missing known models from code
    defaults so scorecard lookups land in the same key space that
    ``scripts/bootstrap_per_signal_brier.py`` writes.
    """
    names = sorted({str(n).strip() for n in model_names if isinstance(n, str) and n.strip()})
    if not names:
        return {}

    mapping: dict[str, list[str]] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name, signal_families, signal_sources "
                    "FROM oracle_models "
                    "WHERE name = ANY(:names)"
                ),
                {"names": names},
            ).fetchall()
        for row in rows or []:
            if not isinstance(row, (tuple, list)) and not hasattr(row, "__getitem__"):
                continue
            try:
                name = row[0]
                fams = _parse_string_list(row[1] if len(row) > 1 else None)
                srcs = _parse_string_list(row[2] if len(row) > 2 else None)
            except (IndexError, TypeError):
                continue
            if not isinstance(name, str) or not name.strip():
                continue
            resolved = fams or srcs
            if resolved:
                mapping[name.strip()] = resolved
    except Exception as exc:  # noqa: BLE001
        log.debug("signal_provenance: oracle_models lookup skipped: {e}", e=str(exc))

    defaults = _default_model_signal_map()
    for name in names:
        if name not in mapping and name in defaults:
            mapping[name] = defaults[name]
    return mapping


def _extract_signal_contributions(
    prediction: Any,
    *,
    model_signal_map: dict[str, list[str]] | None = None,
) -> dict[str, float]:
    """Pull a normalized ``{signal_source: weight}`` dict out of an
    ``EnsemblePrediction``.

    Resolution order:
      1. Explicit per-signal ``signal_contributions`` on the prediction
         (top-level attribute or nested inside ``prediction.signals``).
      2. ``model_votes`` decomposed via each vote's own ``signal_sources`` /
         ``signal_families`` (when present), or via ``model_signal_map``
         (mapping oracle model names to their contributing signal families
         so lookups match the Brier scorecard key space).
      3. Fallback to ``shapley_top_contributor`` / ``shapley_top_share``
         (also mapped through ``model_signal_map`` when provided) plus
         ``"other"`` for any remainder.
    """
    explicit = _normalize_weight_dict(getattr(prediction, "signal_contributions", None))
    if explicit:
        return explicit

    signals_blob = getattr(prediction, "signals", None)
    if isinstance(signals_blob, dict):
        nested = _normalize_weight_dict(signals_blob.get("signal_contributions"))
        if nested:
            return nested

    contributions: dict[str, float] = {}
    lookup = model_signal_map or {}

    def _add_source_weight(name: str, weight: float) -> None:
        mapped = lookup.get(name)
        if mapped:
            share = weight / len(mapped)
            for sig in mapped:
                contributions[sig] = contributions.get(sig, 0.0) + share
        else:
            contributions[name] = contributions.get(name, 0.0) + weight

    top_source = getattr(prediction, "shapley_top_contributor", "") or ""
    top_share = float(getattr(prediction, "shapley_top_share", 0.0) or 0.0)

    model_votes = list(getattr(prediction, "model_votes", []) or [])
    for vote in model_votes:
        if not isinstance(vote, dict):
            continue
        weight_raw = vote.get("weight") or vote.get("vote_weight") or 0.0
        try:
            weight = float(weight_raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(weight) or weight <= 0.0:
            continue

        vote_signals = (
            vote.get("signal_sources")
            or vote.get("signal_families")
        )
        if isinstance(vote_signals, dict):
            norm_vote = _normalize_weight_dict(vote_signals)
            if norm_vote:
                for sig, share in norm_vote.items():
                    contributions[sig] = contributions.get(sig, 0.0) + weight * share
                continue
        else:
            parsed_vote_signals = _parse_string_list(vote_signals)
            if parsed_vote_signals:
                share = weight / len(parsed_vote_signals)
                for sig in parsed_vote_signals:
                    contributions[sig] = contributions.get(sig, 0.0) + share
                continue

        source = vote.get("model_name") or vote.get("model") or vote.get("source")
        if not source:
            continue
        _add_source_weight(str(source), weight)

    if not contributions and top_source:
        _add_source_weight(str(top_source), top_share)
        if top_share < 1.0:
            contributions["other"] = contributions.get("other", 0.0) + (1.0 - top_share)

    total = sum(contributions.values())
    if total <= 0.0:
        return {}
    return {k: v / total for k, v in contributions.items()}


def _resolve_signal_contributions(
    engine: Engine,
    prediction: Any,
) -> dict[str, float]:
    """Resolve per-signal/family contributions for ``prediction`` using
    ``oracle_models`` to translate model names into their signal key space.
    """
    raw_names: list[str] = []
    for vote in list(getattr(prediction, "model_votes", []) or []):
        if isinstance(vote, dict):
            name = vote.get("model_name") or vote.get("model") or vote.get("source")
            if isinstance(name, str) and name.strip():
                raw_names.append(name.strip())
    top_source = getattr(prediction, "shapley_top_contributor", "") or ""
    if isinstance(top_source, str) and top_source.strip():
        raw_names.append(top_source.strip())

    model_signal_map = _load_model_signal_map(engine, raw_names)
    return _extract_signal_contributions(prediction, model_signal_map=model_signal_map)


def _lookup_scorecard(
    engine: Engine,
    source: str,
    horizon_days: int,
    regime: str | None,
) -> SignalScorecard | None:
    """Look up a per-signal/regime scorecard, reconciling ``feature:<family>``
    and bare ``<family>`` key spellings written by different bootstrap paths.
    """
    card = get_scorecard_with_regime_fallback(engine, source, horizon_days, regime)
    if card is not None:
        return card
    if source.startswith("feature:"):
        bare = source.split(":", 1)[1].strip()
        if bare:
            return get_scorecard_with_regime_fallback(engine, bare, horizon_days, regime)
    elif ":" not in source and source not in ("other", "oracle_aggregate"):
        return get_scorecard_with_regime_fallback(
            engine, f"feature:{source}", horizon_days, regime
        )
    return None


# ── Shipping fudge alert lookup (sector-gated) ────────────────────────────


# Sectors materially exposed to maritime/freight/port statistics, using the
# canonical sector taxonomy from ``analysis.sector_map.SECTOR_MAP`` and
# ``analysis.market_universe.MARKET_UNIVERSE`` (stored lowercase for case-
# insensitive matching, plus ``marine`` / ``transportation`` aliases).
SHIPPING_EXPOSED_SECTORS: frozenset[str] = frozenset({
    "industrials",
    "transportation",
    "transportation & logistics",
    "marine",
    "energy",
    "materials",
})

_TICKER_SECTORS_CACHE: dict[str, frozenset[str]] | None = None


def _load_ticker_sector_index() -> dict[str, frozenset[str]]:
    """Build a cached ``{TICKER: frozenset[sector_lower]}`` index from the
    codebase's existing ticker sector taxonomies (``SECTOR_MAP`` and
    ``MARKET_UNIVERSE``). Never raises.
    """
    global _TICKER_SECTORS_CACHE
    if _TICKER_SECTORS_CACHE is not None:
        return _TICKER_SECTORS_CACHE

    acc: dict[str, set[str]] = {}

    def _add(ticker_raw: Any, sector_raw: Any) -> None:
        if not isinstance(ticker_raw, str) or not isinstance(sector_raw, str):
            return
        t = ticker_raw.strip().upper()
        s = sector_raw.strip().lower()
        if t and s:
            acc.setdefault(t, set()).add(s)

    try:
        from analysis.sector_map import SECTOR_MAP

        for sector_name, sector_data in (SECTOR_MAP or {}).items():
            if not isinstance(sector_data, dict):
                continue
            _add(sector_data.get("etf"), sector_name)
            for sub in (sector_data.get("subsectors") or {}).values():
                if not isinstance(sub, dict):
                    continue
                for actor in sub.get("actors") or []:
                    if not isinstance(actor, dict):
                        continue
                    if actor.get("type") == "company":
                        _add(actor.get("ticker"), sector_name)
    except Exception as exc:  # noqa: BLE001
        log.debug("signal_provenance: SECTOR_MAP load skipped: {e}", e=str(exc))

    try:
        from analysis.market_universe import MARKET_UNIVERSE

        for sector_name, sector_data in (MARKET_UNIVERSE or {}).items():
            if not isinstance(sector_data, dict):
                continue
            _add(sector_data.get("etf"), sector_name)
            for ind_name, ind_data in (sector_data.get("industries") or {}).items():
                if not isinstance(ind_data, dict):
                    continue
                _add(ind_data.get("etf"), sector_name)
                for comp in ind_data.get("companies") or []:
                    if isinstance(comp, dict):
                        _add(comp.get("ticker"), sector_name)
                        if isinstance(ind_name, str) and "transport" in ind_name.lower():
                            _add(comp.get("ticker"), "transportation")
    except Exception as exc:  # noqa: BLE001
        log.debug("signal_provenance: MARKET_UNIVERSE load skipped: {e}", e=str(exc))

    _TICKER_SECTORS_CACHE = {k: frozenset(v) for k, v in acc.items()}
    return _TICKER_SECTORS_CACHE


def _is_shipping_exposed_ticker(ticker: str) -> bool:
    """True iff ``ticker`` belongs to a sector in :data:`SHIPPING_EXPOSED_SECTORS`.

    Fails closed (returns ``False``) for empty or unknown tickers so shipping
    alerts never penalize unexposed names.
    """
    symbol = (ticker or "").strip().upper()
    if not symbol:
        return False
    sectors = _load_ticker_sector_index().get(symbol)
    if not sectors:
        return False
    return bool(sectors & SHIPPING_EXPOSED_SECTORS)


def _recent_fudge_alerts(
    engine: Engine,
    *,
    ticker: str,
    window_days: int = 7,
) -> list[dict[str, Any]] | None:
    """Pull shipping-category cross-reference alerts from the last
    ``window_days`` for tickers in :data:`SHIPPING_EXPOSED_SECTORS`.

    Returns:
      - ``None`` when ``ticker`` is not in a shipping-exposed sector (layer
        not applicable / unavailable) OR when the DB lookup fails.
      - A list of alert dicts (possibly empty ``[]`` when checked cleanly with
        zero active alerts) for shipping-exposed tickers.
    """
    if not _is_shipping_exposed_ticker(ticker):
        log.debug(
            "signal_provenance: shipping fudge alerts unavailable for non-shipping ticker {t}",
            t=ticker,
        )
        return None
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT name, assessment, implication,
                           divergence_zscore, confidence, checked_at
                    FROM cross_reference_checks
                    WHERE category = 'shipping'
                      AND assessment IN ('major_divergence', 'contradiction')
                      AND checked_at >= NOW() - (:w || ' days')::interval
                    ORDER BY checked_at DESC, ABS(divergence_zscore) DESC
                    LIMIT 10
                    """
                ),
                {"w": int(window_days)},
            ).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: fudge alert lookup failed for {t}: {e}",
            t=ticker, e=str(exc),
        )
        return None


# ── Causation chain extraction ────────────────────────────────────────────


def _extract_causation(prediction: Any) -> CausationChain:
    """Derive the lever→flow→actor chain from the prediction metadata.

    Per the user memory SOP, every prediction must name a lever, flow
    direction, and actor. This helper reads them off the prediction
    and marks the chain complete when all three are non-empty.
    """
    lever = (
        getattr(prediction, "catalyst_type", "")
        or getattr(prediction, "liquidity_state", "")
        or ""
    )
    actor = (
        getattr(prediction, "shapley_top_contributor", "")
        or getattr(prediction, "fci_regime", "")
        or ""
    )
    # Flow direction: derive from oracle direction + FCI regime
    direction = (getattr(prediction, "direction", "") or "").lower()
    if direction == "bullish":
        flow = "open"
    elif direction == "bearish":
        flow = "close"
    else:
        flow = "neutral"

    complete = bool(lever) and bool(actor) and flow != "neutral"
    return CausationChain(
        lever=lever,
        flow_direction=flow,
        actor=actor,
        complete=complete,
    )


# ── Coercion & convergence scanner lookup ───────────────────────────────────


def _finite_float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _lookup_convergence(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    target_direction: str,
    window_days: int = 7,
) -> float | None:
    """Query ``signal_convergence_scanner.scan_convergence`` so empty scans
    report ``None`` instead of 1.0 without editing the scanner module.
    """
    report = _convergence_mod.scan_convergence(
        engine,
        ticker=ticker,
        as_of=as_of,
        target_direction=target_direction,
        window_days=window_days,
    )
    if report.n_active_streams <= 0:
        log.debug(
            "signal_provenance: convergence unavailable for {t} (0 active streams)",
            t=ticker,
        )
        return None
    return _finite_float_or_none(report.conviction_multiplier)


# ── Main entry point ──────────────────────────────────────────────────────


def build_provenance_report(
    engine: Engine,
    *,
    prediction: Any,
    red_team_epistemic_risk: float | None = None,
) -> TradeProvenanceReport:
    """Build the full per-ticker provenance report from an
    ``EnsemblePrediction``.

    The prediction is expected to already be computed by
    ``oracle.engine.EnsemblePredictor.predict()`` and to carry the SWEEP
    fields (fragility_multiplier, crowdedness, market-implied prob,
    etc.). This function does NOT call predict() itself — callers pass
    it in so provenance is testable in isolation.
    """
    ticker = getattr(prediction, "ticker", "") or ""
    horizon_days = int(getattr(prediction, "horizon", 7) or 7)
    regime = getattr(prediction, "regime", None) or None

    contributions = _resolve_signal_contributions(engine, prediction)

    signal_evidence: list[SignalEvidence] = []
    regime_calibrated_count = 0
    for source, weight in contributions.items():
        # Regime-conditional Brier first, with graceful fallback to the
        # flat per-signal scorecard (CAT-180 wiring), reconciled across
        # bare family and ``feature:<family>`` spellings.
        scorecard = _lookup_scorecard(engine, source, horizon_days, regime)
        if scorecard is not None and regime is not None:
            regime_calibrated_count += 1
        signal_evidence.append(
            SignalEvidence(
                signal_source=source,
                shapley_weight=float(weight),
                scorecard=scorecard,
                classification=_classify_evidence(scorecard),
            )
        )

    # Non-applicable layers are kept in layer_coverage as "not_applicable"
    # and excluded from the coverage denominator (layers_total).
    not_applicable_layers: list[str] = ["edge_signal"]
    if _is_shipping_exposed_ticker(ticker):
        fudge_alerts_raw = _recent_fudge_alerts(engine, ticker=ticker)
    else:
        fudge_alerts_raw = None
        not_applicable_layers.append("fudge_alerts")
    causation = _extract_causation(prediction)

    # Pair lift multiplier (CAT-177): None if fewer than two firing signals,
    # stale, uncalibrated, or lookup fails.
    try:
        cooccurrence_lift = _finite_float_or_none(
            get_lift_multiplier(engine, contributions)
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: cooccurrence lift lookup failed: {e}", e=str(exc)
        )
        cooccurrence_lift = None

    # Confidence-bucket calibration (CAT-180): None on cold-start / missing /
    # DB error so absence is counted honestly in coverage.
    conf_value = float(getattr(prediction, "confidence", 0.0) or 0.0)
    try:
        confidence_bucket_mult = _finite_float_or_none(
            conviction_multiplier_for_bucket(
                engine, confidence=conf_value, horizon_days=horizon_days
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: confidence bucket lookup failed: {e}", e=str(exc)
        )
        confidence_bucket_mult = None

    today = date.today()
    direction_str = str(getattr(prediction, "direction", "") or "")

    # Historical scenario analog multiplier (CAT-176).
    try:
        scenario_mult = _finite_float_or_none(
            scenario_conviction_multiplier(
                engine,
                as_of=today,
                horizon_days=horizon_days,
                direction=getattr(prediction, "direction", None),
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: scenario multiplier lookup failed: {e}", e=str(exc)
        )
        scenario_mult = None

    # Null hypothesis skeptic penalty (CAT-186).
    try:
        null_penalty = _finite_float_or_none(
            null_hypothesis_penalty(engine, horizon_days=horizon_days)
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: null-hypothesis penalty lookup failed: {e}", e=str(exc)
        )
        null_penalty = None

    # Meta-learning per-condition edge (CAT-193).
    try:
        signals_blob = getattr(prediction, "signals", None) or {}
        vix_level = None
        if isinstance(signals_blob, dict):
            vix_level = signals_blob.get("vix_level")
        condition = build_condition_tuple(
            horizon_days=horizon_days,
            liquidity_regime=regime,
            fci_regime=getattr(prediction, "fci_regime", None),
            vix_level=vix_level,
        )
        meta_mult = _finite_float_or_none(
            get_aggregate_weight_multiplier(
                engine,
                signal_contributions=contributions,
                condition=condition,
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: meta_learning lookup failed: {e}", e=str(exc)
        )
        meta_mult = None

    # Contra-indicator ensemble (CAT-184).
    try:
        contra_mult = _finite_float_or_none(
            contra_conviction_multiplier(
                engine, as_of=today, trade_direction=direction_str
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: contra lookup failed: {e}", e=str(exc)
        )
        contra_mult = None

    # Short squeeze composite (CAT-138).
    try:
        squeeze_mult = _finite_float_or_none(
            squeeze_conviction_multiplier(
                engine,
                ticker=ticker,
                as_of=today,
                trade_direction=direction_str,
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: squeeze lookup failed: {e}", e=str(exc)
        )
        squeeze_mult = None

    # Prediction-market arbitrage (CAT-183).
    try:
        arb_mult = _finite_float_or_none(
            arbitrage_conviction_multiplier(
                engine,
                ticker=ticker,
                as_of=today,
                direction=direction_str,
                horizon_days=horizon_days,
                oracle_confidence=conf_value,
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: arbitrage lookup failed: {e}", e=str(exc)
        )
        arb_mult = None

    # Convergence scanner (dots-connector) — uses scan_convergence report API
    # without editing signal_convergence_scanner.py.
    try:
        convergence_mult = _lookup_convergence(
            engine,
            ticker=ticker,
            as_of=today,
            target_direction=direction_str,
            window_days=7,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: convergence scan failed: {e}", e=str(exc)
        )
        convergence_mult = None

    # Money flow engine (14th adjuster layer).
    try:
        money_flow_mult = _finite_float_or_none(
            money_flow_conviction_multiplier(
                engine,
                as_of=today,
                trade_direction=direction_str,
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: money flow lookup failed: {e}", e=str(exc)
        )
        money_flow_mult = None

    # ReasoningBank prior (15th adjuster layer).
    try:
        memory_fp: dict[str, Any] = {}
        if ticker:
            memory_fp["ticker"] = str(ticker).upper()
        if direction_str:
            memory_fp["direction"] = direction_str.lower()
        if regime:
            memory_fp["regime"] = str(regime)
        fci_val = getattr(prediction, "fci_regime", None)
        if fci_val:
            memory_fp["fci_bucket"] = str(fci_val)
        try:
            from intelligence.meta_learning_matrix import bucket_horizon as _bh
            memory_fp["horizon_bucket"] = _bh(int(horizon_days))
        except Exception:
            pass
        memory_lesson_mult = _finite_float_or_none(
            memory_lesson_conviction_multiplier(engine, fingerprint=memory_fp)
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: memory lesson lookup failed: {e}", e=str(exc)
        )
        memory_lesson_mult = None

    # Prediction-level fields kept for report display only (disagreement and
    # fragility are already applied to confidence in EnsemblePredictor.predict;
    # red_team is advisory only).
    fragility_raw = getattr(prediction, "fragility_multiplier", None)
    disagreement_raw = getattr(prediction, "disagreement_score", None)

    agg = aggregate_conviction_with_coverage(
        signal_evidence,
        fudge_alert_count=(None if fudge_alerts_raw is None else len(fudge_alerts_raw)),
        cooccurrence_lift=cooccurrence_lift,
        confidence_bucket_multiplier=confidence_bucket_mult,
        scenario_multiplier=scenario_mult,
        null_hypothesis_penalty_value=null_penalty,
        meta_learning_multiplier=meta_mult,
        contra_indicator_multiplier=contra_mult,
        squeeze_multiplier=squeeze_mult,
        arbitrage_multiplier=arb_mult,
        convergence_multiplier=convergence_mult,
        money_flow_multiplier=money_flow_mult,
        memory_lesson_multiplier=memory_lesson_mult,
        edge_signal_multiplier=None,
        not_applicable=not_applicable_layers,
    )
    aggregate = agg.value

    verdict = _verdict_from_aggregate(
        aggregate,
        conf_value,
        layer_coverage_ratio=agg.layer_coverage_ratio,
        evidence_coverage=agg.evidence_coverage,
    )
    reason = _verdict_reason(
        verdict,
        aggregate,
        conf_value,
        layer_coverage_ratio=agg.layer_coverage_ratio,
        evidence_coverage=agg.evidence_coverage,
    )

    def _or1(value: float | None) -> float:
        # Report fields stay floats for API compatibility; absence is carried
        # by layer_coverage, not by the number.
        return 1.0 if value is None else float(value)

    return TradeProvenanceReport(
        ticker=ticker,
        generated_at=datetime.now(timezone.utc).isoformat(),
        direction=getattr(prediction, "direction", "") or "",
        score=int(getattr(prediction, "score", 50) or 50),
        confidence=float(getattr(prediction, "confidence", 0.0) or 0.0),
        confidence_lower=float(getattr(prediction, "confidence_lower", 0.0) or 0.0),
        confidence_upper=float(getattr(prediction, "confidence_upper", 0.0) or 0.0),
        horizon_days=horizon_days,
        regime=getattr(prediction, "regime", "") or "",
        fci_regime=getattr(prediction, "fci_regime", "") or "",
        signal_evidence=signal_evidence,
        top_shapley_contributor=getattr(prediction, "shapley_top_contributor", "") or "",
        top_shapley_share=float(getattr(prediction, "shapley_top_share", 0.0) or 0.0),
        fragility_multiplier=_or1(fragility_raw),
        disagreement_score=(0.0 if disagreement_raw is None else float(disagreement_raw)),
        crowd_aligned=bool(getattr(prediction, "crowd_aligned", False)),
        market_implied_prob=float(
            getattr(prediction, "market_implied_prob", 0.0) or 0.0
        ),
        red_team_epistemic_risk=_finite_float_or_none(red_team_epistemic_risk),
        shipping_fudge_alerts=([] if fudge_alerts_raw is None else list(fudge_alerts_raw)),
        causation=causation,
        cooccurrence_lift=_or1(cooccurrence_lift),
        regime_calibrated_signal_count=regime_calibrated_count,
        confidence_bucket_multiplier=_or1(confidence_bucket_mult),
        scenario_multiplier=_or1(scenario_mult),
        null_hypothesis_penalty=_or1(null_penalty),
        meta_learning_multiplier=_or1(meta_mult),
        contra_indicator_multiplier=_or1(contra_mult),
        squeeze_multiplier=_or1(squeeze_mult),
        arbitrage_multiplier=_or1(arb_mult),
        convergence_multiplier=_or1(convergence_mult),
        money_flow_multiplier=_or1(money_flow_mult),
        memory_lesson_multiplier=_or1(memory_lesson_mult),
        aggregate_conviction=aggregate,
        verdict=verdict,
        layer_coverage=dict(agg.layer_coverage),
        layers_present=agg.layers_present,
        layers_total=agg.layers_total,
        evidence_coverage=agg.evidence_coverage,
        verdict_reason=reason,
    )
