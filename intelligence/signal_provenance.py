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

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from features.per_signal_brier import (
    SignalScorecard,
)
from features.regime_conditional_brier import (
    get_scorecard_with_regime_fallback,
)
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
from intelligence.signal_convergence_scanner import (
    convergence_conviction_multiplier,
)
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
    red_team_epistemic_risk: float
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
    # actually computed a value, and how much Shapley weight sits on signals
    # with a calibrated scorecard. A layer that failed or was never attempted
    # is reported as absent instead of silently reading as neutral 1.0.
    layer_coverage: dict[str, bool] = field(default_factory=dict)
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
            "red_team_epistemic_risk": round(self.red_team_epistemic_risk, 4),
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


# The adjuster layers, in the order they were added. A layer is *present*
# when its caller computed a real value; ``None`` means the upstream lookup
# failed, had no data, or was never attempted. Presence is reported as
# coverage so the verdict can distinguish "checked and neutral" from "not
# checked" — the distinction whose absence produced the 11.9%-hit-rate HIGH
# bucket (docs/planning/GRID-4-PRODUCT-PIVOT.md §2.2 and §7; LEVER-PACKAGE.md
# §3D / §7 T0.3). Before 2026-09-10 every missing layer read as 1.0.
CONVICTION_LAYERS: tuple[str, ...] = (
    "disagreement",
    "fragility",
    "red_team",
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

# Evidence rows whose scorecard has enough history to mean something.
_CALIBRATED_CLASSIFICATIONS = frozenset({"strong", "neutral", "weak", "anti_predictive"})


@dataclass(frozen=True)
class ConvictionAggregate:
    """Aggregate conviction plus the coverage that produced it."""

    value: float
    layer_coverage: dict[str, bool]
    evidence_coverage: float

    @property
    def layers_present(self) -> int:
        return sum(1 for present in self.layer_coverage.values() if present)

    @property
    def layers_total(self) -> int:
        return len(self.layer_coverage)

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
) -> ConvictionAggregate:
    """Combine per-signal conviction weights into a single scalar, and say
    which layers actually contributed.

    Formula (pure, deterministic):

        base = Σ (shapley_weight_i × conviction_weight_i)
        penalty = Π over PRESENT layers of that layer's clamped multiplier
        aggregate = clamp(base × penalty, 0.0, 1.5)

    A layer passed as ``None`` is absent: it multiplies nothing (the value is
    identical to the old neutral-1.0 behaviour) but is reported as
    ``layer_coverage[name] = False``. Callers that want the scalar only use
    :func:`compute_aggregate_conviction`.

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

    coverage: dict[str, bool] = {}
    penalty = 1.0

    def layer(name: str, value: Any, multiplier_of: Callable[[Any], float]) -> None:
        nonlocal penalty
        present = value is not None
        coverage[name] = present
        if present:
            penalty *= multiplier_of(value)

    layer("disagreement", disagreement_score,
          lambda v: max(0.0, 1.0 - 0.4 * _clamp(v, 0.0, 1.0)))
    layer("fragility", fragility_multiplier, lambda v: _clamp(v, 0.0, 1.5))
    layer("red_team", red_team_epistemic_risk,
          lambda v: max(0.0, 1.0 - 0.5 * _clamp(v, 0.0, 1.0)))
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


# ── Shapley contribution extraction ───────────────────────────────────────


def _extract_signal_contributions(
    prediction: Any,
) -> dict[str, float]:
    """Pull a normalized {signal_source: weight} dict out of an
    ``EnsemblePrediction``.

    The Shapley attribution from ALPHA-9 lives on the prediction in two
    places: ``shapley_top_contributor`` / ``shapley_top_share`` carries
    the dominant single source, and ``model_votes`` carries per-model
    vote weights that can be decomposed into per-signal shares via the
    model's ``signal_sources`` list (when available).

    This helper is defensive: when model_votes is empty or lacks
    contribution data, it falls back to a single-entry dict with the
    top contributor at its reported share plus an implicit "other"
    bucket for the remainder.
    """
    contributions: dict[str, float] = {}

    top_source = getattr(prediction, "shapley_top_contributor", "") or ""
    top_share = float(getattr(prediction, "shapley_top_share", 0.0) or 0.0)

    model_votes = list(getattr(prediction, "model_votes", []) or [])
    # Per-model vote weight is roughly per-source weight when each
    # model is single-source, which is a reasonable zeroth-order
    # approximation.
    for vote in model_votes:
        if not isinstance(vote, dict):
            continue
        source = vote.get("model_name") or vote.get("model") or vote.get("source")
        weight = vote.get("weight") or vote.get("vote_weight") or 0.0
        if not source or weight <= 0:
            continue
        contributions[str(source)] = contributions.get(str(source), 0.0) + float(weight)

    if not contributions and top_source:
        # Fallback: the top contributor at its reported share, with
        # the remainder attributed to an opaque "other" bucket so the
        # weights sum to 1.0 and compute_aggregate_conviction is valid.
        contributions[top_source] = top_share
        if top_share < 1.0:
            contributions["other"] = 1.0 - top_share

    total = sum(contributions.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in contributions.items()}


# ── Shipping fudge alert lookup ───────────────────────────────────────────


def _recent_fudge_alerts(
    engine: Engine,
    *,
    ticker: str,
    window_days: int = 7,
) -> list[dict[str, Any]]:
    """Pull any shipping-category cross-reference alerts from the last
    ``window_days`` that could plausibly overlap the ticker's sector.

    This is a generous match — we don't have a ticker→port lookup yet,
    so every shipping alert bubbles up to commodity/industrial tickers
    and the trader can filter. Future work: build a sector→port map.
    """
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
        return []


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


# ── Main entry point ──────────────────────────────────────────────────────


def build_provenance_report(
    engine: Engine,
    *,
    prediction: Any,
    red_team_epistemic_risk: float = 0.0,
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

    contributions = _extract_signal_contributions(prediction)

    signal_evidence: list[SignalEvidence] = []
    regime_calibrated_count = 0
    for source, weight in contributions.items():
        # Regime-conditional Brier first, with graceful fallback to the
        # flat per-signal scorecard (CAT-180 wiring). The fallback API
        # guarantees we never have to branch here.
        scorecard = get_scorecard_with_regime_fallback(
            engine, source, horizon_days, regime
        )
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

    fudge_alerts = _recent_fudge_alerts(engine, ticker=ticker)
    causation = _extract_causation(prediction)

    # Pair lift multiplier (CAT-177): history of which firing signal
    # pairs amplify or drag each other down. Neutral 1.0 if fewer than
    # two firing signals or no calibrated pair stats.
    try:
        cooccurrence_lift = float(get_lift_multiplier(engine, contributions))
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: cooccurrence lift lookup failed: {e}", e=str(exc)
        )
        cooccurrence_lift = None

    # Confidence-bucket calibration (CAT-180): does the oracle's claimed
    # probability match reality in this bucket historically? Over-confident
    # buckets get dampened, under-confident ones get a mild boost.
    conf_value = float(getattr(prediction, "confidence", 0.0) or 0.0)
    try:
        confidence_bucket_mult = float(
            conviction_multiplier_for_bucket(
                engine, confidence=conf_value, horizon_days=horizon_days
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: confidence bucket lookup failed: {e}", e=str(exc)
        )
        confidence_bucket_mult = None

    # Historical scenario analog multiplier (CAT-176): how did setups that
    # looked like TODAY'S macro snapshot actually play out? Uses PIT price
    # replay internally so no lookahead leak.
    try:
        scenario_mult = float(
            scenario_conviction_multiplier(
                engine,
                as_of=date.today(),
                horizon_days=horizon_days,
                direction=getattr(prediction, "direction", None),
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: scenario multiplier lookup failed: {e}", e=str(exc)
        )
        scenario_mult = None

    # Null hypothesis skeptic penalty (CAT-186): if the oracle barely
    # beats a dumb baseline on its recent history, haircut the conviction.
    try:
        null_penalty = float(
            null_hypothesis_penalty(engine, horizon_days=horizon_days)
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: null-hypothesis penalty lookup failed: {e}", e=str(exc)
        )
        null_penalty = None

    today = date.today()
    direction_str = str(getattr(prediction, "direction", "") or "")

    # Meta-learning per-condition edge (CAT-193): has this signal×regime×
    # vol×fci combination historically produced edge, or has it been noise?
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
        meta_mult = float(
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

    # Contra-indicator ensemble (CAT-184): is retail/sell-side extreme in
    # a direction that favors (or opposes) this trade?
    try:
        contra_mult = float(
            contra_conviction_multiplier(
                engine, as_of=today, trade_direction=direction_str
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: contra lookup failed: {e}", e=str(exc)
        )
        contra_mult = None

    # Short squeeze composite (CAT-138): per-ticker squeeze loadedness —
    # bullish calls on high-squeeze names get a boost, bearish gets a
    # haircut (shorting loaded guns is dangerous).
    try:
        squeeze_mult = float(
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

    # Prediction-market arbitrage (CAT-183): oracle-vs-Polymarket edge.
    try:
        arb_mult = float(
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

    # Convergence scanner — the dots-connector. Scans congressional /
    # insider / dark-pool / options-flow / smart-money / 13F / social /
    # prediction-market streams for orthogonal confirmation of the
    # target direction in the last 7 days. Rewards rare multi-stream
    # alignment; penalizes when orthogonal streams oppose the call.
    try:
        convergence_mult = float(
            convergence_conviction_multiplier(
                engine,
                ticker=ticker,
                as_of=today,
                target_direction=direction_str,
                window_days=7,
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: convergence scan failed: {e}", e=str(exc)
        )
        convergence_mult = None

    # Money flow engine (14th adjuster layer). Walks the 8-layer
    # junction-point graph and returns [0.70, 1.30] based on whether
    # the trade is aligned with inferred global capital rotation.
    try:
        money_flow_mult = float(
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

    # ReasoningBank prior (15th adjuster layer). Counts outcome classes
    # of past distilled lessons matching this (ticker, direction, regime,
    # horizon) fingerprint. Narrow range [0.85, 1.15] — a prior, not
    # direct evidence. Empty bank → neutral 1.0.
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
        memory_lesson_mult = float(
            memory_lesson_conviction_multiplier(engine, fingerprint=memory_fp)
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "signal_provenance: memory lesson lookup failed: {e}", e=str(exc)
        )
        memory_lesson_mult = None

    # Prediction-level adjusters are absent (None) when the prediction object
    # does not carry them, instead of reading as neutral.
    fragility_raw = getattr(prediction, "fragility_multiplier", None)
    disagreement_raw = getattr(prediction, "disagreement_score", None)

    agg = aggregate_conviction_with_coverage(
        signal_evidence,
        fragility_multiplier=(None if fragility_raw is None else float(fragility_raw)),
        disagreement_score=(None if disagreement_raw is None else float(disagreement_raw)),
        red_team_epistemic_risk=float(red_team_epistemic_risk or 0.0),
        fudge_alert_count=len(fudge_alerts),
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
        red_team_epistemic_risk=float(red_team_epistemic_risk or 0.0),
        shipping_fudge_alerts=fudge_alerts,
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
