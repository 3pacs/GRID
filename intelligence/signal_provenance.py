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

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

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


@dataclass(frozen=True, kw_only=True)
class TradeProvenanceReport:
    """Full per-ticker evidence report. Serialized to the API layer.

    ``kw_only=True`` so future conviction-stack layers can be added with
    a default without forcing every caller to update positional ordering.
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
    memory_lesson_multiplier: float = 1.0  # ∈ [0.85, 1.15] (15th layer — reasoning_bank). Default 1.0 (neutral) so new layers can be added without breaking every caller.
    aggregate_conviction: float
    verdict: str  # 'high' / 'medium' / 'low' / 'no_trade'

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
        }


# ── Aggregate conviction ──────────────────────────────────────────────────


def compute_aggregate_conviction(
    signal_evidence: list[SignalEvidence],
    *,
    fragility_multiplier: float = 1.0,
    disagreement_score: float = 0.0,
    red_team_epistemic_risk: float = 0.0,
    fudge_alert_count: int = 0,
    cooccurrence_lift: float = 1.0,
    confidence_bucket_multiplier: float = 1.0,
    scenario_multiplier: float = 1.0,
    null_hypothesis_penalty_value: float = 1.0,
    meta_learning_multiplier: float = 1.0,
    contra_indicator_multiplier: float = 1.0,
    squeeze_multiplier: float = 1.0,
    arbitrage_multiplier: float = 1.0,
    convergence_multiplier: float = 1.0,
    money_flow_multiplier: float = 1.0,
    memory_lesson_multiplier: float = 1.0,
    edge_signal_multiplier: float = 1.0,
) -> float:
    """Combine per-signal conviction weights into a single scalar.

    Formula (pure, deterministic):

        base = Σ (shapley_weight_i × conviction_weight_i)
        penalty = (1 - 0.4 × disagreement_score)
                × fragility_multiplier
                × (1 - 0.5 × red_team_epistemic_risk)
                × max(0.1, 1 - 0.15 × fudge_alert_count)
                × clamp(cooccurrence_lift, 0.75, 1.25)
        aggregate = base × penalty

    Clamped to [0.0, 1.5]. Callers use this as the single conviction
    number to drive Kelly sizing downstream.

    ``cooccurrence_lift`` comes from ``intelligence.signal_cooccurrence.
    get_lift_multiplier`` (CAT-177): pairs of firing signals that
    historically hit together get a boost; pairs that dragged each other
    down get a discount. Neutral (1.0) when fewer than two firing
    signals or no calibrated pair history.
    """
    base = 0.0
    for ev in signal_evidence:
        if ev.scorecard is None:
            base += ev.shapley_weight * 1.0  # neutral on no-history
        else:
            base += ev.shapley_weight * ev.scorecard.conviction_weight

    penalty = 1.0
    penalty *= max(0.0, 1.0 - 0.4 * max(0.0, min(1.0, disagreement_score)))
    penalty *= max(0.0, min(1.5, fragility_multiplier))
    penalty *= max(0.0, 1.0 - 0.5 * max(0.0, min(1.0, red_team_epistemic_risk)))
    penalty *= max(0.1, 1.0 - 0.15 * max(0, int(fudge_alert_count)))
    penalty *= max(0.75, min(1.25, float(cooccurrence_lift or 1.0)))
    # Closing-the-loop calibration layers — each clamped to its own range
    # by the upstream module so we only need a defensive float cast here.
    penalty *= max(0.50, min(1.10, float(confidence_bucket_multiplier or 1.0)))
    penalty *= max(0.60, min(1.15, float(scenario_multiplier or 1.0)))
    penalty *= max(0.40, min(1.00, float(null_hypothesis_penalty_value or 1.0)))
    # Second-wave amplifiers: meta-learning edge, contra-indicator crowd,
    # per-ticker squeeze loadedness, and oracle-vs-market arbitrage.
    penalty *= max(0.40, min(1.50, float(meta_learning_multiplier or 1.0)))
    penalty *= max(0.80, min(1.20, float(contra_indicator_multiplier or 1.0)))
    penalty *= max(0.85, min(1.20, float(squeeze_multiplier or 1.0)))
    penalty *= max(0.90, min(1.15, float(arbitrage_multiplier or 1.0)))
    # The dots-connector: rewards orthogonal multi-stream convergence
    # (insider + congress + whales + dark-pool + smart-money lined up).
    penalty *= max(0.90, min(1.30, float(convergence_multiplier or 1.0)))
    # 14th layer — money-flow engine: 8-layer junction-point aggregate.
    # Trade aligned with inferred capital rotation gets a boost; opposed
    # gets a haircut. Clamped hard to [0.70, 1.30].
    penalty *= max(0.70, min(1.30, float(money_flow_multiplier or 1.0)))
    # 15th layer — ReasoningBank memory prior: distilled lessons from
    # past trades / postmortems / oracle disagreements at this fingerprint.
    # Narrow range: this is a prior, not direct evidence.
    penalty *= max(
        MEMORY_LESSON_MULT_MIN,
        min(MEMORY_LESSON_MULT_MAX, float(memory_lesson_multiplier or 1.0)),
    )
    # 16th layer — EDGE multipliers from the backtest edge_table. Default
    # 1.0 means "no effect"; non-1.0 only when the caller has computed
    # the aggregate via ``intelligence.edge_signals
    # .compute_aggregate_edge_multiplier`` AND
    # ``GRID_EDGE_SIGNALS_ENABLED`` is on. Bounded the same as a single
    # edge ([EDGE_MULTIPLIER_MIN, EDGE_MULTIPLIER_MAX]) at the source so
    # we just need a defensive clamp here matching the existing layers.
    penalty *= max(0.40, min(1.80, float(edge_signal_multiplier or 1.0)))

    return max(0.0, min(1.5, base * penalty))


def _verdict_from_aggregate(conviction: float, confidence: float) -> str:
    """Classify the final trade verdict.

    Rules (deterministic):
      - aggregate conviction < 0.3 → no_trade
      - aggregate conviction < 0.7 OR raw confidence < 0.55 → low
      - aggregate conviction >= 1.15 AND confidence >= 0.7 → high
      - otherwise → medium
    """
    if conviction < 0.3:
        return "no_trade"
    if conviction < 0.7 or confidence < 0.55:
        return "low"
    if conviction >= 1.15 and confidence >= 0.7:
        return "high"
    return "medium"


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
        cooccurrence_lift = 1.0

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
        confidence_bucket_mult = 1.0

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
        scenario_mult = 1.0

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
        null_penalty = 1.0

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
        meta_mult = 1.0

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
        contra_mult = 1.0

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
        squeeze_mult = 1.0

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
        arb_mult = 1.0

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
        convergence_mult = 1.0

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
        money_flow_mult = 1.0

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
        memory_lesson_mult = 1.0

    aggregate = compute_aggregate_conviction(
        signal_evidence,
        fragility_multiplier=float(
            getattr(prediction, "fragility_multiplier", 1.0) or 1.0
        ),
        disagreement_score=float(
            getattr(prediction, "disagreement_score", 0.0) or 0.0
        ),
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

    verdict = _verdict_from_aggregate(
        aggregate,
        float(getattr(prediction, "confidence", 0.0) or 0.0),
    )

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
        fragility_multiplier=float(
            getattr(prediction, "fragility_multiplier", 1.0) or 1.0
        ),
        disagreement_score=float(
            getattr(prediction, "disagreement_score", 0.0) or 0.0
        ),
        crowd_aligned=bool(getattr(prediction, "crowd_aligned", False)),
        market_implied_prob=float(
            getattr(prediction, "market_implied_prob", 0.0) or 0.0
        ),
        red_team_epistemic_risk=float(red_team_epistemic_risk or 0.0),
        shipping_fudge_alerts=fudge_alerts,
        causation=causation,
        cooccurrence_lift=cooccurrence_lift,
        regime_calibrated_signal_count=regime_calibrated_count,
        confidence_bucket_multiplier=confidence_bucket_mult,
        scenario_multiplier=scenario_mult,
        null_hypothesis_penalty=null_penalty,
        meta_learning_multiplier=meta_mult,
        contra_indicator_multiplier=contra_mult,
        squeeze_multiplier=squeeze_mult,
        arbitrage_multiplier=arb_mult,
        convergence_multiplier=convergence_mult,
        money_flow_multiplier=money_flow_mult,
        memory_lesson_multiplier=memory_lesson_mult,
        aggregate_conviction=aggregate,
        verdict=verdict,
    )
