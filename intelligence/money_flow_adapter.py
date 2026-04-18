"""Money flow conviction adapter — the 14th live-stack multiplier.

Wraps ``analysis.money_flow_engine.build_flow_map`` (the 8-layer junction-point
inference engine) and collapses its inferred flow edges into a single
conviction multiplier in ``[0.70, 1.30]``.

The adapter is defensive by design:

* Every call to ``build_flow_map`` is wrapped in try/except — any upstream
  failure returns a neutral report with ``conviction_multiplier = 1.0``.
* Per-layer success is counted from the FlowMap's returned layers (empty
  nodes = failed layer, already swallowed by the engine). A build is only
  "evaluated" if it produced at least one node.
* ``money_flow_conviction_multiplier`` — the live-path entry point used by
  ``intelligence.signal_provenance.build_provenance_report`` — never raises.

Directional scoring walks every ``FlowEdge`` returned by
``infer_flow_edges`` (driven by the hardcoded ``FLOW_CHANNELS`` graph) and
tallies bullish vs bearish weight using ``value_usd * confidence_weight``:

* ``direction == "inflow"``  → bullish (capital rotating into the target)
* ``direction == "outflow"`` → bearish (capital rotating back to source)
* ``direction == "neutral"`` → excluded from the tally

The bullish/bearish delta is normalized to ``[-1.0, +1.0]`` via
``(bullish - bearish) / max(bullish + bearish, eps)`` and mapped to the
final multiplier against the caller's trade direction.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from analysis.money_flow_engine import build_flow_map
from analysis.money_flow_engine.types import FlowEdge, FlowLayer, FlowMap


# ── Module constants ─────────────────────────────────────────────────────

# Confidence-string → numeric weight. Mirrors the ``_CONF_RANK`` ordering
# in ``flow_inference.py`` but maps it to a multiplicative weight so the
# adapter can combine confidence with ``value_usd``.
_CONFIDENCE_WEIGHT: dict[str, float] = {
    "confirmed": 1.00,
    "derived":   0.75,
    "estimated": 0.50,
    "rumored":   0.25,
    "inferred":  0.50,
}
_DEFAULT_CONFIDENCE_WEIGHT: float = 0.50

# Directional-score normalization guard (avoid divide-by-zero).
_EPS: float = 1e-9

# Multiplier bounds (hard clamp at the penalty stack as well).
MULTIPLIER_FLOOR: float = 0.70
MULTIPLIER_CEILING: float = 1.30
NEUTRAL_MULTIPLIER: float = 1.00

# Coverage floor — below this many successful layers, we refuse to score.
MIN_LAYERS_FOR_SCORING: int = 3

# Score magnitude tiers (by |directional_score|).
SCORE_STRONG: float = 0.50
SCORE_MEDIUM: float = 0.30
SCORE_WEAK:   float = 0.15

# Layer-count tiers (matched against score tier).
LAYERS_STRONG: int = 5
LAYERS_MEDIUM: int = 4
LAYERS_WEAK:   int = 3

# Aligned multipliers (trade direction matches the sign of directional_score).
ALIGNED_STRONG: float = 1.30
ALIGNED_MEDIUM: float = 1.15
ALIGNED_WEAK:   float = 1.08

# Opposed multipliers (trade direction opposes the sign of directional_score).
OPPOSED_STRONG: float = 0.70
OPPOSED_MEDIUM: float = 0.85
OPPOSED_WEAK:   float = 0.92

# Accepted direction tokens.
_BULLISH_DIRECTIONS: frozenset[str] = frozenset({"bullish", "long", "up", "buy"})
_BEARISH_DIRECTIONS: frozenset[str] = frozenset({"bearish", "short", "down", "sell"})
_NEUTRAL_DIRECTIONS: frozenset[str] = frozenset({"neutral", "flat", "none", ""})


# ── Report dataclass ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class MoneyFlowConvictionReport:
    """Immutable result of a money-flow conviction walk."""

    as_of: str
    layers_evaluated: int
    total_edges: int
    net_bullish_edges: int
    net_bearish_edges: int
    directional_score: float
    conviction_multiplier: float
    advisory: str
    layer_summaries: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "layers_evaluated": int(self.layers_evaluated),
            "total_edges": int(self.total_edges),
            "net_bullish_edges": int(self.net_bullish_edges),
            "net_bearish_edges": int(self.net_bearish_edges),
            "directional_score": round(float(self.directional_score), 4),
            "conviction_multiplier": round(float(self.conviction_multiplier), 4),
            "advisory": self.advisory,
            "layer_summaries": dict(self.layer_summaries),
        }


# ── Pure helpers ─────────────────────────────────────────────────────────


def _normalize_direction(raw: str) -> str:
    """Canonicalize a trade direction string to 'bullish'/'bearish'/'neutral'."""
    token = (raw or "").strip().lower()
    if token in _BULLISH_DIRECTIONS:
        return "bullish"
    if token in _BEARISH_DIRECTIONS:
        return "bearish"
    if token in _NEUTRAL_DIRECTIONS:
        return "neutral"
    return "unknown"


def _confidence_weight(confidence: str | None) -> float:
    """Map a confidence label to a numeric weight in [0, 1]."""
    return _CONFIDENCE_WEIGHT.get((confidence or "").lower(), _DEFAULT_CONFIDENCE_WEIGHT)


def _count_evaluated_layers(layers: tuple[FlowLayer, ...]) -> int:
    """A layer "counts" only if it produced at least one node."""
    return sum(1 for la in layers if la.nodes)


def _aggregate_edges(edges: tuple[FlowEdge, ...]) -> tuple[float, float, int, int]:
    """Walk edges, return (bullish_weighted, bearish_weighted, bull_count, bear_count)."""
    bullish_weight = 0.0
    bearish_weight = 0.0
    bull_count = 0
    bear_count = 0
    for edge in edges:
        direction = (edge.direction or "").lower()
        if direction == "inflow":
            weight = abs(float(edge.value_usd or 0.0)) * _confidence_weight(edge.confidence)
            bullish_weight += weight
            bull_count += 1
        elif direction == "outflow":
            weight = abs(float(edge.value_usd or 0.0)) * _confidence_weight(edge.confidence)
            bearish_weight += weight
            bear_count += 1
        # neutral edges are intentionally excluded
    return bullish_weight, bearish_weight, bull_count, bear_count


def _summarize_layers(
    layers: tuple[FlowLayer, ...],
    edges: tuple[FlowEdge, ...],
) -> dict[str, dict[str, Any]]:
    """Per-layer summary of edge counts and net direction."""
    summary: dict[str, dict[str, Any]] = {}
    for la in layers:
        layer_edges = [
            e for e in edges
            if e.source_layer == la.id or e.target_layer == la.id
        ]
        bulls = sum(1 for e in layer_edges if (e.direction or "").lower() == "inflow")
        bears = sum(1 for e in layer_edges if (e.direction or "").lower() == "outflow")
        if bulls > bears:
            net = "bullish"
        elif bears > bulls:
            net = "bearish"
        else:
            net = "neutral"
        summary[la.id] = {
            "node_count": len(la.nodes),
            "edge_count": len(layer_edges),
            "bullish_edges": bulls,
            "bearish_edges": bears,
            "net_direction": net,
            "confidence": la.confidence,
        }
    return summary


def _directional_score(bullish: float, bearish: float) -> float:
    """Normalize bullish/bearish weights to [-1.0, +1.0]."""
    denominator = max(bullish + bearish, _EPS)
    return (bullish - bearish) / denominator


def _multiplier_for(
    *,
    directional_score: float,
    layers_evaluated: int,
    trade_direction: str,
) -> float:
    """Map (score, layer_count, direction) → conviction multiplier."""
    if layers_evaluated < MIN_LAYERS_FOR_SCORING:
        return NEUTRAL_MULTIPLIER

    canonical = _normalize_direction(trade_direction)
    if canonical in {"neutral", "unknown"}:
        return NEUTRAL_MULTIPLIER

    # Score sign → implied flow direction.
    if directional_score > 0:
        flow_direction = "bullish"
    elif directional_score < 0:
        flow_direction = "bearish"
    else:
        return NEUTRAL_MULTIPLIER

    aligned = canonical == flow_direction
    magnitude = abs(directional_score)

    if magnitude >= SCORE_STRONG and layers_evaluated >= LAYERS_STRONG:
        return ALIGNED_STRONG if aligned else OPPOSED_STRONG
    if magnitude >= SCORE_MEDIUM and layers_evaluated >= LAYERS_MEDIUM:
        return ALIGNED_MEDIUM if aligned else OPPOSED_MEDIUM
    if magnitude >= SCORE_WEAK and layers_evaluated >= LAYERS_WEAK:
        return ALIGNED_WEAK if aligned else OPPOSED_WEAK
    return NEUTRAL_MULTIPLIER


def _advisory_text(
    *,
    layers_evaluated: int,
    directional_score: float,
    multiplier: float,
    trade_direction: str,
) -> str:
    """Build a one-line human-readable advisory."""
    if layers_evaluated < MIN_LAYERS_FOR_SCORING:
        return (
            f"money_flow: insufficient layer coverage ({layers_evaluated} layers) "
            f"— multiplier held at neutral {NEUTRAL_MULTIPLIER:.2f}"
        )
    sign = "bullish" if directional_score > 0 else ("bearish" if directional_score < 0 else "flat")
    return (
        f"money_flow: {sign} net flow (score={directional_score:+.2f}) across "
        f"{layers_evaluated} layers → {trade_direction} trade multiplier {multiplier:.2f}"
    )


def _neutral_report(as_of: date, reason: str) -> MoneyFlowConvictionReport:
    """Build a defensive neutral-1.0 report (used on any upstream failure)."""
    return MoneyFlowConvictionReport(
        as_of=datetime.combine(as_of, datetime.min.time(), tzinfo=timezone.utc).date().isoformat(),
        layers_evaluated=0,
        total_edges=0,
        net_bullish_edges=0,
        net_bearish_edges=0,
        directional_score=0.0,
        conviction_multiplier=NEUTRAL_MULTIPLIER,
        advisory=f"money_flow: {reason} → multiplier held at neutral {NEUTRAL_MULTIPLIER:.2f}",
        layer_summaries={},
    )


# ── Public API ───────────────────────────────────────────────────────────


def compute_money_flow_conviction(
    engine: Engine,
    *,
    as_of: date,
    direction: str,
) -> MoneyFlowConvictionReport:
    """Walk all 8 money-flow layers, infer edges, and return a conviction multiplier.

    Never raises. On any exception, returns a neutral 1.00 report so the
    live conviction stack is protected from upstream failures.
    """
    try:
        flow_map: FlowMap = build_flow_map(engine, as_of=as_of)
    except Exception as exc:  # noqa: BLE001 — live path, never raise
        log.debug("money_flow_adapter: build_flow_map failed: {e}", e=str(exc))
        return _neutral_report(as_of, reason=f"build_flow_map raised ({type(exc).__name__})")

    layers = flow_map.layers or ()
    edges = flow_map.edges or ()

    layers_evaluated = _count_evaluated_layers(layers)
    bullish_weight, bearish_weight, bull_count, bear_count = _aggregate_edges(edges)
    directional_score = _directional_score(bullish_weight, bearish_weight)

    multiplier = _multiplier_for(
        directional_score=directional_score,
        layers_evaluated=layers_evaluated,
        trade_direction=direction,
    )
    # Defensive clamp (already bounded by the thresholds but re-clamp for safety).
    multiplier = max(MULTIPLIER_FLOOR, min(MULTIPLIER_CEILING, float(multiplier)))

    summaries = _summarize_layers(layers, edges)
    advisory = _advisory_text(
        layers_evaluated=layers_evaluated,
        directional_score=directional_score,
        multiplier=multiplier,
        trade_direction=direction,
    )

    as_of_iso = (
        flow_map.generated_at
        or datetime.combine(as_of, datetime.min.time(), tzinfo=timezone.utc).isoformat()
    )

    return MoneyFlowConvictionReport(
        as_of=as_of_iso,
        layers_evaluated=layers_evaluated,
        total_edges=len(edges),
        net_bullish_edges=bull_count,
        net_bearish_edges=bear_count,
        directional_score=directional_score,
        conviction_multiplier=multiplier,
        advisory=advisory,
        layer_summaries=summaries,
    )


def money_flow_conviction_multiplier(
    engine: Engine,
    *,
    as_of: date,
    trade_direction: str,
) -> float:
    """Live-path convenience wrapper. Returns 1.0 on any failure. Never raises.

    This is the entry point imported by ``intelligence.signal_provenance.
    build_provenance_report`` — it MUST be cheap, defensive, and silent on
    failure so the conviction stack can never be broken by a money-flow fault.
    """
    try:
        report = compute_money_flow_conviction(
            engine, as_of=as_of, direction=trade_direction,
        )
        return float(report.conviction_multiplier)
    except Exception as exc:  # noqa: BLE001 — live path, never raise
        log.debug(
            "money_flow_adapter: conviction multiplier lookup failed: {e}", e=str(exc),
        )
        return NEUTRAL_MULTIPLIER
