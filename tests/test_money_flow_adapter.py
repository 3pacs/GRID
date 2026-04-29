"""Tests for intelligence/money_flow_adapter.py — the 14th conviction adjuster."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from analysis.money_flow_engine.types import FlowEdge, FlowLayer, FlowMap, FlowNode
from intelligence.money_flow_adapter import (
    ALIGNED_MEDIUM,
    ALIGNED_STRONG,
    ALIGNED_WEAK,
    LAYERS_MEDIUM,
    LAYERS_STRONG,
    LAYERS_WEAK,
    MIN_LAYERS_FOR_SCORING,
    MULTIPLIER_CEILING,
    MULTIPLIER_FLOOR,
    NEUTRAL_MULTIPLIER,
    OPPOSED_MEDIUM,
    OPPOSED_STRONG,
    OPPOSED_WEAK,
    SCORE_MEDIUM,
    SCORE_STRONG,
    SCORE_WEAK,
    MoneyFlowConvictionReport,
    _aggregate_edges,
    _confidence_weight,
    _directional_score,
    _multiplier_for,
    _normalize_direction,
    compute_money_flow_conviction,
    money_flow_conviction_multiplier,
)


# ── Fixtures / builders ──────────────────────────────────────────────────


def _node(layer_id: str, node_id: str, confidence: str = "derived") -> FlowNode:
    return FlowNode(
        id=node_id,
        label=node_id.replace("_", " ").title(),
        layer=layer_id,
        value=1.0e12,
        change_1m=0.02,
        confidence=confidence,
    )


def _layer(layer_id: str, order: int, node_count: int = 2) -> FlowLayer:
    return FlowLayer(
        id=layer_id,
        label=layer_id.title(),
        order=order,
        nodes=tuple(_node(layer_id, f"{layer_id}_n{i}") for i in range(node_count)),
        confidence="derived",
    )


def _empty_layer(layer_id: str, order: int) -> FlowLayer:
    return FlowLayer(
        id=layer_id,
        label=layer_id.title(),
        order=order,
        nodes=(),
        confidence="estimated",
    )


def _edge(
    source_layer: str,
    target_layer: str,
    direction: str,
    value_usd: float = 1.0e9,
    confidence: str = "derived",
) -> FlowEdge:
    return FlowEdge(
        source_node=f"{source_layer}_n0",
        target_node=f"{target_layer}_n0",
        source_layer=source_layer,
        target_layer=target_layer,
        value_usd=value_usd,
        direction=direction,
        confidence=confidence,
        label=f"{source_layer}->{target_layer}",
        channel="test_channel",
    )


def _flow_map(layers: tuple[FlowLayer, ...], edges: tuple[FlowEdge, ...]) -> FlowMap:
    return FlowMap(
        layers=layers,
        edges=edges,
        generated_at="2026-04-14T00:00:00+00:00",
    )


# ── Pure-helper unit tests ───────────────────────────────────────────────


class TestNormalizeDirection:
    def test_bullish_variants(self):
        for raw in ("bullish", "long", "UP", " buy "):
            assert _normalize_direction(raw) == "bullish"

    def test_bearish_variants(self):
        for raw in ("bearish", "short", "DOWN", "sell"):
            assert _normalize_direction(raw) == "bearish"

    def test_neutral_variants(self):
        for raw in ("neutral", "flat", "", "none"):
            assert _normalize_direction(raw) == "neutral"

    def test_unknown_direction(self):
        assert _normalize_direction("sideways") == "unknown"


class TestConfidenceWeight:
    def test_known_labels(self):
        assert _confidence_weight("confirmed") == 1.00
        assert _confidence_weight("derived") == 0.75
        assert _confidence_weight("estimated") == 0.50
        assert _confidence_weight("rumored") == 0.25

    def test_unknown_label_defaults(self):
        assert _confidence_weight("mystery") == 0.50
        assert _confidence_weight(None) == 0.50


class TestDirectionalScore:
    def test_all_bullish(self):
        assert _directional_score(100.0, 0.0) == pytest.approx(1.0)

    def test_all_bearish(self):
        assert _directional_score(0.0, 100.0) == pytest.approx(-1.0)

    def test_balanced(self):
        assert _directional_score(50.0, 50.0) == pytest.approx(0.0)

    def test_zero_safe(self):
        assert _directional_score(0.0, 0.0) == pytest.approx(0.0)


class TestAggregateEdges:
    def test_mixed_directions(self):
        edges = (
            _edge("a", "b", "inflow", value_usd=2.0e9, confidence="confirmed"),
            _edge("b", "c", "outflow", value_usd=1.0e9, confidence="derived"),
            _edge("c", "d", "neutral", value_usd=5.0e9, confidence="confirmed"),
        )
        bull, bear, bull_n, bear_n = _aggregate_edges(edges)
        assert bull_n == 1
        assert bear_n == 1
        # Bullish weight = 2e9 * 1.0
        assert bull == pytest.approx(2.0e9)
        # Bearish weight = 1e9 * 0.75
        assert bear == pytest.approx(7.5e8)

    def test_empty_edges(self):
        assert _aggregate_edges(()) == (0.0, 0.0, 0, 0)


class TestMultiplierFor:
    def test_below_min_layers_neutral(self):
        mult = _multiplier_for(
            directional_score=0.9, layers_evaluated=2, trade_direction="bullish",
        )
        assert mult == NEUTRAL_MULTIPLIER

    def test_aligned_strong(self):
        mult = _multiplier_for(
            directional_score=0.6, layers_evaluated=6, trade_direction="bullish",
        )
        assert mult == ALIGNED_STRONG

    def test_opposed_strong(self):
        mult = _multiplier_for(
            directional_score=0.6, layers_evaluated=6, trade_direction="bearish",
        )
        assert mult == OPPOSED_STRONG

    def test_aligned_medium(self):
        mult = _multiplier_for(
            directional_score=0.35, layers_evaluated=4, trade_direction="bearish",
        )
        # score is positive → flow is bullish → bearish trade is OPPOSED
        assert mult == OPPOSED_MEDIUM

    def test_aligned_weak(self):
        mult = _multiplier_for(
            directional_score=0.20, layers_evaluated=3, trade_direction="bullish",
        )
        assert mult == ALIGNED_WEAK

    def test_tiny_score_neutral(self):
        mult = _multiplier_for(
            directional_score=0.05, layers_evaluated=8, trade_direction="bullish",
        )
        assert mult == NEUTRAL_MULTIPLIER

    def test_unknown_direction_neutral(self):
        mult = _multiplier_for(
            directional_score=0.9, layers_evaluated=8, trade_direction="spacewalk",
        )
        assert mult == NEUTRAL_MULTIPLIER

    def test_neutral_direction_returns_neutral(self):
        mult = _multiplier_for(
            directional_score=0.9, layers_evaluated=8, trade_direction="neutral",
        )
        assert mult == NEUTRAL_MULTIPLIER


# ── compute_money_flow_conviction integration ────────────────────────────


class TestComputeMoneyFlowConviction:
    def test_all_eight_layers_bullish_aligned_strong(self):
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        # 6 bullish (inflow) edges at confirmed, 4 bearish at derived
        edges = tuple(
            [_edge("monetary", "credit", "inflow", value_usd=5.0e9, confidence="confirmed")
             for _ in range(6)]
            + [_edge("monetary", "credit", "outflow", value_usd=1.0e9, confidence="derived")
               for _ in range(4)]
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert report.layers_evaluated == 8
        assert report.total_edges == 10
        assert report.net_bullish_edges == 6
        assert report.net_bearish_edges == 4
        assert report.directional_score > SCORE_STRONG
        assert report.conviction_multiplier == ALIGNED_STRONG

    def test_all_eight_layers_bearish_dominant_opposed_strong(self):
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        edges = tuple(
            [_edge("monetary", "credit", "outflow", value_usd=5.0e9, confidence="confirmed")
             for _ in range(6)]
            + [_edge("monetary", "credit", "inflow", value_usd=1.0e9, confidence="derived")
               for _ in range(4)]
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert report.layers_evaluated == 8
        assert report.directional_score < -SCORE_STRONG
        assert report.conviction_multiplier == OPPOSED_STRONG

    def test_three_layers_weak_aligned(self):
        layers = (
            _layer("monetary", 0),
            _layer("credit", 1),
            _layer("market", 2),
            _empty_layer("institutional", 3),
            _empty_layer("corporate", 4),
            _empty_layer("sovereign", 5),
            _empty_layer("retail", 6),
            _empty_layer("crypto", 7),
        )
        # Small directional imbalance → score magnitude around 0.20.
        # (55 bull - 45 bear) / 100 = 0.10 ... let's tune to land in weak band.
        bulls = [_edge("monetary", "credit", "inflow", value_usd=6.0e8, confidence="estimated")
                 for _ in range(6)]
        bears = [_edge("credit", "market", "outflow", value_usd=4.0e8, confidence="estimated")
                 for _ in range(4)]
        # weights: bull = 6*6e8*0.5 = 1.8e9 ; bear = 4*4e8*0.5 = 0.8e9
        # score = (1.8 - 0.8) / 2.6 ≈ 0.385 — that'll hit medium band with 3 layers,
        # which resolves to weak (medium needs layers>=4).
        edges = tuple(bulls + bears)
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert report.layers_evaluated == 3
        assert report.directional_score >= SCORE_WEAK
        # Only 3 layers → caps at weak tier regardless of score magnitude
        assert report.conviction_multiplier == ALIGNED_WEAK

    def test_two_layers_below_threshold_neutral(self):
        layers = (
            _layer("monetary", 0),
            _layer("credit", 1),
            _empty_layer("institutional", 2),
            _empty_layer("market", 3),
            _empty_layer("corporate", 4),
            _empty_layer("sovereign", 5),
            _empty_layer("retail", 6),
            _empty_layer("crypto", 7),
        )
        edges = (
            _edge("monetary", "credit", "inflow", value_usd=9e9, confidence="confirmed"),
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert report.layers_evaluated == 2
        # Below MIN_LAYERS_FOR_SCORING → neutral regardless of score
        assert report.conviction_multiplier == NEUTRAL_MULTIPLIER

    def test_all_layers_empty_neutral_report(self):
        layers = tuple(
            _empty_layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, ()),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert report.layers_evaluated == 0
        assert report.total_edges == 0
        assert report.conviction_multiplier == NEUTRAL_MULTIPLIER

    def test_build_flow_map_raises_neutral_report(self):
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            side_effect=RuntimeError("db blew up"),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert report.layers_evaluated == 0
        assert report.conviction_multiplier == NEUTRAL_MULTIPLIER
        assert "build_flow_map" in report.advisory

    def test_unknown_trade_direction_neutral(self):
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        edges = tuple(
            _edge("monetary", "credit", "inflow", value_usd=5.0e9, confidence="confirmed")
            for _ in range(6)
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="sideways",
            )
        # Flow says strongly bullish, but direction is unknown → neutral
        assert report.conviction_multiplier == NEUTRAL_MULTIPLIER

    def test_multiplier_never_exceeds_bounds(self):
        # Force a pathological score via a tiny weight imbalance at max layers
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        edges = tuple(
            _edge("monetary", "credit", "inflow", value_usd=1e12, confidence="confirmed")
            for _ in range(20)
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert MULTIPLIER_FLOOR <= report.conviction_multiplier <= MULTIPLIER_CEILING

    def test_layer_summaries_populated(self):
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        edges = (
            _edge("monetary", "credit", "inflow"),
            _edge("credit", "market", "inflow"),
            _edge("market", "retail", "outflow"),
        )
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            report = compute_money_flow_conviction(
                engine, as_of=date(2026, 4, 14), direction="bullish",
            )
        assert set(report.layer_summaries.keys()) == {
            "monetary", "credit", "institutional", "market",
            "corporate", "sovereign", "retail", "crypto",
        }
        assert report.layer_summaries["credit"]["edge_count"] == 2


# ── to_dict round-trip ───────────────────────────────────────────────────


class TestToDict:
    def test_round_trip_contains_every_field(self):
        report = MoneyFlowConvictionReport(
            as_of="2026-04-14T00:00:00+00:00",
            layers_evaluated=5,
            total_edges=12,
            net_bullish_edges=8,
            net_bearish_edges=4,
            directional_score=0.45,
            conviction_multiplier=1.15,
            advisory="money_flow: bullish net flow",
            layer_summaries={
                "monetary": {"node_count": 3, "edge_count": 2, "net_direction": "bullish"},
            },
        )
        d = report.to_dict()
        for key in (
            "as_of", "layers_evaluated", "total_edges",
            "net_bullish_edges", "net_bearish_edges",
            "directional_score", "conviction_multiplier",
            "advisory", "layer_summaries",
        ):
            assert key in d
        assert d["layers_evaluated"] == 5
        assert d["total_edges"] == 12
        assert d["conviction_multiplier"] == pytest.approx(1.15)
        assert d["directional_score"] == pytest.approx(0.45)
        assert d["layer_summaries"]["monetary"]["net_direction"] == "bullish"


# ── money_flow_conviction_multiplier (live-path entry) ───────────────────


class TestLivePathMultiplier:
    def test_returns_float(self):
        engine = MagicMock()
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        edges = tuple(
            _edge("monetary", "credit", "inflow", value_usd=5e9, confidence="confirmed")
            for _ in range(8)
        )
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            result = money_flow_conviction_multiplier(
                engine, as_of=date(2026, 4, 14), trade_direction="bullish",
            )
        assert isinstance(result, float)
        assert result == ALIGNED_STRONG

    def test_engine_exception_returns_neutral(self):
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            side_effect=ValueError("db outage"),
        ):
            result = money_flow_conviction_multiplier(
                engine, as_of=date(2026, 4, 14), trade_direction="bullish",
            )
        assert result == NEUTRAL_MULTIPLIER

    def test_compute_raises_still_returns_neutral(self):
        """If compute_money_flow_conviction itself raises (belt-and-braces),
        the live-path wrapper still returns neutral 1.0."""
        engine = MagicMock()
        with patch(
            "intelligence.money_flow_adapter.compute_money_flow_conviction",
            side_effect=RuntimeError("unexpected"),
        ):
            result = money_flow_conviction_multiplier(
                engine, as_of=date(2026, 4, 14), trade_direction="bullish",
            )
        assert result == NEUTRAL_MULTIPLIER

    def test_neutral_direction_input(self):
        engine = MagicMock()
        layers = tuple(
            _layer(name, i) for i, name in enumerate([
                "monetary", "credit", "institutional", "market",
                "corporate", "sovereign", "retail", "crypto",
            ])
        )
        edges = tuple(
            _edge("monetary", "credit", "inflow", value_usd=5e9, confidence="confirmed")
            for _ in range(8)
        )
        with patch(
            "intelligence.money_flow_adapter.build_flow_map",
            return_value=_flow_map(layers, edges),
        ):
            result = money_flow_conviction_multiplier(
                engine, as_of=date(2026, 4, 14), trade_direction="neutral",
            )
        assert result == NEUTRAL_MULTIPLIER
