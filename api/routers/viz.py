"""
GRID Visualization Intelligence API.

Endpoints that return VizSpec objects — complete rendering instructions
for living graphs. The frontend takes a VizSpec and renders it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query
from loguru import logger as log

from analysis.viz_intelligence import (
    ChartType,
    VizSpec,
    AnimationConfig,
    WeightSchedule,
    WEIGHT_SCHEDULES,
    VISUALIZATION_RULES,
    compute_source_weights,
    get_all_rules,
    select_visualization,
)

router = APIRouter(prefix="/api/v1/viz", tags=["visualization"])


@router.get("/recommend")
async def recommend_visualization(
    description: str = Query(..., description="What data you want to visualize"),
    features: str = Query("", description="Comma-separated feature names"),
    relation: str = Query("", description="Relationship type: flow, correlation, causation, etc."),
    question: str = Query("", description="The question being answered"),
) -> dict[str, Any]:
    """Recommend the optimal visualization for a data pattern.

    Returns a VizSpec that the frontend can render directly.
    """
    feature_list = [f.strip() for f in features.split(",") if f.strip()] if features else None
    spec = select_visualization(
        data_description=description,
        features=feature_list,
        relation=relation or None,
        question=question or None,
    )
    return {
        "spec": spec.to_dict(),
        "reasoning": next(
            (r["why"] for r in VISUALIZATION_RULES
             if r["chart"].value == spec.chart_type.value),
            "Default time series visualization.",
        ),
    }


@router.get("/rules")
async def list_visualization_rules() -> list[dict]:
    """Return all visualization intelligence rules.

    Each rule explains: when to use this chart type, and WHY
    it's the best expression of that data pattern.
    """
    return get_all_rules()


@router.get("/weights")
async def get_source_weights(
    families: str = Query("", description="Comma-separated family names (empty = all)"),
) -> dict[str, Any]:
    """Get current visual weights for data families.

    Weights reflect freshness: real-time data = 1.0, stale monthly data = 0.2.
    The frontend uses these to modulate opacity, size, and pulse rate.
    """
    if families:
        family_list = [f.strip() for f in families.split(",")]
    else:
        family_list = list(WEIGHT_SCHEDULES.keys())

    weights = compute_source_weights(family_list)
    schedules = {
        f: {
            "weight": weights.get(f, 0.5),
            "cadence": WEIGHT_SCHEDULES[f].cadence if f in WEIGHT_SCHEDULES else "unknown",
            "pulse_on_update": WEIGHT_SCHEDULES[f].pulse_on_update if f in WEIGHT_SCHEDULES else False,
            "half_life_hours": WEIGHT_SCHEDULES[f].freshness_half_life_hours if f in WEIGHT_SCHEDULES else 24,
        }
        for f in family_list
    }
    return {"weights": weights, "schedules": schedules}


@router.get("/spec/capital-flows")
async def capital_flow_viz_spec() -> dict:
    """Pre-built VizSpec for the capital flow living graph."""
    spec = VizSpec(
        chart_type=ChartType.SANKEY_TEMPORAL,
        title="Capital Flow",
        subtitle="Where money is moving — scrub time to see the story unfold",
        data_endpoint="/api/v1/flows/sankey",
        time_field="as_of",
        time_scrubber=True,
        time_range="3M",
        color_field="direction",
        size_field="magnitude",
        regime_bands=True,
        animation=AnimationConfig(
            transition_ms=400,
            auto_play=True,
            play_speed_ms=300,
            trail_opacity=0.15,
        ),
        weight_schedules=[
            WEIGHT_SCHEDULES["equity"],
            WEIGHT_SCHEDULES["flows"],
            WEIGHT_SCHEDULES["options"],
        ],
    )
    return spec.to_dict()


@router.get("/spec/regime-phase")
async def regime_phase_viz_spec() -> dict:
    """Pre-built VizSpec for the regime phase space trajectory."""
    spec = VizSpec(
        chart_type=ChartType.PHASE_SPACE,
        title="Regime Phase Space",
        subtitle="Market trajectory through state space — attractors are regime centers",
        data_endpoint="/api/v1/regime/trajectory",
        x_field="pc1",
        y_fields=["pc2"],
        color_field="regime_state",
        size_field="confidence",
        time_scrubber=True,
        time_range="1Y",
        animation=AnimationConfig(
            transition_ms=300,
            trail_opacity=0.2,
            trail_length=30,
            auto_play=True,
            play_speed_ms=150,
        ),
        weight_schedules=[WEIGHT_SCHEDULES["regime"]],
    )
    return spec.to_dict()


@router.get("/spec/feature-network")
async def feature_network_viz_spec() -> dict:
    """Pre-built VizSpec for the feature correlation/importance network."""
    spec = VizSpec(
        chart_type=ChartType.FORCE_NETWORK,
        title="Feature Intelligence Network",
        subtitle="Node size = importance. Edge thickness = correlation. Layout shifts with regime.",
        data_endpoint="/api/v1/discovery/smart-heatmap",
        size_field="importance",
        weight_field="correlation",
        color_field="family",
        group_field="family",
        time_scrubber=True,
        animation=AnimationConfig(
            transition_ms=800,
            stagger_ms=20,
        ),
        weight_schedules=[
            WEIGHT_SCHEDULES[f] for f in ["rates", "credit", "vol", "equity", "macro"]
        ],
    )
    return spec.to_dict()


@router.get("/spec/energy-particle")
async def energy_particle_viz_spec() -> dict:
    """Pre-built VizSpec for the market energy particle system."""
    spec = VizSpec(
        chart_type=ChartType.PARTICLE_SYSTEM,
        title="Market Energy Field",
        subtitle="Kinetic energy (momentum) vs potential energy (deviation). Conservation violations glow red.",
        data_endpoint="/api/v1/physics/dashboard",
        x_field="kinetic_energy",
        y_fields=["potential_energy"],
        color_field="energy_level",
        size_field="total_energy",
        animation=AnimationConfig(
            transition_ms=200,
            trail_opacity=0.1,
            trail_length=15,
            pulse_duration_ms=500,
        ),
        weight_schedules=[
            WEIGHT_SCHEDULES["physics"],
            WEIGHT_SCHEDULES["news_energy"],
        ],
    )
    return spec.to_dict()


@router.get("/spec/sector-orbital")
async def sector_orbital_viz_spec() -> dict:
    """Pre-built VizSpec for the sector rotation orbital diagram."""
    spec = VizSpec(
        chart_type=ChartType.ORBITAL,
        title="Sector Rotation Orbit",
        subtitle="Sectors orbit SPY. Distance = relative performance. Trail shows rotation history.",
        data_endpoint="/api/v1/flows/sectors",
        color_field="signal",
        size_field="volume",
        label_field="etf",
        time_scrubber=True,
        time_range="6M",
        animation=AnimationConfig(
            trail_opacity=0.25,
            trail_length=60,
            auto_play=True,
            play_speed_ms=200,
        ),
        weight_schedules=[
            WEIGHT_SCHEDULES["equity"],
            WEIGHT_SCHEDULES["flows"],
        ],
    )
    return spec.to_dict()


@router.get("/spec/lead-lag-river")
async def lead_lag_river_viz_spec() -> dict:
    """Pre-built VizSpec for lead/lag causal river flow."""
    spec = VizSpec(
        chart_type=ChartType.RIVER_FLOW,
        title="Causal River",
        subtitle="Upstream features signal before downstream reacts. Width = correlation strength.",
        data_endpoint="/api/v1/associations/lag-analysis",
        size_field="correlation",
        color_field="lag_days",
        time_scrubber=True,
        animation=AnimationConfig(
            transition_ms=600,
            auto_play=True,
        ),
    )
    return spec.to_dict()
