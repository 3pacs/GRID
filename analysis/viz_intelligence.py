"""
GRID Visualization Intelligence Engine.

Selects the optimal chart type for any data pattern based on:
  - Data shape (time series, cross-section, hierarchical, network)
  - Dimensionality (univariate, bivariate, multivariate)
  - Temporal dynamics (static snapshot, evolving, regime-dependent)
  - Relationship type (flow, correlation, causation, composition)
  - User context (what question is being asked)

Each VizSpec encodes: chart type, data bindings, weight schedule,
animation config, and interaction model. The frontend renders
any VizSpec into a living graph.

Learned rules (encoded from discovery):
  - Capital flows → Sankey + TIME SCRUBBER (flow direction changes over time)
  - Regime transitions → animated phase space (states are attractors)
  - Feature importance → force-directed network (weights = spring constants)
  - Correlation structure → chord diagram with temporal decay
  - Energy dynamics → particle system (KE/PE as motion/position)
  - Sector rotation → orbital diagram (sectors orbit a center of gravity)
  - Lead/lag relationships → river flow (upstream features lead downstream)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from enum import Enum
from typing import Any


# ── Chart Type Taxonomy ─────────────────────────────────────────────────────
# Each type maps to a D3/React renderer in the frontend

class ChartType(str, Enum):
    # Time-domain
    SPARKLINE = "sparkline"                    # Single feature, compact
    MULTI_LINE = "multi_line"                  # Multiple features overlaid
    AREA_STACK = "area_stack"                  # Composition over time
    CANDLESTICK = "candlestick"               # OHLCV price action
    HORIZON = "horizon"                        # Dense multi-series comparison

    # Flow & hierarchy
    SANKEY_TEMPORAL = "sankey_temporal"         # Flow with time scrubber
    TREEMAP = "treemap"                        # Hierarchical composition
    SUNBURST = "sunburst"                      # Nested drill-down

    # Network & relationship
    FORCE_NETWORK = "force_network"            # Correlation/causation graph
    CHORD = "chord"                            # Pairwise flow between entities
    ARC_DIAGRAM = "arc_diagram"                # Ordered nodes with curved links

    # Distribution & comparison
    HEATMAP = "heatmap"                        # Dense cross-section grid
    RIDGELINE = "ridgeline"                    # Overlapping distributions over time
    BEESWARM = "beeswarm"                      # Individual points clustered

    # Physics & dynamics
    PHASE_SPACE = "phase_space"                # 2D/3D state trajectory
    PARTICLE_SYSTEM = "particle_system"        # Energy as motion
    ORBITAL = "orbital"                        # Sectors orbiting center of gravity
    RIVER_FLOW = "river_flow"                  # Lead/lag causal stream

    # Gauges & indicators
    GAUGE = "gauge"                            # Single metric, semicircle
    THERMOMETER = "thermometer"                # Position on gradient
    BULLET = "bullet"                          # Target vs actual

    # Composite
    SMALL_MULTIPLES = "small_multiples"        # Grid of mini charts
    DASHBOARD_GRID = "dashboard_grid"          # Mixed chart types in grid


# ── Data Shape Classification ───────────────────────────────────────────────

class DataShape(str, Enum):
    TIME_SERIES = "time_series"               # Values indexed by date
    CROSS_SECTION = "cross_section"           # Values at a single point
    PANEL = "panel"                           # Time series × entities
    HIERARCHICAL = "hierarchical"             # Tree/nested structure
    NETWORK = "network"                       # Nodes + edges
    DISTRIBUTION = "distribution"             # Statistical spread


class RelationType(str, Enum):
    FLOW = "flow"                             # Capital, energy, information
    CORRELATION = "correlation"               # Statistical co-movement
    CAUSATION = "causation"                   # Lead/lag, Granger
    COMPOSITION = "composition"               # Parts of a whole
    COMPARISON = "comparison"                 # Side-by-side ranking
    TRAJECTORY = "trajectory"                 # Path through state space
    DIVERGENCE = "divergence"                 # Spreading apart over time


# ── Weight Schedule ─────────────────────────────────────────────────────────
# Different data sources update at different cadences. The visualization
# should reflect this: real-time data pulses, weekly data is steady.

@dataclass
class WeightSchedule:
    """Defines how a data source's visual weight changes over time."""
    source: str
    cadence: str                              # "realtime", "hourly", "daily", "weekly", "monthly"
    freshness_half_life_hours: float = 24.0   # Weight decays by 50% after this many hours
    peak_weight: float = 1.0                  # Max visual weight when fresh
    min_weight: float = 0.2                   # Floor weight even when stale
    pulse_on_update: bool = True              # Animate a pulse when new data arrives


# Default weight schedules by data family
WEIGHT_SCHEDULES = {
    "equity": WeightSchedule("yfinance", "realtime", 1.0, 1.0, 0.5, True),
    "vol": WeightSchedule("cboe", "realtime", 2.0, 1.0, 0.4, True),
    "rates": WeightSchedule("fred", "daily", 24.0, 0.9, 0.3, False),
    "credit": WeightSchedule("fred", "daily", 24.0, 0.8, 0.3, False),
    "macro": WeightSchedule("fred", "monthly", 168.0, 0.7, 0.2, False),
    "commodity": WeightSchedule("yfinance", "daily", 12.0, 0.9, 0.4, True),
    "crypto": WeightSchedule("binance", "realtime", 1.0, 1.0, 0.5, True),
    "sentiment": WeightSchedule("worldnews", "daily", 12.0, 0.6, 0.2, False),
    "options": WeightSchedule("yfinance", "daily", 8.0, 0.8, 0.3, True),
    "flows": WeightSchedule("computed", "4h", 8.0, 0.9, 0.4, True),
    "physics": WeightSchedule("computed", "hourly", 4.0, 0.7, 0.3, True),
    "alternative": WeightSchedule("various", "weekly", 72.0, 0.5, 0.1, False),
    "news_energy": WeightSchedule("crucix", "hourly", 3.0, 0.8, 0.3, True),
    "regime": WeightSchedule("computed", "hourly", 6.0, 1.0, 0.6, True),
}


# ── Animation Config ────────────────────────────────────────────────────────

@dataclass
class AnimationConfig:
    """Controls how the living graph animates."""
    transition_ms: int = 400                  # Default transition duration
    stagger_ms: int = 30                      # Delay between sequential elements
    easing: str = "cubic-bezier(0.4, 0, 0.2, 1)"  # Material ease
    pulse_duration_ms: int = 800              # New-data pulse animation
    trail_opacity: float = 0.15               # Ghost trail for trajectory charts
    trail_length: int = 20                    # Number of trailing points
    auto_play: bool = True                    # Auto-advance time scrubber
    play_speed_ms: int = 200                  # Ms per time step in auto-play
    loop: bool = False                        # Loop time scrubber


# ── VizSpec ─────────────────────────────────────────────────────────────────

@dataclass
class VizSpec:
    """Complete specification for a living graph.

    The frontend takes this spec and renders it. The spec encodes
    everything needed: chart type, data bindings, weights, animations,
    interactions, and narrative annotations.
    """
    chart_type: ChartType
    title: str
    subtitle: str = ""

    # Data bindings
    data_endpoint: str = ""                    # API endpoint to fetch data
    data_params: dict = field(default_factory=dict)
    x_field: str = ""                          # Field for x-axis (usually date)
    y_fields: list[str] = field(default_factory=list)  # Fields for y-axis
    color_field: str = ""                      # Field for color encoding
    size_field: str = ""                       # Field for size encoding
    group_field: str = ""                      # Field for grouping/faceting
    label_field: str = ""                      # Field for labels
    weight_field: str = ""                     # Field for visual weight

    # Temporal
    time_field: str = "obs_date"               # Time dimension
    time_range: str = "1Y"                     # Default visible range
    time_scrubber: bool = False                # Show time scrubber control
    time_comparison: list[str] = field(default_factory=list)  # Side-by-side periods

    # Weight schedules (which sources pulse, which are steady)
    weight_schedules: list[WeightSchedule] = field(default_factory=list)

    # Animation
    animation: AnimationConfig = field(default_factory=AnimationConfig)

    # Interactions
    zoom: bool = True
    pan: bool = True
    hover_detail: bool = True
    click_drill: bool = False                  # Click to drill down
    brush_select: bool = False                 # Brush to select range
    crossfilter: bool = False                  # Linked filtering across charts

    # Layout
    width: str = "100%"
    height: str = "400px"
    responsive: bool = True
    dark_mode: bool = True

    # Annotations
    regime_bands: bool = False                 # Show regime background colors
    threshold_lines: list[dict] = field(default_factory=list)  # [{value, label, color}]
    narrative_overlay: str = ""                # LLM narrative text overlay

    # Composition (for dashboard grids)
    children: list["VizSpec"] = field(default_factory=list)
    grid_cols: int = 2
    grid_gap: str = "16px"

    def to_dict(self) -> dict:
        """Serialize to JSON-compatible dict for API response."""
        d = {}
        for k, v in asdict(self).items():
            if isinstance(v, Enum):
                d[k] = v.value
            elif isinstance(v, list) and v and hasattr(v[0], '__dataclass_fields__'):
                d[k] = [asdict(item) for item in v]
            else:
                d[k] = v
        d["chart_type"] = self.chart_type.value
        return d


# ── Intelligence Rules ──────────────────────────────────────────────────────
# These encode learned knowledge about which visualization best expresses
# each type of data. Discovered through iteration, not guessing.

VISUALIZATION_RULES: list[dict[str, Any]] = [
    {
        "name": "capital_flows",
        "when": {"data_contains": ["sector", "flow", "rotation"], "relation": "flow"},
        "why": "Capital flow direction changes over time. Static snapshot misses the story. "
               "Time scrubber reveals when money moved and whether it's accelerating.",
        "chart": ChartType.SANKEY_TEMPORAL,
        "config": {
            "time_scrubber": True,
            "time_field": "as_of",
            "color_field": "direction",       # inflow=green, outflow=red
            "size_field": "magnitude",
            "animation": {"auto_play": True, "play_speed_ms": 300},
            "regime_bands": True,
        },
    },
    {
        "name": "regime_trajectory",
        "when": {"data_contains": ["regime", "pca", "state"], "relation": "trajectory"},
        "why": "Regime states are attractors in feature space. A phase diagram shows "
               "the market's trajectory toward/away from these attractors. Distance = "
               "transition probability. Velocity = urgency.",
        "chart": ChartType.PHASE_SPACE,
        "config": {
            "x_field": "pc1",
            "y_fields": ["pc2"],
            "color_field": "regime_state",
            "size_field": "confidence",
            "animation": {"trail_opacity": 0.2, "trail_length": 30},
            "time_scrubber": True,
        },
    },
    {
        "name": "feature_importance_network",
        "when": {"data_contains": ["feature", "importance", "correlation"], "relation": "correlation"},
        "why": "Feature relationships form a network. Importance = node size. "
               "Correlation = edge weight (spring constant). Regime-dependent weights "
               "mean the network topology CHANGES with regime — animate the transition.",
        "chart": ChartType.FORCE_NETWORK,
        "config": {
            "size_field": "importance",
            "weight_field": "correlation",
            "color_field": "family",
            "group_field": "family",
            "animation": {"transition_ms": 800},  # Slow for topology changes
            "time_scrubber": True,                 # Scrub through regimes
        },
    },
    {
        "name": "energy_dynamics",
        "when": {"data_contains": ["kinetic", "potential", "energy"], "relation": "trajectory"},
        "why": "Market energy decomposes into kinetic (rate of change) and potential "
               "(deviation from equilibrium). A particle system makes this intuitive: "
               "fast particles = high KE, stretched springs = high PE. "
               "Conservation violations glow red.",
        "chart": ChartType.PARTICLE_SYSTEM,
        "config": {
            "x_field": "kinetic_energy",
            "y_fields": ["potential_energy"],
            "color_field": "energy_level",
            "size_field": "total_energy",
            "animation": {"transition_ms": 200, "trail_opacity": 0.1},
        },
    },
    {
        "name": "sector_rotation",
        "when": {"data_contains": ["sector", "relative_strength", "rotation"], "relation": "flow"},
        "why": "Sectors orbit a center of gravity (SPY). Distance from center = "
               "relative performance. Angular position = sector cycle phase. "
               "The orbit trail shows rotation history — where money has been flowing.",
        "chart": ChartType.ORBITAL,
        "config": {
            "color_field": "signal",
            "size_field": "volume",
            "label_field": "etf",
            "animation": {"trail_opacity": 0.25, "trail_length": 60, "auto_play": True},
            "time_scrubber": True,
            "time_range": "6M",
        },
    },
    {
        "name": "lead_lag_causation",
        "when": {"data_contains": ["lag", "lead", "correlation", "granger"], "relation": "causation"},
        "why": "Lead/lag relationships are rivers: upstream features signal before "
               "downstream ones react. Width = correlation strength. "
               "Delay = river distance. Time scrubber shows how the flow pattern "
               "changes across regimes.",
        "chart": ChartType.RIVER_FLOW,
        "config": {
            "size_field": "correlation",
            "color_field": "lag_days",
            "animation": {"transition_ms": 600},
            "time_scrubber": True,
        },
    },
    {
        "name": "multi_timeframe_comparison",
        "when": {"data_contains": ["timeframe", "5d", "5w", "1y", "5y"]},
        "why": "Comparing the same feature across timeframes reveals whether current "
               "behavior is noise or structural. Small multiples let the eye compare "
               "shape, not just numbers.",
        "chart": ChartType.SMALL_MULTIPLES,
        "config": {
            "grid_cols": 5,
            "time_comparison": ["5D", "5W", "3M", "1Y", "5Y"],
            "animation": {"stagger_ms": 50},
        },
    },
    {
        "name": "z_score_landscape",
        "when": {"data_contains": ["z_score", "feature", "family"], "relation": "comparison"},
        "why": "Z-scores across all features form a landscape. Ridgeline plots "
               "show how the distribution of z-scores shifts over time — when the "
               "market is stressed, the distribution widens and skews.",
        "chart": ChartType.RIDGELINE,
        "config": {
            "x_field": "z_score",
            "group_field": "family",
            "time_scrubber": True,
            "animation": {"transition_ms": 500},
        },
    },
    {
        "name": "correlation_evolution",
        "when": {"data_contains": ["correlation", "matrix", "rolling"], "relation": "correlation"},
        "why": "Correlations aren't static. A chord diagram with time scrubber "
               "shows how the correlation structure reorganizes during regime shifts. "
               "Thick chords = strong correlation. Color = direction.",
        "chart": ChartType.CHORD,
        "config": {
            "size_field": "correlation",
            "color_field": "direction",
            "time_scrubber": True,
            "time_range": "3M",
            "animation": {"transition_ms": 600},
        },
    },
    {
        "name": "news_force_field",
        "when": {"data_contains": ["news", "energy", "source", "coherence"]},
        "why": "News sources exert forces on market sentiment. When sources align, "
               "coherence is high and the force is strong. When they diverge, "
               "the field is chaotic. Show as a force field with arrows.",
        "chart": ChartType.FORCE_NETWORK,
        "config": {
            "size_field": "energy",
            "color_field": "direction_label",
            "weight_field": "coherence",
            "animation": {"transition_ms": 300, "pulse_duration_ms": 600},
        },
    },
    {
        "name": "options_surface",
        "when": {"data_contains": ["iv", "strike", "expiry", "options"]},
        "why": "Options implied volatility forms a surface across strike and expiry. "
               "The surface shape reveals market expectations: skew = crash fear, "
               "term structure = near vs far uncertainty.",
        "chart": ChartType.HEATMAP,
        "config": {
            "x_field": "strike_pct",       # % from spot
            "y_fields": ["expiry_days"],
            "color_field": "iv",
            "time_scrubber": True,
            "animation": {"transition_ms": 400},
        },
    },
    {
        "name": "weight_cadence_dashboard",
        "when": {"data_contains": ["weight", "cadence", "freshness"]},
        "why": "Different data sources update at different rates. Real-time equity "
               "prices pulse constantly while monthly macro data is a slow heartbeat. "
               "Show the rhythm of information arrival.",
        "chart": ChartType.DASHBOARD_GRID,
        "config": {
            "grid_cols": 3,
            # Children are generated dynamically per source
        },
    },
]


def select_visualization(
    data_description: str,
    features: list[str] | None = None,
    relation: str | None = None,
    question: str | None = None,
) -> VizSpec:
    """Select the optimal visualization for a data pattern.

    Parameters:
        data_description: What the data represents (e.g., "sector capital flows over 6 months")
        features: List of feature names involved
        relation: Type of relationship (flow, correlation, causation, etc.)
        question: The question being asked (e.g., "where is money flowing?")

    Returns:
        VizSpec ready for frontend rendering.
    """
    desc_lower = data_description.lower()
    features_lower = [f.lower() for f in (features or [])]
    all_terms = desc_lower + " " + " ".join(features_lower) + " " + (question or "").lower()

    best_rule = None
    best_score = 0

    for rule in VISUALIZATION_RULES:
        score = 0
        when = rule["when"]

        # Match data_contains terms
        for term in when.get("data_contains", []):
            if term in all_terms:
                score += 2

        # Match relation type
        if relation and when.get("relation") == relation:
            score += 3

        if score > best_score:
            best_score = score
            best_rule = rule

    if best_rule is None:
        # Default: multi-line time series
        return VizSpec(
            chart_type=ChartType.MULTI_LINE,
            title=data_description,
            time_scrubber=True,
            regime_bands=True,
        )

    config = best_rule["config"]
    spec = VizSpec(
        chart_type=best_rule["chart"],
        title=best_rule["name"].replace("_", " ").title(),
        subtitle=best_rule["why"][:120] + "...",
        **{k: v for k, v in config.items() if k != "animation"},
    )

    if "animation" in config:
        for k, v in config["animation"].items():
            setattr(spec.animation, k, v)

    # Attach weight schedules for involved families
    if features:
        families = set()
        for feat in features:
            for fam, schedule in WEIGHT_SCHEDULES.items():
                if fam in feat.lower():
                    families.add(fam)
        spec.weight_schedules = [WEIGHT_SCHEDULES[f] for f in families if f in WEIGHT_SCHEDULES]

    return spec


def get_all_rules() -> list[dict]:
    """Return all visualization rules for the frontend to display."""
    return [
        {
            "name": r["name"],
            "chart_type": r["chart"].value,
            "why": r["why"],
            "triggers": r["when"],
        }
        for r in VISUALIZATION_RULES
    ]


# ── Living Graph Update Protocol ────────────────────────────────────────────

def compute_source_weights(
    families: list[str],
    as_of: datetime | None = None,
) -> dict[str, float]:
    """Compute current visual weights for each data family.

    Weights decay from peak based on time since last update.
    Real-time sources stay near 1.0, monthly macro decays slowly.
    """
    import math

    if as_of is None:
        as_of = datetime.utcnow()

    weights = {}
    for family in families:
        schedule = WEIGHT_SCHEDULES.get(family)
        if not schedule:
            weights[family] = 0.5
            continue

        # For now, assume data is fresh (in production, check last_pull_at)
        # Decay formula: w = min_w + (peak_w - min_w) * exp(-t / half_life)
        hours_since_update = 0  # TODO: query last_pull_at from source_catalog
        decay = math.exp(-hours_since_update / schedule.freshness_half_life_hours)
        weight = schedule.min_weight + (schedule.peak_weight - schedule.min_weight) * decay
        weights[family] = round(weight, 3)

    return weights
