"""
GRID Regime-Matched Historical Analog Engine.

Constructs multi-dimensional macro state vectors, finds historically
analogous episodes, generates conditional outcome distributions, and
classifies the current environment into named regime labels.

Usage:
    from intelligence.regime import (
        compute_state_vector,
        find_analogous_episodes,
        generate_conditional_forecast,
        classify_regime,
    )

    engine = get_db_engine()
    sv = compute_state_vector(engine)
    matches = find_analogous_episodes(engine, sv)
    forecast = generate_conditional_forecast(engine, matches)
    regime = classify_regime(sv)
"""

from intelligence.regime.state_vector import (
    StateVector,
    compute_state_vector,
    compute_state_vector_series,
    get_or_compute_state_vector,
)
from intelligence.regime.episode_matcher import (
    MatchedEpisode,
    MatchResult,
    find_analogous_episodes,
)
from intelligence.regime.forecast import (
    OutcomeDistribution,
    ConditionalForecast,
    generate_conditional_forecast,
)
from intelligence.regime.classifier import (
    RegimeLabel,
    RegimeClassification,
    classify_regime,
)

__all__ = [
    "StateVector",
    "compute_state_vector",
    "compute_state_vector_series",
    "get_or_compute_state_vector",
    "MatchedEpisode",
    "MatchResult",
    "find_analogous_episodes",
    "OutcomeDistribution",
    "ConditionalForecast",
    "generate_conditional_forecast",
    "RegimeLabel",
    "RegimeClassification",
    "classify_regime",
]
