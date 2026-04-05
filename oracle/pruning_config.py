"""Prompt pruning system configuration — constants, anchors, thresholds."""

from __future__ import annotations

from dataclasses import dataclass

# Features that are NEVER pruned regardless of utility score.
ANCHOR_FEATURES: frozenset[str] = frozenset({
    "vix_spot", "vix_3m_ratio",
    "spy", "spy_full", "qqq", "qqq_full",
    "yld_curve_2s10s", "fed_funds_rate",
    "dxy_index",
    "hy_spread_proxy",
})

# Features critical during crises — protected from pruning even with low citation_rate.
CRISIS_ONLY_FEATURES: frozenset[str] = frozenset({
    "vix_1m_chg", "hy_spread_3m_chg", "ig_spread_proxy",
    "sp500_pct_above_200ma", "ted_spread",
})

# Entire families treated as crisis-aware
CRISIS_FAMILIES: frozenset[str] = frozenset({"systemic", "credit"})


@dataclass(frozen=True)
class PruningThresholds:
    min_utility: float = 0.15
    max_prompt_features: int = 50
    min_observations: int = 20
    recency_half_life_days: float = 30.0
    w_citation_rate: float = 0.30
    w_hit_correlation: float = 0.40
    w_information_gain: float = 0.15
    w_recency: float = 0.15
    ab_test_fraction: float = 0.8
    regime_shift_z_threshold: float = 2.5
    cold_start_minimum: int = 50


DEFAULT_THRESHOLDS = PruningThresholds()
