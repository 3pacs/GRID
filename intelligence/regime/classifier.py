"""
Regime classification engine.

Classifies the macro state into named regime labels across 5 axes:
  1. Risk Appetite (risk_on / risk_off / neutral)
  2. Monetary Policy (tightening / easing / holding)
  3. Economic Cycle (expansion / contraction / late_cycle / early_cycle)
  4. Stress Level (stress / complacency / normal)
  5. Data Credibility (high_divergence / low_divergence / moderate)

Each label includes confidence score and the dimensions that drove it.
Also computes regime transition probabilities from historical data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy.engine import Engine

from intelligence.regime.state_vector import (
    StateVector,
    DIM_NAMES,
    load_cached_vectors,
)


@dataclass(frozen=True)
class RegimeLabel:
    """Single regime axis classification."""
    axis: str           # e.g. 'risk_appetite'
    label: str          # e.g. 'risk_off'
    confidence: float   # 0-1
    drivers: tuple[str, ...]  # which dimensions drove this

    def to_dict(self) -> dict[str, Any]:
        return {
            'axis': self.axis,
            'label': self.label,
            'confidence': round(self.confidence, 3),
            'drivers': list(self.drivers),
        }


@dataclass(frozen=True)
class RegimeClassification:
    """Complete regime classification across all axes."""
    labels: tuple[RegimeLabel, ...]
    composite_label: str               # e.g. "RISK_OFF | TIGHTENING | LATE_CYCLE | STRESS"
    historical_frequency: float        # how often this combo has occurred
    avg_duration_days: float | None    # typical regime duration
    transition_probabilities: dict[str, float]  # probability of shifting to each state

    def to_dict(self) -> dict[str, Any]:
        return {
            'composite_label': self.composite_label,
            'axes': [l.to_dict() for l in self.labels],
            'historical_frequency': round(self.historical_frequency, 4),
            'avg_duration_days': round(self.avg_duration_days, 1) if self.avg_duration_days else None,
            'transition_probabilities': {k: round(v, 3) for k, v in self.transition_probabilities.items()},
        }


# ── Dimension index helpers ──────────────────────────────────────────────

def _idx(name: str) -> int:
    return DIM_NAMES.index(name)


def _val(sv: StateVector, name: str) -> float | None:
    try:
        return sv.values[_idx(name)]
    except (ValueError, IndexError):
        return None


# ── Axis classifiers ─────────────────────────────────────────────────────

def _classify_risk_appetite(sv: StateVector) -> RegimeLabel:
    """Risk-on / risk-off / neutral based on VIX, spreads, momentum."""
    vix = _val(sv, 'vix_level')
    vix_pct = _val(sv, 'vix_percentile')
    hy = _val(sv, 'hy_spread_level')
    momentum = _val(sv, 'spy_momentum')
    rsi = _val(sv, 'spy_rsi')

    score = 0.0  # positive = risk-on, negative = risk-off
    drivers = []
    n_inputs = 0

    if vix is not None:
        n_inputs += 1
        if vix < -0.5:   # below-average VIX (z-scored)
            score += 1.0
            drivers.append('low_vix')
        elif vix > 0.5:
            score -= 1.0
            drivers.append('high_vix')

    if vix_pct is not None:
        n_inputs += 1
        if vix_pct < 0.3:
            score += 0.8
            drivers.append('vix_low_percentile')
        elif vix_pct > 0.7:
            score -= 0.8
            drivers.append('vix_high_percentile')

    if hy is not None:
        n_inputs += 1
        if hy < -0.5:
            score += 0.7
            drivers.append('tight_hy_spread')
        elif hy > 0.5:
            score -= 0.7
            drivers.append('wide_hy_spread')

    if momentum is not None:
        n_inputs += 1
        if momentum > 1.02:
            score += 0.6
            drivers.append('bullish_momentum')
        elif momentum < 0.98:
            score -= 0.6
            drivers.append('bearish_momentum')

    if rsi is not None:
        n_inputs += 1
        if rsi > 0.5:  # RSI > 62.5 (normalized)
            score += 0.4
            drivers.append('overbought_rsi')
        elif rsi < -0.5:  # RSI < 37.5
            score -= 0.4
            drivers.append('oversold_rsi')

    if n_inputs == 0:
        return RegimeLabel('risk_appetite', 'unknown', 0.0, ())

    norm_score = score / n_inputs
    confidence = min(1.0, abs(norm_score))

    if norm_score > 0.3:
        label = 'risk_on'
    elif norm_score < -0.3:
        label = 'risk_off'
    else:
        label = 'neutral'

    return RegimeLabel('risk_appetite', label, confidence, tuple(drivers))


def _classify_monetary_policy(sv: StateVector) -> RegimeLabel:
    """Tightening / easing / holding based on rates, liquidity, M2."""
    ff_dir = _val(sv, 'fed_funds_dir')
    liq_chg = _val(sv, 'fed_net_liq_chg')
    m2 = _val(sv, 'm2_growth')
    real_ff = _val(sv, 'real_fed_funds')

    score = 0.0  # positive = tightening, negative = easing
    drivers = []
    n_inputs = 0

    if ff_dir is not None:
        n_inputs += 1
        if ff_dir > 0.25:
            score += 1.0
            drivers.append('hiking')
        elif ff_dir < -0.25:
            score -= 1.0
            drivers.append('cutting')

    if liq_chg is not None:
        n_inputs += 1
        if liq_chg < -0.5:  # z-scored: net liquidity declining
            score += 0.8
            drivers.append('liquidity_draining')
        elif liq_chg > 0.5:
            score -= 0.8
            drivers.append('liquidity_injecting')

    if m2 is not None:
        n_inputs += 1
        if m2 < -0.02:
            score += 0.6
            drivers.append('m2_contracting')
        elif m2 > 0.05:
            score -= 0.6
            drivers.append('m2_expanding')

    if real_ff is not None:
        n_inputs += 1
        if real_ff > 2.0:
            score += 0.5
            drivers.append('restrictive_real_rate')
        elif real_ff < 0:
            score -= 0.5
            drivers.append('negative_real_rate')

    if n_inputs == 0:
        return RegimeLabel('monetary_policy', 'unknown', 0.0, ())

    norm_score = score / n_inputs
    confidence = min(1.0, abs(norm_score))

    if norm_score > 0.3:
        label = 'tightening'
    elif norm_score < -0.3:
        label = 'easing'
    else:
        label = 'holding'

    return RegimeLabel('monetary_policy', label, confidence, tuple(drivers))


def _classify_economic_cycle(sv: StateVector) -> RegimeLabel:
    """Expansion / contraction / late_cycle / early_cycle."""
    ue_level = _val(sv, 'unemployment_level')
    ue_dir = _val(sv, 'unemployment_dir')
    ip_yoy = _val(sv, 'industrial_prod_yoy')
    curve = _val(sv, 'yield_curve_level')
    sentiment = _val(sv, 'consumer_sentiment')
    cap_util = _val(sv, 'capacity_util')

    drivers = []
    signals = {}

    if ue_dir is not None:
        signals['ue_rising'] = ue_dir > 0.1
        signals['ue_falling'] = ue_dir < -0.1

    if ip_yoy is not None:
        signals['ip_positive'] = ip_yoy > 0
        signals['ip_negative'] = ip_yoy < 0

    if curve is not None:
        signals['curve_inverted'] = curve < -0.5  # z-scored
        signals['curve_steep'] = curve > 0.5

    if ue_level is not None:
        signals['ue_low'] = ue_level < -0.5  # z-scored: below average
        signals['ue_high'] = ue_level > 0.5

    if not signals:
        return RegimeLabel('economic_cycle', 'unknown', 0.0, ())

    # Late cycle: low unemployment + inverted curve
    if signals.get('ue_low') and signals.get('curve_inverted'):
        drivers.extend(['low_unemployment', 'inverted_curve'])
        return RegimeLabel('economic_cycle', 'late_cycle', 0.8, tuple(drivers))

    # Early cycle: high unemployment + steepening curve + cutting
    if signals.get('ue_high') and signals.get('curve_steep'):
        drivers.extend(['high_unemployment', 'steep_curve'])
        return RegimeLabel('economic_cycle', 'early_cycle', 0.7, tuple(drivers))

    # Contraction: rising UE + negative IP
    if signals.get('ue_rising') and signals.get('ip_negative'):
        drivers.extend(['rising_unemployment', 'negative_ip'])
        return RegimeLabel('economic_cycle', 'contraction', 0.8, tuple(drivers))

    # Expansion: falling UE + positive IP
    if signals.get('ue_falling') and signals.get('ip_positive'):
        drivers.extend(['falling_unemployment', 'positive_ip'])
        return RegimeLabel('economic_cycle', 'expansion', 0.7, tuple(drivers))

    return RegimeLabel('economic_cycle', 'mixed', 0.4, tuple(drivers))


def _classify_stress(sv: StateVector) -> RegimeLabel:
    """Stress / complacency / normal."""
    vix = _val(sv, 'vix_level')
    hy = _val(sv, 'hy_spread_level')
    ig = _val(sv, 'ig_spread_level')
    vix_pct = _val(sv, 'vix_percentile')

    score = 0.0  # positive = stressed, negative = complacent
    drivers = []
    n_inputs = 0

    if vix is not None:
        n_inputs += 1
        if vix > 1.5:
            score += 1.0
            drivers.append('vix_extreme')
        elif vix > 0.5:
            score += 0.5
            drivers.append('vix_elevated')
        elif vix < -1.0:
            score -= 1.0
            drivers.append('vix_suppressed')

    if hy is not None:
        n_inputs += 1
        if hy > 1.5:
            score += 0.8
            drivers.append('hy_blowout')
        elif hy < -0.8:
            score -= 0.6
            drivers.append('hy_compressed')

    if ig is not None:
        n_inputs += 1
        if ig > 1.5:
            score += 0.6
            drivers.append('ig_stress')
        elif ig < -0.8:
            score -= 0.4
            drivers.append('ig_tight')

    if vix_pct is not None:
        n_inputs += 1
        if vix_pct < 0.15:
            score -= 0.8
            drivers.append('extreme_complacency')
        elif vix_pct > 0.85:
            score += 0.8
            drivers.append('extreme_fear')

    if n_inputs == 0:
        return RegimeLabel('stress_level', 'unknown', 0.0, ())

    norm_score = score / n_inputs
    confidence = min(1.0, abs(norm_score))

    if norm_score > 0.4:
        label = 'stress'
    elif norm_score < -0.4:
        label = 'complacency'
    else:
        label = 'normal'

    return RegimeLabel('stress_level', label, confidence, tuple(drivers))


def _classify_credibility(sv: StateVector) -> RegimeLabel:
    """Data credibility based on cross-reference divergence score."""
    xref = _val(sv, 'crossref_divergence')

    if xref is None:
        return RegimeLabel('data_credibility', 'unknown', 0.0, ())

    if xref > 2.0:
        return RegimeLabel('data_credibility', 'high_divergence', 0.9, ('crossref_score_extreme',))
    elif xref > 1.0:
        return RegimeLabel('data_credibility', 'moderate_divergence', 0.6, ('crossref_score_elevated',))
    else:
        return RegimeLabel('data_credibility', 'credible', 0.7, ('crossref_score_low',))


# ── Main classifier ──────────────────────────────────────────────────────

def classify_regime(sv: StateVector) -> RegimeClassification:
    """Classify the current macro state across all 5 axes."""
    labels = (
        _classify_risk_appetite(sv),
        _classify_monetary_policy(sv),
        _classify_economic_cycle(sv),
        _classify_stress(sv),
        _classify_credibility(sv),
    )

    # Composite label
    parts = [l.label.upper() for l in labels if l.label != 'unknown']
    composite = ' | '.join(parts) if parts else 'UNKNOWN'

    return RegimeClassification(
        labels=labels,
        composite_label=composite,
        historical_frequency=0.0,    # populated by transition analysis
        avg_duration_days=None,
        transition_probabilities={},
    )


def classify_regime_with_history(
    engine: Engine,
    sv: StateVector,
    lookback_days: int = 60,
) -> RegimeClassification:
    """Classify regime and compute transition probabilities from history."""
    base = classify_regime(sv)

    # Load historical vectors and classify them
    cached = load_cached_vectors(engine)
    if len(cached) < 50:
        return base

    # Classify all historical states
    historical_labels: list[tuple[date, str]] = []
    for hsv in cached:
        hclass = classify_regime(hsv)
        historical_labels.append((hsv.as_of_date, hclass.composite_label))

    # Frequency of current composite label
    current_label = base.composite_label
    match_count = sum(1 for _, l in historical_labels if l == current_label)
    frequency = match_count / len(historical_labels) if historical_labels else 0.0

    # Average duration: consecutive stretches of same label
    durations: list[int] = []
    current_stretch = 1
    for i in range(1, len(historical_labels)):
        if historical_labels[i][1] == historical_labels[i-1][1]:
            current_stretch += 1
        else:
            if historical_labels[i-1][1] == current_label:
                days = (historical_labels[i][0] - historical_labels[i - current_stretch][0]).days
                durations.append(days)
            current_stretch = 1

    avg_duration = float(np.mean(durations)) if durations else None

    # Transition probabilities: what label follows the current one
    transitions: dict[str, int] = {}
    for i in range(len(historical_labels) - 1):
        if historical_labels[i][1] == current_label:
            next_label = historical_labels[i + 1][1]
            transitions[next_label] = transitions.get(next_label, 0) + 1

    total_transitions = sum(transitions.values())
    transition_probs = {
        k: v / total_transitions
        for k, v in sorted(transitions.items(), key=lambda x: x[1], reverse=True)
    } if total_transitions > 0 else {}

    return RegimeClassification(
        labels=base.labels,
        composite_label=current_label,
        historical_frequency=frequency,
        avg_duration_days=avg_duration,
        transition_probabilities=transition_probs,
    )
