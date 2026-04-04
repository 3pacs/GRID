"""
Conditional forecast generation from matched historical episodes.

Produces full outcome distributions — percentiles, histograms, agreement
scores, disagreement flags — not just means. Each forecast is decomposed
by asset class and horizon, weighted by episode match quality.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
from loguru import logger as log

from intelligence.regime.episode_matcher import MatchResult, MatchedEpisode


@dataclass(frozen=True)
class OutcomeDistribution:
    """Full distribution of outcomes for one ticker at one horizon."""
    horizon_days: int
    ticker: str
    n_episodes: int
    mean_return: float
    median_return: float
    quality_weighted_mean: float    # weighted by match quality
    std_return: float
    percentiles: dict[int, float]   # 5th, 10th, 25th, 50th, 75th, 90th, 95th
    agreement_score: float          # 0=split, 1=unanimous direction
    histogram_bins: list[float]
    histogram_counts: list[float]   # quality-weighted counts

    def to_dict(self) -> dict[str, Any]:
        return {
            'horizon_days': self.horizon_days,
            'ticker': self.ticker,
            'n_episodes': self.n_episodes,
            'mean_return': round(self.mean_return, 4),
            'median_return': round(self.median_return, 4),
            'quality_weighted_mean': round(self.quality_weighted_mean, 4),
            'std_return': round(self.std_return, 4),
            'percentiles': {str(k): round(v, 4) for k, v in self.percentiles.items()},
            'agreement_score': round(self.agreement_score, 3),
            'direction': 'BULLISH' if self.quality_weighted_mean > 0.005 else 'BEARISH' if self.quality_weighted_mean < -0.005 else 'NEUTRAL',
            'confidence': _confidence_label(self.agreement_score, self.n_episodes),
        }


@dataclass(frozen=True)
class ConditionalForecast:
    """Complete conditional forecast from matched episodes."""
    query_date: date
    n_episodes: int
    mean_quality: float
    effective_sample_size: float
    outcomes: dict[str, list[OutcomeDistribution]]  # ticker -> horizons
    regime_summary: str
    dominant_drivers: list[str]         # top dims driving the matches
    disagreement_flags: list[str]       # when matches contradict
    confidence_level: str               # HIGH / MEDIUM / LOW / UNRELIABLE

    def to_dict(self) -> dict[str, Any]:
        return {
            'query_date': self.query_date.isoformat(),
            'n_episodes': self.n_episodes,
            'mean_quality': round(self.mean_quality, 4),
            'effective_sample_size': round(self.effective_sample_size, 1),
            'confidence_level': self.confidence_level,
            'regime_summary': self.regime_summary,
            'dominant_drivers': self.dominant_drivers,
            'disagreement_flags': self.disagreement_flags,
            'outcomes': {
                ticker: [o.to_dict() for o in dists]
                for ticker, dists in self.outcomes.items()
            },
        }


def _confidence_label(agreement: float, n: int) -> str:
    if n < 5:
        return 'UNRELIABLE'
    if agreement > 0.8 and n >= 10:
        return 'HIGH'
    if agreement > 0.6 and n >= 7:
        return 'MEDIUM'
    return 'LOW'


# ── Distribution computation ─────────────────────────────────────────────

def _compute_distribution(
    returns: np.ndarray,
    weights: np.ndarray,
    horizon: int,
    ticker: str,
    n_bins: int = 20,
) -> OutcomeDistribution:
    """Compute full outcome distribution from matched episode returns."""

    if len(returns) == 0:
        return OutcomeDistribution(
            horizon_days=horizon, ticker=ticker, n_episodes=0,
            mean_return=0.0, median_return=0.0, quality_weighted_mean=0.0,
            std_return=0.0, percentiles={}, agreement_score=0.0,
            histogram_bins=[], histogram_counts=[],
        )

    # Basic stats
    mean_ret = float(np.mean(returns))
    median_ret = float(np.median(returns))
    std_ret = float(np.std(returns)) if len(returns) > 1 else 0.0

    # Quality-weighted mean
    w_sum = weights.sum()
    if w_sum > 0:
        qw_mean = float(np.average(returns, weights=weights))
    else:
        qw_mean = mean_ret

    # Percentiles
    pcts = {
        5: float(np.percentile(returns, 5)),
        10: float(np.percentile(returns, 10)),
        25: float(np.percentile(returns, 25)),
        50: float(np.percentile(returns, 50)),
        75: float(np.percentile(returns, 75)),
        90: float(np.percentile(returns, 90)),
        95: float(np.percentile(returns, 95)),
    }

    # Agreement score: what fraction agree on direction
    n_positive = np.sum(returns > 0)
    n_negative = np.sum(returns < 0)
    n_total = len(returns)
    agreement = float(abs(n_positive - n_negative) / n_total) if n_total > 0 else 0.0

    # Weighted histogram
    if len(returns) >= 3:
        lo = float(np.percentile(returns, 2))
        hi = float(np.percentile(returns, 98))
        if abs(hi - lo) < 1e-8:
            hi = lo + 0.01
        bins = np.linspace(lo, hi, n_bins + 1)
        counts = np.zeros(n_bins)
        for i in range(len(returns)):
            idx = np.searchsorted(bins[1:], returns[i])
            idx = min(idx, n_bins - 1)
            counts[idx] += weights[i] if w_sum > 0 else 1.0
        # Normalize
        if counts.sum() > 0:
            counts = counts / counts.sum()
        hist_bins = ((bins[:-1] + bins[1:]) / 2).tolist()
        hist_counts = counts.tolist()
    else:
        hist_bins = []
        hist_counts = []

    return OutcomeDistribution(
        horizon_days=horizon,
        ticker=ticker,
        n_episodes=len(returns),
        mean_return=mean_ret,
        median_return=median_ret,
        quality_weighted_mean=qw_mean,
        std_return=std_ret,
        percentiles=pcts,
        agreement_score=agreement,
        histogram_bins=hist_bins,
        histogram_counts=hist_counts,
    )


# ── Disagreement detection ───────────────────────────────────────────────

def _detect_disagreements(
    episodes: list[MatchedEpisode],
    ticker: str,
    horizon: int,
) -> list[str]:
    """Flag when matched episodes tell contradictory stories."""
    flags: list[str] = []

    returns = []
    qualities = []
    for ep in episodes:
        r = ep.forward_returns.get(ticker, {}).get(horizon)
        if r is not None:
            returns.append(r)
            qualities.append(ep.match_quality)

    if len(returns) < 4:
        return flags

    returns = np.array(returns)
    qualities = np.array(qualities)

    # Top 5 vs bottom 5 quality matches
    order = np.argsort(qualities)[::-1]
    if len(order) >= 8:
        top5 = returns[order[:5]]
        bot5 = returns[order[-5:]]
        top_dir = np.mean(top5) > 0
        bot_dir = np.mean(bot5) > 0
        if top_dir != bot_dir:
            flags.append(
                f"{ticker} {horizon}d: best matches say {'UP' if top_dir else 'DOWN'} "
                f"but worst matches say {'UP' if bot_dir else 'DOWN'}"
            )

    # Recent vs old matches
    dates = [ep.as_of_date for ep in episodes if ep.forward_returns.get(ticker, {}).get(horizon) is not None]
    if len(dates) >= 6:
        sorted_idx = np.argsort(dates)
        recent = returns[sorted_idx[-3:]]
        old = returns[sorted_idx[:3]]
        if (np.mean(recent) > 0) != (np.mean(old) > 0):
            flags.append(
                f"{ticker} {horizon}d: recent analogs disagree with older analogs (possible regime shift)"
            )

    # Bimodal distribution check
    if len(returns) >= 8:
        median = np.median(returns)
        above = returns[returns > median]
        below = returns[returns <= median]
        if len(above) >= 3 and len(below) >= 3:
            gap = np.min(above) - np.max(below)
            spread = np.std(returns)
            if spread > 0 and gap / spread > 0.5:
                flags.append(
                    f"{ticker} {horizon}d: bimodal distribution detected — "
                    f"outcomes cluster at {np.mean(below):.1%} and {np.mean(above):.1%}"
                )

    return flags


# ── Main forecast generation ─────────────────────────────────────────────

_DEFAULT_TICKERS = ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'XLE', 'XLF', 'VIX', 'HY_SPREAD']
_DEFAULT_HORIZONS = [7, 14, 30, 60, 90, 128]


def generate_conditional_forecast(
    engine: object,  # not used directly, but kept for API consistency
    match_result: MatchResult,
    tickers: list[str] | None = None,
    horizons: list[int] | None = None,
) -> ConditionalForecast:
    """Generate conditional outcome distributions from matched episodes.

    For each ticker and horizon, collects forward returns from all matched
    episodes, weights by match quality, and computes full distribution
    statistics including percentiles, histograms, and disagreement flags.
    """
    tickers = tickers or _DEFAULT_TICKERS
    horizons = horizons or _DEFAULT_HORIZONS
    episodes = match_result.episodes

    if not episodes:
        return ConditionalForecast(
            query_date=match_result.query_date,
            n_episodes=0,
            mean_quality=0.0,
            effective_sample_size=0.0,
            outcomes={},
            regime_summary="No analogous episodes found",
            dominant_drivers=[],
            disagreement_flags=["INSUFFICIENT DATA: no historical matches found"],
            confidence_level='UNRELIABLE',
        )

    outcomes: dict[str, list[OutcomeDistribution]] = {}
    all_flags: list[str] = []

    # Tickers that use absolute changes instead of percentage returns
    absolute_tickers = {'VIX', 'HY_SPREAD'}

    for ticker in tickers:
        ticker_dists: list[OutcomeDistribution] = []
        is_absolute = ticker in absolute_tickers

        for horizon in horizons:
            # Collect returns and qualities for this ticker/horizon
            returns_list: list[float] = []
            quality_list: list[float] = []

            for ep in episodes:
                r = ep.forward_returns.get(ticker, {}).get(horizon)
                if r is not None:
                    returns_list.append(r)
                    quality_list.append(ep.match_quality)

            returns = np.array(returns_list)
            qualities = np.array(quality_list)

            dist = _compute_distribution(returns, qualities, horizon, ticker)
            ticker_dists.append(dist)

            # Check for disagreements
            flags = _detect_disagreements(episodes, ticker, horizon)
            all_flags.extend(flags)

        outcomes[ticker] = ticker_dists

    # Dominant drivers from match result
    drivers = sorted(
        match_result.dimension_importance.items(),
        key=lambda x: x[1], reverse=True,
    )[:5]
    dominant_drivers = [f"{name} ({importance:.1%})" for name, importance in drivers]

    # Overall confidence
    spy_30d = next(
        (d for d in outcomes.get('SPY', []) if d.horizon_days == 30),
        None,
    )
    if spy_30d:
        confidence = _confidence_label(spy_30d.agreement_score, spy_30d.n_episodes)
    else:
        confidence = 'UNRELIABLE'

    # Regime summary
    regime_parts = []
    if spy_30d and spy_30d.n_episodes >= 5:
        direction = "bullish" if spy_30d.quality_weighted_mean > 0.01 else "bearish" if spy_30d.quality_weighted_mean < -0.01 else "neutral"
        regime_parts.append(f"SPY 30d outlook: {direction} ({spy_30d.quality_weighted_mean:+.1%})")
        regime_parts.append(f"agreement: {spy_30d.agreement_score:.0%}")
        regime_parts.append(f"range: [{spy_30d.percentiles.get(10, 0):.1%} to {spy_30d.percentiles.get(90, 0):.1%}]")

    vix_30d = next(
        (d for d in outcomes.get('VIX', []) if d.horizon_days == 30),
        None,
    )
    if vix_30d and vix_30d.n_episodes >= 5:
        vix_dir = "higher" if vix_30d.quality_weighted_mean > 0 else "lower"
        regime_parts.append(f"VIX 30d: {vix_dir} ({vix_30d.quality_weighted_mean:+.1f}pts)")

    regime_summary = " | ".join(regime_parts) if regime_parts else "Insufficient data for regime summary"

    return ConditionalForecast(
        query_date=match_result.query_date,
        n_episodes=len(episodes),
        mean_quality=match_result.mean_quality,
        effective_sample_size=match_result.effective_sample_size,
        outcomes=outcomes,
        regime_summary=regime_summary,
        dominant_drivers=dominant_drivers,
        disagreement_flags=all_flags,
        confidence_level=confidence,
    )
