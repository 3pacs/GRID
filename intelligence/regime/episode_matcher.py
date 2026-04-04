"""
Episode matching engine for the regime analog system.

Finds historically analogous macro environments using weighted cosine
similarity with quality adjustments — completeness penalties, staleness
penalties, temporal diversity bonuses, and exclusion windows to prevent
clustered matches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.regime.state_vector import (
    StateVector,
    DIM_NAMES,
    DIM_WEIGHTS,
    load_cached_vectors,
)


@dataclass(frozen=True)
class MatchedEpisode:
    """A single historical episode matched to the query state."""
    as_of_date: date
    similarity_score: float         # raw cosine similarity (0-1)
    match_quality: float            # adjusted score after penalties/bonuses
    dimension_contributions: dict[str, float]  # which dims drove this match
    forward_returns: dict[str, dict[int, float]]  # ticker -> {horizon_days: return}

    def to_dict(self) -> dict[str, Any]:
        return {
            'as_of_date': self.as_of_date.isoformat(),
            'similarity_score': round(self.similarity_score, 4),
            'match_quality': round(self.match_quality, 4),
            'top_drivers': dict(sorted(
                self.dimension_contributions.items(),
                key=lambda x: x[1], reverse=True,
            )[:8]),
            'forward_returns': {
                t: {str(h): round(r, 4) for h, r in hrs.items()}
                for t, hrs in self.forward_returns.items()
            },
        }


@dataclass(frozen=True)
class MatchResult:
    """Complete result of an episode matching query."""
    query_date: date
    query_vector: StateVector
    episodes: list[MatchedEpisode]
    mean_quality: float
    effective_sample_size: float     # penalized N when one period dominates
    dimension_importance: dict[str, float]  # aggregate dim importance across matches

    def to_dict(self) -> dict[str, Any]:
        return {
            'query_date': self.query_date.isoformat(),
            'n_matches': len(self.episodes),
            'mean_quality': round(self.mean_quality, 4),
            'effective_sample_size': round(self.effective_sample_size, 1),
            'top_dimensions': dict(sorted(
                self.dimension_importance.items(),
                key=lambda x: x[1], reverse=True,
            )[:10]),
            'episodes': [e.to_dict() for e in self.episodes],
        }


# ── Similarity computation ───────────────────────────────────────────────

def _weighted_cosine_similarity(
    v1: np.ndarray,
    v2: np.ndarray,
    weights: np.ndarray,
) -> tuple[float, np.ndarray]:
    """Weighted cosine similarity handling NaN values.

    Only computes over mutually non-null dimensions.
    Returns (similarity, per-dimension contributions).
    """
    mask = np.isfinite(v1) & np.isfinite(v2)
    if mask.sum() < 5:  # need at least 5 shared dimensions
        return 0.0, np.zeros(len(v1))

    v1m = np.where(mask, v1, 0.0)
    v2m = np.where(mask, v2, 0.0)
    wm = np.where(mask, weights, 0.0)

    # Weighted dot product
    wv1 = v1m * wm
    wv2 = v2m * wm
    dot = np.sum(wv1 * wv2)
    norm1 = np.sqrt(np.sum(wv1 ** 2))
    norm2 = np.sqrt(np.sum(wv2 ** 2))

    if norm1 < 1e-10 or norm2 < 1e-10:
        return 0.0, np.zeros(len(v1))

    sim = dot / (norm1 * norm2)
    sim = max(0.0, min(1.0, (sim + 1.0) / 2.0))  # map [-1,1] to [0,1]

    # Per-dimension contribution: how much each dim helped the match
    contributions = np.zeros(len(v1))
    for i in range(len(v1)):
        if mask[i]:
            # Contribution = weight * (1 - normalized distance)
            diff = abs(v1[i] - v2[i])
            max_range = max(abs(v1[i]), abs(v2[i]), 1.0)
            contributions[i] = wm[i] * max(0, 1.0 - diff / max_range)

    return float(sim), contributions


# ── Forward returns ──────────────────────────────────────────────────────

_FORWARD_TICKERS = {
    'SPY': 'YF:SPY:close',
    'QQQ': 'YF:QQQ:close',
    'IWM': 'YF:IWM:close',
    'TLT': 'YF:TLT:close',
    'GLD': 'YF:GLD:close',
    'XLE': 'YF:XLE:close',
    'XLF': 'YF:XLF:close',
    'VIX': 'VIXCLS',
    'HY_SPREAD': 'BAMLH0A0HYM2',
}

_FORWARD_HORIZONS = [7, 14, 30, 60, 90, 128]


def _compute_forward_returns(
    engine: Engine,
    as_of: date,
    horizons: list[int] | None = None,
) -> dict[str, dict[int, float]]:
    """Compute what actually happened after a historical date."""
    horizons = horizons or _FORWARD_HORIZONS
    max_horizon = max(horizons)
    end_date = as_of + timedelta(days=int(max_horizon * 1.5))  # buffer for weekends

    results: dict[str, dict[int, float]] = {}

    for label, series_id in _FORWARD_TICKERS.items():
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date, value FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "AND obs_date >= :start AND obs_date <= :end "
                    "ORDER BY obs_date"
                ),
                {"sid": series_id, "start": as_of, "end": end_date},
            ).fetchall()

        if not rows:
            continue

        prices = pd.Series(
            {r[0]: float(r[1]) for r in rows},
            dtype=float,
        ).sort_index()

        if prices.empty:
            continue

        base_val = prices.iloc[0]
        if abs(base_val) < 1e-8:
            continue

        horizon_returns: dict[int, float] = {}
        for h in horizons:
            target_date = as_of + timedelta(days=h)
            # Find closest available date
            future = prices[prices.index >= target_date]
            if future.empty:
                continue
            future_val = future.iloc[0]
            # For spreads/VIX, use absolute change not pct
            if label in ('VIX', 'HY_SPREAD'):
                horizon_returns[h] = float(future_val - base_val)
            else:
                horizon_returns[h] = float((future_val - base_val) / base_val)

        if horizon_returns:
            results[label] = horizon_returns

    return results


# ── Main matching function ───────────────────────────────────────────────

def find_analogous_episodes(
    engine: Engine,
    query_vector: StateVector,
    n: int = 20,
    min_quality: float = 0.5,
    exclusion_window_days: int = 30,
    max_candidates: int = 5000,
) -> MatchResult:
    """Find the N most similar historical macro environments.

    Quality adjustments beyond raw cosine similarity:
      1. Completeness penalty — matches with missing dims penalized
      2. Staleness penalty — dims with stale data reduce quality
      3. Temporal diversity — slight bonus for matches from different decades
      4. Exclusion window — prevents clustering matches in one period
      5. Recency cap — excludes episodes too recent to have forward returns

    Args:
        engine: Database engine.
        query_vector: Current state to match against.
        n: Number of matches to return.
        min_quality: Minimum adjusted quality threshold.
        exclusion_window_days: Min days between matched episodes.
        max_candidates: Max historical vectors to evaluate.
    """
    log.info("Finding analogous episodes for {dt}", dt=query_vector.as_of_date)

    # Load historical vectors
    candidates = load_cached_vectors(engine)
    if not candidates:
        log.warning("No cached state vectors — run backfill first")
        return MatchResult(
            query_date=query_vector.as_of_date,
            query_vector=query_vector,
            episodes=[],
            mean_quality=0.0,
            effective_sample_size=0.0,
            dimension_importance={},
        )

    # Filter: exclude vectors within 128 days of today (need forward returns)
    max_date = query_vector.as_of_date - timedelta(days=128)
    candidates = [c for c in candidates if c.as_of_date <= max_date]

    if len(candidates) > max_candidates:
        # Sample evenly across time
        step = len(candidates) // max_candidates
        candidates = candidates[::step]

    query_arr = query_vector.array
    query_decade = query_vector.as_of_date.year // 10

    # Score all candidates
    scored: list[tuple[float, float, StateVector, np.ndarray]] = []
    for cand in candidates:
        cand_arr = cand.array
        sim, contributions = _weighted_cosine_similarity(query_arr, cand_arr, DIM_WEIGHTS)

        if sim < 0.3:  # skip obviously poor matches
            continue

        # ── Quality adjustments ──
        quality = sim

        # 1. Completeness penalty
        shared_completeness = min(query_vector.completeness, cand.completeness)
        quality *= (0.5 + 0.5 * shared_completeness)  # floor at 50% of sim

        # 2. Staleness penalty
        stale_count = len(cand.stale_dimensions)
        quality -= 0.015 * stale_count

        # 3. Temporal diversity bonus
        cand_decade = cand.as_of_date.year // 10
        if cand_decade != query_decade:
            quality += 0.01  # small bonus for cross-decade matches

        # 4. Data density bonus — more shared dims = more reliable match
        shared_dims = np.isfinite(query_arr) & np.isfinite(cand_arr)
        quality *= (0.7 + 0.3 * (shared_dims.sum() / len(DIM_NAMES)))

        quality = max(0.0, quality)
        scored.append((quality, sim, cand, contributions))

    # Sort by quality descending
    scored.sort(key=lambda x: x[0], reverse=True)

    # Apply exclusion window — greedy selection
    selected: list[tuple[float, float, StateVector, np.ndarray]] = []
    selected_dates: list[date] = []

    for quality, sim, cand, contribs in scored:
        if quality < min_quality:
            break
        if len(selected) >= n:
            break

        # Check exclusion window
        too_close = False
        for sel_date in selected_dates:
            if abs((cand.as_of_date - sel_date).days) < exclusion_window_days:
                too_close = True
                break
        if too_close:
            continue

        selected.append((quality, sim, cand, contribs))
        selected_dates.append(cand.as_of_date)

    # Build episodes with forward returns
    episodes: list[MatchedEpisode] = []
    agg_contributions = np.zeros(len(DIM_NAMES))

    for quality, sim, cand, contribs in selected:
        dim_contribs = {DIM_NAMES[i]: float(contribs[i]) for i in range(len(DIM_NAMES)) if contribs[i] > 0.01}

        forward = _compute_forward_returns(engine, cand.as_of_date)

        episode = MatchedEpisode(
            as_of_date=cand.as_of_date,
            similarity_score=sim,
            match_quality=quality,
            dimension_contributions=dim_contribs,
            forward_returns=forward,
        )
        episodes.append(episode)
        agg_contributions += contribs

    # Compute aggregate statistics
    mean_quality = np.mean([e.match_quality for e in episodes]) if episodes else 0.0

    # Effective sample size: penalize when one match dominates
    if episodes:
        qualities = np.array([e.match_quality for e in episodes])
        weights = qualities / qualities.sum() if qualities.sum() > 0 else np.ones(len(qualities)) / len(qualities)
        ess = 1.0 / np.sum(weights ** 2)  # Kish's effective sample size
    else:
        ess = 0.0

    # Aggregate dimension importance
    dim_importance = {}
    if agg_contributions.sum() > 0:
        normalized = agg_contributions / agg_contributions.sum()
        dim_importance = {DIM_NAMES[i]: float(normalized[i]) for i in range(len(DIM_NAMES)) if normalized[i] > 0.01}

    result = MatchResult(
        query_date=query_vector.as_of_date,
        query_vector=query_vector,
        episodes=episodes,
        mean_quality=float(mean_quality),
        effective_sample_size=float(ess),
        dimension_importance=dim_importance,
    )

    log.info(
        "Found {n} analogous episodes (mean quality={q:.3f}, ESS={e:.1f})",
        n=len(episodes), q=mean_quality, e=ess,
    )
    return result
