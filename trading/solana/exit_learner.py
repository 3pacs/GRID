"""
Self-learning exit-policy selector.

Implements a contextual multi-armed bandit over the seed policy variants
in :mod:`trading.solana.exit_policy`. Each ``(variant_id, source_type)``
pair is an arm — so the learner automatically specialises per signal
source once enough trades land.

Selection uses **Thompson sampling** with an online Gaussian posterior:

  * for each arm, maintain running mean ``μ`` and Welford sum-of-squares
    ``M2`` (stored in ``solana_policy_variants`` via ``ExitStateStore``)
  * on select, draw a candidate reward ``~ Normal(μ, σ / sqrt(n+1))``
    from each arm's posterior and pick ``argmax``
  * arms with zero observations get an *optimistic* prior sample so the
    bandit explores them until they have real data

Reward is computed from the blended realised pnl of a closed trade,
clipped to ``[-1.0, 3.0]`` to keep variance stable even when a single
memecoin prints a 50x.

The design stays honest by persisting every update to the DB rather than
keeping state in memory — so the learner survives restarts and multiple
workers see the same posterior.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Sequence

from loguru import logger as log
from sqlalchemy.engine import Engine

from trading.solana.exit_policy import (
    ExitPolicy,
    SEED_VARIANTS,
    policy_by_id,
)
from trading.solana.exit_state import (
    SOURCE_UNKNOWN,
    ExitStateStore,
    VariantStatsRow,
)


# Reward shaping — keep variance bounded
REWARD_CLIP_LOW = -1.0
REWARD_CLIP_HIGH = 3.0

# Prior for a never-seen arm: Gaussian centred at 0 with wide variance.
# This forces exploration early without over-weighting the optimism.
PRIOR_MEAN = 0.0
PRIOR_STDDEV = 1.0


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class VariantPosterior:
    variant_id: str
    source_type: str
    n_samples: int
    mean: float
    stddev: float
    sampled: float


# ----------------------------------------------------------------------
# Learner
# ----------------------------------------------------------------------
class ExitLearner:
    """Thompson-sampling bandit over exit policy variants.

    The learner is cheap to construct — it just wraps a store and a
    Random instance. Every meaningful piece of state lives in the DB.
    """

    def __init__(
        self,
        engine: Engine | None = None,
        store: ExitStateStore | None = None,
        variants: Sequence[ExitPolicy] = SEED_VARIANTS,
        random_state: random.Random | None = None,
    ) -> None:
        if store is None:
            if engine is None:
                raise ValueError("either engine or store must be provided")
            store = ExitStateStore(engine)
        self.store = store
        self.variants: tuple[ExitPolicy, ...] = tuple(variants)
        self._rng = random_state or random.Random()

        if not self.variants:
            raise ValueError("ExitLearner requires at least one variant")

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    def ensure_variants(self, source_types: Sequence[str] = (SOURCE_UNKNOWN,)) -> None:
        """Insert a zeroed stats row for every (variant × source_type).

        Safe to call on every startup — it upserts.
        """
        for variant in self.variants:
            for src in source_types:
                self.store.upsert_variant(
                    variant_id=variant.variant_id,
                    source_type=src,
                    params={
                        "description": variant.description,
                        "stop_loss_pct": variant.stop_loss_pct,
                        "trailing_stop_pct": variant.trailing_stop_pct,
                        "max_hold_seconds": variant.max_hold_seconds,
                        "rungs": [
                            {
                                "trigger": r.trigger_pnl_pct,
                                "fraction": r.close_fraction,
                            }
                            for r in variant.take_profit_rungs
                        ],
                    },
                )

    # ------------------------------------------------------------------
    # Selection — Thompson sampling
    # ------------------------------------------------------------------
    def select_policy(
        self,
        source_type: str = SOURCE_UNKNOWN,
    ) -> ExitPolicy:
        """Pick a variant for the next open position."""
        posteriors = self._sample_posteriors(source_type)
        winner = max(posteriors, key=lambda p: p.sampled)
        log.info(
            "ExitLearner select: {v} (src={s}, sampled={x:.3f}, "
            "mean={m:.3f}, n={n})",
            v=winner.variant_id, s=winner.source_type,
            x=winner.sampled, m=winner.mean, n=winner.n_samples,
        )
        policy = policy_by_id(winner.variant_id)
        if policy is None:
            # Defensive: if DB has a stale variant_id, fall back to the
            # first configured one.
            log.warning(
                "Selected variant {v} not in seed list; falling back to {f}",
                v=winner.variant_id, f=self.variants[0].variant_id,
            )
            return self.variants[0]
        return policy

    def debug_posteriors(
        self,
        source_type: str = SOURCE_UNKNOWN,
    ) -> list[VariantPosterior]:
        """Return the sampled posteriors — useful for tests and dashboards."""
        return self._sample_posteriors(source_type)

    def _sample_posteriors(self, source_type: str) -> list[VariantPosterior]:
        posteriors: list[VariantPosterior] = []
        for variant in self.variants:
            stats = self.store.get_variant_stats(variant.variant_id, source_type)
            sampled = self._sample_one(variant.variant_id, source_type, stats)
            posteriors.append(sampled)
        return posteriors

    def _sample_one(
        self,
        variant_id: str,
        source_type: str,
        stats: VariantStatsRow | None,
    ) -> VariantPosterior:
        if stats is None or stats.n_samples == 0:
            # Unseen arm — optimistic prior.
            sampled = self._rng.gauss(PRIOR_MEAN, PRIOR_STDDEV)
            return VariantPosterior(
                variant_id=variant_id,
                source_type=source_type,
                n_samples=0,
                mean=PRIOR_MEAN,
                stddev=PRIOR_STDDEV,
                sampled=sampled,
            )

        # Posterior stddev shrinks as 1/sqrt(n+1) — Bayesian Gaussian update
        # with a weak prior. The prior stddev gets blended in so a
        # 1-sample arm doesn't pretend to be certain.
        base_std = max(stats.reward_stddev, 1e-6)
        posterior_std = base_std / math.sqrt(stats.n_samples + 1)
        # Add a small prior-weighted term so the very first sample
        # doesn't collapse variance to zero.
        posterior_std = math.sqrt(
            posterior_std**2 + (PRIOR_STDDEV**2 / (stats.n_samples + 1))
        )
        sampled = self._rng.gauss(stats.reward_mean, posterior_std)
        return VariantPosterior(
            variant_id=variant_id,
            source_type=source_type,
            n_samples=stats.n_samples,
            mean=stats.reward_mean,
            stddev=posterior_std,
            sampled=sampled,
        )

    # ------------------------------------------------------------------
    # Update — call this once per closed trade
    # ------------------------------------------------------------------
    def record_outcome(
        self,
        variant_id: str,
        source_type: str,
        pnl_pct: float,
    ) -> float:
        """Fold one trade outcome into the variant's posterior.

        Returns the clipped reward that was actually applied.
        """
        reward = _clip_reward(pnl_pct)
        self.store.update_variant_stats(
            variant_id=variant_id,
            source_type=source_type,
            new_reward=reward,
            pnl_pct=pnl_pct,
        )
        return reward

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def stats_snapshot(
        self,
        source_type: str = SOURCE_UNKNOWN,
    ) -> list[dict]:
        """Return one dict per variant summarising current posterior."""
        out: list[dict] = []
        for variant in self.variants:
            stats = self.store.get_variant_stats(variant.variant_id, source_type)
            if stats is None:
                out.append(
                    {
                        "variant_id": variant.variant_id,
                        "source_type": source_type,
                        "n_samples": 0,
                        "reward_mean": 0.0,
                        "reward_stddev": 0.0,
                        "total_pnl_pct": 0.0,
                        "wins": 0,
                        "losses": 0,
                        "win_rate": None,
                    }
                )
                continue
            n = stats.n_samples
            win_rate = (stats.wins / n) if n > 0 else None
            out.append(
                {
                    "variant_id": stats.variant_id,
                    "source_type": stats.source_type,
                    "n_samples": n,
                    "reward_mean": stats.reward_mean,
                    "reward_stddev": stats.reward_stddev,
                    "total_pnl_pct": stats.total_pnl_pct,
                    "wins": stats.wins,
                    "losses": stats.losses,
                    "win_rate": win_rate,
                }
            )
        return out


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _clip_reward(pnl_pct: float) -> float:
    if pnl_pct != pnl_pct:  # NaN check
        return 0.0
    if pnl_pct < REWARD_CLIP_LOW:
        return REWARD_CLIP_LOW
    if pnl_pct > REWARD_CLIP_HIGH:
        return REWARD_CLIP_HIGH
    return float(pnl_pct)
