"""
GRID Subnet Bayesian Reputation System.

Mirrors the Bayesian trust scoring pattern from intelligence/trust_scorer.py
but adapted for miner reputation in the distributed compute network.

Each miner has a Beta(alpha, beta) distribution representing their quality.
- Reputation = alpha / (alpha + beta)
- Starts at 0.5 (uninformative prior: alpha=2, beta=2)
- Good work increases alpha (slowly)
- Bad work increases beta (3x faster — asymmetric punishment)
- Honeypot failures are severe (beta += 5)
- Sybil detection is near-permanent (beta += 50)
- Recency half-life: 14 days (recent behavior matters more)
- Inactivity decay: reputation shrinks toward 0.5 after 7 days idle
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

PRIOR_ALPHA = 2.0
PRIOR_BETA = 2.0
RECENCY_HALFLIFE_DAYS = 14.0
INACTIVITY_THRESHOLD_DAYS = 7
INACTIVITY_DECAY_RATE = 0.05  # per day of inactivity beyond threshold
BAN_THRESHOLD = 0.10
MIN_TASKS_FOR_TIER_UPGRADE = 10

# Reputation tiers for task prioritization
TIER_THRESHOLDS = {
    1: 0.80,  # Top tier — gets hardest/highest-value tasks
    2: 0.60,  # Good — S&P 500 profiles, sector analysis
    3: 0.30,  # Acceptable — generic research
    4: 0.00,  # Probation — honeypots only
}


@dataclass
class ReputationUpdate:
    """A single reputation update event."""
    miner_id: str
    update_type: str  # 'task_pass', 'task_fail', 'honeypot_fail', 'sybil', 'deadline_miss', 'inactivity'
    alpha_delta: float
    beta_delta: float
    new_reputation: float
    timestamp: datetime


class BayesianReputation:
    """Per-miner Bayesian reputation tracker."""

    def __init__(self, alpha: float = PRIOR_ALPHA, beta: float = PRIOR_BETA) -> None:
        self.alpha = alpha
        self.beta = beta
        self.last_update = datetime.now(timezone.utc)
        self.total_tasks = 0

    @property
    def reputation(self) -> float:
        """Current reputation score (0-1)."""
        return self.alpha / (self.alpha + self.beta)

    @property
    def confidence(self) -> float:
        """How much evidence we have (higher = more certain)."""
        return (self.alpha + self.beta - PRIOR_ALPHA - PRIOR_BETA) / 100.0

    @property
    def tier(self) -> int:
        """Current miner tier (1=best, 4=probation)."""
        rep = self.reputation
        for tier, threshold in sorted(TIER_THRESHOLDS.items()):
            if rep >= threshold:
                best_tier = tier
        return best_tier if self.total_tasks >= MIN_TASKS_FOR_TIER_UPGRADE else min(3, best_tier)

    @property
    def is_banned(self) -> bool:
        return self.reputation < BAN_THRESHOLD

    def update_success(self, score: float, task_weight: float = 1.0) -> ReputationUpdate:
        """Update after a successful task completion.

        Alpha grows proportional to score * weight.
        A perfect score (1.0) on a hard task (weight=3) adds 3.0 to alpha.
        A mediocre score (0.4) on an easy task (weight=1) adds 0.4.
        """
        delta = score * task_weight
        self.alpha += delta
        self.total_tasks += 1
        self.last_update = datetime.now(timezone.utc)

        return ReputationUpdate(
            miner_id="", update_type="task_pass",
            alpha_delta=delta, beta_delta=0,
            new_reputation=self.reputation,
            timestamp=self.last_update,
        )

    def update_failure(self, score: float, task_weight: float = 1.0) -> ReputationUpdate:
        """Update after a failed task (score < threshold).

        Beta grows 3x faster than alpha — asymmetric punishment.
        One failure costs ~3 successful tasks to recover from.
        """
        delta = (1.0 - score) * task_weight * 3.0
        self.beta += delta
        self.total_tasks += 1
        self.last_update = datetime.now(timezone.utc)

        return ReputationUpdate(
            miner_id="", update_type="task_fail",
            alpha_delta=0, beta_delta=delta,
            new_reputation=self.reputation,
            timestamp=self.last_update,
        )

    def update_honeypot_fail(self) -> ReputationUpdate:
        """Severe penalty for failing a honeypot (known-answer task).

        Beta += 5 means the miner needs ~25 good tasks to recover.
        This makes lazy/garbage responses extremely costly.
        """
        self.beta += 5.0
        self.total_tasks += 1
        self.last_update = datetime.now(timezone.utc)

        return ReputationUpdate(
            miner_id="", update_type="honeypot_fail",
            alpha_delta=0, beta_delta=5.0,
            new_reputation=self.reputation,
            timestamp=self.last_update,
        )

    def update_sybil_detected(self) -> ReputationUpdate:
        """Near-permanent ban for confirmed Sybil behavior.

        Beta += 50 makes recovery essentially impossible.
        Combined with stake forfeiture, this is the nuclear option.
        """
        self.beta += 50.0
        self.last_update = datetime.now(timezone.utc)

        return ReputationUpdate(
            miner_id="", update_type="sybil",
            alpha_delta=0, beta_delta=50.0,
            new_reputation=self.reputation,
            timestamp=self.last_update,
        )

    def update_deadline_miss(self) -> ReputationUpdate:
        """Moderate penalty for missing a task deadline."""
        self.beta += 2.0
        self.last_update = datetime.now(timezone.utc)

        return ReputationUpdate(
            miner_id="", update_type="deadline_miss",
            alpha_delta=0, beta_delta=2.0,
            new_reputation=self.reputation,
            timestamp=self.last_update,
        )

    def apply_recency_decay(self) -> None:
        """Decay old evidence so recent behavior matters more.

        Applies exponential decay with RECENCY_HALFLIFE_DAYS half-life.
        This shrinks both alpha and beta toward the prior, but preserves
        the ratio (reputation stays the same, confidence decreases).
        """
        now = datetime.now(timezone.utc)
        days_since = (now - self.last_update).total_seconds() / 86400
        if days_since < 1:
            return

        decay = math.exp(-math.log(2) * days_since / RECENCY_HALFLIFE_DAYS)

        # Decay excess above prior toward prior
        self.alpha = PRIOR_ALPHA + (self.alpha - PRIOR_ALPHA) * decay
        self.beta = PRIOR_BETA + (self.beta - PRIOR_BETA) * decay

    def apply_inactivity_decay(self, days_inactive: int) -> None:
        """Slowly decay reputation for inactive miners.

        After INACTIVITY_THRESHOLD_DAYS, reputation drifts toward 0.5.
        This prevents miners from building reputation and selling identities.
        """
        if days_inactive <= INACTIVITY_THRESHOLD_DAYS:
            return

        excess_days = days_inactive - INACTIVITY_THRESHOLD_DAYS
        decay_factor = max(0.5, 1.0 - INACTIVITY_DECAY_RATE * excess_days)

        self.alpha = PRIOR_ALPHA + (self.alpha - PRIOR_ALPHA) * decay_factor
        self.beta = PRIOR_BETA + (self.beta - PRIOR_BETA) * decay_factor


class ReputationManager:
    """Manages reputation for all miners in the database."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_reputation(self, miner_id: str) -> BayesianReputation:
        """Load a miner's reputation from DB."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT rep_alpha, rep_beta, last_active "
                    "FROM compute_miners WHERE miner_id = :mid"
                ), {"mid": miner_id}).fetchone()

                if row and row[0] is not None:
                    rep = BayesianReputation(alpha=row[0], beta=row[1])
                    if row[2]:
                        rep.last_update = row[2]
                    return rep
        except Exception:
            pass

        return BayesianReputation()

    def save_reputation(self, miner_id: str, rep: BayesianReputation) -> None:
        """Save a miner's reputation to DB."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE compute_miners SET "
                    "rep_alpha = :a, rep_beta = :b, rep_last_decay = NOW() "
                    "WHERE miner_id = :mid"
                ), {"a": rep.alpha, "b": rep.beta, "mid": miner_id})
        except Exception as exc:
            log.debug("Failed to save reputation for {m}: {e}", m=miner_id, e=str(exc))

    def update_after_task(
        self, miner_id: str, score: float, task_weight: float = 1.0,
        is_honeypot: bool = False, passed: bool = True,
    ) -> ReputationUpdate:
        """Update reputation after a task result is scored."""
        rep = self.get_reputation(miner_id)

        if is_honeypot and not passed:
            update = rep.update_honeypot_fail()
        elif passed and score >= 0.3:
            update = rep.update_success(score, task_weight)
        else:
            update = rep.update_failure(score, task_weight)

        update.miner_id = miner_id
        self.save_reputation(miner_id, rep)

        if rep.is_banned:
            log.warning("Miner {m} BANNED — reputation {r:.3f}", m=miner_id, r=rep.reputation)
            self._ban_miner(miner_id)

        return update

    def update_sybil(self, miner_id: str) -> ReputationUpdate:
        """Ban a miner for Sybil behavior."""
        rep = self.get_reputation(miner_id)
        update = rep.update_sybil_detected()
        update.miner_id = miner_id
        self.save_reputation(miner_id, rep)
        self._ban_miner(miner_id)
        return update

    def update_deadline_miss(self, miner_id: str) -> ReputationUpdate:
        """Penalize for missing deadline."""
        rep = self.get_reputation(miner_id)
        update = rep.update_deadline_miss()
        update.miner_id = miner_id
        self.save_reputation(miner_id, rep)
        return update

    def get_tier(self, miner_id: str) -> int:
        """Get miner's current tier for task prioritization."""
        rep = self.get_reputation(miner_id)
        return rep.tier

    def is_banned(self, miner_id: str) -> bool:
        """Check if miner is banned."""
        rep = self.get_reputation(miner_id)
        return rep.is_banned

    def decay_all(self) -> int:
        """Apply recency + inactivity decay to all miners. Run in scheduler."""
        count = 0
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT miner_id, rep_alpha, rep_beta, last_active "
                    "FROM compute_miners WHERE rep_alpha IS NOT NULL"
                )).fetchall()

            for row in rows:
                rep = BayesianReputation(alpha=row[1] or PRIOR_ALPHA, beta=row[2] or PRIOR_BETA)
                if row[3]:
                    rep.last_update = row[3]
                    days_inactive = (datetime.now(timezone.utc) - row[3]).days
                    rep.apply_recency_decay()
                    rep.apply_inactivity_decay(days_inactive)
                    self.save_reputation(row[0], rep)
                    count += 1
        except Exception as exc:
            log.debug("Reputation decay failed: {e}", e=str(exc))

        return count

    def get_leaderboard(self, limit: int = 20) -> list[dict]:
        """Top miners by reputation."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT miner_id, rep_alpha, rep_beta, total_tasks, "
                    "rep_alpha / (rep_alpha + rep_beta) as reputation "
                    "FROM compute_miners "
                    "WHERE rep_alpha IS NOT NULL AND banned = FALSE "
                    "ORDER BY reputation DESC LIMIT :lim"
                ), {"lim": limit}).fetchall()
                return [
                    {
                        "miner_id": r[0],
                        "reputation": round(r[4], 4) if r[4] else 0.5,
                        "total_tasks": r[3] or 0,
                        "tier": BayesianReputation(r[1] or 2, r[2] or 2).tier,
                    }
                    for r in rows
                ]
        except Exception:
            return []

    def _ban_miner(self, miner_id: str) -> None:
        """Ban a miner — no more tasks, stake potentially forfeited."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE compute_miners SET banned = TRUE WHERE miner_id = :mid"
                ), {"mid": miner_id})
        except Exception:
            pass
