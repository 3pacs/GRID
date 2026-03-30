"""
GRID Distributed Compute Engine.

BOINC-style volunteer compute network where edge contributors earn API credits
for processing GRID intelligence research tasks.

    ┌──────────────┐      GET /task       ┌──────────────┐
    │  GRID Server  │◄────────────────────│  Edge Client  │
    │  (validator)  │────────────────────►│  (GPU / CPU)  │
    └──────┬───────┘     POST /submit     └──────┬───────┘
           │                                      │
    ┌──────▼───────┐                      ┌──────▼───────┐
    │  PostgreSQL   │                      │  llama.cpp   │
    │  task backlog │                      │  or ollama   │
    └──────────────┘                      └──────────────┘

Revenue model:
    - Miners earn API credits for quality research responses
    - API credits grant access to GRID intelligence (dealer gamma, actor
      networks, options analytics, cross-reference, etc.)
    - 1000 credits = $1 of API access value
    - Quality scoring, cross-validation, honeypots, and sybil detection
      prevent gaming

Future mining integration hook:
    - The `mining_bonus` field in rewards is reserved for future
      crypto mining integration (Akash, Render, or direct token rewards)
    - See GRID roadmap for timing

Usage (server):
    from subnet.distributed_compute import compute_router
    app.include_router(compute_router)

Usage (edge):
    python subnet/distributed_compute.py --grid-url https://grid.stepdad.finance
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import secrets
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from loguru import logger as log

# Ensure grid root is on path
_GRID_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

# ── Hardening Module Imports (graceful degradation) ──────────────────
_HARDENING_AVAILABLE = {
    "reputation": False,
    "stake_verifier": False,
    "honeypot": False,
    "semantic_scorer": False,
    "dynamic_scorer": False,
    "sybil_detector": False,
}

try:
    from subnet.reputation import ReputationManager
    _HARDENING_AVAILABLE["reputation"] = True
except ImportError:
    log.debug("Hardening: reputation module not available")

try:
    from subnet.stake_verifier import StakeVerifier
    _HARDENING_AVAILABLE["stake_verifier"] = True
except ImportError:
    log.debug("Hardening: stake_verifier module not available")

try:
    from subnet.honeypot import HoneypotInjector
    _HARDENING_AVAILABLE["honeypot"] = True
except ImportError:
    log.debug("Hardening: honeypot module not available")

try:
    from subnet.semantic_scorer import SemanticScorer
    _HARDENING_AVAILABLE["semantic_scorer"] = True
except ImportError:
    log.debug("Hardening: semantic_scorer module not available")

try:
    from subnet.dynamic_scorer import DynamicScorer
    _HARDENING_AVAILABLE["dynamic_scorer"] = True
except ImportError:
    log.debug("Hardening: dynamic_scorer module not available")

try:
    from subnet.sybil_detector import SybilDetector
    _HARDENING_AVAILABLE["sybil_detector"] = True
except ImportError:
    log.debug("Hardening: sybil_detector module not available")


# ═══════════════════════════════════════════════════════════════════════════
# PART 1: DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════


class TaskStatus(str, Enum):
    PENDING = "pending"
    DISTRIBUTED = "distributed"
    SUBMITTED = "submitted"
    VALIDATED = "validated"
    REJECTED = "rejected"
    EXPIRED = "expired"


class RewardType(str, Enum):
    API_CREDITS = "api_credits"
    MINING_BONUS = "mining_bonus"  # reserved for future crypto integration


@dataclass
class MinerIdentity:
    """Tracks a registered edge miner."""
    miner_id: str
    hotkey: str = ""
    api_key: str = ""
    reputation: float = 0.5
    total_tasks: int = 0
    total_accepted: int = 0
    api_credits: int = 0
    stake_deposited: float = 0.0
    registered_at: str = ""
    last_seen: str = ""
    is_banned: bool = False


@dataclass
class TaskAssignment:
    """A task assigned to a specific miner, with cross-validation tracking."""
    task_id: int
    miner_id: str
    assigned_at: float = 0.0
    deadline: float = 0.0
    cross_validation_group: str = ""


@dataclass
class EarningsSnapshot:
    """Current earnings for a miner."""
    api_credits: int = 0
    mining_bonus: float = 0.0  # reserved for future crypto integration
    tasks_completed: int = 0
    avg_quality_score: float = 0.0
    reputation: float = 0.5


# ═══════════════════════════════════════════════════════════════════════════
# PART 2: SERVER SIDE — TASK DISTRIBUTION & REWARD ENGINE
# ═══════════════════════════════════════════════════════════════════════════


class ComputeCoordinator:
    """Server-side coordinator for distributed compute.

    Manages the lifecycle: task distribution -> submission -> scoring ->
    cross-validation -> reward calculation -> storage.
    """

    # Cross-validation: send each task to this many miners
    CROSS_VALIDATION_FACTOR = 3

    # Minimum reputation to receive tasks (prevents new-account spam)
    MIN_REPUTATION_FOR_TASKS = 0.1

    # Internal GRID miners — auto-verified, boosted reputation
    INTERNAL_HOTKEYS = {
        "test_claude_agent", "grid_hermes_operator", "grid_qwen_worker",
    }

    # Minimum stake to participate (can be $0 for invite-only beta)
    MIN_STAKE = 0.01

    # Task deadline in seconds (2 minutes for a single inference)
    TASK_DEADLINE_SECONDS = 120

    # Quality threshold for accepting a response
    MIN_QUALITY_SCORE = 0.3

    # API credits earned per quality point (score * difficulty * multiplier)
    # At avg score 0.5, difficulty 1: 50 credits/task = $0.05
    # At avg score 0.7, difficulty 3 (ICIJ): 210 credits/task = $0.21
    API_CREDIT_MULTIPLIER = 100

    # 1000 credits = $1 API access
    CREDITS_PER_DOLLAR = 1000

    # Difficulty multipliers — harder tasks earn more
    DIFFICULTY_WEIGHT = {1: 1.0, 2: 2.0, 3: 3.0}

    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self._ensure_tables()

        # ── Initialize hardening modules ──────────────────────────────
        self.reputation = None
        self.stake_verifier = None
        self.honeypot = None
        self.semantic_scorer = None
        self.dynamic_scorer = None
        self.sybil_detector = None

        try:
            if _HARDENING_AVAILABLE["reputation"]:
                self.reputation = ReputationManager(engine)
        except Exception as exc:
            log.warning("Failed to init ReputationManager: {e}", e=str(exc))

        try:
            if _HARDENING_AVAILABLE["stake_verifier"]:
                self.stake_verifier = StakeVerifier(engine)
        except Exception as exc:
            log.warning("Failed to init StakeVerifier: {e}", e=str(exc))

        try:
            if _HARDENING_AVAILABLE["honeypot"]:
                self.honeypot = HoneypotInjector(engine)
        except Exception as exc:
            log.warning("Failed to init HoneypotInjector: {e}", e=str(exc))

        try:
            if _HARDENING_AVAILABLE["semantic_scorer"]:
                self.semantic_scorer = SemanticScorer(engine)
        except Exception as exc:
            log.warning("Failed to init SemanticScorer: {e}", e=str(exc))

        try:
            if _HARDENING_AVAILABLE["dynamic_scorer"]:
                self.dynamic_scorer = DynamicScorer()
        except Exception as exc:
            log.warning("Failed to init DynamicScorer: {e}", e=str(exc))

        try:
            if _HARDENING_AVAILABLE["sybil_detector"]:
                self.sybil_detector = SybilDetector(engine)
        except Exception as exc:
            log.warning("Failed to init SybilDetector: {e}", e=str(exc))

    def _ensure_tables(self) -> None:
        """Create distributed compute tables."""
        from sqlalchemy import text

        ddl = """
        CREATE TABLE IF NOT EXISTS compute_miners (
            miner_id       TEXT PRIMARY KEY,
            hotkey         TEXT DEFAULT '',
            api_key        TEXT DEFAULT '',
            reputation     DOUBLE PRECISION DEFAULT 0.5,
            total_tasks    BIGINT DEFAULT 0,
            total_accepted BIGINT DEFAULT 0,
            api_credits    BIGINT DEFAULT 0,
            stake_deposited DOUBLE PRECISION DEFAULT 0,
            is_banned      BOOLEAN DEFAULT FALSE,
            registered_at  TIMESTAMPTZ DEFAULT NOW(),
            last_seen      TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS compute_assignments (
            id                    BIGSERIAL PRIMARY KEY,
            task_id               BIGINT NOT NULL,
            miner_id              TEXT NOT NULL,
            assigned_at           TIMESTAMPTZ DEFAULT NOW(),
            deadline              TIMESTAMPTZ NOT NULL,
            cross_validation_group TEXT DEFAULT '',
            response              TEXT DEFAULT '',
            score                 DOUBLE PRECISION DEFAULT 0,
            status                TEXT DEFAULT 'assigned',
            submitted_at          TIMESTAMPTZ,
            scored_at             TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS idx_assignments_miner
            ON compute_assignments(miner_id, status);
        CREATE INDEX IF NOT EXISTS idx_assignments_cv_group
            ON compute_assignments(cross_validation_group);
        CREATE INDEX IF NOT EXISTS idx_assignments_task
            ON compute_assignments(task_id, status);

        CREATE TABLE IF NOT EXISTS compute_rewards (
            id          BIGSERIAL PRIMARY KEY,
            miner_id    TEXT NOT NULL,
            reward_type TEXT NOT NULL,
            amount      DOUBLE PRECISION NOT NULL,
            task_id     BIGINT,
            reason      TEXT DEFAULT '',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_rewards_miner
            ON compute_rewards(miner_id, created_at);
        """

        try:
            with self.engine.begin() as conn:
                for statement in ddl.split(";"):
                    stmt = statement.strip()
                    if stmt:
                        conn.execute(text(stmt))
        except Exception as exc:
            log.warning("Table creation (non-fatal): {e}", e=str(exc))

    # ── Miner Registration ─────────────────────────────────────────────

    def register_miner(self, hotkey: str = "", api_key: str = "") -> MinerIdentity:
        """Register a new edge miner or return existing one."""
        from sqlalchemy import text

        identity_seed = hotkey or api_key or secrets.token_hex(16)
        miner_id = hashlib.sha256(identity_seed.encode()).hexdigest()[:16]

        with self.engine.begin() as conn:
            existing = conn.execute(text(
                "SELECT miner_id FROM compute_miners WHERE miner_id = :mid"
            ), {"mid": miner_id}).fetchone()

            if existing:
                conn.execute(text(
                    "UPDATE compute_miners SET last_seen = NOW() WHERE miner_id = :mid"
                ), {"mid": miner_id})
            else:
                conn.execute(text(
                    "INSERT INTO compute_miners (miner_id, hotkey, api_key) "
                    "VALUES (:mid, :hk, :ak)"
                ), {"mid": miner_id, "hk": hotkey, "ak": api_key})

            # Auto-verify internal GRID miners
            if hotkey in self.INTERNAL_HOTKEYS:
                conn.execute(text(
                    "UPDATE compute_miners SET stake_verified = TRUE, "
                    "stake_verified_at = NOW(), stake_deposited = 100.0, "
                    "reputation = 0.9, rep_alpha = 20.0, rep_beta = 2.0 "
                    "WHERE miner_id = :mid"
                ), {"mid": miner_id})

        return MinerIdentity(miner_id=miner_id, hotkey=hotkey, api_key=api_key)

    def _is_internal_miner(self, miner_id: str) -> bool:
        """Check if miner_id belongs to an internal GRID miner."""
        for hotkey in self.INTERNAL_HOTKEYS:
            if hashlib.sha256(hotkey.encode()).hexdigest()[:16] == miner_id:
                return True
        return False

    # ── Task Distribution ──────────────────────────────────────────────

    def pull_task(self, miner_id: str) -> dict | None:
        """Pull the next available task for a miner.

        Tasks are assigned with cross-validation: the same task goes to
        CROSS_VALIDATION_FACTOR miners. Once all have submitted, responses
        are compared for consistency.

        Returns None if no tasks available or miner is banned/low-rep.
        """
        from sqlalchemy import text

        # ── Hardening gate checks ─────────────────────────────────────
        is_internal = self._is_internal_miner(miner_id)

        # 1. Stake verification (skip for internal miners)
        try:
            if self.stake_verifier and not is_internal and not self.stake_verifier.is_verified(miner_id):
                log.debug("Miner {m} rejected: stake not verified", m=miner_id[:8])
                return None
        except Exception as exc:
            log.debug("Stake check failed (non-fatal): {e}", e=str(exc))

        # 2. Rate limiting (skip for internal miners)
        try:
            if self.sybil_detector and not is_internal and not self.sybil_detector.check_rate_limit(miner_id):
                log.debug("Miner {m} rejected: rate limited", m=miner_id[:8])
                return None
        except Exception as exc:
            log.debug("Rate limit check failed (non-fatal): {e}", e=str(exc))

        # 3. Reputation ban check
        try:
            if self.reputation and not is_internal and self.reputation.is_banned(miner_id):
                log.debug("Miner {m} rejected: banned by reputation", m=miner_id[:8])
                return None
        except Exception as exc:
            log.debug("Reputation ban check failed (non-fatal): {e}", e=str(exc))

        # 4. Get miner tier for task filtering
        miner_tier = 3  # default: acceptable
        try:
            if self.reputation:
                miner_tier = self.reputation.get_tier(miner_id)
        except Exception as exc:
            log.debug("Tier lookup failed (non-fatal): {e}", e=str(exc))

        # Legacy eligibility check (fallback)
        with self.engine.connect() as conn:
            miner = conn.execute(text(
                "SELECT reputation, is_banned, stake_deposited "
                "FROM compute_miners WHERE miner_id = :mid"
            ), {"mid": miner_id}).fetchone()

        if not miner:
            return None
        if not is_internal and miner[1]:  # is_banned
            return None
        if not is_internal and miner[0] < self.MIN_REPUTATION_FOR_TASKS:
            return None

        # 5. Honeypot injection
        try:
            if self.honeypot:
                needed = self.honeypot.needs_injection()
                if needed > 0:
                    self.honeypot.generate_batch(n=min(needed, 5))
        except Exception as exc:
            log.debug("Honeypot injection check failed (non-fatal): {e}", e=str(exc))

        # Find a task — prefer joining existing cross-validation groups
        with self.engine.begin() as conn:
            row = conn.execute(text("""
                SELECT ca.task_id, ca.cross_validation_group,
                       t.task_type, t.prompt, t.context
                FROM compute_assignments ca
                JOIN llm_task_backlog t ON t.id = ca.task_id
                WHERE ca.status = 'assigned'
                  AND ca.task_id NOT IN (
                      SELECT task_id FROM compute_assignments WHERE miner_id = :mid
                  )
                GROUP BY ca.task_id, ca.cross_validation_group,
                         t.task_type, t.prompt, t.context
                HAVING COUNT(*) < :cvf
                ORDER BY RANDOM()
                LIMIT 1
            """), {"mid": miner_id, "cvf": self.CROSS_VALIDATION_FACTOR}).fetchone()

            if row:
                task_id = row[0]
                cv_group = row[1]
                task_type = row[2]
                prompt = row[3]
                context = row[4]
            else:
                # Pull a fresh task — filter by difficulty based on tier
                min_priority = max(1, miner_tier)
                fresh = conn.execute(text("""
                    UPDATE llm_task_backlog SET status = 'distributed'
                    WHERE id = (
                        SELECT id FROM llm_task_backlog
                        WHERE status = 'pending'
                          AND priority >= :min_pri
                          AND id NOT IN (
                              SELECT task_id FROM compute_assignments
                              WHERE miner_id = :mid
                          )
                        ORDER BY priority ASC, RANDOM()
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, task_type, prompt, context
                """), {"mid": miner_id, "min_pri": min_priority}).fetchone()

                if not fresh:
                    return None

                task_id = fresh[0]
                task_type = fresh[1]
                prompt = fresh[2]
                context = fresh[3]
                cv_group = f"cv_{task_id}_{secrets.token_hex(4)}"

            # Record the assignment
            from datetime import timedelta
            deadline_ts = datetime.now(timezone.utc) + timedelta(seconds=self.TASK_DEADLINE_SECONDS)

            conn.execute(text("""
                INSERT INTO compute_assignments
                    (task_id, miner_id, deadline, cross_validation_group, status)
                VALUES (:tid, :mid, :dl, :cvg, 'assigned')
            """), {
                "tid": task_id,
                "mid": miner_id,
                "dl": deadline_ts.isoformat(),
                "cvg": cv_group,
            })

            conn.execute(text(
                "UPDATE compute_miners SET last_seen = NOW() WHERE miner_id = :mid"
            ), {"mid": miner_id})

        ctx = context if isinstance(context, dict) else json.loads(context or "{}")

        return {
            "task_id": task_id,
            "task_type": task_type,
            "prompt": prompt,
            "context": ctx,
            "deadline_seconds": self.TASK_DEADLINE_SECONDS,
            "cross_validation_group": cv_group,
        }

    # ── Submission & Scoring ───────────────────────────────────────────

    def submit_result(self, miner_id: str, task_id: int, response: str) -> dict:
        """Accept a miner's response, score it, and trigger cross-validation."""
        from sqlalchemy import text

        with self.engine.begin() as conn:
            assignment = conn.execute(text("""
                SELECT id, cross_validation_group, deadline
                FROM compute_assignments
                WHERE task_id = :tid AND miner_id = :mid AND status = 'assigned'
                LIMIT 1
            """), {"tid": task_id, "mid": miner_id}).fetchone()

            if not assignment:
                return {"error": "no_active_assignment"}

            assignment_id = assignment[0]
            cv_group = assignment[1]

            # Check deadline
            deadline = assignment[2]
            if deadline and datetime.now(timezone.utc) > deadline:
                conn.execute(text(
                    "UPDATE compute_assignments SET status = 'expired' WHERE id = :aid"
                ), {"aid": assignment_id})
                # Reputation penalty for deadline miss
                try:
                    if self.reputation:
                        self.reputation.update_deadline_miss(miner_id)
                except Exception:
                    pass
                return {"error": "deadline_expired"}

            # ── Score the response ────────────────────────────────────

            # Check if this is a honeypot task
            honeypot_score = None
            is_honeypot = False
            try:
                if self.honeypot and self.honeypot.is_honeypot(task_id):
                    is_honeypot = True
                    honeypot_score = self.honeypot.score_honeypot(task_id, response)
            except Exception as exc:
                log.debug("Honeypot scoring failed (non-fatal): {e}", e=str(exc))

            # Dynamic scoring
            score = {"total": 0.0, "tier": "poor"}
            try:
                if self.dynamic_scorer:
                    score = self.dynamic_scorer.score(response, task_id=task_id)
                else:
                    # Fallback: basic length/keyword scoring
                    words = len(response.split())
                    score["total"] = min(1.0, max(0.1, words / 500))
                    score["tier"] = "good" if score["total"] > 0.6 else "fair" if score["total"] > 0.3 else "poor"
            except Exception as exc:
                log.debug("Dynamic scoring failed, using fallback: {e}", e=str(exc))
                words = len(response.split())
                score["total"] = min(1.0, max(0.1, words / 500))

            # Blend honeypot score if applicable
            if honeypot_score is not None:
                score["total"] = 0.4 * honeypot_score + 0.6 * score["total"]
                score["honeypot"] = True

            # Save the response and score
            conn.execute(text("""
                UPDATE compute_assignments
                SET response = :resp, score = :score, status = 'submitted',
                    submitted_at = NOW(), scored_at = NOW()
                WHERE id = :aid
            """), {"resp": response, "score": score["total"], "aid": assignment_id})

            # Update miner task count
            conn.execute(text(
                "UPDATE compute_miners SET total_tasks = total_tasks + 1 WHERE miner_id = :mid"
            ), {"mid": miner_id})

        # Cross-validation check
        cv_result = self._cross_validate(cv_group)

        # Update reputation
        try:
            if self.reputation:
                passed = score["total"] >= self.MIN_QUALITY_SCORE
                self.reputation.update_after_task(
                    miner_id, score["total"],
                    is_honeypot=is_honeypot,
                    passed=passed,
                )
                if is_honeypot and honeypot_score is not None and honeypot_score < 0.2:
                    self.reputation.update_after_task(
                        miner_id, honeypot_score,
                        is_honeypot=True, passed=False,
                    )
        except Exception as exc:
            log.debug("Reputation update failed (non-fatal): {e}", e=str(exc))

        # Calculate rewards
        rewards = self._calculate_rewards(miner_id, task_id, score, cv_result)

        return {
            "assignment_id": assignment_id,
            "score": score,
            "cross_validation": cv_result,
            "rewards": rewards,
        }

    def _cross_validate(self, cv_group: str) -> dict:
        """Check if all miners in a CV group have submitted, then compare."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            submissions = conn.execute(text("""
                SELECT miner_id, response, score
                FROM compute_assignments
                WHERE cross_validation_group = :cvg AND status = 'submitted'
            """), {"cvg": cv_group}).fetchall()

        if len(submissions) < self.CROSS_VALIDATION_FACTOR:
            return {"status": "pending", "submitted": len(submissions), "required": self.CROSS_VALIDATION_FACTOR}

        # All miners submitted — compare responses
        scores = [float(row[2] or 0) for row in submissions]
        avg_score = sum(scores) / len(scores) if scores else 0

        # Detect outliers (score deviates more than 0.3 from average)
        outliers = []
        for row in submissions:
            if abs(float(row[2] or 0) - avg_score) > 0.3:
                outliers.append(row[0])

        if outliers:
            self._penalize_outliers(outliers, cv_group)

        # Semantic similarity check if available
        try:
            if self.semantic_scorer and len(submissions) >= 2:
                responses = [row[1] for row in submissions]
                collusion = self.semantic_scorer.detect_collusion(responses)
                if collusion:
                    log.warning("Collusion detected in group {g}", g=cv_group)
        except Exception:
            pass

        return {
            "status": "complete",
            "submitted": len(submissions),
            "avg_score": round(avg_score, 3),
            "outliers": outliers,
        }

    def _penalize_outliers(self, miner_ids: list[str], cv_group: str) -> None:
        """Penalize miners whose responses deviate significantly from peers."""
        from sqlalchemy import text

        with self.engine.begin() as conn:
            for mid in miner_ids:
                conn.execute(text("""
                    UPDATE compute_miners
                    SET reputation = GREATEST(0, reputation - 0.05)
                    WHERE miner_id = :mid
                """), {"mid": mid})

                conn.execute(text("""
                    UPDATE compute_assignments
                    SET status = 'rejected'
                    WHERE cross_validation_group = :cvg AND miner_id = :mid
                """), {"cvg": cv_group, "mid": mid})

                log.warning(
                    "Miner {mid} penalized for outlier response in group {g}",
                    mid=mid, g=cv_group,
                )

    # ── Reward Calculation ─────────────────────────────────────────────

    def _calculate_rewards(
        self, miner_id: str, task_id: int, score: dict, cv_result: dict
    ) -> dict:
        """Calculate and record API credit rewards.

        Economics:
            credits = floor(score * difficulty_weight * API_CREDIT_MULTIPLIER)
            1000 credits = $1 of API access

            At avg score 0.5, difficulty 1: 50 credits = $0.05/task
            At avg score 0.7, difficulty 3: 210 credits = $0.21/task
            A miner doing 100 tasks/day at avg earns ~$5-20/day in API access

        Reputation update:
            score > 0.6: rep += 0.01
            score 0.4-0.6: rep += 0.005
            score < 0.4: rep -= 0.03
        """
        from sqlalchemy import text

        total_score = score.get("total", 0)
        was_outlier = miner_id in cv_result.get("outliers", [])

        if was_outlier or total_score < self.MIN_QUALITY_SCORE:
            return {"api_credits": 0, "api_value_usd": 0, "reason": "below_threshold"}

        # Look up task difficulty (priority 1=high value → difficulty 3)
        difficulty = 1
        with self.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT priority FROM llm_task_backlog WHERE id = :tid"
            ), {"tid": task_id}).fetchone()
            if row:
                difficulty = max(1, 5 - (row[0] or 3))

        difficulty_weight = self.DIFFICULTY_WEIGHT.get(difficulty, 1.0)
        credits = int(total_score * difficulty_weight * self.API_CREDIT_MULTIPLIER)

        with self.engine.begin() as conn:
            # Grant credits + update reputation
            conn.execute(text("""
                UPDATE compute_miners
                SET api_credits = api_credits + :credits,
                    total_accepted = total_accepted + 1,
                    reputation = LEAST(1.0, reputation + CASE
                        WHEN :score > 0.6 THEN 0.01
                        WHEN :score > 0.4 THEN 0.005
                        ELSE -0.03
                    END)
                WHERE miner_id = :mid
            """), {"credits": credits, "score": total_score, "mid": miner_id})

            # Record reward
            conn.execute(text("""
                INSERT INTO compute_rewards (miner_id, reward_type, amount, task_id, reason)
                VALUES (:mid, 'api_credits', :amt, :tid, :reason)
            """), {
                "mid": miner_id,
                "amt": credits,
                "tid": task_id,
                "reason": f"score={total_score:.3f} diff={difficulty} weight={difficulty_weight}",
            })

        return {
            "api_credits": credits,
            "api_value_usd": round(credits / self.CREDITS_PER_DOLLAR, 4),
            "difficulty": difficulty,
            "reputation_delta": 0.01 if total_score > 0.6 else (0.005 if total_score > 0.4 else -0.03),
        }

    # ── Miner Stats ────────────────────────────────────────────────────

    def get_miner_stats(self, miner_id: str) -> dict:
        """Return full earnings and performance stats for a miner."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            miner = conn.execute(text("""
                SELECT reputation, total_tasks, total_accepted,
                       api_credits, stake_deposited, registered_at, last_seen
                FROM compute_miners
                WHERE miner_id = :mid
            """), {"mid": miner_id}).fetchone()

            if not miner:
                return {"error": "miner_not_found"}

            rewards = conn.execute(text("""
                SELECT reward_type, SUM(amount) as total, COUNT(*) as count
                FROM compute_rewards
                WHERE miner_id = :mid
                GROUP BY reward_type
            """), {"mid": miner_id}).fetchall()

            recent = conn.execute(text("""
                SELECT AVG(score), COUNT(*), MAX(submitted_at)
                FROM compute_assignments
                WHERE miner_id = :mid AND status IN ('submitted', 'validated')
                  AND submitted_at > NOW() - INTERVAL '7 days'
            """), {"mid": miner_id}).fetchone()

        reward_summary = {}
        for row in rewards:
            reward_summary[row[0]] = {"total": float(row[1]), "count": int(row[2])}

        api_credits = int(miner[3])
        return {
            "miner_id": miner_id,
            "reputation": float(miner[0]),
            "total_tasks": int(miner[1]),
            "total_accepted": int(miner[2]),
            "acceptance_rate": round(miner[2] / max(miner[1], 1), 3),
            "earnings": {
                "api_credits": api_credits,
                "api_value_usd": round(api_credits / self.CREDITS_PER_DOLLAR, 2),
            },
            "stake": float(miner[4]),
            "rewards_breakdown": reward_summary,
            "recent_7d": {
                "avg_score": round(float(recent[0] or 0), 3),
                "tasks": int(recent[1] or 0),
                "last_submission": str(recent[2] or ""),
            },
            "registered_at": str(miner[5]),
            "last_seen": str(miner[6]),
        }

    # ── Leaderboard ────────────────────────────────────────────────────

    def get_leaderboard(self, limit: int = 25) -> list[dict]:
        """Top miners by reputation and task volume."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT miner_id, reputation, total_tasks, total_accepted, api_credits
                FROM compute_miners
                WHERE is_banned = FALSE AND total_tasks > 0
                ORDER BY reputation DESC, total_accepted DESC
                LIMIT :lim
            """), {"lim": limit}).fetchall()

        return [
            {
                "rank": i + 1,
                "miner_id": row[0],
                "reputation": round(float(row[1]), 3),
                "total_tasks": int(row[2]),
                "acceptance_rate": round(int(row[3]) / max(int(row[2]), 1), 3),
                "api_credits": int(row[4]),
                "api_value_usd": round(int(row[4]) / self.CREDITS_PER_DOLLAR, 2),
            }
            for i, row in enumerate(rows)
        ]

    # ── Stake Management ───────────────────────────────────────────────

    def record_stake(self, miner_id: str, amount: float) -> dict:
        """Record a stake deposit from a miner."""
        from sqlalchemy import text

        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE compute_miners
                SET stake_deposited = stake_deposited + :amt
                WHERE miner_id = :mid
            """), {"amt": amount, "mid": miner_id})

        return {"miner_id": miner_id, "stake_added": amount}

    # ── Expire Stale Assignments ───────────────────────────────────────

    def expire_stale_assignments(self) -> int:
        """Mark overdue assignments as expired and penalize miners."""
        from sqlalchemy import text

        with self.engine.begin() as conn:
            expired = conn.execute(text("""
                UPDATE compute_assignments
                SET status = 'expired'
                WHERE status = 'assigned' AND deadline < NOW()
                RETURNING miner_id
            """)).fetchall()

            for row in expired:
                conn.execute(text("""
                    UPDATE compute_miners
                    SET reputation = GREATEST(0, reputation - 0.02)
                    WHERE miner_id = :mid
                """), {"mid": row[0]})

        if expired:
            log.info("Expired {n} stale assignments", n=len(expired))
        return len(expired)


# ═══════════════════════════════════════════════════════════════════════════
# PART 3: FASTAPI ROUTES — TASK DISTRIBUTION API
# ═══════════════════════════════════════════════════════════════════════════

try:
    from fastapi import APIRouter, Depends, HTTPException, Header, Request
    from pydantic import BaseModel

    compute_router = APIRouter(prefix="/api/v1/compute", tags=["compute"])

    class RegisterRequest(BaseModel):
        hotkey: str = ""
        api_key: str = ""

    class SubmitRequest(BaseModel):
        task_id: int
        response: str

    class StakeRequest(BaseModel):
        amount: float

    def _get_coordinator() -> ComputeCoordinator:
        from db import get_engine
        return ComputeCoordinator(get_engine())

    def _extract_miner_id(authorization: str = Header(default="")) -> str:
        """Extract miner_id from Authorization header (Bearer <api_key>)."""
        if not authorization:
            raise HTTPException(401, "Missing Authorization header")
        parts = authorization.split(" ", 1)
        if len(parts) != 2:
            raise HTTPException(401, "Invalid Authorization format")
        _auth_type, credential = parts
        return hashlib.sha256(credential.encode()).hexdigest()[:16]

    @compute_router.post("/register")
    def register_miner(body: RegisterRequest, coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Register as a compute contributor. Returns miner_id."""
        identity = coord.register_miner(hotkey=body.hotkey, api_key=body.api_key)
        return {"miner_id": identity.miner_id, "reputation": identity.reputation, "status": "registered"}

    @compute_router.get("/task")
    def pull_task(miner_id: str = Depends(_extract_miner_id),
                  coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Pull the next research task. Returns 204 if none available."""
        task = coord.pull_task(miner_id)
        if not task:
            raise HTTPException(204, "No tasks available")
        return task

    @compute_router.post("/submit")
    def submit_result(body: SubmitRequest,
                      miner_id: str = Depends(_extract_miner_id),
                      coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Submit a completed research response."""
        if not body.response or len(body.response.strip()) < 50:
            raise HTTPException(400, "Response too short (minimum 50 characters)")
        result = coord.submit_result(miner_id, body.task_id, body.response)
        if "error" in result:
            raise HTTPException(400, result["error"])
        return result

    @compute_router.get("/stats")
    def miner_stats(miner_id: str = Depends(_extract_miner_id),
                    coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Get earnings and performance stats."""
        stats = coord.get_miner_stats(miner_id)
        if "error" in stats:
            raise HTTPException(404, stats["error"])
        return stats

    @compute_router.get("/leaderboard")
    def leaderboard(limit: int = 25,
                    coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Top miners by reputation and contribution volume."""
        return coord.get_leaderboard(limit=min(limit, 100))

    @compute_router.post("/stake")
    def record_stake(body: StakeRequest,
                     miner_id: str = Depends(_extract_miner_id),
                     coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Record a stake deposit. Required to participate."""
        if body.amount <= 0:
            raise HTTPException(400, "Stake amount must be positive")
        return coord.record_stake(miner_id, body.amount)

    # Admin endpoints
    try:
        from api.auth import require_auth
        _admin_auth_available = True
    except ImportError:
        _admin_auth_available = False

    if _admin_auth_available:
        @compute_router.post("/admin/expire-stale")
        def expire_stale(
            _token: str = Depends(require_auth),
            coord: ComputeCoordinator = Depends(_get_coordinator),
        ):
            """Admin: expire overdue assignments and penalize miners."""
            return {"expired": coord.expire_stale_assignments()}
    else:
        @compute_router.post("/admin/expire-stale")
        def expire_stale(coord: ComputeCoordinator = Depends(_get_coordinator)):
            """Admin: expire overdue assignments (WARNING: unprotected)."""
            return {"expired": coord.expire_stale_assignments()}

except ImportError:
    compute_router = None  # type: ignore
    log.debug("FastAPI not available — server routes disabled")


# ═══════════════════════════════════════════════════════════════════════════
# PART 4: EDGE CLIENT
# ═══════════════════════════════════════════════════════════════════════════


class GPUDetector:
    """Detect available GPU hardware on the edge node."""

    @staticmethod
    def detect() -> dict:
        gpu_info = {
            "has_gpu": False,
            "vendor": "none",
            "name": "CPU only",
            "vram_mb": 0,
            "cuda_available": False,
            "recommended_quant": "Q4_K_M",
        }

        # NVIDIA
        try:
            import subprocess
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split(",")
                gpu_info["has_gpu"] = True
                gpu_info["vendor"] = "nvidia"
                gpu_info["name"] = parts[0].strip()
                gpu_info["vram_mb"] = int(float(parts[1].strip()))
                gpu_info["cuda_available"] = True
                vram = gpu_info["vram_mb"]
                if vram >= 24000:
                    gpu_info["recommended_quant"] = "Q8_0"
                elif vram >= 12000:
                    gpu_info["recommended_quant"] = "Q5_K_M"
                elif vram >= 8000:
                    gpu_info["recommended_quant"] = "Q4_K_M"
                else:
                    gpu_info["recommended_quant"] = "Q3_K_M"
                return gpu_info
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # AMD ROCm
        try:
            result = subprocess.run(
                ["rocm-smi", "--showmeminfo", "vram", "--csv"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                gpu_info["has_gpu"] = True
                gpu_info["vendor"] = "amd"
                gpu_info["name"] = "AMD ROCm GPU"
                gpu_info["recommended_quant"] = "Q4_K_M"
                return gpu_info
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Apple Silicon
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            gpu_info["has_gpu"] = True
            gpu_info["vendor"] = "apple"
            gpu_info["name"] = "Apple Silicon (Metal)"
            gpu_info["recommended_quant"] = "Q4_K_M"

        return gpu_info


class EdgeMiner:
    """Edge client that pulls GRID research tasks and earns API credits.

    State machine:
        IDLE -> FETCHING_TASK -> GPU_INFERENCE -> SUBMITTING -> IDLE
    """

    class State(str, Enum):
        IDLE = "idle"
        FETCHING = "fetching_task"
        INFERENCE = "gpu_inference"
        SUBMITTING = "submitting"
        COOLDOWN = "cooldown"

    def __init__(
        self,
        grid_url: str = "http://localhost:8000",
        api_key: str = "",
        backend: str = "llamacpp",
    ) -> None:
        self.grid_url = grid_url.rstrip("/")
        self.api_key = api_key
        self.backend = backend
        self.state = self.State.IDLE

        # GPU inference
        from subnet.miner import LocalInference
        self.inference = LocalInference(backend=backend)

        # Stats
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.total_earnings = EarningsSnapshot()
        self._start_time = time.monotonic()

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}"}

    async def start(self) -> None:
        """Start the edge miner."""
        gpu = GPUDetector.detect()
        log.info("GPU detected: {name} ({vram}MB)", name=gpu["name"], vram=gpu["vram_mb"])
        await self._register()
        await self._task_loop()

    async def _register(self) -> None:
        """Register with the GRID compute coordinator."""
        import requests

        try:
            resp = requests.post(
                f"{self.grid_url}/api/v1/compute/register",
                json={"api_key": self.api_key},
                headers=self._headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                log.info("Registered as miner {mid}", mid=data.get("miner_id"))
            else:
                log.warning("Registration failed: {s}", s=resp.status_code)
        except Exception as exc:
            log.warning("Registration error (will retry): {e}", e=str(exc))

    async def _task_loop(self) -> None:
        """Main loop: pull task -> inference -> submit -> repeat."""
        import requests

        consecutive_empty = 0

        while True:
            try:
                # ── 1. Fetch task ──
                self.state = self.State.FETCHING

                try:
                    resp = requests.get(
                        f"{self.grid_url}/api/v1/compute/task",
                        headers=self._headers(),
                        timeout=15,
                    )
                except Exception as exc:
                    log.warning("Task fetch failed: {e}", e=str(exc))
                    await asyncio.sleep(30)
                    continue

                if resp.status_code == 204 or resp.status_code != 200:
                    consecutive_empty += 1
                    wait = min(5 * (2 ** min(consecutive_empty, 5)), 120)
                    self.state = self.State.IDLE
                    log.debug("No tasks available, waiting {w}s", w=wait)
                    await asyncio.sleep(wait)
                    continue

                consecutive_empty = 0
                task = resp.json()
                task_id = task.get("task_id")
                log.info(
                    "Task {id} received: {t} ({n} char prompt)",
                    id=task_id, t=task.get("task_type"),
                    n=len(task.get("prompt", "")),
                )

                # ── 2. GPU inference ──
                self.state = self.State.INFERENCE

                start = time.monotonic()
                response = self.inference.generate(
                    task.get("prompt", ""),
                    max_tokens=500,
                )
                elapsed = time.monotonic() - start

                if not response or len(response.strip()) < 50:
                    log.warning("Inference produced empty/short response for task {id}", id=task_id)
                    self.tasks_failed += 1
                    continue

                log.info(
                    "Inference complete: {w} words in {t:.1f}s",
                    w=len(response.split()), t=elapsed,
                )

                # ── 3. Submit result ──
                self.state = self.State.SUBMITTING

                try:
                    resp = requests.post(
                        f"{self.grid_url}/api/v1/compute/submit",
                        headers=self._headers(),
                        json={"task_id": task_id, "response": response},
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        score_val = result.get("score", {}).get("total", 0)
                        credits = result.get("rewards", {}).get("api_credits", 0)
                        self.tasks_completed += 1
                        self.total_earnings.api_credits += credits
                        self.total_earnings.tasks_completed = self.tasks_completed
                        self.total_earnings.avg_quality_score = (
                            (self.total_earnings.avg_quality_score * (self.tasks_completed - 1) + score_val)
                            / self.tasks_completed
                        )
                        log.info(
                            "Task {id} submitted — score={s:.3f}, credits={c}, total={n}",
                            id=task_id, s=score_val, c=credits, n=self.tasks_completed,
                        )
                    else:
                        log.warning("Submit failed: {s} {b}", s=resp.status_code, b=resp.text[:200])
                        self.tasks_failed += 1
                except Exception as exc:
                    log.warning("Submit error: {e}", e=str(exc))
                    self.tasks_failed += 1

                self.state = self.State.IDLE
                await asyncio.sleep(2)

            except KeyboardInterrupt:
                log.info("Shutting down edge miner")
                break
            except Exception as exc:
                log.error("Task loop error: {e}", e=str(exc))
                await asyncio.sleep(10)

    def get_dashboard(self) -> dict:
        """Return a dashboard snapshot."""
        uptime_hours = (time.monotonic() - self._start_time) / 3600

        return {
            "state": self.state.value,
            "uptime_hours": round(uptime_hours, 2),
            "gpu": GPUDetector.detect(),
            "tasks": {
                "completed": self.tasks_completed,
                "failed": self.tasks_failed,
                "avg_quality": round(self.total_earnings.avg_quality_score, 3),
            },
            "earnings": {
                "api_credits": self.total_earnings.api_credits,
                "api_value_usd": round(self.total_earnings.api_credits / 1000, 2),
            },
        }


# ═══════════════════════════════════════════════════════════════════════════
# PART 5: BACKGROUND SCHEDULER (server-side)
# ═══════════════════════════════════════════════════════════════════════════


class ComputeScheduler:
    """Server-side background tasks for the compute network.

    Runs on the GRID server to manage:
        - Expire stale assignments
        - Sybil cluster detection
        - Reputation decay
        - Honeypot maintenance
        - Network health reporting
    """

    def __init__(self, engine: Any) -> None:
        self.coordinator = ComputeCoordinator(engine)
        self.engine = engine

    async def run_forever(self, interval: int = 60) -> None:
        """Main scheduler loop."""
        log.info("Compute scheduler starting (interval={i}s)", i=interval)

        cycle = 0
        while True:
            try:
                cycle += 1

                # Every cycle: expire stale assignments
                self.coordinator.expire_stale_assignments()

                # Every 5 cycles: sybil cluster detection
                if cycle % 5 == 0:
                    try:
                        if self.coordinator.sybil_detector:
                            clusters = self.coordinator.sybil_detector.detect_clusters()
                            if clusters:
                                log.warning("Sybil clusters detected: {n}", n=len(clusters))
                                if self.coordinator.reputation:
                                    for cluster in clusters:
                                        for mid in cluster:
                                            self.coordinator.reputation.update_sybil(mid)
                    except Exception as exc:
                        log.debug("Sybil detection failed (non-fatal): {e}", e=str(exc))

                # Every 10 cycles: reputation decay + honeypot maintenance
                if cycle % 10 == 0:
                    try:
                        if self.coordinator.reputation:
                            decayed = self.coordinator.reputation.decay_all()
                            if decayed:
                                log.info("Reputation decay applied to {n} miners", n=decayed)
                    except Exception as exc:
                        log.debug("Reputation decay failed: {e}", e=str(exc))

                    try:
                        if self.coordinator.honeypot:
                            needed = self.coordinator.honeypot.needs_injection()
                            if needed > 0:
                                created = self.coordinator.honeypot.generate_batch(n=min(needed, 20))
                                log.info("Honeypot: injected {n} tasks", n=len(created))
                    except Exception as exc:
                        log.debug("Honeypot maintenance failed: {e}", e=str(exc))

                    try:
                        if self.coordinator.sybil_detector:
                            saved = self.coordinator.sybil_detector.save_profiles()
                            if saved:
                                log.info("Saved {n} behavioral profiles", n=saved)
                    except Exception as exc:
                        log.debug("Profile saving failed: {e}", e=str(exc))

                # Every 30 cycles: health report
                if cycle % 30 == 0:
                    self._log_network_health()

            except KeyboardInterrupt:
                break
            except Exception as exc:
                log.error("Scheduler error: {e}", e=str(exc))

            await asyncio.sleep(interval)

    def _log_network_health(self) -> None:
        """Log network health metrics."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            stats = conn.execute(text("""
                SELECT
                    (SELECT COUNT(*) FROM compute_miners WHERE last_seen > NOW() - INTERVAL '1 hour') as active_miners,
                    (SELECT COUNT(*) FROM compute_miners WHERE is_banned = TRUE) as banned_miners,
                    (SELECT COUNT(*) FROM compute_assignments WHERE status = 'assigned') as pending_assignments,
                    (SELECT AVG(score) FROM compute_assignments WHERE scored_at > NOW() - INTERVAL '1 hour') as avg_score_1h,
                    (SELECT COUNT(*) FROM llm_task_backlog WHERE status = 'pending') as pending_tasks
            """)).fetchone()

        log.info(
            "Network health — miners={m}, banned={b}, pending_tasks={pt}, "
            "in_flight={if_}, avg_score_1h={s}",
            m=stats[0], b=stats[1], pt=stats[4],
            if_=stats[2], s=round(float(stats[3] or 0), 3),
        )


# ═══════════════════════════════════════════════════════════════════════════
# PART 6: EDGE CLIENT CLI
# ═══════════════════════════════════════════════════════════════════════════


def _print_banner() -> None:
    print("""
 ██████╗ ██████╗ ██╗██████╗     ███████╗██████╗  ██████╗ ███████╗
██╔════╝ ██╔══██╗██║██╔══██╗    ██╔════╝██╔══██╗██╔════╝ ██╔════╝
██║  ███╗██████╔╝██║██║  ██║    █████╗  ██║  ██║██║  ███╗█████╗
██║   ██║██╔══██╗██║██║  ██║    ██╔══╝  ██║  ██║██║   ██║██╔══╝
╚██████╔╝██║  ██║██║██████╔╝    ███████╗██████╔╝╚██████╔╝███████╗
 ╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝     ╚══════╝╚═════╝  ╚═════╝ ╚══════╝

  Distributed Compute — Earn GRID API Credits
  Contribute GPU time for intelligence research tasks
""")


def main():
    """Edge client entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="GRID Distributed Compute Edge Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start mining (earn API credits for research tasks)
  python subnet/distributed_compute.py \\
      --grid-url https://grid.stepdad.finance \\
      --api-key YOUR_KEY

  # Detect GPU and exit
  python subnet/distributed_compute.py --detect-gpu
        """,
    )
    parser.add_argument("--grid-url", default="https://grid.stepdad.finance")
    parser.add_argument("--api-key", default=os.getenv("GRID_API_KEY", ""))
    parser.add_argument("--backend", default="llamacpp", choices=["llamacpp", "ollama"])
    parser.add_argument("--detect-gpu", action="store_true", help="Detect GPU and exit")
    parser.add_argument("--scheduler", action="store_true", help="Run server-side scheduler")
    parser.add_argument("--scheduler-interval", type=int, default=60)

    args = parser.parse_args()

    _print_banner()

    if args.detect_gpu:
        gpu = GPUDetector.detect()
        print(f"  GPU Detected:      {gpu['has_gpu']}")
        print(f"  Vendor:            {gpu['vendor']}")
        print(f"  Name:              {gpu['name']}")
        print(f"  VRAM:              {gpu['vram_mb']} MB")
        print(f"  CUDA Available:    {gpu['cuda_available']}")
        print(f"  Recommended Quant: {gpu['recommended_quant']}")
        return

    if args.scheduler:
        from db import get_engine
        engine = get_engine()
        scheduler = ComputeScheduler(engine)
        asyncio.run(scheduler.run_forever(interval=args.scheduler_interval))
        return

    if not args.api_key:
        log.error("--api-key required (or set GRID_API_KEY env var)")
        sys.exit(1)

    gpu = GPUDetector.detect()
    print(f"  GPU:    {gpu['name']} ({gpu['vram_mb']}MB)")
    print(f"  Quant:  {gpu['recommended_quant']}")
    print(f"  Server: {args.grid_url}")
    print()

    miner = EdgeMiner(
        grid_url=args.grid_url,
        api_key=args.api_key,
        backend=args.backend,
    )

    asyncio.run(miner.start())


if __name__ == "__main__":
    main()
