"""
GRID Distributed Compute Engine.

Orchestrates a three-headed reward system for edge compute contributors:

    1. Bittensor/TAO  — LLM research tasks scored by validator, TAO emissions
    2. Monero/XMR     — CPU mining as fallback when GPU is idle between tasks
    3. GRID API       — quality scores earn intelligence API credits

The server side (FastAPI routes) manages task distribution, submission
scoring, cross-validation, and reward accounting.

The edge client (DualMiningManager + EdgeClient) runs on contributor
machines, managing GPU inference and CPU mining simultaneously.

Architecture:
    ┌──────────────┐      GET /task       ┌──────────────┐
    │  GRID Server  │◄────────────────────│  Edge Client  │
    │  (validator)  │────────────────────►│  (miner GPU)  │
    │               │     POST /submit    │  (XMR CPU)    │
    └──────┬───────┘                      └──────┬───────┘
           │                                      │
    ┌──────▼───────┐                      ┌──────▼───────┐
    │  PostgreSQL   │                      │  llama.cpp   │
    │  task backlog │                      │  xmrig       │
    └──────────────┘                      └──────────────┘

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
import shutil
import subprocess
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
    TAO = "tao"
    XMR = "xmr"
    API_CREDITS = "api_credits"


@dataclass
class MinerIdentity:
    """Tracks a registered edge miner."""
    miner_id: str                        # unique ID (hash of hotkey or API key)
    hotkey: str = ""                     # Bittensor hotkey if registered
    api_key: str = ""                    # GRID API key for standalone miners
    reputation: float = 0.5             # 0-1 historical quality score
    total_tasks: int = 0
    total_accepted: int = 0
    tao_earned: float = 0.0
    xmr_earned: float = 0.0
    api_credits: int = 0
    stake_deposited: float = 0.0        # TAO or XMR deposit for anti-gaming
    registered_at: str = ""
    last_seen: str = ""
    is_banned: bool = False


@dataclass
class TaskAssignment:
    """A task assigned to a specific miner, with cross-validation tracking."""
    task_id: int
    miner_id: str
    assigned_at: float = 0.0            # unix timestamp
    deadline: float = 0.0               # must submit before this
    cross_validation_group: str = ""    # group ID for multi-miner verification


@dataclass
class EarningsSnapshot:
    """Current earnings for a miner across all three streams."""
    tao_earned: float = 0.0
    tao_pending: float = 0.0
    xmr_earned: float = 0.0
    xmr_hashrate: float = 0.0          # H/s from CPU mining
    api_credits: int = 0
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

    # Minimum stake required (in TAO equivalent) to participate
    MIN_STAKE = 0.01

    # Task deadline in seconds (2 minutes for a single inference)
    TASK_DEADLINE_SECONDS = 120

    # Quality threshold for accepting a response
    MIN_QUALITY_SCORE = 0.3

    # API credits earned per quality point (score * multiplier)
    API_CREDIT_MULTIPLIER = 100

    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self._ensure_tables()

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
            tao_earned     DOUBLE PRECISION DEFAULT 0,
            xmr_earned     DOUBLE PRECISION DEFAULT 0,
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

        # Derive miner_id from hotkey or API key
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

        return MinerIdentity(miner_id=miner_id, hotkey=hotkey, api_key=api_key)

    # ── Task Distribution ──────────────────────────────────────────────

    def pull_task(self, miner_id: str) -> dict | None:
        """Pull the next available task for a miner.

        Tasks are assigned with cross-validation: the same task goes to
        CROSS_VALIDATION_FACTOR miners. Once all have submitted, responses
        are compared for consistency.

        Returns None if no tasks available or miner is banned/low-rep.
        """
        from sqlalchemy import text

        # Check miner eligibility
        with self.engine.connect() as conn:
            miner = conn.execute(text(
                "SELECT reputation, is_banned, stake_deposited "
                "FROM compute_miners WHERE miner_id = :mid"
            ), {"mid": miner_id}).fetchone()

        if not miner:
            return None
        if miner[1]:  # is_banned
            return None
        if miner[0] < self.MIN_REPUTATION_FOR_TASKS:
            return None

        # Find a task that this miner hasn't been assigned yet, preferring
        # tasks that already have some but not all cross-validation assignments
        with self.engine.begin() as conn:
            # First: try to join an existing cross-validation group
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
                # No existing group to join — pull a fresh task
                fresh = conn.execute(text("""
                    UPDATE llm_task_backlog SET status = 'distributed'
                    WHERE id = (
                        SELECT id FROM llm_task_backlog
                        WHERE status = 'pending'
                          AND id NOT IN (
                              SELECT task_id FROM compute_assignments
                              WHERE miner_id = :mid
                          )
                        ORDER BY priority ASC, RANDOM()
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, task_type, prompt, context
                """), {"mid": miner_id}).fetchone()

                if not fresh:
                    return None

                task_id = fresh[0]
                task_type = fresh[1]
                prompt = fresh[2]
                context = fresh[3]
                cv_group = f"cv_{task_id}_{secrets.token_hex(4)}"

            # Record the assignment
            deadline = datetime.now(timezone.utc).timestamp() + self.TASK_DEADLINE_SECONDS
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

            # Update miner last_seen
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
        """Accept a miner's response, score it, and trigger cross-validation.

        Returns the score breakdown and any rewards earned.
        """
        from sqlalchemy import text

        # Validate the assignment exists and isn't expired
        with self.engine.begin() as conn:
            assignment = conn.execute(text("""
                SELECT id, cross_validation_group, deadline
                FROM compute_assignments
                WHERE task_id = :tid AND miner_id = :mid AND status = 'assigned'
                LIMIT 1
            """), {"tid": task_id, "mid": miner_id}).fetchone()

            if not assignment:
                return {"error": "no_assignment", "detail": "Task not assigned to this miner"}

            assignment_id = assignment[0]
            cv_group = assignment[1]

            # Check deadline (allow 30s grace for network latency)
            # In production, compare against deadline column

            # Score the response using the existing validator scorer
            from subnet.validator import ResponseScorer
            scorer = ResponseScorer(self.engine)

            task_row = conn.execute(text(
                "SELECT task_type, prompt, context FROM llm_task_backlog WHERE id = :tid"
            ), {"tid": task_id}).fetchone()

            if not task_row:
                return {"error": "task_not_found"}

            task = {
                "task_id": task_id,
                "task_type": task_row[0],
                "prompt": task_row[1],
                "context": task_row[2] if isinstance(task_row[2], dict)
                           else json.loads(task_row[2] or "{}"),
            }

            score = scorer.score(task, response)

            # Store the submission
            conn.execute(text("""
                UPDATE compute_assignments
                SET response = :resp, score = :sc, status = 'submitted',
                    submitted_at = NOW(), scored_at = NOW()
                WHERE id = :aid
            """), {"resp": response[:5000], "sc": score["total"], "aid": assignment_id})

            # Update miner stats
            conn.execute(text("""
                UPDATE compute_miners
                SET total_tasks = total_tasks + 1,
                    last_seen = NOW()
                WHERE miner_id = :mid
            """), {"mid": miner_id})

        # Attempt cross-validation if all miners in the group have submitted
        cv_result = self._cross_validate(cv_group)

        # Calculate and distribute rewards
        rewards = self._calculate_rewards(miner_id, task_id, score, cv_result)

        return {
            "score": score,
            "cross_validation": cv_result,
            "rewards": rewards,
        }

    def _cross_validate(self, cv_group: str) -> dict:
        """Compare responses from all miners in a cross-validation group.

        When CROSS_VALIDATION_FACTOR miners have submitted for the same
        task, compare their responses for consistency. Penalize outliers.

        Returns a dict with agreement scores per miner.
        """
        from sqlalchemy import text

        with self.engine.connect() as conn:
            submissions = conn.execute(text("""
                SELECT miner_id, response, score
                FROM compute_assignments
                WHERE cross_validation_group = :cvg AND status = 'submitted'
                ORDER BY score DESC
            """), {"cvg": cv_group}).fetchall()

        if len(submissions) < 2:
            return {"status": "pending", "submissions": len(submissions)}

        # Simple agreement metric: compare response similarity
        # In production, use embedding similarity or LLM-as-judge
        responses = [(row[0], row[1], row[2]) for row in submissions]
        agreements = {}

        for i, (mid_a, resp_a, score_a) in enumerate(responses):
            # Agreement = fraction of key terms shared with other responses
            terms_a = set(resp_a.lower().split()) if resp_a else set()
            agreement_scores = []

            for j, (mid_b, resp_b, score_b) in enumerate(responses):
                if i == j:
                    continue
                terms_b = set(resp_b.lower().split()) if resp_b else set()
                if terms_a or terms_b:
                    jaccard = len(terms_a & terms_b) / max(len(terms_a | terms_b), 1)
                    agreement_scores.append(jaccard)

            avg_agreement = sum(agreement_scores) / max(len(agreement_scores), 1)
            agreements[mid_a] = round(avg_agreement, 3)

        # Flag outliers (agreement < 0.1 when others agree > 0.3)
        avg_all = sum(agreements.values()) / max(len(agreements), 1)
        outliers = [
            mid for mid, agr in agreements.items()
            if agr < 0.1 and avg_all > 0.3
        ]

        if outliers:
            self._penalize_outliers(outliers, cv_group)

        return {
            "status": "complete",
            "submissions": len(submissions),
            "agreements": agreements,
            "outliers": outliers,
        }

    def _penalize_outliers(self, miner_ids: list[str], cv_group: str) -> None:
        """Reduce reputation of miners that submitted garbage."""
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
        """Calculate and record rewards across all three streams.

        Reward formula:
            - API credits = floor(score * API_CREDIT_MULTIPLIER)
            - TAO = proportional to score relative to other miners (set by validator)
            - XMR = tracked separately via mining pool, not calculated here

        Reputation update:
            - Good score (>0.6): reputation += 0.01 (capped at 1.0)
            - Bad score (<0.3): reputation -= 0.03 (floored at 0.0)
            - Outlier in CV: additional -0.05 (applied in _penalize_outliers)
        """
        from sqlalchemy import text

        total_score = score.get("total", 0)
        was_outlier = miner_id in cv_result.get("outliers", [])

        # No rewards for rejected submissions
        if was_outlier or total_score < self.MIN_QUALITY_SCORE:
            return {"api_credits": 0, "tao_pending": 0, "reason": "below_threshold"}

        # API credits: immediate reward
        credits = int(total_score * self.API_CREDIT_MULTIPLIER)

        with self.engine.begin() as conn:
            # Grant API credits
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

            # Record the reward
            conn.execute(text("""
                INSERT INTO compute_rewards (miner_id, reward_type, amount, task_id, reason)
                VALUES (:mid, 'api_credits', :amt, :tid, :reason)
            """), {
                "mid": miner_id,
                "amt": credits,
                "tid": task_id,
                "reason": f"score={total_score:.3f}",
            })

            # TAO rewards are set by the validator when it updates weights
            # on the Bittensor metagraph. We record the pending amount here
            # and the validator reconciles after each epoch.
            tao_pending = total_score  # normalized score, actual TAO set by subnet

            conn.execute(text("""
                INSERT INTO compute_rewards (miner_id, reward_type, amount, task_id, reason)
                VALUES (:mid, 'tao_pending', :amt, :tid, 'validator_weight')
            """), {"mid": miner_id, "amt": tao_pending, "tid": task_id})

        return {
            "api_credits": credits,
            "tao_pending": round(tao_pending, 4),
            "reputation_delta": 0.01 if total_score > 0.6 else -0.03,
        }

    # ── Miner Stats ────────────────────────────────────────────────────

    def get_miner_stats(self, miner_id: str) -> dict:
        """Return full earnings and performance stats for a miner."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            miner = conn.execute(text("""
                SELECT reputation, total_tasks, total_accepted,
                       tao_earned, xmr_earned, api_credits,
                       stake_deposited, registered_at, last_seen
                FROM compute_miners
                WHERE miner_id = :mid
            """), {"mid": miner_id}).fetchone()

            if not miner:
                return {"error": "miner_not_found"}

            # Recent reward history
            rewards = conn.execute(text("""
                SELECT reward_type, SUM(amount) as total, COUNT(*) as count
                FROM compute_rewards
                WHERE miner_id = :mid
                GROUP BY reward_type
            """), {"mid": miner_id}).fetchall()

            # Recent task performance
            recent = conn.execute(text("""
                SELECT AVG(score), COUNT(*), MAX(submitted_at)
                FROM compute_assignments
                WHERE miner_id = :mid AND status IN ('submitted', 'validated')
                  AND submitted_at > NOW() - INTERVAL '7 days'
            """), {"mid": miner_id}).fetchone()

        reward_summary = {}
        for row in rewards:
            reward_summary[row[0]] = {"total": float(row[1]), "count": int(row[2])}

        return {
            "miner_id": miner_id,
            "reputation": float(miner[0]),
            "total_tasks": int(miner[1]),
            "total_accepted": int(miner[2]),
            "acceptance_rate": round(miner[2] / max(miner[1], 1), 3),
            "earnings": {
                "tao": float(miner[3]),
                "xmr": float(miner[4]),
                "api_credits": int(miner[5]),
            },
            "stake": float(miner[6]),
            "rewards_breakdown": reward_summary,
            "recent_7d": {
                "avg_score": round(float(recent[0] or 0), 3),
                "tasks": int(recent[1] or 0),
                "last_submission": str(recent[2] or ""),
            },
            "registered_at": str(miner[7]),
            "last_seen": str(miner[8]),
        }

    # ── Leaderboard ────────────────────────────────────────────────────

    def get_leaderboard(self, limit: int = 25) -> list[dict]:
        """Top miners by reputation and task volume."""
        from sqlalchemy import text

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT miner_id, reputation, total_tasks, total_accepted,
                       tao_earned, xmr_earned, api_credits
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
                "tao_earned": float(row[4]),
                "xmr_earned": float(row[5]),
                "api_credits": int(row[6]),
            }
            for i, row in enumerate(rows)
        ]

    # ── Stake Management ───────────────────────────────────────────────

    def record_stake(self, miner_id: str, amount: float, currency: str = "tao") -> dict:
        """Record a stake deposit from a miner.

        In production, verify the on-chain transaction before crediting.
        """
        from sqlalchemy import text

        # TODO: Verify on-chain transaction
        # For TAO: check bittensor substrate for transfer to validator coldkey
        # For XMR: check monero-wallet-rpc for incoming transfer

        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE compute_miners
                SET stake_deposited = stake_deposited + :amt
                WHERE miner_id = :mid
            """), {"amt": amount, "mid": miner_id})

        return {"miner_id": miner_id, "stake_added": amount, "currency": currency}

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

            # Penalize miners who let assignments expire
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

    # ── Request/Response Models ────────────────────────────────────────

    class RegisterRequest(BaseModel):
        hotkey: str = ""
        api_key: str = ""

    class SubmitRequest(BaseModel):
        task_id: int
        response: str

    class StakeRequest(BaseModel):
        amount: float
        currency: str = "tao"
        tx_hash: str = ""  # on-chain transaction hash for verification

    # ── Dependency: get coordinator ────────────────────────────────────

    def _get_coordinator() -> ComputeCoordinator:
        from db import get_engine
        return ComputeCoordinator(get_engine())

    def _extract_miner_id(authorization: str = Header(default="")) -> str:
        """Extract miner_id from the Authorization header.

        Accepts either:
            - Bearer <api_key>  (standalone miners)
            - Hotkey <bittensor_hotkey>  (Bittensor miners)
        """
        if not authorization:
            raise HTTPException(401, "Missing Authorization header")

        parts = authorization.split(" ", 1)
        if len(parts) != 2:
            raise HTTPException(401, "Invalid Authorization format")

        auth_type, credential = parts
        miner_id = hashlib.sha256(credential.encode()).hexdigest()[:16]
        return miner_id

    # ── POST /register ─────────────────────────────────────────────────

    @compute_router.post("/register")
    def register_miner(body: RegisterRequest, coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Register as a compute contributor.

        Returns a miner_id to use in subsequent requests.
        Accepts either a Bittensor hotkey or a GRID API key.
        """
        identity = coord.register_miner(hotkey=body.hotkey, api_key=body.api_key)
        return {
            "miner_id": identity.miner_id,
            "reputation": identity.reputation,
            "status": "registered",
        }

    # ── GET /task ──────────────────────────────────────────────────────

    @compute_router.get("/task")
    def pull_task(miner_id: str = Depends(_extract_miner_id),
                  coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Pull the next research task to process.

        Returns a task with prompt, context, and deadline. The same task
        may be sent to multiple miners for cross-validation.

        Returns 204 if no tasks are available.
        """
        task = coord.pull_task(miner_id)
        if not task:
            raise HTTPException(204, "No tasks available")
        return task

    # ── POST /submit ───────────────────────────────────────────────────

    @compute_router.post("/submit")
    def submit_result(body: SubmitRequest,
                      miner_id: str = Depends(_extract_miner_id),
                      coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Submit a completed research response.

        The response is scored immediately. Cross-validation triggers
        when all miners in the group have submitted. Rewards are
        calculated and credited to the miner's account.
        """
        if not body.response or len(body.response.strip()) < 50:
            raise HTTPException(400, "Response too short (minimum 50 characters)")

        result = coord.submit_result(miner_id, body.task_id, body.response)
        if "error" in result:
            raise HTTPException(400, result["error"])
        return result

    # ── GET /stats ─────────────────────────────────────────────────────

    @compute_router.get("/stats")
    def miner_stats(miner_id: str = Depends(_extract_miner_id),
                    coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Get earnings and performance stats for the authenticated miner.

        Returns TAO earned, XMR mined, API credits, reputation, task
        history, and recent 7-day performance.
        """
        stats = coord.get_miner_stats(miner_id)
        if "error" in stats:
            raise HTTPException(404, stats["error"])
        return stats

    # ── GET /leaderboard ───────────────────────────────────────────────

    @compute_router.get("/leaderboard")
    def leaderboard(limit: int = 25,
                    coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Top miners by reputation and contribution volume."""
        return coord.get_leaderboard(limit=min(limit, 100))

    # ── POST /stake ────────────────────────────────────────────────────

    @compute_router.post("/stake")
    def record_stake(body: StakeRequest,
                     miner_id: str = Depends(_extract_miner_id),
                     coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Record a stake deposit (TAO or XMR).

        Staking is required to participate. The minimum stake prevents
        sybil attacks (spinning up 100 accounts to spam garbage).
        """
        if body.amount <= 0:
            raise HTTPException(400, "Stake amount must be positive")
        # TODO: Verify on-chain tx_hash before crediting
        return coord.record_stake(miner_id, body.amount, body.currency)

    # ── Admin: GET /admin/expire ───────────────────────────────────────

    @compute_router.post("/admin/expire-stale")
    def expire_stale(coord: ComputeCoordinator = Depends(_get_coordinator)):
        """Admin endpoint: expire overdue assignments and penalize miners."""
        # TODO: Add admin auth check
        count = coord.expire_stale_assignments()
        return {"expired": count}

except ImportError:
    # FastAPI not available (edge client doesn't need it)
    compute_router = None  # type: ignore
    log.debug("FastAPI not available — server routes disabled")


# ═══════════════════════════════════════════════════════════════════════════
# PART 4: EDGE CLIENT — DUAL MINING MANAGER
# ═══════════════════════════════════════════════════════════════════════════


class GPUDetector:
    """Detect available GPU hardware on the edge node."""

    @staticmethod
    def detect() -> dict:
        """Return GPU info or indicate CPU-only mode."""
        gpu_info = {
            "has_gpu": False,
            "vendor": "none",
            "name": "CPU only",
            "vram_mb": 0,
            "cuda_available": False,
            "recommended_quant": "Q4_K_M",
        }

        # Check NVIDIA
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split(",")
                gpu_info["has_gpu"] = True
                gpu_info["vendor"] = "nvidia"
                gpu_info["name"] = parts[0].strip()
                gpu_info["vram_mb"] = int(parts[1].strip()) if len(parts) > 1 else 0
                gpu_info["cuda_available"] = True

                # Recommend quantization based on VRAM
                vram = gpu_info["vram_mb"]
                if vram >= 16000:
                    gpu_info["recommended_quant"] = "Q8_0"     # 16GB+ -> Q8
                elif vram >= 8000:
                    gpu_info["recommended_quant"] = "Q4_K_M"   # 8GB -> Q4
                elif vram >= 6000:
                    gpu_info["recommended_quant"] = "Q3_K_M"   # 6GB -> Q3
                else:
                    gpu_info["recommended_quant"] = "Q2_K"     # 4GB -> Q2

                return gpu_info
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Check AMD ROCm
        try:
            result = subprocess.run(
                ["rocm-smi", "--showproductname", "--showmeminfo", "vram"],
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

        # Check Apple Silicon (macOS)
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            gpu_info["has_gpu"] = True
            gpu_info["vendor"] = "apple"
            gpu_info["name"] = "Apple Silicon (Metal)"
            gpu_info["recommended_quant"] = "Q4_K_M"

        return gpu_info


class XMRMiner:
    """Manages XMR CPU mining as a secondary revenue stream.

    Runs xmrig in the background at reduced priority so it doesn't
    compete with LLM inference for GPU resources. CPU-only mining
    fills the gaps between LLM tasks.
    """

    def __init__(
        self,
        pool_url: str = "pool.hashvault.pro:443",
        wallet_address: str = "",
        worker_name: str = "grid_edge",
        threads: int = 0,  # 0 = auto-detect (half of available cores)
    ) -> None:
        self.pool_url = pool_url
        self.wallet_address = wallet_address
        self.worker_name = worker_name
        self.threads = threads or max(1, (os.cpu_count() or 2) // 2)
        self._process: subprocess.Popen | None = None
        self._hashrate: float = 0.0
        self._shares_accepted: int = 0
        self._running = False

    def start(self) -> bool:
        """Start xmrig CPU mining in the background."""
        if not self.wallet_address:
            log.warning("XMR mining disabled — no wallet address configured")
            return False

        xmrig_path = shutil.which("xmrig")
        if not xmrig_path:
            log.warning("xmrig not found in PATH — install from github.com/xmrig/xmrig")
            return False

        cmd = [
            xmrig_path,
            "--url", self.pool_url,
            "--user", self.wallet_address,
            "--rig-id", self.worker_name,
            "--threads", str(self.threads),
            "--no-color",
            "--background",         # daemonize
            "--cpu-priority", "1",  # low priority (1=idle, 5=highest)
            "--donate-level", "0",
            "--tls",
        ]

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._running = True
            log.info(
                "XMR mining started — pool={p}, threads={t}",
                p=self.pool_url, t=self.threads,
            )
            return True
        except Exception as exc:
            log.error("Failed to start xmrig: {e}", e=str(exc))
            return False

    def stop(self) -> None:
        """Stop xmrig mining."""
        if self._process:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
            self._process = None
            self._running = False
            log.info("XMR mining stopped")

    def throttle(self, threads: int) -> None:
        """Adjust mining intensity (fewer threads when GPU is busy).

        When an LLM task arrives, reduce CPU threads to avoid thermal
        throttling. When GPU is idle, ramp back up.
        """
        if not self._running:
            return

        self.threads = max(1, threads)
        # xmrig supports live config reload via its HTTP API
        # TODO: Send PUT to xmrig's HTTP API to update thread count
        # Default: http://127.0.0.1:39746/1/config
        log.debug("XMR mining throttled to {t} threads", t=self.threads)

    def get_stats(self) -> dict:
        """Return current mining stats.

        In production, query xmrig's HTTP API at http://127.0.0.1:39746/2/summary
        """
        # TODO: Query xmrig HTTP API for live stats
        return {
            "running": self._running,
            "hashrate_h_s": self._hashrate,
            "threads": self.threads,
            "shares_accepted": self._shares_accepted,
            "pool": self.pool_url,
        }

    @property
    def is_running(self) -> bool:
        return self._running and self._process is not None and self._process.poll() is None


class DualMiningManager:
    """Orchestrates GPU (LLM tasks) and CPU (XMR) mining simultaneously.

    State machine:
        IDLE -> FETCHING_TASK -> GPU_INFERENCE -> SUBMITTING -> IDLE
                                    |
                          (CPU mines XMR throughout)

    When GPU is running an LLM task:
        - CPU mines XMR at reduced thread count (thermal headroom)
    When GPU is idle (waiting for next task):
        - CPU mines XMR at full thread count
    When no tasks available:
        - CPU mines XMR at full thread count
        - GPU is idle (or could mine XMR via RandomX if no LLM work)
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
        xmr_wallet: str = "",
        xmr_pool: str = "pool.hashvault.pro:443",
        backend: str = "llamacpp",
        xmr_threads: int = 0,
    ) -> None:
        self.grid_url = grid_url.rstrip("/")
        self.api_key = api_key
        self.backend = backend
        self.state = self.State.IDLE

        # GPU inference (LLM tasks)
        from subnet.miner import LocalInference
        self.inference = LocalInference(backend=backend)

        # CPU mining (XMR)
        self.xmr = XMRMiner(
            pool_url=xmr_pool,
            wallet_address=xmr_wallet,
            threads=xmr_threads,
        )
        self._full_threads = xmr_threads or max(1, (os.cpu_count() or 2) // 2)
        self._reduced_threads = max(1, self._full_threads // 2)

        # Stats
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.total_earnings = EarningsSnapshot()
        self._start_time = time.monotonic()

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}"}

    async def start(self) -> None:
        """Start dual mining: GPU for LLM tasks + CPU for XMR."""
        gpu = GPUDetector.detect()
        log.info("GPU detected: {name} ({vram}MB)", name=gpu["name"], vram=gpu["vram_mb"])

        # Start XMR CPU mining in background
        self.xmr.start()

        # Register with GRID coordinator
        await self._register()

        # Main loop
        await self._mining_loop()

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

    async def _mining_loop(self) -> None:
        """Main loop: pull task -> inference -> submit -> repeat."""
        import requests

        consecutive_empty = 0

        while True:
            try:
                # ── 1. Fetch task ──
                self.state = self.State.FETCHING
                self.xmr.throttle(self._full_threads)  # full CPU while waiting

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
                    # Exponential backoff: 5s, 10s, 20s, ... max 120s
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
                self.xmr.throttle(self._reduced_threads)  # reduce CPU during inference

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
                self.xmr.throttle(self._full_threads)  # full CPU while submitting

                try:
                    resp = requests.post(
                        f"{self.grid_url}/api/v1/compute/submit",
                        headers=self._headers(),
                        json={"task_id": task_id, "response": response},
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        score = result.get("score", {}).get("total", 0)
                        credits = result.get("rewards", {}).get("api_credits", 0)
                        self.tasks_completed += 1
                        self.total_earnings.api_credits += credits
                        self.total_earnings.tasks_completed = self.tasks_completed
                        self.total_earnings.avg_quality_score = (
                            (self.total_earnings.avg_quality_score * (self.tasks_completed - 1) + score)
                            / self.tasks_completed
                        )
                        log.info(
                            "Task {id} submitted — score={s:.3f}, credits={c}, total_tasks={n}",
                            id=task_id, s=score, c=credits, n=self.tasks_completed,
                        )
                    else:
                        log.warning("Submit failed: {s} {b}", s=resp.status_code, b=resp.text[:200])
                        self.tasks_failed += 1
                except Exception as exc:
                    log.warning("Submit error: {e}", e=str(exc))
                    self.tasks_failed += 1

                self.state = self.State.IDLE

                # Brief cooldown between tasks to avoid hammering the API
                await asyncio.sleep(2)

            except KeyboardInterrupt:
                log.info("Shutting down dual mining manager")
                self.xmr.stop()
                break
            except Exception as exc:
                log.error("Mining loop error: {e}", e=str(exc))
                await asyncio.sleep(10)

    def get_dashboard(self) -> dict:
        """Return a dashboard snapshot for the edge client UI."""
        uptime_hours = (time.monotonic() - self._start_time) / 3600
        xmr_stats = self.xmr.get_stats()

        return {
            "state": self.state.value,
            "uptime_hours": round(uptime_hours, 2),
            "gpu": GPUDetector.detect(),
            "llm_tasks": {
                "completed": self.tasks_completed,
                "failed": self.tasks_failed,
                "avg_quality": round(self.total_earnings.avg_quality_score, 3),
            },
            "earnings": {
                "tao_earned": self.total_earnings.tao_earned,
                "tao_pending": self.total_earnings.tao_pending,
                "xmr_hashrate_h_s": xmr_stats.get("hashrate_h_s", 0),
                "xmr_shares": xmr_stats.get("shares_accepted", 0),
                "api_credits": self.total_earnings.api_credits,
            },
            "xmr_mining": xmr_stats,
        }


# ═══════════════════════════════════════════════════════════════════════════
# PART 5: BITTENSOR SUBNET INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════


class BittensorSubnetBridge:
    """Bridge between GRID's compute coordinator and the Bittensor network.

    This class handles:
        - Registering the GRID subnet on Bittensor
        - Setting miner weights based on quality scores
        - Converting quality scores to TAO emissions
        - Syncing miner hotkeys between Bittensor and GRID

    All Bittensor-specific calls are marked with TODO since they
    require the bittensor SDK and a registered subnet.
    """

    def __init__(self, netuid: int = 1, network: str = "finney") -> None:
        self.netuid = netuid
        self.network = network
        self._subtensor = None
        self._wallet = None
        self._metagraph = None

    def initialize(self) -> None:
        """Connect to Bittensor network and load metagraph."""
        # TODO: Initialize Bittensor connection
        # import bittensor as bt
        # self._subtensor = bt.subtensor(network=self.network)
        # self._wallet = bt.wallet(name="grid_validator")
        # self._metagraph = self._subtensor.metagraph(netuid=self.netuid)
        log.info("Bittensor bridge initialized (netuid={n})", n=self.netuid)

    def sync_metagraph(self) -> dict:
        """Sync the metagraph to get current miners and their stakes."""
        # TODO: Sync from chain
        # self._metagraph.sync(subtensor=self._subtensor)
        # return {
        #     "n_miners": self._metagraph.n,
        #     "total_stake": float(self._metagraph.total_stake),
        #     "block": self._metagraph.block.item(),
        # }
        return {"n_miners": 0, "total_stake": 0, "block": 0}

    def set_weights(self, scores: dict[str, float]) -> bool:
        """Set miner weights on the Bittensor network.

        Args:
            scores: mapping of hotkey -> normalized score (0-1)

        The subnet's emission schedule distributes TAO proportional
        to these weights. Higher-quality miners earn more TAO.
        """
        if not scores:
            return False

        # TODO: Set weights on chain
        # import torch
        # uids = []
        # weights = []
        # for hotkey, score in scores.items():
        #     uid = self._metagraph.hotkeys.index(hotkey)
        #     uids.append(uid)
        #     weights.append(score)
        #
        # uids_tensor = torch.tensor(uids, dtype=torch.int64)
        # weights_tensor = torch.tensor(weights, dtype=torch.float32)
        # weights_tensor = weights_tensor / weights_tensor.sum()  # normalize
        #
        # success, msg = self._subtensor.set_weights(
        #     wallet=self._wallet,
        #     netuid=self.netuid,
        #     uids=uids_tensor,
        #     weights=weights_tensor,
        #     wait_for_inclusion=True,
        # )
        # return success

        log.info("Would set weights for {n} miners", n=len(scores))
        return True

    def get_miner_emissions(self) -> dict[str, float]:
        """Get current TAO emissions per miner from the chain."""
        # TODO: Read from metagraph
        # emissions = {}
        # for uid in range(self._metagraph.n):
        #     hotkey = self._metagraph.hotkeys[uid]
        #     emission = float(self._metagraph.emission[uid])
        #     emissions[hotkey] = emission
        # return emissions
        return {}

    def register_subnet(self) -> dict:
        """Register a new subnet on Bittensor (one-time operation).

        Cost: ~1 TAO for subnet registration on finney.
        """
        # TODO: Register subnet
        # success, msg = self._subtensor.register_subnetwork(
        #     wallet=self._wallet,
        #     wait_for_inclusion=True,
        # )
        # return {"success": success, "message": msg, "netuid": self.netuid}
        return {"success": False, "message": "Not implemented", "netuid": self.netuid}


# ═══════════════════════════════════════════════════════════════════════════
# PART 6: BACKGROUND TASKS (server-side scheduler)
# ═══════════════════════════════════════════════════════════════════════════


class ComputeScheduler:
    """Server-side background tasks for the compute network.

    Runs on the GRID server to manage ongoing operations:
        - Expire stale assignments
        - Reconcile Bittensor weights with quality scores
        - Sync XMR pool earnings for miners
        - Generate network health reports
    """

    def __init__(self, engine: Any, bt_bridge: BittensorSubnetBridge | None = None) -> None:
        self.coordinator = ComputeCoordinator(engine)
        self.bt_bridge = bt_bridge
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

                # Every 5 cycles: update Bittensor weights
                if cycle % 5 == 0 and self.bt_bridge:
                    await self._update_bittensor_weights()

                # Every 10 cycles: sync XMR pool earnings
                if cycle % 10 == 0:
                    await self._sync_xmr_earnings()

                # Every 30 cycles: health report
                if cycle % 30 == 0:
                    self._log_network_health()

            except KeyboardInterrupt:
                break
            except Exception as exc:
                log.error("Scheduler error: {e}", e=str(exc))

            await asyncio.sleep(interval)

    async def _update_bittensor_weights(self) -> None:
        """Aggregate quality scores and set Bittensor weights."""
        if not self.bt_bridge:
            return

        from sqlalchemy import text

        with self.engine.connect() as conn:
            # Get average scores per miner over the last epoch
            rows = conn.execute(text("""
                SELECT cm.hotkey, AVG(ca.score) as avg_score
                FROM compute_assignments ca
                JOIN compute_miners cm ON cm.miner_id = ca.miner_id
                WHERE ca.status = 'submitted'
                  AND ca.scored_at > NOW() - INTERVAL '1 hour'
                  AND cm.hotkey != ''
                GROUP BY cm.hotkey
                HAVING COUNT(*) >= 3
            """)).fetchall()

        if not rows:
            return

        scores = {row[0]: float(row[1]) for row in rows}
        self.bt_bridge.set_weights(scores)
        log.info("Updated Bittensor weights for {n} miners", n=len(scores))

    async def _sync_xmr_earnings(self) -> None:
        """Query XMR mining pool API to update miner earnings.

        Each miner reports their XMR wallet address. We query the pool
        to check their credited shares/payments.
        """
        # TODO: Query mining pool API (e.g., HashVault)
        # import requests
        # from sqlalchemy import text
        #
        # with self.engine.connect() as conn:
        #     miners = conn.execute(text(
        #         "SELECT miner_id FROM compute_miners WHERE xmr_wallet != ''"
        #     )).fetchall()
        #
        # for row in miners:
        #     # GET https://pool.hashvault.pro/api/miner/{wallet}/stats
        #     pass
        pass

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
# PART 7: EDGE CLIENT CLI
# ═══════════════════════════════════════════════════════════════════════════


def _print_banner() -> None:
    print("""
 ██████╗ ██████╗ ██╗██████╗     ███████╗██████╗  ██████╗ ███████╗
██╔════╝ ██╔══██╗██║██╔══██╗    ██╔════╝██╔══██╗██╔════╝ ██╔════╝
██║  ███╗██████╔╝██║██║  ██║    █████╗  ██║  ██║██║  ███╗█████╗
██║   ██║██╔══██╗██║██║  ██║    ██╔══╝  ██║  ██║██║   ██║██╔══╝
╚██████╔╝██║  ██║██║██████╔╝    ███████╗██████╔╝╚██████╔╝███████╗
 ╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝     ╚══════╝╚═════╝  ╚═════╝ ╚══════╝

  Distributed Compute Client — Earn TAO + XMR + API Credits
  github.com/stepdadfinance/grid
""")


def main():
    """Edge client entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="GRID Distributed Compute Edge Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full dual mining (LLM tasks + XMR)
  python subnet/distributed_compute.py \\
      --grid-url https://grid.stepdad.finance \\
      --api-key YOUR_KEY \\
      --xmr-wallet YOUR_XMR_ADDRESS

  # LLM tasks only (no XMR mining)
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
    parser.add_argument("--xmr-wallet", default=os.getenv("XMR_WALLET", ""))
    parser.add_argument("--xmr-pool", default="pool.hashvault.pro:443")
    parser.add_argument("--xmr-threads", type=int, default=0, help="CPU threads for XMR (0=auto)")
    parser.add_argument("--detect-gpu", action="store_true", help="Detect GPU and exit")

    # Server-side scheduler mode
    parser.add_argument("--scheduler", action="store_true", help="Run server-side scheduler")
    parser.add_argument("--scheduler-interval", type=int, default=60)

    args = parser.parse_args()

    _print_banner()

    # GPU detection mode
    if args.detect_gpu:
        gpu = GPUDetector.detect()
        print(f"  GPU Detected:     {gpu['has_gpu']}")
        print(f"  Vendor:           {gpu['vendor']}")
        print(f"  Name:             {gpu['name']}")
        print(f"  VRAM:             {gpu['vram_mb']} MB")
        print(f"  CUDA Available:   {gpu['cuda_available']}")
        print(f"  Recommended Quant: {gpu['recommended_quant']}")
        return

    # Server-side scheduler mode
    if args.scheduler:
        from db import get_engine
        engine = get_engine()
        bt_bridge = BittensorSubnetBridge()
        scheduler = ComputeScheduler(engine, bt_bridge)
        asyncio.run(scheduler.run_forever(interval=args.scheduler_interval))
        return

    # Edge client mode
    if not args.api_key:
        log.error("--api-key required (or set GRID_API_KEY env var)")
        sys.exit(1)

    gpu = GPUDetector.detect()
    print(f"  GPU:    {gpu['name']} ({gpu['vram_mb']}MB)")
    print(f"  Quant:  {gpu['recommended_quant']}")
    print(f"  XMR:    {'enabled' if args.xmr_wallet else 'disabled'}")
    print(f"  Server: {args.grid_url}")
    print()

    manager = DualMiningManager(
        grid_url=args.grid_url,
        api_key=args.api_key,
        xmr_wallet=args.xmr_wallet,
        xmr_pool=args.xmr_pool,
        backend=args.backend,
        xmr_threads=args.xmr_threads,
    )

    asyncio.run(manager.start())


if __name__ == "__main__":
    main()
