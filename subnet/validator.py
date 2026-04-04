"""
GRID Bittensor Subnet Validator.

Distributes financial research tasks to miners, scores their responses
against confirmed data, and stores results in the encrypted intel store.

The validator is the only component that has access to the full GRID
database — miners never see the answer key.

Usage:
    python subnet/validator.py --netuid <SUBNET_ID> --wallet.name grid
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

# Ensure grid root is on path
_GRID_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)


# ── Task Distribution ────────────────────────────────────────────────────

class TaskDistributor:
    """Pulls tasks from llm_task_backlog and formats them for miners."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def get_batch(self, batch_size: int = 10) -> list[dict]:
        """Pull a batch of pending tasks for distribution to miners."""
        from sqlalchemy import text

        tasks = []
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(text(
                    "UPDATE llm_task_backlog SET status = 'distributed' "
                    "WHERE id IN ("
                    "  SELECT id FROM llm_task_backlog "
                    "  WHERE status = 'pending' "
                    "  ORDER BY priority ASC, RANDOM() "
                    "  LIMIT :lim "
                    "  FOR UPDATE SKIP LOCKED"
                    ") RETURNING id, task_type, prompt, context"
                ), {"lim": batch_size}).fetchall()

                for row in rows:
                    tasks.append({
                        "task_id": row[0],
                        "task_type": row[1],
                        "prompt": row[2],
                        "context": row[3] if isinstance(row[3], dict) else json.loads(row[3] or "{}"),
                    })
        except Exception as exc:
            log.warning("Task distribution failed: {e}", e=str(exc))

        return tasks


# ── Response Scoring ─────────────────────────────────────────────────────

class ResponseScorer:
    """Scores miner responses against GRID's confirmed data.

    The scoring algorithm is the validator's secret sauce — miners
    can't game what they can't see.

    Scoring dimensions:
        1. Specificity (0-1): Does the response contain names, numbers, dates?
        2. Structure (0-1): Does it follow confidence labeling conventions?
        3. Accuracy (0-1): Cross-referenced against known facts in our DB
        4. Novelty (0-1): New information not already in our database
        5. Consistency (0-1): Doesn't contradict confirmed facts

    Final score = weighted average, range 0-1.
    """

    # Weights for each scoring dimension
    WEIGHTS = {
        "specificity": 0.25,
        "structure": 0.15,
        "accuracy": 0.30,
        "novelty": 0.15,
        "consistency": 0.15,
    }

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def score(self, task: dict, response: str) -> dict:
        """Score a miner's response. Returns score breakdown."""
        if not response or len(response.strip()) < 50:
            return {"total": 0.0, "reason": "response too short"}

        specificity = self._score_specificity(response)
        structure = self._score_structure(response)
        accuracy = self._score_accuracy(task, response)
        novelty = self._score_novelty(task, response)
        consistency = self._score_consistency(task, response)

        total = (
            specificity * self.WEIGHTS["specificity"]
            + structure * self.WEIGHTS["structure"]
            + accuracy * self.WEIGHTS["accuracy"]
            + novelty * self.WEIGHTS["novelty"]
            + consistency * self.WEIGHTS["consistency"]
        )

        return {
            "total": round(total, 4),
            "specificity": round(specificity, 3),
            "structure": round(structure, 3),
            "accuracy": round(accuracy, 3),
            "novelty": round(novelty, 3),
            "consistency": round(consistency, 3),
        }

    def _score_specificity(self, response: str) -> float:
        """Higher score for responses with specific names, numbers, dates."""
        import re
        score = 0.3  # base

        # Count specific data points
        numbers = len(re.findall(r'\$[\d,.]+[BMKTbmkt]?|\d+%|\d{4}-\d{2}-\d{2}', response))
        names = len(re.findall(r'[A-Z][a-z]+ [A-Z][a-z]+', response))
        tickers = len(re.findall(r'\b[A-Z]{2,5}\b', response))

        score += min(0.3, numbers * 0.03)  # up to 0.3 for 10+ numbers
        score += min(0.2, names * 0.02)    # up to 0.2 for 10+ names
        score += min(0.2, tickers * 0.02)  # up to 0.2 for 10+ tickers

        return min(1.0, score)

    def _score_structure(self, response: str) -> float:
        """Higher score for responses using GRID confidence labels."""
        score = 0.2
        labels = ["confirmed", "derived", "estimated", "rumored", "inferred"]
        for label in labels:
            if label in response.lower():
                score += 0.16
        return min(1.0, score)

    def _score_accuracy(self, task: dict, response: str) -> float:
        """Cross-reference claims against known facts in our DB."""
        from sqlalchemy import text

        score = 0.5  # neutral default

        # Check if response mentions actors we know about
        try:
            with self.engine.connect() as conn:
                # Extract potential actor names from response
                import re
                potential_names = re.findall(r'[A-Z][a-z]+ [A-Z][a-z]+', response)[:10]

                for name in potential_names:
                    row = conn.execute(text(
                        "SELECT COUNT(*) FROM actors WHERE LOWER(name) LIKE LOWER(:n)"
                    ), {"n": f"%{name}%"}).fetchone()
                    if row and row[0] > 0:
                        score += 0.05  # bonus for mentioning known actors
        except Exception:
            pass

        return min(1.0, score)

    def _score_novelty(self, task: dict, response: str) -> float:
        """Higher score for information not already in our database."""
        # Placeholder — full implementation would check encrypted_intelligence
        return 0.5

    def _score_consistency(self, task: dict, response: str) -> float:
        """Penalize responses that contradict confirmed facts."""
        # Placeholder — full implementation would fact-check against DB
        return 0.7


# ── Result Storage ───────────────────────────────────────────────────────

class ResultStore:
    """Stores validated miner responses in the encrypted intel store."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def store_result(self, task: dict, response: str, score: dict, miner_uid: int) -> None:
        """Store a scored miner response."""
        from sqlalchemy import text

        # Only store if quality is sufficient
        if score["total"] < 0.3:
            return

        # Determine confidence from score
        if score["total"] >= 0.8:
            confidence = "derived"
        elif score["total"] >= 0.6:
            confidence = "estimated"
        elif score["total"] >= 0.4:
            confidence = "rumored"
        else:
            confidence = "inferred"

        try:
            # Store in encrypted intelligence
            from intelligence.opsec import EncryptedIntelStore
            store = EncryptedIntelStore(self.engine)
            store.store(
                category=f"subnet_{task.get('task_type', 'unknown')}",
                subject=task.get("prompt", "")[:100],
                data={
                    "task_id": task.get("task_id"),
                    "task_type": task.get("task_type"),
                    "response": response[:2000],
                    "score": score,
                    "miner_uid": miner_uid,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                confidence=confidence,
                min_tier="analyst",
                tags=[task.get("task_type", ""), f"miner_{miner_uid}"],
            )

            # Mark task as done in backlog
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE llm_task_backlog SET status = 'done' WHERE id = :id"
                ), {"id": task.get("task_id")})

        except Exception as exc:
            log.debug("Result storage failed: {e}", e=str(exc))

    def record_miner_score(self, miner_uid: int, score: float) -> None:
        """Track cumulative miner performance for API credit rewards."""
        from sqlalchemy import text
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO subnet_miner_scores (miner_uid, total_tasks, avg_score, last_active)
                    VALUES (:uid, 1, :score, NOW())
                    ON CONFLICT (miner_uid) DO UPDATE SET
                        total_tasks = subnet_miner_scores.total_tasks + 1,
                        avg_score = (subnet_miner_scores.avg_score * subnet_miner_scores.total_tasks + :score)
                                    / (subnet_miner_scores.total_tasks + 1),
                        last_active = NOW()
                """), {"uid": miner_uid, "score": score})
        except Exception:
            pass


# ── Validator Core ───────────────────────────────────────────────────────

class GRIDValidator:
    """Main validator loop for the GRID Bittensor subnet.

    Lifecycle:
        1. Pull tasks from llm_task_backlog
        2. Send to miners via Bittensor protocol
        3. Receive responses
        4. Score responses against our confirmed data
        5. Set weights on the network (reward good miners)
        6. Store quality responses in encrypted intel
        7. Repeat
    """

    def __init__(self, engine: Any, netuid: int = 1) -> None:
        self.engine = engine
        self.netuid = netuid
        self.distributor = TaskDistributor(engine)
        self.scorer = ResponseScorer(engine)
        self.result_store = ResultStore(engine)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create subnet-specific tables."""
        from sqlalchemy import text
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS subnet_miner_scores (
                        miner_uid INTEGER PRIMARY KEY,
                        total_tasks BIGINT DEFAULT 0,
                        avg_score DOUBLE PRECISION DEFAULT 0,
                        api_credits BIGINT DEFAULT 0,
                        last_active TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS subnet_task_log (
                        id BIGSERIAL PRIMARY KEY,
                        task_id BIGINT,
                        miner_uid INTEGER,
                        score DOUBLE PRECISION,
                        response_len INTEGER,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
        except Exception:
            pass

    async def validation_step(self) -> dict:
        """One validation cycle: distribute → collect → score → reward."""
        # 1. Get tasks
        tasks = self.distributor.get_batch(batch_size=10)
        if not tasks:
            log.info("No pending tasks in backlog")
            return {"tasks": 0}

        log.info("Distributing {n} tasks to miners", n=len(tasks))

        # 2. Send to miners and collect responses
        # In production, this uses bittensor's dendrite to query miners
        # For now, simulate with local inference
        results = []
        for task in tasks:
            # TODO: Replace with actual bittensor miner query
            # response = await self.dendrite.query(miners, task["prompt"])
            response = self._local_inference(task)
            results.append((task, response, 0))  # (task, response, miner_uid)

        # 3. Score responses
        scores = []
        for task, response, miner_uid in results:
            score = self.scorer.score(task, response)
            scores.append(score["total"])

            # 4. Store quality results
            self.result_store.store_result(task, response, score, miner_uid)
            self.result_store.record_miner_score(miner_uid, score["total"])

            # Log
            from sqlalchemy import text
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(
                        "INSERT INTO subnet_task_log (task_id, miner_uid, score, response_len) "
                        "VALUES (:tid, :uid, :s, :rlen)"
                    ), {"tid": task["task_id"], "uid": miner_uid,
                        "s": score["total"], "rlen": len(response or "")})
            except Exception:
                pass

        # 5. Set weights (in production, updates bittensor metagraph)
        avg_score = sum(scores) / len(scores) if scores else 0

        return {
            "tasks": len(tasks),
            "avg_score": round(avg_score, 3),
            "scores": [round(s, 3) for s in scores],
        }

    def _local_inference(self, task: dict) -> str:
        """Fallback: run task locally via llama.cpp when no miners available."""
        try:
            from llm.router import get_llm, Tier
            _client = get_llm(Tier.REASON)
            return _client.generate(task["prompt"], num_predict=500, temperature=0.3) or ""
        except Exception:
            return ""

    async def run_forever(self, interval: int = 30) -> None:
        """Main validator loop."""
        log.info("GRID Subnet Validator starting — netuid={n}", n=self.netuid)

        while True:
            try:
                result = await self.validation_step()
                log.info(
                    "Validation step: {t} tasks, avg_score={s}",
                    t=result["tasks"], s=result.get("avg_score", 0),
                )
            except KeyboardInterrupt:
                break
            except Exception as exc:
                log.error("Validation error: {e}", e=str(exc))

            await asyncio.sleep(interval)


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    """Entry point for the validator."""
    import argparse

    parser = argparse.ArgumentParser(description="GRID Subnet Validator")
    parser.add_argument("--netuid", type=int, default=1, help="Subnet UID")
    parser.add_argument("--interval", type=int, default=30, help="Seconds between cycles")
    args = parser.parse_args()

    from db import get_engine
    engine = get_engine()

    validator = GRIDValidator(engine, netuid=args.netuid)
    asyncio.run(validator.run_forever(interval=args.interval))


if __name__ == "__main__":
    main()
