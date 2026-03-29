"""
GRID Subnet Sybil Detection.

Multi-layered defense against Sybil attacks (one operator running many
miner identities to game the system).

Layers:
    1. Economic barrier — 1 TAO stake per identity ($400+)
    2. Behavioral fingerprinting — timing, vocabulary, response patterns
    3. Cluster detection — DBSCAN on behavioral distance vectors
    4. Rate limiting — max tasks per hour per miner
    5. Cross-validation collusion detection — miners agreeing too often

Miners can use ANY model or agent (Claude, GPT, local Hermes, custom agent).
The scoring is output-quality-based, not method-based. High quality = high
reward regardless of how it was produced. This incentivizes miners to use
the best tools available, which benefits GRID.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

MAX_TASKS_PER_HOUR = 60       # Rate limit (generous — allows agent-based miners)
MAX_TASKS_PER_MINUTE = 5      # Burst limit
CLUSTER_SIMILARITY_THRESHOLD = 0.85  # behavioral similarity to flag as potential Sybil
MIN_TASKS_FOR_PROFILING = 10  # need enough data to build a profile
COLLUSION_AGREEMENT_THRESHOLD = 0.95  # if two miners agree 95%+ of the time → suspicious


class BehavioralProfile:
    """Behavioral fingerprint of a miner based on their submission patterns."""

    def __init__(self) -> None:
        self.response_lengths: list[int] = []
        self.response_times_seconds: list[float] = []
        self.submission_hours: list[int] = []  # hour of day (0-23)
        self.vocabulary_richness: list[float] = []  # type-token ratios
        self.task_types: dict[str, int] = defaultdict(int)
        self.quality_scores: list[float] = []
        self.total_submissions: int = 0

    def add_submission(
        self,
        response_length: int,
        response_time_s: float,
        submission_hour: int,
        ttr: float,
        task_type: str,
        quality_score: float,
    ) -> None:
        """Record a submission for profiling."""
        self.response_lengths.append(response_length)
        self.response_times_seconds.append(response_time_s)
        self.submission_hours.append(submission_hour)
        self.vocabulary_richness.append(ttr)
        self.task_types[task_type] += 1
        self.quality_scores.append(quality_score)
        self.total_submissions += 1

    def to_feature_vector(self) -> list[float]:
        """Convert profile to a numeric feature vector for distance computation."""
        if self.total_submissions < MIN_TASKS_FOR_PROFILING:
            return []

        features = [
            # Response length stats
            statistics.mean(self.response_lengths) / 1000.0,  # normalized
            statistics.stdev(self.response_lengths) / 500.0 if len(self.response_lengths) > 1 else 0,

            # Response time stats
            statistics.mean(self.response_times_seconds) / 60.0,  # in minutes
            statistics.stdev(self.response_times_seconds) / 30.0 if len(self.response_times_seconds) > 1 else 0,

            # Submission timing
            statistics.mean(self.submission_hours) / 24.0,
            statistics.stdev(self.submission_hours) / 12.0 if len(self.submission_hours) > 1 else 0,

            # Vocabulary richness
            statistics.mean(self.vocabulary_richness),
            statistics.stdev(self.vocabulary_richness) if len(self.vocabulary_richness) > 1 else 0,

            # Quality consistency
            statistics.mean(self.quality_scores),
            statistics.stdev(self.quality_scores) if len(self.quality_scores) > 1 else 0,
        ]
        return features

    def to_dict(self) -> dict:
        """Serialize for DB storage."""
        return {
            "total_submissions": self.total_submissions,
            "avg_response_length": statistics.mean(self.response_lengths) if self.response_lengths else 0,
            "avg_response_time_s": statistics.mean(self.response_times_seconds) if self.response_times_seconds else 0,
            "avg_vocabulary_richness": statistics.mean(self.vocabulary_richness) if self.vocabulary_richness else 0,
            "avg_quality_score": statistics.mean(self.quality_scores) if self.quality_scores else 0,
            "submission_hour_distribution": dict(
                sorted(defaultdict(int, {h: self.submission_hours.count(h) for h in set(self.submission_hours)}).items())
            ) if self.submission_hours else {},
        }


class SybilDetector:
    """Detects Sybil attacks through behavioral analysis and rate limiting."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._profiles: dict[str, BehavioralProfile] = {}
        self._rate_limits: dict[str, list[float]] = defaultdict(list)  # miner_id -> timestamps

    def check_rate_limit(self, miner_id: str) -> bool:
        """Check if miner is within rate limits.

        Returns True if within limits, False if should be throttled.
        """
        now = time.time()
        timestamps = self._rate_limits[miner_id]

        # Clean old entries
        cutoff_hour = now - 3600
        cutoff_minute = now - 60
        self._rate_limits[miner_id] = [t for t in timestamps if t > cutoff_hour]
        timestamps = self._rate_limits[miner_id]

        # Check hourly limit
        if len(timestamps) >= MAX_TASKS_PER_HOUR:
            log.warning("Miner {m} hit hourly rate limit ({n}/{max})",
                        m=miner_id[:8], n=len(timestamps), max=MAX_TASKS_PER_HOUR)
            return False

        # Check burst limit
        recent = [t for t in timestamps if t > cutoff_minute]
        if len(recent) >= MAX_TASKS_PER_MINUTE:
            log.warning("Miner {m} hit burst rate limit ({n}/{max}/min)",
                        m=miner_id[:8], n=len(recent), max=MAX_TASKS_PER_MINUTE)
            return False

        # Record this request
        self._rate_limits[miner_id].append(now)
        return True

    def record_submission(
        self,
        miner_id: str,
        response: str,
        response_time_s: float,
        task_type: str,
        quality_score: float,
    ) -> None:
        """Record a submission for behavioral profiling."""
        if miner_id not in self._profiles:
            self._profiles[miner_id] = BehavioralProfile()

        words = response.split()
        unique_words = len(set(w.lower() for w in words))
        ttr = unique_words / max(len(words), 1)

        self._profiles[miner_id].add_submission(
            response_length=len(response),
            response_time_s=response_time_s,
            submission_hour=datetime.now(timezone.utc).hour,
            ttr=ttr,
            task_type=task_type,
            quality_score=quality_score,
        )

    def detect_clusters(self) -> list[set[str]]:
        """Find clusters of miners with suspiciously similar behavior.

        Uses a simplified DBSCAN-like approach:
        1. Compute pairwise distances between all miner profiles
        2. Flag pairs with distance below threshold
        3. Group connected components into clusters
        """
        # Build feature vectors
        vectors: dict[str, list[float]] = {}
        for miner_id, profile in self._profiles.items():
            vec = profile.to_feature_vector()
            if vec:
                vectors[miner_id] = vec

        if len(vectors) < 2:
            return []

        # Compute pairwise cosine similarity
        suspicious_pairs: list[tuple[str, str]] = []
        miner_ids = list(vectors.keys())

        for i in range(len(miner_ids)):
            for j in range(i + 1, len(miner_ids)):
                sim = self._cosine_similarity(vectors[miner_ids[i]], vectors[miner_ids[j]])
                if sim > CLUSTER_SIMILARITY_THRESHOLD:
                    suspicious_pairs.append((miner_ids[i], miner_ids[j]))

        if not suspicious_pairs:
            return []

        # Group into connected components (clusters)
        clusters = self._connected_components(suspicious_pairs)

        if clusters:
            log.warning("Sybil detector found {n} suspicious clusters", n=len(clusters))

        return clusters

    def check_collusion(self, cross_validation_history: list[dict]) -> list[tuple[str, str]]:
        """Check if any pair of miners consistently agree on tasks.

        If two miners agree > COLLUSION_AGREEMENT_THRESHOLD of the time
        across multiple tasks, they may be colluding (or the same operator).
        """
        # Count agreements per pair
        pair_agreements: dict[tuple[str, str], int] = defaultdict(int)
        pair_total: dict[tuple[str, str], int] = defaultdict(int)

        for cv_result in cross_validation_history:
            miners = cv_result.get("miners", [])
            agreements = cv_result.get("agreements", {})

            for i in range(len(miners)):
                for j in range(i + 1, len(miners)):
                    pair = tuple(sorted([miners[i], miners[j]]))
                    pair_total[pair] += 1
                    if agreements.get(f"{miners[i]}_{miners[j]}", 0) > 0.7:
                        pair_agreements[pair] += 1

        # Flag pairs with high agreement rate
        colluding = []
        for pair, total in pair_total.items():
            if total >= 5:  # need enough samples
                rate = pair_agreements.get(pair, 0) / total
                if rate > COLLUSION_AGREEMENT_THRESHOLD:
                    colluding.append(pair)
                    log.warning(
                        "Collusion detected: {a} and {b} agree {r:.0%} of the time ({n} samples)",
                        a=pair[0][:8], b=pair[1][:8], r=rate, n=total,
                    )

        return colluding

    def save_profiles(self) -> int:
        """Persist behavioral profiles to DB."""
        import json
        count = 0
        try:
            with self.engine.begin() as conn:
                for miner_id, profile in self._profiles.items():
                    if profile.total_submissions >= MIN_TASKS_FOR_PROFILING:
                        conn.execute(text(
                            "INSERT INTO miner_behavioral_profiles (miner_id, profile_data, updated_at) "
                            "VALUES (:mid, CAST(:data AS jsonb), NOW()) "
                            "ON CONFLICT (miner_id) DO UPDATE SET "
                            "profile_data = EXCLUDED.profile_data, updated_at = NOW()"
                        ), {"mid": miner_id, "data": json.dumps(profile.to_dict())})
                        count += 1
        except Exception as exc:
            log.debug("Failed to save profiles: {e}", e=str(exc))
        return count

    @staticmethod
    def _cosine_similarity(a: list[float], b: list[float]) -> float:
        """Cosine similarity between two vectors."""
        if len(a) != len(b) or not a:
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    @staticmethod
    def _connected_components(pairs: list[tuple[str, str]]) -> list[set[str]]:
        """Find connected components from edge pairs."""
        graph: dict[str, set[str]] = defaultdict(set)
        for a, b in pairs:
            graph[a].add(b)
            graph[b].add(a)

        visited: set[str] = set()
        components: list[set[str]] = []

        for node in graph:
            if node in visited:
                continue
            component: set[str] = set()
            stack = [node]
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                component.add(current)
                stack.extend(graph[current] - visited)
            if len(component) >= 2:
                components.append(component)

        return components


# Need time module for rate limiting
import time
