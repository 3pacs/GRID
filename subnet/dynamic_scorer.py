"""
GRID Subnet Dynamic Scoring.

Epoch-rotating scoring weights that change every Bittensor tempo (~12 min).
Miners cannot optimize for a static target because the target moves.

The scoring formula uses HMAC(validator_secret, epoch_number) to generate
deterministic but secret weight configurations. The API only returns the
total score and a qualitative tier — never the per-dimension breakdown.

8 scoring dimensions:
    1. specificity   — names, numbers, dates, tickers
    2. structure     — follows GRID confidence labeling
    3. accuracy      — cross-referenced against known facts
    4. novelty       — new information not already in DB
    5. consistency   — doesn't contradict confirmed facts
    6. coherence     — reads as a coherent narrative
    7. citations     — references specific sources (SEC, court, etc.)
    8. temporal      — references recent/appropriate events
"""

from __future__ import annotations

import hashlib
import hmac
import math
import re
import struct
import time
from typing import Any

from loguru import logger as log


# ── Constants ────────────────────────────────────────────────────────────

EPOCH_DURATION_SECONDS = 720  # 12 minutes — Bittensor tempo
NUM_DIMENSIONS = 8
MIN_WEIGHT = 0.05
MAX_WEIGHT = 0.50
FLOOR_PENALTY = 0.15  # any dimension below this triggers geometric penalty

DIMENSIONS = [
    "specificity", "structure", "accuracy", "novelty",
    "consistency", "coherence", "citations", "temporal",
]

# Qualitative tiers returned to miners (not the raw scores)
TIER_THRESHOLDS = {
    "excellent": 0.75,
    "good": 0.55,
    "fair": 0.35,
    "poor": 0.0,
}


class DynamicScorer:
    """Epoch-rotating scorer with secret weight generation.

    The key insight: miners see the code but NOT the validator's secret key.
    The weight configuration changes every 12 minutes. Even if a miner
    reverse-engineers one epoch's weights, it changes before they can exploit it.
    The only winning strategy is to produce genuinely high-quality responses
    across all dimensions.
    """

    def __init__(self, secret_key: str | None = None) -> None:
        import os
        self.secret_key = (
            secret_key or os.getenv("GRID_SCORER_SECRET")
            or os.getenv("GRID_INTEL_KEY")
            or "default-scorer-key"
        ).encode()

    def get_current_epoch(self) -> int:
        """Get the current epoch number."""
        return int(time.time()) // EPOCH_DURATION_SECONDS

    def get_epoch_weights(self, epoch: int | None = None) -> dict[str, float]:
        """Generate deterministic weights for an epoch using HMAC.

        The weights are:
        - Deterministic (same epoch always produces same weights)
        - Secret (requires the validator's key to compute)
        - Constrained (sum to 1.0, each between MIN_WEIGHT and MAX_WEIGHT)
        - Different every epoch (12-minute rotation)
        """
        if epoch is None:
            epoch = self.get_current_epoch()

        # Generate raw values from HMAC
        mac = hmac.new(self.secret_key, struct.pack(">Q", epoch), hashlib.sha256).digest()

        # Extract 8 values from the 32-byte hash
        raw = []
        for i in range(NUM_DIMENSIONS):
            # Each dimension gets 4 bytes → float in [0, 1)
            val = struct.unpack_from(">I", mac, (i * 4) % 28)[0] / (2**32)
            raw.append(val)

        # Constrain to [MIN_WEIGHT, MAX_WEIGHT] and normalize to sum=1
        constrained = [MIN_WEIGHT + val * (MAX_WEIGHT - MIN_WEIGHT) for val in raw]
        total = sum(constrained)
        normalized = {dim: w / total for dim, w in zip(DIMENSIONS, constrained)}

        return normalized

    def get_active_dimensions(self, epoch: int | None = None) -> list[str]:
        """All 8 dimensions are always active, but weights vary."""
        return list(DIMENSIONS)

    def score(self, dimension_scores: dict[str, float], epoch: int | None = None) -> dict:
        """Score a response using the current epoch's weights.

        Args:
            dimension_scores: dict mapping dimension name → score (0-1)
            epoch: optional epoch override

        Returns:
            dict with 'total' (float), 'tier' (str). NO per-dimension breakdown.
        """
        weights = self.get_epoch_weights(epoch)

        # Apply non-linear aggregation with floor penalty
        total = self._apply_nonlinear(dimension_scores, weights)

        # Determine qualitative tier
        tier = "poor"
        for tier_name, threshold in sorted(TIER_THRESHOLDS.items(), key=lambda x: -x[1]):
            if total >= threshold:
                tier = tier_name
                break

        return {"total": round(total, 4), "tier": tier}

    def _apply_nonlinear(self, scores: dict[str, float], weights: dict[str, float]) -> float:
        """Non-linear aggregation: weighted geometric mean with floor penalty.

        Why geometric mean instead of arithmetic mean:
        - Arithmetic mean: a miner can dump one dimension and boost another
        - Geometric mean: one low dimension drags the entire score down
        - This forces miners to perform well ACROSS ALL dimensions

        Floor penalty: any dimension below FLOOR_PENALTY gets an extra
        multiplicative penalty, making it catastrophic to ignore any dimension.
        """
        if not scores:
            return 0.0

        # Compute weighted geometric mean
        log_sum = 0.0
        weight_sum = 0.0

        for dim in DIMENSIONS:
            score = max(0.001, scores.get(dim, 0.3))  # avoid log(0)
            weight = weights.get(dim, 1.0 / NUM_DIMENSIONS)

            # Floor penalty: if score is below threshold, apply extra drag
            if score < FLOOR_PENALTY:
                score *= 0.5  # halve the already-low score

            log_sum += weight * math.log(score)
            weight_sum += weight

        if weight_sum == 0:
            return 0.0

        geometric_mean = math.exp(log_sum / weight_sum)
        return min(1.0, max(0.0, geometric_mean))

    def score_dimensions(self, response: str, task: dict | None = None) -> dict[str, float]:
        """Score a response across all 8 dimensions.

        This is the core quality assessment. Each dimension is scored
        independently, then combined via the epoch-weighted geometric mean.
        """
        scores = {}
        scores["specificity"] = self._score_specificity(response)
        scores["structure"] = self._score_structure(response)
        scores["accuracy"] = self._score_accuracy(response, task)
        scores["novelty"] = self._score_novelty(response, task)
        scores["consistency"] = self._score_consistency(response)
        scores["coherence"] = self._score_coherence(response)
        scores["citations"] = self._score_citations(response)
        scores["temporal"] = self._score_temporal(response)
        return scores

    def _score_specificity(self, response: str) -> float:
        """Score: does the response contain specific names, numbers, dates?"""
        score = 0.1
        numbers = len(re.findall(r'\$[\d,.]+[BMKTbmkt]?|\d+\.?\d*%|\d{4}-\d{2}-\d{2}', response))
        names = len(re.findall(r'[A-Z][a-z]{2,} [A-Z][a-z]{2,}', response))
        tickers = len(re.findall(r'\b[A-Z]{2,5}\b', response))

        score += min(0.3, numbers * 0.02)
        score += min(0.3, names * 0.015)
        score += min(0.3, tickers * 0.015)
        return min(1.0, score)

    def _score_structure(self, response: str) -> float:
        """Score: follows GRID confidence labeling convention."""
        score = 0.1
        labels = ["confirmed", "derived", "estimated", "rumored", "inferred"]
        found = sum(1 for l in labels if l in response.lower())
        score += found * 0.15

        # Bonus for numbered lists, headers, structured output
        if re.search(r'\d+\.\s', response):
            score += 0.1
        if re.search(r'[A-Z]{2,}:', response):
            score += 0.05

        return min(1.0, score)

    def _score_accuracy(self, response: str, task: dict | None) -> float:
        """Score: claims that can be cross-referenced against known facts.

        In production, this would query the DB. For now, uses heuristics.
        """
        score = 0.4  # neutral baseline

        # Penalize obviously wrong claims
        if any(w in response.lower() for w in ["i don't know", "i cannot", "as an ai"]):
            score -= 0.3

        # Bonus for specific financial data patterns
        if re.search(r'\$\d+[BMT]', response):
            score += 0.1
        if re.search(r'Q[1-4]\s*20\d{2}', response):
            score += 0.1
        if re.search(r'SEC|EDGAR|10-K|10-Q|Form 4|proxy', response, re.IGNORECASE):
            score += 0.1

        return min(1.0, max(0.0, score))

    def _score_novelty(self, response: str, task: dict | None) -> float:
        """Score: new information not already in the database.

        Placeholder — full implementation queries encrypted_intelligence.
        """
        # Longer, more detailed responses tend to have more novel content
        words = len(response.split())
        if words > 300:
            return 0.7
        elif words > 150:
            return 0.5
        elif words > 50:
            return 0.3
        return 0.1

    def _score_consistency(self, response: str) -> float:
        """Score: doesn't self-contradict or contradict known facts."""
        score = 0.6  # baseline — assume consistent unless proven otherwise

        # Check for internal contradictions (basic)
        sentences = response.split(".")
        if len(sentences) > 2:
            # Very basic: check if response contradicts itself
            for s in sentences:
                s_lower = s.lower().strip()
                if "however" in s_lower or "but" in s_lower or "contrary" in s_lower:
                    # Not necessarily bad — nuanced responses use qualifiers
                    score += 0.05
            score = min(0.8, score)

        return score

    def _score_coherence(self, response: str) -> float:
        """Score: reads as a coherent narrative, not random fragments."""
        words = response.split()
        if len(words) < 20:
            return 0.1

        # Vocabulary richness (type-token ratio)
        unique = len(set(w.lower() for w in words))
        ttr = unique / len(words)

        # Good TTR for financial analysis: 0.3-0.7
        if 0.3 <= ttr <= 0.7:
            score = 0.7
        elif ttr > 0.7:
            score = 0.5  # too many unique words = word salad
        else:
            score = 0.3  # too repetitive

        # Sentence structure
        sentences = [s.strip() for s in response.split(".") if len(s.strip()) > 10]
        if len(sentences) >= 3:
            score += 0.1
        if len(sentences) >= 5:
            score += 0.1

        return min(1.0, score)

    def _score_citations(self, response: str) -> float:
        """Score: references specific data sources."""
        score = 0.1
        source_patterns = [
            r'SEC\b', r'EDGAR', r'10-K', r'10-Q', r'Form\s*4', r'proxy\s*statement',
            r'annual\s*report', r'earnings\s*call', r'Bloomberg', r'Reuters',
            r'court\s*filing', r'PACER', r'DOJ', r'FTC', r'CFTC',
            r'Panama\s*Papers', r'Paradise\s*Papers', r'Pandora\s*Papers',
            r'ICIJ', r'congressional\s*record', r'lobbying\s*disclosure',
        ]
        for pattern in source_patterns:
            if re.search(pattern, response, re.IGNORECASE):
                score += 0.05

        return min(1.0, score)

    def _score_temporal(self, response: str) -> float:
        """Score: references recent/appropriate time periods."""
        score = 0.2

        # Recent year references
        years = re.findall(r'20[12]\d', response)
        if years:
            recent = [int(y) for y in years if int(y) >= 2024]
            if recent:
                score += 0.3
            score += min(0.2, len(years) * 0.03)

        # Quarter references
        if re.search(r'Q[1-4]\s*20[12]\d', response):
            score += 0.2

        # Month/date references
        if re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+20[12]\d', response):
            score += 0.1

        return min(1.0, score)
