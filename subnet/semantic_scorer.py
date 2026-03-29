"""
GRID Subnet Semantic Scorer.

Embedding-based semantic scoring for miner response validation.
Replaces Jaccard word-overlap cross-validation and keyword-counting
quality scoring with proper semantic analysis using cosine similarity.

Embedding backends (graceful degradation):
  1. sentence-transformers (best quality)
  2. sklearn TF-IDF (good fallback)
  3. Simple word-frequency vectors (zero-dependency fallback)

Usage:
    from subnet.semantic_scorer import SemanticScorer
    scorer = SemanticScorer(engine)
    result = scorer.cross_validate([(miner_id, response_text), ...])
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter
from typing import Any

import numpy as np
from loguru import logger as log

# ---------------------------------------------------------------------------
# Ensure grid root is on path
# ---------------------------------------------------------------------------
_GRID_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

# ---------------------------------------------------------------------------
# Embedding backend selection (graceful degradation)
# ---------------------------------------------------------------------------
_EMBEDDING_BACKEND: str = "word_freq"  # worst-case default
_ST_MODEL = None
_TFIDF_VECTORIZER = None

try:
    from sentence_transformers import SentenceTransformer

    _ST_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    _EMBEDDING_BACKEND = "sentence_transformers"
    log.info("SemanticScorer using sentence-transformers backend")
except ImportError:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer

        _TFIDF_VECTORIZER = TfidfVectorizer(
            max_features=4096,
            stop_words="english",
            sublinear_tf=True,
        )
        _EMBEDDING_BACKEND = "tfidf"
        log.info("SemanticScorer using TF-IDF backend (sklearn)")
    except ImportError:
        log.warning(
            "SemanticScorer falling back to word-frequency vectors. "
            "Install sentence-transformers or scikit-learn for better quality."
        )


# ---------------------------------------------------------------------------
# Confidence labels used across GRID intelligence layer
# ---------------------------------------------------------------------------
_CONFIDENCE_LABELS = ["confirmed", "derived", "estimated", "rumored", "inferred"]


class SemanticScorer:
    """Embedding-based scorer for GRID subnet miner responses.

    Provides semantic cross-validation, collusion detection, and
    multi-dimensional quality scoring using cosine similarity over
    text embeddings instead of naive word overlap.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine for actor/intelligence lookups during scoring.
    """

    # Weights for quality scoring dimensions
    QUALITY_WEIGHTS = {
        "specificity": 0.25,
        "coherence": 0.20,
        "accuracy": 0.30,
        "novelty": 0.25,
    }

    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self._tfidf_fitted = False
        # Cache embeddings within a scoring session to avoid recomputation
        self._embed_cache: dict[int, np.ndarray] = {}

    # ------------------------------------------------------------------
    # Core embedding + similarity
    # ------------------------------------------------------------------

    def embed(self, text: str) -> np.ndarray:
        """Generate an embedding vector for the given text.

        Uses the best available backend:
          1. sentence-transformers  -> dense 384-d vector
          2. sklearn TF-IDF        -> sparse-to-dense vector
          3. word-frequency         -> simple bag-of-words vector

        Returns
        -------
        np.ndarray
            1-D float32 embedding vector (L2-normalized).
        """
        if not text or not text.strip():
            return np.zeros(384, dtype=np.float32)

        cache_key = hash(text)
        if cache_key in self._embed_cache:
            return self._embed_cache[cache_key]

        if _EMBEDDING_BACKEND == "sentence_transformers":
            vec = self._embed_st(text)
        elif _EMBEDDING_BACKEND == "tfidf":
            vec = self._embed_tfidf(text)
        else:
            vec = self._embed_word_freq(text)

        # L2-normalize
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm

        self._embed_cache[cache_key] = vec
        return vec

    @staticmethod
    def _embed_st(text: str) -> np.ndarray:
        """Embed using sentence-transformers."""
        return _ST_MODEL.encode(text, show_progress_bar=False).astype(np.float32)

    def _embed_tfidf(self, text: str) -> np.ndarray:
        """Embed using TF-IDF vectorizer.

        On first call, fits the vectorizer on the provided text.
        Subsequent calls transform against the fitted vocabulary.
        If the vectorizer is already fitted, uses transform only.
        """
        global _TFIDF_VECTORIZER
        try:
            if not self._tfidf_fitted:
                # Fit on this text (will be re-fitted with corpus in cross_validate)
                vec = _TFIDF_VECTORIZER.fit_transform([text]).toarray()[0]
                self._tfidf_fitted = True
            else:
                vec = _TFIDF_VECTORIZER.transform([text]).toarray()[0]
        except Exception:
            # If transform fails (e.g. empty vocabulary), fall back
            return self._embed_word_freq(text)
        return vec.astype(np.float32)

    def _fit_tfidf_corpus(self, texts: list[str]) -> None:
        """Fit TF-IDF vectorizer on a full corpus for consistent embeddings."""
        global _TFIDF_VECTORIZER
        if _EMBEDDING_BACKEND == "tfidf" and texts:
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer

                _TFIDF_VECTORIZER = TfidfVectorizer(
                    max_features=4096,
                    stop_words="english",
                    sublinear_tf=True,
                )
                _TFIDF_VECTORIZER.fit(texts)
                self._tfidf_fitted = True
                # Clear embedding cache since vocabulary changed
                self._embed_cache.clear()
            except Exception as e:
                log.warning(f"TF-IDF corpus fit failed: {e}")

    @staticmethod
    def _embed_word_freq(text: str) -> np.ndarray:
        """Zero-dependency fallback: hash-bucketed word frequency vector.

        Maps each word to a bucket via hash and builds a frequency vector.
        Not great quality, but works without any ML dependencies.
        """
        dim = 384  # match sentence-transformers output size
        vec = np.zeros(dim, dtype=np.float32)
        words = re.findall(r"\w+", text.lower())
        if not words:
            return vec
        for word in words:
            bucket = hash(word) % dim
            vec[bucket] += 1.0
        return vec

    @staticmethod
    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        """Compute cosine similarity between two vectors.

        Both vectors are assumed L2-normalized by embed(), so this
        reduces to a dot product. Handles zero vectors gracefully.

        Returns
        -------
        float
            Cosine similarity in [-1, 1]. Returns 0.0 for zero vectors.
        """
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    # ------------------------------------------------------------------
    # Cross-validation (replaces Jaccard)
    # ------------------------------------------------------------------

    def cross_validate(
        self, responses: list[tuple[str, str]]
    ) -> dict:
        """Cross-validate miner responses using cosine similarity.

        Compares each miner's response embedding against all others.
        Outliers (low agreement) and colluders (suspiciously high
        agreement) are flagged.

        Parameters
        ----------
        responses : list[tuple[str, str]]
            List of (miner_id, response_text) pairs.

        Returns
        -------
        dict
            {
                "status": "complete" | "pending",
                "submissions": int,
                "agreements": {miner_id: float},
                "outliers": [miner_id, ...],
                "collusion_pairs": [(miner_id_a, miner_id_b), ...],
                "backend": str,
            }
        """
        if len(responses) < 2:
            return {"status": "pending", "submissions": len(responses)}

        # For TF-IDF, fit on the full corpus first for consistent vocabulary
        if _EMBEDDING_BACKEND == "tfidf":
            corpus = [resp for _, resp in responses if resp]
            self._fit_tfidf_corpus(corpus)

        # Compute embeddings
        embeddings: list[tuple[str, np.ndarray]] = []
        for miner_id, text in responses:
            vec = self.embed(text or "")
            embeddings.append((miner_id, vec))

        # Pairwise cosine similarity
        agreements: dict[str, float] = {}
        collusion_pairs: list[tuple[str, str]] = []

        for i, (mid_a, vec_a) in enumerate(embeddings):
            sims = []
            for j, (mid_b, vec_b) in enumerate(embeddings):
                if i == j:
                    continue
                sim = self.cosine_similarity(vec_a, vec_b)
                sims.append(sim)

                # Collusion detection: only check each pair once
                if i < j and sim >= 0.98:
                    collusion_pairs.append((mid_a, mid_b))
                    log.warning(
                        f"Collusion detected: {mid_a} <-> {mid_b} "
                        f"(cosine={sim:.4f})"
                    )

            avg_sim = sum(sims) / max(len(sims), 1)
            agreements[mid_a] = round(avg_sim, 4)

        # Flag outliers: agreement < 0.15 when group average > 0.3
        avg_all = sum(agreements.values()) / max(len(agreements), 1)
        outliers = [
            mid
            for mid, agr in agreements.items()
            if agr < 0.15 and avg_all > 0.3
        ]

        if outliers:
            log.info(
                f"Cross-validation outliers: {outliers} "
                f"(group avg={avg_all:.3f})"
            )

        return {
            "status": "complete",
            "submissions": len(responses),
            "agreements": agreements,
            "outliers": outliers,
            "collusion_pairs": collusion_pairs,
            "backend": _EMBEDDING_BACKEND,
        }

    # ------------------------------------------------------------------
    # Collusion detection
    # ------------------------------------------------------------------

    def detect_collusion(
        self,
        responses: list[tuple[str, str]],
        threshold: float = 0.98,
    ) -> list[tuple[str, str]]:
        """Detect suspiciously identical responses between miners.

        Copy-paste or template sharing produces cosine > 0.98.
        Legitimate independent research typically yields 0.4-0.8.

        Parameters
        ----------
        responses : list[tuple[str, str]]
            List of (miner_id, response_text) pairs.
        threshold : float
            Cosine similarity above which a pair is flagged.

        Returns
        -------
        list[tuple[str, str]]
            Pairs of (miner_id_a, miner_id_b) flagged as colluding.
        """
        if len(responses) < 2:
            return []

        # For TF-IDF, fit on full corpus
        if _EMBEDDING_BACKEND == "tfidf":
            corpus = [resp for _, resp in responses if resp]
            self._fit_tfidf_corpus(corpus)

        embeddings = [(mid, self.embed(text or "")) for mid, text in responses]
        colluders: list[tuple[str, str]] = []

        for i in range(len(embeddings)):
            mid_a, vec_a = embeddings[i]
            for j in range(i + 1, len(embeddings)):
                mid_b, vec_b = embeddings[j]
                sim = self.cosine_similarity(vec_a, vec_b)
                if sim >= threshold:
                    colluders.append((mid_a, mid_b))
                    log.warning(
                        f"Collusion flagged: {mid_a} <-> {mid_b} "
                        f"(cosine={sim:.4f}, threshold={threshold})"
                    )

        return colluders

    # ------------------------------------------------------------------
    # Quality scoring (replaces ResponseScorer keyword counting)
    # ------------------------------------------------------------------

    def score_quality(
        self,
        response: str,
        task: dict,
        ground_truth: str | None = None,
    ) -> dict:
        """Comprehensive quality score with multiple dimensions.

        Dimensions
        ----------
        - specificity (0-1): Named entities, dollar amounts, dates, tickers
        - coherence (0-1): GRID confidence labels, structural markers
        - accuracy (0-1): Cross-reference against DB + ground truth
        - novelty (0-1): Information not already in our database

        Parameters
        ----------
        response : str
            Miner's response text.
        task : dict
            The task definition (contains prompt, topic, etc.).
        ground_truth : str, optional
            Known-good answer for honeypot calibration.

        Returns
        -------
        dict
            Score breakdown with 'total' and per-dimension scores.
        """
        if not response or len(response.strip()) < 50:
            return {"total": 0.0, "reason": "response too short"}

        specificity = self._score_specificity(response)
        coherence = self._score_coherence(response)
        accuracy = self._score_accuracy(task, response, ground_truth)
        novelty = self._score_novelty(task, response)

        total = (
            specificity * self.QUALITY_WEIGHTS["specificity"]
            + coherence * self.QUALITY_WEIGHTS["coherence"]
            + accuracy * self.QUALITY_WEIGHTS["accuracy"]
            + novelty * self.QUALITY_WEIGHTS["novelty"]
        )

        return {
            "total": round(total, 4),
            "specificity": round(specificity, 3),
            "coherence": round(coherence, 3),
            "accuracy": round(accuracy, 3),
            "novelty": round(novelty, 3),
            "backend": _EMBEDDING_BACKEND,
        }

    def score_against_ground_truth(
        self, response: str, ground_truth: str
    ) -> float:
        """Cosine similarity between response and ground truth.

        Used for honeypot calibration: validators inject tasks with
        known answers to calibrate miner scoring.

        Returns
        -------
        float
            Cosine similarity in [0, 1].
        """
        if not response or not ground_truth:
            return 0.0

        vec_r = self.embed(response)
        vec_gt = self.embed(ground_truth)
        sim = self.cosine_similarity(vec_r, vec_gt)
        # Clamp to [0, 1] since negative similarity = completely wrong
        return max(0.0, sim)

    # ------------------------------------------------------------------
    # Scoring sub-dimensions
    # ------------------------------------------------------------------

    @staticmethod
    def _score_specificity(response: str) -> float:
        """Score based on specific data points: names, amounts, dates, tickers."""
        score = 0.2  # base score for any non-trivial response

        # Dollar amounts and percentages
        amounts = re.findall(
            r"\$[\d,.]+[BMKTbmkt]?|\d+(?:\.\d+)?%|\d{4}-\d{2}-\d{2}", response
        )
        score += min(0.3, len(amounts) * 0.025)

        # Named entities (First Last pattern)
        names = re.findall(r"[A-Z][a-z]+ [A-Z][a-z]+", response)
        score += min(0.25, len(names) * 0.02)

        # Stock tickers
        tickers = re.findall(r"\b[A-Z]{2,5}\b", response)
        # Filter common English words that look like tickers
        noise = {
            "THE", "AND", "FOR", "BUT", "NOT", "ARE", "WAS", "HAS",
            "HAD", "ITS", "ALL", "CAN", "MAY", "NOW", "NEW", "ONE",
            "TWO", "ANY", "USE", "WAY", "WHO", "OUR", "OUT", "SAY",
            "HER", "HIM", "HOW", "MAN", "OLD", "SEE", "GET", "SET",
            "LET", "PUT", "RUN", "SAW", "GOT", "DID", "HIS", "ALSO",
            "BEEN", "FROM", "HAVE", "INTO", "JUST", "LIKE", "MADE",
            "MANY", "MUCH", "MUST", "NEED", "ONLY", "OVER", "SOME",
            "SUCH", "THAN", "THAT", "THEM", "THEN", "THEY", "THIS",
            "VERY", "WHEN", "WILL", "WITH", "WHAT", "WERE", "BEEN",
            "MORE", "MOST", "EACH", "GRID",
        }
        real_tickers = [t for t in tickers if t not in noise]
        score += min(0.25, len(real_tickers) * 0.02)

        return min(1.0, score)

    @staticmethod
    def _score_coherence(response: str) -> float:
        """Score structural quality and use of GRID confidence labels."""
        score = 0.15  # base

        # GRID confidence labels
        for label in _CONFIDENCE_LABELS:
            if label in response.lower():
                score += 0.12

        # Structural markers (paragraphs, bullet points, sections)
        if "\n\n" in response:
            score += 0.1  # has paragraphs
        if re.search(r"^[\-\*\d]+[\.\)]\s", response, re.MULTILINE):
            score += 0.1  # has bullet points or numbered items
        if len(response) > 500:
            score += 0.05  # substantial length

        return min(1.0, score)

    def _score_accuracy(
        self, task: dict, response: str, ground_truth: str | None = None
    ) -> float:
        """Cross-reference response against database and ground truth.

        If ground_truth is provided (honeypot), uses semantic similarity.
        Otherwise, checks whether mentioned actors exist in our DB.
        """
        score = 0.4  # neutral baseline

        # Ground truth comparison (honeypot)
        if ground_truth:
            gt_sim = self.score_against_ground_truth(response, ground_truth)
            # Ground truth similarity dominates accuracy scoring
            return min(1.0, 0.2 + gt_sim * 0.8)

        # DB cross-reference: check mentioned actors
        try:
            from sqlalchemy import text

            potential_names = re.findall(r"[A-Z][a-z]+ [A-Z][a-z]+", response)[:15]
            if not potential_names:
                return score

            with self.engine.connect() as conn:
                matched = 0
                for name in potential_names:
                    row = conn.execute(
                        text(
                            "SELECT COUNT(*) FROM actors "
                            "WHERE LOWER(name) LIKE LOWER(:n)"
                        ),
                        {"n": f"%{name}%"},
                    ).fetchone()
                    if row and row[0] > 0:
                        matched += 1

                if potential_names:
                    match_ratio = matched / len(potential_names)
                    score += match_ratio * 0.4  # up to 0.4 bonus
        except Exception as e:
            log.debug(f"Accuracy DB check failed (non-fatal): {e}")

        return min(1.0, score)

    def _score_novelty(self, task: dict, response: str) -> float:
        """Score information novelty relative to existing intelligence.

        Compares the response embedding against recent intel summaries
        stored in encrypted_intelligence. Lower similarity to existing
        data = higher novelty.
        """
        base_score = 0.5  # neutral default

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                # Fetch recent intelligence summaries for comparison
                rows = conn.execute(
                    text(
                        "SELECT summary FROM encrypted_intelligence "
                        "ORDER BY created_at DESC LIMIT 20"
                    )
                ).fetchall()

            if not rows:
                return base_score

            existing_texts = [row[0] for row in rows if row[0]]
            if not existing_texts:
                return base_score

            response_vec = self.embed(response)
            max_sim = 0.0

            for existing in existing_texts:
                existing_vec = self.embed(existing)
                sim = self.cosine_similarity(response_vec, existing_vec)
                max_sim = max(max_sim, sim)

            # High similarity to existing intel = low novelty
            # Invert: novelty = 1 - max_similarity (clamped)
            novelty = max(0.1, 1.0 - max_sim)
            return min(1.0, novelty)

        except Exception as e:
            log.debug(f"Novelty scoring failed (non-fatal): {e}")
            return base_score

    # ------------------------------------------------------------------
    # Factual claim extraction + verification
    # ------------------------------------------------------------------

    def extract_claims(self, response: str) -> list[dict]:
        """Extract verifiable factual claims from a response.

        Looks for patterns like:
          - "$X invested in Y"
          - "A acquired B for $C"
          - "X owns Y% of Z"
          - Named entities with associated actions

        Returns
        -------
        list[dict]
            Each claim: {"text": str, "type": str, "entities": list[str]}
        """
        claims: list[dict] = []

        # Financial transaction patterns
        tx_patterns = [
            (
                r"([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]{2,5})\s+"
                r"(?:invested|acquired|bought|sold|divested|raised)\s+"
                r".*?\$[\d,.]+[BMKTbmkt]?",
                "transaction",
            ),
            (
                r"([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]{2,5})\s+"
                r"(?:owns?|holds?|controls?)\s+\d+(?:\.\d+)?%",
                "ownership",
            ),
            (
                r"([A-Z][a-z]+ [A-Z][a-z]+)\s+"
                r"(?:was appointed|became|joined|left|resigned)",
                "personnel",
            ),
        ]

        for pattern, claim_type in tx_patterns:
            for match in re.finditer(pattern, response):
                entities = re.findall(
                    r"[A-Z][a-z]+ [A-Z][a-z]+|[A-Z]{2,5}", match.group()
                )
                claims.append(
                    {
                        "text": match.group().strip(),
                        "type": claim_type,
                        "entities": entities,
                    }
                )

        return claims

    def verify_claims(
        self,
        claims: list[dict],
        llm_endpoint: str | None = None,
    ) -> list[dict]:
        """Verify extracted claims against DB and optionally via LLM.

        Parameters
        ----------
        claims : list[dict]
            Claims from extract_claims().
        llm_endpoint : str, optional
            URL of server-side LLM for fact-checking (e.g. local llama.cpp).

        Returns
        -------
        list[dict]
            Each claim annotated with:
              - "verified": bool | None
              - "confidence": str (confirmed/derived/estimated/rumored/inferred)
              - "source": str
        """
        verified_claims = []

        for claim in claims:
            result = {**claim, "verified": None, "confidence": "inferred", "source": "unverified"}

            # Step 1: Check entities against our actor database
            try:
                from sqlalchemy import text

                with self.engine.connect() as conn:
                    for entity in claim.get("entities", []):
                        row = conn.execute(
                            text(
                                "SELECT name, category FROM actors "
                                "WHERE LOWER(name) LIKE LOWER(:n) LIMIT 1"
                            ),
                            {"n": f"%{entity}%"},
                        ).fetchone()
                        if row:
                            result["confidence"] = "derived"
                            result["source"] = "actor_db"
                            break
            except Exception:
                pass

            # Step 2: LLM verification if endpoint is available
            if llm_endpoint and result["verified"] is None:
                try:
                    import urllib.request

                    payload = {
                        "prompt": (
                            f"Fact-check this claim. Reply with ONLY "
                            f"'TRUE', 'FALSE', or 'UNKNOWN':\n\n"
                            f"{claim['text']}"
                        ),
                        "max_tokens": 10,
                        "temperature": 0.0,
                    }
                    req = urllib.request.Request(
                        llm_endpoint,
                        data=json.dumps(payload).encode(),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        llm_result = json.loads(resp.read().decode())
                        answer = llm_result.get("content", "").strip().upper()
                        if "TRUE" in answer:
                            result["verified"] = True
                            result["confidence"] = "confirmed"
                            result["source"] = "llm_verification"
                        elif "FALSE" in answer:
                            result["verified"] = False
                            result["confidence"] = "confirmed"
                            result["source"] = "llm_verification"
                except Exception as e:
                    log.debug(f"LLM claim verification failed: {e}")

            verified_claims.append(result)

        return verified_claims

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the embedding cache between scoring sessions."""
        self._embed_cache.clear()
        self._tfidf_fitted = False

    def backend_info(self) -> dict:
        """Return information about the active embedding backend."""
        info = {
            "backend": _EMBEDDING_BACKEND,
            "embedding_dim": 384,
            "cache_size": len(self._embed_cache),
        }
        if _EMBEDDING_BACKEND == "sentence_transformers":
            info["model"] = "all-MiniLM-L6-v2"
        elif _EMBEDDING_BACKEND == "tfidf":
            info["max_features"] = 4096
            info["fitted"] = self._tfidf_fitted
        return info

