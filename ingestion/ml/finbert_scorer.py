"""
FinBERT sentiment scoring pipeline for GRID.

Batch-scores text from raw_series (news headlines, Reddit posts, etc.)
using ProsusAI/finbert and writes compound sentiment scores back as new
series with ``finbert.`` prefix.

Supports GPU (CUDA) and CPU with automatic detection.
Designed for incremental scoring — tracks which rows have been scored
to avoid redundant work on subsequent runs.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from typing import Any

import torch
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Sources whose raw_payload contains scoreable text, mapped to the
# JSON keys that hold the text content (tried in order).
SOURCE_TEXT_KEYS: dict[str, list[str]] = {
    "social_sentiment": ["title", "text", "headline"],
    "GDELT": ["title", "headline", "article_title"],
    "hf_financial_news": ["title", "headline", "text"],
    "alphavantage_news_sentiment": ["title", "headline", "summary"],
    "pushshift_reddit": ["title", "selftext", "body", "text"],
}

MODEL_NAME = "ProsusAI/finbert"
# Cache dir follows existing model path pattern
MODEL_CACHE_DIR = "/data/grid_v4/grid_repo/grid/models/finbert"


class FinBERTScorer:
    """Batch FinBERT sentiment scorer for GRID raw_series text data.

    Parameters:
        db_engine: SQLAlchemy engine for database operations.
        batch_size: Number of texts to score per GPU/CPU batch.
        device: Force 'cuda' or 'cpu'. Auto-detects if None.
    """

    def __init__(
        self,
        db_engine: Engine,
        batch_size: int = 64,
        device: str | None = None,
    ) -> None:
        self.engine = db_engine
        self.batch_size = batch_size

        # Auto-detect device
        if device is not None:
            self.device = torch.device(device)
        elif torch.cuda.is_available():
            self.device = torch.device("cuda")
            log.info("CUDA available — using GPU: {d}", d=torch.cuda.get_device_name(0))
        else:
            self.device = torch.device("cpu")
            log.info("No CUDA — using CPU")

        self.tokenizer = None
        self.model = None
        self.source_id: int | None = None

    # ── Model loading ─────────────────────────────────────────────────

    def load_model(self) -> None:
        """Download / load ProsusAI/finbert from cache.

        Model and tokenizer are placed on ``self.device``.
        """
        log.info("Loading FinBERT model ({m}) on {d}", m=MODEL_NAME, d=self.device)
        t0 = time.time()

        self.tokenizer = AutoTokenizer.from_pretrained(
            MODEL_NAME, cache_dir=MODEL_CACHE_DIR
        )
        self.model = AutoModelForSequenceClassification.from_pretrained(
            MODEL_NAME, cache_dir=MODEL_CACHE_DIR
        ).to(self.device)
        self.model.eval()

        elapsed = time.time() - t0
        log.info("FinBERT loaded in {t:.1f}s", t=elapsed)

    def _ensure_model(self) -> None:
        """Lazy-load model on first use."""
        if self.model is None or self.tokenizer is None:
            self.load_model()

    # ── Source catalog ────────────────────────────────────────────────

    def _ensure_source_id(self) -> int:
        """Resolve or create the FinBERT source_catalog entry."""
        if self.source_id is not None:
            return self.source_id

        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "FinBERT"},
            ).fetchone()

        if row is not None:
            self.source_id = row[0]
            return self.source_id

        log.info("Auto-creating source_catalog entry for FinBERT")
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    "INSERT INTO source_catalog "
                    "(name, base_url, cost_tier, latency_class, pit_available, "
                    "revision_behavior, trust_score, priority_rank, active) "
                    "VALUES ('FinBERT', 'local://finbert', 'FREE', 'BATCH', "
                    "FALSE, 'NEVER', 'MED', 60, TRUE) "
                    "ON CONFLICT (name) DO NOTHING "
                    "RETURNING id"
                ),
            )
            new_row = result.fetchone()
            if new_row:
                self.source_id = new_row[0]
                return self.source_id

        # Re-fetch on conflict
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "FinBERT"},
            ).fetchone()
        self.source_id = row[0]
        return self.source_id

    # ── Batch scoring ─────────────────────────────────────────────────

    def score_batch(self, texts: list[str]) -> list[dict]:
        """Score a batch of texts with FinBERT.

        Parameters:
            texts: List of text strings (headlines, titles, posts).

        Returns:
            List of dicts with keys: label, score, positive, negative, neutral.
        """
        self._ensure_model()

        # Truncate and clean
        cleaned = [t[:512] if t else "" for t in texts]

        inputs = self.tokenizer(
            cleaned,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt",
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            probs = torch.nn.functional.softmax(outputs.logits, dim=-1)

        # FinBERT labels: positive=0, negative=1, neutral=2
        label_map = {0: "positive", 1: "negative", 2: "neutral"}
        results = []

        for i in range(len(texts)):
            pos = probs[i][0].item()
            neg = probs[i][1].item()
            neu = probs[i][2].item()
            top_idx = probs[i].argmax().item()

            results.append({
                "label": label_map[top_idx],
                "score": probs[i][top_idx].item(),
                "positive": round(pos, 6),
                "negative": round(neg, 6),
                "neutral": round(neu, 6),
            })

        return results

    # ── Source-level scoring ──────────────────────────────────────────

    @staticmethod
    def _extract_text(raw_payload: dict | str | None, text_keys: list[str]) -> str | None:
        """Extract scoreable text from a raw_payload JSON blob.

        Tries each key in ``text_keys`` in order.  Returns the first
        non-empty string found, or None.
        """
        if raw_payload is None:
            return None
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except (json.JSONDecodeError, TypeError):
                return raw_payload if len(raw_payload) > 5 else None

        if not isinstance(raw_payload, dict):
            return None

        for key in text_keys:
            val = raw_payload.get(key)
            if val and isinstance(val, str) and len(val.strip()) > 5:
                return val.strip()
        return None

    def _get_scored_series_ids(self, source_name: str, conn: Any) -> set[str]:
        """Fetch all original series_ids already scored for a source.

        Used for incremental scoring — only score rows not yet processed.
        """
        src_id = self._ensure_source_id()
        rows = conn.execute(
            text(
                "SELECT DISTINCT series_id FROM raw_series "
                "WHERE source_id = :src AND series_id LIKE :prefix "
                "AND pull_status = 'SUCCESS'"
            ),
            {"src": src_id, "prefix": f"finbert.{source_name}.%"},
        ).fetchall()
        # Return the original series_ids (strip finbert. prefix)
        return {r[0] for r in rows}

    def score_source(
        self,
        source_name: str,
        limit: int = 10000,
        rescore: bool = False,
    ) -> dict[str, Any]:
        """Score unscored text rows from raw_series for a source.

        Parameters:
            source_name: Source name matching SOURCE_TEXT_KEYS.
            limit: Max rows to pull for scoring.
            rescore: If True, re-score previously scored rows.

        Returns:
            Summary dict with rows_scored, avg_sentiment, elapsed_s.
        """
        self._ensure_model()
        src_id = self._ensure_source_id()
        text_keys = SOURCE_TEXT_KEYS.get(source_name)

        if text_keys is None:
            log.warning("No text_keys configured for source {s}", s=source_name)
            return {"source": source_name, "rows_scored": 0, "status": "SKIPPED"}

        t0 = time.time()
        log.info("Scoring {s} (limit={l}, rescore={r})", s=source_name, l=limit, r=rescore)

        # Fetch rows from raw_series for this source
        with self.engine.connect() as conn:
            # Get the source_id for the original source
            orig_src = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": source_name},
            ).fetchone()

            if orig_src is None:
                log.warning("Source {s} not found in source_catalog", s=source_name)
                return {"source": source_name, "rows_scored": 0, "status": "SOURCE_NOT_FOUND"}

            orig_source_id = orig_src[0]

            # Build query — exclude already-scored rows unless rescore=True
            if rescore:
                query = text(
                    "SELECT id, series_id, obs_date, raw_payload "
                    "FROM raw_series "
                    "WHERE source_id = :src AND pull_status = 'SUCCESS' "
                    "AND raw_payload IS NOT NULL "
                    "ORDER BY obs_date DESC "
                    "LIMIT :lim"
                )
                params: dict[str, Any] = {"src": orig_source_id, "lim": limit}
            else:
                # Incremental: only score rows whose finbert. counterpart doesn't exist
                query = text(
                    "SELECT r.id, r.series_id, r.obs_date, r.raw_payload "
                    "FROM raw_series r "
                    "WHERE r.source_id = :src AND r.pull_status = 'SUCCESS' "
                    "AND r.raw_payload IS NOT NULL "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM raw_series fb "
                    "  WHERE fb.source_id = :fb_src "
                    "  AND fb.series_id = 'finbert.' || r.series_id "
                    "  AND fb.obs_date = r.obs_date "
                    "  AND fb.pull_status = 'SUCCESS'"
                    ") "
                    "ORDER BY r.obs_date DESC "
                    "LIMIT :lim"
                )
                params = {"src": orig_source_id, "fb_src": src_id, "lim": limit}

            rows = conn.execute(query, params).fetchall()

        if not rows:
            log.info("No unscored rows for {s}", s=source_name)
            return {"source": source_name, "rows_scored": 0, "status": "UP_TO_DATE"}

        log.info("Found {n} rows to score for {s}", n=len(rows), s=source_name)

        # Extract text and build scoring batches
        scoreable: list[tuple[Any, str, date, str]] = []  # (id, series_id, obs_date, text)
        for row in rows:
            row_id, series_id, obs_date, payload = row
            txt = self._extract_text(payload, text_keys)
            if txt:
                scoreable.append((row_id, series_id, obs_date, txt))

        if not scoreable:
            log.info("No extractable text for {s}", s=source_name)
            return {"source": source_name, "rows_scored": 0, "status": "NO_TEXT"}

        # Score in batches
        all_scores: list[dict] = []
        for batch_start in range(0, len(scoreable), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(scoreable))
            batch_texts = [s[3] for s in scoreable[batch_start:batch_end]]
            batch_scores = self.score_batch(batch_texts)
            all_scores.extend(batch_scores)

            if batch_start > 0 and batch_start % (self.batch_size * 10) == 0:
                log.info(
                    "  {s}: scored {n}/{t} rows",
                    s=source_name, n=batch_start, t=len(scoreable),
                )

        # Write results to raw_series
        rows_written = 0
        with self.engine.begin() as conn:
            for i, (row_id, series_id, obs_date, txt) in enumerate(scoreable):
                score_data = all_scores[i]
                compound = score_data["positive"] - score_data["negative"]

                fb_series_id = f"finbert.{series_id}"
                fb_payload = {
                    "label": score_data["label"],
                    "positive": score_data["positive"],
                    "negative": score_data["negative"],
                    "neutral": score_data["neutral"],
                    "original_series_id": series_id,
                    "original_obs_date": obs_date.isoformat() if isinstance(obs_date, date) else str(obs_date),
                    "text_snippet": txt[:200],
                }

                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                        "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                    ),
                    {
                        "sid": fb_series_id,
                        "src": src_id,
                        "od": obs_date,
                        "val": round(compound, 6),
                        "payload": json.dumps(fb_payload),
                    },
                )
                rows_written += 1

        elapsed = time.time() - t0
        avg_sentiment = (
            sum(s["positive"] - s["negative"] for s in all_scores) / len(all_scores)
            if all_scores
            else 0.0
        )

        log.info(
            "{s}: scored {n} rows in {t:.1f}s — avg sentiment {avg:+.4f}",
            s=source_name, n=rows_written, t=elapsed, avg=avg_sentiment,
        )

        return {
            "source": source_name,
            "rows_scored": rows_written,
            "avg_sentiment": round(avg_sentiment, 6),
            "elapsed_s": round(elapsed, 2),
            "status": "SUCCESS",
        }

    # ── Score all configured sources ──────────────────────────────────

    def score_all_sources(self) -> list[dict[str, Any]]:
        """Iterate through all configured sources and score unscored rows.

        Returns:
            List of per-source summary dicts.
        """
        results = []
        for source_name in SOURCE_TEXT_KEYS:
            try:
                result = self.score_source(source_name)
                results.append(result)
            except Exception as exc:
                log.error("FinBERT scoring failed for {s}: {e}", s=source_name, e=str(exc))
                results.append({
                    "source": source_name,
                    "rows_scored": 0,
                    "status": "FAILED",
                    "error": str(exc),
                })
        return results
