"""
GRID RAG (Retrieval-Augmented Generation) Intelligence System.

Core retrieval layer for querying the full GRID intelligence corpus:
analytical snapshots, actors, ICIJ entities, oracle predictions, and more.

Uses pgvector for semantic similarity search with metadata-filtered reranking.
Falls back gracefully to TF-IDF when sentence-transformers unavailable,
and to text search when pgvector extension is not installed.

Usage:
    # Index all intelligence data
    python -m intelligence.rag index

    # Semantic search
    python -m intelligence.rag search "offshore connections to Pfizer board"

    # Ask a question (retrieves context, sends to local LLM)
    python -m intelligence.rag ask "Who has offshore connections to pharma?"
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import numpy as np
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Ensure grid root is on sys.path
# ---------------------------------------------------------------------------
_GRID_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

# ---------------------------------------------------------------------------
# Embedding backend selection (graceful degradation)
# Same pattern as subnet/semantic_scorer.py
# ---------------------------------------------------------------------------
_EMBEDDING_BACKEND: str = "word_freq"
_ST_MODEL = None
_TFIDF_VECTORIZER = None
_EMBED_DIM = 384

try:
    from sentence_transformers import SentenceTransformer

    _ST_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    _EMBEDDING_BACKEND = "sentence_transformers"
    log.info("RAG using sentence-transformers backend (384-dim)")
except ImportError:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer

        _TFIDF_VECTORIZER = TfidfVectorizer(
            max_features=_EMBED_DIM,
            stop_words="english",
            sublinear_tf=True,
        )
        _EMBEDDING_BACKEND = "tfidf"
        log.info("RAG using TF-IDF backend (sklearn)")
    except ImportError:
        log.warning(
            "RAG falling back to word-frequency vectors. "
            "Install sentence-transformers or scikit-learn for better quality."
        )


# ---------------------------------------------------------------------------
# Trust weights for confidence-based reranking
# ---------------------------------------------------------------------------
TRUST_WEIGHTS = {
    "confirmed": 1.0,
    "derived": 0.8,
    "estimated": 0.6,
    "rumored": 0.4,
    "inferred": 0.3,
}

# Max chunk size in characters (~512 tokens * 4 chars/token)
MAX_CHUNK_CHARS = 2048


# ---------------------------------------------------------------------------
# Embedding helpers
# ---------------------------------------------------------------------------
def _embed_text(text_input: str) -> np.ndarray:
    """Generate a 384-dim embedding vector for the given text.

    Uses best available backend: sentence-transformers > TF-IDF > word-freq.
    Returns L2-normalized float32 vector.
    """
    if not text_input or not text_input.strip():
        return np.zeros(_EMBED_DIM, dtype=np.float32)

    if _EMBEDDING_BACKEND == "sentence_transformers":
        vec = _ST_MODEL.encode(text_input, show_progress_bar=False).astype(np.float32)
    elif _EMBEDDING_BACKEND == "tfidf":
        vec = _embed_tfidf(text_input)
    else:
        vec = _embed_word_freq(text_input)

    # L2-normalize
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec


def _embed_batch(texts: list[str]) -> np.ndarray:
    """Embed a batch of texts. Returns (N, 384) array."""
    if _EMBEDDING_BACKEND == "sentence_transformers":
        vecs = _ST_MODEL.encode(texts, show_progress_bar=False, batch_size=64).astype(
            np.float32
        )
        # L2-normalize each row
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return vecs / norms
    else:
        return np.array([_embed_text(t) for t in texts], dtype=np.float32)


def _embed_tfidf(text_input: str) -> np.ndarray:
    """Embed using TF-IDF vectorizer."""
    global _TFIDF_VECTORIZER
    try:
        vec = _TFIDF_VECTORIZER.transform([text_input]).toarray()[0]
    except Exception:
        return _embed_word_freq(text_input)
    # Pad or truncate to _EMBED_DIM
    if len(vec) < _EMBED_DIM:
        vec = np.pad(vec, (0, _EMBED_DIM - len(vec)))
    return vec[:_EMBED_DIM].astype(np.float32)


def _embed_word_freq(text_input: str) -> np.ndarray:
    """Simple word-frequency embedding (zero-dependency fallback)."""
    from collections import Counter

    words = text_input.lower().split()
    if not words:
        return np.zeros(_EMBED_DIM, dtype=np.float32)
    counts = Counter(words)
    vec = np.zeros(_EMBED_DIM, dtype=np.float32)
    for word, count in counts.items():
        idx = hash(word) % _EMBED_DIM
        vec[idx] += count
    return vec


def _chunk_text(text_input: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    """Split text into chunks of max_chars, breaking on sentence boundaries."""
    if not text_input or len(text_input) <= max_chars:
        return [text_input] if text_input else []

    chunks = []
    remaining = text_input
    while remaining:
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break
        # Find last sentence boundary within max_chars
        boundary = remaining[:max_chars].rfind(". ")
        if boundary < max_chars // 4:
            # No good sentence boundary — break on space
            boundary = remaining[:max_chars].rfind(" ")
        if boundary < 1:
            boundary = max_chars
        else:
            boundary += 1  # Include the period/space
        chunks.append(remaining[:boundary].strip())
        remaining = remaining[boundary:].strip()
    return [c for c in chunks if c]


def _vec_to_pg_literal(vec: np.ndarray) -> str:
    """Convert numpy vector to pgvector string literal '[0.1,0.2,...]'."""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


# ===================================================================
# RAGIndexer — indexes intelligence data into pgvector
# ===================================================================
class RAGIndexer:
    """Indexes GRID intelligence data into pgvector for semantic retrieval.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine for PostgreSQL with pgvector.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._pgvector_available: bool | None = None
        self._tfidf_fitted = False

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------

    def ensure_tables(self) -> bool:
        """Create intelligence_embeddings table with pgvector.

        Attempts CREATE EXTENSION vector first (graceful if already exists
        or if user lacks permissions). Returns True if pgvector is available.
        """
        # Try to enable pgvector extension
        with self.engine.begin() as conn:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                log.info("pgvector extension enabled")
            except Exception as exc:
                log.warning(
                    "Could not create pgvector extension (may already exist "
                    "or require superuser): {e}",
                    e=str(exc)[:200],
                )

        # Check if pgvector is actually available
        self._pgvector_available = self._check_pgvector()

        if self._pgvector_available:
            ddl = text("""
                CREATE TABLE IF NOT EXISTS intelligence_embeddings (
                    id          BIGSERIAL PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id   TEXT NOT NULL,
                    chunk_text  TEXT NOT NULL,
                    embedding   vector(384) NOT NULL,
                    metadata    JSONB DEFAULT '{}',
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        else:
            # Fallback: store embedding as JSONB array (no vector search)
            log.warning(
                "pgvector not available — creating fallback table without vector type"
            )
            ddl = text("""
                CREATE TABLE IF NOT EXISTS intelligence_embeddings (
                    id          BIGSERIAL PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id   TEXT NOT NULL,
                    chunk_text  TEXT NOT NULL,
                    embedding   JSONB NOT NULL,
                    metadata    JSONB DEFAULT '{}',
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)

        with self.engine.begin() as conn:
            conn.execute(ddl)

            if self._pgvector_available:
                # HNSW index for fast ANN search
                try:
                    conn.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_embeddings_vector
                            ON intelligence_embeddings
                            USING hnsw (embedding vector_cosine_ops)
                            WITH (m = 16, ef_construction = 64)
                    """))
                except Exception as exc:
                    log.warning("Could not create HNSW index: {e}", e=str(exc)[:200])

            # Metadata indexes
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_embeddings_source
                    ON intelligence_embeddings (source_type, source_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_embeddings_metadata
                    ON intelligence_embeddings USING gin (metadata)
            """))
            # Full-text search index for fallback
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_embeddings_text_search
                    ON intelligence_embeddings
                    USING gin (to_tsvector('english', chunk_text))
            """))

        log.info(
            "intelligence_embeddings table ready (pgvector={pv})",
            pv=self._pgvector_available,
        )
        return self._pgvector_available

    def _check_pgvector(self) -> bool:
        """Check if pgvector extension is installed and functional."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
                    )
                )
                return result.fetchone() is not None
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Batch insert helper
    # ------------------------------------------------------------------

    def _insert_embeddings(
        self,
        rows: list[dict],
        batch_size: int = 1000,
    ) -> int:
        """Batch insert embedding rows into intelligence_embeddings.

        Each row dict must have: source_type, source_id, chunk_text,
        embedding (np.ndarray), metadata (dict).

        Returns count of inserted rows.
        """
        if not rows:
            return 0

        inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = []
            for row in rows[i : i + batch_size]:
                batch.append({
                    "_vec": row["embedding"],
                    "source_type": row["source_type"],
                    "source_id": row["source_id"],
                    "chunk_text": row["chunk_text"],
                    "metadata": row.get("metadata", {}),
                })

            try:
                with self.engine.begin() as conn:
                    for row_params in batch:
                        vec = row_params.pop("_vec")
                        if self._pgvector_available:
                            vec_literal = _vec_to_pg_literal(vec)
                            conn.execute(
                                text(
                                    "INSERT INTO intelligence_embeddings "
                                    "(source_type, source_id, chunk_text, embedding, metadata) "
                                    f"VALUES (:st, :si, :ct, '{vec_literal}'::vector, :md::jsonb)"
                                ),
                                {
                                    "st": row_params["source_type"],
                                    "si": str(row_params["source_id"]),
                                    "ct": row_params["chunk_text"],
                                    "md": json.dumps(row_params.get("metadata", {})),
                                },
                            )
                        else:
                            conn.execute(
                                text(
                                    "INSERT INTO intelligence_embeddings "
                                    "(source_type, source_id, chunk_text, embedding, metadata) "
                                    "VALUES (:st, :si, :ct, :emb::jsonb, :md::jsonb)"
                                ),
                                {
                                    "st": row_params["source_type"],
                                    "si": str(row_params["source_id"]),
                                    "ct": row_params["chunk_text"],
                                    "emb": json.dumps(vec.tolist()),
                                    "md": json.dumps(row_params.get("metadata", {})),
                                },
                            )
                inserted += len(batch)
            except Exception as exc:
                log.error(
                    "Failed to insert batch at offset {i}: {e}",
                    i=i,
                    e=str(exc)[:300],
                )

        return inserted

    # ------------------------------------------------------------------
    # Indexers
    # ------------------------------------------------------------------

    def index_snapshots(self, batch_size: int = 100) -> int:
        """Index analytical_snapshots — completed research results.

        Each snapshot's payload JSONB is serialized to text, chunked,
        and embedded. Metadata includes category, subcategory, date.
        """
        log.info("Indexing analytical_snapshots...")

        # Clear existing snapshot embeddings to avoid duplicates
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM intelligence_embeddings "
                    "WHERE source_type = 'snapshot'"
                )
            )

        total = 0
        offset = 0

        while True:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, category, subcategory, snapshot_date, "
                        "payload::text AS payload_text "
                        "FROM analytical_snapshots "
                        "ORDER BY id "
                        "LIMIT :limit OFFSET :offset"
                    ).bindparams(limit=batch_size, offset=offset)
                ).fetchall()

            if not rows:
                break

            embed_rows = []
            texts = []
            row_data = []

            for row in rows:
                payload_text = row.payload_text
                if not payload_text or payload_text.strip() in ("", "null", "{}"):
                    continue

                # Build readable text from payload
                content = f"[{row.category}]"
                if row.subcategory:
                    content += f" [{row.subcategory}]"
                content += f" {payload_text}"

                chunks = _chunk_text(content)
                for chunk_idx, chunk in enumerate(chunks):
                    texts.append(chunk)
                    row_data.append({
                        "source_id": f"{row.id}:{chunk_idx}",
                        "chunk_text": chunk,
                        "metadata": {
                            "category": row.category,
                            "subcategory": row.subcategory,
                            "snapshot_date": str(row.snapshot_date),
                            "snapshot_id": row.id,
                        },
                    })

            if texts:
                embeddings = _embed_batch(texts)
                for idx, rd in enumerate(row_data):
                    embed_rows.append({
                        "source_type": "snapshot",
                        "source_id": rd["source_id"],
                        "chunk_text": rd["chunk_text"],
                        "embedding": embeddings[idx],
                        "metadata": rd["metadata"],
                    })

                count = self._insert_embeddings(embed_rows)
                total += count

            offset += batch_size
            log.debug("Indexed {n} snapshot chunks so far", n=total)

        log.info("Indexed {n} snapshot chunks total", n=total)
        return total

    def index_actors(self, batch_size: int = 500) -> int:
        """Index actors — name + title + category + tier.

        Prioritizes higher-tier actors (god_tier, tier_1 first).
        Metadata includes tier, category, influence_score.
        """
        log.info("Indexing actors...")

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM intelligence_embeddings "
                    "WHERE source_type = 'actor'"
                )
            )

        total = 0
        offset = 0

        while True:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, name, tier, category, title, "
                        "influence_score, trust_score, motivation_model "
                        "FROM actors "
                        "ORDER BY "
                        "  CASE tier "
                        "    WHEN 'god_tier' THEN 1 "
                        "    WHEN 'tier_1' THEN 2 "
                        "    WHEN 'tier_2' THEN 3 "
                        "    WHEN 'tier_3' THEN 4 "
                        "    ELSE 5 "
                        "  END, "
                        "  influence_score DESC NULLS LAST "
                        "LIMIT :limit OFFSET :offset"
                    ).bindparams(limit=batch_size, offset=offset)
                ).fetchall()

            if not rows:
                break

            texts = []
            row_data = []

            for row in rows:
                parts = [row.name]
                if row.title:
                    parts.append(row.title)
                parts.append(f"Category: {row.category}")
                parts.append(f"Tier: {row.tier}")
                if row.motivation_model and row.motivation_model != "unknown":
                    parts.append(f"Motivation: {row.motivation_model}")

                actor_text = ". ".join(parts)
                if not actor_text.strip():
                    continue

                texts.append(actor_text)
                row_data.append({
                    "source_id": row.id,
                    "chunk_text": actor_text,
                    "metadata": {
                        "tier": row.tier,
                        "category": row.category,
                        "influence_score": (
                            float(row.influence_score) if row.influence_score else None
                        ),
                        "trust_score": (
                            float(row.trust_score) if row.trust_score else None
                        ),
                        "actor_name": row.name,
                    },
                })

            if texts:
                embeddings = _embed_batch(texts)
                embed_rows = []
                for idx, rd in enumerate(row_data):
                    embed_rows.append({
                        "source_type": "actor",
                        "source_id": rd["source_id"],
                        "chunk_text": rd["chunk_text"],
                        "embedding": embeddings[idx],
                        "metadata": rd["metadata"],
                    })
                count = self._insert_embeddings(embed_rows)
                total += count

            offset += batch_size
            if offset % 5000 == 0:
                log.debug("Indexed {n} actor chunks so far", n=total)

        log.info("Indexed {n} actor chunks total", n=total)
        return total

    def index_predictions(self) -> int:
        """Index oracle predictions with outcomes.

        Chunk: "{ticker} {direction} conf={confidence}. Model: {model}. Verdict: {verdict}."
        """
        log.info("Indexing oracle_predictions...")

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM intelligence_embeddings "
                    "WHERE source_type = 'prediction'"
                )
            )

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, ticker, direction, confidence, model_name, "
                    "model_version, expected_move_pct, expiry, "
                    "prediction_type, signal_strength, coherence, "
                    "created_at "
                    "FROM oracle_predictions "
                    "ORDER BY created_at DESC"
                )
            ).fetchall()

        if not rows:
            log.info("No oracle_predictions to index")
            return 0

        texts = []
        row_data = []

        for row in rows:
            parts = [
                f"{row.ticker} {row.direction}",
                f"confidence={row.confidence}",
            ]
            if row.model_name:
                parts.append(f"model={row.model_name}")
            if row.expected_move_pct:
                parts.append(f"expected_move={row.expected_move_pct}%")
            if row.prediction_type:
                parts.append(f"type={row.prediction_type}")

            pred_text = ". ".join(parts)
            if not pred_text.strip():
                continue

            texts.append(pred_text)
            row_data.append({
                "source_id": str(row.id),
                "chunk_text": pred_text,
                "metadata": {
                    "ticker": row.ticker,
                    "direction": row.direction,
                    "confidence": float(row.confidence) if row.confidence else None,
                    "model_name": row.model_name,
                    "created_at": str(row.created_at) if row.created_at else None,
                    "expiry": str(row.expiry) if row.expiry else None,
                },
            })

        if not texts:
            return 0

        # Batch embed
        embed_rows = []
        batch_sz = 1000
        for i in range(0, len(texts), batch_sz):
            batch_texts = texts[i : i + batch_sz]
            embeddings = _embed_batch(batch_texts)
            for j, rd in enumerate(row_data[i : i + batch_sz]):
                embed_rows.append({
                    "source_type": "prediction",
                    "source_id": rd["source_id"],
                    "chunk_text": rd["chunk_text"],
                    "embedding": embeddings[j],
                    "metadata": rd["metadata"],
                })

        total = self._insert_embeddings(embed_rows)
        log.info("Indexed {n} prediction chunks", n=total)
        return total

    def index_all(self) -> dict[str, int]:
        """Run all indexers and report counts."""
        self.ensure_tables()

        counts = {}
        t0 = time.time()

        for name, method in [
            ("snapshots", self.index_snapshots),
            ("actors", self.index_actors),
            ("predictions", self.index_predictions),
        ]:
            try:
                counts[name] = method()
            except Exception as exc:
                log.error("Failed to index {name}: {e}", name=name, e=str(exc)[:300])
                counts[name] = -1

        elapsed = time.time() - t0
        total = sum(v for v in counts.values() if v > 0)
        log.info(
            "RAG index_all complete — {total} chunks in {elapsed:.1f}s: {counts}",
            total=total,
            elapsed=elapsed,
            counts=counts,
        )
        return counts


# ===================================================================
# RAGRetriever — semantic search over intelligence embeddings
# ===================================================================
class RAGRetriever:
    """Semantic search over GRID intelligence embeddings.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine for PostgreSQL.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._pgvector_available: bool | None = None

    def _check_pgvector(self) -> bool:
        """Check if pgvector extension is available."""
        if self._pgvector_available is not None:
            return self._pgvector_available
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                )
                self._pgvector_available = result.fetchone() is not None
        except Exception:
            self._pgvector_available = False
        return self._pgvector_available

    def search(
        self,
        query: str,
        top_k: int = 10,
        source_types: list[str] | None = None,
        min_confidence: float | None = None,
    ) -> list[dict]:
        """Embed query and perform pgvector cosine similarity search.

        Falls back to PostgreSQL full-text search if pgvector unavailable.

        Parameters
        ----------
        query : str
            Natural language query.
        top_k : int
            Number of results to return.
        source_types : list[str] | None
            Filter by source type (e.g. ['snapshot', 'actor']).
        min_confidence : float | None
            Minimum confidence threshold from metadata.

        Returns
        -------
        list[dict]
            Results with keys: text, source_type, source_id, similarity,
            confidence, metadata.
        """
        if not query or not query.strip():
            return []

        if self._check_pgvector():
            return self._vector_search(query, top_k, source_types, min_confidence)
        else:
            return self._text_search(query, top_k, source_types, min_confidence)

    def _vector_search(
        self,
        query: str,
        top_k: int,
        source_types: list[str] | None,
        min_confidence: float | None,
    ) -> list[dict]:
        """pgvector cosine similarity search."""
        query_vec = _embed_text(query)
        vec_literal = _vec_to_pg_literal(query_vec)

        # Build WHERE clauses
        where_parts = []
        params: dict[str, Any] = {"top_k": top_k}

        if source_types:
            where_parts.append("source_type = ANY(:source_types)")
            params["source_types"] = source_types

        if min_confidence is not None:
            where_parts.append(
                "(metadata->>'confidence')::float >= :min_confidence"
            )
            params["min_confidence"] = min_confidence

        where_clause = ""
        if where_parts:
            where_clause = "WHERE " + " AND ".join(where_parts)

        sql = (
            f"SELECT id, source_type, source_id, chunk_text, metadata, "
            f"1 - (embedding <=> '{vec_literal}'::vector) AS similarity "
            f"FROM intelligence_embeddings "
            f"{where_clause} "
            f"ORDER BY embedding <=> '{vec_literal}'::vector "
            f"LIMIT :top_k"
        )

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql).bindparams(**params)).fetchall()

        results = []
        for row in rows:
            meta = row.metadata if isinstance(row.metadata, dict) else {}
            results.append({
                "text": row.chunk_text,
                "source_type": row.source_type,
                "source_id": row.source_id,
                "similarity": float(row.similarity),
                "confidence": meta.get("confidence"),
                "metadata": meta,
            })

        return results

    def _text_search(
        self,
        query: str,
        top_k: int,
        source_types: list[str] | None,
        min_confidence: float | None,
    ) -> list[dict]:
        """PostgreSQL full-text search fallback when pgvector unavailable."""
        where_parts = [
            "to_tsvector('english', chunk_text) @@ plainto_tsquery('english', :query)"
        ]
        params: dict[str, Any] = {"query": query, "top_k": top_k}

        if source_types:
            where_parts.append("source_type = ANY(:source_types)")
            params["source_types"] = source_types

        if min_confidence is not None:
            where_parts.append(
                "(metadata->>'confidence')::float >= :min_confidence"
            )
            params["min_confidence"] = min_confidence

        where_clause = "WHERE " + " AND ".join(where_parts)

        sql = (
            f"SELECT id, source_type, source_id, chunk_text, metadata, "
            f"ts_rank(to_tsvector('english', chunk_text), "
            f"plainto_tsquery('english', :query)) AS similarity "
            f"FROM intelligence_embeddings "
            f"{where_clause} "
            f"ORDER BY similarity DESC "
            f"LIMIT :top_k"
        )

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql).bindparams(**params)).fetchall()

        results = []
        for row in rows:
            meta = row.metadata if isinstance(row.metadata, dict) else {}
            results.append({
                "text": row.chunk_text,
                "source_type": row.source_type,
                "source_id": row.source_id,
                "similarity": float(row.similarity),
                "confidence": meta.get("confidence"),
                "metadata": meta,
            })

        return results

    def search_and_rerank(
        self,
        query: str,
        top_k: int = 10,
        source_types: list[str] | None = None,
        min_confidence: float | None = None,
    ) -> list[dict]:
        """Search then rerank by trust weight * similarity.

        Fetches 3x top_k candidates, reranks by:
            score = similarity * trust_weight(confidence_label)

        Trust weights:
            confirmed=1.0, derived=0.8, estimated=0.6,
            rumored=0.4, inferred=0.3

        Returns top_k results sorted by final score.
        """
        # Fetch 3x candidates for reranking headroom
        candidates = self.search(
            query,
            top_k=top_k * 3,
            source_types=source_types,
            min_confidence=min_confidence,
        )

        for result in candidates:
            confidence_label = (
                result.get("metadata", {}).get("confidence_label", "")
                or ""
            )
            trust = TRUST_WEIGHTS.get(confidence_label.lower(), 0.5)
            result["trust_weight"] = trust
            result["rerank_score"] = result["similarity"] * trust

        # Sort by rerank score descending
        candidates.sort(key=lambda r: r["rerank_score"], reverse=True)

        return candidates[:top_k]


# ===================================================================
# ask() — RAG question-answering via local LLM
# ===================================================================
def ask(
    engine: Engine,
    query: str,
    top_k: int = 5,
    llm_url: str = "http://localhost:8080/completion",
    timeout: int = 60,
) -> dict:
    """Retrieve context from intelligence corpus and answer via local LLM.

    1. Retrieve top-K chunks via RAGRetriever.search_and_rerank
    2. Build context string with citations
    3. Send to local Qwen (llama.cpp server) with context
    4. Return {answer, citations, query_time_ms}

    Parameters
    ----------
    engine : Engine
        Database engine.
    query : str
        Natural language question.
    top_k : int
        Number of context chunks to retrieve.
    llm_url : str
        URL of local LLM completion endpoint.
    timeout : int
        LLM request timeout in seconds.

    Returns
    -------
    dict
        {answer: str, citations: list[dict], query_time_ms: float}
    """
    t0 = time.time()

    retriever = RAGRetriever(engine)
    results = retriever.search_and_rerank(query, top_k=top_k)

    # Build context with numbered citations
    context_parts = []
    citations = []
    for i, r in enumerate(results, 1):
        source_label = f"[{i}] ({r['source_type']}/{r['source_id']})"
        sim_pct = f"{r['similarity'] * 100:.0f}%"

        context_parts.append(
            f"{source_label} [relevance: {sim_pct}]\n{r['text']}"
        )
        citations.append({
            "index": i,
            "source_type": r["source_type"],
            "source_id": r["source_id"],
            "text": r["text"][:500],
            "similarity": r["similarity"],
            "confidence": r.get("confidence"),
        })

    context_str = "\n\n".join(context_parts)

    # Build prompt
    prompt = (
        "You are GRID, a financial intelligence system. "
        "Answer the question using ONLY the provided context. "
        "Cite sources using [N] notation. "
        "If the context is insufficient, say so explicitly. "
        "Label confidence: confirmed/derived/estimated/rumored/inferred.\n\n"
        f"CONTEXT:\n{context_str}\n\n"
        f"QUESTION: {query}\n\n"
        "ANSWER:"
    )

    # Call local LLM
    answer = ""
    try:
        resp = requests.post(
            llm_url,
            json={
                "prompt": prompt,
                "n_predict": 1024,
                "temperature": 0.3,
                "stop": ["\n\nQUESTION:", "\n\nCONTEXT:"],
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("content", data.get("text", "")).strip()
    except requests.exceptions.ConnectionError:
        answer = (
            "[LLM offline] Could not reach local Qwen at "
            f"{llm_url}. Returning raw context.\n\n"
            f"Top {len(results)} results for: {query}\n\n"
            + "\n\n".join(
                f"[{c['index']}] {c['source_type']}: {c['text'][:300]}"
                for c in citations
            )
        )
    except Exception as exc:
        answer = f"[LLM error] {str(exc)[:300]}"

    elapsed_ms = (time.time() - t0) * 1000

    return {
        "answer": answer,
        "citations": citations,
        "query_time_ms": round(elapsed_ms, 1),
    }


# ===================================================================
# CLI
# ===================================================================
def main():
    """CLI entrypoint: index, search, ask."""
    import argparse

    parser = argparse.ArgumentParser(
        description="GRID RAG Intelligence System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m intelligence.rag index\n"
            '  python -m intelligence.rag search "offshore pharma connections"\n'
            '  python -m intelligence.rag ask "Who controls energy flows in OPEC?"\n'
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # index
    idx_parser = sub.add_parser("index", help="Index all intelligence data")
    idx_parser.add_argument(
        "--only",
        choices=["snapshots", "actors", "predictions"],
        help="Index only a specific source type",
    )

    # search
    search_parser = sub.add_parser("search", help="Semantic search")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("-k", "--top-k", type=int, default=10)
    search_parser.add_argument(
        "-t",
        "--types",
        nargs="+",
        help="Filter by source types",
    )
    search_parser.add_argument(
        "--rerank", action="store_true", help="Apply trust-weighted reranking"
    )

    # ask
    ask_parser = sub.add_parser("ask", help="Ask a question (RAG + LLM)")
    ask_parser.add_argument("question", help="Question to answer")
    ask_parser.add_argument("-k", "--top-k", type=int, default=5)
    ask_parser.add_argument(
        "--llm-url",
        default="http://localhost:8080/completion",
        help="LLM endpoint URL",
    )

    args = parser.parse_args()

    # Get engine
    from db import get_engine

    engine = get_engine()

    if args.command == "index":
        indexer = RAGIndexer(engine)
        if args.only:
            indexer.ensure_tables()
            method = getattr(indexer, f"index_{args.only}")
            count = method()
            print(f"Indexed {count} {args.only} chunks")
        else:
            counts = indexer.index_all()
            total = sum(v for v in counts.values() if v > 0)
            print(f"Indexed {total} total chunks:")
            for name, count in counts.items():
                status = f"{count}" if count >= 0 else "FAILED"
                print(f"  {name}: {status}")

    elif args.command == "search":
        retriever = RAGRetriever(engine)
        source_types = args.types if args.types else None

        if args.rerank:
            results = retriever.search_and_rerank(
                args.query, top_k=args.top_k, source_types=source_types
            )
        else:
            results = retriever.search(
                args.query, top_k=args.top_k, source_types=source_types
            )

        if not results:
            print("No results found.")
            return

        print(f"\nTop {len(results)} results for: {args.query}\n")
        for i, r in enumerate(results, 1):
            sim_pct = f"{r['similarity'] * 100:.1f}%"
            rerank = (
                f" [rerank={r['rerank_score']:.3f}]"
                if "rerank_score" in r
                else ""
            )
            print(f"[{i}] {r['source_type']}/{r['source_id']} "
                  f"sim={sim_pct}{rerank}")
            print(f"    {r['text'][:200]}")
            print()

    elif args.command == "ask":
        result = ask(engine, args.question, top_k=args.top_k, llm_url=args.llm_url)
        print(f"\n--- Answer ({result['query_time_ms']:.0f}ms) ---\n")
        print(result["answer"])
        print(f"\n--- Citations ({len(result['citations'])}) ---")
        for c in result["citations"]:
            print(
                f"  [{c['index']}] {c['source_type']}/{c['source_id']} "
                f"sim={c['similarity']:.2f}"
            )


if __name__ == "__main__":
    main()
