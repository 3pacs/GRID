"""
RAG Retriever — semantic search via pgvector cosine similarity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class SearchResult:
    """A single retrieval result."""
    chunk_text: str
    source_type: str
    source_id: str
    similarity: float
    metadata: str


def _embed_query(query: str, base_url: str = "http://localhost:11434") -> list[float]:
    """Embed a single query string."""
    resp = requests.post(
        f"{base_url}/api/embed",
        json={"model": "nomic-embed-text", "input": query},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"][0]


def search(
    engine: Engine,
    query: str,
    top_k: int = 5,
    source_type: str | None = None,
    min_similarity: float = 0.3,
    ollama_url: str = "http://localhost:11434",
) -> list[SearchResult]:
    """Semantic search over the embeddings table.

    Args:
        engine: SQLAlchemy engine.
        query: Natural language query.
        top_k: Number of results to return.
        source_type: Optional filter by source type.
        min_similarity: Minimum cosine similarity threshold.
        ollama_url: Ollama base URL.

    Returns:
        List of SearchResult ordered by similarity (descending).
    """
    try:
        query_embedding = _embed_query(query, base_url=ollama_url)
    except Exception as exc:
        log.error("Query embedding failed: {e}", e=str(exc))
        return []

    emb_str = str(query_embedding)

    if source_type:
        sql = (
            "SELECT chunk_text, source_type, source_id, "
            "1 - (embedding <=> :emb::vector) AS similarity, metadata "
            "FROM embeddings "
            "WHERE source_type = :st "
            "AND 1 - (embedding <=> :emb::vector) >= :min_sim "
            "ORDER BY embedding <=> :emb::vector "
            "LIMIT :k"
        )
        params: dict[str, Any] = {
            "emb": emb_str, "st": source_type, "min_sim": min_similarity, "k": top_k,
        }
    else:
        sql = (
            "SELECT chunk_text, source_type, source_id, "
            "1 - (embedding <=> :emb::vector) AS similarity, metadata "
            "FROM embeddings "
            "WHERE 1 - (embedding <=> :emb::vector) >= :min_sim "
            "ORDER BY embedding <=> :emb::vector "
            "LIMIT :k"
        )
        params = {"emb": emb_str, "min_sim": min_similarity, "k": top_k}

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    results = [
        SearchResult(
            chunk_text=row[0],
            source_type=row[1],
            source_id=row[2],
            similarity=float(row[3]),
            metadata=row[4],
        )
        for row in rows
    ]

    log.debug("RAG search: {q!r} → {n} results (top sim={s:.3f})",
              q=query[:50], n=len(results),
              s=results[0].similarity if results else 0.0)
    return results
