"""
RAG Indexer — chunks documents, generates embeddings, stores in pgvector.

Uses Ollama nomic-embed-text (768 dims) for embeddings.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from rag.chunker import Chunk, chunk_markdown


def _embed_texts(texts: list[str], base_url: str = "http://localhost:11434") -> list[list[float]]:
    """Generate embeddings via Ollama nomic-embed-text.

    Args:
        texts: Strings to embed.
        base_url: Ollama API URL.

    Returns:
        List of embedding vectors (768-dim each).
    """
    embeddings: list[list[float]] = []
    for t in texts:
        resp = requests.post(
            f"{base_url}/api/embed",
            json={"model": "nomic-embed-text", "input": t},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        embeddings.append(data["embeddings"][0])
    return embeddings


def _batch_embed(
    texts: list[str],
    batch_size: int = 32,
    base_url: str = "http://localhost:11434",
) -> list[list[float]]:
    """Embed texts in batches."""
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        all_embeddings.extend(_embed_texts(batch, base_url))
    return all_embeddings


def index_document(
    engine: Engine,
    source_type: str,
    source_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
    max_tokens: int = 512,
    ollama_url: str = "http://localhost:11434",
) -> int:
    """Chunk, embed, and store a document in the embeddings table.

    Args:
        engine: SQLAlchemy engine.
        source_type: One of: knowledge, actor, briefing, filing, news.
        source_id: Unique identifier for the source document.
        content: Full document text.
        metadata: Optional JSON metadata.
        max_tokens: Max tokens per chunk.
        ollama_url: Ollama base URL.

    Returns:
        Number of chunks indexed.
    """
    chunks = chunk_markdown(content, source_id, max_tokens=max_tokens)
    if not chunks:
        log.warning("No chunks produced for {s}/{sid}", s=source_type, sid=source_id)
        return 0

    texts = [c.text for c in chunks]

    try:
        embeddings = _batch_embed(texts, base_url=ollama_url)
    except Exception as exc:
        log.error("Embedding failed for {sid}: {e}", sid=source_id, e=str(exc))
        return 0

    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        # Clear existing chunks for this source
        conn.execute(
            text("DELETE FROM embeddings WHERE source_type = :st AND source_id = :sid"),
            {"st": source_type, "sid": source_id},
        )

        for chunk, embedding in zip(chunks, embeddings):
            conn.execute(
                text(
                    "INSERT INTO embeddings "
                    "(source_type, source_id, chunk_text, embedding, metadata, created_at) "
                    "VALUES (:st, :sid, :txt, :emb, :meta, :ts)"
                ),
                {
                    "st": source_type,
                    "sid": f"{source_id}#{chunk.index}",
                    "txt": chunk.text,
                    "emb": str(embedding),
                    "meta": str(metadata or {}),
                    "ts": now,
                },
            )

    log.info("Indexed {n} chunks for {st}/{sid}", n=len(chunks), st=source_type, sid=source_id)
    return len(chunks)


def index_knowledge_base(engine: Engine, ollama_url: str = "http://localhost:11434") -> int:
    """Index all knowledge .md files into pgvector.

    Returns:
        Total chunks indexed.
    """
    from knowledge.loader import KNOWLEDGE_DIR
    from pathlib import Path

    knowledge_dir = Path(KNOWLEDGE_DIR) if isinstance(KNOWLEDGE_DIR, str) else KNOWLEDGE_DIR
    if not knowledge_dir.exists():
        log.warning("Knowledge directory not found: {d}", d=knowledge_dir)
        return 0

    total = 0
    for md_file in sorted(knowledge_dir.glob("*.md")):
        content = md_file.read_text(encoding="utf-8")
        n = index_document(
            engine, "knowledge", md_file.stem, content, ollama_url=ollama_url,
        )
        total += n

    log.info("Knowledge base indexed: {t} total chunks", t=total)
    return total
