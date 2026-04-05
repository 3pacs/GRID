"""
RAG Pipeline — retrieve context, augment prompt, send to LLM.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from rag.retriever import SearchResult, search


def rag_query(
    engine: Engine,
    question: str,
    top_k: int = 5,
    source_type: str | None = None,
    ollama_url: str = "http://localhost:11434",
) -> dict[str, Any]:
    """Full RAG pipeline: retrieve + augment + generate.

    Args:
        engine: SQLAlchemy engine.
        question: User question.
        top_k: Number of context chunks.
        source_type: Optional filter.
        ollama_url: Ollama base URL.

    Returns:
        Dict with answer, sources, and context used.
    """
    from llm.router import get_llm, Tier

    # 1. Retrieve relevant context
    results = search(engine, question, top_k=top_k, source_type=source_type,
                     ollama_url=ollama_url)

    if not results:
        return {
            "answer": None,
            "sources": [],
            "context_chunks": 0,
            "error": "No relevant context found",
        }

    # 2. Build augmented prompt
    context_block = _format_context(results)
    augmented_prompt = (
        f"Use the following context to answer the question. "
        f"Cite sources when possible.\n\n"
        f"--- CONTEXT ---\n{context_block}\n--- END CONTEXT ---\n\n"
        f"Question: {question}"
    )

    # 3. Send to LLM
    client = get_llm(Tier.REASON)
    answer = client.generate(
        prompt=augmented_prompt,
        system="You are GRID's intelligence analyst. Answer based on the provided context. "
               "If the context is insufficient, say so.",
        temperature=0.2,
        num_predict=2048,
    )

    sources = [
        {"source_id": r.source_id, "source_type": r.source_type, "similarity": r.similarity}
        for r in results
    ]

    return {
        "answer": answer,
        "sources": sources,
        "context_chunks": len(results),
    }


def _format_context(results: list[SearchResult]) -> str:
    """Format search results into a context block for the LLM."""
    parts: list[str] = []
    for i, r in enumerate(results, 1):
        parts.append(
            f"[{i}] ({r.source_type}/{r.source_id}, sim={r.similarity:.3f})\n{r.chunk_text}"
        )
    return "\n\n".join(parts)
