"""
GRID Knowledge Selector — TF-IDF + orthogonality-based doc selection.

Instead of blindly injecting all requested knowledge docs (which can blow
past the LLM context window), this module scores each doc against the
user prompt and selects a compact, non-redundant subset that fits within
a token budget.

Algorithm:
  1. TF-IDF: score each candidate doc by term overlap with the prompt
  2. Orthogonality: among scored docs, greedily pick the next-best doc
     that is least similar to already-selected docs (cosine similarity
     on term vectors)
  3. Budget: stop when adding the next doc would exceed the char budget

No external dependencies — uses pure Python counters for TF-IDF.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any

from loguru import logger as log


# ---------------------------------------------------------------------------
# Tokenisation (lightweight, no nltk needed)
# ---------------------------------------------------------------------------
_STOP_WORDS = frozenset({
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "this", "that",
    "these", "those", "it", "its", "not", "no", "so", "if", "as", "we",
    "our", "you", "your", "they", "their", "them", "he", "she", "his",
    "her", "all", "each", "every", "any", "both", "few", "more", "most",
    "other", "some", "such", "than", "too", "very", "just", "also",
    "about", "above", "after", "again", "between", "into", "through",
    "during", "before", "below", "up", "down", "out", "off", "over",
    "under", "then", "once", "here", "there", "when", "where", "why",
    "how", "what", "which", "who", "whom", "while",
})

_TOKEN_RE = re.compile(r"[a-z0-9]{2,}")


def _tokenize(text: str) -> list[str]:
    """Lowercase, extract alphanumeric tokens, drop stop words."""
    return [t for t in _TOKEN_RE.findall(text.lower()) if t not in _STOP_WORDS]


# ---------------------------------------------------------------------------
# TF-IDF scoring
# ---------------------------------------------------------------------------

def _tf(tokens: list[str]) -> dict[str, float]:
    """Term frequency (normalized by doc length)."""
    counts = Counter(tokens)
    total = len(tokens) or 1
    return {t: c / total for t, c in counts.items()}


def _idf(doc_token_sets: list[set[str]], vocab: set[str]) -> dict[str, float]:
    """Inverse document frequency across corpus."""
    n_docs = len(doc_token_sets) or 1
    idf_scores: dict[str, float] = {}
    for term in vocab:
        df = sum(1 for doc_set in doc_token_sets if term in doc_set)
        idf_scores[term] = math.log((n_docs + 1) / (df + 1)) + 1  # smoothed
    return idf_scores


def _tfidf_vector(
    tf_scores: dict[str, float],
    idf_scores: dict[str, float],
) -> dict[str, float]:
    """TF-IDF vector for a single document."""
    return {t: tf_scores[t] * idf_scores.get(t, 1.0) for t in tf_scores}


def _cosine_sim(
    vec_a: dict[str, float],
    vec_b: dict[str, float],
) -> float:
    """Cosine similarity between two sparse vectors."""
    common = set(vec_a) & set(vec_b)
    if not common:
        return 0.0
    dot = sum(vec_a[t] * vec_b[t] for t in common)
    mag_a = math.sqrt(sum(v * v for v in vec_a.values()))
    mag_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# ~4 chars per token is a rough approximation for English text
_DEFAULT_CHAR_BUDGET = 6000  # ~1.5K tokens — safe for 4096 ctx with room for prompt + generation
_MIN_RELEVANCE = 0.02  # docs below this TF-IDF relevance score get dropped
_MAX_REDUNDANCY = 0.85  # docs with cosine sim > this to any selected doc get dropped


def select_knowledge(
    prompt: str,
    candidates: dict[str, str],
    char_budget: int = _DEFAULT_CHAR_BUDGET,
    max_docs: int = 4,
    min_relevance: float = _MIN_RELEVANCE,
    max_redundancy: float = _MAX_REDUNDANCY,
) -> list[tuple[str, str, float]]:
    """Select the most relevant, non-redundant knowledge docs for a prompt.

    Parameters:
        prompt: The user message / combined prompt text to match against.
        candidates: Dict of {doc_name: doc_content} to choose from.
        char_budget: Max total chars of selected knowledge.
        max_docs: Max number of docs to select.
        min_relevance: Minimum TF-IDF relevance score to consider a doc.
        max_redundancy: Max cosine similarity between a new doc and any
            already-selected doc (higher = more redundancy allowed).

    Returns:
        List of (doc_name, doc_content, relevance_score) tuples, ordered
        by selection priority.
    """
    if not prompt or not candidates:
        return []

    # Tokenize everything
    prompt_tokens = _tokenize(prompt)
    if not prompt_tokens:
        # No meaningful tokens in prompt — return first doc as fallback
        for name, content in candidates.items():
            return [(name, content, 0.0)]
        return []

    prompt_tf = _tf(prompt_tokens)

    doc_names = list(candidates.keys())
    doc_contents = [candidates[n] for n in doc_names]
    doc_token_lists = [_tokenize(c) for c in doc_contents]
    doc_token_sets = [set(tl) for tl in doc_token_lists]

    # Build IDF from all docs + the prompt (prompt is a pseudo-doc)
    all_sets = doc_token_sets + [set(prompt_tokens)]
    vocab = set()
    for s in all_sets:
        vocab.update(s)
    idf_scores = _idf(all_sets, vocab)

    # Score each doc by TF-IDF similarity to the prompt
    prompt_vec = _tfidf_vector(prompt_tf, idf_scores)
    doc_vecs: list[dict[str, float]] = []
    relevance_scores: list[float] = []

    for i, token_list in enumerate(doc_token_lists):
        doc_tf = _tf(token_list)
        doc_vec = _tfidf_vector(doc_tf, idf_scores)
        doc_vecs.append(doc_vec)
        relevance_scores.append(_cosine_sim(prompt_vec, doc_vec))

    # Sort by relevance, filter by minimum threshold
    scored = sorted(
        zip(doc_names, doc_contents, doc_vecs, relevance_scores),
        key=lambda x: x[3],
        reverse=True,
    )
    scored = [(n, c, v, s) for n, c, v, s in scored if s >= min_relevance]

    if not scored:
        # Nothing relevant — return the highest-scored doc anyway as fallback
        best_idx = max(range(len(relevance_scores)), key=lambda i: relevance_scores[i])
        return [(doc_names[best_idx], doc_contents[best_idx], relevance_scores[best_idx])]

    # Greedy orthogonal selection
    selected: list[tuple[str, str, float]] = []
    selected_vecs: list[dict[str, float]] = []
    total_chars = 0

    for name, content, vec, score in scored:
        if len(selected) >= max_docs:
            break

        doc_chars = len(content)
        if total_chars + doc_chars > char_budget:
            continue  # skip this doc, try smaller ones

        # Check redundancy against already-selected docs
        if selected_vecs:
            max_sim = max(_cosine_sim(vec, sv) for sv in selected_vecs)
            if max_sim > max_redundancy:
                log.debug(
                    "Knowledge selector: skipping '{n}' (redundancy={s:.2f} > {t:.2f})",
                    n=name, s=max_sim, t=max_redundancy,
                )
                continue

        selected.append((name, content, score))
        selected_vecs.append(vec)
        total_chars += doc_chars

    log.info(
        "Knowledge selector: {sel}/{tot} docs, {chars} chars "
        "(budget={budget}, scores={scores})",
        sel=len(selected),
        tot=len(candidates),
        chars=total_chars,
        budget=char_budget,
        scores=[f"{n}:{s:.3f}" for n, _, s in selected],
    )

    return selected


def select_and_format(
    prompt: str,
    candidates: dict[str, str],
    char_budget: int = _DEFAULT_CHAR_BUDGET,
    max_docs: int = 4,
) -> str:
    """Select relevant knowledge and format as a single context block.

    Parameters:
        prompt: The user prompt to match against.
        candidates: Dict of {doc_name: doc_content}.
        char_budget: Max total chars.
        max_docs: Max docs to include.

    Returns:
        str: Formatted knowledge block, or empty string if nothing selected.
    """
    selected = select_knowledge(prompt, candidates, char_budget, max_docs)
    if not selected:
        return ""

    parts = [content for _, content, _ in selected]
    return "\n\n".join(parts)
