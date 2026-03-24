"""
GRID Knowledge Tree — stores and indexes LLM Q&A interactions.

Accumulates institutional knowledge over time so the LLM can reference
past questions and answers when generating new responses.

All SQL uses parameterized queries via psycopg2 %s / %(name)s placeholders.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log


# ---------------------------------------------------------------------------
# Category detection keywords
# ---------------------------------------------------------------------------
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "macro": [
        "gdp", "inflation", "cpi", "pce", "employment", "unemployment",
        "nonfarm", "payroll", "fed", "fomc", "rate", "yield", "treasury",
        "bond", "fiscal", "monetary", "recession", "expansion", "interest",
        "deficit", "surplus", "trade balance", "bls", "fred", "ecb",
        "boj", "central bank", "quantitative", "tightening", "easing",
    ],
    "regime": [
        "regime", "cluster", "transition", "state", "phase", "hmm",
        "hidden markov", "risk-on", "risk-off", "volatility regime",
        "mean-revert", "trending", "regime change", "regime shift",
    ],
    "technical": [
        "rsi", "macd", "moving average", "bollinger", "support", "resistance",
        "momentum", "trend", "breakout", "candlestick", "fibonacci",
        "volume", "oscillator", "stochastic", "ichimoku", "divergence",
        "overbought", "oversold", "sma", "ema", "vwap", "atr",
    ],
    "sentiment": [
        "sentiment", "fear", "greed", "vix", "put/call", "put call",
        "bullish", "bearish", "confidence", "survey", "positioning",
        "flow", "retail", "institutional",
    ],
    "risk": [
        "risk", "drawdown", "var", "cvar", "sharpe", "sortino",
        "max drawdown", "correlation", "hedge", "diversif", "tail risk",
        "stress test", "exposure", "leverage", "margin",
    ],
}

# Common ticker patterns
_TICKER_RE = re.compile(
    r"""
    (?:^|[\s,;(])          # word boundary or punctuation
    \$([A-Z]{1,5})         # $TICKER explicit
    (?:[\s,;).]|$)         # trailing boundary
    """,
    re.VERBOSE,
)

_KNOWN_TICKERS = {
    "SPY", "SPX", "QQQ", "DIA", "IWM", "VIX", "TLT", "GLD", "SLV",
    "BTC", "ETH", "AAPL", "MSFT", "GOOG", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "GS", "BAC", "XLE", "XLF", "XLK", "XLV",
    "USO", "UNG", "DXY", "EURUSD", "USDJPY", "GBPUSD",
}

# Stopwords for tag extraction
_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "must",
    "to", "of", "in", "for", "on", "with", "at", "by", "from", "as",
    "into", "through", "during", "before", "after", "above", "below",
    "between", "out", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such", "no",
    "nor", "not", "only", "own", "same", "so", "than", "too", "very",
    "just", "because", "but", "and", "or", "if", "while", "about", "what",
    "which", "who", "whom", "this", "that", "these", "those", "am", "it",
    "its", "my", "your", "his", "her", "their", "our", "me", "him", "them",
    "i", "we", "you", "he", "she", "they", "up", "down",
}


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------
_TABLE_CREATED = False


def _ensure_table() -> None:
    """Create the knowledge_tree table if it does not already exist."""
    global _TABLE_CREATED
    if _TABLE_CREATED:
        return

    from db import get_connection

    ddl = """
    CREATE TABLE IF NOT EXISTS knowledge_tree (
        id              SERIAL PRIMARY KEY,
        question        TEXT NOT NULL,
        answer          TEXT NOT NULL,
        category        TEXT,
        tags            TEXT[],
        source_model    TEXT,
        confidence      FLOAT,
        referenced_features TEXT[],
        referenced_tickers  TEXT[],
        parent_id       INTEGER REFERENCES knowledge_tree(id),
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        created_by      TEXT DEFAULT 'operator'
    );

    CREATE INDEX IF NOT EXISTS idx_knowledge_tree_category
        ON knowledge_tree(category);
    CREATE INDEX IF NOT EXISTS idx_knowledge_tree_created_at
        ON knowledge_tree(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_knowledge_tree_parent_id
        ON knowledge_tree(parent_id);
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        _TABLE_CREATED = True
        log.info("knowledge_tree table ensured")
    except Exception as exc:
        log.error("Failed to create knowledge_tree table: {e}", e=str(exc))
        raise


# ---------------------------------------------------------------------------
# Auto-extraction helpers
# ---------------------------------------------------------------------------

def _detect_category(text: str) -> str:
    """Detect Q&A category from combined question+answer text."""
    lower = text.lower()
    scores: dict[str, int] = {}
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in lower)
        if score > 0:
            scores[cat] = score

    if not scores:
        return "general"
    return max(scores, key=scores.get)  # type: ignore[arg-type]


def _extract_tags(text: str, max_tags: int = 10) -> list[str]:
    """Extract significant words/phrases as tags."""
    # Tokenize, lowercase, remove short/stop words
    words = re.findall(r"[a-zA-Z]{3,}", text.lower())
    word_freq: dict[str, int] = {}
    for w in words:
        if w not in _STOPWORDS and len(w) > 2:
            word_freq[w] = word_freq.get(w, 0) + 1

    # Sort by frequency descending, take top N
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [w for w, _ in sorted_words[:max_tags]]


def _extract_tickers(text: str) -> list[str]:
    """Extract ticker symbols from text."""
    found: set[str] = set()

    # Explicit $TICKER patterns
    for match in _TICKER_RE.finditer(text):
        found.add(match.group(1).upper())

    # Check for known tickers as standalone words
    upper_text = text.upper()
    for ticker in _KNOWN_TICKERS:
        pattern = r"(?:^|[\s,;(])" + re.escape(ticker) + r"(?:[\s,;).]|$)"
        if re.search(pattern, upper_text):
            found.add(ticker)

    return sorted(found)


def _extract_features(text: str) -> list[str]:
    """Extract referenced feature names from text.

    Checks against the feature_registry table if available.
    Falls back to detecting common GRID feature naming patterns.
    """
    features: set[str] = set()

    # Try to load known features from DB
    known_features: set[str] = set()
    try:
        from db import execute_sql
        rows = execute_sql("SELECT DISTINCT feature_id FROM feature_registry")
        known_features = {r["feature_id"] for r in rows}
    except Exception:
        pass

    if known_features:
        lower_text = text.lower()
        for feat in known_features:
            if feat.lower() in lower_text:
                features.add(feat)
    else:
        # Fallback: detect common patterns like z_score_*, slope_*, ratio_*
        pattern = re.compile(
            r"\b(z_score_\w+|slope_\w+|ratio_\w+|spread_\w+|"
            r"pct_change_\w+|vol_\w+|diff_\w+)\b",
            re.IGNORECASE,
        )
        for match in pattern.finditer(text):
            features.add(match.group(1).lower())

    return sorted(features)


def _estimate_confidence(answer: str) -> float:
    """Estimate response quality/confidence from answer text.

    Heuristic based on length, hedging language, and specificity.
    Returns a float between 0.0 and 1.0.
    """
    if not answer:
        return 0.0

    score = 0.5  # base

    # Length bonus (longer = more thorough)
    word_count = len(answer.split())
    if word_count > 200:
        score += 0.15
    elif word_count > 100:
        score += 0.1
    elif word_count < 20:
        score -= 0.15

    # Hedging penalty
    hedges = ["i'm not sure", "i don't know", "uncertain", "unclear",
              "cannot determine", "hard to say", "difficult to assess"]
    lower = answer.lower()
    hedge_count = sum(1 for h in hedges if h in lower)
    score -= hedge_count * 0.1

    # Specificity bonus (numbers, percentages, dates)
    specifics = len(re.findall(r"\d+\.?\d*%|\d{4}-\d{2}|\$[\d,.]+", answer))
    score += min(specifics * 0.05, 0.2)

    return max(0.0, min(1.0, round(score, 2)))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def store_qa(
    question: str,
    answer: str,
    model: str = "unknown",
    created_by: str = "operator",
    parent_id: int | None = None,
) -> dict[str, Any]:
    """Store a Q&A interaction in the knowledge tree.

    Automatically extracts category, tags, tickers, features, and confidence.

    Parameters:
        question: The user's question.
        answer: The LLM's answer.
        model: Which LLM model answered (e.g. 'hermes', 'gpt-4o').
        created_by: Who asked the question.
        parent_id: Optional ID of a parent Q&A for follow-up threads.

    Returns:
        dict with the stored entry's id and extracted metadata.
    """
    _ensure_table()

    combined = f"{question} {answer}"
    category = _detect_category(combined)
    tags = _extract_tags(combined)
    tickers = _extract_tickers(combined)
    features = _extract_features(combined)
    confidence = _estimate_confidence(answer)

    from db import get_connection

    sql = """
    INSERT INTO knowledge_tree
        (question, answer, category, tags, source_model, confidence,
         referenced_features, referenced_tickers, parent_id, created_by)
    VALUES
        (%(question)s, %(answer)s, %(category)s, %(tags)s, %(model)s,
         %(confidence)s, %(features)s, %(tickers)s, %(parent_id)s, %(created_by)s)
    RETURNING id, created_at
    """
    params = {
        "question": question,
        "answer": answer,
        "category": category,
        "tags": tags,
        "model": model,
        "confidence": confidence,
        "features": features,
        "tickers": tickers,
        "parent_id": parent_id,
        "created_by": created_by,
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()

    entry_id = row[0] if row else None
    created_at = row[1] if row else None

    log.info(
        "Knowledge stored: id={id} cat={cat} tags={n_tags} tickers={tickers}",
        id=entry_id, cat=category, n_tags=len(tags), tickers=tickers,
    )

    return {
        "id": entry_id,
        "category": category,
        "tags": tags,
        "tickers": tickers,
        "features": features,
        "confidence": confidence,
        "created_at": str(created_at) if created_at else None,
    }


def search_knowledge(
    query: str,
    category: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Full-text search across questions and answers.

    Parameters:
        query: Search text (matched with ILIKE).
        category: Optional category filter.
        limit: Max results.
        offset: Pagination offset.

    Returns:
        dict with 'entries' list and 'total' count.
    """
    _ensure_table()
    from db import get_connection

    where_clauses = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if query and query.strip():
        where_clauses.append(
            "(question ILIKE %(q)s OR answer ILIKE %(q)s OR %(q_raw)s = ANY(tags))"
        )
        params["q"] = f"%{query.strip()}%"
        params["q_raw"] = query.strip().lower()

    if category:
        where_clauses.append("category = %(category)s")
        params["category"] = category

    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    count_sql = f"SELECT COUNT(*) FROM knowledge_tree {where}"
    data_sql = f"""
    SELECT id, question, answer, category, tags, source_model, confidence,
           referenced_features, referenced_tickers, parent_id,
           created_at, created_by
    FROM knowledge_tree
    {where}
    ORDER BY created_at DESC
    LIMIT %(limit)s OFFSET %(offset)s
    """

    with get_connection() as conn:
        import psycopg2.extras
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = cur.fetchone()[0]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(data_sql, params)
            entries = [dict(row) for row in cur.fetchall()]

    return {"entries": entries, "total": total}


def get_related(question: str, limit: int = 5) -> list[dict[str, Any]]:
    """Find similar past Q&As based on keyword overlap.

    Uses a simple word-overlap scoring approach.

    Parameters:
        question: The question to find relatives for.
        limit: Max related entries to return.

    Returns:
        List of related entries sorted by relevance.
    """
    _ensure_table()
    from db import get_connection

    # Extract keywords from the question
    words = re.findall(r"[a-zA-Z]{3,}", question.lower())
    keywords = [w for w in words if w not in _STOPWORDS]

    if not keywords:
        return []

    # Build a query that scores by keyword overlap in question + answer
    # Use array overlap for efficiency
    like_conditions = []
    params: dict[str, Any] = {"limit": limit}
    for i, kw in enumerate(keywords[:10]):  # cap at 10 keywords
        key = f"kw_{i}"
        like_conditions.append(
            f"(CASE WHEN question ILIKE %({key})s OR answer ILIKE %({key})s "
            f"THEN 1 ELSE 0 END)"
        )
        params[key] = f"%{kw}%"

    score_expr = " + ".join(like_conditions)

    sql = f"""
    SELECT id, question, answer, category, tags, confidence, created_at,
           ({score_expr}) AS relevance
    FROM knowledge_tree
    WHERE ({score_expr}) > 0
    ORDER BY relevance DESC, created_at DESC
    LIMIT %(limit)s
    """

    with get_connection() as conn:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def get_knowledge_summary() -> dict[str, Any]:
    """Get summary stats: total entries, by category, recent topics.

    Returns:
        dict with total, categories breakdown, and recent entries.
    """
    _ensure_table()
    from db import get_connection
    import psycopg2.extras

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM knowledge_tree")
            total = cur.fetchone()[0]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT category, COUNT(*) AS count
                FROM knowledge_tree
                GROUP BY category
                ORDER BY count DESC
            """)
            categories = [dict(row) for row in cur.fetchall()]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, question, category, created_at
                FROM knowledge_tree
                ORDER BY created_at DESC
                LIMIT 10
            """)
            recent = [dict(row) for row in cur.fetchall()]

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM knowledge_tree
                WHERE created_at >= NOW() - INTERVAL '7 days'
            """)
            this_week = cur.fetchone()[0]

    return {
        "total": total,
        "this_week": this_week,
        "categories": categories,
        "recent": recent,
    }


def get_context_for_prompt(topic: str, limit: int = 5) -> str:
    """Retrieve relevant past Q&As formatted for LLM context injection.

    Parameters:
        topic: The current question/topic to find context for.
        limit: Max past Q&As to include.

    Returns:
        Formatted string of past Q&As suitable for system prompt injection.
    """
    related = get_related(topic, limit=limit)

    if not related:
        return ""

    lines = ["--- Institutional Memory (past Q&A) ---"]
    for entry in related:
        q = entry.get("question", "")
        a = entry.get("answer", "")
        cat = entry.get("category", "general")
        # Truncate long answers to keep context manageable
        if len(a) > 500:
            a = a[:497] + "..."
        lines.append(f"\nQ ({cat}): {q}")
        lines.append(f"A: {a}")
    lines.append("\n--- End Institutional Memory ---")

    return "\n".join(lines)


def get_entry_by_id(entry_id: int) -> dict[str, Any] | None:
    """Get a single knowledge tree entry by ID.

    Parameters:
        entry_id: The entry's primary key.

    Returns:
        dict with entry data, or None if not found.
    """
    _ensure_table()
    from db import get_connection
    import psycopg2.extras

    sql = """
    SELECT id, question, answer, category, tags, source_model, confidence,
           referenced_features, referenced_tickers, parent_id,
           created_at, created_by
    FROM knowledge_tree
    WHERE id = %(id)s
    """

    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"id": entry_id})
            row = cur.fetchone()
            return dict(row) if row else None


def delete_entry(entry_id: int) -> bool:
    """Delete a knowledge tree entry.

    Parameters:
        entry_id: The entry to delete.

    Returns:
        True if deleted, False if not found.
    """
    _ensure_table()
    from db import get_connection

    # First nullify any parent_id references
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE knowledge_tree SET parent_id = NULL WHERE parent_id = %(id)s",
                {"id": entry_id},
            )
            cur.execute(
                "DELETE FROM knowledge_tree WHERE id = %(id)s",
                {"id": entry_id},
            )
            deleted = cur.rowcount > 0

    if deleted:
        log.info("Knowledge entry {id} deleted", id=entry_id)
    return deleted
