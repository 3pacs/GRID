"""
GRID Intelligence — News Intelligence & Narrative Analysis.

Higher-level news intelligence built on top of the news scraper.
Detects narrative shifts, correlates news flow with price moves,
and generates LLM briefings from aggregated news.

Pipeline:
  1. get_news_feed        — recent news with sentiment, sorted by relevance
  2. detect_narrative_shift — detect when media narrative changes direction
  3. find_news_before_move  — forensic: what news preceded a price move?
  4. generate_news_briefing — LLM-generated daily briefing from news flow
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── 1. News Feed ─────────────────────────────────────────────────────────

def get_news_feed(
    engine: Engine,
    ticker: str | None = None,
    hours: int = 24,
) -> list[dict]:
    """Get recent news with sentiment, sorted by relevance.

    Relevance ranking: articles mentioning specific tickers are ranked
    higher, high-confidence sentiment articles are ranked higher, and
    more recent articles are ranked higher.

    Args:
        engine: SQLAlchemy engine.
        ticker: Optional ticker to filter by.
        hours: Hours to look back (default 24).

    Returns:
        List of article dicts with sentiment, sorted by relevance score.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    if ticker:
        query = text("""
            SELECT title, source, url, published_at, summary, tickers,
                   sentiment, confidence, llm_summary,
                   EXTRACT(EPOCH FROM (NOW() - COALESCE(published_at, created_at))) AS age_seconds
            FROM news_articles
            WHERE :ticker = ANY(tickers)
              AND created_at >= :cutoff
            ORDER BY confidence DESC, published_at DESC NULLS LAST
            LIMIT 100
        """)
        params: dict[str, Any] = {"ticker": ticker.upper(), "cutoff": cutoff}
    else:
        query = text("""
            SELECT title, source, url, published_at, summary, tickers,
                   sentiment, confidence, llm_summary,
                   EXTRACT(EPOCH FROM (NOW() - COALESCE(published_at, created_at))) AS age_seconds
            FROM news_articles
            WHERE created_at >= :cutoff
            ORDER BY confidence DESC, published_at DESC NULLS LAST
            LIMIT 200
        """)
        params = {"cutoff": cutoff}

    try:
        with engine.connect() as conn:
            rows = conn.execute(query, params).fetchall()
    except Exception as exc:
        log.warning("get_news_feed query failed: {e}", e=str(exc))
        return []

    articles = []
    for r in rows:
        age_hours = (r[9] or 0) / 3600.0
        # Relevance: confidence * recency decay
        recency_weight = max(0.1, 1.0 - (age_hours / max(hours, 1)) * 0.5)
        relevance = (r[7] or 0.5) * recency_weight

        articles.append({
            "title": r[0],
            "source": r[1],
            "url": r[2],
            "published": r[3].isoformat() if r[3] else None,
            "summary": r[4],
            "tickers": r[5] or [],
            "sentiment": r[6],
            "confidence": r[7],
            "llm_summary": r[8],
            "relevance": round(relevance, 3),
        })

    # Sort by relevance score
    articles.sort(key=lambda a: a["relevance"], reverse=True)
    return articles


# ── 2. Narrative Shift Detection ─────────────────────────────────────────

def detect_narrative_shift(
    engine: Engine,
    ticker: str,
    days: int = 7,
) -> dict:
    """Detect when media narrative changes direction on a ticker.

    Compares sentiment distribution in the recent window (last 2 days)
    vs the prior window to find shifts from bullish->bearish or vice versa.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol to analyze.
        days: Total lookback period (default 7).

    Returns:
        Dict with shift_detected, direction, magnitude, and evidence.
    """
    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(days=2)
    prior_cutoff = now - timedelta(days=days)

    result: dict[str, Any] = {
        "ticker": ticker.upper(),
        "period_days": days,
        "shift_detected": False,
        "direction": None,
        "magnitude": 0.0,
        "recent_sentiment": {},
        "prior_sentiment": {},
        "evidence": [],
    }

    try:
        with engine.connect() as conn:
            # Recent window (last 2 days)
            recent = conn.execute(text("""
                SELECT sentiment, COUNT(*), AVG(confidence)
                FROM news_articles
                WHERE :ticker = ANY(tickers)
                  AND created_at >= :recent_cutoff
                GROUP BY sentiment
            """), {"ticker": ticker.upper(), "recent_cutoff": recent_cutoff}).fetchall()

            # Prior window (days - 2 to 2 days ago)
            prior = conn.execute(text("""
                SELECT sentiment, COUNT(*), AVG(confidence)
                FROM news_articles
                WHERE :ticker = ANY(tickers)
                  AND created_at >= :prior_cutoff
                  AND created_at < :recent_cutoff
                GROUP BY sentiment
            """), {
                "ticker": ticker.upper(),
                "prior_cutoff": prior_cutoff,
                "recent_cutoff": recent_cutoff,
            }).fetchall()

            # Recent key articles as evidence
            evidence_rows = conn.execute(text("""
                SELECT title, sentiment, confidence, published_at
                FROM news_articles
                WHERE :ticker = ANY(tickers)
                  AND created_at >= :recent_cutoff
                ORDER BY confidence DESC
                LIMIT 5
            """), {"ticker": ticker.upper(), "recent_cutoff": recent_cutoff}).fetchall()

    except Exception as exc:
        log.warning("Narrative shift query failed for {t}: {e}", t=ticker, e=str(exc))
        return result

    # Parse sentiment distributions
    def _parse_dist(rows) -> dict[str, Any]:
        total = sum(r[1] for r in rows)
        if total == 0:
            return {"total": 0, "bullish_pct": 0.0, "bearish_pct": 0.0, "score": 0.0}
        bullish = sum(r[1] for r in rows if r[0] == "BULLISH")
        bearish = sum(r[1] for r in rows if r[0] == "BEARISH")
        avg_conf = sum(r[1] * (r[2] or 0.5) for r in rows) / total
        score = (bullish - bearish) / total  # -1 (full bearish) to +1 (full bullish)
        return {
            "total": total,
            "bullish_pct": round(bullish / total, 3),
            "bearish_pct": round(bearish / total, 3),
            "avg_confidence": round(avg_conf, 3),
            "score": round(score, 3),
        }

    recent_dist = _parse_dist(recent)
    prior_dist = _parse_dist(prior)

    result["recent_sentiment"] = recent_dist
    result["prior_sentiment"] = prior_dist

    # Detect shift: significant change in sentiment score
    if recent_dist["total"] >= 2 and prior_dist["total"] >= 2:
        delta = recent_dist["score"] - prior_dist["score"]
        magnitude = abs(delta)

        if magnitude >= 0.3:  # 30% swing threshold
            result["shift_detected"] = True
            result["magnitude"] = round(magnitude, 3)
            if delta > 0:
                result["direction"] = "bearish_to_bullish"
            else:
                result["direction"] = "bullish_to_bearish"

    # Evidence: recent high-confidence articles
    result["evidence"] = [
        {
            "title": r[0],
            "sentiment": r[1],
            "confidence": r[2],
            "published": r[3].isoformat() if r[3] else None,
        }
        for r in evidence_rows
    ]

    if result["shift_detected"]:
        log.info(
            "Narrative shift detected for {t}: {d} (magnitude={m:.2f})",
            t=ticker, d=result["direction"], m=result["magnitude"],
        )

    return result


# ── 3. News Before Move (Forensic) ──────────────────────────────────────

def find_news_before_move(
    engine: Engine,
    ticker: str,
    move_date: date | str,
) -> list[dict]:
    """What news preceded a significant price move? Forensic analysis.

    Looks back 3 days before the move_date for any news mentioning
    the ticker, and scores how predictive each article was.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.
        move_date: Date of the price move (str or date).

    Returns:
        List of articles that preceded the move, with timing metadata.
    """
    if isinstance(move_date, str):
        move_date = date.fromisoformat(move_date)

    lookback_start = datetime.combine(
        move_date - timedelta(days=3), datetime.min.time(), tzinfo=timezone.utc,
    )
    move_start = datetime.combine(move_date, datetime.min.time(), tzinfo=timezone.utc)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT title, source, url, published_at, summary,
                       sentiment, confidence, llm_summary
                FROM news_articles
                WHERE :ticker = ANY(tickers)
                  AND published_at >= :lb_start
                  AND published_at < :mv_start
                ORDER BY published_at DESC
                LIMIT 50
            """), {
                "ticker": ticker.upper(),
                "lb_start": lookback_start,
                "mv_start": move_start,
            }).fetchall()
    except Exception as exc:
        log.warning(
            "find_news_before_move query failed for {t} on {d}: {e}",
            t=ticker, d=move_date, e=str(exc),
        )
        return []

    articles = []
    for r in rows:
        published = r[3]
        hours_before = None
        if published:
            delta = move_start - published
            hours_before = round(delta.total_seconds() / 3600, 1)

        articles.append({
            "title": r[0],
            "source": r[1],
            "url": r[2],
            "published": published.isoformat() if published else None,
            "summary": r[4],
            "sentiment": r[5],
            "confidence": r[6],
            "llm_summary": r[7],
            "hours_before_move": hours_before,
        })

    if articles:
        sentiments = [a["sentiment"] for a in articles]
        bullish_pct = sentiments.count("BULLISH") / len(sentiments)
        bearish_pct = sentiments.count("BEARISH") / len(sentiments)
        log.info(
            "News before {t} move on {d}: {n} articles ({b:.0%} bullish, {br:.0%} bearish)",
            t=ticker, d=move_date, n=len(articles), b=bullish_pct, br=bearish_pct,
        )

    return articles


# ── 4. News Briefing (LLM-generated) ────────────────────────────────────

def generate_news_briefing(engine: Engine) -> str:
    """LLM-generated briefing from today's news flow.

    Pulls the most recent high-confidence articles, groups by theme,
    and asks the LLM to produce a concise market briefing.

    Args:
        engine: SQLAlchemy engine.

    Returns:
        Markdown-formatted briefing string, or fallback summary if LLM unavailable.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT title, source, sentiment, confidence, llm_summary, tickers
                FROM news_articles
                WHERE created_at >= :cutoff
                ORDER BY confidence DESC
                LIMIT 30
            """), {"cutoff": cutoff}).fetchall()
    except Exception as exc:
        log.warning("News briefing query failed: {e}", e=str(exc))
        return "News briefing unavailable — database query failed."

    if not rows:
        return "No news articles in the last 24 hours."

    # Build article summaries for LLM
    article_lines = []
    sentiment_counts = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}
    all_tickers: set[str] = set()

    for r in rows:
        title, source, sentiment, confidence, llm_summary, tickers = r
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        if tickers:
            all_tickers.update(tickers)
        summary_text = llm_summary or title
        article_lines.append(
            f"- [{sentiment} {confidence:.0%}] ({source}) {summary_text}"
        )

    # Try LLM briefing
    try:
        from llamacpp.client import get_client
        llm = get_client()

        if llm and llm.is_available:
            prompt = (
                "Generate a concise financial market news briefing from these articles. "
                "Group by theme (macro, tech, energy, etc.). Highlight key moves and "
                "what traders should watch. Use markdown formatting.\n\n"
                "Articles:\n" + "\n".join(article_lines[:20])
            )

            system = (
                "You are GRID's market intelligence briefing writer. "
                "Write concise, actionable briefings for systematic traders. "
                "Focus on what matters for positioning. No fluff."
            )

            response = llm.generate(
                prompt=prompt,
                system=system,
                temperature=0.3,
                num_predict=1000,
            )

            if response:
                return response
    except Exception as exc:
        log.debug("LLM briefing generation failed: {e}", e=str(exc))

    # Fallback: structured summary without LLM
    total = len(rows)
    lines = [
        f"## News Briefing — {date.today().isoformat()}",
        "",
        f"**{total} articles** in the last 24 hours",
        f"- Bullish: {sentiment_counts.get('BULLISH', 0)}",
        f"- Bearish: {sentiment_counts.get('BEARISH', 0)}",
        f"- Neutral: {sentiment_counts.get('NEUTRAL', 0)}",
        "",
        f"**Tickers mentioned:** {', '.join(sorted(all_tickers)[:20]) or 'none'}",
        "",
        "### Top Headlines",
        "",
    ]
    for line in article_lines[:10]:
        lines.append(line)

    return "\n".join(lines)


# ── 5. Aggregate Stats ──────────────────────────────────────────────────

def get_news_stats(engine: Engine, hours: int = 24) -> dict[str, Any]:
    """Get aggregate news statistics for the dashboard.

    Args:
        engine: SQLAlchemy engine.
        hours: Lookback hours.

    Returns:
        Dict with counts, sentiment breakdown, top tickers, top sources.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    try:
        with engine.connect() as conn:
            # Sentiment breakdown
            sentiment_rows = conn.execute(text("""
                SELECT sentiment, COUNT(*), AVG(confidence)
                FROM news_articles
                WHERE created_at >= :cutoff
                GROUP BY sentiment
            """), {"cutoff": cutoff}).fetchall()

            # Top tickers by mention count
            ticker_rows = conn.execute(text("""
                SELECT unnest(tickers) AS ticker, COUNT(*) AS cnt
                FROM news_articles
                WHERE created_at >= :cutoff AND tickers IS NOT NULL
                GROUP BY ticker
                ORDER BY cnt DESC
                LIMIT 20
            """), {"cutoff": cutoff}).fetchall()

            # Source breakdown
            source_rows = conn.execute(text("""
                SELECT source, COUNT(*)
                FROM news_articles
                WHERE created_at >= :cutoff
                GROUP BY source
                ORDER BY COUNT(*) DESC
            """), {"cutoff": cutoff}).fetchall()

    except Exception as exc:
        log.warning("News stats query failed: {e}", e=str(exc))
        return {"error": str(exc)}

    total = sum(r[1] for r in sentiment_rows) if sentiment_rows else 0
    sentiment = {r[0]: {"count": r[1], "avg_confidence": round(r[2] or 0, 3)} for r in sentiment_rows}

    return {
        "hours": hours,
        "total_articles": total,
        "sentiment": sentiment,
        "top_tickers": [{"ticker": r[0], "mentions": r[1]} for r in ticker_rows],
        "sources": {r[0]: r[1] for r in source_rows},
    }
