"""Intelligence sub-router: News, event sequences, and pattern engine endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── News Intelligence Endpoints ──────────────────────────────────────────


@router.get("/news")
async def get_news_feed_endpoint(
    ticker: str | None = Query(None, description="Filter by ticker symbol"),
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get recent news with sentiment, sorted by relevance.

    Optionally filter by ticker. Returns articles from the last N hours
    with LLM sentiment scores and relevance ranking.
    """
    try:
        from intelligence.news_intel import get_news_feed

        engine = get_db_engine()
        articles = get_news_feed(engine, ticker=ticker, hours=hours)
        return {
            "ticker": ticker,
            "hours": hours,
            "count": len(articles),
            "articles": articles,
        }
    except Exception as exc:
        log.warning("News feed endpoint failed: {e}", e=str(exc))
        return {"articles": [], "count": 0, "error": str(exc)}


@router.get("/news/stats")
async def get_news_stats_endpoint(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get aggregate news statistics — sentiment breakdown, top tickers, sources."""
    try:
        from intelligence.news_intel import get_news_stats

        engine = get_db_engine()
        return get_news_stats(engine, hours=hours)
    except Exception as exc:
        log.warning("News stats endpoint failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/news/narrative-shift/{ticker}")
async def get_narrative_shift_endpoint(
    ticker: str,
    days: int = Query(7, ge=2, le=30, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect when media narrative changes direction on a ticker.

    Compares recent (2-day) vs prior sentiment distribution to find
    significant shifts from bullish to bearish or vice versa.
    """
    try:
        from intelligence.news_intel import detect_narrative_shift

        engine = get_db_engine()
        return detect_narrative_shift(engine, ticker=ticker, days=days)
    except Exception as exc:
        log.warning("Narrative shift endpoint failed: {e}", e=str(exc))
        return {"ticker": ticker, "shift_detected": False, "error": str(exc)}


@router.get("/news/before-move/{ticker}")
async def get_news_before_move_endpoint(
    ticker: str,
    move_date: str = Query(..., description="Date of the price move (YYYY-MM-DD)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Forensic analysis: what news preceded a significant price move?

    Looks back 3 days before the move_date for news mentioning the ticker.
    """
    try:
        from intelligence.news_intel import find_news_before_move

        engine = get_db_engine()
        articles = find_news_before_move(engine, ticker=ticker, move_date=move_date)
        return {
            "ticker": ticker,
            "move_date": move_date,
            "articles_found": len(articles),
            "articles": articles,
        }
    except Exception as exc:
        log.warning("News-before-move endpoint failed: {e}", e=str(exc))
        return {"ticker": ticker, "articles": [], "error": str(exc)}


@router.get("/news/briefing")
async def get_news_briefing_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """LLM-generated briefing from today's news flow.

    Returns a markdown-formatted market briefing synthesized from
    recent high-confidence news articles.
    """
    try:
        from intelligence.news_intel import generate_news_briefing

        engine = get_db_engine()
        briefing = generate_news_briefing(engine)
        return {"briefing": briefing}
    except Exception as exc:
        log.warning("News briefing endpoint failed: {e}", e=str(exc))
        return {"briefing": f"News briefing unavailable: {exc}", "error": str(exc)}


# ── Event Sequence Endpoints ───────────────────────────────────────────────


@router.get("/events")
async def get_event_sequence(
    ticker: str | None = Query(None, description="Ticker symbol"),
    sector: str | None = Query(None, description="Sector name or ETF (e.g., Technology, XLK)"),
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    with_lead_times: bool = Query(False, description="Compute lead times to next price move"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Build a chronological timeline of ALL events for a ticker or sector.

    Pulls from signal_sources, news_articles, options_daily_signals,
    decision_journal, cross_reference_checks, and earnings_calendar.

    Supply either ``ticker`` or ``sector`` (not both).  If ``sector`` is
    provided, events for all constituent tickers are returned.
    """
    if not ticker and not sector:
        return {"error": "Provide either 'ticker' or 'sector' query parameter", "events": []}

    try:
        from intelligence.event_sequence import (
            build_sequence,
            build_sector_sequence,
            build_sequence_with_lead_times,
            events_to_dicts,
        )

        engine = get_db_engine()

        if sector:
            events = build_sector_sequence(engine, sector=sector, days=days)
        elif with_lead_times:
            events = build_sequence_with_lead_times(engine, ticker=ticker, days=days)
        else:
            events = build_sequence(engine, ticker=ticker, days=days)

        type_counts: dict[str, int] = {}
        direction_counts: dict[str, int] = {}
        for e in events:
            type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1
            direction_counts[e.direction] = direction_counts.get(e.direction, 0) + 1

        return {
            "events": events_to_dicts(events),
            "count": len(events),
            "ticker": ticker,
            "sector": sector,
            "days": days,
            "type_counts": type_counts,
            "direction_counts": direction_counts,
        }
    except Exception as exc:
        log.warning("Event sequence failed: {e}", e=str(exc))
        return {
            "events": [],
            "count": 0,
            "ticker": ticker,
            "sector": sector,
            "error": str(exc),
        }


@router.get("/events/patterns")
async def get_recurring_patterns(
    min_occurrences: int = Query(3, ge=2, le=50, description="Minimum pattern occurrences"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect recurring event sequences across all tracked tickers.

    Finds 2- and 3-event sequences that repeat at least ``min_occurrences``
    times.  Examples: "insider sell -> dark pool spike -> price drop".
    """
    try:
        from intelligence.event_sequence import find_recurring_patterns

        engine = get_db_engine()
        patterns = find_recurring_patterns(engine, min_occurrences=min_occurrences)
        return {
            "patterns": patterns,
            "count": len(patterns),
            "min_occurrences": min_occurrences,
        }
    except Exception as exc:
        log.warning("Recurring pattern detection failed: {e}", e=str(exc))
        return {"patterns": [], "count": 0, "error": str(exc)}


# ── Pattern Engine Endpoints ─────────────────────────────────────────────


@router.get("/patterns")
async def get_discovered_patterns(
    min_occurrences: int = Query(3, ge=2, le=50, description="Minimum pattern occurrences"),
    max_sequence_length: int = Query(4, ge=2, le=4, description="Max sequence length"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """All discovered recurring event patterns.

    Scans historical event sequences across all watchlist tickers to find
    recurring 2-, 3-, and 4-event sequences.  Only returns patterns with a
    hit rate above 50%.  Sorted by confidence x actionable return.
    """
    try:
        from intelligence.pattern_engine import discover_patterns

        engine = get_db_engine()
        patterns = discover_patterns(
            engine,
            min_occurrences=min_occurrences,
            max_sequence_length=max_sequence_length,
        )
        return {
            "patterns": [p.to_dict() for p in patterns],
            "count": len(patterns),
            "actionable_count": sum(1 for p in patterns if p.actionable),
            "min_occurrences": min_occurrences,
            "max_sequence_length": max_sequence_length,
        }
    except Exception as exc:
        log.warning("Pattern discovery failed: {e}", e=str(exc))
        return {"patterns": [], "count": 0, "error": str(exc)}


@router.get("/patterns/active")
async def get_active_patterns(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Currently in-progress patterns — the prediction engine.

    For each discovered pattern, checks whether the first N-1 steps have
    already occurred for any watchlist ticker.  Returns what step comes next
    and when it is expected.
    """
    try:
        from intelligence.pattern_engine import match_active_patterns

        engine = get_db_engine()
        active = match_active_patterns(engine)
        return {
            "active_patterns": active,
            "count": len(active),
            "actionable_count": sum(1 for a in active if a.get("actionable")),
        }
    except Exception as exc:
        log.warning("Active pattern matching failed: {e}", e=str(exc))
        return {"active_patterns": [], "count": 0, "error": str(exc)}


@router.get("/patterns/{ticker}")
async def get_patterns_for_ticker_endpoint(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Patterns observed for a specific ticker, including any currently active.

    Returns both historical patterns where this ticker appeared and any
    patterns that are partially matched (in progress) right now.
    """
    try:
        from intelligence.pattern_engine import get_patterns_for_ticker

        engine = get_db_engine()
        patterns = get_patterns_for_ticker(engine, ticker)
        active_count = sum(1 for p in patterns if p.get("active_match"))
        return {
            "ticker": ticker.upper(),
            "patterns": patterns,
            "count": len(patterns),
            "active_count": active_count,
        }
    except Exception as exc:
        log.warning("Pattern lookup for {t} failed: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker.upper(), "patterns": [], "count": 0, "error": str(exc)}
