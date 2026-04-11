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


# ── News Momentum Endpoints ───────────────────────────────────────────────


@router.get("/news/momentum")
async def get_news_momentum(
    ticker: str | None = Query(None, description="Filter by ticker"),
    signal_type: str | None = Query(None, description="Filter: ACCELERATING, DECELERATING, DIVERGENCE, STEADY"),
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get recent news momentum signals — sentiment acceleration/deceleration.

    Detects whether sentiment is accelerating (getting more bullish/bearish faster),
    decelerating, or diverging from price direction.
    """
    try:
        from intelligence.news_momentum import NewsMomentumEngine

        engine = get_db_engine()
        nme = NewsMomentumEngine(engine)
        signals = nme.get_recent_signals(
            ticker=ticker, hours=hours, signal_type=signal_type,
        )
        return {
            "signals": signals,
            "count": len(signals),
            "ticker": ticker,
            "signal_type": signal_type,
        }
    except Exception as exc:
        log.warning("News momentum endpoint failed: {e}", e=str(exc))
        return {"signals": [], "count": 0, "error": str(exc)}


@router.get("/news/momentum/divergences")
async def get_momentum_divergences(
    hours: int = Query(48, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get sentiment-price divergences — highest-value alerts.

    A divergence means price is moving one way but sentiment is moving
    the opposite direction. These often precede reversals.
    """
    try:
        from intelligence.news_momentum import NewsMomentumEngine

        engine = get_db_engine()
        nme = NewsMomentumEngine(engine)
        divergences = nme.get_divergences(hours=hours)
        return {
            "divergences": divergences,
            "count": len(divergences),
        }
    except Exception as exc:
        log.warning("Divergences endpoint failed: {e}", e=str(exc))
        return {"divergences": [], "count": 0, "error": str(exc)}


@router.post("/news/momentum/scan")
async def run_momentum_scan(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger a full news momentum scan across all active tickers.

    Computes sentiment velocity, acceleration, and divergence detection
    for every ticker with sufficient recent news coverage.
    """
    try:
        from intelligence.news_momentum import NewsMomentumEngine

        engine = get_db_engine()
        nme = NewsMomentumEngine(engine)
        return nme.run_full_scan()
    except Exception as exc:
        log.warning("Momentum scan failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ── Deal Detection Endpoints ─────────────────────────────────────────────


@router.get("/deals")
async def get_active_deals(
    deal_type: str | None = Query(None, description="Filter by deal type (MERGER, ACQUISITION, etc.)"),
    ticker: str | None = Query(None, description="Filter by ticker"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get active deals in the M&A pipeline.

    Returns all deals that haven't reached a terminal state (CLOSED, FAILED, WITHDRAWN).
    """
    try:
        from intelligence.deal_detector import DealDetector

        engine = get_db_engine()
        dd = DealDetector(engine)
        deals = dd.get_active_deals(deal_type=deal_type, ticker=ticker)
        return {
            "deals": deals,
            "count": len(deals),
            "deal_type": deal_type,
            "ticker": ticker,
        }
    except Exception as exc:
        log.warning("Active deals endpoint failed: {e}", e=str(exc))
        return {"deals": [], "count": 0, "error": str(exc)}


@router.get("/deals/pipeline")
async def get_deal_pipeline_summary(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get summary of the entire deal pipeline — counts by type and stage."""
    try:
        from intelligence.deal_detector import DealDetector

        engine = get_db_engine()
        dd = DealDetector(engine)
        return dd.get_pipeline_summary()
    except Exception as exc:
        log.warning("Deal pipeline summary failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/deals/history/{ticker}")
async def get_deal_history(
    ticker: str,
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get deal history for a specific ticker, including closed/failed deals."""
    try:
        from intelligence.deal_detector import DealDetector

        engine = get_db_engine()
        dd = DealDetector(engine)
        deals = dd.get_deal_history(ticker=ticker, days=days)
        return {
            "ticker": ticker.upper(),
            "deals": deals,
            "count": len(deals),
            "days": days,
        }
    except Exception as exc:
        log.warning("Deal history endpoint failed: {e}", e=str(exc))
        return {"ticker": ticker.upper(), "deals": [], "count": 0, "error": str(exc)}


@router.post("/deals/scan")
async def run_deal_scan(
    hours: int = Query(12, ge=1, le=48, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger a deal detection scan across recent news articles."""
    try:
        from intelligence.deal_detector import DealDetector

        engine = get_db_engine()
        dd = DealDetector(engine)
        return dd.scan_recent_news(hours=hours)
    except Exception as exc:
        log.warning("Deal scan failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ── Business Events Endpoints ────────────────────────────────────────────


@router.get("/news/business-events")
async def get_business_events(
    category: str | None = Query(None, description="Filter by category"),
    ticker: str | None = Query(None, description="Filter by ticker"),
    direction: str | None = Query(None, description="Filter: bullish, bearish, neutral"),
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get structured business events extracted from news.

    Events include: executive changes, product launches, restructurings,
    regulatory actions, capital raises, guidance changes, earnings surprises,
    contract wins, and more.
    """
    try:
        from intelligence.business_news_parser import BusinessNewsParser

        engine = get_db_engine()
        parser = BusinessNewsParser(engine)
        events = parser.get_recent_events(
            category=category, ticker=ticker,
            direction=direction, hours=hours,
        )
        return {
            "events": events,
            "count": len(events),
            "filters": {
                "category": category,
                "ticker": ticker,
                "direction": direction,
            },
        }
    except Exception as exc:
        log.warning("Business events endpoint failed: {e}", e=str(exc))
        return {"events": [], "count": 0, "error": str(exc)}


@router.get("/news/business-events/summary")
async def get_business_event_summary(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Aggregate business event statistics for the dashboard."""
    try:
        from intelligence.business_news_parser import BusinessNewsParser

        engine = get_db_engine()
        parser = BusinessNewsParser(engine)
        return parser.get_event_summary(hours=hours)
    except Exception as exc:
        log.warning("Business event summary failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.post("/news/business-events/scan")
async def run_business_event_scan(
    hours: int = Query(12, ge=1, le=48, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger a business event scan across recent news."""
    try:
        from intelligence.business_news_parser import BusinessNewsParser

        engine = get_db_engine()
        parser = BusinessNewsParser(engine)
        return parser.scan_recent_news(hours=hours)
    except Exception as exc:
        log.warning("Business event scan failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ── Earnings Transcript Analysis Endpoints ───────────────────────────────


@router.get("/earnings/transcript/{ticker}")
async def get_earnings_transcript_analysis(
    ticker: str,
    limit: int = Query(5, ge=1, le=20, description="Max results"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get earnings transcript tone analysis for a ticker.

    Includes management tone scoring, guidance phrase extraction,
    risk mentions, hedging language detection, and comparison with
    prior quarter tone.
    """
    try:
        from intelligence.earnings_transcript_analyzer import EarningsTranscriptAnalyzer

        engine = get_db_engine()
        analyzer = EarningsTranscriptAnalyzer(engine)
        analyses = analyzer.get_analysis(ticker=ticker, limit=limit)
        return {
            "ticker": ticker.upper(),
            "analyses": analyses,
            "count": len(analyses),
        }
    except Exception as exc:
        log.warning("Earnings transcript analysis failed: {e}", e=str(exc))
        return {"ticker": ticker.upper(), "analyses": [], "count": 0, "error": str(exc)}


@router.get("/earnings/tone-shifts")
async def get_earnings_tone_shifts(
    min_shift: float = Query(0.2, ge=0.05, le=1.0, description="Minimum absolute tone shift"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get tickers with significant tone shifts vs prior quarter earnings.

    A large positive shift means management became more optimistic;
    a large negative shift means they became more cautious/defensive.
    """
    try:
        from intelligence.earnings_transcript_analyzer import EarningsTranscriptAnalyzer

        engine = get_db_engine()
        analyzer = EarningsTranscriptAnalyzer(engine)
        shifts = analyzer.get_tone_shifts(min_shift=min_shift)
        return {
            "shifts": shifts,
            "count": len(shifts),
            "min_shift": min_shift,
        }
    except Exception as exc:
        log.warning("Tone shifts endpoint failed: {e}", e=str(exc))
        return {"shifts": [], "count": 0, "error": str(exc)}


@router.post("/earnings/transcript/analyze")
async def run_earnings_transcript_analysis(
    days_back: int = Query(90, ge=1, le=365, description="Days to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run earnings transcript analysis on recent filings."""
    try:
        from intelligence.earnings_transcript_analyzer import EarningsTranscriptAnalyzer

        engine = get_db_engine()
        analyzer = EarningsTranscriptAnalyzer(engine)
        return analyzer.run_analysis(days_back=days_back)
    except Exception as exc:
        log.warning("Earnings analysis run failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ── SEC Filing Extraction Endpoints ──────────────────────────────────────


@router.get("/sec/facts")
async def get_sec_material_facts(
    ticker: str | None = Query(None, description="Filter by ticker"),
    item_number: str | None = Query(None, description="Filter by 8-K item number (e.g. '2.01')"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get material facts extracted from SEC 8-K filings.

    Each fact is classified by item type, market direction, estimated
    impact in basis points, dollar values, and named entities.
    """
    try:
        from intelligence.sec_filing_extractor import SECFilingExtractor

        engine = get_db_engine()
        extractor = SECFilingExtractor(engine)
        facts = extractor.get_recent_facts(
            ticker=ticker, item_number=item_number, days=days,
        )
        return {
            "facts": facts,
            "count": len(facts),
            "ticker": ticker,
            "item_number": item_number,
        }
    except Exception as exc:
        log.warning("SEC facts endpoint failed: {e}", e=str(exc))
        return {"facts": [], "count": 0, "error": str(exc)}


@router.get("/sec/facts/high-impact")
async def get_high_impact_facts(
    days: int = Query(7, ge=1, le=90, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get high-impact SEC filing facts (>= 200 bps estimated impact).

    Filters for material events like acquisitions, bankruptcies,
    restructurings, and material impairments.
    """
    try:
        from intelligence.sec_filing_extractor import SECFilingExtractor

        engine = get_db_engine()
        extractor = SECFilingExtractor(engine)
        facts = extractor.get_high_impact_facts(days=days)
        return {
            "facts": facts,
            "count": len(facts),
        }
    except Exception as exc:
        log.warning("High impact facts endpoint failed: {e}", e=str(exc))
        return {"facts": [], "count": 0, "error": str(exc)}


@router.post("/sec/facts/extract")
async def run_sec_fact_extraction(
    days_back: int = Query(90, ge=1, le=365, description="Days to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run material fact extraction on recent SEC filings."""
    try:
        from intelligence.sec_filing_extractor import SECFilingExtractor

        engine = get_db_engine()
        extractor = SECFilingExtractor(engine)
        return extractor.run_extraction(days_back=days_back)
    except Exception as exc:
        log.warning("SEC extraction run failed: {e}", e=str(exc))
        return {"error": str(exc)}
