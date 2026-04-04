"""
GRID API — Ask GRID conversational chat endpoint.

Gathers system context (regime, watchlist, cross-reference, trust scores,
lever-puller activity, options, GEX) and sends a structured prompt to the
LLM (llamacpp -> ollama fallback).  Falls back to rule-based summaries
when no LLM is available.

  POST /api/v1/chat/ask  — conversational question with optional history
"""

from __future__ import annotations

import re as _re
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from loguru import logger as log
from pydantic import BaseModel, Field, field_validator

from api.auth import require_auth

router = APIRouter(
    prefix="/api/v1/chat",
    tags=["chat"],
    dependencies=[Depends(require_auth)],
)


# ── Request / Response models ───────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str = "user"
    content: str = ""


_TICKER_RE = _re.compile(r"^[A-Z0-9.\-]{1,15}$")
_VALID_TIMEFRAMES = {"1d", "1w", "1m", "3m", "6m"}


class ChatAskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    context_ticker: str | None = Field(None, max_length=15)
    timeframe: str | None = None
    history: list[ChatMessage] = Field(default_factory=list, max_length=50)

    @field_validator("context_ticker")
    @classmethod
    def validate_ticker(cls, v):
        if v is not None and not _TICKER_RE.match(v.strip().upper()):
            raise ValueError("Invalid ticker format")
        return v

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, v):
        if v is not None and v not in _VALID_TIMEFRAMES:
            raise ValueError(f"timeframe must be one of {_VALID_TIMEFRAMES}")
        return v


class ChatAskResponse(BaseModel):
    answer: str
    sources_used: list[str]
    confidence: float
    generated_at: str
    model_used: str | None = None
    answer_b: str | None = None  # A/B test: second model response
    model_b: str | None = None   # A/B test: second model name


# ── Helpers: gather context from various GRID subsystems ────────────────

def _get_db_engine():
    """Get the shared SQLAlchemy engine."""
    from db import get_engine
    return get_engine()


def _gather_regime_context() -> tuple[str, str]:
    """Return current regime state from DB."""
    try:
        engine = _get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT regime_label, confidence, recorded_at "
                "FROM regime_history ORDER BY recorded_at DESC LIMIT 1"
            )).fetchone()
            if row:
                return (
                    f"Current regime: {row[0]} (confidence: {row[1]}, as of {row[2]})",
                    "regime_history",
                )
    except Exception as exc:
        log.debug("Chat context: regime history query failed: {e}", e=str(exc))
    return "", ""


def _gather_watchlist_context(ticker: str | None) -> tuple[str, str]:
    """Return price + technical data for a specific ticker from feature_registry."""
    if not ticker:
        return "", ""

    parts = [f"Ticker {ticker}:"]
    t_lower = ticker.lower()

    try:
        engine = _get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            # Find features for this ticker (exact prefix match only)
            features = conn.execute(text(
                "SELECT fr.id, fr.name, fr.description "
                "FROM feature_registry fr "
                "WHERE (fr.name = :exact OR fr.name LIKE :pat_under) "
                "ORDER BY fr.name"
            ), {
                "exact": t_lower,
                "pat_under": f"{t_lower}\\_%",
            }).fetchall()

            # Also get DEX data for crypto tickers
            dex_features = conn.execute(text(
                "SELECT fr.id, fr.name, fr.description "
                "FROM feature_registry fr "
                "WHERE fr.name LIKE :dex_pat "
                "ORDER BY fr.name"
            ), {"dex_pat": f"dex_{t_lower}\\_%"}).fetchall()
            features = list(features) + list(dex_features)

            if not features:
                return "", ""

            # Get latest value for each feature
            for fid, fname, fdesc in features:
                row = conn.execute(text(
                    "SELECT value, obs_date FROM resolved_series "
                    "WHERE feature_id = :fid ORDER BY obs_date DESC LIMIT 1"
                ), {"fid": fid}).fetchone()
                if row and row[0] is not None:
                    val = float(row[0])
                    date = row[1]
                    # Format nicely based on the feature name
                    if "close" in fname or fname in (t_lower, f"{t_lower}_full", f"{t_lower}_usd_full"):
                        parts.append(f"  Price: ${val:,.2f} (as of {date})")
                    elif "market_cap" in fname:
                        if val > 1e12:
                            parts.append(f"  Market cap: ${val/1e12:.2f}T ({date})")
                        elif val > 1e9:
                            parts.append(f"  Market cap: ${val/1e9:.2f}B ({date})")
                        else:
                            parts.append(f"  Market cap: ${val/1e6:.0f}M ({date})")
                    elif "fifty_day" in fname or "50d" in fname:
                        parts.append(f"  50-day avg: ${val:,.2f}")
                    elif "two_hundred" in fname or "200d" in fname:
                        parts.append(f"  200-day avg: ${val:,.2f}")
                    elif "fifty_two_high" in fname or "52w_high" in fname:
                        parts.append(f"  52-week high: ${val:,.2f}")
                    elif "fifty_two_low" in fname or "52w_low" in fname:
                        parts.append(f"  52-week low: ${val:,.2f}")
                    elif "rsi" in fname:
                        parts.append(f"  RSI: {val:.1f}")
                    elif "macd" in fname:
                        parts.append(f"  MACD: {val:.4f}")
                    elif "volume" in fname:
                        parts.append(f"  {fdesc or fname}: {val:,.0f}")
                    elif "fear" in fname or "greed" in fname:
                        parts.append(f"  {fdesc or fname}: {val:.0f}")
                    elif "dominance" in fname:
                        parts.append(f"  {fdesc or fname}: {val:.2f}%")
                    else:
                        parts.append(f"  {fdesc or fname}: {val:.4f}")

            # Get price history for momentum
            price_feat = conn.execute(text(
                "SELECT id FROM feature_registry "
                "WHERE name IN (:n1, :n2, :n3) LIMIT 1"
            ), {
                "n1": f"{t_lower}_usd_full",
                "n2": f"{t_lower}_full",
                "n3": t_lower,
            }).fetchone()

            if price_feat:
                hist = conn.execute(text(
                    "SELECT value, obs_date FROM resolved_series "
                    "WHERE feature_id = :fid ORDER BY obs_date DESC LIMIT 10"
                ), {"fid": price_feat[0]}).fetchall()
                if hist and len(hist) >= 2:
                    latest = float(hist[0][0])
                    prev = float(hist[1][0])
                    pct_1d = (latest - prev) / prev * 100 if prev else 0
                    parts.append(f"  1d change: {pct_1d:+.2f}%")
                if hist and len(hist) >= 5:
                    latest = float(hist[0][0])
                    week_ago = float(hist[4][0])
                    pct_5d = (latest - week_ago) / week_ago * 100 if week_ago else 0
                    parts.append(f"  5d change: {pct_5d:+.2f}%")
                if hist and len(hist) >= 10:
                    latest = float(hist[0][0])
                    ten_ago = float(hist[9][0])
                    pct_10d = (latest - ten_ago) / ten_ago * 100 if ten_ago else 0
                    parts.append(f"  10d change: {pct_10d:+.2f}%")

    except Exception as exc:
        log.debug("Chat context: watchlist context query failed: {e}", e=str(exc))

    if len(parts) > 1:
        return "\n".join(parts), f"watchlist/{ticker}"
    return "", ""


def _gather_cross_reference() -> tuple[str, str]:
    """Return latest cross-reference checks — focus on divergences."""
    try:
        engine = _get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT category, official_source, official_value, "
                "physical_source, physical_value, assessment, implication, confidence "
                "FROM cross_reference_checks "
                "WHERE checked_at > NOW() - INTERVAL '48 hours' "
                "ORDER BY CASE assessment WHEN 'major_divergence' THEN 0 "
                "  WHEN 'minor_divergence' THEN 1 WHEN 'contradictory' THEN 2 ELSE 3 END, "
                "  checked_at DESC LIMIT 10"
            )).fetchall()
            if rows:
                lines = ["Cross-reference lie detector (last 48h):"]
                divergences = [r for r in rows if r[5] in ('major_divergence', 'minor_divergence', 'contradictory')]
                if divergences:
                    for r in divergences[:5]:
                        lines.append(f"  RED FLAG [{r[0]}]: {r[1]}={r[2]} vs {r[3]}={r[4]} → {r[5]} ({r[6]})")
                else:
                    lines.append(f"  No divergences detected across {len(rows)} checks — official data consistent with reality")
                return "\n".join(lines), "cross_reference"
    except Exception as exc:
        log.debug("Chat context: cross-reference query failed: {e}", e=str(exc))
    return "", ""


def _gather_convergence() -> tuple[str, str]:
    """Return signal convergence — multiple sources agreeing."""
    try:
        engine = _get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            # Check for recent signals that converge on same direction
            rows = conn.execute(text(
                "SELECT ticker, direction, COUNT(*) as signal_count, "
                "ARRAY_AGG(DISTINCT source) as sources "
                "FROM signal_data "
                "WHERE recorded_at > NOW() - INTERVAL '48 hours' "
                "AND direction IS NOT NULL "
                "GROUP BY ticker, direction "
                "HAVING COUNT(*) >= 2 "
                "ORDER BY signal_count DESC LIMIT 8"
            )).fetchall()
            if rows:
                lines = ["Signal convergence (last 48h):"]
                for r in rows:
                    sources = r[3] if isinstance(r[3], list) else []
                    lines.append(f"  {r[0]} → {r[1]} ({r[2]} signals from: {', '.join(str(s) for s in sources[:4])})")
                return "\n".join(lines), "signal_convergence"
    except Exception as exc:
        log.debug("Chat context: signal convergence query failed: {e}", e=str(exc))
    return "", ""


def _gather_lever_pullers() -> tuple[str, str]:
    """Return recent lever-puller activity."""
    try:
        engine = _get_db_engine()
        from intelligence.lever_pullers import get_active_lever_events
        events = get_active_lever_events(engine)
        if events:
            lines = ["Recent lever-puller activity:"]
            for ev in events[:8]:
                name = getattr(ev.puller, 'name', '?') if hasattr(ev, 'puller') else '?'
                action = getattr(ev, 'action', '?')
                tickers = getattr(ev, 'tickers', [])
                confidence = getattr(ev, 'confidence', '?')
                ticker_str = ', '.join(tickers[:3]) if tickers else 'N/A'
                lines.append(f"  - {name}: {action} [{ticker_str}] (confidence: {confidence})")
            return "\n".join(lines), "lever_pullers"
    except Exception as exc:
        log.debug("Chat context: lever pullers query failed: {e}", e=str(exc))
    return "", ""


def _gather_options_context(ticker: str | None) -> tuple[str, str]:
    """Return options signals from DB."""
    try:
        engine = _get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker, signal_type, direction, confidence, detail, signal_date "
                "FROM options_daily_signals "
                "WHERE (:t IS NULL OR ticker = :t) "
                "ORDER BY signal_date DESC LIMIT :lim"
            ), {"t": ticker, "lim": 8}).fetchall()
            if rows:
                lines = ["Options signals:"]
                for r in rows:
                    detail = r[4][:80] if r[4] else ""
                    lines.append(f"  {r[0]} [{r[1]}]: {r[2]} ({r[3]:.0%} confidence) — {detail}")
                return "\n".join(lines), "options_signals"
    except Exception as exc:
        log.debug("Chat context: options signals query failed: {e}", e=str(exc))
    return "", ""


def _gather_gex(ticker: str | None) -> tuple[str, str]:
    """Return GEX summary from dealer gamma engine."""
    try:
        engine = _get_db_engine()
        from physics.dealer_gamma import DealerGammaEngine
        dge = DealerGammaEngine(engine)
        target = ticker or "SPY"
        profile = dge.compute_gex_profile(target)
        if profile and isinstance(profile, dict) and "error" not in profile:
            regime = profile.get("regime", "?")
            net_gex = profile.get("net_gex", profile.get("total_gex", "?"))
            flip = profile.get("gamma_flip", profile.get("flip_strike", "?"))
            return (
                f"GEX ({target}): regime={regime}, net_gex={net_gex}, gamma_flip={flip}",
                f"gex/{target}",
            )
    except Exception as exc:
        log.debug("Chat context: GEX query failed: {e}", e=str(exc))
    return "", ""


def _gather_predictions(ticker: str | None) -> tuple[str, str]:
    """Return active predictions from the oracle system."""
    try:
        engine = _get_db_engine()
        from oracle.scoreboard import build_oracle_scoreboard
        scoreboard = build_oracle_scoreboard(engine)
        if scoreboard and isinstance(scoreboard, dict):
            lines = ["Active predictions & oracle track record:"]
            # Overall accuracy
            accuracy = scoreboard.get("overall_accuracy")
            if accuracy is not None:
                lines.append(f"  Overall oracle accuracy: {accuracy:.1%}")
            # Per-model stats
            by_model = scoreboard.get("by_model", {})
            for model_name, stats in list(by_model.items())[:5]:
                acc = stats.get("accuracy", "?")
                n = stats.get("total", "?")
                lines.append(f"  {model_name}: {acc} accuracy ({n} predictions)")
            return "\n".join(lines), "oracle/scoreboard"
    except Exception as exc:
        log.debug("Chat context: oracle scoreboard failed: {e}", e=str(exc))

    # Fallback: try latest predictions directly
    try:
        engine = _get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT model, ticker, direction, confidence, created_at "
                "FROM predictions WHERE status = 'active' "
                "ORDER BY created_at DESC LIMIT 10"
            )).fetchall()
            if rows:
                lines = ["Active predictions:"]
                for r in rows:
                    lines.append(f"  {r.model} → {r.ticker} {r.direction} ({r.confidence:.0%} confidence)")
                return "\n".join(lines), "oracle/predictions"
    except Exception as exc:
        log.debug("Chat context: oracle predictions fallback failed: {e}", e=str(exc))
    return "", ""


def _gather_news() -> tuple[str, str]:
    """Return recent news sentiment and top stories."""
    try:
        engine = _get_db_engine()
        from intelligence.news_intel import get_news_stats
        stats = get_news_stats(engine, hours=24)
        if stats and isinstance(stats, dict):
            lines = ["News intelligence (last 24h):"]
            sentiment = stats.get("sentiment_breakdown", {})
            if sentiment:
                lines.append(f"  Sentiment: bullish={sentiment.get('bullish', 0)}, "
                           f"bearish={sentiment.get('bearish', 0)}, "
                           f"neutral={sentiment.get('neutral', 0)}")
            top_tickers = stats.get("top_tickers", [])
            if top_tickers:
                lines.append(f"  Most mentioned: {', '.join(str(t) for t in top_tickers[:8])}")
            return "\n".join(lines), "news_intel"
    except Exception as exc:
        log.debug("Chat context: news intel stats failed: {e}", e=str(exc))

    # Fallback: grab recent headlines
    try:
        engine = _get_db_engine()
        from intelligence.news_intel import get_news_feed
        feed = get_news_feed(engine, hours=12)
        if feed:
            lines = ["Recent news:"]
            for item in feed[:5]:
                if isinstance(item, dict):
                    headline = item.get("title", item.get("headline", "?"))
                    sent = item.get("sentiment", "")
                    lines.append(f"  - [{sent}] {headline}")
            return "\n".join(lines), "news_intel"
    except Exception as exc:
        log.debug("Chat context: news intel feed fallback failed: {e}", e=str(exc))
    return "", ""


def _gather_thesis() -> tuple[str, str]:
    """Return current thesis state and conviction."""
    try:
        engine = _get_db_engine()
        from intelligence.thesis_tracker import get_thesis_history
        history = get_thesis_history(engine, days=7)
        if history:
            latest = history[0] if isinstance(history, list) else history
            if isinstance(latest, dict):
                direction = latest.get("direction", latest.get("thesis_direction", "?"))
                conviction = latest.get("conviction", latest.get("confidence", "?"))
                drivers = latest.get("key_drivers", latest.get("drivers", []))
                lines = [f"Current thesis: {direction} (conviction: {conviction})"]
                if drivers:
                    lines.append(f"  Key drivers: {', '.join(str(d) for d in drivers[:5])}")
                return "\n".join(lines), "thesis_tracker"
            elif hasattr(latest, "direction"):
                lines = [f"Current thesis: {latest.direction} (conviction: {getattr(latest, 'conviction', '?')})"]
                return "\n".join(lines), "thesis_tracker"
    except Exception as exc:
        log.debug("Chat context: thesis tracker failed: {e}", e=str(exc))
    return "", ""


def _gather_money_flows() -> tuple[str, str]:
    """Return money flow summary across layers."""
    try:
        engine = _get_db_engine()
        from analysis.money_flow_engine import build_flow_map
        flow_map = build_flow_map(engine)
        if flow_map:
            lines = ["Money flow summary:"]
            liq = getattr(flow_map, "global_liquidity_total", None)
            liq_change = getattr(flow_map, "global_liquidity_change_1m", None)
            policy = getattr(flow_map, "global_policy_score", None)
            narrative = getattr(flow_map, "narrative", None)
            if liq is not None:
                lines.append(f"  Global liquidity: ${liq:,.0f}B" if liq > 1000 else f"  Global liquidity: ${liq:,.0f}M")
            if liq_change is not None:
                lines.append(f"  Liquidity 1m change: {liq_change:+.1%}" if isinstance(liq_change, float) else f"  Liquidity 1m change: {liq_change}")
            if policy is not None:
                lines.append(f"  Policy score: {policy}")
            if narrative:
                lines.append(f"  Narrative: {narrative}")
            return "\n".join(lines), "money_flow_engine"
    except Exception as exc:
        log.debug("Chat context: money flow engine failed: {e}", e=str(exc))
    return "", ""


def _gather_deep_dive() -> tuple[str, str]:
    """Return most recent deep dive insights."""
    try:
        engine = _get_db_engine()
        from intelligence.deep_dive import get_deep_dives
        dives = get_deep_dives(engine, days=3, limit=1)
        if dives:
            latest = dives[0] if isinstance(dives, list) else dives
            if isinstance(latest, dict):
                insights = latest.get("key_insights", [])
                contrarian = latest.get("contrarian_signals", [])
                blind_spots = latest.get("risk_blind_spots", [])
                lines = ["Latest deep dive:"]
                if insights:
                    lines.append("  Key insights: " + "; ".join(str(i) for i in insights[:3]))
                if contrarian:
                    lines.append("  Contrarian signals: " + "; ".join(str(c) for c in contrarian[:3]))
                if blind_spots:
                    lines.append("  Blind spots: " + "; ".join(str(b) for b in blind_spots[:3]))
                if len(lines) > 1:
                    return "\n".join(lines), "deep_dive"
    except Exception as exc:
        log.debug("Chat context: deep dive failed: {e}", e=str(exc))
    return "", ""


def _build_context_block(question: str, ticker: str | None) -> tuple[str, list[str]]:
    """Gather all context and return (context_text, list_of_sources)."""
    blocks: list[str] = []
    sources: list[str] = []

    gatherers = [
        _gather_regime_context,
        lambda: _gather_watchlist_context(ticker),
        _gather_cross_reference,
        _gather_convergence,
        _gather_lever_pullers,
        lambda: _gather_options_context(ticker),
        lambda: _gather_gex(ticker),
        lambda: _gather_predictions(ticker),
        _gather_news,
        _gather_thesis,
        _gather_money_flows,
        _gather_deep_dive,
        lambda: _research_chain(question, ticker),
    ]

    for fn in gatherers:
        try:
            text, source = fn()
            if text:
                blocks.append(text)
            if source:
                sources.append(source)
        except Exception as exc:
            log.debug("Context gather failed: {e}", e=str(exc))

    return "\n\n".join(blocks), sources


# ── LLM interaction ─────────────────────────────────────────────────────

GRID_SYSTEM_CONTEXT = """You are GRID Intelligence — a synthesis engine, not a chatbot. You have 50+ live data feeds, 10 thesis models, an oracle prediction system with scored track records, news sentiment, money flow maps, deep dive analyses, lever-puller tracking, and cross-reference lie detection.

YOUR JOB: Take the live GRID context below, weigh it, synthesize it, and deliver a conclusion. You are not summarizing — you are ANALYZING. The user already has the raw data. They need YOU to connect the dots.

DATA HIERARCHY (how to weigh signals):
1. MONEY FLOWS trump narrative. Where dollars actually move > what people say.
2. INSIDER ACTIONS trump analyst opinions. What actors DO > what they SAY.
3. CONVERGENCE beats any single signal. When 3+ independent sources agree, that's the read.
4. ORACLE TRACK RECORD matters. If a model has 70% accuracy, weight it. If it's at 40%, discount it. Cite the track record.
5. CROSS-REFERENCE RED FLAGS are high priority. When official data contradicts physical reality, that's the story.
6. REGIME determines strategy. Bull regime + bearish signal = potential dip buy. Bear regime + bullish signal = dead cat bounce. Always frame within regime.
7. GEX/OPTIONS POSITIONING is the near-term driver. Gamma flip levels, dealer positioning, and whale flow tell you what happens THIS WEEK.
8. NEWS SENTIMENT is a lagging indicator. Use it to confirm, not to lead.

RESPONSE FORMAT (MANDATORY):
1. SYNTHESIS FIRST: One paragraph. What is happening, why, and what it means. This is your read. Own it.
2. EVIDENCE: Bullet the specific data points from GRID context that support your read. Cite the source (oracle, thesis, news, flows, etc.).
3. CONFLICTS: If signals disagree, say so. "Flows say X but news says Y — I weight flows because [reason]."
4. ACTION CALLS: End with 1-3 specific, actionable items. Price levels, tickers, triggers, timeframes. Not "monitor the situation" — that's useless.

BANNED PHRASES (will get you fired):
- "It's important to note..."
- "While I can't predict..."
- "This is not financial advice"
- "Past performance doesn't guarantee..."
- "I hope this helps"
- "Let me know if you need..."
- "Consider monitoring..."
- "Please note that..."
- "It's worth mentioning..."
- "As always, do your own research"
- Any variation of the above

CRITICAL DATA INTEGRITY RULES:
- NEVER make up prices, levels, or numbers. If you don't have the specific price for a ticker, say "I don't have current price data for X" — do NOT guess.
- ONLY cite data that appears in the GRID Context section below. If it's not in the context, you don't know it.
- If the user asks about a specific ticker and you have no ticker-specific data, say so explicitly and give them what you DO have (macro, regime, flows).
- Wrong data is worse than no data. Silence is better than hallucination.

You are an intelligence analyst delivering a briefing, not a customer service chatbot. Be direct. Be specific. Be useful or be quiet."""

# Build the system prompt: static context + dynamic codebase state
def _build_system_prompt() -> str:
    """Combine static GRID context with live codebase state."""
    parts = [GRID_SYSTEM_CONTEXT]
    try:
        from intelligence.codebase_context import get_system_context
        live = get_system_context()
        if live:
            parts.append(live)
    except Exception as exc:
        log.debug("Chat: codebase context fetch failed: {e}", e=str(exc))
    return "\n\n".join(parts)



def _get_llm_client():
    """Get best available LLM client: llamacpp first, then ollama."""
    # Use the LLM router (handles fallback chain automatically)
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.REASON)
        if client.is_available:
            return client, "router"
    except Exception as exc:
        log.debug("Chat: LLM router unavailable: {e}", e=str(exc))

    # Direct ollama fallback
    try:
        from ollama.client import get_client as get_ollama
        client = get_ollama()
        if client.is_available:
            return client, "ollama"
    except Exception as exc:
        log.debug("Chat: ollama client unavailable: {e}", e=str(exc))

    return None, None


def _build_rule_based_response(context_text: str, question: str, sources: list[str]) -> str:
    """Generate a structured response from raw context when no LLM is available."""
    if not context_text.strip():
        return (
            "I don't have enough live data to answer that right now. "
            "The system may still be loading context from its data sources."
        )

    lines = ["Based on current GRID data:\n"]
    # Just return the context blocks as a structured answer
    for block in context_text.split("\n\n"):
        block = block.strip()
        if block:
            lines.append(block)

    q_lower = question.lower()
    if any(w in q_lower for w in ("watch", "alert", "attention", "focus")):
        lines.append(
            "\nFocus on any red flags and convergence events listed above."
        )
    elif any(w in q_lower for w in ("regime", "state", "phase", "cycle")):
        lines.append(
            "\nThe regime state drives strategy selection and position sizing."
        )
    elif any(w in q_lower for w in ("option", "vol", "gamma", "gex")):
        lines.append(
            "\nReview the options positioning and GEX data above for vol context."
        )

    return "\n".join(lines)


# ── Research chain — active investigation for user queries ─────────────

def _research_chain(question: str, ticker: str | None) -> tuple[str, str]:
    """Fire the Sleuth engine to actively investigate a user question.

    Creates an ad-hoc lead from the user's question, investigates it
    (LLM + data), and returns synthesized findings. This turns the
    chatbot from a passive context reader into an active researcher.

    Returns (findings_text, source_label).
    """
    try:
        from intelligence.sleuth import Sleuth, Lead
        engine = _get_db_engine()
        sleuth = Sleuth(engine)

        # Create an ad-hoc lead from the user's question
        import uuid as _uuid
        lead = Lead(
            id=f"chat-{_uuid.uuid4().hex[:12]}",
            question=question[:500],
            category="connection_found" if ticker else "data_anomaly",
            priority=0.9,  # user queries are high priority
            evidence=[{
                "source": "user_query",
                "ticker": ticker,
                "question": question,
            }],
        )

        # Investigate (LLM + data gathering + context)
        investigated = sleuth.investigate_lead(lead)

        if investigated.findings and investigated.findings != "LLM unavailable — investigation deferred.":
            lines = ["Research findings:"]
            lines.append(f"  Conclusion: {investigated.findings}")

            if investigated.hypotheses:
                lines.append("  Hypotheses:")
                for h in investigated.hypotheses[:3]:
                    if isinstance(h, dict):
                        lines.append(f"    - {h.get('hypothesis', '?')} ({h.get('confidence', '?')} confidence)")

            if investigated.follow_up_leads:
                # Load follow-up questions for additional context
                for fu_id in investigated.follow_up_leads[:2]:
                    child = sleuth._load_lead(fu_id)
                    if child:
                        lines.append(f"  Follow-up: {child.question}")

            return "\n".join(lines), "sleuth/investigation"
    except Exception as exc:
        log.debug("Research chain failed: {e}", e=str(exc))

    return "", ""


# ── TimesFM background trigger ─────────────────────────────────────────

_timesfm_last_run: dict[str, datetime] = {}
_timesfm_lock = threading.Lock()
_TIMESFM_COOLDOWN_HOURS = 6


def _maybe_trigger_timesfm(ticker: str) -> None:
    """Fire a background TimesFM forecast for a ticker if not run recently."""
    if not ticker:
        return
    now = datetime.now(timezone.utc)
    with _timesfm_lock:
        last = _timesfm_last_run.get(ticker)
        if last and (now - last).total_seconds() < _TIMESFM_COOLDOWN_HOURS * 3600:
            return
        _timesfm_last_run[ticker] = now

    def _run():
        try:
            from timeseries.timesfm_forecaster import get_forecaster
            forecaster = get_forecaster()
            if not forecaster.is_available:
                return

            engine = _get_db_engine()
            from sqlalchemy import text as sa_text
            import pandas as pd

            # Fetch price series for the ticker
            with engine.connect() as conn:
                rows = conn.execute(sa_text(
                    "SELECT date, value FROM resolved_series "
                    "WHERE series_id = :sid ORDER BY date DESC LIMIT 512"
                ), {"sid": f"price_{ticker.lower()}"}).fetchall()

            if not rows or len(rows) < 30:
                return

            series = pd.Series(
                [float(r[1]) for r in reversed(rows)],
                index=[r[0] for r in reversed(rows)],
            )

            result = forecaster.forecast(
                series=series.values,
                horizon=20,
                frequency="daily",
                series_id=f"price_{ticker.lower()}",
            )
            log.info(
                "TimesFM background forecast completed for {t}: {h}d horizon, "
                "latest prediction={p:.2f}",
                t=ticker, h=result.horizon,
                p=result.predictions[-1] if result.predictions else 0,
            )
        except Exception as exc:
            log.debug("TimesFM background trigger failed for {t}: {e}", t=ticker, e=str(exc))

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()


# ── Main endpoint ───────────────────────────────────────────────────────

@router.post("/ask", response_model=ChatAskResponse)
async def ask_grid(req: ChatAskRequest) -> ChatAskResponse:
    """Conversational Q&A with full GRID context.

    Gathers regime, watchlist, cross-reference, trust, lever-puller,
    options, and GEX context.  Sends to LLM with conversation history.
    Falls back to rule-based response when no LLM is online.
    """
    now = datetime.now(timezone.utc)
    question = req.question.strip()
    ticker = req.context_ticker.strip().upper() if req.context_ticker else None
    timeframe = req.timeframe  # "1d", "1w", "1m", "3m", "6m"

    # 0. Fire background TimesFM forecast if ticker specified
    if ticker:
        _maybe_trigger_timesfm(ticker)

    # 1. Gather context
    context_text, sources = _build_context_block(question, ticker)
    confidence = 0.5  # base

    # 1a. Post-query data gap scan (async, non-blocking)
    try:
        from intelligence.post_query_scanner import spawn_post_query_scan
        from db import get_engine
        spawn_post_query_scan(get_engine(), question, ticker, sources)
    except Exception as scan_exc:
        log.debug("Post-query scan init failed: {e}", e=str(scan_exc))

    # 2. Try LLM
    client, backend = _get_llm_client()
    if client is not None:
        # Build messages
        system_content = _build_system_prompt()
        if context_text:
            system_content += f"\n\n## Current GRID Context\n\n{context_text}"

        # Add timeframe instruction if specified
        if timeframe:
            tf_map = {
                "1d": "Focus on TODAY and the next 24 hours. Intraday signals, GEX levels, options expiry, news catalysts. What happens by market close tomorrow.",
                "1w": "Focus on THIS WEEK. Near-term catalysts, earnings, FOMC, options expiry cycles, dealer gamma positioning. What happens in the next 5 trading days.",
                "1m": "Focus on the NEXT MONTH. Macro regime, sector rotation, institutional flows, 13F positioning. Medium-term thesis. What plays out over 20 trading days.",
                "3m": "Focus on the NEXT QUARTER. Macro cycles, Fed policy trajectory, earnings season trends, sector momentum. What plays out over 60 trading days.",
                "6m": "Focus on the NEXT 6+ MONTHS. Secular trends, regime changes, structural shifts, long-term positioning. What plays out over 120+ trading days.",
            }
            tf_instruction = tf_map.get(timeframe, "")
            if tf_instruction:
                system_content += f"\n\n## Timeframe: {timeframe}\n{tf_instruction}"

        messages: list[dict[str, str]] = [
            {"role": "system", "content": system_content},
        ]

        # Append conversation history (last 10 turns max, roles restricted)
        _ALLOWED_ROLES = {"user", "assistant"}
        for msg in req.history[-10:]:
            role = msg.role if msg.role in _ALLOWED_ROLES else "user"
            messages.append({"role": role, "content": msg.content[:4000]})

        # Append current question
        messages.append({"role": "user", "content": question})

        try:
            answer = client.chat(
                messages,
                temperature=0.3,
                num_predict=2000,
            )
            if answer:
                sources.append(f"llm/{backend}")
                confidence = 0.75 if context_text else 0.5
                model_used = getattr(client, "model", backend)

                # A/B test: fire Opus in background for comparison
                answer_b = None
                model_b = None
                try:
                    from config import settings
                    or_key = getattr(settings, "OPENROUTER_API_KEY", "")
                    if or_key:
                        from llm.router import OpenAIClient
                        opus_client = OpenAIClient(
                            api_key=or_key,
                            base_url="https://openrouter.ai/api/v1",
                            model="anthropic/claude-opus-4",
                            timeout=120,
                        )
                        answer_b = opus_client.chat(
                            messages,
                            temperature=0.3,
                            num_predict=2000,
                        )
                        model_b = "anthropic/claude-opus-4"
                except Exception as ab_exc:
                    log.debug("A/B Opus call failed: {e}", e=str(ab_exc))

                return ChatAskResponse(
                    answer=answer,
                    sources_used=sources,
                    confidence=confidence,
                    generated_at=now.isoformat(),
                    model_used=model_used,
                    answer_b=answer_b,
                    model_b=model_b,
                )
        except Exception as exc:
            log.warning("LLM chat failed, falling back to rule-based: {e}", e=str(exc))

    # 3. Fallback: rule-based
    answer = _build_rule_based_response(context_text, question, sources)
    sources.append("rule_based")
    confidence = 0.3 if context_text else 0.1

    return ChatAskResponse(
        answer=answer,
        sources_used=sources,
        confidence=confidence,
        generated_at=now.isoformat(),
    )
