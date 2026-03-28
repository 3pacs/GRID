"""
GRID API — Ask GRID conversational chat endpoint.

Gathers system context (regime, watchlist, cross-reference, trust scores,
lever-puller activity, options, GEX) and sends a structured prompt to the
LLM (llamacpp -> ollama fallback).  Falls back to rule-based summaries
when no LLM is available.

  POST /api/v1/chat/ask  — conversational question with optional history
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from loguru import logger as log
from pydantic import BaseModel, Field

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


class ChatAskRequest(BaseModel):
    question: str
    context_ticker: str | None = None
    history: list[ChatMessage] = Field(default_factory=list)


class ChatAskResponse(BaseModel):
    answer: str
    sources_used: list[str]
    confidence: float
    generated_at: str


# ── Helpers: gather context from various GRID subsystems ────────────────

def _gather_regime_context() -> tuple[str, str]:
    """Return (text_block, source_label) for current regime."""
    try:
        from journal.log import get_latest_regime  # type: ignore[import]
        regime = get_latest_regime()
        if regime:
            return (
                f"Current regime: {regime.get('state', 'unknown')} "
                f"(confidence {regime.get('confidence', '?')}, "
                f"as of {regime.get('recorded_at', '?')})",
                "decision_journal/regime",
            )
    except Exception:
        pass

    try:
        from discovery.clustering import load_latest_regime  # type: ignore[import]
        regime = load_latest_regime()
        if regime:
            return (
                f"Regime cluster: {regime.get('label', regime.get('cluster', '?'))}",
                "clustering/regime",
            )
    except Exception:
        pass

    return "", ""


def _gather_watchlist_context(ticker: str | None) -> tuple[str, str]:
    """Return enriched watchlist data for a specific ticker."""
    if not ticker:
        return "", ""
    try:
        from api.routers.watchlist import _get_enriched_item  # type: ignore[import]
        item = _get_enriched_item(ticker)
        if item:
            parts = [f"Ticker {ticker}:"]
            for k in ("price", "pct_1d", "pct_5d", "sector", "market_cap",
                       "pe_ratio", "rsi_14", "sma_50_dist", "sma_200_dist"):
                if k in item and item[k] is not None:
                    parts.append(f"  {k}: {item[k]}")
            return "\n".join(parts), f"watchlist/{ticker}"
    except Exception:
        pass

    # Fallback: try overview endpoint logic
    try:
        from api.routers.watchlist import _ticker_overview  # type: ignore[import]
        ov = _ticker_overview(ticker)
        if ov:
            return f"Ticker {ticker}: {ov}", f"watchlist/{ticker}"
    except Exception:
        pass

    return "", ""


def _gather_cross_reference() -> tuple[str, str]:
    """Return latest cross-reference red flags."""
    try:
        from intelligence.cross_reference import CrossReferenceEngine  # type: ignore[import]
        engine = CrossReferenceEngine()
        results = engine.run()
        if results and isinstance(results, dict):
            flags = results.get("red_flags", results.get("flags", []))
            if flags:
                lines = ["Cross-reference red flags:"]
                for f in flags[:5]:
                    if isinstance(f, dict):
                        lines.append(f"  - {f.get('indicator', '?')}: {f.get('description', f.get('detail', '?'))}")
                    else:
                        lines.append(f"  - {f}")
                return "\n".join(lines), "cross_reference"
    except Exception:
        pass
    return "", ""


def _gather_convergence() -> tuple[str, str]:
    """Return active convergence events from trust scorer."""
    try:
        from intelligence.trust_scorer import TrustScorer  # type: ignore[import]
        scorer = TrustScorer()
        events = scorer.get_convergence_events()
        if events:
            lines = ["Active convergence events:"]
            for ev in events[:5]:
                if isinstance(ev, dict):
                    lines.append(f"  - {ev.get('description', ev.get('event', str(ev)))}")
                else:
                    lines.append(f"  - {ev}")
            return "\n".join(lines), "trust_scorer/convergence"
    except Exception:
        pass
    return "", ""


def _gather_lever_pullers() -> tuple[str, str]:
    """Return recent lever-puller activity."""
    try:
        from intelligence.lever_pullers import get_recent_activity  # type: ignore[import]
        activity = get_recent_activity(limit=5)
        if activity:
            lines = ["Recent lever-puller activity:"]
            for a in activity:
                if isinstance(a, dict):
                    actor = a.get("actor", a.get("name", "?"))
                    action = a.get("action", a.get("description", "?"))
                    lines.append(f"  - {actor}: {action}")
                else:
                    lines.append(f"  - {a}")
            return "\n".join(lines), "lever_pullers"
    except Exception:
        pass
    return "", ""


def _gather_options_context(ticker: str | None) -> tuple[str, str]:
    """Return options positioning data."""
    try:
        from discovery.options_scanner import OptionsScanner  # type: ignore[import]
        scanner = OptionsScanner()
        signals = scanner.get_latest_signals(ticker=ticker, limit=5)
        if signals:
            lines = ["Options positioning:"]
            for s in signals[:5]:
                if isinstance(s, dict):
                    t = s.get("ticker", "?")
                    desc = s.get("signal", s.get("description", "?"))
                    score = s.get("score", "?")
                    lines.append(f"  - {t}: {desc} (score: {score})")
                else:
                    lines.append(f"  - {s}")
            return "\n".join(lines), "options_scanner"
    except Exception:
        pass
    return "", ""


def _gather_gex(ticker: str | None) -> tuple[str, str]:
    """Return GEX regime data."""
    target = ticker or "SPY"
    try:
        from physics.dealer_gamma import get_gex_profile  # type: ignore[import]
        gex = get_gex_profile(target)
        if gex and isinstance(gex, dict):
            regime = gex.get("regime", "?")
            net_gex = gex.get("net_gex", gex.get("total_gex", "?"))
            flip = gex.get("gamma_flip", gex.get("flip_strike", "?"))
            return (
                f"GEX regime ({target}): {regime}, net GEX: {net_gex}, "
                f"gamma flip: {flip}",
                f"gex/{target}",
            )
    except Exception:
        pass
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

SYSTEM_PROMPT = (
    "You are GRID, a trading intelligence system. You answer questions about "
    "the market, portfolio positioning, and the intelligence picture. Be "
    "concise, data-driven, and precise. Use specific numbers when available. "
    "Cite your sources (regime state, watchlist data, options flow, etc). "
    "If you are uncertain, say so. Never fabricate data."
)


def _get_llm_client():
    """Get best available LLM client: llamacpp first, then ollama."""
    # Try llama.cpp first
    try:
        from llamacpp.client import get_client as get_llamacpp
        client = get_llamacpp()
        if client.is_available:
            return client, "llamacpp"
    except Exception:
        pass

    # Fallback to ollama
    try:
        from ollama.client import get_client as get_ollama
        client = get_ollama()
        if client.is_available:
            return client, "ollama"
    except Exception:
        pass

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

    # 1. Gather context
    context_text, sources = _build_context_block(question, ticker)
    confidence = 0.5  # base

    # 2. Try LLM
    client, backend = _get_llm_client()
    if client is not None:
        # Build messages
        system_content = SYSTEM_PROMPT
        if context_text:
            system_content += f"\n\n## Current GRID Context\n\n{context_text}"

        messages: list[dict[str, str]] = [
            {"role": "system", "content": system_content},
        ]

        # Append conversation history (last 10 turns max)
        for msg in req.history[-10:]:
            messages.append({"role": msg.role, "content": msg.content})

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
                return ChatAskResponse(
                    answer=answer,
                    sources_used=sources,
                    confidence=confidence,
                    generated_at=now.isoformat(),
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
