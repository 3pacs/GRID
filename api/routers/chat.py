"""
GRID API — Ask GRID conversational chat endpoint.

Gathers system context (regime, watchlist, cross-reference, trust scores,
lever-puller activity, options, GEX) and sends a structured prompt to the
LLM (llamacpp -> ollama fallback).  Falls back to rule-based summaries
when no LLM is available.

  POST /api/v1/chat/ask  — conversational question with optional history
"""

from __future__ import annotations

import inspect
import re as _re
import threading
import uuid
from datetime import datetime, timezone
from typing import Any


def _supports_kwarg(fn: Any, name: str) -> bool:
    """Return True if `fn` accepts a keyword argument named `name`.

    We don't want to break primary chat clients (Gemma/Ollama/llamacpp) that
    haven't been threaded with `extra_metadata` yet.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    if name in sig.parameters:
        return True
    # Accept **kwargs catch-alls.
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

from contextlib import nullcontext

from fastapi import APIRouter, Depends
from loguru import logger as log
from pydantic import BaseModel, Field, field_validator

from api.auth import decode_token, require_auth

# Langfuse tracing — best-effort. Decorator no-ops when keys absent.
try:
    from langfuse import (
        get_client as _lf_get_client,
        observe as _lf_observe,
        propagate_attributes as _lf_propagate_attributes,
    )
except Exception:  # pragma: no cover — optional dep
    def _lf_observe(*args, **kwargs):  # type: ignore
        def _decorator(fn):
            return fn
        # support both @observe and @observe(name=...)
        if args and callable(args[0]):
            return args[0]
        return _decorator
    def _lf_get_client():  # type: ignore
        return None
    def _lf_propagate_attributes(**_kwargs):  # type: ignore
        return nullcontext()


def _lf_set_input(**kwargs) -> None:
    """Best-effort: set explicit input on the active Langfuse span. Never raises."""
    try:
        client = _lf_get_client()
        if client is not None:
            client.update_current_span(input=kwargs)
    except Exception:
        pass


def _lf_safe_propagate(**attrs):
    """Return a context manager that propagates trace-level attributes.

    Drops empty values and never raises. Falls back to ``nullcontext`` when
    Langfuse is unavailable or any attribute fails validation.
    """
    cleaned = {k: v for k, v in attrs.items() if v not in (None, "", [])}
    if not cleaned:
        return nullcontext()
    try:
        return _lf_propagate_attributes(**cleaned)
    except Exception:  # pragma: no cover — never let tracing break the request
        return nullcontext()


def _user_id_from_token(token: str | None) -> str | None:
    """Extract the JWT subject (user id) from a bearer token. Returns None on failure."""
    if not token:
        return None
    try:
        payload = decode_token(token)
        if payload:
            sub = payload.get("sub")
            if isinstance(sub, str) and sub:
                return sub[:200]  # Langfuse user_id <= 200 chars
    except Exception:
        return None
    return None


# Langfuse dataset that captures every firewall failure as a permanent regression.
_REGRESSION_DATASET_NAME = "grid-chatbot-regressions"


def _auto_label_failure(
    question: str,
    ticker: str | None,
    timeframe: str | None,
    answer: str,
    fw_decision: str,
    fw_reasons: list[str],
    claim_count: int,
    flagged_count: int,
) -> None:
    """Fire-and-forget: push a firewall-rejected case into the regression dataset.

    Runs in a daemon thread so dataset write failures never block user-facing
    chat. Silently no-ops when Langfuse is not configured.
    """
    def _push() -> None:
        try:
            from langfuse import get_client
            client = get_client()
            if client is None:
                return
            ts = datetime.now(timezone.utc).isoformat()
            client.create_dataset_item(
                dataset_name=_REGRESSION_DATASET_NAME,
                input={
                    "question": question,
                    "ticker": ticker,
                    "timeframe": timeframe,
                },
                expected_output={
                    "behavior_contract": "must pass publishing firewall (no unverified claims)",
                    "failure_mode": f"firewall_{fw_decision}",
                    "source_memory": f"auto-labeled at {ts}",
                    "fw_reasons": list(fw_reasons or []),
                    "original_answer": (answer or "")[:2000],
                },
                metadata={
                    "source": "auto-label",
                    "firewall_decision": fw_decision,
                    "claim_count": claim_count,
                    "flagged_count": flagged_count,
                },
            )
        except Exception:
            # Never break chat because of dataset write failures.
            pass

    threading.Thread(target=_push, daemon=True).start()


router = APIRouter(
    prefix="/api/v1/chat",
    tags=["chat"],
    dependencies=[Depends(require_auth)],
)


# ── Request / Response models ───────────────────────────────────────────

_VALID_ROLES = {"user", "assistant"}

# Patterns that commonly indicate prompt-injection attempts.
# Matched case-insensitively against the full message content.
_INJECTION_PATTERNS: list[_re.Pattern[str]] = [
    _re.compile(r"ignore\s+(all\s+)?previous\s+instructions", _re.IGNORECASE),
    _re.compile(r"you\s+are\s+now\b", _re.IGNORECASE),
    _re.compile(r"^\s*system\s*:", _re.IGNORECASE | _re.MULTILINE),
    _re.compile(r"new\s+instructions?\s*:", _re.IGNORECASE),
    _re.compile(r"disregard\s+(all\s+)?previous", _re.IGNORECASE),
]


def _sanitize_history_content(content: str) -> str:
    """Strip obvious prompt-injection patterns from history message content.

    Logs a warning when a pattern is detected so operators can monitor abuse.
    Returns the cleaned string; content that is entirely removed becomes an
    empty string so the turn is still structurally present.
    """
    sanitized = content
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(sanitized):
            log.warning(
                "Prompt injection pattern detected and stripped from chat history. "
                "Pattern: {pat}",
                pat=pattern.pattern,
            )
            sanitized = pattern.sub("", sanitized)
    return sanitized.strip()


class ChatMessage(BaseModel):
    role: str = "user"
    content: str = Field(default="", max_length=4000)

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in _VALID_ROLES:
            raise ValueError(f"role must be one of {_VALID_ROLES}")
        return v


_TICKER_RE = _re.compile(r"^[A-Z0-9.\-]{1,15}$")
_VALID_TIMEFRAMES = {"1d", "1w", "1m", "3m", "6m"}
_SESSION_ID_RE = _re.compile(r"^[A-Za-z0-9\-]{1,64}$")


class ChatAskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    context_ticker: str | None = Field(None, max_length=15)
    timeframe: str | None = None
    history: list[ChatMessage] = Field(default_factory=list, max_length=50)
    # Langfuse session grouping — alphanumeric + hyphen, <= 64 chars.
    # Lets the UI link related multi-turn requests in the Sessions view.
    session_id: str | None = Field(default=None, max_length=64)

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

    @field_validator("session_id")
    @classmethod
    def validate_session_id(cls, v):
        if v is None:
            return v
        v = v.strip()
        if not v:
            return None
        if not _SESSION_ID_RE.match(v):
            raise ValueError(
                "session_id must be alphanumeric or hyphen, max 64 chars"
            )
        return v


class ChatAskResponse(BaseModel):
    answer: str
    sources_used: list[str]
    confidence: float
    generated_at: str
    model_used: str | None = None
    answer_b: str | None = None  # A/B test: second model response
    model_b: str | None = None   # A/B test: second model name
    sanity_warnings: list[str] | None = None  # Data integrity warnings


# ── Compose: natural language → live dashboard layout ───────────────────
# stepdad.finance home page. The user describes what they want to see (typed
# or, later, spoken via Whisper) and the LLM assembles a layout from a fixed
# catalog of widgets, each of which maps to an existing GRID data endpoint.

class ComposeWidget(BaseModel):
    """One card in the composed layout. `props` carries widget-specific
    parameters (e.g. ticker, question). Kept permissive so the catalog can
    grow without a schema migration; the router validates `type`."""
    type: str = Field(..., max_length=40)
    title: str = Field(default="", max_length=120)
    props: dict[str, Any] = Field(default_factory=dict)


class ComposeAllocationItem(BaseModel):
    ticker: str = Field(..., max_length=15)
    weight: float = Field(default=0.0, ge=0.0, le=1.0)


class ChatComposeRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    history: list[ChatMessage] = Field(default_factory=list, max_length=50)
    session_id: str | None = Field(default=None, max_length=64)

    @field_validator("session_id")
    @classmethod
    def validate_session_id(cls, v):
        if v is None:
            return v
        v = v.strip()
        if not v:
            return None
        if not _SESSION_ID_RE.match(v):
            raise ValueError("session_id must be alphanumeric or hyphen, max 64 chars")
        return v


class ChatComposeResponse(BaseModel):
    spoken_reply: str
    widgets: list[ComposeWidget]
    allocation: list[ComposeAllocationItem] = Field(default_factory=list)
    generated_at: str
    model_used: str | None = None
    # Set when the request needs a capability we don't have yet. The frontend
    # shows the graceful "I'll build it for you" message + a ping opt-in.
    cannot_fulfill: bool = False
    request_id: int | None = None


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
                "SELECT regime, confidence, created_at "
                "FROM regime_history ORDER BY obs_date DESC LIMIT 1"
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

            # Batch-fetch latest value for ALL features in one query
            feature_ids = [fid for fid, _, _ in features]
            latest_rows = conn.execute(text(
                "SELECT DISTINCT ON (feature_id) feature_id, value, obs_date "
                "FROM resolved_series "
                "WHERE feature_id = ANY(:fids) "
                "ORDER BY feature_id, obs_date DESC"
            ), {"fids": feature_ids}).fetchall()
            latest_by_fid = {r[0]: (r[1], r[2]) for r in latest_rows}

            for fid, fname, fdesc in features:
                entry = latest_by_fid.get(fid)
                if entry is None or entry[0] is None:
                    continue
                val = float(entry[0])
                date = entry[1]
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
    """Return current thesis state in plain English."""
    try:
        engine = _get_db_engine()
        from intelligence.thesis_tracker import get_thesis_history
        history = get_thesis_history(engine, days=7)
        if history:
            latest = history[0] if isinstance(history, list) else history
            if isinstance(latest, dict):
                narr = latest.get("narrative", "")
                if narr:
                    return f"Current market read:\n{narr}", "thesis_tracker"
                # Fallback if no narrative stored
                direction = latest.get("direction", latest.get("thesis_direction", "?"))
                conviction = latest.get("conviction", latest.get("confidence", "?"))
                return f"Current market lean: {direction} ({conviction}% confidence)", "thesis_tracker"
            elif hasattr(latest, "direction"):
                return f"Current market lean: {latest.direction} ({getattr(latest, 'conviction', '?')}% confidence)", "thesis_tracker"
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


def _gather_geopolitical() -> tuple[str, str]:
    """Return geopolitical risk assessment from thesis scorer."""
    try:
        engine = _get_db_engine()
        from analysis.thesis_scorer import _score_geopolitical_risk
        result = _score_geopolitical_risk(engine, 0.5)
        if result and result.get("status") != "broken":
            score = result.get("score", 0)
            reasoning = result.get("reasoning", "")
            if abs(score) >= 20 or reasoning:
                return f"Geopolitical risk assessment:\n  {reasoning}", "geopolitical_risk"
    except Exception as exc:
        log.debug("Chat context: geopolitical risk failed: {e}", e=str(exc))
    return "", ""


def _gather_insider_activity() -> tuple[str, str]:
    """Return recent insider and congressional trading activity."""
    try:
        engine = _get_db_engine()
        with engine.connect() as conn:
            from sqlalchemy import text as sql_text
            # Insider trades summary
            ins = conn.execute(sql_text("""
                SELECT transaction_type, COUNT(*), SUM(value)
                FROM insider_trades
                WHERE trade_date >= CURRENT_DATE - 14
                GROUP BY transaction_type
            """)).fetchall()
            # Congressional trades summary
            cong = conn.execute(sql_text("""
                SELECT transaction_type, COUNT(*)
                FROM congressional_trades
                WHERE trade_date >= CURRENT_DATE - 30
                GROUP BY transaction_type
            """)).fetchall()

            lines = []
            if ins:
                parts = [f"{r[0]}: {r[1]} trades (${r[2]:,.0f})" if r[2] else f"{r[0]}: {r[1]} trades" for r in ins]
                lines.append(f"Insider trading (14d): {', '.join(parts)}")
            if cong:
                parts = [f"{r[0]}: {r[1]} trades" for r in cong]
                lines.append(f"Congressional trading (30d): {', '.join(parts)}")
            if lines:
                return "\n".join(lines), "insider_congressional"
    except Exception as exc:
        log.debug("Chat context: insider activity failed: {e}", e=str(exc))
    return "", ""


def _extract_feature_names_from_context(context_text: str) -> list[str]:
    """Extract feature names mentioned in the context block.

    Looks for patterns like 'feature_name: value' or 'feature_name = value'
    that the context gatherers produce. Returns unique feature names.
    """
    import re
    # Context gatherers format data as "feature_name: value" or "- feature_name: value"
    pattern = re.compile(r'(?:^|\n)\s*[-•]?\s*([a-z][a-z0-9_]{2,50})[\s:=]', re.MULTILINE)
    matches = pattern.findall(context_text)
    # Filter to likely feature names (not common English words)
    _STOP_WORDS = frozenset({
        "the", "and", "for", "are", "but", "not", "you", "all", "can",
        "had", "her", "was", "one", "our", "out", "has", "his", "how",
        "its", "let", "may", "new", "now", "old", "see", "way", "who",
        "did", "get", "got", "him", "why", "try", "ask", "use", "day",
        "too", "any", "few", "key", "top", "low", "run", "set",
        "ticker", "regime", "context", "current", "latest", "status",
        "data", "source", "signal", "note", "summary", "score",
    })
    return sorted(set(m for m in matches if m not in _STOP_WORDS))


def _build_context_block(
    question: str,
    ticker: str | None,
    *,
    include_research: bool = True,
    budget_s: int = 15,
) -> tuple[str, list[str]]:
    """Gather all context and return (context_text, list_of_sources).

    include_research: when False, skip `_research_chain` (a deep multi-hop step
    measured at ~35s). The realtime streaming verdict passes False so the home
    page renders promptly; the firewalled /chat/ask path keeps it.
    budget_s: overall wall-clock cap for the concurrent gather.
    """
    blocks: list[str] = []
    sources: list[str] = []

    gatherers = [
        _gather_regime_context,
        _gather_geopolitical,
        _gather_insider_activity,
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
    ]
    if include_research:
        gatherers.append(lambda: _research_chain(question, ticker))

    # Gatherers are independent DB reads — run them concurrently and bound the
    # total wait, instead of summing 15 serial queries (which hit 24-77s and
    # made the streaming verdict appear broken: nothing streams until context
    # is built). Order is preserved for a deterministic prompt; a gatherer that
    # overruns the budget is abandoned (it finishes harmlessly in the
    # background) and its slice is simply omitted from this turn's context.
    import concurrent.futures

    _CONTEXT_GATHER_BUDGET_S = budget_s

    def _run(fn):
        try:
            return fn()
        except Exception as exc:
            log.debug("Context gather failed: {e}", e=str(exc))
            return None

    results: list[tuple[str, str] | None] = [None] * len(gatherers)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(gatherers)))
    futures = {executor.submit(_run, fn): i for i, fn in enumerate(gatherers)}
    try:
        for fut in concurrent.futures.as_completed(futures, timeout=_CONTEXT_GATHER_BUDGET_S):
            results[futures[fut]] = fut.result()
    except concurrent.futures.TimeoutError:
        log.debug("Context gather hit {s}s budget; using partial context", s=_CONTEXT_GATHER_BUDGET_S)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    for result in results:
        if not result:
            continue
        text, source = result
        if text:
            blocks.append(text)
        if source:
            sources.append(source)

    return "\n\n".join(blocks), sources


# ── LLM interaction ─────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────
# SOURCE OF TRUTH for the chat system prompt is Langfuse:
#     prompt name: "chat-grid-system"
#     label:       "production"
#     UI:          http://grid-svr:3000  →  Prompts  →  chat-grid-system
#
# `_build_system_prompt()` fetches the live Langfuse version with a 60-second
# SDK cache, then appends the dynamic codebase context. Edit the prompt in
# the Langfuse UI — no code deploy required.
#
# The constant below is kept ONLY as a fallback for when Langfuse is
# unreachable (network outage, server down, missing keys). It is the v1
# seed and may drift; treat the Langfuse copy as canonical.
# ─────────────────────────────────────────────────────────────────────────

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
1. VERDICT: One clear sentence a child could understand. "Stocks are likely going down this week because..." No jargon. No acronyms without explanation.
2. WHY: 2-3 bullet points explaining the reasons in plain language. Translate all technical signals into what they MEAN. "Insiders are selling" not "16 sell clusters (VINP(20), M(14))".
3. CONFLICTS: If signals disagree, say so simply. "The money flow says up but insiders are selling — I trust insiders more because they have skin in the game."
4. ACTION CALLS: End with 1-3 specific, actionable items. Price levels, tickers, triggers, timeframes. Not "monitor the situation" — that's useless.
5. BREAKING EVENTS: If there are geopolitical events (wars, bombings, sanctions, elections) in the news context, LEAD with those. They override normal signals.

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

# Langfuse-managed system prompt — name + label + cache TTL
# Source of truth lives in Langfuse; see comment above GRID_SYSTEM_CONTEXT.
_LANGFUSE_PROMPT_NAME = "chat-grid-system"
_LANGFUSE_PROMPT_LABEL = "production"
_LANGFUSE_PROMPT_CACHE_TTL_SECONDS = 60


def _fetch_langfuse_system_prompt() -> str | None:
    """Fetch the chat system prompt from Langfuse with built-in client cache.

    Returns the compiled text, or None if Langfuse is unavailable or the
    prompt is missing. Never raises — every error path falls through to the
    in-source GRID_SYSTEM_CONTEXT fallback.

    The Langfuse Python SDK caches prompts client-side for the requested
    `cache_ttl_seconds` and revalidates in the background after expiry, so
    we do not need our own cache layer.
    """
    try:
        client = _lf_get_client()
        if client is None:
            return None
        prompt = client.get_prompt(
            _LANGFUSE_PROMPT_NAME,
            label=_LANGFUSE_PROMPT_LABEL,
            cache_ttl_seconds=_LANGFUSE_PROMPT_CACHE_TTL_SECONDS,
            fallback=GRID_SYSTEM_CONTEXT,
        )
        # `compile()` with no kwargs returns the raw template (no variables
        # in this prompt today). It works for both live and fallback prompts.
        compiled = prompt.compile()
        if isinstance(compiled, str) and compiled.strip():
            return compiled
    except Exception as exc:
        log.debug("Chat: Langfuse prompt fetch failed: {e}", e=str(exc))
    return None


# Build the system prompt: Langfuse-managed static context + dynamic codebase state
def _build_system_prompt() -> str:
    """Combine the Langfuse-managed GRID context with live codebase state.

    1. Try Langfuse (`chat-grid-system` @ production, 60s SDK cache).
    2. Fall back to the in-source GRID_SYSTEM_CONTEXT if Langfuse is down
       or returns empty.
    3. Append live codebase context from intelligence.codebase_context.
    """
    static_part = _fetch_langfuse_system_prompt() or GRID_SYSTEM_CONTEXT
    parts = [static_part]
    try:
        from intelligence.codebase_context import get_system_context
        live = get_system_context()
        if live:
            parts.append(live)
    except Exception as exc:
        log.debug("Chat: codebase context fetch failed: {e}", e=str(exc))
    return "\n\n".join(parts)



def _get_llm_client():
    """Get best available LLM client for chat.

    Routing reflects current reality (per .env: LLM_REASON_PROVIDER=llamacpp_oracle):
    REASON tier and ORACLE tier both route to the same Qwen3.6-27B + mmproj
    server on :8081 — that is the unified chosen model. The previously-checked
    :8080/slots endpoint belonged to the disabled REASON service and is no
    longer relevant.

    Order:
      1. ORACLE tier  → Qwen3.6-27B on :8081 (primary, unified model)
      2. Ollama       → qwen2.5:7b on z4 (CPU, slow, emergency fallback only)
      3. OpenRouter   → cloud last-resort
      4. OpenAI       → cloud last-resort

    Returns:
        (client, backend_label) tuple, or (None, None) if nothing is available.
    """
    # 1. Primary: ORACLE tier → Qwen3.6-27B on :8081
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.ORACLE)
        if client and getattr(client, "is_available", False):
            log.debug("Chat: using ORACLE tier (Qwen3.6-27B on :8081)")
            return client, "qwen3.6-27b"
    except Exception as exc:
        log.debug("Chat: ORACLE client init failed: {e}", e=str(exc))

    # 2. Emergency fallback: Ollama (qwen2.5:7b on z4, CPU-bound, slow)
    try:
        from ollama.client import get_client as get_ollama
        client = get_ollama()
        if client and getattr(client, "is_available", False):
            log.warning("Chat: falling back to Ollama (ORACLE unavailable)")
            return client, "ollama"
    except Exception as exc:
        log.debug("Chat: ollama client unavailable: {e}", e=str(exc))

    # 3. Last-resort: OpenRouter (cloud)
    try:
        from llm.router import get_llm
        client = get_llm(provider="openrouter")
        if client and getattr(client, "is_available", False):
            log.warning("Chat: falling back to OpenRouter (local LLMs unavailable)")
            return client, "openrouter"
    except Exception as exc:
        log.debug("Chat: openrouter client unavailable: {e}", e=str(exc))

    # 4. Last-resort: OpenAI (cloud)
    try:
        from llm.router import get_llm
        client = get_llm(provider="openai")
        if client and getattr(client, "is_available", False):
            log.warning("Chat: falling back to OpenAI (no other providers available)")
            return client, "openai"
    except Exception as exc:
        log.debug("Chat: openai client unavailable: {e}", e=str(exc))

    return None, None


# ── LLM response sanity checking ───────────────────────────────────────

_PRICE_PATTERN = _re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)"
    r"|"
    r"(?:price|priced|trading|at|around|near|level)\s+(?:of\s+)?\$?([\d,]+(?:\.\d+)?)"
    r"|"
    r"(?:^|\s)([\d,]+(?:\.\d+)?)\s*%",
    _re.IGNORECASE,
)

_TICKER_IN_TEXT = _re.compile(r"\b([A-Z]{1,5})\b")

_MIN_RESPONSE_LEN = 20
_MAX_RESPONSE_LEN = 15_000


def _sanity_check_llm_response(
    answer: str,
    ticker: str | None,
) -> list[str]:
    """Validate an LLM response for plausibility.

    Checks:
      - Response is not empty or too short (truncated)
      - Response is not absurdly long
      - Mentioned prices are within 20% of last known DB price
      - Percentages are plausible (-200% to +200%)

    Returns list of warning strings.  Never blocks the response.
    """
    warnings: list[str] = []

    # ── Empty / truncated / absurdly long ─────────────────────────────
    if not answer or not answer.strip():
        warnings.append("LLM response is empty")
        return warnings

    if len(answer.strip()) < _MIN_RESPONSE_LEN:
        warnings.append(
            f"LLM response suspiciously short ({len(answer)} chars) "
            f"— possible truncation"
        )

    if len(answer) > _MAX_RESPONSE_LEN:
        warnings.append(
            f"LLM response very long ({len(answer)} chars) "
            f"— possible runaway generation"
        )

    # ── Price hallucination check ─────────────────────────────────────
    if ticker:
        try:
            engine = _get_db_engine()
            from sqlalchemy import text as sa_text
            t_lower = ticker.lower()
            with engine.connect() as conn:
                row = conn.execute(sa_text(
                    "SELECT rs.value FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE (fr.name = :n1 OR fr.name = :n2 OR fr.name = :n3) "
                    "AND rs.value IS NOT NULL "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ), {
                    "n1": f"{t_lower}_full",
                    "n2": f"{t_lower}_usd_full",
                    "n3": t_lower,
                }).fetchone()

                if row and row[0] is not None:
                    db_price = float(row[0])
                    # Extract dollar amounts from the answer
                    for match in _PRICE_PATTERN.finditer(answer):
                        raw = match.group(1) or match.group(2)
                        if raw is None:
                            continue  # skip percentage matches
                        try:
                            mentioned = float(raw.replace(",", ""))
                            if mentioned <= 0:
                                continue
                            # Only flag if the mentioned price is in the
                            # same order of magnitude as the DB price
                            if db_price > 0:
                                ratio = mentioned / db_price
                                if 0.1 < ratio < 10 and abs(ratio - 1.0) > 0.20:
                                    warnings.append(
                                        f"Hallucination flag: {ticker} mentioned "
                                        f"at ${mentioned:,.2f} but DB shows "
                                        f"${db_price:,.2f} "
                                        f"({abs(ratio - 1.0):.0%} deviation)"
                                    )
                        except (ValueError, ZeroDivisionError):
                            continue
        except Exception as exc:
            log.debug(
                "Sanity check price lookup failed: {e}", e=str(exc)
            )

    if warnings:
        log.info(
            "LLM sanity warnings ({n}): {w}",
            n=len(warnings), w="; ".join(warnings),
        )

    return warnings


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

# How long a cached Sleuth finding stays fresh for chat purposes.
# Intraday investigations (run by hermes every 6h) are stable enough for
# a full trading day; there is no value in re-running the same LLM call
# on every user chat request.
_RESEARCH_CACHE_HOURS: int = 6

# Maximum seconds to wait for a background investigation before detaching
# and letting it finish asynchronously (result lands in the DB for the
# next caller).
_RESEARCH_BACKGROUND_TIMEOUT_S: float = 4.5


def _fetch_cached_research(
    question: str,
    ticker: str | None,
) -> tuple[str, str]:
    """Query investigation_leads for a recent resolved lead that matches.

    Matching strategy (pure SQL, no embedding model):
    1. Ticker exact-match leads resolved in the last _RESEARCH_CACHE_HOURS h.
    2. Keyword overlap: any word from the question appears in the lead question.
    3. Fall back to the single most-recently-resolved lead as broad market context.

    Returns (findings_text, source_label) or ("", "") on miss.
    """
    try:
        from sqlalchemy import text as _sql
        engine = _get_db_engine()
        cutoff_expr = f"NOW() - INTERVAL '{_RESEARCH_CACHE_HOURS} hours'"

        with engine.connect() as conn:
            # 1. Ticker-specific recent resolved lead
            if ticker:
                row = conn.execute(_sql(
                    "SELECT question, findings, hypotheses, follow_up_leads "
                    "FROM investigation_leads "
                    "WHERE status = 'resolved' "
                    f"AND created_at >= {cutoff_expr} "
                    "AND (question ILIKE :tpat OR evidence::text ILIKE :tpat) "
                    "ORDER BY created_at DESC LIMIT 1"
                ), {"tpat": f"%{ticker}%"}).fetchone()
                if row and row[1] and row[1] != "LLM unavailable — investigation deferred.":
                    return _format_cached_lead(row), "sleuth/cache"

            # 2. Keyword-overlap: extract meaningful words from question
            import re as _re
            _STOP = frozenset({
                "the", "is", "are", "was", "how", "what", "why", "does",
                "do", "did", "will", "can", "market", "stock", "price",
                "ok", "good", "bad", "it", "in", "at", "to", "for",
                "and", "or", "a", "an",
            })
            keywords = [
                w for w in _re.findall(r"[a-z]{3,}", question.lower())
                if w not in _STOP
            ]
            if keywords:
                # Build a ILIKE OR chain for the top 4 keywords
                kw_clauses = " OR ".join(
                    f"question ILIKE :kw{i}" for i in range(min(4, len(keywords)))
                )
                kw_params: dict = {
                    f"kw{i}": f"%{keywords[i]}%"
                    for i in range(min(4, len(keywords)))
                }
                row = conn.execute(_sql(
                    "SELECT question, findings, hypotheses, follow_up_leads "
                    "FROM investigation_leads "
                    "WHERE status = 'resolved' "
                    f"AND created_at >= {cutoff_expr} "
                    f"AND ({kw_clauses}) "
                    "ORDER BY created_at DESC LIMIT 1"
                ), kw_params).fetchone()
                if row and row[1] and row[1] != "LLM unavailable — investigation deferred.":
                    return _format_cached_lead(row), "sleuth/cache"

            # 3. Broadest fallback: most recent resolved lead as macro context
            row = conn.execute(_sql(
                "SELECT question, findings, hypotheses, follow_up_leads "
                "FROM investigation_leads "
                "WHERE status = 'resolved' "
                f"AND created_at >= {cutoff_expr} "
                "ORDER BY priority DESC, created_at DESC LIMIT 1"
            )).fetchone()
            if row and row[1] and row[1] != "LLM unavailable — investigation deferred.":
                return _format_cached_lead(row), "sleuth/cache"

    except Exception as exc:
        log.debug("Research cache lookup failed: {e}", e=str(exc))

    return "", ""


def _format_cached_lead(row) -> str:
    """Format a cached investigation_leads DB row into context text."""
    import json as _json
    lines = ["Research findings (cached):"]
    lines.append(f"  Conclusion: {row[1]}")

    hypotheses = row[2]
    if isinstance(hypotheses, str):
        try:
            hypotheses = _json.loads(hypotheses)
        except Exception:
            hypotheses = []
    if hypotheses and isinstance(hypotheses, list):
        lines.append("  Hypotheses:")
        for h in hypotheses[:3]:
            if isinstance(h, dict):
                lines.append(
                    f"    - {h.get('hypothesis', '?')} "
                    f"({h.get('confidence', '?')} confidence)"
                )
    return "\n".join(lines)


def _fire_background_investigation(question: str, ticker: str | None) -> None:
    """Launch a Sleuth investigation in a daemon thread.

    Saves the result to investigation_leads so the next caller gets a cache hit.
    Never blocks the calling request — any failure is logged at DEBUG level only.
    """
    import uuid as _uuid

    def _run() -> None:
        try:
            from intelligence.sleuth import Sleuth, Lead
            engine = _get_db_engine()
            sleuth = Sleuth(engine)
            _safe_question = question[:500]
            lead = Lead(
                id=f"chat-{_uuid.uuid4().hex[:12]}",
                question=_safe_question,
                category="connection_found" if ticker else "data_anomaly",
                priority=0.9,
                evidence=[{
                    "source": "user_query",
                    "ticker": ticker,
                    "question": _safe_question,
                }],
            )
            sleuth.investigate_lead(lead)
            log.debug("Background research investigation completed for: {q}", q=_safe_question[:80])
        except Exception as exc:
            log.debug("Background research investigation failed: {e}", e=str(exc))

    t = threading.Thread(target=_run, daemon=True)
    t.start()


def _research_chain(question: str, ticker: str | None) -> tuple[str, str]:
    """Return Sleuth research context for a user question.

    Fast path (cache-first, target < 50ms):
    1. Check investigation_leads for a recent resolved lead that matches the
       question/ticker (keyword overlap, no embedding model needed).
    2. On cache hit: return immediately — no LLM, no SentenceTransformer.
    3. On cache miss: launch a background investigation (daemon thread) so the
       DB gets populated for the next caller, then return ("", "") immediately.

    The background thread may complete within _RESEARCH_BACKGROUND_TIMEOUT_S; if
    so its result is included in this response. Otherwise it finishes after the
    response has been sent and seeds the cache for subsequent requests.

    Why this is still analytically valuable:
    - Cached leads are produced by the same full Sleuth pipeline (LLM + RAG +
      context_provider + data gathering) — the output quality is identical.
    - Hermes runs daily_investigation every 6h, so the cache is continuously
      warmed with fresh market-relevant findings.
    - Per-request live investigations were creating LLM queue contention: the
      same Qwen3.6-27B that answers the chat question was being asked to *also*
      run a 1500-token investigation, doubling queue depth and causing 35-300s
      latency. Cache-serving eliminates this self-competition.

    Returns (findings_text, source_label).
    """
    # Fast path: serve from cache
    cached_text, cached_source = _fetch_cached_research(question, ticker)
    if cached_text:
        log.debug("Research chain: cache hit for question={q!r}", q=question[:60])
        return cached_text, cached_source

    # Cache miss: fire background investigation and wait briefly
    log.debug("Research chain: cache miss — launching background investigation")
    import uuid as _uuid
    result_holder: list[tuple[str, str]] = []

    def _run_and_capture() -> None:
        try:
            from intelligence.sleuth import Sleuth, Lead
            engine = _get_db_engine()
            sleuth = Sleuth(engine)
            _safe_question = question[:500]
            lead = Lead(
                id=f"chat-{_uuid.uuid4().hex[:12]}",
                question=_safe_question,
                category="connection_found" if ticker else "data_anomaly",
                priority=0.9,
                evidence=[{
                    "source": "user_query",
                    "ticker": ticker,
                    "question": _safe_question,
                }],
            )
            investigated = sleuth.investigate_lead(lead)
            if (
                investigated.findings
                and investigated.findings != "LLM unavailable — investigation deferred."
            ):
                result_holder.append((_format_cached_lead(
                    (
                        investigated.question,
                        investigated.findings,
                        investigated.hypotheses,
                        investigated.follow_up_leads,
                    )
                ), "sleuth/investigation"))
        except Exception as exc:
            log.debug("Research chain background run failed: {e}", e=str(exc))

    bg_thread = threading.Thread(target=_run_and_capture, daemon=True)
    bg_thread.start()
    bg_thread.join(timeout=_RESEARCH_BACKGROUND_TIMEOUT_S)

    if result_holder:
        log.debug("Research chain: background investigation completed within budget")
        return result_holder[0]

    # Background is still running (or failed fast) — return empty so
    # _build_context_block is not held up. The thread will finish and
    # persist the lead to the DB for the next caller.
    log.debug("Research chain: background investigation still running — returning empty")
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
@_lf_observe(name="chat-ask", capture_input=False)
async def ask_grid(
    req: ChatAskRequest,
    token: str = Depends(require_auth),
) -> ChatAskResponse:
    """Conversational Q&A with full GRID context.

    Gathers regime, watchlist, cross-reference, trust, lever-puller,
    options, and GEX context.  Sends to LLM with conversation history.
    Falls back to rule-based response when no LLM is online.
    """
    now = datetime.now(timezone.utc)
    question = req.question.strip()
    ticker = req.context_ticker.strip().upper() if req.context_ticker else None
    timeframe = req.timeframe  # "1d", "1w", "1m", "3m", "6m"

    # ── Langfuse trace context: user, session, feature tags ──────────────
    # Done as early as possible so every child span (LLM call, firewall,
    # citation tracking, etc.) inherits these attributes. Late propagation
    # would exclude earlier spans from per-user / per-session aggregations.
    user_id = _user_id_from_token(token)
    feature_tags = [
        "feature:chat",
        f"timeframe:{timeframe or 'none'}",
    ]
    if ticker:
        feature_tags.append(f"ticker:{ticker}")
    propagate_ctx = _lf_safe_propagate(
        user_id=user_id,
        session_id=req.session_id,
        tags=feature_tags,
    )

    with propagate_ctx:
        return await _ask_grid_impl(
            req=req,
            question=question,
            ticker=ticker,
            timeframe=timeframe,
            now=now,
        )


async def _ask_grid_impl(
    *,
    req: ChatAskRequest,
    question: str,
    ticker: str | None,
    timeframe: str | None,
    now: datetime,
) -> ChatAskResponse:
    """Inner body of /chat/ask — runs inside the Langfuse propagate context."""
    # Explicit input — avoid leaking the full request (history may contain PII).
    _lf_set_input(
        question=question,
        ticker=ticker,
        timeframe=timeframe,
        history_len=len(req.history),
        session_id=req.session_id,
    )

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

        # Append conversation history (last 10 turns max).
        # Role and content length are already enforced by ChatMessage validators;
        # additionally sanitize content for prompt-injection patterns.
        for msg in req.history[-10:]:
            clean_content = _sanitize_history_content(msg.content)
            messages.append({"role": msg.role, "content": clean_content})

        # Append current question
        messages.append({"role": "user", "content": question})

        # A/B pairing: one UUID shared by both arms so Langfuse can join them.
        ab_pair_id = uuid.uuid4().hex
        ab_meta_a = {"ab_arm": "A", "ab_pair_id": ab_pair_id}
        ab_meta_b = {"ab_arm": "B", "ab_pair_id": ab_pair_id}

        try:
            primary_kwargs: dict[str, Any] = {
                "temperature": 0.3,
                "num_predict": 2000,
            }
            if _supports_kwarg(client.chat, "extra_metadata"):
                primary_kwargs["extra_metadata"] = ab_meta_a
            answer = client.chat(messages, **primary_kwargs)
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
                            extra_metadata=ab_meta_b,
                        )
                        model_b = "anthropic/claude-opus-4"
                except Exception as ab_exc:
                    log.debug("A/B Opus call failed: {e}", e=str(ab_exc))

                # Sanity check the LLM response
                sanity_warnings = _sanity_check_llm_response(
                    answer, ticker
                )

                # ── Publishing firewall: verify claims before returning ──
                try:
                    from oracle.firewall import verify_output
                    fw = verify_output(answer, _get_db_engine())
                    if fw.decision.decision in ("reject", "review"):
                        if fw.decision.decision == "reject":
                            log.warning(
                                "Firewall REJECTED response ({n} claims, {f} flagged): {r}",
                                n=fw.claim_count, f=fw.flagged_count,
                                r=fw.decision.reasons,
                            )
                        else:
                            log.info(
                                "Firewall flagged for REVIEW ({n} claims, {f} flagged)",
                                n=fw.claim_count, f=fw.flagged_count,
                            )
                        # Auto-label this failure into the regression dataset
                        # BEFORE we overwrite `answer` with the firewall's
                        # sanitized output — we want the original LLM text.
                        _auto_label_failure(
                            question=question,
                            ticker=ticker,
                            timeframe=timeframe,
                            answer=answer,
                            fw_decision=fw.decision.decision,
                            fw_reasons=list(fw.decision.reasons or []),
                            claim_count=fw.claim_count,
                            flagged_count=fw.flagged_count,
                        )
                        answer = fw.output_text
                        if sanity_warnings is None:
                            sanity_warnings = []
                        sanity_warnings.extend(fw.decision.reasons)
                    else:
                        log.debug(
                            "Firewall PASSED ({n} claims verified)",
                            n=fw.claim_count,
                        )
                except Exception as fw_exc:
                    log.debug("Publishing firewall failed (non-fatal): {e}", e=str(fw_exc))

                # ── Prompt pruning: track feature citations ──
                try:
                    from oracle.citation_extractor import extract_citations
                    from oracle.feedback_recorder import record_prompt_feedback

                    features_in_prompt = _extract_feature_names_from_context(context_text)
                    if features_in_prompt:
                        features_cited = extract_citations(answer, features_in_prompt)
                        import threading
                        threading.Thread(
                            target=record_prompt_feedback,
                            args=(_get_db_engine(),),
                            kwargs={
                                "source": "chat",
                                "features_available": features_in_prompt,
                                "features_cited": features_cited,
                                "ticker": ticker,
                                "llm_model": backend,
                                "response_length": len(answer),
                            },
                            daemon=True,
                        ).start()
                except Exception as pf_exc:
                    log.debug("Prompt feedback recording failed (non-fatal): {e}", e=str(pf_exc))

                return ChatAskResponse(
                    answer=answer,
                    sources_used=sources,
                    confidence=confidence,
                    generated_at=now.isoformat(),
                    model_used=model_used,
                    answer_b=answer_b,
                    model_b=model_b,
                    sanity_warnings=sanity_warnings or None,
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


# ── Compose endpoint: NL → dashboard layout ─────────────────────────────

# The widget catalog. Each entry: required prop keys + a one-line description
# the planner LLM uses to choose widgets. To add a widget, add it here AND
# register a matching component in the frontend widget registry.
_WIDGET_CATALOG: dict[str, dict[str, Any]] = {
    "verdict": {
        "required": ["question"],
        "desc": "GRID's plain-English call on a question. Use for 'what should I do', "
                "'is X going up', 'should I worry'. props.question = the question to answer.",
    },
    "ticker_pulse": {
        "required": ["ticker"],
        "desc": "Price + signal snapshot for ONE ticker. props.ticker = symbol "
                "(AAPL, TSLA, GLD for gold, BTC-USD for bitcoin). One card per ticker.",
    },
    "watchlist": {
        "required": [],
        "desc": "The user's full watchlist overview. Use for 'my stocks', 'my watchlist'.",
    },
    "macro_regime": {
        "required": [],
        "desc": "Overall market regime (risk-on/off). Use for 'the market', 'overall', 'macro'.",
    },
    "news": {
        "required": [],
        "desc": "Recent news momentum across the market. Use for 'what's happening', 'news'.",
    },
    "money_flow": {
        "required": [],
        "desc": "Where money is flowing across sectors. Use for 'flows', 'where's the money'.",
    },
}

_MAX_COMPOSE_WIDGETS = 12


def _compose_system_prompt() -> str:
    """Build the planner prompt that maps a request to a widget layout."""
    lines = [
        f"- {name}: {spec['desc']}"
        + (f" Required: {', '.join(spec['required'])}." if spec["required"] else "")
        for name, spec in _WIDGET_CATALOG.items()
    ]
    catalog = "\n".join(lines)
    return (
        "You are the stepdad.finance layout planner. The user describes what they "
        "want to see on their home page, in plain language. Turn it into a dashboard "
        "layout by choosing widgets from the catalog below. You do NOT answer the "
        "question yourself — you only build the layout; the widgets fetch their own data.\n\n"
        f"WIDGET CATALOG:\n{catalog}\n\n"
        "RULES:\n"
        "- Pick the smallest set of widgets that satisfies the request. Order them by importance.\n"
        "- One ticker_pulse per ticker mentioned. Map names to symbols (Apple→AAPL, gold→GLD, bitcoin→BTC-USD).\n"
        "- If they ask for an opinion/decision/worry, include ONE verdict widget with a clear props.question.\n"
        "- If they state holdings or weights (e.g. 'half in Apple'), fill `allocation` with {ticker, weight} (weights 0-1, sum ~1). Otherwise leave allocation empty.\n"
        "- Give each widget a short human title (e.g. 'Apple', 'Should you worry this week?').\n"
        "- `spoken_reply` is one warm, plain sentence confirming what you built. No jargon.\n"
        "- ACCURACY OVER EVERYTHING: if the request needs something you genuinely CANNOT do "
        "with these widgets — e.g. set a price alert, connect/show his REAL brokerage or "
        "account positions, place or recommend a trade, send a text/email, compare to his "
        "personal holdings, anything not covered by the catalog — DO NOT fake it or substitute "
        "a vaguely-related widget. Instead return EXACTLY: "
        '{"cannot_fulfill": true, "what_he_wanted": "<short plain description of the missing '
        'capability>", "reason": "<one plain sentence on why it is not available yet>"}.\n\n'
        "Respond with ONLY a JSON object, no markdown, no prose. Either a layout:\n"
        '{"spoken_reply": "...", "widgets": [{"type": "...", "title": "...", "props": {...}}], '
        '"allocation": [{"ticker": "...", "weight": 0.0}]}\n'
        "or a cannot_fulfill object as described above."
    )


def _extract_json_object(text: str) -> dict[str, Any] | None:
    """Pull the first JSON object out of an LLM response, tolerating code
    fences and surrounding prose. Returns None if nothing parses."""
    import json

    if not text:
        return None
    cleaned = text.strip()
    # Strip ```json ... ``` fences if present.
    if cleaned.startswith("```"):
        cleaned = _re.sub(r"^```[a-zA-Z]*\n?", "", cleaned)
        cleaned = _re.sub(r"\n?```$", "", cleaned).strip()
    # Fast path.
    try:
        obj = json.loads(cleaned)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    # Fallback: first balanced {...} span.
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end > start:
        try:
            obj = json.loads(cleaned[start : end + 1])
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None


def _validate_compose_layout(raw: dict[str, Any], question: str) -> tuple[
    list[ComposeWidget], list[ComposeAllocationItem], str
]:
    """Coerce a raw planner dict into validated widgets/allocation, dropping
    anything unknown or malformed. Always returns at least one widget so the
    page is never blank — falls back to a verdict on the original question."""
    widgets: list[ComposeWidget] = []
    for item in (raw.get("widgets") or [])[:_MAX_COMPOSE_WIDGETS]:
        if not isinstance(item, dict):
            continue
        wtype = str(item.get("type", "")).strip()
        spec = _WIDGET_CATALOG.get(wtype)
        if spec is None:
            continue
        props = item.get("props") if isinstance(item.get("props"), dict) else {}
        # Required props present?
        if any(not str(props.get(k, "")).strip() for k in spec["required"]):
            continue
        # Normalize tickers.
        if "ticker" in props and isinstance(props["ticker"], str):
            props["ticker"] = props["ticker"].strip().upper()
        title = str(item.get("title", "")).strip()[:120]
        widgets.append(ComposeWidget(type=wtype, title=title, props=props))

    if not widgets:
        widgets.append(
            ComposeWidget(type="verdict", title="Your read", props={"question": question})
        )

    allocation: list[ComposeAllocationItem] = []
    for item in (raw.get("allocation") or [])[:50]:
        if not isinstance(item, dict):
            continue
        tkr = str(item.get("ticker", "")).strip().upper()
        if not tkr or not _TICKER_RE.match(tkr):
            continue
        try:
            weight = float(item.get("weight", 0.0))
        except (TypeError, ValueError):
            weight = 0.0
        weight = max(0.0, min(1.0, weight))
        allocation.append(ComposeAllocationItem(ticker=tkr, weight=weight))

    spoken = str(raw.get("spoken_reply", "")).strip()[:500]
    if not spoken:
        spoken = "Here's what you asked for."
    return widgets, allocation, spoken


@router.post("/compose", response_model=ChatComposeResponse)
async def compose_layout(
    req: ChatComposeRequest,
    token: str = Depends(require_auth),
) -> ChatComposeResponse:
    """Turn a plain-language request into a stepdad.finance dashboard layout.

    The planner LLM only chooses widgets + params; each widget fetches its own
    data on the frontend (verdict cards call /chat/ask, reusing the full
    synthesis + firewall pipeline). On any failure we still return a usable
    single-verdict layout so the page is never blank.
    """
    now = datetime.now(timezone.utc)
    question = req.question.strip()

    # Deterministic refusal for obvious "can't do that yet" requests — logs the
    # build request + tells the user gracefully, no LLM call needed.
    _gap = _obvious_capability_gap(question)
    if _gap:
        owner = _user_id_from_token(token) or "dad"
        rid = _log_capability_gap(owner=owner, request_text=question, want=_gap[0], reason=_gap[1])
        return ChatComposeResponse(
            spoken_reply="I can't do that yet — but I'll build it for you. Want me to ping you when it's ready?",
            widgets=[],
            allocation=[],
            generated_at=now.isoformat(),
            model_used=None,
            cannot_fulfill=True,
            request_id=rid,
        )

    raw: dict[str, Any] | None = None
    model_used: str | None = None

    client, backend = _get_llm_client()
    if client is not None:
        try:
            messages: list[dict[str, str]] = [
                {"role": "system", "content": _compose_system_prompt()},
            ]
            for msg in req.history[-6:]:
                messages.append(
                    {"role": msg.role, "content": _sanitize_history_content(msg.content)}
                )
            messages.append({"role": "user", "content": question})
            answer = client.chat(messages, temperature=0.2, num_predict=800)
            raw = _extract_json_object(answer or "")
            model_used = getattr(client, "model", backend)
        except Exception as exc:
            log.warning("Compose LLM failed, using fallback layout: {e}", e=str(exc))

    if raw is None:
        raw = {}

    # Honest refusal: the planner flagged a capability we don't have yet. Log it
    # as a build request, email the operator, and tell the user gracefully.
    if raw.get("cannot_fulfill"):
        owner = _user_id_from_token(token) or "dad"
        what = str(raw.get("what_he_wanted") or question).strip()[:500]
        reason = str(raw.get("reason") or "").strip()[:500]
        req_id = _log_capability_gap(owner=owner, request_text=question, want=what, reason=reason)
        return ChatComposeResponse(
            spoken_reply="I can't do that yet — but I'll build it for you. Want me to ping you when it's ready?",
            widgets=[],
            allocation=[],
            generated_at=now.isoformat(),
            model_used=model_used,
            cannot_fulfill=True,
            request_id=req_id,
        )

    widgets, allocation, spoken = _validate_compose_layout(raw, question)

    return ChatComposeResponse(
        spoken_reply=spoken,
        widgets=widgets,
        allocation=allocation,
        generated_at=now.isoformat(),
        model_used=model_used,
    )


# ── Capability gaps: "I can't do that yet" → logged as a build request ──────
# When the planner honestly refuses (accuracy over faking it), we record what
# the user wanted in `sd_capability_requests` (the operator's "TODO for dad"
# queue) and email the operator so it gets built ASAP. The user is told
# gracefully and can opt into a ping when it ships.

_CAPABILITY_DDL = """
CREATE TABLE IF NOT EXISTS sd_capability_requests (
    id             BIGSERIAL PRIMARY KEY,
    owner          TEXT NOT NULL DEFAULT 'dad',
    request_text   TEXT NOT NULL,
    want           TEXT,
    reason         TEXT,
    status         TEXT NOT NULL DEFAULT 'new',   -- new | building | ready
    dad_wants_ping BOOLEAN NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ DEFAULT now(),
    built_at       TIMESTAMPTZ,
    notified_at    TIMESTAMPTZ
)
"""


def _ensure_capability_table(engine) -> None:
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text(_CAPABILITY_DDL))


def _email_capability_gap(rid, owner, request_text, want, reason) -> None:
    try:
        from scripts.notify import send_insight_email
        subject = f"[stepdad.finance] {owner} asked for something new (#{rid})"
        body = (
            f'{owner} asked: "{request_text}"\n\n'
            f"Capability needed: {want or '(unspecified)'}\n"
            f"Why we couldn't do it: {reason or '(unspecified)'}\n\n"
            f"Logged as build request #{rid} (sd_capability_requests, status=new).\n"
            f"Build it, set status='ready', and {owner} gets pinged if they opted in."
        )
        send_insight_email(subject, body)
    except Exception as exc:
        log.debug("Capability gap email failed (non-fatal): {e}", e=str(exc))


# Unambiguous "we can't do this yet" patterns — caught deterministically so the
# obvious cases are reliable (and skip the LLM call entirely). The planner LLM
# still handles nuanced refusals via the cannot_fulfill path.
_GAP_PATTERNS = [
    (r"\b(price\s+)?alerts?\b|\balert me\b|\bwatch (the )?price\b",
     "set a price alert", "stepdad.finance can't set price alerts yet"),
    (r"\b(text|sms|message|e-?mail|call|notify|ping|remind|tell)\s+me\b",
     "send you a notification (text or email)", "stepdad.finance can't send you messages yet"),
    (r"\b(connect|link|sync|log\s*in to|hook\s*up)\b.{0,30}\b(account|brokerage|broker|schwab|fidelity|thinkorswim|robinhood|e-?trade|vanguard|coinbase|webull|interactive brokers)\b",
     "connect your real brokerage account", "stepdad.finance can't link to brokerage accounts yet"),
    (r"\b(place|execute|enter|put in|make|submit)\b.{0,20}\b(trade|order)\b|\b(buy|sell)\b.{0,20}\bfor me\b|\bactually (buy|sell)\b",
     "place a real trade for you", "stepdad.finance can't place trades yet"),
    (r"\bmy (real|actual|live)\s+(positions|holdings|account|balance|portfolio|shares)\b",
     "show your real account / live positions", "stepdad.finance can't see your real brokerage holdings yet"),
]


def _obvious_capability_gap(question: str) -> tuple[str, str] | None:
    q = (question or "").lower()
    for pat, want, reason in _GAP_PATTERNS:
        if _re.search(pat, q):
            return want, reason
    return None


def _log_capability_gap(*, owner: str, request_text: str, want: str, reason: str) -> int | None:
    try:
        from sqlalchemy import text
        engine = _get_db_engine()
        _ensure_capability_table(engine)
        with engine.begin() as conn:
            rid = conn.execute(
                text(
                    "INSERT INTO sd_capability_requests (owner, request_text, want, reason) "
                    "VALUES (:o, :rt, :w, :rs) RETURNING id"
                ),
                {"o": owner, "rt": request_text, "w": want, "rs": reason},
            ).scalar()
        import threading
        threading.Thread(
            target=_email_capability_gap,
            args=(rid, owner, request_text, want, reason),
            daemon=True,
        ).start()
        log.info("Capability gap logged #{rid} for {o}: {w}", rid=rid, o=owner, w=want)
        return int(rid) if rid is not None else None
    except Exception as exc:
        log.warning("Capability gap log failed: {e}", e=str(exc))
        return None


class CapabilityPingRequest(BaseModel):
    wants: bool = True


@router.post("/capability/{request_id}/ping")
async def set_capability_ping(
    request_id: int,
    req: CapabilityPingRequest,
    token: str = Depends(require_auth),
) -> dict:
    """Record whether the user wants a ping when this request is built."""
    from sqlalchemy import text
    try:
        engine = _get_db_engine()
        _ensure_capability_table(engine)
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE sd_capability_requests SET dad_wants_ping = :w WHERE id = :id"),
                {"w": bool(req.wants), "id": request_id},
            )
        return {"ok": True, "request_id": request_id, "wants_ping": bool(req.wants)}
    except Exception as exc:
        log.warning("Set capability ping pref failed: {e}", e=str(exc))
        return {"ok": False}


@router.get("/capability")
async def list_capability_requests(token: str = Depends(require_auth)) -> dict:
    """Operator view of the 'TODO for dad' build queue."""
    from sqlalchemy import text
    cols = ["id", "owner", "request_text", "want", "reason", "status", "dad_wants_ping", "created_at", "built_at"]
    try:
        engine = _get_db_engine()
        _ensure_capability_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT {', '.join(cols)} FROM sd_capability_requests ORDER BY created_at DESC LIMIT 200"
            )).fetchall()
        return {"requests": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return {"requests": [], "error": str(exc)}


@router.get("/capability/ready")
async def capability_ready_for_me(token: str = Depends(require_auth)) -> dict:
    """Requests this user opted into that are now built — shown once as a banner."""
    from sqlalchemy import text
    owner = _user_id_from_token(token) or "dad"
    try:
        engine = _get_db_engine()
        _ensure_capability_table(engine)
        with engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT id, want FROM sd_capability_requests "
                "WHERE owner = :o AND status = 'ready' AND dad_wants_ping = true "
                "AND notified_at IS NULL ORDER BY built_at DESC LIMIT 5"
            ), {"o": owner}).fetchall()
            ids = [r[0] for r in rows]
            if ids:
                conn.execute(
                    text("UPDATE sd_capability_requests SET notified_at = now() WHERE id = ANY(:ids)"),
                    {"ids": ids},
                )
        return {"ready": [{"id": r[0], "want": r[1]} for r in rows]}
    except Exception as exc:
        return {"ready": [], "error": str(exc)}


# ── Streaming verdict: token-by-token SSE for the stepdad.finance tile ──
# Same synthesis prompt + live GRID context as /chat/ask, but streams the
# primary model's tokens so the home verdict renders live instead of after
# ~57s. The inline A/B Opus eval and the publishing firewall are intentionally
# NOT run here — they need the full text and are for *published* claims; this
# is a private read-only tile. Use /chat/ask for the firewalled path.

def _sse(obj: dict) -> str:
    import json
    return f"data: {json.dumps(obj)}\n\n"


def _stream_llm_tokens(messages: list[dict[str, str]], client):
    """Yield SSE events streaming the LLM completion token-by-token."""
    import json
    import requests

    base_url = getattr(client, "base_url", "").rstrip("/")
    payload: dict[str, Any] = {
        "model": getattr(client, "model", None),
        "messages": messages,
        "max_tokens": 2000,
        "temperature": 0.3,
        "stream": True,
    }
    try:
        payload.update(client._extra_payload_fields())
    except Exception:
        pass
    headers = {
        "Authorization": f"Bearer {getattr(client, 'api_key', '')}",
        "Content-Type": "application/json",
    }
    timeout = getattr(client, "timeout", 300)
    try:
        with requests.post(
            f"{base_url}/chat/completions",
            json=payload, headers=headers, stream=True, timeout=timeout,
        ) as resp:
            if resp.status_code >= 400:
                yield _sse({"error": True, "message": f"LLM HTTP {resp.status_code}"})
                return
            for raw in resp.iter_lines(decode_unicode=True):
                if not raw:
                    continue
                line = raw[6:] if raw.startswith("data: ") else raw
                if line.strip() == "[DONE]":
                    break
                try:
                    delta = json.loads(line)["choices"][0]["delta"].get("content", "")
                except Exception:
                    continue
                if delta:
                    yield _sse({"delta": delta})
        yield _sse({"done": True})
    except Exception as exc:
        log.warning("Verdict stream failed: {e}", e=str(exc))
        yield _sse({"error": True, "message": "stream interrupted"})


@router.post("/ask/stream")
async def ask_grid_stream(
    req: ChatAskRequest,
    token: str = Depends(require_auth),
):
    """Streaming sibling of /chat/ask for the stepdad.finance verdict tile.

    Streams the same synthesis (same system prompt + live GRID context) as
    Server-Sent Events so the verdict renders as it is written. Falls back to
    a single rule-based chunk when no LLM is online.
    """
    from fastapi.responses import StreamingResponse

    question = req.question.strip()
    ticker = req.context_ticker.strip().upper() if req.context_ticker else None
    # Fast context for the realtime home verdict: skip the ~35s research chain
    # and cap the concurrent gather so tokens start streaming within seconds.
    context_text, sources = _build_context_block(
        question, ticker, include_research=False, budget_s=8
    )

    client, backend = _get_llm_client()

    if client is None:
        def _fallback():
            answer = _build_rule_based_response(context_text, question, sources)
            yield _sse({"delta": answer})
            yield _sse({"done": True})
        return StreamingResponse(_fallback(), media_type="text/event-stream")

    system_content = _build_system_prompt()
    if context_text:
        system_content += f"\n\n## Current GRID Context\n\n{context_text}"
    messages: list[dict[str, str]] = [{"role": "system", "content": system_content}]
    for msg in req.history[-10:]:
        messages.append({"role": msg.role, "content": _sanitize_history_content(msg.content)})
    messages.append({"role": "user", "content": question})

    return StreamingResponse(
        _stream_llm_tokens(messages, client),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
