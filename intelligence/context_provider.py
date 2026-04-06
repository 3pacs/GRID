"""Context provider for LLM prompt injection.

Provides compact, token-efficient summaries of:
- Active hypotheses (thesis/antithesis pairs)
- Recent kill postmortems (failure lessons)
- Company profiles (governance/lobbying context)

All functions return formatted strings ready to inject into LLM prompts.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def get_active_hypotheses(engine: Engine, limit: int = 10) -> str:
    """Return compact summary of top active hypotheses with thesis/antithesis pairs.

    Sorted by confidence descending. Returns empty string if no active hypotheses.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT h.id, h.thesis, h.pattern_type, h.confidence,
                           h.times_tested, h.times_correct, h.role,
                           h.created_at,
                           a.thesis AS antithesis_text, a.confidence AS anti_confidence
                    FROM discovered_hypotheses h
                    LEFT JOIN discovered_hypotheses a
                        ON a.pair_id = h.id AND a.role = 'antithesis'
                    WHERE h.status = 'active' AND h.role = 'thesis'
                    ORDER BY h.confidence DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            ).fetchall()

        if not rows:
            return ""

        lines = ["### ACTIVE HYPOTHESES (thesis/antithesis pairs)"]
        for r in rows:
            created = r.created_at
            if isinstance(created, str):
                try:
                    created = datetime.fromisoformat(created.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    created = None
            if created:
                now = datetime.now(timezone.utc)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                age = (now - created).days
            else:
                age = "?"
            accuracy = (
                f"{r.times_correct}/{r.times_tested}"
                if r.times_tested > 0
                else "untested"
            )
            lines.append(
                f"- [{r.pattern_type}] {r.thesis[:120]} "
                f"(conf={r.confidence:.2f}, {accuracy}, age={age}d)"
            )
            if r.antithesis_text:
                lines.append(
                    f"  ANTI: {r.antithesis_text[:100]} (conf={r.anti_confidence:.2f})"
                )
        return "\n".join(lines)

    except Exception as exc:
        log.debug("context_provider: failed to get hypotheses: {e}", e=str(exc))
        return ""


def get_recent_postmortems(engine: Engine, days: int = 30, limit: int = 5) -> str:
    """Return compact summary of recent hypothesis kill postmortems.

    Focuses on lessons learned from failed hypotheses.
    Returns empty string if no postmortems exist.
    """
    try:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT kill_reason, thesis_text, antithesis_text,
                           confidence_at_death, times_tested, times_correct,
                           lifespan_days, created_at
                    FROM hypothesis_postmortems
                    WHERE created_at >= :since
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                {"since": since, "limit": limit},
            ).fetchall()

        if not rows:
            return ""

        lines = ["### RECENT KILL POSTMORTEMS (hypothesis failures)"]
        for r in rows:
            accuracy = (
                f"{r.times_correct}/{r.times_tested}"
                if r.times_tested > 0
                else "0/0"
            )
            lines.append(
                f"- KILLED [{r.kill_reason}]: {r.thesis_text[:120]} "
                f"(conf_at_death={r.confidence_at_death:.2f}, {accuracy}, "
                f"lived={r.lifespan_days}d)"
            )
            if r.antithesis_text:
                lines.append(f"  Winner: {r.antithesis_text[:100]}")
        lines.append(
            "Use these failures to avoid repeating the same analytical mistakes."
        )
        return "\n".join(lines)

    except Exception as exc:
        log.debug("context_provider: failed to get postmortems: {e}", e=str(exc))
        return ""


def get_company_context(engine: Engine, tickers: list[str] | None = None, limit: int = 5) -> str:
    """Return compact governance/lobbying context for relevant companies.

    If tickers provided, returns profiles for those tickers.
    Otherwise returns top profiles by suspicion score.
    Returns empty string if no profiles exist.
    """
    try:
        with engine.connect() as conn:
            if tickers:
                rows = conn.execute(
                    text("""
                        SELECT ticker, suspicion_score, sector, profile
                        FROM company_profiles
                        WHERE ticker = ANY(:tickers)
                        ORDER BY suspicion_score DESC
                        LIMIT :limit
                    """),
                    {"tickers": tickers, "limit": limit},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("""
                        SELECT ticker, suspicion_score, sector, profile
                        FROM company_profiles
                        WHERE suspicion_score > 0
                        ORDER BY suspicion_score DESC
                        LIMIT :limit
                    """),
                    {"limit": limit},
                ).fetchall()

        if not rows:
            return ""

        lines = ["### COMPANY INTELLIGENCE PROFILES"]
        for r in rows:
            profile = r.profile if isinstance(r.profile, dict) else {}
            congress = profile.get("congress_holders", [])
            lobbying = profile.get("lobbying_spend_annual", 0)
            insider = profile.get("insider_net_direction", "neutral")
            gov_contracts = profile.get("gov_contracts_total", 0)

            parts = [f"- {r.ticker} ({r.sector}, suspicion={r.suspicion_score:.2f})"]
            if congress:
                members = ", ".join(h.get("member", "?")[:20] for h in congress[:3])
                parts.append(f"  Congress: {members}")
            if lobbying:
                parts.append(f"  Lobbying: ${lobbying:,.0f}/yr")
            if gov_contracts:
                parts.append(f"  Gov contracts: ${gov_contracts:,.0f}")
            parts.append(f"  Insider: {insider}")
            lines.extend(parts)

        return "\n".join(lines)

    except Exception as exc:
        log.debug("context_provider: failed to get company profiles: {e}", e=str(exc))
        return ""


def get_hypothesis_context_for_ticker(engine: Engine, ticker: str) -> str:
    """Return hypotheses specifically mentioning a ticker."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT thesis, pattern_type, confidence, status, role
                    FROM discovered_hypotheses
                    WHERE LOWER(thesis) LIKE LOWER(:pattern) AND status = 'active'
                    ORDER BY confidence DESC
                    LIMIT 5
                """),
                {"pattern": f"%{ticker}%"},
            ).fetchall()

        if not rows:
            return ""

        lines = [f"### HYPOTHESES MENTIONING {ticker}"]
        for r in rows:
            lines.append(
                f"- [{r.pattern_type}/{r.role}] {r.thesis[:120]} (conf={r.confidence:.2f})"
            )
        return "\n".join(lines)

    except Exception as exc:
        log.debug("context_provider: ticker hypothesis lookup failed: {e}", e=str(exc))
        return ""


def build_full_context(
    engine: Engine,
    tickers: list[str] | None = None,
    max_hypotheses: int = 10,
    max_postmortems: int = 5,
    max_companies: int = 5,
) -> str:
    """Build complete intelligence context block for LLM prompt injection.

    Returns a formatted string combining hypotheses, postmortems, and company profiles.
    Designed to be appended to any LLM prompt's data section.
    """
    sections = []

    hyp = get_active_hypotheses(engine, limit=max_hypotheses)
    if hyp:
        sections.append(hyp)

    pm = get_recent_postmortems(engine, limit=max_postmortems)
    if pm:
        sections.append(pm)

    cp = get_company_context(engine, tickers=tickers, limit=max_companies)
    if cp:
        sections.append(cp)

    if not sections:
        return ""

    return "\n\n".join(sections)
