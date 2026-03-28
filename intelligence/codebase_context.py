"""
GRID Codebase Context — dynamic state injected into every LLM prompt.

Builds a live summary of GRID's current state so the onboard LLMs
(Qwen 32B via llama.cpp, Ollama) always know what data is available,
what modules exist, and what the latest intelligence picture looks like.

Usage:
    from intelligence.codebase_context import get_system_context
    context = get_system_context()  # returns a string block
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log


def get_system_context() -> str:
    """Return the current GRID system context for LLM prompts.

    Dynamically gathers live state from the database and intelligence
    modules.  Every section is wrapped in a try/except so a single
    failure never breaks the entire context.

    Returns:
        str: Multi-line context block ready for injection into a
             system prompt.  Empty string if nothing could be gathered.
    """
    sections: list[str] = []

    # 1. Feature registry stats
    section = _get_feature_stats()
    if section:
        sections.append(section)

    # 2. Resolved series stats
    section = _get_resolved_stats()
    if section:
        sections.append(section)

    # 3. Latest thesis direction + conviction
    section = _get_thesis_state()
    if section:
        sections.append(section)

    # 4. Active convergence alerts
    section = _get_convergence_alerts()
    if section:
        sections.append(section)

    # 5. Latest cross-reference red flags
    section = _get_red_flags()
    if section:
        sections.append(section)

    # 6. Recent lever-puller actions
    section = _get_lever_puller_actions()
    if section:
        sections.append(section)

    # 7. Recent dollar flows summary
    section = _get_dollar_flows_summary()
    if section:
        sections.append(section)

    # 8. Active predictions
    section = _get_active_predictions()
    if section:
        sections.append(section)

    if not sections:
        return ""

    header = f"## GRID Live State (as of {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"
    return header + "\n\n" + "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Individual section builders — each returns a string or empty string
# ---------------------------------------------------------------------------

def _get_engine() -> Any:
    """Get the SQLAlchemy engine, or None."""
    try:
        from db import get_engine
        return get_engine()
    except Exception:
        return None


def _get_feature_stats() -> str:
    """Count features in feature_registry."""
    engine = _get_engine()
    if engine is None:
        return ""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT COUNT(*) AS total, "
                "       COUNT(*) FILTER (WHERE model_eligible = TRUE) AS eligible, "
                "       COUNT(DISTINCT family) AS families "
                "FROM feature_registry"
            )).fetchone()
            if row:
                total, eligible, families = row[0], row[1], row[2]
                return (
                    f"FEATURES: {total:,} total ({eligible:,} model-eligible) "
                    f"across {families} families"
                )
    except Exception as exc:
        log.debug("codebase_context: feature stats failed: {e}", e=str(exc))
    return ""


def _get_resolved_stats() -> str:
    """Count rows and date range in resolved_series."""
    engine = _get_engine()
    if engine is None:
        return ""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT COUNT(*), MIN(obs_date), MAX(obs_date) "
                "FROM resolved_series"
            )).fetchone()
            if row and row[0]:
                count, min_date, max_date = row[0], row[1], row[2]
                return (
                    f"RESOLVED DATA: {count:,} rows, "
                    f"date range {min_date} to {max_date}"
                )
    except Exception as exc:
        log.debug("codebase_context: resolved stats failed: {e}", e=str(exc))
    return ""


def _get_thesis_state() -> str:
    """Get latest thesis direction and conviction from thesis_tracker."""
    try:
        from intelligence.thesis_tracker import ThesisTracker
        tracker = ThesisTracker()
        thesis = tracker.get_current_thesis()
        if thesis and isinstance(thesis, dict):
            direction = thesis.get("direction", thesis.get("bias", "unknown"))
            conviction = thesis.get("conviction", thesis.get("confidence", "?"))
            summary = thesis.get("summary", thesis.get("narrative", ""))
            parts = [f"THESIS: Direction={direction}, Conviction={conviction}"]
            if summary:
                parts.append(f"  Summary: {str(summary)[:200]}")
            return "\n".join(parts)
    except Exception as exc:
        log.debug("codebase_context: thesis state failed: {e}", e=str(exc))

    # Fallback: check analytical_snapshots
    engine = _get_engine()
    if engine is None:
        return ""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT payload FROM analytical_snapshots "
                "WHERE category LIKE '%thesis%' "
                "ORDER BY created_at DESC LIMIT 1"
            )).fetchone()
            if row:
                payload = row[0]
                if isinstance(payload, dict):
                    direction = payload.get("direction", payload.get("bias", "?"))
                    conviction = payload.get("conviction", payload.get("confidence", "?"))
                    return f"THESIS: Direction={direction}, Conviction={conviction}"
                return f"THESIS (raw): {str(payload)[:200]}"
    except Exception:
        pass
    return ""


def _get_convergence_alerts() -> str:
    """Get active convergence events from trust_scorer."""
    try:
        from intelligence.trust_scorer import TrustScorer
        scorer = TrustScorer()
        events = scorer.get_convergence_events()
        if events:
            lines = ["CONVERGENCE ALERTS:"]
            for ev in events[:5]:
                if isinstance(ev, dict):
                    desc = ev.get("description", ev.get("event", str(ev)))
                    lines.append(f"  - {desc}")
                else:
                    lines.append(f"  - {ev}")
            return "\n".join(lines)
    except Exception as exc:
        log.debug("codebase_context: convergence failed: {e}", e=str(exc))
    return ""


def _get_red_flags() -> str:
    """Get latest cross-reference red flags."""
    try:
        from intelligence.cross_reference import CrossReferenceEngine
        engine = CrossReferenceEngine()
        results = engine.run()
        if results and isinstance(results, dict):
            flags = results.get("red_flags", results.get("flags", []))
            if flags:
                lines = ["RED FLAGS (cross-reference):"]
                for f in flags[:5]:
                    if isinstance(f, dict):
                        indicator = f.get("indicator", "?")
                        detail = f.get("description", f.get("detail", "?"))
                        lines.append(f"  - {indicator}: {detail}")
                    else:
                        lines.append(f"  - {f}")
                return "\n".join(lines)
    except Exception as exc:
        log.debug("codebase_context: red flags failed: {e}", e=str(exc))
    return ""


def _get_lever_puller_actions() -> str:
    """Get recent lever-puller activity."""
    try:
        from intelligence.lever_pullers import get_recent_activity
        activity = get_recent_activity(limit=5)
        if activity:
            lines = ["RECENT LEVER-PULLER ACTIONS:"]
            for a in activity:
                if isinstance(a, dict):
                    actor = a.get("actor", a.get("name", "?"))
                    action = a.get("action", a.get("description", "?"))
                    amount = a.get("amount_usd", a.get("amount", ""))
                    entry = f"  - {actor}: {action}"
                    if amount:
                        entry += f" (${amount:,.0f})" if isinstance(amount, (int, float)) else f" ({amount})"
                    lines.append(entry)
                else:
                    lines.append(f"  - {a}")
            return "\n".join(lines)
    except Exception as exc:
        log.debug("codebase_context: lever pullers failed: {e}", e=str(exc))
    return ""


def _get_dollar_flows_summary() -> str:
    """Get a summary of recent dollar flows."""
    engine = _get_engine()
    if engine is None:
        return ""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            # Try the dollar_flows table
            row = conn.execute(text(
                "SELECT COUNT(*), "
                "       SUM(CASE WHEN amount_usd > 0 THEN amount_usd ELSE 0 END) AS inflows, "
                "       SUM(CASE WHEN amount_usd < 0 THEN ABS(amount_usd) ELSE 0 END) AS outflows "
                "FROM dollar_flows "
                "WHERE created_at >= NOW() - INTERVAL '7 days'"
            )).fetchone()
            if row and row[0] and row[0] > 0:
                count = row[0]
                inflows = row[1] or 0
                outflows = row[2] or 0
                net = inflows - outflows
                direction = "net inflow" if net >= 0 else "net outflow"
                return (
                    f"DOLLAR FLOWS (7d): {count:,} transactions, "
                    f"${inflows:,.0f} in / ${outflows:,.0f} out "
                    f"= ${abs(net):,.0f} {direction}"
                )
    except Exception as exc:
        log.debug("codebase_context: dollar flows failed: {e}", e=str(exc))
    return ""


def _get_active_predictions() -> str:
    """Get active oracle predictions."""
    engine = _get_engine()
    if engine is None:
        return ""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT prediction_text, confidence, target_date "
                "FROM oracle_predictions "
                "WHERE status = 'ACTIVE' AND target_date > CURRENT_DATE "
                "ORDER BY confidence DESC LIMIT 3"
            )).fetchall()
            if rows:
                lines = ["ACTIVE PREDICTIONS:"]
                for r in rows:
                    text_val = str(r[0])[:100]
                    conf = r[1]
                    target = r[2]
                    lines.append(f"  - {text_val} (conf={conf}, target={target})")
                return "\n".join(lines)
    except Exception as exc:
        log.debug("codebase_context: predictions failed: {e}", e=str(exc))
    return ""
