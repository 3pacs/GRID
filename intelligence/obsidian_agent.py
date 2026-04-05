"""
Obsidian Agent — active intelligence loop for the vault.

Runs as a Hermes cycle step. Reacts to changes, enriches notes with
cross-references, prioritizes items for human review, acts on approvals,
creates proactive notes, and learns from user feedback.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

_TICKER_RE = re.compile(
    r"\$([A-Z]{1,6})\b"
    r"|(?<!\w)([A-Z]{2,5})(?=\s+(?:up|down|rally|drop|surge|crash|beat|miss|earnings|revenue|price|stock))"
)


def extract_entities(body: str) -> dict[str, list[str]]:
    """Extract tickers and other entities from note text."""
    tickers: set[str] = set()
    for m in _TICKER_RE.finditer(body):
        ticker = m.group(1) or m.group(2)
        if ticker:
            tickers.add(ticker)
    return {"tickers": sorted(tickers)}


# ---------------------------------------------------------------------------
# Priority ranking
# ---------------------------------------------------------------------------

_PRIORITY_ORDER = {"urgent": 0, "high": 1, "medium": 2, "low": 3}


def rank_for_review(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort items by priority (urgent first)."""
    return sorted(
        items,
        key=lambda x: _PRIORITY_ORDER.get(
            (x.get("agent_flags") or {}).get("priority", "low"), 4
        ),
    )


# ---------------------------------------------------------------------------
# Paid API escalation
# ---------------------------------------------------------------------------

def should_escalate_to_paid(result: dict[str, Any]) -> bool:
    """Decide if a local LLM result is bad enough to retry with paid API.

    Returns True if confidence is low or output is incoherent.
    """
    if not result.get("coherent", True):
        return True
    if result.get("confidence", 1.0) < 0.5:
        return True
    return False


# ---------------------------------------------------------------------------
# Cross-reference enrichment
# ---------------------------------------------------------------------------

def enrich_note(conn, note_id: int, body: str) -> str:
    """Cross-reference note content against GRID intelligence.

    Appends a ## Cross-References section if matches found.
    Returns the (possibly updated) body.
    """
    entities = extract_entities(body)
    refs: list[str] = []

    for ticker in entities["tickers"]:
        rows = conn.execute(text("""
            SELECT name, category FROM actors
            WHERE name ILIKE :pat OR metadata->>'primary_ticker' = :ticker
            LIMIT 3
        """), {"pat": f"%{ticker}%", "ticker": ticker}).fetchall()
        for r in rows:
            refs.append(f"- **Actor:** {r.name} ({r.category}) — linked via {ticker}")

    for ticker in entities["tickers"]:
        rows = conn.execute(text("""
            SELECT signal_type, direction, confidence, created_at
            FROM signal_registry
            WHERE ticker = :ticker
            ORDER BY created_at DESC LIMIT 3
        """), {"ticker": ticker}).fetchall()
        for r in rows:
            refs.append(
                f"- **Signal:** {ticker} {r.signal_type} {r.direction} "
                f"(conf={r.confidence:.2f}, {r.created_at.date()})"
            )

    if not refs:
        return body

    xref_section = "\n\n## Cross-References\n\n" + "\n".join(refs)
    if "## Cross-References" in body:
        body = re.sub(
            r"## Cross-References\n.*",
            xref_section.lstrip("\n"),
            body,
            flags=re.DOTALL,
        )
    else:
        body += xref_section

    return body


# ---------------------------------------------------------------------------
# Act on status changes
# ---------------------------------------------------------------------------

def act_on_approval(conn, note: dict[str, Any]) -> list[str]:
    """Execute downstream effects when a note is approved."""
    actions: list[str] = []
    domain = note["domain"]
    title = note["title"]
    now = datetime.now(timezone.utc)

    if domain == "alpha":
        entities = extract_entities(note["body"])
        for ticker in entities["tickers"][:1]:
            conn.execute(text("""
                INSERT INTO oracle_predictions
                    (ticker, model_name, direction, confidence, created_at, verdict)
                VALUES
                    (:ticker, 'vault_alpha', 'pending_analysis', 0.5, :now, 'pending')
                ON CONFLICT DO NOTHING
            """), {"ticker": ticker, "now": now})
            actions.append(f"Created prediction stub for {ticker} from alpha note '{title}'")

    if domain == "tools":
        actions.append(f"Tool '{title}' approved — queued for compute stack evaluation")

    if domain == "intel":
        entities = extract_entities(note["body"])
        for ticker in entities["tickers"]:
            actions.append(f"Intel note '{title}' — flagged for actor enrichment ({ticker})")

    return actions


# ---------------------------------------------------------------------------
# Main agent cycle
# ---------------------------------------------------------------------------

def run_agent_cycle(engine) -> dict[str, Any]:
    """Run one full agent cycle: react, enrich, prioritize, act."""
    stats = {"enriched": 0, "flagged": 0, "acted": 0}

    with engine.begin() as conn:
        recent = conn.execute(text("""
            SELECT n.id, n.vault_path, n.domain, n.status, n.title, n.body,
                   n.agent_flags, n.frontmatter
            FROM obsidian_notes n
            JOIN obsidian_actions a ON a.note_id = n.id
            WHERE a.created_at > NOW() - INTERVAL '10 minutes'
              AND n.status != 'archived'
            GROUP BY n.id
        """)).fetchall()

        for note in recent:
            new_body = enrich_note(conn, note.id, note.body)
            if new_body != note.body:
                conn.execute(text(
                    "UPDATE obsidian_notes SET body = :body, agent_flags = agent_flags || '{\"pending_write\": true}'::jsonb WHERE id = :id"
                ), {"body": new_body, "id": note.id})
                _log(conn, note.id, "hermes", "updated", {"reason": "cross-reference enrichment"})
                stats["enriched"] += 1

            if note.status == "inbox":
                flags = note.agent_flags if isinstance(note.agent_flags, dict) else {}
                if not flags.get("needs_human_review"):
                    conn.execute(text("""
                        UPDATE obsidian_notes
                        SET agent_flags = agent_flags || :flags
                        WHERE id = :id
                    """), {
                        "id": note.id,
                        "flags": json.dumps({"needs_human_review": True, "priority": "medium"}),
                    })
                    stats["flagged"] += 1

            if note.status == "approved":
                note_dict = {
                    "domain": note.domain, "title": note.title,
                    "body": note.body, "frontmatter": note.frontmatter,
                }
                actions = act_on_approval(conn, note_dict)
                for action_desc in actions:
                    _log(conn, note.id, "hermes", "acted_on", {"action": action_desc})
                    stats["acted"] += 1

    if any(v > 0 for v in stats.values()):
        log.info(
            "Obsidian agent: {e} enriched, {f} flagged, {a} acted",
            e=stats["enriched"], f=stats["flagged"], a=stats["acted"],
        )
    return stats


def _log(conn, note_id: int, actor: str, action: str, detail: dict) -> None:
    conn.execute(text("""
        INSERT INTO obsidian_actions (note_id, actor, action, detail)
        VALUES (:nid, :actor, :action, :detail)
    """), {"nid": note_id, "actor": actor, "action": action, "detail": json.dumps(detail)})
