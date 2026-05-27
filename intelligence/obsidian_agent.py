"""
Obsidian Agent — active intelligence loop for the vault.

Runs as a Hermes cycle step. Reacts to changes, enriches notes with
cross-references, prioritizes items for human review, acts on approvals,
creates proactive notes, and learns from user feedback.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
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
            prediction_id = f"vault_alpha:{ticker}:{now.date().isoformat()}"
            expiry = (now + timedelta(days=7)).date()
            conn.execute(text("""
                INSERT INTO oracle_predictions
                    (id, ticker, prediction_type, direction, target_price, entry_price,
                     expiry, confidence, model_name, model_version, signals, flow_context,
                     created_at, verdict)
                VALUES
                    (:id, :ticker, :prediction_type, :direction, NULL, :entry_price,
                     :expiry, :confidence, :model_name, :model_version,
                     CAST(:signals AS jsonb), CAST(:flow_context AS jsonb), :now, 'pending')
                ON CONFLICT (
                    ticker, direction, expiry, prediction_type,
                    (COALESCE(model_version, '')),
                    ((created_at AT TIME ZONE 'UTC')::date)
                ) WHERE dedup_keep = TRUE
                DO UPDATE SET
                    confidence = GREATEST(EXCLUDED.confidence, oracle_predictions.confidence),
                    signals = EXCLUDED.signals,
                    flow_context = EXCLUDED.flow_context,
                    verdict = EXCLUDED.verdict
            """), {
                "id": prediction_id,
                "ticker": ticker,
                "prediction_type": "vault_alpha",
                "direction": "pending_analysis",
                "entry_price": 0.0,
                "expiry": expiry,
                "confidence": 0.5,
                "model_name": "vault_alpha",
                "model_version": "obsidian-agent-v1",
                "signals": json.dumps([
                    {
                        "name": "obsidian_alpha_note",
                        "detail": title,
                    }
                ]),
                "flow_context": json.dumps({
                    "source": "obsidian_agent",
                    "note_title": title,
                    "domain": domain,
                }),
                "now": now,
            })
            actions.append(f"Created prediction stub for {ticker} from alpha note '{title}'")

    if domain == "tools":
        actions.append(f"Tool '{title}' approved — queued for compute stack evaluation")

    if domain == "intel":
        entities = extract_entities(note["body"])
        for ticker in entities["tickers"]:
            actions.append(f"Intel note '{title}' — flagged for actor enrichment ({ticker})")

    return actions


# ---------------------------------------------------------------------------
# Learning loop
# ---------------------------------------------------------------------------

def compute_preferences(actions: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze approval/rejection patterns to learn user preferences.

    Returns a preferences dict with:
    - domain_approval_rate: {domain: float} approval rate per domain
    - approved_tags: set of tags that appear in approved items
    - rejected_tags: set of tags that appear in rejected items
    - min_relevance_threshold: int, minimum relevance to surface (raised if low-relevance items get rejected)
    """
    if not actions:
        return {
            "domain_approval_rate": {},
            "approved_tags": set(),
            "rejected_tags": set(),
            "min_relevance_threshold": 5,
        }

    domain_counts: dict[str, dict[str, int]] = {}
    approved_tags: set[str] = set()
    rejected_tags: set[str] = set()
    rejected_relevances: list[int] = []

    for a in actions:
        d = a.get("domain", "unknown")
        s = a.get("status", "")
        tags = a.get("tags", [])
        rel = a.get("relevance", 5)

        domain_counts.setdefault(d, {"approved": 0, "rejected": 0, "total": 0})
        domain_counts[d]["total"] += 1

        if s == "approved":
            domain_counts[d]["approved"] += 1
            approved_tags.update(tags)
        elif s == "rejected":
            domain_counts[d]["rejected"] += 1
            rejected_tags.update(tags)
            rejected_relevances.append(rel)

    domain_approval_rate = {
        d: c["approved"] / c["total"] if c["total"] > 0 else 0.0
        for d, c in domain_counts.items()
    }

    # Raise threshold above the max rejected relevance
    min_threshold = 5
    if rejected_relevances:
        min_threshold = max(min_threshold, max(rejected_relevances) + 1)

    return {
        "domain_approval_rate": domain_approval_rate,
        "approved_tags": approved_tags,
        "rejected_tags": rejected_tags,
        "min_relevance_threshold": min_threshold,
    }


# ---------------------------------------------------------------------------
# Proactive note creation
# ---------------------------------------------------------------------------

def build_proactive_note(
    event_type: str,
    title: str,
    body: str,
    domain: str = "intel",
    tags: list[str] | None = None,
    priority: str = "medium",
) -> dict[str, Any]:
    """Build a note dict for proactive creation from GRID system events.

    Returns a dict ready to be inserted into obsidian_notes.
    """
    from ingestion.altdata.obsidian_sync import domain_to_folder

    slug = title.lower().replace(" ", "-").replace("/", "-").replace(":", "")[:60]
    folder = domain_to_folder(domain)
    vault_path = f"{folder}/{slug}.md"
    now = datetime.now(timezone.utc)

    fm = {
        "title": title,
        "domain": domain,
        "status": "inbox",
        "tags": tags or [],
        "confidence": "derived",
        "source": event_type,
        "created_by": "hermes",
        "created_at": now.isoformat(),
    }

    return {
        "vault_path": vault_path,
        "domain": domain,
        "status": "inbox",
        "title": title,
        "body": body,
        "frontmatter": fm,
        "agent_flags": {
            "pending_write": True,
            "needs_human_review": True,
            "priority": priority,
            "source_event": event_type,
        },
    }


def create_proactive_note(
    engine,
    event_type: str,
    title: str,
    body: str,
    domain: str = "intel",
    tags: list[str] | None = None,
    priority: str = "medium",
) -> int | None:
    """Create a proactive note in obsidian_notes from a GRID system event.

    Returns the note ID or None if creation failed.
    """
    import json as _json
    from ingestion.altdata.obsidian_sync import content_hash

    note = build_proactive_note(event_type, title, body, domain, tags, priority)
    now = datetime.now(timezone.utc)

    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO obsidian_notes
                    (vault_path, domain, status, title, content_hash, frontmatter, body,
                     agent_flags, modified_at, synced_at, created_at)
                VALUES
                    (:vp, :domain, :status, :title, :hash, :fm, :body,
                     :flags, :now, :now, :now)
                ON CONFLICT (vault_path) DO NOTHING
                RETURNING id
            """), {
                "vp": note["vault_path"],
                "domain": note["domain"],
                "status": note["status"],
                "title": note["title"],
                "hash": content_hash(note["body"]),
                "fm": _json.dumps(note["frontmatter"]),
                "body": note["body"],
                "flags": _json.dumps(note["agent_flags"]),
                "now": now,
            })
            row = result.fetchone()
            if row:
                _log(conn, row.id, "hermes", "created", {
                    "reason": f"proactive: {event_type}",
                    "title": title,
                })
                log.info("Proactive note created: {t} [{d}]", t=title, d=domain)
                return row.id
    except Exception as e:
        log.error("Failed to create proactive note: {e}", e=e)

    return None


# ---------------------------------------------------------------------------
# Preference tracking
# ---------------------------------------------------------------------------

def _update_preferences(engine) -> None:
    """Compute and store learned preferences from approval/rejection history."""
    import json as _json

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT domain, status, frontmatter, agent_flags
            FROM obsidian_notes
            WHERE status IN ('approved', 'rejected')
        """)).fetchall()

        actions = []
        for r in rows:
            fm = r.frontmatter if isinstance(r.frontmatter, dict) else {}
            actions.append({
                "domain": r.domain,
                "status": r.status,
                "tags": fm.get("tags", []),
                "relevance": fm.get("relevance", 5),
            })

        prefs = compute_preferences(actions)

        body = "# Agent Preferences\n\n"
        body += "*Auto-updated by Hermes agent*\n\n"
        body += "## Approval Rates by Domain\n\n"
        for d, rate in sorted(prefs["domain_approval_rate"].items()):
            body += f"- **{d}:** {rate:.0%}\n"
        body += "\n## Relevance Threshold\n\n"
        body += f"Minimum relevance to surface: **{prefs['min_relevance_threshold']}/10**\n\n"
        if prefs["approved_tags"]:
            body += "## Approved Tags\n\n"
            body += ", ".join(sorted(prefs["approved_tags"])) + "\n\n"
        if prefs["rejected_tags"]:
            body += "## Rejected Tags\n\n"
            body += ", ".join(sorted(prefs["rejected_tags"])) + "\n\n"

        fm_dict = {
            "title": "Agent Preferences",
            "domain": "grid",
            "status": "active",
            "tags": ["meta", "preferences"],
            "confidence": "derived",
        }

        conn.execute(text("""
            INSERT INTO obsidian_notes
                (vault_path, domain, status, title, content_hash, frontmatter, body,
                 agent_flags, modified_at, synced_at, created_at)
            VALUES
                ('05-GRID/agent-preferences.md', 'grid', 'active', 'Agent Preferences',
                 :hash, :fm, :body, '{"pending_write": true}'::jsonb, :now, :now, :now)
            ON CONFLICT (vault_path) DO UPDATE SET
                body = EXCLUDED.body, content_hash = EXCLUDED.content_hash,
                frontmatter = EXCLUDED.frontmatter, modified_at = EXCLUDED.modified_at,
                agent_flags = obsidian_notes.agent_flags || '{"pending_write": true}'::jsonb
        """), {
            "hash": __import__("hashlib").sha256(body.encode()).hexdigest(),
            "fm": _json.dumps(fm_dict),
            "body": body,
            "now": datetime.now(timezone.utc),
        })

    log.info("Agent preferences updated")


# ---------------------------------------------------------------------------
# Main agent cycle
# ---------------------------------------------------------------------------

def run_agent_cycle(engine) -> dict[str, Any]:
    """Run one full agent cycle: react, enrich, prioritize, act."""
    stats = {"enriched": 0, "flagged": 0, "acted": 0}

    # Pre-migration safety: act_on_approval below inserts into oracle_predictions
    # with an ON CONFLICT targeting the partial unique index
    # oracle_predictions_dedup_unique. Ensure it exists (once/process) so that
    # insert can't raise 42P10 on a not-yet-migrated DB.
    from oracle.dedup_index import ensure_dedup_index
    ensure_dedup_index(engine)

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

    try:
        _update_preferences(engine)
    except Exception as e:
        log.warning("Could not update agent preferences: {e}", e=e)

    return stats


def _log(conn, note_id: int, actor: str, action: str, detail: dict) -> None:
    conn.execute(text("""
        INSERT INTO obsidian_actions (note_id, actor, action, detail)
        VALUES (:nid, :actor, :action, :detail)
    """), {"nid": note_id, "actor": actor, "action": action, "detail": json.dumps(detail)})
