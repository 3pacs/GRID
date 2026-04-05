"""
Obsidian vault <-> Postgres bidirectional sync engine.

Runs every Hermes cycle (5 min). Syncs vault markdown files to
obsidian_notes table and writes agent-pending changes back to vault.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from loguru import logger as log
from sqlalchemy import text

from config import settings

# ---------------------------------------------------------------------------
# Domain mapping from vault path prefix
# ---------------------------------------------------------------------------

_DOMAIN_MAP: dict[str, str] = {
    "00-DASHBOARD": "dashboard",
    "01-Pipeline": "pipeline",
    "02-Tools": "tools",
    "03-Alpha": "alpha",
    "04-Intel": "intel",
    "05-GRID": "grid",
}


def domain_from_path(vault_path: str) -> str:
    """Derive domain from the vault-relative path prefix."""
    first = vault_path.split("/")[0]
    # Strip .md suffix for root-level files (e.g. "00-DASHBOARD.md" -> "00-DASHBOARD")
    first = first.removesuffix(".md")
    return _DOMAIN_MAP.get(first, "grid")


def content_hash(text_content: str) -> str:
    """SHA-256 hex digest of file content."""
    return hashlib.sha256(text_content.encode()).hexdigest()


def parse_frontmatter(raw: str) -> tuple[dict[str, Any], str]:
    """Split YAML frontmatter from markdown body.

    Returns (frontmatter_dict, body_string). If no frontmatter,
    returns ({}, full_text).
    """
    if not raw.startswith("---"):
        return {}, raw

    parts = raw.split("---", 2)
    if len(parts) < 3:
        return {}, raw

    try:
        fm = yaml.safe_load(parts[1]) or {}
    except yaml.YAMLError:
        fm = {}

    body = parts[2].strip()
    return fm, body


def scan_vault(vault_path: Path) -> list[dict[str, Any]]:
    """Walk the vault directory, return parsed note dicts.

    Skips .obsidian/ and non-.md files.
    """
    results: list[dict[str, Any]] = []

    for root, dirs, files in os.walk(vault_path):
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for fname in files:
            if not fname.endswith(".md"):
                continue

            fpath = Path(root) / fname
            rel = str(fpath.relative_to(vault_path))
            raw = fpath.read_text(encoding="utf-8", errors="replace")
            fm, body = parse_frontmatter(raw)
            mtime = datetime.fromtimestamp(fpath.stat().st_mtime, tz=timezone.utc)

            domain = fm.get("domain") or domain_from_path(rel)
            status = fm.get("status", "active")
            title = fm.get("title") or fname.removesuffix(".md")

            results.append({
                "vault_path": rel,
                "domain": domain,
                "status": status,
                "title": title,
                "content_hash": content_hash(raw),
                "frontmatter": fm,
                "body": body,
                "modified_at": mtime,
                "raw": raw,
            })

    return results


def sync_inbound(engine, vault_path: Path | None = None) -> dict[str, int]:
    """Sync vault files -> Postgres. Returns counts of inserts/updates/archives."""
    vault = vault_path or Path(settings.OBSIDIAN_VAULT_PATH)
    if not vault.exists():
        log.warning("Obsidian vault not found at {p}", p=vault)
        return {"inserted": 0, "updated": 0, "archived": 0}

    notes = scan_vault(vault)
    vault_paths = {n["vault_path"] for n in notes}
    now = datetime.now(timezone.utc)
    counts = {"inserted": 0, "updated": 0, "archived": 0}

    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT id, vault_path, content_hash FROM obsidian_notes"
        )).fetchall()
        existing = {r.vault_path: (r.id, r.content_hash) for r in rows}

        for note in notes:
            vp = note["vault_path"]
            if vp in existing:
                note_id, old_hash = existing[vp]
                if old_hash != note["content_hash"]:
                    conn.execute(text("""
                        UPDATE obsidian_notes
                        SET domain = :domain, status = :status, title = :title,
                            content_hash = :content_hash, frontmatter = :frontmatter,
                            body = :body, modified_at = :modified_at, synced_at = :synced_at
                        WHERE id = :id
                    """), {
                        "id": note_id,
                        "domain": note["domain"],
                        "status": note["status"],
                        "title": note["title"],
                        "content_hash": note["content_hash"],
                        "frontmatter": json.dumps(note["frontmatter"]),
                        "body": note["body"],
                        "modified_at": note["modified_at"],
                        "synced_at": now,
                    })
                    _log_action(conn, note_id, "user", "updated", {
                        "reason": "vault file changed",
                        "old_hash": old_hash[:12],
                        "new_hash": note["content_hash"][:12],
                    })
                    counts["updated"] += 1
            else:
                result = conn.execute(text("""
                    INSERT INTO obsidian_notes
                        (vault_path, domain, status, title, content_hash,
                         frontmatter, body, modified_at, synced_at, created_at)
                    VALUES
                        (:vault_path, :domain, :status, :title, :content_hash,
                         :frontmatter, :body, :modified_at, :synced_at, :created_at)
                    RETURNING id
                """), {
                    "vault_path": vp,
                    "domain": note["domain"],
                    "status": note["status"],
                    "title": note["title"],
                    "content_hash": note["content_hash"],
                    "frontmatter": json.dumps(note["frontmatter"]),
                    "body": note["body"],
                    "modified_at": note["modified_at"],
                    "synced_at": now,
                    "created_at": now,
                })
                note_id = result.scalar()
                _log_action(conn, note_id, "sync", "created", {
                    "reason": "new vault file",
                    "vault_path": vp,
                })
                counts["inserted"] += 1

        for vp, (note_id, _) in existing.items():
            if vp not in vault_paths:
                conn.execute(text("""
                    UPDATE obsidian_notes SET status = 'archived', synced_at = :now
                    WHERE id = :id AND status != 'archived'
                """), {"id": note_id, "now": now})
                _log_action(conn, note_id, "sync", "status_changed", {
                    "reason": "vault file deleted",
                    "old_status": "active",
                    "new_status": "archived",
                })
                counts["archived"] += 1

    log.info(
        "Obsidian inbound sync: {ins} inserted, {upd} updated, {arc} archived",
        ins=counts["inserted"], upd=counts["updated"], arc=counts["archived"],
    )
    return counts


def _log_action(
    conn, note_id: int, actor: str, action: str, detail: dict[str, Any]
) -> None:
    """Insert an immutable action record."""
    conn.execute(text("""
        INSERT INTO obsidian_actions (note_id, actor, action, detail)
        VALUES (:note_id, :actor, :action, :detail)
    """), {
        "note_id": note_id,
        "actor": actor,
        "action": action,
        "detail": json.dumps(detail),
    })
