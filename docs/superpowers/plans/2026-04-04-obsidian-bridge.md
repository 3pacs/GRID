# Obsidian Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect Obsidian vault to GRID as a bidirectional knowledge layer with active agent behavior.

**Architecture:** Postgres stores structured note data with FTS. A sync engine runs every Hermes cycle (5 min) to keep vault files and DB in lockstep. An active agent reads changes, cross-references GRID intelligence, creates proactive notes, and acts on approvals. API + frontend expose the vault to humans.

**Tech Stack:** Python 3.11+, FastAPI, SQLAlchemy, PostgreSQL (JSONB + tsvector), React 18, pyyaml, FastMCP

**Spec:** `docs/superpowers/specs/2026-04-04-obsidian-bridge-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `schema_obsidian.sql` | DDL for `obsidian_notes` + `obsidian_actions` tables |
| `ingestion/altdata/obsidian_sync.py` | Bidirectional sync engine (vault ↔ Postgres) |
| `intelligence/obsidian_agent.py` | Active agent loop (react/enrich/prioritize/act/create/learn) |
| `api/routers/vault.py` | REST API for vault CRUD, search, dashboard |
| `mcp_server.py` | Add 5 vault MCP tools (existing file, append) |
| `pwa/src/views/Vault.jsx` | Frontend view for browsing/approving vault notes |
| `pwa/src/api.js` | Add vault API methods (existing file, append) |
| `pwa/src/App.jsx` | Register vault route (existing file, modify) |
| `config.py` | Add `OBSIDIAN_VAULT_PATH` setting (existing file, modify) |
| `api/main.py` | Register vault router (existing file, modify) |
| `tests/test_obsidian_sync.py` | Sync engine tests |
| `tests/test_obsidian_agent.py` | Agent logic tests |
| `tests/test_vault_api.py` | API endpoint tests |

---

### Task 1: Database Schema

**Files:**
- Create: `schema_obsidian.sql`
- Test: manual `psql` verification

- [ ] **Step 1: Create schema file**

```sql
-- schema_obsidian.sql
-- Obsidian Bridge tables — run after schema.sql
-- Execute: psql -U grid_user -d grid -f schema_obsidian.sql

CREATE TABLE IF NOT EXISTS obsidian_notes (
    id              SERIAL PRIMARY KEY,
    vault_path      TEXT NOT NULL UNIQUE,
    domain          TEXT NOT NULL DEFAULT 'grid',
    status          TEXT NOT NULL DEFAULT 'inbox',
    title           TEXT NOT NULL DEFAULT '',
    content_hash    TEXT NOT NULL DEFAULT '',
    frontmatter     JSONB NOT NULL DEFAULT '{}',
    body            TEXT NOT NULL DEFAULT '',
    body_tsvector   TSVECTOR,
    agent_flags     JSONB NOT NULL DEFAULT '{}',
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_obsidian_notes_domain ON obsidian_notes(domain);
CREATE INDEX IF NOT EXISTS idx_obsidian_notes_status ON obsidian_notes(status);
CREATE INDEX IF NOT EXISTS idx_obsidian_notes_fts ON obsidian_notes USING gin(body_tsvector);
CREATE INDEX IF NOT EXISTS idx_obsidian_notes_agent_flags ON obsidian_notes USING gin(agent_flags);

-- Auto-update tsvector on insert/update
CREATE OR REPLACE FUNCTION obsidian_notes_tsvector_update() RETURNS trigger AS $$
BEGIN
    NEW.body_tsvector := to_tsvector('english', COALESCE(NEW.title, '') || ' ' || COALESCE(NEW.body, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_obsidian_notes_tsvector ON obsidian_notes;
CREATE TRIGGER trg_obsidian_notes_tsvector
    BEFORE INSERT OR UPDATE OF title, body ON obsidian_notes
    FOR EACH ROW EXECUTE FUNCTION obsidian_notes_tsvector_update();

CREATE TABLE IF NOT EXISTS obsidian_actions (
    id              SERIAL PRIMARY KEY,
    note_id         INTEGER NOT NULL REFERENCES obsidian_notes(id) ON DELETE CASCADE,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    detail          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_obsidian_actions_note_id ON obsidian_actions(note_id);
CREATE INDEX IF NOT EXISTS idx_obsidian_actions_created ON obsidian_actions(created_at DESC);
```

- [ ] **Step 2: Verify schema applies cleanly**

Run: `psql -U grid_user -d grid -f schema_obsidian.sql`
Expected: No errors. Tables `obsidian_notes` and `obsidian_actions` exist.

Verify: `psql -U grid_user -d grid -c "\dt obsidian_*"`
Expected: Two tables listed.

- [ ] **Step 3: Commit**

```bash
git add schema_obsidian.sql
git commit -m "feat(obsidian): add obsidian_notes and obsidian_actions schema"
```

---

### Task 2: Config — Add OBSIDIAN_VAULT_PATH

**Files:**
- Modify: `config.py:228-235`

- [ ] **Step 1: Add OBSIDIAN_VAULT_PATH to Settings class**

In `config.py`, find the bookmark config section (line ~228) and add the new setting:

```python
    # Obsidian Vault (bidirectional knowledge layer)
    OBSIDIAN_VAULT_PATH: str = os.path.expanduser("~/Documents/Obsidian Vault")
    OBSIDIAN_SYNC_ENABLED: bool = True
```

Keep the existing `BOOKMARKS_OBSIDIAN_PATH` for backward compatibility — the bookmark module still references it. The new `OBSIDIAN_VAULT_PATH` is the canonical one used by the sync engine.

- [ ] **Step 2: Add to .env.example**

Append to `.env.example`:

```
# Obsidian Bridge
OBSIDIAN_VAULT_PATH=~/Documents/Obsidian Vault
OBSIDIAN_SYNC_ENABLED=true
```

- [ ] **Step 3: Commit**

```bash
git add config.py .env.example
git commit -m "feat(obsidian): add OBSIDIAN_VAULT_PATH config"
```

---

### Task 3: Sync Engine — Vault → Postgres (Inbound)

**Files:**
- Create: `ingestion/altdata/obsidian_sync.py`
- Test: `tests/test_obsidian_sync.py`

- [ ] **Step 1: Write failing tests for inbound sync**

Create `tests/test_obsidian_sync.py`:

```python
"""Tests for Obsidian vault ↔ Postgres sync engine."""

from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _make_note(tmp: Path, rel_path: str, content: str) -> Path:
    """Write a markdown file into a temp vault directory."""
    fp = tmp / rel_path
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(content)
    return fp


SAMPLE_NOTE = """\
---
title: Test Tool
domain: tools
status: evaluating
tags: [testing, ci]
confidence: confirmed
priority: medium
---

# Test Tool

This is a test tool evaluation.
"""

SAMPLE_NOTE_NO_FM = """\
# Raw Note

No frontmatter here, just markdown.
"""


class TestParseFrontmatter:
    def test_parses_yaml_frontmatter(self):
        from ingestion.altdata.obsidian_sync import parse_frontmatter

        fm, body = parse_frontmatter(SAMPLE_NOTE)
        assert fm["title"] == "Test Tool"
        assert fm["domain"] == "tools"
        assert fm["status"] == "evaluating"
        assert fm["tags"] == ["testing", "ci"]
        assert "# Test Tool" in body

    def test_no_frontmatter_returns_empty_dict(self):
        from ingestion.altdata.obsidian_sync import parse_frontmatter

        fm, body = parse_frontmatter(SAMPLE_NOTE_NO_FM)
        assert fm == {}
        assert "# Raw Note" in body

    def test_empty_string(self):
        from ingestion.altdata.obsidian_sync import parse_frontmatter

        fm, body = parse_frontmatter("")
        assert fm == {}
        assert body == ""


class TestDomainFromPath:
    def test_tools_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("02-Tools/Firecrawl.md") == "tools"

    def test_pipeline_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("01-Pipeline/Inbox.md") == "pipeline"

    def test_alpha_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("03-Alpha/nvda-signal.md") == "alpha"

    def test_intel_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("04-Intel/fed-analysis.md") == "intel"

    def test_grid_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("05-GRID/notes.md") == "grid"

    def test_dashboard_domain(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("00-DASHBOARD.md") == "dashboard"

    def test_unknown_defaults_to_grid(self):
        from ingestion.altdata.obsidian_sync import domain_from_path

        assert domain_from_path("random/file.md") == "grid"


class TestContentHash:
    def test_sha256_of_content(self):
        from ingestion.altdata.obsidian_sync import content_hash

        expected = hashlib.sha256(SAMPLE_NOTE.encode()).hexdigest()
        assert content_hash(SAMPLE_NOTE) == expected


class TestScanVault:
    def test_finds_markdown_files(self):
        from ingestion.altdata.obsidian_sync import scan_vault

        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            _make_note(vault, "02-Tools/Firecrawl.md", SAMPLE_NOTE)
            _make_note(vault, "03-Alpha/signal.md", SAMPLE_NOTE_NO_FM)
            # .obsidian should be skipped
            (vault / ".obsidian").mkdir()
            (vault / ".obsidian" / "config.json").write_text("{}")

            results = scan_vault(vault)
            paths = [r["vault_path"] for r in results]
            assert "02-Tools/Firecrawl.md" in paths
            assert "03-Alpha/signal.md" in paths
            assert not any(".obsidian" in p for p in paths)

    def test_parses_frontmatter_from_files(self):
        from ingestion.altdata.obsidian_sync import scan_vault

        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            _make_note(vault, "02-Tools/Test.md", SAMPLE_NOTE)

            results = scan_vault(vault)
            assert len(results) == 1
            assert results[0]["frontmatter"]["title"] == "Test Tool"
            assert results[0]["domain"] == "tools"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.altdata.obsidian_sync'`

- [ ] **Step 3: Implement the sync engine (inbound half)**

Create `ingestion/altdata/obsidian_sync.py`:

```python
"""
Obsidian vault ↔ Postgres bidirectional sync engine.

Runs every Hermes cycle (5 min). Syncs vault markdown files to
obsidian_notes table and writes agent-pending changes back to vault.
"""

from __future__ import annotations

import hashlib
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
        # Skip .obsidian config directory
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
    """Sync vault files → Postgres. Returns counts of inserts/updates/archives."""
    vault = vault_path or Path(settings.OBSIDIAN_VAULT_PATH)
    if not vault.exists():
        log.warning("Obsidian vault not found at {p}", p=vault)
        return {"inserted": 0, "updated": 0, "archived": 0}

    notes = scan_vault(vault)
    vault_paths = {n["vault_path"] for n in notes}
    now = datetime.now(timezone.utc)
    counts = {"inserted": 0, "updated": 0, "archived": 0}

    with engine.begin() as conn:
        # Fetch existing notes
        rows = conn.execute(text(
            "SELECT id, vault_path, content_hash FROM obsidian_notes"
        )).fetchall()
        existing = {r.vault_path: (r.id, r.content_hash) for r in rows}

        for note in notes:
            vp = note["vault_path"]
            if vp in existing:
                note_id, old_hash = existing[vp]
                if old_hash != note["content_hash"]:
                    # File changed — update
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
                        "frontmatter": _json_str(note["frontmatter"]),
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
                # New file — insert
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
                    "frontmatter": _json_str(note["frontmatter"]),
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

        # Archive notes whose vault files are gone
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
    import json
    conn.execute(text("""
        INSERT INTO obsidian_actions (note_id, actor, action, detail)
        VALUES (:note_id, :actor, :action, :detail)
    """), {
        "note_id": note_id,
        "actor": actor,
        "action": action,
        "detail": json.dumps(detail),
    })


def _json_str(val: Any) -> str:
    """Serialize dict to JSON string for Postgres JSONB columns."""
    import json
    return json.dumps(val) if isinstance(val, dict) else str(val)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py -v`
Expected: All tests PASS (the pure-function tests don't need a DB)

- [ ] **Step 5: Commit**

```bash
git add ingestion/altdata/obsidian_sync.py tests/test_obsidian_sync.py
git commit -m "feat(obsidian): sync engine inbound — vault to Postgres"
```

---

### Task 4: Sync Engine — Postgres → Vault (Outbound)

**Files:**
- Modify: `ingestion/altdata/obsidian_sync.py`
- Test: `tests/test_obsidian_sync.py`

- [ ] **Step 1: Write failing tests for outbound sync**

Append to `tests/test_obsidian_sync.py`:

```python
class TestBuildFrontmatter:
    def test_renders_yaml_block(self):
        from ingestion.altdata.obsidian_sync import build_frontmatter

        fm = {"title": "Test", "domain": "tools", "status": "approved", "tags": ["a", "b"]}
        result = build_frontmatter(fm)
        assert result.startswith("---\n")
        assert result.endswith("---\n")
        assert "title: Test" in result
        assert "domain: tools" in result

    def test_empty_frontmatter(self):
        from ingestion.altdata.obsidian_sync import build_frontmatter

        result = build_frontmatter({})
        assert result.startswith("---\n")
        assert result.endswith("---\n")


class TestBuildNoteFile:
    def test_combines_frontmatter_and_body(self):
        from ingestion.altdata.obsidian_sync import build_note_file

        fm = {"title": "Test", "domain": "tools"}
        body = "# Test\n\nSome content."
        result = build_note_file(fm, body)
        assert result.startswith("---\n")
        assert "# Test" in result
        assert "Some content." in result


class TestDomainToFolder:
    def test_tools_folder(self):
        from ingestion.altdata.obsidian_sync import domain_to_folder

        assert domain_to_folder("tools") == "02-Tools"

    def test_alpha_folder(self):
        from ingestion.altdata.obsidian_sync import domain_to_folder

        assert domain_to_folder("alpha") == "03-Alpha"

    def test_unknown_defaults_to_grid(self):
        from ingestion.altdata.obsidian_sync import domain_to_folder

        assert domain_to_folder("unknown") == "05-GRID"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py::TestBuildFrontmatter -v`
Expected: FAIL — `ImportError: cannot import name 'build_frontmatter'`

- [ ] **Step 3: Implement outbound sync functions**

Add to `ingestion/altdata/obsidian_sync.py`:

```python
# ---------------------------------------------------------------------------
# Reverse domain map (domain → folder prefix)
# ---------------------------------------------------------------------------

_FOLDER_MAP: dict[str, str] = {v: k for k, v in _DOMAIN_MAP.items()}


def domain_to_folder(domain: str) -> str:
    """Convert domain name back to vault folder prefix."""
    return _FOLDER_MAP.get(domain, "05-GRID")


def build_frontmatter(fm: dict[str, Any]) -> str:
    """Render a YAML frontmatter block."""
    return "---\n" + yaml.dump(fm, default_flow_style=False, sort_keys=False) + "---\n"


def build_note_file(fm: dict[str, Any], body: str) -> str:
    """Combine frontmatter + body into a complete markdown file."""
    return build_frontmatter(fm) + "\n" + body


def sync_outbound(engine, vault_path: Path | None = None) -> int:
    """Write agent-pending notes from Postgres → vault files.

    Returns count of files written.
    """
    vault = vault_path or Path(settings.OBSIDIAN_VAULT_PATH)
    now = datetime.now(timezone.utc)
    written = 0

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, vault_path, domain, status, title, frontmatter, body, agent_flags
            FROM obsidian_notes
            WHERE agent_flags->>'pending_write' = 'true'
        """)).fetchall()

        for row in rows:
            fm = row.frontmatter if isinstance(row.frontmatter, dict) else {}
            fm.update({
                "title": row.title,
                "domain": row.domain,
                "status": row.status,
                "last_synced": now.isoformat(),
            })

            # Determine file path
            if row.vault_path:
                fpath = vault / row.vault_path
            else:
                folder = domain_to_folder(row.domain)
                slug = row.title.lower().replace(" ", "-").replace("/", "-")
                rel = f"{folder}/{slug}.md"
                fpath = vault / rel
                # Update vault_path in DB
                conn.execute(text(
                    "UPDATE obsidian_notes SET vault_path = :vp WHERE id = :id"
                ), {"vp": rel, "id": row.id})

            fpath.parent.mkdir(parents=True, exist_ok=True)
            content = build_note_file(fm, row.body)
            fpath.write_text(content, encoding="utf-8")

            # Clear pending_write flag, update hash
            new_hash = content_hash(content)
            flags = row.agent_flags if isinstance(row.agent_flags, dict) else {}
            flags.pop("pending_write", None)

            conn.execute(text("""
                UPDATE obsidian_notes
                SET agent_flags = :flags, content_hash = :hash, synced_at = :now
                WHERE id = :id
            """), {
                "flags": _json_str(flags),
                "hash": new_hash,
                "now": now,
                "id": row.id,
            })

            _log_action(conn, row.id, "sync", "updated", {
                "reason": "outbound write to vault",
                "vault_path": str(fpath.relative_to(vault)),
            })
            written += 1

    if written:
        log.info("Obsidian outbound sync: {n} files written to vault", n=written)
    return written


def run_sync(engine, vault_path: Path | None = None) -> dict[str, Any]:
    """Run full bidirectional sync. Outbound first (agent writes),
    then inbound (pick up human edits)."""
    outbound = sync_outbound(engine, vault_path)
    inbound = sync_inbound(engine, vault_path)
    return {"outbound_written": outbound, **inbound}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ingestion/altdata/obsidian_sync.py tests/test_obsidian_sync.py
git commit -m "feat(obsidian): sync engine outbound — Postgres to vault files"
```

---

### Task 5: Dashboard Regeneration

**Files:**
- Modify: `ingestion/altdata/obsidian_sync.py`
- Test: `tests/test_obsidian_sync.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_obsidian_sync.py`:

```python
class TestGenerateDashboard:
    def test_generates_markdown_with_stats(self):
        from ingestion.altdata.obsidian_sync import generate_dashboard

        notes = [
            {"domain": "tools", "status": "inbox", "title": "T1", "agent_flags": {"needs_human_review": True, "priority": "urgent"}},
            {"domain": "tools", "status": "approved", "title": "T2", "agent_flags": {}},
            {"domain": "alpha", "status": "evaluating", "title": "A1", "agent_flags": {"needs_human_review": True, "priority": "high"}},
        ]
        actions = [
            {"action": "created", "detail": {"vault_path": "03-Alpha/signal.md"}, "actor": "hermes", "created_at": "2026-04-04T12:00:00Z"},
        ]
        md = generate_dashboard(notes, actions)
        assert "# GRID Intelligence Vault" in md
        assert "Needs Your Review" in md
        assert "Pipeline Stats" in md
        assert "tools" in md.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py::TestGenerateDashboard -v`
Expected: FAIL

- [ ] **Step 3: Implement dashboard generator**

Add to `ingestion/altdata/obsidian_sync.py`:

```python
def generate_dashboard(
    notes: list[dict[str, Any]],
    recent_actions: list[dict[str, Any]],
) -> str:
    """Build the 00-DASHBOARD.md content from current state."""
    lines = ["# GRID Intelligence Vault\n"]

    # Items needing review
    review_items = [
        n for n in notes
        if isinstance(n.get("agent_flags"), dict)
        and n["agent_flags"].get("needs_human_review")
    ]
    if review_items:
        lines.append("## Needs Your Review\n")
        for item in sorted(review_items, key=lambda x: _priority_rank(x.get("agent_flags", {}).get("priority", "low"))):
            pri = (item.get("agent_flags") or {}).get("priority", "medium").upper()
            lines.append(f"- [{pri}] {item['domain'].title()}: {item['title']}")
        lines.append("")

    # Recent agent actions
    if recent_actions:
        lines.append("## Recent Agent Actions\n")
        for act in recent_actions[:10]:
            detail = act.get("detail", {})
            vp = detail.get("vault_path", "")
            reason = detail.get("reason", act.get("action", ""))
            lines.append(f"- {act['action'].title()}: {vp} ({reason})")
        lines.append("")

    # Pipeline stats
    lines.append("## Pipeline Stats\n")
    lines.append("| Domain | Inbox | Evaluating | Approved | Rejected | Active |")
    lines.append("|--------|-------|------------|----------|----------|--------|")
    domains = ("tools", "alpha", "intel", "pipeline", "grid")
    for d in domains:
        domain_notes = [n for n in notes if n.get("domain") == d]
        counts = {s: 0 for s in ("inbox", "evaluating", "approved", "rejected", "active")}
        for n in domain_notes:
            s = n.get("status", "active")
            if s in counts:
                counts[s] += 1
        lines.append(
            f"| {d:<8} | {counts['inbox']:<5} | {counts['evaluating']:<10} | "
            f"{counts['approved']:<8} | {counts['rejected']:<8} | {counts['active']:<6} |"
        )
    lines.append("")

    return "\n".join(lines)


def _priority_rank(priority: str) -> int:
    """Lower number = higher priority (for sorting)."""
    return {"urgent": 0, "high": 1, "medium": 2, "low": 3}.get(priority, 4)


def regenerate_dashboard(engine, vault_path: Path | None = None) -> None:
    """Query DB and write 00-DASHBOARD.md to vault."""
    vault = vault_path or Path(settings.OBSIDIAN_VAULT_PATH)

    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT domain, status, title, agent_flags FROM obsidian_notes WHERE status != 'archived'"
        )).fetchall()
        notes = [
            {"domain": r.domain, "status": r.status, "title": r.title,
             "agent_flags": r.agent_flags if isinstance(r.agent_flags, dict) else {}}
            for r in rows
        ]

        action_rows = conn.execute(text(
            "SELECT action, detail, actor, created_at FROM obsidian_actions ORDER BY created_at DESC LIMIT 20"
        )).fetchall()
        actions = [
            {"action": r.action, "detail": r.detail if isinstance(r.detail, dict) else {},
             "actor": r.actor, "created_at": str(r.created_at)}
            for r in action_rows
        ]

    md = generate_dashboard(notes, actions)
    dash_path = vault / "00-DASHBOARD.md"
    dash_path.write_text(md, encoding="utf-8")
    log.info("Dashboard regenerated at {p}", p=dash_path)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ingestion/altdata/obsidian_sync.py tests/test_obsidian_sync.py
git commit -m "feat(obsidian): dashboard regeneration from DB state"
```

---

### Task 6: API Router — Vault Endpoints

**Files:**
- Create: `api/routers/vault.py`
- Modify: `api/main.py:526-528`
- Test: `tests/test_vault_api.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_vault_api.py`:

```python
"""Tests for vault API router."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestVaultRouter:
    """Test that vault router endpoints are importable and well-formed."""

    def test_router_has_correct_prefix(self):
        from api.routers.vault import router
        assert router.prefix == "/api/v1/vault"

    def test_router_has_tag(self):
        from api.routers.vault import router
        assert "vault" in router.tags

    def test_list_notes_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert "/notes" in paths

    def test_search_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert "/search" in paths

    def test_dashboard_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert "/dashboard" in paths
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_vault_api.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the router**

Create `api/routers/vault.py`:

```python
"""Vault API — CRUD, search, and dashboard for Obsidian bridge."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from loguru import logger as log

router = APIRouter(
    prefix="/api/v1/vault",
    tags=["vault"],
    dependencies=[Depends(require_auth)],
)


def _engine():
    from db import get_engine
    return get_engine()


# ── Request / Response Models ────────────────────────────────────

class NoteOut(BaseModel):
    id: int
    vault_path: str
    domain: str
    status: str
    title: str
    frontmatter: dict[str, Any]
    body: str
    agent_flags: dict[str, Any]
    modified_at: str
    created_at: str


class NoteCreate(BaseModel):
    title: str
    domain: str
    body: str
    status: str = "inbox"
    tags: list[str] = []


class NoteUpdate(BaseModel):
    title: str | None = None
    body: str | None = None
    status: str | None = None
    tags: list[str] | None = None


class StatusChange(BaseModel):
    status: str


class ActionOut(BaseModel):
    id: int
    note_id: int
    actor: str
    action: str
    detail: dict[str, Any]
    created_at: str


# ── Endpoints ────────────────────────────────────────────────────

@router.get("/notes")
async def list_notes(
    domain: str | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """List vault notes with optional filters."""
    eng = _engine()
    clauses = ["status != 'archived'"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if domain:
        clauses.append("domain = :domain")
        params["domain"] = domain
    if status:
        clauses.append("status = :status")
        params["status"] = status

    where = " AND ".join(clauses)

    with eng.connect() as conn:
        total = conn.execute(text(
            f"SELECT COUNT(*) FROM obsidian_notes WHERE {where}"
        ), params).scalar()

        rows = conn.execute(text(f"""
            SELECT id, vault_path, domain, status, title, frontmatter, body,
                   agent_flags, modified_at, created_at
            FROM obsidian_notes
            WHERE {where}
            ORDER BY modified_at DESC
            LIMIT :limit OFFSET :offset
        """), params).fetchall()

    return {
        "total": total,
        "notes": [_row_to_note(r) for r in rows],
    }


@router.get("/notes/{note_id}")
async def get_note(note_id: int) -> NoteOut:
    """Read a single note by ID."""
    eng = _engine()
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT * FROM obsidian_notes WHERE id = :id"
        ), {"id": note_id}).fetchone()

    if not row:
        raise HTTPException(404, "Note not found")
    return _row_to_note(row)


@router.post("/notes", status_code=201)
async def create_note(req: NoteCreate) -> NoteOut:
    """Create a new vault note. Will be synced to vault on next cycle."""
    eng = _engine()
    import json
    from ingestion.altdata.obsidian_sync import domain_to_folder, content_hash as chash

    slug = req.title.lower().replace(" ", "-").replace("/", "-")
    folder = domain_to_folder(req.domain)
    vault_path = f"{folder}/{slug}.md"
    fm = {"title": req.title, "domain": req.domain, "status": req.status, "tags": req.tags}
    now = datetime.now(timezone.utc)

    with eng.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO obsidian_notes
                (vault_path, domain, status, title, content_hash, frontmatter, body,
                 agent_flags, modified_at, synced_at, created_at)
            VALUES
                (:vault_path, :domain, :status, :title, :content_hash, :frontmatter, :body,
                 :agent_flags, :modified_at, :synced_at, :created_at)
            RETURNING *
        """), {
            "vault_path": vault_path,
            "domain": req.domain,
            "status": req.status,
            "title": req.title,
            "content_hash": chash(req.body),
            "frontmatter": json.dumps(fm),
            "body": req.body,
            "agent_flags": json.dumps({"pending_write": True}),
            "modified_at": now,
            "synced_at": now,
            "created_at": now,
        })
        row = result.fetchone()

        conn.execute(text("""
            INSERT INTO obsidian_actions (note_id, actor, action, detail)
            VALUES (:note_id, 'user', 'created', :detail)
        """), {"note_id": row.id, "detail": json.dumps({"via": "api", "title": req.title})})

    return _row_to_note(row)


@router.patch("/notes/{note_id}/status")
async def change_status(note_id: int, req: StatusChange) -> NoteOut:
    """Change a note's status (approve, reject, archive)."""
    eng = _engine()
    import json
    now = datetime.now(timezone.utc)

    with eng.begin() as conn:
        old = conn.execute(text(
            "SELECT id, status FROM obsidian_notes WHERE id = :id"
        ), {"id": note_id}).fetchone()

        if not old:
            raise HTTPException(404, "Note not found")

        conn.execute(text("""
            UPDATE obsidian_notes
            SET status = :status, agent_flags = agent_flags || :flag, modified_at = :now
            WHERE id = :id
        """), {
            "id": note_id,
            "status": req.status,
            "flag": json.dumps({"pending_write": True}),
            "now": now,
        })

        conn.execute(text("""
            INSERT INTO obsidian_actions (note_id, actor, action, detail)
            VALUES (:note_id, 'user', 'status_changed', :detail)
        """), {
            "note_id": note_id,
            "detail": json.dumps({
                "old_status": old.status,
                "new_status": req.status,
                "via": "api",
            }),
        })

        row = conn.execute(text(
            "SELECT * FROM obsidian_notes WHERE id = :id"
        ), {"id": note_id}).fetchone()

    return _row_to_note(row)


@router.get("/search")
async def search_notes(
    q: str = Query(..., min_length=1),
    domain: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Full-text search across vault notes."""
    eng = _engine()
    params: dict[str, Any] = {"q": q, "limit": limit}
    domain_clause = ""
    if domain:
        domain_clause = "AND domain = :domain"
        params["domain"] = domain

    with eng.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, vault_path, domain, status, title, frontmatter, body,
                   agent_flags, modified_at, created_at,
                   ts_rank(body_tsvector, plainto_tsquery('english', :q)) AS rank
            FROM obsidian_notes
            WHERE body_tsvector @@ plainto_tsquery('english', :q)
              AND status != 'archived'
              {domain_clause}
            ORDER BY rank DESC
            LIMIT :limit
        """), params).fetchall()

    return {
        "query": q,
        "results": [_row_to_note(r) for r in rows],
    }


@router.get("/actions")
async def list_actions(
    note_id: int | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> list[dict[str, Any]]:
    """Audit log of vault actions."""
    eng = _engine()
    params: dict[str, Any] = {"limit": limit}
    where = "1=1"
    if note_id:
        where = "note_id = :note_id"
        params["note_id"] = note_id

    with eng.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, note_id, actor, action, detail, created_at
            FROM obsidian_actions
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT :limit
        """), params).fetchall()

    return [
        {
            "id": r.id, "note_id": r.note_id, "actor": r.actor,
            "action": r.action, "detail": r.detail, "created_at": str(r.created_at),
        }
        for r in rows
    ]


@router.get("/dashboard")
async def vault_dashboard() -> dict[str, Any]:
    """Prioritized items for review + stats."""
    eng = _engine()
    with eng.connect() as conn:
        notes = conn.execute(text(
            "SELECT domain, status, title, agent_flags FROM obsidian_notes WHERE status != 'archived'"
        )).fetchall()

        actions = conn.execute(text(
            "SELECT action, detail, actor, created_at FROM obsidian_actions ORDER BY created_at DESC LIMIT 20"
        )).fetchall()

    review_items = []
    stats: dict[str, dict[str, int]] = {}

    for n in notes:
        flags = n.agent_flags if isinstance(n.agent_flags, dict) else {}
        if flags.get("needs_human_review"):
            review_items.append({
                "domain": n.domain,
                "title": n.title,
                "priority": flags.get("priority", "medium"),
                "status": n.status,
            })
        stats.setdefault(n.domain, {})
        stats[n.domain][n.status] = stats[n.domain].get(n.status, 0) + 1

    return {
        "review_items": sorted(review_items, key=lambda x: {"urgent": 0, "high": 1, "medium": 2, "low": 3}.get(x["priority"], 4)),
        "stats": stats,
        "recent_actions": [
            {"action": a.action, "actor": a.actor, "detail": a.detail, "created_at": str(a.created_at)}
            for a in actions
        ],
    }


@router.post("/sync")
async def trigger_sync() -> dict[str, Any]:
    """Manually trigger vault ↔ Postgres sync."""
    from ingestion.altdata.obsidian_sync import run_sync
    eng = _engine()
    result = run_sync(eng)
    return {"status": "ok", **result}


# ── Helpers ──────────────────────────────────────────────────────

def _row_to_note(row) -> dict[str, Any]:
    return {
        "id": row.id,
        "vault_path": row.vault_path,
        "domain": row.domain,
        "status": row.status,
        "title": row.title,
        "frontmatter": row.frontmatter if isinstance(row.frontmatter, dict) else {},
        "body": row.body,
        "agent_flags": row.agent_flags if isinstance(row.agent_flags, dict) else {},
        "modified_at": str(row.modified_at),
        "created_at": str(row.created_at),
    }
```

- [ ] **Step 4: Register the router in api/main.py**

In `api/main.py`, add to the router list (around line 526, before the closing `]`):

```python
    ("vault", "api.routers.vault", False),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_vault_api.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add api/routers/vault.py tests/test_vault_api.py api/main.py
git commit -m "feat(obsidian): vault API router with CRUD, FTS search, dashboard"
```

---

### Task 7: MCP Server — Vault Tools

**Files:**
- Modify: `mcp_server.py` (append new tools)
- Test: manual verification via `python mcp_server.py` (FastMCP self-test)

- [ ] **Step 1: Append vault tools to mcp_server.py**

Add at the end of `mcp_server.py`, before the `if __name__` block:

```python
# ---------------------------------------------------------------------------
# Obsidian Vault tools
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_vault_search(query: str, domain: str = "", status: str = "") -> str:
    """Full-text search across GRID's Obsidian knowledge vault.

    Args:
        query: Search terms.
        domain: Optional filter — pipeline, tools, alpha, intel, grid.
        status: Optional filter — inbox, evaluating, approved, rejected, active.
    """
    eng = _get_engine()
    params: dict[str, Any] = {"q": query, "limit": 20}
    clauses = [
        "body_tsvector @@ plainto_tsquery('english', :q)",
        "status != 'archived'",
    ]
    if domain:
        clauses.append("domain = :domain")
        params["domain"] = domain
    if status:
        clauses.append("status = :status")
        params["status"] = status

    where = " AND ".join(clauses)
    with eng.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT vault_path, domain, status, title, body,
                   ts_rank(body_tsvector, plainto_tsquery('english', :q)) AS rank
            FROM obsidian_notes WHERE {where}
            ORDER BY rank DESC LIMIT :limit
        """), params).fetchall()

    if not rows:
        return f"No vault notes matched '{query}'."

    out = [f"## Vault Search: '{query}' ({len(rows)} results)\n"]
    for r in rows:
        out.append(f"### {r.title} [{r.domain}/{r.status}]\n**Path:** {r.vault_path}\n")
        preview = r.body[:500] + ("..." if len(r.body) > 500 else "")
        out.append(preview + "\n")
    return "\n".join(out)


@mcp.tool()
def grid_vault_read(vault_path: str) -> str:
    """Read a specific note from the GRID Obsidian vault.

    Args:
        vault_path: Relative path in vault (e.g., '02-Tools/Firecrawl.md').
    """
    eng = _get_engine()
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT title, domain, status, frontmatter, body, agent_flags FROM obsidian_notes WHERE vault_path = :vp"
        ), {"vp": vault_path}).fetchone()

    if not row:
        return f"Note not found: {vault_path}"

    flags = row.agent_flags if isinstance(row.agent_flags, dict) else {}
    flag_str = ", ".join(f"{k}={v}" for k, v in flags.items()) if flags else "none"
    return (
        f"# {row.title}\n\n"
        f"**Domain:** {row.domain} | **Status:** {row.status} | **Flags:** {flag_str}\n\n"
        f"{row.body}"
    )


@mcp.tool()
def grid_vault_write(title: str, body: str, domain: str = "grid", status: str = "inbox") -> str:
    """Create or update a note in the GRID Obsidian vault.

    Args:
        title: Note title.
        body: Markdown body content.
        domain: One of: pipeline, tools, alpha, intel, grid.
        status: One of: inbox, evaluating, approved, rejected, active.
    """
    eng = _get_engine()
    from ingestion.altdata.obsidian_sync import domain_to_folder, content_hash as chash

    slug = title.lower().replace(" ", "-").replace("/", "-")
    folder = domain_to_folder(domain)
    vault_path = f"{folder}/{slug}.md"
    fm = {"title": title, "domain": domain, "status": status}
    now = datetime.now(timezone.utc).isoformat()

    with eng.begin() as conn:
        existing = conn.execute(text(
            "SELECT id FROM obsidian_notes WHERE vault_path = :vp"
        ), {"vp": vault_path}).fetchone()

        if existing:
            conn.execute(text("""
                UPDATE obsidian_notes
                SET body = :body, title = :title, domain = :domain, status = :status,
                    frontmatter = :fm, content_hash = :hash,
                    agent_flags = agent_flags || '{"pending_write": true}'::jsonb,
                    modified_at = :now
                WHERE id = :id
            """), {
                "body": body, "title": title, "domain": domain, "status": status,
                "fm": json.dumps(fm), "hash": chash(body), "now": now, "id": existing.id,
            })
            return f"Updated existing note: {vault_path}"
        else:
            conn.execute(text("""
                INSERT INTO obsidian_notes
                    (vault_path, domain, status, title, content_hash, frontmatter, body, agent_flags, modified_at, synced_at, created_at)
                VALUES
                    (:vp, :domain, :status, :title, :hash, :fm, :body, '{"pending_write": true}'::jsonb, :now, :now, :now)
            """), {
                "vp": vault_path, "domain": domain, "status": status, "title": title,
                "hash": chash(body), "fm": json.dumps(fm), "body": body, "now": now,
            })
            return f"Created new note: {vault_path} (will sync to vault on next cycle)"


@mcp.tool()
def grid_vault_flag(vault_path: str, priority: str, reason: str) -> str:
    """Flag a vault note for human review.

    Args:
        vault_path: Relative path (e.g., '02-Tools/Firecrawl.md').
        priority: One of: urgent, high, medium, low.
        reason: Why this needs human attention.
    """
    eng = _get_engine()
    with eng.begin() as conn:
        row = conn.execute(text(
            "SELECT id FROM obsidian_notes WHERE vault_path = :vp"
        ), {"vp": vault_path}).fetchone()

        if not row:
            return f"Note not found: {vault_path}"

        conn.execute(text("""
            UPDATE obsidian_notes
            SET agent_flags = agent_flags || :flags
            WHERE id = :id
        """), {
            "id": row.id,
            "flags": json.dumps({"needs_human_review": True, "priority": priority, "flag_reason": reason}),
        })

        conn.execute(text("""
            INSERT INTO obsidian_actions (note_id, actor, action, detail)
            VALUES (:nid, 'claude', 'flagged', :detail)
        """), {"nid": row.id, "detail": json.dumps({"priority": priority, "reason": reason})})

    return f"Flagged {vault_path} as {priority}: {reason}"


@mcp.tool()
def grid_vault_act(vault_path: str, action_type: str, detail: str = "") -> str:
    """Trigger a downstream action on a vault note.

    Args:
        vault_path: Relative path.
        action_type: One of: approve, reject, archive, create_trade_ticket, add_to_backlog.
        detail: Optional context for the action.
    """
    eng = _get_engine()
    with eng.begin() as conn:
        row = conn.execute(text(
            "SELECT id, domain, status, title FROM obsidian_notes WHERE vault_path = :vp"
        ), {"vp": vault_path}).fetchone()

        if not row:
            return f"Note not found: {vault_path}"

        status_map = {"approve": "approved", "reject": "rejected", "archive": "archived"}
        new_status = status_map.get(action_type)

        if new_status:
            conn.execute(text(
                "UPDATE obsidian_notes SET status = :s, agent_flags = agent_flags || '{\"pending_write\": true}'::jsonb WHERE id = :id"
            ), {"s": new_status, "id": row.id})

        conn.execute(text("""
            INSERT INTO obsidian_actions (note_id, actor, action, detail)
            VALUES (:nid, 'claude', 'acted_on', :detail)
        """), {"nid": row.id, "detail": json.dumps({"action_type": action_type, "detail": detail})})

    return f"Action '{action_type}' applied to {vault_path}"
```

- [ ] **Step 2: Verify MCP server imports cleanly**

Run: `cd /Users/anikdang/dev/GRID && python -c "from mcp_server import mcp; print([t.name for t in mcp._tools.values()])"`
Expected: List includes `grid_vault_search`, `grid_vault_read`, `grid_vault_write`, `grid_vault_flag`, `grid_vault_act`

- [ ] **Step 3: Commit**

```bash
git add mcp_server.py
git commit -m "feat(obsidian): add 5 vault MCP tools for Claude integration"
```

---

### Task 8: Obsidian Agent — Active Loop

**Files:**
- Create: `intelligence/obsidian_agent.py`
- Test: `tests/test_obsidian_agent.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_obsidian_agent.py`:

```python
"""Tests for the active Obsidian agent."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestExtractEntities:
    def test_extracts_tickers(self):
        from intelligence.obsidian_agent import extract_entities

        text = "Watching $NVDA and $AAPL for earnings momentum. Also $BTC looking strong."
        entities = extract_entities(text)
        assert "NVDA" in entities["tickers"]
        assert "AAPL" in entities["tickers"]
        assert "BTC" in entities["tickers"]

    def test_no_tickers_returns_empty(self):
        from intelligence.obsidian_agent import extract_entities

        entities = extract_entities("No market references here.")
        assert entities["tickers"] == []

    def test_extracts_cashtags_and_plain(self):
        from intelligence.obsidian_agent import extract_entities

        text = "NVDA up 5% after $MSFT earnings beat"
        entities = extract_entities(text)
        assert "NVDA" in entities["tickers"]
        assert "MSFT" in entities["tickers"]


class TestPriorityRanking:
    def test_urgent_sorts_first(self):
        from intelligence.obsidian_agent import rank_for_review

        items = [
            {"agent_flags": {"priority": "low"}, "title": "A"},
            {"agent_flags": {"priority": "urgent"}, "title": "B"},
            {"agent_flags": {"priority": "high"}, "title": "C"},
        ]
        ranked = rank_for_review(items)
        assert ranked[0]["title"] == "B"
        assert ranked[1]["title"] == "C"
        assert ranked[2]["title"] == "A"


class TestShouldEscalateToPaid:
    def test_low_confidence_triggers_escalation(self):
        from intelligence.obsidian_agent import should_escalate_to_paid

        result = {"confidence": 0.3, "coherent": False}
        assert should_escalate_to_paid(result) is True

    def test_high_confidence_no_escalation(self):
        from intelligence.obsidian_agent import should_escalate_to_paid

        result = {"confidence": 0.9, "coherent": True}
        assert should_escalate_to_paid(result) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_agent.py -v`
Expected: FAIL

- [ ] **Step 3: Implement the agent**

Create `intelligence/obsidian_agent.py`:

```python
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

_TICKER_RE = re.compile(r"\$([A-Z]{1,6})\b|(?<!\w)([A-Z]{2,5})(?=\s+(?:up|down|rally|drop|surge|crash|beat|miss|earnings|revenue|price|stock))")


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

    # Check tickers against actors table
    for ticker in entities["tickers"]:
        rows = conn.execute(text("""
            SELECT name, category FROM actors
            WHERE name ILIKE :pat OR metadata->>'primary_ticker' = :ticker
            LIMIT 3
        """), {"pat": f"%{ticker}%", "ticker": ticker}).fetchall()
        for r in rows:
            refs.append(f"- **Actor:** {r.name} ({r.category}) — linked via {ticker}")

    # Check tickers against recent signals
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

    # Append or replace cross-references section
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
    """Execute downstream effects when a note is approved.

    Returns list of actions taken.
    """
    actions: list[str] = []
    domain = note["domain"]
    title = note["title"]
    now = datetime.now(timezone.utc)

    if domain == "alpha":
        # Create prediction stub in oracle_predictions if ticker found
        entities = extract_entities(note["body"])
        for ticker in entities["tickers"][:1]:  # first ticker only
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
        # Try to link to actors
        entities = extract_entities(note["body"])
        for ticker in entities["tickers"]:
            actions.append(f"Intel note '{title}' — flagged for actor enrichment ({ticker})")

    return actions


# ---------------------------------------------------------------------------
# Main agent cycle
# ---------------------------------------------------------------------------

def run_agent_cycle(engine) -> dict[str, Any]:
    """Run one full agent cycle: react, enrich, prioritize, act.

    Called by Hermes operator after sync.
    """
    stats = {"enriched": 0, "flagged": 0, "acted": 0}
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        # Find recently changed notes (last 10 minutes)
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
            # ENRICH: cross-reference
            new_body = enrich_note(conn, note.id, note.body)
            if new_body != note.body:
                conn.execute(text(
                    "UPDATE obsidian_notes SET body = :body, agent_flags = agent_flags || '{\"pending_write\": true}'::jsonb WHERE id = :id"
                ), {"body": new_body, "id": note.id})
                _log(conn, note.id, "hermes", "updated", {"reason": "cross-reference enrichment"})
                stats["enriched"] += 1

            # PRIORITIZE: flag new inbox items for review
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

            # ACT: handle approved notes
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_agent.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add intelligence/obsidian_agent.py tests/test_obsidian_agent.py
git commit -m "feat(obsidian): active agent — enrich, prioritize, act on vault notes"
```

---

### Task 9: Hermes Integration

**Files:**
- Modify: `scripts/hermes_operator.py` (add obsidian sync step)

- [ ] **Step 1: Find the cycle loop in hermes_operator.py**

Search for the main cycle function — look for `_run_cycle` or similar. The obsidian sync should run after health check but before pipeline work.

- [ ] **Step 2: Add obsidian sync + agent step**

Add to the source registry:

```python
"obsidian": {"mod": "ingestion.altdata.obsidian_sync", "fn": "run_sync", "interval_h": 0.083},  # every 5 min
```

Add a new method to the operator class:

```python
def _run_obsidian_cycle(self) -> None:
    """Run vault sync + agent loop."""
    try:
        from ingestion.altdata.obsidian_sync import run_sync, regenerate_dashboard
        from intelligence.obsidian_agent import run_agent_cycle

        eng = self._get_engine()

        # 1. Sync vault <-> Postgres
        sync_result = run_sync(eng)
        log.info("Obsidian sync: {r}", r=sync_result)

        # 2. Run active agent
        agent_result = run_agent_cycle(eng)
        log.info("Obsidian agent: {r}", r=agent_result)

        # 3. Regenerate dashboard if anything changed
        total_changes = (
            sync_result.get("inserted", 0) + sync_result.get("updated", 0)
            + sync_result.get("outbound_written", 0)
            + agent_result.get("enriched", 0) + agent_result.get("acted", 0)
        )
        if total_changes > 0:
            regenerate_dashboard(eng)

    except Exception as e:
        log.error("Obsidian cycle failed: {e}", e=e)
```

Call `_run_obsidian_cycle()` at the start of each Hermes cycle, after health check.

- [ ] **Step 3: Verify hermes operator still imports cleanly**

Run: `cd /Users/anikdang/dev/GRID && python -c "import scripts.hermes_operator; print('OK')"`
Expected: `OK` (no import errors)

- [ ] **Step 4: Commit**

```bash
git add scripts/hermes_operator.py
git commit -m "feat(obsidian): integrate vault sync into Hermes operator cycle"
```

---

### Task 10: Frontend — Vault View

**Files:**
- Create: `pwa/src/views/Vault.jsx`
- Modify: `pwa/src/App.jsx` (add route)
- Modify: `pwa/src/api.js` (add vault methods)

- [ ] **Step 1: Add vault API methods to api.js**

Find the end of the `GRIDApi` class methods in `pwa/src/api.js` and add:

```javascript
    // ── Vault ────────────────────────────────────────────────────
    async vaultNotes(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/vault/notes?${qs}`);
    }

    async vaultNote(id) {
        return this._fetch(`/api/v1/vault/notes/${id}`);
    }

    async vaultSearch(q, domain = '') {
        const qs = new URLSearchParams({ q, ...(domain && { domain }) }).toString();
        return this._fetch(`/api/v1/vault/search?${qs}`);
    }

    async vaultDashboard() {
        return this._fetch('/api/v1/vault/dashboard');
    }

    async vaultChangeStatus(id, status) {
        return this._fetch(`/api/v1/vault/notes/${id}/status`, {
            method: 'PATCH',
            body: JSON.stringify({ status }),
        });
    }

    async vaultCreateNote(data) {
        return this._fetch('/api/v1/vault/notes', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }

    async vaultSync() {
        return this._fetch('/api/v1/vault/sync', { method: 'POST' });
    }

    async vaultActions(noteId = null, limit = 50) {
        const qs = new URLSearchParams({ limit, ...(noteId && { note_id: noteId }) }).toString();
        return this._fetch(`/api/v1/vault/actions?${qs}`);
    }
```

- [ ] **Step 2: Register vault route in App.jsx**

In `pwa/src/App.jsx`, add to the `routeComponents` object:

```javascript
    vault:              React.lazy(() => import('./views/Vault.jsx')),
```

- [ ] **Step 3: Create Vault.jsx**

Create `pwa/src/views/Vault.jsx`:

```jsx
import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import {
    Search, CheckCircle, XCircle, Archive, AlertTriangle,
    RefreshCw, Plus, ChevronDown, FileText, Activity
} from 'lucide-react';

const DOMAINS = ['pipeline', 'tools', 'alpha', 'intel', 'grid'];
const STATUSES = ['inbox', 'evaluating', 'approved', 'rejected', 'active'];

const PRIORITY_COLORS = {
    urgent: '#ef4444',
    high: '#f59e0b',
    medium: '#6b7280',
    low: '#374151',
};

const STATUS_ICONS = {
    inbox: FileText,
    evaluating: Activity,
    approved: CheckCircle,
    rejected: XCircle,
    active: CheckCircle,
};

export default function Vault() {
    const [domain, setDomain] = useState('');
    const [status, setStatus] = useState('');
    const [notes, setNotes] = useState([]);
    const [total, setTotal] = useState(0);
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState(null);
    const [dashboard, setDashboard] = useState(null);
    const [selectedNote, setSelectedNote] = useState(null);
    const [actions, setActions] = useState([]);
    const [loading, setLoading] = useState(false);
    const [syncing, setSyncing] = useState(false);

    const loadNotes = useCallback(async () => {
        setLoading(true);
        try {
            const params = {};
            if (domain) params.domain = domain;
            if (status) params.status = status;
            const data = await api.vaultNotes(params);
            setNotes(data.notes || []);
            setTotal(data.total || 0);
        } catch (e) {
            console.error('Failed to load notes:', e);
        }
        setLoading(false);
    }, [domain, status]);

    const loadDashboard = useCallback(async () => {
        try {
            const data = await api.vaultDashboard();
            setDashboard(data);
        } catch (e) {
            console.error('Failed to load dashboard:', e);
        }
    }, []);

    useEffect(() => { loadNotes(); loadDashboard(); }, [loadNotes, loadDashboard]);

    const handleSearch = async () => {
        if (!searchQuery.trim()) { setSearchResults(null); return; }
        const data = await api.vaultSearch(searchQuery, domain);
        setSearchResults(data.results || []);
    };

    const handleStatusChange = async (noteId, newStatus) => {
        await api.vaultChangeStatus(noteId, newStatus);
        loadNotes();
        loadDashboard();
    };

    const handleSync = async () => {
        setSyncing(true);
        await api.vaultSync();
        await loadNotes();
        await loadDashboard();
        setSyncing(false);
    };

    const selectNote = async (note) => {
        setSelectedNote(note);
        const acts = await api.vaultActions(note.id);
        setActions(acts || []);
    };

    const displayNotes = searchResults || notes;

    return (
        <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
                <h1 style={{ fontSize: 24, fontWeight: 700 }}>Intelligence Vault</h1>
                <button
                    onClick={handleSync}
                    disabled={syncing}
                    style={{
                        display: 'flex', alignItems: 'center', gap: 8, padding: '8px 16px',
                        background: '#1a1a2e', border: '1px solid #333', borderRadius: 8,
                        color: '#e0e0e0', cursor: 'pointer',
                    }}
                >
                    <RefreshCw size={16} className={syncing ? 'spin' : ''} />
                    {syncing ? 'Syncing...' : 'Sync Now'}
                </button>
            </div>

            {/* Review items from dashboard */}
            {dashboard?.review_items?.length > 0 && (
                <div style={{
                    background: '#1a1a2e', border: '1px solid #333', borderRadius: 12,
                    padding: 16, marginBottom: 24,
                }}>
                    <h3 style={{ fontSize: 14, color: '#888', marginBottom: 12 }}>NEEDS YOUR REVIEW</h3>
                    {dashboard.review_items.map((item, i) => (
                        <div key={i} style={{
                            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                            padding: '8px 0', borderBottom: i < dashboard.review_items.length - 1 ? '1px solid #222' : 'none',
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <span style={{
                                    fontSize: 11, fontWeight: 700, padding: '2px 6px', borderRadius: 4,
                                    background: PRIORITY_COLORS[item.priority] + '22',
                                    color: PRIORITY_COLORS[item.priority],
                                }}>
                                    {item.priority.toUpperCase()}
                                </span>
                                <span style={{ color: '#aaa', fontSize: 12 }}>{item.domain}</span>
                                <span>{item.title}</span>
                            </div>
                            <div style={{ display: 'flex', gap: 8 }}>
                                <button
                                    onClick={() => handleStatusChange(item.id, 'approved')}
                                    style={{ background: '#16a34a22', border: '1px solid #16a34a', borderRadius: 6, padding: '4px 12px', color: '#16a34a', cursor: 'pointer' }}
                                >
                                    Approve
                                </button>
                                <button
                                    onClick={() => handleStatusChange(item.id, 'rejected')}
                                    style={{ background: '#ef444422', border: '1px solid #ef4444', borderRadius: 6, padding: '4px 12px', color: '#ef4444', cursor: 'pointer' }}
                                >
                                    Reject
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Search + Filters */}
            <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
                <div style={{ display: 'flex', flex: 1, minWidth: 200 }}>
                    <input
                        type="text"
                        placeholder="Search vault..."
                        value={searchQuery}
                        onChange={e => setSearchQuery(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && handleSearch()}
                        style={{
                            flex: 1, padding: '8px 12px', background: '#111', border: '1px solid #333',
                            borderRadius: '8px 0 0 8px', color: '#e0e0e0', outline: 'none',
                        }}
                    />
                    <button onClick={handleSearch} style={{
                        padding: '8px 12px', background: '#1a1a2e', border: '1px solid #333',
                        borderLeft: 'none', borderRadius: '0 8px 8px 0', color: '#e0e0e0', cursor: 'pointer',
                    }}>
                        <Search size={16} />
                    </button>
                </div>

                <select value={domain} onChange={e => setDomain(e.target.value)} style={{
                    padding: '8px 12px', background: '#111', border: '1px solid #333',
                    borderRadius: 8, color: '#e0e0e0',
                }}>
                    <option value="">All Domains</option>
                    {DOMAINS.map(d => <option key={d} value={d}>{d}</option>)}
                </select>

                <select value={status} onChange={e => setStatus(e.target.value)} style={{
                    padding: '8px 12px', background: '#111', border: '1px solid #333',
                    borderRadius: 8, color: '#e0e0e0',
                }}>
                    <option value="">All Statuses</option>
                    {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
            </div>

            {/* Notes list + detail panel */}
            <div style={{ display: 'grid', gridTemplateColumns: selectedNote ? '1fr 1fr' : '1fr', gap: 16 }}>
                <div>
                    <div style={{ fontSize: 12, color: '#666', marginBottom: 8 }}>
                        {searchResults ? `${searchResults.length} search results` : `${total} notes`}
                    </div>
                    {displayNotes.map(note => {
                        const Icon = STATUS_ICONS[note.status] || FileText;
                        return (
                            <div
                                key={note.id}
                                onClick={() => selectNote(note)}
                                style={{
                                    padding: 12, background: selectedNote?.id === note.id ? '#1a1a3e' : '#111',
                                    border: '1px solid #222', borderRadius: 8, marginBottom: 8, cursor: 'pointer',
                                }}
                            >
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                                    <Icon size={14} style={{ color: note.status === 'approved' ? '#16a34a' : note.status === 'rejected' ? '#ef4444' : '#6b7280' }} />
                                    <span style={{ fontWeight: 600 }}>{note.title}</span>
                                    <span style={{ fontSize: 11, color: '#666', marginLeft: 'auto' }}>{note.domain}</span>
                                </div>
                                <div style={{ fontSize: 12, color: '#888' }}>
                                    {note.vault_path} &middot; {note.status}
                                </div>
                            </div>
                        );
                    })}
                </div>

                {selectedNote && (
                    <div style={{ background: '#111', border: '1px solid #222', borderRadius: 12, padding: 16, maxHeight: '80vh', overflow: 'auto' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                            <h2 style={{ fontSize: 18, fontWeight: 700 }}>{selectedNote.title}</h2>
                            <div style={{ display: 'flex', gap: 8 }}>
                                {selectedNote.status !== 'approved' && (
                                    <button onClick={() => handleStatusChange(selectedNote.id, 'approved')} style={{ background: '#16a34a22', border: '1px solid #16a34a', borderRadius: 6, padding: '4px 12px', color: '#16a34a', cursor: 'pointer', fontSize: 12 }}>Approve</button>
                                )}
                                {selectedNote.status !== 'rejected' && (
                                    <button onClick={() => handleStatusChange(selectedNote.id, 'rejected')} style={{ background: '#ef444422', border: '1px solid #ef4444', borderRadius: 6, padding: '4px 12px', color: '#ef4444', cursor: 'pointer', fontSize: 12 }}>Reject</button>
                                )}
                                <button onClick={() => handleStatusChange(selectedNote.id, 'archived')} style={{ background: '#33333322', border: '1px solid #333', borderRadius: 6, padding: '4px 12px', color: '#888', cursor: 'pointer', fontSize: 12 }}>Archive</button>
                            </div>
                        </div>

                        <div style={{ fontSize: 12, color: '#888', marginBottom: 16, display: 'flex', gap: 16 }}>
                            <span>{selectedNote.domain}</span>
                            <span>{selectedNote.status}</span>
                            <span>{selectedNote.vault_path}</span>
                        </div>

                        <pre style={{ whiteSpace: 'pre-wrap', fontFamily: 'inherit', fontSize: 14, color: '#ccc', lineHeight: 1.6 }}>
                            {selectedNote.body}
                        </pre>

                        {actions.length > 0 && (
                            <div style={{ marginTop: 24, borderTop: '1px solid #222', paddingTop: 16 }}>
                                <h3 style={{ fontSize: 14, color: '#888', marginBottom: 8 }}>Activity Log</h3>
                                {actions.map(a => (
                                    <div key={a.id} style={{ fontSize: 12, color: '#666', padding: '4px 0' }}>
                                        <span style={{ color: '#aaa' }}>{a.actor}</span> {a.action}
                                        {a.detail?.reason && <span> — {a.detail.reason}</span>}
                                        <span style={{ marginLeft: 8, color: '#444' }}>{new Date(a.created_at).toLocaleString()}</span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
```

- [ ] **Step 4: Verify frontend builds**

Run: `cd /Users/anikdang/dev/GRID/pwa && npm run build`
Expected: Build succeeds with no errors

- [ ] **Step 5: Commit**

```bash
git add pwa/src/views/Vault.jsx pwa/src/api.js pwa/src/App.jsx
git commit -m "feat(obsidian): vault frontend view with search, approve/reject, activity log"
```

---

### Task 11: Integration Test — End-to-End

**Files:**
- Test: manual verification

- [ ] **Step 1: Apply schema to DB**

Run: `psql -U grid_user -d grid -f schema_obsidian.sql`

- [ ] **Step 2: Run initial sync to populate DB from existing vault**

```bash
cd /Users/anikdang/dev/GRID && python -c "
from db import get_engine
from ingestion.altdata.obsidian_sync import run_sync, regenerate_dashboard
eng = get_engine()
result = run_sync(eng)
print('Sync result:', result)
regenerate_dashboard(eng)
print('Dashboard regenerated')
"
```

Expected: Should insert all existing vault notes (8 tool evaluations, index files, etc.)

- [ ] **Step 3: Verify API returns data**

```bash
cd /Users/anikdang/dev/GRID && python -c "
from db import get_engine
from sqlalchemy import text
eng = get_engine()
with eng.connect() as conn:
    count = conn.execute(text('SELECT COUNT(*) FROM obsidian_notes')).scalar()
    print(f'{count} notes synced')
    domains = conn.execute(text('SELECT domain, COUNT(*) FROM obsidian_notes GROUP BY domain')).fetchall()
    for d in domains:
        print(f'  {d[0]}: {d[1]}')
"
```

Expected: Notes across tools, alpha, intel, pipeline, grid, dashboard domains

- [ ] **Step 4: Run full test suite**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_obsidian_sync.py tests/test_obsidian_agent.py tests/test_vault_api.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit integration test results**

```bash
git add -A
git commit -m "feat(obsidian): complete Obsidian bridge — sync, agent, API, frontend"
```

---

## Summary

| Task | What | Depends On |
|------|------|------------|
| 1 | Database schema | — |
| 2 | Config settings | — |
| 3 | Sync engine (inbound) | 1, 2 |
| 4 | Sync engine (outbound) | 3 |
| 5 | Dashboard regeneration | 4 |
| 6 | API router | 3 |
| 7 | MCP tools | 3, 4 |
| 8 | Active agent | 3, 6 |
| 9 | Hermes integration | 3, 4, 5, 8 |
| 10 | Frontend view | 6 |
| 11 | Integration test | all |

Tasks 1+2 can run in parallel. Tasks 3-5 are sequential. Tasks 6, 7, 8 can run in parallel after 3. Task 9 needs 3-5 + 8. Task 10 needs 6. Task 11 is final validation.
