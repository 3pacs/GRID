"""
Twitter/X Bookmark Intelligence Pipeline.

Ingests bookmarks from a local SQLite export, triages them through multiple
LLMs for cross-model comparison, and outputs to Obsidian vault for operator
review.

Flow:
    sync.py (Playwright daily cron) → bookmarks.db (SQLite)
        → this module → multi-LLM triage → Obsidian Inbox

Config (via .env / config.py):
    BOOKMARKS_DB_PATH         path to bookmarks.db
    BOOKMARKS_OBSIDIAN_PATH   path to Obsidian vault
    BOOKMARKS_SYNC_ENABLED    enable/disable the triage pipeline
    GROQ_API_KEY              Groq backend (free, fast)
    GEMINI_API_KEY            Google Gemini backend
    LLAMACPP_BASE_URL         llama.cpp server (already in GRID config)
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from config import log, settings

# ─── Defaults ────────────────────────────────────────────────────────────────

BOOKMARKS_DB = Path(
    os.environ.get(
        "BOOKMARKS_DB_PATH",
        os.path.expanduser("~/.ft-bookmarks/bookmarks.db"),
    )
)
OBSIDIAN_VAULT = Path(
    os.environ.get(
        "BOOKMARKS_OBSIDIAN_PATH",
        os.path.expanduser("~/Documents/Obsidian Vault"),
    )
)

TRIAGE_PROMPT = """Analyze this Twitter/X bookmark for an operator building an AI-powered financial intelligence platform (trading, compute, LLMs, crypto).

Tweet by @{author} ({date}):
"{text}"

Respond in EXACTLY this JSON format, no other text:
{{
  "category": "tools|workflows|alpha|intel|noise",
  "relevance": 1-10,
  "summary": "one line summary",
  "action": "specific action to take or null",
  "stale_risk": "low|medium|high",
  "tags": ["tag1", "tag2"]
}}

Categories:
- tools: specific software, repos, frameworks to evaluate
- workflows: how-to guides, build processes, strategies
- alpha: market intel, trading signals, financial data
- intel: trends, capabilities, cultural signals worth tracking
- noise: memes, entertainment, empty content, generic listicles"""


# ─── LLM Backends ───────────────────────────────────────────────────────────


def _parse_json_response(text: str) -> dict | None:
    """Extract JSON from LLM response, tolerating markdown fences."""
    text = text.strip()
    if "```" in text:
        for part in text.split("```"):
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                text = part
                break
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    return None


def _query_groq(prompt: str) -> tuple[dict | None, str]:
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        return None, "no_key"
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 500,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"
        content = resp.json()["choices"][0]["message"]["content"]
        return _parse_json_response(content), "ok"
    except Exception as e:
        return None, str(e)


def _query_gemini(prompt: str) -> tuple[dict | None, str]:
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        return None, "no_key"
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.3, "maxOutputTokens": 500},
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"
        content = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        return _parse_json_response(content), "ok"
    except Exception as e:
        return None, str(e)


def _query_llamacpp(prompt: str) -> tuple[dict | None, str]:
    base_url = getattr(settings, "LLAMACPP_BASE_URL", "http://localhost:8080")
    if not getattr(settings, "LLAMACPP_ENABLED", False):
        return None, "disabled"
    try:
        resp = requests.post(
            f"{base_url}/v1/chat/completions",
            headers={"Content-Type": "application/json"},
            json={
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 500,
            },
            timeout=int(getattr(settings, "LLAMACPP_TIMEOUT_SECONDS", 300)),
        )
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"
        content = resp.json()["choices"][0]["message"]["content"]
        return _parse_json_response(content), "ok"
    except requests.ConnectionError:
        return None, "not_running"
    except Exception as e:
        return None, str(e)


BACKENDS: list[tuple[str, Any]] = [
    ("groq", _query_groq),
    ("gemini", _query_gemini),
    ("llamacpp", _query_llamacpp),
]


# ─── Triage Engine ──────────────────────────────────────────────────────────


def triage_bookmark(bookmark: dict) -> dict[str, dict]:
    """Run a single bookmark through all available LLM backends."""
    prompt = TRIAGE_PROMPT.format(
        author=bookmark.get("author_username", "unknown"),
        date=(bookmark.get("created_at") or "unknown")[:10],
        text=(bookmark.get("text") or "")[:500],
    )
    results: dict[str, dict] = {}
    for name, fn in BACKENDS:
        result, status = fn(prompt)
        results[name] = {"result": result, "status": status}
        if status == "ok" and result:
            log.info(
                "  {name}: cat={cat} rel={rel} | {summary}",
                name=name,
                cat=result.get("category", "?"),
                rel=result.get("relevance", "?"),
                summary=(result.get("summary") or "")[:60],
            )
        else:
            log.debug("  {name}: {status}", name=name, status=status)
    return results


def compare_results(results: dict[str, dict]) -> dict:
    """Find disagreements between LLM assessments."""
    active = {
        k: v["result"]
        for k, v in results.items()
        if v["status"] == "ok" and v["result"]
    }
    if len(active) < 2:
        return {"consensus": True, "active_llms": len(active)}

    categories = {k: v.get("category") for k, v in active.items()}
    relevances = {k: v.get("relevance", 5) for k, v in active.items()}

    cat_consensus = len(set(categories.values())) == 1
    rel_values = [v for v in relevances.values() if isinstance(v, (int, float))]
    rel_spread = max(rel_values) - min(rel_values) if rel_values else 0

    return {
        "consensus": cat_consensus and rel_spread <= 2,
        "category_agreement": cat_consensus,
        "categories": categories,
        "relevance_spread": rel_spread,
        "relevances": relevances,
        "active_llms": len(active),
    }


# ─── Obsidian Output ────────────────────────────────────────────────────────


def _ensure_obsidian_dirs() -> None:
    for d in ["01-Pipeline", "02-Tools", "03-Alpha", "04-Intel", "05-GRID"]:
        (OBSIDIAN_VAULT / d).mkdir(parents=True, exist_ok=True)


def write_inbox_entry(
    bookmark: dict,
    llm_results: dict[str, dict],
    comparison: dict,
) -> None:
    """Append a triaged bookmark to the Obsidian Inbox."""
    _ensure_obsidian_dirs()
    inbox = OBSIDIAN_VAULT / "01-Pipeline" / "Inbox.md"

    active = {
        k: v["result"]
        for k, v in llm_results.items()
        if v["status"] == "ok" and v["result"]
    }

    summary = ""
    action = ""
    tags: list[str] = []
    for name in ("groq", "gemini", "llamacpp"):
        if name in active and active[name]:
            if not summary:
                summary = active[name].get("summary", "")
            if not action:
                action = active[name].get("action") or ""
            tags.extend(active[name].get("tags", []))
    tags = list(set(tags))

    consensus_icon = "✅" if comparison.get("consensus") else "⚠️"
    date = (bookmark.get("created_at") or "")[:10]
    author = bookmark.get("author_username", "unknown")
    text = (bookmark.get("text") or "")[:200].replace("\n", " ")

    # Extract tweet_url from raw_json if available
    tweet_url = ""
    raw = bookmark.get("raw_json", "")
    if raw:
        try:
            tweet_url = json.loads(raw).get("tweet_url", "")
        except (json.JSONDecodeError, TypeError):
            pass

    entry = f"""
---

### {consensus_icon} @{author} — {date}
> {text}

| LLM | Category | Relevance | Summary |
|-----|----------|-----------|---------|
"""
    for name in ("groq", "gemini", "llamacpp"):
        if name in active and active[name]:
            r = active[name]
            entry += (
                f"| {name} | {r.get('category', '?')} "
                f"| {r.get('relevance', '?')}/10 "
                f"| {r.get('summary', '')} |\n"
            )

    if action:
        entry += f"\n**Action:** {action}\n"
    if tags:
        entry += f"\n**Tags:** {' '.join('#' + t for t in tags)}\n"
    if tweet_url:
        entry += f"\n[Original Tweet]({tweet_url})\n"

    with open(inbox, "a") as f:
        f.write(entry)


def write_dashboard() -> None:
    """Regenerate the Obsidian dashboard with current stats."""
    _ensure_obsidian_dirs()

    if not BOOKMARKS_DB.exists():
        log.warning("Bookmarks DB not found at {path}", path=BOOKMARKS_DB)
        return

    conn = sqlite3.connect(str(BOOKMARKS_DB))
    conn.row_factory = sqlite3.Row
    total = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]

    # Check if llm_triage column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(bookmarks)").fetchall()]
    triaged = 0
    if "llm_triage" in cols:
        triaged = conn.execute(
            "SELECT COUNT(*) FROM bookmarks WHERE llm_triage IS NOT NULL"
        ).fetchone()[0]

    recent = conn.execute(
        "SELECT author_username, text, created_at, tags FROM bookmarks "
        "ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    conn.close()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    content = f"""# GRID Command Center
*Last updated: {now}*

## Pipeline Status
| Metric | Count |
|--------|-------|
| Total Bookmarks | {total} |
| Triaged | {triaged} |
| Awaiting Triage | {total - triaged} |

## Recent Bookmarks
"""
    for r in recent:
        tag_list = json.loads(r["tags"]) if r["tags"] else []
        tag = tag_list[0] if tag_list else "?"
        text = (r["text"] or "")[:80].replace("\n", " ")
        content += f"- [{tag}] @{r['author_username']} — {text}\n"

    content += """
## Quick Links
- [[01-Pipeline/Inbox|Inbox]] — New bookmarks awaiting review
- [[02-Tools/index|Tools]] — Vetted tools and frameworks
- [[03-Alpha/index|Alpha]] — Market intel and signals
- [[04-Intel/index|Intel]] — Trends and intelligence
- [[05-GRID/index|GRID]] — Platform notes

## Multi-LLM Pipeline
Each bookmark is triaged by 3 LLMs independently:
- **Groq** (Llama 3.3 70B) — free, fast
- **Gemini** (2.0 Flash) — Google's perspective
- **llama.cpp** (Nemotron-Super-49B on server) — local, no data leaves

Disagreements are flagged for manual review.
"""
    (OBSIDIAN_VAULT / "00-DASHBOARD.md").write_text(content)
    log.info("Dashboard updated at {path}", path=OBSIDIAN_VAULT / "00-DASHBOARD.md")


# ─── Main Pipeline ──────────────────────────────────────────────────────────


def _add_triage_column(conn: sqlite3.Connection) -> None:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(bookmarks)").fetchall()]
    if "llm_triage" not in cols:
        conn.execute("ALTER TABLE bookmarks ADD COLUMN llm_triage TEXT")
        conn.commit()


def run_triage(
    limit: int | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Triage untriaged bookmarks through the multi-LLM pipeline.

    Args:
        limit: Max bookmarks to process (None = all).
        force: Re-triage already-processed bookmarks.

    Returns:
        Dict with counts: total, triaged, disagreements.
    """
    if not BOOKMARKS_DB.exists():
        log.error("Bookmarks DB not found at {path}", path=BOOKMARKS_DB)
        return {"total": 0, "triaged": 0, "disagreements": 0}

    conn = sqlite3.connect(str(BOOKMARKS_DB))
    conn.row_factory = sqlite3.Row
    _add_triage_column(conn)

    if force:
        rows = conn.execute(
            "SELECT * FROM bookmarks ORDER BY created_at DESC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM bookmarks WHERE llm_triage IS NULL "
            "ORDER BY created_at DESC"
        ).fetchall()

    if limit:
        rows = rows[:limit]

    log.info("Triaging {n} bookmarks", n=len(rows))

    # Check available backends
    available = []
    if os.environ.get("GROQ_API_KEY"):
        available.append("groq")
    if os.environ.get("GEMINI_API_KEY"):
        available.append("gemini")
    if getattr(settings, "LLAMACPP_ENABLED", False):
        try:
            base = getattr(settings, "LLAMACPP_BASE_URL", "http://localhost:8080")
            r = requests.get(f"{base}/health", timeout=3)
            if r.status_code == 200:
                available.append("llamacpp")
        except Exception:
            pass

    if not available:
        log.error(
            "No LLM backends available. Set GROQ_API_KEY, GEMINI_API_KEY, "
            "or ensure llama.cpp is running."
        )
        return {"total": len(rows), "triaged": 0, "disagreements": 0}

    log.info("Active backends: {backends}", backends=", ".join(available))

    triaged_count = 0
    disagreements = 0

    for i, row in enumerate(rows):
        bookmark = dict(row)
        log.info(
            "[{i}/{n}] @{author} ({date})",
            i=i + 1,
            n=len(rows),
            author=bookmark.get("author_username", "?"),
            date=(bookmark.get("created_at") or "?")[:10],
        )

        results = triage_bookmark(bookmark)
        comparison = compare_results(results)

        if not comparison.get("consensus"):
            log.warning("  DISAGREEMENT: {cats}", cats=comparison.get("categories", {}))
            disagreements += 1

        triage_data = {
            "triaged_at": datetime.now(timezone.utc).isoformat(),
            "results": {
                k: v["result"]
                for k, v in results.items()
                if v["status"] == "ok"
            },
            "comparison": comparison,
        }
        conn.execute(
            "UPDATE bookmarks SET llm_triage = ? WHERE tweet_id = ?",
            (json.dumps(triage_data), bookmark["tweet_id"]),
        )
        conn.commit()

        write_inbox_entry(bookmark, results, comparison)
        triaged_count += 1

    write_dashboard()
    conn.close()

    summary = {
        "total": len(rows),
        "triaged": triaged_count,
        "disagreements": disagreements,
    }
    log.info("Triage complete: {s}", s=summary)
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID bookmark triage pipeline")
    parser.add_argument("--limit", type=int, help="Max bookmarks to triage")
    parser.add_argument("--force", action="store_true", help="Re-triage all")
    parser.add_argument("--dashboard", action="store_true", help="Only update dashboard")
    args = parser.parse_args()

    if args.dashboard:
        write_dashboard()
    else:
        run_triage(limit=args.limit, force=args.force)
