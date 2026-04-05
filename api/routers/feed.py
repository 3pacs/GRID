"""
GRID Signal Feed — running list of anomalies, discoveries, and interesting signals.

Serves both JSON API and RSS/Atom feed for external consumption.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from config import settings
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/feed", tags=["feed"])


# ── JSON Endpoints ─────────────────────────────────────────────


@router.get("/signals")
async def get_signal_feed(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    signal_type: str | None = None,
    severity: str | None = None,
    ticker: str | None = None,
    _auth=Depends(require_auth),
) -> dict[str, Any]:
    """Get the signal feed — running list of anomalies and discoveries."""
    engine = get_db_engine()

    where_clauses = ["1=1"]
    params: dict[str, Any] = {"lim": limit, "off": offset}

    if signal_type:
        where_clauses.append("signal_type = :stype")
        params["stype"] = signal_type
    if severity:
        where_clauses.append("severity = :sev")
        params["sev"] = severity
    if ticker:
        where_clauses.append("ticker = :ticker")
        params["ticker"] = ticker.upper()

    # SAFETY: where_sql is built from static strings only; user values are bind params
    where_sql = " AND ".join(where_clauses)

    with engine.connect() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM signal_feed WHERE {where_sql}"),
            params,
        ).scalar()

        rows = conn.execute(
            text(
                f"SELECT id, created_at, signal_type, severity, title, body, "
                f"ticker, family, value, z_score, metadata "
                f"FROM signal_feed WHERE {where_sql} "
                f"ORDER BY created_at DESC LIMIT :lim OFFSET :off"
            ),
            params,
        ).fetchall()

    items = [
        {
            "id": r[0],
            "created_at": r[1].isoformat() if r[1] else None,
            "signal_type": r[2],
            "severity": r[3],
            "title": r[4],
            "body": r[5],
            "ticker": r[6],
            "family": r[7],
            "value": r[8],
            "z_score": r[9],
            "metadata": r[10],
        }
        for r in rows
    ]

    return {"total": count, "items": items}


@router.get("/signals/latest")
async def get_latest_signals(
    hours: int = Query(24, ge=1, le=168),
    _auth=Depends(require_auth),
) -> dict[str, Any]:
    """Get signals from the last N hours."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, created_at, signal_type, severity, title, body,
                       ticker, family, value, z_score, metadata
                FROM signal_feed
                WHERE created_at >= NOW() - MAKE_INTERVAL(hours => :hours)
                ORDER BY created_at DESC
            """),
            {"hours": hours},
        ).fetchall()

    items = [
        {
            "id": r[0],
            "created_at": r[1].isoformat() if r[1] else None,
            "signal_type": r[2],
            "severity": r[3],
            "title": r[4],
            "body": r[5],
            "ticker": r[6],
            "family": r[7],
            "value": r[8],
            "z_score": r[9],
            "metadata": r[10],
        }
        for r in rows
    ]

    by_severity = {}
    for item in items:
        sev = item["severity"]
        by_severity[sev] = by_severity.get(sev, 0) + 1

    return {"hours": hours, "total": len(items), "by_severity": by_severity, "items": items}


# ── RSS Feed ───────────────────────────────────────────────────


@router.get("/rss", response_class=Response)
async def get_rss_feed(
    limit: int = Query(50, ge=1, le=200),
) -> Response:
    """RSS 2.0 feed of GRID signals — no auth required for feed readers."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, created_at, signal_type, severity, title, body,
                       ticker, family, value, z_score
                FROM signal_feed
                ORDER BY created_at DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()

    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    items_xml = []
    for r in rows:
        pub_date = r[1].strftime("%a, %d %b %Y %H:%M:%S +0000") if r[1] else now
        severity_tag = f"[{r[3].upper()}]" if r[3] else ""
        ticker_tag = f"[{r[6]}]" if r[6] else ""
        title = _escape_xml(f"{severity_tag} {ticker_tag} {r[4]}".strip())
        body = _escape_xml(r[5] or "")
        category = _escape_xml(r[2] or "signal")

        items_xml.append(f"""    <item>
      <title>{title}</title>
      <description>{body}</description>
      <category>{category}</category>
      <pubDate>{pub_date}</pubDate>
      <guid isPermaLink="false">grid-signal-{r[0]}</guid>
    </item>""")

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>GRID Intelligence Feed</title>
    <link>https://grid.stepdad.finance</link>
    <description>Real-time anomalies, signals, and market intelligence from GRID</description>
    <language>en-us</language>
    <lastBuildDate>{now}</lastBuildDate>
    <atom:link href="https://grid.stepdad.finance/api/v1/feed/rss" rel="self" type="application/rss+xml"/>
{chr(10).join(items_xml)}
  </channel>
</rss>"""

    return Response(content=rss, media_type="application/rss+xml")


# ── Atom Feed ──────────────────────────────────────────────────


@router.get("/atom", response_class=Response)
async def get_atom_feed(
    limit: int = Query(50, ge=1, le=200),
) -> Response:
    """Atom feed of GRID signals — no auth required for feed readers."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, created_at, signal_type, severity, title, body,
                       ticker, family, value, z_score
                FROM signal_feed
                ORDER BY created_at DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    entries_xml = []
    for r in rows:
        updated = r[1].strftime("%Y-%m-%dT%H:%M:%SZ") if r[1] else now
        severity_tag = f"[{r[3].upper()}]" if r[3] else ""
        ticker_tag = f"[{r[6]}]" if r[6] else ""
        title = _escape_xml(f"{severity_tag} {ticker_tag} {r[4]}".strip())
        body = _escape_xml(r[5] or "")

        entries_xml.append(f"""  <entry>
    <title>{title}</title>
    <id>urn:grid:signal:{r[0]}</id>
    <updated>{updated}</updated>
    <summary>{body}</summary>
    <category term="{_escape_xml(r[2] or 'signal')}"/>
  </entry>""")

    atom = f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>GRID Intelligence Feed</title>
  <link href="https://grid.stepdad.finance/api/v1/feed/atom" rel="self"/>
  <link href="https://grid.stepdad.finance"/>
  <id>urn:grid:feed</id>
  <updated>{now}</updated>
{chr(10).join(entries_xml)}
</feed>"""

    return Response(content=atom, media_type="application/atom+xml")


def _escape_xml(s: str) -> str:
    """Escape XML special characters."""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )
