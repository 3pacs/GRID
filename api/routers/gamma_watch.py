"""Read-only bridge to the co-located Gamma Watch collector and receipt journal."""
from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Response

from api.auth import require_auth

router = APIRouter(prefix="/api/v1/gamma-watch", tags=["gamma-watch"],
                   dependencies=[Depends(require_auth)])
UPSTREAM = "http://127.0.0.1:8769"
MAX_BYTES = 4 * 1024 * 1024
SYMBOLS = "SPY QQQ IWM XLK XLF XLE XLV XLI XLY XLP XLU XLB XLRE XLC SMH".split()
JOURNAL = Path(os.environ.get("GAMMA_WATCH_JOURNAL", "/data/agent-home/anikdang/gex-watch/observations.sqlite3"))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _reject_constant(value):
    raise ValueError("Non-finite JSON number")


def _read_upstream(path: str) -> dict:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    req = urllib.request.Request(UPSTREAM + path, headers={"Accept": "application/json"})
    with opener.open(req, timeout=3) as stream:
        body = stream.read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise ValueError("Snapshot too large")
    data = json.loads(body, parse_constant=_reject_constant)
    if not isinstance(data, dict):
        raise ValueError("Invalid snapshot")
    return data


def _bridge(path: str, response: Response) -> dict:
    response.headers["Cache-Control"] = "no-store"
    now = datetime.now(timezone.utc)
    try:
        data = _read_upstream(path)
        if path == "/api/state":
            source = datetime.fromisoformat(data["served_at"].replace("Z", "+00:00"))
            age = (now - source).total_seconds()
            if not -5 <= age <= 20:
                raise ValueError("Collector response stale")
        return {"status": "available", "bridge_received_at": now.isoformat(), "data": data,
                "note": "Transport available does not mean live inputs. Preserve each source timestamp, error, coverage and modeling assumption. No signal promotion."}
    except (OSError, ValueError, KeyError, TypeError):
        response.status_code = 503
        return {"status": "unavailable", "bridge_received_at": now.isoformat(), "data": None,
                "reason": "Gamma Watch collector unavailable or invalid; no cached substitute."}


@router.get("/state")
def get_state(response: Response):
    return _bridge("/api/state", response)


@router.get("/journal")
def get_journal(response: Response, since: float = Query(0, ge=0, allow_inf_nan=False)):
    return _bridge("/api/journal?" + urlencode({"since": since}), response)


@router.get("/contracts")
def get_contracts(response: Response, symbol: str = Query("SPY", pattern="^(" + "|".join(SYMBOLS) + ")$")):
    # Listing only. The upstream scenario route writes a record and is not proxied.
    return _bridge("/api/contracts?" + urlencode({"symbol": symbol}), response)


@router.get("/archive")
def get_archive(response: Response, stream: Literal["artifacts", "events", "samples"] = "artifacts",
                after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)):
    """Page the existing receipt journal without copying, changing or backdating it."""
    response.headers["Cache-Control"] = "no-store"
    columns = {"artifacts": "rowid,kind,version,received,body", "events": "id,received,source_time,kind,body",
               "samples": "rowid,received,source_time,price,source"}
    try:
        # mode=ro prevents silently creating an empty database when the collector is absent.
        db = sqlite3.connect(JOURNAL.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
        try:
            db.execute("PRAGMA query_only=ON")
            db.row_factory = sqlite3.Row
            rows = db.execute(f"SELECT {columns[stream]} FROM {stream} WHERE rowid>? ORDER BY rowid LIMIT ?", (after, limit))
            records = []
            size = 0
            for row in rows:
                item = dict(row)
                size += len(json.dumps(item).encode())
                if size > MAX_BYTES:
                    if not records:
                        raise ValueError("Archive record too large")
                    break
                if "body" in item:
                    item["body"] = json.loads(item["body"], parse_constant=_reject_constant)
                records.append(item)
        finally:
            db.close()
        cursor = records[-1].get("rowid", records[-1].get("id")) if records else after
        return {"status": "available", "stream": stream, "records": records, "next_after": cursor,
                "limit": limit, "note": "Receipt journal since collection began; no pre-start coverage. Cursor is scoped to this stream. Structural, sector, scenario and level artifacts retain source semantics."}
    except (OSError, sqlite3.Error, ValueError):
        response.status_code = 503
        return {"status": "unavailable", "records": None, "reason": "Gamma Watch journal unavailable or invalid."}
