"""Read-only bridge to the co-located Gamma Watch collector and receipt journal."""
from __future__ import annotations

import json
import math
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Response

from api.auth import require_auth, require_role

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


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    return number


def _read_upstream(path: str) -> dict:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    req = urllib.request.Request(UPSTREAM + path, headers={"Accept": "application/json"})
    with opener.open(req, timeout=3) as stream:
        body = stream.read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise ValueError("Snapshot too large")
    data = json.loads(body, parse_constant=_reject_constant, parse_float=_finite_float)
    if not isinstance(data, dict):
        raise ValueError("Invalid snapshot")
    return data


UNAVAILABLE_REASON = "Gamma Watch collector unavailable or invalid; no cached substitute."
ROUTE_NOT_SERVED_REASON = "route not served by collector"


def _parse_served_at(value) -> datetime:
    """Collector clock as an aware datetime.

    The legacy ``/api/state`` sends an ISO-8601 string (``Z`` or an offset);
    the ``gex-screen/2`` frame sends epoch seconds as a number. Anything else
    (missing, bool, non-finite, naive, unparseable) is invalid and the bridge
    fails closed rather than guessing.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("served_at missing or not a clock")
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise ValueError("served_at not finite")
        return datetime.fromtimestamp(value, tz=timezone.utc)
    source = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if source.tzinfo is None:
        raise ValueError("served_at has no UTC offset")
    return source


def _bridge(path: str, response: Response) -> dict:
    response.headers["Cache-Control"] = "no-store"
    now = datetime.now(timezone.utc)
    reason = UNAVAILABLE_REASON
    try:
        data = _read_upstream(path)
        if path == "/api/state":
            source = _parse_served_at(data.get("served_at"))
            age = (now - source).total_seconds()
            if not -5 <= age <= 20:
                raise ValueError("Collector response stale")
        return {"status": "available", "bridge_received_at": now.isoformat(), "data": data,
                "note": "Transport available does not mean live inputs. Preserve each source timestamp, error, coverage and modeling assumption. No signal promotion."}
    except urllib.error.HTTPError as exc:
        # The v4 collector (gex-screen/2) does not serve /api/journal or
        # /api/contracts; say so instead of reporting the collector as down.
        if exc.code == 404:
            reason = ROUTE_NOT_SERVED_REASON
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError):
        pass
    response.status_code = 503
    return {"status": "unavailable", "bridge_received_at": now.isoformat(), "data": None,
            "reason": reason}


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


# These projections can contain private Schwab-derived values. They must never
# inherit the ordinary contributor-access /state bridge's audience. Importing
# this code neither enables the private bridge nor changes any existing route.
PRIVATE_VIEWS = {
    "levels": None,  # The existing level book has no schema tag.
    "context": "gex-context/1",
    "confirmations": "gex-confirmations/1",
    "scoreboard": "gex-scoreboard/1",
    "panel": "gex-panel/1",
    "trail": "gex-trail/1",
    "heatmap": "gex-heatmap/1",
    "heatmap_live": "gex-heatmap/1",
    "records": "gex-records/1",
}
PrivateView = Literal[
    "levels", "context", "confirmations", "scoreboard", "panel", "trail",
    "heatmap", "heatmap_live", "records",
]


def _private_views_enabled() -> bool:
    from config import settings

    # Settings assignment through the admin config API is not type-validated.
    # A raw string such as "false" must never enable private data transport.
    return settings.GAMMA_WATCH_PRIVATE_VIEWS_ENABLED is True


@router.get("/private/{view}", dependencies=[Depends(require_role("admin"))])
def get_private_view(view: PrivateView, response: Response) -> dict:
    """Read an existing GEX projection without reinterpreting or promoting it.

    This is transport parity, not a new calculation, provider fetch, storage
    writer or freshness decision. All clocks, provenance, unavailable values,
    volume containment and historical labels remain inside the untouched data.
    The upstream HTTP handler only reads the existing projection. Private state,
    raw contracts, scenarios and arbitrary upstream paths are not routable.
    """
    response.headers["Cache-Control"] = "no-store, private"
    response.headers["X-GRID-Audience"] = "private"
    now = datetime.now(timezone.utc).isoformat()
    envelope = {"schema": "grid-gex-private-bridge/1", "audience": "private",
                "redistribution": "prohibited", "view": view,
                "bridge_evaluated_at": now, "bridge_received_at": None}
    reason = "Private GEX views are disabled (GAMMA_WATCH_PRIVATE_VIEWS_ENABLED=false)."
    if _private_views_enabled():
        reason = UNAVAILABLE_REASON
        try:
            # The enum is also checked here so direct Python calls cannot add an
            # upstream path. No caller query parameter is forwarded.
            expected = PRIVATE_VIEWS[view]
            data = _read_upstream("/api/" + view)
            if expected is not None and data.get("schema") != expected:
                raise ValueError("Unexpected projection schema")
            statuses = ("observed", "unavailable") if view == "confirmations" else ("ok", "degraded", "unavailable")
            if data.get("status") not in statuses:
                raise ValueError("Projection status missing or invalid")
            # Receipt is measured after the read. Never replace an upstream
            # source clock or claim that receipt makes old evidence current.
            envelope["bridge_received_at"] = datetime.now(timezone.utc).isoformat()
            return {**envelope, "status": data["status"],
                    "transport_status": "available", "data": data,
                    "note": "Existing GEX projection, unchanged. Transport receipt does not validate inputs, freshness, signals or trading edge."}
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                reason = ROUTE_NOT_SERVED_REASON
        except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError):
            pass
    response.status_code = 503
    return {**envelope, "status": "unavailable", "transport_status": "unavailable",
            "data": None, "reason": reason}
