"""Private, admin-only Schwab quote endpoint (newly wired 2026-10-31).

Authorization is the existing JWT role gate (``api.auth.require_role("admin")``):
no token -> 401, non-admin -> 403. On top of that the router is **off by default**
(``settings.SCHWAB_PRIVATE_QUOTES_ENABLED``); while off it answers 503 without touching
the reader, credentials or the network, so deploying this code changes nothing until an
operator flips the flag in a reviewed change.

When enabled and hit by an admin it builds the reviewed reader
(``ingestion.schwab_market.SchwabMarketClient``), reads through the quote path
(``read_private_quotes``) and materializes the result in the process-local
:class:`PrivateQuoteStore`. The response carries the classified snapshot and the
store view, both availability-contract payloads with provenance, provider quote time
(as-of), receipt time (vintage), identity (requested and provider symbol) and an explicit
status; ``live`` is true only for ``fresh`` rows. Responses are ``Cache-Control:
no-store, private``: Schwab data may not be redistributed, and this router is the only
GRID surface that serves it. No persistence, no scheduling, no public projection.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from api.auth import require_role
from config import log, settings
from ingestion.schwab_private_store import SCHEMA as VIEW_SCHEMA
from ingestion.schwab_private_store import PrivateQuoteStore, ProvenanceError
from ingestion.schwab_quote_path import (
    DEFAULT_MAX_QUOTE_AGE_S,
    QuoteSource,
    read_private_quotes,
)

router = APIRouter(
    prefix="/api/v1/schwab/private",
    tags=["schwab-private"],
    dependencies=[Depends(require_role("admin"))],
)

MAX_SYMBOLS_PER_READ = 10
_SYMBOL = re.compile(r"^[A-Z0-9.$^/-]{1,12}$")
_STORE = PrivateQuoteStore()
NO_STORE = "no-store, private"


def _enabled() -> bool:
    return bool(getattr(settings, "SCHWAB_PRIVATE_QUOTES_ENABLED", False))


def _quote_source() -> QuoteSource:
    """Build the reviewed reader. Only reached when the flag is on and the caller is admin.

    Credential and token-file handling is entirely the reader's (PR #931); nothing is
    read or logged here. Tests replace this function.
    """
    from ingestion.schwab_market import (
        SchwabConfigError,
        SchwabMarketClient,
        is_configured,
    )

    if not is_configured():
        raise HTTPException(status_code=503, detail="Schwab reader is not configured on this host")
    try:
        return SchwabMarketClient()
    except SchwabConfigError as exc:
        raise HTTPException(status_code=503, detail="Schwab reader is not configured on this host") from exc


def _symbols(raw: str) -> list[str]:
    out: list[str] = []
    for part in raw.split(","):
        sym = part.strip().upper()
        if not sym:
            continue
        if not _SYMBOL.match(sym):
            raise HTTPException(status_code=422, detail="symbols must be comma-separated tickers")
        if sym not in out:
            out.append(sym)
    if not out:
        raise HTTPException(status_code=422, detail="at least one symbol is required")
    if len(out) > MAX_SYMBOLS_PER_READ:
        raise HTTPException(status_code=422, detail=f"at most {MAX_SYMBOLS_PER_READ} symbols per read")
    return out


PRIVATE_HEADERS = {"Cache-Control": NO_STORE, "X-GRID-Audience": "private"}


def _private(response: Response) -> None:
    response.headers.update(PRIVATE_HEADERS)


def _private_error(status_code: int, detail: str) -> JSONResponse:
    """Error responses keep the private headers (raising HTTPException would drop them)."""
    return JSONResponse(status_code=status_code, content={"detail": detail}, headers=PRIVATE_HEADERS)


@router.get("/status")
def private_status(response: Response) -> dict[str, Any]:
    """Flag state plus the status-only projection of the store (no measured values)."""
    _private(response)
    now = datetime.now(timezone.utc)
    rows = {}
    for sym in _STORE.symbols():
        row = _STORE.get(sym, now=now)
        if row is not None:
            rows[sym] = {"quote_status": row.status, "live": row.live, "vintage": row.vintage.isoformat()}
    return {
        "enabled": _enabled(),
        "schema": VIEW_SCHEMA,
        "audience": "private",
        "redistribution": "prohibited",
        "evaluated_at": now.isoformat(),
        "stale_after_seconds": DEFAULT_MAX_QUOTE_AGE_S,
        "rows": rows,
    }


@router.get("/quotes")
def private_quotes(
    response: Response,
    symbols: str = Query("SPY", min_length=1, max_length=160, pattern=r"^[A-Za-z0-9.$^/, -]+$"),
) -> Any:
    """Read and classify quotes through the reviewed reader; materialize; return the view."""
    _private(response)
    wanted = _symbols(symbols)
    if not _enabled():
        return _private_error(503, "Schwab private quotes are disabled (SCHWAB_PRIVATE_QUOTES_ENABLED=false)")
    try:
        source = _quote_source()
    except HTTPException as exc:
        return _private_error(exc.status_code, str(exc.detail))
    now = datetime.now(timezone.utc)
    snapshot = read_private_quotes(source, wanted, now=now)
    try:
        stored = _STORE.put(snapshot)
    except ProvenanceError as exc:
        log.error("Schwab private snapshot failed materialization: {e}", e=str(exc))
        return _private_error(500, "snapshot lost provenance")
    return {
        "snapshot": snapshot.to_private_dict(),
        "view": _STORE.view(now=now, symbols=wanted),
        "stored_rows": stored,
    }
