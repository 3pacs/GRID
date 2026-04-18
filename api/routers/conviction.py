"""Conviction API surface — FastAPI endpoints exposing the full decision stack
for the operator dashboard. Every endpoint: (1) acquires the DB engine via
get_db_engine dependency, (2) requires auth, (3) runs the corresponding
confidence-stack call in a try/except that surfaces errors as HTTP 500 with
structured detail, (4) serializes the response via to_dict() on the stack's
dataclasses.

Endpoints
---------

  GET  /api/v1/conviction/ticker/{ticker}
  GET  /api/v1/conviction/top?universe=SP500&k=20
  GET  /api/v1/conviction/pair/{long_ticker}/{short_ticker}
  GET  /api/v1/conviction/pair/candidates
  GET  /api/v1/conviction/health
  GET  /api/v1/conviction/narrative/{ticker}

Downstream modules
------------------

Every endpoint delegates to a single capstone function from the existing
intelligence stack — nothing is reimplemented here:

  * ``intelligence.decision_gateway.should_i_trade``       (DecisionResponse)
  * ``intelligence.universe_ranker.rank_universe``         (UniverseRankingReport)
  * ``intelligence.pair_conviction.generate_pair_ticket``  (PairTradeTicket | None)
  * ``intelligence.pair_conviction.scan_candidate_pairs``  (list[PairTradeTicket])
  * ``intelligence.signal_health_monitor.audit_all_series``(SignalHealthReport)
  * ``intelligence.llm_narrator.narrate_trade``            (NarrativeReport)

Registration
------------

This router is IMPORTABLE as ``from api.routers.conviction import router``.
Wiring it into the main FastAPI app (``app.include_router(conviction_router)``)
is the operator's responsibility and is intentionally NOT done here — the
task brief forbids edits to ``api/main.py``.

Auth + engine pattern
---------------------

The ``get_db_engine`` + ``require_auth`` dependency shape is mirrored from
``api/routers/oracle.py`` (imports at the top of that file). ``oracle.py``
calls ``get_db_engine()`` directly inside the handler; this module uses
``Depends(get_db_engine)`` as the task brief explicitly requires so test
suites can override the engine via ``app.dependency_overrides``.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine
from intelligence.decision_gateway import should_i_trade
from intelligence.llm_narrator import narrate_trade
from intelligence.pair_conviction import (
    DEFAULT_PAIR_CANDIDATES,
    generate_pair_ticket,
    scan_candidate_pairs,
)
from intelligence.signal_health_monitor import audit_all_series
from intelligence.universe_ranker import rank_universe


# ── Router ────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/v1/conviction", tags=["conviction"])


# ── Constants ─────────────────────────────────────────────────────────────

_DEFAULT_ACCOUNT_SIZE_USD: float = 100_000.0
_DEFAULT_HORIZON_DAYS: int = 7
_DEFAULT_INSTRUMENT: str = "equity"
_DEFAULT_TOP_K: int = 20
_MAX_TOP_K: int = 500
_ALLOWED_UNIVERSES: frozenset[str] = frozenset({"SP500", "NASDAQ100"})


# ── Helpers ───────────────────────────────────────────────────────────────


def _to_serializable(value: Any) -> Any:
    """Return a JSON-safe copy of ``value``.

    Most downstream modules already return JSON-safe dicts from their
    ``to_dict()`` methods, but a few fields may carry sets, frozensets,
    or ``datetime``/``date`` objects. This helper walks the structure
    defensively so the endpoint never raises a serialization error.

    Rules applied (recursively):

      * ``dict``            → dict with recursively-serialized values
      * ``list`` / ``tuple``→ list with recursively-serialized entries
      * ``set`` / ``frozenset``→ sorted list (sorted for deterministic
        test assertions when possible; falls back to list on TypeError)
      * ``datetime`` / ``date``→ ISO-formatted string
      * objects with ``to_dict()``→ result of ``to_dict()`` (recursed)
      * primitives          → returned as-is
      * anything else       → ``str(value)`` as a last-resort fallback
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_serializable(v) for v in value]
    if isinstance(value, (set, frozenset)):
        try:
            items = sorted(value)
        except TypeError:
            items = list(value)
        return [_to_serializable(v) for v in items]
    # Dataclass / object with a to_dict() method
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _to_serializable(to_dict())
        except Exception as exc:  # noqa: BLE001
            log.debug("conviction: to_dict serialization fell back: {e}", e=exc)
    # Last-resort: string form
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        return None


def _normalize_ticker(ticker: str) -> str:
    """Uppercase and strip a ticker before handing off to the stack.

    Empty / whitespace-only tickers raise HTTP 422 so the operator gets
    a clear validation error rather than a confusing downstream crash.
    """
    if not ticker or not ticker.strip():
        raise HTTPException(status_code=422, detail="ticker must be non-empty")
    return ticker.strip().upper()


def _serialize_response(payload: Any) -> dict[str, Any]:
    """Run a downstream stack response through ``to_dict()`` + serializer."""
    if payload is None:
        return {}
    to_dict = getattr(payload, "to_dict", None)
    if callable(to_dict):
        return _to_serializable(to_dict())
    return _to_serializable(payload)


def _error(status_code: int, stage: str, exc: Exception) -> HTTPException:
    """Build a structured HTTPException so the operator sees the failing stage."""
    return HTTPException(
        status_code=status_code,
        detail={
            "stage": stage,
            "error": str(exc),
            "error_type": type(exc).__name__,
        },
    )


# ── GET /ticker/{ticker} ─────────────────────────────────────────────────


@router.get("/ticker/{ticker}")
async def get_ticker_conviction(
    ticker: str,
    account_size_usd: float = Query(
        _DEFAULT_ACCOUNT_SIZE_USD,
        gt=0,
        description="Account size in USD for Kelly sizing.",
    ),
    current_price: float | None = Query(
        None,
        description="Current market price (required for trade ticket generation).",
    ),
    vol_30d: float | None = Query(
        None,
        description="30-day realized volatility (optional, improves Kelly sizing).",
    ),
    horizon_days: int = Query(
        _DEFAULT_HORIZON_DAYS,
        ge=1,
        le=365,
        description="Forecast horizon in days.",
    ),
    instrument: str = Query(
        _DEFAULT_INSTRUMENT,
        description="Instrument type: 'equity' | 'option' | 'future'.",
    ),
    engine: Engine = Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full single-ticker decision: ``should_i_trade`` + ``narrate_trade``.

    Returns every ``DecisionResponse`` field plus a ``narrative`` field
    from the template-path narrator (no LLM). This is the endpoint the
    PWA dashboard hits when an operator clicks on a ticker.
    """
    symbol = _normalize_ticker(ticker)
    try:
        decision = should_i_trade(
            engine,
            symbol,
            account_size_usd=account_size_usd,
            current_price=current_price,
            vol_30d=vol_30d,
            horizon_days=horizon_days,
            instrument=instrument,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("conviction /ticker/{t}: should_i_trade raised: {e}", t=symbol, e=exc)
        raise _error(500, "should_i_trade", exc) from exc

    try:
        narrative = narrate_trade(
            getattr(decision, "provenance_report", None),
            getattr(decision, "stress_report", None),
            llm_client=None,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("conviction /ticker/{t}: narrate_trade raised: {e}", t=symbol, e=exc)
        raise _error(500, "narrate_trade", exc) from exc

    payload = _serialize_response(decision)
    payload["narrative"] = _serialize_response(narrative)
    return payload


# ── GET /top ─────────────────────────────────────────────────────────────


@router.get("/top")
async def get_top_conviction(
    universe: str = Query(
        "SP500",
        description="Named universe: 'SP500' or 'NASDAQ100'.",
    ),
    k: int = Query(
        _DEFAULT_TOP_K,
        ge=1,
        le=_MAX_TOP_K,
        description="Top-K names to surface.",
    ),
    account_size_usd: float = Query(
        _DEFAULT_ACCOUNT_SIZE_USD,
        gt=0,
        description="Account size in USD for Kelly sizing.",
    ),
    parallel: bool = Query(
        True,
        description="Fan out decision_gateway calls via a thread pool.",
    ),
    engine: Engine = Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Rank an entire ticker universe by composite conviction score.

    Wraps ``intelligence.universe_ranker.rank_universe``. Returns a
    fully-serialized ``UniverseRankingReport.to_dict()`` payload.
    """
    universe_upper = (universe or "").strip().upper()
    if universe_upper not in _ALLOWED_UNIVERSES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"universe must be one of {sorted(_ALLOWED_UNIVERSES)}; "
                f"got '{universe}'"
            ),
        )

    try:
        report = rank_universe(
            engine,
            universe_upper,
            account_size_usd=account_size_usd,
            top_k=k,
            parallel=parallel,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("conviction /top: rank_universe raised: {e}", e=exc)
        raise _error(500, "rank_universe", exc) from exc

    return _serialize_response(report)


# ── GET /pair/candidates ─────────────────────────────────────────────────
#
# NOTE: This route is declared BEFORE ``/pair/{long}/{short}`` on purpose.
# FastAPI resolves routes in declaration order and the literal
# ``/pair/candidates`` must match before the parametric
# ``/pair/{long_ticker}/{short_ticker}`` ever gets a chance.


@router.get("/pair/candidates")
async def get_pair_candidates(
    account_size_usd: float = Query(
        _DEFAULT_ACCOUNT_SIZE_USD,
        gt=0,
        description="Account size in USD for per-leg Kelly sizing.",
    ),
    engine: Engine = Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Scan the curated ``DEFAULT_PAIR_CANDIDATES`` list and return every
    ticket that survives the decision gateway + spread-sharpness gates.

    Response envelope:

        {
          "tickets": [PairTradeTicket.to_dict(), ...],
          "count":   int,
          "candidates_scanned": int,
        }

    An empty list is NOT an error — the universe simply had no qualifying
    spreads at the moment of the call.
    """
    try:
        tickets = scan_candidate_pairs(
            engine,
            DEFAULT_PAIR_CANDIDATES,
            account_size_usd=account_size_usd,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("conviction /pair/candidates: scan raised: {e}", e=exc)
        raise _error(500, "scan_candidate_pairs", exc) from exc

    serialized = [_serialize_response(t) for t in (tickets or [])]
    return {
        "tickets": serialized,
        "count": len(serialized),
        "candidates_scanned": len(DEFAULT_PAIR_CANDIDATES),
    }


# ── GET /pair/{long_ticker}/{short_ticker} ───────────────────────────────


@router.get("/pair/{long_ticker}/{short_ticker}")
async def get_pair_ticket(
    long_ticker: str,
    short_ticker: str,
    account_size_usd: float = Query(
        _DEFAULT_ACCOUNT_SIZE_USD,
        gt=0,
        description="Account size in USD for per-leg Kelly sizing.",
    ),
    engine: Engine = Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a relative-value ticket for an explicit (long, short) pair.

    Returns a serialized ``PairTradeTicket.to_dict()`` payload when the
    decision gateway + spread sharpness gates all pass. Returns a
    structured rejection envelope — not an HTTP error — when any gate
    fails, so the client can distinguish "no trade" from "backend error".
    """
    long_sym = _normalize_ticker(long_ticker)
    short_sym = _normalize_ticker(short_ticker)

    try:
        ticket = generate_pair_ticket(
            engine,
            long_sym,
            short_sym,
            account_size_usd=account_size_usd,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "conviction /pair/{l}/{s}: generate_pair_ticket raised: {e}",
            l=long_sym, s=short_sym, e=exc,
        )
        raise _error(500, "generate_pair_ticket", exc) from exc

    if ticket is None:
        return {
            "ticket": None,
            "reason": (
                f"pair {long_sym}/{short_sym} rejected by decision gateway "
                "— insufficient leg conviction, fragile stress, correlated "
                "risk trap, or spread below sharpness floor."
            ),
            "long_ticker": long_sym,
            "short_ticker": short_sym,
        }

    return _serialize_response(ticket)


# ── GET /health ──────────────────────────────────────────────────────────


@router.get("/health")
async def get_signal_health(
    engine: Engine = Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run ``audit_all_series`` and return the signal health report.

    Surfaces the green / yellow / orange / red rollup across every
    ``series_id`` the puller fleet emits. The dashboard's 'data health'
    widget polls this endpoint.
    """
    try:
        report = audit_all_series(engine)
    except Exception as exc:  # noqa: BLE001
        log.debug("conviction /health: audit_all_series raised: {e}", e=exc)
        raise _error(500, "audit_all_series", exc) from exc

    return _serialize_response(report)


# ── GET /narrative/{ticker} ──────────────────────────────────────────────


@router.get("/narrative/{ticker}")
async def get_ticker_narrative(
    ticker: str,
    account_size_usd: float = Query(
        _DEFAULT_ACCOUNT_SIZE_USD,
        gt=0,
        description="Account size in USD (passed through to should_i_trade).",
    ),
    horizon_days: int = Query(
        _DEFAULT_HORIZON_DAYS,
        ge=1,
        le=365,
        description="Forecast horizon in days.",
    ),
    engine: Engine = Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Lighter-weight narrative-only endpoint.

    Runs ``should_i_trade`` to get a provenance report, then hands that
    (plus the stress report if present) to ``narrate_trade`` and returns
    ONLY the ``NarrativeReport.to_dict()`` payload. Skips the serialized
    ``DecisionResponse`` payload that ``/ticker/{ticker}`` returns.
    """
    symbol = _normalize_ticker(ticker)

    try:
        decision = should_i_trade(
            engine,
            symbol,
            account_size_usd=account_size_usd,
            horizon_days=horizon_days,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "conviction /narrative/{t}: should_i_trade raised: {e}",
            t=symbol, e=exc,
        )
        raise _error(500, "should_i_trade", exc) from exc

    try:
        narrative = narrate_trade(
            getattr(decision, "provenance_report", None),
            getattr(decision, "stress_report", None),
            llm_client=None,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "conviction /narrative/{t}: narrate_trade raised: {e}",
            t=symbol, e=exc,
        )
        raise _error(500, "narrate_trade", exc) from exc

    return _serialize_response(narrative)


__all__ = ["router"]
