"""Cross-reference intelligence endpoints — lie detector for government statistics."""

from __future__ import annotations

import threading
import time
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence", "cross-reference"])

# ── TTL cache (10 min) with per-key lock ────────────────────────────────
# The prior version was not thundering-herd-safe: 3 concurrent first-hits
# on the same key would each run the slow path in parallel, each opening
# a DB connection, each running the same `raw_series LIKE '%...%'`
# full-table scan. On the live corpus that saturated the connection pool
# and took down the lever page, the NVDA chart, and everything else
# sharing the pool. Now a single thread computes while the others wait.
_cache: dict[str, tuple[float, Any]] = {}
_cache_locks: dict[str, threading.Lock] = {}
_cache_locks_lock = threading.Lock()
_CACHE_TTL = 600.0  # seconds


def _lock_for(key: str) -> threading.Lock:
    with _cache_locks_lock:
        lock = _cache_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _cache_locks[key] = lock
        return lock


def _cached(key: str, fn):
    """Return cached result if fresh, otherwise compute and cache.

    Thundering-herd safe: acquires a per-key lock so concurrent
    first-hits wait for the first computer instead of all piling into the
    DB in parallel.
    """
    now = time.time()
    if key in _cache and now - _cache[key][0] < _CACHE_TTL:
        return _cache[key][1]
    lock = _lock_for(key)
    with lock:
        # Re-check under lock — whoever got in first may have filled it.
        now = time.time()
        if key in _cache and now - _cache[key][0] < _CACHE_TTL:
            return _cache[key][1]
        result = fn()
        _cache[key] = (now, result)
        return result


@router.get("/cross-reference")
async def get_cross_reference(
    fast: bool = Query(
        True,
        description=(
            "Skip the LLM narrative generation. Default True — the narrative "
            "is the slow part (20s+ cold), the checks themselves are ~1s. "
            "Fetch the narrative separately from /cross-reference/narrative."
        ),
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run all cross-reference checks and return the LieDetectorReport.

    Compares government statistics against physical reality indicators
    across GDP, trade, inflation, central bank, and employment categories.
    Red flags indicate where official data diverges from ground truth.

    The narrative is gated behind ``fast=False`` because LLM generation
    dominates cold-call latency (22.6s observed in the 2026-05-16 audit
    vs ~1s for checks-only). Frontend should call this endpoint with the
    default ``fast=true`` for the data, then load ``/cross-reference/narrative``
    lazily for the prose layer. Mirrors the postmortem-lessons async
    pattern shipped earlier in PR for dashboard cold-load.
    """
    try:
        from intelligence.cross_reference import run_all_checks

        engine = get_db_engine()
        cache_key = "cross_ref_all_fast" if fast else "cross_ref_all"

        def _compute():
            report = run_all_checks(engine, skip_narrative=fast)
            return {
                "checks": [asdict(c) for c in report.checks],
                "red_flags": [asdict(c) for c in report.red_flags],
                "narrative": report.narrative,
                "summary": report.summary,
                "generated_at": report.generated_at,
                "narrative_pending": fast,  # signals to frontend to fetch separately
            }

        return _cached(cache_key, _compute)
    except Exception as exc:
        log.warning("Cross-reference engine failed: {e}", e=str(exc))
        return {
            "checks": [],
            "red_flags": [],
            "narrative": f"Cross-reference engine error: {exc}",
            "summary": {},
            "generated_at": None,
            "error": str(exc),
        }


@router.get("/cross-reference/narrative")
async def get_cross_reference_narrative(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return only the LLM-generated narrative.

    Split from the main ``/cross-reference`` endpoint so the data layer
    can load fast (~1s) and the prose layer can load lazily (~20s).
    The narrative is cached separately so a hit on this endpoint doesn't
    re-run the checks.
    """
    try:
        from intelligence.cross_reference import run_all_checks

        engine = get_db_engine()

        def _compute_narrative():
            report = run_all_checks(engine, skip_narrative=False)
            return {
                "narrative": report.narrative,
                "generated_at": report.generated_at,
            }

        return _cached("cross_ref_narrative", _compute_narrative)
    except Exception as exc:
        log.warning("Cross-reference narrative failed: {e}", e=str(exc))
        return {"narrative": "", "error": str(exc)}


@router.get("/cross-reference/category/{category}")
async def get_cross_reference_by_category(
    category: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run cross-reference checks for a specific category.

    Valid categories: gdp, trade, inflation, central_bank, employment.
    """
    try:
        from intelligence.cross_reference import (
            check_gdp_vs_physical,
            check_trade_bilateral,
            check_inflation_vs_inputs,
            check_central_bank_actions_vs_words,
            check_employment_reality,
        )

        engine = get_db_engine()

        from intelligence.cross_reference import (
            check_liquidity_reality,
            check_credit_housing,
            check_insider_divergence,
        )

        category_map = {
            "gdp": lambda: (
                check_gdp_vs_physical(engine, "US")
                + check_gdp_vs_physical(engine, "CN")
                + check_gdp_vs_physical(engine, "EU")
            ),
            "trade": lambda: check_trade_bilateral(engine),
            "inflation": lambda: check_inflation_vs_inputs(engine),
            "central_bank": lambda: check_central_bank_actions_vs_words(engine),
            "employment": lambda: check_employment_reality(engine),
            "liquidity": lambda: check_liquidity_reality(engine),
            "credit": lambda: check_credit_housing(engine),
            "insider": lambda: check_insider_divergence(engine),
        }

        check_fn = category_map.get(category.lower())
        if check_fn is None:
            return {
                "error": (
                    f"Unknown category '{category}'. "
                    f"Valid: {', '.join(category_map.keys())}"
                ),
                "checks": [],
            }

        def _compute_category():
            checks = check_fn()
            red_flags = [
                c for c in checks
                if c.assessment in ("major_divergence", "contradiction")
            ]
            return {
                "category": category,
                "checks": [asdict(c) for c in checks],
                "red_flags": [asdict(c) for c in red_flags],
                "total": len(checks),
                "red_flag_count": len(red_flags),
            }

        return _cached(f"cross_ref_{category.lower()}", _compute_category)
    except Exception as exc:
        log.warning(
            "Cross-reference category {c} failed: {e}", c=category, e=str(exc),
        )
        return {"category": category, "checks": [], "error": str(exc)}


@router.get("/cross-reference/ticker/{ticker}")
async def get_cross_reference_for_ticker(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return cross-reference checks relevant to a specific ticker.

    Maps tickers to the categories and country-specific checks that
    affect them. E.g., EEM maps to EM GDP vs physical + trade flows.
    """
    try:
        from intelligence.cross_reference import get_cross_ref_for_ticker

        engine = get_db_engine()
        return get_cross_ref_for_ticker(engine, ticker)
    except Exception as exc:
        log.warning("Ticker cross-ref {t} failed: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker, "mapped": False, "checks": [], "error": str(exc)}


@router.get("/cross-reference/history")
async def get_cross_reference_history(
    category: str | None = Query(None, description="Filter by category"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    assessment: str | None = Query(None, description="Filter by assessment level"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Fetch historical cross-reference checks for trend analysis.

    Shows how divergences have evolved over time, enabling detection
    of persistent vs transient inconsistencies.
    """
    try:
        from intelligence.cross_reference import get_historical_checks

        engine = get_db_engine()
        records = get_historical_checks(engine, category, days, assessment)
        return {
            "records": records,
            "count": len(records),
            "filters": {
                "category": category,
                "days": days,
                "assessment": assessment,
            },
        }
    except Exception as exc:
        log.warning("Cross-reference history failed: {e}", e=str(exc))
        return {"records": [], "count": 0, "error": str(exc)}
