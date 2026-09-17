"""Options scanner API endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/options", tags=["options"])

# The ONLY place this sentence is allowed to be produced, and only from a
# genuine ImportError. Audit C-M1/C-L4: it used to be returned verbatim on
# the DB-failure path too, and — because the module-level import below was
# wrong — on every single call, so "refresh" never refreshed and said so in
# words that were false.
ENGINE_MISSING_REASON = "Options recommender module is not installed"


# ── Recommendation helpers ───────────────────────────────────


def _persist_recommendations(engine, recommendations: list[dict]) -> int:
    """Insert new recommendations into the log table, skipping duplicates.

    Duplicates are identified by (ticker, strike, expiry).
    Returns the number of newly inserted rows.
    """
    from sqlalchemy import text

    if not recommendations:
        return 0

    inserted = 0
    with engine.begin() as conn:
        for rec in recommendations:
            # Check for existing duplicate
            exists = conn.execute(
                text(
                    "SELECT 1 FROM options_recommendations "
                    "WHERE ticker = :ticker AND strike = :strike AND expiry = :expiry "
                    "LIMIT 1"
                ),
                {
                    "ticker": rec.get("ticker"),
                    "strike": rec.get("strike"),
                    "expiry": rec.get("expiry"),
                },
            ).fetchone()
            if exists:
                continue

            conn.execute(
                text(
                    "INSERT INTO options_recommendations "
                    "(ticker, direction, strike, expiry, entry_price, target_price, "
                    "stop_loss, expected_return, kelly_fraction, confidence, thesis, "
                    "dealer_context, sanity_status, scanner_score, generated_at) "
                    "VALUES (:ticker, :direction, :strike, :expiry, :entry_price, "
                    ":target_price, :stop_loss, :expected_return, :kelly_fraction, "
                    ":confidence, :thesis, :dealer_context, :sanity_status, "
                    ":scanner_score, :generated_at)"
                ),
                {
                    "ticker": rec.get("ticker"),
                    "direction": rec.get("direction"),
                    "strike": rec.get("strike"),
                    "expiry": rec.get("expiry"),
                    "entry_price": rec.get("entry_price"),
                    "target_price": rec.get("target_price"),
                    "stop_loss": rec.get("stop_loss"),
                    "expected_return": rec.get("expected_return"),
                    "kelly_fraction": rec.get("kelly_fraction"),
                    "confidence": rec.get("confidence"),
                    "thesis": rec.get("thesis"),
                    "dealer_context": rec.get("dealer_context"),
                    "sanity_status": rec.get("sanity_status"),
                    # Keeps the empirical win-rate lookup's bucket key on
                    # every row this path writes, not just the recommender's.
                    "scanner_score": rec.get("scanner_score"),
                    "generated_at": rec.get("generated_at", datetime.now(timezone.utc).isoformat()),
                },
            )
            inserted += 1

    return inserted


def _format_recommendation_response(
    recommendations: list[dict],
    scan_summary: dict | None = None,
    generated_at: str | None = None,
) -> dict:
    """Build the standard response envelope for recommendations."""
    now = generated_at or datetime.now(timezone.utc).isoformat()
    summary = scan_summary or {
        "total_scanned": len(recommendations),
        "passed_sanity": len(recommendations),
        "rejected": 0,
    }
    return {
        "recommendations": recommendations,
        "generated_at": now,
        "scan_summary": summary,
    }


def _generate_recommendations(engine) -> dict:
    """Run the recommendation engine and return a response envelope.

    ``generate_recommendations`` is a METHOD of ``OptionsRecommender``, not a
    module-level function — the previous ``from trading.options_recommender
    import generate_recommendations`` raised ``ImportError`` on every call,
    so both endpoints below silently fell through to the persisted-rows path
    and reported it as "module is not installed" (audit C-M1).

    The recommender re-scans on every invocation; there is no cache to
    bypass, so there is no ``force_refresh`` flag to honour. ``fresh_scan``
    in the summary says whether this payload came from a live scan.
    """
    from trading.options_recommender import OptionsRecommender

    recommender = OptionsRecommender(db_engine=engine)
    recs = recommender.generate_recommendations(engine)
    return {
        "recommendations": [r.to_dict() for r in recs],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scan_summary": {
            "total_scanned": len(recs),
            "passed_sanity": len(recs),
            "rejected": 0,
            "source": "live_scan",
            "fresh_scan": True,
        },
    }


def _serialize_saved_recommendation(row: Any) -> dict[str, Any]:
    """Normalize a saved options recommendation row for API responses."""
    sanity_status = row[11]
    if isinstance(sanity_status, str):
        try:
            sanity_status = json.loads(sanity_status)
        except Exception:
            pass

    return {
        "ticker": row[0],
        "direction": row[1],
        "strike": float(row[2]) if row[2] is not None else None,
        "expiry": str(row[3]) if row[3] else None,
        "entry_price": float(row[4]) if row[4] is not None else None,
        "target_price": float(row[5]) if row[5] is not None else None,
        "stop_loss": float(row[6]) if row[6] is not None else None,
        "expected_return": float(row[7]) if row[7] is not None else None,
        "kelly_fraction": float(row[8]) if row[8] is not None else None,
        "confidence": float(row[9]) if row[9] is not None else None,
        "thesis": row[10],
        "sanity_status": sanity_status,
        "dealer_context": row[12],
        "generated_at": row[13].isoformat() if row[13] else None,
        "outcome": row[14],
    }


def _load_saved_recommendations(
    engine,
    *,
    ticker: str | None = None,
    limit: int = 50,
    engine_unavailable_reason: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    """Return persisted open recommendations when the live engine is unavailable.

    ``engine_unavailable_reason`` says why we are on this path at all. It is
    prefixed onto the reason we report, and the rest of the reason describes
    what the caller is actually looking at: how many persisted rows, and how
    old the newest one is. ``fresh_scan`` is ``False`` on every branch here —
    nothing was scanned.

    Audit C-L4: "engine unavailable" and "the persisted-row query itself
    failed" are different facts and used to be reported with the same
    sentence. They are now distinct, and neither claims a scan happened.
    """
    from sqlalchemy import text

    try:
        query = (
            "SELECT ticker, direction, strike, expiry, entry_price, target_price, "
            "stop_loss, expected_return, kelly_fraction, confidence, thesis, "
            "sanity_status, dealer_context, generated_at, outcome "
            "FROM options_recommendations "
            "WHERE (outcome IS NULL OR outcome = 'OPEN')"
        )
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            query += " AND ticker = :ticker"
            params["ticker"] = ticker.upper()
        query += " ORDER BY confidence DESC NULLS LAST, generated_at DESC LIMIT :limit"

        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        recommendations = [_serialize_saved_recommendation(row) for row in rows]

        # Age is reported from the NEWEST persisted row, and the envelope's
        # generated_at is that row's timestamp — not now(). A stale table
        # served at "now" is the defect this batch exists to remove.
        newest = _newest_generated_at(recommendations)
        age_seconds = _age_seconds(newest)
        generated_at = newest or datetime.now(timezone.utc).isoformat()

        if recommendations:
            age_phrase = (
                f"newest of {len(recommendations)} persisted row(s) is "
                f"{_humanize_age(age_seconds)} old (generated_at={newest})"
            )
        else:
            age_phrase = "no persisted open recommendations exist"

        return (
            recommendations,
            {
                "total_scanned": 0,
                "passed_sanity": len(recommendations),
                "rejected": 0,
                "source": "persisted",
                "fresh_scan": False,
                "persisted_row_count": len(recommendations),
                "persisted_newest_generated_at": newest,
                "persisted_age_seconds": age_seconds,
                "reason": f"{engine_unavailable_reason}; serving persisted rows — {age_phrase}",
            },
            generated_at,
        )
    except Exception as exc:
        log.warning("Saved recommendations fallback failed: {e}", e=str(exc))
        now = datetime.now(timezone.utc).isoformat()
        return (
            [],
            {
                "total_scanned": 0,
                "passed_sanity": 0,
                "rejected": 0,
                "source": "unavailable",
                "fresh_scan": False,
                "persisted_row_count": None,
                "persisted_newest_generated_at": None,
                "persisted_age_seconds": None,
                "reason": (
                    f"{engine_unavailable_reason}; and the persisted "
                    f"options_recommendations query also failed, so the age of "
                    f"the stored rows is unknown: {exc}"
                ),
            },
            now,
        )


def _newest_generated_at(recommendations: list[dict[str, Any]]) -> str | None:
    """Newest ``generated_at`` across serialized rows, or None."""
    stamps = [r.get("generated_at") for r in recommendations if r.get("generated_at")]
    return max(stamps) if stamps else None


def _age_seconds(generated_at: str | None) -> float | None:
    """Seconds between ``generated_at`` and now, or None when unknowable."""
    if not generated_at:
        return None
    try:
        parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - parsed).total_seconds(), 1)


def _humanize_age(age_seconds: float | None) -> str:
    """Render an age in the largest sensible unit, or say it is unknown."""
    if age_seconds is None:
        return "an unknown amount of time"
    if age_seconds < 120:
        return f"{age_seconds:.0f}s"
    if age_seconds < 7200:
        return f"{age_seconds / 60:.0f}m"
    if age_seconds < 172800:
        return f"{age_seconds / 3600:.1f}h"
    return f"{age_seconds / 86400:.1f}d"


# ── Recommendation endpoints ────────────────────────────────


@router.get("/recommendations")
async def get_recommendations(
    ticker: str | None = Query(None, description="Filter to a single ticker"),
    _token: str = Depends(require_auth),
) -> dict:
    """Generate options trade recommendations and persist new ones."""
    engine = get_db_engine()
    try:
        result = _generate_recommendations(engine)

        # result is expected to be a dict with at least 'recommendations' list
        recommendations = result.get("recommendations", [])
        scan_summary = result.get("scan_summary")
        generated_at = result.get("generated_at", datetime.now(timezone.utc).isoformat())

        # Persist new recommendations (skip duplicates)
        _persist_recommendations(engine, recommendations)

        # Filter by ticker if requested
        if ticker:
            recommendations = [
                r for r in recommendations if r.get("ticker", "").upper() == ticker.upper()
            ]

        return _format_recommendation_response(recommendations, scan_summary, generated_at)

    except ImportError as exc:
        log.warning("trading.options_recommender import failed: {e}", e=str(exc))
        recommendations, scan_summary, generated_at = _load_saved_recommendations(
            engine,
            ticker=ticker,
            engine_unavailable_reason=f"{ENGINE_MISSING_REASON} ({exc})",
        )
        return _format_recommendation_response(recommendations, scan_summary, generated_at)
    except Exception as exc:
        log.error("Recommendation generation failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Recommendation generation failed: {exc}",
        )


@router.post("/recommendations/refresh")
async def refresh_recommendations(
    _token: str = Depends(require_auth),
) -> dict:
    """Force a fresh recommendation scan, bypassing any cache."""
    engine = get_db_engine()
    try:
        result = _generate_recommendations(engine)

        recommendations = result.get("recommendations", [])
        scan_summary = result.get("scan_summary")
        generated_at = result.get("generated_at", datetime.now(timezone.utc).isoformat())

        # Persist new recommendations (skip duplicates)
        _persist_recommendations(engine, recommendations)

        return _format_recommendation_response(recommendations, scan_summary, generated_at)

    except ImportError as exc:
        log.warning("trading.options_recommender import failed: {e}", e=str(exc))
        recommendations, scan_summary, generated_at = _load_saved_recommendations(
            engine,
            engine_unavailable_reason=f"{ENGINE_MISSING_REASON} ({exc})",
        )
        return _format_recommendation_response(recommendations, scan_summary, generated_at)
    except Exception as exc:
        log.error("Recommendation refresh failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Recommendation refresh failed: {exc}",
        )


@router.get("/recommendations/history")
def get_recommendation_history(
    ticker: str | None = Query(None, description="Filter by ticker"),
    outcome: str | None = Query(None, description="Filter by outcome (WIN/LOSS/EXPIRED/OPEN)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return past recommendations with outcome data from the log table."""
    from sqlalchemy import text

    engine = get_db_engine()

    base_query = (
        "SELECT id, ticker, direction, strike, expiry, entry_price, target_price, "
        "stop_loss, expected_return, kelly_fraction, confidence, thesis, "
        "dealer_context, sanity_status, generated_at, outcome, actual_return, closed_at "
        "FROM options_recommendations WHERE 1=1"
    )
    count_query = "SELECT COUNT(*) FROM options_recommendations WHERE 1=1"
    params: dict[str, Any] = {"lim": limit, "off": offset}

    if ticker:
        base_query += " AND ticker = :ticker"
        count_query += " AND ticker = :ticker"
        params["ticker"] = ticker.upper()
    if outcome:
        base_query += " AND outcome = :outcome"
        count_query += " AND outcome = :outcome"
        params["outcome"] = outcome.upper()

    base_query += " ORDER BY generated_at DESC LIMIT :lim OFFSET :off"

    with engine.connect() as conn:
        total_row = conn.execute(text(count_query), params).fetchone()
        total = total_row[0] if total_row else 0

        rows = conn.execute(text(base_query), params).fetchall()

    history = [
        {
            "id": r[0],
            "ticker": r[1],
            "direction": r[2],
            "strike": float(r[3]) if r[3] is not None else None,
            "expiry": str(r[4]) if r[4] else None,
            "entry_price": float(r[5]) if r[5] is not None else None,
            "target_price": float(r[6]) if r[6] is not None else None,
            "stop_loss": float(r[7]) if r[7] is not None else None,
            "expected_return": float(r[8]) if r[8] is not None else None,
            "kelly_fraction": float(r[9]) if r[9] is not None else None,
            "confidence": float(r[10]) if r[10] is not None else None,
            "thesis": r[11],
            "dealer_context": r[12],
            "sanity_status": r[13],
            "generated_at": r[14].isoformat() if r[14] else None,
            "outcome": r[15],
            "actual_return": float(r[16]) if r[16] is not None else None,
            "closed_at": r[17].isoformat() if r[17] else None,
        }
        for r in rows
    ]

    return {
        "history": history,
        "count": len(history),
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/signals")
def get_options_signals(
    ticker: str | None = Query(None, description="Filter by ticker"),
    limit: int = Query(50, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict:
    """Return latest options daily signals."""
    from sqlalchemy import text

    engine = get_db_engine()
    with engine.connect() as conn:
        if ticker:
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_date, put_call_ratio, max_pain, "
                    "iv_skew, total_oi, total_volume, near_expiry, spot_price, "
                    "iv_atm, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker "
                    "ORDER BY signal_date DESC LIMIT :lim"
                ),
                {"ticker": ticker, "lim": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_date, put_call_ratio, max_pain, "
                    "iv_skew, total_oi, total_volume, near_expiry, spot_price, "
                    "iv_atm, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "ORDER BY signal_date DESC, ticker LIMIT :lim"
                ),
                {"lim": limit},
            ).fetchall()

    signals = [
        {
            "ticker": r[0],
            "signal_date": str(r[1]),
            "put_call_ratio": r[2],
            "max_pain": r[3],
            "iv_skew": r[4],
            "total_oi": r[5],
            "total_volume": r[6],
            "near_expiry": str(r[7]) if r[7] else None,
            "spot_price": r[8],
            "iv_atm": r[9],
            "term_structure_slope": r[10],
            "oi_concentration": r[11],
        }
        for r in rows
    ]

    return {"signals": signals, "count": len(signals)}


@router.get("/scan")
async def scan_mispricing(
    min_score: float = Query(5.0, ge=0, le=10, description="Minimum score"),
    _token: str = Depends(require_auth),
) -> dict:
    """Run the mispricing scanner and return flagged opportunities."""
    try:
        from discovery.options_scanner import OptionsScanner

        engine = get_db_engine()
        scanner = OptionsScanner(engine)
        opps = scanner.scan_all(min_score=min_score)

        results = [
            {
                "ticker": o.ticker,
                "scan_date": str(o.scan_date),
                "score": o.score,
                "estimated_payoff_multiple": o.estimated_payoff_multiple,
                "direction": o.direction,
                "thesis": o.thesis,
                "strikes": o.strikes,
                "expiry": o.expiry,
                "spot_price": o.spot_price,
                "iv_atm": o.iv_atm,
                "confidence": o.confidence,
                # Audit C-M20: was "is_100x", a bare boolean asserting a
                # market fact that was built entirely from tuning constants.
                # Renamed, nullable, and shipped with its inputs.
                "heuristic_payoff_flag": o.heuristic_payoff_flag,
                "payoff_inputs": o.payoff_inputs,
            }
            for o in opps
        ]

        return {
            "opportunities": results,
            "count": len(results),
            "heuristic_payoff_flag_count": sum(
                1 for o in opps if o.heuristic_payoff_flag
            ),
            "payoff_unmodelled_count": sum(
                1 for o in opps if o.heuristic_payoff_flag is None
            ),
        }
    except Exception as exc:
        log.warning("Options scan failed: {e}", e=str(exc))
        return {"opportunities": [], "count": 0, "error": str(exc)}


@router.get("/heuristic-payoff")
@router.get("/100x", deprecated=True)
async def get_heuristic_payoff_opportunities(
    _token: str = Depends(require_auth),
) -> dict:
    """Opportunities whose MODELLED payoff multiple clears the 100x threshold.

    The ``/100x`` path is retained as a deprecated alias because
    ``pwa/src/api.js`` ships in installed PWAs that update on their own
    schedule; ``/heuristic-payoff`` is the name to use. Both return the same
    payload, in which every flagged item carries the ``payoff_inputs`` the
    model used (audit C-M20) — the flag was previously a bare ``is_100x``
    boolean whose entire derivation was invisible to the reader.
    """
    try:
        from discovery.options_scanner import OptionsScanner

        engine = get_db_engine()
        scanner = OptionsScanner(engine)
        opps = scanner.get_100x_opportunities()

        results = [
            {
                "ticker": o.ticker,
                "scan_date": str(o.scan_date),
                "score": o.score,
                "estimated_payoff_multiple": o.estimated_payoff_multiple,
                "direction": o.direction,
                "thesis": o.thesis,
                "strikes": o.strikes,
                "expiry": o.expiry,
                "spot_price": o.spot_price,
                "iv_atm": o.iv_atm,
                "confidence": o.confidence,
                "heuristic_payoff_flag": o.heuristic_payoff_flag,
                "payoff_inputs": o.payoff_inputs,
            }
            for o in opps
        ]

        return {
            "opportunities": results,
            "count": len(results),
            "flag_basis": "modelled_heuristic",
        }
    except Exception as exc:
        log.warning("Heuristic payoff scan failed: {e}", e=str(exc))
        return {"opportunities": [], "count": 0, "error": str(exc)}


@router.get("/history")
def get_scan_history(
    ticker: str | None = Query(None),
    days: int = Query(30, ge=1, le=365),
    only_flagged: bool = Query(
        False,
        description="Only rows whose modelled payoff cleared the threshold",
    ),
    limit: int = Query(100, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict:
    """Return historical mispricing scan results."""
    from sqlalchemy import text

    engine = get_db_engine()

    # Build query safely — all conditions use parameterized placeholders
    base_query = (
        # is_100x / payoff_multiple are the historical COLUMN names; the
        # response renames them to say what they actually are.
        "SELECT ticker, scan_date, score, payoff_multiple, direction, "
        "thesis, confidence, is_100x, spot_price, iv_atm, payoff_inputs "
        "FROM options_mispricing_scans "
        "WHERE scan_date >= CURRENT_DATE - make_interval(days => :days)"
    )
    params: dict[str, Any] = {"days": days, "lim": limit}

    if ticker:
        base_query += " AND ticker = :ticker"
        params["ticker"] = ticker
    if only_flagged:
        base_query += " AND is_100x = TRUE"

    base_query += " ORDER BY score DESC LIMIT :lim"

    with engine.connect() as conn:
        rows = conn.execute(
            text(base_query), params,
        ).fetchall()

    results = [
        {
            "ticker": r[0],
            "scan_date": str(r[1]),
            "score": r[2],
            "payoff_multiple": r[3],
            "direction": r[4],
            "thesis": r[5],
            "confidence": r[6],
            "heuristic_payoff_flag": r[7],
            "spot_price": r[8],
            "iv_atm": r[9],
            "payoff_inputs": r[10],
        }
        for r in rows
    ]

    return {"history": results, "count": len(results)}
