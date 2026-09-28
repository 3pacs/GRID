"""Async post-mortem lessons endpoint with 6h DB cache.

Cold path: heavy LLM synthesis (5-9s on grid-svr). Dashboard's
`/api/v1/intelligence/dashboard` cold load (task #62, branch
`perf/dashboard-cold-load-2026-05-16`) dropped this from its hot path
to hit <10s. Frontend now fetches lessons asynchronously from this
endpoint after the dashboard renders.

Cache: single-row `postmortem_lessons_cached` (id=1) refreshed every
6 hours unless `?refresh=1`. Forced refresh re-runs the local LLM
(`generate_lessons_learned`) and writes through.

Task #63, branch `perf/postmortem-lessons-async-2026-05-16`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1", tags=["intelligence"])


_CACHE_TTL = timedelta(hours=6)
_CACHE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS postmortem_lessons_cached (
    id           INTEGER PRIMARY KEY DEFAULT 1,
    lessons      JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    n            INTEGER NOT NULL DEFAULT 5,
    days         INTEGER NOT NULL DEFAULT 30,
    CONSTRAINT postmortem_lessons_cached_singleton CHECK (id = 1)
);
"""


def _ensure_cache_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_CACHE_TABLE_DDL))


def _read_cache(engine) -> dict[str, Any] | None:
    _ensure_cache_table(engine)
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT lessons, generated_at, n, days FROM postmortem_lessons_cached WHERE id = 1"
        )).fetchone()
    if not row:
        return None
    return {
        "lessons": row[0],
        "generated_at": row[1].isoformat() if row[1] else None,
        "n": row[2],
        "days": row[3],
        "_ts": row[1],
    }


def _write_cache(engine, lessons_payload: Any, n: int, days: int) -> str:
    import json as _json
    payload = lessons_payload
    if not isinstance(payload, (dict, list)):
        payload = {"text": str(payload)}
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO postmortem_lessons_cached (id, lessons, generated_at, n, days)
            VALUES (1, CAST(:lessons AS JSONB), :ts, :n, :days)
            ON CONFLICT (id) DO UPDATE
                SET lessons = EXCLUDED.lessons,
                    generated_at = EXCLUDED.generated_at,
                    n = EXCLUDED.n,
                    days = EXCLUDED.days
        """), {
            "lessons": _json.dumps(payload),
            "ts": now,
            "n": int(n),
            "days": int(days),
        })
    return now.isoformat()


def _is_fresh(ts: datetime | None) -> bool:
    if ts is None:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts) < _CACHE_TTL


def _date_range_label(iso_timestamps: list[str | None]) -> str | None:
    """Format the min..max date of ``iso_timestamps`` as "YYYY-MM-DD to YYYY-MM-DD".

    Returns a single date (no "to") when every timestamp falls on the same
    day, or None if nothing parseable was given.
    """
    parsed: list[datetime] = []
    for ts in iso_timestamps:
        if not ts:
            continue
        try:
            parsed.append(datetime.fromisoformat(str(ts).replace("Z", "+00:00")))
        except (ValueError, TypeError):
            continue
    if not parsed:
        return None
    lo, hi = min(parsed).date(), max(parsed).date()
    if lo == hi:
        return lo.isoformat()
    return f"{lo.isoformat()} to {hi.isoformat()}"


def _cached_label(lessons: Any) -> str | None:
    """Best-effort label for a cached row.

    Rows written before the Wave 3 #6 fix have no ``label`` key -- fall back
    to a count-only label (no date range) rather than showing nothing.
    """
    if isinstance(lessons, dict):
        if lessons.get("label"):
            return lessons["label"]
        count = lessons.get("count")
        if isinstance(count, int) and count > 0:
            return _lessons_label(count, None)
    return None


def _lessons_label(count: int, date_range: str | None) -> str:
    """The honest, non-committal label the report asked for: "LLM summary of
    N postmortems dated X-Y" (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md #6)."""
    if date_range:
        return f"LLM summary of {count} postmortems dated {date_range}"
    return f"LLM summary of {count} postmortems"


def _generate(engine, n: int, days: int) -> Any:
    """Run the LLM lessons synthesis from the most recent n postmortems.

    Never calls the LLM when the source window is empty (Wave 3 #6) — a
    "summary" synthesized from zero rows would read as a fabricated generic
    answer, not an honest one. Returns ``count: 0`` and no ``label`` instead.
    """
    from intelligence.postmortem import (
        load_postmortems,
        generate_lessons_learned,
        PostMortem,
    )

    records = load_postmortems(engine, days=days)
    if not records:
        return {
            "text": f"No post-mortems found in the last {days} days.",
            "count": 0,
            "label": None,
        }

    records = records[: int(n)] if n and n > 0 else records

    pms: list[PostMortem] = []
    for r in records:
        full = r.get("full_analysis") or {}
        if not full:
            continue
        try:
            pms.append(PostMortem(
                trade_id=full.get("trade_id", 0),
                ticker=full.get("ticker", r.get("ticker", "")),
                direction=full.get("direction", ""),
                outcome=full.get("outcome", r.get("outcome", "")),
                actual_return=full.get("actual_return", 0.0),
                data_at_decision=full.get("data_at_decision", {}),
                thesis_at_decision=full.get("thesis_at_decision", ""),
                sanity_results_at_decision=full.get("sanity_results_at_decision", {}),
                what_actually_happened=full.get("what_actually_happened", ""),
                price_path=full.get("price_path", []),
                failure_category=full.get("failure_category", r.get("failure_category", "")),
                root_cause=full.get("root_cause", r.get("root_cause", "")),
                which_signals_were_wrong=full.get("which_signals_were_wrong", []),
                which_signals_were_right=full.get("which_signals_were_right", []),
                what_we_missed=full.get("what_we_missed", r.get("what_we_missed", "")),
                recommended_fix=full.get("recommended_fix", r.get("recommended_fix", "")),
                confidence_in_analysis=full.get("confidence_in_analysis", 0.5),
                generated_at=full.get("generated_at", ""),
            ))
        except Exception:
            continue

    if not pms:
        return {
            "text": "No hydrated post-mortems available for synthesis.",
            "count": 0,
            "label": None,
        }

    text_out = generate_lessons_learned(engine, pms)
    date_range = _date_range_label([r.get("generated_at") for r in records[: len(pms)]])
    return {
        "text": text_out,
        "count": len(pms),
        "label": _lessons_label(len(pms), date_range),
    }


@router.get("/postmortem-lessons")
async def get_postmortem_lessons(
    n: int = Query(5, ge=1, le=200, description="Top-N postmortems to synthesize from"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    refresh: int = Query(0, ge=0, le=1, description="Set 1 to force regenerate"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return LLM-synthesized post-mortem lessons (6h DB cache).

    Async path lifted off the dashboard cold load — frontend should
    fetch this after the dashboard renders.
    """
    engine = get_db_engine()

    if not refresh:
        cached = _read_cache(engine)
        if cached and _is_fresh(cached.get("_ts")):
            if cached.get("n") == int(n) and cached.get("days") == int(days):
                return {
                    "lessons": cached["lessons"],
                    "generated_at": cached["generated_at"],
                    "n": cached["n"],
                    "days": cached["days"],
                    "cached": True,
                    "label": _cached_label(cached["lessons"]),
                }

    try:
        payload = _generate(engine, n=n, days=days)
    except Exception as exc:
        log.warning("postmortem-lessons synthesis failed: {e}", e=str(exc))
        cached = _read_cache(engine)
        if cached:
            return {
                "lessons": cached["lessons"],
                "generated_at": cached["generated_at"],
                "n": cached["n"],
                "days": cached["days"],
                "cached": True,
                "error": str(exc),
                "label": _cached_label(cached["lessons"]),
            }
        return {"lessons": None, "generated_at": None, "error": str(exc), "cached": False}

    if not payload.get("count"):
        # Wave 3 #6: the source window is empty. Never regenerate a
        # fabricated-sounding "summary" from zero postmortems, and never let
        # that empty result overwrite a still-useful prior cache before its
        # TTL — the cache row age (e.g. "09-24") already tells the operator
        # nobody has opened the dashboard recently; wiping it on an empty
        # window would instead read as "nothing was ever learned."
        note = f"no postmortems in the last {days} days"
        cached = _read_cache(engine)
        if cached:
            return {
                "lessons": cached["lessons"],
                "generated_at": cached["generated_at"],
                "n": cached["n"],
                "days": cached["days"],
                "cached": True,
                "note": note,
                "label": _cached_label(cached["lessons"]),
            }
        return {
            "lessons": {"lessons": [], "text": payload.get("text"), "count": 0},
            "generated_at": None,
            "n": int(n),
            "days": int(days),
            "cached": False,
            "note": note,
            "label": None,
        }

    gen_at = _write_cache(engine, payload, n=n, days=days)
    return {
        "lessons": payload,
        "generated_at": gen_at,
        "n": int(n),
        "days": int(days),
        "cached": False,
        "label": payload.get("label"),
    }
