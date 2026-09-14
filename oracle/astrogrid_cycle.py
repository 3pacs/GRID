"""AstroGrid celestial production cycle.

AstroGrid's learning half — scoring, backtests, weight review — has run on its
own ``astrogrid-learning.timer`` since March 2026 and is healthy. Its celestial
half never had a scheduler at all: ``save_snapshot`` was reachable only from
``GET /astrogrid/snapshot`` and ``save_interpretation`` only from
``POST /astrogrid/interpret``, so both tables stopped the day a browser stopped
asking. ``astrogrid.sky_snapshot`` last gained a row on 2026-04-28 and
``astrogrid.persona_run`` on 2026-05-01, while ``seer_run`` and ``engine_run``
never held one.

This module is the missing producer. Hermes calls :func:`run_celestial_cycle`
once per cycle; it builds the deterministic sky state, interprets it, and
persists both through the same :class:`~store.astrogrid.AstroGridStore` the
routes use.

It deliberately calls ``build_snapshot``/``build_interpretation`` in-process
rather than issuing HTTP to our own API: an out-of-band call would make the
scheduler depend on the API being up, and would let the two paths drift.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger as log

__all__ = ["run_celestial_cycle"]

# The interpretation is framed as a standing question rather than a user's,
# because nobody is asking — this is the scheduled heartbeat of the celestial
# state, not a reply to anyone.
SCHEDULED_QUESTION = "What celestial threads matter for markets right now?"

# "chorus" runs every allowed lens and merges them, which is what an unattended
# cycle wants; "solo" and "intersection" exist for a caller with an opinion.
SCHEDULED_MODE = "chorus"


def run_celestial_cycle(
    engine: Any,
    *,
    target: date | None = None,
    persist: bool = True,
    interpret: bool = True,
) -> dict[str, Any]:
    """Produce one celestial cycle: sky snapshot, then interpretation.

    Args:
        engine: SQLAlchemy engine for the GRID database.
        target: Date to build state for. Defaults to today (UTC date as the
            rest of AstroGrid reckons it).
        persist: When ``False``, build everything and return it without
            writing.
        interpret: When ``False``, stop after the sky snapshot and skip the
            interpretation. The Hermes dry run passes ``False`` for both: it
            exercises the deterministic build, which is sub-second and has no
            side effects, without spending a local-LLM call on a rehearsal.

    Returns:
        A summary dict with ``date``, ``snapshot_id``, ``interpretation_ids``,
        ``used_llm`` and ``persisted``. Never raises: a celestial cycle that
        fails must not take a Hermes cycle down with it, so failures are
        reported in the returned ``errors`` list and logged at warning.
    """
    # Imported here rather than at module scope: api.routers pulls in FastAPI
    # and the whole router tree, which Hermes has no reason to import unless
    # this step actually runs.
    from api.routers.astrogrid_helpers import (
        AstrogridInterpretRequest,
        build_interpretation,
        build_snapshot,
    )

    evaluation_date = target or date.today()
    summary: dict[str, Any] = {
        "date": evaluation_date.isoformat(),
        "snapshot_id": None,
        "interpretation_ids": {},
        "used_llm": False,
        "persisted": bool(persist),
        "errors": [],
    }

    try:
        snapshot = build_snapshot(evaluation_date, engine)
    except Exception as exc:
        log.warning("AstroGrid celestial snapshot build failed: {e}", e=str(exc))
        summary["errors"].append(f"snapshot: {exc}")
        return summary

    summary["events"] = len(snapshot.get("events") or [])
    summary["aspects"] = len(snapshot.get("aspects") or [])

    store = None
    if persist:
        try:
            from api.dependencies import get_astrogrid_store

            store = get_astrogrid_store()
            summary["snapshot_id"] = store.save_snapshot(snapshot)
        except Exception as exc:
            log.warning("AstroGrid celestial snapshot persist failed: {e}", e=str(exc))
            summary["errors"].append(f"snapshot_persist: {exc}")

    # The interpretation is fed the snapshot we just built, so the seer reads
    # the same sky that was stored rather than recomputing a drifted one.
    req = AstrogridInterpretRequest(
        question=SCHEDULED_QUESTION,
        mode=SCHEDULED_MODE,
        snapshot=snapshot,
        seer=snapshot.get("seer") or {},
    )

    if not interpret:
        summary["interpretation_skipped"] = True
        log.info(
            "AstroGrid celestial cycle — date={d} snapshot={s} interpretation skipped",
            d=summary["date"],
            s=summary["snapshot_id"],
        )
        return summary

    try:
        interpretation = build_interpretation(req)
    except Exception as exc:
        # build_interpretation already swallows its own failures and returns a
        # deterministic fallback, so reaching here means something unexpected.
        log.warning("AstroGrid celestial interpretation failed: {e}", e=str(exc))
        summary["errors"].append(f"interpretation: {exc}")
        return summary

    summary["used_llm"] = bool(interpretation.get("used_llm"))
    if interpretation.get("error"):
        summary["errors"].append(f"interpretation: {interpretation['error']}")

    if persist and store is not None:
        try:
            summary["interpretation_ids"] = store.save_interpretation(
                req.model_dump(), interpretation
            )
        except Exception as exc:
            log.warning(
                "AstroGrid celestial interpretation persist failed: {e}", e=str(exc)
            )
            summary["errors"].append(f"interpretation_persist: {exc}")

    log.info(
        "AstroGrid celestial cycle — date={d} snapshot={s} llm={l} errors={e}",
        d=summary["date"],
        s=summary["snapshot_id"],
        l=summary["used_llm"],
        e=len(summary["errors"]),
    )
    return summary
