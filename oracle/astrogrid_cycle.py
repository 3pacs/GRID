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
    engine_runs: bool | None = None,
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
        engine_runs: Run the twelve lens engines over the snapshot and store
            them as ``astrogrid.engine_run`` rows (see
            :mod:`oracle.astrogrid_engines`). ``None`` reads
            ``settings.ASTROGRID_ENGINE_RUNS_ENABLED``, which defaults off.
            Only for today's date: a past ``target`` would be a back-computed
            row, and the #500 decision is forward-only.

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

    if engine_runs is None:
        from config import settings

        engine_runs = bool(settings.ASTROGRID_ENGINE_RUNS_ENABLED)
    if engine_runs:
        summary["engine_runs"] = _run_engines(
            snapshot,
            evaluation_date=evaluation_date,
            store=store,
            snapshot_id=summary["snapshot_id"],
        )
        if summary["engine_runs"].get("error"):
            summary["errors"].append(f"engine_runs: {summary['engine_runs']['error']}")

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


def _run_engines(
    snapshot: dict[str, Any],
    *,
    evaluation_date: date,
    store: Any,
    snapshot_id: int | None,
) -> dict[str, Any]:
    """Run the lens engines and store them against the day's sky_snapshot.

    The outputs are deliberately NOT added to the interpretation request.
    Twelve compacted outputs grow the interpretation prompt about fivefold
    (~5.5k to ~25k characters measured on 2026-11-21), which with
    ``num_predict=1200`` overruns the 3090 oracle's 8192-token context. So the
    Seer row keeps citing ``[]`` until someone decides how the Seer should
    read them; citing runs it never saw would be false provenance.
    """
    from oracle.astrogrid_engines import ENGINE_PRODUCER, compute_engine_outputs

    report: dict[str, Any] = {"engine_run_ids": [], "reused": False, "error": None}
    if evaluation_date != date.today():
        report["skipped"] = "not_today"
        return report

    computed = compute_engine_outputs(snapshot, mode=SCHEDULED_MODE)
    report["engines"] = len(computed["engine_outputs"])
    report["code_sha256"] = computed.get("code_sha256") or {}
    if computed.get("error"):
        report["error"] = computed["error"]
        return report

    if store is None or snapshot_id is None:
        # persist=False, or the snapshot row could not be written: an
        # engine_run row must cite a stored sky_snapshot, so write nothing.
        report["skipped"] = "no_stored_snapshot"
        return report

    lens_set_id = store.ensure_lens_set(SCHEDULED_MODE, [])
    saved = store.save_engine_runs(
        snapshot_id,
        lens_set_id,
        computed["engine_outputs"],
        producer=ENGINE_PRODUCER,
    )
    report["engine_run_ids"] = list(saved.get("engine_run_ids") or [])
    report["reused"] = bool(saved.get("reused"))
    if saved.get("error"):
        report["error"] = f"persist: {saved['error']}"
    return report
