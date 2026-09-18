"""
GRID Research Run Status — read-only surface over the autoresearch run-state
trail persisted by scripts/autoresearch.py.

scripts/autoresearch.py::run_autoresearch() writes a "research_run" snapshot
(category="research_run", subcategory="autoresearch") to the existing
``analytical_snapshots`` table (store/snapshots.py) at start, after each
iteration, and at end — see that module's ``_record_research_run`` for the
exact record shape (run_id, status, phase, error, error_category, iteration,
iterations, skip_reasons, failure_reasons, duration_s, generation, code_sha,
inputs).

This module is intentionally separate from that write path and from
``api/``: it is the read-only surface a future API/UI layer (GRID W4c) wires
up to expose "what is the research loop doing right now" without importing
scripts/autoresearch.py itself (which pulls in psycopg2, Ollama, and the
walk-forward backtester — heavy dependencies an API process should not need
just to answer a status question).

No new table: this reads the same ``analytical_snapshots`` rows
AnalyticalSnapshotStore already knows how to write.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

RESEARCH_RUN_CATEGORY = "research_run"
RESEARCH_RUN_SUBCATEGORY = "autoresearch"


def _query_latest_research_run_row(engine: Engine) -> tuple[Any, Any, Any] | None:
    """Run the actual query. Raises on any DB/table error; returns None on
    an empty (but reachable) table. Kept separate from the two public
    functions below so each can decide, independently, what "no result"
    should look like to ITS caller.
    """
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT id, created_at, payload "
                "FROM analytical_snapshots "
                "WHERE category = :cat AND subcategory = :sub "
                "ORDER BY created_at DESC, id DESC "
                "LIMIT 1"
            ),
            {"cat": RESEARCH_RUN_CATEGORY, "sub": RESEARCH_RUN_SUBCATEGORY},
        ).fetchone()


def _row_to_record(row: tuple[Any, Any, Any]) -> dict[str, Any]:
    snap_id, created_at, payload = row[0], row[1], row[2]
    payload = dict(payload) if payload else {}
    result: dict[str, Any] = {
        "id": snap_id,
        "created_at": created_at.isoformat() if created_at else None,
    }
    result.update(payload)
    return result


def latest_research_run(engine: Engine) -> dict[str, Any] | None:
    """Return the most recently written autoresearch run record, or None.

    "Most recent" is the row with the newest ``created_at`` in the
    ``research_run``/``autoresearch`` category/subcategory — since a single
    run_id gets several rows over its lifetime (started -> running x N ->
    ok/failed/timeout/abandoned), this is the latest EVENT for the latest
    run, not necessarily a terminal one: a caller checking on an in-progress
    run will see its latest "running" checkpoint.

    Parameters:
        engine: SQLAlchemy engine for database access.

    Returns:
        dict with the snapshot row's ``id``, ``created_at``, and the full
        ``payload`` merged to the top level (run_id, status, phase, error,
        error_category, iteration, iterations, skip_reasons,
        failure_reasons, duration_s, generation, code_sha, inputs), or None
        if no research_run rows exist yet or the table/database is
        unreachable (logged, not raised — this is a status read, not a
        critical path).

    Note: this collapses "no rows yet" and "table/database unreachable"
    into the same None — callers that must tell those apart (e.g. an API
    endpoint that should never 500 on a missing table but should say so
    explicitly) want ``latest_research_run_result`` instead.
    """
    try:
        row = _query_latest_research_run_row(engine)
    except Exception as exc:
        # Operational, not an application bug: the table may not exist yet
        # on a fresh install, or the database may be briefly unreachable.
        log.warning("Could not read latest research run: {e}", e=str(exc))
        return None

    if row is None:
        return None
    return _row_to_record(row)


def latest_research_run_result(engine: Engine) -> dict[str, Any]:
    """Like ``latest_research_run``, but distinguishes "no rows yet" from
    "table/database unreachable" instead of collapsing both to None — for
    callers (the ``/api/v1/snapshots/research/latest`` endpoint) that must
    say which one happened rather than silently returning nothing either
    way.

    Returns exactly one of:
        {"status": "ok", **record}            - a research_run event exists;
                                                  note "status" here is the
                                                  RECORD's own lifecycle
                                                  status (started/running/
                                                  ok/failed/timeout/
                                                  abandoned) merged in from
                                                  the payload, not a fixed
                                                  "ok" sentinel — none of
                                                  those values collide with
                                                  "no_runs"/"unavailable"
                                                  below.
        {"status": "no_runs"}                  - table reachable, zero rows.
        {"status": "unavailable", "reason": str} - the query itself failed
                                                      (missing table,
                                                      unreachable database,
                                                      ...). Never raises.
    """
    try:
        row = _query_latest_research_run_row(engine)
    except Exception as exc:
        log.warning("Could not read latest research run: {e}", e=str(exc))
        return {"status": "unavailable", "reason": str(exc)}

    if row is None:
        return {"status": "no_runs"}
    return _row_to_record(row)


def latest_hypothesis_outcome(engine: Engine) -> dict[str, Any] | None:
    """Return the most recently updated ``hypothesis_registry`` row's
    outcome summary, or None.

    IMPORTANT — this is a SEPARATE read, not an extraction from the
    research_run record above: the "research_run" snapshot payload
    ``_record_research_run`` writes (scripts/autoresearch.py) does not
    currently carry any hypothesis_id/statement/verdict field — it tracks
    iteration counts and status, not which specific hypothesis a run
    tested. There is today no stored link from a research_run row to a
    hypothesis_registry row. This function independently reads the latest
    hypothesis_registry row so a caller can show "what did the research
    loop most recently decide" alongside "is the loop currently running" —
    the two questions the /research/latest endpoint answers together.

    Parameters:
        engine: SQLAlchemy engine for database access.

    Returns:
        dict with ``id``, ``statement``, ``layer``, ``state``,
        ``kill_reason``, ``updated_at`` (ISO string), or None if the table
        is empty/unreachable (logged, not raised).
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, statement, layer, state, kill_reason, updated_at "
                    "FROM hypothesis_registry "
                    "ORDER BY updated_at DESC, id DESC "
                    "LIMIT 1"
                )
            ).fetchone()
    except Exception as exc:
        log.warning("Could not read latest hypothesis outcome: {e}", e=str(exc))
        return None

    if row is None:
        return None

    return {
        "id": row[0],
        "statement": row[1],
        "layer": row[2],
        "state": row[3],
        "kill_reason": row[4],
        "updated_at": row[5].isoformat() if row[5] else None,
    }
