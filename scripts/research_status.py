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
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, created_at, payload "
                    "FROM analytical_snapshots "
                    "WHERE category = :cat AND subcategory = :sub "
                    "ORDER BY created_at DESC, id DESC "
                    "LIMIT 1"
                ),
                {"cat": RESEARCH_RUN_CATEGORY, "sub": RESEARCH_RUN_SUBCATEGORY},
            ).fetchone()
    except Exception as exc:
        # Operational, not an application bug: the table may not exist yet
        # on a fresh install, or the database may be briefly unreachable.
        log.warning("Could not read latest research run: {e}", e=str(exc))
        return None

    if row is None:
        return None

    snap_id, created_at, payload = row[0], row[1], row[2]
    payload = dict(payload) if payload else {}
    result: dict[str, Any] = {
        "id": snap_id,
        "created_at": created_at.isoformat() if created_at else None,
    }
    result.update(payload)
    return result
