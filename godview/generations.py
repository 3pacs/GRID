"""godview.generations — atomic per-run bookkeeping shared by every God View pillar.

``godview_generations`` (created by migrations/versions/godview_pit_cftc_0918.py)
records one row per materializer *attempt*: did it complete, fail, or is it
mid-flight, how many rows did it add, and when was it published. It is a
per-run audit/health record, not a data partition -- see
docs/reference/GODVIEW_PILLAR_CONTRACT.md section 7 for why a strict-PIT read
still queries the pillar's own table directly rather than joining on "the
latest generation's rows."

Helpers here take a live ``Connection`` (never an ``Engine``) because the
whole point is that the caller controls the transaction: ``record_generation``
must run on the SAME connection/transaction as the pillar's own INSERT-only
writes for a ``status="complete"`` publish to be atomic. See
``godview/cftc_pillar.py::materialize_cftc_pillar`` for the only caller.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

STATUS_BUILDING = "building"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"
_VALID_STATUSES = frozenset({STATUS_BUILDING, STATUS_COMPLETE, STATUS_FAILED})


def new_generation_id() -> str:
    """A fresh generation id. Always a str (not a uuid.UUID) -- the DB column is TEXT."""
    return str(uuid.uuid4())


@dataclass(frozen=True)
class GenerationRecord:
    pillar: str
    generation_id: str
    status: str
    row_count: int | None
    coverage_fraction: float | None
    published_at: Any  # datetime | None -- left loose to avoid importing the DBAPI's tz type here


def record_generation(
    conn: Connection,
    *,
    pillar: str,
    generation_id: str,
    status: str,
    row_count: int | None = None,
    coverage_fraction: float | None = None,
    failure_reason: str | None = None,
) -> None:
    """Insert (or, on retry with the same generation_id, update) one generation's bookkeeping row.

    ``published_at`` is set (to ``NOW()``) if and only if ``status == "complete"`` --
    a failed or building row never gets a ``published_at``, so "does this
    generation have a published_at" is itself a reliable complete/not-complete check.
    """
    if status not in _VALID_STATUSES:
        raise ValueError(f"unknown generation status: {status!r}")
    if coverage_fraction is not None and not (0.0 <= coverage_fraction <= 1.0):
        raise ValueError(f"coverage_fraction out of range [0,1]: {coverage_fraction!r}")

    conn.execute(
        text(
            """
            INSERT INTO godview_generations
                (pillar, generation_id, status, row_count, coverage_fraction,
                 failure_reason, published_at)
            VALUES
                (:pillar, :gid, :status, :row_count, :coverage,
                 :failure_reason,
                 CASE WHEN :status = 'complete' THEN NOW() ELSE NULL END)
            ON CONFLICT (generation_id) DO UPDATE SET
                status = EXCLUDED.status,
                row_count = EXCLUDED.row_count,
                coverage_fraction = EXCLUDED.coverage_fraction,
                failure_reason = EXCLUDED.failure_reason,
                published_at = CASE WHEN EXCLUDED.status = 'complete'
                                     THEN NOW()
                                     ELSE godview_generations.published_at END
            """
        ),
        {
            "pillar": pillar,
            "gid": generation_id,
            "status": status,
            "row_count": row_count,
            "coverage": coverage_fraction,
            "failure_reason": failure_reason,
        },
    )


def latest_complete_generation(conn: Connection, pillar: str) -> dict[str, Any] | None:
    """The most recently published complete generation for ``pillar``, or ``None``.

    ``None`` covers two cases the caller must distinguish itself by also
    checking ``latest_attempt``: "never configured" (no row at all for this
    pillar) vs. "has run before but the latest attempt failed."
    """
    row = conn.execute(
        text(
            """
            SELECT generation_id, row_count, coverage_fraction, published_at
            FROM godview_generations
            WHERE pillar = :pillar AND status = 'complete'
            ORDER BY published_at DESC
            LIMIT 1
            """
        ),
        {"pillar": pillar},
    ).mappings().fetchone()
    return dict(row) if row is not None else None


def latest_attempt(conn: Connection, pillar: str) -> dict[str, Any] | None:
    """The most recent generation attempt of any status for ``pillar``, or ``None``."""
    row = conn.execute(
        text(
            """
            SELECT generation_id, status, failure_reason, started_at, published_at
            FROM godview_generations
            WHERE pillar = :pillar
            ORDER BY started_at DESC
            LIMIT 1
            """
        ),
        {"pillar": pillar},
    ).mappings().fetchone()
    return dict(row) if row is not None else None
