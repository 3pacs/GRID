"""Dead-letter store for the contracts layer.

Failed handler dispatches land here and are either retried automatically on
a 1min / 10min / 1hr schedule, or replayed manually at any time via CLI or
the PWA ops dashboard.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine


# Retry cadence in seconds: 1 min, 10 min, 1 hr.
RETRY_SCHEDULE: tuple[int, ...] = (60, 600, 3600)


@dataclass(frozen=True)
class DeadLetterEntry:
    id: int
    event_id: UUID
    contract_type: str
    payload: dict[str, Any]
    consumer: str
    error_type: str
    error_detail: str
    retry_count: int
    next_retry_at: datetime | None
    failed_at: datetime
    correlation_id: UUID | None


def record_failure(
    engine: Engine,
    *,
    event_id: UUID,
    contract_type: str,
    payload: dict[str, Any],
    consumer: str,
    error_type: str,
    error_detail: str,
    correlation_id: UUID | None = None,
) -> int:
    """Write a new dead-letter row and schedule its first retry."""
    now = datetime.now(timezone.utc)
    next_retry = schedule_next_retry(retry_count=0, now=now)

    sql = text(
        """
        INSERT INTO contracts_dead_letter (
            event_id, contract_type, payload, consumer,
            error_type, error_detail, retry_count, next_retry_at,
            failed_at, correlation_id
        ) VALUES (
            :event_id, :contract_type, CAST(:payload AS JSONB), :consumer,
            :error_type, :error_detail, 0, :next_retry_at,
            :failed_at, :correlation_id
        )
        RETURNING id
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            sql.bindparams(
                event_id=str(event_id),
                contract_type=contract_type,
                payload=json.dumps(payload, default=str),
                consumer=consumer,
                error_type=error_type,
                error_detail=error_detail,
                next_retry_at=next_retry,
                failed_at=now,
                correlation_id=str(correlation_id) if correlation_id else None,
            )
        )
        return int(result.fetchone()[0])


def pending_retries(
    engine: Engine, now: datetime | None = None, limit: int = 100
) -> list[DeadLetterEntry]:
    """Return unresolved entries whose ``next_retry_at`` is due."""
    now = now or datetime.now(timezone.utc)
    sql = text(
        """
        SELECT id, event_id, contract_type, payload, consumer,
               error_type, error_detail, retry_count, next_retry_at,
               failed_at, correlation_id
        FROM contracts_dead_letter
        WHERE resolved_at IS NULL
          AND next_retry_at IS NOT NULL
          AND next_retry_at <= :now
        ORDER BY next_retry_at
        LIMIT :limit
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql.bindparams(now=now, limit=limit)).fetchall()

    out: list[DeadLetterEntry] = []
    for r in rows:
        out.append(
            DeadLetterEntry(
                id=int(r[0]),
                event_id=UUID(str(r[1])),
                contract_type=str(r[2]),
                payload=r[3] if isinstance(r[3], dict) else json.loads(r[3]),
                consumer=str(r[4]),
                error_type=str(r[5]),
                error_detail=str(r[6]),
                retry_count=int(r[7]),
                next_retry_at=r[8],
                failed_at=r[9],
                correlation_id=UUID(str(r[10])) if r[10] else None,
            )
        )
    return out


def mark_resolved(engine: Engine, entry_id: int) -> None:
    """Mark a dead-letter entry as resolved (after successful retry/replay)."""
    sql = text(
        """
        UPDATE contracts_dead_letter
        SET resolved_at = NOW()
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(sql.bindparams(id=entry_id))


def bump_retry(engine: Engine, entry_id: int, retry_count: int) -> None:
    """Record a failed retry and schedule the next one (or give up)."""
    next_retry = schedule_next_retry(retry_count=retry_count + 1)
    sql = text(
        """
        UPDATE contracts_dead_letter
        SET retry_count = :rc, next_retry_at = :nra
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql.bindparams(id=entry_id, rc=retry_count + 1, nra=next_retry)
        )


def schedule_next_retry(
    retry_count: int, now: datetime | None = None
) -> datetime | None:
    """Compute when the next retry should run, or None if budget exhausted."""
    if retry_count >= len(RETRY_SCHEDULE):
        return None
    now = now or datetime.now(timezone.utc)
    return now + timedelta(seconds=RETRY_SCHEDULE[retry_count])
