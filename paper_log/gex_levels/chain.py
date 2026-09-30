"""PIT-safe options-chain selection.

Pre-registration: "Chain: GRID `options_snapshots` for SPY, the latest
`snap_date` whose rows were created before the run. Record `snap_date` and
`created_at`."

GRID's PIT rule is "only data created before the pre-open run". A day can
contain multiple immutable captures. Selection is by complete capture batch,
with ``MAX(created_at)`` for that batch strictly before the run. Legacy rows
without batch identity are ineligible because the engine cannot replay them
without risking a same-day capture swap.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import Date, DateTime, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ChainSelection:
    snap_date: date
    created_at: datetime  # MAX(created_at) within this capture batch
    capture_batch_id: str
    capture_ordinal: int


_SELECT_LATEST_COMPLETE_BATCH = text(
    """
    SELECT snap_date, capture_batch_id, capture_ordinal,
           MAX(created_at) AS batch_created_at
    FROM options_snapshots
    WHERE ticker = :ticker AND capture_batch_id IS NOT NULL
      AND capture_ordinal IS NOT NULL
      AND (CAST(:requested_batch_id AS TEXT) IS NULL OR capture_batch_id = :requested_batch_id)
      AND (CAST(:requested_ordinal AS BIGINT) IS NULL OR capture_ordinal = :requested_ordinal)
    GROUP BY snap_date, capture_batch_id, capture_ordinal
    HAVING MAX(created_at) < :run_at
    ORDER BY snap_date DESC, capture_ordinal DESC
    LIMIT 1
    """
).columns(snap_date=Date(), batch_created_at=DateTime(timezone=True))


def select_chain_snapshot(engine: Engine, ticker: str, run_at: datetime, *,
                          capture_batch_id: str | None = None,
                          capture_ordinal: int | None = None) -> ChainSelection | None:
    """Return the latest fully-pre-run batch, or an explicitly pinned batch.

    ``run_at`` must be timezone-aware (UTC) — compared directly against the
    TIMESTAMPTZ `created_at` column, so Postgres does the timezone-correct
    comparison regardless of session timezone.
    """
    if (capture_batch_id is None) != (capture_ordinal is None):
        raise ValueError("capture batch ID and ordinal must be supplied together")
    with engine.connect() as conn:
        row = conn.execute(
            _SELECT_LATEST_COMPLETE_BATCH,
            {"ticker": ticker, "run_at": run_at,
             "requested_batch_id": capture_batch_id,
             "requested_ordinal": capture_ordinal},
        ).fetchone()

    if row is None:
        return None
    return ChainSelection(snap_date=row.snap_date, created_at=row.batch_created_at,
                          capture_batch_id=row.capture_batch_id,
                          capture_ordinal=row.capture_ordinal)
