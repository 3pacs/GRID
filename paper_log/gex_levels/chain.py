"""PIT-safe options-chain selection.

Pre-registration: "Chain: GRID `options_snapshots` for SPY, the latest
`snap_date` whose rows were created before the run. Record `snap_date` and
`created_at`."

GRID's non-negotiable PIT rule (repo CLAUDE.md, task spec's HARD RULES) is
"only data created before the pre-open run". A `snap_date` batch can be
written over several minutes; picking a `snap_date` whose *some* rows
predate the run but whose others don't would let ``DealerGammaEngine``
(which loads the *whole* `snap_date` regardless of `created_at`) read rows
created after the run started. So the query below requires the *entire*
batch (``MAX(created_at)`` for that `snap_date`) to predate the run before
that `snap_date` is eligible — not just the latest row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import Date, DateTime, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ChainSelection:
    snap_date: date
    created_at: datetime  # MAX(created_at) across that snap_date's rows


_SELECT_LATEST_COMPLETE_BATCH = text(
    """
    SELECT snap_date, MAX(created_at) AS batch_created_at
    FROM options_snapshots
    WHERE ticker = :ticker
    GROUP BY snap_date
    HAVING MAX(created_at) < :run_at
    ORDER BY snap_date DESC
    LIMIT 1
    """
).columns(snap_date=Date(), batch_created_at=DateTime(timezone=True))


def select_chain_snapshot(engine: Engine, ticker: str, run_at: datetime) -> ChainSelection | None:
    """Return the latest fully-pre-run `snap_date` batch, or None (no_chain).

    ``run_at`` must be timezone-aware (UTC) — compared directly against the
    TIMESTAMPTZ `created_at` column, so Postgres does the timezone-correct
    comparison regardless of session timezone.
    """
    with engine.connect() as conn:
        row = conn.execute(
            _SELECT_LATEST_COMPLETE_BATCH, {"ticker": ticker, "run_at": run_at}
        ).fetchone()

    if row is None:
        return None
    return ChainSelection(snap_date=row.snap_date, created_at=row.batch_created_at)
