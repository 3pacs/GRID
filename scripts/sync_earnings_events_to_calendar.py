"""Task #186: mirror earnings_events → earnings_calendar.

Tiny wrapper around the SQL function sync_earnings_events_to_calendar() so
hermes_operator can schedule it.  Idempotent.  Logs (inserted, total).
"""

from __future__ import annotations

import sys

from loguru import logger as log
from sqlalchemy import text

from db import get_engine


def run() -> dict[str, int]:
    """Call sync_earnings_events_to_calendar() and log the outcome.

    Returns:
        dict with keys ``inserted`` and ``total_events``.
    """
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT inserted_count, total_events "
                 "FROM sync_earnings_events_to_calendar()"),
        ).fetchone()

    inserted = int(row[0]) if row and row[0] is not None else 0
    total = int(row[1]) if row and row[1] is not None else 0
    log.info(
        "earnings_events → earnings_calendar mirror: inserted={i} of {t} eligible",
        i=inserted, t=total,
    )
    return {"inserted": inserted, "total_events": total}


if __name__ == "__main__":
    result = run()
    print(f"inserted={result['inserted']} total_events={result['total_events']}")
    sys.exit(0)
