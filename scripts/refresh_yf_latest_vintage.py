#!/usr/bin/env python3
"""Refresh ``public.yf_latest_daily_vintage`` (latest Yahoo daily vintage per day).

The view is created ``WITH NO DATA`` by
``migrations/versions/yf_latest_daily_vintage_20261019.py``. This script
populates it: the first run (view unpopulated) does a plain
``REFRESH MATERIALIZED VIEW`` (``CONCURRENTLY`` is refused on an unpopulated
view); every later run does ``REFRESH MATERIALIZED VIEW CONCURRENTLY`` so
readers are never blocked. Idempotent: re-running just re-derives the same
rows from ``raw_series``.

Each refresh runs in one transaction with ``statement_timeout`` 1200 s and the
planner hints (``enable_hashjoin/mergejoin/seqscan/bitmapscan = off``) that keep
the per-series LATERAL index probes; without them the planner picks a hash join
over a 511 GB scan. Expect ~10 minutes on production.

The view is a convenience cache, never a point-in-time source; see
``docs/reference/YF_LATEST_DAILY_VINTAGE.md``.

Usage:
    python3 scripts/refresh_yf_latest_vintage.py             # refresh
    python3 scripts/refresh_yf_latest_vintage.py --dry-run   # print SQL, touch no DB

Exit codes: 0 refreshed (or dry run); 1 refresh failed; 2 the view does not
exist (migration not applied).

Timer template (not installed): deploy/systemd/grid-yf-latest-vintage.timer.template
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from loguru import logger as log  # noqa: E402

from migrations.versions.yf_latest_daily_vintage_20261019 import (  # noqa: E402
    PLANNER_HINTS,
    VIEW_NAME,
)

STATEMENT_TIMEOUT_S = 1200
LOCK_TIMEOUT_S = 30

REFRESH_SQL = f"REFRESH MATERIALIZED VIEW {VIEW_NAME}"
REFRESH_CONCURRENTLY_SQL = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {VIEW_NAME}"

_STATE_SQL = (
    "SELECT c.relispopulated FROM pg_class c "
    "WHERE c.oid = to_regclass(:view_name)"
)
_COUNT_SQL = f"SELECT count(*) FROM {VIEW_NAME}"


def refresh_statements(populated: bool) -> list[str]:
    """The statements run, in order, inside the refresh transaction."""
    return [
        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_S}s'",
        f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_S}s'",
        *PLANNER_HINTS,
        REFRESH_CONCURRENTLY_SQL if populated else REFRESH_SQL,
    ]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--dry-run", action="store_true",
                   help="print the SQL that would run and exit without touching the database")
    return p


def _print_dry_run() -> None:
    print("-- dry run: no database connection is opened")
    print("-- view unpopulated (first run after the migration):")
    for stmt in refresh_statements(populated=False):
        print(f"{stmt};")
    print("-- view populated (every later run):")
    for stmt in refresh_statements(populated=True):
        print(f"{stmt};")
    print(f"-- afterwards, in a separate transaction:\n{_COUNT_SQL};")


def main(argv: list[str] | None = None, engine=None) -> int:
    args = build_parser().parse_args(argv)
    if args.dry_run:
        _print_dry_run()
        return 0

    from sqlalchemy import text

    if engine is None:
        from db import get_engine

        engine = get_engine()

    with engine.connect() as conn:
        state = conn.execute(text(_STATE_SQL), {"view_name": VIEW_NAME}).scalar()
        conn.rollback()
    if state is None:
        log.warning("{view} does not exist; apply migration yf_latest_daily_vintage_20261019 first",
                    view=VIEW_NAME)
        print(f"{VIEW_NAME} does not exist: apply migration yf_latest_daily_vintage_20261019",
              file=sys.stderr)
        return 2
    populated = bool(state)

    started = time.monotonic()
    try:
        with engine.connect() as conn:
            with conn.begin():
                for stmt in refresh_statements(populated):
                    conn.execute(text(stmt))
        duration = time.monotonic() - started
        # Separate transaction: the count must not run under the planner hints.
        with engine.connect() as conn:
            rows = conn.execute(text(_COUNT_SQL)).scalar()
            conn.rollback()
    except Exception as exc:  # noqa: BLE001 - report and exit non-zero for the timer
        log.warning("{view} refresh failed after {secs:.0f}s: {err}",
                    view=VIEW_NAME, secs=time.monotonic() - started, err=exc)
        print(f"refresh failed: {exc}", file=sys.stderr)
        return 1

    mode = "concurrent" if populated else "initial (non-concurrent)"
    log.info("{view} refreshed ({mode}): {rows} rows in {secs:.1f}s",
             view=VIEW_NAME, mode=mode, rows=rows, secs=duration)
    print(f"{VIEW_NAME} refreshed ({mode}): {rows} rows in {duration:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
