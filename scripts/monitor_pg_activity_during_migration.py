#!/usr/bin/env python3
"""Continuously sample pg_locks + pg_stat_activity for the duration of a migration or
backfill attempt -- the specific observability gap the #596 incident's own diagnosis
named and left open.

From the bounded remediation plan
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md):

    "pg_locks/pg_stat_activity were checked immediately before the failed rerun, never
    *continuously during* the ~35-second execution window itself -- so a transient lock
    holder, a connection pool exhaustion moment, or something else entirely could have
    caused it and left no trace."

This script is that continuous sampler. It is a plain read-only observer -- it never
touches `actors`, never runs a migration, never runs the backfill, and takes no locks of
its own worth mentioning (each sample is a single fast SELECT against the catalog views).
Run it in a SEPARATE session, started BEFORE and stopped AFTER a future migration/backfill
attempt, so that if the same transient slowdown recurs, there is finally a trace to
diagnose from instead of another "genuinely unknown" root cause.

Usage:
    # Terminal 1: start the monitor first, redirect to a file
    python3 scripts/monitor_pg_activity_during_migration.py --interval 1 --relation actors \\
        > /tmp/pg-activity-actors-migration-$(date -u +%Y%m%dT%H%M%SZ).jsonl

    # Terminal 2: once the monitor is confirmed running, THEN start the actual attempt
    #   (alembic upgrade head, or scripts/backfill_actor_provenance.py)

    # After the attempt finishes (success or failure): Ctrl-C the monitor (SIGINT), or
    # `kill -TERM <pid>` -- both flush and exit cleanly, the file is complete either way.

Output: one JSON object per line (JSON Lines), one line per (sampled_at, pid) row from
the query below. Safe to `jq` or `grep` after the fact. Blocked sessions (granted=false)
and any session waiting on a lock are the first thing to look for in a post-mortem.

This script makes NO decision and takes NO action -- it only observes and records.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone

from loguru import logger as log
from sqlalchemy import text

from db import get_engine

_QUERY = """
    SELECT
        a.pid,
        a.usename,
        a.application_name,
        a.client_addr::text AS client_addr,
        a.state,
        a.wait_event_type,
        a.wait_event,
        a.query_start,
        EXTRACT(EPOCH FROM (now() - a.query_start)) AS query_duration_secs,
        left(a.query, 300) AS query_snippet,
        l.locktype,
        l.mode,
        l.granted,
        l.relation::regclass::text AS relation
    FROM pg_stat_activity a
    LEFT JOIN pg_locks l ON l.pid = a.pid
    WHERE a.pid != pg_backend_pid()
      AND (a.state IS DISTINCT FROM 'idle' OR l.granted = false)
    ORDER BY a.query_start NULLS LAST
"""

_QUERY_FILTERED = _QUERY.replace(
    "WHERE a.pid != pg_backend_pid()",
    "WHERE a.pid != pg_backend_pid() AND (l.relation::regclass::text = :relation OR l.relation IS NULL)",
)

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between samples (default: 1.0)")
    parser.add_argument("--relation", default=None, help="only include rows locking this relation (or no lock at all); default: no filter")
    parser.add_argument("--max-samples", type=int, default=0, help="stop after N samples (default: 0 = run until interrupted)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    engine = get_engine()
    query = text(_QUERY_FILTERED if args.relation else _QUERY)
    params = {"relation": args.relation} if args.relation else {}

    log.info(
        "monitor_pg_activity_during_migration: interval={i}s relation={r} -- Ctrl-C to stop",
        i=args.interval, r=args.relation or "(none, all activity)",
    )

    sample_count = 0
    row_count = 0
    while not _stop:
        sampled_at = datetime.now(timezone.utc).isoformat()
        try:
            with engine.connect() as conn:
                rows = conn.execute(query, params).mappings().all()
        except Exception as exc:  # pragma: no cover - defensive, keep sampling
            print(json.dumps({"sampled_at": sampled_at, "sampler_error": str(exc)}), flush=True)
            time.sleep(args.interval)
            continue

        if not rows:
            print(json.dumps({"sampled_at": sampled_at, "rows": 0}), flush=True)
        for row in rows:
            record = {"sampled_at": sampled_at, **dict(row)}
            # datetime/Decimal-safe serialization
            print(json.dumps(record, default=str), flush=True)
            row_count += 1

        sample_count += 1
        if args.max_samples and sample_count >= args.max_samples:
            break
        time.sleep(args.interval)

    log.info("monitor stopped: {s} samples, {r} activity rows recorded", s=sample_count, r=row_count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
