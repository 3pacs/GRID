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
its own worth mentioning. Run it in a SEPARATE session, started BEFORE and stopped AFTER
a future migration/backfill attempt, so that if the same transient slowdown recurs, there
is finally a trace to diagnose from instead of another "genuinely unknown" root cause.

Bounded duration (default, not opt-in)
---------------------------------------
Defaults to a 30-minute wall-clock cap (`--max-duration-seconds`, default 1800) -- this
tool is meant to watch one bounded attempt, not run indefinitely unattended. It also
still stops cleanly on Ctrl-C / SIGTERM before that cap, and `--max-samples` remains
available for a sample-count bound instead of a time bound. Whichever limit is hit first
stops the run; the reason is logged.

Sampling cost
-------------
Each sample's OWN query execution time (against pg_stat_activity/pg_locks, not against
any application table) is measured and logged. Measured directly on a disposable
PostgreSQL instance (gridz4, empty activity): ~1-3 ms per sample at the default 1s
interval -- see the reconciliation report for the exact figure from the run that
produced this script's test receipt. This is negligible against the `lock_timeout`/
`statement_timeout` values the migration and backfill actually run under (5s/30s), and
the interval is directly configurable (`--interval`) if a slower cadence is ever wanted.

Output redaction
-----------------
The query TEXT captured in `pg_stat_activity.query` is NOT parameter-placeholder-only --
verified directly (not assumed): psycopg2's default parameter style substitutes bound
values into the SQL text CLIENT-SIDE before sending it, so a literal value passed as a
bound parameter (e.g. `cur.execute("... = %s", (value,))`) is exactly what the server
records and what this script would otherwise capture verbatim. This applies to
SQLAlchemy's default psycopg2 dialect too (same client-side substitution under the
hood), so GRID's own "always use bound parameters" SQL-safety rule does not, by itself,
prevent this. Every captured query snippet is therefore passed through a best-effort
regex redaction (`_redact_query`) BEFORE being printed: single-quoted string literals and
bare numeric literals are replaced with placeholders. This is not a real SQL parser and
can be fooled by unusual quoting or non-literal-looking sensitive tokens (a bare
identifier, say) -- it is a pragmatic reduction of exposure for a diagnostic tool whose
actual purpose is lock/wait STRUCTURE (which relation, which lock mode, which session is
blocking which), not query DATA, not a guarantee that no sensitive value can ever appear
in its output. Treat the output file with the same handling as any other operational log
that might contain fragments of production data, not as pre-cleared for wide sharing.

Usage:
    # Terminal 1: start the monitor first, redirect to a file
    python3 scripts/monitor_pg_activity_during_migration.py --interval 1 --relation actors \\
        > /tmp/pg-activity-actors-migration-$(date -u +%Y%m%dT%H%M%SZ).jsonl

    # Terminal 2: once the monitor is confirmed running, THEN start the actual attempt
    #   (alembic upgrade head, or scripts/backfill_actor_provenance.py)

    # After the attempt finishes (success or failure): Ctrl-C the monitor (SIGINT), or
    # `kill -TERM <pid>` -- both flush and exit cleanly, the file is complete either way.
    # It also stops on its own after --max-duration-seconds (default 1800s / 30 min).

Output: one JSON object per line (JSON Lines), one line per (sampled_at, pid) row from
the query below. Safe to `jq` or `grep` after the fact. Blocked sessions (granted=false)
and any session waiting on a lock are the first thing to look for in a post-mortem.

This script makes NO decision and takes NO action -- it only observes and records.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import sys
import time
from datetime import datetime, timezone

from loguru import logger as log
from sqlalchemy import text

from db import get_engine

# Fetches more than the final display length so redaction (below) has room to work
# without operating on an already-truncated, possibly mid-literal fragment.
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
        left(a.query, 500) AS query_snippet,
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

# Best-effort only -- see module docstring's "Output redaction" section for limits.
_STRING_LITERAL_RE = re.compile(r"'(?:[^'\\]|\\.)*'")
_NUMERIC_LITERAL_RE = re.compile(r"(?<![\w$])-?\d+\.?\d*(?![\w$])")
_DISPLAY_SNIPPET_LEN = 300

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True


def _redact_query(query: str | None) -> str | None:
    """Best-effort literal redaction, then truncate for display. See module
    docstring's "Output redaction" section -- this is not a SQL parser."""
    if not query:
        return query
    redacted = _STRING_LITERAL_RE.sub("'<redacted>'", query)
    redacted = _NUMERIC_LITERAL_RE.sub("<n>", redacted)
    if len(redacted) > _DISPLAY_SNIPPET_LEN:
        redacted = redacted[:_DISPLAY_SNIPPET_LEN] + "...<truncated>"
    return redacted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between samples (default: 1.0)")
    parser.add_argument("--relation", default=None, help="only include rows locking this relation (or no lock at all); default: no filter")
    parser.add_argument("--max-samples", type=int, default=0, help="stop after N samples (default: 0 = no sample-count bound)")
    parser.add_argument("--max-duration-seconds", type=float, default=1800.0, help="stop after this many seconds regardless of sample count (default: 1800 = 30 min; this is the default bound, not opt-in)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    engine = get_engine()
    query = text(_QUERY_FILTERED if args.relation else _QUERY)
    params = {"relation": args.relation} if args.relation else {}

    log.info(
        "monitor_pg_activity_during_migration: interval={i}s relation={r} max_duration_seconds={md} "
        "max_samples={ms} -- Ctrl-C to stop earlier",
        i=args.interval, r=args.relation or "(none, all activity)",
        md=args.max_duration_seconds, ms=args.max_samples or "(none)",
    )

    started_at = time.monotonic()
    sample_count = 0
    row_count = 0
    stop_reason = "interrupted"
    while not _stop:
        elapsed = time.monotonic() - started_at
        if args.max_duration_seconds and elapsed >= args.max_duration_seconds:
            stop_reason = "max_duration_seconds"
            break

        sampled_at = datetime.now(timezone.utc).isoformat()
        query_started = time.monotonic()
        try:
            with engine.connect() as conn:
                rows = conn.execute(query, params).mappings().all()
            query_cost_ms = (time.monotonic() - query_started) * 1000.0
        except Exception as exc:  # pragma: no cover - defensive, keep sampling
            print(json.dumps({"sampled_at": sampled_at, "sampler_error": str(exc)}), flush=True)
            time.sleep(args.interval)
            continue

        if not rows:
            print(json.dumps({
                "sampled_at": sampled_at, "rows": 0, "sample_query_cost_ms": round(query_cost_ms, 2),
            }), flush=True)
        for row in rows:
            record = dict(row)
            record["query_snippet"] = _redact_query(record.get("query_snippet"))
            record = {"sampled_at": sampled_at, "sample_query_cost_ms": round(query_cost_ms, 2), **record}
            print(json.dumps(record, default=str), flush=True)
            row_count += 1

        sample_count += 1
        if args.max_samples and sample_count >= args.max_samples:
            stop_reason = "max_samples"
            break
        time.sleep(args.interval)

    log.info(
        "monitor stopped ({reason}): {s} samples over {e:.1f}s, {r} activity rows recorded",
        reason=stop_reason, s=sample_count, e=time.monotonic() - started_at, r=row_count,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
