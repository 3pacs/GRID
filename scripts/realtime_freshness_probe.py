#!/usr/bin/env python3
"""Post-restart freshness probe for grid-realtime, consulted by deploy.yml's
"Verify grid-realtime is delivering fresh data" step.

## What this proves, precisely

`created_at > restart_ts` is what proves "a NEW insert happened after this
restart, not a stale pre-existing row": `created_at` is set only by an
actual INSERT (see INSERT_SQL in ingestion/realtime/flusher.py -- `ON
CONFLICT DO NOTHING` means a colliding write either lands as a fresh INSERT
for a genuinely new bucket, or is silently discarded for a colliding one;
there is no UPDATE path that could touch an existing row's `created_at`).
`restart_ts` is captured by deploy.yml immediately after the blocking
`systemctl restart` command returns, so it already unambiguously post-dates
the dying old process's entire shutdown sequence -- a row with
`created_at > restart_ts` could only have been written by the new process.

## Why this query is ALSO bounded by `ts`, and what that bound does NOT prove

A bare `WHERE created_at > :restart_ts` has no supporting index --
`realtime_candles` has none on `created_at` -- and forces a full sequential
scan. Against the real production table (2M+ rows spanning months) that
exceeds Postgres's own `statement_timeout` (confirmed in production: this
exact unbounded query was cancelled with `psycopg2.errors.QueryCanceled:
canceling statement due to statement timeout` during grid-realtime's
first-ever activation, 2026-09-16 -- it had only ever been exercised
against small, fresh CI tables before that). Adding `ts > :ts_floor` lets
Postgres use the existing `idx_rt_candles_ts` index instead (validated
directly against production, read-only, no index added and no setting
changed: the unbounded query plans as `Parallel Seq Scan`, cost ~39911;
the bounded one plans as `Index Scan using idx_rt_candles_ts`, cost ~286,
observed execution time under 1ms).

**This bound is a performance optimization, not something proven
equivalent to the unbounded scan in every case.** Traced against the
current code (candle_builder.py, feeds/binance.py, feeds/yahoo.py,
feeds/dex_scanner.py -- the only three ingest() callers, and the only
CandleBuilder() instantiation in the codebase, at ws_listener.py:50, using
the default 5-minute interval): every feed assigns a candle's `ts` (its
bucket floor) from `datetime.now(timezone.utc)` at the moment of ingest,
never from an externally-supplied or historical timestamp, so a genuinely
fresh row's `ts` should always be close to its own `created_at`, bounded
by the interval size. TS_WINDOW_SECONDS below is chosen generously wide
relative to that (4x the 300s flush interval) specifically to tolerate
delayed writes -- e.g. flusher.py's own buffer-and-retry-on-failure path,
which can hold candles across several flush cycles before a delayed
successful write. But this has NOT been proven as an exhaustive guarantee
covering every possible code path or third-party library behavior (e.g.
exact internal timing inside `websockets`, `yfinance`, or `aiohttp` was
not audited) -- so this is accurately described as a bounded recent-
activity probe, not a query proven to find everything the unbounded one
would. See tests/test_realtime_freshness_probe.py for a boundary test
that demonstrates (rather than hides) exactly where that bound falls.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

# 4x flusher.py's FLUSH_INTERVAL (300s) -- generous slack for a delayed
# write (e.g. flusher.py's own buffer-and-retry path) while still pruning
# the index scan to a small fraction of the table. See module docstring
# for what this bound does and does not prove.
TS_WINDOW_SECONDS = 1200

FRESHNESS_QUERY = """
    SELECT source, count(*), max(ts), max(created_at)
    FROM realtime_candles
    WHERE ts > :ts_floor
      AND created_at > :restart_ts
    GROUP BY source
"""


def _parse_iso(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def build_query_params(restart_iso: str, window_seconds: int = TS_WINDOW_SECONDS) -> dict[str, str]:
    """Pure function, no DB required -- exercised directly by boundary tests."""
    restart_dt = _parse_iso(restart_iso)
    ts_floor_dt = restart_dt - timedelta(seconds=window_seconds)
    return {
        "restart_ts": restart_iso,
        "ts_floor": ts_floor_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def run_probe(restart_iso: str) -> list[str]:
    """Returns SEEN/UNVERIFIED lines for yahoo and binance, in that order."""
    from db import get_engine
    from sqlalchemy import text

    params = build_query_params(restart_iso)
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(FRESHNESS_QUERY), params).fetchall()
    sources = {r[0]: (r[1], r[2], r[3]) for r in rows}

    lines = []
    for src in ("yahoo", "binance"):
        if src in sources:
            n, latest_ts, latest_created = sources[src]
            lines.append(f"SEEN {src} count={n} latest_candle_ts={latest_ts} latest_row_created_at={latest_created}")
        else:
            lines.append(f"UNVERIFIED {src} -- no row with created_at strictly after restart_ts ({restart_iso})")
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("restart_iso", help="Restart timestamp, e.g. 2026-09-16T19:19:34Z")
    args = parser.parse_args()

    for line in run_probe(args.restart_iso):
        print(line)


if __name__ == "__main__":
    main()
