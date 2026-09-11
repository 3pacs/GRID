"""
GRID conflict resolution module.

Resolves raw_series observations into resolved_series by selecting the
highest-priority source and detecting value conflicts across sources.

Also the entry point for bounded catch-up: ``python -m normalization.resolver
--since 2026-04-04 --chunk-days 7`` walks the backlog in date chunks of
``raw_series.pull_timestamp`` instead of one unbounded scan.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from normalization.entity_map import EntityMap

# Two values are considered conflicting if they differ by more than 0.5%
CONFLICT_THRESHOLD: float = 0.005

# Per-family thresholds for features with different volatility profiles
FAMILY_CONFLICT_THRESHOLDS: dict[str, float] = {
    "vol": 0.02,         # VIX and volatility features: 2%
    "commodity": 0.015,  # Commodities: 1.5%
    "crypto": 0.03,      # Crypto: 3%
    "equity": 0.01,      # Equity indices/ETFs: 1%
    "alternative": 0.05, # Alt data (weather, patents): 5%
    "flows": 0.02,       # Capital flows: 2%
    "systemic": 0.02,    # Systemic risk: 2%
    "trade": 0.02,       # Trade data: 2%
}

# ─── raw_series pull window ──────────────────────────────────────────────
# Three fixed predicates over raw_series.pull_timestamp, selected by
# _pull_window(). They are constants rather than built strings so no value
# is ever interpolated into SQL — the bounds always travel as bound params.

_WINDOW_RELATIVE: str = "rs.pull_timestamp >= NOW() - :lookback * INTERVAL '1 day'"
_WINDOW_FROM: str = "rs.pull_timestamp >= :since"
_WINDOW_FROM_UNTIL: str = "rs.pull_timestamp >= :since AND rs.pull_timestamp < :until"

# Every raw_series read goes through these two statements plus one of the
# window predicates above. raw_series is aliased `rs` in both so a single
# set of predicates fits either.
_DISTINCT_SERIES_SQL: str = (
    "SELECT DISTINCT rs.series_id "
    "FROM raw_series rs "
    "WHERE rs.pull_status = 'SUCCESS' AND "
)
_PARTITION_SQL: str = (
    "SELECT rs.series_id, rs.obs_date, rs.value, "
    "rs.source_id, rs.pull_timestamp, "
    "sc.priority_rank, sc.name AS source_name "
    "FROM raw_series rs "
    "JOIN source_catalog sc ON rs.source_id = sc.id "
    "WHERE rs.series_id = ANY(:sids) "
    "AND rs.pull_status = 'SUCCESS' AND "
)
_PARTITION_ORDER_BY: str = " ORDER BY rs.series_id, rs.obs_date, sc.priority_rank ASC"


def _as_utc(value: date | datetime) -> datetime:
    """Normalise a date or datetime to an aware UTC datetime.

    A bare ``date`` compared against ``pull_timestamp`` (TIMESTAMPTZ) would
    be cast at the server's local midnight, so chunk boundaries would drift
    with the server timezone. Pinning to UTC keeps the window reproducible.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _pull_window(
    lookback_days: int,
    since: date | datetime | None = None,
    until: date | datetime | None = None,
) -> tuple[str, dict[str, Any]]:
    """Return the pull_timestamp predicate to use and its bound params.

    Absolute bounds win over the relative lookback so the catch-up CLI can
    walk fixed date chunks; the per-cycle caller keeps the relative window.

    Args:
        lookback_days: Relative window, used only when ``since`` is None.
        since: Inclusive lower bound on pull_timestamp.
        until: Exclusive upper bound on pull_timestamp. Requires ``since``.

    Returns:
        (sql_predicate, params) where sql_predicate is one of the module
        constants and params carries the bound values.

    Raises:
        ValueError: if ``until`` is given without ``since``.
    """
    if since is None:
        if until is not None:
            raise ValueError("until requires since")
        return _WINDOW_RELATIVE, {"lookback": lookback_days}
    if until is None:
        return _WINDOW_FROM, {"since": _as_utc(since)}
    return _WINDOW_FROM_UNTIL, {"since": _as_utc(since), "until": _as_utc(until)}


def _flush_batch(engine: Engine, batch: list[dict]) -> int:
    """Insert a batch of resolved rows, skipping conflicts. Returns count inserted."""
    if not batch:
        return 0
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO resolved_series "
                    "(feature_id, obs_date, release_date, vintage_date, "
                    "value, source_priority_used, conflict_flag, conflict_detail) "
                    "VALUES (:fid, :od, :rd, :vd, :val, :src, :cf, :cd) "
                    "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING"
                ),
                batch,
            )
        return len(batch)
    except Exception as exc:
        log.error("Batch insert failed ({n} rows): {e}", n=len(batch), e=str(exc))
        return 0


class Resolver:
    """Resolves raw observations into canonical resolved_series rows.

    For each (series_id, obs_date) combination across multiple sources,
    determines the winning value using source priority and flags conflicts
    when values diverge beyond the configured threshold.

    Attributes:
        engine: SQLAlchemy engine for database operations.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the resolver.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        log.info("Resolver initialised")

    # Bulk resolution scans tens of millions of raw_series rows; the
    # default per-statement timeout (120s, see db.get_engine) is too tight
    # for the DISTINCT scan and per-partition fetch. Override locally
    # inside transactions via _set_statement_timeout.
    _RESOLVE_STATEMENT_TIMEOUT_MS: int = 600_000  # 10 minutes

    def _set_statement_timeout(self, conn: Any) -> None:
        """Raise this transaction's statement_timeout for the bulk scans.

        Uses ``set_config(..., is_local => true)`` — equivalent to
        ``SET LOCAL`` but parameterisable. SET's grammar only accepts a
        literal, which would mean interpolating the value into the SQL
        string; set_config takes a bound parameter instead.
        """
        conn.execute(
            text("SELECT set_config('statement_timeout', :timeout_ms, true)"),
            {"timeout_ms": str(self._RESOLVE_STATEMENT_TIMEOUT_MS)},
        )

    def resolve_pending(
        self,
        lookback_days: int = 30,
        workers: int = 8,
        dry_run: bool = False,
        since: date | datetime | None = None,
        until: date | datetime | None = None,
    ) -> dict[str, Any]:
        """Resolve raw_series → resolved_series using multithreaded workers.

        Fetches distinct series_ids with pending data, partitions them across
        worker threads, and each worker resolves its partition independently
        with batched inserts.

        Args:
            lookback_days: Only process raw rows pulled within this window.
                Ignored when ``since`` is given.
            workers: Number of concurrent resolver threads.
            dry_run: Run every phase — distinct-series fetch, per-partition
                fetch, grouping and conflict detection — but skip the
                resolved_series writes. Use it to measure what a window
                costs before trusting it in the per-cycle hot loop.
            since: Inclusive lower bound on pull_timestamp (absolute window).
            until: Exclusive upper bound on pull_timestamp. Requires ``since``.

        Returns:
            dict carrying the historical counts (``resolved``,
            ``conflicts_found``, ``errors``), the volume observed at each
            phase (``series_count``, ``raw_rows``, ``groups``,
            ``unmapped_groups``, ``candidates``), a ``dry_run`` flag and a
            ``timings`` dict of per-phase seconds. Under dry_run
            ``resolved`` is 0 and ``candidates`` is what would have been
            written.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        window_sql, window_params = _pull_window(lookback_days, since, until)

        log.info(
            "Starting multithreaded resolution (workers={w}, window={q}, dry_run={d})",
            w=workers, q=window_params, d=dry_run,
        )

        started = time.perf_counter()
        timings: dict[str, float] = {}
        counts: dict[str, int] = {
            "resolved": 0,
            "conflicts_found": 0,
            "errors": 0,
            "series_count": 0,
            "raw_rows": 0,
            "groups": 0,
            "unmapped_groups": 0,
            "candidates": 0,
        }

        def _summary() -> dict[str, Any]:
            timings["total_s"] = round(time.perf_counter() - started, 3)
            summary: dict[str, Any] = dict(counts)
            summary["dry_run"] = dry_run
            summary["timings"] = timings
            return summary

        # Pre-load entity map and feature families (shared, read-only)
        phase = time.perf_counter()
        try:
            entity_map = EntityMap(self.engine)
        except Exception as exc:
            log.error("Failed to load entity map: {e}", e=str(exc))
            counts["errors"] += 1
            timings["entity_map_s"] = round(time.perf_counter() - phase, 3)
            return _summary()
        timings["entity_map_s"] = round(time.perf_counter() - phase, 3)

        phase = time.perf_counter()
        feature_families: dict[int, str] = {}
        try:
            with self.engine.connect() as conn:
                fam_rows = conn.execute(
                    text("SELECT id, family FROM feature_registry")
                ).fetchall()
                feature_families = {row[0]: row[1] for row in fam_rows}
        except Exception as exc:
            log.warning("Could not load feature families: {e}", e=str(exc))
        timings["feature_families_s"] = round(time.perf_counter() - phase, 3)

        # Fetch distinct series_ids in the window. Wrapped in a transaction
        # so the statement_timeout override applies; the global 120s default
        # is too short for this DISTINCT scan once raw_series grows past a
        # few million rows.
        log.info("Fetching distinct series_ids...")
        phase = time.perf_counter()
        with self.engine.begin() as conn:
            self._set_statement_timeout(conn)
            series_rows = conn.execute(
                text(_DISTINCT_SERIES_SQL + window_sql), window_params
            ).fetchall()
        timings["distinct_series_s"] = round(time.perf_counter() - phase, 3)

        all_series = [r[0] for r in series_rows]
        counts["series_count"] = len(all_series)
        log.info(
            "Found {n} distinct series_ids to resolve ({t}s)",
            n=len(all_series), t=timings["distinct_series_s"],
        )

        if not all_series:
            log.info("No pending observations to resolve")
            return _summary()

        # Partition series_ids across workers
        chunk_size = max(1, len(all_series) // workers)
        partitions = [
            all_series[i:i + chunk_size]
            for i in range(0, len(all_series), chunk_size)
        ]

        # Counters and per-phase worker timings (thread-safe)
        lock = threading.Lock()
        worker_timings = {"fetch_s": 0.0, "group_s": 0.0, "flush_s": 0.0}

        def _resolve_partition(partition: list[str], worker_id: int) -> dict[str, int]:
            """Resolve a partition of series_ids."""
            local = dict.fromkeys(counts, 0)
            local_t = {"fetch_s": 0.0, "group_s": 0.0, "flush_s": 0.0}
            INSERT_BATCH = 500

            def _flush(batch: list[dict]) -> None:
                """Account for a prepared batch, writing it unless dry_run."""
                if not batch:
                    return
                local["candidates"] += len(batch)
                if dry_run:
                    return
                mark = time.perf_counter()
                local["resolved"] += _flush_batch(self.engine, batch)
                local_t["flush_s"] += time.perf_counter() - mark

            try:
                mark = time.perf_counter()
                with self.engine.begin() as conn:
                    self._set_statement_timeout(conn)
                    rows = conn.execute(
                        text(_PARTITION_SQL + window_sql + _PARTITION_ORDER_BY),
                        {"sids": partition, **window_params},
                    ).fetchall()
                local_t["fetch_s"] += time.perf_counter() - mark
                local["raw_rows"] += len(rows)

                mark = time.perf_counter()

                # Group by (series_id, obs_date)
                groups: dict[tuple[str, Any], list[dict]] = {}
                for row in rows:
                    key = (row[0], row[1])
                    if key not in groups:
                        groups[key] = []
                    groups[key].append({
                        "value": row[2], "source_id": row[3],
                        "pull_timestamp": row[4], "priority_rank": row[5],
                        "source_name": row[6],
                    })
                local["groups"] += len(groups)

                # Resolve and batch insert
                insert_batch: list[dict] = []

                for (series_id, obs_date_val), sources in groups.items():
                    feature_id = entity_map.get_feature_id(series_id)
                    if feature_id is None:
                        local["unmapped_groups"] += 1
                        continue

                    sources.sort(key=lambda s: s["priority_rank"])
                    winner = sources[0]

                    family = feature_families.get(feature_id, "")
                    threshold = FAMILY_CONFLICT_THRESHOLDS.get(family, CONFLICT_THRESHOLD)

                    conflict_flag = False
                    conflict_detail = None

                    if len(sources) > 1:
                        ref_val = winner["value"]
                        for s in sources[1:]:
                            if ref_val != 0:
                                pct_diff = abs(s["value"] - ref_val) / abs(ref_val)
                            else:
                                pct_diff = float("inf") if s["value"] != 0 else 0.0
                            if pct_diff > threshold:
                                conflict_flag = True
                                break

                        if conflict_flag:
                            conflict_detail = json.dumps({
                                "sources": [
                                    {"source_name": s["source_name"],
                                     "source_id": s["source_id"],
                                     "value": s["value"],
                                     "priority_rank": s["priority_rank"]}
                                    for s in sources
                                ],
                                "threshold": threshold, "family": family,
                            })
                            local["conflicts_found"] += 1

                    release_dt = (winner["pull_timestamp"].date()
                                  if hasattr(winner["pull_timestamp"], "date")
                                  else winner["pull_timestamp"])

                    insert_batch.append({
                        "fid": feature_id, "od": obs_date_val,
                        "rd": release_dt, "vd": release_dt,
                        "val": winner["value"], "src": winner["source_id"],
                        "cf": conflict_flag, "cd": conflict_detail,
                    })

                    if len(insert_batch) >= INSERT_BATCH:
                        local_t["group_s"] += time.perf_counter() - mark
                        _flush(insert_batch)
                        insert_batch = []
                        mark = time.perf_counter()

                local_t["group_s"] += time.perf_counter() - mark

                # Flush remaining
                _flush(insert_batch)

                log.info("Worker {w}: resolved={r}, candidates={n}, conflicts={c}",
                         w=worker_id, r=local["resolved"],
                         n=local["candidates"], c=local["conflicts_found"])

            except Exception as exc:
                log.error("Worker {w} failed: {e}", w=worker_id, e=str(exc))
                local["errors"] += 1

            with lock:
                for k in counts:
                    counts[k] += local[k]
                for k in worker_timings:
                    worker_timings[k] += local_t[k]

            return local

        # Launch workers
        phase = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_resolve_partition, part, i): i
                for i, part in enumerate(partitions)
            }
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    log.error("Worker {w} raised: {e}", w=worker_id, e=str(exc))
                    counts["errors"] += 1
        timings["workers_wall_s"] = round(time.perf_counter() - phase, 3)
        # Summed across workers, so these can exceed workers_wall_s.
        for key, value in worker_timings.items():
            timings[key] = round(value, 3)

        summary = _summary()
        log.info(
            "Resolution complete — resolved={r}, candidates={n}, conflicts={c}, "
            "errors={e}, raw_rows={rr} in {t}s",
            r=counts["resolved"], n=counts["candidates"],
            c=counts["conflicts_found"], e=counts["errors"],
            rr=counts["raw_rows"], t=timings["total_s"],
        )
        return summary

    def resolve_backlog(
        self,
        since: date | datetime,
        until: date | datetime | None = None,
        chunk_days: int = 7,
        workers: int = 8,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Resolve a historical backlog in bounded pull_timestamp chunks.

        One unbounded scan over years of raw_series is the reason the old
        catch-up path was never run; walking fixed date chunks keeps each
        statement inside the timeout and makes progress observable and
        restartable. Idempotent — repeated runs hit
        ``ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING``.

        Args:
            since: Inclusive lower bound on pull_timestamp.
            until: Exclusive upper bound. Defaults to tomorrow (UTC) so
                everything pulled so far is covered.
            chunk_days: Width of each chunk in days (minimum 1).
            workers: Resolver threads per chunk.
            dry_run: Measure without writing (see resolve_pending).

        Returns:
            dict with the window, one summary per chunk, and summed totals.
        """
        lower = _as_utc(since)
        upper = (
            _as_utc(until) if until is not None
            else _as_utc(datetime.now(timezone.utc).date() + timedelta(days=1))
        )
        if upper <= lower:
            raise ValueError(f"until ({upper}) must be after since ({lower})")
        width = timedelta(days=max(1, int(chunk_days)))

        bounds: list[tuple[datetime, datetime]] = []
        cursor = lower
        while cursor < upper:
            nxt = min(cursor + width, upper)
            bounds.append((cursor, nxt))
            cursor = nxt

        log.info(
            "Backlog resolution: {n} chunk(s) of {d}d from {a} to {b} (dry_run={dr})",
            n=len(bounds), d=width.days, a=lower.date(), b=upper.date(), dr=dry_run,
        )

        totals: dict[str, int] = {
            "resolved": 0,
            "conflicts_found": 0,
            "errors": 0,
            "series_count": 0,
            "raw_rows": 0,
            "groups": 0,
            "unmapped_groups": 0,
            "candidates": 0,
        }
        chunks: list[dict[str, Any]] = []
        started = time.perf_counter()

        for index, (chunk_from, chunk_to) in enumerate(bounds, start=1):
            summary = self.resolve_pending(
                workers=workers,
                dry_run=dry_run,
                since=chunk_from,
                until=chunk_to,
            )
            for key in totals:
                totals[key] += int(summary.get(key, 0))
            record = {
                "chunk": index,
                "since": chunk_from.date().isoformat(),
                "until": chunk_to.date().isoformat(),
                **summary,
            }
            chunks.append(record)
            log.info(
                "Chunk {i}/{n} [{a} → {b}): resolved={r}, candidates={c}, "
                "series={s}, raw_rows={rr}, errors={e}, {t}s",
                i=index, n=len(bounds), a=record["since"], b=record["until"],
                r=summary["resolved"], c=summary["candidates"],
                s=summary["series_count"], rr=summary["raw_rows"],
                e=summary["errors"], t=summary["timings"]["total_s"],
            )

        elapsed = round(time.perf_counter() - started, 3)
        log.info(
            "Backlog complete — resolved={r}, candidates={c}, errors={e} "
            "across {n} chunk(s) in {t}s",
            r=totals["resolved"], c=totals["candidates"], e=totals["errors"],
            n=len(bounds), t=elapsed,
        )
        return {
            "since": lower.date().isoformat(),
            "until": upper.date().isoformat(),
            "chunk_days": width.days,
            "workers": workers,
            "dry_run": dry_run,
            "chunks": chunks,
            "totals": totals,
            "elapsed_s": elapsed,
        }

    def get_conflict_report(self) -> pd.DataFrame:
        """Return all conflicted resolved_series rows with feature and source names.

        Returns:
            pd.DataFrame: DataFrame with columns including feature name,
                          source name, obs_date, value, and conflict details.
        """
        query = text("""
            SELECT
                rs.id,
                fr.name AS feature_name,
                rs.obs_date,
                rs.value,
                sc.name AS source_name,
                rs.conflict_detail,
                rs.release_date,
                rs.vintage_date
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            JOIN source_catalog sc ON rs.source_priority_used = sc.id
            WHERE rs.conflict_flag = TRUE
            ORDER BY rs.obs_date DESC
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(query).fetchall()

        df = pd.DataFrame(
            rows,
            columns=[
                "id", "feature_name", "obs_date", "value",
                "source_name", "conflict_detail", "release_date", "vintage_date",
            ],
        )
        log.info("Conflict report generated — {n} rows", n=len(df))
        return df


# ─── CLI ─────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for ``python -m normalization.resolver``."""
    parser = argparse.ArgumentParser(
        prog="python -m normalization.resolver",
        description=(
            "Resolve raw_series into resolved_series. With no arguments this "
            "runs the default rolling window and prints the conflict report; "
            "with --since it walks a historical backlog in date chunks."
        ),
    )
    parser.add_argument(
        "--since", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
        help="Resolve the backlog from this pull_timestamp date (inclusive).",
    )
    parser.add_argument(
        "--until", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
        help="Stop before this pull_timestamp date (exclusive). "
             "Defaults to tomorrow (UTC). Requires --since.",
    )
    parser.add_argument(
        "--chunk-days", type=int, default=7, metavar="N",
        help="Width of each backlog chunk in days (default: 7).",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=30, metavar="N",
        help="Rolling window when --since is not given (default: 30).",
    )
    parser.add_argument(
        "--workers", type=int, default=8, metavar="N",
        help="Resolver threads (default: 8).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run every phase but skip the resolved_series writes, "
             "reporting the counts and per-phase timings only.",
    )
    parser.add_argument(
        "--no-conflict-report", action="store_true",
        help="Skip the trailing conflict report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    args = build_parser().parse_args(argv)

    if args.until is not None and args.since is None:
        log.error("--until requires --since")
        return 2
    if args.chunk_days < 1:
        log.error("--chunk-days must be >= 1")
        return 2
    if args.workers < 1:
        log.error("--workers must be >= 1")
        return 2

    from db import get_engine

    resolver = Resolver(db_engine=get_engine())

    if args.since is not None:
        result = resolver.resolve_backlog(
            since=args.since,
            until=args.until,
            chunk_days=args.chunk_days,
            workers=args.workers,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2, default=str))
        return 1 if result["totals"]["errors"] else 0

    summary = resolver.resolve_pending(
        lookback_days=args.lookback_days,
        workers=args.workers,
        dry_run=args.dry_run,
    )
    print(f"Resolution summary: {json.dumps(summary, default=str)}")

    if not args.no_conflict_report:
        report = resolver.get_conflict_report()
        if not report.empty:
            print(f"\nConflicts found: {len(report)}")
            print(report.head(10))
        else:
            print("\nNo conflicts found")

    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
