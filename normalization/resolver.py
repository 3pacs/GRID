"""
GRID conflict resolution module.

Resolves raw_series observations into resolved_series by selecting the
highest-priority source and detecting value conflicts across sources.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from normalization.entity_map import EntityMap

# Default window for manual/CLI runs. The Hermes cycle passes a much smaller
# window (see scripts/hermes_operator.py::RESOLUTION_CYCLE_LOOKBACK_DAYS) plus
# a watermark, because it runs every 5 minutes and cannot afford a 30-day scan.
DEFAULT_LOOKBACK_DAYS: int = 30

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


def _as_datetime(value: datetime | date) -> datetime:
    """Normalise a date or datetime bound to a datetime."""
    if isinstance(value, datetime):
        return value
    return datetime(value.year, value.month, value.day)


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
    # inside transactions via SET LOCAL.
    _RESOLVE_STATEMENT_TIMEOUT_MS: int = 600_000  # 10 minutes

    def resolve_pending(
        self,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        workers: int = 8,
        since: datetime | date | None = None,
        until: datetime | date | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Resolve raw_series → resolved_series using multithreaded workers.

        Fetches distinct series_ids with pending data, partitions them across
        worker threads, and each worker resolves its partition independently
        with batched inserts.

        The raw_series window is ``pull_timestamp >= since`` when a watermark
        is supplied, otherwise ``pull_timestamp >= NOW() - lookback_days``.
        Callers on a tight budget (the 5-minute Hermes cycle) pass a small
        window and/or a watermark; manual/backfill runs keep the 30-day
        default. Both bounds are bound parameters — never interpolated.

        Args:
            lookback_days: Fallback window when ``since`` is not supplied.
            workers: Number of concurrent resolver threads.
            since: Absolute lower bound on ``raw_series.pull_timestamp``
                (a persisted watermark, usually minus an overlap margin).
                Overrides ``lookback_days`` when given.
            until: Optional exclusive upper bound, used to chunk backfills.
            dry_run: Run the SELECT + grouping + conflict detection but skip
                every INSERT. ``resolved`` then counts rows that *would* be
                written. Used to measure cost before changing the cycle.

        Returns:
            dict with resolved, conflicts_found, errors, series_scanned,
            duration_s and dry_run.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        started_at = time.monotonic()
        log.info(
            "Starting resolution (workers={w}, lookback={d}d, since={s}, "
            "until={u}, dry_run={dr})",
            w=workers, d=lookback_days, s=since, u=until, dr=dry_run,
        )

        # Window bounds are passed to both statements below as bound
        # parameters; CAST(... AS timestamptz) lets PostgreSQL type the
        # NULL case without any string building.
        window_params: dict[str, Any] = {
            "lookback": lookback_days,
            "since": since,
            "until": until,
        }

        def _summary(
            resolved: int = 0,
            conflicts: int = 0,
            errors: int = 0,
            series: int = 0,
        ) -> dict[str, Any]:
            return {
                "resolved": resolved,
                "conflicts_found": conflicts,
                "errors": errors,
                "series_scanned": series,
                "duration_s": round(time.monotonic() - started_at, 2),
                "dry_run": dry_run,
            }

        # Pre-load entity map and feature families (shared, read-only)
        try:
            entity_map = EntityMap(self.engine)
        except Exception as exc:
            log.error("Failed to load entity map: {e}", e=str(exc))
            return _summary(errors=1)
        feature_families: dict[int, str] = {}
        try:
            with self.engine.connect() as conn:
                fam_rows = conn.execute(
                    text("SELECT id, family FROM feature_registry")
                ).fetchall()
                feature_families = {row[0]: row[1] for row in fam_rows}
        except Exception as exc:
            log.warning("Could not load feature families: {e}", e=str(exc))

        # Fetch distinct series_ids with recent data. Wrap in a transaction
        # so SET LOCAL applies; the global 120s timeout is too short for
        # this DISTINCT scan once raw_series grows past a few million rows.
        log.info("Fetching distinct series_ids...")
        with self.engine.begin() as conn:
            conn.execute(
                text(f"SET LOCAL statement_timeout = {self._RESOLVE_STATEMENT_TIMEOUT_MS}")
            )
            series_rows = conn.execute(text("""
                SELECT DISTINCT rs.series_id
                FROM raw_series rs
                WHERE rs.pull_status = 'SUCCESS'
                  AND rs.pull_timestamp >= COALESCE(
                        CAST(:since AS timestamptz),
                        NOW() - :lookback * INTERVAL '1 day')
                  AND (CAST(:until AS timestamptz) IS NULL
                       OR rs.pull_timestamp < CAST(:until AS timestamptz))
            """), window_params).fetchall()

        all_series = [r[0] for r in series_rows]
        log.info("Found {n} distinct series_ids to resolve", n=len(all_series))

        if not all_series:
            log.info("No pending observations to resolve")
            return _summary()

        # Partition series_ids across workers
        chunk_size = max(1, len(all_series) // workers)
        partitions = [
            all_series[i:i + chunk_size]
            for i in range(0, len(all_series), chunk_size)
        ]

        # Counters (thread-safe)
        lock = threading.Lock()
        totals = {"resolved": 0, "conflicts_found": 0, "errors": 0}

        def _flush(batch: list[dict]) -> int:
            """Persist a batch, or just count it when dry-running."""
            if dry_run:
                return len(batch)
            return _flush_batch(self.engine, batch)

        def _resolve_partition(partition: list[str], worker_id: int) -> dict[str, int]:
            """Resolve a partition of series_ids."""
            local = {"resolved": 0, "conflicts_found": 0, "errors": 0}
            INSERT_BATCH = 500

            try:
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"SET LOCAL statement_timeout = {self._RESOLVE_STATEMENT_TIMEOUT_MS}"
                        )
                    )
                    rows = conn.execute(text("""
                        SELECT rs.series_id, rs.obs_date, rs.value,
                               rs.source_id, rs.pull_timestamp,
                               sc.priority_rank, sc.name AS source_name
                        FROM raw_series rs
                        JOIN source_catalog sc ON rs.source_id = sc.id
                        WHERE rs.series_id = ANY(:sids)
                          AND rs.pull_status = 'SUCCESS'
                          AND rs.pull_timestamp >= COALESCE(
                                CAST(:since AS timestamptz),
                                NOW() - :lookback * INTERVAL '1 day')
                          AND (CAST(:until AS timestamptz) IS NULL
                               OR rs.pull_timestamp < CAST(:until AS timestamptz))
                        ORDER BY rs.series_id, rs.obs_date, sc.priority_rank ASC
                    """), {"sids": partition, **window_params}).fetchall()

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

                # Resolve and batch insert
                insert_batch: list[dict] = []

                for (series_id, obs_date_val), sources in groups.items():
                    feature_id = entity_map.get_feature_id(series_id)
                    if feature_id is None:
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
                        local["resolved"] += _flush(insert_batch)
                        insert_batch = []

                # Flush remaining
                if insert_batch:
                    local["resolved"] += _flush(insert_batch)

                log.info("Worker {w}: resolved={r}, conflicts={c}{d}",
                         w=worker_id, r=local["resolved"],
                         c=local["conflicts_found"],
                         d=" (dry run — nothing written)" if dry_run else "")

            except Exception as exc:
                log.error("Worker {w} failed: {e}", w=worker_id, e=str(exc))
                local["errors"] += 1

            with lock:
                for k in totals:
                    totals[k] += local[k]

            return local

        # Launch workers
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
                    totals["errors"] += 1

        summary = _summary(
            resolved=totals["resolved"],
            conflicts=totals["conflicts_found"],
            errors=totals["errors"],
            series=len(all_series),
        )
        log.info(
            "Resolution complete — resolved={r}, conflicts={c}, errors={e}, "
            "series={s}, {t}s{d}",
            r=summary["resolved"], c=summary["conflicts_found"],
            e=summary["errors"], s=summary["series_scanned"],
            t=summary["duration_s"],
            d=" (dry run — nothing written)" if dry_run else "",
        )
        return summary

    def resolve_range(
        self,
        since: datetime | date,
        until: datetime | date | None = None,
        chunk_days: int = 7,
        workers: int = 8,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Resolve a historical pull_timestamp range in bounded chunks.

        Catch-up entry point: a single 5-month window would hold one
        transaction open for hours, so the range is walked in
        ``chunk_days``-wide slices and each slice is resolved independently.
        A failed slice is counted and the walk continues.

        Args:
            since: Inclusive lower bound on ``raw_series.pull_timestamp``.
            until: Exclusive upper bound. Defaults to now.
            chunk_days: Width of each slice in days.
            workers: Concurrent resolver threads per slice.
            dry_run: Measure without writing (see ``resolve_pending``).

        Returns:
            dict with the same keys as ``resolve_pending`` plus ``chunks``.
        """
        if chunk_days < 1:
            raise ValueError("chunk_days must be >= 1")

        start = _as_datetime(since)
        end = _as_datetime(until) if until is not None else datetime.now(start.tzinfo)
        if end <= start:
            raise ValueError("until must be after since")

        totals: dict[str, Any] = {
            "resolved": 0, "conflicts_found": 0, "errors": 0,
            "series_scanned": 0, "duration_s": 0.0,
            "dry_run": dry_run, "chunks": 0,
        }
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + timedelta(days=chunk_days), end)
            log.info("Resolving chunk {a} → {b}", a=cursor, b=chunk_end)
            result = self.resolve_pending(
                workers=workers,
                since=cursor,
                until=chunk_end,
                dry_run=dry_run,
            )
            for key in ("resolved", "conflicts_found", "errors", "series_scanned"):
                totals[key] += result[key]
            totals["duration_s"] = round(totals["duration_s"] + result["duration_s"], 2)
            totals["chunks"] += 1
            cursor = chunk_end

        log.info(
            "Range resolution complete — chunks={n}, resolved={r}, errors={e}, {t}s",
            n=totals["chunks"], r=totals["resolved"],
            e=totals["errors"], t=totals["duration_s"],
        )
        return totals

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


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: ad-hoc resolution, bounded backfills, dry runs."""
    parser = argparse.ArgumentParser(
        description="Resolve raw_series → resolved_series (PIT conflict resolution)",
    )
    parser.add_argument(
        "--since",
        help="Resolve raw rows pulled on/after this date (YYYY-MM-DD). "
             "Walks the range in --chunk-days slices.",
    )
    parser.add_argument(
        "--until",
        help="Exclusive upper bound (YYYY-MM-DD). Defaults to now.",
    )
    parser.add_argument(
        "--chunk-days", type=int, default=7,
        help="Chunk width for --since backfills (default: 7).",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"Window when --since is absent (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument("--workers", type=int, default=8, help="Resolver threads.")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Select and group without inserting; reports what would be written.",
    )
    parser.add_argument(
        "--conflict-report", action="store_true",
        help="Print the conflict report after resolving.",
    )
    args = parser.parse_args(argv)

    from db import get_engine

    resolver = Resolver(db_engine=get_engine())
    if args.since:
        summary = resolver.resolve_range(
            since=date.fromisoformat(args.since),
            until=date.fromisoformat(args.until) if args.until else None,
            chunk_days=args.chunk_days,
            workers=args.workers,
            dry_run=args.dry_run,
        )
    else:
        summary = resolver.resolve_pending(
            lookback_days=args.lookback_days,
            workers=args.workers,
            dry_run=args.dry_run,
        )
    print(f"Resolution summary: {summary}")

    if args.conflict_report:
        report = resolver.get_conflict_report()
        if not report.empty:
            print(f"\nConflicts found: {len(report)}")
            print(report.head(10))
        else:
            print("\nNo conflicts found")
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
