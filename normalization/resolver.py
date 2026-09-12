"""
GRID conflict resolution module.

Resolves raw_series observations into resolved_series by selecting the
highest-priority source and detecting value conflicts across sources.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
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

# Backfill chunk width. Chunking bounds each transaction and isolates a
# failure to one slice; it is not a performance knob. The cold-heap-read
# theory it was originally sized against was wrong: before _window_bounds,
# a 7-day chunk (ops-exec run 34551047779) and a 1-day chunk (run
# 34619633738) both died at the same 600s statement timeout, because the
# window never reached the planner as an index bound and every chunk
# scanned the whole table regardless of its width.
DEFAULT_BACKFILL_CHUNK_DAYS: int = 1

# Floor for adaptive chunk narrowing. A chunk that outgrows the statement
# timeout is halved and retried rather than skipped; this is where splitting
# stops being the answer. Measured 2026-09-11: with the window reaching the
# planner as an index bound, most days resolve in ~8 min, but heavy days
# (2026-04-05 was the first) still exceed the 600s statement timeout.
MIN_BACKFILL_CHUNK_SECONDS: int = 3600  # one hour


def _is_statement_timeout(exc: BaseException) -> bool:
    """True when a failure is the statement timeout that narrowing can fix.

    Only a window too large to finish inside ``statement_timeout`` is helped
    by a smaller window. A schema error or a dropped connection repeats at
    every width, so narrowing those would burn the whole split budget on a
    failure that was never about size.
    """
    blob = f"{type(exc).__name__} {exc}".lower().replace("_", "")
    return (
        "querycanceled" in blob
        or "statementtimeout" in blob
        or "canceling statement due to statement timeout" in blob
    )


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


def _window_bounds(
    since: datetime | None,
    until: datetime | None,
    lookback_days: int,
) -> dict[str, Any]:
    """Resolve the pull_timestamp window into two plain bound parameters.

    Both statements that scan ``raw_series`` compare ``pull_timestamp``
    directly against ``:since`` and ``:until`` so the planner can use them
    as index bounds. They used to carry the window inline instead::

        rs.pull_timestamp >= COALESCE(CAST(:since AS timestamptz),
                                      NOW() - :lookback * INTERVAL '1 day')
        AND (CAST(:until AS timestamptz) IS NULL
             OR rs.pull_timestamp < CAST(:until AS timestamptz))

    psycopg2 renders a naive datetime as ``timestamp without time zone``,
    and ``timestamp -> timestamptz`` is STABLE — its result depends on the
    session TimeZone — so it is not folded to a constant at plan time.
    Wrapped in COALESCE and in the ``IS NULL OR`` disjunction, that left
    the planner no usable bound on the backfill path: it chose a full
    sequential scan of ``raw_series`` (~1.93B rows) for every chunk, no
    matter how narrow. Chunk width never entered the cost, which is why a
    1-day chunk and a 7-day chunk both died at the same 600s statement
    timeout (ops-exec runs 34551047779 and 34619633738).

    The rolling cycle never showed the fault. With both parameters NULL
    the disjunction folds away and the lower bound becomes
    ``now() - interval``, which the planner does use as an index bound —
    hence 2.2s for the live 2-day window against 600s for every backfill
    chunk.

    ``until`` stays optional: ``COALESCE(:until, 'infinity'::timestamptz)``
    in the SQL keeps the upper end unbounded when it is None, without the
    ``IS NULL OR`` that defeated the index. Both forms were checked on a
    ``raw_series``-shaped table and both bounds land in ``Index Cond``.

    A caller-supplied ``since`` is passed through untouched, so an
    operator's ``--since 2026-04-04`` keeps being read in the session's
    time zone exactly as before. Only the default is made explicit, and it
    is UTC-aware so the rolling window stays anchored to an absolute
    instant the way the server-side ``NOW()`` it replaces was.
    """
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    return {"since": since, "until": until}


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
    # for the DISTINCT scan and per-partition fetch. Raised per transaction
    # by _set_statement_timeout.
    _RESOLVE_STATEMENT_TIMEOUT_MS: int = 600_000  # 10 minutes

    def _set_statement_timeout(self, conn: Any) -> None:
        """Raise this transaction's statement_timeout for the bulk scans.

        ``set_config(..., is_local => true)`` is ``SET LOCAL`` with a bound
        parameter. Plain ``SET`` only accepts a literal, so it would force
        the value into the SQL string — which is what .claude/rules
        forbids, and what this replaced.
        """
        conn.execute(
            text("SELECT set_config('statement_timeout', :timeout_ms, true)"),
            {"timeout_ms": str(self._RESOLVE_STATEMENT_TIMEOUT_MS)},
        )

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
            duration_s, dry_run and unmapped (the EntityMap miss summary —
            see EntityMap.missing_feature_report). Every return path carries
            the same keys.
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
        # parameters, as plain comparisons the planner can use as index
        # bounds. See _window_bounds for what the previous inline form
        # cost every backfill chunk.
        window_params: dict[str, Any] = _window_bounds(
            since, until, lookback_days
        )

        # Shape of "nothing was missed", so every return path below carries
        # the same keys whether or not an EntityMap was ever constructed.
        empty_unmapped: dict[str, Any] = {
            "lookups_missed": 0, "series_ids": 0,
            "unregistered_features": [], "top_series": [],
        }

        def _summary(
            resolved: int = 0,
            conflicts: int = 0,
            errors: int = 0,
            series: int = 0,
            unmapped: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return {
                "resolved": resolved,
                "conflicts_found": conflicts,
                "errors": errors,
                "series_scanned": series,
                "duration_s": round(time.monotonic() - started_at, 2),
                "dry_run": dry_run,
                "unmapped": unmapped if unmapped is not None else dict(empty_unmapped),
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
            self._set_statement_timeout(conn)
            series_rows = conn.execute(text("""
                SELECT DISTINCT rs.series_id
                FROM raw_series rs
                WHERE rs.pull_status = 'SUCCESS'
                  AND rs.pull_timestamp >= :since
                  AND rs.pull_timestamp < COALESCE(
                        :until, 'infinity'::timestamptz)
            """), window_params).fetchall()

        all_series = [r[0] for r in series_rows]
        log.info("Found {n} distinct series_ids to resolve", n=len(all_series))

        if not all_series:
            log.info("No pending observations to resolve")
            return _summary(unmapped=entity_map.missing_feature_report())

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

            # Memoise series_id -> feature_id for this partition. The loop
            # below runs once per (series_id, obs_date) group, and a live
            # 2-day window produced 569,400 groups over 18,916 distinct
            # series_ids — ~30 identical lookups each. Partitions are
            # disjoint by series_id, so a plain dict per worker is
            # lock-free and shares nothing.
            feature_ids: dict[str, int | None] = {}

            def _feature_id(series_id: str) -> int | None:
                """entity_map.get_feature_id, resolved once per series_id."""
                if series_id not in feature_ids:
                    feature_ids[series_id] = entity_map.get_feature_id(series_id)
                return feature_ids[series_id]

            try:
                with self.engine.begin() as conn:
                    self._set_statement_timeout(conn)
                    rows = conn.execute(text("""
                        SELECT rs.series_id, rs.obs_date, rs.value,
                               rs.source_id, rs.pull_timestamp,
                               sc.priority_rank, sc.name AS source_name
                        FROM raw_series rs
                        JOIN source_catalog sc ON rs.source_id = sc.id
                        WHERE rs.series_id = ANY(:sids)
                          AND rs.pull_status = 'SUCCESS'
                          AND rs.pull_timestamp >= :since
                          AND rs.pull_timestamp < COALESCE(
                                :until, 'infinity'::timestamptz)
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
                    feature_id = _feature_id(series_id)
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

        # One line for the whole run instead of one warning per group:
        # EntityMap counts its misses and reports them here.
        unmapped = entity_map.missing_feature_report()
        if unmapped["lookups_missed"]:
            log.warning(
                "Resolution skipped {n} lookup(s) across {s} unmapped "
                "series_id(s); {u} mapped feature name(s) are missing from "
                "feature_registry: {names}. Worst offenders: {top}",
                n=unmapped["lookups_missed"], s=unmapped["series_ids"],
                u=len(unmapped["unregistered_features"]),
                names=unmapped["unregistered_features"][:10],
                top=unmapped["top_series"],
            )

        summary = _summary(
            resolved=totals["resolved"],
            conflicts=totals["conflicts_found"],
            errors=totals["errors"],
            series=len(all_series),
            unmapped=unmapped,
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

    def _resolve_window(
        self,
        since: datetime,
        until: datetime,
        workers: int,
        dry_run: bool,
        totals: dict[str, Any],
    ) -> None:
        """Resolve one window, halving it and retrying on a statement timeout.

        A window that outgrows ``statement_timeout`` used to be recorded in
        ``failed_ranges`` and skipped, and nothing ever went back for it — so
        a backfill reported success having silently dropped every day too
        heavy to finish. Observed live on 2026-09-11: the very second chunk of
        the April→September walk (2026-04-05) died at the 600s timeout and the
        walk moved on as if it had not.

        Narrowing is the fix rather than a bigger timeout because it is
        self-correcting: whatever the true cost of a day, halving reaches a
        width that fits, and a day that is merely twice as heavy as its
        neighbour costs one extra attempt instead of a raised ceiling for
        everything. Splitting stops at ``MIN_BACKFILL_CHUNK_SECONDS``, below
        which a timeout is no longer about window size and is recorded for a
        human.

        Only a timeout is narrowed (see ``_is_statement_timeout``); any other
        failure repeats at every width, so it is recorded once and the walk
        continues.

        Accumulates into ``totals`` in place; returns nothing.
        """
        span_s = (until - since).total_seconds()
        chunk_t0 = time.monotonic()
        log.info("Resolving chunk {a} → {b}", a=since, b=until)
        try:
            result = self.resolve_pending(
                workers=workers, since=since, until=until, dry_run=dry_run,
            )
        except Exception as exc:
            elapsed = round(time.monotonic() - chunk_t0, 2)
            totals["duration_s"] = round(totals["duration_s"] + elapsed, 2)
            if _is_statement_timeout(exc) and span_s > MIN_BACKFILL_CHUNK_SECONDS:
                midpoint = since + timedelta(seconds=span_s / 2)
                log.warning(
                    "Chunk {a} → {b} hit the statement timeout after {e}s — "
                    "narrowing to two {h:.1f}h halves and retrying",
                    a=since, b=until, e=elapsed, h=span_s / 7200,
                )
                totals["chunks_narrowed"] += 1
                self._resolve_window(since, midpoint, workers, dry_run, totals)
                self._resolve_window(midpoint, until, workers, dry_run, totals)
                return
            # Warning, not error: a window that cannot be narrowed further is
            # an operational problem to look at, not a crash to propagate.
            log.warning(
                "Chunk {a} → {b} failed ({c}) after {e}s: {m} — continuing",
                a=since, b=until, c=type(exc).__name__, e=elapsed, m=str(exc),
            )
            totals["errors"] += 1
            totals["chunks"] += 1
            totals["chunks_failed"] += 1
            totals["failed_ranges"].append({
                "since": since.isoformat(),
                "until": until.isoformat(),
                "error": str(exc),
                "error_class": type(exc).__name__,
            })
            return

        for key in ("resolved", "conflicts_found", "errors", "series_scanned"):
            totals[key] += result[key]
        totals["duration_s"] = round(totals["duration_s"] + result["duration_s"], 2)
        totals["chunks"] += 1
        if result["errors"]:
            # A worker that failed inside resolve_pending never raises out of
            # it, so this range is incomplete too — same retry.
            totals["chunks_failed"] += 1
            totals["failed_ranges"].append({
                "since": since.isoformat(),
                "until": until.isoformat(),
                "error": f"{result['errors']} worker error(s)",
                "error_class": "WorkerError",
            })

    def resolve_range(
        self,
        since: datetime | date,
        until: datetime | date | None = None,
        chunk_days: int = DEFAULT_BACKFILL_CHUNK_DAYS,
        workers: int = 8,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Resolve a historical pull_timestamp range in bounded chunks.

        Catch-up entry point: a single 5-month window would hold one
        transaction open for hours, so the range is walked in
        ``chunk_days``-wide slices and each slice is resolved independently.

        A slice that raises — a statement timeout on the DISTINCT scan is
        the one we have actually seen — is recorded with its date range and
        the walk continues, because the slices are independent and every
        insert is ``ON CONFLICT ... DO NOTHING``. Losing the whole run to
        one wide chunk would throw away every slice that already landed.
        ``failed_ranges`` in the result lists exactly what to retry, so the
        operator re-runs those days alone (narrower) rather than the range.

        Chunk width bounds the transaction and the blast radius of a
        failure. It is not the cost driver it was once taken for: with the
        pre-_window_bounds predicate, 1-day and 7-day chunks both hit the
        600s statement timeout, because neither reached the planner as an
        index bound and each scanned the whole of raw_series. Widen only on
        measured headroom even so — the scan is real work.

        Args:
            since: Inclusive lower bound on ``raw_series.pull_timestamp``.
            until: Exclusive upper bound. Defaults to now.
            chunk_days: Width of each slice in days.
            workers: Concurrent resolver threads per slice.
            dry_run: Measure without writing (see ``resolve_pending``).

        Returns:
            dict with the same keys as ``resolve_pending`` plus ``chunks``,
            ``chunks_failed`` and ``failed_ranges`` (one entry per slice
            that raised or reported worker errors, carrying ``since``,
            ``until``, ``error`` and ``error_class``).
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
            "dry_run": dry_run, "chunks": 0, "chunks_failed": 0,
            "chunks_narrowed": 0, "failed_ranges": [],
        }
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + timedelta(days=chunk_days), end)
            self._resolve_window(cursor, chunk_end, workers, dry_run, totals)
            cursor = chunk_end

        if totals["failed_ranges"]:
            log.warning(
                "Range resolution: {n} of {t} chunk(s) incomplete — retry "
                "these ranges alone: {r}",
                n=totals["chunks_failed"], t=totals["chunks"],
                r=[(f["since"], f["until"]) for f in totals["failed_ranges"]],
            )
        log.info(
            "Range resolution complete — chunks={n} ({f} failed, {w} narrowed "
            "after a timeout), resolved={r}, errors={e}, {t}s",
            n=totals["chunks"], f=totals["chunks_failed"],
            w=totals["chunks_narrowed"], r=totals["resolved"],
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
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Backfill recipe — dry-run one day first, then write:\n"
            "  python -m normalization.resolver --since 2026-04-04 "
            "--until 2026-04-05 --chunk-days 1 --dry-run\n"
            "  python -m normalization.resolver --since 2026-04-04 "
            "--until 2026-04-18 --chunk-days 1\n\n"
            "Chunk width bounds each transaction; it is not a speed knob. "
            "Before the window bounds\nwere made index-usable, 1-day and "
            "7-day chunks alike hit the 600s statement timeout,\nbecause "
            "every chunk scanned the whole of raw_series no matter how "
            "narrow it was.\nRe-running a chunk is safe: every insert is ON "
            "CONFLICT (feature_id, obs_date,\nvintage_date) DO NOTHING. "
            "Chunks that fail are listed under failed_ranges; retry\njust "
            "those, narrower."
        ),
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
        "--chunk-days", type=int, default=DEFAULT_BACKFILL_CHUNK_DAYS,
        help=f"Chunk width in days for --since backfills "
             f"(default: {DEFAULT_BACKFILL_CHUNK_DAYS}). Keep 1 for "
             f"historical windows; widen only after a --dry-run shows "
             f"headroom against the 600s statement timeout.",
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

    for failed in summary.get("failed_ranges", []):
        print(
            f"  RETRY: --since {failed['since']} --until {failed['until']}   "
            f"({failed['error_class']}: {failed['error']})"
        )

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
