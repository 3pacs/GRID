"""
GRID conflict resolution module.

Resolves raw_series observations into resolved_series by selecting the
highest-priority source and detecting value conflicts across sources.
"""

from __future__ import annotations

import json
from datetime import date
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

    def resolve_pending(
        self,
        lookback_days: int = 30,
        workers: int = 8,
    ) -> dict[str, int]:
        """Resolve raw_series → resolved_series using multithreaded workers.

        Fetches distinct series_ids with pending data, partitions them across
        worker threads, and each worker resolves its partition independently
        with batched inserts.

        Args:
            lookback_days: Only process raw rows pulled within this window.
            workers: Number of concurrent resolver threads.

        Returns:
            dict with resolved, conflicts_found, errors counts.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        log.info("Starting multithreaded resolution (workers={w}, lookback={d}d)",
                 w=workers, d=lookback_days)

        # Pre-load entity map and feature families (shared, read-only)
        entity_map = EntityMap(self.engine)
        feature_families: dict[int, str] = {}
        try:
            with self.engine.connect() as conn:
                fam_rows = conn.execute(
                    text("SELECT id, family FROM feature_registry")
                ).fetchall()
                feature_families = {row[0]: row[1] for row in fam_rows}
        except Exception as exc:
            log.warning("Could not load feature families: {e}", e=str(exc))

        # Fetch distinct series_ids with recent data
        log.info("Fetching distinct series_ids...")
        with self.engine.connect() as conn:
            series_rows = conn.execute(text("""
                SELECT DISTINCT series_id
                FROM raw_series
                WHERE pull_status = 'SUCCESS'
                  AND pull_timestamp >= NOW() - :lookback * INTERVAL '1 day'
            """), {"lookback": lookback_days}).fetchall()

        all_series = [r[0] for r in series_rows]
        log.info("Found {n} distinct series_ids to resolve", n=len(all_series))

        if not all_series:
            log.info("No pending observations to resolve")
            return {"resolved": 0, "conflicts_found": 0, "errors": 0}

        # Partition series_ids across workers
        chunk_size = max(1, len(all_series) // workers)
        partitions = [
            all_series[i:i + chunk_size]
            for i in range(0, len(all_series), chunk_size)
        ]

        # Counters (thread-safe)
        lock = threading.Lock()
        totals = {"resolved": 0, "conflicts_found": 0, "errors": 0}

        def _resolve_partition(partition: list[str], worker_id: int) -> dict[str, int]:
            """Resolve a partition of series_ids."""
            local = {"resolved": 0, "conflicts_found": 0, "errors": 0}
            INSERT_BATCH = 500

            try:
                with self.engine.connect() as conn:
                    rows = conn.execute(text("""
                        SELECT rs.series_id, rs.obs_date, rs.value,
                               rs.source_id, rs.pull_timestamp,
                               sc.priority_rank, sc.name AS source_name
                        FROM raw_series rs
                        JOIN source_catalog sc ON rs.source_id = sc.id
                        WHERE rs.series_id = ANY(:sids)
                          AND rs.pull_status = 'SUCCESS'
                          AND rs.pull_timestamp >= NOW() - :lookback * INTERVAL '1 day'
                        ORDER BY rs.series_id, rs.obs_date, sc.priority_rank ASC
                    """), {"sids": partition, "lookback": lookback_days}).fetchall()

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
                        local["resolved"] += _flush_batch(self.engine, insert_batch)
                        insert_batch = []

                # Flush remaining
                if insert_batch:
                    local["resolved"] += _flush_batch(self.engine, insert_batch)

                log.info("Worker {w}: resolved={r}, conflicts={c}",
                         w=worker_id, r=local["resolved"], c=local["conflicts_found"])

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

        log.info(
            "Resolution complete — resolved={r}, conflicts={c}, errors={e}",
            r=totals["resolved"], c=totals["conflicts_found"], e=totals["errors"],
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


if __name__ == "__main__":
    from db import get_engine

    resolver = Resolver(db_engine=get_engine())
    summary = resolver.resolve_pending()
    print(f"Resolution summary: {summary}")

    report = resolver.get_conflict_report()
    if not report.empty:
        print(f"\nConflicts found: {len(report)}")
        print(report.head(10))
    else:
        print("\nNo conflicts found")
