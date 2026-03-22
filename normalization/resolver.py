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

# Two values are considered conflicting if they differ by more than 0.5%
CONFLICT_THRESHOLD: float = 0.005


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

    def resolve_pending(self) -> dict[str, int]:
        """Resolve all raw_series observations not yet in resolved_series.

        For each (series_id, obs_date) group:
        - If multiple sources provide data and values diverge by more than
          CONFLICT_THRESHOLD, sets ``conflict_flag=True`` and stores all
          values in ``conflict_detail``.
        - Uses the value from the highest-priority source (lowest priority_rank).
        - Uses release_date from pull_timestamp as proxy.

        Returns:
            dict: Summary with keys ``resolved``, ``conflicts_found``, ``errors``.
        """
        log.info("Starting conflict resolution for pending raw_series rows")
        result: dict[str, int] = {"resolved": 0, "conflicts_found": 0, "errors": 0}

        # Find all pending observations that need resolution
        # A pending observation is one in raw_series with SUCCESS status
        # that doesn't yet have a corresponding resolved_series entry.
        pending_query = text("""
            SELECT
                rs.series_id,
                rs.obs_date,
                rs.value,
                rs.source_id,
                rs.pull_timestamp,
                sc.priority_rank,
                sc.name AS source_name
            FROM raw_series rs
            JOIN source_catalog sc ON rs.source_id = sc.id
            WHERE rs.pull_status = 'SUCCESS'
            ORDER BY rs.series_id, rs.obs_date, sc.priority_rank ASC
        """)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(pending_query).fetchall()
        except Exception as exc:
            log.error("Failed to query pending raw_series: {err}", err=str(exc))
            result["errors"] = 1
            return result

        if not rows:
            log.info("No pending observations to resolve")
            return result

        # Group by (series_id, obs_date)
        groups: dict[tuple[str, date], list[dict[str, Any]]] = {}
        for row in rows:
            key = (row[0], row[1])  # series_id, obs_date
            if key not in groups:
                groups[key] = []
            groups[key].append({
                "value": row[2],
                "source_id": row[3],
                "pull_timestamp": row[4],
                "priority_rank": row[5],
                "source_name": row[6],
            })

        log.info("Found {n} unique (series_id, obs_date) groups to resolve", n=len(groups))

        # Look up feature mappings
        from normalization.entity_map import EntityMap

        entity_map = EntityMap(self.engine)

        with self.engine.begin() as conn:
            for (series_id, obs_date_val), sources in groups.items():
                feature_id = entity_map.get_feature_id(series_id)
                if feature_id is None:
                    continue  # Skip unmapped series

                # Check if already resolved
                existing = conn.execute(
                    text(
                        "SELECT 1 FROM resolved_series "
                        "WHERE feature_id = :fid AND obs_date = :od "
                        "LIMIT 1"
                    ),
                    {"fid": feature_id, "od": obs_date_val},
                ).fetchone()
                if existing is not None:
                    continue

                # Sort by priority (lowest rank = highest priority)
                sources.sort(key=lambda s: s["priority_rank"])
                winner = sources[0]

                # Detect conflicts across sources
                conflict_flag = False
                conflict_detail = None

                if len(sources) > 1:
                    ref_val = winner["value"]
                    for s in sources[1:]:
                        if ref_val != 0:
                            pct_diff = abs(s["value"] - ref_val) / abs(ref_val)
                        else:
                            # When reference is zero, any non-zero value is
                            # an infinite percentage difference — always flag.
                            pct_diff = float("inf") if s["value"] != 0 else 0.0

                        if pct_diff > CONFLICT_THRESHOLD:
                            conflict_flag = True
                            break

                    if conflict_flag:
                        conflict_detail = json.dumps({
                            "sources": [
                                {
                                    "source_name": s["source_name"],
                                    "source_id": s["source_id"],
                                    "value": s["value"],
                                    "priority_rank": s["priority_rank"],
                                }
                                for s in sources
                            ],
                            "threshold": CONFLICT_THRESHOLD,
                        })
                        result["conflicts_found"] += 1

                # Use pull_timestamp date as release_date proxy
                release_dt = winner["pull_timestamp"].date() if hasattr(
                    winner["pull_timestamp"], "date"
                ) else winner["pull_timestamp"]
                vintage_dt = release_dt

                try:
                    conn.execute(
                        text(
                            "INSERT INTO resolved_series "
                            "(feature_id, obs_date, release_date, vintage_date, "
                            "value, source_priority_used, conflict_flag, conflict_detail) "
                            "VALUES (:fid, :od, :rd, :vd, :val, :src, :cf, :cd) "
                            "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING"
                        ),
                        {
                            "fid": feature_id,
                            "od": obs_date_val,
                            "rd": release_dt,
                            "vd": vintage_dt,
                            "val": winner["value"],
                            "src": winner["source_id"],
                            "cf": conflict_flag,
                            "cd": conflict_detail,
                        },
                    )
                    result["resolved"] += 1
                except Exception as exc:
                    log.error(
                        "Failed to insert resolved row for {sid}/{od}: {err}",
                        sid=series_id,
                        od=obs_date_val,
                        err=str(exc),
                    )
                    result["errors"] += 1

        log.info(
            "Resolution complete — resolved={r}, conflicts={c}, errors={e}",
            r=result["resolved"],
            c=result["conflicts_found"],
            e=result["errors"],
        )
        return result

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
