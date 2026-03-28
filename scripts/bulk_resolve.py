#!/usr/bin/env python3
"""Fast bulk resolver -- bypasses the slow per-row resolver for initial data population.

Directly inserts into resolved_series from raw_series using entity_map lookups,
processing in batches to handle 2.5M+ rows without memory issues.

Usage:
  python scripts/bulk_resolve.py                        # resolve all gaps
  python scripts/bulk_resolve.py --family macro          # resolve one family
  python scripts/bulk_resolve.py --feature ofr_fsi       # resolve one feature
  python scripts/bulk_resolve.py --series-id "YF:EURUSD=X:close"  # resolve one series_id
  python scripts/bulk_resolve.py --dry-run               # show what would be resolved
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Allow running from repo root: python scripts/bulk_resolve.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from normalization.entity_map import SEED_MAPPINGS, NEW_MAPPINGS_V2

BATCH_SIZE = 10_000


def build_series_to_feature(engine) -> dict[str, int]:
    """Build a combined series_id -> feature_id mapping from both dictionaries.

    Merges SEED_MAPPINGS and NEW_MAPPINGS_V2, then resolves each feature name
    to its feature_registry.id via a single DB query.

    Returns:
        dict mapping raw series_id strings to integer feature_ids.
    """
    # Merge both mapping dicts (V2 overwrites seeds on collision, which is fine)
    combined: dict[str, str] = {}
    combined.update(SEED_MAPPINGS)
    combined.update(NEW_MAPPINGS_V2)

    # Load all feature_registry rows in one shot
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name, family FROM feature_registry")).fetchall()

    name_to_id: dict[str, int] = {r[1]: r[0] for r in rows}
    name_to_family: dict[str, str] = {r[1]: r[2] for r in rows}

    series_to_feature: dict[str, int] = {}
    for series_id, feature_name in combined.items():
        fid = name_to_id.get(feature_name)
        if fid is not None:
            series_to_feature[series_id] = fid

    log.info(
        "Mapping table built: {m} series_ids -> {f} unique feature_ids",
        m=len(series_to_feature),
        f=len(set(series_to_feature.values())),
    )
    return series_to_feature, name_to_id, name_to_family


def get_features_needing_data(engine, feature_ids: set[int]) -> set[int]:
    """Return the subset of feature_ids that have zero rows in resolved_series."""
    if not feature_ids:
        return set()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT feature_id FROM resolved_series "
                "WHERE feature_id = ANY(:ids)"
            ),
            {"ids": list(feature_ids)},
        ).fetchall()

    already_have = {r[0] for r in rows}
    gaps = feature_ids - already_have
    log.info(
        "Features with data: {have}, features with gaps: {gap}",
        have=len(already_have),
        gap=len(gaps),
    )
    return gaps


def resolve_feature_batch(
    engine,
    feature_id: int,
    series_ids: list[str],
    dry_run: bool = False,
) -> int:
    """Resolve all raw_series rows for a single feature, inserting in batches.

    Selects from raw_series WHERE series_id IN (...) AND pull_status = 'SUCCESS',
    groups by obs_date (taking the row with lowest source priority_rank), and
    inserts directly into resolved_series.

    Returns:
        Number of rows inserted.
    """
    # Use a server-side approach: let Postgres do the heavy lifting with a
    # single INSERT ... SELECT with window functions for priority resolution.
    # This avoids pulling millions of rows into Python.

    if dry_run:
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT COUNT(DISTINCT obs_date) FROM raw_series "
                    "WHERE series_id = ANY(:sids) AND pull_status = 'SUCCESS'"
                ),
                {"sids": series_ids},
            ).scalar()
        return count or 0

    # Batched insert using OFFSET/LIMIT on the source query to keep memory
    # bounded. We use a CTE with ROW_NUMBER to pick the highest-priority
    # source per obs_date, then insert in slices.
    total_inserted = 0
    offset = 0

    while True:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    WITH ranked AS (
                        SELECT
                            rs.obs_date,
                            rs.value,
                            rs.pull_timestamp,
                            rs.source_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY rs.obs_date
                                ORDER BY sc.priority_rank ASC, rs.pull_timestamp DESC
                            ) AS rn
                        FROM raw_series rs
                        JOIN source_catalog sc ON rs.source_id = sc.id
                        WHERE rs.series_id = ANY(:sids)
                          AND rs.pull_status = 'SUCCESS'
                    ),
                    winners AS (
                        SELECT obs_date, value, pull_timestamp, source_id
                        FROM ranked
                        WHERE rn = 1
                        ORDER BY obs_date
                        LIMIT :batch_size OFFSET :offset
                    )
                    INSERT INTO resolved_series
                        (feature_id, obs_date, value, release_date, vintage_date,
                         source_priority_used, conflict_flag)
                    SELECT
                        :feature_id,
                        w.obs_date,
                        w.value,
                        w.pull_timestamp::date,
                        w.pull_timestamp::date,
                        1,
                        FALSE
                    FROM winners w
                    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
                """),
                {
                    "sids": series_ids,
                    "feature_id": feature_id,
                    "batch_size": BATCH_SIZE,
                    "offset": offset,
                },
            )
            rows_affected = result.rowcount
            total_inserted += max(rows_affected, 0)

            # If we got fewer than BATCH_SIZE from the source query, we're done
            if rows_affected < BATCH_SIZE:
                break
            offset += BATCH_SIZE

    return total_inserted


def resolve_single_series(engine, series_id: str, dry_run: bool = False) -> None:
    """Resolve a single series_id directly, regardless of gap status."""
    series_map, name_to_id, _ = build_series_to_feature(engine)

    feature_id = series_map.get(series_id)
    if feature_id is None:
        log.error("Series '{sid}' has no mapping in SEED_MAPPINGS or NEW_MAPPINGS_V2", sid=series_id)
        sys.exit(1)

    feature_name = next(
        (name for name, fid in name_to_id.items() if fid == feature_id), "?"
    )
    log.info(
        "Resolving series_id='{sid}' -> feature={fn} (id={fid})",
        sid=series_id,
        fn=feature_name,
        fid=feature_id,
    )

    t0 = time.perf_counter()
    inserted = resolve_feature_batch(engine, feature_id, [series_id], dry_run=dry_run)
    elapsed = time.perf_counter() - t0

    if dry_run:
        log.info("DRY RUN: {n} obs_dates would be resolved ({t:.1f}s)", n=inserted, t=elapsed)
    else:
        log.info("Inserted {n} rows in {t:.1f}s", n=inserted, t=elapsed)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fast bulk resolver for GRID resolved_series",
    )
    parser.add_argument("--family", type=str, default=None, help="Resolve only features in this family")
    parser.add_argument("--feature", type=str, default=None, help="Resolve only this feature name")
    parser.add_argument("--series-id", type=str, default=None, help="Resolve a single raw series_id")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be resolved without writing")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Rows per INSERT batch (default 10000)")
    args = parser.parse_args()

    BATCH_SIZE = args.batch_size

    engine = get_engine()
    log.info("Bulk resolver starting (dry_run={dr})", dr=args.dry_run)

    # Handle --series-id shortcut
    if args.series_id:
        resolve_single_series(engine, args.series_id, dry_run=args.dry_run)
        return

    # Build full mapping
    series_map, name_to_id, name_to_family = build_series_to_feature(engine)

    # Invert: feature_id -> [series_ids]
    feature_to_series: dict[int, list[str]] = {}
    for sid, fid in series_map.items():
        feature_to_series.setdefault(fid, []).append(sid)

    # Apply --family filter
    if args.family:
        target_fids = {
            fid for name, fid in name_to_id.items()
            if name_to_family.get(name) == args.family and fid in feature_to_series
        }
        if not target_fids:
            log.warning("No mapped features found for family '{f}'", f=args.family)
            return
        log.info("Filtered to {n} features in family '{f}'", n=len(target_fids), f=args.family)
    elif args.feature:
        fid = name_to_id.get(args.feature)
        if fid is None:
            log.error("Feature '{f}' not found in feature_registry", f=args.feature)
            sys.exit(1)
        if fid not in feature_to_series:
            log.error("Feature '{f}' (id={fid}) has no series_id mappings", f=args.feature, fid=fid)
            sys.exit(1)
        target_fids = {fid}
        log.info("Targeting single feature: {f} (id={fid})", f=args.feature, fid=fid)
    else:
        target_fids = set(feature_to_series.keys())

    # Find features that need data (skip those already populated)
    gaps = get_features_needing_data(engine, target_fids)
    if not gaps:
        log.info("All {n} features already have resolved_series data -- nothing to do", n=len(target_fids))
        return

    # Reverse-lookup feature names for logging
    id_to_name: dict[int, str] = {v: k for k, v in name_to_id.items()}

    log.info("Processing {n} features with gaps", n=len(gaps))
    t_start = time.perf_counter()
    total_features = 0
    total_rows = 0

    for i, fid in enumerate(sorted(gaps), 1):
        fname = id_to_name.get(fid, f"id={fid}")
        sids = feature_to_series[fid]

        inserted = resolve_feature_batch(engine, fid, sids, dry_run=args.dry_run)
        total_features += 1
        total_rows += inserted

        if inserted > 0 or args.dry_run:
            action = "would insert" if args.dry_run else "inserted"
            log.info(
                "[{i}/{n}] {fn}: {action} {r} rows (series_ids: {s})",
                i=i,
                n=len(gaps),
                fn=fname,
                action=action,
                r=inserted,
                s=len(sids),
            )

    elapsed = time.perf_counter() - t_start
    log.info(
        "Bulk resolve complete: {feat} features processed, {rows} rows {verb}, {t:.1f}s elapsed",
        feat=total_features,
        rows=total_rows,
        verb="would be inserted" if args.dry_run else "inserted",
        t=elapsed,
    )

    # ── Post-resolve audit ──────────────────────────────────────────────────
    if not args.dry_run and total_rows > 0:
        try:
            from intelligence.resolution_audit import audit_after_resolve

            resolved_names = [id_to_name.get(fid) for fid in sorted(gaps)]
            resolved_names = [n for n in resolved_names if n is not None]
            audit_result = audit_after_resolve(engine, resolved_names or None)
            audit_summary = audit_result.get("summary", {})
            log.info(
                "Post-resolve audit: {total} findings "
                "(critical={c}, warning={w}, info={i})",
                total=audit_summary.get("total_findings", 0),
                c=audit_summary.get("by_severity", {}).get("critical", 0),
                w=audit_summary.get("by_severity", {}).get("warning", 0),
                i=audit_summary.get("by_severity", {}).get("info", 0),
            )
        except Exception as exc:
            log.warning("Post-resolve audit failed (non-fatal): {e}", e=str(exc))


if __name__ == "__main__":
    main()
