#!/usr/bin/env python3
"""Read-only count: ``resolved_series`` rows whose ``release_date`` is before their ``vintage_date``.

Knowable-at for a resolved row is the later of ``release_date`` and
``vintage_date``. The resolver stamps both with the pull date. A row with
``release_date < vintage_date`` claims a release earlier than the day GRID
wrote it: the TwelveData bridge
(``scripts/backfill_resolved_series_from_ticker_metrics.sql`` before
2026-11-21, E1-V7c) wrote ``release_date = obs_date`` with ``vintage_date`` =
the fetch day. A reader that fenced ``release_date <= as_of`` alone served
those rows to every ``as_of`` in ``[release_date, vintage_date)``;
``store/pit.py`` (GRID#1365) and the direct readers now fence
``vintage_date`` too.

Per feature it reports, split by ``source_priority_used`` (with its
``source_catalog`` name):

* ``rows``: rows with ``release_date < vintage_date``;
* ``gap_days_max`` / ``gap_days_mean``: ``vintage_date - release_date``, the
  length of the window those rows leaked into under a release-only fence;
* ``obs_min`` / ``obs_max``;
* ``already_retracted``: rows that already have a
  ``resolved_series_retractions`` row (0 when the table does not exist);
* ``cells_left_with_no_value``: ``(feature_id, obs_date)`` cells holding an
  unretracted early row and no unretracted honest vintage
  (``release_date >= vintage_date``). Retracting the early rows would leave
  these cells with no value at all from the retraction day on.

``retraction_preview`` sums what a retraction run over these rows WOULD cover.
This script writes nothing: retractions are append-only and need the owner's
decision (see ``store/pit.py`` and
``migrations/versions/resolved_retractions_20260927.py``).

Safety (same contract as ``scripts/audit_backdated_vintages.py``):

* every statement runs in a READ ONLY transaction with a
  ``statement_timeout`` (default 30 s), one transaction per feature;
* every ``resolved_series`` predicate starts from ``feature_id`` (the
  ``(feature_id, obs_date, vintage_date)`` key), never a full-table scan;
* it refuses to run between 03:30 and 10:30 UTC (the nightly backup and DB
  window) unless ``--allow-db-window`` is passed for a non-production test
  DB, re-checking before every feature.

Exit status is non-zero when any feature reports an ``error`` or the run
stopped at the DB window.

Usage::

    python scripts/count_release_before_vintage.py --feature aapl_full
    python scripts/count_release_before_vintage.py --all-features
    python scripts/count_release_before_vintage.py --all-features --source TWELVEDATA
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

from scripts.audit_backdated_vintages import (  # noqa: E402
    DEFAULT_STATEMENT_TIMEOUT_MS,
    DbWindowRefused,
    _readonly,
    check_window,
)

# Two static statements (with and without the retractions table): no SQL is
# assembled at run time.
_COUNTS_WITH_RETRACTIONS_SQL = text("""
    WITH early AS (
        SELECT rs.obs_date, rs.vintage_date, rs.release_date, rs.source_priority_used,
               EXISTS (
                   SELECT 1 FROM resolved_series_retractions rr
                   WHERE rr.feature_id = rs.feature_id
                     AND rr.obs_date = rs.obs_date
                     AND rr.vintage_date = rs.vintage_date
               ) AS retracted
        FROM resolved_series rs
        WHERE rs.feature_id = :fid
          AND rs.release_date < rs.vintage_date
    )
    SELECT e.source_priority_used,
           sc.name,
           count(*)                                              AS rows,
           max(e.vintage_date - e.release_date)                  AS gap_max,
           avg(e.vintage_date - e.release_date)::float           AS gap_mean,
           min(e.obs_date)                                       AS obs_min,
           max(e.obs_date)                                       AS obs_max,
           count(*) FILTER (WHERE e.retracted)                   AS already_retracted,
           count(DISTINCT e.obs_date) FILTER (WHERE NOT e.retracted AND NOT EXISTS (
               SELECT 1 FROM resolved_series o
               WHERE o.feature_id = :fid
                 AND o.obs_date = e.obs_date
                 AND o.release_date >= o.vintage_date
                 AND NOT EXISTS (
                     SELECT 1 FROM resolved_series_retractions r2
                     WHERE r2.feature_id = o.feature_id
                       AND r2.obs_date = o.obs_date
                       AND r2.vintage_date = o.vintage_date
                 )
           ))                                                    AS cells_left_empty
    FROM early e
    LEFT JOIN source_catalog sc ON sc.id = e.source_priority_used
    WHERE (CAST(:source AS text) IS NULL OR sc.name = CAST(:source AS text))
    GROUP BY e.source_priority_used, sc.name
    ORDER BY count(*) DESC
""")

_COUNTS_NO_RETRACTIONS_SQL = text("""
    WITH early AS (
        SELECT rs.obs_date, rs.vintage_date, rs.release_date, rs.source_priority_used
        FROM resolved_series rs
        WHERE rs.feature_id = :fid
          AND rs.release_date < rs.vintage_date
    )
    SELECT e.source_priority_used,
           sc.name,
           count(*)                                              AS rows,
           max(e.vintage_date - e.release_date)                  AS gap_max,
           avg(e.vintage_date - e.release_date)::float           AS gap_mean,
           min(e.obs_date)                                       AS obs_min,
           max(e.obs_date)                                       AS obs_max,
           0                                                     AS already_retracted,
           count(DISTINCT e.obs_date) FILTER (WHERE NOT EXISTS (
               SELECT 1 FROM resolved_series o
               WHERE o.feature_id = :fid
                 AND o.obs_date = e.obs_date
                 AND o.release_date >= o.vintage_date
           ))                                                    AS cells_left_empty
    FROM early e
    LEFT JOIN source_catalog sc ON sc.id = e.source_priority_used
    WHERE (CAST(:source AS text) IS NULL OR sc.name = CAST(:source AS text))
    GROUP BY e.source_priority_used, sc.name
    ORDER BY count(*) DESC
""")


@dataclass
class FeatureCount:
    feature: str
    feature_id: int | None = None
    rows: int = 0
    gap_days_max: int | None = None
    already_retracted: int = 0
    cells_left_with_no_value: int = 0
    obs_min: str | None = None
    obs_max: str | None = None
    by_source: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def count_feature(engine: Engine, feature: str, *, source: str | None = None,
                  timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS) -> FeatureCount:
    out = FeatureCount(feature=feature)
    try:
        with engine.connect() as conn, conn.begin():
            _readonly(conn, timeout_ms)
            row = conn.execute(text("SELECT id FROM feature_registry WHERE name = :n"), {"n": feature}).fetchone()
            if row is None:
                out.error = "feature not in feature_registry"
                return out
            out.feature_id = int(row[0])
            has_retractions = conn.execute(
                text("SELECT to_regclass('resolved_series_retractions') IS NOT NULL")).scalar()
            sql = _COUNTS_WITH_RETRACTIONS_SQL if has_retractions else _COUNTS_NO_RETRACTIONS_SQL
            for src, name, rows, gap_max, gap_mean, lo, hi, retracted, only in conn.execute(
                sql, {"fid": out.feature_id, "source": source},
            ):
                out.by_source.append({
                    "source_priority_used": src, "source_name": name, "rows": int(rows),
                    "gap_days_max": int(gap_max), "gap_days_mean": round(float(gap_mean), 2),
                    "obs_min": lo.isoformat(), "obs_max": hi.isoformat(),
                    "already_retracted": int(retracted), "cells_left_with_no_value": int(only),
                })
                out.rows += int(rows)
                out.already_retracted += int(retracted)
                out.cells_left_with_no_value += int(only)
                out.gap_days_max = int(gap_max) if out.gap_days_max is None else max(out.gap_days_max, int(gap_max))
                out.obs_min = lo.isoformat() if out.obs_min is None else min(out.obs_min, lo.isoformat())
                out.obs_max = hi.isoformat() if out.obs_max is None else max(out.obs_max, hi.isoformat())
    except Exception as exc:  # one feature's timeout must not hide the others
        out.error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
    return out


def list_features(engine: Engine, limit: int | None,
                  timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS) -> list[str]:
    with engine.connect() as conn, conn.begin():
        _readonly(conn, timeout_ms)
        rows = conn.execute(
            text("SELECT name FROM feature_registry ORDER BY name LIMIT :n"),
            {"n": limit},  # LIMIT NULL = no limit
        )
        return [r[0] for r in rows]


def run(engine: Engine, features: list[str], *, source: str | None = None,
        timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS, allow_db_window: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    started = clock()
    check_window(started, allow_db_window)
    counts: list[FeatureCount] = []
    stopped = None
    for f in features:
        try:
            check_window(clock(), allow_db_window)
        except DbWindowRefused as exc:
            stopped = f"db_window: {exc}"
            break
        counts.append(count_feature(engine, f, source=source, timeout_ms=timeout_ms))

    hits = [c for c in counts if c.rows or c.error]
    by_source: dict[str, dict[str, int]] = {}
    for c in counts:
        for s in c.by_source:
            agg = by_source.setdefault(str(s["source_name"] or s["source_priority_used"]),
                                       {"rows": 0, "features": 0, "gap_days_max": 0})
            agg["rows"] += s["rows"]
            agg["features"] += 1
            agg["gap_days_max"] = max(agg["gap_days_max"], s["gap_days_max"])
    would = [c for c in counts if c.rows - c.already_retracted > 0]
    return {
        "generated_at": started.astimezone(timezone.utc).isoformat(),
        "read_only": True,
        "statement_timeout_ms": timeout_ms,
        "source_filter": source,
        "features_scanned": len(counts),
        "features_with_rows": sum(1 for c in counts if c.rows),
        "rows_release_before_vintage": sum(c.rows for c in counts),
        "by_source": by_source,
        "retraction_preview": {
            "written": False,
            "rows": sum(c.rows - c.already_retracted for c in counts),
            "features": len(would),
            "cells_left_with_no_value": sum(c.cells_left_with_no_value for c in counts),
            "note": ("What a retraction of these rows would cover. Nothing was written: retractions are "
                     "append-only and need the owner's decision. A retracted row reads as no data from the "
                     "retraction day on, also for as_of dates after its vintage_date, where it is honest."),
        },
        "features": [asdict(c) for c in hits],
        "stopped": stopped,
        "complete": stopped is None and len(counts) == len(features),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--feature", action="append", default=[], help="feature_registry.name (repeatable)")
    p.add_argument("--all-features", action="store_true", help="every feature_registry row, in name order")
    p.add_argument("--max-features", type=int, default=None, help="cap for --all-features (default: none)")
    p.add_argument("--source", default=None, help="only rows whose source_catalog.name is this (e.g. TWELVEDATA)")
    p.add_argument("--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS)
    p.add_argument("--allow-db-window", action="store_true",
                   help="run inside 03:30-10:30 UTC (test databases only)")
    p.add_argument("--db-url", default=None, help="database URL (default: db.get_engine())")
    args = p.parse_args(argv)
    if not args.feature and not args.all_features:
        p.error("name at least one --feature, or --all-features")
    try:
        check_window(datetime.now(timezone.utc), args.allow_db_window)
    except DbWindowRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if args.db_url:
        from sqlalchemy import create_engine

        engine = create_engine(args.db_url)
    else:
        from db import get_engine

        engine = get_engine()
    features = list(args.feature)
    if args.all_features:
        features += [f for f in list_features(engine, args.max_features, args.statement_timeout_ms)
                     if f not in features]
    report = run(engine, features, source=args.source, timeout_ms=args.statement_timeout_ms,
                 allow_db_window=args.allow_db_window)
    json.dump(report, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    failed = [c["feature"] for c in report["features"] if c["error"]]
    if failed or not report["complete"]:
        print(f"incomplete count: errors in {failed}; stopped={report['stopped']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
