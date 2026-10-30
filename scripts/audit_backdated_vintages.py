#!/usr/bin/env python3
"""Read-only audit: ``resolved_series`` rows whose vintage claims to predate GRID's data.

A row with ``release_date = obs_date`` says "GRID knew this value on its
observation day". That is only possible from the day GRID first pulled the
series. Per feature this script reports, with bounded queries:

* ``total_rows`` and ``backdated_rows`` (``release_date = obs_date``), split by
  ``source_priority_used`` (with its ``source_catalog`` name) and by whether
  ``vintage_date = obs_date`` too;
* ``first_raw_pull``: the earliest SUCCESS ``raw_series.pull_timestamp`` over the
  feature's mapped series ids (``normalization.entity_map`` seeds, plus
  ``YF:<TICKER>:close`` for ``<ticker>_full``, plus ``--series`` overrides).
  Every source is counted: Tiingo also writes ``YF:<TICKER>:close`` (under its
  own source id), so its pulls are included;
* ``backdated_before_first_pull``: backdated rows whose ``obs_date`` precedes
  that first pull -- values that cannot have been known on the day they claim;
* ``already_retracted``: how many of the backdated rows already have a
  ``resolved_series_retractions`` row (0 when that table does not exist).

The exit status is non-zero when any feature reports an ``error`` (a timeout
must not read as a clean audit) or the run stopped at the DB window.

Safety (GRID-DFa, R7):

* every statement runs in a READ ONLY transaction with a ``statement_timeout``
  (default 30 s), one transaction per feature;
* every predicate is indexed: ``resolved_series`` by ``feature_id``,
  ``raw_series`` by ``series_id`` only (never a raw_series full scan; the
  ``min()`` is written so the planner cannot walk the pull_timestamp index);
* it refuses to run between 03:30 and 10:30 UTC (the nightly backup and DB
  window) unless ``--allow-db-window`` is passed for a non-production test DB;
  the window is re-checked before every feature, and a run that reaches it
  stops with a partial report.

Usage::

    python scripts/audit_backdated_vintages.py --feature spy_full
    python scripts/audit_backdated_vintages.py --feature spy_full --series spy_full=YF:SPY:close
    python scripts/audit_backdated_vintages.py --all-features --max-features 50
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Connection, Engine  # noqa: E402

DB_WINDOW_START = time(3, 30)
DB_WINDOW_END = time(10, 30)
DEFAULT_STATEMENT_TIMEOUT_MS = 30_000


class DbWindowRefused(RuntimeError):
    """The audit was asked to run inside the 03:30-10:30 UTC DB window."""


def in_db_window(now: datetime) -> bool:
    utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return DB_WINDOW_START <= utc.time() < DB_WINDOW_END


def check_window(now: datetime, allow: bool = False) -> None:
    if in_db_window(now) and not allow:
        raise DbWindowRefused(
            f"refusing to query between {DB_WINDOW_START:%H:%M} and {DB_WINDOW_END:%H:%M} UTC "
            f"(now {now.astimezone(timezone.utc):%H:%M}Z): nightly backup / DB window"
        )


def mapped_series(feature: str, overrides: dict[str, list[str]] | None = None) -> list[str]:
    """Raw series ids that resolve into ``feature`` (static mappings, no DB)."""
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

    series = {sid for sid, name in {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}.items() if name == feature}
    if feature.endswith("_full"):
        series.add(f"YF:{feature[: -len('_full')].upper().replace('_', '-')}:close")
    series.update((overrides or {}).get(feature, []))
    return sorted(series)


@dataclass
class FeatureAudit:
    feature: str
    feature_id: int | None = None
    series: list[str] = field(default_factory=list)
    first_raw_pull: str | None = None
    total_rows: int = 0
    backdated_rows: int = 0
    backdated_vintage_too: int = 0
    backdated_before_first_pull: int | None = None
    backdated_obs_min: str | None = None
    backdated_obs_max: str | None = None
    already_retracted: int | None = None
    by_source: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def _readonly(conn: Connection, timeout_ms: int) -> None:
    conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(int(timeout_ms))})


_FIRST_PULL_SQL = text("""
    -- "+ interval '0'" keeps this a plain aggregate over the series_id index:
    -- a bare min(pull_timestamp) can be planned as a walk of the whole
    -- pull_timestamp index, i.e. a raw_series full scan.
    SELECT min(pull_timestamp + interval '0')
    FROM raw_series
    WHERE series_id = :sid AND pull_status = 'SUCCESS'
""")

_COUNTS_SQL = text("""
    SELECT rs.source_priority_used,
           sc.name,
           count(*)                                              AS rows,
           count(*) FILTER (WHERE rs.vintage_date = rs.obs_date) AS vintage_too,
           count(*) FILTER (WHERE CAST(:first_pull AS date) IS NOT NULL
                              AND rs.obs_date < CAST(:first_pull AS date)) AS before_first_pull,
           min(rs.obs_date), max(rs.obs_date)
    FROM resolved_series rs
    LEFT JOIN source_catalog sc ON sc.id = rs.source_priority_used
    WHERE rs.feature_id = :fid AND rs.release_date = rs.obs_date
    GROUP BY rs.source_priority_used, sc.name
    ORDER BY count(*) DESC
""")


_RETRACTED_SQL = text("""
    SELECT count(*) FROM resolved_series rs
    JOIN resolved_series_retractions r
      ON r.feature_id = rs.feature_id AND r.obs_date = rs.obs_date AND r.vintage_date = rs.vintage_date
    WHERE rs.feature_id = :fid AND rs.release_date = rs.obs_date
""")


def audit_feature(engine: Engine, feature: str, *, overrides: dict[str, list[str]] | None = None,
                  timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS) -> FeatureAudit:
    out = FeatureAudit(feature=feature, series=mapped_series(feature, overrides))
    try:
        with engine.connect() as conn, conn.begin():
            _readonly(conn, timeout_ms)
            row = conn.execute(text("SELECT id FROM feature_registry WHERE name = :n"), {"n": feature}).fetchone()
            if row is None:
                out.error = "feature not in feature_registry"
                return out
            out.feature_id = int(row[0])
            pulls = [conn.execute(_FIRST_PULL_SQL, {"sid": sid}).scalar() for sid in out.series]
            pulls = [p for p in pulls if p is not None]
            first = min(pulls) if pulls else None
            first_day: date | None = first.astimezone(timezone.utc).date() if first is not None else None
            out.first_raw_pull = first.isoformat() if first is not None else None
            out.total_rows = int(conn.execute(
                text("SELECT count(*) FROM resolved_series WHERE feature_id = :fid"), {"fid": out.feature_id},
            ).scalar())
            before = 0
            obs_lo: date | None = None
            obs_hi: date | None = None
            for src, name, rows, vintage_too, before_first, lo, hi in conn.execute(
                _COUNTS_SQL, {"fid": out.feature_id, "first_pull": first_day},
            ):
                out.by_source.append({"source_priority_used": src, "source_name": name, "rows": int(rows),
                                      "vintage_also_obs": int(vintage_too),
                                      "before_first_pull": int(before_first) if first_day else None,
                                      "obs_min": lo.isoformat(), "obs_max": hi.isoformat()})
                out.backdated_rows += int(rows)
                out.backdated_vintage_too += int(vintage_too)
                before += int(before_first)
                obs_lo = lo if obs_lo is None or lo < obs_lo else obs_lo
                obs_hi = hi if obs_hi is None or hi > obs_hi else obs_hi
            out.backdated_before_first_pull = before if first_day is not None else None
            out.backdated_obs_min = obs_lo.isoformat() if obs_lo else None
            out.backdated_obs_max = obs_hi.isoformat() if obs_hi else None
            has_retractions = conn.execute(
                text("SELECT to_regclass('resolved_series_retractions') IS NOT NULL")).scalar()
            out.already_retracted = int(conn.execute(_RETRACTED_SQL, {"fid": out.feature_id}).scalar()) \
                if has_retractions else 0
    except Exception as exc:  # one feature's timeout must not hide the others
        out.error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
    return out


def list_features(engine: Engine, limit: int, timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS) -> list[str]:
    with engine.connect() as conn, conn.begin():
        _readonly(conn, timeout_ms)
        rows = conn.execute(text("SELECT name FROM feature_registry ORDER BY name LIMIT :n"), {"n": limit})
        return [r[0] for r in rows]


def run(engine: Engine, features: list[str], *, overrides: dict[str, list[str]] | None = None,
        timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS, allow_db_window: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    started = clock()
    check_window(started, allow_db_window)
    audits: list[dict[str, Any]] = []
    stopped = None
    for f in features:
        try:
            check_window(clock(), allow_db_window)
        except DbWindowRefused as exc:
            stopped = f"db_window: {exc}"
            break
        audits.append(asdict(audit_feature(engine, f, overrides=overrides, timeout_ms=timeout_ms)))
    return {"generated_at": started.astimezone(timezone.utc).isoformat(), "statement_timeout_ms": timeout_ms,
            "read_only": True, "features": audits, "stopped": stopped,
            "complete": stopped is None and len(audits) == len(features)}


def _parse_overrides(values: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for item in values:
        feature, _, series = item.partition("=")
        if not feature or not series:
            raise SystemExit(f"--series wants FEATURE=SERIES_ID[,SERIES_ID...], got {item!r}")
        out.setdefault(feature, []).extend(s for s in series.split(",") if s)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--feature", action="append", default=[], help="feature_registry.name (repeatable)")
    p.add_argument("--series", action="append", default=[], help="FEATURE=SERIES_ID[,...] extra raw series")
    p.add_argument("--all-features", action="store_true", help="audit feature_registry in name order")
    p.add_argument("--max-features", type=int, default=50, help="cap for --all-features (default 50)")
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
    report = run(engine, features, overrides=_parse_overrides(args.series),
                 timeout_ms=args.statement_timeout_ms, allow_db_window=args.allow_db_window)
    json.dump(report, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    failed = [a["feature"] for a in report["features"] if a["error"]]
    if failed or not report["complete"]:
        print(f"incomplete audit: errors in {failed}; stopped={report['stopped']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
