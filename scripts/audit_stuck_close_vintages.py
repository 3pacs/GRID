#!/usr/bin/env python3
"""Read-only audit: resolved cells still serving a pre-close value after a POLICY_ELIGIBLE close arrived (#1194).

Until #978 (bf74c47c, deployed 2026-11-06) the bulk resolver collapsed every
pull-date vintage of one ``(series_id, obs_date)`` in its window into a single
winner. When that winner's vintage was already resolved, the insert hit
``ON CONFLICT DO NOTHING`` and the newer vintage was dropped, for good once the
watermark moved past it. 2026-10-27 ``sp500_full`` is the known case: the
13:31Z intraday 7701.57 is still served although the POLICY_ELIGIBLE close
7722.60 (``YF:^GSPC:close``, pulled 2026-10-28 13:31Z) is in ``raw_series``.

For each feature fed by a daily-contract yfinance series (``price_close_contract``)
this script replays the resolver's own vintage rule over the raw rows and
compares it with ``resolved_series``. Per ``(feature, obs_date)`` cell:

* ``stuck``: some raw row is a POLICY_ELIGIBLE daily capture
  (``is_daily_policy_capture``), yet the latest non-retracted resolved
  vintage is older than the first eligible pull's vintage, or there is none.
  The cell serves a value GRID knew was not the close. This is #1194's
  headline count.
* ``eligible_vintage_missing``: the stuck cell's first eligible vintage has no
  resolved row at all (not even a retracted one). This is the dropped-vintage
  signature; a stuck cell without it has some other cause.
* ``missing_vintages``: every raw pull-date vintage with no resolved row,
  eligible or not (context only: an ``obs_since`` backfill skips vintages by
  design).

Vintages are computed with ``normalization.resolver._vintage_of`` over rows
read through the same engine, so they match what the resolver would write.
Rows the resolver itself ignores (TIINGO rows past a ticker's last session,
``tiingo_symbol_contract``) are ignored here too.

Safety (same rules as ``audit_backdated_vintages.py``): one READ ONLY
transaction per feature with a ``statement_timeout``; ``raw_series`` is read
by ``series_id`` plus an ``obs_date`` range only; it refuses to run between
03:30 and 10:30 UTC unless ``--allow-db-window`` is passed for a test DB, and
re-checks before every feature. It writes nothing.

Usage::

    python scripts/audit_stuck_close_vintages.py                         # all daily-contract features, 2026
    python scripts/audit_stuck_close_vintages.py --series 'YF:^GSPC:close' --since 2026-10-20
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

from normalization.resolver import _vintage_of  # noqa: E402
from price_close_contract import (  # noqa: E402
    NY_LABEL_DAY_SESSION_CONVENTIONS,
    UTC_DAILY_TICKERS,
    is_daily_policy_capture,
)
from scripts.audit_backdated_vintages import (  # noqa: E402
    DbWindowRefused,
    _readonly,
    check_window,
)
from tiingo_symbol_contract import is_foreign_tiingo_row  # noqa: E402

DEFAULT_STATEMENT_TIMEOUT_MS = 60_000
DEFAULT_SINCE = date(2026, 1, 1)  # the daily capture contract only covers 2026
DEFAULT_MAX_EXAMPLES = 25
PRICE_FIELDS = ("close", "adj_close")


def default_series() -> list[str]:
    """Every daily-contract yfinance price series: the only rows that can be POLICY_ELIGIBLE."""
    tickers = sorted(set(NY_LABEL_DAY_SESSION_CONVENTIONS) | set(UTC_DAILY_TICKERS))
    return [f"YF:{t}:{f}" for t in tickers for f in PRICE_FIELDS]


@dataclass
class FeatureGap:
    feature_id: int
    feature: str | None = None
    series: list[str] = field(default_factory=list)
    raw_rows: int = 0
    cells: int = 0
    cells_with_eligible: int = 0
    stuck_cells: int = 0
    eligible_vintage_missing_cells: int = 0
    missing_vintage_cells: int = 0
    missing_vintages: int = 0
    stuck: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def classify(raw_rows: Iterable[dict[str, Any]], resolved_rows: Iterable[dict[str, Any]],
             *, max_examples: int = DEFAULT_MAX_EXAMPLES,
             vintage_of: Callable[[Any], Any] = _vintage_of) -> dict[str, Any]:
    """Compare one feature's raw vintages with its resolved rows (pure; no DB).

    ``raw_rows``: dicts with series_id, obs_date, pull_timestamp (tz-aware),
    value, source_name, raw_payload. ``resolved_rows``: dicts with obs_date,
    vintage_date, value, retracted.
    """
    raw_by_cell: dict[date, list[dict[str, Any]]] = {}
    n_raw = 0
    for row in raw_rows:
        if is_foreign_tiingo_row(row["series_id"], row.get("source_name"), row["obs_date"]):
            continue
        n_raw += 1
        raw_by_cell.setdefault(row["obs_date"], []).append(row)
    res_by_cell: dict[date, list[dict[str, Any]]] = {}
    for row in resolved_rows:
        res_by_cell.setdefault(row["obs_date"], []).append(row)

    out: dict[str, Any] = {"raw_rows": n_raw, "cells": 0, "cells_with_eligible": 0, "stuck_cells": 0,
                           "eligible_vintage_missing_cells": 0, "missing_vintage_cells": 0,
                           "missing_vintages": 0, "stuck": []}
    for obs_date in sorted(set(raw_by_cell) | set(res_by_cell)):
        out["cells"] += 1
        raws = raw_by_cell.get(obs_date, [])
        resolved = res_by_cell.get(obs_date, [])
        resolved_vintages = {r["vintage_date"] for r in resolved}
        missing = {vintage_of(r["pull_timestamp"]) for r in raws} - resolved_vintages
        if missing:
            out["missing_vintage_cells"] += 1
            out["missing_vintages"] += len(missing)
        eligible = sorted(
            (r for r in raws if is_daily_policy_capture(
                r.get("raw_payload"), r["series_id"], obs_date, r["pull_timestamp"])),
            key=lambda r: r["pull_timestamp"],
        )
        if not eligible:
            continue
        out["cells_with_eligible"] += 1
        first_vintage = vintage_of(eligible[0]["pull_timestamp"])
        live = [r for r in resolved if not r.get("retracted")]
        served = max(live, key=lambda r: r["vintage_date"]) if live else None
        if served is not None and served["vintage_date"] >= first_vintage:
            continue
        out["stuck_cells"] += 1
        dropped = first_vintage not in resolved_vintages
        out["eligible_vintage_missing_cells"] += int(dropped)
        if len(out["stuck"]) < max_examples:
            out["stuck"].append({
                "obs_date": obs_date.isoformat(),
                "served_vintage": served["vintage_date"].isoformat() if served else None,
                "served_value": served["value"] if served else None,
                "eligible_vintage": first_vintage.isoformat(),
                "eligible_series": eligible[0]["series_id"],
                "eligible_pull": eligible[0]["pull_timestamp"].isoformat(),
                "eligible_value": eligible[-1]["value"],
                "eligible_vintage_missing": dropped,
            })
    return out


_RAW_SQL = text("""
    SELECT rs.series_id, rs.obs_date, rs.pull_timestamp, rs.value, sc.name, rs.raw_payload
    FROM raw_series rs
    JOIN source_catalog sc ON sc.id = rs.source_id
    WHERE rs.series_id = :sid AND rs.pull_status = 'SUCCESS'
      AND rs.obs_date >= :since AND rs.obs_date < :until
""")

_RESOLVED_SQL = text("""
    SELECT rs.obs_date, rs.vintage_date, rs.value, (r.feature_id IS NOT NULL) AS retracted
    FROM resolved_series rs
    LEFT JOIN resolved_series_retractions r
      ON r.feature_id = rs.feature_id AND r.obs_date = rs.obs_date AND r.vintage_date = rs.vintage_date
    WHERE rs.feature_id = :fid AND rs.obs_date >= :since AND rs.obs_date < :until
""")

_RESOLVED_NO_RETRACTIONS_SQL = text("""
    SELECT rs.obs_date, rs.vintage_date, rs.value, FALSE AS retracted
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.obs_date >= :since AND rs.obs_date < :until
""")


def audit_feature(engine: Engine, feature_id: int, series: list[str], since: date, until: date, *,
                  timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
                  max_examples: int = DEFAULT_MAX_EXAMPLES) -> FeatureGap:
    out = FeatureGap(feature_id=feature_id, series=sorted(series))
    params = {"since": since, "until": until}
    try:
        with engine.connect() as conn, conn.begin():
            _readonly(conn, timeout_ms)
            out.feature = conn.execute(text("SELECT name FROM feature_registry WHERE id = :fid"),
                                       {"fid": feature_id}).scalar()
            raw: list[dict[str, Any]] = []
            for sid in out.series:
                raw.extend(
                    {"series_id": r[0], "obs_date": r[1], "pull_timestamp": r[2], "value": r[3],
                     "source_name": r[4], "raw_payload": r[5]}
                    for r in conn.execute(_RAW_SQL, {"sid": sid, **params})
                )
            has_retractions = conn.execute(
                text("SELECT to_regclass('resolved_series_retractions') IS NOT NULL")).scalar()
            sql = _RESOLVED_SQL if has_retractions else _RESOLVED_NO_RETRACTIONS_SQL
            resolved = [{"obs_date": r[0], "vintage_date": r[1], "value": r[2], "retracted": bool(r[3])}
                        for r in conn.execute(sql, {"fid": feature_id, **params})]
        for key, value in classify(raw, resolved, max_examples=max_examples).items():
            setattr(out, key, value)
    except Exception as exc:  # one feature's timeout must not hide the others
        out.error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
    return out


def map_series(engine: Engine, series: list[str]) -> tuple[dict[int, list[str]], list[str]]:
    """Group series by the feature the resolver would write them to (EntityMap, as the resolver does)."""
    from normalization.entity_map import EntityMap

    emap = EntityMap(engine)
    by_feature: dict[int, list[str]] = {}
    unmapped: list[str] = []
    for sid in series:
        fid = emap.get_feature_id(sid)
        if fid is None:
            unmapped.append(sid)
        else:
            by_feature.setdefault(int(fid), []).append(sid)
    return by_feature, unmapped


_TOTAL_KEYS = ("raw_rows", "cells", "cells_with_eligible", "stuck_cells",
               "eligible_vintage_missing_cells", "missing_vintage_cells", "missing_vintages")


def run(engine: Engine, series: list[str], since: date, until: date, *,
        timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS, max_examples: int = DEFAULT_MAX_EXAMPLES,
        allow_db_window: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    started = clock()
    check_window(started, allow_db_window)
    by_feature, unmapped = map_series(engine, series)
    gaps: list[dict[str, Any]] = []
    stopped = None
    for fid in sorted(by_feature):
        try:
            check_window(clock(), allow_db_window)
        except DbWindowRefused as exc:
            stopped = f"db_window: {exc}"
            break
        gaps.append(asdict(audit_feature(engine, fid, by_feature[fid], since, until,
                                         timeout_ms=timeout_ms, max_examples=max_examples)))
    totals = {k: sum(g[k] for g in gaps) for k in _TOTAL_KEYS}
    totals["features"] = len(gaps)
    totals["features_with_stuck"] = sum(1 for g in gaps if g["stuck_cells"])
    return {"generated_at": started.astimezone(timezone.utc).isoformat(), "read_only": True,
            "statement_timeout_ms": timeout_ms, "obs_since": since.isoformat(),
            "obs_until": until.isoformat(), "series_requested": len(series), "unmapped_series": unmapped,
            "totals": totals, "features": gaps, "stopped": stopped,
            "complete": stopped is None and len(gaps) == len(by_feature)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--series", action="append", default=[],
                   help="raw series id (repeatable); default: every daily-contract YF close/adj_close")
    p.add_argument("--since", type=date.fromisoformat, default=DEFAULT_SINCE,
                   help=f"inclusive obs_date floor (default {DEFAULT_SINCE})")
    p.add_argument("--until", type=date.fromisoformat, default=None,
                   help="exclusive obs_date ceiling (default: today UTC)")
    p.add_argument("--max-examples", type=int, default=DEFAULT_MAX_EXAMPLES,
                   help="stuck cells listed per feature")
    p.add_argument("--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS)
    p.add_argument("--allow-db-window", action="store_true",
                   help="run inside 03:30-10:30 UTC (test databases only)")
    p.add_argument("--db-url", default=None, help="database URL (default: db.get_engine())")
    args = p.parse_args(argv)
    until = args.until or datetime.now(timezone.utc).date()
    if until <= args.since:
        p.error("--until must be after --since")
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
    report = run(engine, args.series or default_series(), args.since, until,
                 timeout_ms=args.statement_timeout_ms, max_examples=args.max_examples,
                 allow_db_window=args.allow_db_window)
    json.dump(report, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    failed = [g["feature"] or g["feature_id"] for g in report["features"] if g["error"]]
    if failed or not report["complete"]:
        print(f"incomplete audit: errors in {failed}; stopped={report['stopped']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
