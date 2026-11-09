#!/usr/bin/env python3
"""Retract resolved cells whose value is another instrument's TIINGO price.

Dry run by default: reads only, reports counts, writes nothing. ``--apply``
needs ``--run-tag`` and the operator's explicit go-ahead. It only INSERTs
into ``resolved_series_retractions``; it never UPDATEs or DELETEs a row.

Background (``tiingo_symbol_contract``, 2026-11-09). The Tiingo puller used to
strip ``=F``/``=X``, so TIINGO's ``YF:CL=F:close`` is Colgate-Palmolive and
``YF:HG=F:close`` Hamilton Insurance. yfinance outranks TIINGO in the
resolver, but on dates yfinance has no row the TIINGO value won:
``cl_close`` served Colgate on 2,722 of 9,304 dates (every date 1990-01-02 to
2000-08-22, plus scattered 2026 dates) and ``copper`` served Hamilton on 7
dates in 2026. ``gold_full`` (fed by ``YF:GC=F:close``; Tiingo has no gold
futures) holds 313 TIINGO cells of Barrick Gold (29.78-64.14 against gold
near 4,180) from the ``YF:GOLD`` pattern collision fixed 2026-11-08, 7 of
them still the served value. The code fix keeps those raw rows out of every reader. The
cells already resolved are still served. There is no clean replacement for
them (yfinance has no row on those dates), which is the case retractions
exist for: point-in-time readers see "no data" from the retraction on, and
replays ``as_of`` earlier dates are reproduced exactly.

Scope: every feature statically fed (SEED + V2 mappings) by a ``YF:`` series
that has a TIINGO cutoff (``tiingo_symbol_contract.tiingo_cutoff``). Its
resolved cells with ``source_priority_used`` = TIINGO and ``obs_date`` after
the cutoff are retracted, whichever raw series produced them: for a symbol
Tiingo cannot serve, no TIINGO value is that instrument.

    python scripts/retract_foreign_tiingo_resolutions.py                  # dry run
    python scripts/retract_foreign_tiingo_resolutions.py --apply --run-tag tiingo-foreign-20261109
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from sqlalchemy.engine import Engine

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS
from tiingo_symbol_contract import TIINGO_SOURCE, tiingo_cutoff

DB_WINDOW_START = time(3, 30)
DB_WINDOW_END = time(10, 30)
STATEMENT_TIMEOUT_MS = 120_000
BATCH = 1000
REASON = ("wrong instrument for {feature} (fed by {series}): a TIINGO value dated after {cutoff} "
          "is another listing's price (tiingo_symbol_contract)")

_CANDIDATES_SQL = text("""
    SELECT rs.obs_date, rs.vintage_date
    FROM resolved_series rs
    WHERE rs.feature_id = :fid
      AND rs.source_priority_used = :src
      AND rs.obs_date > :cutoff
      AND NOT EXISTS (
          SELECT 1 FROM resolved_series_retractions rr
          WHERE rr.feature_id = rs.feature_id AND rr.obs_date = rs.obs_date
            AND rr.vintage_date = rs.vintage_date)
    ORDER BY rs.obs_date, rs.vintage_date
""")
_RETRACT_SQL = text("""
    INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, reason, run_tag)
    VALUES (:fid, :od, :vd, :reason, :tag)
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")


def scope(mappings: dict[str, str] | None = None) -> dict[str, tuple[str, date]]:
    """``feature -> (series_id, cutoff)`` for features fed by a series with a TIINGO cutoff.
    A feature fed by several such series keeps the earliest cutoff."""
    out: dict[str, tuple[str, date]] = {}
    merged = mappings if mappings is not None else {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
    for series_id, feature in sorted(merged.items()):
        cutoff = tiingo_cutoff(series_id) if isinstance(series_id, str) else None
        if cutoff is None:
            continue
        if feature not in out or cutoff < out[feature][1]:
            out[feature] = (series_id, cutoff)
    return out


def run(engine: Engine, *, apply: bool, run_tag: str | None,
        mappings: dict[str, str] | None = None) -> dict:
    report: dict = {"apply": apply, "run_tag": run_tag, "features": []}
    with engine.connect() as conn:
        src = conn.execute(text("SELECT id FROM source_catalog WHERE upper(name) = :n"),
                           {"n": TIINGO_SOURCE}).scalar()
        if src is None:
            raise RuntimeError("no TIINGO row in source_catalog")
        for feature, (series_id, cutoff) in scope(mappings).items():
            fid = conn.execute(text("SELECT id FROM feature_registry WHERE name = :n"),
                               {"n": feature}).scalar()
            entry = {"feature": feature, "feature_id": fid, "series_id": series_id,
                     "cutoff": cutoff.isoformat(), "cells": 0, "retracted": 0}
            report["features"].append(entry)
            if fid is None:
                entry["status"] = "NOT_REGISTERED"
                continue
            # date.min is not a valid Postgres date literal edge; 0001-01-01 is.
            cells = conn.execute(_CANDIDATES_SQL, {"fid": fid, "src": src,
                                                   "cutoff": max(cutoff, date(1, 1, 1))}).fetchall()
            entry["cells"] = len(cells)
            if cells:
                entry["obs_date_range"] = [str(cells[0][0])[:10], str(cells[-1][0])[:10]]
                entry["distinct_obs_dates"] = len({c[0] for c in cells})
            if not apply or not cells:
                entry["status"] = "PLANNED" if cells else "CLEAN"
                continue
            reason = REASON.format(feature=feature, series=series_id, cutoff=cutoff.isoformat())
            done = 0
            for i in range(0, len(cells), BATCH):
                with engine.begin() as tx:
                    if tx.dialect.name == "postgresql":
                        tx.execute(text("SELECT set_config('statement_timeout', :t, true)"),
                                   {"t": str(STATEMENT_TIMEOUT_MS)})
                    res = tx.execute(_RETRACT_SQL, [
                        {"fid": fid, "od": od, "vd": vd, "reason": reason, "tag": run_tag}
                        for od, vd in cells[i:i + BATCH]])
                    done += max(res.rowcount or 0, 0)
            entry["retracted"] = done
            entry["status"] = "RETRACTED"
    report["cells"] = sum(f["cells"] for f in report["features"])
    report["retracted"] = sum(f["retracted"] for f in report["features"])
    return report


def main(argv: list[str] | None = None, engine: Engine | None = None,
         now: datetime | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--apply", action="store_true", help="write the retractions (needs --run-tag)")
    p.add_argument("--run-tag", default=None)
    p.add_argument("--allow-db-window", action="store_true",
                   help="allow --apply inside 03:30-10:30 UTC (non-production databases)")
    args = p.parse_args(argv)
    if args.apply and not args.run_tag:
        print("--apply requires --run-tag", file=sys.stderr)
        return 2
    t = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).time()
    if args.apply and not args.allow_db_window and DB_WINDOW_START <= t <= DB_WINDOW_END:
        print("inside the 03:30-10:30 UTC DB window; run --apply outside it", file=sys.stderr)
        return 1
    if engine is None:
        from db import (
            get_engine,
        )

        engine = get_engine()
    print(json.dumps(run(engine, apply=args.apply, run_tag=args.run_tag), indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
