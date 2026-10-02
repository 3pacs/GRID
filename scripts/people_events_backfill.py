"""GD3: backfill Form 4 history into ``people_events`` from the SEC Form 3/4/5 parquet.

No provider calls: the only input is ``derived/nonderiv_transactions.parquet``
(built from the SEC DERA quarterly zips already on grid-svr). Requires the
``people_events_v2_20261001`` migration.

Modes
-----
``plan`` (default)   build the canonical events and the write plan against
                     what is already stored; write a plan receipt; no writes.
``execute``          apply the plan in throttled batches (``writer.apply_write_plan``,
                     one ``people_events_runs`` row per batch). Refuses to start
                     or continue inside 03:30-10:30Z. Watches table growth and
                     stops if the projected size leaves the declared band.
``verify``           read-only checks after execute: counts by year and code
                     equal the plan, ``known_at`` present on 100%, no row known
                     before its act (look-ahead), and a fresh plan is all
                     ``unchanged`` (idempotent).

Scope is chosen by ``--codes`` (default P,S,A: open-market buys, sales,
awards -- the owner-approved GD3 scope) and optionally ``--quarters``. The
plan never retracts (``complete_channels`` is empty): the parquet is a
historical source, not a full-scope view of the live channel.

Example (grid-svr, after #780 is deployed, outside 03:30-10:30Z)::

    python -m scripts.people_events_backfill plan    --form345 .../nonderiv_transactions.parquet --out-dir ~/research/gd3
    python -m scripts.people_events_backfill execute --form345 ... --out-dir ~/research/gd3 --batch-rows 20000 --sleep 2
    python -m scripts.people_events_backfill verify  --form345 ... --out-dir ~/research/gd3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from intelligence.people_events_pipeline import PIPELINE_VERSION
from intelligence.people_events_pipeline import adapters as A
from intelligence.people_events_pipeline import dryrun as D
from intelligence.people_events_pipeline import merge as M
from intelligence.people_events_pipeline import plan as P
from intelligence.people_events_pipeline import readonly as RO
from intelligence.people_events_pipeline import security as S

DEFAULT_CODES = ("P", "S", "A")
# Expected on-disk cost per row (heap + TOAST + indexes), from the design doc
# estimate (0.8-1.0 KB/row) with margin. Outside this band the run stops.
BYTES_PER_ROW_BAND = (300.0, 2000.0)
GROWTH_CHECK_MIN_ROWS = 100_000

_SIZE_SQL = "SELECT pg_total_relation_size('people_events') AS bytes"
_COUNT_BY_YEAR_CODE_SQL = """
    SELECT extract(year FROM known_at AT TIME ZONE 'UTC')::int AS year, transaction_code AS code, count(*) AS n
    FROM people_events
    WHERE channel = 'form4' AND source = 'sec_form345' AND superseded_at IS NULL AND retracted_at IS NULL
    GROUP BY 1, 2
"""
_INVARIANTS_SQL = """
    SELECT count(*) FILTER (WHERE known_at IS NULL) AS missing_known_at,
           count(*) FILTER (WHERE known_at < event_time) AS known_before_event,
           count(*) AS n
    FROM people_events
    WHERE channel = 'form4' AND source = 'sec_form345'
"""


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def build_events(form345: pd.DataFrame, codes: tuple[str, ...], identifiers: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Parquet rows -> resolved canonical Form 4 events restricted to ``codes``."""
    cands, skips = A.form4_from_form345(form345)
    merged = M.merge_candidates(cands)
    events = merged.events
    keep = events["transaction_code"].isin(codes)
    events = events[keep.to_numpy()].reset_index(drop=True)
    events = S.resolve_securities(events, identifiers)
    stats = {
        "candidates": int(len(cands)),
        "events_all_codes": int(len(merged.events)),
        "events_selected": int(len(events)),
        "skips": {k: int(v) for k, v in sorted(skips.items()) if v},
        "by_code": {str(k): int(v) for k, v in events["transaction_code"].value_counts().sort_index().items()},
        "pit": M.pit_violations(events),
    }
    return events, stats


def expected_counts(events: pd.DataFrame) -> dict[str, int]:
    years = pd.to_datetime(events["known_at"], utc=True).dt.year.astype(str)
    return {f"{y}|{c}": int(n) for (y, c), n in events.groupby([years, events["transaction_code"]]).size().items()}


def growth_check(bytes_before: int, bytes_now: int, rows_written: int, rows_planned: int,
                 max_gb: float) -> tuple[bool, dict[str, Any]]:
    """(ok, info): stop when bytes/row leaves the band or the projected total exceeds ``max_gb``."""
    info: dict[str, Any] = {"rows_written": rows_written, "bytes_growth": bytes_now - bytes_before}
    if rows_written < GROWTH_CHECK_MIN_ROWS:
        return True, info
    per_row = (bytes_now - bytes_before) / rows_written
    projected_gb = per_row * rows_planned / 1e9
    info.update({"bytes_per_row": round(per_row, 1), "projected_gb": round(projected_gb, 3)})
    lo, hi = BYTES_PER_ROW_BAND
    ok = lo <= per_row <= hi and projected_gb <= max_gb
    return ok, info


def _rw_engine(url: str) -> Engine:
    options = "-c statement_timeout=120000 -c lock_timeout=5000 -c application_name=people_events_backfill"
    return create_engine(url, connect_args={"options": options}, pool_size=2, max_overflow=0, pool_pre_ping=True)


def _scalar(engine: Engine, sql: str) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(RO.guard_sql(sql))).scalar()


def _load(args: argparse.Namespace, url: str) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    from scripts.people_events_dry_run import load_form345

    quarters = [q.strip() for q in args.quarters.split(",")] if args.quarters else None
    form345 = load_form345(args.form345, quarters, None, 0)
    identifiers = RO.read_security_identifiers(url)
    codes = tuple(c.strip().upper() for c in args.codes.split(",") if c.strip())
    events, stats = build_events(form345, codes, identifiers)
    stored = RO.read_stored_rows(url, ["form4"])
    return events, stats, stored


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=("plan", "execute", "verify"))
    ap.add_argument("--form345", type=Path, required=True)
    ap.add_argument("--quarters", help="comma-separated quarter labels (default: all)")
    ap.add_argument("--codes", default=",".join(DEFAULT_CODES))
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--db-url-env", help="env var holding the database URL (default: config.settings.DB_URL)")
    ap.add_argument("--batch-rows", type=int, default=20_000)
    ap.add_argument("--sleep", type=float, default=2.0, help="seconds between batches")
    ap.add_argument("--max-gb", type=float, default=8.0, help="stop if projected table growth exceeds this")
    ap.add_argument("--max-batches", type=int, help="stop after this many batches (staged runs)")
    args = ap.parse_args(argv)

    import os

    if args.db_url_env:
        url = os.environ[args.db_url_env]
    else:
        from config import settings

        url = settings.DB_URL
    RO.assert_db_window_open()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    t0 = time.perf_counter()
    events, stats, stored = _load(args, url)
    observed_at = datetime.now(timezone.utc)  # after every read
    plan = P.build_write_plan(events, stored, pd.Timestamp(observed_at))
    counts = P.plan_counts(plan).get("form4", {})
    receipt: dict[str, Any] = {
        "mode": args.mode, "pipeline_version": PIPELINE_VERSION, "observed_at": observed_at.isoformat(),
        "form345": str(args.form345), "form345_sha256": sha256_file(args.form345),
        "quarters": args.quarters, "codes": args.codes, "stats": stats, "plan": counts,
        "stored_rows_before": int(len(stored)), "load_s": round(time.perf_counter() - t0, 1),
    }

    if args.mode == "plan":
        receipt["expected_counts"] = expected_counts(events)
        out = args.out_dir / f"plan_{stamp}.json"
        out.write_text(json.dumps(D.to_jsonable(receipt), indent=2, sort_keys=True, default=str))
        print(json.dumps({"out": str(out), "plan": counts, "pit": stats["pit"]}, default=str))
        return 0

    if args.mode == "verify":
        engine = RO.readonly_engine(url)
        try:
            with engine.connect() as conn:
                RO._assert_read_only(conn)
                got = RO._query(conn, _COUNT_BY_YEAR_CODE_SQL, {})
                inv = RO._query(conn, _INVARIANTS_SQL, {}).iloc[0].to_dict()
        finally:
            engine.dispose()
        stored_counts = {f"{int(r.year)}|{r.code}": int(r.n) for r in got.itertuples()}
        want = expected_counts(events)
        mismatch = {k: (want.get(k, 0), stored_counts.get(k, 0)) for k in sorted(set(want) | set(stored_counts))
                    if want.get(k, 0) != stored_counts.get(k, 0)}
        ok = (not mismatch and int(inv["missing_known_at"]) == 0 and int(inv["known_before_event"]) == 0
              and set(plan["op"]) <= {"unchanged"})
        receipt.update({"invariants": {k: int(v) for k, v in inv.items()}, "count_mismatches": mismatch,
                        "fresh_plan_ops": sorted(set(plan["op"])), "verdict": "PASS" if ok else "FAIL"})
        out = args.out_dir / f"verify_{stamp}.json"
        out.write_text(json.dumps(D.to_jsonable(receipt), indent=2, sort_keys=True, default=str))
        print(json.dumps({"out": str(out), "verdict": receipt["verdict"], "invariants": receipt["invariants"],
                          "mismatches": len(mismatch)}, default=str))
        return 0 if ok else 1

    # execute
    from intelligence.people_events_pipeline.writer import apply_write_plan

    if stats["pit"]["known_before_event"] or stats["pit"]["missing_known_at"]:
        print("refusing to execute: PIT invariants violated in the plan", file=sys.stderr)
        return 3
    todo = plan[plan["op"] != "unchanged"].reset_index(drop=True)
    rows_planned = int((todo["op"] == "insert").sum())
    engine = _rw_engine(url)
    bytes_before = int(_scalar(engine, _SIZE_SQL))
    receipt.update({"bytes_before": bytes_before, "rows_planned": rows_planned, "batches": []})
    log_path = args.out_dir / f"execute_{stamp}.json"
    position = {k: i for i, k in enumerate(zip(events["channel"], events["dedup_key"]))}
    written = 0
    status = "DONE"
    try:
        for n, start in enumerate(range(0, len(todo), args.batch_rows)):
            if args.max_batches is not None and n >= args.max_batches:
                status = "STOPPED_MAX_BATCHES"
                break
            try:
                RO.assert_db_window_open()
            except RO.WindowClosed:
                status = "STOPPED_WINDOW"
                break
            batch = todo.iloc[start:start + args.batch_rows]
            rows = sorted({position[k] for k in zip(batch["channel"], batch["dedup_key"]) if k in position})
            ev_batch = events.iloc[rows]
            run_id = f"gd3-{stamp}-b{n:05d}"
            res = apply_write_plan(engine, ev_batch, batch, run_id=run_id, mode="backfill",
                                   observed_at=observed_at,
                                   inputs={"form345_sha256": receipt["form345_sha256"], "batch": n})
            written += int(res["counts"]["insert"])
            bytes_now = int(_scalar(engine, _SIZE_SQL))
            ok, info = growth_check(bytes_before, bytes_now, written, rows_planned, args.max_gb)
            receipt["batches"].append({"run_id": run_id, "status": res["status"], **info})
            log_path.write_text(json.dumps(D.to_jsonable(receipt), indent=2, sort_keys=True, default=str))
            if not ok:
                status = "STOPPED_GROWTH"
                break
            time.sleep(args.sleep)
    finally:
        receipt["status"] = status
        receipt["rows_written"] = written
        receipt["bytes_after"] = int(_scalar(engine, _SIZE_SQL))
        log_path.write_text(json.dumps(D.to_jsonable(receipt), indent=2, sort_keys=True, default=str))
        engine.dispose()
    print(json.dumps({"out": str(log_path), "status": status, "rows_written": written,
                      "bytes_growth": receipt["bytes_after"] - bytes_before}, default=str))
    return 0 if status == "DONE" else 4


if __name__ == "__main__":
    sys.exit(main())
