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
                     or continue inside the operational blackout windows. Watches total table growth and
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
    python -m scripts.people_events_backfill execute --form345 ... --out-dir ~/research/gd3 --batch-rows 50 --sleep 2 --baseline-bytes <pre-GD3 size> --expect-source-sha256 <plan source hash>
    python -m scripts.people_events_backfill verify  --form345 ... --out-dir ~/research/gd3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timedelta, timezone
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
from scripts import people_events_backfill_safety as G

DEFAULT_CODES = ("P", "S", "A")
# Expected on-disk cost per row (heap + TOAST + indexes), from the design doc
# estimate (0.8-1.0 KB/row) with margin. Outside this band the run stops.
BYTES_PER_ROW_BAND = (300.0, 2000.0)
GROWTH_CHECK_MIN_ROWS = 100_000
# The load + merge of the full parquet takes ~30 min before the first write;
# execute refuses to start unless this much time remains before 03:30Z.
MIN_MINUTES_BEFORE_WINDOW = 60
VERIFY_STATEMENT_TIMEOUT_MS = 600_000

_SIZE_SQL = """SELECT pg_total_relation_size('people_events')
    + pg_total_relation_size('people_event_revisions')
    + pg_total_relation_size('people_events_runs') AS bytes"""
_RUN_AUDIT_SQL = """
    SELECT run_id, mode, materializer_version, inputs, counts, status, started_at, finished_at, error
    FROM people_events_runs WHERE left(run_id, length(:prefix)) = :prefix
    ORDER BY length(run_id), run_id
"""
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


def minutes_until_window(now: datetime | None = None) -> float:
    """Minutes from ``now`` until the next 03:30Z (0 inside the window)."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    lo, hi = RO.BACKUP_WINDOW_UTC
    if lo <= now.time() < hi:
        return 0.0
    start = datetime.combine(now.date(), lo, tzinfo=timezone.utc)
    if now >= start:
        start += timedelta(days=1)
    return (start - now).total_seconds() / 60.0


def _write(path: Path, receipt: dict[str, Any]) -> None:
    G.atomic_json(path, D.to_jsonable(receipt))


def _rw_engine(url: str) -> Engine:
    options = (f"-c statement_timeout={G.STATEMENT_TIMEOUT_MS} -c lock_timeout={G.LOCK_TIMEOUT_MS} "
               "-c application_name=people_events_backfill")
    return create_engine(url, connect_args={"options": options}, pool_size=2, max_overflow=0, pool_pre_ping=True)


def _scalar(engine: Engine, sql: str) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(RO.guard_sql(sql))).scalar()


def _run_audit(engine: Engine, prefix: str, source_hash: str) -> dict:
    """One streamed full audit at startup/resume; never scan run history per batch.

    Validate JSON types before using counters: SQL casts accept string integers
    and can fail unpredictably on malformed state. Invalid runs remain evidence
    and make validate_resume refuse continuation without reconciliation.
    """
    audit = dict(batches=0, successful=0, inserted=0, written=0, invalid=0)
    digest = hashlib.sha256()
    required_counts = {"insert", "written", *G.ZERO_COUNT_KEYS}
    with engine.connect().execution_options(stream_results=True) as conn:
        for row in conn.execute(text(_RUN_AUDIT_SQL), {"prefix": prefix}).mappings():
            batch = audit["batches"]
            inputs, counts = row["inputs"], row["counts"]
            valid_inputs = (isinstance(inputs, dict) and set(inputs) == {"batch", "form345_sha256"}
                            and type(inputs["batch"]) is int and inputs["batch"] == batch
                            and inputs["form345_sha256"] == source_hash)
            valid_counts = (isinstance(counts, dict) and set(counts) == required_counts
                            and all(type(value) is int for value in counts.values())
                            and 1 <= counts["insert"] <= G.MAX_TRANSACTION_ROWS
                            and counts["written"] == counts["insert"]
                            and all(counts[key] == 0 for key in G.ZERO_COUNT_KEYS))
            completed = (isinstance(row["started_at"], datetime) and isinstance(row["finished_at"], datetime)
                         and row["finished_at"] >= row["started_at"] and row["error"] is None)
            valid = (row["run_id"] == f"{prefix}-b{batch:05d}" and valid_inputs and valid_counts
                     and row["mode"] == "backfill" and row["materializer_version"] == PIPELINE_VERSION
                     and row["status"] == "SUCCESS" and completed)
            audit["batches"] += 1
            audit["successful"] += row["status"] == "SUCCESS"
            for key, total in (("insert", "inserted"), ("written", "written")):
                if isinstance(counts, dict) and type(counts.get(key)) is int:
                    audit[total] += counts[key]
            if not valid:
                audit["invalid"] += 1
                continue
            if batch:
                digest.update(b"\n")
            digest.update(G.batch_audit_record(row["run_id"], inputs["batch"], counts))
    return {**audit, "progress_digest": digest.hexdigest()}


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
    ap.add_argument("--batch-rows", type=int, default=50, help="rows per transaction; hard maximum 50")
    ap.add_argument("--sleep", type=float, default=2.0, help="seconds between logical 20,000-row groups")
    ap.add_argument("--max-gb", type=float, default=8.0, help="stop if projected table growth exceeds this")
    ap.add_argument("--max-batches", type=int, help="stop after this many batches (staged runs)")
    ap.add_argument("--baseline-bytes", type=int, help="sum of all three table sizes at the pre-GD3 backup; required on first execute")
    ap.add_argument("--expect-source-sha256", help="approved full-history plan input hash; required on first execute")
    args = ap.parse_args(argv)
    if (args.batch_rows < 1 or args.sleep < 0 or not 0 < args.max_gb <= 8
            or (args.max_batches is not None and args.max_batches < 1)):
        ap.error("positive batch/batch-limit, nonnegative sleep and 0 < --max-gb <= 8 required")
    args.batch_rows = min(args.batch_rows, G.MAX_TRANSACTION_ROWS)
    codes = [code.strip().upper() for code in args.codes.split(",")]
    if sorted(codes) != sorted(DEFAULT_CODES):
        ap.error("GD3 scope is exactly P,S,A")
    args.codes = ",".join(sorted(codes))

    import os

    if args.db_url_env:
        url = os.environ[args.db_url_env]
    else:
        from config import settings

        url = settings.DB_URL
    RO.assert_db_window_open()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.mode == "execute":
        if args.quarters:
            # Dedup keys (owner-name collision splits, cross-accession merges)
            # depend on the loaded scope: a partial load could store a key a
            # later full load would split, i.e. duplicate acts. Full scope only.
            print("refusing --quarters in execute: the backfill must load the full history", file=sys.stderr)
            return 2
        if not (args.out_dir / G.MANIFEST_NAME).exists() and not args.expect_source_sha256:
            print("refusing first execute: --expect-source-sha256 from the reviewed plan is required", file=sys.stderr)
            return 2
        if not G.write_window_open():
            print("refusing to start: GD3 write blackout or within 60 minutes of nightly backup",
                  file=sys.stderr)
            return 2

    t0 = time.perf_counter()
    source_hash = sha256_file(args.form345)
    if args.expect_source_sha256 and source_hash != args.expect_source_sha256:
        raise RuntimeError("Form345 source does not match the reviewed plan hash")
    try:
        events, stats, stored = _load(args, url)
    except RO.WindowClosed as exc:
        _write(args.out_dir / f"{args.mode}_{stamp}.json",
               {"mode": args.mode, "status": "STOPPED_WINDOW", "error": str(exc)})
        print(json.dumps({"status": "STOPPED_WINDOW"}))
        return 4
    observed_at = datetime.now(timezone.utc)  # after every read
    if sha256_file(args.form345) != source_hash:
        raise RuntimeError("Form345 source changed while loading; refusing plan/execute/verify")
    stats["pit"] = M.pit_violations(events, pd.Timestamp(observed_at))
    plan = P.build_write_plan(events, stored, pd.Timestamp(observed_at))
    counts = P.plan_counts(plan).get("form4", {})
    receipt: dict[str, Any] = {
        "mode": args.mode, "pipeline_version": PIPELINE_VERSION, "observed_at": observed_at.isoformat(),
        "form345": str(args.form345), "form345_sha256": source_hash,
        "quarters": args.quarters, "codes": args.codes, "stats": stats, "plan": counts,
        "stored_rows_before": int(len(stored)), "load_s": round(time.perf_counter() - t0, 1),
    }

    if args.mode == "plan":
        receipt["expected_counts"] = expected_counts(events)
        receipt["size_estimate_gb"] = {"design_800_bytes_per_event": round(len(events) * 800 / 1e9, 3),
                                       "design_1000_bytes_per_event": round(len(events) * 1000 / 1e9, 3),
                                       "guard_2000_bytes_per_event": round(len(events) * 2000 / 1e9, 3)}
        out = args.out_dir / f"plan_{stamp}.json"
        out.write_text(json.dumps(D.to_jsonable(receipt), indent=2, sort_keys=True, default=str))
        print(json.dumps({"out": str(out), "plan": counts, "pit": stats["pit"]}, default=str))
        return 0

    if args.mode == "verify":
        # Precondition: before the backfill the table held no form4 rows from
        # another source (true on 2026-10-02: empty), so every planned act is a
        # sec_form345 row. Full-table aggregates need more than the 20 s default.
        engine = RO.readonly_engine(url, statement_timeout_ms=VERIFY_STATEMENT_TIMEOUT_MS)
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

    if (stats["pit"]["known_before_event"] or stats["pit"]["missing_known_at"]
            or stats["pit"]["known_after_observation"]):
        print("refusing to execute: PIT invariants violated in the plan", file=sys.stderr)
        return 3
    if not set(plan["op"]) <= {"insert", "unchanged"}:
        print("refusing execute: GD3 accepts insert/unchanged plans only (revision writes exceed row budget)",
              file=sys.stderr)
        return 3
    if not G.write_window_open():
        print("refusing execute after loading: GD3 write window closed", file=sys.stderr)
        return 4
    todo = plan[plan["op"] != "unchanged"].reset_index(drop=True)
    rows_planned = int((todo["op"] == "insert").sum())
    engine = _rw_engine(url)
    receipt.update({"rows_planned": rows_planned, "batch_rows": args.batch_rows,
                    "throttle_rows": G.THROTTLE_ROWS, "progress_path": str(args.out_dir / G.PROGRESS_NAME)})
    log_path = args.out_dir / f"execute_{stamp}.json"
    position = {k: i for i, k in enumerate(zip(events["channel"], events["dedup_key"]))}
    written = 0
    status = "DONE"
    run_id = None
    batches_before = global_before = 0
    baseline_bytes = None
    # One immutable baseline for every staged execution, one durable append
    # per committed batch, and one constant-size final receipt per invocation.
    try:
        with G.execution_lock(args.out_dir):
            bytes_now = int(_scalar(engine, _SIZE_SQL))
            if not (args.out_dir / G.MANIFEST_NAME).exists() and len(stored):
                raise RuntimeError("first GD3 execute requires an empty form4 scope")
            manifest, resumed = G.load_manifest(args.out_dir, G.scope_identity(receipt, expected_counts(events)),
                baseline_bytes=args.baseline_bytes, current_bytes=bytes_now, max_gb=args.max_gb, stamp=stamp)
            baseline_bytes = manifest["baseline_bytes"]
            batches_before, global_before, progress_digest = G.read_progress(
                args.out_dir / G.PROGRESS_NAME, manifest["run_prefix"])
            audit = _run_audit(engine, manifest["run_prefix"], source_hash)
            G.validate_resume(batches=batches_before, rows=global_before, database=audit,
                stored_rows=len(stored), plan_counts=counts, total_rows=len(events), progress_digest=progress_digest)
            receipt.update({"baseline_bytes": baseline_bytes, "bytes_before": bytes_now,
                            "resumed": resumed, "batches_before": batches_before})
            throttle_rows = 0
            for n, start in enumerate(range(0, len(todo), args.batch_rows)):
                if args.max_batches is not None and n >= args.max_batches:
                    status = "STOPPED_MAX_BATCHES"
                    break
                if not G.write_window_open():
                    status = "STOPPED_WINDOW"
                    break
                bytes_now = int(_scalar(engine, _SIZE_SQL))
                ok, info = growth_check(baseline_bytes, bytes_now, global_before + written, len(events), args.max_gb)
                if not ok or bytes_now - baseline_bytes >= int(args.max_gb * 1e9) - G.GROWTH_RESERVE_BYTES:
                    status = "STOPPED_GROWTH"
                    break
                batch = todo.iloc[start:start + args.batch_rows]
                rows = sorted({position[k] for k in zip(batch["channel"], batch["dedup_key"]) if k in position})
                ev_batch = events.iloc[rows]
                run_id = f"{manifest['run_prefix']}-b{batches_before + n:05d}"
                res = apply_write_plan(engine, ev_batch, batch, run_id=run_id, mode="backfill",
                    observed_at=observed_at, inputs={"form345_sha256": source_hash, "batch": batches_before + n},
                    before_transaction=G.require_write_entry_window, before_commit=G.require_write_window)
                if res["status"] != "SUCCESS" or int(res["counts"]["insert"]) != len(batch):
                    raise RuntimeError("GD3 writer did not commit the complete insert-only batch")
                written += int(res["counts"]["insert"])
                bytes_now = int(_scalar(engine, _SIZE_SQL))
                ok, info = growth_check(baseline_bytes, bytes_now, global_before + written, len(events), args.max_gb)
                G.append_progress(args.out_dir / G.PROGRESS_NAME, {"run_id": run_id,
                    "status": res["status"], "batch_rows": len(batch), **info})
                receipt["growth"] = info
                if not ok or bytes_now - baseline_bytes >= int(args.max_gb * 1e9) - G.GROWTH_RESERVE_BYTES:
                    status = "STOPPED_GROWTH"
                    break
                throttle_rows += len(batch)
                if throttle_rows >= G.THROTTLE_ROWS:
                    time.sleep(args.sleep)
                    throttle_rows = 0
    except G.WriteWindowClosed as exc:
        status = "STOPPED_WINDOW"
        receipt["error"] = str(exc)
        receipt["failed_run_id"] = run_id
    except BaseException as exc:  # noqa: BLE001 -- recorded, then re-raised
        status = "FAILED"
        receipt["error"] = repr(exc)[:2000]
        receipt["failed_run_id"] = run_id
        raise
    finally:
        receipt["status"] = status
        receipt["rows_written"] = written
        receipt["global_rows_written"] = global_before + written
        try:
            receipt["bytes_after"] = int(_scalar(engine, _SIZE_SQL))
        except Exception as exc:  # noqa: BLE001 -- never mask the original error
            receipt["bytes_after"] = None
            receipt["bytes_after_error"] = repr(exc)[:500]
        _write(log_path, receipt)
        engine.dispose()
    growth = None if receipt["bytes_after"] is None or baseline_bytes is None else receipt["bytes_after"] - baseline_bytes
    print(json.dumps({"out": str(log_path), "status": status, "rows_written": written,
                      "bytes_growth": growth}, default=str))
    return 0 if status == "DONE" else 4


if __name__ == "__main__":
    sys.exit(main())
