#!/usr/bin/env python3
"""Replace the legacy GDELT daily features with a history rebuilt from GDELT's daily files.

Slice 2 of moving GDELT onto its bulk files; the audit and the choice are in
docs/audits/2026-11-16-gdelt-history-lookahead.md (3pacs/GRID#1233). Anik
chose option B on 2026-11-16: copy the legacy rows to a legacy table, delete
them from the live tables, rebuild the history dated D+1 or later, take a
snapshot first, and keep the PIT rule that ``release_date`` never equals
``obs_date``.

Two steps, the first read-only:

``plan``
    Reads the legacy set and builds every day from 2013-04-01 to 2026-03-19
    (the loader, ``ingestion/altdata/gdelt_bulk.py``, owns 2026-03-20 on).
    Writes nothing to the database; writes a plan JSON.

    * Legacy set: every ``resolved_series`` row of the 13 GDELT daily
      features (the 12 of ``gdelt_bulk.FEATURES`` plus ``gdelt_article_count``)
      with ``release_date <= obs_date`` (on 2026-11-16 all 23,270 of them;
      the loader's rows are dated D+1 and never match), and every
      ``gdelt_daily_summary`` row up to 2026-03-19 with no ``day_basis``
      (1,878, grouped by SQLDATE). Their ids and a sha256 of them go into
      the plan, so ``apply`` refuses if the set changed.
    * A day comes from GDELT 1.0's daily export ``YYYYMMDD.export.CSV.zip``,
      read from ``--archive`` (grid-svr ``/data/gdelt/v1/events``) or fetched
      when absent. Its publication instant is the ``Last-Modified`` of the
      file on data.gdeltproject.org (a HEAD per day; the archive's mtimes are
      download times and are never used). When GDELT has no daily file
      (2025-06-14 .. 07-01 measured 404), the day is built from the 96
      GDELT 2.0 files stamped D, fetched, dated by the latest of their
      ``Last-Modified``; a slot that is missing leaves the day out.
    * ``release_date = vintage_date = gdelt_bulk.release_day(D, published)``:
      the later of D+1 and the publication date. A day with no publication
      instant is left out, never dated by a guess.

``apply --plan P --run-tag T --snapshot-dir DIR``
    Refused unless the legacy set still hashes to the plan's. Then:

    1. Snapshot: the legacy ``resolved_series`` and ``gdelt_daily_summary``
       rows to gzip CSV files in DIR, with their sha256 and row counts
       checked before anything is written.
    2. One transaction: copy the legacy rows into
       ``resolved_series_gdelt_legacy_20261116`` and
       ``gdelt_daily_summary_gdelt_legacy_20261116`` (tables, not views;
       refused if either exists), delete them from the live tables, insert
       the rebuilt summary rows (``day_basis`` ``dateadded_v1`` or
       ``dateadded_v2``) and feature points (source ``GDELT_BULK``, ``ON
       CONFLICT DO NOTHING``), and check that no row of the 13 features is
       left with ``release_date <= obs_date`` before committing.

    ``gdelt_article_count`` is deleted and not rebuilt: its source is the
    GDELT 1.0 GKG, which slice 3 replaces under ``gkg_*`` names.

Never touched: ``gdelt_events``, ``offline_research_proof.py``, the decision
journal, ``oracle_predictions``. No LLM. All SQL is parameterized; the only
identifiers formatted into SQL are the constants below.

Usage (grid-svr)::

    python scripts/rebuild_gdelt_history.py plan --out /data/gdelt/rebuild/plan.json
    python scripts/rebuild_gdelt_history.py apply --plan /data/gdelt/rebuild/plan.json \\
        --run-tag gdelt-history-20261116 --snapshot-dir /data/gdelt/rebuild/snapshot
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import sys
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.altdata import gdelt_bulk as gb

FIRST_DAY = date(2013, 4, 1)       # first GDELT 1.0 daily file with DATEADDED and SOURCEURL
LAST_DAY = date(2026, 3, 19)       # the loader starts 2026-03-20
V1_URL = "https://data.gdeltproject.org/events/{day}.export.CSV.zip"
DEFAULT_ARCHIVE = Path("/data/gdelt/v1/events")
LEGACY_FEATURES: tuple[str, ...] = (*gb.FEATURES, "gdelt_article_count")
RS_LEGACY_TABLE = "resolved_series_gdelt_legacy_20261116"
SUMMARY_LEGACY_TABLE = "gdelt_daily_summary_gdelt_legacy_20261116"
PLAN_VERSION = 1

Fetch = Callable[[str, bool], gb.Fetched]   # (url, head_only) -> Fetched


class RebuildRefused(RuntimeError):
    """A precondition failed; nothing was written."""


# --- fetching -------------------------------------------------------------------------


def _default_fetch(url: str, head_only: bool) -> gb.Fetched:
    import requests

    fetched = datetime.now(timezone.utc)
    call = requests.head if head_only else requests.get
    resp = call(url, timeout=gb.HTTP_TIMEOUT_S, headers={"User-Agent": gb.USER_AGENT})
    return gb.Fetched(resp.status_code, b"" if head_only else resp.content,
                      resp.headers.get("Last-Modified"), fetched)


def _last_modified(got: gb.Fetched) -> datetime | None:
    when, basis = gb.published_at(got.last_modified, got.fetched_at)
    return when if basis == "last_modified" else None


# --- one day --------------------------------------------------------------------------


def build_day(day: date, archive: Path | None, fetch: Fetch = _default_fetch) -> dict[str, Any]:
    """One day's values and dating, or a ``status`` saying why it is left out."""
    key = day.strftime("%Y%m%d")
    url = V1_URL.format(day=key)
    local = archive / f"{key}.export.CSV.zip" if archive else None
    head = fetch(url, True)
    if head.status == 200:
        published = _last_modified(head)
        if published is None:
            return {"day": day.isoformat(), "status": "undated", "source": "v1"}
        if local is not None and local.exists():
            data, origin = local.read_bytes(), "archive"
        else:
            got = fetch(url, False)
            if got.status != 200:
                return {"day": day.isoformat(), "status": f"http_{got.status}", "source": "v1"}
            data, origin = got.body, "fetched"
        parsed = gb.parse_daily_v1(data, day)
        return _day_record(day, parsed.aggregate, published, "dateadded_v1", origin,
                           files=1, rejected=parsed.rejected, mismatch=parsed.dateadded_mismatch,
                           md5=hashlib.md5(data).hexdigest())
    if head.status != 404:
        return {"day": day.isoformat(), "status": f"http_{head.status}", "source": "v1"}

    # No daily file at GDELT: the 96 GDELT 2.0 files of the day, all or nothing.
    agg = gb.FileAggregate()
    latest: datetime | None = None
    rejected = mismatch = 0
    digest = hashlib.md5()
    for stamp in gb.day_stamps(day):
        got = fetch(gb.export_url(stamp), False)
        if got.status != 200:
            return {"day": day.isoformat(), "status": "missing", "source": "v2",
                    "first_missing": gb.format_stamp(stamp)}
        published = _last_modified(got)
        if published is None:
            return {"day": day.isoformat(), "status": "undated", "source": "v2"}
        parsed = gb.parse_export(got.body, stamp)
        agg.add(parsed.aggregate)
        rejected += parsed.rejected
        mismatch += parsed.dateadded_mismatch
        digest.update(got.body)
        latest = published if latest is None else max(latest, published)
    assert latest is not None
    return _day_record(day, agg, latest, gb.DAY_BASIS, "fetched", files=gb.SLOTS_PER_DAY,
                       rejected=rejected, mismatch=mismatch, md5=digest.hexdigest())


def _day_record(day: date, agg: gb.FileAggregate, published: datetime, basis: str, origin: str, *,
                files: int, rejected: int, mismatch: int, md5: str) -> dict[str, Any]:
    release = gb.release_day(day, published)
    return {
        "day": day.isoformat(), "status": "ok", "source": basis, "origin": origin,
        "published_at": published.astimezone(timezone.utc).isoformat(), "release_date": release.isoformat(),
        "files": files, "rejected": rejected, "dateadded_mismatch": mismatch, "md5": md5,
        "aggregate": agg.__dict__, "values": gb.feature_values(agg),
    }


def _build_day_job(job: tuple[str, str | None]) -> dict[str, Any]:
    day, archive = job
    try:
        return build_day(date.fromisoformat(day), Path(archive) if archive else None)
    except Exception as exc:  # noqa: BLE001 -- one bad day is reported, not fatal to the plan
        return {"day": day, "status": "error", "error": f"{type(exc).__name__}: {exc}"}


# --- legacy set -----------------------------------------------------------------------


def _legacy_ids(conn: Connection) -> list[int]:
    return [r[0] for r in conn.execute(text(
        "SELECT r.id FROM resolved_series r JOIN feature_registry f ON f.id = r.feature_id "
        "WHERE f.name = ANY(:names) AND r.release_date <= r.obs_date ORDER BY r.id"),
        {"names": list(LEGACY_FEATURES)})]


def _legacy_summary_dates(conn: Connection) -> list[date]:
    return [r[0] for r in conn.execute(text(
        "SELECT summary_date FROM gdelt_daily_summary WHERE summary_date <= :last AND day_basis IS NULL "
        "ORDER BY summary_date"), {"last": LAST_DAY})]


def _hash(items: list[Any]) -> str:
    return hashlib.sha256("\n".join(str(i) for i in items).encode()).hexdigest()


def legacy_set(conn: Connection) -> dict[str, Any]:
    ids = _legacy_ids(conn)
    dates = _legacy_summary_dates(conn)
    by_feature = {r[0]: r[1] for r in conn.execute(text(
        "SELECT f.name, count(*) FROM resolved_series r JOIN feature_registry f ON f.id = r.feature_id "
        "WHERE r.id = ANY(:ids) GROUP BY f.name ORDER BY f.name"), {"ids": ids})}
    referenced = 0
    if conn.execute(text("SELECT to_regclass('astrogrid.price_close_receipt')")).scalar() is not None:
        referenced = conn.execute(text(
            "SELECT count(*) FROM astrogrid.price_close_receipt WHERE resolved_series_id = ANY(:ids)"),
            {"ids": ids}).scalar_one()
    retracted = 0
    if conn.execute(text("SELECT to_regclass('resolved_series_retractions')")).scalar() is not None:
        retracted = conn.execute(text(
            "SELECT count(*) FROM resolved_series_retractions x JOIN resolved_series r "
            "ON (r.feature_id, r.obs_date, r.vintage_date) = (x.feature_id, x.obs_date, x.vintage_date) "
            "WHERE r.id = ANY(:ids)"), {"ids": ids}).scalar_one()
    return {"rows": len(ids), "ids_sha256": _hash(ids), "by_feature": by_feature,
            "summary_rows": len(dates), "summary_sha256": _hash(dates),
            "referenced_elsewhere": referenced + retracted, "ids": ids,
            "summary_dates": [d.isoformat() for d in dates]}


# --- plan -----------------------------------------------------------------------------


def plan(engine: Engine, *, archive: Path | None, first: date = FIRST_DAY, last: date = LAST_DAY,
         workers: int = 8, fetch: Fetch | None = None) -> dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        legacy = legacy_set(conn)
        legacy_values = {(r[0], r[1].isoformat()): r[2] for r in conn.execute(text(
            "SELECT f.name, r.obs_date, r.value FROM resolved_series r JOIN feature_registry f "
            "ON f.id = r.feature_id WHERE r.id = ANY(:ids)"), {"ids": legacy["ids"]})}
    days = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    if fetch is not None or workers <= 1:
        records = [build_day(date.fromisoformat(d), archive, fetch or _default_fetch) for d in days]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            records = list(pool.map(_build_day_job, [(d, str(archive) if archive else None) for d in days],
                                    chunksize=4))
    ok = [r for r in records if r["status"] == "ok"]
    lags = sorted((date.fromisoformat(r["release_date"]) - date.fromisoformat(r["day"])).days for r in ok)
    ratios = sorted(
        r["values"]["gdelt_total_events"] / legacy_values[("gdelt_total_events", r["day"])]
        for r in ok if legacy_values.get(("gdelt_total_events", r["day"])))
    return {
        "plan_version": PLAN_VERSION, "made_at": datetime.now(timezone.utc).isoformat(),
        "first_day": first.isoformat(), "last_day": last.isoformat(), "legacy": legacy,
        "days": records,
        "report": {
            "days": len(records), "days_ok": len(ok),
            "days_by_source": _count(r.get("source") for r in ok),
            "days_left_out": {r["day"]: r["status"] for r in records if r["status"] != "ok"},
            "points_to_write": sum(len(r["values"]) for r in ok),
            "release_lag_days": {"min": lags[0], "median": lags[len(lags) // 2], "max": lags[-1],
                                 "counts": _count(lags)} if lags else None,
            "rows_rejected": sum(r["rejected"] for r in ok),
            "dateadded_mismatch": sum(r["dateadded_mismatch"] for r in ok),
            "total_events_vs_legacy": _quantiles(ratios),
        },
    }


def _count(items: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for i in items:
        out[str(i)] = out.get(str(i), 0) + 1
    return dict(sorted(out.items()))


def _quantiles(xs: list[float]) -> dict[str, float] | None:
    if not xs:
        return None
    pick = lambda q: round(xs[min(len(xs) - 1, int(q * len(xs)))], 4)
    return {"n": len(xs), "p05": pick(0.05), "p50": pick(0.5), "p95": pick(0.95), "max": round(xs[-1], 4)}


# --- apply ----------------------------------------------------------------------------


def snapshot(conn: Connection, legacy: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, sql, params, want in (
        ("resolved_series", "SELECT * FROM resolved_series WHERE id = ANY(:ids) ORDER BY id",
         {"ids": legacy["ids"]}, legacy["rows"]),
        ("gdelt_daily_summary", ("SELECT * FROM gdelt_daily_summary WHERE summary_date = ANY(:d) "
                                 "ORDER BY summary_date"), {"d": [date.fromisoformat(d) for d in legacy["summary_dates"]]},
         legacy["summary_rows"]),
    ):
        path = out_dir / f"{name}_gdelt_legacy_20261116.csv.gz"
        if path.exists():
            raise RebuildRefused(f"snapshot {path} exists; move it aside, a snapshot is never overwritten")
        result = conn.execute(text(sql), params)
        n = 0
        with gzip.open(path, "wt", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(result.keys())
            for row in result:
                writer.writerow([json.dumps(v) if isinstance(v, dict | list) else v for v in row])
                n += 1
        if n != want:
            raise RebuildRefused(f"snapshot {name}: wrote {n} rows, expected {want}")
        files[name] = {"path": str(path), "rows": n, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return files


def apply(engine: Engine, the_plan: dict[str, Any], *, run_tag: str, snapshot_dir: Path,
          statement_timeout_ms: int = 600_000) -> dict[str, Any]:
    if the_plan.get("plan_version") != PLAN_VERSION:
        raise RebuildRefused("plan version mismatch; re-run plan")
    planned = the_plan["legacy"]
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        legacy = legacy_set(conn)
        if (legacy["ids_sha256"], legacy["summary_sha256"]) != (planned["ids_sha256"], planned["summary_sha256"]):
            raise RebuildRefused(
                f"legacy set changed since the plan ({legacy['rows']} rows / {legacy['summary_rows']} summary "
                f"rows now, {planned['rows']} / {planned['summary_rows']} planned); re-run plan")
        if legacy["referenced_elsewhere"]:
            raise RebuildRefused(f"{legacy['referenced_elsewhere']} legacy rows are referenced elsewhere")
        snap = snapshot(conn, legacy, snapshot_dir)

    ok = [d for d in the_plan["days"] if d["status"] == "ok"]
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(statement_timeout_ms)})
        for table in (RS_LEGACY_TABLE, SUMMARY_LEGACY_TABLE):
            if conn.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar() is not None:
                raise RebuildRefused(f"{table} exists; the rebuild has run before")
        ids = legacy["ids"]
        dates = [date.fromisoformat(d) for d in legacy["summary_dates"]]
        conn.execute(text(
            f"CREATE TABLE {RS_LEGACY_TABLE} AS SELECT r.*, now() AS copied_at, "
            "CAST(:tag AS TEXT) AS run_tag FROM resolved_series r WHERE r.id = ANY(:ids)"),
            {"tag": run_tag, "ids": ids})
        conn.execute(text(
            f"CREATE TABLE {SUMMARY_LEGACY_TABLE} AS SELECT s.*, now() AS copied_at, "
            "CAST(:tag AS TEXT) AS run_tag FROM gdelt_daily_summary s WHERE s.summary_date = ANY(:d)"),
            {"tag": run_tag, "d": dates})
        copied = conn.execute(text(f"SELECT count(*) FROM {RS_LEGACY_TABLE}")).scalar_one()
        copied_s = conn.execute(text(f"SELECT count(*) FROM {SUMMARY_LEGACY_TABLE}")).scalar_one()
        if (copied, copied_s) != (len(ids), len(dates)):
            raise RebuildRefused(f"copy counts {copied}/{copied_s} != {len(ids)}/{len(dates)}")
        conn.execute(text(
            f"COMMENT ON TABLE {RS_LEGACY_TABLE} IS 'Legacy GDELT daily feature rows (release_date = obs_date), "
            "removed from resolved_series by scripts/rebuild_gdelt_history.py, 3pacs/GRID#1233 option B. "
            "Evidence only: never read by models.'"))
        conn.execute(text(
            f"COMMENT ON TABLE {SUMMARY_LEGACY_TABLE} IS 'Legacy SQLDATE-grouped gdelt_daily_summary rows, "
            "replaced by scripts/rebuild_gdelt_history.py, 3pacs/GRID#1233 option B.'"))
        deleted = conn.execute(text("DELETE FROM resolved_series WHERE id = ANY(:ids)"), {"ids": ids}).rowcount
        deleted_s = conn.execute(text("DELETE FROM gdelt_daily_summary WHERE summary_date = ANY(:d)"),
                                 {"d": dates}).rowcount
        if (deleted, deleted_s) != (len(ids), len(dates)):
            raise RebuildRefused(f"delete counts {deleted}/{deleted_s} != {len(ids)}/{len(dates)}")

        src, feat_ids = gb.GdeltBulkLoader(engine)._registry(conn)
        summaries = written = skipped = 0
        for d in ok:
            agg = gb.FileAggregate.from_json(d["aggregate"])
            day = date.fromisoformat(d["day"])
            release = date.fromisoformat(d["release_date"])
            if release <= day:
                raise RebuildRefused(f"{d['day']}: release {release} is not after the observed day")
            values = d["values"]
            hit = conn.execute(text(
                "INSERT INTO gdelt_daily_summary (summary_date, total_events, conflict_count, cooperation_count, "
                "avg_goldstein, avg_tone, total_mentions, country_events, published_at, day_basis, files_loaded, "
                "files_missing) VALUES (:d, :total, :conflict, :coop, :gold, :tone, :mentions, "
                "CAST(:countries AS JSONB), :pub, :basis, :files, 0) ON CONFLICT (summary_date) DO NOTHING "
                "RETURNING summary_date"),
                {"d": day, "total": agg.events, "conflict": agg.conflict, "coop": agg.cooperation,
                 "gold": values.get("gdelt_avg_goldstein"), "tone": values.get("gdelt_avg_tone"),
                 "mentions": agg.mentions,
                 "countries": json.dumps({code: agg.countries[k] for k, code in gb.TRACKED_COUNTRIES.items()}),
                 "pub": datetime.fromisoformat(d["published_at"]), "basis": d["source"],
                 "files": d["files"]}).scalar()
            summaries += hit is not None
            for name, value in values.items():
                row = conn.execute(text(
                    "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                    "source_priority_used) VALUES (:fid, :od, :rel, :rel, :val, :src) "
                    "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING RETURNING id"),
                    {"fid": feat_ids[name], "od": day, "rel": release, "val": value, "src": src}).scalar()
                if row is None:
                    skipped += 1
                else:
                    written += 1
        left = conn.execute(text(
            "SELECT count(*) FROM resolved_series r JOIN feature_registry f ON f.id = r.feature_id "
            "WHERE f.name = ANY(:names) AND r.release_date <= r.obs_date"),
            {"names": list(LEGACY_FEATURES)}).scalar_one()
        if left:
            raise RebuildRefused(f"{left} rows would still have release_date <= obs_date; rolled back")
    return {"run_tag": run_tag, "snapshot": snap, "legacy_rows_copied": copied, "legacy_rows_deleted": deleted,
            "legacy_summary_copied": copied_s, "legacy_summary_deleted": deleted_s,
            "summary_rows_written": summaries, "points_written": written, "points_skipped_existing": skipped,
            "days_rebuilt": len(ok), "rows_left_same_day": left}


# --- CLI ------------------------------------------------------------------------------


def _engine(url: str | None) -> Engine:
    if url:
        return create_engine(url)
    from db import get_engine

    return get_engine()


def main(argv: list[str] | None = None, engine: Engine | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db-url", help="SQLAlchemy URL (default: db.get_engine())")
    sub = p.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("plan", help="read-only: build the days, write a plan JSON")
    pl.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    pl.add_argument("--first", type=date.fromisoformat, default=FIRST_DAY)
    pl.add_argument("--last", type=date.fromisoformat, default=LAST_DAY)
    pl.add_argument("--workers", type=int, default=8)
    pl.add_argument("--out", type=Path, required=True)
    ap = sub.add_parser("apply", help="WRITE: snapshot, copy, delete, rebuild (one transaction)")
    ap.add_argument("--plan", type=Path, required=True)
    ap.add_argument("--run-tag", required=True)
    ap.add_argument("--snapshot-dir", type=Path, required=True)
    args = p.parse_args(argv)
    if args.cmd == "plan" and args.last > LAST_DAY:
        print(f"--last must be <= {LAST_DAY}: the loader owns later days", file=sys.stderr)
        return 2
    engine = engine or _engine(args.db_url)
    try:
        if args.cmd == "plan":
            result = plan(engine, archive=args.archive, first=args.first, last=args.last, workers=args.workers)
            args.out.write_text(json.dumps(result, default=str))
            shown = {k: v for k, v in result["legacy"].items() if k not in ("ids", "summary_dates")}
            print(json.dumps({"out": str(args.out), "legacy": shown, "report": result["report"]}, indent=1))
            return 0 if result["report"]["days_ok"] else 1
        result = apply(engine, json.loads(args.plan.read_text()), run_tag=args.run_tag,
                       snapshot_dir=args.snapshot_dir)
        print(json.dumps(result, indent=1, default=str))
        return 0
    except RebuildRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
