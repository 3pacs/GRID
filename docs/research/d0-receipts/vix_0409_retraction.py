#!/usr/bin/env python3
"""One-off: retract the 2026-04-09 yfinance vintage of ``vix_spot`` (append-only).

Approved by the operator 2026-11-13 ("Yes, after 10:30Z").

Background:
- The yfinance pull of 2026-04-09 wrote 9,134 ``vix_spot`` rows. 1,640 of them are
  more than 5 % off the other sources (2025-07-07: 33.43 against 17.79).
- The 2026-04-10 vintage supersedes every row. Receipts are in #1142,
  ``docs/research/d0-receipts/vintage_20260409*``.

This is bookkeeping. ``store/pit.py`` hides a retracted row only for ``as_of`` on or
after the retraction date, so replays at earlier ``as_of`` are unchanged.

Safety:
- Only INSERTs into ``resolved_series_retractions``; never UPDATE or DELETE.
- Refuses to run between 03:30 and 10:30 UTC (the backup/DB window).
- One write transaction with ``lock_timeout`` and ``statement_timeout``. It rolls
  back unless the candidate count equals ``--expect-rows`` and every candidate ends
  up retracted.
- Dry run unless ``--apply``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, time, timezone
from typing import Any, Callable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

FEATURE = "vix_spot"
SOURCE = "yfinance"
VINTAGE = date(2026, 4, 9)
DB_WINDOW_START = time(3, 30)
DB_WINDOW_END = time(10, 30)
REASON = (
    "2026-04-09 yfinance pull of ^VIX: 1,640 of 9,134 vix_spot rows in this vintage are more than 5 % off "
    "the other sources (e.g. 2025-07-07 33.43 vs 17.79); whole vintage retracted; superseded by 2026-04-10 "
    "(3pacs/GRID#1142 d0-receipts/vintage_20260409)"
)

_IDS = text("""
    SELECT (SELECT id FROM feature_registry WHERE name = :feature) AS fid,
           (SELECT id FROM source_catalog WHERE name = :source) AS sid
""")
_COUNTS = text("""
    SELECT count(*) AS candidates,
           count(*) FILTER (WHERE EXISTS (
               SELECT 1 FROM resolved_series_retractions x
               WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date
                 AND x.vintage_date = rs.vintage_date)) AS already_retracted,
           count(*) FILTER (WHERE rs.source_priority_used <> :sid) AS other_source_rows
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.vintage_date = :vintage
""")
_INSERT = text("""
    INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, reason, run_tag)
    SELECT rs.feature_id, rs.obs_date, rs.vintage_date, :reason, :run_tag
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.vintage_date = :vintage AND rs.source_priority_used = :sid
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")
_TAGGED = text("""
    SELECT count(*) FROM resolved_series_retractions WHERE run_tag = :run_tag
""")
_SERVED = text("""
    SELECT count(*) FROM (
      SELECT DISTINCT ON (rs.obs_date) rs.obs_date, rs.vintage_date
      FROM resolved_series rs
      WHERE rs.feature_id = :fid
        AND NOT EXISTS (SELECT 1 FROM resolved_series_retractions x
                        WHERE x.feature_id = rs.feature_id AND x.obs_date = rs.obs_date
                          AND x.vintage_date = rs.vintage_date)
      ORDER BY rs.obs_date, rs.vintage_date DESC
    ) live WHERE live.vintage_date = :vintage
""")


class Refused(RuntimeError):
    pass


def check_window(now: datetime) -> None:
    t = now.astimezone(timezone.utc).time()
    if DB_WINDOW_START <= t <= DB_WINDOW_END:
        raise Refused(f"{now.astimezone(timezone.utc).isoformat()} is inside the 03:30-10:30 UTC DB window")


def _limits(conn: Connection, *, read_only: bool) -> None:
    if read_only:
        conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SELECT set_config('statement_timeout', '120000', true)"))
    conn.execute(text("SELECT set_config('lock_timeout', '10000', true)"))


def _ids(conn: Connection) -> tuple[int, int]:
    row = conn.execute(_IDS, {"feature": FEATURE, "source": SOURCE}).one()
    if row.fid is None or row.sid is None:
        raise Refused(f"feature {FEATURE!r} or source {SOURCE!r} not found")
    return int(row.fid), int(row.sid)


def _counts(conn: Connection, fid: int, sid: int) -> dict[str, int]:
    r = conn.execute(_COUNTS, {"fid": fid, "sid": sid, "vintage": VINTAGE}).one()
    return {"candidates": int(r.candidates), "already_retracted": int(r.already_retracted),
            "other_source_rows": int(r.other_source_rows)}


def run(engine: Engine, *, apply: bool, run_tag: str, expect_rows: int,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    check_window(now())
    out: dict[str, Any] = {"feature": FEATURE, "source": SOURCE, "vintage": VINTAGE.isoformat(),
                           "run_tag": run_tag, "apply": apply, "expect_rows": expect_rows,
                           "started_at": now().isoformat(timespec="seconds")}
    with engine.connect() as conn:
        with conn.begin():
            _limits(conn, read_only=True)
            fid, sid = _ids(conn)
            before = _counts(conn, fid, sid)
            served_before = int(conn.execute(_SERVED, {"fid": fid, "vintage": VINTAGE}).scalar_one())
    out.update(feature_id=fid, source_id=sid, before=before, served_from_vintage_before=served_before)
    if before["other_source_rows"]:
        raise Refused(f"vintage holds {before['other_source_rows']} rows from another source")
    if before["candidates"] != expect_rows:
        raise Refused(f"expected {expect_rows} candidate rows, found {before['candidates']}")
    if not apply:
        out["result"] = "DRY_RUN"
        return out

    check_window(now())
    with engine.connect() as conn:
        with conn.begin():
            _limits(conn, read_only=False)
            again = _counts(conn, fid, sid)
            if again["candidates"] != expect_rows:
                raise Refused(f"candidate count changed to {again['candidates']}")
            inserted = conn.execute(_INSERT, {"fid": fid, "sid": sid, "vintage": VINTAGE,
                                              "reason": REASON, "run_tag": run_tag}).rowcount
            after_txn = _counts(conn, fid, sid)
            if after_txn["already_retracted"] != expect_rows:
                raise Refused(f"only {after_txn['already_retracted']} of {expect_rows} retracted; rolled back")
    out["inserted"] = int(inserted)

    with engine.connect() as conn:
        with conn.begin():
            _limits(conn, read_only=True)
            out["after"] = _counts(conn, fid, sid)
            out["tagged"] = int(conn.execute(_TAGGED, {"run_tag": run_tag}).scalar_one())
            out["served_from_vintage_after"] = int(conn.execute(_SERVED, {"fid": fid, "vintage": VINTAGE}).scalar_one())
    out["finished_at"] = now().isoformat(timespec="seconds")
    out["result"] = "APPLIED"
    return out


def _engine() -> Engine:
    url = os.environ.get("RETRACT_TEST_DB_URL")
    if url:
        return create_engine(url)
    from db import get_engine  # production: the service .env settings
    return get_engine()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--apply", action="store_true")
    p.add_argument("--run-tag", required=True)
    p.add_argument("--expect-rows", type=int, required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    try:
        res = run(_engine(), apply=a.apply, run_tag=a.run_tag, expect_rows=a.expect_rows)
        rc = 0
    except Refused as exc:
        res, rc = {"result": "REFUSED", "why": str(exc)}, 2
    body = json.dumps(res, sort_keys=True, indent=1, default=str) + "\n"
    with open(a.out, "x", encoding="utf-8") as fh:  # never overwrite another run's output
        fh.write(body)
    print(body, end="")
    print("receipt_sha256=" + hashlib.sha256(body.encode("utf-8")).hexdigest())
    return rc


if __name__ == "__main__":
    sys.exit(main())
