"""Fetch FRED/ALFRED vintages into ``fred_vintages`` (``store/fred_vintages.py``).

For each chunk of observation dates ``[o0, o1]`` the fetcher asks ALFRED for every version published between ``o0``
and ``o1 + PAD_DAYS`` (``fred/series/observations`` with ``realtime_start``/``realtime_end``). ``PAD_DAYS`` exceeds the
reader's ``MAX_AGE_DAYS``, so for any observation every version live within ``MAX_AGE_DAYS`` of it is stored.

    python -m ingestion.fred_vintages backfill --since 2014-03-27      # all series in SERIES
    python -m ingestion.fred_vintages refresh                          # incremental, from the last stored vintage

Graceful: a missing FRED_API_KEY or a failed request logs a warning and returns a FAILED/PARTIAL outcome; nothing
raises into the caller.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from store import fred_vintages as V

API = "https://api.stlouisfed.org/fred/series/observations"
PAD_DAYS = 60                 # > V.MAX_AGE_DAYS
CHUNK_DAYS = 900              # obs days per request (~3 years of vintages answers in ~1 s)
FIRST_VINTAGE = date(2014, 3, 27)   # ALFRED's first vintage of the daily uncertainty indexes

# Daily newspaper-based uncertainty indexes (Baker, Bloom, Davis), daily, revised for weeks after first print.
SERIES: dict[str, str] = {
    "USEPUINDXD": "Economic Policy Uncertainty Index for United States (daily)",
    "WLEMUINDXD": "Equity Market-related Economic Uncertainty Index (daily)",
}


def _key() -> str | None:
    k = os.environ.get("FRED_API_KEY")
    if k:
        return k
    try:
        from config import settings

        return settings.FRED_API_KEY or None
    except Exception:
        return None


def fetch(series_id: str, obs_start: date, obs_end: date, rt_start: date, rt_end: date, key: str) -> list[dict[str, Any]]:
    """Every version of ``series_id`` observations in ``[obs_start, obs_end]`` live in ``[rt_start, rt_end]``."""
    params = {"series_id": series_id, "observation_start": obs_start.isoformat(), "observation_end": obs_end.isoformat(),
              "realtime_start": rt_start.isoformat(), "realtime_end": rt_end.isoformat(), "limit": "100000",
              "api_key": key, "file_type": "json"}
    url = API + "?" + urllib.parse.urlencode(params)
    last: Exception | None = None
    for i in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "grid-fred-vintages/1"}),
                                        timeout=120) as r:
                body = json.loads(r.read())
            break
        except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError) as exc:
            last = exc
            time.sleep(2 ** i * 2)
    else:
        raise RuntimeError(f"FRED vintages {series_id} {obs_start}..{obs_end}: {last}")
    out = []
    for o in body.get("observations", []):
        v = o.get("value")
        out.append({"obs_date": date.fromisoformat(o["date"]),
                    "realtime_start": date.fromisoformat(o["realtime_start"]),
                    "realtime_end": date.fromisoformat(o["realtime_end"]),
                    "value": None if v in (None, "", ".") else float(v)})
    return out


def backfill(engine: Engine, series_id: str, since: date, until: date | None = None) -> dict[str, Any]:
    key = _key()
    if not key:
        log.warning("fred_vintages: FRED_API_KEY missing; backfill skipped")
        return {"series": series_id, "status": "SKIPPED", "reason": "no FRED_API_KEY"}
    until = until or date.today()
    sent, chunks, errors = 0, 0, []
    o0 = since
    while o0 <= until:
        o1 = min(o0 + timedelta(days=CHUNK_DAYS - 1), until)
        try:
            rows = fetch(series_id, o0, o1, max(o0, FIRST_VINTAGE), V.OPEN_END if o1 + timedelta(days=PAD_DAYS) >= until
                         else o1 + timedelta(days=PAD_DAYS), key)
            sent += V.upsert(engine, series_id, rows)
            chunks += 1
        except Exception as exc:
            log.warning("fred_vintages: {s} {a}..{b} failed: {e}", s=series_id, a=o0, b=o1, e=str(exc))
            errors.append(f"{o0}..{o1}")
        o0 = o1 + timedelta(days=1)
    return {"series": series_id, "status": "SUCCESS" if not errors else ("PARTIAL" if chunks else "FAILED"),
            "chunks": chunks, "rows_sent": sent, "failed_chunks": errors, "coverage": V.coverage(engine, series_id)}


def refresh(engine: Engine, series_id: str, today: date | None = None) -> dict[str, Any]:
    """Versions published since the last stored vintage, for observations in the last ``PAD_DAYS`` before it."""
    today = today or date.today()
    cov = V.coverage(engine, series_id)
    if not cov["last_vintage"]:
        return backfill(engine, series_id, FIRST_VINTAGE, today)
    last = date.fromisoformat(cov["last_vintage"])
    key = _key()
    if not key:
        log.warning("fred_vintages: FRED_API_KEY missing; refresh skipped")
        return {"series": series_id, "status": "SKIPPED", "reason": "no FRED_API_KEY"}
    try:
        rows = fetch(series_id, last - timedelta(days=PAD_DAYS), today, last, V.OPEN_END, key)
        sent = V.upsert(engine, series_id, rows)
    except Exception as exc:
        log.warning("fred_vintages: refresh {s} failed: {e}", s=series_id, e=str(exc))
        return {"series": series_id, "status": "FAILED", "error": type(exc).__name__}
    return {"series": series_id, "status": "SUCCESS", "rows_sent": sent, "coverage": V.coverage(engine, series_id)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["backfill", "refresh"])
    ap.add_argument("--since", type=date.fromisoformat, default=FIRST_VINTAGE)
    ap.add_argument("--series", nargs="*", default=list(SERIES))
    a = ap.parse_args()
    from db import get_engine

    eng = get_engine()
    out = [backfill(eng, s, a.since) if a.what == "backfill" else refresh(eng, s) for s in a.series]
    print(json.dumps(out, default=str, indent=1))


if __name__ == "__main__":
    main()
