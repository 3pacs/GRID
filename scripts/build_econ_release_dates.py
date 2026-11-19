#!/usr/bin/env python3
"""Build config/econ_release_dates.json: official release dates of the scheduled US macro events.

The market-state record (``intelligence/regime/market_state.py``) marks scheduled-event days from this file. Dates
come from FRED's release-dates API (``fred/release/dates``), which lists every past release date and the scheduled
future ones. Each release id is checked against its FRED name before its dates are used, so a wrong id fails loudly.

    python scripts/build_econ_release_dates.py [--start 2010-01-01] [--out config/econ_release_dates.json] [--check]

Needs FRED_API_KEY (config.settings). Writes the file atomically; ``--check`` prints counts and writes nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

API = "https://api.stlouisfed.org/fred"
# event -> (FRED release id, substring its FRED name must contain)
RELEASES: dict[str, tuple[int, str]] = {
    "cpi": (10, "Consumer Price Index"),
    "ppi": (46, "Producer Price Index"),
    "nfp": (50, "Employment Situation"),
    "gdp": (53, "Gross Domestic Product"),
    "pce": (54, "Personal Income and Outlays"),
    "retail_sales": (9, "Retail"),
}
# FOMC statement days are not a FRED release (FRED's "FOMC Press Release" updates daily), so they come from the Fed's
# own calendar pages: the historical page per year through 2020 and fomccalendars.htm from 2021 (scheduled meetings,
# including tentative future ones). Statement day = last day of a scheduled meeting; unscheduled meetings, conference
# calls and notation votes are left out. Parser ported from gex edge search 3 (agentic-20261105/edge3/fetch_edge3.py).
FED = "https://www.federalreserve.gov/monetarypolicy"
MONTHS = {m: i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July", "August",
                                      "September", "October", "November", "December"], 1)}
ABBR = {m[:3]: i for m, i in MONTHS.items()}
NOT_SCHEDULED = {"2003-09-15"}   # listed as a one-day "Meeting" the day before the scheduled 2003-09-16 meeting


def _get(path: str, params: dict[str, str], key: str) -> dict:
    q = dict(params, api_key=key, file_type="json")
    url = f"{API}/{path}?{urllib.parse.urlencode(q)}"
    for i in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "grid-econ-calendar/1"}),
                                        timeout=60) as r:
                return json.loads(r.read())
        except Exception:  # transient network / 429
            time.sleep(2 ** i * 2)
    raise RuntimeError(f"FRED {path} failed after retries")


def _html(url: str) -> str:
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "grid-econ-calendar/1"}),
                                timeout=120) as r:
        return r.read().decode(errors="replace")


def _last_day(month_txt: str, days_txt: str, year: int) -> date:
    """'February', '2-3' -> Feb 3; 'January 31-February', '1' or 'Apr/May', '30-1' -> the second month's day."""
    days_txt = days_txt.replace("*", "").strip()
    last = int(re.findall(r"\d+", days_txt)[-1])
    names = re.findall(r"[A-Z][a-z]+", month_txt)
    months = [MONTHS.get(n) or ABBR.get(n[:3]) for n in names]
    m = months[-1]
    first_days = re.findall(r"\d+", days_txt)
    if len(months) == 1 and len(first_days) == 2 and int(first_days[1]) < int(first_days[0]):
        m = m + 1
    return date(year, m, last)


def fomc_statement_days(start: date) -> list[str]:
    out: list[str] = []
    for y in range(max(start.year, 1994), 2021):
        html = _html(f"{FED}/fomchistorical{y}.htm")
        for h in re.findall(r"<h5[^>]*>(.*?)</h5>", html, re.S):
            t = re.sub(r"\s+", " ", re.sub("<[^>]+>", "", h)).strip()
            m = re.match(r"^(.*?)\s+Meeting\s*-\s*(\d{4})$", t)
            if not m or "unscheduled" in t.lower() or "conference call" in t.lower():
                continue
            mm = re.match(r"^([A-Za-z]+(?:/[A-Za-z]+|\s+\d+\s*-\s*[A-Za-z]+)?)\s*([\d\-\s]+)$", m.group(1))
            if mm:
                out.append(_last_day(mm.group(1), mm.group(2), int(m.group(2))).isoformat())
    html = _html(f"{FED}/fomccalendars.htm")
    for sec in re.split(r'<div class="panel panel-default">', html)[1:]:
        ym = re.search(r"(\d{4}) FOMC Meetings", sec)
        if not ym:
            continue
        for mon, days in re.findall(r'fomc-meeting__month[^>]*>\s*<strong>([^<]+)</strong>.*?fomc-meeting__date[^>]*>([^<]+)<', sec, re.S):
            if "unscheduled" in days.lower() or "notation" in days.lower() or not re.search(r"\d", days):
                continue
            out.append(_last_day(mon.strip(), days.strip(), int(ym.group(1))).isoformat())
    return sorted(d for d in set(out) if d >= start.isoformat() and d not in NOT_SCHEDULED)


def release_dates(release_id: int, start: date, key: str) -> list[str]:
    out: list[str] = []
    offset = 0
    while True:
        d = _get("release/dates", {"release_id": str(release_id), "realtime_start": start.isoformat(),
                                   "realtime_end": "9999-12-31", "include_release_dates_with_no_data": "true",
                                   "sort_order": "asc", "limit": "10000", "offset": str(offset)}, key)
        rows = d.get("release_dates") or []
        out.extend(r["date"] for r in rows if r.get("date") and r["date"] >= start.isoformat())
        if len(rows) < 10000:
            break
        offset += len(rows)
    return sorted(set(out))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2010-01-01")
    ap.add_argument("--out", default=str(ROOT / "config" / "econ_release_dates.json"))
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    key = os.environ.get("FRED_API_KEY")
    if not key:
        from config import settings
        key = settings.FRED_API_KEY
    start = date.fromisoformat(a.start)
    events: dict[str, list[str]] = {}
    sources: dict[str, dict] = {}
    for name, (rid, must) in RELEASES.items():
        info = (_get("release", {"release_id": str(rid)}, key).get("releases") or [{}])[0]
        if must.lower() not in str(info.get("name", "")).lower():
            sys.exit(f"release {rid} is {info.get('name')!r}, expected a name containing {must!r}; fix RELEASES")
        events[name] = release_dates(rid, start, key)
        sources[name] = {"fred_release_id": rid, "fred_name": info.get("name"), "link": info.get("link")}
    events["fomc"] = fomc_statement_days(start)
    sources["fomc"] = {"fed_pages": [f"{FED}/fomchistoricalYYYY.htm (to 2020)", f"{FED}/fomccalendars.htm"],
                       "rule": "statement day = last day of a scheduled meeting"}
    summary = {k: {"n": len(v), "first": v[0] if v else None, "last": v[-1] if v else None} for k, v in events.items()}
    print(json.dumps(summary, indent=1))
    if a.check:
        return
    doc = {
        "_comment": "Official release dates of scheduled US macro events, past and scheduled, from FRED fred/release/dates "
                    "(regenerate: python scripts/build_econ_release_dates.py). Read by intelligence/regime/market_state.py.",
        "version": "econ-release-dates/1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "start": a.start,
        "sources": sources,
        "events": events,
    }
    out = Path(a.out)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=1) + "\n")
    os.replace(tmp, out)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
