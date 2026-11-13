#!/usr/bin/env python3
"""Build config/sp500_membership.json: S&P 500 members by date, as a start set plus dated changes.

Source: fja05680/sp500, "S&P 500 Historical Components & Changes (Updated).csv" (one row per change date with the full
member list after it; MIT licence). Members on day d = the start set with every change dated on or before d applied.
The DIX series (``ingestion/altdata/finra_dix.py``) weights dark-pool short volume across these members.

    python scripts/build_sp500_membership.py [--since 2017-01-01] [--csv local.csv] [--out config/sp500_membership.json]

Tickers are kept as the file writes them (class shares with a dot: BRK.B). The file lags S&P's announcements; its
last change date is recorded as ``covers_to`` and readers treat later days with the last known set.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URL = ("https://raw.githubusercontent.com/fja05680/sp500/master/"
       "S%26P%20500%20Historical%20Components%20%26%20Changes%20(Updated).csv")


def changes_from_rows(rows: list[tuple[str, set[str]]], since: str) -> dict:
    rows = sorted(rows)
    base_i = max([i for i, (d, _) in enumerate(rows) if d <= since] or [0])
    base_date, base = rows[base_i]
    changes, prev = [], base
    for d, members in rows[base_i + 1:]:
        add, remove = sorted(members - prev), sorted(prev - members)
        if add or remove:
            changes.append({"date": d, "add": add, "remove": remove})
        prev = members
    return {"base_date": base_date, "base_members": sorted(base), "changes": changes,
            "covers_to": rows[-1][0], "last_count": len(rows[-1][1])}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2017-01-01")
    ap.add_argument("--csv", default=None)
    ap.add_argument("--out", default=str(ROOT / "config" / "sp500_membership.json"))
    a = ap.parse_args()
    if a.csv:
        raw = Path(a.csv).read_text()
    else:
        with urllib.request.urlopen(urllib.request.Request(URL, headers={"User-Agent": "grid-sp500-membership/1"}),
                                    timeout=120) as r:
            raw = r.read().decode()
    rows = [(r["date"], {t.strip() for t in r["tickers"].split(",") if t.strip()}) for r in csv.DictReader(io.StringIO(raw))]
    doc = {"_comment": "S&P 500 members by date (start set + dated changes) from fja05680/sp500 (MIT). "
                       "Regenerate: python scripts/build_sp500_membership.py. Read by ingestion/altdata/finra_dix.py.",
           "version": "sp500-membership/1", "source": URL,
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **changes_from_rows(rows, a.since)}
    out = Path(a.out)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=1) + "\n")
    os.replace(tmp, out)
    print(json.dumps({"base_date": doc["base_date"], "base": len(doc["base_members"]), "changes": len(doc["changes"]),
                      "covers_to": doc["covers_to"], "last_count": doc["last_count"]}))


if __name__ == "__main__":
    main()
