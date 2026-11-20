#!/usr/bin/env python3
"""Find sector-map tickers Tiingo has stopped pricing (read-only).

Tiingo's daily endpoint keeps answering for a delisted symbol: one row every
session at the last price with volume 0 (2026-11-10 audit: CHL 36,358 such
rows since its 2021-01-08 endDate, CEO 34,612 since 2021-03-08, TTM 7,675
since 2023-01-23, FL/GOEV/NOVA/HBI/ZK through 2026). The nightly pull stored
them, so sector-map readers saw dead companies at a flat price.

For every sector-map ticker that is a Tiingo equity and not already in
analysis/sector_map_ticker_status.yaml, read Tiingo's meta
(``/tiingo/daily/{ticker}``: startDate/endDate/name/exchangeCode) and report
the ones whose endDate is more than ``--stale-days`` before today. Prints YAML
entries for the sidecar (status ``delisted``, ``last_session`` = endDate);
writes nothing. Acquirers and successors are not guessed: review the list.

    python scripts/classify_tiingo_dead_tickers.py > /tmp/dead.yaml
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests

from analysis.sector_map import SECTOR_MAP, TICKER_STATUS
from ingestion.tiingo_pull import _tiingo_headers, is_tiingo_equity

META_URL = "https://api.tiingo.com/tiingo/daily/{}"


def sector_tickers() -> list[str]:
    out = set()
    for sector in SECTOR_MAP.values():
        if sector.get("etf"):
            out.add(sector["etf"])
        for sub in sector.get("subsectors", {}).values():
            out.update(a["ticker"] for a in sub.get("actors", []) if a.get("ticker"))
    return sorted(t for t in out if is_tiingo_equity(t) and t not in TICKER_STATUS)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--stale-days", type=int, default=7, help="endDate older than this many days = stopped")
    p.add_argument("--interval", type=float, default=0.25, help="seconds between requests")
    p.add_argument("--limit", type=int, default=0, help="check only the first N tickers (0 = all)")
    args = p.parse_args(argv)
    cutoff = date.today() - timedelta(days=args.stale_days)  # noqa: DTZ011 -- a calendar-day staleness bound
    tickers = sector_tickers()[: args.limit or None]
    dead, errors, checked = [], [], 0
    for t in tickers:
        time.sleep(args.interval)
        try:
            r = requests.get(META_URL.format(t), headers=_tiingo_headers(), timeout=15)
        except requests.RequestException as exc:
            errors.append((t, type(exc).__name__))
            continue
        checked += 1
        if r.status_code != 200:
            errors.append((t, f"HTTP {r.status_code}"))
            continue
        meta = r.json() if r.content else {}
        end = (meta.get("endDate") or "")[:10]
        if end and date.fromisoformat(end) < cutoff:
            dead.append((t, end, (meta.get("name") or "").strip(), meta.get("exchangeCode") or ""))
    print(f"# classify_tiingo_dead_tickers: {checked}/{len(tickers)} checked, {len(dead)} stopped "
          f"(endDate before {cutoff.isoformat()}), {len(errors)} errors")
    for t, end, name, exch in sorted(dead, key=lambda x: x[1]):
        print(f"  {t}: {{status: delisted, last_session: {end}, note: 'Tiingo endDate; "
              f"frozen zero-volume rows after it (2026-11-10 audit). {name.replace(chr(39), '')} ({exch})'}}")
    for t, why in errors:
        print(f"# error {t}: {why}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
