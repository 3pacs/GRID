"""DIX: the dollar-weighted dark-pool short-volume ratio of S&P 500 members, from FINRA's daily Reg SHO file.

DIX_d = sum(ShortVolume_i,d * close_i,d) / sum(TotalVolume_i,d * close_i,d) over day d's S&P 500 members
(SqueezeMetrics' definition in "Short is Long", 2018). Inputs:

* FINRA consolidated file ``CNMSshvol<YYYYMMDD>.txt`` (cdn.finra.org; public history from 2018-08), parsed with the
  producer's own ``parse_daily_short_volume_file``;
* members from ``config/sp500_membership.json`` (``scripts/build_sp500_membership.py``);
* each member's unadjusted Tiingo close for the same day (dollar weights). Weighting by shares instead tracks
  SqueezeMetrics' published DIX at only 0.90 correlation; same-day dollar weights reach 0.997 (gex edge search 11,
  2,083 days 2018-08..2026-11).

Writes one raw_series row per trade date: ``finra:dix_sp500`` (source FINRA_SHORT_VOLUME, the file's own catalog
row), with matched/member counts in the payload. A day with fewer than :data:`MIN_MATCHED` members matched is not
written. Each row carries its pull's own ``pull_timestamp``; readers model availability at one business day after the
trade date (the file posts that evening). Closes are used to compute the ratio and are not stored.

    python -m ingestion.altdata.finra_dix --backfill 2018-08-01 2026-11-12
    python -m ingestion.altdata.finra_dix --recent 5
"""

from __future__ import annotations

import argparse
import bisect
import json
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

from loguru import logger as log

from ingestion.altdata.finra_short_volume import parse_daily_short_volume_file
from ingestion.base import BasePuller

DIX_SERIES = "finra:dix_sp500"
MIN_MATCHED = 450
FILE_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{ymd}.txt"
TIINGO_URL = "https://api.tiingo.com/tiingo/daily/{t}/prices?startDate={a}&endDate={b}&format=json"
MEMBERSHIP_PATH = Path(__file__).resolve().parents[2] / "config" / "sp500_membership.json"


# ── membership ──────────────────────────────────────────────────────────

class Membership:
    """S&P 500 members on any date from the start set + dated changes."""

    def __init__(self, doc: dict[str, Any]) -> None:
        self.base_date = doc["base_date"]
        self.covers_to = doc["covers_to"]
        self._dates, self._sets = [self.base_date], [frozenset(doc["base_members"])]
        cur = set(doc["base_members"])
        for ch in sorted(doc["changes"], key=lambda c: c["date"]):
            cur = (cur - set(ch["remove"])) | set(ch["add"])
            self._dates.append(ch["date"])
            self._sets.append(frozenset(cur))

    @classmethod
    def load(cls, path: Path = MEMBERSHIP_PATH) -> "Membership":
        return cls(json.loads(path.read_text()))

    def on(self, d: date) -> frozenset[str]:
        i = bisect.bisect_right(self._dates, d.isoformat()) - 1
        return self._sets[i] if i >= 0 else frozenset()

    def union(self, start: date, end: date) -> set[str]:
        out = set(self.on(start))
        for d, s in zip(self._dates, self._sets):
            if start.isoformat() <= d <= end.isoformat():
                out |= s
        return out


def finra_symbol(member: str) -> str:
    """Membership writes class shares with a dot (BRK.B), FINRA with a slash (BRK/B)."""
    return member.replace(".", "/")


def tiingo_symbol(member: str) -> str:
    return member.replace(".", "-").lower()


# ── the ratio ───────────────────────────────────────────────────────────

def compute_dix(rows: list[dict[str, Any]], members: Iterable[str], closes: dict[str, float]) -> dict[str, Any] | None:
    """DIX for one FINRA file's parsed rows. ``closes``: {member ticker: same-day unadjusted close}."""
    by_symbol: dict[str, tuple[float, float]] = {}
    for r in rows:
        s = r["symbol"].strip().upper()
        sv, tv = by_symbol.get(s, (0.0, 0.0))
        by_symbol[s] = (sv + float(r["short_volume"] or 0.0), tv + float(r["total_volume"] or 0.0))
    num = den = 0.0
    matched = n = 0
    for m in members:
        n += 1
        fv = by_symbol.get(finra_symbol(m))
        px = closes.get(m)
        if fv is None or px is None or not px > 0 or not fv[1] > 0:
            continue
        matched += 1
        num += fv[0] * px
        den += fv[1] * px
    if matched < MIN_MATCHED or den <= 0:
        return None
    return {"value": num / den, "matched": matched, "members": n, "dollar_volume_m": round(den / 1e6, 1)}


# ── fetchers ────────────────────────────────────────────────────────────

def _get(url: str, headers: dict[str, str] | None = None, tries: int = 4) -> tuple[int, bytes]:
    h = {"User-Agent": "grid-finra-dix/1", **(headers or {})}
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=60) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return e.code, b""
            time.sleep(2 ** i * 2)
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(2 ** i * 2)
    return 0, b""


def fetch_file_rows(d: date) -> list[dict[str, Any]] | None:
    code, body = _get(FILE_URL.format(ymd=d.strftime("%Y%m%d")))
    if code != 200 or not body:
        return None
    return parse_daily_short_volume_file(body.decode("utf-8", "replace")).get("rows") or []


def fetch_closes(tickers: Iterable[str], start: date, end: date, key: str, pause_s: float = 0.15) -> dict[date, dict[str, float]]:
    """{date: {member: unadjusted close}} from Tiingo, one request per ticker."""
    out: dict[date, dict[str, float]] = {}
    for t in sorted(tickers):
        code, body = _get(TIINGO_URL.format(t=tiingo_symbol(t), a=start.isoformat(), b=end.isoformat()),
                          {"Authorization": f"Token {key}"})
        if code == 200 and body:
            try:
                for r in json.loads(body):
                    if r.get("close"):
                        out.setdefault(date.fromisoformat(r["date"][:10]), {})[t] = float(r["close"])
            except ValueError:
                pass
        time.sleep(pause_s)
    return out


# ── the puller ──────────────────────────────────────────────────────────

class FINRADixPuller(BasePuller):
    """Writes ``finra:dix_sp500`` rows. Same source_catalog row as the FINRA short-volume producer."""

    SOURCE_NAME: str = "FINRA_SHORT_VOLUME"

    def __init__(self, db_engine: Any, membership: Membership | None = None) -> None:
        super().__init__(db_engine)
        self.membership = membership or Membership.load()

    def _key(self) -> str | None:
        import os

        k = os.environ.get("TIINGO_API_KEY")
        if k:
            return k
        try:
            from config import settings

            return getattr(settings, "TIINGO_API_KEY", None) or None
        except Exception:
            return None

    def _existing(self, start: date, end: date) -> set[date]:
        with self.engine.connect() as conn:
            pairs = self._get_existing_pairs_in_range(conn, start, end)
        return set(pairs.get(DIX_SERIES, set()))

    def _write(self, d: date, res: dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            self._insert_raw(conn=conn, series_id=DIX_SERIES, obs_date=d, value=res["value"],
                             raw_payload={"method": "sum(short*close)/sum(total*close) over S&P 500 members",
                                          "matched": res["matched"], "members": res["members"],
                                          "dollar_volume_m": res["dollar_volume_m"],
                                          "membership_covers_to": self.membership.covers_to})

    def pull_range(self, start: date, end: date) -> dict[str, Any]:
        """Compute and write DIX for every trade date in ``[start, end]`` that has no row yet."""
        key = self._key()
        if not key:
            log.warning("finra_dix: TIINGO_API_KEY missing; DIX not computed")
            return {"source": DIX_SERIES, "status": "SKIPPED", "reason": "no TIINGO_API_KEY", "rows_inserted": 0}
        from ingestion.market_calendar import trading_days_between

        have = self._existing(start, end)
        days = [d for d in trading_days_between(start, end) if d not in have]
        if not days:
            return {"source": DIX_SERIES, "status": "SUCCESS", "rows_inserted": 0, "already_present": len(have)}
        closes = fetch_closes(self.membership.union(days[0], days[-1]), days[0], days[-1], key)
        written, missing_file, thin = 0, [], []
        for d in days:
            rows = fetch_file_rows(d)
            if rows is None:
                missing_file.append(d.isoformat())
                continue
            res = compute_dix(rows, self.membership.on(d), closes.get(d, {}))
            if res is None:
                thin.append(d.isoformat())
                continue
            self._write(d, res)
            written += 1
        status = "SUCCESS" if written and not (missing_file or thin) else ("PARTIAL" if written else "FAILED")
        if missing_file or thin:
            log.warning("finra_dix: {w} written; no FINRA file for {m}; too few members priced for {t}",
                        w=written, m=missing_file[:5], t=thin[:5])
        return {"source": DIX_SERIES, "status": status, "rows_inserted": written,
                "missing_file": missing_file, "too_few_members": thin}

    def pull_recent(self, weekdays_back: int = 5, anchor_date: date | None = None) -> dict[str, Any]:
        end = anchor_date or (date.today() - timedelta(days=1))
        start = end - timedelta(days=int(weekdays_back * 7 / 5) + 3)
        return self.pull_range(start, end)


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--backfill", nargs=2, type=date.fromisoformat, metavar=("START", "END"))
    g.add_argument("--recent", type=int, metavar="WEEKDAYS")
    a = ap.parse_args()
    from db import get_engine

    p = FINRADixPuller(get_engine())
    if a.backfill:
        out, cur = [], a.backfill[0]
        while cur <= a.backfill[1]:                 # a year at a time: one Tiingo request per ticker per year
            end = min(date(cur.year, 12, 31), a.backfill[1])
            out.append(p.pull_range(cur, end))
            cur = end + timedelta(days=1)
    else:
        out = [p.pull_recent(a.recent)]
    print(json.dumps(out, default=str, indent=1))


if __name__ == "__main__":
    main()
