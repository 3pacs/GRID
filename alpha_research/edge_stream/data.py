"""Bar sources for looks: Polygon 1-minute bars (regular hours) with a local cache.

Polygon (GRID's ``POLYGON_API_KEY``, bearer header, never in a URL or a log) serves
next-day minute bars over a rolling ~2-year window; a same-day request answers 403.
The cache keeps every fetched session (and every weekday that had no bars, as an
empty session) in SQLite under the stream's data dir, so a look recomputed after a
crash reads exactly the same bytes, and the window's oldest days survive the roll.

A source never returns a session on or after ``today`` (ET): forward looks cannot see
an unfinished day. Bars are ``(minute_et 'HH:MM', open, high, low, close, volume)``
with the minute the bar *starts*; only 09:30 <= start < 16:00 is kept.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from loguru import logger as log  # same logger config.log re-exports; no settings import

ET = ZoneInfo("America/New_York")
POLYGON_BASE = "https://api.polygon.io"

Bar = tuple  # (minute 'HH:MM', o, h, l, c, v)


class SourceError(RuntimeError):
    """Data could not be fetched now (network, 429 storm, entitlement). Retry later."""


class RequestCapReached(SourceError):
    """This cycle's request budget is used up: not a failure of any one registration."""


#: Polygon's retention is ~728 days; a request older than this is refused, never cached empty.
RETENTION_DAYS = 725
#: A weekday with no bars is cached as empty only if it is an NYSE holiday, or if it was
#: fetched at least this many days after it (publication settled; a thin ticker that truly
#: did not trade). Otherwise it stays uncached and is fetched again next cycle.
EMPTY_DAY_SETTLE_DAYS = 3
NYSE_HOLIDAYS = frozenset({
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-06-19", "2024-07-04",
    "2024-09-02", "2024-11-28", "2024-12-25",
    "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26", "2025-06-19",
    "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25", "2026-06-19", "2026-07-03",
    "2026-09-07", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31", "2027-06-18", "2027-07-05",
    "2027-09-06", "2027-11-25", "2027-12-24",
})


class BarSource(Protocol):
    def sessions(self, ticker: str, start: date, end: date, require_complete: bool = False) -> dict[str, list[Bar]]:
        """Sessions with start <= day < end that have bars; empty days are omitted.

        ``require_complete``: raise :class:`SourceError` unless every non-holiday weekday in the
        range is either served or settled-empty (a history look never runs on a silent gap)."""


def today_et(now: datetime | None = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(ET).date()


def manifest_sha256(used: dict[str, dict[str, list[Bar]]]) -> str:
    """sha256 over exactly the bars a look read: {ticker: {day: bars}}."""
    payload = {t: {d: [list(b) for b in bars] for d, bars in sorted(days.items())}
               for t, days in sorted(used.items())}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _rth(results: list[dict]) -> dict[str, list[Bar]]:
    out: dict[str, list[Bar]] = {}
    for r in results:
        ts = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).astimezone(ET)
        hm = ts.strftime("%H:%M")
        if "09:30" <= hm < "16:00":
            out.setdefault(ts.date().isoformat(), []).append(
                (hm, float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"]), float(r.get("v") or 0.0)))
    for bars in out.values():
        bars.sort()
    return out


class StaticSource:
    """In-memory bars (tests and synthetic dry runs)."""

    def __init__(self, bars: dict[str, dict[str, list[Bar]]], today: date | None = None) -> None:
        self.bars = bars
        self.today = today

    def sessions(self, ticker: str, start: date, end: date, require_complete: bool = False) -> dict[str, list[Bar]]:
        if self.today is not None:
            end = min(end, self.today)
        days = self.bars.get(ticker, {})
        return {d: list(b) for d, b in sorted(days.items()) if start.isoformat() <= d < end.isoformat() and b}


class PolygonMinuteSource:
    """Polygon 1-minute aggregates, fetched by calendar month, cached per session."""

    def __init__(self, cache_path: str | Path, api_key: str | None = None, *, gap_s: float = 0.6,
                 max_requests: int = 400, now: datetime | None = None, timeout_s: float = 30.0) -> None:
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        if api_key is None:
            from config import settings
            api_key = getattr(settings, "POLYGON_API_KEY", "") or ""
        self._key = api_key
        self.gap_s = gap_s
        self.max_requests = max_requests
        self.requests = 0
        self.now = now
        self.timeout_s = timeout_s
        with self._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS sessions (ticker TEXT, day TEXT, bars TEXT, fetched_at TEXT,"
                       " PRIMARY KEY (ticker, day))")

    def _db(self) -> sqlite3.Connection:
        return sqlite3.connect(self.cache_path)

    def _cached(self, ticker: str, start: date, end: date) -> dict[str, list[Bar] | None]:
        with self._db() as db:
            rows = db.execute("SELECT day, bars FROM sessions WHERE ticker = ? AND day >= ? AND day < ?",
                              (ticker, start.isoformat(), end.isoformat())).fetchall()
        return {d: [tuple(b) for b in json.loads(bars)] for d, bars in rows}

    def _get(self, path: str) -> dict:
        import requests

        if not self._key:
            raise SourceError("POLYGON_API_KEY not configured")
        for attempt in range(6):
            if self.requests >= self.max_requests:
                raise RequestCapReached(f"request cap {self.max_requests} reached this cycle")
            self.requests += 1
            time.sleep(self.gap_s)
            try:
                resp = requests.get(POLYGON_BASE + path, headers={"Authorization": f"Bearer {self._key}"},
                                    timeout=self.timeout_s)
            except requests.RequestException as exc:
                log.warning("edge_stream polygon {p}: {e}", p=path.split("?")[0], e=type(exc).__name__)
                time.sleep(5 * (attempt + 1))
                continue
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After") or 15)
                time.sleep(min(wait, 65))
                continue
            if resp.status_code >= 400:
                raise SourceError(f"polygon {resp.status_code} for {path.split('?')[0]}")
            try:
                return resp.json()
            except ValueError:
                raise SourceError(f"polygon answered non-JSON for {path.split('?')[0]}") from None
        raise SourceError(f"polygon gave up on {path.split('?')[0]} after retries")

    def _fetch_month(self, ticker: str, first: date, last: date) -> None:
        results: list[dict] = []
        path = (f"/v2/aggs/ticker/{ticker}/range/1/minute/{first.isoformat()}/{last.isoformat()}"
                "?adjusted=true&sort=asc&limit=50000")
        while path:
            body = self._get(path)
            results.extend(body.get("results") or [])
            nxt = body.get("next_url")
            path = nxt.replace(POLYGON_BASE, "") if nxt else ""
        by_day = _rth(results)
        stamp = datetime.now(timezone.utc).isoformat()
        fetched_on = today_et(self.now)
        rows = []
        d = first
        while d <= last:
            day = d.isoformat()
            bars = by_day.get(day, [])
            settled = day in NYSE_HOLIDAYS or (fetched_on - d).days >= EMPTY_DAY_SETTLE_DAYS
            if d.weekday() < 5 and (bars or settled):
                rows.append((ticker, day, json.dumps(bars), stamp))
            d += timedelta(days=1)
        with self._db() as db:
            db.executemany("INSERT OR IGNORE INTO sessions VALUES (?, ?, ?, ?)", rows)

    def sessions(self, ticker: str, start: date, end: date, require_complete: bool = False) -> dict[str, list[Bar]]:
        end = min(end, today_et(self.now))
        cached = self._cached(ticker, start, end)
        oldest = today_et(self.now) - timedelta(days=RETENTION_DAYS)
        if start < oldest and any(d.weekday() < 5 and d.isoformat() not in cached
                                  for d in (start + timedelta(days=i) for i in range((oldest - start).days))):
            raise SourceError(f"{ticker} {start} is outside Polygon's retention (oldest {oldest}) and not cached")
        cached = self._cached(ticker, start, end)
        missing = []
        d = start
        while d < end:
            if d.weekday() < 5 and d.isoformat() not in cached:
                missing.append(d)
            d += timedelta(days=1)
        months: dict[tuple[int, int], list[date]] = {}
        for d in missing:
            months.setdefault((d.year, d.month), []).append(d)
        for days in months.values():
            self._fetch_month(ticker, min(days), max(days))
        if months:
            cached = self._cached(ticker, start, end)
        # a weekday still missing (unpublished, or outside the plan) is not silently dropped:
        # the look waits (SourceError counts as one attempt) rather than run on fewer sessions
        gaps = [d for d in missing if d.isoformat() not in cached and d.isoformat() not in NYSE_HOLIDAYS
                and (require_complete or (today_et(self.now) - d).days >= EMPTY_DAY_SETTLE_DAYS)]
        if gaps:
            raise SourceError(f"{ticker}: {len(gaps)} weekday(s) neither served nor settled, first {gaps[0]}")
        return {d: b for d, b in sorted(cached.items()) if b}
