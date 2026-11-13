"""Sources for looks: Polygon 1-minute bars (regular hours), and for the multi-day event
families Polygon daily bars and SEC EDGAR 8-K filings, all with a local cache.

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


# --- daily bars and filing events (multi-day event families) --------------------------

DailyBar = tuple  # (day 'YYYY-MM-DD', open, close)

SEC_DATA_BASE = "https://data.sec.gov"
SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
#: A filer whose window needs more than this many extra submission pages (a bank that
#: files thousands of prospectus supplements a month) is left out rather than half read.
EDGAR_MAX_EXTRA_PAGES = 120
EARNINGS_ITEM = "2.02"  # 8-K Item 2.02, Results of Operations and Financial Condition


class DailySource(Protocol):
    def daily(self, ticker: str, start: date, end: date, require_complete: bool = False) -> list[DailyBar]:
        """Split-adjusted daily (day, open, close) with start <= day < end and day < today."""


class EventSource(Protocol):
    def ready(self) -> str | None:
        """``None`` when events can be fetched, else why not (nothing is registered meanwhile)."""

    def events(self, ticker: str, start: date, end: date) -> list[dict] | None:
        """8-K Item 2.02 filings with start <= filing date < end, oldest first; ``None`` when the
        filer's history over the range could not be read whole (the ticker is then left out)."""


def parse_acceptance(raw: str) -> datetime | None:
    """EDGAR ``acceptanceDateTime`` as an Eastern wall-clock time (naive).

    EDGAR shows acceptance in Eastern time; the submissions API renders the same field with
    a ``Z`` suffix. The value is read as Eastern wall clock. If a value were in fact UTC, this
    reading places it four or five hours late, never early, so a filing can only be assigned
    to a later reaction day than the true one and nothing after the decision is ever used.
    A value with a real numeric offset is converted.
    """
    if not raw:
        return None
    text = str(raw).strip()
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text[:-1]).replace(tzinfo=None)
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(ET).replace(tzinfo=None) if parsed.tzinfo is not None else parsed


def earnings_filings(page: dict, start: date, end: date) -> tuple[list[dict], str | None]:
    """(8-K Item 2.02 rows with start <= filing date < end, oldest filing date on the page). Pure."""
    forms = page.get("form") or []
    dates = page.get("filingDate") or []
    accepted = page.get("acceptanceDateTime") or []
    items = page.get("items") or []
    accessions = page.get("accessionNumber") or []
    out = []
    for i, form in enumerate(forms):
        if form != "8-K" or i >= len(dates) or not (start.isoformat() <= dates[i] < end.isoformat()):
            continue
        if EARNINGS_ITEM not in [x.strip() for x in str(items[i] if i < len(items) else "").split(",")]:
            continue
        when = parse_acceptance(accepted[i] if i < len(accepted) else "")
        if when is None:
            continue
        out.append({"accession": accessions[i] if i < len(accessions) else None, "filing_date": dates[i],
                    "accepted_et": when.isoformat(timespec="seconds")})
    return out, (min(dates) if dates else None)


class StaticDailySource:
    """In-memory daily bars (tests and synthetic dry runs): {ticker: [(day, open, close), ...]}."""

    def __init__(self, bars: dict[str, list[DailyBar]], today: date | None = None) -> None:
        self.bars = bars
        self.today = today

    def daily(self, ticker: str, start: date, end: date, require_complete: bool = False) -> list[DailyBar]:
        if self.today is not None:
            end = min(end, self.today)
        return [tuple(b) for b in sorted(self.bars.get(ticker, [])) if start.isoformat() <= b[0] < end.isoformat()]


class StaticEventSource:
    """In-memory filings: {ticker: [{"accepted_et", "filing_date", "accession"}, ...]}; a ticker
    mapped to ``None`` has unreadable history."""

    def __init__(self, events: dict[str, list[dict] | None], why_not_ready: str | None = None) -> None:
        self._events = events
        self._why = why_not_ready

    def ready(self) -> str | None:
        return self._why

    def events(self, ticker: str, start: date, end: date) -> list[dict] | None:
        rows = self._events.get(ticker, [])
        if rows is None:
            return None
        return sorted((dict(e) for e in rows if start.isoformat() <= e["filing_date"] < end.isoformat()),
                      key=lambda e: (e["accepted_et"], e.get("accession") or ""))


class PolygonDailySource:
    """Polygon daily aggregates, one request per ticker for the whole range, cached per ticker.

    One response is on one split-adjustment basis, so a ticker's bars are never a mix of
    two bases. Requests, key and request cap are the minute source's.
    """

    def __init__(self, minute: PolygonMinuteSource) -> None:
        self._minute = minute
        with minute._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS daily (ticker TEXT PRIMARY KEY, start TEXT, end TEXT, bars TEXT,"
                       " fetched_at TEXT)")

    def daily(self, ticker: str, start: date, end: date, require_complete: bool = False) -> list[DailyBar]:
        today = today_et(self._minute.now)
        end = min(end, today)
        with self._minute._db() as db:
            row = db.execute("SELECT start, end, bars FROM daily WHERE ticker = ?", (ticker,)).fetchone()
        if row is None or not (row[0] <= start.isoformat() and row[1] >= end.isoformat()):
            if start < today - timedelta(days=RETENTION_DAYS):
                raise SourceError(f"{ticker} {start} is outside Polygon's retention and not cached")
            path = (f"/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{(end - timedelta(days=1)).isoformat()}"
                    "?adjusted=true&sort=asc&limit=50000")
            results = self._minute._get(path).get("results") or []
            bars = []
            for r in results:
                day = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).astimezone(ET).date().isoformat()
                if start.isoformat() <= day < end.isoformat():
                    bars.append((day, float(r["o"]), float(r["c"])))
            bars.sort()
            row = (start.isoformat(), end.isoformat(), json.dumps(bars))
            with self._minute._db() as db:
                db.execute("INSERT OR REPLACE INTO daily VALUES (?, ?, ?, ?, ?)",
                           (ticker, row[0], row[1], row[2], datetime.now(timezone.utc).isoformat()))
        out = [tuple(b) for b in json.loads(row[2]) if start.isoformat() <= b[0] < end.isoformat()]
        if require_complete:
            have = {b[0] for b in out}
            gaps = [d for d in (start + timedelta(days=i) for i in range((end - start).days))
                    if d.weekday() < 5 and d.isoformat() not in have and d.isoformat() not in NYSE_HOLIDAYS]
            if gaps:
                with self._minute._db() as db:  # not cached: the next cycle asks again
                    db.execute("DELETE FROM daily WHERE ticker = ?", (ticker,))
                raise SourceError(f"{ticker}: {len(gaps)} weekday(s) without a daily bar, first {gaps[0]}")
        return out


class EdgarEventSource:
    """8-K Item 2.02 filings from SEC EDGAR's submissions API, cached per ticker.

    Free and official; SEC's fair-access rule needs an identifying User-Agent
    (``SEC_USER_AGENT`` in GRID's settings). Without one :meth:`ready` says so and the
    stream registers nothing that needs filings.
    """

    def __init__(self, cache_path: str | Path, user_agent: str | None = None, *, gap_s: float = 0.15,
                 max_requests: int = 2500, now: datetime | None = None, timeout_s: float = 30.0) -> None:
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        if user_agent is None:
            from config import settings
            user_agent = getattr(settings, "SEC_USER_AGENT", "") or ""
        self._ua = user_agent
        self.gap_s = gap_s
        self.max_requests = max_requests
        self.requests = 0
        self.now = now
        self.timeout_s = timeout_s
        self._ciks: dict[str, str] | None = None
        with self._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS edgar_events (ticker TEXT PRIMARY KEY, start TEXT, end TEXT,"
                       " events TEXT, fetched_at TEXT)")

    def _db(self) -> sqlite3.Connection:
        return sqlite3.connect(self.cache_path)

    def ready(self) -> str | None:
        return None if self._ua else "SEC_USER_AGENT not configured"

    def _get(self, url: str) -> dict:
        import requests

        if not self._ua:
            raise SourceError("SEC_USER_AGENT not configured")
        if self.requests >= self.max_requests:
            raise RequestCapReached(f"EDGAR request cap {self.max_requests} reached this cycle")
        self.requests += 1
        time.sleep(self.gap_s)
        try:
            resp = requests.get(url, headers={"User-Agent": self._ua, "Accept-Encoding": "gzip, deflate"},
                                timeout=self.timeout_s)
        except requests.RequestException as exc:
            raise SourceError(f"EDGAR {type(exc).__name__}") from None
        if resp.status_code >= 400:
            raise SourceError(f"EDGAR {resp.status_code} for {url.rsplit('/', 1)[-1]}")
        try:
            return resp.json()
        except ValueError:
            raise SourceError("EDGAR answered non-JSON") from None

    def _cik(self, ticker: str) -> str | None:
        if self._ciks is None:
            body = self._get(SEC_TICKER_MAP_URL)
            self._ciks = {str(v["ticker"]).upper(): f"{int(v['cik_str']):010d}" for v in body.values()}
        return self._ciks.get(ticker.upper())

    def _fetch(self, ticker: str, start: date, end: date) -> list[dict] | None:
        cik = self._cik(ticker)
        if cik is None:
            return None
        body = self._get(f"{SEC_DATA_BASE}/submissions/CIK{cik}.json")
        filings = body.get("filings") or {}
        rows, oldest = earnings_filings(filings.get("recent") or {}, start, end)
        pages = [f for f in (filings.get("files") or [])
                 if str(f.get("filingTo", "")) >= start.isoformat() and str(f.get("filingFrom", "9")) < end.isoformat()]
        if len(pages) > EDGAR_MAX_EXTRA_PAGES:
            return None
        for f in pages:
            more, old = earnings_filings(self._get(f"{SEC_DATA_BASE}/submissions/{f['name']}"), start, end)
            rows += more
            oldest = min(x for x in (oldest, old) if x) if (oldest or old) else None
        covered = (oldest is not None and oldest <= start.isoformat()) or not (filings.get("files") or [])
        if not covered:
            return None
        seen, out = set(), []
        for e in sorted(rows, key=lambda e: (e["accepted_et"], e.get("accession") or "")):
            if e["accession"] not in seen:
                seen.add(e["accession"])
                out.append(e)
        return out

    def events(self, ticker: str, start: date, end: date) -> list[dict] | None:
        end = min(end, today_et(self.now))
        with self._db() as db:
            row = db.execute("SELECT start, end, events FROM edgar_events WHERE ticker = ?", (ticker,)).fetchone()
        if row is None or not (row[0] <= start.isoformat() and row[1] >= end.isoformat()):
            found = self._fetch(ticker, start, end)
            row = (start.isoformat(), end.isoformat(), json.dumps(found))
            with self._db() as db:
                db.execute("INSERT OR REPLACE INTO edgar_events VALUES (?, ?, ?, ?, ?)",
                           (ticker, row[0], row[1], row[2], datetime.now(timezone.utc).isoformat()))
        found = json.loads(row[2])
        if found is None:
            return None
        return [e for e in found if start.isoformat() <= e["filing_date"] < end.isoformat()]
