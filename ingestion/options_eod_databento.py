"""End-of-day option chains for the hand-listed core, from Databento OPRA history (2026-11-21).

Why: on 2026-11-20 the live options source refused every request and GRID
stored no option chain for the session (see ``ingestion/options_schwab.py``).
A live capture that fails cannot be repeated later. OPRA history can: the
session's open interest, closing quotes and daily bars stay on file at
Databento, and under the owner's plan they are free for the trailing year
(measured 2026-11-17 and 2026-11-19 in 3pacs/gex). This module keeps a copy of
every session for the hand-listed core while it is free. It is the backup the
owner asked for on 2026-11-21 ("schwab and databento as a backup"). It is not
real time: a session is served by Databento only after it ends.

What is stored, per session day, per contract of ``EQUITY_TICKERS``:

* ``oi``     OPRA ``statistics`` stat_type 9 in the 05:00-07:30 New York window
             of the session day: the OCC open interest published that morning,
             i.e. the prior close. It is the number a live chain shows all
             session. Every exchange republishes it; the most common value is
             kept (ties to the largest).
* ``close``  OPRA ``cbbo-1m`` in the last minute before the cash close
             (``market_calendar.market_close_time``, so 12:59 on a half day):
             the consolidated bid and ask with the provider's receive time,
             and the row's last-sale price with its own time. Measured on KO,
             2026-11-19: 497 of 498 such prices equal an exchange's daily
             close and all are timed on the session day; a contract that did
             not trade that day carries none. A contract with no quote row in
             that minute has no bid, ask or last here.
* ``bars``   OPRA ``ohlcv-1d`` for the session's UTC day: volume. The feed
             holds one bar per exchange per contract (up to 20 on that day),
             so volume is their sum. The bars cannot say which exchange traded
             last, so no price is taken from them.

Only standard contracts are kept: the OSI root must equal the ticker's root
(``BRKB`` for ``BRK-B``); adjusted roots (``KO1``) are left out.

What is not stored: implied volatility and greeks (the source does not carry
them and none is computed here), and the underlying's price (not an OPRA
field). Nothing is written to ``options_snapshots_all``: the
``options_snapshots`` view shows the newest batch for a ticker and day, so a
chain loaded the next morning would stand in for that day's live capture in
every reader. Reading this archive into a consumer is a separate, reviewed
step; :func:`read_chain` is the read side for it.

Point in time: a row was not known to GRID before its part's
``fetch_log.fetched_at``. A reader that needs an as-of answer must use that
clock, not ``day``.

Spend: every request is priced first with the free ``metadata.get_cost``. The
run's running total of estimates may not pass ``--cap-usd`` (default 0.00, so
free data only); a priced part is logged ``priced`` and never fetched, so a
lapsed plan fails closed. A part whose window Databento does not serve yet is
left for a later run. ``ok`` and ``no_data`` parts are never fetched again.

Store: one SQLite file (``OPTIONS_EOD_STORE``/chains.db, directory 0700). The
key is read from ``DATABENTO_API_KEY_FILE`` (owner-only file) and is never
logged or printed.

    python -m ingestion.options_eod_databento price  [--day YYYY-MM-DD] [--tickers SPY,QQQ]
    python -m ingestion.options_eod_databento run    [--day D | --since D] [--tickers ...] [--cap-usd 0]
    python -m ingestion.options_eod_databento status
"""

from __future__ import annotations

import argparse
import base64
import collections
import csv
import json
import math
import os
import sqlite3
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from config import log, settings
from ingestion.market_calendar import is_market_open, market_close_time
from ingestion.options import EQUITY_TICKERS

ET = ZoneInfo("America/New_York")
DATASET = "OPRA.PILLAR"
HIST_BASE = "https://hist.databento.com/v0/"
STAT_OPEN_INTEREST = "9"
PARTS: tuple[str, ...] = ("oi", "close", "bars")
SCHEMAS: dict[str, str] = {"oi": "statistics", "close": "cbbo-1m", "bars": "ohlcv-1d"}
OI_WINDOW_ET = ((5, 0), (7, 30))
FINAL: frozenset[str] = frozenset({"ok", "no_data"})
DEFAULT_LOOKBACK_SESSIONS = 5
REQUEST_TIMEOUT_SECONDS = 300
RETRY_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
MAX_ATTEMPTS = 4

DDL = """
create table if not exists contracts (
    day text not null, root text not null, expiry text not null, cp text not null, strike_milli integer not null,
    oi integer, bid real, ask real, quote_ts text, last real, last_ts text, volume integer,
    primary key (day, root, expiry, cp, strike_milli)) without rowid;
create table if not exists fetch_log (
    day text not null, root text not null, part text not null, status text not null, rows integer,
    cost_usd real, detail text, fetched_at text not null,
    primary key (day, root, part)) without rowid;
"""


class DatabentoError(RuntimeError):
    """A Databento request failed. Carries the HTTP status and a short provider code; never the key."""

    def __init__(self, status: int, detail: str) -> None:
        self.status = status
        self.detail = detail
        super().__init__(f"Databento HTTP {status}: {detail}" if status else f"Databento {detail}")


# ── pure helpers ─────────────────────────────────────────────────────────


def opra_root(ticker: str) -> str:
    """GRID ticker -> OSI root (``BRK-B`` -> ``BRKB``)."""
    return ticker.strip().upper().replace("-", "").replace(".", "").replace("/", "")


def parse_osi(symbol: Any) -> tuple[str, str, str, int] | None:
    """``'KO    261218C00070000'`` -> ``('KO', '2026-12-18', 'C', 70000)``; None when not OSI-shaped."""
    if not isinstance(symbol, str):
        return None
    text = symbol.strip()
    if len(text) < 16:
        return None
    tail, root = text[-15:], text[:-15].strip()
    yymmdd, side, strike = tail[:6], tail[6], tail[7:]
    if not root.isalpha() or side not in "CP" or not yymmdd.isdigit() or not strike.isdigit():
        return None
    try:
        expiry = datetime.strptime(yymmdd, "%y%m%d").replace(tzinfo=timezone.utc).date()
    except ValueError:
        return None
    return root, expiry.isoformat(), side, int(strike)


def _utc(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET).astimezone(timezone.utc)


def part_window(part: str, day: date) -> tuple[datetime, datetime]:
    """The UTC request window of one part for one session day."""
    if part == "oi":
        (h0, m0), (h1, m1) = OI_WINDOW_ET
        return _utc(day, h0, m0), _utc(day, h1, m1)
    if part == "close":
        close = market_close_time(day)
        end = _utc(day, close.hour, close.minute)
        return end - timedelta(minutes=1), end
    if part == "bars":
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        return start, start + timedelta(days=1)
    raise ValueError(f"unknown part {part!r}")


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _whole(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None and number >= 0 and number == int(number) else None


def _contract(row: dict[str, Any], root: str) -> tuple[str, str, int] | None:
    parsed = parse_osi(row.get("symbol"))
    return parsed[1:] if parsed is not None and parsed[0] == root else None


def parse_oi(rows: Iterable[dict[str, Any]], root: str) -> tuple[dict[tuple[str, str, int], int], int]:
    """statistics rows -> ({contract: open interest}, contracts whose publishers disagreed)."""
    values: dict[tuple[str, str, int], collections.Counter[int]] = collections.defaultdict(collections.Counter)
    for row in rows:
        if str(row.get("stat_type")) != STAT_OPEN_INTEREST:
            continue
        contract, quantity = _contract(row, root), _whole(row.get("quantity"))
        if contract is not None and quantity is not None:
            values[contract][quantity] += 1
    chosen = {k: max(c.items(), key=lambda item: (item[1], item[0]))[0] for k, c in values.items()}
    return chosen, sum(1 for c in values.values() if len(c) > 1)


_Quote = tuple[float | None, float | None, str | None, float | None, str | None]


def parse_close(rows: Iterable[dict[str, Any]], root: str) -> dict[tuple[str, str, int], _Quote]:
    """cbbo-1m rows -> {contract: (bid, ask, receive time, last sale, last sale time)}; the latest row wins."""
    out: dict[tuple[str, str, int], _Quote] = {}
    for row in rows:
        contract = _contract(row, root)
        if contract is None:
            continue
        stamp = row.get("ts_recv") or row.get("ts_event") or None
        held = out.get(contract)
        if held is not None and held[2] is not None and stamp is not None and str(stamp) < held[2]:
            continue
        bid, ask, last = _number(row.get("bid_px_00")), _number(row.get("ask_px_00")), _number(row.get("price"))
        traded = last is not None and last >= 0
        out[contract] = (bid if bid is not None and bid >= 0 else None,
                         ask if ask is not None and ask >= 0 else None, str(stamp) if stamp else None,
                         last if traded else None, (row.get("ts_event") or None) if traded else None)
    return out


def parse_bars(rows: Iterable[dict[str, Any]], root: str) -> dict[tuple[str, str, int], int]:
    """ohlcv-1d rows (one per exchange per contract) -> {contract: volume summed over exchanges}."""
    out: dict[tuple[str, str, int], int] = {}
    for row in rows:
        contract, volume = _contract(row, root), _whole(row.get("volume"))
        if contract is not None and volume is not None:
            out[contract] = out.get(contract, 0) + volume
    return out


def sessions(since: date, until: date) -> list[date]:
    days, day = [], since
    while day <= until:
        if is_market_open(day):
            days.append(day)
        day += timedelta(days=1)
    return days


def recent_sessions(today: date, count: int = DEFAULT_LOOKBACK_SESSIONS) -> list[date]:
    """The ``count`` sessions before ``today`` (today's own session is never complete at Databento)."""
    days: list[date] = []
    day = today - timedelta(days=1)
    while len(days) < count:
        if is_market_open(day):
            days.append(day)
        day -= timedelta(days=1)
    return sorted(days)


# ── Databento access ─────────────────────────────────────────────────────


def load_key(path: str | os.PathLike[str] | None = None) -> str:
    """Read the API key from an owner-only file. The key is never logged."""
    key_path = Path(path if path is not None else settings.DATABENTO_API_KEY_FILE).expanduser()
    try:
        info = key_path.stat()
    except OSError:
        raise DatabentoError(0, f"key file {key_path} is missing") from None
    if info.st_mode & 0o077:
        raise DatabentoError(0, f"key file {key_path} must be readable by its owner only (0600)")
    key = key_path.read_text().strip()
    if not key:
        raise DatabentoError(0, f"key file {key_path} is empty")
    return key


class DatabentoHistorical:
    """The three historical calls this module needs, over plain HTTPS (no SDK dependency)."""

    def __init__(self, key: str, *, session: Any | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._auth = "Basic " + base64.b64encode((key + ":").encode()).decode()
        self._session = session if session is not None else requests.Session()
        self._sleep = sleep
        self._ends: dict[str, datetime] | None = None

    def _post(self, path: str, params: dict[str, str], *, stream: bool = False, get: bool = False) -> Any:
        problem = "network"
        for attempt in range(MAX_ATTEMPTS):
            try:
                if get:  # metadata.get_dataset_range answers 405 to POST (seen 2026-11-21)
                    resp = self._session.get(
                        HIST_BASE + path, params=params, timeout=REQUEST_TIMEOUT_SECONDS,
                        headers={"Authorization": self._auth},
                    )
                else:
                    resp = self._session.post(
                        HIST_BASE + path, data=params, stream=stream, timeout=REQUEST_TIMEOUT_SECONDS,
                        headers={"Authorization": self._auth},
                    )
            except requests.RequestException as exc:
                problem = f"network ({type(exc).__name__})"
                self._sleep(10.0 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp
            if resp.status_code in RETRY_STATUSES and attempt + 1 < MAX_ATTEMPTS:
                self._sleep(5.0 * (attempt + 1))
                continue
            raise DatabentoError(resp.status_code, _error_code(resp))
        raise DatabentoError(0, problem)

    @staticmethod
    def _query(schema: str, root: str, start: datetime, end: datetime) -> dict[str, str]:
        return {"dataset": DATASET, "schema": schema, "symbols": f"{root}.OPT", "stype_in": "parent",
                "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "end": end.strftime("%Y-%m-%dT%H:%M:%SZ")}

    def dataset_end(self, schema: str) -> datetime:
        """The end of what Databento serves for ``schema`` (one call per client)."""
        if self._ends is None:
            body = self._post("metadata.get_dataset_range", {"dataset": DATASET}, get=True).json()
            self._ends = {name: _parse_end(span["end"]) for name, span in body.get("schema", {}).items()}
            self._ends[""] = _parse_end(body["end"])
        return self._ends.get(schema, self._ends[""])

    def cost(self, schema: str, root: str, start: datetime, end: datetime) -> float:
        return float(self._post("metadata.get_cost", self._query(schema, root, start, end)).json())

    def rows(self, schema: str, root: str, start: datetime, end: datetime) -> Iterator[dict[str, str]]:
        params = dict(self._query(schema, root, start, end), encoding="csv", pretty_px="true",
                      pretty_ts="true", map_symbols="true")
        resp = self._post("timeseries.get_range", params, stream=True)
        resp.encoding = "utf-8"
        yield from csv.DictReader(resp.iter_lines(decode_unicode=True))


def _parse_end(text: str) -> datetime:
    return datetime.fromisoformat(text.split(".")[0].rstrip("Z")).replace(tzinfo=timezone.utc)


def _error_code(resp: Any) -> str:
    """A short provider code from an error body (``symbology_invalid_request``), never the body itself."""
    try:
        body = resp.json()
    except ValueError:
        return "error"
    detail = body.get("detail") if isinstance(body, dict) else None
    case = detail.get("case") if isinstance(detail, dict) else None
    return str(case)[:80] if case else "error"


# ── store ────────────────────────────────────────────────────────────────


def open_store(store: str | os.PathLike[str] | None = None) -> sqlite3.Connection:
    directory = Path(store if store is not None else settings.OPTIONS_EOD_STORE).expanduser()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    con = sqlite3.connect(directory / "chains.db", timeout=60)
    con.execute("pragma journal_mode=wal")
    con.executescript(DDL)
    return con


def _log(con: sqlite3.Connection, day: str, root: str, part: str, status: str, *,
         rows: int | None = None, cost: float | None = None, detail: str | None = None,
         now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
    con.execute("insert or replace into fetch_log values (?,?,?,?,?,?,?,?)",
                (day, root, part, status, rows, cost, detail, now().strftime("%Y-%m-%dT%H:%M:%SZ")))


_UPSERTS: dict[str, str] = {
    "oi": "insert into contracts (day, root, expiry, cp, strike_milli, oi) values (?,?,?,?,?,?) "
          "on conflict (day, root, expiry, cp, strike_milli) do update set oi = excluded.oi",
    "close": "insert into contracts (day, root, expiry, cp, strike_milli, bid, ask, quote_ts, last, last_ts) "
             "values (?,?,?,?,?,?,?,?,?,?) on conflict (day, root, expiry, cp, strike_milli) do update set "
             "bid = excluded.bid, ask = excluded.ask, quote_ts = excluded.quote_ts, "
             "last = excluded.last, last_ts = excluded.last_ts",
    "bars": "insert into contracts (day, root, expiry, cp, strike_milli, volume) values (?,?,?,?,?,?) "
            "on conflict (day, root, expiry, cp, strike_milli) do update set volume = excluded.volume",
}


def _parse_part(part: str, rows: Iterable[dict[str, Any]], root: str) -> tuple[list[tuple[Any, ...]], str | None]:
    if part == "oi":
        chosen, disagree = parse_oi(rows, root)
        return [(*k, v) for k, v in sorted(chosen.items())], (f"{disagree} contract(s) with differing values" if disagree else None)
    if part == "close":
        return [(*k, *v) for k, v in sorted(parse_close(rows, root).items())], None
    return [(*k, v) for k, v in sorted(parse_bars(rows, root).items())], None


# ── run ──────────────────────────────────────────────────────────────────


def capture(api: Any, con: sqlite3.Connection, tickers: Iterable[str], days: Iterable[date], *,
            cap_usd: float = 0.0, fetch: bool = True, out: Callable[[str], None] = print,
            now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    """Price, and unless ``fetch`` is False fetch and store, every missing part. Returns counts and spend.

    One failed part never stops the rest; it is logged ``error`` and stays retryable.
    """
    done = {(d, r, p) for d, r, p, s in con.execute("select day, root, part, status from fetch_log") if s in FINAL}
    summary: collections.Counter[str] = collections.Counter()
    spent = 0.0
    for day in days:
        for ticker in tickers:
            root, day_text = opra_root(ticker), day.isoformat()
            for part in PARTS:
                if (day_text, root, part) in done:
                    summary["already"] += 1
                    continue
                schema, (start, end) = SCHEMAS[part], part_window(part, day)
                try:
                    if end > api.dataset_end(schema):
                        summary["pending"] += 1   # not served yet; a later run picks it up
                        continue
                    cost = api.cost(schema, root, start, end)
                    if spent + cost > cap_usd + 1e-12:
                        summary["priced"] += 1
                        out(f"{day_text} {root} {part} priced ${cost:.4f} (cap ${cap_usd:.2f}): not fetched")
                        if fetch:
                            with con:
                                _log(con, day_text, root, part, "priced", cost=cost,
                                     detail=f"estimate exceeds cap {cap_usd:.2f}", now=now)
                        continue
                    if not fetch:
                        summary["free" if cost == 0 else "within_cap"] += 1
                        spent += cost
                        continue
                    spent += cost
                    records, detail = _parse_part(part, api.rows(schema, root, start, end), root)
                    with con:
                        if records:
                            con.executemany(_UPSERTS[part], [(day_text, root, *record) for record in records])
                        _log(con, day_text, root, part, "ok" if records else "no_data",
                             rows=len(records), cost=cost, detail=detail, now=now)
                    summary["ok" if records else "no_data"] += 1
                    out(f"{day_text} {root} {part} {'ok' if records else 'no_data'} {len(records)} contracts ${cost:.4f}")
                except DatabentoError as exc:
                    if exc.detail == "symbology_invalid_request":
                        # A served day with no listed instruments for the root: final, like no rows.
                        if fetch:
                            with con:
                                _log(con, day_text, root, part, "no_data", detail="no instruments for the root", now=now)
                        summary["no_data"] += 1
                        continue
                    summary["error"] += 1
                    log.warning("options EOD {d} {r} {p}: {e}", d=day_text, r=root, p=part, e=str(exc))
                    if fetch:
                        with con:
                            _log(con, day_text, root, part, "error", detail=str(exc)[:200], now=now)
    return {"counts": dict(summary), "estimated_usd": round(spent, 4), "cap_usd": cap_usd}


def read_chain(con: sqlite3.Connection, day: date, ticker: str) -> dict[str, Any]:
    """One ticker's archived chain for a session, with what each part's status and known-at clock were."""
    root, day_text = opra_root(ticker), day.isoformat()
    parts = {part: {"status": status, "rows": rows, "known_at": fetched_at}
             for part, status, rows, fetched_at in con.execute(
                 "select part, status, rows, fetched_at from fetch_log where day = ? and root = ?", (day_text, root))}
    contracts = [
        {"expiry": expiry, "opt_type": "call" if cp == "C" else "put", "strike": strike_milli / 1000.0,
         "open_interest": oi, "bid": bid, "ask": ask, "quote_ts": quote_ts, "last_price": last,
         "last_ts": last_ts, "volume": volume}
        for expiry, cp, strike_milli, oi, bid, ask, quote_ts, last, last_ts, volume in con.execute(
            "select expiry, cp, strike_milli, oi, bid, ask, quote_ts, last, last_ts, volume from contracts "
            "where day = ? and root = ? order by expiry, strike_milli, cp", (day_text, root))
    ]
    return {"ticker": ticker, "day": day_text, "provider": "databento", "dataset": DATASET,
            "audience": "private", "parts": parts, "contracts": contracts,
            "oi_basis": "OCC open interest published the morning of the session (the prior close)",
            "implied_vol": "not supplied by the source and not computed"}


def status(con: sqlite3.Connection) -> dict[str, Any]:
    by_status = {f"{part}:{state}": n for part, state, n in con.execute(
        "select part, status, count(*) from fetch_log group by 1, 2 order by 1, 2")}
    first, last, days = con.execute(
        "select min(day), max(day), count(distinct day) from fetch_log where status = 'ok'").fetchone()
    contracts = con.execute("select count(*) from contracts").fetchone()[0]
    return {"parts": by_status, "first_day": first, "last_day": last, "days": days, "contracts": contracts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=("price", "run", "status"))
    parser.add_argument("--day", type=date.fromisoformat, help="one session day")
    parser.add_argument("--since", type=date.fromisoformat, help="every session from this day to yesterday")
    parser.add_argument("--tickers", help="comma-separated; default is the hand-listed core")
    parser.add_argument("--cap-usd", type=float, default=0.0)
    parser.add_argument("--store")
    args = parser.parse_args(argv)

    con = open_store(args.store)
    if args.command == "status":
        print(json.dumps(status(con), indent=1))
        return 0
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()] if args.tickers else list(EQUITY_TICKERS)
    unknown = [t for t in tickers if t not in EQUITY_TICKERS]
    if unknown:
        print(f"not in the hand-listed core: {', '.join(unknown)}", file=sys.stderr)
        return 2
    today = datetime.now(timezone.utc).astimezone(ET).date()
    if args.day is not None:
        days = [args.day] if is_market_open(args.day) and args.day < today else []
    elif args.since is not None:
        days = sessions(args.since, today - timedelta(days=1))
    else:
        days = recent_sessions(today)
    if not days:
        print("no completed session in the requested range", file=sys.stderr)
        return 2
    result = capture(DatabentoHistorical(load_key()), con, tickers, days,
                     cap_usd=args.cap_usd, fetch=args.command == "run")
    print(json.dumps(result, indent=1))
    counts = result["counts"]
    return 1 if counts.get("error") or counts.get("priced") else 0


if __name__ == "__main__":
    raise SystemExit(main())
