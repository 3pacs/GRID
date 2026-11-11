"""Immutable private observations for the intraday GEX strike/time chart."""

from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from api.gamma_board_symbols import provider_symbol
from ingestion.market_calendar import (
    is_market_open,
    last_trading_day,
    market_close_time,
)

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
UNITS = "USD per 1% underlying move"
DEFAULT_FILE = "/var/lib/grid-gamma-time/frames.sqlite3"
MAX_LEVELS = 1024
MAX_PAYLOAD = 48_000
MAX_MATRIX = 400_000
PROTECTED = {"SPX", "SPY", "QQQ"}
log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS frames(
    symbol TEXT NOT NULL, session_date TEXT NOT NULL, minute INTEGER NOT NULL,
    observed_at TEXT NOT NULL, expiry TEXT NOT NULL, spot REAL NOT NULL,
    source TEXT NOT NULL, levels TEXT NOT NULL,
    chain_as_of TEXT, spot_as_of TEXT, oi_retrieved_at TEXT,
    PRIMARY KEY(symbol, session_date, minute)
) WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS frames_no_update BEFORE UPDATE ON frames
BEGIN SELECT RAISE(ABORT, 'Immutable frame'); END;
CREATE TRIGGER IF NOT EXISTS frames_no_delete BEFORE DELETE ON frames
BEGIN SELECT RAISE(ABORT, 'Immutable frame'); END;
CREATE TRIGGER IF NOT EXISTS frames_no_replace BEFORE INSERT ON frames
WHEN EXISTS(SELECT 1 FROM frames WHERE symbol=NEW.symbol
    AND session_date=NEW.session_date AND minute=NEW.minute)
BEGIN SELECT RAISE(IGNORE); END;
"""


def _file(path):
    return Path(path or os.environ.get("GRID_GAMMA_TIME_DB") or DEFAULT_FILE)


def _bounds(day):
    return (
        datetime.combine(day, time(9, 30), NY).astimezone(UTC),
        datetime.combine(day, market_close_time(day), NY).astimezone(UTC),
    )


def _number(value):
    return type(value) in (float, int) and math.isfinite(value)


def write_frame(
    symbol: str,
    *,
    session_date: date,
    observed_at: datetime,
    expiry: date,
    spot: float,
    levels: dict[float, float],
    source: str,
    chain_as_of: datetime | None = None,
    spot_as_of: datetime | None = None,
    oi_retrieved_at: datetime | None = None,
    path: str | Path | None = None,
) -> bool:
    """Freeze the first valid receipt for the minute. Return False on duplicate."""
    provider_symbol(symbol)
    if (
        type(session_date) is not date
        or type(expiry) is not date
        or not is_market_open(session_date)
    ):
        raise ValueError("Invalid trading date")
    if expiry < session_date or (symbol in PROTECTED and expiry != session_date):
        raise ValueError("Expiry outside the instrument's session contract")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("Observation requires a timezone")
    clock = observed_at.astimezone(UTC)
    start, end = _bounds(session_date)
    source_clocks = (chain_as_of, spot_as_of, oi_retrieved_at)
    if any(
        t is not None
        and (not isinstance(t, datetime) or t.utcoffset() is None or t > clock)
        for t in source_clocks
    ):
        raise ValueError("Invalid source time at observation")
    if not start <= clock < end or clock > datetime.now(UTC):
        raise ValueError("Observation outside elapsed cash session")
    if (
        not _number(spot)
        or spot <= 0
        or not isinstance(source, str)
        or not source.strip()
        or len(source) > 200
    ):
        raise ValueError("Invalid spot or source")
    if not isinstance(levels, dict) or not 0 < len(levels) <= MAX_LEVELS:
        raise ValueError("Invalid strike coverage")
    if any(not _number(k) or k <= 0 or not _number(v) for k, v in levels.items()):
        raise ValueError("Invalid strike exposure")
    payload = json.dumps(sorted(levels.items()), separators=(",", ":"), allow_nan=False)
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("Oversized frame")
    db = _file(path)
    db.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Limit permissions from the first instant the file exists, even outside systemd.
    try:
        os.close(os.open(db, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    except FileExistsError:
        pass
    with closing(sqlite3.connect(db, timeout=2)) as con, con:
        con.executescript(_SCHEMA)
        con.execute("BEGIN IMMEDIATE")
        existing = con.execute(
            "SELECT expiry,source FROM frames WHERE symbol=? AND session_date=? LIMIT 1",
            (symbol, session_date.isoformat()),
        ).fetchone()
        if existing and existing != (expiry.isoformat(), source):
            raise ValueError("Expiry or source changed within a session")
        inserted = con.execute(
            """INSERT INTO frames(symbol,session_date,minute,observed_at,expiry,spot,source,levels,
                chain_as_of,spot_as_of,oi_retrieved_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                symbol,
                session_date.isoformat(),
                int(clock.timestamp()) // 60,
                clock.isoformat(),
                expiry.isoformat(),
                spot,
                source,
                payload,
                *(t.astimezone(UTC).isoformat() if t else None for t in source_clocks),
            ),
        )
        return inserted.rowcount == 1


def _empty(symbol, day, reason):
    start, end = _bounds(day) if is_market_open(day) else (None, None)
    return dict(
        symbol=symbol,
        status="unavailable",
        reason=reason,
        session_date=day.isoformat(),
        session_start=start.isoformat() if start else None,
        session_end=end.isoformat() if end else None,
        units=UNITS,
        expiry=None,
        source=None,
        times=[],
        strikes=[],
        values=[],
        spot=[],
    )


def _read(db, symbol, day, now):
    if not db.exists():
        return None
    with closing(
        sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    ) as con:
        con.execute("PRAGMA query_only=ON")
        # The recorder never returns more than one frame per cash-session minute.
        rows = con.execute(
            """SELECT minute,observed_at,expiry,spot,source,
                   CASE WHEN length(levels)<=? THEN levels ELSE NULL END,
                   chain_as_of,spot_as_of,oi_retrieved_at FROM frames
            WHERE symbol=? AND session_date=? AND minute<=?
            ORDER BY minute LIMIT 391""",
            (MAX_PAYLOAD, symbol, day.isoformat(), int(now.timestamp()) // 60),
        ).fetchall()
    if not rows:
        return None
    start, end = _bounds(day)
    first, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    if len(rows) > (end_ms - first) // 60000:
        raise ValueError("Invalid frame count")
    expiry, source = rows[0][2], rows[0][4]
    if date.fromisoformat(expiry) < day or (
        symbol in PROTECTED and expiry != day.isoformat()
    ):
        raise ValueError("Invalid stored expiry")
    times = []
    spot = []
    frames = []
    strikes = set()
    for (
        minute,
        stamp,
        exp,
        price,
        src,
        levels,
        chain_clock,
        spot_clock,
        oi_clock,
    ) in rows:
        clock = datetime.fromisoformat(stamp)
        if clock.tzinfo is None or clock.utcoffset() is None:
            raise ValueError("Invalid stored clock")
        ms = int(clock.timestamp() * 1000)
        if (
            not first <= ms < end_ms
            or ms // 60000 != minute
            or exp != expiry
            or src != source
            or not _number(price)
            or price <= 0
        ):
            raise ValueError("Invalid stored session")
        if clock > now:
            continue
        if len(levels) > MAX_PAYLOAD:
            raise ValueError("Oversized stored frame")
        pairs = json.loads(levels)
        if not isinstance(pairs, list) or not 0 < len(pairs) <= MAX_LEVELS:
            raise ValueError("Invalid stored strike count")
        values = dict(pairs)
        if len(values) != len(pairs) or any(
            not _number(k) or k <= 0 or not _number(v) for k, v in values.items()
        ):
            raise ValueError("Invalid stored strike inputs")
        strikes.update(values)
        if len(strikes) * len(rows) > MAX_MATRIX:
            raise ValueError("Oversized heatmap")
        times.append(ms)
        spot.append([ms, price])
        frames.append(values)
        latest_clocks = dict(
            chain_as_of=chain_clock, spot_as_of=spot_clock, oi_retrieved_at=oi_clock
        )
    if not times:
        return None
    ordered = sorted(strikes)
    return dict(
        symbol=symbol,
        status="available",
        session_date=day.isoformat(),
        expiry=expiry,
        session_start=start.isoformat(),
        session_end=end.isoformat(),
        source=source,
        units=UNITS,
        times=times,
        strikes=ordered,
        values=[[frame.get(k) for frame in frames] for k in ordered],
        spot=spot,
        as_of=datetime.fromtimestamp(times[-1] / 1000, UTC).isoformat(),
        frame_count=len(times),
        **latest_clocks,
    )


def read_time_map(
    symbol: str,
    session_day: date | None = None,
    *,
    now: datetime | None = None,
    path: str | Path | None = None,
) -> dict:
    provider_symbol(symbol)
    now = now or datetime.now(UTC)
    et = now.astimezone(NY)
    if session_day is None:
        session_day = last_trading_day(
            et.date() if et.time() >= time(9, 30) else et.date() - timedelta(days=1)
        )
    if (
        session_day > et.date()
        or not is_market_open(session_day)
        or (session_day == et.date() and et.time() < time(9, 30))
    ):
        return _empty(symbol, session_day, "No elapsed regular session on this date.")
    try:
        panel = _read(_file(path), symbol, session_day, now)
        if panel is not None:
            return panel
        if symbol == "SPY":
            from api.gamma_time_spy import read_spy

            alternate = read_spy(session_day)
            if alternate is not None:
                return alternate
    except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
        log.warning(
            "Private gamma time map unavailable for %s (%s)", symbol, type(exc).__name__
        )
        return _empty(symbol, session_day, "Strike history unavailable.")
    return _empty(symbol, session_day, "No strike history recorded for this session.")
