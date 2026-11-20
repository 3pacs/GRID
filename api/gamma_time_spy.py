"""Read the original SPY GEX dashboard's recorded same-day strike book; never write its database."""

import math
import os
import sqlite3
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ingestion.market_calendar import is_market_open, market_close_time

NY = ZoneInfo("America/New_York")
DEFAULT_DB = "/data/agent-home/anikdang/gex-watch/strike_gex.sqlite3"


def read_spy(session_day: date, *, path=None):
    if not is_market_open(session_day):
        return None
    start = datetime.combine(session_day, time(9, 30), NY).astimezone(timezone.utc)
    end = datetime.combine(session_day, market_close_time(session_day), NY).astimezone(
        timezone.utc
    )
    db = Path(path or os.environ.get("GRID_SPY_STRIKE_DB", DEFAULT_DB))
    params = (session_day.isoformat(), start.isoformat(), end.isoformat())
    try:
        con = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
        try:
            con.execute("PRAGMA query_only=ON")
            con.execute("BEGIN")
            meta = con.execute(
                """SELECT ts,spot FROM strike_gex_meta WHERE expiry=? AND ts>=? AND ts<? ORDER BY ts LIMIT 392""",
                params,
            ).fetchall()
            if not meta or len(meta) > 390:
                return None
            rows = con.execute(
                """SELECT ts,strike,net_gex_m FROM strike_gex WHERE expiry=? AND ts>=? AND ts<? ORDER BY ts,strike LIMIT 125001""",
                params,
            ).fetchall()
        finally:
            con.close()
        if not rows or len(rows) > 125000:
            return None
        columns = {}
        times, spot = [], []
        for ts, price in meta:
            dt = datetime.fromisoformat(ts)
            if (
                dt.tzinfo is None
                or not start <= dt < end
                or dt.second
                or dt.microsecond
            ):
                return None
            clock = int(dt.timestamp() * 1000)
            columns[ts] = len(times)
            times.append(clock)
            if isinstance(price, (int, float)) and math.isfinite(price) and price > 0:
                spot.append([clock, price])
        strikes = sorted(
            {
                k
                for ts, k, val in rows
                if ts in columns
                and isinstance(k, (float, int))
                and math.isfinite(k)
                and k > 0
            }
        )
        if not strikes or len(strikes) > 512:
            return None
        matrix = {k: [None] * len(times) for k in strikes}
        count = 0
        for ts, k, val in rows:
            if (
                ts in columns
                and k in matrix
                and isinstance(val, (float, int))
                and math.isfinite(val * 1e6)
            ):
                matrix[k][columns[ts]] = round(val * 1e6, 2)
                count += 1
        if not count:
            return None
        return dict(
            symbol="SPY",
            status="available",
            session_date=session_day.isoformat(),
            session_start=start.isoformat(),
            session_end=end.isoformat(),
            expiry=session_day.isoformat(),
            source="Original SPY GEX dashboard · recorded 0DTE level book (mixed live and delayed inputs)",
            units="USD per 1% underlying move",
            times=times,
            strikes=strikes,
            values=[matrix[k] for k in strikes],
            spot=spot,
        )
    except (OSError, sqlite3.Error, ValueError, OverflowError):
        return None
