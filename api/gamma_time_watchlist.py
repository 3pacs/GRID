"""Opt-in recording for up to three extra private board symbols, shared across browser sessions."""

from contextlib import closing
import os
from pathlib import Path
import sqlite3

from api.gamma_board_symbols import provider_symbol

PINNED = ("SPX", "SPY", "QQQ")
MAX_EXTRA = 3


class TrackingLimit(ValueError):
    pass


def _file():
    db = Path(
        os.getenv("GRID_GAMMA_TIME_DB", "/var/lib/grid-gamma-time/frames.sqlite3")
    )
    return db.with_name("tracking.sqlite3")


def tracked_symbols():
    path = _file()
    if not path.exists():
        return PINNED
    with closing(
        sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    ) as con:
        rows = con.execute(
            "SELECT symbol FROM tracked ORDER BY symbol LIMIT ?", (MAX_EXTRA,)
        ).fetchall()
    return PINNED + tuple(
        s for (s,) in rows if provider_symbol(s) == s and s not in PINNED
    )


def is_tracked(symbol):
    return symbol in PINNED or symbol in tracked_symbols()


def set_tracking(symbol, enabled):
    provider_symbol(symbol)
    if symbol in PINNED:
        if not enabled:
            raise TrackingLimit("The three default symbols are always recorded.")
        return True
    with closing(sqlite3.connect(_file(), timeout=3)) as con, con:
        con.execute("BEGIN IMMEDIATE")
        con.execute("CREATE TABLE IF NOT EXISTS tracked (symbol TEXT PRIMARY KEY)")
        if not enabled:
            con.execute("DELETE FROM tracked WHERE symbol=?", (symbol,))
        elif not con.execute(
            "SELECT 1 FROM tracked WHERE symbol=?", (symbol,)
        ).fetchone():
            if con.execute("SELECT count(*) FROM tracked").fetchone()[0] >= MAX_EXTRA:
                raise TrackingLimit(
                    "Three extra tickers are already recording. Stop one first."
                )
            con.execute("INSERT INTO tracked VALUES (?)", (symbol,))
    return enabled
