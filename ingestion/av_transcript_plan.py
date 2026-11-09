"""Offline fiscal-period validation and persistent AV transcript scheduling.

This is not an account-wide quota service. Only callers sharing this state file
share its reservation counter and pacing. No credentials or response bodies are
stored here. A reservation is spent before HTTP, including uncertain failures.
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TranscriptPeriod:
    quarter: str
    period_end: date
    filed: date
    accession: str


def verified_periods(
    payloads: list[Any], filings: list[dict[str, Any]], today: date,
) -> list[TranscriptPeriod]:
    """Match stored SEC facts to their own current-period filing, not comparatives.

    XBRL fy/fp describe the filing, not necessarily the fact's period. Requiring
    BOTH accession and reportDate to match prevents prior-year comparison facts
    from becoming current-year requests. Missing/conflicting labels fail closed.
    FY in a 10-K identifies Q4; no calendar-month/year-end approximation is used.
    """
    current = {
        (f.get("accession"), f.get("period")): f
        for f in filings if f.get("norm_form") in {"10-K", "10-Q"}
        and isinstance(f.get("filed"), date) and f["filed"] <= today
    }
    by_end: dict[date, dict[str, TranscriptPeriod]] = {}
    for raw in payloads:
        try:
            p = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(p, dict):
                continue
            end = date.fromisoformat(p["period_end"])
            filed = date.fromisoformat(p["filed"])
            accession = p["accession"]
            f = current.get((accession, end))
            if not f or f["filed"] != filed or not end <= filed <= today:
                continue
            year = p["fiscal_year"]
            fp = p["fiscal_period"]
            if isinstance(year, bool) or not str(year).isdigit():
                continue
            year = int(year)
            if not 2010 <= year <= today.year + 1 or abs(year - end.year) > 1:
                continue
            if fp == "FY" and f["norm_form"] == "10-K":
                fp = "Q4"
            elif fp not in {"Q1", "Q2", "Q3"} or f["norm_form"] != "10-Q":
                continue
            quarter = f"{year}{fp}"
            options = by_end.setdefault(end, {})
            period = TranscriptPeriod(quarter, end, filed, accession)
            # Prefer the original filing over a later amendment of the same quarter.
            if quarter not in options or filed < options[quarter].filed:
                options[quarter] = period
        except (KeyError, TypeError, ValueError):
            continue
    periods = [next(iter(options.values())) for options in by_end.values() if len(options) == 1]
    # Ambiguous fiscal-year transitions can map two end dates to the same label.
    labels = [p.quarter for p in periods]
    return sorted(
        [p for p in periods if labels.count(p.quarter) == 1],
        key=lambda p: p.period_end, reverse=True,
    )[:2]


class TranscriptState:
    """Atomic persistent round-robin cursor, pacing and optional UTC-day budget."""

    def __init__(self, path: str | Path, requests_per_minute: float, daily_limit: int | None):
        if not math.isfinite(requests_per_minute) or requests_per_minute <= 0:
            raise ValueError("AV requests per minute must be finite and positive")
        if daily_limit is not None and daily_limit < 0:
            raise ValueError("AV daily limit must be nonnegative")
        self.path = Path(path)
        self.interval = 60.0 / requests_per_minute
        self.daily_limit = daily_limit
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), "
                         "cursor TEXT, day TEXT, calls INTEGER NOT NULL, next_at REAL NOT NULL)")
            conn.execute("INSERT OR IGNORE INTO state VALUES (1, NULL, NULL, 0, 0)")

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def reserve(self, keys: list[str], now: float | None = None) -> tuple[str, float] | None:
        """Advance before HTTP so errors/empty results never pin the queue head.

        Caller supplies unattempted keys for this run. Sorted keys include ticker
        and quarter, allowing restart after a budget ends mid-ticker. Reserving a
        pacing slot is durable and counts even if the process subsequently exits.
        """
        if not keys:
            return None
        now = time.time() if now is None else now
        day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor, old_day, calls, next_at = conn.execute(
                "SELECT cursor, day, calls, next_at FROM state WHERE id=1"
            ).fetchone()
            calls = calls if day == old_day else 0
            if self.daily_limit is not None and calls >= self.daily_limit:
                return None
            keys = sorted(set(keys))
            key = next((key for key in keys if cursor is None or key > cursor), keys[0])
            scheduled = max(now, next_at)
            # Do not reserve tomorrow's slot against today's budget.
            if datetime.fromtimestamp(scheduled, timezone.utc).date().isoformat() != day:
                return None
            conn.execute("UPDATE state SET cursor=?, day=?, calls=?, next_at=? WHERE id=1",
                         (key, day, calls + 1, scheduled + self.interval))
            return key, scheduled - now
