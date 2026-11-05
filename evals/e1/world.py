"""The E1 synthetic fixture world (deterministic, no network, no production data).

``raw_series`` / ``source_catalog`` shaped like production (the columns the
readers touch), on in-memory SQLite or in a throwaway PostgreSQL schema.
History is seeded the way griddb looks: every row up to ``HIST_END`` was
backfilled by one pull at ``BACKFILL_TS`` (so availability before then comes
only from each series' declared publication lag), then live pulls land the
morning after each observation.

"Future" rows for an ``as_of`` are rows no point-in-time reader may use at
that ``as_of``: later vintages of dates already known (revisions pulled after
``as_of``), observations dated after ``as_of``, and observations whose
declared publication is after ``as_of``. A late *backfill* of an old,
previously absent date is deliberately not "future" (by design it enters past
modeled reads, see ``store.observations.read_window_known_at``).
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, datetime, timedelta, timezone

import numpy as np
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FRED_SRC, YF_SRC, SEC_SRC = 1, 2, 3
SOURCES = {FRED_SRC: "fred", YF_SRC: "yfinance", SEC_SRC: "SEC_INSIDER"}

HIST_START = date(2021, 1, 1)
HIST_END = date(2026, 3, 20)
BACKFILL_TS = datetime(2026, 3, 24, 6, 0, 0)  # griddb: FRED history pulled >= this
LIVE_END = date(2026, 9, 25)
LATE_TS = datetime(2026, 9, 28, 23, 0, 0)  # "today": when future rows arrive

DAILY = ("VIXCLS", "T10Y2Y", "DFF", "BAMLH0A0HYM2", "BAMLC0A0CM", "T5YIE")
MONTHLY = ("UNRATE", "INDPRO", "TCU", "M2SL", "UMCSENT")


# ── engines ──────────────────────────────────────────────────────────────


def sqlite_engine() -> Engine:
    engine = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    seed_sources(engine)
    return engine


PG_DDL = (
    "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    # schema.sql's raw_series (FK kept; release/other catalog columns irrelevant here)
    """CREATE TABLE raw_series (
        id BIGSERIAL PRIMARY KEY,
        series_id TEXT NOT NULL,
        source_id INTEGER NOT NULL REFERENCES source_catalog(id),
        obs_date DATE NOT NULL,
        pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        value DOUBLE PRECISION NOT NULL,
        raw_payload JSONB,
        pull_status TEXT NOT NULL CHECK (pull_status IN ('SUCCESS','PARTIAL','FAILED','QUARANTINED'))
    )""",
    "CREATE UNIQUE INDEX uq_raw_series_composite ON raw_series (series_id, source_id, obs_date, pull_timestamp)",
    "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """CREATE TABLE resolved_series (
        id BIGSERIAL PRIMARY KEY,
        feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date DATE NOT NULL,
        release_date DATE NOT NULL,
        vintage_date DATE NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        source_priority_used INTEGER NOT NULL DEFAULT 1
    )""",
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """CREATE TABLE resolved_series_retractions (
        feature_id INTEGER NOT NULL,
        obs_date DATE NOT NULL,
        vintage_date DATE NOT NULL,
        retracted_at TIMESTAMPTZ NOT NULL
    )""",
    """CREATE TABLE cross_reference_checks (
        id BIGSERIAL PRIMARY KEY,
        checked_at TIMESTAMPTZ NOT NULL,
        divergence_zscore DOUBLE PRECISION
    )""",
)


def seed_sources(engine: Engine) -> None:
    with engine.begin() as c:
        c.execute(
            text("INSERT INTO source_catalog (id, name) VALUES (:id, :name)"),
            [{"id": k, "name": v} for k, v in SOURCES.items()],
        )


# ── rows ─────────────────────────────────────────────────────────────────


def row(sid: str, d: date, v: float, ts: datetime = BACKFILL_TS, src: int = FRED_SRC,
        status: str = "SUCCESS") -> dict:
    return {"sid": sid, "src": src, "d": d, "ts": ts, "v": float(v), "st": status}


def insert(engine: Engine, rows: list[dict]) -> None:
    if not rows:
        return
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, "
                "raw_payload, pull_status) VALUES (:sid, :src, :d, :ts, :v, '{}', :st)"
            ),
            rows,
        )


def bdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def months(start: date, end: date):
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield date(y, m, 1)
        m += 1
        if m == 13:
            y, m = y + 1, 1


def saturdays(start: date, end: date):
    d = start + timedelta(days=(5 - start.weekday()) % 7)
    while d <= end:
        yield d
        d += timedelta(days=7)


def live_ts(d: date) -> datetime:
    """A live pull: 06:00 UTC the next calendar day."""
    return datetime.combine(d + timedelta(days=1), datetime.min.time()) + timedelta(hours=6)


def spy_path(start: date, end: date, seed: int = 20260930) -> dict[date, float]:
    """A seeded geometric random walk: forward returns are unpredictable from the past."""
    days = list(bdays(start, end))
    rng = np.random.default_rng(seed)
    level = 300.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.011, len(days))))
    return dict(zip(days, (float(x) for x in level)))


def macro_rows(end: date = HIST_END, ts: datetime = BACKFILL_TS) -> list[dict]:
    """Deterministic macro + SPY + claims history, all pulled in one backfill at ``ts``."""
    rows = []
    for k, sid in enumerate(DAILY):
        for i, d in enumerate(bdays(HIST_START, end)):
            rows.append(row(sid, d, 2.0 + k + math.sin(i / 17.0 + k), ts))
    for d, v in spy_path(HIST_START, end).items():
        rows.append(row("YF:SPY:close", d, v, ts, YF_SRC))
    for k, sid in enumerate(MONTHLY):
        for i, d in enumerate(months(HIST_START, end)):
            rows.append(row(sid, d, 50.0 + 3 * k + i * 0.05 + math.sin(i / 3.0 + k), ts))
    for i, d in enumerate(saturdays(HIST_START, end)):
        rows.append(row("ICSA", d, 220000.0 + 1000 * math.sin(i / 5.0), ts))
    return rows


def live_rows(start: date = HIST_END + timedelta(days=1), end: date = LIVE_END) -> list[dict]:
    """Live daily pulls after the backfill (the ``pulled`` availability path)."""
    rows = []
    spy = spy_path(HIST_START, end)
    for k, sid in enumerate(DAILY):
        for i, d in enumerate(bdays(start, end)):
            rows.append(row(sid, d, 2.5 + k + math.cos(i / 11.0 + k), live_ts(d)))
    for d in bdays(start, end):
        rows.append(row("YF:SPY:close", d, spy[d], live_ts(d), YF_SRC))
    return rows


def insider_rows(start: date, end: date, *, ts_of=live_ts, seed: int = 7) -> list[dict]:
    """INSIDER:{ticker}:{name}:{BUY|SELL} rows dated the transaction, pulled after filing."""
    rng = np.random.default_rng(seed)
    rows = []
    for d in bdays(start, end):
        for side in ("BUY", "SELL"):
            if rng.random() < 0.3:
                sid = f"INSIDER:AAA:insider{int(rng.integers(0, 4))}:{side}"
                rows.append(row(sid, d, float(rng.integers(1, 50)) * 1000.0, ts_of(d), SEC_SRC))
    return rows


def as_utc(ts: datetime) -> datetime:
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
