"""Offline append-only SQLite retention for immutable native-minute revisions.

Explicit local initialization only; never touches GRID production databases.
Atomic batches, content hashes, append-only guards and a read-only API loader.
"""

from __future__ import annotations
from dataclasses import asdict
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from ingestion.realtime.research_intervals import MinuteBar, Session, _utc, rollup

APP_ID = 0x47524944
VERSION = 1
SCHEMA = """
CREATE TABLE minute_revisions (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 symbol TEXT NOT NULL, source TEXT NOT NULL, price_basis TEXT NOT NULL,
 event_at TEXT NOT NULL, available_at TEXT NOT NULL, revision TEXT NOT NULL,
 record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL,
 UNIQUE(symbol, source, price_basis, revision)
);
CREATE INDEX minute_lookup ON minute_revisions(symbol,source,event_at,available_at);
CREATE TRIGGER no_minute_update BEFORE UPDATE ON minute_revisions
 BEGIN SELECT RAISE(ABORT, 'append-only research records'); END;
CREATE TRIGGER no_minute_delete BEFORE DELETE ON minute_revisions
 BEGIN SELECT RAISE(ABORT, 'append-only research records'); END;
"""


def _connection(path, *, readonly=False):
    uri = Path(path).absolute().as_uri() + ("?mode=ro" if readonly else "?mode=rw")
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    if (
        conn.execute("PRAGMA application_id").fetchone()[0] != APP_ID
        or conn.execute("PRAGMA user_version").fetchone()[0] != VERSION
    ):
        conn.close()
        raise ValueError("not a supported native-minute research store")
    return conn


def initialize(path):
    """Create new dedicated local store; existing files must already be ours."""
    p = Path(path)
    try:
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        conn = _connection(p, readonly=True)
        conn.close()
        return
    os.close(fd)
    conn = sqlite3.connect(str(p))
    try:
        conn.executescript(
            "BEGIN IMMEDIATE;"
            + SCHEMA
            + f"PRAGMA application_id={APP_ID};PRAGMA user_version={VERSION};COMMIT;"
        )
    finally:
        conn.close()


def _encoded(bar):
    # Same validation contract as rollups, isolated to the exact source minute.
    ts = _utc(bar.ts)
    receipt = _utc(bar.available_at)
    rollup(
        [bar],
        Session("validation", ts, ts + timedelta(minutes=1), "validation"),
        "1m",
        as_of=receipt,
    )
    values = asdict(bar)
    values["ts"] = ts.isoformat()
    values["available_at"] = receipt.isoformat()
    raw = json.dumps(values, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return values, raw, hashlib.sha256(raw.encode()).hexdigest()


def append(path, bars, *, max_rows=100_000):
    """Atomic append. Exact replay is idempotent; mutation of a revision refuses."""
    records = []
    for b in bars:
        if len(records) >= max_rows:
            raise ValueError("append batch exceeds bound")
        records.append(_encoded(b))
    conn = _connection(path)
    inserted = 0
    try:
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("BEGIN IMMEDIATE")
        for val, raw, digest in records:
            identity = (
                val["symbol"],
                val["source"],
                val["price_basis"],
                val["revision"],
            )
            old = conn.execute(
                "SELECT record_sha256 FROM minute_revisions WHERE symbol=? AND source=? AND price_basis=? AND revision=?",
                identity,
            ).fetchone()
            if old:
                if old[0] != digest:
                    raise ValueError("immutable revision content conflict")
                continue
            conn.execute(
                "INSERT INTO minute_revisions(symbol,source,price_basis,event_at,available_at,revision,record_json,record_sha256) VALUES(?,?,?,?,?,?,?,?)",
                (
                    val["symbol"],
                    val["source"],
                    val["price_basis"],
                    val["ts"],
                    val["available_at"],
                    val["revision"],
                    raw,
                    digest,
                ),
            )
            inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return inserted


def sqlite_loader(path, *, max_rows=100_000):
    """Bind a private retained store. Queries cannot write or resolve user paths."""

    def load(symbol, source, session, as_of):
        conn = _connection(path, readonly=True)
        try:
            records = conn.execute(
                "SELECT record_json,record_sha256 FROM minute_revisions WHERE symbol=? AND source=? AND event_at>=? AND event_at<? AND available_at<=? ORDER BY event_at,available_at,seq LIMIT ?",
                (
                    symbol,
                    source,
                    _utc(session.opens).isoformat(),
                    _utc(session.closes).isoformat(),
                    _utc(as_of).isoformat(),
                    max_rows + 1,
                ),
            ).fetchall()
        finally:
            conn.close()
        if len(records) > max_rows:
            raise ValueError("retained query exceeds bound")
        output = []
        for raw, digest in records:
            if hashlib.sha256(raw.encode()).hexdigest() != digest:
                raise ValueError("retained content hash mismatch")
            row = json.loads(raw)
            row["ts"] = datetime.fromisoformat(row["ts"])
            row["available_at"] = datetime.fromisoformat(row["available_at"])
            output.append(MinuteBar(**row))
        return tuple(output)

    return load
