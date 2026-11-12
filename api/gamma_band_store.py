"""Private, append-only SPY band signals and target observations (v1)."""

from contextlib import closing
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from api.gamma_board_intraday import _session

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
FIRST_DAY = date(2026, 11, 13)
KINDS = ("first_cross", "failed_reclaim")
BUCKETS = ("opening", "morning", "midday", "afternoon", "closing")
RECOVERY_ATTEMPTS = 2
RECOVERY_RETRY_MINUTES = 2
RECOVERY_GRACE_MINUTES = 5
SCHEMA = """
CREATE TABLE IF NOT EXISTS tracker_day(day TEXT PRIMARY KEY, minute INTEGER NOT NULL, state TEXT NOT NULL, last_paired INTEGER);
CREATE TABLE IF NOT EXISTS observed_bar(day TEXT NOT NULL, minute INTEGER NOT NULL, low REAL NOT NULL,
 high REAL NOT NULL, PRIMARY KEY(day,minute)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS flag(id INTEGER PRIMARY KEY, day TEXT NOT NULL, kind TEXT NOT NULL,
 red REAL NOT NULL, lower REAL NOT NULL, upper REAL NOT NULL, target REAL NOT NULL, close REAL NOT NULL,
 signal_time TEXT NOT NULL, issued_at TEXT NOT NULL, bucket TEXT NOT NULL, first_bar INTEGER NOT NULL,
 deadline INTEGER NOT NULL, UNIQUE(day,red,kind));
CREATE TABLE IF NOT EXISTS grade(flag_id INTEGER PRIMARY KEY REFERENCES flag(id), status TEXT NOT NULL,
 outcome_minute INTEGER NOT NULL, decided_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recovery_attempt(
 flag_id INTEGER NOT NULL REFERENCES flag(id), attempt INTEGER NOT NULL,
 claimed_minute INTEGER NOT NULL, finished_at TEXT, result TEXT,
 PRIMARY KEY(flag_id,attempt));
"""


def _file():
    return Path(
        os.environ.get(
            "GRID_GAMMA_BAND_DB", "/var/lib/grid-gamma-time/spy-band-v1.sqlite3"
        )
    )


def initialize(path=None):
    with closing(sqlite3.connect(path or _file())) as db, db:
        db.executescript(SCHEMA)
        keys = {
            "flag": "id=NEW.id OR (day=NEW.day AND red=NEW.red AND kind=NEW.kind)",
            "grade": "flag_id=NEW.flag_id",
            "observed_bar": "day=NEW.day AND minute=NEW.minute",
        }
        for table, key in keys.items():
            db.execute(
                f"CREATE TRIGGER IF NOT EXISTS {table}_no_replace BEFORE INSERT ON {table}"
                f" WHEN EXISTS(SELECT 1 FROM {table} WHERE {key})"
                " BEGIN SELECT RAISE(IGNORE); END"
            )
            for op in ("update", "delete"):
                db.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {table}_no_{op} BEFORE {op} ON {table}"
                    " BEGIN SELECT RAISE(ABORT,'Immutable SPY band record'); END"
                )


def _stamp(minute):
    return datetime.fromtimestamp(minute * 60, UTC).isoformat()


def time_bucket(minute):
    et = datetime.fromtimestamp(minute * 60, NY)
    clock = et.hour * 60 + et.minute
    return BUCKETS[sum(clock >= threshold for threshold in (600, 690, 810, 930))]


def _grade(db, now):
    """Only complete post-issuance bars can prove a touch. Missing coverage is explicit."""
    now_minute = int(now.timestamp() // 60)
    pending = db.execute("""SELECT f.id,f.day,f.kind,f.target,f.first_bar,f.deadline FROM flag f
        LEFT JOIN grade g ON f.id=g.flag_id WHERE g.flag_id IS NULL""").fetchall()
    for fid, day, kind, target, first, deadline in pending:
        until = min(deadline, now_minute)
        hit, count = db.execute(
            """SELECT min(CASE WHEN (?='first_cross' AND high>=?) OR (?='failed_reclaim' AND low<=?) THEN minute END),count(*)
            FROM observed_bar WHERE day=? AND minute>=? AND minute<?""",
            (kind, target, kind, target, day, first, until),
        ).fetchone()
        if hit is not None:
            status, ended = "hit", hit + 1
        elif now_minute >= deadline and count > 0 and count == deadline - first:
            status, ended = "miss", deadline
        elif now_minute >= deadline + RECOVERY_GRACE_MINUTES:
            # A zero-length window at the close has no recoverable bar.
            if first < deadline:
                final = db.execute(
                    """SELECT finished_at,claimed_minute FROM recovery_attempt
                    WHERE flag_id=? AND attempt=?""",
                    (fid, RECOVERY_ATTEMPTS),
                ).fetchone()
                if not final or (
                    final[0] is None and now_minute < final[1] + RECOVERY_RETRY_MINUTES
                ):
                    continue
            status, ended = "censored", deadline
        else:
            continue
        db.execute(
            "INSERT INTO grade VALUES (?,?,?,?)", (fid, status, ended, now.isoformat())
        )


def advance(*, day, bars, latest, levels, session_end, clock, path=None):
    """Atomically advance the live detector and grade already-issued signals.

    bars are validated, completed SPY candles, timestamped by opening minute.
    latest denotes the single completed bar eligible to generate a *live* flag.
    """
    from api.gamma_band_rules import step

    if day < FIRST_DAY:
        return
    ds = day.isoformat()
    with closing(sqlite3.connect(path or _file(), timeout=3)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        now = clock().astimezone(UTC)
        start, end, _ = _session(now)
        # Bar evidence may be recovered after a restart, never used to emit a late flag.
        if start.astimezone(NY).date() != day or session_end != int(
            end.timestamp() // 60
        ):
            raise ValueError("SPY cash session mismatch")
        first = int(start.timestamp() // 60)
        finished = min(int(now.timestamp() // 60), session_end)
        db.executemany(
            "INSERT OR IGNORE INTO observed_bar VALUES (?,?,?,?)",
            (
                (ds, m, b["low"], b["high"])
                for m, b in bars.items()
                if first <= m < finished
            ),
        )
        previous = db.execute(
            "SELECT minute,state FROM tracker_day WHERE day=?", (ds,)
        ).fetchone()
        if (
            latest in bars
            and first <= latest < finished
            and (not previous or previous[0] < latest)
            and 0 <= now.timestamp() - (latest + 1) * 60 < 55
        ):
            state = json.loads(previous[1]) if previous else {}
            state, flags = step(state, minute=latest, bar=bars[latest], levels=levels)
            # Freeze the flag before inspecting any later candle. The receipt clock
            # is read after processing, with an extra full-minute boundary for grading.
            issued = clock().astimezone(UTC)
            if 0 <= issued.timestamp() - (latest + 1) * 60 < 55 and issued >= now:
                first_bar = int(issued.timestamp() // 60) + 1
                for flag in flags:
                    db.execute(
                        """INSERT INTO flag(day,kind,red,lower,upper,target,close,signal_time,
                        issued_at,bucket,first_bar,deadline) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            ds,
                            flag["kind"],
                            flag["red"],
                            flag["lower"],
                            flag["upper"],
                            flag["target"],
                            flag["close"],
                            _stamp(latest + 1),
                            issued.isoformat(),
                            time_bucket(latest + 1),
                            first_bar,
                            min(first_bar + 30, session_end),
                        ),
                    )
                db.execute(
                    """INSERT INTO tracker_day VALUES(?,?,?,?) ON CONFLICT(day) DO UPDATE SET
                    minute=excluded.minute,state=excluded.state,
                    last_paired=coalesce(excluded.last_paired,tracker_day.last_paired)""",
                    (ds, latest, json.dumps(state), latest if levels else None),
                )
        _grade(db, clock().astimezone(UTC))


def claim_recovery(now, path=None):
    """Reserve one bounded read before I/O. Reservations survive restarts."""
    minute = int(now.timestamp() // 60)
    with closing(sqlite3.connect(path or _file(), timeout=3)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        _grade(db, now)
        row = db.execute(
            """
            SELECT f.id, f.first_bar, f.deadline, count(r.attempt)
            FROM flag f LEFT JOIN grade g ON g.flag_id=f.id
            LEFT JOIN recovery_attempt r ON r.flag_id=f.id
            WHERE g.flag_id IS NULL AND f.first_bar<f.deadline
              AND f.deadline<=? AND f.deadline-f.first_bar<=30
            GROUP BY f.id
            HAVING count(r.attempt)<? AND (max(r.claimed_minute) IS NULL OR max(r.claimed_minute)<=?)
            ORDER BY count(r.attempt),f.deadline,f.id LIMIT 1
        """,
            (
                minute - RECOVERY_GRACE_MINUTES,
                RECOVERY_ATTEMPTS,
                minute - RECOVERY_RETRY_MINUTES,
            ),
        ).fetchone()
        if row is None:
            return None
        fid, first, deadline, count = row
        attempt = count + 1
        db.execute(
            "INSERT INTO recovery_attempt(flag_id,attempt,claimed_minute) VALUES(?,?,?)",
            (fid, attempt, minute),
        )
        return fid, attempt, first, deadline


def finish_recovery(job, bars, now, path=None):
    """Fill holes only for the claimed, still-pending flag. Never enter the detector."""
    fid, attempt, _, _ = job
    with closing(sqlite3.connect(path or _file(), timeout=3)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        flag = db.execute(
            """SELECT f.day,f.first_bar,f.deadline,g.status
            FROM flag f JOIN recovery_attempt r ON r.flag_id=f.id
            LEFT JOIN grade g ON g.flag_id=f.id
            WHERE f.id=? AND r.attempt=? AND r.finished_at IS NULL""",
            (fid, attempt),
        ).fetchone()
        if flag is None:
            return
        day, first, deadline, status = flag
        if status is None and bars is not None:
            db.executemany(
                "INSERT OR IGNORE INTO observed_bar VALUES(?,?,?,?)",
                (
                    (day, m, b["low"], b["high"])
                    for m, b in bars.items()
                    if first <= m < deadline
                ),
            )
        db.execute(
            "UPDATE recovery_attempt SET finished_at=?,result=? WHERE flag_id=? AND attempt=?",
            (
                now.isoformat(),
                "read" if bars is not None else "unavailable",
                fid,
                attempt,
            ),
        )
        _grade(db, now)


def settle_expired(clock, path=None):
    with closing(sqlite3.connect(path or _file(), timeout=3)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        _grade(db, clock().astimezone(UTC))


def snapshot(session_date=None, *, now=None, path=None):
    day = session_date or _session(now or datetime.now(UTC))[0].astimezone(NY).date()
    base = dict(
        session_date=day.isoformat(),
        start_date=FIRST_DAY.isoformat(),
        events=[],
        stats=[],
        as_of=None,
    )
    try:
        with closing(
            sqlite3.connect(
                Path(path or _file()).resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=2,
            )
        ) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            last = db.execute(
                "SELECT last_paired FROM tracker_day WHERE day=?", (day.isoformat(),)
            ).fetchone()
            rows = db.execute(
                """SELECT kind,red,lower,upper,target,close,signal_time,issued_at,bucket,
                deadline,coalesce(g.status,'pending') status,g.outcome_minute FROM flag f
                LEFT JOIN grade g ON g.flag_id=f.id WHERE f.day=? ORDER BY signal_time DESC,id DESC""",
                (day.isoformat(),),
            ).fetchall()
            totals = db.execute("""SELECT kind,bucket,coalesce(g.status,'pending') status,count(*) n
                FROM flag f LEFT JOIN grade g ON f.id=g.flag_id GROUP BY kind,bucket,status""").fetchall()
    except (OSError, sqlite3.Error):
        return dict(base, status="unavailable", reason="SPY flag journal unavailable.")
    summary = {
        (k, b): dict(kind=k, bucket=b, hits=0, misses=0, pending=0, censored=0)
        for k in KINDS
        for b in ("all", *BUCKETS)
    }
    for row in totals:
        for bucket in ("all", row["bucket"]):
            summary[row["kind"], bucket][
                {"hit": "hits", "miss": "misses"}.get(row["status"], row["status"])
            ] += row["n"]
    for row in rows:
        item = dict(row)
        outcome = item.pop("outcome_minute")
        item.update(
            deadline=_stamp(item["deadline"]),
            outcome_time=_stamp(outcome) if outcome is not None else None,
        )
        base["events"].append(item)
    return dict(
        base,
        status="available",
        as_of=_stamp(last[0] + 1) if last and last[0] is not None else None,
        stats=list(summary.values()),
    )
