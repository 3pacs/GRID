"""Hermes repair history in PostgreSQL, so a restart does not forget it.

Hermes's repair bookkeeping (scripts/hermes_fixers.py) lived in process
memory: the repeated-failure count, the long skip it trips, the time a repair
last recovered a source, and the ordinary per-source cooldown. Every restart
cleared all of it. On 2026-11-27 grid-hermes started 15 times, and the
sources whose long skip had tripped (TIINGO, polygon, Asset_Registries,
yfinance_options) were repaired again after each start. Repairs also left no
row anywhere, so none of this could be read from the database.

``hermes_repair_events`` (migration hermes_repair_events_20261128) is an
append-only log of four events per source:

- ``repair_failed``: a repair that did not recover the source and that the
  repeated-failure rule counted, with the digit-normalized reason it compares
  (``fingerprint``).
- ``long_skip``: the rule tripped; ``skip_until`` and ``strikes`` (long skips
  so far, which sets the doubling).
- ``recovered``: a repair completed a check of the source.
- ``cleared``: an operator said to retry now (``python -m
  scripts.hermes_repair_store clear SOURCE --reason ...``).

On its first pass after a start Hermes reads the last LOOKBACK_DAYS and
replays them (``replay``) with the same rules as the live code, so it resumes
with the same counts, skips and recovery times it had.

One order, live and replayed. Every write for a source takes that source's
transaction advisory lock first, and its rows are stamped with
``clock_timestamp()`` after the lock, so for one source the order of
``recorded_at`` is the order of commits: a clear that starts first but
commits last is ordered last, and a repair write that waits on an open clear
is ordered after it. The replay orders by (``recorded_at``, ``id``). A
running Hermes applies a ``cleared`` event on its next pass only when it is
newer than the latest event it knows for that source, which is exactly where
a replay would place it; an older clear is superseded in both. Each pass
reads every clear in the replay window it has not yet looked at, so a clear
that commits late (whatever its time) is still seen.

The failure that trips a long skip and the long skip are written in one
transaction. A history that still shows the threshold reached with no long
skip after it (a long-skip row lost some other way) is replayed as the live
code would have acted: the skip is rebuilt from the failure's time.

The cooldown restored is approximate and is not a safety guarantee: it counts
only the failures the repeated-failure rule counted (budget stops, in-flight
skips and missing keys are not stored), so after a restart a source can get
its next ordinary attempt somewhat sooner than one process would have given it.

What is not kept: a deterministic skip for a code or wiring defect, a missing
API key or a source with no handler. Those are fixed by a deploy or an .env
change, which restarts Hermes, so forgetting them at a restart is the retry
that fix needs, as before.

Every database call here is best effort. Without the table, or when a read
or write fails, Hermes keeps its bookkeeping in memory as it did before; the
caller logs the failure (throttled).
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

EVENT_TABLE = "hermes_repair_events"
EVENTS = ("repair_failed", "recovered", "long_skip", "cleared")
# Longer than the longest long skip (7 days), so a skip in force and the
# count that led to it are both read back.
LOOKBACK_DAYS = 8
LOCK_TIMEOUT_MS = 2000
STATEMENT_TIMEOUT_MS = 2000
_REPO = Path(__file__).resolve().parents[1]
_RELEASE: dict[str, str | None] = {}


def current_release() -> str | None:
    """The commit this process runs (provenance on each row), or None."""
    if "sha" not in _RELEASE:
        try:
            out = subprocess.run(
                ["git", "-C", str(_REPO), "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=2, check=True,
            ).stdout.strip()
            _RELEASE["sha"] = out or None
        except Exception:
            _RELEASE["sha"] = None
    return _RELEASE["sha"]


@dataclass(frozen=True)
class RepairEvent:
    id: int
    recorded_at: datetime
    source_key: str
    source_name: str
    event: str
    fingerprint: str | None = None
    reason: str | None = None
    skip_until: datetime | None = None
    strikes: int | None = None
    occurred_at: datetime | None = None


@dataclass
class SourceHistory:
    """One source's bookkeeping as the live code would hold it now."""

    source_name: str
    repeated: list[Any] | None = None          # [fingerprint, count, strikes], as _REPEATED_FAILURES
    skip_until: datetime | None = None
    skip_reason: str | None = None
    recovered_at: datetime | None = None
    last_attempt: datetime | None = None
    consecutive_fails: int = 0
    last_error: str | None = None
    last_event_at: datetime | None = None    # the newest stored event for the source, in stored order


@dataclass
class Replayed:
    sources: dict[str, SourceHistory] = field(default_factory=dict)
    clear_ids: set[int] = field(default_factory=set)


@dataclass(frozen=True)
class Rules:
    """The repeated-failure rule's numbers, as hermes_fixers holds them."""

    threshold: int
    base_seconds: float
    max_seconds: float

    def long_skip_seconds(self, strikes_before: int) -> float:
        return min(self.base_seconds * (2 ** strikes_before), self.max_seconds)


def _utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def replay(events: list[RepairEvent], now: datetime, *, rules: Rules) -> Replayed:
    """The bookkeeping these events leave at ``now``, by the live rules.

    Mirrors hermes_fixers.note_repair_failure / note_repair_recovered: a
    fingerprint unlike the current one restarts the count and keeps the
    strikes; the failure that reaches the threshold trips a long skip, which
    resets the count and adds a strike; a recovery drops the count and
    strikes and stamps the recovery (it does not end a long skip in force, as
    in the live code); a clear drops everything for the source.

    The skip is tripped here on the failure itself, with the live formula
    and the failure's time, and the ``long_skip`` row written with it in the
    same transaction then sets the exact end and strikes the live code
    used. So a history whose long-skip row is missing still holds. A skip
    that has ended by ``now`` is not returned.
    """
    out = Replayed()
    for ev in sorted(events, key=lambda e: (e.recorded_at, e.id)):
        hist = out.sources.setdefault(ev.source_key, SourceHistory(source_name=ev.source_name))
        hist.source_name = ev.source_name
        at = _utc(ev.recorded_at)
        hist.last_event_at = at
        if ev.event == "repair_failed":
            entry = hist.repeated
            if entry is None or entry[0] != ev.fingerprint:
                entry = [ev.fingerprint, 0, entry[2] if entry is not None else 0]
            entry[1] += 1
            if entry[1] >= rules.threshold:
                hist.skip_until = at + timedelta(seconds=rules.long_skip_seconds(entry[2]))
                hist.skip_reason = f"same failure {entry[1]}x in a row: {str(ev.reason or '')[:160]}"
                entry[1] = 0
                entry[2] += 1
            hist.repeated = entry
            hist.last_attempt, hist.last_error = at, ev.reason
            hist.consecutive_fails += 1
        elif ev.event == "long_skip":
            entry = hist.repeated or [ev.fingerprint, 0, 0]
            entry[1] = 0
            entry[2] = int(ev.strikes or 0)
            hist.repeated = entry
            hist.skip_until, hist.skip_reason = _utc(ev.skip_until), ev.reason
        elif ev.event == "recovered":
            hist.repeated = None
            hist.recovered_at = _utc(ev.occurred_at) or at
            hist.last_attempt, hist.consecutive_fails, hist.last_error = at, 0, None
        elif ev.event == "cleared":
            out.clear_ids.add(ev.id)
            out.sources[ev.source_key] = SourceHistory(
                source_name=ev.source_name, recovered_at=hist.recovered_at, last_event_at=at,
            )
    now_utc = _utc(now)
    for hist in out.sources.values():
        if hist.skip_until is not None and hist.skip_until <= now_utc:
            hist.skip_until = hist.skip_reason = None
    return out


class RepairEventStore:
    """Reads and writes ``hermes_repair_events``. Raises on a failed call;
    the caller decides how loudly to say so."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def available(self) -> bool:
        from sqlalchemy import text

        with self.engine.connect() as conn:
            # Exactly True: anything else (NULL, a stand-in object) is "not
            # known to be there", and Hermes keeps its bookkeeping in memory.
            return conn.execute(
                text("SELECT to_regclass(:t) IS NOT NULL"), {"t": EVENT_TABLE},
            ).scalar() is True

    def record(
        self, event: str, source_key: str, source_name: str, *,
        fingerprint: str | None = None, reason: str | None = None,
        skip_until: datetime | None = None, strikes: int | None = None,
        occurred_at: datetime | None = None,
    ) -> int:
        """Append one event; returns its id."""
        return self.record_many([dict(
            event=event, source_key=source_key, source_name=source_name, fingerprint=fingerprint,
            reason=reason, skip_until=skip_until, strikes=strikes, occurred_at=occurred_at,
        )])[0][0]

    def record_many(self, events: list[dict[str, Any]]) -> list[tuple[int, datetime]]:
        """Append events in one transaction: all of them are stored or none.

        Returns (id, recorded_at) per event, in the order given.
        """
        with self.engine.begin() as conn:
            return self.insert(conn, events)

    def insert(self, conn: Any, events: list[dict[str, Any]]) -> list[tuple[int, datetime]]:
        """Insert on the caller's transaction, after the per-source locks."""
        from sqlalchemy import text

        from utils.redaction import redact_secrets

        for ev in events:
            if ev.get("event") not in EVENTS:
                raise ValueError(f"unknown repair event {ev.get('event')!r}")
        conn.execute(text(f"SET LOCAL statement_timeout = {int(STATEMENT_TIMEOUT_MS)}"))
        conn.execute(text(f"SET LOCAL lock_timeout = {int(LOCK_TIMEOUT_MS)}"))
        for key in sorted({ev["source_key"] for ev in events}):
            # One writer per source at a time, held to commit, so the rows'
            # clock_timestamp() order is that source's commit order.
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
                         {"k": f"{EVENT_TABLE}:{key}"})
        out: list[tuple[int, datetime]] = []
        for ev in events:
            # Provider errors embed request URLs, keys and tokens: nothing
            # reaches the table unredacted, whoever the caller.
            reason = redact_secrets(ev.get("reason"))
            row = conn.execute(text(
                f"INSERT INTO {EVENT_TABLE} (source_key, source_name, event, fingerprint, "
                "reason, skip_until, strikes, occurred_at, release) "
                "VALUES (:k, :n, :e, :f, :r, :u, :s, :o, :rel) RETURNING id, recorded_at"),
                {"k": ev["source_key"], "n": ev["source_name"], "e": ev["event"],
                 "f": redact_secrets(ev.get("fingerprint")),
                 "r": None if reason is None else str(reason)[:300], "u": ev.get("skip_until"),
                 "s": ev.get("strikes"), "o": ev.get("occurred_at"), "rel": current_release()},
            ).one()
            out.append((int(row[0]), _utc(row[1])))
        return out

    def _read(self, where: str, params: dict[str, Any]) -> list[RepairEvent]:
        from sqlalchemy import text

        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, recorded_at, source_key, source_name, event, fingerprint, reason, "
                f"skip_until, strikes, occurred_at FROM {EVENT_TABLE} WHERE {where} "
                "ORDER BY recorded_at, id"),
                params).fetchall()
        return [RepairEvent(*row) for row in rows]

    def read_since(self, since: datetime) -> list[RepairEvent]:
        return self._read("recorded_at > :since", {"since": since})

    def read_clears_since(self, since: datetime) -> list[RepairEvent]:
        """Every clear recorded after ``since``. The caller looks at each id
        once; neither an id nor a time is a watermark, since a clear can
        commit after later rows are visible."""
        return self._read("event = 'cleared' AND recorded_at > :since", {"since": since})


def _normalize(source: str) -> str:
    return source.strip().lower().replace(" ", "_").replace("-", "_")


def main(argv: list[str] | None = None) -> int:
    """Operator commands: ``show [SOURCE]`` (read only) and ``clear SOURCE --reason TEXT``."""
    import argparse

    parser = argparse.ArgumentParser(prog="python -m scripts.hermes_repair_store")
    sub = parser.add_subparsers(dest="command", required=True)
    show = sub.add_parser("show", help="what a restart would restore, per source")
    show.add_argument("source", nargs="?")
    clear = sub.add_parser("clear", help="retry SOURCE on Hermes's next pass")
    clear.add_argument("source")
    clear.add_argument("--reason", required=True)
    args = parser.parse_args(argv)

    from db import get_engine
    from scripts.hermes_fixers import repair_rules

    store = RepairEventStore(get_engine())
    if not store.available():
        print(f"{EVENT_TABLE} is not present; nothing is kept across restarts")
        return 1
    if args.command == "clear":
        event_id = store.record("cleared", _normalize(args.source), args.source, reason=args.reason)
        print(f"cleared {args.source} (event {event_id}); Hermes applies it on its next pass")
        return 0
    now = datetime.now(timezone.utc)
    state = replay(store.read_since(now - timedelta(days=LOOKBACK_DAYS)), now, rules=repair_rules())
    for key, hist in sorted(state.sources.items()):
        if args.source and key != _normalize(args.source):
            continue
        print(f"{hist.source_name}: count={hist.repeated[1] if hist.repeated else 0} "
              f"strikes={hist.repeated[2] if hist.repeated else 0} "
              f"skip_until={hist.skip_until.isoformat() if hist.skip_until else '-'} "
              f"recovered_at={hist.recovered_at.isoformat() if hist.recovered_at else '-'} "
              f"fails={hist.consecutive_fails}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
