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
with the same counts, skips, recovery times and cooldowns it had. A running
Hermes also applies a new ``cleared`` event on its next pass.

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


@dataclass
class Replayed:
    sources: dict[str, SourceHistory] = field(default_factory=dict)
    last_event_id: int = 0


def _utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def replay(events: list[RepairEvent], now: datetime, *, threshold: int) -> Replayed:
    """The bookkeeping these events leave at ``now``, by the live rules.

    Mirrors hermes_fixers.note_repair_failure / note_repair_recovered:
    a fingerprint unlike the current one restarts the count and keeps the
    strikes; a long skip resets the count and sets the strikes; a recovery
    drops the count and strikes and stamps the recovery (it does not end a
    long skip in force, as in the live code); a clear drops everything for
    the source. A skip that has ended by ``now`` is not returned.
    ``threshold`` is only used to refuse a history the live code could not
    have written (a count at or past the threshold with no long skip after
    it), which is then left at threshold - 1 so the next failure trips it.
    """
    out = Replayed()
    for ev in sorted(events, key=lambda e: (e.recorded_at, e.id)):
        out.last_event_id = max(out.last_event_id, ev.id)
        hist = out.sources.setdefault(ev.source_key, SourceHistory(source_name=ev.source_name))
        hist.source_name = ev.source_name
        at = _utc(ev.recorded_at)
        if ev.event == "repair_failed":
            entry = hist.repeated
            if entry is None or entry[0] != ev.fingerprint:
                entry = [ev.fingerprint, 0, entry[2] if entry is not None else 0]
            entry[1] += 1
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
            hist.recovered_at = at
            hist.last_attempt, hist.consecutive_fails, hist.last_error = at, 0, None
        elif ev.event == "cleared":
            out.sources[ev.source_key] = SourceHistory(
                source_name=ev.source_name, recovered_at=hist.recovered_at,
            )
    now_utc = _utc(now)
    for hist in out.sources.values():
        if hist.skip_until is not None and hist.skip_until <= now_utc:
            hist.skip_until = hist.skip_reason = None
        if hist.repeated is not None and hist.repeated[1] >= threshold:
            hist.repeated[1] = threshold - 1
    return out


class RepairEventStore:
    """Reads and writes ``hermes_repair_events``. Raises on a failed call;
    the caller decides how loudly to say so."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def available(self) -> bool:
        from sqlalchemy import text

        with self.engine.connect() as conn:
            return bool(conn.execute(
                text("SELECT to_regclass(:t) IS NOT NULL"), {"t": EVENT_TABLE},
            ).scalar())

    def record(
        self, event: str, source_key: str, source_name: str, *,
        fingerprint: str | None = None, reason: str | None = None,
        skip_until: datetime | None = None, strikes: int | None = None,
        recorded_at: datetime | None = None,
    ) -> int:
        """Append one event; ``recorded_at`` defaults to the database clock."""
        from sqlalchemy import text

        if event not in EVENTS:
            raise ValueError(f"unknown repair event {event!r}")
        with self.engine.begin() as conn:
            conn.execute(text(f"SET LOCAL statement_timeout = {int(STATEMENT_TIMEOUT_MS)}"))
            return int(conn.execute(text(
                f"INSERT INTO {EVENT_TABLE} (recorded_at, source_key, source_name, event, "
                "fingerprint, reason, skip_until, strikes, release) "
                "VALUES (COALESCE(CAST(:at AS TIMESTAMPTZ), NOW()), :k, :n, :e, :f, :r, :u, :s, :rel) "
                "RETURNING id"),
                {"at": recorded_at, "k": source_key, "n": source_name, "e": event, "f": fingerprint,
                 "r": None if reason is None else str(reason)[:300], "u": skip_until,
                 "s": strikes, "rel": current_release()},
            ).scalar())

    def _read(self, where: str, params: dict[str, Any]) -> list[RepairEvent]:
        from sqlalchemy import text

        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, recorded_at, source_key, source_name, event, fingerprint, reason, "
                f"skip_until, strikes FROM {EVENT_TABLE} WHERE {where} ORDER BY recorded_at, id"),
                params).fetchall()
        return [RepairEvent(*row) for row in rows]

    def read_since(self, since: datetime) -> list[RepairEvent]:
        return self._read("recorded_at > :since", {"since": since})

    def read_clears_after(self, event_id: int) -> list[RepairEvent]:
        return self._read("event = 'cleared' AND id > :id", {"id": int(event_id)})


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
    from scripts.hermes_fixers import REPEATED_FAILURE_THRESHOLD

    store = RepairEventStore(get_engine())
    if not store.available():
        print(f"{EVENT_TABLE} is not present; nothing is kept across restarts")
        return 1
    if args.command == "clear":
        event_id = store.record("cleared", _normalize(args.source), args.source, reason=args.reason)
        print(f"cleared {args.source} (event {event_id}); Hermes applies it on its next pass")
        return 0
    now = datetime.now(timezone.utc)
    state = replay(store.read_since(now - timedelta(days=LOOKBACK_DAYS)), now,
                   threshold=REPEATED_FAILURE_THRESHOLD)
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
