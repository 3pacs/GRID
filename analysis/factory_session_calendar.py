"""Pinned historical XNYS schedule for factory development, no observed-day fallback."""

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ingestion.market_calendar import ManifestCalendar

PINS = (
    (
        "factory-xnys-2022-11-01-2024-12-31.json",
        "d9f9c57bd39c3ce9c1dd7d626fee7afba957f0fa58bbb2a1b3ca51de7e06771f",
    ),
    (
        "factory-xnys-2025-01-01-2026-11-10.json",
        "8ba2e6ee00506cb60a4128c550d977090518b25f9d571e828122826bb5b7e141",
    ),
)


class FactoryCalendar:
    def __init__(self, root=None):
        root = (
            root
            or Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
        )
        self.parts = tuple(
            ManifestCalendar(Path(root) / name, sha) for name, sha in PINS
        )
        if any(
            b.start != a.end + timedelta(days=1)
            for a, b in zip(self.parts, self.parts[1:])
        ):
            raise ValueError("calendar parts must have contiguous date coverage")
        self.sessions = tuple(s for p in self.parts for s in p.sessions)
        self.by_day = {s.key.split(":")[1]: s for s in self.sessions}
        self.index = {s.key.split(":")[1]: i for i, s in enumerate(self.sessions)}
        self.version = "pinned XNYS: " + ",".join(sha for _, sha in PINS)

    def for_day(self, day):
        d = date.fromisoformat(day)
        if not self.parts[0].start <= d <= self.parts[-1].end:
            raise ValueError("factory date outside qualified calendar")
        return self.by_day.get(day)

    def previous_day(self, day):
        if self.for_day(day) is None:
            raise ValueError("closed day has no previous session selection")
        i = self.index[day]
        if i == 0:
            raise ValueError("calendar lacks preceding session")
        return self.sessions[i - 1].key.split(":")[1]

    def validate_window(self, first, last):
        self.for_day(first)
        self.for_day(last)


# ── factory 3 (3pacs/GRID#1157): the pinned event calendar ─────────────────────
#
# The calendar class reads scheduled US events from one hash-pinned manifest, not
# from config/econ_release_dates.json directly: that file is regenerated in place
# and holds no known-at, so a regeneration could silently change a run's events.
# The manifest is derived from it by ``derive_events`` (reproducible; a test checks
# the derivation while the source file is unchanged) and adds what point-in-time
# needs: for every event date, the latest moment its date was public (``known_at``)
# and the basis of that bound. An event counts for a decision only when its
# known_at is strictly before the decision.

EVENT_PIN = (
    "factory-events-2022-11-01-2026-11-10.json",
    "ca32edcb18e0ede4ac535c4a7dc9b7e0c5d652c8ea1ba12a945c5ca2b4a7def2",
)
EVENT_SOURCE = "config/econ_release_dates.json"
EVENT_SCHEMA = "factory-events/1"
EVENT_COVERAGE = ("2022-11-01", "2026-11-10")
EVENTS = ("fomc", "cpi", "nfp")
EVENT_TIME = {"fomc": "14:00", "cpi": "08:30", "nfp": "08:30"}
# FRED release dates that are not the monthly report (include_release_dates_with_no_data):
# each is a second date in a month that already holds that month's report.
EVENT_EXCLUDED = {
    ("cpi", "2023-02-10"): "CPI seasonal-factor revision; January's CPI came out 2023-02-14",
    ("cpi", "2024-02-09"): "CPI seasonal-factor revision; January's CPI came out 2024-02-13",
    ("nfp", "2024-01-10"): "a second Employment Situation date; December's report came out 2024-01-05",
    ("nfp", "2024-08-21"): "the preliminary benchmark revision; July's report came out 2024-08-02",
}
KNOWN_AT_RULES = {
    "fomc": "fed_schedule_prior_year: the Fed publishes a year's scheduled meetings during the year before "
            "(the 2026-11-13 source snapshot already lists 2027's); bound: 00:00 ET on Dec 31 of the prior year. "
            "Unscheduled meetings are not in the source and never count.",
    "cpi": "release_day_bound: BLS announces every release date before its release day (the 2025 and 2026 "
           "shutdown reschedules included), but the as-published annual schedule is not captured here; so a "
           "date counts only from 00:00 ET on its own day, never the day before.",
    "nfp": "release_day_bound: as cpi.",
}


def _et_midnight(day: str) -> str:
    return datetime.fromisoformat(f"{day}T00:00").replace(tzinfo=ZoneInfo("America/New_York")).isoformat()


def derive_events(econ: dict, source_sha256: str, start=EVENT_COVERAGE[0], end=EVENT_COVERAGE[1]) -> dict:
    """The factory event manifest from the GRID econ calendar document. Pure."""
    events = []
    excluded = []
    for name in EVENTS:
        for day in sorted(econ["events"][name]):
            if not start <= day <= end:
                continue
            if (name, day) in EVENT_EXCLUDED:
                excluded.append({"event": name, "day": day, "reason": EVENT_EXCLUDED[(name, day)]})
                continue
            if name == "fomc":
                known_at, basis = _et_midnight(f"{int(day[:4]) - 1}-12-31"), "fed_schedule_prior_year"
            else:
                known_at, basis = _et_midnight(day), "release_day_bound"
            events.append({"event": name, "day": day, "time_et": EVENT_TIME[name], "known_at": known_at,
                           "basis": basis})
    events.sort(key=lambda e: (e["day"], e["event"]))
    return {
        "schema": EVENT_SCHEMA,
        "coverage": {"start": start, "end": end, "events": list(EVENTS), "complete": True},
        "derived_from": {"path": EVENT_SOURCE, "version": econ.get("version"),
                         "generated_at": econ.get("generated_at"), "sha256": source_sha256},
        "known_at_rules": KNOWN_AT_RULES,
        "excluded": excluded,
        "events": events,
    }


def event_manifest_bytes(doc: dict) -> bytes:
    return (json.dumps(doc, indent=1, sort_keys=True) + "\n").encode()


class FactoryEvents:
    """Scheduled events known at a decision, from the pinned manifest (or a given document, for tests).

    ``known(day, decided)``: the events on ``day`` whose known_at is strictly before ``decided``, or None
    when ``day`` is outside the manifest's coverage: an empty answer means "none known" only inside it
    (an empty covered calendar never proves a quiet day for another source).
    """

    def __init__(self, root=None, pin=EVENT_PIN, doc=None):
        if doc is None:
            root = root or Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
            raw = (Path(root) / pin[0]).read_bytes()
            sha = hashlib.sha256(raw).hexdigest()
            if sha != pin[1]:
                raise ValueError(f"event manifest {pin[0]} does not match its pinned sha256")
            doc = json.loads(raw)
            self.version = f"{EVENT_SCHEMA}: {pin[0]} {sha}"
        else:
            self.version = f"{EVENT_SCHEMA}: unpinned document"
        if doc.get("schema") != EVENT_SCHEMA or not doc.get("coverage", {}).get("complete"):
            raise ValueError("a complete factory-events/1 document is required")
        self.start, self.end = doc["coverage"]["start"], doc["coverage"]["end"]
        self.by_day = {}
        for e in doc["events"]:
            known = datetime.fromisoformat(e["known_at"])
            if known.utcoffset() is None:
                raise ValueError("event known_at must carry its offset")
            self.by_day.setdefault(e["day"], []).append((e["event"], known))

    def known(self, day, decided):
        if decided.utcoffset() is None:
            raise ValueError("decision time must be timezone aware")
        if not self.start <= day <= self.end:
            return None
        return frozenset(name for name, known in self.by_day.get(day, ()) if known < decided)
