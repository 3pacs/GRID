"""Pinned historical XNYS schedule for factory development, no observed-day fallback."""

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
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
    (
        "factory-xnys-2026-11-11-2027-12-31.json",
        "f82a4f593d3625ee1ff6a2732795cf7d3400fe112fcead67e47546c683bba9b4",
    ),
)
# PINS lists every part (scripts/prepare_factory_calendars.py normalizes all of them in a deploy
# candidate); research runs read the first two only. The third is the forward part (#1161).
RESEARCH_PINS = PINS[:2]


class FactoryCalendar:
    """Research reads ``RESEARCH_PINS`` (so a recorded run reproduces byte for byte); the live
    recorder and the parity check pass ``pins=PINS``, the research parts plus the forward part."""

    def __init__(self, root=None, pins=None):
        pins = pins or RESEARCH_PINS
        root = (
            root
            or Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
        )
        self.parts = tuple(
            ManifestCalendar(Path(root) / name, sha) for name, sha in pins
        )
        if any(
            b.start != a.end + timedelta(days=1)
            for a, b in zip(self.parts, self.parts[1:])
        ):
            raise ValueError("calendar parts must have contiguous date coverage")
        self.sessions = tuple(s for p in self.parts for s in p.sessions)
        self.by_day = {s.key.split(":")[1]: s for s in self.sessions}
        self.index = {s.key.split(":")[1]: i for i, s in enumerate(self.sessions)}
        self.version = "pinned XNYS: " + ",".join(sha for _, sha in pins)

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
FORWARD_EVENT_PIN = (
    "factory-events-2026-11-11-2026-12-31.json",
    "20d8c4c502709cc905f935273002c280b100c3f10dcbd281b78b40313baa6c6b",
)
EVENT_PINS = (EVENT_PIN, FORWARD_EVENT_PIN)  # live; research reads EVENT_PIN alone
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
    """Scheduled events known at a decision, from the pinned manifest parts (or a given document, for tests).

    ``known(day, decided)``: the events on ``day`` known strictly before ``decided``, or None when no part
    answers for that day at that time: an empty answer means "none known" only where a part answers (an
    empty covered calendar never proves a quiet day for another source). Research reads ``EVENT_PIN``
    (the default, so a recorded run reproduces byte for byte); the live recorder passes ``pins=EVENT_PINS``.

    As-of (2026-11-21, review of #1293 at 91299cf5). The first part is the historical reconstruction and
    answers at any decision time. Every later part must carry ``available_at``: the instant its source
    snapshot existed. Before that instant the part does not answer at all (None, never "quiet"), and an
    event in it counts only from the later of its own known_at and the part's available_at. A part with
    ``supersedes`` is a prospective correction: it overlays days the base parts already cover and answers
    only for decisions after its own (later) available_at, so every earlier decision keeps its answer.
    For a (day, decided) pair the answering part is the one, among those covering the day and available
    before ``decided``, with the latest available_at.
    """

    def __init__(self, root=None, pin=None, doc=None, pins=None):
        if doc is None:
            root = root or Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
            pins = (pin,) if pin is not None else (pins or (EVENT_PIN,))
            docs, names, pinned = [], [], []
            for name, expected in pins:
                raw = (Path(root) / name).read_bytes()
                sha = hashlib.sha256(raw).hexdigest()
                if sha != expected:
                    raise ValueError(f"event manifest {name} does not match its pinned sha256")
                docs.append(json.loads(raw))
                names.append(f"{name} {sha}")
                pinned.append(name)
            self.version = f"{EVENT_SCHEMA}: " + ", ".join(names)
        else:
            docs = list(doc) if isinstance(doc, (list, tuple)) else [doc]
            pinned = None
            self.version = f"{EVENT_SCHEMA}: unpinned document"
        self.parts = []
        for k, d in enumerate(docs):
            if d.get("schema") != EVENT_SCHEMA or not d.get("coverage", {}).get("complete"):
                raise ValueError("a complete factory-events/1 document is required")
            avail = d.get("available_at")
            if avail is None:
                if k:
                    raise ValueError("every part after the first must carry available_at (when its source existed)")
            else:
                avail = datetime.fromisoformat(avail)
                if avail.utcoffset() is None:
                    raise ValueError("available_at must carry its offset")
            cov = d["coverage"]
            by_day = {}
            for e in d["events"]:
                if not cov["start"] <= e["day"] <= cov["end"]:
                    raise ValueError("event outside its part's coverage")
                known = datetime.fromisoformat(e["known_at"])
                if known.utcoffset() is None:
                    raise ValueError("event known_at must carry its offset")
                by_day.setdefault(e["day"], []).append((e["event"], known if avail is None else max(known, avail)))
            # Pin order is availability order (review of a92dbbdb): every part after the first becomes available
            # strictly after every part pinned before it, corrections included. A new part therefore answers
            # only decisions later than anything already pinned, and no earlier (day, decided) answer can move.
            prior = [q["available_at"] for q in self.parts if q["available_at"] is not None]
            if avail is not None and prior and not avail > max(prior):
                raise ValueError("each part must become available after every part pinned before it")
            sup = d.get("supersedes") or []
            if sup and pinned is not None and not set(sup) <= set(pinned[:k]):
                raise ValueError("a correction may supersede only parts pinned before it")
            self.parts.append({"start": cov["start"], "end": cov["end"], "available_at": avail,
                               "correction": bool(sup), "by_day": by_day})
        base = [q for q in self.parts if not q["correction"]]
        if not base or self.parts[0]["correction"]:
            raise ValueError("the first part must be a base part")
        for a, b in zip(base, base[1:]):
            if date.fromisoformat(b["start"]) != date.fromisoformat(a["end"]) + timedelta(days=1):
                raise ValueError("event manifest parts must have contiguous date coverage")
        self.start, self.end = base[0]["start"], base[-1]["end"]
        for q in self.parts:
            if not q["correction"]:
                continue
            if not (self.start <= q["start"] <= q["end"] <= self.end):
                raise ValueError("a correction must lie inside the base coverage")
            under = [b for b in self.parts[:self.parts.index(q)] if b["start"] <= q["end"] and q["start"] <= b["end"]]
            if not under or any(b["available_at"] is not None and b["available_at"] >= q["available_at"] for b in under):
                raise ValueError("a correction must become available after every part it overlays")
        self.by_day = self._merged()   # the latest answer per day (inspection only; known() is as-of)

    def _merged(self):
        out = {}
        for q in sorted(self.parts, key=lambda q: (q["available_at"] is not None, q["available_at"] or datetime.min.replace(tzinfo=timezone.utc))):
            for day in [d for d in out if q["start"] <= d <= q["end"]]:
                del out[day]
            out.update({d: list(v) for d, v in q["by_day"].items()})
        return out

    def _part(self, day, decided):
        best = None
        for q in self.parts:
            if not q["start"] <= day <= q["end"]:
                continue
            if q["available_at"] is not None and not q["available_at"] < decided:
                continue
            if best is None or (q["available_at"] is not None and (best["available_at"] is None or q["available_at"] > best["available_at"])):
                best = q
        return best

    def known(self, day, decided):
        if decided.utcoffset() is None:
            raise ValueError("decision time must be timezone aware")
        q = self._part(day, decided)
        if q is None:
            return None
        return frozenset(name for name, known in q["by_day"].get(day, ()) if known < decided)


# ── factory 7 prerequisite (3pacs/GRID#1161): forward parts ────────────────────
#
# A live slot decides on sessions after the development window, so both pinned
# calendars continue forward in further immutable parts, each contiguous with the
# one before. The same known_at rules apply forward as in research (a forward CPI
# or NFP date counts only from 00:00 ET on its own day; an FOMC date from Dec 31 of
# the year before), so a live feature equals its research definition.
#
# FRED lists BLS dates about a month ahead, so an events part may end no later than
# ``forward_event_end_bound`` of the source it is derived from: the end of the month
# holding the latest listed CPI and NFP report (BLS publishes one report a month), and
# before the latest listed FOMC meeting. Past a part's end, ``known`` is None and a
# live module must not decide. A scheduled date can still move after a part is pinned
# (the 2025 shutdown did); ``check_forward_events`` re-derives each forward part from a
# refreshed source and lists what moved. A part whose days have started is never
# rewritten; a move on a day still ahead is a prospective correction (``derive_correction``): a new pinned
# part with a later available_at that answers only for later decisions, so effective history never changes.
# Every forward part carries available_at, the instant its source snapshot existed; before it the part
# answers None (2026-11-21, review of #1293: the 11-11 part's source was generated 11-13 04:12Z).
#
# The XNYS forward part is a slice of the live recorder's pinned
# ``xnys-rth-20261001-20271231.json`` (same exchange_calendars build), re-serialized
# exactly as the other XNYS parts.

LIVE_XNYS = ("xnys-rth-20261001-20271231.json",
             "efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad")
FORWARD_START = "2026-11-11"


def _month_end(day: str) -> str:
    d = date.fromisoformat(day)
    nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return (nxt - timedelta(days=1)).isoformat()


def forward_event_end_bound(econ: dict) -> str:
    """The last day an events part derived from ``econ`` may cover."""
    lists = [econ["events"].get(name) or [] for name in ("cpi", "nfp", "fomc")]
    if not all(lists):
        return ""   # a source missing CPI, NFP or FOMC dates supports no forward day at all
    bls = min(_month_end(max(days)) for days in lists[:2])
    fomc = max(lists[2])
    return min(bls, (date.fromisoformat(fomc) - timedelta(days=1)).isoformat())


def _available_at(econ: dict, available_at) -> str:
    src = econ.get("generated_at")
    if not src:
        raise ValueError("the source carries no generated_at, so when it existed is unknown")
    at = datetime.fromisoformat(available_at or src)
    if at.utcoffset() is None:
        raise ValueError("available_at must carry its offset")
    if at < datetime.fromisoformat(src):
        raise ValueError("a part cannot be available before its source snapshot was generated")
    return at.isoformat()


def derive_forward_events(econ: dict, source_sha256: str, start: str, end: str, available_at=None) -> dict:
    """A forward events part. ``available_at`` (default: the source's generated_at) is the instant from which
    the part answers; before it ``FactoryEvents.known`` returns None for every day the part covers."""
    if end > forward_event_end_bound(econ):
        raise ValueError(f"source lists BLS/FOMC dates only through {forward_event_end_bound(econ)}")
    if start > end:
        raise ValueError("empty forward part")
    return {**derive_events(econ, source_sha256, start, end), "available_at": _available_at(econ, available_at)}


def derive_correction(econ: dict, source_sha256: str, start: str, end: str, available_at: str, supersedes) -> dict:
    """A prospective correction part: re-derived [start, end] from a refreshed source, answering only for
    decisions after ``available_at`` (which must be later than every part it overlays). Earlier decisions keep
    the answer the superseded part gave them; pin it like any part, after the parts it supersedes."""
    doc = derive_forward_events(econ, source_sha256, start, end, available_at)
    return {**doc, "supersedes": sorted(supersedes)}


def _event_key(e):
    return (e["event"], e["day"])


def check_forward_events(econ: dict, source_sha256: str, root=None, pins=None) -> list:
    """Differences between each pinned forward part and the same part re-derived from ``econ``: the pinned
    digest, the coverage, a source horizon that no longer reaches the part's end ("horizon"), events added or
    removed, and same-day changes of time_et, known_at or basis.
    An empty list means every forward part still matches the refreshed source. Pure but for the file reads."""
    root = Path(root or Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests")
    out = []
    for name, expected in (pins or EVENT_PINS)[1:]:
        raw = (root / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            out.append({"part": name, "event": None, "day": None, "change": "digest",
                        "old": expected, "new": hashlib.sha256(raw).hexdigest()})
        doc = json.loads(raw)
        start, end = doc["coverage"]["start"], doc["coverage"]["end"]
        fresh = derive_events(econ, source_sha256, start, end)
        bound = forward_event_end_bound(econ)
        if end > bound:   # the refreshed source no longer lists BLS/FOMC dates through this part's end
            out.append({"part": name, "event": None, "day": None, "change": "horizon", "old": end, "new": bound})
        if fresh["coverage"] != doc["coverage"]:
            out.append({"part": name, "event": None, "day": None, "change": "coverage", "old": doc["coverage"], "new": fresh["coverage"]})
        if doc.get("available_at") is None:
            out.append({"part": name, "event": None, "day": None, "change": "no_available_at", "old": None, "new": None})
        old = {_event_key(e): e for e in doc["events"]}
        new = {_event_key(e): e for e in fresh["events"]}
        out += [{"part": name, "event": k[0], "day": k[1], "change": "removed", "old": old[k], "new": None} for k in sorted(old.keys() - new.keys())]
        out += [{"part": name, "event": k[0], "day": k[1], "change": "added", "old": None, "new": new[k]} for k in sorted(new.keys() - old.keys())]
        for k in sorted(old.keys() & new.keys()):
            a = {f: old[k].get(f) for f in ("time_et", "known_at", "basis")}
            b = {f: new[k].get(f) for f in ("time_et", "known_at", "basis")}
            if a != b:
                out.append({"part": name, "event": k[0], "day": k[1], "change": "changed", "old": a, "new": b})
    return sorted(out, key=lambda r: (r["day"] or "", r["event"] or "", r["change"]))


def slice_xnys(raw: bytes, start: str, end: str) -> bytes:
    """A contiguous XNYS part from a pinned manifest's bytes: same build, sessions in [start, end]."""
    doc = json.loads(raw)
    lo, hi = doc["calendar_version"].split(":")[-2:]
    if not (doc["start"] <= start <= end <= doc["end"] and lo <= start and end <= hi):
        raise ValueError("slice outside the source manifest's coverage")
    sessions = [s for s in doc["sessions"] if start <= s["key"].split(":")[1] <= end]
    out = {**doc, "start": start, "end": end, "sessions": sessions}
    return json.dumps(out, sort_keys=True, separators=(",", ":")).encode()
