"""Pinned offline XNYS manifest export and dependency-free runtime calendar.

Export in the reviewed calendar environment only. Runtime consumes an immutable
operator-pinned SHA256, not an untrusted hash beside a downloaded file. This is a
schedule snapshot, not a holiday approximation or exchange-announcement watcher.
No network, dependency installation or production mutation occurs here.
"""

from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from ingestion.realtime.research_intervals import Session

SCHEMA = "grid.xnys-session-manifest.v1"
MAX_BYTES = 1_048_576
VERSION = re.compile(
    r"^exchange_calendars:4\.11\.2:pandas:2\.3\.3:numpy:[0-9.]+:tzdata:[0-9.]+:XNYS:\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}$"
)
KEY = re.compile(r"^XNYS:(\d{4}-\d{2}-\d{2}):RTH$")


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("ISO date required")
    return date.fromisoformat(value)


def _utc(value):
    if not isinstance(value, str):
        raise ValueError("UTC ISO timestamp required")
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.utcoffset().total_seconds() != 0
        or result.second
        or result.microsecond
    ):
        raise ValueError("minute-aligned UTC timestamp required")
    return result.astimezone(timezone.utc)


def export_manifest(calendar, *, start, end):
    """Produce canonical bytes from the existing pinned XNYSCalendar adapter."""
    begin, finish = _day(start), _day(end)
    if (
        begin > finish
        or (finish - begin).days > 1096
        or not VERSION.fullmatch(calendar.version)
    ):
        raise ValueError("unsupported calendar version or coverage")
    if begin < calendar.start or finish > calendar.end:
        raise ValueError("requested manifest exceeds verified calendar bounds")
    sessions = []
    for label in calendar.calendar.sessions_in_range(start, end):
        key = f"XNYS:{label.date().isoformat()}:RTH"
        session = calendar(key)
        sessions.append(
            {
                "key": key,
                "opens": session.opens.astimezone(timezone.utc).isoformat(),
                "closes": session.closes.astimezone(timezone.utc).isoformat(),
            }
        )
    value = {
        "schema": SCHEMA,
        "exchange": "XNYS",
        "session_type": "RTH",
        "timezone": "America/New_York",
        "start": start,
        "end": end,
        "calendar_version": calendar.version,
        "sessions": sessions,
    }
    raw = canonical(value)
    _parse(raw, hashlib.sha256(raw).hexdigest())
    return raw


def _parse(raw, expected_sha256):
    if not isinstance(expected_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_sha256
    ):
        raise ValueError("operator-pinned SHA256 required")
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("manifest size or hash mismatch")

    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("duplicate JSON key")
            out[key] = value
        return out

    doc = json.loads(raw, object_pairs_hook=unique)
    fields = {
        "schema",
        "exchange",
        "session_type",
        "timezone",
        "start",
        "end",
        "calendar_version",
        "sessions",
    }
    if (
        not isinstance(doc, dict)
        or set(doc) != fields
        or (doc["schema"], doc["exchange"], doc["session_type"], doc["timezone"])
        != (SCHEMA, "XNYS", "RTH", "America/New_York")
    ):
        raise ValueError("manifest identity/schema mismatch")
    if not isinstance(doc["calendar_version"], str) or not VERSION.fullmatch(
        doc["calendar_version"]
    ):
        raise ValueError("unreviewed dependency provenance")
    begin, finish = _day(doc["start"]), _day(doc["end"])
    if begin > finish or (finish - begin).days > 1096:
        raise ValueError("invalid coverage bounds")
    dependency_bounds = doc["calendar_version"].split(":")[-2:]
    if begin < _day(dependency_bounds[0]) or finish > _day(dependency_bounds[1]):
        raise ValueError("manifest exceeds dependency calendar coverage")
    rows = doc["sessions"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
        raise ValueError("bounded nonempty sessions required")
    output = []
    last_day = None
    last_close = None
    version = f"manifest:{expected_sha256}:{doc['calendar_version']}"
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != {"key", "opens", "closes"}
            or not isinstance(row["key"], str)
        ):
            raise ValueError("session shape")
        match = KEY.fullmatch(row["key"])
        if not match:
            raise ValueError("session identity")
        day = _day(match.group(1))
        op, cl = _utc(row["opens"]), _utc(row["closes"])
        minutes = (cl - op).total_seconds() / 60
        if (
            not begin <= day <= finish
            or (last_day and day <= last_day)
            or op.date() != day
            or cl.date() != day
            or op.minute != 30
            or op.hour not in (13, 14)
            or not 0 < minutes <= 390
            or (last_close and op <= last_close)
        ):
            raise ValueError("invalid session order/bounds")
        output.append(Session(row["key"], op, cl, version))
        last_day = day
        last_close = cl
    if raw != canonical(doc):
        raise ValueError("canonical manifest bytes required")
    return doc, tuple(output)


class ManifestCalendar:
    """Runtime requires only Python stdlib and existing Session dataclass."""

    def __init__(self, path, expected_sha256):
        p = Path(path).absolute()
        if p.resolve() != p:
            raise ValueError("manifest symlink refused")
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            st = os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or stat.S_IMODE(st.st_mode) & 0o022:
                raise ValueError("regular non-group/world-writable manifest required")
            raw = stream.read(MAX_BYTES + 1)
        doc, self.sessions = _parse(raw, expected_sha256)
        self.start, self.end = _day(doc["start"]), _day(doc["end"])
        self.version = self.sessions[0].calendar_version
        self._by_key = {s.key: s for s in self.sessions}
        self._index = {s.key: i for i, s in enumerate(self.sessions)}

    def __call__(self, key):
        if not isinstance(key, str) or key not in self._by_key:
            raise ValueError("unknown/closed/out-of-range manifest session")
        return self._by_key[key]

    def prior_sessions(self, key, count=10):
        self(key)
        if type(count) is not int or not 1 <= count <= 60:
            raise ValueError("bounded prior-session count required")
        i = self._index[key]
        if i < count:
            raise ValueError("insufficient manifest history")
        return self.sessions[i - count : i]
