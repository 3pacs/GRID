"""Pinned historical XNYS schedule for factory development, no observed-day fallback."""

from datetime import date, timedelta
from pathlib import Path

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
