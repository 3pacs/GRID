"""Version-pinned XNYS regular-session calendar adapter for offline research.

Optional dependency, imported only when explicitly constructed. Never falls
back to GRID's approximate holiday helper; excludes unsupported/closed dates.
Exchange announcements can override packaged schedules; version and bounded
coverage must remain in every result. No claim to model intraday trading halts.
"""

from datetime import date, datetime, timezone
from importlib.metadata import version
import re
from ingestion.realtime.research_intervals import Session

PINNED_VERSION = "4.11.2"
_KEY = re.compile(r"^XNYS:(\d{4}-\d{2}-\d{2}):RTH$")


class XNYSCalendar:
    def __init__(self, *, start="2020-01-01", end="2027-12-31"):
        import exchange_calendars

        installed = version("exchange_calendars")
        if installed != PINNED_VERSION or version("pandas") != "2.3.3":
            raise ValueError("unreviewed calendar dependency version")
        self.start, self.end = date.fromisoformat(start), date.fromisoformat(end)
        if self.end < self.start:
            raise ValueError("invalid calendar coverage")
        self.calendar = exchange_calendars.get_calendar("XNYS", start=start, end=end)
        self.version = f"exchange_calendars:{installed}:pandas:{version('pandas')}:numpy:{version('numpy')}:tzdata:{version('tzdata')}:XNYS:{start}:{end}"

    def prior_sessions(self, key, count=10):
        """Exact immediately preceding sessions, including early-close sessions."""
        if type(count) is not int or not 1 <= count <= 60:
            raise ValueError("bounded positive session count required")
        current = self(key)
        labels = self.calendar.sessions_window(key.split(":")[1], -(count + 1))
        sessions = tuple(self(f"XNYS:{str(day.date())}:RTH") for day in labels)
        if len(sessions) != count + 1 or sessions[-1] != current:
            raise ValueError("insufficient calendar history")
        return sessions[:-1]

    def __call__(self, key):
        match = _KEY.fullmatch(key)
        if not match:
            raise ValueError("XNYS regular-session key required")
        day = date.fromisoformat(match.group(1))
        if not (self.start <= day <= self.end):
            raise ValueError("calendar date outside verified bounds")
        label = day.isoformat()
        if not self.calendar.is_session(label):
            raise ValueError("not a trading session")
        opens = self.calendar.session_open(label).to_pydatetime()
        closes = self.calendar.session_close(label).to_pydatetime()
        if (
            not isinstance(opens, datetime)
            or opens.tzinfo is None
            or closes.tzinfo is None
        ):
            raise ValueError("calendar returned unzoned timestamps")
        return Session(
            key,
            opens.astimezone(timezone.utc),
            closes.astimezone(timezone.utc),
            self.version,
        )
