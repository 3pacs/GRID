"""Time source for the paper log — the single seam tests monkeypatch.

Every entry point (preopen/postclose) takes an injectable ``now_fn``
defaulting to :func:`now_utc` here, so tests never depend on wall-clock
time and production always uses one unambiguous "now" per run (captured
once, at the top of the run, and threaded through).
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from paper_log.gex_levels.config import EASTERN


def now_utc() -> datetime:
    """Real wall-clock time, timezone-aware UTC. The production default."""
    return datetime.now(timezone.utc)


def to_eastern(dt: datetime) -> datetime:
    """Convert an aware UTC (or any aware) datetime to America/New_York."""
    if dt.tzinfo is None:
        raise ValueError("to_eastern requires a timezone-aware datetime")
    return dt.astimezone(EASTERN)


def session_date_for(dt: datetime) -> date:
    """The America/New_York calendar date a run at ``dt`` concerns."""
    return to_eastern(dt).date()
