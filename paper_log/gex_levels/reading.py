"""Shared helpers for reading the JSONL log back.

Used by both ``status.py`` and ``evaluate.py`` so the two commands can
never disagree about what counts as a "valid session" — status.py's
session counts and evaluate.py's hypothesis sample sizes are built from
the exact same definition.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_log.gex_levels.storage import PaperLogStore


@dataclass(frozen=True)
class SessionPair:
    session_date: str  # ISO string, as persisted in JSONL
    preopen: dict[str, Any] | None
    postclose: dict[str, Any] | None

    @property
    def valid(self) -> bool:
        """Both legs exist and neither was excluded.

        A session missing its postclose leg entirely (job hasn't run yet,
        or the operator is looking mid-day) is neither valid nor excluded
        — it simply isn't done yet, and is left out of both counts rather
        than guessed at.
        """
        return (
            self.preopen is not None
            and not self.preopen.get("excluded")
            and self.postclose is not None
            and not self.postclose.get("excluded")
        )


def load_session_pairs(log_dir: Path) -> dict[str, SessionPair]:
    """One entry per session_date seen in the log, pairing its latest
    preopen and latest postclose record (if either exists)."""
    store = PaperLogStore(Path(log_dir))
    records = store.read_all()

    preopen_by_date: dict[str, dict[str, Any]] = {}
    postclose_by_date: dict[str, dict[str, Any]] = {}
    for r in records:
        d = r.get("session_date")
        if r.get("kind") == "preopen":
            preopen_by_date[d] = r  # last write for this date wins
        elif r.get("kind") == "postclose":
            postclose_by_date[d] = r

    all_dates = set(preopen_by_date) | set(postclose_by_date)
    return {
        d: SessionPair(
            session_date=d,
            preopen=preopen_by_date.get(d),
            postclose=postclose_by_date.get(d),
        )
        for d in all_dates
    }


def valid_session_pairs(log_dir: Path) -> list[SessionPair]:
    """Valid sessions only, sorted by session_date ascending."""
    pairs = load_session_pairs(log_dir)
    return sorted((p for p in pairs.values() if p.valid), key=lambda p: p.session_date)
