"""Freshness of the daily publication review chain, for ``/api/v1/system/health``.

The chain is: ``scripts/daily_publication_packet.py`` (10:35Z) writes the day's packet to
the public tree -> ``scripts/grade_publication_review.py`` (11:35Z) writes review.jsonl and
hands it to ``scripts/ingest_publication_review.py`` -> rows in ``publication_review_runs``.
Before this module nothing reported a missing day anywhere a person or agent looks; a
failed producer left a FAILED receipt in a directory and a missing review left a REFUSED
receipt next to it. Health now carries the latest packet day on disk and the latest
ingested review day, and degrades when either is older than ``STALE_AFTER_DAYS``.

Bounded by construction: one ``MAX`` over a small table (one row per ingest run) and one
directory listing. Nothing here opens the packets.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

#: A day is published for the previous UTC day at 10:35Z and graded at 11:35Z, so at any
#: instant the newest expected day is "yesterday". Two days of slack covers a late producer
#: run, a weekend of flat publications and a re-run by hand before health goes red.
STALE_AFTER_DAYS = 2
DEFAULT_PUBLIC_ROOT = Path(os.environ.get("GRID_PUBLICATION_PACKET_PUBLIC_ROOT",
                                          "/data/grid/outputs/publication-packets"))
_DAY_DIR = re.compile(r"\d{4}-\d{2}-\d{2}")
_TABLE_EXISTS = text("SELECT to_regclass('public.publication_review_runs') IS NOT NULL")
_LATEST_RUN = text("SELECT MAX(publication_day), MAX(graded_at), MAX(ingested_at) FROM publication_review_runs")


@dataclass
class ReviewChainStatus:
    packet_root: str
    latest_packet_day: str | None = None
    latest_review_day: str | None = None
    latest_review_graded_at: str | None = None
    latest_review_ingested_at: str | None = None
    review_table_present: bool | None = None
    degraded_reasons: list[str] = field(default_factory=list)

    def as_check(self) -> dict[str, Any]:
        return {"packet_root": self.packet_root, "latest_packet_day": self.latest_packet_day,
                "latest_review_day": self.latest_review_day, "latest_review_graded_at": self.latest_review_graded_at,
                "latest_review_ingested_at": self.latest_review_ingested_at,
                "review_table_present": self.review_table_present, "stale": bool(self.degraded_reasons)}


def latest_packet_day(public_root: Path) -> date | None:
    """Newest ``YYYY-MM-DD`` directory under the public packet tree that holds a receipt."""
    try:
        names = os.listdir(public_root)
    except OSError:
        return None
    days: list[date] = []
    for name in names:
        if not _DAY_DIR.fullmatch(name):
            continue
        try:
            day = date.fromisoformat(name)
        except ValueError:
            continue
        if (public_root / name / "receipt.json").is_file():
            days.append(day)
    return max(days) if days else None


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return None if value is None else str(value)


def review_chain_status(conn: Any, *, public_root: Path | None = None, now: datetime | None = None) -> ReviewChainStatus:
    """Latest packet day on disk and latest ingested review day; degraded reasons when stale.

    Never raises: a missing table or an unreadable tree is reported, not thrown. The
    table-existence probe runs first so a missing table cannot poison the caller's
    transaction (a failed statement would abort every later one on the same connection).
    """
    now = now or datetime.now(timezone.utc)
    root = (public_root or DEFAULT_PUBLIC_ROOT).expanduser()
    status = ReviewChainStatus(packet_root=str(root))
    expected = now.date() - timedelta(days=1)
    stale_before = expected - timedelta(days=STALE_AFTER_DAYS)

    packet_day = latest_packet_day(root)
    status.latest_packet_day = packet_day.isoformat() if packet_day else None
    if root.is_dir():
        if packet_day is None:
            status.degraded_reasons.append("publication packets: no day published yet")
        elif packet_day < stale_before:
            status.degraded_reasons.append(f"publication packets stale: latest {packet_day}, expected {expected}")
    # No tree at all (a dev box, a fresh host) is "not configured", not a degradation.

    try:
        present = bool(conn.execute(_TABLE_EXISTS).scalar())
    except Exception:  # the probe itself failed: report unknown, never raise from health
        status.review_table_present = None
        return status
    status.review_table_present = present
    if not present:
        return status
    try:
        row = conn.execute(_LATEST_RUN).fetchone()
        values = tuple(row) if row is not None else ()
        review_day, graded_at, ingested_at = (*values, None, None, None)[:3]
        if isinstance(review_day, datetime):
            review_day = review_day.date()
        status.latest_review_day = review_day.isoformat() if isinstance(review_day, date) else None
        status.latest_review_graded_at = _iso(graded_at)
        status.latest_review_ingested_at = _iso(ingested_at)
    except Exception:
        status.degraded_reasons.append("publication review: latest-run query failed")
        return status
    if not isinstance(review_day, date):
        review_day = None
    if review_day is None:
        status.degraded_reasons.append("publication review: no day ingested yet")
    elif review_day < stale_before:
        status.degraded_reasons.append(f"publication review stale: latest {review_day}, expected {expected}")
    return status
