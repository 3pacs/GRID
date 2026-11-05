"""Source-level freshness for the SmartScheduler registry, read from pull_log.

The daily ``grid-data-freshness-check`` audit (task #153) is ticker-level: it
answers "does this ticker have a recent price", and cannot see a source that
stopped pulling. ``/system/pipeline-health`` reads ``raw_series`` rows per
``source_catalog`` entry, which says whether rows landed but not whether the
scheduler is still attempting the source, nor why it stopped.

This module answers the per-source question from ``pull_log`` — the record
both the grid-scheduler (``PullContext``) and Hermes's SmartScheduler
(``puller_name = "smart:" + registry name``) write for every attempt.

One distinction matters more than the rest. Since the honest-outcome contract
(``ingestion/smart_scheduler.py``, 2026-09-29), a pull that does not report a
committed row count, or that reports a per-item status the classifier does
not recognise (``NO_DATA``, ``NO_CIK``), is logged FAILED or PARTIAL even when
rows were written. Such a source is not "dead": the log cannot say either
way. It is reported as ``UNKNOWN`` (or ``PARTIAL`` when the message records a
positive "N rows written"), never as ``STALE``.

Read-only: ``fetch_pull_history`` issues two parameterized SELECTs in a
read-only transaction with a bounded statement timeout. Nothing is written.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.smart_scheduler import (
    PULLER_REGISTRY,
    SMART_PULL_LOG_PREFIX,
    catalog_name_for,
)

FRESH = "FRESH"
PARTIAL = "PARTIAL"          # rows landed since the last SUCCESS, but no run was logged SUCCESS
UNKNOWN = "UNKNOWN"          # attempts since the last SUCCESS failed the honest-outcome contract with unreported writes
STALE = "STALE"              # no SUCCESS within the window and no evidence of rows landing
NEVER = "NEVER"              # no pull_log row at all in the lookback

VERDICT_ORDER = (STALE, NEVER, UNKNOWN, PARTIAL, FRESH)

# Messages produced by the honest-outcome classifier in ingestion/smart_scheduler.py.
CONTRACT_MARKERS = (
    "did not report committed write count",
    "unrecognized puller status",
    "coverage incomplete or row count unknown",
)
_ROWS_WRITTEN = re.compile(r"(\d+) rows written")

MIN_STALE_AFTER_H = 48.0


@dataclass(frozen=True)
class PullRow:
    puller_name: str
    source_id: int | None
    started_at: datetime
    completed_at: datetime | None
    status: str
    rows_inserted: int | None
    error_message: str | None


@dataclass(frozen=True)
class SourceFreshness:
    name: str
    catalog_name: str
    freq_h: float
    verdict: str
    last_success: datetime | None
    last_attempt: datetime | None
    last_status: str | None
    last_error_class: str | None
    last_error: str | None
    attempts_since_success: int
    rows_reported_since_success: int | None

    def as_dict(self) -> dict[str, Any]:
        out = dict(self.__dict__)
        for key in ("last_success", "last_attempt"):
            if out[key] is not None:
                out[key] = out[key].isoformat()
        return out


def stale_after_hours(freq_h: float) -> float:
    """A source is fresh while its last SUCCESS is within two cadences (min 48 h)."""
    return max(2.0 * float(freq_h), MIN_STALE_AFTER_H)


def error_class(message: str | None) -> str | None:
    """Coarse cause of a non-SUCCESS pull, from its logged message."""
    if not message:
        return None
    m = message
    if any(marker in m for marker in CONTRACT_MARKERS):
        return "CONTRACT"
    if "TIMEOUT" in m or "timed out" in m or "Gateway Timeout" in m:
        return "TIMEOUT"
    if "not configured" in m or "no_api_key" in m or "auth_failed" in m:
        return "CONFIG"
    if "items completed" in m or "PARTIAL" in m:
        return "PARTIAL"
    return "OTHER"


def rows_written(row: PullRow) -> int | None:
    """Rows a pull reports writing: rows_inserted if positive, else parsed from the message.

    ``None`` means the log does not say; it is not zero.
    """
    if row.rows_inserted:
        return row.rows_inserted
    if row.error_message:
        found = _ROWS_WRITTEN.findall(row.error_message)
        if found:
            return max(int(n) for n in found)
    return None


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _attribution(registry: Sequence[Mapping[str, Any]]) -> tuple[dict[str, set[str]], frozenset[str]]:
    """Which pull_log names belong to which registry entry.

    ``smart:<name>`` is always unique. The catalog name identifies an entry
    only when no other entry shares it (several registry entries can write
    under one source_catalog row, e.g. three FRED-backed pullers); a shared
    catalog name is attributed to none of them. ``claimed`` is every name
    reserved this way, so a row carrying another entry's name is never
    picked up through a shared ``source_id``.
    """
    catalogs = [catalog_name_for(e["name"]) for e in registry]
    shared = {c for c in catalogs if catalogs.count(c) > 1}
    names = {
        e["name"]: {SMART_PULL_LOG_PREFIX + e["name"]} | ({c} if c not in shared else set())
        for e, c in zip(registry, catalogs)
    }
    claimed = frozenset().union(*names.values()) | frozenset(shared)
    return names, claimed


def _rows_for(
    entry: Mapping[str, Any],
    catalog_ids: Mapping[str, int],
    rows: Iterable[PullRow],
    names: set[str] | None = None,
    claimed: frozenset[str] = frozenset(),
) -> list[PullRow]:
    name = entry["name"]
    catalog = catalog_name_for(name)
    if names is None:
        names = {SMART_PULL_LOG_PREFIX + name, catalog}
    sid = catalog_ids.get(catalog.lower())
    return [
        r for r in rows
        if r.status != "RUNNING"
        and (
            r.puller_name in names
            or (sid is not None and r.source_id == sid and r.puller_name not in claimed)
        )
    ]


def assess_source(
    entry: Mapping[str, Any],
    catalog_ids: Mapping[str, int],
    rows: Iterable[PullRow],
    now: datetime,
    names: set[str] | None = None,
    claimed: frozenset[str] = frozenset(),
) -> SourceFreshness:
    name = entry["name"]
    freq_h = float(entry.get("freq_h", 24))
    mine = sorted(_rows_for(entry, catalog_ids, rows, names, claimed), key=lambda r: _as_utc(r.started_at))
    base = {"name": name, "catalog_name": catalog_name_for(name), "freq_h": freq_h}
    if not mine:
        return SourceFreshness(**base, verdict=NEVER, last_success=None, last_attempt=None,
                               last_status=None, last_error_class=None, last_error=None,
                               attempts_since_success=0, rows_reported_since_success=None)

    successes = [_as_utc(r.completed_at or r.started_at) for r in mine if r.status == "SUCCESS"]
    last_success = max(successes) if successes else None
    last = mine[-1]
    since = [r for r in mine if last_success is None or _as_utc(r.started_at) > last_success]
    since_failures = [r for r in since if r.status != "SUCCESS"]

    known = [n for n in (rows_written(r) for r in since_failures) if n is not None]
    rows_since = sum(known) if known else None

    if last_success is not None and now - last_success <= timedelta(hours=stale_after_hours(freq_h)):
        verdict = FRESH
    elif any(n for n in known):
        verdict = PARTIAL
    elif any(error_class(r.error_message) == "CONTRACT" and rows_written(r) is None for r in since_failures):
        verdict = UNKNOWN
    else:
        verdict = STALE

    return SourceFreshness(
        **base,
        verdict=verdict,
        last_success=last_success,
        last_attempt=_as_utc(last.started_at),
        last_status=last.status,
        last_error_class=error_class(last.error_message) if last.status != "SUCCESS" else None,
        last_error=(last.error_message or None) if last.status != "SUCCESS" else None,
        attempts_since_success=len(since_failures),
        rows_reported_since_success=rows_since,
    )


def assess(
    registry: Sequence[Mapping[str, Any]],
    catalog_ids: Mapping[str, int],
    rows: Sequence[PullRow],
    now: datetime | None = None,
) -> list[SourceFreshness]:
    """Classify every registry entry; worst verdicts first, then oldest success."""
    now = _as_utc(now) or datetime.now(timezone.utc)
    names, claimed = _attribution(registry)
    out = [assess_source(e, catalog_ids, rows, now, names[e["name"]], claimed) for e in registry]
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    return sorted(out, key=lambda s: (VERDICT_ORDER.index(s.verdict), s.last_success or epoch, s.name))


_CATALOG_SQL = text("SELECT id, name FROM source_catalog")
_PULL_LOG_SQL = text(
    "SELECT puller_name, source_id, started_at, completed_at, status, rows_inserted, error_message "
    "FROM pull_log WHERE started_at >= :since ORDER BY started_at"
)


def fetch_pull_history(engine: Engine, lookback_days: int = 45) -> tuple[dict[str, int], list[PullRow]]:
    """Read source_catalog ids and pull_log rows for the lookback window (read-only)."""
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    with engine.connect() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL statement_timeout = '30s'"))
        catalog = {str(name).lower(): int(sid) for sid, name in conn.execute(_CATALOG_SQL)}
        rows = [
            PullRow(
                puller_name=r[0], source_id=r[1], started_at=r[2], completed_at=r[3],
                status=r[4], rows_inserted=r[5], error_message=r[6],
            )
            for r in conn.execute(_PULL_LOG_SQL, {"since": since})
        ]
    return catalog, rows


def check(engine: Engine, lookback_days: int = 45, now: datetime | None = None) -> list[SourceFreshness]:
    catalog, rows = fetch_pull_history(engine, lookback_days)
    return assess(PULLER_REGISTRY, catalog, rows, now)
