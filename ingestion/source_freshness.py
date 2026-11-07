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
not recognise (e.g. ``NO_CIK``), is logged FAILED or PARTIAL even when
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


# ── Catalog-level run state (``/api/v1/system/pipeline-health``) ──────────
#
# ``assess`` above is registry-centric (one verdict per SmartScheduler entry).
# The pipeline-health endpoint is catalog-centric: one row per
# ``source_catalog`` entry, many of which are written by the grid-scheduler
# (``PullContext``), by bulk loaders, or by nothing at all. Before 2026-11-07
# that endpoint classified sources from ``raw_series`` alone, so a source
# that writes its own tables (options, FEC, lobbying, dark pool), a monthly
# source between releases, a source missing its API key, and a duplicate
# catalog row that never ran all read the same: "broken" (74 of 175 that
# day). These helpers classify each catalog row from its ``pull_log`` runs.

PIPELINE_HEALTHY = "healthy"                # committed rows within the freshness window
PIPELINE_STALE = "stale"                    # committed rows, but only within 2x the window
PIPELINE_QUIET = "quiet"                    # latest recent run succeeded with no new data
PIPELINE_SKIPPED_CONFIG = "skipped_config"  # latest recent run skipped / refused for missing key or config
PIPELINE_FAILING = "failing"                # latest recent run FAILED, TIMEOUT, or PARTIAL with 0 rows
PIPELINE_STOPPED = "stopped"                # ran or wrote before, nothing within 2x the window
PIPELINE_INACTIVE = "inactive"              # never ran, never wrote

PIPELINE_STATUSES = (
    PIPELINE_HEALTHY,
    PIPELINE_STALE,
    PIPELINE_QUIET,
    PIPELINE_SKIPPED_CONFIG,
    PIPELINE_FAILING,
    PIPELINE_STOPPED,
    PIPELINE_INACTIVE,
)
# summary.broken keeps meaning "needs an operator": failing + stopped.
PIPELINE_BROKEN_STATUSES = frozenset({PIPELINE_FAILING, PIPELINE_STOPPED})

RUN_ROWS = "rows"
RUN_NO_NEW_DATA = "no_new_data"
RUN_SKIPPED = "skipped"
RUN_FAILED = "failed"

LAST_ERROR_MAX_CHARS = 300
# Annual sources have a 400-day window; nothing older can change a status.
CATALOG_RUN_LOOKBACK_DAYS = 400
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class CatalogRunSummary:
    """The pull_log facts pipeline-health needs for one source_catalog row."""

    last_run_at: datetime | None
    last_success_with_rows_at: datetime | None
    last_status: str | None
    last_rows_inserted: int | None
    last_error: str | None
    runs_7d: int


# One pass over the bounded pull_log window. pull_log is small (14k rows,
# 4.9 MB on griddb 2026-11-07, ~250 rows/day) and has no source_id index, so
# a single scan beats 175 per-source probes. Grouped by
# (source_id, puller_name) so a row logged without a source_id can still be
# attributed by name. RUNNING rows are excluded: a restart can orphan them.
CATALOG_RUN_SUMMARY_SQL = text(
    "WITH pl AS ("
    "  SELECT source_id, puller_name, started_at, completed_at, status, "
    "         rows_inserted, error_message "
    "  FROM pull_log "
    "  WHERE started_at >= NOW() - make_interval(days => :lookback_days) "
    "    AND status <> 'RUNNING'"
    "), agg AS ("
    "  SELECT source_id, puller_name, "
    "    MAX(started_at) AS last_run_at, "
    "    MAX(COALESCE(completed_at, started_at)) FILTER ("
    "      WHERE status IN ('SUCCESS', 'PARTIAL') AND rows_inserted > 0"
    "    ) AS last_rows_at, "
    "    COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '7 days') AS runs_7d "
    "  FROM pl GROUP BY source_id, puller_name"
    "), latest AS ("
    "  SELECT DISTINCT ON (source_id, puller_name) "
    "    source_id, puller_name, status, rows_inserted, "
    "    LEFT(error_message, :error_chars) AS error_message "
    "  FROM pl ORDER BY source_id, puller_name, started_at DESC"
    ") "
    "SELECT a.source_id, a.puller_name, a.last_run_at, a.last_rows_at, a.runs_7d, "
    "  l.status, l.rows_inserted, l.error_message "
    "FROM agg a JOIN latest l "
    "  ON l.source_id IS NOT DISTINCT FROM a.source_id AND l.puller_name = a.puller_name"
)
# Read this many characters of each message; safe_last_error trims further
# after redaction (redaction can only shorten or keep a query string's
# parameter names, so 1000 is enough to redact a truncated URL correctly).
CATALOG_RUN_ERROR_CHARS = 1000


def _catalog_key_for_puller(puller_name: str) -> str:
    """The lower-cased catalog name a source_id-less pull_log row belongs to."""
    if puller_name.startswith(SMART_PULL_LOG_PREFIX):
        return catalog_name_for(puller_name[len(SMART_PULL_LOG_PREFIX):]).lower()
    return puller_name.lower()


def merge_catalog_runs(
    rows: Iterable[Sequence[Any]],
    catalog_ids: Mapping[str, int],
) -> dict[int, CatalogRunSummary]:
    """Fold ``CATALOG_RUN_SUMMARY_SQL`` rows into one summary per catalog id.

    Several pullers can write under one catalog row; the latest run among
    them supplies ``last_status``/``last_error``. A row without a
    ``source_id`` is attributed by name (``smart:<entry>`` through the
    registry's catalog mapping, otherwise the puller name itself) and is
    dropped when no catalog row has that name. ``catalog_ids`` is keyed by
    lower-cased catalog name.
    """
    merged: dict[int, CatalogRunSummary] = {}
    for row in rows:
        source_id, puller_name, last_run_at, last_rows_at, runs_7d, status, rows_inserted, error = row[:8]
        if source_id is None:
            source_id = catalog_ids.get(_catalog_key_for_puller(str(puller_name or "")))
            if source_id is None:
                continue
        new = CatalogRunSummary(
            last_run_at=_as_utc(last_run_at),
            last_success_with_rows_at=_as_utc(last_rows_at),
            last_status=status,
            last_rows_inserted=rows_inserted,
            last_error=error,
            runs_7d=int(runs_7d or 0),
        )
        old = merged.get(int(source_id))
        if old is None:
            merged[int(source_id)] = new
            continue
        latest = new if (new.last_run_at or _EPOCH) > (old.last_run_at or _EPOCH) else old
        rows_at = [t for t in (old.last_success_with_rows_at, new.last_success_with_rows_at) if t]
        merged[int(source_id)] = CatalogRunSummary(
            last_run_at=latest.last_run_at,
            last_success_with_rows_at=max(rows_at) if rows_at else None,
            last_status=latest.last_status,
            last_rows_inserted=latest.last_rows_inserted,
            last_error=latest.last_error,
            runs_7d=old.runs_7d + new.runs_7d,
        )
    return merged


def run_outcome(status: str | None, rows_inserted: int | None, error_message: str | None) -> str:
    """What one pull_log run did: committed rows, found nothing, was skipped, or failed.

    pull_log's CHECK constraint has no SKIPPED / NO_NEW_DATA status, so both
    are SUCCESS rows marked by their ``error_message`` prefix (see
    ``PullContext.set_note`` and ``SmartScheduler._log_run``). A FAILED or
    PARTIAL run whose message names a missing key or config
    (``error_class == "CONFIG"``: ``no_api_key``, ``auth_failed``,
    ``... not configured``) is a configuration skip, not an outage.
    """
    message = error_message or ""
    if status in ("SUCCESS", "PARTIAL") and rows_inserted and rows_inserted > 0:
        return RUN_ROWS
    if message.startswith("SKIPPED"):
        return RUN_SKIPPED
    if status == "SUCCESS":
        return RUN_NO_NEW_DATA  # NO_NEW_DATA marker, or an honest 0-row success
    if error_class(message) == "CONFIG":
        return RUN_SKIPPED
    return RUN_FAILED


def catalog_freshness_hours(name: str, explicit: Mapping[str, tuple[str, int]] | None = None) -> float:
    """Freshness window for one catalog row, in hours.

    An explicit per-name threshold (pipeline-health's schedule map) wins;
    otherwise the fastest SmartScheduler cadence writing under that catalog
    name, as ``stale_after_hours``; otherwise 7 days.
    """
    if explicit and name in explicit:
        return float(explicit[name][1])
    key = name.lower()
    freqs = [
        float(e.get("freq_h", 24))
        for e in PULLER_REGISTRY
        if catalog_name_for(e["name"]).lower() == key
    ]
    if freqs:
        return stale_after_hours(min(freqs))
    return 168.0


def classify_catalog_source(
    *,
    now: datetime,
    fresh_hours: float,
    last_write_at: datetime | None,
    run: CatalogRunSummary | None,
) -> str:
    """One catalog row's pipeline status (one of ``PIPELINE_STATUSES``).

    ``last_write_at`` is the newest raw_series write, or the catalog's own
    ``last_pull_at`` (bumped only on a fresh pull). ``run`` is the row's
    pull_log summary, ``None`` when it has no runs in the lookback. A
    "recent" run is one within twice the freshness window, the band the
    endpoint has always called "stale".
    """
    now = _as_utc(now) or datetime.now(timezone.utc)
    last_rows_at = newest_rows_at(last_write_at, run)
    fresh = timedelta(hours=fresh_hours)
    recent = timedelta(hours=2 * fresh_hours)

    if last_rows_at is not None and now - last_rows_at <= fresh:
        return PIPELINE_HEALTHY
    last_run_at = run.last_run_at if run else None
    activity = [t for t in (last_rows_at, last_run_at) if t]
    if not activity:
        return PIPELINE_INACTIVE
    if now - max(activity) > recent:
        return PIPELINE_STOPPED
    if last_run_at is None or now - last_run_at > recent:
        return PIPELINE_STALE  # rows landed within 2x the window; no run of record since
    outcome = run_outcome(run.last_status, run.last_rows_inserted, run.last_error)
    if outcome == RUN_ROWS:
        return PIPELINE_STALE
    if outcome == RUN_NO_NEW_DATA:
        return PIPELINE_QUIET
    if outcome == RUN_SKIPPED:
        return PIPELINE_SKIPPED_CONFIG
    return PIPELINE_FAILING


def newest_rows_at(last_write_at: datetime | None, run: CatalogRunSummary | None) -> datetime | None:
    """Newest committed-rows time from either raw_series or pull_log."""
    rows_at = [t for t in (_as_utc(last_write_at), run.last_success_with_rows_at if run else None) if t]
    return max(rows_at) if rows_at else None


def safe_last_error(message: str | None, max_chars: int = LAST_ERROR_MAX_CHARS) -> str | None:
    """A pull_log message fit for an API response: credentials stripped, then truncated."""
    from utils.redaction import redact_secrets

    if not message:
        return None
    cleaned = redact_secrets(message) or ""
    if len(cleaned) > max_chars:
        cleaned = cleaned[: max_chars - 1].rstrip() + "…"
    return cleaned
