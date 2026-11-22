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
from bisect import bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.smart_scheduler import (
    PULLER_REGISTRY,
    SMART_PULL_LOG_PREFIX,
    catalog_name_for,
)
from store.observations import read_latest

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

    The exception is a catalog row in ``SERIES_ANCHORS``: runs of the other
    registry entries that share it (``smart:finra_dix`` under
    FINRA_SHORT_VOLUME) are not its runs and are dropped, so a DIX success
    can neither stand in for the short-volume puller's run nor a DIX
    failure for its failure.
    """
    merged: dict[int, CatalogRunSummary] = {}
    for row in rows:
        source_id, puller_name, last_run_at, last_rows_at, runs_7d, status, rows_inserted, error = row[:8]
        if str(puller_name or "") in GUEST_PULL_LOG_NAMES:
            continue
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


# ── Shared catalog rows: judged by the owner's own series ─────────────────
#
# Several registry entries can write under one source_catalog row. The
# source-level checks (scripts/hermes_health.py::check_db_health's
# stale_sources, which feeds the db.stale_sources alert and Hermes's
# stale-refresh; /system/pipeline-health; /system/freshness; the
# architecture puller stats) judge a row by "the newest raw_series row, or
# source_catalog.last_pull_at, for the source". On a shared row that reads
# one writer's output as another's.
#
# FINRA_SHORT_VOLUME is the row where that cost data. finra_dix writes
# finra:dix_sp500 under it every morning, and SmartScheduler._update_last_pull
# bumps its last_pull_at for each DIX run (REGISTRY_CATALOG_NAMES). From
# 2026-11-12 the short-volume puller skipped obs 11-12, 11-13 and 11-18..11-20
# (PR #1325) and every one of those checks still read the row as fresh for
# 8 days.
#
# A row listed here is judged by whether its owner's own series cover the
# newest trade date that is due by now:
#
# * due date: the newest NYSE session on the hash-pinned XNYS calendar
#   (intelligence.regime.market_state.xnys_sessions, the calendar the
#   market-state freshness rule uses, never the approximate scheduler
#   helpers) whose release clock has passed: published at the anchor's local
#   release time on that session, plus the owner's scheduler cadence for the
#   pull to land;
# * coverage: each anchor series' newest accepted observation, read through
#   store.observations (SUCCESS rows, newest obs_date at or before today,
#   that date's latest vintage, this catalog row's rows only), is dated on
#   or after the due date. The stalest anchor decides, so either half of the
#   short-volume write (the pinned producer's per-symbol rows, or the
#   aggregate subclass's ratios) going dark reads stale.
#
# When the newest observation landed (its pull_timestamp) is reported, never
# judged: a repair or re-pull of an old trade date lands a fresh vintage of
# an old date and says nothing about the current one. (Codex review
# 6009985487: obs 2026-11-10 re-landed at 17:40Z on 11-20 read HEALTHY under
# the landing rule.) Neither last_pull_at nor any other series under the same
# source_id counts, and pull_log runs of the other entries sharing the row
# are not its runs (merge_catalog_runs).


@dataclass(frozen=True)
class ReleaseClock:
    """When the observation for trade session ``d`` is due.

    Public at ``local_time`` on ``d`` in ``tz`` (so daylight saving is the
    exchange's), then ``due_after`` of elapsed time for the owner's scheduled
    pull to land it. Late, never early: a source is not called stale while
    its normal pull could still be on its way.
    """

    local_time: time
    tz: str
    due_after: timedelta
    basis: str

    def due_at(self, session: date) -> datetime:
        from zoneinfo import ZoneInfo

        public = datetime.combine(session, self.local_time, tzinfo=ZoneInfo(self.tz))
        return public.astimezone(timezone.utc) + self.due_after


@dataclass(frozen=True)
class SeriesAnchor:
    """A source_catalog row judged by the series its owning registry entry writes."""

    catalog_name: str
    owner: str  # PULLER_REGISTRY name whose freshness the catalog row reports
    series: tuple[str, ...]
    release: ReleaseClock


SERIES_ANCHORS: tuple[SeriesAnchor, ...] = (
    SeriesAnchor(
        catalog_name="FINRA_SHORT_VOLUME",
        owner="finra_short_volume",
        series=(
            # finra_short_volume_agg.AGG_MARKET_RATIO: one row per trade date.
            "finra:short_volume_agg:market_ratio",
            # FINRAShortVolumePuller.series_id("SPY"): the pinned producer's
            # per-symbol rows, present in every CNMS file.
            "finra:short_volume:SPY",
        ),
        release=ReleaseClock(
            local_time=time(18, 0),
            tz="America/New_York",
            # The owner's freq_h (tests/test_source_freshness_anchors.py pins it).
            due_after=timedelta(hours=24),
            basis=(
                "FINRA posts each session's Reg SHO daily file that evening, ~18:00 ET "
                "(smart_scheduler._finra_short_volume_trade_date's cutoff); due one "
                "24 h scheduler cadence later"
            ),
        ),
    ),
)


def anchor_for(catalog_name: str | None) -> SeriesAnchor | None:
    """The anchor for a catalog row (case-insensitive), or ``None``."""
    key = (catalog_name or "").strip().lower()
    return next((a for a in SERIES_ANCHORS if a.catalog_name.lower() == key), None)


def guest_pull_log_names(anchor: SeriesAnchor) -> frozenset[str]:
    """pull_log names of the other registry entries writing under ``anchor``'s row."""
    key = anchor.catalog_name.lower()
    return frozenset(
        SMART_PULL_LOG_PREFIX + e["name"]
        for e in PULLER_REGISTRY
        if e["name"] != anchor.owner and catalog_name_for(e["name"]).lower() == key
    )


GUEST_PULL_LOG_NAMES: frozenset[str] = frozenset().union(
    *(guest_pull_log_names(a) for a in SERIES_ANCHORS)
)


def expected_obs_date(anchor: SeriesAnchor, now: datetime) -> tuple[date, str]:
    """The newest pinned-XNYS session whose observation is due by ``now``, and the calendar version.

    Raises ValueError when ``now`` is outside the pinned calendar (extend the
    manifests rather than guess) or the manifests fail their pin check.
    """
    from intelligence.regime.market_state import xnys_sessions  # pinned manifests, read once per process

    start, end, days, version = xnys_sessions()
    now_utc = _as_utc(now) or datetime.now(timezone.utc)
    today = now_utc.date()  # no session dated after the UTC date can be due yet
    if not start <= today <= end:
        raise ValueError(
            f"{today} is outside the pinned XNYS calendar ({start}..{end}); extend the manifests"
        )
    i = bisect_right(days, today) - 1
    while i >= 0:
        if anchor.release.due_at(days[i]) <= now_utc:
            return days[i], version
        i -= 1
    raise ValueError(f"no XNYS session in the pinned calendar is due by {now_utc.isoformat()}")


@dataclass(frozen=True)
class AnchorReading:
    """The newest accepted observation of one anchor series."""

    series_id: str
    obs_date: date | None
    landed_at: datetime | None  # pull_timestamp of that observation, UTC

    def as_dict(self) -> dict[str, Any]:
        return {
            "series_id": self.series_id,
            "obs_date": self.obs_date.isoformat() if self.obs_date else None,
            "landed_at": self.landed_at.isoformat() if self.landed_at else None,
        }


@dataclass(frozen=True)
class AnchoredFreshness:
    """A shared catalog row's coverage of its due trade date, read from its owner's anchors."""

    catalog_name: str
    owner: str
    readings: tuple[AnchorReading, ...]
    expected_obs_date: date | None = None
    release_basis: str = ""
    calendar: str | None = None
    error: str | None = None

    @property
    def stalest(self) -> AnchorReading | None:
        """The anchor that decides: one with no observation, else the oldest newest-observation date."""
        oldest: AnchorReading | None = None
        for reading in self.readings:
            if reading.obs_date is None:
                return reading
            if oldest is None or (oldest.obs_date is not None and reading.obs_date < oldest.obs_date):
                oldest = reading
        return oldest

    @property
    def covered(self) -> bool:
        """Every anchor has an accepted observation dated on or after the due trade date.

        False when anything could not be read: an unread source is not a
        fresh one.
        """
        if self.error is not None or self.expected_obs_date is None or not self.readings:
            return False
        due = self.expected_obs_date
        return all(r.obs_date is not None and r.obs_date >= due for r in self.readings)

    @property
    def newest_obs_date(self) -> date | None:
        """The stalest anchor's newest observation date (what coverage is judged on)."""
        stalest = self.stalest
        return stalest.obs_date if stalest is not None else None

    @property
    def landed_at(self) -> datetime | None:
        """When the stalest anchor's newest observation landed. Reported, never judged."""
        stalest = self.stalest
        return stalest.landed_at if stalest is not None else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "owner": self.owner,
            "rule": "trade_date_coverage",
            "covered": self.covered,
            "expected_obs_date": self.expected_obs_date.isoformat() if self.expected_obs_date else None,
            "newest_obs_date": self.newest_obs_date.isoformat() if self.newest_obs_date else None,
            "landed_at": self.landed_at.isoformat() if self.landed_at else None,
            "release": self.release_basis,
            "calendar": self.calendar,
            "series": [r.as_dict() for r in self.readings],
            "error": self.error,
        }


def read_anchored_freshness(conn: Any, anchor: SeriesAnchor, *, now: datetime) -> AnchoredFreshness:
    """The due trade date at ``now`` and each anchor series' newest accepted observation (read-only).

    One ``store.observations.read_latest`` per anchor, constrained to the
    anchor's catalog row and to ``obs_date <= now``'s UTC date, each an index
    seek on ``idx_raw_series_series_obs``. The reads run in a savepoint, so a
    failed read never aborts the caller's transaction. A failed read or an
    unreadable calendar is reported in ``error`` and makes ``covered`` False.
    """
    now_utc = _as_utc(now) or datetime.now(timezone.utc)
    base: dict[str, Any] = {
        "catalog_name": anchor.catalog_name,
        "owner": anchor.owner,
        "release_basis": anchor.release.basis,
    }
    errors: list[str] = []
    try:
        expected, calendar = expected_obs_date(anchor, now_utc)
        base.update(expected_obs_date=expected, calendar=calendar)
    except Exception as exc:  # noqa: BLE001 - a health read reports, it never raises
        errors.append(f"calendar: {type(exc).__name__}: {exc}")
    readings: list[AnchorReading] = []
    try:
        with conn.begin_nested():
            for series_id in anchor.series:
                obs = read_latest(conn, series_id, source=anchor.catalog_name, as_of=now_utc.date())
                readings.append(AnchorReading(
                    series_id=series_id,
                    obs_date=obs.obs_date if obs is not None else None,
                    landed_at=_as_utc(obs.pull_timestamp) if obs is not None else None,
                ))
    except Exception as exc:  # noqa: BLE001 - a health read reports, it never raises
        errors.append(f"read: {type(exc).__name__}: {exc}")
    error = "; ".join(errors)[:LAST_ERROR_MAX_CHARS] if errors else None
    return AnchoredFreshness(readings=tuple(readings), error=error, **base)


def anchored_run(run: CatalogRunSummary | None) -> CatalogRunSummary | None:
    """An anchored row's pull_log summary, with the write evidence left to its anchors.

    pull_log still says whether and how the owner last ran (FAILED, skipped).
    Whether it wrote the due trade date is the anchor series' call: a run
    that committed per-symbol rows but no aggregate must not read healthy.
    """
    return replace(run, last_success_with_rows_at=None) if run is not None else None


def classify_anchored_source(
    *,
    now: datetime,
    fresh_hours: float,
    anchored: AnchoredFreshness,
    run: CatalogRunSummary | None,
) -> str:
    """Pipeline status for a ``SERIES_ANCHORS`` row (one of ``PIPELINE_STATUSES``).

    ``healthy`` only when the anchors cover the due trade date. Otherwise the
    owner's own runs say why (failing, skipped_config, stopped, inactive),
    and a recent landing or a "no new data" run reads ``stale``: the calendar
    says a trade date is due and it is not there, so it is neither healthy
    nor quiet.
    """
    if anchored.covered:
        return PIPELINE_HEALTHY
    status = classify_catalog_source(
        now=now, fresh_hours=fresh_hours, last_write_at=anchored.landed_at, run=anchored_run(run),
    )
    return PIPELINE_STALE if status in (PIPELINE_HEALTHY, PIPELINE_QUIET) else status
