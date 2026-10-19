"""Read-only provenance envelope for the tables the flows API reads directly.

``api/routers/flows.py`` reads ``insider_trades``, ``congressional_trades``,
``dark_pool_weekly``, ``etf_flows`` and ``junction_point_readings`` straight
from tables that ``ingestion/flow_materializer.py::sync_all`` fills. When that
sync stops running, the API keeps serving the last materialized rows with no
signal that they are old. This module computes, per table, a small envelope the
API attaches to its response::

    {"table": ..., "row_count": ..., "max_obs_date": ..., "materialized_at": ...,
     "source_last_ingested": ..., "age_days": ..., "stale_after_days": ...,
     "unavailable": bool, "reason": str | None}

It describes **materialization state, not data truth**: "available" means the
table has rows whose newest observation date is within the table's threshold;
it does not say the values are right or complete. See
``docs/flows_provenance_contract.md``.

Safety:

* Every statement is a fixed literal in :data:`TABLE_SPECS` (identifiers cannot
  be bound; none comes from a caller), values are bound parameters.
* Every probe runs in its own short transaction under
  ``SET LOCAL statement_timeout = 5000``. A timeout, a missing table or any
  other error becomes ``unavailable = True`` with a reason; nothing is raised.
* ``raw_series`` is only touched with ``series_id`` and ``obs_date`` bounds, and
  only to find ``source_last_ingested``; a failure there yields ``null`` and
  never changes availability.
* Results are cached for :data:`CACHE_TTL_SECONDS` so a busy endpoint does not
  re-probe on every request.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

from loguru import logger as log
from sqlalchemy import text

from utils.ttl_cache import TTLCache

STATEMENT_TIMEOUT_MS = 5000
_SET_TIMEOUT_SQL = "SET LOCAL statement_timeout = 5000"  # keep in step with STATEMENT_TIMEOUT_MS
CACHE_TTL_SECONDS = 30.0

DAILY_STALE_AFTER_DAYS = 3
WEEKLY_STALE_AFTER_DAYS = 10

_SOURCE_LOOKBACK_DAYS = 30

_cache: TTLCache = TTLCache(ttl=CACHE_TTL_SECONDS, max_size=64)


@dataclass(frozen=True)
class TableSpec:
    """One table the flows API reads.

    ``sql`` returns exactly ``(row_count, max_obs_date, materialized_at)``;
    ``row_count`` is NULL for tables too large to count on a request path.
    ``source_signal_type`` names the ``signal_sources.source_type`` that feeds
    the table (indexed on ``(source_type, signal_date DESC)``);
    ``source_raw_series_ids`` names ``raw_series`` series that feed it.
    """

    table: str
    cadence: str  # "daily" | "weekly"
    sql: str
    source_signal_type: str | None = None
    source_raw_series_ids: tuple[str, ...] = ()

    @property
    def stale_after_days(self) -> int:
        return WEEKLY_STALE_AFTER_DAYS if self.cadence == "weekly" else DAILY_STALE_AFTER_DAYS


# Materialized tables (ingestion/flow_materializer.py). created_at is the time
# a row was first inserted: the materializer upserts, so rows it only updates
# do not move it. materialized_at is therefore a lower bound on the last sync.
TABLE_SPECS: dict[str, TableSpec] = {
    "insider_trades": TableSpec(
        "insider_trades", "daily",
        "SELECT count(*), max(trade_date), max(created_at) FROM insider_trades",
        source_signal_type="insider",
    ),
    "congressional_trades": TableSpec(
        "congressional_trades", "daily",
        "SELECT count(*), max(disclosure_date), max(created_at) FROM congressional_trades",
        source_signal_type="congressional",
    ),
    "dark_pool_weekly": TableSpec(
        "dark_pool_weekly", "weekly",
        "SELECT count(*), max(report_date), max(created_at) FROM dark_pool_weekly",
        source_signal_type="darkpool",
    ),
    "etf_flows": TableSpec(
        "etf_flows", "daily",
        "SELECT count(*), max(flow_date), max(created_at) FROM etf_flows",
    ),
    "junction_point_readings": TableSpec(
        "junction_point_readings", "daily",
        "SELECT count(*), max(obs_date), max(created_at) FROM junction_point_readings",
        # FRED series behind ingestion/flow_materializer.JUNCTION_SERIES.
        source_raw_series_ids=("WALCL", "RRPONTSYD", "WTREGEN", "M2SL", "TOTBKCR"),
    ),
    # Not materialized by flow_materializer; read directly by the money-map,
    # company drill-down, sankey and sector endpoints. Counting is skipped.
    "signal_sources": TableSpec(
        "signal_sources", "daily",
        "SELECT NULL::bigint, max(signal_date), max(created_at) FROM signal_sources "
        "WHERE source_type IN ('insider', 'congressional', 'darkpool') "
        "AND signal_date >= CURRENT_DATE - 30",
    ),
    "options_daily_signals": TableSpec(
        "options_daily_signals", "daily",
        "SELECT NULL::bigint, max(signal_date), max(created_at) FROM options_daily_signals "
        "WHERE signal_date >= CURRENT_DATE - 30",
    ),
    "capital_flow_snapshots": TableSpec(
        "capital_flow_snapshots", "daily",
        "SELECT count(*), max(snapshot_date), max(generated_at) FROM capital_flow_snapshots",
    ),
}

_SOURCE_BY_SIGNAL_SQL = (
    "SELECT max(created_at) FROM signal_sources "
    "WHERE source_type = :source_type AND signal_date >= CURRENT_DATE - 30"
)
_SOURCE_BY_RAW_SERIES_SQL = (
    "SELECT max(pull_timestamp) FROM raw_series "
    "WHERE series_id = ANY(:series_ids) AND obs_date >= CURRENT_DATE - 30 "
    "AND pull_status = 'SUCCESS'"
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _is_timeout(exc: BaseException) -> bool:
    orig = getattr(exc, "orig", exc)
    if getattr(orig, "pgcode", None) == "57014":
        return True
    msg = str(exc).lower()
    return "statement timeout" in msg or "canceling statement" in msg


def _is_missing_table(exc: BaseException) -> bool:
    orig = getattr(exc, "orig", exc)
    return getattr(orig, "pgcode", None) == "42P01" or "does not exist" in str(exc).lower()


def _unavailable(spec: TableSpec, reason: str, now: datetime, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "table": spec.table,
        "row_count": None,
        "max_obs_date": None,
        "materialized_at": None,
        "source_last_ingested": None,
        "age_days": None,
        "stale_after_days": spec.stale_after_days,
        "unavailable": True,
        "reason": reason,
    }
    out.update(extra)
    return out


def _source_last_ingested(engine: Any, spec: TableSpec) -> str | None:
    """Newest ingest time of the rows that feed ``spec``; ``None`` if not derivable."""
    if spec.source_signal_type:
        sql, params = _SOURCE_BY_SIGNAL_SQL, {"source_type": spec.source_signal_type}
    elif spec.source_raw_series_ids:
        sql, params = _SOURCE_BY_RAW_SERIES_SQL, {"series_ids": list(spec.source_raw_series_ids)}
    else:
        return None
    try:
        with engine.connect() as conn:
            conn.execute(text(_SET_TIMEOUT_SQL))
            return _iso(conn.execute(text(sql), params).scalar())
    except Exception as exc:  # noqa: BLE001 - upstream lineage is best-effort
        log.debug("provenance: source lookup failed for {t}: {e}", t=spec.table, e=str(exc))
        return None


def table_provenance(engine: Any, table: str, *, now: datetime | None = None,
                     use_cache: bool = True) -> dict[str, Any]:
    """Provenance envelope for one table. Never raises."""
    now = now or _now_utc()
    spec = TABLE_SPECS.get(table)
    if spec is None:
        return {
            "table": table, "row_count": None, "max_obs_date": None, "materialized_at": None,
            "source_last_ingested": None, "age_days": None, "stale_after_days": None,
            "unavailable": True, "reason": "unknown_table",
        }
    if use_cache:
        cached = _cache.get(table)
        if cached is not None:
            return dict(cached)

    result = _probe(engine, spec, now)
    if use_cache:
        _cache.set(table, dict(result))
    return result


def _probe(engine: Any, spec: TableSpec, now: datetime) -> dict[str, Any]:
    try:
        with engine.connect() as conn:
            conn.execute(text(_SET_TIMEOUT_SQL))
            if conn.execute(text("SELECT to_regclass(:t)"), {"t": spec.table}).scalar() is None:
                return _unavailable(spec, "missing_table", now)
            row = conn.execute(text(spec.sql)).fetchone()
    except Exception as exc:  # noqa: BLE001 - the envelope must never break the endpoint
        if _is_timeout(exc):
            return _unavailable(spec, "timeout", now)
        if _is_missing_table(exc):
            return _unavailable(spec, "missing_table", now)
        log.warning("provenance: probe of {t} failed: {e}", t=spec.table, e=str(exc))
        return _unavailable(spec, f"error:{type(exc).__name__}", now)

    row_count, max_obs, materialized_at = tuple(row) if row else (0, None, None)
    base = {
        "table": spec.table,
        "row_count": int(row_count) if row_count is not None else None,
        "max_obs_date": _iso(max_obs),
        "materialized_at": _iso(materialized_at),
        "source_last_ingested": _source_last_ingested(engine, spec),
        "age_days": None,
        "stale_after_days": spec.stale_after_days,
        "unavailable": False,
        "reason": None,
    }
    if (row_count == 0) or max_obs is None:
        base.update(unavailable=True, reason="empty")
        return base

    obs_day = max_obs.date() if isinstance(max_obs, datetime) else max_obs
    if not isinstance(obs_day, date):
        base.update(unavailable=True, reason="error:bad_date")
        return base
    age = (now.date() - obs_day).days
    base["age_days"] = age
    if age > spec.stale_after_days:
        base.update(unavailable=True, reason="stale")
    return base


def endpoint_provenance(engine: Any, tables: Iterable[str], *,
                        now: datetime | None = None) -> dict[str, Any]:
    """``{<table>: envelope, ..., "generated_at": iso}`` for an endpoint. Never raises."""
    now = now or _now_utc()
    out: dict[str, Any] = {}
    for table in tables:
        try:
            out[table] = table_provenance(engine, table, now=now)
        except Exception as exc:  # noqa: BLE001 - defence in depth
            out[table] = {"table": table, "unavailable": True, "reason": f"error:{type(exc).__name__}"}
    out["generated_at"] = now.isoformat()
    return out


def clear_cache() -> None:
    _cache.clear()
