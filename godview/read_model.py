"""godview.read_model — read-only, point-in-time reader for the god-view pillars (slice G8).

The API (``api/routers/godview.py``) is a thin wrapper around this module.
Plan: ``GRID-GODVIEW-MATERIALIZATION-PLAN-20260926.md`` §5 and slice G8.

What it reads
-------------
The three pillar tables and the ``godview_runs`` ledger added by G2
(``migrations/versions/godview_writers_20260926.py``). It reads them directly,
not through ``market_god_view_daily``. G6 replaces that matview with a
PIT-correct view, but the API does not depend on G6 and works whether G6 is
applied or not.

The rules
---------
* **Provenance rows only.** A row with ``provenance IS NULL`` is a legacy or
  incident row (fabricated WALCL constants, CFTC market-mixed z-scores,
  impossible GEX flips). It is never served, not even as a fallback.
* **Point in time.** A row is visible at ``as_of`` only when
  ``available_at <= as_of`` and ``release_at <= as_of``. The tables hold the
  latest vintage of each key. A writer that revises a row bumps its
  ``available_at``, so an ``as_of`` before the revision sees no row for that
  key (conservative: never the revised value early). Ledger rows are
  visible only once ``finished_at <= as_of``.
* **Honest states.** Each pillar is ``available``, ``stale``, ``partial`` (CFTC:
  some tracked markets missing or stale) or ``unavailable``. An unavailable
  pillar carries a ``reason`` from the schema probe or the run ledger, and
  every value is ``None``. Nothing here returns a 0, a midpoint or a
  "neutral" in place of a missing value.
* **GEX is modeled.** Every served GEX row is ``provenance = 'modeled'`` and
  ``estimated = True`` with the engine's basis and sign convention. Until the
  G5 writer has run, the pillar is ``unavailable`` with reason ``never_run``.
* **Read-only.** Every statement is a ``SELECT`` (``tests/godview/
  test_godview_api_readonly_pg.py`` asserts this against PostgreSQL). A
  missing table or column degrades to ``unavailable``; it never raises.

No SQL here is built with f-strings or ``.format()``: every statement is a
static ``text()`` with bound parameters.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ingestion.altdata.cftc_markets import MARKETS as CFTC_MARKETS
from ingestion.market_calendar import last_trading_day
from store.availability import measured_or_none

ET = ZoneInfo("America/New_York")

PILLAR_FED = "fed_liquidity"
PILLAR_CFTC = "cftc"
PILLAR_GEX = "dealer_gex"
PILLARS: tuple[str, ...] = (PILLAR_FED, PILLAR_CFTC, PILLAR_GEX)

PILLAR_TABLES: dict[str, str] = {
    PILLAR_FED: "fed_net_liquidity_daily",
    PILLAR_CFTC: "cftc_positioning_daily",
    PILLAR_GEX: "dealer_gex_daily",
}
RUNS_TABLE = "godview_runs"

STATUS_AVAILABLE = "available"
STATUS_STALE = "stale"
STATUS_PARTIAL = "partial"
STATUS_UNAVAILABLE = "unavailable"

# Unavailable reasons. The ledger-driven ones mirror godview_runs.status.
REASON_SCHEMA_NOT_MIGRATED = "schema_not_migrated"
REASON_NEVER_RUN = "never_run"
REASON_WRITER_FAILED = "writer_failed"
REASON_BLOCKED_BY_LEGACY = "blocked_by_legacy_rows"
REASON_NO_ROWS = "no_rows_available_at_as_of"
REASON_NO_MARKET_ROW = "no_row_for_market"
REASON_STALE = "stale"
_LEDGER_PASSTHROUGH_REASONS = frozenset(
    {"inputs_missing", "inputs_stale", "non_session", "no_completed_capture", "no_verified_spot"}
)

# Cadence rules (plan §5). Fed: the H.4.1 Wednesday is weekly, so more than
# 9 days means at least one release was missed. CFTC: weekly Friday release,
# 10 days after release_at. GEX: the row must be from the previous NYSE
# session or later.
FED_STALE_AFTER_DAYS = 9
CFTC_STALE_AFTER_DAYS = 10

#: v1 GEX grain: SPY only (the engine's only verified spot contract).
GEX_TICKER = "SPY"
GEX_MODEL_NOTE = (
    "Modeled estimate: dealer side assumed (long calls / short puts); "
    "not measured positioning."
)

FED_UNIT = "USD millions"

HISTORY_MAX_LIMIT = 1000
HISTORY_DEFAULT_LIMIT = 500
HISTORY_DEFAULT_DAYS = 365

#: Tracked CFTC markets in display order, keyed by root symbol.
CFTC_TRACKED: tuple[tuple[str, str, str], ...] = tuple(
    (m.root, m.code, m.label) for m in CFTC_MARKETS.values()
)
CFTC_TRACKED_ROOTS = frozenset(root for root, _, _ in CFTC_TRACKED)

# Clock skew allowance for a caller-supplied as_of that is marginally ahead.
_FUTURE_TOLERANCE = timedelta(seconds=60)


# ---------------------------------------------------------------------------
# as_of
# ---------------------------------------------------------------------------


class AsOfError(ValueError):
    """The caller's ``as_of`` could not be used (unparseable, naive, or in the future)."""


def resolve_as_of(raw: str | None, *, now: datetime) -> tuple[datetime, str]:
    """Return ``(cutoff, source)`` for a request.

    * ``None``: ``now``; source ``server_now``.
    * ``YYYY-MM-DD``: the end of that day in America/New_York (so a Friday
      15:30 ET CFTC release is visible "as of Friday"), capped at ``now``.
    * An ISO-8601 datetime with an offset (``Z`` accepted). A naive datetime
      is refused: the cutoff must not depend on the server's timezone.

    A cutoff more than a minute after ``now`` is refused rather than clamped:
    the caller asked for a moment whose data cannot exist yet.
    """
    now = _aware_utc(now)
    if raw is None or raw.strip() == "":
        return now, "server_now"
    raw = raw.strip()
    if len(raw) == 10:
        try:
            day = date.fromisoformat(raw)
        except ValueError as exc:
            raise AsOfError(f"as_of is not an ISO date: {raw!r}") from exc
        end_of_day = datetime.combine(day + timedelta(days=1), time(0), tzinfo=ET) - timedelta(microseconds=1)
        start_of_day = datetime.combine(day, time(0), tzinfo=ET)
        if start_of_day > now + _FUTURE_TOLERANCE:
            raise AsOfError("as_of is in the future")
        return min(end_of_day.astimezone(timezone.utc), now), "request"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AsOfError(f"as_of is not an ISO-8601 date or datetime: {raw!r}") from exc
    if parsed.tzinfo is None:
        raise AsOfError("as_of datetime must carry a UTC offset (e.g. 2026-09-25T20:00:00Z)")
    parsed = parsed.astimezone(timezone.utc)
    if parsed > now + _FUTURE_TOLERANCE:
        raise AsOfError("as_of is in the future")
    return min(parsed, now), "request"


def et_date(ts: datetime) -> date:
    return _aware_utc(ts).astimezone(ET).date()


# ---------------------------------------------------------------------------
# Pure classification
# ---------------------------------------------------------------------------


def fed_is_stale(obs_date: date, as_of: datetime) -> bool:
    return (et_date(as_of) - obs_date).days > FED_STALE_AFTER_DAYS


def cftc_is_stale(release_at: datetime, as_of: datetime) -> bool:
    return _aware_utc(as_of) - _aware_utc(release_at) > timedelta(days=CFTC_STALE_AFTER_DAYS)


def gex_is_stale(obs_date: date, as_of: datetime) -> bool:
    """Stale when older than the NYSE session before the as_of ET date."""
    previous_session = last_trading_day(et_date(as_of) - timedelta(days=1))
    return obs_date < previous_session


def reason_from_ledger(last_run: Mapping[str, Any] | None) -> str:
    """Why a pillar has no qualifying row, from its latest finished run at as_of."""
    if last_run is None:
        return REASON_NEVER_RUN
    status = last_run.get("status")
    if status == "failed":
        return REASON_WRITER_FAILED
    if status == "partial_blocked_by_legacy":
        return REASON_BLOCKED_BY_LEGACY
    if status in _LEDGER_PASSTHROUGH_REASONS:
        return str(status)
    return REASON_NO_ROWS


def cftc_pillar_status(market_statuses: list[str]) -> str:
    """Pillar status from the per-market statuses of every tracked market."""
    n_available = sum(1 for s in market_statuses if s == STATUS_AVAILABLE)
    n_stale = sum(1 for s in market_statuses if s == STATUS_STALE)
    if n_available + n_stale == 0:
        return STATUS_UNAVAILABLE
    if n_available == len(market_statuses):
        return STATUS_AVAILABLE
    if n_stale == len(market_statuses):
        return STATUS_STALE
    return STATUS_PARTIAL


# ---------------------------------------------------------------------------
# Schema probe
# ---------------------------------------------------------------------------

_PROBE_TABLES_SQL = text(
    """
    SELECT to_regclass('godview_runs') IS NOT NULL AS runs,
           to_regclass('fed_net_liquidity_daily') IS NOT NULL AS fed,
           to_regclass('cftc_positioning_daily') IS NOT NULL AS cftc,
           to_regclass('dealer_gex_daily') IS NOT NULL AS gex
    """
)

# G2 columns each pillar read needs. Probing them keeps a pre-G2 database
# (production until A2) at "schema_not_migrated" instead of a SQL error.
_PROBE_COLUMNS_SQL = text(
    """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = ANY (current_schemas(false))
      AND table_name IN ('fed_net_liquidity_daily', 'cftc_positioning_daily', 'dealer_gex_daily')
      AND column_name IN ('provenance', 'available_at', 'release_at', 'availability_basis',
                          'delta_1w_m', 'cftc_market_code', 'gex_aggregate')
    """
)

_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    PILLAR_FED: frozenset({"provenance", "available_at", "release_at", "availability_basis", "delta_1w_m"}),
    PILLAR_CFTC: frozenset({"provenance", "available_at", "release_at", "availability_basis", "cftc_market_code"}),
    PILLAR_GEX: frozenset({"provenance", "available_at", "release_at", "availability_basis", "gex_aggregate"}),
}


@dataclass(frozen=True)
class SchemaState:
    runs_table: bool
    pillar_ready: dict[str, bool]

    @property
    def migrated(self) -> bool:
        return self.runs_table and all(self.pillar_ready.values())

    def missing(self) -> list[str]:
        out = [] if self.runs_table else [RUNS_TABLE]
        out += [PILLAR_TABLES[p] for p, ok in self.pillar_ready.items() if not ok]
        return out


def probe_schema(conn: Connection) -> SchemaState:
    tables = conn.execute(_PROBE_TABLES_SQL).mappings().one()
    columns: dict[str, set[str]] = {}
    for row in conn.execute(_PROBE_COLUMNS_SQL).mappings():
        columns.setdefault(row["table_name"], set()).add(row["column_name"])
    ready = {}
    for pillar, key in ((PILLAR_FED, "fed"), (PILLAR_CFTC, "cftc"), (PILLAR_GEX, "gex")):
        have = columns.get(PILLAR_TABLES[pillar], set())
        ready[pillar] = bool(tables[key]) and _REQUIRED_COLUMNS[pillar] <= have
    return SchemaState(runs_table=bool(tables["runs"]), pillar_ready=ready)


# ---------------------------------------------------------------------------
# SQL (static text, bound parameters only)
# ---------------------------------------------------------------------------

_LAST_RUNS_SQL = text(
    """
    SELECT DISTINCT ON (pillar)
           pillar, run_id, status, started_at, finished_at,
           rows_written, rows_skipped, reasons, code_sha
    FROM godview_runs
    WHERE pillar IN ('fed_liquidity', 'cftc', 'dealer_gex')
      AND status <> 'running'
      AND finished_at <= :as_of
    ORDER BY pillar, finished_at DESC, started_at DESC
    """
)

_FED_LATEST_SQL = text(
    """
    SELECT
    obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp, net_liquidity_usd_m,
    rrp_as_pct_of_peak, delta_1w_m, delta_4w_m, liquidity_regime, coverage_fraction,
    release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha
    FROM fed_net_liquidity_daily
    WHERE provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
    ORDER BY obs_date DESC
    LIMIT 1
    """
)

_FED_HISTORY_COUNT_SQL = text(
    """
    SELECT count(*) FROM fed_net_liquidity_daily
    WHERE provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
      AND obs_date BETWEEN :date_from AND :date_to
    """
)

_FED_HISTORY_SQL = text(
    """
    SELECT
    obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp, net_liquidity_usd_m,
    rrp_as_pct_of_peak, delta_1w_m, delta_4w_m, liquidity_regime, coverage_fraction,
    release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha
    FROM fed_net_liquidity_daily
    WHERE provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
      AND obs_date BETWEEN :date_from AND :date_to
    ORDER BY obs_date ASC
    LIMIT :limit OFFSET :offset
    """
)

_CFTC_LATEST_SQL = text(
    """
    SELECT DISTINCT ON (contract_code)
    contract_code, cftc_market_code, market_name, contract_name, asset_class, report_date,
    total_open_interest, commercial_long, commercial_short, commercial_net,
    noncommercial_long, noncommercial_short, noncommercial_net, spec_net_pct_oi,
    z_score_1y, z_score_3y, percentile_3y, crowding_regime, coverage_fraction,
    release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha
    FROM cftc_positioning_daily
    WHERE provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
    ORDER BY contract_code, report_date DESC
    """
)

_CFTC_HISTORY_COUNT_SQL = text(
    """
    SELECT count(*) FROM cftc_positioning_daily
    WHERE provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
      AND report_date BETWEEN :date_from AND :date_to
      AND (CAST(:market AS TEXT) IS NULL OR contract_code = :market)
    """
)

_CFTC_HISTORY_SQL = text(
    """
    SELECT
    contract_code, cftc_market_code, market_name, contract_name, asset_class, report_date,
    total_open_interest, commercial_long, commercial_short, commercial_net,
    noncommercial_long, noncommercial_short, noncommercial_net, spec_net_pct_oi,
    z_score_1y, z_score_3y, percentile_3y, crowding_regime, coverage_fraction,
    release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha
    FROM cftc_positioning_daily
    WHERE provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
      AND report_date BETWEEN :date_from AND :date_to
      AND (CAST(:market AS TEXT) IS NULL OR contract_code = :market)
    ORDER BY report_date ASC, contract_code ASC
    LIMIT :limit OFFSET :offset
    """
)

_GEX_LATEST_SQL = text(
    """
    SELECT
    obs_date, ticker, spot, gex_aggregate, gex_normalized, gamma_flip, gamma_flip_crossings,
    regime, gamma_wall, put_wall, call_wall, dealer_delta, model_basis, sign_convention,
    chain_capture_batch_id, chain_capture_ordinal, chain_capture_started_at,
    chain_capture_completed_at, spot_source, spot_basis, spot_obs_date, spot_available_at,
    spot_receipt_id, coverage_fraction,
    release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha
    FROM dealer_gex_daily
    WHERE ticker = :ticker AND provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
    ORDER BY obs_date DESC
    LIMIT 1
    """
)

_GEX_HISTORY_COUNT_SQL = text(
    """
    SELECT count(*) FROM dealer_gex_daily
    WHERE ticker = :ticker AND provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
      AND obs_date BETWEEN :date_from AND :date_to
    """
)

_GEX_HISTORY_SQL = text(
    """
    SELECT
    obs_date, ticker, spot, gex_aggregate, gex_normalized, gamma_flip, gamma_flip_crossings,
    regime, gamma_wall, put_wall, call_wall, dealer_delta, model_basis, sign_convention,
    chain_capture_batch_id, chain_capture_ordinal, chain_capture_started_at,
    chain_capture_completed_at, spot_source, spot_basis, spot_obs_date, spot_available_at,
    spot_receipt_id, coverage_fraction,
    release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha
    FROM dealer_gex_daily
    WHERE ticker = :ticker AND provenance IS NOT NULL
      AND available_at <= :as_of AND release_at <= :as_of
      AND obs_date BETWEEN :date_from AND :date_to
    ORDER BY obs_date ASC
    LIMIT :limit OFFSET :offset
    """
)


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------


def _aware_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _ts(value: Any) -> str | None:
    return _aware_utc(value).isoformat() if isinstance(value, datetime) else None


def _day(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat() if isinstance(value, date) else None


def _num(value: Any) -> float | None:
    return measured_or_none(value)


def _int(value: Any) -> int | None:
    f = measured_or_none(value)
    return None if f is None else int(f)


def _text(value: Any) -> str | None:
    return None if value is None else str(value)


def _json(value: Any) -> Any:
    if value is None or isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _receipt(row: Mapping[str, Any]) -> dict[str, Any]:
    """The provenance fields every served row carries."""
    return {
        "release_at": _ts(row["release_at"]),
        "available_at": _ts(row["available_at"]),
        "availability_basis": _text(row["availability_basis"]),
        "provenance": _text(row["provenance"]),
        "run_id": _text(row["run_id"]),
        "code_sha": _text(row["code_sha"]),
    }


def _run_dict(run: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "run_id": _text(run["run_id"]),
        "status": _text(run["status"]),
        "started_at": _ts(run["started_at"]),
        "finished_at": _ts(run["finished_at"]),
        "rows_written": _int(run["rows_written"]),
        "rows_skipped": _int(run["rows_skipped"]),
        "reasons": _json(run["reasons"]),
        "code_sha": _text(run["code_sha"]),
    }


def fed_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "obs_date": _day(row["obs_date"]),
        "net_liquidity_usd_m": _num(row["net_liquidity_usd_m"]),
        "walcl_usd_m": _num(row["fed_assets_walcl"]),
        "tga_usd_m": _num(row["treasury_tga_wtregen"]),
        "rrp_usd_m": _num(row["reverse_repo_rrp"]),
        "delta_1w_m": _num(row["delta_1w_m"]),
        "delta_4w_m": _num(row["delta_4w_m"]),
        "rrp_as_pct_of_peak": _num(row["rrp_as_pct_of_peak"]),
        "liquidity_regime": _text(row["liquidity_regime"]),
        "coverage_fraction": _num(row["coverage_fraction"]),
        "unit": FED_UNIT,
        "inputs": (_json(row["source_ref"]) or {}).get("inputs"),
        **_receipt(row),
    }


def cftc_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "market": _text(row["contract_code"]),
        "cftc_market_code": _text(row["cftc_market_code"]),
        "market_name": _text(row["market_name"]) or _text(row["contract_name"]),
        "asset_class": _text(row["asset_class"]),
        "report_date": _day(row["report_date"]),
        "total_open_interest": _int(row["total_open_interest"]),
        "commercial_long": _int(row["commercial_long"]),
        "commercial_short": _int(row["commercial_short"]),
        "commercial_net": _int(row["commercial_net"]),
        "noncommercial_long": _int(row["noncommercial_long"]),
        "noncommercial_short": _int(row["noncommercial_short"]),
        "noncommercial_net": _int(row["noncommercial_net"]),
        "spec_net_pct_oi": _num(row["spec_net_pct_oi"]),
        "z_score_1y": _num(row["z_score_1y"]),
        "z_score_3y": _num(row["z_score_3y"]),
        "percentile_3y": _num(row["percentile_3y"]),
        "crowding_regime": _text(row["crowding_regime"]),
        "coverage_fraction": _num(row["coverage_fraction"]),
        **_receipt(row),
    }


def gex_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "obs_date": _day(row["obs_date"]),
        "ticker": _text(row["ticker"]),
        "spot": _num(row["spot"]),
        "gex_aggregate": _num(row["gex_aggregate"]),
        "gex_normalized": _num(row["gex_normalized"]),
        "gamma_flip": _num(row["gamma_flip"]),
        "gamma_flip_crossings": _int(row["gamma_flip_crossings"]),
        "regime": _text(row["regime"]),
        "gamma_wall": _num(row["gamma_wall"]),
        "put_wall": _num(row["put_wall"]),
        "call_wall": _num(row["call_wall"]),
        "dealer_delta": _num(row["dealer_delta"]),
        "estimated": True,
        "basis": _text(row["model_basis"]),
        "sign_convention": _text(row["sign_convention"]),
        "model_note": GEX_MODEL_NOTE,
        "chain_capture_batch_id": _text(row["chain_capture_batch_id"]),
        "chain_capture_ordinal": _int(row["chain_capture_ordinal"]),
        "chain_capture_started_at": _ts(row["chain_capture_started_at"]),
        "chain_capture_completed_at": _ts(row["chain_capture_completed_at"]),
        "spot_source": _text(row["spot_source"]),
        "spot_basis": _text(row["spot_basis"]),
        "spot_obs_date": _day(row["spot_obs_date"]),
        "spot_available_at": _ts(row["spot_available_at"]),
        "spot_receipt_id": _int(row["spot_receipt_id"]),
        "coverage_fraction": _num(row["coverage_fraction"]),
        **_receipt(row),
    }


# ---------------------------------------------------------------------------
# Pillar payloads
# ---------------------------------------------------------------------------


def _unavailable_pillar(pillar: str, reason: str, last_run: Mapping[str, Any] | None, **extra: Any) -> dict[str, Any]:
    return {
        "pillar": pillar,
        "status": STATUS_UNAVAILABLE,
        "available": False,
        "reason": reason,
        "as_of": None,
        "release_at": None,
        "available_at": None,
        "availability_basis": None,
        "provenance": None,
        "estimated": pillar == PILLAR_GEX,
        "last_run": _run_dict(last_run),
        "data": None,
        **extra,
    }


def build_fed_pillar(row: Mapping[str, Any] | None, last_run: Mapping[str, Any] | None, as_of: datetime) -> dict[str, Any]:
    if row is None:
        return _unavailable_pillar(PILLAR_FED, reason_from_ledger(last_run), last_run, stale_after_days=FED_STALE_AFTER_DAYS)
    data = fed_row(row)
    stale = fed_is_stale(row["obs_date"], as_of)
    return {
        "pillar": PILLAR_FED,
        "status": STATUS_STALE if stale else STATUS_AVAILABLE,
        "available": True,
        "reason": REASON_STALE if stale else None,
        "as_of": data["obs_date"],
        "release_at": data["release_at"],
        "available_at": data["available_at"],
        "availability_basis": data["availability_basis"],
        "provenance": data["provenance"],
        "estimated": False,
        "stale_after_days": FED_STALE_AFTER_DAYS,
        "last_run": _run_dict(last_run),
        "data": data,
    }


def build_cftc_pillar(rows: list[Mapping[str, Any]], last_run: Mapping[str, Any] | None, as_of: datetime) -> dict[str, Any]:
    by_root = {r["contract_code"]: r for r in rows}
    markets: list[dict[str, Any]] = []
    for root, code, label in CFTC_TRACKED:
        row = by_root.get(root)
        if row is None:
            markets.append({
                "market": root,
                "cftc_market_code": code,
                "market_name": label,
                "status": STATUS_UNAVAILABLE,
                "available": False,
                "reason": REASON_NO_MARKET_ROW,
                "report_date": None,
            })
            continue
        entry = cftc_row(row)
        stale = cftc_is_stale(row["release_at"], as_of)
        entry.update({
            "status": STATUS_STALE if stale else STATUS_AVAILABLE,
            "available": True,
            "reason": REASON_STALE if stale else None,
        })
        markets.append(entry)

    statuses = [m["status"] for m in markets]
    status = cftc_pillar_status(statuses)
    coverage = {
        "tracked": len(markets),
        "available": statuses.count(STATUS_AVAILABLE),
        "stale": statuses.count(STATUS_STALE),
        "unavailable": statuses.count(STATUS_UNAVAILABLE),
    }
    if status == STATUS_UNAVAILABLE:
        return _unavailable_pillar(
            PILLAR_CFTC, reason_from_ledger(last_run), last_run,
            stale_after_days=CFTC_STALE_AFTER_DAYS, coverage=coverage,
        )
    served = [m for m in markets if m["available"]]
    latest = max(served, key=lambda m: m["report_date"])
    bases = sorted({m["availability_basis"] for m in served if m["availability_basis"]})
    provenances = sorted({m["provenance"] for m in served if m["provenance"]})
    return {
        "pillar": PILLAR_CFTC,
        "status": status,
        "available": True,
        "reason": {STATUS_STALE: REASON_STALE, STATUS_PARTIAL: "partial_coverage"}.get(status),
        "as_of": latest["report_date"],
        "release_at": latest["release_at"],
        "available_at": max(m["available_at"] for m in served),
        "availability_basis": bases[0] if len(bases) == 1 else ("mixed" if bases else None),
        "provenance": provenances[0] if len(provenances) == 1 else ("mixed" if provenances else None),
        "estimated": False,
        "stale_after_days": CFTC_STALE_AFTER_DAYS,
        "coverage": coverage,
        "last_run": _run_dict(last_run),
        "data": {"markets": markets},
    }


def build_gex_pillar(row: Mapping[str, Any] | None, last_run: Mapping[str, Any] | None, as_of: datetime) -> dict[str, Any]:
    if row is None:
        return _unavailable_pillar(
            PILLAR_GEX, reason_from_ledger(last_run), last_run,
            ticker=GEX_TICKER, model_note=GEX_MODEL_NOTE,
        )
    data = gex_row(row)
    stale = gex_is_stale(row["obs_date"], as_of)
    return {
        "pillar": PILLAR_GEX,
        "status": STATUS_STALE if stale else STATUS_AVAILABLE,
        "available": True,
        "reason": REASON_STALE if stale else None,
        "as_of": data["obs_date"],
        "release_at": data["release_at"],
        "available_at": data["available_at"],
        "availability_basis": data["availability_basis"],
        "provenance": data["provenance"],
        "estimated": True,
        "basis": data["basis"],
        "ticker": GEX_TICKER,
        "model_note": GEX_MODEL_NOTE,
        "last_run": _run_dict(last_run),
        "data": data,
    }


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def _last_runs(conn: Connection, schema: SchemaState, as_of: datetime) -> dict[str, Mapping[str, Any]]:
    if not schema.runs_table:
        return {}
    return {r["pillar"]: r for r in conn.execute(_LAST_RUNS_SQL, {"as_of": as_of}).mappings()}


def read_latest(conn: Connection, *, as_of: datetime, as_of_source: str = "server_now") -> dict[str, Any]:
    """The god-view summary at ``as_of``: one payload per pillar, SELECT-only."""
    as_of = _aware_utc(as_of)
    schema = probe_schema(conn)
    runs = _last_runs(conn, schema, as_of)
    params = {"as_of": as_of}

    pillars: dict[str, Any] = {}
    if schema.runs_table and schema.pillar_ready[PILLAR_FED]:
        row = conn.execute(_FED_LATEST_SQL, params).mappings().first()
        pillars[PILLAR_FED] = build_fed_pillar(row, runs.get(PILLAR_FED), as_of)
    else:
        pillars[PILLAR_FED] = _unavailable_pillar(PILLAR_FED, REASON_SCHEMA_NOT_MIGRATED, None)

    if schema.runs_table and schema.pillar_ready[PILLAR_CFTC]:
        rows = list(conn.execute(_CFTC_LATEST_SQL, params).mappings())
        pillars[PILLAR_CFTC] = build_cftc_pillar(rows, runs.get(PILLAR_CFTC), as_of)
    else:
        pillars[PILLAR_CFTC] = _unavailable_pillar(PILLAR_CFTC, REASON_SCHEMA_NOT_MIGRATED, None)

    if schema.runs_table and schema.pillar_ready[PILLAR_GEX]:
        row = conn.execute(_GEX_LATEST_SQL, {**params, "ticker": GEX_TICKER}).mappings().first()
        pillars[PILLAR_GEX] = build_gex_pillar(row, runs.get(PILLAR_GEX), as_of)
    else:
        pillars[PILLAR_GEX] = _unavailable_pillar(
            PILLAR_GEX, REASON_SCHEMA_NOT_MIGRATED, None, ticker=GEX_TICKER, model_note=GEX_MODEL_NOTE,
        )

    return {
        "as_of": as_of.isoformat(),
        "as_of_source": as_of_source,
        "schema": {"migrated": schema.migrated, "missing": schema.missing()},
        "pillars": pillars,
    }


_HISTORY_SQL = {
    PILLAR_FED: (_FED_HISTORY_COUNT_SQL, _FED_HISTORY_SQL, fed_row),
    PILLAR_CFTC: (_CFTC_HISTORY_COUNT_SQL, _CFTC_HISTORY_SQL, cftc_row),
    PILLAR_GEX: (_GEX_HISTORY_COUNT_SQL, _GEX_HISTORY_SQL, gex_row),
}


def read_history(
    conn: Connection,
    *,
    pillar: str,
    date_from: date,
    date_to: date,
    as_of: datetime,
    market: str | None = None,
    limit: int = HISTORY_DEFAULT_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    """Provenance rows of one pillar with an observation date in [from, to], PIT at ``as_of``."""
    if pillar not in _HISTORY_SQL:
        raise ValueError(f"unknown pillar {pillar!r}")
    as_of = _aware_utc(as_of)
    envelope: dict[str, Any] = {
        "pillar": pillar,
        "from": date_from.isoformat(),
        "to": date_to.isoformat(),
        "as_of": as_of.isoformat(),
        "market": market,
        "ticker": GEX_TICKER if pillar == PILLAR_GEX else None,
        "limit": limit,
        "offset": offset,
    }
    schema = probe_schema(conn)
    if not (schema.runs_table and schema.pillar_ready[pillar]):
        return {**envelope, "status": STATUS_UNAVAILABLE, "reason": REASON_SCHEMA_NOT_MIGRATED,
                "total": 0, "has_more": False, "entries": [], "last_run": None}

    count_sql, page_sql, serialise = _HISTORY_SQL[pillar]
    params: dict[str, Any] = {"as_of": as_of, "date_from": date_from, "date_to": date_to}
    if pillar == PILLAR_CFTC:
        params["market"] = market
    if pillar == PILLAR_GEX:
        params["ticker"] = GEX_TICKER
    total = int(conn.execute(count_sql, params).scalar() or 0)
    rows = list(conn.execute(page_sql, {**params, "limit": limit, "offset": offset}).mappings())
    last_run = _last_runs(conn, schema, as_of).get(pillar)
    entries = [serialise(r) for r in rows]
    return {
        **envelope,
        "status": STATUS_AVAILABLE if total else STATUS_UNAVAILABLE,
        "reason": None if total else reason_from_ledger(last_run),
        "total": total,
        "has_more": offset + len(entries) < total,
        "entries": entries,
        "last_run": _run_dict(last_run),
    }
