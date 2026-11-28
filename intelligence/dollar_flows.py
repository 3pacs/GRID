"""
GRID Intelligence — Dollar Flow Normalizer.

Converts all signal sources into estimated USD amounts for apples-to-apples
comparison across congressional trades, insider filings, dark pool activity,
13F position changes, ETF flows, whale options, and prediction markets.

Each signal type has a different native unit; this module normalizes them
all into a single `amount_usd` figure with confidence labels and provenance.

Conversion rules:
  - Congressional: midpoint of reported range (e.g., "$1M-$5M" -> $3M)
  - Insider (Form 4): shares x price at transaction date
  - Dark pool: volume x a real VWAP proxy (latest accepted yfinance close
    at/before the signal date, via store.observations.read_latest — never a
    fabricated default; a row with no real price is dropped and counted,
    see GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3 item 1)
  - 13F: quarterly holdings delta (value_usd from raw_series)
  - ETF flows: daily dollar volume from ETF_FLOW series
  - Whale options: notional premium from signal_value
  - Prediction markets: probability-implied stake if available

Key entry points:
  normalize_all_flows    — scan signal_sources + raw_series, normalize to USD
  get_flows_by_ticker    — filter flows for a single ticker
  get_flows_by_sector    — aggregate flows by GICS sector (via sector_map)
  get_aggregate_flows    — net flow per sector, per actor tier
  get_biggest_movers     — top 10 by absolute dollar amount
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import inspect as sa_inspect, text
from sqlalchemy.engine import Engine

from ingestion.market_calendar import trading_days_between
from store.observations import read_latest


# ── Constants ─────────────────────────────────────────────────────────────

# Amount range mapping (mirrors congressional.py)
_AMOUNT_RANGES: dict[str, tuple[int, int]] = {
    "A": (1_001, 15_000),
    "B": (15_001, 50_000),
    "C": (50_001, 100_000),
    "D": (100_001, 250_000),
    "E": (250_001, 500_000),
    "F": (500_001, 1_000_000),
    "G": (1_000_001, 5_000_000),
    "H": (5_000_001, 25_000_000),
    "I": (25_000_001, 50_000_000),
    "J": (50_000_001, 999_999_999),
}

# A VWAP observation older than this (in trading days, via
# ingestion/market_calendar.py's real NYSE calendar) relative to the
# signal's own date is too stale to price a dark-pool row with — the row is
# dropped and counted in skipped_stale_vwap rather than priced off a stale
# close. Pinned per code review on PR #712.
_VWAP_MAX_AGE_TRADING_DAYS: int = 5

# Valid 4-part 13F position-change actions (13F:{cik}:{cusip}:{action}).
# 3-part portfolio summary keys from ingestion/edgar.py (TOTAL_VALUE,
# NUM_POSITIONS, TOP5_CONCENTRATION) and unrecognized actions are never flows.
_VALID_13F_ACTIONS: frozenset[str] = frozenset({"NEW", "INCREASED", "DECREASED", "CLOSED"})
# Absent from a current filing whose cover page declares confidential omissions
# (the writer's NOT_SHOWN_ACTION). It is not a flow and never becomes one, but
# it is the newest statement about its position: it takes part in the
# per-position resolution so that it replaces an older CLOSED of the same
# filing pair, and is then dropped.
_NOT_SHOWN_13F_ACTION = "NOT_SHOWN"

# SEC Form 13F XML <value> cutover: whole dollars >= 2023-01-03, thousands earlier.
_13F_DOLLAR_CUTOVER_DATE: date = date(2023, 1, 3)

# Documented share-based flow estimation methods accepted for 13F directional flows.
# Owner decision 2026-11-23 (Anik, "Yes, non-directional"): no 13F row is a
# verified direction until CUSIP identity and corporate-action coverage exist.
# A NEW/CLOSED position is stored as an observed filing appearance or
# disappearance at reference prices (direction "observed"); every served
# directional reader excludes source_type '13f'.
OBSERVED_13F_DIRECTION = "observed"
OBSERVED_13F_MEASURE = "observed_filing_holdings_change_at_reference_prices"
_13F_OBSERVATION_REASONS: frozenset[str] = frozenset({"no_identity_coverage"})

# ── Stored-row contract ───────────────────────────────────────────────────
# The daily writer rebuilds only the dates inside its ``--days`` window
# (``_persist_flows`` deletes ``flow_date`` between the earliest and latest
# date the run produced). Every older date keeps the shape of whatever rule
# wrote it. A rule change therefore never reaches old rows: on 2026-11-27
# production still held 49 13F portfolio summaries stored as ``inflow`` by
# the rule before the 11-21/22 rework, outside every later window.
#
# A contract names, per source type, the one shape the current rule can
# write. It is enforced three times from this single definition:
#   - ``_persist_flows`` checks every flow before its window DELETE; one that
#     breaks it makes the run write nothing (the last good window is kept)
#     and ``scripts/run_dollar_flows.py`` exits 4 (unexpected: a regression);
#   - ``count_stored_contract_violations`` reads the WHOLE table after every
#     run, so an old row in a superseded shape is reported and the runner
#     exits 3 after a good window committed (expected until the repair);
#   - the CHECK constraint ``DOLLAR_FLOWS_CONTRACT_CONSTRAINT`` (alembic
#     revision ``df_13f_observed_20261127``) rejects it for every
#     other writer.
# Removing or rewriting rows that break it is a reviewed repair, never
# automatic: each contract has its own CHECK constraint and its own remedy,
# so one contract's repair never touches another's rows.
#   - 13f: delete (scripts/repair_dollar_flows_stored_contract.py). Its CHECK
#     is ``DOLLAR_FLOWS_CONTRACT_CHECK_SQL`` / ``DOLLAR_FLOWS_CONTRACT_CONSTRAINT``.
#   - etf_flow: re-derive in place as the current rule writes it
#     (scripts/rederive_dollar_flows_etf_legacy.py; owner decision
#     2026-11-28 "Yes, re-derive later"). Its CHECK is
#     ``ETF_FLOW_CONTRACT_CHECK_SQL`` / ``ETF_FLOW_CONTRACT_CONSTRAINT``
#     (alembic revision ``df_etf_flow_basis_20261128``).
DOLLAR_FLOWS_CONTRACT_CONSTRAINT = "dollar_flows_13f_observed_only"
DOLLAR_FLOWS_CONTRACT_CHECK_SQL = "(source_type <> '13f' OR direction = 'observed')"

# ETF dollar volume is unsigned: the current rule (``_normalize_etf_flows``)
# records why a row has its direction in ``evidence.direction_basis`` and
# writes 'neutral' unless a signed measure exists. Before 2026-11-16 an older
# rule stored every ETF row as 'inflow' with no basis (693 rows on production,
# 2026-02-27..11-13). NULL-safe: a missing, JSON-null or empty basis, a NULL
# or non-object evidence, all break it (``->>`` yields NULL, COALESCE '').
ETF_FLOW_CONTRACT_CONSTRAINT = "dollar_flows_etf_flow_direction_basis"
UNSIGNED_VOLUME_BASIS = "unsigned_volume"
ETF_FLOW_CONTRACT_CHECK_SQL = (
    "(source_type <> 'etf_flow' OR (COALESCE(evidence ->> 'direction_basis', '') <> '' "
    "AND (direction = 'neutral' OR evidence ->> 'direction_basis' <> 'unsigned_volume')))"
)
# The same predicate for SQLite (evidence is JSON text there).
_ETF_FLOW_CONTRACT_CHECK_SQLITE = (
    "(source_type <> 'etf_flow' OR (COALESCE(json_extract(evidence, '$.direction_basis'), '') <> '' "
    "AND (direction = 'neutral' OR json_extract(evidence, '$.direction_basis') <> 'unsigned_volume')))"
)


def _evidence_dict(flow: dict[str, Any]) -> dict[str, Any]:
    ev = flow.get("evidence")
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except (json.JSONDecodeError, TypeError):
            return {}
    return ev if isinstance(ev, dict) else {}


def _conforms_13f(flow: dict[str, Any]) -> bool:
    return flow.get("direction") == OBSERVED_13F_DIRECTION


def _conforms_etf_flow(flow: dict[str, Any]) -> bool:
    basis = _evidence_dict(flow).get("direction_basis")
    if not isinstance(basis, str) or basis == "":
        return False
    return flow.get("direction") == "neutral" or basis != UNSIGNED_VOLUME_BASIS


STORED_ROW_CONTRACTS: dict[str, dict[str, Any]] = {
    "13f": {
        "column": "direction",
        "value": OBSERVED_13F_DIRECTION,
        "rule": "13F is non-directional (owner decision 2026-11-23): every stored 13f row has direction 'observed'",
        "constraint": DOLLAR_FLOWS_CONTRACT_CONSTRAINT,
        "check_sql": DOLLAR_FLOWS_CONTRACT_CHECK_SQL,
        "check_sql_sqlite": DOLLAR_FLOWS_CONTRACT_CHECK_SQL,
        "conforms": _conforms_13f,
        "remedy": "delete",
    },
    "etf_flow": {
        "rule": "ETF dollar volume is unsigned: every stored etf_flow row names its evidence.direction_basis, "
                "and is 'neutral' when that basis is 'unsigned_volume'",
        "constraint": ETF_FLOW_CONTRACT_CONSTRAINT,
        "check_sql": ETF_FLOW_CONTRACT_CHECK_SQL,
        "check_sql_sqlite": _ETF_FLOW_CONTRACT_CHECK_SQLITE,
        "conforms": _conforms_etf_flow,
        "remedy": "rederive",
    },
}


def flow_contract_violation(flow: dict[str, Any]) -> str | None:
    """Return the broken contract's source type, or ``None`` if the flow conforms."""
    stype = flow.get("source_type")
    contract = STORED_ROW_CONTRACTS.get(stype) if isinstance(stype, str) else None
    if contract is None:
        return None
    return None if contract["conforms"](flow) else stype


def contract_check_sql(stype: str, dialect: str = "postgresql") -> str:
    """The CHECK expression of ``stype``'s contract for ``dialect`` (no bound parameters)."""
    contract = STORED_ROW_CONTRACTS[stype]
    return contract["check_sql_sqlite"] if dialect == "sqlite" else contract["check_sql"]


def count_stored_contract_violations(conn: Any) -> dict[str, int]:
    """Count stored rows, at any ``flow_date``, that break their source's contract."""
    dialect = conn.dialect.name
    found: dict[str, int] = {}
    for stype in sorted(STORED_ROW_CONTRACTS):
        n = conn.execute(
            text(
                f"SELECT COUNT(*) FROM dollar_flows WHERE source_type = :st "
                f"AND NOT {contract_check_sql(stype, dialect)}"
            ),
            {"st": stype},
        ).scalar()
        if n:
            found[stype] = int(n)
    return found


_QUALIFIED_13F_FLOW_METHODS: frozenset[str] = frozenset({
    "delta_shares_x_curr_quarter_end_price",
    "delta_shares_x_prev_quarter_end_price",
})

# Strict equity/ETF ticker pattern (rejects issuer names like "APPLE INC").
_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")

_UNSET = object()
_CUSIP_TICKER_MAP: Any = None

# Source types we normalize
_SOURCE_TYPES = [
    "congressional",
    "insider",
    "darkpool",
    "options_flow",
    "prediction_market",
]


# ``dollar_flows`` is a current view: each normalization deletes the rows of
# the dates it covers and writes them again, so ``created_at`` is the time of
# the last rebuild, not the time a flow was first known, and a row dropped or
# corrected since leaves no trace. Nothing in the table can say what it held
# on a past date. A reader asked for a past date gives no answer and this
# reason, rather than today's rows under an old date.
CURRENT_VIEW_WITHHELD_REASON = "dollar_flows_is_a_current_view_without_a_receipt_clock"


def _utc_today() -> date:
    """Today's UTC date. One name so a test can set it."""
    return datetime.now(timezone.utc).date()


def is_past_as_of(as_of: date | None) -> bool:
    """True when ``as_of`` names a UTC date that has ended."""
    return as_of is not None and as_of < _utc_today()


# ── Table DDL ─────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dollar_flows (
    id SERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,
    actor_name TEXT,
    ticker TEXT,
    amount_usd NUMERIC NOT NULL,
    direction TEXT NOT NULL,
    confidence TEXT DEFAULT 'estimated',
    evidence JSONB,
    flow_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT dollar_flows_13f_observed_only CHECK (source_type <> '13f' OR direction = 'observed'),
    CONSTRAINT dollar_flows_etf_flow_direction_basis CHECK (source_type <> 'etf_flow' OR (COALESCE(evidence ->> 'direction_basis', '') <> '' AND (direction = 'neutral' OR evidence ->> 'direction_basis' <> 'unsigned_volume')))
);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_ticker
    ON dollar_flows (ticker, flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_source
    ON dollar_flows (source_type, flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_date
    ON dollar_flows (flow_date DESC);
CREATE INDEX IF NOT EXISTS idx_dollar_flows_amount
    ON dollar_flows (amount_usd DESC);
"""


# ── Helpers ───────────────────────────────────────────────────────────────

def _as_date(value: Any) -> date | None:
    """Coerce a raw signal_date/obs_date value to a real ``date``.

    Postgres drivers (psycopg2) already return native ``date``/``datetime``
    objects for DATE/TIMESTAMPTZ columns, but a bare ``conn.execute(text(...))``
    read against SQLite (used only in tests) returns the adapted ISO string
    instead. Coercing here — mirrors ``store/observations.py``'s
    ``_coerce_date`` — keeps every ``signal_date > today`` /
    ``flow_date > today`` comparison correct on both backends instead of
    silently raising or comparing wrong types.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _coerce_ts(value: Any) -> datetime | None:
    """Coerce a raw pull_timestamp value to ``datetime`` (Postgres or SQLite)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _ts_utc(ts: datetime | None) -> datetime:
    """Normalize a timestamp to UTC (naive taken as UTC, matching griddb)."""
    if ts is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _utc_date(ts: datetime | None) -> date | None:
    if ts is None:
        return None
    return _ts_utc(ts).date()


def _dedup_raw_series_vintages(
    rows: list[Any],
    *,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
    filter_pull_by_as_of: bool = False,
) -> list[tuple[str, date, Any, Any, datetime | None]]:
    """Collapse ``(series_id, obs_date, value, raw_payload, pull_timestamp)``
    rows to the latest PIT-eligible vintage per ``(series_id, obs_date)``.

    Mirrors ``store/observations.py`` ``LATEST_AS_OF`` semantics in Python so
    behavior is identical on PostgreSQL and SQLite.
    """
    as_of_ts_utc = _ts_utc(as_of_ts) if as_of_ts is not None else None
    best: dict[tuple[str, date], tuple[datetime, tuple[str, date, Any, Any, datetime | None]]] = {}

    for row in rows:
        series_id = row[0]
        obs_date = _as_date(row[1])
        if obs_date is None:
            continue
        pull_ts = _coerce_ts(row[4]) if len(row) > 4 else None
        pull_utc = _ts_utc(pull_ts)

        if as_of_ts_utc is not None and pull_ts is not None and pull_utc > as_of_ts_utc:
            continue
        if as_of_ts_utc is None and filter_pull_by_as_of and as_of is not None:
            if pull_ts is not None and pull_utc.date() > as_of:
                continue

        key = (series_id, obs_date)
        prev = best.get(key)
        if prev is None or pull_utc > prev[0]:
            best[key] = (pull_utc, (series_id, obs_date, row[2], row[3], pull_ts))

    ordered = sorted(best.values(), key=lambda item: (item[1][1], item[1][0]), reverse=True)
    return [item[1] for item in ordered]


def _signed_direction_from_payload(
    payload: dict[str, Any],
    *,
    buy_key: str | None = None,
    sell_key: str | None = None,
) -> tuple[str, str]:
    """Derive directional label only when an explicit signed measure exists.

    Unsigned volume fails closed to ``("neutral", "unsigned_volume")`` so it
    never fabricates an inflow or outflow.
    """
    for key in ("net_flow", "signed_flow", "signed_volume", "buy_sell_imbalance"):
        raw = payload.get(key)
        if raw is not None and not isinstance(raw, bool):
            try:
                val = float(raw)
            except (TypeError, ValueError):
                val = float("nan")
            if math.isfinite(val):
                if val > 0:
                    return "inflow", key
                if val < 0:
                    return "outflow", key
                return "neutral", key

    if buy_key and sell_key and payload.get(buy_key) is not None and payload.get(sell_key) is not None:
        raw_b, raw_s = payload.get(buy_key), payload.get(sell_key)
        if not isinstance(raw_b, bool) and not isinstance(raw_s, bool):
            try:
                buy_v, sell_v = float(raw_b), float(raw_s)
            except (TypeError, ValueError):
                buy_v = sell_v = float("nan")
            if math.isfinite(buy_v) and math.isfinite(sell_v):
                basis = f"{buy_key}_vs_{sell_key}"
                if buy_v > sell_v:
                    return "inflow", basis
                if sell_v > buy_v:
                    return "outflow", basis
                return "neutral", basis

    raw_dir = payload.get("direction")
    if isinstance(raw_dir, str) and raw_dir.strip():
        d_low = raw_dir.strip().lower()
        if d_low in ("inflow", "buy", "bullish", "accumulation"):
            return "inflow", "explicit_direction"
        if d_low in ("outflow", "sell", "bearish", "distribution"):
            return "outflow", "explicit_direction"
        if d_low == "neutral":
            return "neutral", "explicit_direction"

    return "neutral", "unsigned_volume"


def _normalize_ticker_candidate(raw: Any) -> str | None:
    """Return a clean uppercase ticker symbol, or ``None`` for non-tickers."""
    if not isinstance(raw, str):
        return None
    candidate = raw.strip().upper()
    if not candidate or not _TICKER_RE.fullmatch(candidate):
        return None
    return candidate


def _get_cusip_ticker_map() -> Any:
    """Lazily load ``CusipTickerMap`` (FTD CSV corpus) for CUSIP -> ticker lookup."""
    global _CUSIP_TICKER_MAP
    if _CUSIP_TICKER_MAP is None:
        try:
            from ingestion.altdata.sec_13f_live import CusipTickerMap
            _CUSIP_TICKER_MAP = CusipTickerMap()
        except Exception as exc:
            log.debug("CusipTickerMap unavailable: {e}", e=str(exc))
            _CUSIP_TICKER_MAP = False
    return _CUSIP_TICKER_MAP if _CUSIP_TICKER_MAP else None


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        return bool(sa_inspect(conn).has_table(table_name))
    except Exception:
        return False


def _resolve_13f_ticker(
    conn: Any,
    cusip: str,
    payload: dict[str, Any],
    obs_date: date,
    *,
    has_sec_ids: bool,
    has_inst_holdings: bool,
    cusip_map: Any,
    cache: dict[tuple[str, date], str | None],
) -> str | None:
    """Resolve a 13F holding to a real ticker symbol (never ``issuer_name``).

    Resolution order (PIT-safe):
      1. Explicit ``payload["ticker"]`` if valid.
      2. ``security_identifiers`` (cusip -> entity_id -> ticker valid on ``obs_date``).
      3. ``institutional_holdings`` (cusip -> ticker filed on/before ``obs_date``).
      4. ``CusipTickerMap`` fallback (if provided or lazily loaded).
    Returns ``None`` when no real ticker mapping exists.
    """
    explicit = _normalize_ticker_candidate(payload.get("ticker"))
    if explicit is not None:
        return explicit

    cusip_norm = cusip.strip().upper()
    if not cusip_norm:
        return None

    cache_key = (cusip_norm, obs_date)
    if cache_key in cache:
        return cache[cache_key]

    resolved: str | None = None

    if has_sec_ids:
        try:
            hit = conn.execute(
                text(
                    "SELECT t.id_value "
                    "FROM security_identifiers c "
                    "JOIN security_identifiers t ON t.entity_id = c.entity_id "
                    "WHERE c.id_scheme = :c_scheme AND UPPER(c.id_value) = :cusip "
                    "AND c.valid_from <= :as_of "
                    "AND (c.valid_to IS NULL OR c.valid_to >= :as_of) "
                    "AND t.id_scheme = :t_scheme "
                    "AND t.valid_from <= :as_of "
                    "AND (t.valid_to IS NULL OR t.valid_to >= :as_of) "
                    "ORDER BY t.is_primary DESC, t.valid_from DESC, t.entity_id ASC "
                    "LIMIT 1"
                ),
                {
                    "c_scheme": "cusip",
                    "t_scheme": "ticker",
                    "cusip": cusip_norm,
                    "as_of": obs_date,
                },
            ).fetchone()
            if hit and hit[0]:
                resolved = _normalize_ticker_candidate(hit[0])
        except Exception as exc:
            log.debug("security_identifiers CUSIP lookup failed for {c}: {e}", c=cusip_norm, e=str(exc))

    if resolved is None and has_inst_holdings:
        try:
            hit = conn.execute(
                text(
                    "SELECT ticker FROM institutional_holdings "
                    "WHERE UPPER(cusip) = :cusip "
                    "AND COALESCE(filed_date, report_date) <= :as_of "
                    "ORDER BY COALESCE(filed_date, report_date) DESC LIMIT 1"
                ),
                {"cusip": cusip_norm, "as_of": obs_date},
            ).fetchone()
            if hit and hit[0]:
                resolved = _normalize_ticker_candidate(hit[0])
        except Exception as exc:
            log.debug("institutional_holdings CUSIP lookup failed for {c}: {e}", c=cusip_norm, e=str(exc))

    if resolved is None:
        cmap = _get_cusip_ticker_map() if cusip_map is _UNSET else cusip_map
        if cmap is not None:
            try:
                resolved = _normalize_ticker_candidate(cmap.lookup(cusip_norm))
            except Exception as exc:
                log.debug("CusipTickerMap lookup failed for {c}: {e}", c=cusip_norm, e=str(exc))

    cache[cache_key] = resolved
    return resolved


def _ensure_table(engine: Engine) -> None:
    """Create dollar_flows table if it does not exist."""
    with engine.begin() as conn:
        if _table_exists(conn, "dollar_flows"):
            return
        for stmt in CREATE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


def _midpoint_amount(amount_range: str) -> float:
    """Get the midpoint dollar value from an amount range code or string.

    Parameters:
        amount_range: Either a code ('A'-'J') or a string like '$1,001 - $15,000'.

    Returns:
        Midpoint dollar value as float.
    """
    if not amount_range:
        return 0.0

    # Try coded range first
    if amount_range.upper() in _AMOUNT_RANGES:
        lo, hi = _AMOUNT_RANGES[amount_range.upper()]
        return (lo + hi) / 2.0

    # Try parsing dollar range string
    nums = re.findall(r"[\d,]+", amount_range.replace(",", ""))
    if len(nums) >= 2:
        try:
            lo = float(nums[0].replace(",", ""))
            hi = float(nums[1].replace(",", ""))
            return (lo + hi) / 2.0
        except ValueError:
            pass

    return 0.0


def _direction_from_signal(signal_type: str) -> str:
    """Map signal_type to 'inflow' or 'outflow'.

    Parameters:
        signal_type: The signal_type field from signal_sources.

    Returns:
        'inflow' or 'outflow'.
    """
    upper = signal_type.upper()
    if any(kw in upper for kw in ("BUY", "PURCHASE", "NEW", "INCREASED", "CALL")):
        return "inflow"
    if any(kw in upper for kw in ("SELL", "SALE", "CLOSED", "DECREASED", "PUT")):
        return "outflow"
    # Default based on context
    return "inflow"


def _get_vwap_observation(engine: Engine, ticker: str, obs_date: date):
    """Real VWAP proxy for a ticker/date: the latest accepted yfinance close
    at or before ``obs_date``, via ``store.observations.read_latest``
    (SUCCESS-only, PIT, one row per obs_date).

    Returns ``None`` — never a fabricated default — when there is no real
    observation to use. This replaces the old ``_get_vwap_estimate``, which
    returned a hard-coded ``_DEFAULT_VWAP_ESTIMATE`` ($50) whenever the
    lookup came up empty, silently inventing dark-pool dollar amounts out
    of thin air (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3 item 1).

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker.
        obs_date: Observation date (the dark-pool signal's own date — never
            ``date.today()``).

    Returns:
        A ``store.observations.Observation`` (with ``.value`` and
        ``.obs_date``), or ``None`` if no real VWAP observation exists.
    """
    if not ticker:
        return None
    try:
        with engine.connect() as conn:
            return read_latest(conn, f"YF:{ticker}:close", source="yfinance", as_of=obs_date)
    except Exception as exc:
        log.warning("VWAP observation lookup failed for {t}: {e}", t=ticker, e=exc)
        return None


def _vwap_age_trading_days(vwap_obs_date: date, signal_date: date) -> int:
    """Trading days between a VWAP observation and the signal it prices.

    0 when they fall on the same session. Uses the real NYSE calendar
    (``ingestion/market_calendar.py``), not a flat weekday count, so a
    Friday close pricing a Monday signal is correctly 1 trading day old,
    not 3.
    """
    if vwap_obs_date >= signal_date:
        return 0
    return len(trading_days_between(vwap_obs_date, signal_date)) - 1


def _is_vwap_too_stale(vwap_obs, signal_date: date) -> bool:
    """True when ``vwap_obs`` is older than ``_VWAP_MAX_AGE_TRADING_DAYS``
    relative to ``signal_date`` — too old to price this row with, per code
    review on PR #712."""
    return _vwap_age_trading_days(vwap_obs.obs_date, signal_date) > _VWAP_MAX_AGE_TRADING_DAYS


def _build_sector_lookup() -> dict[str, str]:
    """Build a ticker -> sector name mapping from the SECTOR_MAP.

    Returns:
        Dict mapping uppercase ticker to sector name.
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except ImportError:
        return {}

    lookup: dict[str, str] = {}
    for sector_name, sector_data in SECTOR_MAP.items():
        etf = sector_data.get("etf")
        if etf:
            lookup[etf.upper()] = sector_name
        for _subsector_name, subsector_data in sector_data.get("subsectors", {}).items():
            for actor in subsector_data.get("actors", []):
                ticker = actor.get("ticker")
                if ticker:
                    lookup[ticker.upper()] = sector_name
    return lookup


_SECTOR_LOOKUP: dict[str, str] | None = None


def _get_sector(ticker: str | None) -> str:
    """Return the sector for a ticker, or 'Unknown'."""
    global _SECTOR_LOOKUP
    if _SECTOR_LOOKUP is None:
        _SECTOR_LOOKUP = _build_sector_lookup()
    if not ticker:
        return "Unknown"
    return _SECTOR_LOOKUP.get(ticker.upper(), "Unknown")


# ── Normalization per source type ─────────────────────────────────────────

def _normalize_congressional(row: dict) -> dict | None:
    """Normalize a congressional trading signal to USD.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None if unparseable.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    amount_range = sv.get("amount_range", "")
    midpoint = sv.get("amount_midpoint")

    if midpoint and float(midpoint) > 0:
        amount = float(midpoint)
    else:
        amount = _midpoint_amount(amount_range)

    if amount <= 0:
        return None

    return {
        "source_type": "congressional",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": _direction_from_signal(row.get("signal_type", "")),
        "confidence": "estimated",
        "evidence": {
            "amount_range": amount_range,
            "chamber": sv.get("chamber"),
            "party": sv.get("party"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_insider(row: dict) -> dict | None:
    """Normalize an insider (Form 4) signal to USD.

    Uses shares x price for individual trades, total_value for cluster buys.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    signal_type = row.get("signal_type", "")

    # Cluster buy: use total_value directly
    if "CLUSTER" in signal_type.upper():
        amount = float(sv.get("total_value", 0))
        if amount <= 0:
            return None
        return {
            "source_type": "insider",
            "actor_name": row.get("source_id", ""),
            "ticker": row.get("ticker"),
            "amount_usd": amount,
            "direction": "inflow",
            "confidence": "confirmed",
            "evidence": {
                "insider_count": sv.get("insider_count"),
                "signal_type": signal_type,
            },
            "flow_date": row.get("signal_date"),
        }

    # Individual trade: shares x price
    shares = float(sv.get("shares", 0))
    price = float(sv.get("price", 0))
    value = float(sv.get("value", 0))

    # Use pre-computed value if available, otherwise shares * price
    amount = value if value > 0 else (shares * price)
    if amount <= 0:
        return None

    return {
        "source_type": "insider",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": _direction_from_signal(signal_type),
        "confidence": "confirmed",
        "evidence": {
            "shares": shares,
            "price": price,
            "insider_title": sv.get("insider_title"),
            "signal_type": signal_type,
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_darkpool(row: dict, engine: Engine) -> tuple[dict | None, str | None]:
    """Normalize a dark pool volume spike to estimated USD.

    Multiplies spike volume by a real observed VWAP proxy (see
    ``_get_vwap_observation``). Never substitutes a fabricated default
    price: when no real VWAP observation exists, or the newest one found is
    older than ``_VWAP_MAX_AGE_TRADING_DAYS``, the row is dropped rather
    than persisted with an invented or stale dollar amount
    (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3 item 1, staleness bound
    added per PR #712 review). ``flow_date`` always comes from the row's
    own ``signal_date`` — never ``date.today()`` (§4.3 item 2).

    Parameters:
        row: Dict with signal_sources columns.
        engine: SQLAlchemy engine for the VWAP lookup.

    Returns:
        ``(flow_dict_or_None, skip_reason)``. ``skip_reason`` is
        ``"no_vwap"`` when no real VWAP observation was found at all,
        ``"stale_vwap"`` when one was found but is too old to price this
        row with, or ``None`` for a normal skip (zero volume / missing
        date) or a successful normalization — callers should count the
        first two separately from each other and from an
        unparseable/zero-volume/missing-date row.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    volume = float(sv.get("volume", 0))
    if volume <= 0:
        return None, None

    ticker = row.get("ticker") or ""
    obs_date = row.get("signal_date")
    if not obs_date:
        # flow_date must come from the row — never default to today.
        return None, None

    vwap_obs = _get_vwap_observation(engine, ticker, obs_date)
    if vwap_obs is None:
        return None, "no_vwap"
    if _is_vwap_too_stale(vwap_obs, obs_date):
        return None, "stale_vwap"

    amount = volume * vwap_obs.value
    direction, direction_basis = _signed_direction_from_payload(
        sv, buy_key="buy_volume", sell_key="sell_volume",
    )

    return {
        "source_type": "darkpool",
        "actor_name": row.get("source_id", ""),
        "ticker": ticker,
        "amount_usd": amount,
        "direction": direction,  # neutral on unsigned ATS volume; signed only when measured
        "confidence": "estimated",
        "evidence": {
            "volume": volume,
            "vwap": vwap_obs.value,
            "vwap_obs_date": vwap_obs.obs_date.isoformat(),
            "spike_ratio": sv.get("spike_ratio"),
            "signal_type": row.get("signal_type"),
            "direction_basis": direction_basis,
        },
        "flow_date": obs_date,
    }, None


def _normalize_whale_options(row: dict) -> dict | None:
    """Normalize a whale options flow signal to USD.

    Uses the notional premium directly (already in USD).

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    notional = float(sv.get("notional", 0))
    if notional <= 0:
        return None

    direction_str = sv.get("direction", "")
    if "CALL" in direction_str.upper() or "BULL" in direction_str.upper():
        direction = "inflow"
    elif "PUT" in direction_str.upper() or "BEAR" in direction_str.upper():
        direction = "outflow"
    else:
        direction = _direction_from_signal(row.get("signal_type", ""))

    return {
        "source_type": "options_flow",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": notional,
        "direction": direction,
        "confidence": "confirmed",
        "evidence": {
            "notional_premium": notional,
            "oi_ratio": sv.get("oi_ratio"),
            "volume_ratio": sv.get("volume_ratio"),
            "signals": sv.get("signals"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_prediction_market(row: dict) -> dict | None:
    """Normalize a prediction market signal to estimated USD.

    Prediction markets report probability shifts, not direct dollar amounts.
    We estimate the implied capital flow from the probability shift magnitude.
    A 10pp shift on a liquid market implies ~$1M directional capital.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    shift = abs(float(sv.get("shift", 0)))
    if shift <= 0:
        return None

    # Estimate: 10pp shift ~ $1M implied capital (rough Polymarket heuristic)
    amount = shift * 100_000

    direction_str = sv.get("direction", "")
    if "UP" in direction_str.upper() or "BULL" in direction_str.upper():
        direction = "inflow"
    elif "DOWN" in direction_str.upper() or "BEAR" in direction_str.upper():
        direction = "outflow"
    else:
        direction = "inflow"

    return {
        "source_type": "prediction_market",
        "actor_name": row.get("source_id", ""),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": direction,
        "confidence": "estimated",
        "evidence": {
            "shift_pct": shift,
            "current_prob": sv.get("current_prob"),
            "title": sv.get("title"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_foreign_lobbying(row: dict) -> dict | None:
    """Normalize a FARA foreign lobbying signal to USD.

    FARA activities include explicit compensation amounts from foreign
    principals to registered agents. These are direct dollar flows.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    amount = float(sv.get("compensation", 0))
    if amount <= 0:
        return None

    return {
        "source_type": "foreign_lobbying",
        "actor_name": sv.get("principal_name", row.get("source_id", "")),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": "inflow",  # Foreign money flowing into US lobbying
        "confidence": "confirmed",
        "evidence": {
            "country": sv.get("country"),
            "activity_type": sv.get("activity_type"),
            "registrant": row.get("source_id"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


def _normalize_lobbying(row: dict) -> dict | None:
    """Normalize a domestic lobbying signal to USD.

    Parameters:
        row: Dict with signal_sources columns.

    Returns:
        Normalized flow dict or None.
    """
    sv = row.get("signal_value") or {}
    if isinstance(sv, str):
        try:
            sv = json.loads(sv)
        except (json.JSONDecodeError, TypeError):
            sv = {}

    amount = float(sv.get("amount", 0))
    if amount <= 0:
        return None

    return {
        "source_type": "lobbying",
        "actor_name": sv.get("client_name", row.get("source_id", "")),
        "ticker": row.get("ticker"),
        "amount_usd": amount,
        "direction": "inflow",
        "confidence": "confirmed",
        "evidence": {
            "registrant_name": sv.get("registrant_name"),
            "issue_codes": sv.get("issue_codes"),
            "signal_type": row.get("signal_type"),
        },
        "flow_date": row.get("signal_date"),
    }


# ── 13F + ETF flow normalization (from raw_series) ───────────────────────

def _parse_13f_shares(raw: Any) -> int | float | None:
    """Parse a non-negative finite 13F share count, or ``None`` if invalid."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    if isinstance(raw, float):
        if not math.isfinite(raw) or raw < 0:
            return None
        return int(raw) if raw.is_integer() else raw
    if isinstance(raw, str):
        cleaned = raw.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            val = float(cleaned)
        except ValueError:
            return None
        if not math.isfinite(val) or val < 0:
            return None
        return int(val) if val.is_integer() else val
    return None


def _parse_positive_float(raw: Any) -> float | None:
    """Parse a strictly positive finite float, or ``None``."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val) or val <= 0:
        return None
    return val


def _parse_signed_number(raw: Any) -> float | None:
    """Parse any finite numeric value (signed), or ``None``."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        val = float(str(raw).replace(",", "").strip() if isinstance(raw, str) else raw)
    except (TypeError, ValueError):
        return None
    return val if math.isfinite(val) else None


def _extract_13f_vintage_meta(payload: dict[str, Any], obs_date: date) -> dict[str, Any]:
    """Extract filing/amendment vintage metadata from a 13F raw_payload."""
    report_date = _as_date(
        payload.get("report_date")
        or payload.get("period_of_report")
        or payload.get("quarter_end")
    )
    filing_date = _as_date(payload.get("filing_date") or payload.get("filed_date")) or obs_date
    form_type = str(payload.get("form_type") or payload.get("form") or "").strip().upper()
    replaces_acc = str(
        payload.get("replaces_accession")
        or payload.get("supersedes_accession")
        or payload.get("restates_accession")
        or payload.get("amended_accession")
        or ""
    ).strip()
    amendment_type = str(payload.get("amendment_type") or "").strip().upper()
    is_amendment = (
        bool(payload.get("is_amendment"))
        or form_type.endswith("/A")
        or form_type in ("13F-A", "13F/A")
        or bool(amendment_type)
        or bool(replaces_acc)
    )
    filing_accession = str(payload.get("filing_accession") or "").strip()
    return {
        "report_date": report_date,
        "filing_date": filing_date,
        "form_type": form_type,
        "is_amendment": is_amendment,
        "amendment_type": amendment_type,
        "filing_accession": filing_accession,
        "replaces_accession": replaces_acc,
    }


def _resolve_13f_amendments(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Ensure amended 13F filings (13F-HR/A restatements) replace originals for
    each filer-quarter instead of double counting.

    Applies three deterministic, PIT-safe rules over already PIT-filtered rows:
      1. Explicit accession supersession (``replaces_accession`` /
         ``supersedes_accession`` / ``restates_accession``).
      2. Filer-quarter ``(cik, report_date)`` replacement: full-filing
         restatements (``13F-HR/A`` unless ``amendment_type == 'NEW HOLDINGS'``)
         replace the entire original filing for that ``(cik, report_date)``.
         ``NEW HOLDINGS`` add-ons supplement the winning base filing and only
         override matching CUSIPs.
      3. Per-position ``(cik, cusip, report_date or obs_date)`` dedup across
         actions so direction flips (e.g. ``INCREASED`` -> ``DECREASED``) never
         emit both sides.
    """
    superseded = 0

    # 1. Explicit accession replacement (scoped by CIK)
    replaced_accs: set[tuple[str, str]] = {
        (c["cik"], c["meta"]["replaces_accession"])
        for c in candidates
        if c["meta"]["replaces_accession"]
    }
    after_explicit: list[dict[str, Any]] = []
    for c in candidates:
        acc = c["meta"]["filing_accession"]
        if acc and (c["cik"], acc) in replaced_accs:
            superseded += 1
            continue
        after_explicit.append(c)

    # 2. Filer-quarter (cik, report_date) replacement
    by_quarter: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
    no_quarter: list[dict[str, Any]] = []
    for c in after_explicit:
        rd = c["meta"]["report_date"]
        if rd is not None:
            by_quarter[(c["cik"], rd)].append(c)
        else:
            no_quarter.append(c)

    after_quarter: list[dict[str, Any]] = list(no_quarter)
    for _qk, q_rows in by_quarter.items():
        restatement_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        addon_rows: list[dict[str, Any]] = []
        for c in q_rows:
            m = c["meta"]
            if m["amendment_type"] == "NEW HOLDINGS":
                addon_rows.append(c)
            else:
                if m["filing_accession"]:
                    gkey: tuple[Any, ...] = ("acc", m["filing_accession"])
                else:
                    gkey = ("date_form", m["filing_date"], m["is_amendment"], m["form_type"])
                restatement_groups[gkey].append(c)

        winning_date = date.min
        if restatement_groups:
            def _group_rank(items: list[dict[str, Any]]) -> tuple[date, int, datetime, str]:
                max_fd = max(x["meta"]["filing_date"] for x in items)
                any_am = 1 if any(x["meta"]["is_amendment"] for x in items) else 0
                max_pt = max(_ts_utc(x["pull_ts"]) for x in items)
                max_acc = max(x["meta"]["filing_accession"] for x in items)
                return (max_fd, any_am, max_pt, max_acc)

            winner_key = max(restatement_groups, key=lambda k: _group_rank(restatement_groups[k]))
            winner_rows = restatement_groups[winner_key]
            winner_rank = _group_rank(winner_rows)
            winning_date = winner_rank[0]
            for k, grp in restatement_groups.items():
                if k == winner_key:
                    after_quarter.extend(grp)
                    continue
                grp_rank = _group_rank(grp)
                # Only supersede an earlier/distinct filing vintage (newer filing_date,
                # an amendment, or two distinct non-empty accessions). A same-day
                # non-amendment row lacking an accession stays eligible for per-CUSIP
                # dedup and qualification/quarantine rather than being misclassified
                # as a superseded vintage.
                both_non_amend_same_day = (
                    winner_rank[0] == grp_rank[0]
                    and winner_rank[1] == 0
                    and grp_rank[1] == 0
                    and (not winner_rank[3] or not grp_rank[3])
                )
                if both_non_amend_same_day:
                    after_quarter.extend(grp)
                else:
                    superseded += len(grp)

        for addon in addon_rows:
            if addon["meta"]["filing_date"] >= winning_date:
                after_quarter.append(addon)
            else:
                superseded += 1

    # 3. Per-position (cik, cusip, report_date or obs_date) dedup across actions
    best_pos: dict[tuple[str, str, date], tuple[tuple[date, int, datetime, str], dict[str, Any]]] = {}
    for c in after_quarter:
        m = c["meta"]
        period = m["report_date"] if m["report_date"] is not None else c["obs_date"]
        pkey = (c["cik"], c["cusip"], period)
        rank = (m["filing_date"], 1 if m["is_amendment"] else 0, _ts_utc(c["pull_ts"]), m["filing_accession"])
        prev = best_pos.get(pkey)
        if prev is None:
            best_pos[pkey] = (rank, c)
        else:
            superseded += 1
            if rank > prev[0]:
                best_pos[pkey] = (rank, c)

    survivors = [item[1] for item in best_pos.values()]
    survivors.sort(key=lambda c: (c["obs_date"], c["cik"], c["cusip"]), reverse=True)
    return survivors, superseded


def _qualify_and_estimate_13f_flow(
    action: str,
    raw_val: Any,
    payload: dict[str, Any],
    obs_date: date,
    meta: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Qualify a 13F position-change row and compute its share-based flow estimate.

    Option (a) documented share-based estimate: ``|delta_shares| * stated price``
    where stated price is the later quarter-end implied price
    (``curr_value_usd / curr_shares``) for ``NEW``/``INCREASED``/``DECREASED``
    and prior quarter-end implied price (``prev_value_usd / prev_shares``) for
    ``CLOSED``.

    Returns:
      - ``(estimate_dict, None)`` for a qualified non-zero directional flow;
      - ``(None, "zero_share_delta")`` when shares are unchanged (0 flow);
      - ``(None, "quarantine:<reason>")`` for legacy whole-position, mixed or
        unknown units, missing share counts, non-equity/PRN, or disqualified rows.
    """
    if payload.get("qualified_for_directional_flow") is False and (
        payload.get("unqualified_reason") not in _13F_OBSERVATION_REASONS
        or action not in ("NEW", "CLOSED")
    ):
        # NEW/CLOSED flagged only for missing identity coverage continue as
        # observations; anything else unqualified is quarantined.
        return None, "quarantine:failed_qualification"
    if payload.get("put_call"):
        return None, "quarantine:non_equity_option"

    share_type = str(payload.get("share_type") or "").strip().upper()
    if share_type and share_type != "SH":
        return None, "quarantine:non_share_unit"

    if (
        str(payload.get("position_basis") or "").strip().lower() == "whole_position"
        or payload.get("is_legacy_whole_position") is True
    ):
        return None, "quarantine:legacy_whole_position"

    declared_method = payload.get("flow_estimation_method")
    if declared_method is not None and str(declared_method).strip() not in _QUALIFIED_13F_FLOW_METHODS:
        return None, "quarantine:unqualified_estimation_method"

    # Unit verification: require explicit value_units='usd' (quarantines legacy
    # pre-fix 1000x-multiplied rows, unknown-unit rows, and mixed-unit rows).
    value_units = str(payload.get("value_units") or "").strip().lower()
    if value_units != "usd":
        return None, "quarantine:unknown_or_mixed_units"

    effective_filing_date = meta["filing_date"] or obs_date
    if "value_multiplier" in payload:
        vm = payload.get("value_multiplier")
        if isinstance(vm, bool) or vm not in (1, 1000):
            return None, "quarantine:unknown_or_mixed_units"
        expected_vm = 1 if effective_filing_date >= _13F_DOLLAR_CUTOVER_DATE else 1000
        if vm != expected_vm:
            return None, "quarantine:mixed_or_invalid_units"

    if "prev_value_multiplier" in payload and payload.get("prev_filing_date"):
        pvm = payload.get("prev_value_multiplier")
        pfd = _as_date(payload.get("prev_filing_date"))
        if isinstance(pvm, bool) or pvm not in (1, 1000):
            return None, "quarantine:unknown_or_mixed_units"
        if pfd is not None:
            expected_pvm = 1 if pfd >= _13F_DOLLAR_CUTOVER_DATE else 1000
            if pvm != expected_pvm:
                return None, "quarantine:mixed_or_invalid_units"

    raw_num = _parse_signed_number(raw_val)
    curr_val = _parse_positive_float(payload.get("curr_value_usd"))
    prev_val = _parse_positive_float(payload.get("prev_value_usd"))
    stated_price = _parse_positive_float(payload.get("valuation_price_usd"))

    if action in ("INCREASED", "DECREASED"):
        prev_shares = _parse_13f_shares(payload.get("prev_shares"))
        curr_shares = _parse_13f_shares(
            payload.get("curr_shares") if "curr_shares" in payload else payload.get("shares")
        )
        if prev_shares is None or prev_shares <= 0 or curr_shares is None or curr_shares <= 0:
            return None, "quarantine:missing_share_counts"

        # Detect legacy whole-position row stored in raw_series.value on INCREASED
        if (
            action == "INCREASED"
            and curr_val is not None
            and raw_num is not None
            and declared_method is None
            and abs(raw_num - curr_val) / curr_val < 1e-6
        ):
            return None, "quarantine:legacy_whole_position"

        delta_shares = curr_shares - prev_shares
        if payload.get("delta_shares") is not None:
            declared_delta = _parse_signed_number(payload.get("delta_shares"))
            if declared_delta is None or abs(declared_delta - delta_shares) > 1e-6:
                return None, "quarantine:inconsistent_share_delta"

        if delta_shares == 0:
            return None, "zero_share_delta"

        if (action == "INCREASED" and delta_shares < 0) or (action == "DECREASED" and delta_shares > 0):
            return None, "quarantine:action_share_direction_mismatch"

        # A split or reverse split changes shares with no trade, and a price
        # move can mask it; with no CUSIP-level corporate-action coverage a
        # continuing position's share change is not a verified direction.
        return None, "quarantine:no_corporate_action_coverage"

    if action == "NEW":
        curr_shares = _parse_13f_shares(
            payload.get("curr_shares") if "curr_shares" in payload else payload.get("shares")
        )
        if curr_shares is None or curr_shares <= 0:
            return None, "quarantine:missing_share_counts"
        if payload.get("prev_shares") is not None:
            ps = _parse_13f_shares(payload.get("prev_shares"))
            if ps is None or ps != 0:
                return None, "quarantine:inconsistent_new_position"

        if curr_val is not None:
            price = curr_val / curr_shares
            if stated_price is not None and abs(price - stated_price) / price > 0.01:
                return None, "quarantine:inconsistent_valuation_price"
        elif stated_price is not None:
            price = stated_price
        elif raw_num is not None and raw_num > 0:
            curr_val = raw_num
            price = curr_val / curr_shares
        else:
            return None, "quarantine:missing_valuation_price"

        estimated_flow = curr_shares * price
        if not math.isfinite(estimated_flow) or estimated_flow <= 0:
            return None, "quarantine:invalid_flow_estimate"

        return {
            "estimated_flow_usd": round(estimated_flow, 2),
            "direction": "inflow",
            "prev_shares": 0,
            "curr_shares": curr_shares,
            "delta_shares": curr_shares,
            "valuation_price_usd": round(price, 6),
            "valuation_price_basis": "curr_quarter_end_implied",
            "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
            "holdings_value_delta_usd": curr_val,
        }, None

    if action == "CLOSED":
        prev_shares = _parse_13f_shares(
            payload.get("prev_shares") if "prev_shares" in payload else payload.get("shares")
        )
        if prev_shares is None or prev_shares <= 0:
            return None, "quarantine:missing_share_counts"
        if payload.get("curr_shares") is not None:
            cs = _parse_13f_shares(payload.get("curr_shares"))
            if cs is None or cs != 0:
                return None, "quarantine:inconsistent_closed_position"

        if prev_val is not None:
            price = prev_val / prev_shares
            if stated_price is not None and abs(price - stated_price) / price > 0.01:
                return None, "quarantine:inconsistent_valuation_price"
        elif stated_price is not None:
            price = stated_price
        elif raw_num is not None and raw_num > 0:
            prev_val = raw_num
            price = prev_val / prev_shares
        else:
            return None, "quarantine:missing_valuation_price"

        estimated_flow = prev_shares * price
        if not math.isfinite(estimated_flow) or estimated_flow <= 0:
            return None, "quarantine:invalid_flow_estimate"

        return {
            "estimated_flow_usd": round(estimated_flow, 2),
            "direction": "outflow",
            "prev_shares": prev_shares,
            "curr_shares": 0,
            "delta_shares": -prev_shares,
            "valuation_price_usd": round(price, 6),
            "valuation_price_basis": "prev_quarter_end_implied",
            "flow_estimation_method": "delta_shares_x_prev_quarter_end_price",
            "holdings_value_delta_usd": -prev_val if prev_val is not None else None,
        }, None

    return None, "quarantine:unknown_action"


def is_qualified_13f_dollar_flow_row(confidence: str | None, raw_evidence: Any) -> bool:
    """Public qualification test for every served reader of 13F ``dollar_flows`` rows.

    A 13F row is an *estimated holdings-position change at reference prices*
    (share delta x quarter-end implied price), never observed cash or
    transaction flow, and counts only with documented share-based evidence.
    """
    return _is_qualified_13f_dollar_flow_row(confidence, raw_evidence)


def _is_qualified_13f_dollar_flow_row(confidence: str | None, raw_evidence: Any) -> bool:
    """True only when a persisted ``dollar_flows`` 13F row carries documented
    share-based qualification (excludes legacy un-rebuilt mark-to-market rows)."""
    if confidence != "derived":
        return False
    ev = raw_evidence
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except (json.JSONDecodeError, TypeError):
            return False
    if not isinstance(ev, dict):
        return False
    if ev.get("qualified_for_directional_flow") is not True:
        return False
    if str(ev.get("value_units") or "").strip().lower() != "usd":
        return False
    if str(ev.get("flow_estimation_method") or "").strip() not in _QUALIFIED_13F_FLOW_METHODS:
        return False
    delta = _parse_signed_number(ev.get("delta_shares"))
    if delta is None or delta == 0:
        return False
    return True


def _normalize_13f_flows(
    engine: Engine,
    days: int,
    *,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
    cusip_map: Any = _UNSET,
    skipped_counts: dict[str, int] | None = None,
    quarantine_reasons: dict[str, int] | None = None,
) -> list[dict]:
    """Extract 13F position changes (raw_series) and compute share-based flow estimates.

    Only 4-part flow keys ``13F:{cik}:{cusip}:{NEW|INCREASED|DECREASED|CLOSED}``
    are accepted; 3-part portfolio summaries (``TOTAL_VALUE``, ``NUM_POSITIONS``,
    ``TOP5_CONCENTRATION``) and unknown actions are skipped (fail closed).
    Resolves PIT-eligible vintages and 13F-HR/A amendments so restatements
    replace originals for each filer-quarter instead of double counting.
    Computes documented share-based estimates (``|delta_shares| * quarter-end
    implied price``) so constant shares at a new price produce zero flow, and
    quarantines legacy whole-position, mixed/unknown-unit, missing-share, and
    unqualified rows with logged reasons and counts.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days.
        as_of: Optional PIT observation date upper bound (defaults to today).
        as_of_ts: Optional PIT pull-timestamp upper bound.
        cusip_map: Optional CUSIP->ticker mapper override (None disables FTD
            CSV fallback; default lazily loads ``CusipTickerMap``).
        skipped_counts: Optional dict updated in place with ``future_date``,
            ``non_flow_13f``, ``unmapped_cusip``, ``quarantined_13f``,
            ``superseded_13f_vintages``, and ``zero_share_delta_13f``.
        quarantine_reasons: Optional dict updated in place with per-reason
            quarantine counts.

    Returns:
        List of normalized flow dicts.
    """
    effective_as_of = as_of or date.today()
    cutoff = effective_as_of - timedelta(days=days)
    # Direct callers enforce obs_date <= effective_as_of in SQL; normalize_all_flows
    # passes skipped_counts so future-dated 13F rows are counted and dropped below.
    sql_as_of = effective_as_of if skipped_counts is None else as_of
    flows: list[dict] = []
    skipped_non_flow = 0
    unmapped_cusip = 0
    zero_share_delta = 0
    not_shown = 0
    local_quarantine: dict[str, int] = defaultdict(int)
    superseded_vintages = 0

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, value, raw_payload, pull_timestamp "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND pull_status = 'SUCCESS' "
                    "AND obs_date >= :cutoff "
                    "AND (:as_of IS NULL OR obs_date <= :as_of) "
                    "AND (:as_of_ts IS NULL OR pull_timestamp <= :as_of_ts) "
                    "ORDER BY series_id ASC, obs_date DESC, pull_timestamp DESC"
                ),
                {
                    "pattern": "13F:%",
                    "cutoff": cutoff,
                    "as_of": sql_as_of,
                    "as_of_ts": as_of_ts,
                },
            ).fetchall()

            deduped = _dedup_raw_series_vintages(
                rows,
                as_of=as_of,
                as_of_ts=as_of_ts,
                filter_pull_by_as_of=(as_of is not None),
            )
            has_sec_ids = _table_exists(conn, "security_identifiers")
            has_inst_holdings = _table_exists(conn, "institutional_holdings")
            ticker_cache: dict[tuple[str, date], str | None] = {}

            # Stage 1: parse 4-part keys and enforce PIT bounds before amendment resolution
            candidates: list[dict[str, Any]] = []
            for series_id, obs_date, raw_val, raw_payload, pull_ts in deduped:
                parts = series_id.split(":")
                if len(parts) != 4 or parts[0] != "13F" or not parts[1].strip() or not parts[2].strip():
                    skipped_non_flow += 1
                    continue

                cik = parts[1].strip()
                cusip = parts[2].strip().upper()
                action = parts[3].strip().upper()
                if action not in _VALID_13F_ACTIONS and action != _NOT_SHOWN_13F_ACTION:
                    skipped_non_flow += 1
                    continue

                if obs_date > effective_as_of:
                    if skipped_counts is not None:
                        skipped_counts["future_date"] = skipped_counts.get("future_date", 0) + 1
                    continue

                payload = raw_payload or {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                if not isinstance(payload, dict):
                    payload = {}

                meta = _extract_13f_vintage_meta(payload, obs_date)
                if meta["filing_date"] > effective_as_of:
                    if skipped_counts is not None:
                        skipped_counts["future_date"] = skipped_counts.get("future_date", 0) + 1
                    continue

                candidates.append({
                    "series_id": series_id,
                    "cik": cik,
                    "cusip": cusip,
                    "action": action,
                    "obs_date": obs_date,
                    "raw_val": raw_val,
                    "payload": payload,
                    "pull_ts": pull_ts,
                    "meta": meta,
                })

            # Stage 2: resolve 13F-HR/A amendments & filer-quarter vintages before
            # zero-delta / quarantine filtering so restatements supersede originals.
            resolved_candidates, superseded_vintages = _resolve_13f_amendments(candidates)
            not_shown = sum(1 for item in resolved_candidates if item["action"] == _NOT_SHOWN_13F_ACTION)
            resolved_candidates = [
                item for item in resolved_candidates if item["action"] != _NOT_SHOWN_13F_ACTION
            ]

            # Stage 3: qualify rows and compute share-based flow estimates
            for item in resolved_candidates:
                cik = item["cik"]
                cusip = item["cusip"]
                action = item["action"]
                obs_date = item["obs_date"]
                raw_val = item["raw_val"]
                payload = item["payload"]
                meta = item["meta"]

                estimate, reject_reason = _qualify_and_estimate_13f_flow(
                    action, raw_val, payload, obs_date, meta
                )
                if reject_reason == "zero_share_delta":
                    zero_share_delta += 1
                    continue
                if reject_reason is not None and reject_reason.startswith("quarantine:"):
                    reason_key = reject_reason.split(":", 1)[1]
                    local_quarantine[reason_key] += 1
                    continue
                if estimate is None:
                    local_quarantine["failed_qualification"] += 1
                    continue

                manager_name = payload.get("manager_name") or cik
                resolved_ticker = _resolve_13f_ticker(
                    conn,
                    cusip,
                    payload,
                    obs_date,
                    has_sec_ids=has_sec_ids,
                    has_inst_holdings=has_inst_holdings,
                    cusip_map=cusip_map,
                    cache=ticker_cache,
                )
                if resolved_ticker is None:
                    unmapped_cusip += 1

                flows.append({
                    "source_type": "13f",
                    "actor_name": manager_name,
                    "ticker": resolved_ticker,
                    "amount_usd": estimate["estimated_flow_usd"],
                    "direction": OBSERVED_13F_DIRECTION,
                    "confidence": "derived",
                    "evidence": {
                        "cusip": payload.get("cusip") or cusip,
                        "issuer_name": payload.get("issuer_name"),
                        "action": action,
                        "pct_change": payload.get("pct_change"),
                        "shares": estimate["curr_shares"] if action != "CLOSED" else estimate["prev_shares"],
                        "prev_shares": estimate["prev_shares"],
                        "curr_shares": estimate["curr_shares"],
                        "delta_shares": estimate["delta_shares"],
                        "valuation_price_usd": estimate["valuation_price_usd"],
                        "valuation_price_basis": estimate["valuation_price_basis"],
                        "flow_estimation_method": estimate["flow_estimation_method"],
                        "holdings_value_delta_usd": estimate["holdings_value_delta_usd"],
                        "value_units": "usd",
                        "qualified_for_directional_flow": False,
                        "unqualified_reason": "no_identity_coverage",
                        "observation_kind": "filing_appearance" if action == "NEW" else "filing_disappearance",
                        "measure": OBSERVED_13F_MEASURE,
                        "filing_accession": meta["filing_accession"] or payload.get("filing_accession"),
                        "report_date": meta["report_date"].isoformat() if meta["report_date"] else None,
                        "form_type": meta["form_type"] or None,
                        "is_amendment": meta["is_amendment"],
                    },
                    "flow_date": obs_date,
                })

        quarantined_total = sum(local_quarantine.values())
        if quarantine_reasons is not None:
            for r_key, cnt in local_quarantine.items():
                quarantine_reasons[r_key] = quarantine_reasons.get(r_key, 0) + cnt
        if skipped_counts is not None:
            skipped_counts["non_flow_13f"] = skipped_counts.get("non_flow_13f", 0) + skipped_non_flow
            skipped_counts["unmapped_cusip"] = skipped_counts.get("unmapped_cusip", 0) + unmapped_cusip
            skipped_counts["quarantined_13f"] = skipped_counts.get("quarantined_13f", 0) + quarantined_total
            skipped_counts["superseded_13f_vintages"] = (
                skipped_counts.get("superseded_13f_vintages", 0) + superseded_vintages
            )
            skipped_counts["zero_share_delta_13f"] = (
                skipped_counts.get("zero_share_delta_13f", 0) + zero_share_delta
            )
            skipped_counts["not_shown_13f"] = skipped_counts.get("not_shown_13f", 0) + not_shown
        if quarantined_total:
            log.warning(
                "13F normalization quarantined {q} non-qualified/legacy rows from directional flows "
                "(reasons={reasons}, kept={k})",
                q=quarantined_total,
                reasons=dict(local_quarantine),
                k=len(flows),
            )
        if skipped_non_flow or unmapped_cusip or superseded_vintages or zero_share_delta:
            log.debug(
                "13F normalization kept {k} flows (skipped_non_flow={nf}, unmapped_cusip={uc}, "
                "superseded_vintages={sv}, zero_share_delta={zd})",
                k=len(flows),
                nf=skipped_non_flow,
                uc=unmapped_cusip,
                sv=superseded_vintages,
                zd=zero_share_delta,
            )
    except Exception as exc:
        log.warning("13F flow normalization failed: {e}", e=str(exc))

    return flows


def _normalize_etf_flows(
    engine: Engine,
    days: int,
    *,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
    skipped_counts: dict[str, int] | None = None,
) -> list[dict]:
    """Extract ETF flow data from raw_series and normalize to USD.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days.
        as_of: Optional PIT observation date upper bound (defaults to today).
        as_of_ts: Optional PIT pull-timestamp upper bound.
        skipped_counts: Optional dict updated in place with ``future_date``.

    Returns:
        List of normalized flow dicts.
    """
    effective_as_of = as_of or date.today()
    cutoff = effective_as_of - timedelta(days=days)
    sql_as_of = effective_as_of if skipped_counts is None else as_of
    flows: list[dict] = []

    try:
        with engine.connect() as conn:
            # ETF_FLOW:{ticker}:5d series contains 5-day rolling $ flows
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, value, raw_payload, pull_timestamp "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND pull_status = 'SUCCESS' "
                    "AND obs_date >= :cutoff "
                    "AND (:as_of IS NULL OR obs_date <= :as_of) "
                    "AND (:as_of_ts IS NULL OR pull_timestamp <= :as_of_ts) "
                    "ORDER BY series_id ASC, obs_date DESC, pull_timestamp DESC"
                ),
                {
                    "pattern": "ETF_FLOW:%:5d",
                    "cutoff": cutoff,
                    "as_of": sql_as_of,
                    "as_of_ts": as_of_ts,
                },
            ).fetchall()

        deduped = _dedup_raw_series_vintages(
            rows,
            as_of=as_of,
            as_of_ts=as_of_ts,
            filter_pull_by_as_of=(as_of is not None),
        )

        for series_id, obs_date, raw_val, raw_payload, _pull_ts in deduped:
            if obs_date > effective_as_of:
                if skipped_counts is not None:
                    skipped_counts["future_date"] = skipped_counts.get("future_date", 0) + 1
                continue
            if raw_val is None:
                continue
            value = float(raw_val)
            if not math.isfinite(value) or abs(value) <= 0:
                continue

            payload = raw_payload or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}

            # Parse series_id: ETF_FLOW:{ticker}:5d
            # ETF 5d series is unsigned dollar volume (Close * Volume), not
            # signed creations/redemptions. Emit direction='neutral' unless a
            # signed measure is present in the payload.
            parts = series_id.split(":")
            ticker = parts[1] if len(parts) > 1 else ""
            # Conservative magnitude proxy (~0.3% of 5d dollar volume)
            estimated_net = value * 0.003
            direction, direction_basis = _signed_direction_from_payload(payload)

            flows.append({
                "source_type": "etf_flow",
                "actor_name": f"ETF:{ticker}",
                "ticker": ticker,
                "amount_usd": abs(estimated_net),
                "direction": direction,
                "confidence": "estimated",
                "evidence": {
                    "series_id": series_id,
                    "raw_value": value,
                    "direction_basis": direction_basis,
                },
                "flow_date": obs_date,
            })
    except Exception as exc:
        log.warning("ETF flow normalization failed: {e}", e=str(exc))

    return flows


# ── Core API ──────────────────────────────────────────────────────────────

class FlowNormalizationSummary(list):
    """The flows kept from one ``normalize_all_flows`` run.

    Subclasses ``list`` so existing callers (``len(flows)``, iteration —
    see ``api/routers/intelligence_govflow.py``) keep working unchanged.
    Adds this run's honesty counters from
    GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3: how many rows were
    dropped for lacking a real VWAP (never fabricated at the old $50
    default), for carrying only a stale VWAP (older than
    ``_VWAP_MAX_AGE_TRADING_DAYS``, per PR #712 review), or for carrying a
    future-dated ``signal_date``/``obs_date`` (never persisted).
    """

    def __init__(
        self,
        flows: list[dict],
        *,
        skipped_no_vwap: int = 0,
        skipped_stale_vwap: int = 0,
        skipped_future_date: int = 0,
        quarantined_13f: int = 0,
        quarantined_13f_reasons: dict[str, int] | None = None,
        superseded_13f_vintages: int = 0,
        zero_share_delta_13f: int = 0,
        by_source: dict[str, int] | None = None,
        persisted: int = 0,
        dry_run: bool = False,
        rejected_by_contract: dict[str, int] | None = None,
        stored_contract_violations: dict[str, int] | None = None,
    ) -> None:
        super().__init__(flows)
        self.skipped_no_vwap = skipped_no_vwap
        self.skipped_stale_vwap = skipped_stale_vwap
        self.skipped_future_date = skipped_future_date
        self.quarantined_13f = quarantined_13f
        self.quarantined_13f_reasons = dict(quarantined_13f_reasons or {})
        self.superseded_13f_vintages = superseded_13f_vintages
        self.zero_share_delta_13f = zero_share_delta_13f
        self.by_source = by_source or {}
        self.persisted = persisted
        self.dry_run = dry_run
        # Flows this run refused to store because they break their source's
        # stored-row contract (a writer regression), and stored rows at any
        # date that break it (a superseded rule's leftovers).
        self.rejected_by_contract = dict(rejected_by_contract or {})
        self.stored_contract_violations = dict(stored_contract_violations or {})

    @property
    def contract_ok(self) -> bool:
        return not self.rejected_by_contract and not self.stored_contract_violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "flows_count": len(self),
            "persisted": self.persisted,
            "skipped_no_vwap": self.skipped_no_vwap,
            "skipped_stale_vwap": self.skipped_stale_vwap,
            "skipped_future_date": self.skipped_future_date,
            "quarantined_13f": self.quarantined_13f,
            "quarantined_13f_reasons": self.quarantined_13f_reasons,
            "superseded_13f_vintages": self.superseded_13f_vintages,
            "zero_share_delta_13f": self.zero_share_delta_13f,
            "by_source": self.by_source,
            "dry_run": self.dry_run,
            "rejected_by_contract": self.rejected_by_contract,
            "stored_contract_violations": self.stored_contract_violations,
        }


def normalize_all_flows(
    engine: Engine, days: int = 90, dry_run: bool = False
) -> FlowNormalizationSummary:
    """Scan signal_sources + raw_series, normalize all signals to USD, and persist.

    Three honesty guards (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3, plus
    the staleness bound added per PR #712 review):
      - A dark-pool row with no real VWAP observation is dropped, never
        fabricated from the old ``_DEFAULT_VWAP_ESTIMATE`` ($50). Counted
        in the returned summary's ``skipped_no_vwap``.
      - A dark-pool row whose only VWAP observation is older than
        ``_VWAP_MAX_AGE_TRADING_DAYS`` is dropped rather than priced off a
        stale close. Counted in ``skipped_stale_vwap``.
      - A row whose ``signal_date`` (or, for 13F/ETF raw_series flows,
        ``obs_date``) is in the future is dropped, never persisted.
        Counted in ``skipped_future_date``.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 90).
        dry_run: When True, compute and count but do not touch the
            ``dollar_flows`` table at all (no ``_ensure_table``, no
            ``_persist_flows`` — no DELETE/INSERT). Use to preview a run
            before scheduling it: see ``scripts/run_dollar_flows.py --dry-run``.

    Returns:
        A ``FlowNormalizationSummary`` (list subclass) of the normalized
        flows kept from this run, with ``.skipped_no_vwap``,
        ``.skipped_stale_vwap``, ``.skipped_future_date``, ``.by_source``,
        ``.persisted`` and ``.dry_run`` attributes.
    """
    if not dry_run:
        _ensure_table(engine)

    today = date.today()
    cutoff = today - timedelta(days=days)
    all_flows: list[dict] = []
    skipped_no_vwap = 0
    skipped_stale_vwap = 0
    skipped_future_date = 0
    by_source: dict[str, int] = defaultdict(int)

    # 1. Normalize signal_sources rows
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT source_type, source_id, ticker, signal_date, "
                    "signal_type, signal_value "
                    "FROM signal_sources "
                    "WHERE signal_date >= :cutoff "
                    "ORDER BY signal_date DESC"
                ),
                {"cutoff": cutoff},
            ).fetchall()
    except Exception as exc:
        log.warning("Failed to fetch signal_sources: {e}", e=str(exc))
        rows = []

    for row in rows:
        row_dict = {
            "source_type": row[0],
            "source_id": row[1],
            "ticker": row[2],
            "signal_date": _as_date(row[3]),
            "signal_type": row[4],
            "signal_value": row[5],
        }

        signal_date = row_dict.get("signal_date")
        if signal_date and signal_date > today:
            # GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3 item 2: never
            # persist a future-dated signal_date.
            skipped_future_date += 1
            continue

        normalized = None
        stype = row_dict["source_type"]

        if stype == "congressional":
            normalized = _normalize_congressional(row_dict)
        elif stype == "insider":
            normalized = _normalize_insider(row_dict)
        elif stype == "darkpool":
            normalized, skip_reason = _normalize_darkpool(row_dict, engine)
            if skip_reason == "no_vwap":
                skipped_no_vwap += 1
            elif skip_reason == "stale_vwap":
                skipped_stale_vwap += 1
        elif stype == "options_flow":
            normalized = _normalize_whale_options(row_dict)
        elif stype == "prediction_market":
            normalized = _normalize_prediction_market(row_dict)
        elif stype == "foreign_lobbying":
            normalized = _normalize_foreign_lobbying(row_dict)
        elif stype == "lobbying":
            normalized = _normalize_lobbying(row_dict)

        if normalized:
            all_flows.append(normalized)
            by_source[normalized["source_type"]] += 1

    # 2. Normalize raw_series-based flows (13F + ETF), same future-date guard
    raw_skips: dict[str, int] = {}
    quarantine_reasons: dict[str, int] = {}
    for f in (
        _normalize_13f_flows(
            engine,
            days,
            skipped_counts=raw_skips,
            quarantine_reasons=quarantine_reasons,
        )
        + _normalize_etf_flows(engine, days, skipped_counts=raw_skips)
    ):
        fdate = f.get("flow_date")
        if fdate and fdate > today:
            skipped_future_date += 1
            continue
        all_flows.append(f)
        by_source[f["source_type"]] += 1
    skipped_future_date += raw_skips.get("future_date", 0)
    quarantined_13f = raw_skips.get("quarantined_13f", 0)
    superseded_13f_vintages = raw_skips.get("superseded_13f_vintages", 0)
    zero_share_delta_13f = raw_skips.get("zero_share_delta_13f", 0)

    # 3. Persist to dollar_flows table (skipped entirely in a dry run)
    persisted = 0
    rejected_by_contract: dict[str, int] = {}
    if not dry_run:
        persisted = _persist_flows(engine, all_flows, rejected_counts=rejected_by_contract)
    else:
        for f in all_flows:
            if (bad := flow_contract_violation(f)) is not None:
                rejected_by_contract[bad] = rejected_by_contract.get(bad, 0) + 1

    # 4. Read the whole table, not only this run's window: rows outside the
    # window are never rewritten, so a superseded rule's rows would otherwise
    # stay forever without anyone being told (read-only, also in a dry run).
    stored_contract_violations: dict[str, int] = {}
    with engine.connect() as conn:
        if _table_exists(conn, "dollar_flows"):
            stored_contract_violations = count_stored_contract_violations(conn)
    if rejected_by_contract or stored_contract_violations:
        log.error(
            "dollar_flows stored-row contract broken: rejected_by_contract={r}, "
            "stored_contract_violations={v}. Stored rows are removed only by the reviewed "
            "repair scripts/repair_dollar_flows_stored_contract.py",
            r=rejected_by_contract,
            v=stored_contract_violations,
        )

    log.info(
        "Normalized {n} dollar flows across {d} days (persisted={p}, "
        "skipped_no_vwap={sv}, skipped_stale_vwap={ss}, "
        "skipped_future_date={sf}, quarantined_13f={q13}, "
        "superseded_13f_vintages={sup13}, zero_share_delta_13f={zd13}, dry_run={dr})",
        n=len(all_flows),
        d=days,
        p=persisted,
        sv=skipped_no_vwap,
        ss=skipped_stale_vwap,
        sf=skipped_future_date,
        q13=quarantined_13f,
        sup13=superseded_13f_vintages,
        zd13=zero_share_delta_13f,
        dr=dry_run,
    )
    return FlowNormalizationSummary(
        all_flows,
        skipped_no_vwap=skipped_no_vwap,
        skipped_stale_vwap=skipped_stale_vwap,
        skipped_future_date=skipped_future_date,
        quarantined_13f=quarantined_13f,
        quarantined_13f_reasons=quarantine_reasons,
        superseded_13f_vintages=superseded_13f_vintages,
        zero_share_delta_13f=zero_share_delta_13f,
        by_source=dict(by_source),
        persisted=persisted,
        dry_run=dry_run,
        rejected_by_contract=rejected_by_contract,
        stored_contract_violations=stored_contract_violations,
    )


def _persist_flows(
    engine: Engine,
    flows: list[dict],
    *,
    rejected_counts: dict[str, int] | None = None,
) -> int:
    """Write normalized flows to the dollar_flows table.

    Uses upsert-like logic: clears existing rows for the date range
    covered by the new flows, then bulk inserts. Defensively drops any
    flow whose ``flow_date`` is in the future — belt-and-suspenders on top
    of the future-date filtering ``normalize_all_flows`` already does,
    since this is a public module function any future caller could invoke
    directly with unfiltered flows and it must never persist a future-dated
    row either (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3 item 2).

    Contract preflight: before anything is deleted, every flow is checked
    against its source's stored-row contract (``STORED_ROW_CONTRACTS``). If
    any breaks it, this run writes nothing at all (no DELETE, no INSERT), so
    the last good window stays as it was; the refusals are counted per source
    type in ``rejected_counts`` when given and the runner exits 4. A breaking
    flow is a writer regression, never an expected state. The refusal is for
    the whole run, so one bad 13F flow also holds back that day's options,
    ETF and insider rows until the regression is fixed; the failed unit says
    so. (A per-source scope would need a per-source DELETE; not done here.)

    A database error on an insert is raised, never swallowed: on PostgreSQL
    the failed statement aborts the transaction, so every later insert and
    the window's DELETE would be lost while the run reported success.

    Parameters:
        engine: SQLAlchemy engine.
        flows: List of normalized flow dicts.
        rejected_counts: Optional dict that receives per-source-type counts
            of contract-breaking flows that made the run refuse.

    Returns:
        Number of rows inserted.
    """
    if not flows:
        return 0

    today = date.today()
    refused: dict[str, int] = {}
    for f in flows:
        if (bad := flow_contract_violation(f)) is not None:
            refused[bad] = refused.get(bad, 0) + 1
    if refused:
        if rejected_counts is not None:
            for st, n in refused.items():
                rejected_counts[st] = rejected_counts.get(st, 0) + n
        log.error(
            "dollar_flows run refused before writing: {r} flow(s) break the stored-row "
            "contract ({rules}); nothing deleted or inserted, the last good window is kept",
            r=refused,
            rules="; ".join(STORED_ROW_CONTRACTS[st]["rule"] for st in sorted(refused)),
        )
        return 0

    inserted = 0
    with engine.begin() as conn:
        # Determine date range (future-dated rows never count toward it,
        # and are dropped below, so they can't widen the DELETE window).
        dates = [
            fd for f in flows
            if f.get("flow_date") and (fd := _as_date(f["flow_date"])) and fd <= today
        ]
        if not dates:
            return 0

        min_date = min(dates)
        max_date = max(dates)

        # Clear stale rows for this date range
        conn.execute(
            text(
                "DELETE FROM dollar_flows "
                "WHERE flow_date >= :min_date AND flow_date <= :max_date"
            ),
            {"min_date": min_date, "max_date": max_date},
        )

        # Bulk insert
        for f in flows:
            if not f.get("flow_date") or not f.get("amount_usd"):
                continue
            fd_check = _as_date(f["flow_date"])
            if fd_check is None or fd_check > today:
                log.debug(
                    "Dropping future-dated/unparseable flow (flow_date={fd}) — never persist",
                    fd=f["flow_date"],
                )
                continue
            try:
                conn.execute(
                    text(
                        "INSERT INTO dollar_flows "
                        "(source_type, actor_name, ticker, amount_usd, "
                        "direction, confidence, evidence, flow_date) "
                        "VALUES (:stype, :actor, :ticker, :amount, "
                        ":direction, :confidence, :evidence, :fdate)"
                    ),
                    {
                        "stype": f["source_type"],
                        "actor": f.get("actor_name"),
                        "ticker": f.get("ticker"),
                        "amount": f["amount_usd"],
                        "direction": f["direction"],
                        "confidence": f.get("confidence", "estimated"),
                        "evidence": json.dumps(f.get("evidence", {})),
                        "fdate": f["flow_date"],
                    },
                )
                inserted += 1
            except Exception as exc:
                log.error(
                    "dollar_flows insert failed; the whole run is rolled back: {e}",
                    e=str(exc),
                )
                raise

    log.info("Persisted {n} dollar_flows rows", n=inserted)
    return inserted


def get_flows_by_ticker(engine: Engine, ticker: str, days: int = 30) -> list[dict]:
    """Retrieve normalized dollar flows for a specific ticker.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker to filter by.
        days: Lookback window in days (default 30).

    Returns:
        List of flow dicts sorted by flow_date descending.
    """
    _ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, actor_name, ticker, amount_usd, "
                "direction, confidence, evidence, flow_date, created_at "
                "FROM dollar_flows "
                "WHERE UPPER(ticker) = :ticker "
                "AND source_type <> '13f' "
                "AND flow_date >= :cutoff AND flow_date <= :today "
                "ORDER BY flow_date DESC"
            ),
            {"ticker": ticker.upper(), "cutoff": cutoff, "today": date.today()},
        ).fetchall()

    return [
        {
            "id": r[0],
            "source_type": r[1],
            "actor_name": r[2],
            "ticker": r[3],
            "amount_usd": float(r[4]) if r[4] else 0.0,
            "direction": r[5],
            "confidence": r[6],
            "evidence": r[7] if isinstance(r[7], dict) else {},
            "flow_date": r[8].isoformat() if r[8] else None,
            "created_at": r[9].isoformat() if r[9] else None,
        }
        for r in rows
    ]


def get_flows_by_sector(engine: Engine, sector: str, days: int = 30) -> list[dict]:
    """Retrieve normalized dollar flows for all tickers in a sector.

    Parameters:
        engine: SQLAlchemy engine.
        sector: Sector name (e.g., 'Technology', 'Financials').
        days: Lookback window in days (default 30).

    Returns:
        List of flow dicts for all tickers in the sector.
    """
    _ensure_table(engine)
    sector_lookup = _build_sector_lookup()

    # Collect all tickers belonging to this sector
    sector_tickers = [
        t for t, s in sector_lookup.items()
        if s.lower() == sector.lower()
    ]

    if not sector_tickers:
        return []

    cutoff = date.today() - timedelta(days=days)

    # Use parameterized IN clause
    placeholders = ", ".join(f":t{i}" for i in range(len(sector_tickers)))
    params: dict[str, Any] = {"cutoff": cutoff, "today": date.today()}
    for i, t in enumerate(sector_tickers):
        params[f"t{i}"] = t

    # placeholders built from validated bind names (:t0, :t1, ...)
    flow_sql = (
        "SELECT id, source_type, actor_name, ticker, amount_usd, "
        "direction, confidence, evidence, flow_date, created_at "
        "FROM dollar_flows "
        "WHERE UPPER(ticker) IN (" + placeholders + ") "
        "AND source_type <> '13f' "
        "AND flow_date >= :cutoff AND flow_date <= :today "
        "ORDER BY flow_date DESC"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(flow_sql), params).fetchall()

    return [
        {
            "id": r[0],
            "source_type": r[1],
            "actor_name": r[2],
            "ticker": r[3],
            "amount_usd": float(r[4]) if r[4] else 0.0,
            "direction": r[5],
            "confidence": r[6],
            "evidence": r[7] if isinstance(r[7], dict) else {},
            "flow_date": r[8].isoformat() if r[8] else None,
            "created_at": r[9].isoformat() if r[9] else None,
            "sector": sector,
        }
        for r in rows
    ]


def get_aggregate_flows(
    engine: Engine, days: int = 30, *, as_of: date | None = None
) -> dict[str, Any]:
    """Compute aggregate net flows per sector and per actor tier.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 30).
        as_of: Optional upper bound on ``flow_date`` (defaults to today's
            UTC date).
            A past date is not answered: the table is a current view (see
            ``CURRENT_VIEW_WITHHELD_REASON``), so the totals come back as
            ``None`` with ``withheld_reason`` set and no row is read.

    Returns:
        Dict with 'by_sector', 'by_source_type', 'by_actor' aggregations
        and 'total_inflow', 'total_outflow', 'net_flow'. 'withheld_reason'
        is ``None`` when the totals are an answer.
    """
    if is_past_as_of(as_of):
        log.warning(
            "get_aggregate_flows: as_of={d} is a past date and dollar_flows is a current view; "
            "no totals returned",
            d=as_of,
        )
        return {
            "by_sector": {},
            "by_source_type": {},
            "by_actor": {},
            "total_inflow": None,
            "total_outflow": None,
            "net_flow": None,
            "days": days,
            "flow_count": None,
            "quarantined_13f": None,
            "as_of": as_of.isoformat(),
            "withheld_reason": CURRENT_VIEW_WITHHELD_REASON,
        }

    _ensure_table(engine)
    # The same clock is_past_as_of uses: on a host whose local date is behind
    # UTC, date.today() would default to a day this function calls past.
    ref_date = as_of or _utc_today()
    cutoff = ref_date - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_type, actor_name, ticker, amount_usd, direction, "
                "confidence, CASE WHEN source_type = '13f' THEN evidence ELSE NULL END "
                "FROM dollar_flows "
                "WHERE flow_date >= :cutoff AND flow_date <= :as_of"
            ),
            {"cutoff": cutoff, "as_of": ref_date},
        ).fetchall()

    sector_flows: dict[str, dict[str, float]] = defaultdict(
        lambda: {"inflow": 0.0, "outflow": 0.0}
    )
    source_type_flows: dict[str, dict[str, float]] = defaultdict(
        lambda: {"inflow": 0.0, "outflow": 0.0}
    )
    actor_flows: dict[str, dict[str, float]] = defaultdict(
        lambda: {"inflow": 0.0, "outflow": 0.0}
    )

    total_inflow = 0.0
    total_outflow = 0.0
    quarantined_13f = 0
    counted_rows = 0

    for r in rows:
        source_type = r[0]
        actor_name = r[1] or "Unknown"
        ticker = r[2]
        amount = float(r[3]) if r[3] else 0.0
        direction = r[4]

        if source_type == "13f":
            # No 13F row is a verified direction (see OBSERVED_13F_DIRECTION).
            quarantined_13f += 1
            continue
        if direction == "quarantined":
            quarantined_13f += 1
            continue

        counted_rows += 1

        # Require a real ticker-to-sector mapping before sector aggregation.
        sector = _get_sector(ticker) if ticker else "Unknown"
        has_real_sector = bool(ticker) and sector != "Unknown"

        if direction == "inflow":
            total_inflow += amount
            if has_real_sector:
                sector_flows[sector]["inflow"] += amount
            source_type_flows[source_type]["inflow"] += amount
            actor_flows[actor_name]["inflow"] += amount
        elif direction == "outflow":
            total_outflow += amount
            if has_real_sector:
                sector_flows[sector]["outflow"] += amount
            source_type_flows[source_type]["outflow"] += amount
            actor_flows[actor_name]["outflow"] += amount
        # Unsigned / neutral rows contribute no directional inflow or outflow.

    if quarantined_13f:
        log.warning(
            "get_aggregate_flows: excluded {n} 13f rows from directional totals (13F is non-directional)",
            n=quarantined_13f,
        )

    # Compute net flows
    def _net(d: dict[str, float]) -> dict[str, float]:
        return {
            "inflow": d["inflow"],
            "outflow": d["outflow"],
            "net": d["inflow"] - d["outflow"],
        }

    # Sort actors by absolute net flow, take top 20
    sorted_actors = sorted(
        actor_flows.items(),
        key=lambda x: abs(x[1]["inflow"] - x[1]["outflow"]),
        reverse=True,
    )[:20]

    return {
        "by_sector": {k: _net(v) for k, v in sector_flows.items()},
        "by_source_type": {k: _net(v) for k, v in source_type_flows.items()},
        "by_actor": {k: _net(v) for k, v in sorted_actors},
        "total_inflow": total_inflow,
        "total_outflow": total_outflow,
        "net_flow": total_inflow - total_outflow,
        "days": days,
        "flow_count": counted_rows,
        "quarantined_13f": quarantined_13f,
        "as_of": ref_date.isoformat(),
        "withheld_reason": None,
    }


def get_biggest_movers(engine: Engine, days: int = 7) -> list[dict]:
    """Return top 10 flows by absolute dollar amount.

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window in days (default 7).

    Returns:
        List of the 10 largest flows by amount_usd.
    """
    _ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, actor_name, ticker, amount_usd, "
                "direction, confidence, evidence, flow_date "
                "FROM dollar_flows "
                "WHERE source_type <> '13f' "
                "AND flow_date >= :cutoff AND flow_date <= :today "
                "ORDER BY amount_usd DESC "
                "LIMIT 10"
            ),
            {"cutoff": cutoff, "today": date.today()},
        ).fetchall()

    return [
        {
            "id": r[0],
            "source_type": r[1],
            "actor_name": r[2],
            "ticker": r[3],
            "amount_usd": float(r[4]) if r[4] else 0.0,
            "direction": r[5],
            "confidence": r[6],
            "evidence": r[7] if isinstance(r[7], dict) else {},
            "flow_date": r[8].isoformat() if r[8] else None,
            "sector": _get_sector(r[3]),
        }
        for r in rows
    ]
