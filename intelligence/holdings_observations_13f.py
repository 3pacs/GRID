"""
GRID Intelligence — 13F holdings observations (non-directional reader).

Serves "which managers' latest 13F filings list this company" to the holdings
graph (``intelligence/deep_graph.py`` layer 9). It reads the labelled 13F
observations that ``ingestion/altdata/institutional_flows.py`` already stores
in ``raw_series`` and writes nothing.

Owner decision 2026-11-23 (Anik, "Yes, non-directional"): 13F filings carry
CUSIPs only, and GRID has no CUSIP-level split, corporate-action or identity
(rekey / merger) coverage. Until it does, a 13F holdings change is a labelled
observation, never a buy, sell or flow. This module keeps to that:

* What it returns is a holding *level*: the filing's reported ``<value>``
  (USD, quarter-end market value, i.e. the report-date reference price) and
  share count. It never returns a change, a sign or a direction, and nothing
  it returns is written anywhere.
* No directional consumer imports it. The ``dollar_flows`` readers,
  ``analysis/flow_aggregator.py``, ``intelligence/sentiment_scorer.py``,
  ``intelligence/signal_convergence_scanner.py``, ``intelligence/wealth_tracker.py``,
  ``intelligence/pocket_lining.py``, the trust scorer and
  ``analysis/money_flow_engine/layer_institutional.py`` read ``dollar_flows``
  or ``signal_sources``; this reader adds rows to neither.

Source rows
-----------
``13F:{cik}:{cusip}:{NEW|INCREASED|DECREASED|CLOSED}`` with ``obs_date`` = the
SEC filing date of the current filing, carrying the #1369 labels:
``measure = observed_filing_holdings_change_at_reference_prices``,
``observation_kind`` (``filing_appearance`` / ``filing_holding_change`` /
``filing_disappearance``, matching the action),
``qualified_for_directional_flow = False``, ``filing_date``, ``report_date``,
``prev_report_date``, ``filing_accession``, ``prev_filing_accession``,
``curr_value_usd`` / ``curr_shares``, ``value_units = 'usd'`` and
``value_multiplier``. Rows without those labels (the pre-#1369 writer stored
no units, report date or labels, and its post-2023 values could be 1000x)
are left out and counted, never repaired.

Point in time
-------------
* A row is visible only once filed (``obs_date`` = ``filing_date`` <= as-of)
  and once received by GRID (``pull_timestamp`` <= the as-of instant; for an
  explicit as-of date, the end of that UTC day).
* Per ``(series_id, obs_date)`` the latest visible vintage wins (the same
  rule as ``store/observations.py`` LATEST_AS_OF), then 13F-HR/A
  restatements replace the filings they restate per filer-quarter
  (``dollar_flows._resolve_13f_amendments``, reused, not copied).
* Per ``(manager, CUSIP)`` the observation with the latest report date wins.
  A CLOSED (``filing_disappearance``) ends the holding: it is never shown as
  held.

Holdings the latest filing did not mention
------------------------------------------
The writer stores changes only: a position whose share count moved by 20 %
or less between two filings leaves no row. A position last observed in an
older filing is shown only when every filing pair from that filing to the
manager's latest filing was compared by the writer (each pair is evidenced
by a row whose ``prev_report_date`` / ``prev_filing_accession`` names the
winning filing of the quarter before). Then no CLOSED was missed, and the
position is shown "held through" the manager's latest report date, valued
at its own (older) report date. Any gap breaks the chain and the position
is left out and counted (``unverified_continuity``). A manager whose latest
observed report date is more than ``MAX_REPORT_AGE_DAYS`` before the as-of
is left out (``stale_manager_filing``).

Ticker mapping
--------------
CUSIP -> ticker reuses ``dollar_flows._resolve_13f_ticker`` (explicit payload
ticker, then ``security_identifiers`` valid on the as-of date, then
``institutional_holdings`` filed on or before it, then the FINRA FTD
``CusipTickerMap``). A CUSIP with no mapping is left out and counted
(``unmapped_cusip``), never guessed. The FTD map is not dated (first symbol
seen wins), so a ticker change after the filing can leave a holding keyed
under the old symbol: it then simply does not match the graph's ticker.

Known limits (also in the PR body)
----------------------------------
* Coverage is change-driven: a position held unchanged since before the
  writer started (or before #1369 labelled rows) is never observed.
  ``institutional_holdings`` (``sec_13f_live``) has full snapshots, but it
  maps tickers at write time with the undated FTD map and upserts in place
  (no vintages); reading it instead is a separate decision.
* Only ``LOOKBACK_DAYS`` of filings are read, so a chain cannot reach a
  position last observed before that.
"""

from __future__ import annotations

import json
import math
import threading
import time
import weakref
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from types import MappingProxyType
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# One mapping and one amendment rule for every 13F reader: reuse dollar_flows'
# helpers instead of copying them (they are module-private there).
from intelligence.dollar_flows import (
    _UNSET,
    _as_date,
    _coerce_ts,
    _extract_13f_vintage_meta,
    _parse_13f_shares,
    _parse_positive_float,
    _resolve_13f_amendments,
    _resolve_13f_ticker,
    _table_exists,
    _ts_utc,
)

# The writer's labels (ingestion/altdata/institutional_flows.py). Repeated as
# literals rather than imported so this read path does not import the puller
# (requests, yfinance, EDGAR config); tests pin them equal.
WRITER_OBSERVATION_MEASURE = "observed_filing_holdings_change_at_reference_prices"
_KIND_FOR_ACTION: Mapping[str, str] = MappingProxyType({
    "NEW": "filing_appearance",
    "INCREASED": "filing_holding_change",
    "DECREASED": "filing_holding_change",
    "CLOSED": "filing_disappearance",
})

# What this reader serves: a holding level, not a change.
HOLDING_MEASURE = "reported_13f_holding_value_at_report_date"
HOLDING_LABEL = "13F filing holdings observation (holding value at report date; not a flow)"
SOURCE_LABEL = "sec_13f_filings_via_institutional_flows"

# A quarter's 13F is due 45 days after quarter end, so at any date the newest
# available filing reports a quarter that ended up to ~135 days earlier. One
# missed quarter of slack on top: ~227 days.
MAX_REPORT_AGE_DAYS = 230
# Filings read for one snapshot: the newest filing plus about four before it.
LOOKBACK_DAYS = 400

_SNAPSHOT_TTL_SECONDS = 600.0

# 13F <value> is whole dollars for filings on/after 2023-01-03, thousands before.
_13F_DOLLAR_CUTOVER_DATE = date(2023, 1, 3)

_READ_SQL = text(
    "SELECT series_id, obs_date, value, raw_payload, pull_timestamp FROM ("
    " SELECT series_id, obs_date, value, raw_payload, pull_timestamp,"
    " ROW_NUMBER() OVER ("
    "  PARTITION BY series_id, obs_date ORDER BY pull_timestamp DESC"
    " ) AS vintage_rank"
    " FROM raw_series"
    " WHERE series_id LIKE :pattern"
    " AND pull_status = 'SUCCESS'"
    " AND obs_date >= :earliest"
    " AND obs_date <= :as_of"
    " AND pull_timestamp <= :as_of_ts"
    ") latest_vintage WHERE vintage_rank = 1"
)


@dataclass(frozen=True)
class HoldingObservation:
    """One manager's position in one CUSIP, as its 13F filings list it."""

    manager_cik: str
    manager_name: str
    cusip: str
    ticker: str
    issuer_name: str
    reported_value_usd: float
    shares: int | float
    value_report_date: date
    held_through_report_date: date
    filing_date: date
    received_at: datetime  # GRID's receipt (raw_series.pull_timestamp), UTC
    filing_accession: str
    observation_kind: str
    carried_forward: bool

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready form, labelled as an observation and never a flow."""
        return {
            "manager_cik": self.manager_cik,
            "manager_name": self.manager_name,
            "cusip": self.cusip,
            "ticker": self.ticker,
            "issuer_name": self.issuer_name,
            "reported_value_usd": self.reported_value_usd,
            "shares": self.shares,
            "value_report_date": self.value_report_date.isoformat(),
            "held_through_report_date": self.held_through_report_date.isoformat(),
            "filing_date": self.filing_date.isoformat(),
            "received_at": self.received_at.isoformat(),
            "filing_accession": self.filing_accession,
            "observation_kind": self.observation_kind,
            "carried_forward": self.carried_forward,
            "measure": HOLDING_MEASURE,
            "directional": False,
        }


@dataclass(frozen=True)
class ManagerHoldings:
    """A manager's mapped holdings, restricted to the tickers asked for."""

    manager_cik: str
    manager_name: str
    held_through_report_date: date
    holdings_by_ticker: Mapping[str, tuple[HoldingObservation, ...]]


@dataclass(frozen=True)
class HoldingsSnapshot:
    """Every mapped 13F holding visible at ``as_of`` / ``as_of_ts``."""

    as_of: date
    as_of_ts: datetime
    holdings: tuple[HoldingObservation, ...]
    counts: Mapping[str, int] = field(default_factory=dict)
    read_ok: bool = True

    def holders_of(self, tickers: Iterable[str]) -> dict[str, ManagerHoldings]:
        """Managers (by CIK) holding any of ``tickers``, with only those tickers."""
        wanted = {t.strip().upper() for t in tickers if t and t.strip()}
        grouped: dict[str, dict[str, list[HoldingObservation]]] = defaultdict(lambda: defaultdict(list))
        first: dict[str, HoldingObservation] = {}
        for h in self.holdings:
            if h.ticker not in wanted:
                continue
            grouped[h.manager_cik][h.ticker].append(h)
            first.setdefault(h.manager_cik, h)
        out: dict[str, ManagerHoldings] = {}
        for cik, by_ticker in grouped.items():
            ref = first[cik]
            out[cik] = ManagerHoldings(
                manager_cik=cik,
                manager_name=ref.manager_name,
                held_through_report_date=ref.held_through_report_date,
                holdings_by_ticker=MappingProxyType({
                    t: tuple(sorted(obs, key=lambda o: o.cusip)) for t, obs in sorted(by_ticker.items())
                }),
            )
        return out

    def meta(self) -> dict[str, Any]:
        """Provenance and exclusion counts for display next to the holdings."""
        return {
            "source": SOURCE_LABEL,
            "measure": HOLDING_MEASURE,
            "label": HOLDING_LABEL,
            "directional": False,
            "as_of": self.as_of.isoformat(),
            "as_of_ts": self.as_of_ts.isoformat(),
            "read_ok": self.read_ok,
            "held_positions": len(self.holdings),
            "excluded": dict(sorted(self.counts.items())),
        }


def _payload_dict(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _strict_date(raw: Any) -> date | None:
    """A payload date: ``YYYY-MM-DD`` string or date, else ``None``."""
    if raw is None or isinstance(raw, bool):
        return None
    return _as_date(raw)


def _label_check(action: str, cik: str, cusip: str, obs_date: date, payload: dict[str, Any]) -> str | None:
    """Reason a row is not a #1369-labelled observation, or ``None``."""
    if payload.get("measure") != WRITER_OBSERVATION_MEASURE:
        return "not_observation_labelled"
    if payload.get("observation_kind") != _KIND_FOR_ACTION[action]:
        return "not_observation_labelled"
    if payload.get("qualified_for_directional_flow") is not False:
        return "not_observation_labelled"
    if str(payload.get("manager_cik") or "").strip() != cik:
        return "identity_mismatch"
    if str(payload.get("cusip") or "").strip().upper() != cusip:
        return "identity_mismatch"
    if not str(payload.get("filing_accession") or "").strip():
        return "invalid_filing_clock"
    filing_date = _strict_date(payload.get("filing_date"))
    report_date = _strict_date(payload.get("report_date"))
    if filing_date is None or filing_date != obs_date:
        return "invalid_filing_clock"
    if report_date is None or report_date > filing_date:
        return "invalid_filing_clock"
    return None


def _held_value(payload: dict[str, Any], filing_date: date) -> tuple[float, int | float] | None:
    """``(reported_value_usd, shares)`` of a held position, or ``None``."""
    if str(payload.get("value_units") or "").strip().lower() != "usd":
        return None
    vm = payload.get("value_multiplier")
    if isinstance(vm, bool) or vm not in (1, 1000):
        return None
    if vm != (1 if filing_date >= _13F_DOLLAR_CUTOVER_DATE else 1000):
        return None
    share_type = str(payload.get("share_type") or "").strip().upper()
    if share_type and share_type != "SH":
        return None
    value = _parse_positive_float(payload.get("curr_value_usd"))
    shares = _parse_13f_shares(payload.get("curr_shares"))
    if value is None or not math.isfinite(value) or shares is None or shares <= 0:
        return None
    return value, shares


def _verified_chain(rows_at: dict[date, list[dict[str, Any]]]) -> set[date]:
    """Report dates linked to the manager's latest one by compared filing pairs.

    The latest report date is always in the chain. An older report date P is
    added when the rows of the next filing up the chain all name P (and the
    accession of P's winning filing) as the filing they were compared
    against. Ambiguity or a gap stops the walk.
    """
    if not rows_at:
        return set()
    current = max(rows_at)
    chain = {current}
    while True:
        links = {
            (
                _strict_date(r["payload"].get("prev_report_date")),
                str(r["payload"].get("prev_filing_accession") or "").strip(),
            )
            for r in rows_at[current]
        }
        if len(links) != 1:
            return chain
        prev_rd, prev_acc = next(iter(links))
        if prev_rd is None or not prev_acc or prev_rd >= current or prev_rd not in rows_at:
            return chain
        prev_accs = {r["meta"]["filing_accession"] for r in rows_at[prev_rd]}
        if prev_accs != {prev_acc}:
            return chain
        chain.add(prev_rd)
        current = prev_rd


def _position_rank(c: dict[str, Any]) -> tuple[date, date, datetime]:
    """Latest report date wins, then the later filing, then the later receipt."""
    return (c["meta"]["report_date"], c["meta"]["filing_date"], _ts_utc(c["pull_ts"]))


def _bump(counts: dict[str, int], key: str, n: int = 1) -> None:
    counts[key] = counts.get(key, 0) + n


def _as_of_instant(as_of: date | None, as_of_ts: datetime | None) -> tuple[date, datetime]:
    """``(filing-date bound, receipt instant)``; never later than now."""
    now = datetime.now(timezone.utc)
    if as_of_ts is not None:
        instant = min(_ts_utc(as_of_ts), now)
    elif as_of is not None:
        instant = min(datetime.combine(as_of, dtime.max, tzinfo=timezone.utc), now)
    else:
        instant = now
    day = instant.date() if as_of is None else min(as_of, instant.date())
    return day, instant


def read_13f_holdings_snapshot(
    engine: Engine,
    *,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
    cusip_map: Any = _UNSET,
    lookback_days: int = LOOKBACK_DAYS,
    use_cache: bool = False,
) -> HoldingsSnapshot:
    """Read every mapped 13F holding visible at the as-of (see module docstring).

    Parameters:
        engine: SQLAlchemy engine.
        as_of: Graph date. Rows filed after it, or received by GRID after the
            end of that UTC day, are invisible. Defaults to now.
        as_of_ts: Exact receipt cutoff (overrides the end-of-day default).
        cusip_map: CUSIP -> ticker fallback passed to
            ``dollar_flows._resolve_13f_ticker`` (``None`` disables the FTD
            map; default loads it lazily).
        lookback_days: Filing-date window read before the as-of.
        use_cache: Reuse a snapshot of the same engine and as-of for
            ``_SNAPSHOT_TTL_SECONDS`` (the graph drills many tickers per call).

    Returns:
        A ``HoldingsSnapshot``. A failed read returns an empty snapshot with
        ``read_ok=False``: no holding is shown rather than a partial one.
    """
    day, instant = _as_of_instant(as_of, as_of_ts)
    cache_key = (day, None if (as_of is None and as_of_ts is None) else instant, lookback_days)
    if use_cache and cusip_map is _UNSET:
        cached = _cache_get(engine, cache_key)
        if cached is not None:
            return cached

    counts: dict[str, int] = {}
    try:
        snapshot = _read(engine, day, instant, cusip_map, lookback_days, counts)
    except Exception as exc:
        log.warning("13F holdings observation read failed: {e}", e=str(exc))
        return HoldingsSnapshot(as_of=day, as_of_ts=instant, holdings=(), counts={"read_failed": 1},
                                read_ok=False)

    if use_cache and cusip_map is _UNSET:
        _cache_put(engine, cache_key, snapshot)
    return snapshot


def _read(
    engine: Engine,
    day: date,
    instant: datetime,
    cusip_map: Any,
    lookback_days: int,
    counts: dict[str, int],
) -> HoldingsSnapshot:
    earliest = day - timedelta(days=max(int(lookback_days), 1))
    with engine.connect() as conn:
        rows = conn.execute(
            _READ_SQL,
            {"pattern": "13F:%", "earliest": earliest, "as_of": day, "as_of_ts": instant},
        ).fetchall()

        # Stage 1: one labelled, visible observation per (series_id, obs_date).
        candidates: list[dict[str, Any]] = []
        for series_id, raw_obs_date, _value, raw_payload, raw_pull_ts in rows:
            _bump(counts, "rows_read")
            parts = str(series_id).split(":")
            if len(parts) != 4 or parts[0] != "13F" or parts[3].strip().upper() not in _KIND_FOR_ACTION:
                _bump(counts, "not_position_series")
                continue
            cik = parts[1].strip()
            cusip = parts[2].strip().upper()
            action = parts[3].strip().upper()
            obs_date = _as_date(raw_obs_date)
            pull_ts = _coerce_ts(raw_pull_ts)
            if not cik or not cusip or obs_date is None:
                _bump(counts, "not_position_series")
                continue
            if obs_date > day or pull_ts is None or _ts_utc(pull_ts) > instant:
                _bump(counts, "not_yet_received")
                continue
            payload = _payload_dict(raw_payload)
            if payload is None:
                _bump(counts, "not_observation_labelled")
                continue
            reason = _label_check(action, cik, cusip, obs_date, payload)
            if reason is not None:
                _bump(counts, reason)
                continue
            candidates.append({
                "series_id": series_id,
                "cik": cik,
                "cusip": cusip,
                "action": action,
                "obs_date": obs_date,
                "payload": payload,
                "pull_ts": pull_ts,
                "meta": _extract_13f_vintage_meta(payload, obs_date),
            })

        # Stage 2: restatements replace the filings they restate (shared rule).
        survivors, superseded = _resolve_13f_amendments(candidates)
        if superseded:
            _bump(counts, "superseded_vintages", superseded)

        by_manager: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for c in survivors:
            by_manager[c["cik"]].append(c)

        has_sec_ids = _table_exists(conn, "security_identifiers")
        has_inst_holdings = _table_exists(conn, "institutional_holdings")
        ticker_cache: dict[tuple[str, date], str | None] = {}
        stale_cutoff = day - timedelta(days=MAX_REPORT_AGE_DAYS)
        holdings: list[HoldingObservation] = []

        for cik in sorted(by_manager):
            items = by_manager[cik]
            rows_at: dict[date, list[dict[str, Any]]] = defaultdict(list)
            for c in items:
                rows_at[c["meta"]["report_date"]].append(c)
            latest_rd = max(rows_at)
            latest_by_cusip: dict[str, dict[str, Any]] = {}
            for c in items:
                prev = latest_by_cusip.get(c["cusip"])
                if prev is None or _position_rank(c) > _position_rank(prev):
                    latest_by_cusip[c["cusip"]] = c

            if latest_rd < stale_cutoff:
                _bump(counts, "stale_manager_filing", len(latest_by_cusip))
                continue

            chain = _verified_chain(rows_at)
            manager_name = str(
                next(
                    (r["payload"].get("manager_name") for r in rows_at[latest_rd] if r["payload"].get("manager_name")),
                    cik,
                )
            )

            for cusip in sorted(latest_by_cusip):
                c = latest_by_cusip[cusip]
                rd = c["meta"]["report_date"]
                if c["action"] == "CLOSED":
                    _bump(counts, "closed")
                    continue
                if rd not in chain:
                    _bump(counts, "unverified_continuity")
                    continue
                held = _held_value(c["payload"], c["obs_date"])
                if held is None:
                    _bump(counts, "invalid_holding_value")
                    continue
                ticker = _resolve_13f_ticker(
                    conn,
                    cusip,
                    c["payload"],
                    day,
                    has_sec_ids=has_sec_ids,
                    has_inst_holdings=has_inst_holdings,
                    cusip_map=cusip_map,
                    cache=ticker_cache,
                )
                if ticker is None:
                    _bump(counts, "unmapped_cusip")
                    continue
                value, shares = held
                holdings.append(HoldingObservation(
                    manager_cik=cik,
                    manager_name=manager_name,
                    cusip=cusip,
                    ticker=ticker,
                    issuer_name=str(c["payload"].get("issuer_name") or ""),
                    reported_value_usd=value,
                    shares=shares,
                    value_report_date=rd,
                    held_through_report_date=latest_rd,
                    filing_date=c["obs_date"],
                    received_at=_ts_utc(c["pull_ts"]),
                    filing_accession=c["meta"]["filing_accession"],
                    observation_kind=c["payload"]["observation_kind"],
                    carried_forward=rd != latest_rd,
                ))

    return HoldingsSnapshot(
        as_of=day,
        as_of_ts=instant,
        holdings=tuple(holdings),
        counts=MappingProxyType(dict(counts)),
    )


# ── Snapshot cache (per engine, short TTL) ────────────────────────────────

_CACHE_LOCK = threading.Lock()
_SNAPSHOT_CACHE: weakref.WeakKeyDictionary[Engine, dict[Any, tuple[float, HoldingsSnapshot]]] = (
    weakref.WeakKeyDictionary()
)


def _cache_get(engine: Engine, key: Any) -> HoldingsSnapshot | None:
    with _CACHE_LOCK:
        try:
            entries = _SNAPSHOT_CACHE.get(engine)
        except TypeError:
            return None
        if not entries or key not in entries:
            return None
        expires, snapshot = entries[key]
        if time.monotonic() >= expires:
            del entries[key]
            return None
        return snapshot


def _cache_put(engine: Engine, key: Any, snapshot: HoldingsSnapshot) -> None:
    with _CACHE_LOCK:
        try:
            entries = _SNAPSHOT_CACHE.setdefault(engine, {})
        except TypeError:
            return
        entries[key] = (time.monotonic() + _SNAPSHOT_TTL_SECONDS, snapshot)


def clear_snapshot_cache() -> None:
    """Drop every cached snapshot (tests; after a manual backfill)."""
    with _CACHE_LOCK:
        _SNAPSHOT_CACHE.clear()
