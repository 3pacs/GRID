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
Two series, both written by the 13F writer in one transaction per filing pair:

* ``13F:{cik}:{cusip}:{NEW|INCREASED|DECREASED|CLOSED}`` (``obs_date`` = the
  current filing's SEC filing date) with the #1369 labels:
  ``measure = observed_filing_holdings_change_at_reference_prices``,
  ``observation_kind`` matching the action, ``qualified_for_directional_flow
  = False``, ``filing_date``, ``report_date``, ``filing_accession``,
  ``prev_filing_accession``, ``curr_value_usd`` / ``curr_shares``,
  ``value_units = 'usd'`` and ``value_multiplier``.
* ``13F-FILING:{cik}:{accession}``, the filing comparison receipt
  (``measure = 13f_filing_comparison_receipt``): which accession was read for
  the filer-quarter, which filing it was compared against, and whether both
  sides were complete (the cover page's ``tableEntryTotal`` matched the
  information table, an amendment was a verified RESTATEMENT). It is written
  even when the comparison found no change, so it is the authority on which
  filing stands for each quarter. ``withheld_incomplete`` receipts record a
  pair the writer refused to compare.

A change row counts only when its filing has a ``compared`` receipt, that
filing is the quarter's winning filing, and the row came from the comparison
the receipt records (same prior accession). Rows without receipts (the
pre-receipt writer), without #1369 labels (the pre-#1369 writer stored no
units, report date or labels; its post-2023 values could be 1000x), or
claiming direction are left out and counted, never repaired.

Point in time
-------------
* A row or receipt is visible only once filed (``obs_date`` = filing date <=
  as-of) and once received by GRID (``pull_timestamp`` <= the as-of instant;
  for an explicit as-of date, the end of that UTC day).
* Per ``(series_id, obs_date)`` the latest visible vintage wins (the same
  rule as ``store/observations.py`` LATEST_AS_OF). Receipts and change rows
  come from one statement, so one PostgreSQL snapshot: a writer committing
  between two reads cannot pair old receipts with new change rows.
* Per filer-quarter, the winning filing is the latest by (filing date,
  amendment, accession) among the filings the receipts name, current or
  prior side: the writer's own selection rule. A restatement therefore
  replaces the original even when it changed nothing.
* Per ``(manager, CUSIP)`` the observation with the latest report date wins.
  A CLOSED (``filing_disappearance``) ends the holding: it is never shown as
  held. A NOT_SHOWN (``absent_under_declared_omissions``) is not shown as
  held either, and is counted apart (``not_shown_omissions_declared``): the
  position was absent from a filing that declares confidential omissions,
  so it may be sold or merely omitted.
* A manager whose latest filing has no ``compared`` receipt (unreadable,
  unclassified amendment, additive supplement, or a fetch that failed once
  the accession was known) shows nothing
  (``managers_latest_filing_unverified``): its last verified filing cannot
  be claimed as current. So does a manager with an unvalidatable receipt
  filed on or after its latest valid one (``managers_receipt_unreadable``). A manager whose latest report
  date is more than ``MAX_REPORT_AGE_DAYS`` old shows nothing
  (``managers_stale_filing``).

Holdings the latest filing did not mention
------------------------------------------
The writer stores changes only: a position whose share count moved by 20 %
or less between two filings leaves no row. A position last observed in an
older filing is shown only when the receipts chain every filing from that
one to the manager's latest: each receipt was ``compared`` and names, as its
prior side, the winning filing of the quarter before. Then no CLOSED was
missed, and the position is shown "held through" the manager's latest report
date, valued at its own (older) report date. Any gap breaks the chain and the
position is left out and counted (``unverified_continuity``).

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
  writer started writing receipts is never observed.
  ``institutional_holdings`` (``sec_13f_live``) has full snapshots, but it
  maps tickers at write time with the undated FTD map and upserts in place
  (no vintages); reading it instead is a separate decision.
* Only ``LOOKBACK_DAYS`` of filings are read, so a chain cannot reach a
  position last observed before that.
* A cover page with confidential treatment (``isConfidentialOmitted``) still
  counts as complete for the entries it lists. A prior position it does not
  list is written as NOT_SHOWN, not CLOSED (rows written before 2026-11-27
  said CLOSED; a later NOT_SHOWN of the same filing pair outranks them by
  receipt time). Either way the position is hidden rather than invented.
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

# One CUSIP mapping for every 13F reader: reuse dollar_flows' helpers instead
# of copying them (they are module-private there).
from intelligence.dollar_flows import (
    _UNSET,
    _as_date,
    _coerce_ts,
    _parse_13f_shares,
    _parse_positive_float,
    _resolve_13f_ticker,
    _table_exists,
    _ts_utc,
)

# The writer's labels (ingestion/altdata/institutional_flows.py). Repeated as
# literals rather than imported so this read path does not import the puller
# (requests, yfinance, EDGAR config); tests pin them equal.
WRITER_OBSERVATION_MEASURE = "observed_filing_holdings_change_at_reference_prices"
WRITER_RECEIPT_MEASURE = "13f_filing_comparison_receipt"
WRITER_RECEIPT_PREFIX = "13F-FILING"
WRITER_COMPLETE_SNAPSHOTS: frozenset[str] = frozenset({"complete", "verified_empty"})
_KIND_FOR_ACTION: Mapping[str, str] = MappingProxyType({
    "NEW": "filing_appearance",
    "INCREASED": "filing_holding_change",
    "DECREASED": "filing_holding_change",
    "CLOSED": "filing_disappearance",
    # Absent from a current filing whose cover page declares confidential
    # omissions: neither a disappearance nor a holding (see the writer's
    # NOT_SHOWN_ACTION).
    "NOT_SHOWN": "absent_under_declared_omissions",
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

# One statement for receipts and change rows: under PostgreSQL READ COMMITTED
# every statement takes its own snapshot, so two reads could pair a receipt
# from before a writer's commit with change rows from after it.
_READ_SQL = text(
    "SELECT series_id, obs_date, value, raw_payload, pull_timestamp FROM ("
    " SELECT series_id, obs_date, value, raw_payload, pull_timestamp,"
    " ROW_NUMBER() OVER ("
    "  PARTITION BY series_id, obs_date ORDER BY pull_timestamp DESC"
    " ) AS vintage_rank"
    " FROM raw_series"
    " WHERE (series_id LIKE :change_pattern OR series_id LIKE :receipt_pattern)"
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


_Rank = tuple[date, int, str]


def _filing_rank(filing_date: date, is_amendment: Any, accession: str) -> _Rank:
    """The writer's own selection rule (``_select_latest_filings_by_report_date``)."""
    return (filing_date, 1 if is_amendment else 0, accession)


def _receipt_check(
    cik: str, accession: str, obs_date: date, received_at: datetime, payload: dict[str, Any]
) -> dict[str, Any] | None:
    """A validated filing comparison receipt, or ``None``.

    The prior filing may have been filed after the current one (a later
    restatement of the prior quarter), but not after GRID received this
    receipt vintage.
    """
    if payload.get("measure") != WRITER_RECEIPT_MEASURE:
        return None
    if payload.get("qualified_for_directional_flow") is not False:
        return None
    if str(payload.get("manager_cik") or "").strip() != cik:
        return None
    if str(payload.get("filing_accession") or "").strip() != accession:
        return None
    filing_date = _strict_date(payload.get("filing_date"))
    report_date = _strict_date(payload.get("report_date"))
    prev_filing_date = _strict_date(payload.get("prev_filing_date"))
    prev_report_date = _strict_date(payload.get("prev_report_date"))
    prev_acc = str(payload.get("prev_filing_accession") or "").strip()
    if filing_date is None or filing_date != obs_date or report_date is None or report_date > filing_date:
        return None
    if not prev_acc or prev_acc == accession or prev_filing_date is None or prev_report_date is None:
        return None
    if prev_filing_date > _ts_utc(received_at).date() or prev_report_date >= report_date:
        return None
    status = payload.get("comparison_status")
    if status not in ("compared", "withheld_incomplete"):
        return None
    curr_snap = payload.get("curr_snapshot") if isinstance(payload.get("curr_snapshot"), dict) else {}
    prev_snap = payload.get("prev_snapshot") if isinstance(payload.get("prev_snapshot"), dict) else {}
    compared = (
        status == "compared"
        and curr_snap.get("status") in WRITER_COMPLETE_SNAPSHOTS
        and prev_snap.get("status") in WRITER_COMPLETE_SNAPSHOTS
        and str(curr_snap.get("accession") or "") == accession
        and str(prev_snap.get("accession") or "") == prev_acc
    )
    if status == "compared" and not compared:
        return None
    return {
        "accession": accession,
        "filing_date": filing_date,
        "report_date": report_date,
        "rank": _filing_rank(filing_date, payload.get("is_amendment"), accession),
        "prev_accession": prev_acc,
        "prev_report_date": prev_report_date,
        "prev_rank": _filing_rank(prev_filing_date, payload.get("prev_is_amendment"), prev_acc),
        "compared": compared,
    }


def _verified_chain(
    cik: str,
    latest_rd: date,
    authority: dict[tuple[str, date], _Rank],
    receipts: dict[tuple[str, str], dict[str, Any]],
) -> set[date]:
    """Report dates linked to the manager's latest one by compared receipts.

    The latest report date is in the chain when its winning filing has a
    ``compared`` receipt. An older report date P joins when that receipt's
    prior side is P's winning filing and P's filing has its own ``compared``
    receipt; the walk continues from P. Anything else stops it.
    """
    chain: set[date] = set()
    current = latest_rd
    while True:
        rec = receipts.get((cik, authority[(cik, current)][2]))
        if rec is None or not rec["compared"]:
            return chain
        chain.add(current)
        prev_rd, prev_acc = rec["prev_report_date"], rec["prev_accession"]
        if prev_rd >= current or authority.get((cik, prev_rd), (None, None, None))[2] != prev_acc:
            return chain
        current = prev_rd


def _position_rank(c: dict[str, Any]) -> tuple[date, datetime]:
    """Latest report date wins, then the later receipt."""
    return (c["report_date"], _ts_utc(c["pull_ts"]))


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


def _latest_vintages(
    conn: Any, earliest: date, day: date, instant: datetime
) -> tuple[list[Any], list[Any]]:
    """``(receipt_rows, change_rows)`` read in one statement (one snapshot)."""
    rows = conn.execute(
        _READ_SQL,
        {
            "change_pattern": "13F:%",
            "receipt_pattern": f"{WRITER_RECEIPT_PREFIX}:%",
            "earliest": earliest,
            "as_of": day,
            "as_of_ts": instant,
        },
    ).fetchall()
    receipt_prefix = f"{WRITER_RECEIPT_PREFIX}:"
    receipts = [r for r in rows if str(r[0]).startswith(receipt_prefix)]
    changes = [r for r in rows if str(r[0]).startswith("13F:")]
    return receipts, changes


def _visible(raw_obs_date: Any, raw_pull_ts: Any, day: date, instant: datetime) -> tuple[date, datetime] | None:
    obs_date = _as_date(raw_obs_date)
    pull_ts = _coerce_ts(raw_pull_ts)
    if obs_date is None or pull_ts is None or obs_date > day or _ts_utc(pull_ts) > instant:
        return None
    return obs_date, pull_ts


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
        receipt_rows, change_rows = _latest_vintages(conn, earliest, day, instant)
        # Stage 1: filing comparison receipts -> per-quarter authority.
        receipts: dict[tuple[str, str], dict[str, Any]] = {}
        # Latest filing date of a receipt that could not be validated, per
        # manager: a newer unreadable receipt must not leave an older valid
        # one standing as the latest.
        unreadable_since: dict[str, date] = {}
        for series_id, raw_obs_date, _value, raw_payload, raw_pull_ts in receipt_rows:
            _bump(counts, "receipts_read")
            parts = str(series_id).split(":")
            seen = _visible(raw_obs_date, raw_pull_ts, day, instant)
            payload = _payload_dict(raw_payload)
            rec = None
            if len(parts) == 3 and parts[0] == WRITER_RECEIPT_PREFIX and seen is not None and payload is not None:
                rec = _receipt_check(parts[1].strip(), parts[2].strip(), seen[0], seen[1], payload)
            if rec is None:
                _bump(counts, "invalid_filing_receipt")
                if len(parts) == 3 and seen is not None:
                    cik = parts[1].strip()
                    unreadable_since[cik] = max(seen[0], unreadable_since.get(cik, date.min))
                continue
            receipts[(parts[1].strip(), rec["accession"])] = rec

        authority: dict[tuple[str, date], _Rank] = {}
        for (cik, _acc), rec in receipts.items():
            for rd, rank in ((rec["report_date"], rec["rank"]), (rec["prev_report_date"], rec["prev_rank"])):
                if (cik, rd) not in authority or rank > authority[(cik, rd)]:
                    authority[(cik, rd)] = rank

        # Stage 2: labelled change rows from a verified, winning comparison.
        by_manager: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for series_id, raw_obs_date, _value, raw_payload, raw_pull_ts in change_rows:
            _bump(counts, "rows_read")
            parts = str(series_id).split(":")
            if len(parts) != 4 or parts[0] != "13F" or parts[3].strip().upper() not in _KIND_FOR_ACTION:
                _bump(counts, "not_position_series")
                continue
            cik, cusip, action = parts[1].strip(), parts[2].strip().upper(), parts[3].strip().upper()
            seen = _visible(raw_obs_date, raw_pull_ts, day, instant)
            if not cik or not cusip or seen is None:
                _bump(counts, "not_position_series")
                continue
            obs_date, pull_ts = seen
            payload = _payload_dict(raw_payload)
            if payload is None:
                _bump(counts, "not_observation_labelled")
                continue
            reason = _label_check(action, cik, cusip, obs_date, payload)
            if reason is not None:
                _bump(counts, reason)
                continue
            accession = str(payload["filing_accession"]).strip()
            report_date = _strict_date(payload.get("report_date"))
            rec = receipts.get((cik, accession))
            if rec is None or not rec["compared"]:
                _bump(counts, "no_filing_receipt")
                continue
            if (
                rec["report_date"] != report_date
                or str(payload.get("prev_filing_accession") or "").strip() != rec["prev_accession"]
            ):
                _bump(counts, "stale_comparison")
                continue
            if authority[(cik, report_date)][2] != accession:
                _bump(counts, "superseded_filing")
                continue
            row = {"cusip": cusip, "action": action, "obs_date": obs_date, "pull_ts": pull_ts,
                   "payload": payload, "report_date": report_date, "accession": accession}
            prev = by_manager[cik].get(cusip)
            if prev is None or _position_rank(row) > _position_rank(prev):
                by_manager[cik][cusip] = row

        has_sec_ids = _table_exists(conn, "security_identifiers")
        has_inst_holdings = _table_exists(conn, "institutional_holdings")
        ticker_cache: dict[tuple[str, date], str | None] = {}
        stale_cutoff = day - timedelta(days=MAX_REPORT_AGE_DAYS)
        holdings: list[HoldingObservation] = []

        for cik in sorted({c for c, _rd in authority}):
            latest_rd = max(rd for c, rd in authority if c == cik)
            latest = receipts.get((cik, authority[(cik, latest_rd)][2]))
            if latest is None or not latest["compared"]:
                _bump(counts, "managers_latest_filing_unverified")
                continue
            if cik in unreadable_since and unreadable_since[cik] >= latest["filing_date"]:
                _bump(counts, "managers_receipt_unreadable")
                continue
            if latest_rd < stale_cutoff:
                _bump(counts, "managers_stale_filing")
                continue
            chain = _verified_chain(cik, latest_rd, authority, receipts)
            positions = by_manager.get(cik, {})
            manager_name = next(
                (str(r["payload"]["manager_name"]) for r in positions.values()
                 if r["report_date"] == latest_rd and r["payload"].get("manager_name")),
                next((str(r["payload"]["manager_name"]) for r in positions.values()
                      if r["payload"].get("manager_name")), cik),
            )

            for cusip in sorted(positions):
                c = positions[cusip]
                rd = c["report_date"]
                if c["action"] == "CLOSED":
                    _bump(counts, "closed")
                    continue
                if c["action"] == "NOT_SHOWN":
                    # Sold or confidential: unknown. Not shown as held, and
                    # counted apart from a filing disappearance.
                    _bump(counts, "not_shown_omissions_declared")
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
                    filing_accession=c["accession"],
                    observation_kind=c["payload"]["observation_kind"],
                    carried_forward=rd != latest_rd,
                ))

        # Rows of managers with no receipt at all were counted above
        # (no_filing_receipt); nothing of theirs is shown.

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


def _cache_get(engine: Engine, cache_slot: Any) -> HoldingsSnapshot | None:
    with _CACHE_LOCK:
        try:
            entries = _SNAPSHOT_CACHE.get(engine)
        except TypeError:
            return None
        if not entries or cache_slot not in entries:
            return None
        expires, snapshot = entries[cache_slot]
        if time.monotonic() >= expires:
            del entries[cache_slot]
            return None
        return snapshot


def _cache_put(engine: Engine, cache_slot: Any, snapshot: HoldingsSnapshot) -> None:
    with _CACHE_LOCK:
        try:
            entries = _SNAPSHOT_CACHE.setdefault(engine, {})
        except TypeError:
            return
        entries[cache_slot] = (time.monotonic() + _SNAPSHOT_TTL_SECONDS, snapshot)


def clear_snapshot_cache() -> None:
    """Drop every cached snapshot (tests; after a manual backfill)."""
    with _CACHE_LOCK:
        _SNAPSHOT_CACHE.clear()
