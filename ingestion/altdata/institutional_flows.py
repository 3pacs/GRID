"""
GRID institutional money flow data ingestion module.

Tracks where big money is going via three complementary data sources:

Source 1: ETF Flow Proxy
    Uses daily $ volume (volume x close price) as a proxy for flows in major
    ETFs across asset classes: SPY, QQQ, IWM, TLT, HYG, GLD, EEM, XLK, XLF,
    XLE, XLV. Computes rolling 5-day and 20-day flow sums plus acceleration.

Source 2: SEC 13F Filings
    Parses quarterly 13F-HR filings from SEC EDGAR for the top 50 institutional
    managers by AUM. Tracks new positions, closed positions, and significant
    increases/decreases. Stored as labelled, non-directional holdings
    observations (nothing goes to signal_sources).

Source 3: CFTC Commitment of Traders
    Already implemented in ``ingestion/altdata/cftc_cot.py`` — this module
    does NOT duplicate that puller. We reference it for completeness and
    provide a convenience wrapper that maps COT series IDs to the
    ``COT:`` prefix format expected by the money flow visualization layer.

Data stored:
    ETF_FLOW:{ticker}:5d      — 5-day rolling $ volume flow
    ETF_FLOW:{ticker}:20d     — 20-day rolling $ volume flow
    ETF_FLOW:{ticker}:accel   — flow acceleration (5d change of 5d flow)
    13F:{manager_cik}:{cusip}:{action} — quarterly position changes
    13F-FILING:{manager_cik}:{accession} — one receipt per filing pair read:
        which accession was compared against which, and whether both sides
        were complete (cover-page entry count matched); see _fetch_13f_filing
"""

from __future__ import annotations

import json

import logging
import math
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from ingestion.altdata.verified_13f_filers import VERIFIED_FILERS
from ingestion.base import BasePuller, _stable_lock_key, retry_on_failure

# yfinance logs expected missing/delisted ticker misses at ERROR internally.
# This module treats those as upstream data absence, not Hermes/app failures.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ── ETF flow proxy configuration ──────────────────────────────────────────────

ETF_FLOW_TICKERS: list[str] = [
    "SPY",   # S&P 500
    "QQQ",   # Nasdaq-100
    "IWM",   # Russell 2000
    "TLT",   # 20+ Year Treasury
    "HYG",   # High Yield Corporate Bond
    "GLD",   # Gold
    "EEM",   # Emerging Markets
    "XLK",   # Technology Select
    "XLF",   # Financial Select
    "XLE",   # Energy Select
    "XLV",   # Healthcare Select
]

# Rolling windows for flow computation
_FLOW_WINDOW_5D: int = 5
_FLOW_WINDOW_20D: int = 20

# Rate limit for yfinance calls
_YF_RATE_DELAY: float = 0.5

# ── SEC 13F configuration ─────────────────────────────────────────────────────

# EDGAR full-text search for 13F-HR filings
_EDGAR_SEARCH_URL: str = (
    "https://efts.sec.gov/LATEST/search-index"
)
_EDGAR_FILING_URL: str = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
)
_EDGAR_13F_XML_BASE: str = "https://www.sec.gov/Archives/edgar/data"

# SEC requires a User-Agent header with contact info
_EDGAR_HEADERS: dict[str, str] = {
    "User-Agent": "GRID-Research research@grid.local",
    "Accept": "application/json",
}

# Top institutional filers by AUM (CIK numbers).
#
# GD0 §1.3 / §6 item 4 (owner decision, adopted 2026-09-28; corrected
# 2026-09-28 -- see ``ingestion/altdata/verified_13f_filers.py``'s module
# docstring): this module used to carry its own hardcoded ``CIK -> fund
# name`` dict that disagreed with both ``ingestion/edgar.py`` and
# ``ingestion/altdata/sec_13f_live.py`` on the *same* CIK for different
# funds (e.g. CIK 1167483 was "Elliott Management" here, "Eton Park
# Capital" in edgar.py, and "Tiger Global Management" -- SEC's verified
# registrant name -- in sec_13f_live.py). This is now a derived view over
# the single verified map, not a second copy that can drift out of sync.
TOP_13F_FILERS: dict[str, str] = {f.cik: f.display_name for f in VERIFIED_FILERS}

# Rate limit between EDGAR API calls (SEC is strict: 10 req/sec max)
_EDGAR_RATE_DELAY: float = 0.15

# Minimum position change threshold to flag as INCREASED/DECREASED (20%)
_POSITION_CHANGE_THRESHOLD: float = 0.20

# A same-CUSIP split or reverse split between two quarter-ends changes the
# reported share count with no trade (100 x $100 -> 200 x $50), and a price
# move can mask it (2-for-1 plus +70% reads $100 -> $85). 13F rows carry
# CUSIPs, not tickers, and GRID has no CUSIP-level corporate-action coverage,
# so a continuing position's share change is never a verified direction: it
# is emitted as an estimated holdings change with
# qualified_for_directional_flow=False. NEW and CLOSED positions are
# split-invariant (whole position value at one quarter-end) and stay qualified.
CONTINUING_POSITION_UNQUALIFIED_REASON = "no_corporate_action_coverage"
# A NEW or CLOSED CUSIP is not a verified trade either: a CUSIP rekey, merger or
# exchange can show as one CUSIP disappearing and another appearing with no
# trade, and GRID has no CUSIP identity/filing coverage to rule that out.
NEW_CLOSED_UNQUALIFIED_REASON = "no_identity_coverage"
# Owner decision 2026-11-23 (Anik, "Yes, non-directional"): until identity and
# split coverage exist, every 13F change is an observed filing change at
# reference prices, recorded for holdings graphs and never read as direction.
OBSERVATION_MEASURE = "observed_filing_holdings_change_at_reference_prices"

# SEC Form 13F XML <value> switched from thousands of dollars to whole dollars
# for filings made on or after 2023-01-03 (SEC Final Rule, Release No. 34-95148).
_13F_DOLLAR_CUTOVER_DATE: date = date(2023, 1, 3)

# Filing comparison receipts (one per compared or withheld filing pair):
# 13F-FILING:{cik}:{accession}, obs_date = the current filing's date. They are
# the dated record of which accession the writer read for each filer-quarter,
# what it compared against, and whether both sides were complete, so a
# restatement with no holdings changes (no change rows) still supersedes the
# original. The hyphenated prefix keeps them out of every '13F:%' reader.
RECEIPT_SERIES_PREFIX = "13F-FILING"
RECEIPT_MEASURE = "13f_filing_comparison_receipt"
SNAPSHOT_COMPLETE = "complete"
SNAPSHOT_VERIFIED_EMPTY = "verified_empty"
COMPLETE_SNAPSHOT_STATUSES: frozenset[str] = frozenset({SNAPSHOT_COMPLETE, SNAPSHOT_VERIFIED_EMPTY})

# One 13F writer at a time, whoever calls (2026-11-26, GRID #1492). Between
# 11-24 03:47Z and 11-26 18:27Z griddb held 6,622 receipts for 73 filings: about
# 92 full passes, against 7 logged smart:etf_flows runs. The rest were Hermes
# repair retries (89 "Retrying INSTITUTIONAL_FLOWS" in the grid-hermes journal,
# each "abandoned" after 60 s while its thread went on writing), and passes
# overlapped (7 in one 31-minute window). The session-level lock below spans
# the whole 13F pass on its own connection. A second writer that finds it held
# (an orphaned thread still running, a manual run) skips the pass and writes
# nothing; PostgreSQL drops the lock if the holder's connection dies. The
# per-manager change-row lock (_lock_change_rows) stays as the inner guard.
_13F_RUN_LOCK_KEY: int = _stable_lock_key("INSTITUTIONAL_FLOWS", "13F-run")

# The 13F cover page (form 13F-HR XML): period, amendment type and the
# summary-page entry/value totals the information table must match.
_13F_COVER_DOCUMENT = "primary_doc.xml"
_COVER_FIELDS: frozenset[str] = frozenset({
    "submissionType", "periodOfReport", "reportCalendarOrQuarter", "isAmendment",
    "amendmentType", "tableEntryTotal", "tableValueTotal", "isConfidentialOmitted",
})
_INFOTABLE_FIELDS: dict[str, str] = {
    "nameOfIssuer": "name",
    "cusip": "cusip",
    "value": "value",  # whole dollars >= 2023-01-03; thousands earlier
    "sshPrnamt": "shares",
    "sshPrnamtType": "share_type",
    "putCall": "put_call",
    "investmentDiscretion": "investment_discretion",
    "Sole": "voting_sole",
    "Shared": "voting_shared",
    "None": "voting_none",
}


def _local_name(tag: Any) -> str:
    """XML tag without its ``{namespace}`` (13F schemas changed namespace over time)."""
    tag = str(tag)
    return tag.split("}", 1)[1] if "}" in tag else tag


def _parse_13f_infotable(
    content: bytes, value_mult: int | None
) -> tuple[list[dict[str, Any]], int, float | None]:
    """Parse a 13F information table, namespace-agnostically.

    Returns ``(holdings, entries, reported_value_total)``: holdings that carry
    an issuer name and CUSIP, the count of every ``infoTable`` element, and
    the sum of their ``<value>`` in the units the filing reports (``None``
    when any value is unreadable). Raises ``ET.ParseError`` on malformed XML
    so a caller can tell an unreadable table from an empty one.
    """
    root = ET.fromstring(content)
    holdings: list[dict[str, Any]] = []
    entries = 0
    value_total: float | None = 0.0
    for entry in root.iter():
        if _local_name(entry.tag) != "infoTable":
            continue
        entries += 1
        holding: dict[str, Any] = {}
        for child in entry.iter():
            key = _INFOTABLE_FIELDS.get(_local_name(child.tag))
            if key and key not in holding and child.text and child.text.strip():
                holding[key] = child.text.strip()
        raw_value: float | None = None
        try:
            raw_value = float(str(holding.get("value", "")).replace(",", ""))
            if not math.isfinite(raw_value) or raw_value < 0:
                raw_value = None
        except ValueError:
            raw_value = None
        if raw_value is None or value_total is None:
            value_total = None
        else:
            value_total += raw_value
        if not (holding.get("name") and holding.get("cusip")):
            continue
        if "share_type" in holding:
            holding["share_type"] = holding["share_type"].upper()
        # Normalize <value> to whole USD per filing date cutover; fail closed
        # (value_units='unknown', value_usd=None) when the date is unknown.
        holding["value_multiplier"] = value_mult
        if value_mult is None:
            holding["value_units"] = "unknown"
            holding["value_usd"] = None
        else:
            holding["value_units"] = "usd"
            holding["reported_value_units"] = "usd" if value_mult == 1 else "thousands_usd"
            holding["value_usd"] = int(round(raw_value)) * value_mult if raw_value is not None else None
        holdings.append(holding)
    return holdings, entries, value_total


def _parse_13f_cover(content: bytes) -> dict[str, str]:
    """First non-empty value of each cover-page field; raises ``ET.ParseError``."""
    root = ET.fromstring(content)
    found: dict[str, str] = {}
    for el in root.iter():
        name = _local_name(el.tag)
        if name in _COVER_FIELDS and name not in found and el.text and el.text.strip():
            found[name] = el.text.strip()
    return found


def _parse_cover_date(raw: str | None) -> date | None:
    """Cover-page dates are ``MM-DD-YYYY``; ISO is accepted too."""
    if not raw:
        return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_cover_bool(raw: str | None) -> bool | None:
    value = (raw or "").strip().lower()
    if value in ("true", "y", "yes", "1"):
        return True
    if value in ("false", "n", "no", "0"):
        return False
    return None


def _parse_nonneg_int(raw: str | None) -> int | None:
    try:
        value = float((raw or "").replace(",", "").strip())
    except ValueError:
        return None
    if not math.isfinite(value) or value < 0 or not value.is_integer():
        return None
    return int(value)


def _failure_kind(exc: BaseException) -> str:
    """Short, secret-free label for a fetch failure (``http_503``, ``timeout``...)."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return f"http_{status}"
    if isinstance(exc, (requests.Timeout, TimeoutError)):
        return "timeout"
    if isinstance(exc, requests.ConnectionError):
        return "connection"
    return type(exc).__name__


@dataclass
class FilingSnapshot:
    """One filing's holdings plus the evidence that they were read in full.

    ``status`` is ``complete`` (every entry the cover page counts was read),
    ``verified_empty`` (the cover page counts zero entries and no table entry
    exists), or the reason the snapshot cannot be compared (missing or
    unparseable cover page / information table, an entry-count mismatch, a
    period or amendment-type disagreement with the index).
    """

    accession: str
    status: str
    holdings: list[dict[str, Any]] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        return self.status in COMPLETE_SNAPSHOT_STATUSES

    def receipt_evidence(self) -> dict[str, Any]:
        return {"status": self.status, **self.evidence}


def _parse_13f_date(value: date | str | None) -> date | None:
    """Parse a date / datetime / ISO string to ``date``, or ``None``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _parse_share_count(raw: Any) -> int | float | None:
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


def _13f_value_multiplier(filing_date: date | str | None) -> int | None:
    """Return USD multiplier for Form 13F ``<value>`` based on filing date.

    Filings made on or after 2023-01-03 report ``<value>`` in whole dollars
    (multiplier 1); earlier filings report in thousands of dollars (1000).
    Fails closed (returns ``None``) when ``filing_date`` is missing or
    unparseable so unknown-unit rows are quarantined rather than guessed.
    """
    parsed = _parse_13f_date(filing_date)
    if parsed is None:
        log.warning(
            "13F filing_date missing/unparseable ({d!r}); value units unknown — failing closed",
            d=filing_date,
        )
        return None
    return 1 if parsed >= _13F_DOLLAR_CUTOVER_DATE else 1000


UNCLASSIFIED_AMENDMENT = "unclassified"


def _select_latest_filings_by_report_date(
    filings: list[dict[str, Any]],
    *,
    count: int | None = None,
) -> list[dict[str, Any]]:
    """Collapse 13F-HR / 13F-HR/A filings to one standing filing per quarter.

    An amendment's ``amendment_type`` comes from its cover page
    (``_fetch_13f_index`` classifies candidates before calling this; EDGAR's
    submissions index does not carry it):

    * ``RESTATEMENT`` replaces the filings before it for that quarter.
    * ``NEW HOLDINGS`` lists only added positions. It never replaces the
      original (that would turn every omitted position into a false CLOSED);
      it is kept apart and listed on the quarter's standing filing as
      ``additive_supplements``, and the comparison is then withheld because
      the original alone is not the quarter's full holdings.
    * Anything else (no type, an unreadable cover, a failed fetch) is
      unclassified. A newer unclassified amendment becomes the quarter's
      standing filing flagged ``amendment_classification = 'unclassified'``,
      so the quarter reads as unknown rather than silently as the original.

    Filings that share a valid ``report_date`` collapse to the latest by
    ``(filing_date, is_amendment, accession)``. Filings with no
    ``report_date`` (legacy callers/tests) stay distinct by ``(accession,
    filing_date)``, and an amendment without a report date that is not a
    RESTATEMENT is skipped (logged). Returned newest quarter first.
    """
    by_report_date: dict[date, tuple[tuple[date, int, str], dict[str, Any]]] = {}
    supplements: dict[date, list[tuple[date, str]]] = {}
    without_report_date: list[tuple[tuple[date, int, str], dict[str, Any]]] = []
    seen_no_rd: set[tuple[str | None, date | None]] = set()

    for raw in filings:
        entry = dict(raw)
        form = str(entry.get("form") or entry.get("form_type") or "").strip().upper()
        is_amend = bool(entry.get("is_amendment")) or form.endswith("/A") or form == "13F-A"
        entry["is_amendment"] = is_amend
        if form:
            entry["form"] = form
        fdate = _parse_13f_date(entry.get("filing_date"))
        rdate = _parse_13f_date(entry.get("report_date"))
        acc = str(entry.get("accession") or "").strip()
        amendment_type = str(entry.get("amendment_type") or "").strip().upper()
        if is_amend and amendment_type != "RESTATEMENT":
            if rdate is not None and amendment_type == "NEW HOLDINGS":
                supplements.setdefault(rdate, []).append((fdate or date.min, acc))
                continue
            if rdate is None:
                log.warning(
                    "13F: skipping amendment {acc} ({form}) with no report date: amendment type "
                    "{t!r} is not a verified RESTATEMENT",
                    acc=acc, form=form, t=entry.get("amendment_type"),
                )
                continue
            entry["amendment_classification"] = UNCLASSIFIED_AMENDMENT
            log.warning(
                "13F: amendment {acc} ({form}, report_date={rd}) is unclassified ({why}); "
                "its quarter reads as unknown, not as the original",
                acc=acc, form=form, rd=rdate, why=entry.get("classification_reason") or "no amendment type",
            )
        rank = (fdate or date.min, 1 if is_amend else 0, acc)
        unclassified = entry.get("amendment_classification") == UNCLASSIFIED_AMENDMENT
        over_key = "unclassified_over_accession" if unclassified else "replaces_accession"

        if rdate is not None:
            prev = by_report_date.get(rdate)
            if prev is None:
                by_report_date[rdate] = (rank, entry)
            elif rank > prev[0]:
                prev_acc = str(prev[1].get("accession") or "").strip()
                if prev_acc and prev_acc != acc:
                    entry[over_key] = prev_acc
                by_report_date[rdate] = (rank, entry)
            else:
                prev_entry = prev[1]
                prev_unclassified = prev_entry.get("amendment_classification") == UNCLASSIFIED_AMENDMENT
                if acc and acc != str(prev_entry.get("accession") or "").strip():
                    prev_entry.setdefault(
                        "unclassified_over_accession" if prev_unclassified else "replaces_accession", acc
                    )
        else:
            key = (acc or None, fdate)
            if key in seen_no_rd:
                continue
            seen_no_rd.add(key)
            without_report_date.append((rank, entry))

    for rdate, items in supplements.items():
        if rdate not in by_report_date:
            continue
        winner_rank, winner = by_report_date[rdate]
        # A restatement filed after a supplement already includes it.
        later = sorted(a for fd, a in items if fd >= winner_rank[0] and a)
        if later:
            winner["additive_supplements"] = sorted(set(winner.get("additive_supplements") or []) | set(later))

    combined: list[tuple[tuple[date, date, int, str], dict[str, Any]]] = []
    for rdate, (rank, entry) in by_report_date.items():
        combined.append(((rdate, rank[0], rank[1], rank[2]), entry))
    for rank, entry in without_report_date:
        combined.append(((rank[0], rank[0], rank[1], rank[2]), entry))

    combined.sort(key=lambda item: item[0], reverse=True)
    ordered = [entry for _, entry in combined]
    return ordered[:count] if count is not None else ordered


def _aggregate_holdings_by_cusip(
    holdings: list[dict[str, Any]],
    *,
    quarantine_counts: dict[str, int] | None = None,
) -> dict[str, dict[str, Any]]:
    """Aggregate parsed 13F infotable entries per CUSIP within a single filing.

    Excludes put/call option rows (which share the underlying CUSIP and are not
    cash equity share holdings) and sums duplicate equity sub-manager lines
    while flagging unit or share-type mismatches as ``mixed``.
    """
    by_cusip: dict[str, dict[str, Any]] = {}
    for h in holdings:
        cusip = str(h.get("cusip") or "").strip().upper()
        if not cusip:
            continue
        if h.get("put_call"):
            if quarantine_counts is not None:
                quarantine_counts["non_equity_option"] = quarantine_counts.get("non_equity_option", 0) + 1
            continue

        row = dict(h)
        row["cusip"] = cusip
        if cusip not in by_cusip:
            by_cusip[cusip] = row
            continue

        agg = by_cusip[cusip]
        # Flag unit or share-type disagreements across lines for the same CUSIP
        if agg.get("value_units") != row.get("value_units"):
            agg["value_units"] = "mixed"
        agg_st = str(agg.get("share_type") or "").strip().upper()
        row_st = str(row.get("share_type") or "").strip().upper()
        if agg_st and row_st and agg_st != row_st:
            agg["share_type"] = "MIXED"

        v1 = _parse_positive_float(agg.get("value_usd"))
        v2 = _parse_positive_float(row.get("value_usd"))
        agg["value_usd"] = (v1 + v2) if (v1 is not None and v2 is not None) else None

        s1 = _parse_share_count(agg.get("shares"))
        s2 = _parse_share_count(row.get("shares"))
        if s1 is not None and s2 is not None:
            agg["shares"] = s1 + s2
        else:
            agg["shares"] = None

    return by_cusip

# Request timeout
_REQUEST_TIMEOUT: int = 30
# A request started just before the run's deadline still gets this long.
_MIN_REQUEST_TIMEOUT: float = 1.0

# The run budget's clock. One name so a test can replay a run on a fake clock.
_monotonic = time.monotonic


class _BudgetSpent(RuntimeError):
    """Raised instead of starting a 13F request once the run budget is spent.

    Deliberately not a ``requests`` / ``OSError`` / ``ValueError``: the
    fetchers turn those into "could not be read" snapshots and
    ``retry_on_failure`` retries them. This one passes through both, so a
    request refused for lack of time is never recorded as a finding about a
    filing and is never retried. ``phase`` says what was refused: a request
    (``request_refused``) or the wait before a retry (``retry_wait_refused``).
    """

    def __init__(self, what: str, phase: str = "request_refused") -> None:
        super().__init__(what)
        self.phase = phase


class _RunBudget:
    """The caller's time budget for one run.

    SmartScheduler passes ``should_continue`` and ``deadline_at`` (a
    ``time.monotonic()`` instant a little inside its own hard timeout) to a
    puller method that accepts them; the Hermes repair passes
    ``should_continue``. With neither, the budget never runs out.

    A run that outlives its caller's timeout is not stopped: its thread is
    abandoned, keeps reading and writing, and the run is logged as a bare
    TIMEOUT. Checking this budget between items lets the run stop itself and
    say how far it got.
    """

    def __init__(
        self,
        should_continue: Callable[[], bool] | None = None,
        deadline_at: float | None = None,
    ) -> None:
        self._should_continue = should_continue
        self._deadline_at = deadline_at

    def spent(self) -> bool:
        if self._deadline_at is not None and _monotonic() >= self._deadline_at:
            return True
        return self._should_continue is not None and not self._should_continue()

    def request_timeout(self) -> float:
        """``_REQUEST_TIMEOUT``, cut to what is left before the deadline.

        A socket timeout, not a wall clock: it bounds connect and each read,
        and a request already in flight can outlast the deadline. What keeps
        the run inside its budget is that no request *starts* after it
        (``InstitutionalFlowsPuller._edgar_get``); how far a request or a
        commit in flight ran past it is reported by ``overrun_s``.
        """
        if self._deadline_at is None:
            return float(_REQUEST_TIMEOUT)
        left = self._deadline_at - _monotonic()
        return max(_MIN_REQUEST_TIMEOUT, min(float(_REQUEST_TIMEOUT), left))

    def time_left(self) -> float | None:
        """Seconds until ``deadline_at``, never negative; None with no deadline."""
        if self._deadline_at is None:
            return None
        return max(0.0, self._deadline_at - _monotonic())

    def refuses_wait(self, seconds: float) -> bool:
        """True when a wait of ``seconds`` must not start: the budget is
        spent, or the wait would end after the deadline."""
        if self.spent():
            return True
        left = self.time_left()
        return left is not None and seconds > left

    def overrun_s(self) -> float | None:
        """Seconds past ``deadline_at`` now; None when no deadline was given."""
        if self._deadline_at is None:
            return None
        return round(max(0.0, _monotonic() - self._deadline_at), 3)


def _budget_stop(feature: str, what: str, read: int, total: int, **extra: Any) -> dict[str, Any]:
    """The result item for a run that stopped itself at its caller's deadline.

    PARTIAL, never SUCCESS: the items after the stop were not read. ``extra``
    carries where the stop was noticed (``stopped_phase``) and how far past
    the deadline the run was by then (``deadline_overrun_s``): the length of
    the request or commit that was in flight when the deadline passed.
    """
    return {
        "feature": feature,
        "status": "PARTIAL",
        "stopped_by_budget": True,
        "rows_inserted": 0,
        "items_total": total,
        "items_read": read,
        "items_not_attempted": total - read,
        "error": (
            f"{what} stopped at the run deadline: {read} of {total} read, "
            f"{total - read} not attempted"
        ),
        **extra,
    }


def _from_resume_point(managers: list[tuple[str, str]], resume_cik: str | None) -> list[tuple[str, str]]:
    """``managers`` starting at ``resume_cik`` and wrapping round to the rest.

    A run that stopped at its deadline leaves the first manager it did not
    finish as the resume point, so the managers at the end of the list are not
    starved by a run that is cut short every time.
    """
    for i, (cik, _name) in enumerate(managers):
        if cik == resume_cik:
            return managers[i:] + managers[:i]
    return managers


# What counts as "read in full" for a withheld comparison (Codex reviews
# 6080519392 and 6080669374). A side is read in full when it is complete, or
# when its status is one of these: each is a finding that rests on that
# filing's own documents, fetched and parsed. Everything else (a failed or
# refused fetch, a document that did not parse, a status not listed here) is
# not a finding about the filing.
#
# Deliberately not listed, though both are stable from run to run:
# - ``additive_supplement_not_merged``: the run stops at the supplement's
#   NEW HOLDINGS cover and never loads the original filing's table. That
#   says why the pair is withheld, not that the pair was read.
# - an amendment whose own index answers 404/410 (``missing_filing_index``):
#   neither its index nor its cover was obtained.
# If stable omissions should one day count as complete for the scheduler,
# that is a separate, named policy, not "read in full".
READ_IN_FULL_NOT_COMPARABLE: frozenset[str] = frozenset({
    "entry_count_mismatch",                 # cover and table parsed; their entry counts disagree
    "unreadable_entries",                   # table parsed; some of its entries are not holdings
    "missing_cover_page",                   # the filing's index was read and lists no cover page
    "missing_infotable",                    # index and cover read; no document holds table entries
    "missing_report_period",                # cover parsed; it states no period
    "report_date_mismatch",                 # cover parsed; its period is not the index's
    "form_mismatch",                        # cover parsed; its form is not the index's
    "amendment_not_verified_restatement",   # cover parsed; not a restatement
    "amendment_flag_mismatch",              # cover parsed; it says amendment, the index does not
    "missing_table_entry_total",            # cover parsed; it states no entry count
})
# An amendment's own cover, when that is what the side's status rests on.
AMENDMENT_COVER_READ: frozenset[str] = frozenset({
    "missing_cover_page",                   # the amendment's index was read and lists no cover page
    "cover_not_an_amendment",
    "report_date_mismatch",
    "unknown_amendment_type",
    "no_report_period",
})


def _not_read_in_full(snap: FilingSnapshot) -> str | None:
    """Why ``snap`` is not a read-in-full side of a filing pair, or None if it is.

    A withheld pair is a finished check only when this is None for both
    sides: reading again would give the same answer. Otherwise the run did
    not establish what the filing says (the fetch failed or was refused, a
    document did not parse) and the next run may.
    """
    if snap.complete or snap.status in READ_IN_FULL_NOT_COMPARABLE:
        return None
    if snap.status == "unclassified_amendment":
        reason = str(snap.evidence.get("classification_reason") or "unknown")
        return None if reason in AMENDMENT_COVER_READ else reason
    if snap.status == "unresolved_undated_amendment":
        reasons = [str(u.get("reason") or "unknown") for u in snap.evidence.get("unresolved_amendments") or []]
        return next((r for r in reasons if r not in AMENDMENT_COVER_READ), None if reasons else "unknown")
    return snap.status


# ── Re-runs write nothing new ────────────────────────────────────────────
# The change rows used BasePuller._row_exists, whose window is one hour: every
# run more than an hour after the last appended every row again (grid-svr
# 2026-11-23: 167,475 rows for 41,762 distinct (series_id, obs_date), from five
# runs in a day). A change row is now skipped only when the newest stored
# vintage of its (series_id, obs_date) is the same observation: the same
# filing pair (filing_accession, prev_filing_accession) and identical content.
# Anything else is a new vintage and is stored, so point-in-time readers keep
# every state GRID ever held: an amendment or a different prior filing, a
# corrected value, a parser change, a failed earlier row.
#
# QUARANTINED rows are left out before the ranking: a quarantined row is never
# the stored state. So an older identical SUCCESS under a newer QUARANTINED
# copy still counts as stored (nothing is written), and a row whose every
# stored copy is quarantined is written again.
#
# Two writers for the same manager and filing date (a timed-out run's orphaned
# thread and the repair worker's retry, 2026-11-23) could both look up before
# either committed and both insert: raw_series' uniqueness includes the write
# timestamp. The lookup and the inserts therefore run under one
# transaction-scoped advisory lock per (source, manager, filing date); the
# second writer waits, then looks up what the first committed.
#
# The wait is bounded. A timed-out run is not stopped: its thread goes on, and
# Hermes' repair worker abandons a retry after PER_SOURCE_REPAIR_SECONDS (60 s)
# and starts another, so several orphaned writers can overlap. A writer waits
# at most _CHANGE_ROW_LOCK_WAIT_S for one manager's lock and at most
# _CHANGE_ROW_LOCK_BUDGET_S in a whole run; after that it only tries the lock.
# A manager whose lock is not obtained is DEFERRED for this run: nothing is
# written for it (no change row, no receipt) and the next run takes it.
#
# What this does not claim. Each writer fetches its filings before it takes
# the lock, and raw_series stamps the write time itself. When two overlapping
# writers hold different payloads, both are stored and the one that commits
# last is the newest vintage, whichever fetch was newer. The vintage order is
# the order of writes, not of fetches.
_CHANGE_ROW_LOCK_WAIT_S: float = 5.0
_CHANGE_ROW_LOCK_BUDGET_S: float = 20.0
_PG_LOCK_NOT_AVAILABLE = "55P03"


class ChangeRowsBusy(RuntimeError):
    """Another writer holds this manager's change rows for this filing date."""
# (uq_raw_series_composite makes pull_timestamp unique per series, source and
# obs_date, so "newest" needs no tie-break.)
_CHANGE_ROW_LOOKUP_CHUNK: int = 500
_LATEST_CHANGE_ROWS_SQL = text(
    "SELECT series_id, pull_status, value, raw_payload FROM ("
    " SELECT series_id, pull_status, value, raw_payload,"
    " ROW_NUMBER() OVER ("
    "  PARTITION BY series_id ORDER BY pull_timestamp DESC"
    " ) AS newest_rank"
    " FROM raw_series"
    " WHERE source_id = :src AND obs_date = :od AND series_id IN :sids"
    " AND pull_status != 'QUARANTINED'"
    ") newest_stored WHERE newest_rank = 1"
).bindparams(bindparam("sids", expanding=True))
_FILING_IDENTITY_KEYS: tuple[str, ...] = ("filing_accession", "prev_filing_accession")


def _is_stored_observation(stored: tuple[Any, Any, Any] | None, value: float, payload: dict[str, Any]) -> bool:
    """True when ``stored`` (status, value, raw_payload of the newest stored
    vintage) is this same observation, so writing it again would add nothing.

    Fails open: anything unreadable or different means "not stored", and the
    caller writes a new vintage (the behaviour before this check existed).
    """
    if stored is None:
        return False
    status, stored_value, stored_payload = stored
    if status != "SUCCESS" or stored_value is None:
        return False
    if isinstance(stored_payload, (str, bytes)):
        try:
            stored_payload = json.loads(stored_payload)
        except ValueError:
            return False
    if not isinstance(stored_payload, dict):
        return False
    for key in _FILING_IDENTITY_KEYS:
        if not payload.get(key) or stored_payload.get(key) != payload.get(key):
            return False
    try:
        same_value = float(stored_value) == float(value)
        # Compare what the store holds: the payload after its JSON round trip.
        same_content = stored_payload == json.loads(json.dumps(payload))
    except (TypeError, ValueError):
        return False
    return same_value and same_content

# In-memory cache of CIKs that have permanently 404'd this process so we
# stop hammering EDGAR (and stop spamming errors.jsonl) for filers whose
# submissions endpoint no longer resolves.
_DEAD_CIKS: set[str] = set()


class InstitutionalFlowsPuller(BasePuller):
    """Pulls institutional money flow data from ETF volumes and SEC 13F filings.

    Provides three complementary views into where big money is flowing:

    1. ETF flow proxies: daily $ volume for 11 major ETFs, with rolling
       5-day and 20-day sums plus flow acceleration.
    2. SEC 13F position changes: quarterly snapshots of top-50 manager holdings,
       with NEW/CLOSED/INCREASED/DECREASED action flags.
    3. CFTC COT: handled by cftc_cot.py — this module provides series_id
       mapping only.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
    """

    SOURCE_NAME: str = "INSTITUTIONAL_FLOWS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.sec.gov/cgi-bin/browse-edgar",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    # First manager the last deadline-stopped 13F run did not finish; None
    # after a run that reached the end of the list. Process-wide, so it is
    # lost on a restart, which only means the next run starts at the top.
    _resume_13f_at_cik: str | None = None

    # No budget until a run is given one (each 13F run sets its own on the
    # instance), so the fetchers work on an instance used outside a run.
    _budget: _RunBudget = _RunBudget()

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the institutional flows puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "InstitutionalFlowsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    def _edgar_get(self, url: str) -> requests.Response:
        """Start one EDGAR request, unless the run budget is already spent.

        Every 13F request in this module starts here (index, amendment
        covers, filing index, cover page, information tables, the one-filing
        fallback), including each retry of ``retry_on_failure``. After the
        deadline it raises ``_BudgetSpent`` instead: no new call.
        """
        if self._budget.spent():
            raise _BudgetSpent(url)
        return requests.get(url, headers=_EDGAR_HEADERS, timeout=self._budget.request_timeout())

    def _before_retry_sleep(self, delay: float) -> None:
        """Refuse a retry's wait that would end after the run's deadline.

        ``retry_on_failure`` waits between attempts (its back-off, or a 429's
        Retry-After up to 30 s). A wait that passes the deadline is not
        started: the run stops and reports instead of sleeping past it.
        """
        if self._budget.refuses_wait(delay):
            raise _BudgetSpent(f"retry wait of {delay:.1f} s", phase="retry_wait_refused")

    # ── ETF Flow Proxy ─────────────────────────────────────────────────────

    def _pull_etf_flows(
        self,
        start_date: date,
        end_date: date,
        budget: _RunBudget | None = None,
    ) -> list[dict[str, Any]]:
        """Compute ETF flow proxies from yfinance daily data.

        For each ticker: fetch daily OHLCV, compute $ volume (close * volume),
        then rolling 5-day and 20-day sums, and flow acceleration.

        Parameters:
            start_date: Earliest date for flow computation.
            end_date: Latest date.

        Returns:
            List of result dicts per ticker/window.
        """
        try:
            import yfinance as yf
        except ImportError:
            log.error("yfinance not installed — ETF flow proxy unavailable")
            return [{"feature": "etf_flows", "status": "FAILED",
                     "error": "yfinance not installed"}]

        results: list[dict[str, Any]] = []

        # Need extra history for rolling windows
        fetch_start = start_date - timedelta(days=_FLOW_WINDOW_20D + 10)

        for position, ticker in enumerate(ETF_FLOW_TICKERS):
            if budget is not None and budget.spent():
                results.append(_budget_stop(
                    "ETF_FLOW:run_budget", "ETF flows", position, len(ETF_FLOW_TICKERS),
                ))
                log.warning(
                    "ETF flows stopped at the run deadline: {r} of {t} tickers read",
                    r=position, t=len(ETF_FLOW_TICKERS),
                )
                break
            try:
                log.info("Fetching ETF flow data for {t}", t=ticker)
                data = yf.download(
                    ticker,
                    start=str(fetch_start),
                    end=str(end_date + timedelta(days=1)),
                    progress=False,
                    auto_adjust=True,
                )

                if data is None or data.empty:
                    log.warning("No yfinance data for {t}", t=ticker)
                    # A liquid ETF with no bars over the window is a failed
                    # fetch (yfinance returns empty on errors), not "no data".
                    results.append({
                        "feature": f"ETF_FLOW:{ticker}",
                        "status": "FAILED",
                        "rows_inserted": 0,
                        "error": f"yfinance returned no bars for {ticker} {fetch_start}..{end_date}",
                    })
                    continue

                # Compute dollar volume
                close_col = "Close"
                vol_col = "Volume"
                # Handle multi-level columns from yfinance
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)

                if close_col not in data.columns or vol_col not in data.columns:
                    log.warning(
                        "Missing columns for {t}: {c}",
                        t=ticker,
                        c=list(data.columns),
                    )
                    continue

                data["dollar_volume"] = data[close_col] * data[vol_col]

                # Rolling sums
                data["flow_5d"] = data["dollar_volume"].rolling(
                    window=_FLOW_WINDOW_5D, min_periods=_FLOW_WINDOW_5D
                ).sum()
                data["flow_20d"] = data["dollar_volume"].rolling(
                    window=_FLOW_WINDOW_20D, min_periods=_FLOW_WINDOW_20D
                ).sum()
                # Acceleration: 5-day change of the 5-day flow
                data["flow_accel"] = data["flow_5d"].diff(periods=_FLOW_WINDOW_5D)

                # Filter to requested date range
                data = data[data.index >= pd.Timestamp(start_date)]

                # Store each metric
                for suffix, col_name in [
                    ("5d", "flow_5d"),
                    ("20d", "flow_20d"),
                    ("accel", "flow_accel"),
                ]:
                    series_id = f"ETF_FLOW:{ticker}:{suffix}"
                    rows_inserted = 0

                    with self.engine.begin() as conn:
                        existing_dates = self._get_existing_dates(series_id, conn)
                        for idx, row in data.iterrows():
                            if pd.isna(row[col_name]):
                                continue
                            obs_date = idx.date() if hasattr(idx, "date") else idx
                            if obs_date in existing_dates:
                                continue
                            self._insert_raw(
                                conn=conn,
                                series_id=series_id,
                                obs_date=obs_date,
                                value=float(row[col_name]),
                                raw_payload={
                                    "ticker": ticker,
                                    "close": float(row[close_col])
                                    if not pd.isna(row[close_col])
                                    else None,
                                    "volume": int(row[vol_col])
                                    if not pd.isna(row[vol_col])
                                    else None,
                                },
                            )
                            rows_inserted += 1

                    results.append({
                        "feature": series_id,
                        "status": "SUCCESS",
                        "rows_inserted": rows_inserted,
                    })

                log.info(
                    "ETF flow {t}: computed 5d/20d/accel flows",
                    t=ticker,
                )

            except Exception as exc:
                log.error(
                    "ETF flow {t} failed: {e}", t=ticker, e=str(exc)
                )
                results.append({
                    "feature": f"ETF_FLOW:{ticker}",
                    "status": "FAILED",
                    "error": str(exc),
                })

            time.sleep(_YF_RATE_DELAY)

        return results

    # ── SEC 13F Filings ────────────────────────────────────────────────────

    # Network-only retries — HTTP 4xx (404 dead CIK, 403 blocked) is permanent
    # and must NOT be retried.  Previously every dead CIK cost 3 attempts plus
    # an ERROR-level retry warning, drowning errors.jsonl in 13F 404s.
    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
        before_sleep=lambda self, delay: self._before_retry_sleep(delay),
    )
    def _fetch_13f_index(
        self,
        cik: str,
        filing_type: str = "13F-HR",
        count: int = 4,
    ) -> list[dict[str, Any]]:
        """Fetch recent 13F filing index entries from EDGAR for a CIK.

        Parameters:
            cik: SEC Central Index Key for the filer.
            filing_type: Filing type to search for.
            count: Number of recent filings to retrieve.

        Returns:
            List of filing metadata dicts with accession numbers and dates.
            Empty list when the CIK is unknown / returns 404 (permanent
            condition — caller treats as "no filings").
        """
        if cik in _DEAD_CIKS:
            return []

        url = (
            f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        )
        resp = self._edgar_get(url)
        # Permanent client errors (bad/retired CIK, forbidden) will never
        # recover from a retry — short-circuit and blacklist the CIK for
        # the remainder of this process so we don't re-hit it every cycle.
        if resp.status_code in (400, 403, 404, 410):
            log.warning(
                "13F CIK={c}: EDGAR returned {s}; blacklisting for session",
                c=cik, s=resp.status_code,
            )
            _DEAD_CIKS.add(cik)
            return []
        resp.raise_for_status()
        data = resp.json()

        raw_filings: list[dict[str, Any]] = []
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])

        accepted_forms = {filing_type}
        if not filing_type.endswith("/A"):
            accepted_forms.add(f"{filing_type}/A")

        for i, form in enumerate(forms):
            if form in accepted_forms:
                raw_filings.append({
                    "accession": accessions[i] if i < len(accessions) else None,
                    "filing_date": dates[i] if i < len(dates) else None,
                    "report_date": report_dates[i] if i < len(report_dates) else None,
                    "form": form,
                    "is_amendment": form.endswith("/A"),
                    "primary_doc": primary_docs[i] if i < len(primary_docs) else None,
                })

        # The submissions index carries no amendment type (and can omit the
        # report date): classify amendment candidates from their own cover
        # pages before choosing a quarter's standing filing.
        def relevant_quarters() -> set[date]:
            quarters = sorted(
                {rd for f in raw_filings if (rd := _parse_13f_date(f.get("report_date"))) is not None},
                reverse=True,
            )
            return set(quarters if count is None else quarters[:count])

        def classify(f: dict[str, Any]) -> date | None:
            time.sleep(_EDGAR_RATE_DELAY)
            amendment_type, how, period = self._classify_13f_amendment(
                cik, str(f["accession"]), _parse_13f_date(f.get("report_date"))
            )
            f["amendment_classified_by"] = how
            if amendment_type:
                f["amendment_type"] = amendment_type
            else:
                f["classification_reason"] = how
            return period

        # 1. An amendment with no index report date that could amend one of the
        #    compared quarters (filed on or after their earliest filing, or of
        #    unknown date) takes its quarter from its cover page. If none can
        #    be recovered, the current quarter is withheld, never left to the
        #    original.
        relevant = relevant_quarters()
        relevant_filed = [
            fd for f in raw_filings
            if _parse_13f_date(f.get("report_date")) in relevant
            and (fd := _parse_13f_date(f.get("filing_date"))) is not None
        ]
        horizon = min(relevant_filed) if relevant_filed else None
        unresolved: list[dict[str, Any]] = []
        for f in raw_filings:
            if not f["is_amendment"] or not f.get("accession") or _parse_13f_date(f.get("report_date")):
                continue
            filed = _parse_13f_date(f.get("filing_date"))
            if horizon is not None and filed is not None and filed < horizon:
                continue
            period = classify(f)
            if period is None:
                unresolved.append({
                    "accession": f["accession"],
                    "filing_date": f.get("filing_date"),
                    "reason": f.get("classification_reason") or "no_report_period",
                })
                f["amendment_classification"] = UNCLASSIFIED_AMENDMENT
                continue
            f["report_date"] = period.isoformat()
            f["report_date_source"] = "cover"

        # 2. Dated amendment candidates of the compared quarters.
        relevant = relevant_quarters()
        for f in raw_filings:
            if (
                f["is_amendment"] and f.get("accession") and "amendment_classified_by" not in f
                and _parse_13f_date(f.get("report_date")) in relevant
            ):
                classify(f)

        selected = _select_latest_filings_by_report_date(
            [f for f in raw_filings if _parse_13f_date(f.get("report_date")) or not f["is_amendment"]],
            count=count,
        )
        if unresolved and selected:
            log.warning(
                "13F CIK={c}: {n} amendment(s) with no recoverable quarter ({a}); withholding the "
                "current quarter instead of letting the original stand",
                c=cik, n=len(unresolved), a=[u["accession"] for u in unresolved],
            )
            selected[0]["unresolved_amendments"] = unresolved
        return selected

    def _classify_13f_amendment(
        self, cik: str, accession: str, index_report_date: date | None = None
    ) -> tuple[str | None, str, date | None]:
        """``(amendment_type, how, cover_period)`` from the amendment's own cover page.

        ``amendment_type`` is ``RESTATEMENT`` or ``NEW HOLDINGS`` when the
        cover says so (``how == 'cover'``), else ``None`` with the reason:
        no or unparseable cover, a cover that is not an amendment, an unknown
        type, a cover period that disagrees with the index, or a failed fetch
        (``fetch_failed:<kind>``). ``cover_period`` is the cover's report
        period when readable (it dates an amendment the index left undated).
        Never raises for a network or HTTP failure: the accession is known, so
        the caller must record it as unclassified rather than fall back to the
        original.
        """
        base_url = f"{_EDGAR_13F_XML_BASE}/{cik}/{accession.replace('-', '')}"
        try:
            resp = self._edgar_get(f"{base_url}/index.json")
            if resp.status_code in (404, 410):
                return None, "missing_filing_index", None
            resp.raise_for_status()
            names = [str(item.get("name") or "") for item in resp.json().get("directory", {}).get("item", [])]
            if _13F_COVER_DOCUMENT not in {n.lower() for n in names}:
                return None, "missing_cover_page", None
            time.sleep(_EDGAR_RATE_DELAY)
            resp = self._edgar_get(f"{base_url}/{_13F_COVER_DOCUMENT}")
            if resp.status_code in (404, 410):
                return None, "missing_cover_page", None
            resp.raise_for_status()
            cover = _parse_13f_cover(resp.content)
        except ET.ParseError:
            return None, "unparseable_cover_page", None
        except (requests.RequestException, OSError, TimeoutError, ValueError) as exc:
            return None, f"fetch_failed:{_failure_kind(exc)}", None
        period = _parse_cover_date(cover.get("reportCalendarOrQuarter") or cover.get("periodOfReport"))
        if _parse_cover_bool(cover.get("isAmendment")) is not True:
            return None, "cover_not_an_amendment", period
        if index_report_date is not None and period is not None and period != index_report_date:
            return None, "report_date_mismatch", period
        amendment_type = str(cover.get("amendmentType") or "").strip().upper()
        if amendment_type in ("RESTATEMENT", "NEW HOLDINGS"):
            return amendment_type, "cover", period
        return None, "unknown_amendment_type", period

    @retry_on_failure(
        max_attempts=2,
        backoff=1.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
        before_sleep=lambda self, delay: self._before_retry_sleep(delay),
    )
    def _fetch_13f_holdings(
        self,
        cik: str,
        accession: str,
        filing_date: date | str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch parsed holdings from a specific 13F filing.

        Attempts to fetch the infotable XML from the filing and parse
        individual holdings with CUSIP, name, value, shares, and share type.

        Parameters:
            cik: SEC CIK of the filer.
            accession: Accession number (e.g., '0001067983-24-000019').
            filing_date: Filing date (``date`` or ISO string) used to determine
                whether ``<value>`` is reported in whole dollars (>= 2023-01-03)
                or thousands of dollars (< 2023-01-03).

        Returns:
            List of holding dicts.
        """
        value_mult = _13f_value_multiplier(filing_date)
        # Build the filing directory URL
        acc_clean = accession.replace("-", "")
        base_url = f"{_EDGAR_13F_XML_BASE}/{cik}/{acc_clean}"

        # First, get the filing index to find the infotable document
        index_url = f"{base_url}/index.json"
        resp = self._edgar_get(index_url)
        resp.raise_for_status()
        index_data = resp.json()

        # Find the infotable document (XML with holdings)
        infotable_name = None
        for item in index_data.get("directory", {}).get("item", []):
            name = item.get("name", "").lower()
            if "infotable" in name and name.endswith(".xml"):
                infotable_name = item["name"]
                break

        if not infotable_name:
            log.debug(
                "No infotable XML found for CIK={c} accession={a}",
                c=cik,
                a=accession,
            )
            return []

        # Fetch and parse the infotable XML
        xml_url = f"{base_url}/{infotable_name}"
        resp = self._edgar_get(xml_url)
        resp.raise_for_status()

        try:
            holdings, _entries, _total = _parse_13f_infotable(resp.content, value_mult)
        except ET.ParseError as exc:
            log.warning(
                "XML parse error for CIK={c}: {e}", c=cik, e=str(exc)
            )
            return []
        return holdings

    @retry_on_failure(
        max_attempts=2,
        backoff=1.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
        before_sleep=lambda self, delay: self._before_retry_sleep(delay),
    )
    def _fetch_13f_filing(self, cik: str, filing: dict[str, Any]) -> FilingSnapshot:
        """Read one filing's holdings with evidence that they are complete.

        Unlike ``_fetch_13f_holdings`` (which returns ``[]`` for a missing or
        unparseable table), this never lets an unreadable table pass for an
        empty one. The cover page (``primary_doc.xml``) must parse, its period
        must match the index's report date, an amendment must be a verified
        RESTATEMENT on the cover page, and the information table must hold
        exactly the cover page's ``tableEntryTotal`` entries, each with an
        issuer and CUSIP. A cover page counting zero entries with no table
        entry is a verified empty snapshot. Anything else returns a snapshot
        whose ``status`` names the gap, and the caller withholds the pair.
        """
        accession = str(filing.get("accession") or "").strip()
        form = str(filing.get("form") or filing.get("form_type") or "13F-HR").strip().upper()
        is_amendment = bool(filing.get("is_amendment")) or form.endswith("/A")
        index_report_date = _parse_13f_date(filing.get("report_date"))
        evidence: dict[str, Any] = {"accession": accession, "form": form}

        def snap(status: str, holdings: list[dict[str, Any]] | None = None) -> FilingSnapshot:
            return FilingSnapshot(accession=accession, status=status, holdings=holdings or [], evidence=evidence)

        value_mult = _13f_value_multiplier(filing.get("filing_date"))
        if value_mult is None or not accession:
            return snap("unknown_filing_date_or_accession")

        base_url = f"{_EDGAR_13F_XML_BASE}/{cik}/{accession.replace('-', '')}"
        resp = self._edgar_get(f"{base_url}/index.json")
        resp.raise_for_status()
        names = [
            str(item.get("name") or "")
            for item in resp.json().get("directory", {}).get("item", [])
            if str(item.get("name") or "").lower().endswith(".xml")
        ]

        cover_name = next((n for n in names if n.lower() == _13F_COVER_DOCUMENT), None)
        if cover_name is None:
            return snap("missing_cover_page")
        time.sleep(_EDGAR_RATE_DELAY)
        resp = self._edgar_get(f"{base_url}/{cover_name}")
        if resp.status_code in (404, 410):
            return snap("missing_cover_page")
        resp.raise_for_status()
        try:
            cover = _parse_13f_cover(resp.content)
        except ET.ParseError:
            return snap("unparseable_cover_page")

        period = _parse_cover_date(cover.get("reportCalendarOrQuarter") or cover.get("periodOfReport"))
        entry_total = _parse_nonneg_int(cover.get("tableEntryTotal"))
        cover_is_amendment = _parse_cover_bool(cover.get("isAmendment"))
        amendment_type = str(cover.get("amendmentType") or "").strip().upper()
        evidence.update({
            "cover_report_date": period.isoformat() if period else None,
            "cover_submission_type": cover.get("submissionType"),
            "cover_is_amendment": cover_is_amendment,
            "cover_amendment_type": amendment_type or None,
            "table_entry_total": entry_total,
            "table_value_total": _parse_nonneg_int(cover.get("tableValueTotal")),
            "confidential_omitted": _parse_cover_bool(cover.get("isConfidentialOmitted")),
        })
        if period is None:
            return snap("missing_report_period")
        if index_report_date is not None and index_report_date != period:
            return snap("report_date_mismatch")
        submission = str(cover.get("submissionType") or "").strip().upper()
        if submission and submission != form:
            return snap("form_mismatch")
        if is_amendment and (cover_is_amendment is not True or amendment_type != "RESTATEMENT"):
            return snap("amendment_not_verified_restatement")
        if not is_amendment and cover_is_amendment is True:
            return snap("amendment_flag_mismatch")
        if entry_total is None:
            return snap("missing_table_entry_total")

        # Information table: a named infotable first, then any other XML.
        candidates = sorted(
            (n for n in names if n != cover_name),
            key=lambda n: (0 if ("infotable" in n.lower() or "informationtable" in n.lower()) else 1, n),
        )
        parse_failed = False
        holdings: list[dict[str, Any]] = []
        entries = 0
        value_total: float | None = None
        for name in candidates:
            time.sleep(_EDGAR_RATE_DELAY)
            resp = self._edgar_get(f"{base_url}/{name}")
            if resp.status_code in (404, 410):
                continue
            resp.raise_for_status()
            try:
                parsed, n_entries, total = _parse_13f_infotable(resp.content, value_mult)
            except ET.ParseError:
                parse_failed = True
                continue
            if n_entries:
                holdings, entries, value_total = parsed, n_entries, total
                evidence["infotable_document"] = name
                break
        evidence.update({
            "entries_read": entries,
            "holdings_parsed": len(holdings),
            "reported_value_total": value_total,
            "table_value_total_matches": (
                value_total is not None
                and evidence["table_value_total"] is not None
                and value_total == evidence["table_value_total"]
            ),
        })
        if entries == 0:
            if entry_total == 0 and not parse_failed:
                return snap(SNAPSHOT_VERIFIED_EMPTY)
            return snap("unparseable_infotable" if parse_failed else "missing_infotable")
        if entries != entry_total:
            return snap("entry_count_mismatch")
        if len(holdings) != entries:
            return snap("unreadable_entries")
        return snap(SNAPSHOT_COMPLETE, holdings)

    def _compare_holdings(
        self,
        prev_holdings: dict[str, dict[str, Any]],
        curr_holdings: dict[str, dict[str, Any]],
        *,
        quarantine_counts: dict[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        """Compare two quarterly snapshots using share-based flow estimates.

        Uses option (a) — ``|delta_shares| * stated quarter-end implied price``:
        - ``NEW`` / ``INCREASED`` / ``DECREASED``: later (current) quarter-end
          implied price ``curr_value_usd / curr_shares``.
        - ``CLOSED``: prior quarter-end implied price ``prev_value_usd / prev_shares``.

        Constant shares at a different price yield ``delta_shares == 0`` (zero
        estimated flow, no event emitted). Rows missing share counts, carrying
        unknown/mixed value units, non-``SH`` share types, or options
        (``putCall``) are quarantined (excluded with reason count).

        Parameters:
            prev_holdings: Previous quarter holdings keyed by CUSIP.
            curr_holdings: Current quarter holdings keyed by CUSIP.
            quarantine_counts: Optional dict updated in place with per-reason
                quarantine counts.

        Returns:
            List of change dicts with action (NEW/CLOSED/INCREASED/DECREASED).
        """
        def _quarantine(reason: str) -> None:
            if quarantine_counts is not None:
                quarantine_counts[reason] = quarantine_counts.get(reason, 0) + 1

        def _qualify(h: dict[str, Any]) -> tuple[float, int | float, str] | tuple[None, None, str]:
            if h.get("put_call"):
                return None, None, "non_equity_option"
            units = h.get("value_units")
            if units is not None and str(units).strip().lower() != "usd":
                return None, None, "unknown_or_mixed_units"
            if "value_multiplier" in h and (
                isinstance(h["value_multiplier"], bool) or h["value_multiplier"] not in (1, 1000)
            ):
                return None, None, "unknown_or_mixed_units"
            st = str(h.get("share_type") or "").strip().upper()
            if st and st != "SH":
                return None, None, "non_share_unit"
            val = _parse_positive_float(h.get("value_usd"))
            if val is None:
                return None, None, "invalid_value_usd"
            shares = _parse_share_count(h.get("shares"))
            if shares is None or shares <= 0:
                return None, None, "missing_share_counts"
            return val, shares, st or "SH"

        changes: list[dict[str, Any]] = []
        prev_cusips = set(prev_holdings.keys())
        curr_cusips = set(curr_holdings.keys())

        # New positions (valued at current quarter-end implied price)
        for cusip in curr_cusips - prev_cusips:
            h = curr_holdings[cusip]
            curr_val, curr_shares, share_type = _qualify(h)
            if curr_val is None or curr_shares is None:
                _quarantine(share_type)
                continue
            curr_price = curr_val / curr_shares
            est_flow = round(curr_shares * curr_price, 2)
            changes.append({
                "cusip": cusip,
                "name": h.get("name", ""),
                "action": "NEW",
                "observation_kind": "filing_appearance",
                "measure": OBSERVATION_MEASURE,
                "value_usd": est_flow,
                "estimated_flow_usd": est_flow,
                "holdings_value_delta_usd": curr_val,
                "curr_value_usd": curr_val,
                "prev_value_usd": 0.0,
                "shares": curr_shares,
                "curr_shares": curr_shares,
                "prev_shares": 0,
                "delta_shares": curr_shares,
                "valuation_price_usd": round(curr_price, 6),
                "valuation_price_basis": "curr_quarter_end_implied",
                "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
                "value_units": "usd",
                "share_type": share_type,
                "qualified_for_directional_flow": False,
                "unqualified_reason": NEW_CLOSED_UNQUALIFIED_REASON,
            })

        # Closed positions (valued at prior quarter-end implied price)
        for cusip in prev_cusips - curr_cusips:
            h = prev_holdings[cusip]
            prev_val, prev_shares, share_type = _qualify(h)
            if prev_val is None or prev_shares is None:
                _quarantine(share_type)
                continue
            prev_price = prev_val / prev_shares
            est_flow = round(prev_shares * prev_price, 2)
            changes.append({
                "cusip": cusip,
                "name": h.get("name", ""),
                "action": "CLOSED",
                "observation_kind": "filing_disappearance",
                "measure": OBSERVATION_MEASURE,
                "value_usd": est_flow,
                "estimated_flow_usd": est_flow,
                "holdings_value_delta_usd": -prev_val,
                "curr_value_usd": 0.0,
                "prev_value_usd": prev_val,
                "shares": prev_shares,
                "curr_shares": 0,
                "prev_shares": prev_shares,
                "delta_shares": -prev_shares,
                "valuation_price_usd": round(prev_price, 6),
                "valuation_price_basis": "prev_quarter_end_implied",
                "flow_estimation_method": "delta_shares_x_prev_quarter_end_price",
                "value_units": "usd",
                "share_type": share_type,
                "qualified_for_directional_flow": False,
                "unqualified_reason": NEW_CLOSED_UNQUALIFIED_REASON,
            })

        # Continuing positions: share-based delta * later quarter-end implied price
        for cusip in prev_cusips & curr_cusips:
            prev_h = prev_holdings[cusip]
            curr_h = curr_holdings[cusip]
            prev_val, prev_shares, prev_st = _qualify(prev_h)
            if prev_val is None or prev_shares is None:
                _quarantine(prev_st)
                continue
            curr_val, curr_shares, curr_st = _qualify(curr_h)
            if curr_val is None or curr_shares is None:
                _quarantine(curr_st)
                continue
            if prev_st != curr_st:
                _quarantine("mixed_share_types")
                continue
            delta_shares = curr_shares - prev_shares
            if delta_shares == 0:
                continue  # Constant shares at a new price = 0 estimated cash flow

            share_pct_change = delta_shares / prev_shares
            if share_pct_change > _POSITION_CHANGE_THRESHOLD:
                action = "INCREASED"
            elif share_pct_change < -_POSITION_CHANGE_THRESHOLD:
                action = "DECREASED"
            else:
                continue  # Skip small share changes

            curr_price = curr_val / curr_shares
            est_flow = round(abs(delta_shares) * curr_price, 2)
            changes.append({
                "cusip": cusip,
                "name": curr_h.get("name", ""),
                "action": action,
                "value_usd": est_flow,
                "estimated_flow_usd": est_flow,
                "holdings_value_delta_usd": curr_val - prev_val,
                "curr_value_usd": curr_val,
                "prev_value_usd": prev_val,
                "pct_change": round(share_pct_change, 4),
                "share_pct_change": round(share_pct_change, 4),
                "shares": curr_shares,
                "curr_shares": curr_shares,
                "prev_shares": prev_shares,
                "delta_shares": delta_shares,
                "valuation_price_usd": round(curr_price, 6),
                "valuation_price_basis": "curr_quarter_end_implied",
                "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
                "value_units": "usd",
                "share_type": curr_st,
                # Unverified direction: a split or reverse split cannot be ruled out.
                "observation_kind": "filing_holding_change",
                "measure": OBSERVATION_MEASURE,
                "qualified_for_directional_flow": False,
                "unqualified_reason": CONTINUING_POSITION_UNQUALIFIED_REASON,
            })

        return changes

    def _read_filing_snapshot(self, cik: str, filing: dict[str, Any]) -> FilingSnapshot:
        """``_fetch_13f_filing``, but a known accession always yields a snapshot.

        An unclassified amendment, a newer amendment with no recoverable
        quarter (``unresolved_amendments``), a quarter with an additive (NEW
        HOLDINGS) supplement, or a fetch that fails after the accession is known (HTTP
        429/503, a failed index.json, a timeout) returns an incomplete
        snapshot instead of raising, so the caller still stores a dated
        ``withheld_incomplete`` receipt for the newer accession and an older
        receipt is never left standing as the latest.
        """
        accession = str(filing.get("accession") or "").strip()
        evidence: dict[str, Any] = {
            "accession": accession,
            "form": str(filing.get("form") or "13F-HR").strip().upper(),
        }
        if filing.get("unresolved_amendments"):
            evidence["unresolved_amendments"] = list(filing["unresolved_amendments"])
            return FilingSnapshot(accession=accession, status="unresolved_undated_amendment", evidence=evidence)
        if filing.get("amendment_classification") == UNCLASSIFIED_AMENDMENT:
            evidence["classification_reason"] = filing.get("classification_reason")
            return FilingSnapshot(accession=accession, status="unclassified_amendment", evidence=evidence)
        if filing.get("additive_supplements"):
            evidence["additive_supplements"] = list(filing["additive_supplements"])
            return FilingSnapshot(accession=accession, status="additive_supplement_not_merged", evidence=evidence)
        try:
            return self._fetch_13f_filing(cik, filing)
        except (requests.RequestException, OSError, TimeoutError, ValueError) as exc:
            kind = _failure_kind(exc)
            log.warning(
                "13F CIK={c}: reading filing {a} failed ({k}); recording it as unverified",
                c=cik, a=accession, k=kind,
            )
            evidence["failure"] = kind
            return FilingSnapshot(accession=accession, status=f"fetch_failed:{kind}", evidence=evidence)

    def _lock_change_rows(self, conn: Any, cik: str, obs_date: date) -> None:
        """Serialize writers of one manager's change rows for one filing date.

        Must be the first statement of the transaction that looks the stored
        rows up and inserts (see the note above ``_LATEST_CHANGE_ROWS_SQL``).
        Waits at most ``_CHANGE_ROW_LOCK_WAIT_S``, at most
        ``_CHANGE_ROW_LOCK_BUDGET_S`` over the whole run, and never past the
        run's deadline; with either spent it only tries the lock. PostgreSQL only; other dialects (the
        SQLite test engines) have one writer and no advisory locks.

        Raises:
            ChangeRowsBusy: the lock was not obtained. The transaction must
                be left without writing.
        """
        if conn.dialect.name != "postgresql":
            return
        key = _stable_lock_key(self.SOURCE_NAME, "13F-change-rows", cik, obs_date.isoformat())
        spent = getattr(self, "_change_row_lock_wait_s", 0.0)
        wait_s = min(_CHANGE_ROW_LOCK_WAIT_S, _CHANGE_ROW_LOCK_BUDGET_S - spent)
        left = self._budget.time_left()
        if left is not None:
            # Never wait for the lock past the run's deadline: with no time
            # left it is only tried.
            wait_s = min(wait_s, left)
        if wait_s <= 0:
            if not conn.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": key}).scalar():
                raise ChangeRowsBusy(f"13F change rows of {cik} for {obs_date} are held by another writer")
            return
        started = time.monotonic()
        conn.execute(text("SELECT set_config('lock_timeout', :ms, true)"),
                     {"ms": f"{max(1, int(wait_s * 1000))}ms"})
        try:
            conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": key})
        except DBAPIError as exc:
            if getattr(exc.orig, "pgcode", None) == _PG_LOCK_NOT_AVAILABLE:
                raise ChangeRowsBusy(
                    f"13F change rows of {cik} for {obs_date} are held by another writer "
                    f"(waited {wait_s:.1f} s)") from exc
            raise
        finally:
            self._change_row_lock_wait_s = spent + (time.monotonic() - started)
        conn.execute(text("SELECT set_config('lock_timeout', '0', true)"))

    def _latest_stored_change_rows(
        self, conn: Any, series_ids: list[str], obs_date: date,
    ) -> dict[str, tuple[Any, Any, Any]]:
        """Newest non-quarantined vintage per series for ``obs_date``:
        ``{series_id: (pull_status, value, raw_payload)}``. Quarantined rows
        are excluded before the ranking, not after it."""
        latest: dict[str, tuple[Any, Any, Any]] = {}
        for start in range(0, len(series_ids), _CHANGE_ROW_LOOKUP_CHUNK):
            chunk = series_ids[start:start + _CHANGE_ROW_LOOKUP_CHUNK]
            for sid, status, value, payload in conn.execute(
                _LATEST_CHANGE_ROWS_SQL,
                {"src": self.source_id, "od": obs_date, "sids": chunk},
            ):
                latest[str(sid)] = (status, value, payload)
        return latest

    def _insert_receipt(
        self,
        conn: Any,
        series_id: str,
        obs_date: date,
        payload: dict[str, Any],
        curr_snap: FilingSnapshot,
    ) -> int:
        """Store one filing comparison receipt (value = the cover's entry count).

        Not deduplicated: each run's receipt is a dated record of what that
        run read, and the newest received vintage is the one that stands (a
        retry within the hour must be able to replace a failure's receipt).
        """
        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=float(curr_snap.evidence.get("table_entry_total") or 0),
            raw_payload=payload,
        )
        return 1

    def _pull_13f_filings(
        self,
        should_continue: Callable[[], bool] | None = None,
        deadline_at: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run the 13F pass as the only 13F writer, or skip it.

        Takes ``_13F_RUN_LOCK_KEY`` (see the note above it) on a dedicated
        connection held for the whole pass. When another writer holds it,
        nothing is read or written and one SKIPPED result is returned.
        PostgreSQL only; other dialects (the SQLite test engines) have one
        writer and no advisory locks.
        """
        lock_conn = None
        if self.engine.dialect.name == "postgresql":
            lock_conn = self.engine.connect()
            try:
                acquired = lock_conn.execute(
                    text("SELECT pg_try_advisory_lock(:k)"), {"k": _13F_RUN_LOCK_KEY},
                ).scalar()
                lock_conn.commit()
            except Exception:
                # Whether this session took the lock is unknown.
                self._discard_lock_connection(lock_conn)
                raise
            if acquired is not True:
                if acquired is False:
                    lock_conn.close()               # nothing held: an ordinary check-in
                else:
                    self._discard_lock_connection(lock_conn)
                log.warning("13F pass skipped: another 13F writer holds the run lock")
                return [{
                    "feature": "13f_filings",
                    "status": "SKIPPED",
                    "reason": "another 13F writer holds the run lock",
                    "rows_inserted": 0,
                }]
        try:
            # The budget goes to the inner pass; the lock is held until that
            # pass returns, including a pass that stops itself at its deadline.
            return self._pull_13f_managers(should_continue, deadline_at)
        finally:
            if lock_conn is not None:
                self._release_run_lock(lock_conn)

    def _release_run_lock(self, lock_conn: Any) -> None:
        """Release the run lock and confirm it, or drop the session that holds it.

        ``pg_advisory_unlock`` returns true only when this session held the
        lock and released it. Anything else (false, an error, a failed
        commit) leaves the release unknown, and a pooled ``close()`` would
        only check the connection in: the backend session lives on in the
        pool with the session-level lock still held, and every later 13F
        pass would skip. So an unconfirmed release ends the session.
        """
        released = False
        try:
            unlocked = lock_conn.execute(
                text("SELECT pg_advisory_unlock(:k)"), {"k": _13F_RUN_LOCK_KEY},
            ).scalar()
            lock_conn.commit()
            released = unlocked is True
            if not released:
                log.warning("13F run lock release not confirmed (pg_advisory_unlock returned {r!r})", r=unlocked)
        except Exception as exc:
            log.warning("13F run lock release failed: {e}", e=str(exc))
        if released:
            lock_conn.close()
        else:
            self._discard_lock_connection(lock_conn)

    @staticmethod
    def _discard_lock_connection(lock_conn: Any) -> None:
        """End the physical session, so PostgreSQL drops any lock it holds."""
        try:
            lock_conn.invalidate()
        except Exception as exc:
            log.error(
                "13F run lock connection could not be invalidated; the run lock may "
                "stay held until that session ends: {e}", e=str(exc),
            )
        finally:
            try:
                lock_conn.close()
            except Exception:
                pass

    def _pull_13f_managers(
        self,
        should_continue: Callable[[], bool] | None = None,
        deadline_at: float | None = None,
    ) -> list[dict[str, Any]]:
        """Pull and process 13F filings for all tracked institutional managers.

        For each manager:
        1. Fetch the two most recent 13F-HR filings.
        2. Read each in full (``_fetch_13f_filing``): cover page plus an
           information table holding every entry the cover page counts.
        3. If either side is incomplete, withhold the comparison and store a
           ``withheld_incomplete`` receipt; nothing else is written.
        4. Otherwise compare to detect NEW/CLOSED/INCREASED/DECREASED
           positions, store them as 13F:{cik}:{cusip}:{action} series, and
           store a ``compared`` receipt as 13F-FILING:{cik}:{accession}.

        Nothing goes to signal_sources (13F is non-directional).

        The run checks its caller's budget before each manager, after the
        index read and after both filings are read. Once the budget is spent
        it stops before writing anything for the manager in hand, appends one
        ``stopped_by_budget`` PARTIAL item saying how many managers were read,
        and leaves that manager as the next run's starting point.

        Parameters:
            should_continue: Returns False once the caller's budget is spent.
            deadline_at: ``time.monotonic()`` instant of the same budget; it
                also caps each request's timeout.

        Returns:
            List of result dicts per manager.
        """
        results: list[dict[str, Any]] = []
        self._change_row_lock_wait_s = 0.0      # lock-wait budget of this run
        budget = self._budget = _RunBudget(should_continue, deadline_at)
        managers = _from_resume_point(
            list(TOP_13F_FILERS.items()), InstitutionalFlowsPuller._resume_13f_at_cik
        )
        stopped_at: int | None = None
        stopped_phase: str | None = None

        for position, (cik, manager_name) in enumerate(managers):
            if budget.spent():
                stopped_at, stopped_phase = position, "between_managers"
                break
            try:
                log.info(
                    "Processing 13F for {m} (CIK={c})",
                    m=manager_name,
                    c=cik,
                )

                # Fetch two most recent quarter filings (amendments replace originals)
                filings = _select_latest_filings_by_report_date(
                    self._fetch_13f_index(cik, count=2),
                    count=2,
                )
                time.sleep(_EDGAR_RATE_DELAY)
                if budget.spent():
                    stopped_at, stopped_phase = position, "after_index_read"
                    break

                if len(filings) < 2:
                    log.debug(
                        "Fewer than 2 filings for {m} — skipping comparison",
                        m=manager_name,
                    )
                    if len(filings) == 1:
                        # Still store current holdings
                        accession = filings[0]["accession"]
                        if accession:
                            holdings = self._fetch_13f_holdings(
                                cik,
                                accession,
                                filing_date=filings[0].get("filing_date"),
                            )
                            time.sleep(_EDGAR_RATE_DELAY)
                            results.append({
                                "feature": f"13F:{cik}",
                                "manager": manager_name,
                                "status": "PARTIAL",
                                "holdings_count": len(holdings),
                                "rows_inserted": 0,
                            })
                    continue

                # Parse both filings
                curr_filing = filings[0]
                prev_filing = filings[1]
                curr_acc = curr_filing.get("accession")
                prev_acc = prev_filing.get("accession")
                curr_date_str = curr_filing.get("filing_date", "")
                prev_date_str = prev_filing.get("filing_date", "")
                curr_report_date = curr_filing.get("report_date")
                prev_report_date = prev_filing.get("report_date")
                curr_form = str(curr_filing.get("form") or "13F-HR").strip().upper()
                curr_is_amendment = bool(curr_filing.get("is_amendment")) or curr_form.endswith("/A")
                replaces_acc = curr_filing.get("replaces_accession")

                if not curr_acc or not prev_acc:
                    continue

                curr_mult = _13f_value_multiplier(curr_date_str)
                prev_mult = _13f_value_multiplier(prev_date_str)
                obs_date = _parse_13f_date(curr_date_str)
                if curr_mult is None or prev_mult is None or obs_date is None:
                    log.warning(
                        "13F {m} (CIK={c}): missing/unparseable filing_date "
                        "(curr={cd!r}, prev={pd!r}) — quarantining filing pair",
                        m=manager_name, c=cik, cd=curr_date_str, pd=prev_date_str,
                    )
                    results.append({
                        "feature": f"13F:{cik}",
                        "manager": manager_name,
                        "status": "QUARANTINED",
                        "reason": "unknown_filing_date_or_units",
                        "rows_inserted": 0,
                    })
                    continue

                curr_snap = self._read_filing_snapshot(cik, curr_filing)
                time.sleep(_EDGAR_RATE_DELAY)
                if budget.spent():
                    # The current filing used the last of the budget: the
                    # prior filing's read is not started.
                    stopped_at, stopped_phase = position, "after_current_filing_read"
                    break
                prev_snap = self._read_filing_snapshot(cik, prev_filing)
                time.sleep(_EDGAR_RATE_DELAY)
                if budget.spent():
                    # A read cut short by the deadline is not a finding about
                    # the filing: no receipt and no change row for this
                    # manager. The next run starts with it.
                    stopped_at, stopped_phase = position, "after_filing_reads"
                    break
                # The cover page is authoritative for a period the index omits.
                curr_report_date = curr_report_date or curr_snap.evidence.get("cover_report_date")
                prev_report_date = prev_report_date or prev_snap.evidence.get("cover_report_date")
                receipt = {
                    "measure": RECEIPT_MEASURE,
                    "qualified_for_directional_flow": False,
                    "manager_cik": cik,
                    "manager_name": manager_name,
                    "filing_accession": curr_acc,
                    "filing_date": obs_date.isoformat(),
                    "report_date": curr_report_date,
                    "form_type": curr_form,
                    "is_amendment": curr_is_amendment,
                    "replaces_accession": replaces_acc,
                    "prev_filing_accession": prev_acc,
                    "prev_filing_date": (
                        _parse_13f_date(prev_date_str).isoformat() if _parse_13f_date(prev_date_str) else None
                    ),
                    "prev_report_date": prev_report_date,
                    "prev_form_type": str(prev_filing.get("form") or "13F-HR").strip().upper(),
                    "prev_is_amendment": bool(prev_filing.get("is_amendment"))
                    or str(prev_filing.get("form") or "").strip().upper().endswith("/A"),
                    "curr_snapshot": curr_snap.receipt_evidence(),
                    "prev_snapshot": prev_snap.receipt_evidence(),
                }
                receipt_series = f"{RECEIPT_SERIES_PREFIX}:{cik}:{curr_acc}"

                if not (curr_snap.complete and prev_snap.complete):
                    # An unreadable or partial table is not an empty snapshot:
                    # comparing it would emit false CLOSED (current side) or
                    # false NEW (prior side). Withhold the pair and record why.
                    log.warning(
                        "13F {m} (CIK={c}): withholding comparison {p} -> {a}: "
                        "current {cs}, prior {ps}",
                        m=manager_name, c=cik, p=prev_acc, a=curr_acc,
                        cs=curr_snap.status, ps=prev_snap.status,
                    )
                    receipt.update({"comparison_status": "withheld_incomplete", "changes_detected": 0})
                    with self.engine.begin() as conn:
                        receipts_inserted = self._insert_receipt(conn, receipt_series, obs_date, receipt,
                                                                 curr_snap)
                    # Both sides read in full (_not_read_in_full) and the
                    # withheld receipt stored is a finished check with nothing
                    # to compare (NO_DATA): the same read gives the same
                    # answer until the manager files again. A side that was
                    # not read in full is a failure of this run and says why.
                    # Either way no change row is written, and the receipt
                    # keeps the manager's latest filing reported as
                    # unverified downstream.
                    fetch_failure = _not_read_in_full(curr_snap) or _not_read_in_full(prev_snap)
                    withheld = {
                        "feature": f"13F:{cik}",
                        "manager": manager_name,
                        "status": "FAILED" if fetch_failure else "NO_DATA",
                        "comparison_status": "withheld_incomplete",
                        "reason": "incomplete_filing_snapshot",
                        "curr_snapshot": curr_snap.status,
                        "prev_snapshot": prev_snap.status,
                        "rows_inserted": 0,
                        "receipts_inserted": receipts_inserted,
                    }
                    if fetch_failure:
                        withheld["error"] = (
                            f"13F {manager_name} (CIK={cik}): filing not read in full "
                            f"({fetch_failure}); comparison withheld"
                        )
                    results.append(withheld)
                    continue
                curr_holdings_list = curr_snap.holdings
                prev_holdings_list = prev_snap.holdings

                # Aggregate by CUSIP (excluding put/call options, summing sub-managers)
                quarantine_counts: dict[str, int] = {}
                curr_by_cusip = _aggregate_holdings_by_cusip(
                    curr_holdings_list, quarantine_counts=quarantine_counts
                )
                prev_by_cusip = _aggregate_holdings_by_cusip(
                    prev_holdings_list, quarantine_counts=quarantine_counts
                )

                # Detect share-based position changes
                changes = self._compare_holdings(
                    prev_by_cusip, curr_by_cusip, quarantine_counts=quarantine_counts
                )
                quarantined_total = sum(quarantine_counts.values())
                if quarantined_total:
                    log.warning(
                        "13F {m} (CIK={c}): quarantined {q} non-qualified holdings ({reasons})",
                        m=manager_name, c=cik, q=quarantined_total, reasons=quarantine_counts,
                    )

                # Store changes
                rows_inserted = 0
                rows_unchanged = 0
                with self.engine.begin() as conn:
                    self._lock_change_rows(conn, cik, obs_date)
                    stored_rows = self._latest_stored_change_rows(
                        conn,
                        [f"13F:{cik}:{chg['cusip']}:{chg['action']}" for chg in changes],
                        obs_date,
                    )
                    for chg in changes:
                        series_id = (
                            f"13F:{cik}:{chg['cusip']}:{chg['action']}"
                        )
                        value = float(chg.get("value_usd", 0))
                        payload = {
                            "manager_cik": cik,
                            "manager_name": manager_name,
                            "cusip": chg["cusip"],
                            "issuer_name": chg.get("name", ""),
                            "action": chg["action"],
                            "estimated_flow_usd": chg.get("estimated_flow_usd"),
                            "holdings_value_delta_usd": chg.get("holdings_value_delta_usd"),
                            "curr_value_usd": chg.get("curr_value_usd"),
                            "prev_value_usd": chg.get("prev_value_usd"),
                            "pct_change": chg.get("pct_change"),
                            "share_pct_change": chg.get("share_pct_change"),
                            "shares": chg.get("shares"),
                            "curr_shares": chg.get("curr_shares"),
                            "prev_shares": chg.get("prev_shares"),
                            "delta_shares": chg.get("delta_shares"),
                            "valuation_price_usd": chg.get("valuation_price_usd"),
                            "valuation_price_basis": chg.get("valuation_price_basis"),
                            "flow_estimation_method": chg.get("flow_estimation_method"),
                            "value_units": chg.get("value_units", "usd"),
                            "value_multiplier": curr_mult,
                            "prev_value_multiplier": prev_mult,
                            "share_type": chg.get("share_type", "SH"),
                            "qualified_for_directional_flow": chg.get("qualified_for_directional_flow", True),
                            "unqualified_reason": chg.get("unqualified_reason"),
                            "observation_kind": chg.get("observation_kind"),
                            "measure": chg.get("measure"),
                            "filing_accession": curr_acc,
                            "prev_filing_accession": prev_acc,
                            "filing_date": obs_date.isoformat(),
                            "prev_filing_date": _parse_13f_date(prev_date_str).isoformat() if _parse_13f_date(prev_date_str) else None,
                            "report_date": curr_report_date,
                            "prev_report_date": prev_report_date,
                            "form_type": curr_form,
                            "is_amendment": curr_is_amendment,
                            "replaces_accession": replaces_acc,
                        }

                        # A re-run that read the same filing pair and found
                        # the same thing writes nothing (see the note above
                        # _LATEST_CHANGE_ROWS_SQL).
                        if _is_stored_observation(stored_rows.get(series_id), value, payload):
                            rows_unchanged += 1
                            continue

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=obs_date,
                            value=value,
                            raw_payload=payload,
                        )
                        rows_inserted += 1

                    # Same transaction as the change rows: the dated record
                    # that this accession was read in full and compared, even
                    # when it changed nothing (a no-delta restatement).
                    receipt.update({
                        "comparison_status": "compared",
                        "changes_detected": len(changes),
                        "quarantined": dict(quarantine_counts),
                    })
                    receipts_inserted = self._insert_receipt(conn, receipt_series, obs_date, receipt, curr_snap)

                    # No 13F row goes to signal_sources (owner decision
                    # 2026-11-23: 13F is non-directional until CUSIP identity
                    # and split coverage exist). The old writer emitted
                    # source_type 'institutional' / NET_POSITION_DELTA with a
                    # dollar value, which wealth_tracker and pocket_lining read
                    # as directional fund flows; its CUSIP-keyed rows never
                    # matched the ticker-keyed holdings graph either. The
                    # observations stay in raw_series (labelled above) and in
                    # dollar_flows as direction "observed".

                results.append({
                    "feature": f"13F:{cik}",
                    "manager": manager_name,
                    "status": "SUCCESS",
                    "changes_detected": len(changes),
                    "rows_inserted": rows_inserted,
                    "rows_unchanged": rows_unchanged,
                    "receipts_inserted": receipts_inserted,
                    "quarantined": quarantined_total,
                })

                log.info(
                    "13F {m}: {n} changes detected, {r} rows inserted, {u} already stored",
                    m=manager_name,
                    n=len(changes),
                    r=rows_inserted,
                    u=rows_unchanged,
                )

            except ChangeRowsBusy as busy:
                # Another writer is on this manager's filing date. Nothing was
                # written by this run for it; the next run takes it.
                log.warning("13F {m} (CIK={c}) deferred: {e}", m=manager_name, c=cik, e=str(busy))
                results.append({
                    "feature": f"13F:{cik}",
                    "manager": manager_name,
                    "status": "DEFERRED",
                    "reason": "change_rows_held_by_another_writer",
                    "rows_inserted": 0,
                })
            except _BudgetSpent as refused:
                # A request (_edgar_get) or the wait before a retry
                # (_before_retry_sleep) was refused for lack of time. Nothing
                # was written for this manager.
                stopped_at, stopped_phase = position, refused.phase
                break
            except requests.HTTPError as http_exc:
                if budget.spent():
                    stopped_at, stopped_phase = position, "request_failed_at_deadline"
                    break
                # Permanent HTTP failures (404 / 403) — log once at WARNING,
                # not ERROR.  Retries are already disabled at the fetcher.
                log.warning(
                    "13F {m} (CIK={c}) skipped: {e}",
                    m=manager_name, c=cik, e=str(http_exc),
                )
                results.append({
                    "feature": f"13F:{cik}",
                    "manager": manager_name,
                    "status": "SKIPPED",
                    "error": str(http_exc),
                })
            except Exception as exc:
                if budget.spent():
                    # A request cut to the time left failed at the deadline:
                    # not attempted, not a failed manager.
                    stopped_at, stopped_phase = position, "request_failed_at_deadline"
                    break
                log.opt(exception=True).error(
                    "13F {m} (CIK={c}) failed: {e}",
                    m=manager_name,
                    c=cik,
                    e=str(exc),
                )
                results.append({
                    "feature": f"13F:{cik}",
                    "manager": manager_name,
                    "status": "FAILED",
                    "error": str(exc),
                })

        if stopped_at is None:
            InstitutionalFlowsPuller._resume_13f_at_cik = None
        else:
            next_cik = managers[stopped_at][0]
            InstitutionalFlowsPuller._resume_13f_at_cik = next_cik
            stop = _budget_stop(
                "13F:run_budget", "13F managers", stopped_at, len(managers),
                next_manager_cik=next_cik,
                managers_not_attempted=[c for c, _m in managers[stopped_at:]],
                stopped_phase=stopped_phase,
                deadline_overrun_s=budget.overrun_s(),
            )
            results.append(stop)
            log.warning("{e}; the next run starts at CIK={c}", e=stop["error"], c=next_cik)

        return results

    # ── Main pull orchestrator ─────────────────────────────────────────────

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
        days_back: int = 60,
        should_continue: Callable[[], bool] | None = None,
        deadline_at: float | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all institutional flow data (ETF flows + 13F filings).

        Parameters:
            start_date: Earliest date for ETF flow computation.
            days_back: Number of days back for incremental ETF flow pulls.
            should_continue: Returns False once the caller's budget is spent.
            deadline_at: ``time.monotonic()`` instant of the same budget.

        Returns:
            Combined list of result dicts. A run that stopped at its budget
            puts its ``stopped_by_budget`` items first, so the stop is what
            the run's status names.
        """
        budget = _RunBudget(should_continue, deadline_at)
        start = (
            date.fromisoformat(start_date)
            if isinstance(start_date, str)
            else start_date
        )
        end = date.today()

        # Use incremental start for ETF flows if we have data
        for ticker in ETF_FLOW_TICKERS[:1]:
            sid = f"ETF_FLOW:{ticker}:5d"
            latest = self._get_latest_date(sid)
            if latest is not None:
                incremental = latest - timedelta(days=7)
                if incremental > start:
                    start = incremental
                    log.info(
                        "InstitutionalFlows: incremental ETF from {d}",
                        d=start,
                    )
                break

        results: list[dict[str, Any]] = []

        # 1. ETF flow proxies (daily)
        log.info("Pulling ETF flow proxies from {s} to {e}", s=start, e=end)
        try:
            etf_results = self._pull_etf_flows(start, end, budget)
            results.extend(etf_results)
        except Exception as exc:
            log.error("ETF flow pull failed: {e}", e=str(exc))
            results.append({
                "feature": "etf_flows",
                "status": "FAILED",
                "error": str(exc),
            })

        # 2. SEC 13F filings (quarterly — always check for new filings)
        log.info("Processing SEC 13F filings for top managers")
        try:
            filing_results = self._pull_13f_filings(
                should_continue=should_continue, deadline_at=deadline_at
            )
            results.extend(filing_results)
        except Exception as exc:
            log.error("13F filing pull failed: {e}", e=str(exc))
            results.append({
                "feature": "13f_filings",
                "status": "FAILED",
                "error": str(exc),
            })

        # Stable: the budget stops move to the front, everything else keeps
        # its order.
        results.sort(key=lambda r: not r.get("stopped_by_budget"))

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        ok = sum(1 for r in results if r.get("status") == "SUCCESS")
        withheld = sum(1 for r in results if r.get("comparison_status") == "withheld_incomplete")
        stopped = any(r.get("stopped_by_budget") for r in results)
        log.info(
            "InstitutionalFlows {state} — {ok}/{total} succeeded, "
            "{w} comparisons withheld, {ins} total rows inserted",
            state="stopped at the run deadline" if stopped else "complete",
            ok=ok,
            total=len(results),
            w=withheld,
            ins=total_inserted,
        )
        return results

    def pull_etf_only(
        self,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull only ETF flow proxies (faster, no SEC API dependency).

        Parameters:
            start_date: Earliest date for computation.

        Returns:
            List of result dicts for ETF flows.
        """
        start = (
            date.fromisoformat(start_date)
            if isinstance(start_date, str)
            else start_date
        )
        return self._pull_etf_flows(start, date.today())

    def pull_13f_only(
        self,
        should_continue: Callable[[], bool] | None = None,
        deadline_at: float | None = None,
    ) -> list[dict[str, Any]]:
        """Pull only SEC 13F filing data.

        Parameters:
            should_continue: Returns False once the caller's budget is spent.
            deadline_at: ``time.monotonic()`` instant of the same budget.

        Returns:
            List of result dicts for 13F filings, a ``stopped_by_budget``
            item first (as in ``pull_all``) so the run's status names it.
        """
        results = self._pull_13f_filings(should_continue=should_continue, deadline_at=deadline_at)
        results.sort(key=lambda r: not r.get("stopped_by_budget"))
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = InstitutionalFlowsPuller(db_engine=get_engine())

    print("=== ETF Flow Proxies ===")
    etf_results = puller.pull_etf_only(start_date="2024-01-01")
    for r in etf_results:
        print(
            f"  {r.get('feature', '?')}: "
            f"{r['status']} ({r.get('rows_inserted', 0)} rows)"
        )

    print("\n=== SEC 13F Filings ===")
    filing_results = puller.pull_13f_only()
    for r in filing_results:
        print(
            f"  {r.get('feature', '?')} ({r.get('manager', '?')}): "
            f"{r['status']} ({r.get('rows_inserted', 0)} rows, "
            f"{r.get('changes_detected', 0)} changes)"
        )
