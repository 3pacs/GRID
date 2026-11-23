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
    increases/decreases. Emits signal_sources entries for trust scoring.

Source 3: CFTC Commitment of Traders
    Already implemented in ``ingestion/altdata/cftc_cot.py`` — this module
    does NOT duplicate that puller. We reference it for completeness and
    provide a convenience wrapper that maps COT series IDs to the
    ``COT:`` prefix format expected by the money flow visualization layer.

Data stored:
    ETF_FLOW:{ticker}:5d      — 5-day rolling $ volume flow
    ETF_FLOW:{ticker}:20d     — 20-day rolling $ volume flow
    ETF_FLOW:{ticker}:accel   — flow acceleration (5d change of 5d flow)
    13F:{manager_cik}:{ticker}:{action} — quarterly position changes
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.altdata.verified_13f_filers import VERIFIED_FILERS
from ingestion.base import BasePuller, retry_on_failure

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


def _select_latest_filings_by_report_date(
    filings: list[dict[str, Any]],
    *,
    count: int | None = None,
) -> list[dict[str, Any]]:
    """Collapse 13F-HR / 13F-HR/A filings so a RESTATEMENT replaces its original.

    Only an amendment whose ``amendment_type`` is verified as ``RESTATEMENT``
    may replace the original. A "NEW HOLDINGS" amendment lists only added
    positions, so treating it as the quarter's full holdings would turn every
    omitted position into a false CLOSED outflow. EDGAR's submissions index
    does not carry the amendment type, so unverified amendments are skipped
    (logged) and the original 13F-HR stands, as before amendments were read.

    Filings that share a valid ``report_date`` collapse to the single latest
    eligible filing ranked by ``(filing_date, is_amendment, accession)``.
    Filings with no ``report_date`` (e.g. legacy callers/tests) remain
    distinct by ``(accession, filing_date)``. Returned newest quarter first.
    """
    by_report_date: dict[date, tuple[tuple[date, int, str], dict[str, Any]]] = {}
    without_report_date: list[tuple[tuple[date, int, str], dict[str, Any]]] = []
    seen_no_rd: set[tuple[str | None, date | None]] = set()

    for raw in filings:
        entry = dict(raw)
        form = str(entry.get("form") or entry.get("form_type") or "").strip().upper()
        is_amend = bool(entry.get("is_amendment")) or form.endswith("/A") or form == "13F-A"
        entry["is_amendment"] = is_amend
        if form:
            entry["form"] = form
        if is_amend and str(entry.get("amendment_type") or "").strip().upper() != "RESTATEMENT":
            log.warning(
                "13F: skipping amendment {acc} ({form}, report_date={rd}): amendment type "
                "{t!r} is not a verified RESTATEMENT; the original filing stands",
                acc=entry.get("accession"),
                form=form,
                rd=entry.get("report_date"),
                t=entry.get("amendment_type"),
            )
            continue
        fdate = _parse_13f_date(entry.get("filing_date"))
        rdate = _parse_13f_date(entry.get("report_date"))
        acc = str(entry.get("accession") or "").strip()
        rank = (fdate or date.min, 1 if is_amend else 0, acc)

        if rdate is not None:
            prev = by_report_date.get(rdate)
            if prev is None:
                by_report_date[rdate] = (rank, entry)
            elif rank > prev[0]:
                prev_acc = str(prev[1].get("accession") or "").strip()
                if prev_acc and prev_acc != acc:
                    entry["replaces_accession"] = prev_acc
                by_report_date[rdate] = (rank, entry)
            else:
                prev_entry = prev[1]
                if acc and acc != str(prev_entry.get("accession") or "").strip():
                    prev_entry.setdefault("replaces_accession", acc)
        else:
            key = (acc or None, fdate)
            if key in seen_no_rd:
                continue
            seen_no_rd.add(key)
            without_report_date.append((rank, entry))

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

    # ── ETF Flow Proxy ─────────────────────────────────────────────────────

    def _pull_etf_flows(
        self,
        start_date: date,
        end_date: date,
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

        for ticker in ETF_FLOW_TICKERS:
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
        resp = requests.get(url, headers=_EDGAR_HEADERS, timeout=_REQUEST_TIMEOUT)
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

        return _select_latest_filings_by_report_date(raw_filings, count=count)

    @retry_on_failure(
        max_attempts=2,
        backoff=1.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
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
        resp = requests.get(
            index_url, headers=_EDGAR_HEADERS, timeout=_REQUEST_TIMEOUT
        )
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
        resp = requests.get(
            xml_url, headers=_EDGAR_HEADERS, timeout=_REQUEST_TIMEOUT
        )
        resp.raise_for_status()

        holdings: list[dict[str, Any]] = []
        try:
            import xml.etree.ElementTree as ET

            root = ET.fromstring(resp.content)
            # 13F XML uses a namespace — handle both with and without
            ns = ""
            for prefix_ns in [
                "{http://www.sec.gov/document/thirteenf}",
                "{http://www.sec.gov/document/thirteenf-2021}",
                "",
            ]:
                test = root.findall(f".//{prefix_ns}infoTable")
                if test:
                    ns = prefix_ns
                    break

            for entry in root.findall(f".//{ns}infoTable"):
                holding: dict[str, Any] = {}
                for field, tag in [
                    ("name", "nameOfIssuer"),
                    ("cusip", "cusip"),
                    ("value", "value"),  # whole dollars >= 2023-01-03; thousands earlier
                    ("shares", "sshPrnamt"),
                    ("share_type", "sshPrnamtType"),
                    ("put_call", "putCall"),
                    ("investment_discretion", "investmentDiscretion"),
                    ("voting_sole", "Sole"),
                    ("voting_shared", "Shared"),
                    ("voting_none", "None_"),
                ]:
                    el = entry.find(f".//{ns}{tag}")
                    if el is not None and el.text:
                        holding[field] = el.text.strip()
                if holding.get("name") and holding.get("cusip"):
                    if "share_type" in holding:
                        holding["share_type"] = holding["share_type"].upper()
                    # Normalize <value> to whole USD per filing date cutover;
                    # fail closed (value_units='unknown', value_usd=None) when
                    # filing_date is missing/unparseable.
                    holding["value_multiplier"] = value_mult
                    if value_mult is None:
                        holding["value_units"] = "unknown"
                        holding["value_usd"] = None
                    else:
                        holding["value_units"] = "usd"
                        holding["reported_value_units"] = (
                            "usd" if value_mult == 1 else "thousands_usd"
                        )
                        if "value" in holding:
                            try:
                                raw_v = float(holding["value"].replace(",", ""))
                                if math.isfinite(raw_v) and raw_v >= 0:
                                    holding["value_usd"] = int(round(raw_v)) * value_mult
                                else:
                                    holding["value_usd"] = None
                            except (ValueError, TypeError):
                                holding["value_usd"] = None
                    holdings.append(holding)

        except ET.ParseError as exc:
            log.warning(
                "XML parse error for CIK={c}: {e}", c=cik, e=str(exc)
            )

        return holdings

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

    def _pull_13f_filings(self) -> list[dict[str, Any]]:
        """Pull and process 13F filings for all tracked institutional managers.

        For each manager:
        1. Fetch the two most recent 13F-HR filings.
        2. Parse holdings from each.
        3. Compare to detect NEW/CLOSED/INCREASED/DECREASED positions.
        4. Store changes as 13F:{cik}:{cusip}:{action} series.
        5. Emit signal_sources entries for trust scoring.

        Returns:
            List of result dicts per manager.
        """
        results: list[dict[str, Any]] = []

        for cik, manager_name in TOP_13F_FILERS.items():
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

                curr_holdings_list = self._fetch_13f_holdings(
                    cik, curr_acc, filing_date=curr_date_str
                )
                time.sleep(_EDGAR_RATE_DELAY)
                prev_holdings_list = self._fetch_13f_holdings(
                    cik, prev_acc, filing_date=prev_date_str
                )
                time.sleep(_EDGAR_RATE_DELAY)

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
                with self.engine.begin() as conn:
                    for chg in changes:
                        series_id = (
                            f"13F:{cik}:{chg['cusip']}:{chg['action']}"
                        )
                        value = float(chg.get("value_usd", 0))

                        if self._row_exists(series_id, obs_date, conn):
                            continue

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=obs_date,
                            value=value,
                            raw_payload={
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
                            },
                        )
                        rows_inserted += 1

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
                    "quarantined": quarantined_total,
                })

                log.info(
                    "13F {m}: {n} changes detected, {r} rows inserted",
                    m=manager_name,
                    n=len(changes),
                    r=rows_inserted,
                )

            except requests.HTTPError as http_exc:
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

        return results

    # ── Main pull orchestrator ─────────────────────────────────────────────

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
        days_back: int = 60,
    ) -> list[dict[str, Any]]:
        """Pull all institutional flow data (ETF flows + 13F filings).

        Parameters:
            start_date: Earliest date for ETF flow computation.
            days_back: Number of days back for incremental ETF flow pulls.

        Returns:
            Combined list of result dicts.
        """
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
            etf_results = self._pull_etf_flows(start, end)
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
            filing_results = self._pull_13f_filings()
            results.extend(filing_results)
        except Exception as exc:
            log.error("13F filing pull failed: {e}", e=str(exc))
            results.append({
                "feature": "13f_filings",
                "status": "FAILED",
                "error": str(exc),
            })

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        ok = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "InstitutionalFlows complete — {ok}/{total} succeeded, "
            "{ins} total rows inserted",
            ok=ok,
            total=len(results),
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

    def pull_13f_only(self) -> list[dict[str, Any]]:
        """Pull only SEC 13F filing data.

        Returns:
            List of result dicts for 13F filings.
        """
        return self._pull_13f_filings()


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
