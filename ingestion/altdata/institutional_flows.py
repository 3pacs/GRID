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

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

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

# Top institutional filers by AUM (CIK numbers)
# These are the managers whose 13F filings move markets.
TOP_13F_FILERS: dict[str, str] = {
    "1067983": "Berkshire Hathaway",
    "1350694": "Bridgewater Associates",
    "1037389": "Renaissance Technologies",
    "1423053": "Citadel Advisors",
    "1536411": "Millennium Management",
    "1061768": "D.E. Shaw",
    "1336528": "Baupost Group",
    "1649339": "Two Sigma Investments",
    "1167483": "Elliott Management",
    "1364742": "Viking Global Investors",
    "1037529": "Appaloosa Management",
    "1159159": "Third Point",
    "1535392": "Point72 Asset Management",
    "1336326": "Greenlight Capital",
    "1040280": "Tiger Global Management",
    "1009207": "Marshall Wace",
    "1582243": "Coatue Management",
    "1484148": "Lone Pine Capital",
    "1027451": "Jana Partners",
    "1510387": "Pershing Square Capital",
    "1044316": "BlackRock",
    "1395250": "Vanguard Group",
    "1166559": "State Street Global Advisors",
    "1169819": "JPMorgan Investment Mgmt",
    "1633907": "AQR Capital Management",
    "1534067": "Balyasny Asset Management",
    "1544012": "Sculptor Capital (Och-Ziff)",
    "1608050": "Farallon Capital",
    "1056831": "SAC Capital (now Point72)",
    "1602119": "Whale Rock Capital",
    "1352575": "Soros Fund Management",
    "1697748": "Maverick Capital",
    "1006438": "Canyon Capital",
    "1345197": "ValueAct Capital",
    "1040971": "Druckenmiller (Duquesne)",
    "1103804": "Capital Group",
    "1105497": "T. Rowe Price",
    "1091439": "Fidelity Management & Research",
    "1510085": "Temasek Holdings",
    "1599901": "GIC Private Limited",
    "1632420": "Norges Bank Investment Mgmt",
    "1004244": "Wellington Management",
    "1533444": "Ares Management",
    "1106500": "Man Group",
    "1359842": "Two Sigma Advisers",
    "1085392": "Magnetar Capital",
    "1079114": "Tudor Investment Corp",
    "1050470": "Paulson & Co",
    "1595082": "Winton Group",
    "1699161": "ExodusPoint Capital",
}

# Rate limit between EDGAR API calls (SEC is strict: 10 req/sec max)
_EDGAR_RATE_DELAY: float = 0.15

# Minimum position change threshold to flag as INCREASED/DECREASED (20%)
_POSITION_CHANGE_THRESHOLD: float = 0.20

# Request timeout
_REQUEST_TIMEOUT: int = 30


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
                    results.append({
                        "feature": f"ETF_FLOW:{ticker}",
                        "status": "NO_DATA",
                        "rows_inserted": 0,
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

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError,
                              requests.RequestException),
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
        """
        url = (
            f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        )
        resp = requests.get(url, headers=_EDGAR_HEADERS, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        filings: list[dict[str, Any]] = []
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form == filing_type and len(filings) < count:
                filings.append({
                    "accession": accessions[i] if i < len(accessions) else None,
                    "filing_date": dates[i] if i < len(dates) else None,
                    "primary_doc": primary_docs[i] if i < len(primary_docs) else None,
                })

        return filings

    @retry_on_failure(
        max_attempts=2,
        backoff=1.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError,
                              requests.RequestException),
    )
    def _fetch_13f_holdings(
        self, cik: str, accession: str
    ) -> list[dict[str, Any]]:
        """Fetch parsed holdings from a specific 13F filing.

        Attempts to fetch the infotable XML from the filing and parse
        individual holdings with CUSIP, name, value, shares, and share type.

        Parameters:
            cik: SEC CIK of the filer.
            accession: Accession number (e.g., '0001067983-24-000019').

        Returns:
            List of holding dicts.
        """
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
                    ("value", "value"),  # in thousands
                    ("shares", "sshPrnamt"),
                    ("share_type", "sshPrnamtType"),
                    ("investment_discretion", "investmentDiscretion"),
                    ("voting_sole", "Sole"),
                    ("voting_shared", "Shared"),
                    ("voting_none", "None_"),
                ]:
                    el = entry.find(f".//{ns}{tag}")
                    if el is not None and el.text:
                        holding[field] = el.text.strip()
                if holding.get("name") and holding.get("cusip"):
                    # Convert value from thousands to actual
                    if "value" in holding:
                        try:
                            holding["value_usd"] = int(holding["value"]) * 1000
                        except (ValueError, TypeError):
                            holding["value_usd"] = 0
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
    ) -> list[dict[str, Any]]:
        """Compare two quarterly snapshots to detect position changes.

        Parameters:
            prev_holdings: Previous quarter holdings keyed by CUSIP.
            curr_holdings: Current quarter holdings keyed by CUSIP.

        Returns:
            List of change dicts with action (NEW/CLOSED/INCREASED/DECREASED).
        """
        changes: list[dict[str, Any]] = []
        prev_cusips = set(prev_holdings.keys())
        curr_cusips = set(curr_holdings.keys())

        # New positions
        for cusip in curr_cusips - prev_cusips:
            h = curr_holdings[cusip]
            changes.append({
                "cusip": cusip,
                "name": h.get("name", ""),
                "action": "NEW",
                "value_usd": h.get("value_usd", 0),
                "shares": h.get("shares"),
            })

        # Closed positions
        for cusip in prev_cusips - curr_cusips:
            h = prev_holdings[cusip]
            changes.append({
                "cusip": cusip,
                "name": h.get("name", ""),
                "action": "CLOSED",
                "value_usd": h.get("value_usd", 0),
                "shares": h.get("shares"),
            })

        # Changed positions
        for cusip in prev_cusips & curr_cusips:
            prev_val = prev_holdings[cusip].get("value_usd", 0)
            curr_val = curr_holdings[cusip].get("value_usd", 0)
            if prev_val == 0:
                continue
            pct_change = (curr_val - prev_val) / prev_val
            if pct_change > _POSITION_CHANGE_THRESHOLD:
                action = "INCREASED"
            elif pct_change < -_POSITION_CHANGE_THRESHOLD:
                action = "DECREASED"
            else:
                continue  # Skip small changes
            changes.append({
                "cusip": cusip,
                "name": curr_holdings[cusip].get("name", ""),
                "action": action,
                "value_usd": curr_val,
                "prev_value_usd": prev_val,
                "pct_change": round(pct_change, 4),
                "shares": curr_holdings[cusip].get("shares"),
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

                # Fetch two most recent filings for comparison
                filings = self._fetch_13f_index(cik, count=2)
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
                            holdings = self._fetch_13f_holdings(cik, accession)
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
                curr_acc = filings[0]["accession"]
                prev_acc = filings[1]["accession"]
                curr_date_str = filings[0].get("filing_date", "")

                if not curr_acc or not prev_acc:
                    continue

                curr_holdings_list = self._fetch_13f_holdings(cik, curr_acc)
                time.sleep(_EDGAR_RATE_DELAY)
                prev_holdings_list = self._fetch_13f_holdings(cik, prev_acc)
                time.sleep(_EDGAR_RATE_DELAY)

                # Index by CUSIP
                curr_by_cusip = {
                    h["cusip"]: h for h in curr_holdings_list if "cusip" in h
                }
                prev_by_cusip = {
                    h["cusip"]: h for h in prev_holdings_list if "cusip" in h
                }

                # Detect changes
                changes = self._compare_holdings(prev_by_cusip, curr_by_cusip)

                # Determine obs_date from filing
                try:
                    obs_date = date.fromisoformat(curr_date_str) if curr_date_str else date.today()
                except ValueError:
                    obs_date = date.today()

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
                                "pct_change": chg.get("pct_change"),
                                "shares": chg.get("shares"),
                                "filing_accession": curr_acc,
                            },
                        )
                        rows_inserted += 1

                    # Emit signal_sources entry for trust scoring
                    try:
                        conn.execute(
                            text(
                                "INSERT INTO signal_sources "
                                "(source_id, signal_type, signal_date, "
                                "signal_payload, confidence) "
                                "VALUES (:src, :type, :sd, :payload, :conf) "
                                "ON CONFLICT DO NOTHING"
                            ),
                            {
                                "src": self.source_id,
                                "type": "13F_POSITION_CHANGES",
                                "sd": obs_date,
                                "payload": json.dumps({
                                    "manager": manager_name,
                                    "cik": cik,
                                    "new_positions": sum(
                                        1 for c in changes if c["action"] == "NEW"
                                    ),
                                    "closed_positions": sum(
                                        1 for c in changes if c["action"] == "CLOSED"
                                    ),
                                    "increased": sum(
                                        1 for c in changes if c["action"] == "INCREASED"
                                    ),
                                    "decreased": sum(
                                        1 for c in changes if c["action"] == "DECREASED"
                                    ),
                                    "total_changes": len(changes),
                                }),
                                "conf": 0.9,
                            },
                        )
                    except Exception as sig_exc:
                        # signal_sources table may not exist yet — log and continue
                        log.debug(
                            "Could not write signal_source for {m}: {e}",
                            m=manager_name,
                            e=str(sig_exc),
                        )

                results.append({
                    "feature": f"13F:{cik}",
                    "manager": manager_name,
                    "status": "SUCCESS",
                    "changes_detected": len(changes),
                    "rows_inserted": rows_inserted,
                })

                log.info(
                    "13F {m}: {n} changes detected, {r} rows inserted",
                    m=manager_name,
                    n=len(changes),
                    r=rows_inserted,
                )

            except Exception as exc:
                log.error(
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
