"""
GRID SEC Form 4 insider trading filings ingestion module.

Pulls insider trading data from SEC EDGAR:
1. EDGAR full-text search API for recent Form 4 filings
2. EDGAR XBRL structured feed for parsed ownership changes

Insider buying is a well-documented alpha signal — insiders buy their
own stock for one reason (they believe it will go up), but sell for
many reasons (diversification, taxes, planned sales). Cluster buys
(multiple insiders buying within a short window) are particularly
strong signals.

Series stored with pattern: INSIDER:{ticker}:{insider_name}:{txn_type}
Fields: ticker, insider_name, insider_title, transaction_type, shares,
        price, value, filing_date, transaction_date
Tracked: cluster buys (multiple insiders buying = strong signal),
         unusual size (>$500K)
"""

from __future__ import annotations

import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- API URLs ----
_EDGAR_SEARCH_URL: str = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_FULL_TEXT_URL: str = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_FORM4_FEED_URL: str = "https://www.sec.gov/cgi-bin/browse-edgar"

# SEC requires identifying User-Agent
_SEC_USER_AGENT: str = os.environ.get(
    "SEC_USER_AGENT",
    "GRID-DataPuller/1.0 (research@grid.local)",
)

# HTTP config
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 0.15  # SEC asks for max 10 req/sec
_PAGE_SIZE: int = 100

# Thresholds for signal classification
_UNUSUAL_VALUE_THRESHOLD: float = 500_000.0  # $500K
_CLUSTER_BUY_WINDOW_DAYS: int = 14  # days within which multiple buys = cluster
_CLUSTER_BUY_MIN_INSIDERS: int = 2  # minimum insiders for cluster signal

# Normalise transaction codes (SEC Form 4 transaction codes)
_TXN_CODES: dict[str, str] = {
    "P": "BUY",       # Open market purchase
    "S": "SELL",      # Open market sale
    "A": "GRANT",     # Award/grant
    "D": "DISPOSE",   # Disposition to issuer
    "F": "TAX",       # Payment of exercise price or tax
    "I": "DISCRETIONARY",  # Discretionary transaction
    "M": "EXERCISE",  # Exercise of derivative
    "C": "CONVERSION",  # Conversion of derivative
    "G": "GIFT",      # Gift
    "J": "OTHER",     # Other
    "K": "EQUITY_SWAP",  # Equity swap
    "V": "VOLUNTARY",  # Voluntary reporting
    "W": "WILL",      # Acquisition by will or laws of descent
}


def _normalize_insider_name(name: str) -> str:
    """Normalise insider name for consistent series_id.

    Parameters:
        name: Raw insider name from filing.

    Returns:
        Normalised name with underscores.
    """
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9 ]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name


def _normalize_ticker(ticker: str) -> str:
    """Clean ticker symbol.

    Parameters:
        ticker: Raw ticker string.

    Returns:
        Uppercased, cleaned ticker.
    """
    return re.sub(r"[^A-Z0-9.]", "", ticker.strip().upper())


class InsiderFilingsPuller(BasePuller):
    """Pulls SEC Form 4 insider trading filings.

    Ingests insider buy/sell transactions from SEC EDGAR, storing
    each transaction as a raw_series row. Detects cluster buys
    (multiple insiders buying same stock within a window) and
    unusual-size transactions for signal emission.

    Series pattern: INSIDER:{ticker}:{insider_name}:{txn_type}
    Value: transaction value in dollars (shares * price).

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for SEC_INSIDER.
    """

    SOURCE_NAME: str = "SEC_INSIDER"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://efts.sec.gov/LATEST",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 38,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the insider filings puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "InsiderFilingsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    def _build_series_id(
        self,
        ticker: str,
        insider_name: str,
        txn_type: str,
    ) -> str:
        """Build series_id from components.

        Parameters:
            ticker: Normalised ticker symbol.
            insider_name: Normalised insider name.
            txn_type: Normalised transaction type.

        Returns:
            Series ID string.
        """
        return f"INSIDER:{ticker}:{insider_name}:{txn_type}"

    # ------------------------------------------------------------------ #
    # SEC EDGAR full-text search
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_form4_search(
        self,
        start_date: date,
        end_date: date | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Search EDGAR for recent Form 4 filings.

        Uses the SEC EFTS full-text search API to find Form 4
        ownership change statements filed within the date range.

        Parameters:
            start_date: Start of date range.
            end_date: End of date range (default: today).
            offset: Pagination offset.

        Returns:
            Search results dict from EDGAR API.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        if end_date is None:
            end_date = date.today()

        params = {
            "q": '"Form 4"',
            "dateRange": "custom",
            "startdt": start_date.isoformat(),
            "enddt": end_date.isoformat(),
            "forms": "4",
            "from": offset,
            "size": _PAGE_SIZE,
        }

        headers = {
            "User-Agent": _SEC_USER_AGENT,
            "Accept": "application/json",
        }

        resp = requests.get(
            _EDGAR_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_filing_detail(self, filing_url: str) -> str | None:
        """Fetch the raw XML/text of a Form 4 filing for parsing.

        Parameters:
            filing_url: Full URL to the filing document.

        Returns:
            Raw text content, or None on failure.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": _SEC_USER_AGENT,
            "Accept": "application/xml, text/xml, text/html",
        }

        resp = requests.get(
            filing_url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text

    def _parse_form4_xml(self, xml_text: str) -> list[dict[str, Any]]:
        """Parse a Form 4 XML filing into transaction records.

        Extracts non-derivative and derivative transactions from the
        structured XML format used by EDGAR.

        Parameters:
            xml_text: Raw XML text of the Form 4 filing.

        Returns:
            List of parsed transaction dicts.
        """
        import xml.etree.ElementTree as ET

        transactions: list[dict[str, Any]] = []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            log.debug("Form 4 XML parse error: {e}", e=str(exc))
            return []

        # Extract issuer info
        issuer_elem = root.find(".//issuer")
        ticker = ""
        if issuer_elem is not None:
            ticker_elem = issuer_elem.find("issuerTradingSymbol")
            if ticker_elem is not None and ticker_elem.text:
                ticker = _normalize_ticker(ticker_elem.text)

        if not ticker:
            return []

        # Extract reporting owner info
        owner_elem = root.find(".//reportingOwner")
        insider_name = ""
        insider_title = ""
        if owner_elem is not None:
            name_elem = owner_elem.find(".//rptOwnerName")
            if name_elem is not None and name_elem.text:
                insider_name = name_elem.text.strip()
            title_elem = owner_elem.find(".//officerTitle")
            if title_elem is not None and title_elem.text:
                insider_title = title_elem.text.strip()

        if not insider_name:
            return []

        # Extract non-derivative transactions
        for txn in root.findall(".//nonDerivativeTransaction"):
            txn_dict = self._parse_transaction_element(
                txn, ticker, insider_name, insider_title
            )
            if txn_dict:
                transactions.append(txn_dict)

        # Extract derivative transactions
        for txn in root.findall(".//derivativeTransaction"):
            txn_dict = self._parse_transaction_element(
                txn, ticker, insider_name, insider_title, is_derivative=True
            )
            if txn_dict:
                transactions.append(txn_dict)

        return transactions

    def _parse_transaction_element(
        self,
        elem: Any,
        ticker: str,
        insider_name: str,
        insider_title: str,
        is_derivative: bool = False,
    ) -> dict[str, Any] | None:
        """Parse a single transaction XML element.

        Parameters:
            elem: XML element for the transaction.
            ticker: Issuer ticker symbol.
            insider_name: Reporting owner name.
            insider_title: Officer title if applicable.
            is_derivative: Whether this is a derivative transaction.

        Returns:
            Parsed transaction dict, or None if unparseable.
        """
        # Transaction code
        code_elem = elem.find(".//transactionCode")
        if code_elem is None or not code_elem.text:
            return None
        txn_code = code_elem.text.strip()
        txn_type = _TXN_CODES.get(txn_code, txn_code)

        # Only track open market purchases and sales
        if txn_type not in ("BUY", "SELL"):
            return None

        # Shares
        shares_elem = elem.find(".//transactionShares/value")
        shares = 0.0
        if shares_elem is not None and shares_elem.text:
            try:
                shares = float(shares_elem.text)
            except ValueError:
                pass

        # Price
        price_elem = elem.find(".//transactionPricePerShare/value")
        price = 0.0
        if price_elem is not None and price_elem.text:
            try:
                price = float(price_elem.text)
            except ValueError:
                pass

        # Transaction date
        date_elem = elem.find(".//transactionDate/value")
        txn_date = None
        if date_elem is not None and date_elem.text:
            try:
                txn_date = date.fromisoformat(date_elem.text[:10])
            except (ValueError, TypeError):
                pass

        if txn_date is None:
            return None

        value = shares * price

        return {
            "ticker": ticker,
            "insider_name": insider_name,
            "insider_name_normalized": _normalize_insider_name(insider_name),
            "insider_title": insider_title,
            "transaction_type": txn_type,
            "transaction_code": txn_code,
            "shares": shares,
            "price": price,
            "value": value,
            "transaction_date": txn_date,
            "is_derivative": is_derivative,
        }

    # ------------------------------------------------------------------ #
    # Cluster buy detection
    # ------------------------------------------------------------------ #

    def _detect_cluster_buys(
        self,
        trades: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect cluster buy signals from a batch of insider trades.

        A cluster buy occurs when multiple different insiders buy the
        same stock within a short window — historically one of the
        strongest insider trading signals.

        Parameters:
            trades: List of normalised trade dicts.

        Returns:
            List of cluster buy signal dicts.
        """
        # Group buys by ticker
        buys_by_ticker: dict[str, list[dict[str, Any]]] = {}
        for t in trades:
            if t["transaction_type"] != "BUY":
                continue
            buys_by_ticker.setdefault(t["ticker"], []).append(t)

        clusters: list[dict[str, Any]] = []

        for ticker, buys in buys_by_ticker.items():
            if len(buys) < _CLUSTER_BUY_MIN_INSIDERS:
                continue

            # Check if buys are within the cluster window
            buys_sorted = sorted(buys, key=lambda x: x["transaction_date"])
            unique_insiders = set()
            window_buys: list[dict[str, Any]] = []

            for buy in buys_sorted:
                # Remove buys outside the window
                window_buys = [
                    b for b in window_buys
                    if (buy["transaction_date"] - b["transaction_date"]).days
                    <= _CLUSTER_BUY_WINDOW_DAYS
                ]
                window_buys.append(buy)

                current_insiders = {b["insider_name"] for b in window_buys}
                if len(current_insiders) >= _CLUSTER_BUY_MIN_INSIDERS:
                    unique_insiders = current_insiders

            if len(unique_insiders) >= _CLUSTER_BUY_MIN_INSIDERS:
                total_value = sum(b["value"] for b in buys)
                clusters.append({
                    "ticker": ticker,
                    "insider_count": len(unique_insiders),
                    "insiders": list(unique_insiders),
                    "total_value": total_value,
                    "date_range_start": buys_sorted[0]["transaction_date"],
                    "date_range_end": buys_sorted[-1]["transaction_date"],
                })

                log.info(
                    "CLUSTER BUY detected: {t} — {n} insiders, ${v:,.0f} total",
                    t=ticker,
                    n=len(unique_insiders),
                    v=total_value,
                )

        return clusters

    # ------------------------------------------------------------------ #
    # Signal emission for trust scoring
    # ------------------------------------------------------------------ #

    def _emit_signal(
        self,
        conn: Any,
        trade: dict[str, Any],
        signal_type: str | None = None,
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Parameters:
            conn: Active database connection (within a transaction).
            trade: Normalised trade dict.
            signal_type: Override signal type (e.g., 'CLUSTER_BUY').
        """
        import json

        if signal_type is None:
            signal_type = trade["transaction_type"]

        is_unusual = trade.get("value", 0) > _UNUSUAL_VALUE_THRESHOLD
        if is_unusual and signal_type in ("BUY", "SELL"):
            signal_type = f"UNUSUAL_{signal_type}"

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "insider",
                "sid": trade["insider_name"],
                "ticker": trade["ticker"],
                "sdate": trade["transaction_date"],
                "stype2": signal_type,
                "sval": json.dumps({
                    "insider_title": trade.get("insider_title", ""),
                    "shares": trade.get("shares", 0),
                    "price": trade.get("price", 0),
                    "value": trade.get("value", 0),
                    "is_derivative": trade.get("is_derivative", False),
                    "is_unusual_size": is_unusual,
                }),
            },
        )

    def _emit_cluster_signal(
        self,
        conn: Any,
        cluster: dict[str, Any],
    ) -> None:
        """Emit a cluster buy signal for trust scoring.

        Parameters:
            conn: Active database connection.
            cluster: Cluster buy detection result.
        """
        import json

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "insider",
                "sid": f"cluster_{cluster['ticker'].lower()}",
                "ticker": cluster["ticker"],
                "sdate": cluster["date_range_end"],
                "stype2": "CLUSTER_BUY",
                "sval": json.dumps({
                    "insider_count": cluster["insider_count"],
                    "insiders": cluster["insiders"],
                    "total_value": cluster["total_value"],
                    "window_days": (
                        cluster["date_range_end"] - cluster["date_range_start"]
                    ).days,
                }),
            },
        )

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_recent(self, days_back: int = 1) -> dict[str, Any]:
        """Pull recent Form 4 insider filings from SEC EDGAR.

        Searches EDGAR for Form 4 filings in the date range, parses
        each filing's XML, stores transactions, detects cluster buys,
        and emits signals for trust scoring.

        Parameters:
            days_back: Number of days of filings to pull (default: 1).

        Returns:
            dict with status, rows_inserted, filings_processed, cluster_buys.
        """
        start_date = date.today() - timedelta(days=days_back)
        end_date = date.today()
        all_trades: list[dict[str, Any]] = []
        filings_processed = 0
        offset = 0

        try:
            while True:
                result = self._fetch_form4_search(
                    start_date=start_date,
                    end_date=end_date,
                    offset=offset,
                )

                hits = result.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    source = hit.get("_source", {})
                    # Construct filing URL from accession number
                    # _id format: "0000950103-26-004828:ownership.xml"
                    hit_id = hit.get("_id", "")
                    if ":" in hit_id:
                        adsh = hit_id.split(":")[0]
                        filename = hit_id.split(":")[1]
                    else:
                        adsh = source.get("adsh", "")
                        filename = "ownership.xml"
                    if not adsh:
                        continue
                    # CIK from the first entry
                    ciks = source.get("ciks", [])
                    cik = ciks[0] if ciks else ""
                    if not cik:
                        continue
                    adsh_clean = adsh.replace("-", "")
                    file_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh_clean}/{filename}"

                    # Respect SEC rate limit
                    time.sleep(_RATE_LIMIT_DELAY)

                    try:
                        xml_text = self._fetch_filing_detail(file_url)
                        if xml_text:
                            trades = self._parse_form4_xml(xml_text)
                            if trades:
                                filing_date_str = source.get("file_date", "")
                                for t in trades:
                                    t["filing_date"] = filing_date_str
                                all_trades.extend(trades)
                            filings_processed += 1
                    except Exception as exc:
                        log.debug(
                            "Insider: failed to parse filing {u}: {e}",
                            u=file_url,
                            e=str(exc),
                        )

                total_hits = result.get("hits", {}).get("total", {})
                total_count = total_hits.get("value", 0) if isinstance(total_hits, dict) else total_hits
                offset += _PAGE_SIZE

                if offset >= total_count or offset >= 500:
                    # Cap at 500 filings per pull to avoid overloading
                    break

                time.sleep(_RATE_LIMIT_DELAY)

        except Exception as exc:
            log.error("Insider filings search failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not all_trades:
            log.info("Insider: no trades found in last {d} days", d=days_back)
            return {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "filings_processed": filings_processed,
                "cluster_buys": 0,
            }

        log.info(
            "Insider: {n} trades from {f} filings",
            n=len(all_trades),
            f=filings_processed,
        )

        # Detect cluster buys before storing
        cluster_buys = self._detect_cluster_buys(all_trades)

        # Store trades
        rows_inserted = 0

        with self.engine.begin() as conn:
            for trade in all_trades:
                series_id = self._build_series_id(
                    ticker=trade["ticker"],
                    insider_name=trade["insider_name_normalized"],
                    txn_type=trade["transaction_type"],
                )

                obs_date = trade["transaction_date"]

                # Dedup check
                if self._row_exists(series_id, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=trade["value"],
                    raw_payload={
                        "ticker": trade["ticker"],
                        "insider_name": trade["insider_name"],
                        "insider_title": trade["insider_title"],
                        "transaction_type": trade["transaction_type"],
                        "transaction_code": trade["transaction_code"],
                        "shares": trade["shares"],
                        "price": trade["price"],
                        "value": trade["value"],
                        "filing_date": trade.get("filing_date", ""),
                        "transaction_date": trade["transaction_date"].isoformat(),
                        "is_derivative": trade["is_derivative"],
                    },
                )
                rows_inserted += 1

                # Emit individual trade signal
                try:
                    self._emit_signal(conn, trade)
                except Exception as exc:
                    log.warning(
                        "Insider: signal emission failed for {i}/{t}: {e}",
                        i=trade["insider_name"],
                        t=trade["ticker"],
                        e=str(exc),
                    )

            # Emit cluster buy signals
            for cluster in cluster_buys:
                try:
                    self._emit_cluster_signal(conn, cluster)
                except Exception as exc:
                    log.warning(
                        "Insider: cluster signal emission failed for {t}: {e}",
                        t=cluster["ticker"],
                        e=str(exc),
                    )

        log.info(
            "Insider: {ins} rows inserted, {c} cluster buys detected",
            ins=rows_inserted,
            c=len(cluster_buys),
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "filings_processed": filings_processed,
            "trades_found": len(all_trades),
            "cluster_buys": len(cluster_buys),
        }

    def pull_all(self, days_back: int = 1) -> dict[str, Any]:
        """Pull all insider filing data.

        Alias for pull_recent() — daily incremental pull.

        Parameters:
            days_back: Number of days of filings to pull.

        Returns:
            dict with status, rows_inserted, etc.
        """
        return self.pull_recent(days_back=days_back)
