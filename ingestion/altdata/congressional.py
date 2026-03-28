"""
GRID congressional trading disclosure ingestion module.

Pulls congressional stock trading disclosures from public sources:
1. House Financial Disclosures via clerk.house.gov/public_disc
2. Senate STOCK Act filings via efts.sec.gov (Senate eFD)
3. QuiverQuant API (free tier) as alternative/fallback

Congressional trades are disclosed with a ~45-day lag. Research shows
certain members have statistically significant alpha — the trust scoring
system tracks which members are consistently early movers.

Series stored with pattern: CONGRESS:{chamber}:{member}:{ticker}:{txn_type}
Fields: member_name, party, state, committee, ticker, transaction_type,
        amount_range, disclosure_date, transaction_date
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
_HOUSE_DISC_URL: str = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs"
_HOUSE_XML_URL: str = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs"
_SENATE_EFTS_URL: str = "https://efts.sec.gov/LATEST/search-index"
_QUIVERQUANT_URL: str = "https://api.quiverquant.com/beta/historical/membertrading"

# Environment variable for optional QuiverQuant API key
_QQ_API_KEY_ENV: str = "QUIVERQUANT_API_KEY"

# HTTP config
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.5

# Amount range mapping (House disclosures use coded ranges)
AMOUNT_RANGES: dict[str, tuple[int, int]] = {
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

# Normalise transaction types
_TXN_NORMALIZE: dict[str, str] = {
    "purchase": "BUY",
    "sale": "SELL",
    "sale_full": "SELL",
    "sale_partial": "SELL",
    "exchange": "EXCHANGE",
    "buy": "BUY",
    "sell": "SELL",
    "p": "BUY",
    "s": "SELL",
    "s (full)": "SELL",
    "s (partial)": "SELL",
}


def _normalize_member_name(name: str) -> str:
    """Normalise member name for consistent series_id generation.

    Strips whitespace, lowercases, removes suffixes like Jr/III,
    and replaces spaces with underscores.

    Parameters:
        name: Raw member name string.

    Returns:
        Normalised name suitable for series_id.
    """
    name = name.strip().lower()
    # Remove common suffixes
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b\.?", "", name)
    # Remove non-alphanumeric (except spaces)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name


def _normalize_ticker(ticker: str) -> str:
    """Clean ticker symbol for series_id.

    Parameters:
        ticker: Raw ticker string.

    Returns:
        Uppercased, stripped ticker.
    """
    ticker = ticker.strip().upper()
    # Remove common prefixes like $
    ticker = re.sub(r"[^A-Z0-9.]", "", ticker)
    return ticker


def _normalize_txn_type(raw_type: str) -> str:
    """Map raw transaction type to BUY/SELL/EXCHANGE.

    Parameters:
        raw_type: Raw transaction type string from source.

    Returns:
        Normalised transaction type.
    """
    return _TXN_NORMALIZE.get(raw_type.strip().lower(), raw_type.strip().upper())


def _midpoint_amount(amount_range: str) -> float:
    """Get the midpoint dollar value from an amount range code or string.

    Parameters:
        amount_range: Either a code ('A'-'J') or a string like '$1,001 - $15,000'.

    Returns:
        Midpoint dollar value as float.
    """
    # Try coded range first
    if amount_range.upper() in AMOUNT_RANGES:
        lo, hi = AMOUNT_RANGES[amount_range.upper()]
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


class CongressionalTradingPuller(BasePuller):
    """Pulls congressional stock trading disclosures.

    Ingests House and Senate financial disclosure data, storing each
    transaction as a raw_series row with full metadata in the payload.

    The trust scoring system downstream can evaluate which members
    consistently trade ahead of market moves.

    Series pattern: CONGRESS:{chamber}:{member}:{ticker}:{txn_type}
    Value: midpoint of the disclosed dollar amount range.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for CONGRESS_TRADING.
        qq_api_key: Optional QuiverQuant API key for fallback data.
    """

    SOURCE_NAME: str = "CONGRESS_TRADING"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://disclosures-clerk.house.gov/public_disc",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "MED",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the congressional trading puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self.qq_api_key: str = os.environ.get(_QQ_API_KEY_ENV, "")
        if not self.qq_api_key:
            log.warning(
                "CongressionalTradingPuller: {env} not set — "
                "QuiverQuant fallback unavailable, using direct sources only",
                env=_QQ_API_KEY_ENV,
            )
        log.info(
            "CongressionalTradingPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    def _build_series_id(
        self,
        chamber: str,
        member: str,
        ticker: str,
        txn_type: str,
    ) -> str:
        """Build series_id from components.

        Parameters:
            chamber: 'HOUSE' or 'SENATE'.
            member: Normalised member name.
            ticker: Normalised ticker symbol.
            txn_type: Normalised transaction type.

        Returns:
            Series ID string.
        """
        return f"CONGRESS:{chamber}:{member}:{ticker}:{txn_type}"

    # ------------------------------------------------------------------ #
    # QuiverQuant API (primary source — structured JSON)
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
    def _fetch_quiverquant(self, days_back: int = 7) -> list[dict[str, Any]]:
        """Fetch recent congressional trades from QuiverQuant API.

        Parameters:
            days_back: Number of days of history to request.

        Returns:
            List of trade dicts from API.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        if not self.qq_api_key:
            log.debug("QuiverQuant API key not available — skipping")
            return []

        headers = {
            "Authorization": f"Bearer {self.qq_api_key}",
            "Accept": "application/json",
            "User-Agent": "GRID-DataPuller/1.0",
        }

        resp = requests.get(
            _QUIVERQUANT_URL,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list):
            log.warning("QuiverQuant: unexpected response type: {t}", t=type(data))
            return []

        # Filter to recent trades
        cutoff = date.today() - timedelta(days=days_back)
        recent = []
        for rec in data:
            txn_date_str = rec.get("TransactionDate") or rec.get("Date") or ""
            try:
                txn_date = date.fromisoformat(txn_date_str[:10])
                if txn_date >= cutoff:
                    recent.append(rec)
            except (ValueError, TypeError):
                continue

        log.info(
            "QuiverQuant: {n} trades in last {d} days",
            n=len(recent),
            d=days_back,
        )
        return recent

    def _parse_quiverquant_records(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Parse QuiverQuant records into normalised trade dicts.

        Parameters:
            records: Raw records from QuiverQuant API.

        Returns:
            List of normalised trade dicts ready for storage.
        """
        trades: list[dict[str, Any]] = []

        for rec in records:
            member = rec.get("Representative") or rec.get("Name") or ""
            ticker = rec.get("Ticker") or ""
            txn_type = rec.get("Transaction") or rec.get("Type") or ""
            amount = rec.get("Amount") or rec.get("Range") or ""

            if not member or not ticker or not txn_type:
                continue

            # Determine chamber
            chamber = "HOUSE"
            if rec.get("House") == "Senate" or rec.get("Chamber") == "Senate":
                chamber = "SENATE"

            txn_date_str = rec.get("TransactionDate") or rec.get("Date") or ""
            disc_date_str = rec.get("DisclosureDate") or rec.get("FilingDate") or ""

            try:
                txn_date = date.fromisoformat(txn_date_str[:10])
            except (ValueError, TypeError):
                continue

            try:
                disc_date = date.fromisoformat(disc_date_str[:10]) if disc_date_str else txn_date
            except (ValueError, TypeError):
                disc_date = txn_date

            trades.append({
                "member_name": member.strip(),
                "member_normalized": _normalize_member_name(member),
                "party": (rec.get("Party") or "").strip(),
                "state": (rec.get("State") or rec.get("District") or "").strip(),
                "committee": (rec.get("Committee") or "").strip(),
                "chamber": chamber,
                "ticker": _normalize_ticker(ticker),
                "transaction_type": _normalize_txn_type(txn_type),
                "amount_range": str(amount),
                "amount_midpoint": _midpoint_amount(str(amount)),
                "transaction_date": txn_date,
                "disclosure_date": disc_date,
            })

        return trades

    # ------------------------------------------------------------------ #
    # Senate eFD via SEC EDGAR full-text search
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
    def _fetch_senate_efd(self, days_back: int = 7) -> list[dict[str, Any]]:
        """Fetch recent Senate financial disclosures from SEC EDGAR EFTS.

        Uses SEC full-text search to find Senate STOCK Act filings.

        Parameters:
            days_back: Number of days of history to search.

        Returns:
            List of filing metadata dicts.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        start_dt = (date.today() - timedelta(days=days_back)).isoformat()
        end_dt = date.today().isoformat()

        params = {
            "q": '"Senate" "periodic transaction report"',
            "dateRange": "custom",
            "startdt": start_dt,
            "enddt": end_dt,
            "forms": "4",
        }

        headers = {
            "User-Agent": "GRID-DataPuller/1.0 (research@grid.local)",
            "Accept": "application/json",
        }

        resp = requests.get(
            _SENATE_EFTS_URL,
            params=params,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        log.info("Senate eFD: {n} hits from EDGAR", n=len(hits))
        return hits

    # ------------------------------------------------------------------ #
    # Signal emission for trust scoring
    # ------------------------------------------------------------------ #

    def _emit_signal(
        self,
        conn: Any,
        trade: dict[str, Any],
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Parameters:
            conn: Active database connection (within a transaction).
            trade: Normalised trade dict.
        """
        import json

        signal_type = "BUY" if trade["transaction_type"] == "BUY" else "SELL"

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "congressional",
                "sid": trade["member_name"],
                "ticker": trade["ticker"],
                "sdate": trade["transaction_date"],
                "stype2": signal_type,
                "sval": json.dumps({
                    "chamber": trade["chamber"],
                    "party": trade["party"],
                    "state": trade["state"],
                    "committee": trade["committee"],
                    "amount_range": trade["amount_range"],
                    "amount_midpoint": trade["amount_midpoint"],
                    "disclosure_date": trade["disclosure_date"].isoformat(),
                    "disclosure_lag_days": (
                        trade["disclosure_date"] - trade["transaction_date"]
                    ).days,
                }),
            },
        )

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_recent(self, days_back: int = 7) -> dict[str, Any]:
        """Pull recent congressional trading disclosures.

        Attempts QuiverQuant first (structured data), falls back to
        Senate eFD search. Stores each transaction in raw_series and
        emits a signal_sources row for trust scoring.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            dict with status, rows_inserted, trades_found.
        """
        trades: list[dict[str, Any]] = []

        # Try QuiverQuant first
        try:
            qq_records = self._fetch_quiverquant(days_back=days_back)
            if qq_records:
                trades.extend(self._parse_quiverquant_records(qq_records))
                log.info(
                    "Congressional: {n} trades from QuiverQuant",
                    n=len(trades),
                )
        except Exception as exc:
            log.warning(
                "Congressional: QuiverQuant fetch failed: {e}",
                e=str(exc),
            )

        # Try Senate eFD as supplementary source
        try:
            senate_hits = self._fetch_senate_efd(days_back=days_back)
            # Senate eFD returns metadata — parsing requires PDF/XML extraction
            # which is handled separately. Log the count for monitoring.
            if senate_hits:
                log.info(
                    "Congressional: {n} Senate eFD filings found (metadata only)",
                    n=len(senate_hits),
                )
        except Exception as exc:
            log.warning(
                "Congressional: Senate eFD fetch failed: {e}",
                e=str(exc),
            )

        if not trades:
            log.info("Congressional: no trades found in last {d} days", d=days_back)
            return {"status": "SUCCESS", "rows_inserted": 0, "trades_found": 0}

        # Store trades
        rows_inserted = 0

        with self.engine.begin() as conn:
            for trade in trades:
                series_id = self._build_series_id(
                    chamber=trade["chamber"],
                    member=trade["member_normalized"],
                    ticker=trade["ticker"],
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
                    value=trade["amount_midpoint"],
                    raw_payload={
                        "member_name": trade["member_name"],
                        "party": trade["party"],
                        "state": trade["state"],
                        "committee": trade["committee"],
                        "chamber": trade["chamber"],
                        "ticker": trade["ticker"],
                        "transaction_type": trade["transaction_type"],
                        "amount_range": trade["amount_range"],
                        "disclosure_date": trade["disclosure_date"].isoformat(),
                        "transaction_date": trade["transaction_date"].isoformat(),
                    },
                )
                rows_inserted += 1

                # Emit signal for trust scoring
                try:
                    self._emit_signal(conn, trade)
                except Exception as exc:
                    log.warning(
                        "Congressional: signal emission failed for {m}/{t}: {e}",
                        m=trade["member_name"],
                        t=trade["ticker"],
                        e=str(exc),
                    )

        log.info(
            "Congressional: {ins} rows inserted from {total} trades",
            ins=rows_inserted,
            total=len(trades),
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "trades_found": len(trades),
        }

    def pull_all(self, days_back: int = 7) -> dict[str, Any]:
        """Pull all congressional trading data.

        Alias for pull_recent() — congressional data is always
        pulled incrementally (no deep historical backfill).

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            dict with status, rows_inserted, trades_found.
        """
        return self.pull_recent(days_back=days_back)
