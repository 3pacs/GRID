"""
Financial Modeling Prep puller — earnings, financials, transcripts, calendars.

FMP provides institutional-grade fundamental data:
- Earnings (EPS actual/estimate, surprise %, revenue)
- Income statements, balance sheets, cash flow (quarterly + annual)
- Earnings call transcripts
- Earnings calendar (upcoming dates)
- Analyst estimates (EPS, revenue forward)
- Stock screener, sector performance
- IPO calendar, stock splits, dividends

API: https://site.financialmodelingprep.com/developer/docs
Free tier: 250 requests/day — enough for daily earnings tracking.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

FMP_BASE = "https://financialmodelingprep.com/api/v3"

# Top 120 tickers to track earnings for
EARNINGS_UNIVERSE: list[str] = [
    # Mega-cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "CRM",
    "AMD", "INTC", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "NOW", "INTU", "ADBE",
    # Financials
    "JPM", "V", "MA", "BRK-B", "GS", "MS", "BLK", "SCHW", "AXP", "C",
    "BAC", "WFC", "PNC", "USB", "TFC", "CME", "ICE", "CB", "MMC", "AIG",
    # Healthcare
    "UNH", "JNJ", "ABBV", "MRK", "PFE", "LLY", "TMO", "ABT", "DHR", "BMY",
    "GILD", "VRTX", "REGN", "CI", "HUM", "ISRG", "SYK", "ZTS", "MDT", "ELV",
    # Consumer
    "PG", "KO", "PEP", "COST", "WMT", "HD", "LOW", "MCD", "NKE", "SBUX",
    "TGT", "CL", "EL", "MDLZ", "PM", "MO", "DG", "DLTR", "ROST", "TJX",
    # Industrial / Energy / Other
    "CAT", "BA", "HON", "UNP", "RTX", "GD", "DE", "MMM", "GE", "LMT",
    "XOM", "CVX", "COP", "SLB", "EOG", "NEE", "DUK", "SO", "AEP", "D",
    # Crypto / Fintech / Growth
    "COIN", "SQ", "SHOP", "PLTR", "CRWD", "SNOW", "DDOG", "NET", "ZS", "SMCI",
    "ARM", "MARA", "RIOT", "PYPL", "FIS", "NFLX", "ABNB", "UBER", "DASH", "RBLX",
]


class FMPPuller(BasePuller):
    """Pull earnings and financial data from Financial Modeling Prep."""

    SOURCE_NAME = "fmp"
    SOURCE_CONFIG = {
        "base_url": FMP_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        super().__init__(db_engine)
        if api_key:
            self.api_key = api_key
        else:
            from config import settings
            self.api_key = getattr(settings, "FMP_API_KEY", "")
        if not self.api_key:
            log.warning("FMP_API_KEY not set — FMP puller disabled")

    @retry_on_failure(max_attempts=2, retryable_exceptions=(ConnectionError, TimeoutError, OSError))
    def _api_get(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Make an FMP API call.

        Raises on 403 immediately (plan limitation, not transient).
        Retries only on connection/timeout errors.

        Args:
            endpoint: API path after /api/v3/
            params: Additional query params.

        Returns:
            JSON response (list or dict).
        """
        url = f"{FMP_BASE}/{endpoint}"
        query = {"apikey": self.api_key}
        if params:
            query.update(params)

        resp = requests.get(url, params=query, timeout=30)

        # 403 = paid endpoint, don't retry
        if resp.status_code == 403:
            log.debug("FMP 403 on {ep} — endpoint requires paid plan", ep=endpoint)
            return []

        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Earnings
    # ------------------------------------------------------------------

    def pull_earnings_history(self, ticker: str) -> list[dict[str, Any]]:
        """Pull historical earnings for a ticker.

        Returns list of: date, epsActual, epsEstimated, revenueActual,
        revenueEstimated, surprise, surprisePercent.
        """
        data = self._api_get(f"historical/earning_calendar/{ticker}", {"limit": 40})
        if not isinstance(data, list):
            return []
        return data

    def pull_earnings_calendar(self, from_date: date | None = None, to_date: date | None = None) -> list[dict[str, Any]]:
        """Pull upcoming earnings calendar.

        Args:
            from_date: Start date (default: today).
            to_date: End date (default: +30 days).

        Returns:
            List of upcoming earnings events.
        """
        if from_date is None:
            from_date = date.today()
        if to_date is None:
            to_date = from_date + timedelta(days=30)

        return self._api_get("earning_calendar", {
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
        })

    def pull_analyst_estimates(self, ticker: str) -> list[dict[str, Any]]:
        """Pull analyst EPS/revenue estimates."""
        data = self._api_get(f"analyst-estimates/{ticker}", {"limit": 12})
        if not isinstance(data, list):
            return []
        return data

    # ------------------------------------------------------------------
    # Financial statements
    # ------------------------------------------------------------------

    def pull_income_statement(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Pull income statements (quarterly or annual).

        Args:
            ticker: Stock ticker.
            period: "quarter" or "annual".
        """
        data = self._api_get(f"income-statement/{ticker}", {"period": period, "limit": 20})
        if not isinstance(data, list):
            return []
        return data

    def pull_balance_sheet(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Pull balance sheet data."""
        data = self._api_get(f"balance-sheet-statement/{ticker}", {"period": period, "limit": 20})
        if not isinstance(data, list):
            return []
        return data

    def pull_cash_flow(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Pull cash flow statements."""
        data = self._api_get(f"cash-flow-statement/{ticker}", {"period": period, "limit": 20})
        if not isinstance(data, list):
            return []
        return data

    # ------------------------------------------------------------------
    # Earnings transcripts
    # ------------------------------------------------------------------

    def pull_transcript(self, ticker: str, year: int, quarter: int) -> dict[str, Any] | None:
        """Pull earnings call transcript.

        Args:
            ticker: Stock ticker.
            year: Year (e.g. 2025).
            quarter: Quarter (1-4).

        Returns:
            Transcript dict with 'content' field, or None.
        """
        data = self._api_get(f"earning_call_transcript/{ticker}", {
            "year": year, "quarter": quarter,
        })
        if isinstance(data, list) and data:
            return data[0]
        return None

    # ------------------------------------------------------------------
    # Free-tier endpoints (always available)
    # ------------------------------------------------------------------

    def pull_quote(self, ticker: str) -> dict[str, Any] | None:
        """Pull real-time quote (price, change, volume, market cap). FREE TIER."""
        data = self._api_get(f"quote/{ticker}")
        if isinstance(data, list) and data:
            return data[0]
        return None

    def pull_profile(self, ticker: str) -> dict[str, Any] | None:
        """Pull company profile (sector, industry, description, CEO). FREE TIER."""
        data = self._api_get(f"profile/{ticker}")
        if isinstance(data, list) and data:
            return data[0]
        return None

    def pull_quote_batch(self, tickers: list[str]) -> list[dict[str, Any]]:
        """Pull quotes for multiple tickers at once. FREE TIER."""
        ticker_str = ",".join(tickers[:50])  # Max 50 per call
        data = self._api_get(f"quote/{ticker_str}")
        if isinstance(data, list):
            return data
        return []

    # ------------------------------------------------------------------
    # Paid-tier endpoints (gracefully returns [] on 403)
    # ------------------------------------------------------------------

    def pull_sector_performance(self) -> list[dict[str, Any]]:
        """Pull sector performance data. May require paid plan."""
        return self._api_get("sector-performance")

    # ------------------------------------------------------------------
    # Main pull
    # ------------------------------------------------------------------

    def pull(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull earnings data for all tracked tickers.

        Args:
            tickers: Override ticker list. Defaults to EARNINGS_UNIVERSE.

        Returns:
            Summary with counts and anomalies.
        """
        if not self.api_key:
            return {"error": "FMP_API_KEY not configured", "earnings": 0}

        if tickers is None:
            tickers = EARNINGS_UNIVERSE

        total_earnings = 0
        total_estimates = 0
        total_quotes = 0
        beats: list[dict[str, Any]] = []
        misses: list[dict[str, Any]] = []
        api_calls = 0
        paid_tier_available = True  # Will flip to False on first 403

        # Always pull batch quotes first (free tier, very efficient)
        for i in range(0, len(tickers), 50):
            batch = tickers[i:i + 50]
            try:
                quotes = self.pull_quote_batch(batch)
                api_calls += 1
                time.sleep(0.5)
                if quotes:
                    with self.engine.begin() as conn:
                        for q in quotes:
                            t = q.get("symbol", "")
                            if not t:
                                continue
                            obs = date.today()
                            for field, suffix in [
                                ("price", "price"), ("changesPercentage", "change_pct"),
                                ("volume", "volume"), ("marketCap", "market_cap"),
                                ("pe", "pe_ratio"), ("eps", "eps_ttm"),
                            ]:
                                val = q.get(field)
                                if val is not None:
                                    try:
                                        self._insert_raw(conn,
                                            series_id=f"fmp:{t}:{suffix}",
                                            obs_date=obs, value=float(val),
                                        )
                                        total_quotes += 1
                                    except (ValueError, TypeError):
                                        pass
            except Exception as exc:
                log.debug("FMP batch quote failed: {e}", e=str(exc))

        for ticker in tickers:
            if api_calls >= 240:
                log.warning("FMP daily limit approaching ({n} calls), stopping", n=api_calls)
                break

            if not paid_tier_available:
                continue  # Skip paid endpoints if 403 detected

            try:
                # Pull earnings history (may require paid plan)
                earnings = self.pull_earnings_history(ticker)
                api_calls += 1
                time.sleep(0.5)

                if not earnings:
                    # Empty list from 403 handler — paid tier not available
                    if api_calls <= 3:
                        paid_tier_available = False
                        log.info("FMP paid tier not available — using free-tier quotes only")
                        continue
                    continue

                with self.engine.begin() as conn:
                    existing_eps = self._get_existing_dates(f"earnings:{ticker}:eps_actual", conn)
                    existing_rev = self._get_existing_dates(f"earnings:{ticker}:revenue_actual", conn)

                    for e in earnings:
                        report_date = e.get("date", "")
                        if not report_date:
                            continue

                        try:
                            obs_date = datetime.strptime(report_date, "%Y-%m-%d").date()
                        except (ValueError, TypeError):
                            continue

                        eps_actual = e.get("eps")
                        eps_estimate = e.get("epsEstimated")
                        rev_actual = e.get("revenue")
                        rev_estimate = e.get("revenueEstimated")

                        # Store EPS actual
                        if eps_actual is not None and obs_date not in existing_eps:
                            self._insert_raw(conn,
                                series_id=f"earnings:{ticker}:eps_actual",
                                obs_date=obs_date,
                                value=float(eps_actual),
                                raw_payload={
                                    "ticker": ticker,
                                    "eps_actual": eps_actual,
                                    "eps_estimate": eps_estimate,
                                    "revenue_actual": rev_actual,
                                    "revenue_estimate": rev_estimate,
                                    "fiscal_period": e.get("fiscalDateEnding", ""),
                                },
                            )
                            total_earnings += 1

                        # Store EPS estimate
                        if eps_estimate is not None and obs_date not in existing_eps:
                            self._insert_raw(conn,
                                series_id=f"earnings:{ticker}:eps_estimate",
                                obs_date=obs_date,
                                value=float(eps_estimate),
                            )
                            total_estimates += 1

                        # Store revenue
                        if rev_actual is not None and obs_date not in existing_rev:
                            self._insert_raw(conn,
                                series_id=f"earnings:{ticker}:revenue_actual",
                                obs_date=obs_date,
                                value=float(rev_actual),
                                raw_payload={"ticker": ticker},
                            )

                        if rev_estimate is not None and obs_date not in existing_rev:
                            self._insert_raw(conn,
                                series_id=f"earnings:{ticker}:revenue_estimate",
                                obs_date=obs_date,
                                value=float(rev_estimate),
                            )

                        # Surprise detection
                        if eps_actual is not None and eps_estimate is not None and eps_estimate != 0:
                            surprise_pct = (eps_actual - eps_estimate) / abs(eps_estimate) * 100

                            self._insert_raw(conn,
                                series_id=f"earnings:{ticker}:surprise_pct",
                                obs_date=obs_date,
                                value=round(surprise_pct, 2),
                            )

                            if surprise_pct > 10:
                                beats.append({
                                    "ticker": ticker,
                                    "date": report_date,
                                    "surprise_pct": round(surprise_pct, 2),
                                    "eps_actual": eps_actual,
                                    "eps_estimate": eps_estimate,
                                })
                            elif surprise_pct < -10:
                                misses.append({
                                    "ticker": ticker,
                                    "date": report_date,
                                    "surprise_pct": round(surprise_pct, 2),
                                    "eps_actual": eps_actual,
                                    "eps_estimate": eps_estimate,
                                })

            except Exception as exc:
                log.debug("FMP earnings pull failed for {t}: {e}", t=ticker, e=str(exc))

        # Pull sector performance (1 API call)
        try:
            sectors = self.pull_sector_performance()
            api_calls += 1
            if isinstance(sectors, list):
                with self.engine.begin() as conn:
                    for s in sectors:
                        sector = s.get("sector", "")
                        change = s.get("changesPercentage", "")
                        if sector and change:
                            try:
                                self._insert_raw(conn,
                                    series_id=f"fmp:sector:{sector.lower().replace(' ', '_')}",
                                    obs_date=date.today(),
                                    value=float(str(change).rstrip("%")),
                                    raw_payload=s,
                                )
                            except (ValueError, TypeError):
                                pass
        except Exception as exc:
            log.debug("FMP sector performance failed: {e}", e=str(exc))

        # Pull upcoming earnings calendar (1 API call)
        try:
            calendar = self.pull_earnings_calendar()
            api_calls += 1
            if isinstance(calendar, list):
                with self.engine.begin() as conn:
                    for event in calendar[:200]:  # Top 200 upcoming
                        ticker = event.get("symbol", "")
                        report_date = event.get("date", "")
                        if ticker and report_date:
                            try:
                                obs_date = datetime.strptime(report_date, "%Y-%m-%d").date()
                                self._insert_raw(conn,
                                    series_id=f"earnings:calendar:{ticker}",
                                    obs_date=obs_date,
                                    value=1.0,
                                    raw_payload=event,
                                )
                            except (ValueError, TypeError):
                                pass
        except Exception as exc:
            log.debug("FMP earnings calendar failed: {e}", e=str(exc))

        log.info(
            "FMP pull: {q} quotes, {e} earnings, {b} beats, {m} misses, {api} API calls (paid_tier={pt})",
            q=total_quotes, e=total_earnings, b=len(beats), m=len(misses),
            api=api_calls, pt=paid_tier_available,
        )

        return {
            "quotes_stored": total_quotes,
            "earnings_stored": total_earnings,
            "estimates_stored": total_estimates,
            "beats": beats,
            "misses": misses,
            "api_calls": api_calls,
            "paid_tier_available": paid_tier_available,
        }

    def pull_financials(self, ticker: str) -> dict[str, Any]:
        """Pull full financial statements for a single ticker.

        Pulls income statement, balance sheet, and cash flow (quarterly).
        Stores key metrics in raw_series.

        Args:
            ticker: Stock ticker.

        Returns:
            Summary of stored records.
        """
        if not self.api_key:
            return {"error": "no_api_key"}

        stored = 0

        # Income statement
        try:
            income = self.pull_income_statement(ticker)
            time.sleep(0.5)
            with self.engine.begin() as conn:
                for stmt in income:
                    report_date = stmt.get("date", "")
                    if not report_date:
                        continue
                    try:
                        obs_date = datetime.strptime(report_date, "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        continue

                    # Store key income metrics
                    for field, series_suffix in [
                        ("revenue", "revenue"),
                        ("grossProfit", "gross_profit"),
                        ("operatingIncome", "operating_income"),
                        ("netIncome", "net_income"),
                        ("eps", "eps"),
                        ("ebitda", "ebitda"),
                    ]:
                        val = stmt.get(field)
                        if val is not None:
                            self._insert_raw(conn,
                                series_id=f"fmp:{ticker}:{series_suffix}",
                                obs_date=obs_date,
                                value=float(val),
                                raw_payload={"period": stmt.get("period", ""), "source": "income_statement"},
                            )
                            stored += 1
        except Exception as exc:
            log.debug("FMP income statement failed for {t}: {e}", t=ticker, e=str(exc))

        # Balance sheet
        try:
            balance = self.pull_balance_sheet(ticker)
            time.sleep(0.5)
            with self.engine.begin() as conn:
                for stmt in balance:
                    report_date = stmt.get("date", "")
                    if not report_date:
                        continue
                    try:
                        obs_date = datetime.strptime(report_date, "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        continue

                    for field, series_suffix in [
                        ("totalAssets", "total_assets"),
                        ("totalDebt", "total_debt"),
                        ("cashAndCashEquivalents", "cash"),
                        ("totalStockholdersEquity", "equity"),
                        ("totalCurrentAssets", "current_assets"),
                        ("totalCurrentLiabilities", "current_liabilities"),
                    ]:
                        val = stmt.get(field)
                        if val is not None:
                            self._insert_raw(conn,
                                series_id=f"fmp:{ticker}:{series_suffix}",
                                obs_date=obs_date,
                                value=float(val),
                                raw_payload={"period": stmt.get("period", ""), "source": "balance_sheet"},
                            )
                            stored += 1
        except Exception as exc:
            log.debug("FMP balance sheet failed for {t}: {e}", t=ticker, e=str(exc))

        # Cash flow
        try:
            cashflow = self.pull_cash_flow(ticker)
            time.sleep(0.5)
            with self.engine.begin() as conn:
                for stmt in cashflow:
                    report_date = stmt.get("date", "")
                    if not report_date:
                        continue
                    try:
                        obs_date = datetime.strptime(report_date, "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        continue

                    for field, series_suffix in [
                        ("operatingCashFlow", "operating_cf"),
                        ("capitalExpenditure", "capex"),
                        ("freeCashFlow", "free_cf"),
                        ("dividendsPaid", "dividends_paid"),
                        ("commonStockRepurchased", "buybacks"),
                    ]:
                        val = stmt.get(field)
                        if val is not None:
                            self._insert_raw(conn,
                                series_id=f"fmp:{ticker}:{series_suffix}",
                                obs_date=obs_date,
                                value=float(val),
                                raw_payload={"period": stmt.get("period", ""), "source": "cash_flow"},
                            )
                            stored += 1
        except Exception as exc:
            log.debug("FMP cash flow failed for {t}: {e}", t=ticker, e=str(exc))

        log.info("FMP financials for {t}: {n} records stored", t=ticker, n=stored)
        return {"ticker": ticker, "records_stored": stored}
