"""
Polygon.io puller — stocks, options with Greeks, crypto, forex, dividends.

This is the Swiss Army knife data source. Polygon provides:
- Options snapshots with full Greeks (delta, gamma, theta, vega, IV)
- Stock snapshots (price, volume, VWAP, pre/post market)
- Ticker details (sector, SIC code, market cap, description)
- Dividends and stock splits
- Crypto tickers and forex pairs

API: https://polygon.io/docs
Free tier: 5 API calls/minute. Paid: unlimited.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

POLYGON_BASE = "https://api.polygon.io"

# Top tickers for options Greeks tracking
OPTIONS_TICKERS = [
    "SPY", "QQQ", "IWM", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "AMD", "JPM", "GS", "BAC", "XLF", "XLE", "XLK", "XLV", "GLD", "SLV",
    "TLT", "HYG", "VIX", "COIN", "MARA", "PLTR", "CRWD", "SMCI", "ARM", "NFLX",
]


class PolygonPuller(BasePuller):
    """Pull stocks, options Greeks, crypto, and reference data from Polygon.io."""

    SOURCE_NAME = "polygon"
    SOURCE_CONFIG = {
        "base_url": POLYGON_BASE,
        "cost_tier": "PAID",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 10,
    }

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        super().__init__(db_engine)
        if api_key:
            self.api_key = api_key
        else:
            from config import settings
            self.api_key = getattr(settings, "POLYGON_API_KEY", "")
        if not self.api_key:
            log.warning("POLYGON_API_KEY not set — Polygon puller disabled")

    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.exceptions.RequestException))
    def _api_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a Polygon API call."""
        url = f"{POLYGON_BASE}{endpoint}"
        query = {"apiKey": self.api_key}
        if params:
            query.update(params)
        resp = requests.get(url, params=query, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Options with Greeks
    # ------------------------------------------------------------------

    def pull_options_chain(self, ticker: str) -> list[dict[str, Any]]:
        """Pull full options chain snapshot with Greeks for a ticker.

        Returns options contracts with: strike, expiry, type (call/put),
        bid, ask, last, volume, OI, IV, delta, gamma, theta, vega.

        Uses /v3/snapshot/options if available (paid), falls back to
        /v3/reference/options/contracts (free) + individual snapshots.
        """
        # Try snapshot endpoint first (paid tier, has Greeks inline)
        try:
            data = self._api_get(f"/v3/snapshot/options/{ticker}", {
                "limit": 250,
                "order": "desc",
                "sort": "volume",
            })
            results = data.get("results", [])
            if results:
                return results
        except Exception:
            pass

        # Fallback: get contract list (free tier) then fetch snapshots
        try:
            data = self._api_get("/v3/reference/options/contracts", {
                "underlying_ticker": ticker,
                "limit": 100,
                "order": "desc",
                "sort": "open_interest",
            })
            contracts = data.get("results", [])
            if not contracts:
                return []

            # For free tier, we get contract metadata but not live Greeks
            # Return what we have — the summary method handles missing Greeks
            return [{"details": c, "greeks": {}, "day": {}} for c in contracts]
        except Exception:
            return []

    def pull_options_greeks_summary(self, ticker: str) -> dict[str, Any]:
        """Pull and summarize options Greeks for a ticker.

        Computes aggregate Greeks: total call/put OI, put/call ratio,
        volume-weighted IV, net delta exposure (GEX proxy).
        """
        chain = self.pull_options_chain(ticker)
        if not chain:
            return {}

        total_call_oi = 0
        total_put_oi = 0
        total_call_vol = 0
        total_put_vol = 0
        weighted_iv_sum = 0.0
        weighted_iv_vol = 0
        net_gamma = 0.0
        net_delta = 0.0

        for contract in chain:
            details = contract.get("details", {})
            greeks = contract.get("greeks", {})
            day = contract.get("day", {})

            contract_type = details.get("contract_type", "")
            oi = contract.get("open_interest", 0) or 0
            vol = day.get("volume", 0) or 0
            iv = contract.get("implied_volatility", 0) or 0
            delta = greeks.get("delta", 0) or 0
            gamma = greeks.get("gamma", 0) or 0

            if contract_type == "call":
                total_call_oi += oi
                total_call_vol += vol
            elif contract_type == "put":
                total_put_oi += oi
                total_put_vol += vol

            if vol > 0 and iv > 0:
                weighted_iv_sum += iv * vol
                weighted_iv_vol += vol

            # Net gamma/delta (calls positive, puts negative)
            multiplier = 100  # standard options multiplier
            sign = 1 if contract_type == "call" else -1
            net_gamma += gamma * oi * multiplier * sign
            net_delta += delta * oi * multiplier * sign

        put_call_ratio = total_put_oi / total_call_oi if total_call_oi > 0 else 0
        avg_iv = weighted_iv_sum / weighted_iv_vol if weighted_iv_vol > 0 else 0

        return {
            "ticker": ticker,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "total_call_volume": total_call_vol,
            "total_put_volume": total_put_vol,
            "put_call_ratio": round(put_call_ratio, 4),
            "avg_implied_vol": round(avg_iv, 4),
            "net_gamma_exposure": round(net_gamma, 2),
            "net_delta_exposure": round(net_delta, 2),
            "contracts_count": len(chain),
        }

    # ------------------------------------------------------------------
    # Stock snapshots
    # ------------------------------------------------------------------

    def pull_stock_snapshot(self, ticker: str) -> dict[str, Any] | None:
        """Pull current stock snapshot (price, volume, VWAP)."""
        data = self._api_get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}")
        return data.get("ticker", None)

    def pull_market_snapshot(self) -> list[dict[str, Any]]:
        """Pull snapshot of all US stock tickers."""
        data = self._api_get("/v2/snapshot/locale/us/markets/stocks/tickers")
        return data.get("tickers", [])

    # ------------------------------------------------------------------
    # Dividends and splits
    # ------------------------------------------------------------------

    def pull_dividends(self, ticker: str) -> list[dict[str, Any]]:
        """Pull dividend history."""
        data = self._api_get("/v3/reference/dividends", {
            "ticker": ticker, "limit": 50, "order": "desc",
        })
        return data.get("results", [])

    # ------------------------------------------------------------------
    # Ticker details
    # ------------------------------------------------------------------

    def pull_ticker_details(self, ticker: str) -> dict[str, Any] | None:
        """Pull ticker metadata (sector, industry, market cap, description)."""
        data = self._api_get(f"/v3/reference/tickers/{ticker}")
        return data.get("results", None)

    # ------------------------------------------------------------------
    # Main pull
    # ------------------------------------------------------------------

    def pull(self) -> dict[str, Any]:
        """Pull options Greeks summaries for tracked tickers.

        Returns:
            Summary with options data counts.
        """
        if not self.api_key:
            return {"error": "POLYGON_API_KEY not configured"}

        today = date.today()
        total_tickers = 0
        api_calls = 0
        anomalies: list[dict[str, Any]] = []

        for ticker in OPTIONS_TICKERS:
            try:
                summary = self.pull_options_greeks_summary(ticker)
                api_calls += 1

                # Rate limit: free tier is 5/min
                time.sleep(12.5)  # ~5 calls/min

                if not summary:
                    continue

                total_tickers += 1

                with self.engine.begin() as conn:
                    # Store aggregate Greeks
                    self._insert_raw(conn,
                        series_id=f"polygon:{ticker}:put_call_ratio",
                        obs_date=today,
                        value=summary["put_call_ratio"],
                        raw_payload=summary,
                    )
                    self._insert_raw(conn,
                        series_id=f"polygon:{ticker}:avg_iv",
                        obs_date=today,
                        value=summary["avg_implied_vol"],
                    )
                    self._insert_raw(conn,
                        series_id=f"polygon:{ticker}:net_gamma",
                        obs_date=today,
                        value=summary["net_gamma_exposure"],
                    )
                    self._insert_raw(conn,
                        series_id=f"polygon:{ticker}:net_delta",
                        obs_date=today,
                        value=summary["net_delta_exposure"],
                    )
                    self._insert_raw(conn,
                        series_id=f"polygon:{ticker}:call_oi",
                        obs_date=today,
                        value=float(summary["total_call_oi"]),
                    )
                    self._insert_raw(conn,
                        series_id=f"polygon:{ticker}:put_oi",
                        obs_date=today,
                        value=float(summary["total_put_oi"]),
                    )

                # Anomaly: extreme put/call ratio
                pcr = summary["put_call_ratio"]
                if pcr > 1.5:
                    anomalies.append({
                        "ticker": ticker, "signal": "extreme_put_call",
                        "put_call_ratio": pcr, "direction": "bearish",
                    })
                elif pcr < 0.5 and pcr > 0:
                    anomalies.append({
                        "ticker": ticker, "signal": "extreme_put_call",
                        "put_call_ratio": pcr, "direction": "bullish",
                    })

            except Exception as exc:
                log.debug("Polygon options failed for {t}: {e}", t=ticker, e=str(exc))

        log.info("Polygon: {t} tickers, {a} anomalies, {api} API calls",
                 t=total_tickers, a=len(anomalies), api=api_calls)

        return {
            "tickers_pulled": total_tickers,
            "anomalies": anomalies,
            "api_calls": api_calls,
        }
