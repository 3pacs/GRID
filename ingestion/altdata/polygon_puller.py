"""
Polygon.io puller — stocks, options with Greeks, crypto, forex, dividends.

This is the Swiss Army knife data source. Polygon provides:
- Options snapshots with full Greeks (delta, gamma, theta, vega, IV)
- Stock snapshots (price, volume, VWAP, pre/post market)
- Ticker details (sector, SIC code, market cap, description)
- Dividends and stock splits
- Crypto tickers and forex pairs

API: https://polygon.io/docs (Polygon.io is now Massive; api.polygon.io still serves)
Free tier: 5 API calls/minute. Paid: unlimited.

Entitlement (probed from grid-svr 2026-11-04): the configured key gets
``403 NOT_AUTHORIZED`` on ``/v3/snapshot/options/{ticker}`` -- the options
snapshot (OI, volume, Greeks) needs an options plan. The free reference
endpoint lists contracts without OI or Greeks, so a "summary" built from it
is all zeros. ``pull()`` therefore stops on the first entitlement refusal and
reports SKIPPED with the operator action instead of storing zero-valued
Greeks for 30 tickers.

The key is sent as an ``Authorization: Bearer`` header, never as the
``apiKey`` query parameter: request URLs end up in exception text and logs.
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

# Polygon's documented status for a plan that does not include the endpoint.
_NOT_ENTITLED_STATUS = "NOT_AUTHORIZED"
_NOT_ENTITLED_REASON = (
    "Polygon/Massive plan is not entitled to /v3/snapshot/options "
    "(HTTP 403 NOT_AUTHORIZED); options Greeks need an options plan "
    "upgrade, or retire this source"
)
_entitlement_warned = False

# Top tickers for options Greeks tracking
OPTIONS_TICKERS = [
    "SPY", "QQQ", "IWM", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "AMD", "JPM", "GS", "BAC", "XLF", "XLE", "XLK", "XLV", "GLD", "SLV",
    "TLT", "HYG", "VIX", "COIN", "MARA", "PLTR", "CRWD", "SMCI", "ARM", "NFLX",
]


class PolygonNotEntitled(RuntimeError):
    """The API key's plan does not include the requested endpoint (permanent)."""


class _PolygonHTTPError(RuntimeError):
    """Non-2xx Polygon response; the message never contains the request URL."""

    def __init__(self, status_code: int, endpoint: str) -> None:
        self.status_code = status_code
        super().__init__(f"Polygon HTTP {status_code} on {endpoint}")


def _inserted(result: Any) -> int:
    """Committed-row contribution of one INSERT; unknown counts fail closed."""
    affected = getattr(result, "rowcount", None)
    if type(affected) is not int or affected < 0:
        raise ValueError("Unknown affected row count")
    return affected


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
        """Make a Polygon API call (key in the Authorization header).

        Raises:
            PolygonNotEntitled: 403 whose body says the plan lacks the endpoint.
            _PolygonHTTPError: any other non-2xx status (not retried).
        """
        url = f"{POLYGON_BASE}{endpoint}"
        resp = requests.get(
            url,
            params=params or None,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30,
        )
        if resp.status_code == 403:
            try:
                status = (resp.json() or {}).get("status")
            except ValueError:
                status = None
            if status == _NOT_ENTITLED_STATUS:
                raise PolygonNotEntitled(f"{endpoint}: {_NOT_ENTITLED_STATUS}")
        if resp.status_code >= 400:
            raise _PolygonHTTPError(resp.status_code, endpoint)
        return resp.json()

    # ------------------------------------------------------------------
    # Options with Greeks
    # ------------------------------------------------------------------

    def pull_options_chain(self, ticker: str) -> list[dict[str, Any]]:
        """Pull the options chain snapshot with Greeks for a ticker.

        Returns options contracts with: strike, expiry, type (call/put),
        bid, ask, last, volume, OI, IV, delta, gamma, theta, vega.

        Uses ``/v3/snapshot/options`` (options plan). There is deliberately
        no fallback to ``/v3/reference/options/contracts``: that list has no
        open interest, volume or Greeks, so every aggregate computed from it
        is zero -- not a measurement.

        Raises:
            PolygonNotEntitled: the key's plan lacks the snapshot endpoint.
        """
        data = self._api_get(f"/v3/snapshot/options/{ticker}", {
            "limit": 250,
            "order": "desc",
            "sort": "volume",
        })
        return data.get("results", []) or []

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
            Write-contract summary: ``status`` plus ``rows_inserted`` (rows
            committed to raw_series) and per-ticker coverage counts.
        """
        global _entitlement_warned
        if not self.api_key:
            return {"status": "SKIPPED", "rows_inserted": 0,
                    "skipped_reason": "POLYGON_API_KEY not configured"}

        today = date.today()
        rows_inserted = 0
        succeeded = 0
        no_data = 0
        failed = 0
        api_calls = 0
        anomalies: list[dict[str, Any]] = []

        for idx, ticker in enumerate(OPTIONS_TICKERS):
            if idx:
                time.sleep(12.5)  # free tier is 5 calls/min
            try:
                api_calls += 1
                summary = self.pull_options_greeks_summary(ticker)
            except PolygonNotEntitled:
                if succeeded or rows_inserted:
                    # Entitlement changed mid-run: keep what was committed.
                    failed += len(OPTIONS_TICKERS) - idx
                    break
                if not _entitlement_warned:
                    log.warning("Polygon: {r}", r=_NOT_ENTITLED_REASON)
                    _entitlement_warned = True
                return {"status": "SKIPPED", "rows_inserted": 0,
                        "skipped_reason": _NOT_ENTITLED_REASON, "api_calls": api_calls}
            except Exception as exc:
                failed += 1
                log.warning("Polygon options failed for {t}: {e}", t=ticker, e=str(exc))
                continue

            if not summary:
                no_data += 1
                continue

            try:
                ticker_rows = 0
                with self.engine.begin() as conn:
                    for suffix, value, payload in (
                        ("put_call_ratio", summary["put_call_ratio"], summary),
                        ("avg_iv", summary["avg_implied_vol"], None),
                        ("net_gamma", summary["net_gamma_exposure"], None),
                        ("net_delta", summary["net_delta_exposure"], None),
                        ("call_oi", float(summary["total_call_oi"]), None),
                        ("put_oi", float(summary["total_put_oi"]), None),
                    ):
                        ticker_rows += _inserted(self._insert_raw(
                            conn,
                            series_id=f"polygon:{ticker}:{suffix}",
                            obs_date=today,
                            value=value,
                            raw_payload=payload,
                            return_result=True,
                        ))
                # Counted only after the transaction committed.
                rows_inserted += ticker_rows
                succeeded += 1
            except Exception as exc:
                failed += 1
                log.warning("Polygon store failed for {t}: {e}", t=ticker, e=str(exc))
                continue

            pcr = summary["put_call_ratio"]
            if pcr > 1.5:
                anomalies.append({
                    "ticker": ticker, "signal": "extreme_put_call",
                    "put_call_ratio": pcr, "direction": "bearish",
                })
            elif 0 < pcr < 0.5:
                anomalies.append({
                    "ticker": ticker, "signal": "extreme_put_call",
                    "put_call_ratio": pcr, "direction": "bullish",
                })

        log.info("Polygon: {ok}/{n} tickers, {r} rows, {a} anomalies, {api} API calls",
                 ok=succeeded, n=len(OPTIONS_TICKERS), r=rows_inserted,
                 a=len(anomalies), api=api_calls)

        total = len(OPTIONS_TICKERS)
        if failed:
            status = "PARTIAL" if (succeeded or rows_inserted) else "FAILED"
        else:
            status = "SUCCESS"
        result: dict[str, Any] = {
            "status": status,
            "rows_inserted": rows_inserted,
            "succeeded": succeeded + no_data,
            "total": total,
            "tickers_pulled": succeeded,
            "tickers_no_data": no_data,
            "tickers_failed": failed,
            "anomalies": anomalies,
            "api_calls": api_calls,
        }
        if failed:
            result["error"] = f"{failed} of {total} tickers failed"
        return result
