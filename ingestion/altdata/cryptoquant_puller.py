"""
CryptoQuant puller — on-chain analytics for BTC and ETH.

Exchange flows, miner flows, network indicators, derivatives metrics.
Deeper than Etherscan — institutional-grade on-chain intelligence.

API: https://docs.cryptoquant.com/
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

CQ_BASE = "https://api.cryptoquant.com/v1"


class CryptoQuantAuthError(RuntimeError):
    """Raised on 403 — key is invalid or not entitled for this endpoint.

    Deliberately outside the retry decorator's retryable tuple so the
    decorator won't retry-and-ERROR-log. Caller treats it as
    pull-cycle-terminal (every subsequent metric will 403 the same way).
    """


class CryptoQuantRateLimitedError(RuntimeError):
    """Raised on 429 — back off for the rest of this cycle."""

# Key metrics to track
BTC_METRICS = [
    # Exchange flows
    "btc/exchange-flows/inflow-total",
    "btc/exchange-flows/outflow-total",
    "btc/exchange-flows/netflow-total",
    "btc/exchange-flows/reserve",
    # Miner flows
    "btc/miner-flows/miner-to-exchange",
    "btc/miner-flows/miner-reserve",
    # Network indicators
    "btc/network-indicator/sopr",
    "btc/network-indicator/nupl",
    "btc/network-indicator/puell-multiple",
    "btc/network-indicator/mvrv-ratio",
    "btc/network-indicator/nvt-ratio",
    # Market data
    "btc/market-data/open-interest",
    "btc/market-data/funding-rates",
    "btc/market-data/estimated-leverage-ratio",
    "btc/market-data/taker-buy-sell-ratio",
]

ETH_METRICS = [
    "eth/exchange-flows/inflow-total",
    "eth/exchange-flows/outflow-total",
    "eth/exchange-flows/netflow-total",
    "eth/exchange-flows/reserve",
    "eth/network-indicator/sopr",
    "eth/market-data/open-interest",
    "eth/market-data/funding-rates",
]

# Stablecoin metrics
STABLE_METRICS = [
    "stablecoin/exchange-stablecoin-ratio",
    "stablecoin/usdt-supply",
    "stablecoin/usdc-supply",
]


class CryptoQuantPuller(BasePuller):
    """Pull on-chain analytics from CryptoQuant."""

    SOURCE_NAME = "cryptoquant"
    SOURCE_CONFIG = {
        "base_url": CQ_BASE,
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        super().__init__(db_engine)
        if api_key:
            self.api_key = api_key
        else:
            from config import settings
            self.api_key = getattr(settings, "CRYPTOQUANT_API_KEY", "")
        if not self.api_key:
            log.warning("CRYPTOQUANT_API_KEY not set — CryptoQuant puller disabled")

    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.exceptions.RequestException))
    def _api_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a CryptoQuant API call.

        Raises:
            CryptoQuantAuthError: On 403 (invalid key or unentitled
                endpoint). Non-retryable — caller aborts the rest of
                the pull cycle.
            CryptoQuantRateLimitedError: On 429. Non-retryable — caller
                aborts the rest of the pull cycle.
        """
        url = f"{CQ_BASE}/{endpoint}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
        if resp.status_code == 403:
            raise CryptoQuantAuthError(
                f"403 on {endpoint} — key invalid or endpoint unentitled"
            )
        if resp.status_code == 429:
            raise CryptoQuantRateLimitedError(f"429 on {endpoint}")
        resp.raise_for_status()
        return resp.json()

    def pull_metric(self, metric: str, window: str = "day", limit: int = 30) -> list[dict[str, Any]]:
        """Pull a single metric time series.

        Args:
            metric: Metric path (e.g. "btc/exchange-flows/inflow-total").
            window: Aggregation window (day, hour).
            limit: Number of data points.

        Returns:
            List of {date, value} dicts.
        """
        data = self._api_get(metric, {"window": window, "limit": limit})
        result = data.get("result", {}).get("data", [])
        if not isinstance(result, list):
            return []
        return result

    def pull(self) -> dict[str, Any]:
        """Pull all tracked metrics for BTC, ETH, and stablecoins.

        Returns:
            Summary with metric counts and anomalies.
        """
        if not self.api_key:
            return {"error": "CRYPTOQUANT_API_KEY not configured"}

        all_metrics = BTC_METRICS + ETH_METRICS + STABLE_METRICS
        total_stored = 0
        total_metrics = 0
        anomalies: list[dict[str, Any]] = []
        api_calls = 0

        for metric in all_metrics:
            try:
                data = self.pull_metric(metric, window="day", limit=30)
                api_calls += 1
                time.sleep(0.5)  # Rate limit

                if not data:
                    continue

                total_metrics += 1
                # Sanitize metric name for series_id
                series_name = metric.replace("/", ":").replace("-", "_")

                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(f"cq:{series_name}", conn)

                    for point in data:
                        # CryptoQuant returns timestamps or date strings
                        ts = point.get("date", point.get("datetime", point.get("timestamp", "")))
                        value = point.get("value", point.get("close", point.get("data")))

                        if not ts or value is None:
                            continue

                        try:
                            if isinstance(ts, (int, float)):
                                obs_date = datetime.utcfromtimestamp(ts).date()
                            else:
                                obs_date = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
                        except (ValueError, TypeError, OSError):
                            continue

                        if obs_date in existing:
                            continue

                        try:
                            self._insert_raw(conn,
                                series_id=f"cq:{series_name}",
                                obs_date=obs_date,
                                value=float(value),
                                raw_payload={"metric": metric, "window": "day"},
                            )
                            total_stored += 1
                        except (ValueError, TypeError):
                            pass

                # Anomaly detection: exchange netflow spikes
                if "netflow" in metric and len(data) >= 7:
                    values = [float(d.get("value", 0)) for d in data if d.get("value") is not None]
                    if len(values) >= 7:
                        import numpy as np
                        recent = values[-1]
                        mean = np.mean(values[:-1])
                        std = np.std(values[:-1])
                        if std > 0 and abs(recent - mean) / std > 3:
                            anomalies.append({
                                "metric": metric,
                                "value": recent,
                                "z_score": round((recent - mean) / std, 2),
                                "direction": "inflow" if recent > mean else "outflow",
                            })

            except CryptoQuantAuthError as exc:
                log.warning(
                    "CryptoQuant auth failure ({e}); aborting pull cycle. "
                    "Check CRYPTOQUANT_API_KEY entitlements.",
                    e=str(exc),
                )
                return {
                    "metrics_pulled": total_metrics,
                    "data_points_stored": total_stored,
                    "anomalies": anomalies,
                    "api_calls": api_calls,
                    "error": "auth_failed",
                }
            except CryptoQuantRateLimitedError as exc:
                log.warning(
                    "CryptoQuant rate-limited ({e}); aborting pull cycle",
                    e=str(exc),
                )
                return {
                    "metrics_pulled": total_metrics,
                    "data_points_stored": total_stored,
                    "anomalies": anomalies,
                    "api_calls": api_calls,
                    "error": "rate_limited",
                }
            except Exception as exc:
                log.debug("CryptoQuant metric {m} failed: {e}", m=metric, e=str(exc))

        log.info(
            "CryptoQuant: {m} metrics, {s} data points, {a} anomalies, {api} API calls",
            m=total_metrics, s=total_stored, a=len(anomalies), api=api_calls,
        )

        return {
            "metrics_pulled": total_metrics,
            "data_points_stored": total_stored,
            "anomalies": anomalies,
            "api_calls": api_calls,
        }
