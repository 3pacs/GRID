"""
GRID Cloudflare Radar data ingestion module.

Pulls internet traffic intelligence from Cloudflare Radar public API
(no API key required) as alternative data signals for financial markets:

1. HTTP traffic trends -- global internet activity proxy, correlates
   with economic activity and consumer behavior.
2. Traffic by top 30 countries -- detect regional economic shifts,
   internet shutdowns, and infrastructure failures.
3. DDoS attack trends -- cybersecurity risk indicator; attack spikes
   correlate with geopolitical tensions.
4. Traffic anomalies -- internet disruptions signal censorship,
   infrastructure failures, or geopolitical events.
5. BGP route changes -- internet infrastructure stability indicator.

Series stored:
- cf_radar:http_traffic:global     -- global HTTP request trend index
- cf_radar:traffic:{country_code}  -- per-country traffic index
- cf_radar:ddos:global             -- DDoS attack volume index
- cf_radar:anomaly:{event_id}      -- traffic anomaly events
- cf_radar:bgp:global              -- BGP route change count

Anomaly detection:
- Flags countries with >30% traffic drop (internet shutdowns,
  infrastructure failures, censorship events).
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API configuration ────────────────────────────────────────────────────

_BASE_URL: str = "https://radar.cloudflare.com/api/v1"

_ENDPOINTS: dict[str, str] = {
    "http_timeseries": f"{_BASE_URL}/http/timeseries",
    "top_locations": f"{_BASE_URL}/traffic/top/locations",
    "ddos_timeseries": f"{_BASE_URL}/attacks/layer3/timeseries",
    "anomalies": f"{_BASE_URL}/traffic/anomalies",
    "bgp_stats": f"{_BASE_URL}/bgp/routes/stats",
}

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0  # seconds between requests

# Top 30 countries by internet traffic (ISO 3166-1 alpha-2)
TOP_COUNTRIES: list[str] = [
    "US", "CN", "DE", "JP", "GB", "FR", "BR", "IN", "KR", "CA",
    "RU", "IT", "ES", "AU", "NL", "MX", "ID", "TR", "PL", "SE",
    "TH", "AR", "ZA", "SG", "TW", "CH", "PH", "VN", "MY", "CL",
]

# Series ID prefix
_SERIES_PREFIX: str = "cf_radar"

# Anomaly detection threshold: flag >30% traffic drop
_TRAFFIC_DROP_THRESHOLD: float = 0.30

_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; GRID-DataPuller/1.0; "
        "+https://github.com/grid-trading)"
    ),
    "Accept": "application/json",
}


def _resolve_cf_token() -> str | None:
    """Return a Cloudflare Radar bearer token from env/settings, or None.

    The Radar API rejects every request without a token (HTTP 403), which
    produces one ERROR per endpoint × cycle. Reading the token here lets
    the puller short-circuit with a single warning instead.
    """
    import os
    for var in ("CF_RADAR_TOKEN", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN"):
        tok = os.environ.get(var)
        if tok:
            return tok
    try:
        from config import settings
        for attr in ("CF_RADAR_TOKEN", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN"):
            tok = getattr(settings, attr, None)
            if tok:
                return tok
    except Exception:
        pass
    return None


class CloudflareRadarPuller(BasePuller):
    """Pulls internet traffic intelligence from Cloudflare Radar.

    Cloudflare handles ~20% of global web traffic, making Radar data
    a proxy for real economic activity.  Traffic drops in specific
    countries often precede or coincide with geopolitical events,
    internet shutdowns, and infrastructure failures.

    Features:
    - cf_radar:http_traffic:global  -- global HTTP request trend
    - cf_radar:traffic:{CC}         -- per-country traffic
    - cf_radar:ddos:global          -- DDoS attack volume
    - cf_radar:anomaly:{event_id}   -- traffic anomaly events
    - cf_radar:bgp:global           -- BGP route changes

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Cloudflare_Radar.
    """

    SOURCE_NAME: str = "Cloudflare_Radar"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _BASE_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the Cloudflare Radar puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "CloudflareRadarPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _series_id(category: str, key: str) -> str:
        """Build the full series_id.

        Parameters:
            category: Series category (e.g., 'http_traffic', 'ddos').
            key: Series key (e.g., 'global', 'US').

        Returns:
            Full series_id like 'cf_radar:http_traffic:global'.
        """
        return f"{_SERIES_PREFIX}:{category}:{key}"

    def _rate_limit(self) -> None:
        """Sleep to respect Cloudflare Radar rate limits."""
        time.sleep(_RATE_LIMIT_DELAY)

    # ------------------------------------------------------------------ #
    # HTTP fetchers (with retry)
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
    def _fetch_json(
        self, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Fetch JSON from a Cloudflare Radar endpoint.

        Parameters:
            url: Full API URL.
            params: Optional query parameters.

        Returns:
            Parsed JSON response dict.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        resp = requests.get(
            url,
            params=params or {},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # 1. HTTP traffic trends (global)
    # ------------------------------------------------------------------ #

    def _pull_http_traffic(
        self,
        days_back: int = 30,
    ) -> list[dict[str, Any]]:
        """Fetch global HTTP traffic timeseries.

        Parameters:
            days_back: Number of days of history to request.

        Returns:
            List of dicts with obs_date, value, raw_payload.
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)
        params = {
            "dateStart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dateEnd": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "aggInterval": "1d",
        }

        try:
            data = self._fetch_json(_ENDPOINTS["http_timeseries"], params)
        except Exception as exc:
            log.error("CF Radar HTTP traffic fetch failed: {e}", e=str(exc))
            return []

        rows: list[dict[str, Any]] = []
        series = (data.get("result") or {}).get("httpRequests", {}).get("timestamps", [])
        values = (data.get("result") or {}).get("httpRequests", {}).get("values", [])

        # Handle alternative response structures
        if not series:
            # Try flat timeseries format
            ts_list = (data.get("result") or {}).get("timeseries", [])
            for point in ts_list:
                ts_str = point.get("timestamp") or point.get("date")
                val = point.get("value") or point.get("requests")
                if ts_str is None or val is None:
                    continue
                try:
                    obs_date = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).date()
                    rows.append({
                        "obs_date": obs_date,
                        "value": float(val),
                        "raw_payload": {"timestamp": ts_str, "source": "http_timeseries"},
                    })
                except (ValueError, TypeError) as exc:
                    log.warning("CF Radar: bad timestamp {t}: {e}", t=ts_str, e=str(exc))
        else:
            for ts_str, val in zip(series, values):
                try:
                    obs_date = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).date()
                    rows.append({
                        "obs_date": obs_date,
                        "value": float(val) if val is not None else 0.0,
                        "raw_payload": {"timestamp": ts_str, "source": "http_timeseries"},
                    })
                except (ValueError, TypeError) as exc:
                    log.warning("CF Radar: bad timestamp {t}: {e}", t=ts_str, e=str(exc))

        return rows

    # ------------------------------------------------------------------ #
    # 2. Traffic by country (top 30)
    # ------------------------------------------------------------------ #

    def _pull_country_traffic(self) -> dict[str, list[dict[str, Any]]]:
        """Fetch traffic rankings for top countries.

        Returns:
            Dict mapping country code to list of row dicts.
        """
        params = {"limit": 30}

        try:
            data = self._fetch_json(_ENDPOINTS["top_locations"], params)
        except Exception as exc:
            log.error("CF Radar top locations fetch failed: {e}", e=str(exc))
            return {}

        result: dict[str, list[dict[str, Any]]] = {}
        locations = (data.get("result") or {}).get("top_0", [])

        # Also try alternative key
        if not locations:
            locations = (data.get("result") or {}).get("locations", [])
        if not locations:
            locations = data.get("result", [])
            if isinstance(locations, dict):
                locations = []

        today = date.today()
        for loc in locations:
            cc = (loc.get("clientCountryAlpha2") or loc.get("country") or "").upper()
            traffic_pct = loc.get("value") or loc.get("traffic") or loc.get("percentage")
            if not cc or traffic_pct is None:
                continue
            try:
                val = float(traffic_pct)
            except (ValueError, TypeError):
                continue

            result.setdefault(cc, []).append({
                "obs_date": today,
                "value": val,
                "raw_payload": {
                    "country": cc,
                    "name": loc.get("clientCountryName") or loc.get("name", cc),
                    "traffic_pct": val,
                    "source": "top_locations",
                },
            })

        return result

    # ------------------------------------------------------------------ #
    # 3. DDoS attack trends
    # ------------------------------------------------------------------ #

    def _pull_ddos_trends(
        self,
        days_back: int = 30,
    ) -> list[dict[str, Any]]:
        """Fetch DDoS layer-3 attack timeseries.

        Parameters:
            days_back: Number of days of history to request.

        Returns:
            List of dicts with obs_date, value, raw_payload.
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)
        params = {
            "dateStart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dateEnd": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "aggInterval": "1d",
        }

        try:
            data = self._fetch_json(_ENDPOINTS["ddos_timeseries"], params)
        except Exception as exc:
            log.error("CF Radar DDoS fetch failed: {e}", e=str(exc))
            return []

        rows: list[dict[str, Any]] = []
        result_data = data.get("result") or {}

        # Try multiple response shapes
        timestamps = result_data.get("timestamps", [])
        values = result_data.get("values", [])

        if timestamps and values:
            for ts_str, val in zip(timestamps, values):
                try:
                    obs_date = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).date()
                    rows.append({
                        "obs_date": obs_date,
                        "value": float(val) if val is not None else 0.0,
                        "raw_payload": {"timestamp": ts_str, "source": "ddos_l3"},
                    })
                except (ValueError, TypeError) as exc:
                    log.warning("CF Radar DDoS: bad ts {t}: {e}", t=ts_str, e=str(exc))
        else:
            # Flat timeseries format
            ts_list = result_data.get("timeseries", [])
            for point in ts_list:
                ts_str = point.get("timestamp") or point.get("date")
                val = point.get("value") or point.get("attacks")
                if ts_str is None or val is None:
                    continue
                try:
                    obs_date = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).date()
                    rows.append({
                        "obs_date": obs_date,
                        "value": float(val),
                        "raw_payload": {"timestamp": ts_str, "source": "ddos_l3"},
                    })
                except (ValueError, TypeError) as exc:
                    log.warning("CF Radar DDoS: bad ts {t}: {e}", t=ts_str, e=str(exc))

        return rows

    # ------------------------------------------------------------------ #
    # 4. Traffic anomalies
    # ------------------------------------------------------------------ #

    def _pull_anomalies(
        self,
        days_back: int = 30,
    ) -> list[dict[str, Any]]:
        """Fetch traffic anomaly events.

        Parameters:
            days_back: Number of days of history to request.

        Returns:
            List of dicts with obs_date, event_id, value, raw_payload.
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)
        params = {
            "dateStart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dateEnd": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        try:
            data = self._fetch_json(_ENDPOINTS["anomalies"], params)
        except Exception as exc:
            log.error("CF Radar anomalies fetch failed: {e}", e=str(exc))
            return []

        rows: list[dict[str, Any]] = []
        anomalies = (data.get("result") or {}).get("anomalies", [])
        if not anomalies:
            anomalies = data.get("result", [])
            if isinstance(anomalies, dict):
                anomalies = []

        for idx, anomaly in enumerate(anomalies):
            ts_str = (
                anomaly.get("startDate")
                or anomaly.get("timestamp")
                or anomaly.get("date")
            )
            if ts_str is None:
                continue

            event_id = anomaly.get("id") or f"evt_{idx}"
            location = anomaly.get("location") or anomaly.get("country", "unknown")
            status = anomaly.get("status", "unknown")
            description = anomaly.get("description", "")

            try:
                obs_date = datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")
                ).date()
            except (ValueError, TypeError) as exc:
                log.warning(
                    "CF Radar anomaly: bad ts {t}: {e}", t=ts_str, e=str(exc)
                )
                continue

            # Severity: use magnitude if provided, else default to 1.0
            magnitude = anomaly.get("magnitude") or anomaly.get("severity") or 1.0

            rows.append({
                "obs_date": obs_date,
                "event_id": str(event_id),
                "value": float(magnitude),
                "raw_payload": {
                    "event_id": str(event_id),
                    "location": location,
                    "status": status,
                    "description": description,
                    "start_date": ts_str,
                    "end_date": anomaly.get("endDate"),
                    "source": "traffic_anomalies",
                },
            })

        return rows

    # ------------------------------------------------------------------ #
    # 5. BGP route changes
    # ------------------------------------------------------------------ #

    def _pull_bgp_stats(self) -> list[dict[str, Any]]:
        """Fetch BGP route statistics.

        Returns:
            List of dicts with obs_date, value, raw_payload.
        """
        try:
            data = self._fetch_json(_ENDPOINTS["bgp_stats"])
        except Exception as exc:
            log.error("CF Radar BGP stats fetch failed: {e}", e=str(exc))
            return []

        result_data = data.get("result") or {}
        rows: list[dict[str, Any]] = []
        today = date.today()

        # BGP stats may return aggregate counts
        routes_total = result_data.get("routes_total") or result_data.get("total")
        if routes_total is not None:
            rows.append({
                "obs_date": today,
                "value": float(routes_total),
                "raw_payload": {
                    "routes_total": routes_total,
                    "routes_origin": result_data.get("routes_origin"),
                    "routes_invalid": result_data.get("routes_invalid"),
                    "rpki_valid": result_data.get("rpki_valid"),
                    "rpki_invalid": result_data.get("rpki_invalid"),
                    "rpki_unknown": result_data.get("rpki_unknown"),
                    "source": "bgp_stats",
                },
            })

        return rows

    # ------------------------------------------------------------------ #
    # Anomaly detection: >30% traffic drop
    # ------------------------------------------------------------------ #

    def detect_traffic_anomalies(
        self,
        country_data: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        """Detect countries with significant traffic drops.

        Compares the most recent value against the mean of prior values
        for each country. Flags drops exceeding the threshold.

        Parameters:
            country_data: Dict mapping country code to row dicts,
                each having 'obs_date' and 'value'.

        Returns:
            List of anomaly dicts with country, drop_pct, details.
        """
        anomalies: list[dict[str, Any]] = []

        for cc, rows in country_data.items():
            if len(rows) < 2:
                continue

            sorted_rows = sorted(rows, key=lambda r: r["obs_date"])
            latest = sorted_rows[-1]["value"]
            prior_values = [r["value"] for r in sorted_rows[:-1]]
            mean_prior = sum(prior_values) / len(prior_values)

            if mean_prior <= 0:
                continue

            drop_pct = (mean_prior - latest) / mean_prior

            if drop_pct > _TRAFFIC_DROP_THRESHOLD:
                anomaly = {
                    "country": cc,
                    "drop_pct": round(drop_pct * 100, 2),
                    "latest_value": latest,
                    "mean_prior": round(mean_prior, 4),
                    "obs_date": sorted_rows[-1]["obs_date"],
                    "severity": "HIGH" if drop_pct > 0.5 else "MEDIUM",
                }
                anomalies.append(anomaly)
                log.warning(
                    "CF Radar ANOMALY: {cc} traffic dropped {pct:.1f}% "
                    "(mean={mean:.2f} -> latest={latest:.2f})",
                    cc=cc,
                    pct=drop_pct * 100,
                    mean=mean_prior,
                    latest=latest,
                )

        return anomalies

    # ------------------------------------------------------------------ #
    # Main pull orchestrator
    # ------------------------------------------------------------------ #

    def pull(
        self,
        days_back: int = 30,
        pull_countries: bool = True,
        pull_ddos: bool = True,
        pull_anomalies: bool = True,
        pull_bgp: bool = True,
    ) -> dict[str, Any]:
        """Run the full Cloudflare Radar data pull.

        Pulls all configured data categories, stores to raw_series,
        and runs anomaly detection on country traffic.

        Parameters:
            days_back: Number of days of history for timeseries endpoints.
            pull_countries: Whether to pull per-country traffic data.
            pull_ddos: Whether to pull DDoS attack trends.
            pull_anomalies: Whether to pull traffic anomaly events.
            pull_bgp: Whether to pull BGP route statistics.

        Returns:
            Summary dict with status, counts, and detected anomalies.
        """
        total_inserted = 0
        per_series: dict[str, int] = {}
        detected_anomalies: list[dict[str, Any]] = []
        errors: list[str] = []

        # Every Radar endpoint 403s without a bearer token — short-circuit
        # the whole pull so we don't log one ERROR per endpoint per cycle.
        if _resolve_cf_token() is None:
            log.warning(
                "CF Radar: no CF_RADAR_TOKEN / CLOUDFLARE_API_TOKEN set; "
                "skipping all endpoints this cycle"
            )
            return {
                "status": "SKIPPED",
                "total_inserted": 0,
                "per_series": {},
                "detected_anomalies": [],
                "errors": ["no CF Radar token configured"],
            }

        # ── 1. Global HTTP traffic ────────────────────────────────────
        log.info("CF Radar: pulling HTTP traffic trends ({d}d)", d=days_back)
        http_rows = self._pull_http_traffic(days_back=days_back)
        sid_http = self._series_id("http_traffic", "global")

        if http_rows:
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(sid_http, conn)
                count = 0
                for row in http_rows:
                    if row["obs_date"] not in existing:
                        self._insert_raw(
                            conn,
                            series_id=sid_http,
                            obs_date=row["obs_date"],
                            value=row["value"],
                            raw_payload=row["raw_payload"],
                        )
                        count += 1
                per_series[sid_http] = count
                total_inserted += count

        self._rate_limit()

        # ── 2. Per-country traffic ────────────────────────────────────
        if pull_countries:
            log.info("CF Radar: pulling traffic for top {n} countries", n=len(TOP_COUNTRIES))
            country_data = self._pull_country_traffic()

            if country_data:
                with self.engine.begin() as conn:
                    for cc, rows in country_data.items():
                        sid = self._series_id("traffic", cc.lower())
                        existing = self._get_existing_dates(sid, conn)
                        count = 0
                        for row in rows:
                            if row["obs_date"] not in existing:
                                self._insert_raw(
                                    conn,
                                    series_id=sid,
                                    obs_date=row["obs_date"],
                                    value=row["value"],
                                    raw_payload=row["raw_payload"],
                                )
                                count += 1
                        per_series[sid] = count
                        total_inserted += count

                # Detect anomalies in country traffic
                detected_anomalies = self.detect_traffic_anomalies(country_data)

            self._rate_limit()

        # ── 3. DDoS attack trends ─────────────────────────────────────
        if pull_ddos:
            log.info("CF Radar: pulling DDoS attack trends ({d}d)", d=days_back)
            ddos_rows = self._pull_ddos_trends(days_back=days_back)
            sid_ddos = self._series_id("ddos", "global")

            if ddos_rows:
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(sid_ddos, conn)
                    count = 0
                    for row in ddos_rows:
                        if row["obs_date"] not in existing:
                            self._insert_raw(
                                conn,
                                series_id=sid_ddos,
                                obs_date=row["obs_date"],
                                value=row["value"],
                                raw_payload=row["raw_payload"],
                            )
                            count += 1
                    per_series[sid_ddos] = count
                    total_inserted += count

            self._rate_limit()

        # ── 4. Traffic anomalies ──────────────────────────────────────
        if pull_anomalies:
            log.info("CF Radar: pulling traffic anomalies ({d}d)", d=days_back)
            anomaly_rows = self._pull_anomalies(days_back=days_back)

            if anomaly_rows:
                with self.engine.begin() as conn:
                    for row in anomaly_rows:
                        sid = self._series_id("anomaly", row["event_id"])
                        existing = self._get_existing_dates(sid, conn)
                        if row["obs_date"] not in existing:
                            self._insert_raw(
                                conn,
                                series_id=sid,
                                obs_date=row["obs_date"],
                                value=row["value"],
                                raw_payload=row["raw_payload"],
                            )
                            per_series[sid] = per_series.get(sid, 0) + 1
                            total_inserted += 1

            self._rate_limit()

        # ── 5. BGP route stats ────────────────────────────────────────
        if pull_bgp:
            log.info("CF Radar: pulling BGP route statistics")
            bgp_rows = self._pull_bgp_stats()
            sid_bgp = self._series_id("bgp", "global")

            if bgp_rows:
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(sid_bgp, conn)
                    count = 0
                    for row in bgp_rows:
                        if row["obs_date"] not in existing:
                            self._insert_raw(
                                conn,
                                series_id=sid_bgp,
                                obs_date=row["obs_date"],
                                value=row["value"],
                                raw_payload=row["raw_payload"],
                            )
                            count += 1
                    per_series[sid_bgp] = count
                    total_inserted += count

        # ── Summary ───────────────────────────────────────────────────
        status = "SUCCESS" if not errors else "PARTIAL"
        summary = {
            "status": status,
            "rows_inserted": total_inserted,
            "per_series": per_series,
            "detected_anomalies": detected_anomalies,
            "errors": errors,
        }

        log.info(
            "CF Radar pull complete: {n} rows inserted, {a} anomalies detected",
            n=total_inserted,
            a=len(detected_anomalies),
        )

        return summary
