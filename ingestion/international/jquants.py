"""
GRID Japan Exchange Group J-Quants API ingestion module (J-Quants API V2).

Pulls the daily TOPIX close from ``GET /v2/indices/bars/daily/topix`` and
stores it as ``raw_series`` series ``japan_topix`` (same series id and row
shape the V1 client wrote, so downstream features are unchanged).

Authentication is the V2 API key, sent only in the ``x-api-key`` header
(never in a URL, never logged). It comes from ``settings.JQUANTS_API_KEY``
unless passed to the constructor. J-Quants discontinued V1 and its
email/password token flow (``/v1/token/auth_user`` ->
``/v1/token/auth_refresh``) on 2026-06-01; see
https://jpx-jquants.com/en/spec/migration-v1-v2 .

Plan: TOPIX prices are not in the Free plan; they need Light or above
(https://jpx-jquants.com/en/spec/data-spec). A key on a plan without TOPIX
gets HTTP 403, which this puller reports as a config-class failure.

Outcomes (scheduler contract, see ``smart_scheduler._classify_outcome``):
  * no API key         -> SKIPPED "JQUANTS_API_KEY not configured", no network call
  * HTTP 401/403       -> FAILED with an ``auth_failed`` reason, not retried
  * HTTP 400/429/4xx   -> FAILED, not retried (429: J-Quants asks for >= 1 min backoff)
  * HTTP 5xx / network -> retried up to 3 attempts, then FAILED
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion.base import BasePuller

_JQUANTS_BASE_URL = "https://api.jquants.com/v2"
_TOPIX_ENDPOINT = "indices/bars/daily/topix"
_RATE_LIMIT_DELAY: float = 1.0
_MAX_PAGES = 50

# J-Quants V2 dataset -> GRID raw_series id. V1 also listed "nikkei225", but
# it was never pulled and V2 does not distribute the Nikkei Stock Average
# cash index (https://jpx-jquants.com/en/spec/idx-bars-daily), so it is
# dropped explicitly.
JQUANTS_TARGETS: dict[str, str] = {
    "topix": "japan_topix",
}

_NOT_CONFIGURED_REASON = "JQUANTS_API_KEY not configured"
_URL_QUERY = re.compile(r"(https?://[^\s?#'\"<>]+)[?#][^\s'\"<>]*")


class JQuantsNotConfigured(ValueError):
    """The API key is missing: a deterministic config error, never retried."""


class JQuantsHTTPError(RuntimeError):
    """A non-2xx J-Quants response; the message never carries a query string."""

    def __init__(self, status: int, path: str) -> None:
        self.status = status
        self.path = path
        if status in (401, 403):
            msg = (f"auth_failed: HTTP {status} on {path} "
                   "(check JQUANTS_API_KEY and that the plan includes this dataset)")
        else:
            msg = f"HTTP {status} on {path}"
        super().__init__(msg)


def _is_transient(exc: BaseException) -> bool:
    """Retry only network faults and 5xx; 4xx and config errors are final."""
    if isinstance(exc, JQuantsHTTPError):
        return exc.status >= 500
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class JQuantsPuller(BasePuller):
    """Pulls Japanese market data from the J-Quants API V2."""

    SOURCE_NAME = "JQuants"
    SOURCE_CONFIG = {"base_url": "https://api.jquants.com/v2", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 25}

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        if api_key is None:
            from config import settings

            api_key = getattr(settings, "JQUANTS_API_KEY", "") or ""
        self._api_key = api_key  # Never log; sent only in the x-api-key header
        super().__init__(db_engine)
        log.info(
            "JQuantsPuller initialised — source_id={sid}, api_key={k}",
            sid=self.source_id,
            k="set" if api_key else "(not set)",
        )

    @property
    def is_configured(self) -> bool:
        """True when a J-Quants V2 API key is present."""
        return bool(self._api_key)

    def _safe_error(self, exc: BaseException) -> str:
        """Exception text safe to log and store: no query strings, no key."""
        if isinstance(exc, (JQuantsHTTPError, JQuantsNotConfigured)):
            msg = str(exc)
        elif isinstance(exc, requests.HTTPError) and exc.response is not None:
            path = urlsplit(getattr(exc.response, "url", "") or "").path
            msg = f"HTTP {exc.response.status_code} on {path}"
        else:
            msg = f"{type(exc).__name__}: {exc}"
        msg = _URL_QUERY.sub(r"\1", msg)
        if self._api_key:
            msg = msg.replace(self._api_key, "[REDACTED]")
        try:
            from server_log.sanitizer import scrub_secrets
        except ImportError:  # scrubbing must never break the pull
            return msg
        return scrub_secrets(msg)  # never raises

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception(_is_transient),
        reraise=True,
    )
    def _get(self, endpoint: str, params: dict[str, str]) -> dict:
        """One authenticated GET; the key travels only in the header."""
        if not self.is_configured:
            raise JQuantsNotConfigured(_NOT_CONFIGURED_REASON)
        url = f"{_JQUANTS_BASE_URL}/{endpoint}"
        resp = requests.get(
            url, headers={"x-api-key": self._api_key}, params=params, timeout=30,
        )
        if resp.status_code >= 400:
            raise JQuantsHTTPError(resp.status_code, urlsplit(url).path)
        return resp.json()

    def _fetch_all(self, endpoint: str, params: dict[str, str]) -> list[dict]:
        """Follow ``pagination_key`` until the response stops returning one."""
        rows: list[dict] = []
        query = dict(params)
        for page in range(_MAX_PAGES):
            if page:
                time.sleep(_RATE_LIMIT_DELAY)
            body = self._get(endpoint, query)
            data = body.get("data")
            if not isinstance(data, list):
                raise TypeError(f"unexpected response shape from /{endpoint}: no 'data' list")
            rows.extend(data)
            key = body.get("pagination_key")
            if not key:
                return rows
            query = {**params, "pagination_key": key}
        raise ValueError(f"/{endpoint} still paginating after {_MAX_PAGES} pages")

    def pull_index_prices(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent TOPIX daily closes into ``japan_topix``."""
        if not self.is_configured:
            log.warning("J-Quants skipped: {r}", r=_NOT_CONFIGURED_REASON)
            return {
                "series_id": "jquants_indices",
                "rows_inserted": 0,
                "status": "SKIPPED",
                "skipped_reason": _NOT_CONFIGURED_REASON,
                "errors": [],
            }

        log.info("Pulling J-Quants index data")
        result: dict[str, Any] = {
            "series_id": "jquants_indices",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }
        series_id = JQUANTS_TARGETS["topix"]

        try:
            start_dt = (date.today() - timedelta(days=days_back)).isoformat()
            records = self._fetch_all(_TOPIX_ENDPOINT, {"from": start_dt})

            inserted = 0
            with self.engine.begin() as conn:
                for record in records:
                    try:
                        obs_dt = datetime.strptime(record["Date"], "%Y-%m-%d").date()
                        close_val = float(record["C"])

                        if self._row_exists(series_id, obs_dt, conn):
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": series_id, "src": self.source_id, "od": obs_dt, "val": close_val},
                        )
                        inserted += 1
                    except (KeyError, ValueError, TypeError) as row_exc:
                        log.debug("Skipping J-Quants row: {err}", err=str(row_exc))

            result["rows_inserted"] = inserted
            log.info("J-Quants: inserted {n} rows", n=inserted)

        except Exception as exc:
            # Upstream/auth failures are operational, not application bugs.
            safe = self._safe_error(exc)
            log.warning("J-Quants pull failed: {err}", err=safe)
            result["status"] = "FAILED"
            result["errors"].append(safe)

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all J-Quants data."""
        return self.pull_index_prices(days_back=90)
