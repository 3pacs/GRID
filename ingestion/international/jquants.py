"""
GRID Japan Exchange Group J-Quants API ingestion module.

Pulls Japanese market data including stock prices and financial statements
from the J-Quants API. Requires email/password authentication.

Credentials default to ``settings.JQUANTS_EMAIL`` / ``settings.JQUANTS_PASSWORD``
when the constructor is not given them (the scheduler builds
``JQuantsPuller(db_engine)``). Without credentials ``pull_all`` returns the
scheduler-contract SKIPPED result instead of raising.

NOTE (2026-11-05): this client speaks the J-Quants V1 token flow
(``/v1/token/auth_user`` -> ``/v1/token/auth_refresh``). J-Quants
discontinued V1 on 2026-06-01; V2 uses a dashboard-issued API key in the
``x-api-key`` header and ``/v2/indices/bars/daily/topix`` (rows under
``"data"``, abbreviated columns such as ``C`` for Close). See
https://jpx-jquants.com/en/spec/migration-v1-v2 . A V2 rebuild is pending.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.base import BasePuller

_JQUANTS_BASE_URL = "https://api.jquants.com/v1"
_RATE_LIMIT_DELAY: float = 1.0

# Key Japanese indices and stocks to track
JQUANTS_TARGETS: dict[str, str] = {
    "topix": "japan_topix",
    "nikkei225": "japan_nikkei225",
}

_NOT_CONFIGURED_REASON = "JQUANTS_EMAIL and JQUANTS_PASSWORD not configured"


class JQuantsNotConfigured(ValueError):
    """Credentials are missing: a deterministic config error, never retried."""


class JQuantsPuller(BasePuller):
    """Pulls Japanese market data from the J-Quants API."""

    SOURCE_NAME = "JQuants"
    SOURCE_CONFIG = {"base_url": "https://api.jquants.com/v1", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 25}

    def __init__(
        self,
        db_engine: Engine,
        email: str | None = None,
        password: str | None = None,
    ) -> None:
        if email is None or password is None:
            from config import settings

            if email is None:
                email = getattr(settings, "JQUANTS_EMAIL", "") or ""
            if password is None:
                password = getattr(settings, "JQUANTS_PASSWORD", "") or ""
        self.email = email
        self.password = password  # Never log — used only in _authenticate()
        self._token: str | None = None
        super().__init__(db_engine)
        log.info(
            "JQuantsPuller initialised — source_id={sid}, email={e}",
            sid=self.source_id,
            e=email[:3] + "***" if email else "(not set)",
        )

    @property
    def is_configured(self) -> bool:
        """True when both J-Quants credentials are present."""
        return bool(self.email and self.password)

    def _authenticate(self) -> str:
        """Authenticate with J-Quants API and get access token.

        Sends email/password to the J-Quants auth endpoint.  The password
        is never logged — only the masked email appears in log output.
        """
        if self._token:
            return self._token

        if not self.is_configured:
            raise JQuantsNotConfigured(_NOT_CONFIGURED_REASON)

        log.debug("Authenticating with J-Quants API")

        # Step 1: Get refresh token
        resp = requests.post(
            f"{_JQUANTS_BASE_URL}/token/auth_user",
            json={"mailaddress": self.email, "password": self.password},
            timeout=30,
        )
        resp.raise_for_status()
        refresh_token = resp.json().get("refreshToken", "")

        # Step 2: Get access token
        resp = requests.post(
            f"{_JQUANTS_BASE_URL}/token/auth_refresh",
            params={"refreshtoken": refresh_token},
            timeout=30,
        )
        resp.raise_for_status()
        self._token = resp.json().get("idToken", "")
        return self._token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_not_exception_type(JQuantsNotConfigured),
        reraise=True,
    )
    def _fetch_data(self, endpoint: str, params: dict | None = None) -> dict:
        """Fetch data from J-Quants API with authentication."""
        token = self._authenticate()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{_JQUANTS_BASE_URL}/{endpoint}"
        resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def pull_index_prices(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent Japanese index price data."""
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

        try:
            start_dt = (date.today() - timedelta(days=days_back)).isoformat()
            data = self._fetch_data("indices/topix", {"from": start_dt})

            inserted = 0
            records = data.get("topix", [])
            with self.engine.begin() as conn:
                for record in records:
                    try:
                        obs_dt = datetime.strptime(record["Date"], "%Y-%m-%d").date()
                        close_val = float(record.get("Close", 0))

                        if self._row_exists("japan_topix", obs_dt, conn):
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": "japan_topix", "src": self.source_id, "od": obs_dt, "val": close_val},
                        )
                        inserted += 1
                    except (KeyError, ValueError, TypeError) as row_exc:
                        log.debug("Skipping J-Quants row: {err}", err=str(row_exc))

            result["rows_inserted"] = inserted
            log.info("J-Quants: inserted {n} rows", n=inserted)

        except Exception as exc:
            # Upstream/auth failures are operational, not application bugs.
            log.warning("J-Quants pull failed: {err}", err=f"{type(exc).__name__}: {exc}")
            result["status"] = "FAILED"
            result["errors"].append(f"{type(exc).__name__}: {exc}")

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all J-Quants data."""
        return self.pull_index_prices(days_back=90)
