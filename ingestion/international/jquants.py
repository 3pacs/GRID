"""
GRID Japan Exchange Group J-Quants API ingestion module.

Pulls Japanese market data including stock prices and financial statements
from the J-Quants API. Requires email/password authentication.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

_JQUANTS_BASE_URL = "https://api.jquants.com/v1"
_RATE_LIMIT_DELAY: float = 1.0

# Key Japanese indices and stocks to track
JQUANTS_TARGETS: dict[str, str] = {
    "topix": "japan_topix",
    "nikkei225": "japan_nikkei225",
}


class JQuantsPuller:
    """Pulls Japanese market data from the J-Quants API."""

    def __init__(self, db_engine: Engine, email: str = "", password: str = "") -> None:
        self.engine = db_engine
        self.email = email
        self.password = password  # Never log — used only in _authenticate()
        self._token: str | None = None
        self.source_id = self._resolve_source_id()
        log.info(
            "JQuantsPuller initialised — source_id={sid}, email={e}",
            sid=self.source_id,
            e=email[:3] + "***" if email else "(not set)",
        )

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "JQuants"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'DAILY', FALSE, 'NEVER', 'HIGH', 23, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "JQuants", "url": _JQUANTS_BASE_URL},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

    def _authenticate(self) -> str:
        """Authenticate with J-Quants API and get access token.

        Sends email/password to the J-Quants auth endpoint.  The password
        is never logged — only the masked email appears in log output.
        """
        if self._token:
            return self._token

        if not self.email or not self.password:
            raise ValueError(
                "JQUANTS_EMAIL and JQUANTS_PASSWORD must be set for J-Quants authentication"
            )

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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
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
            log.error("J-Quants pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all J-Quants data."""
        return self.pull_index_prices(days_back=90)
