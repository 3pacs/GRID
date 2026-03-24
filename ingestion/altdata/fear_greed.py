"""
GRID Fear & Greed Index ingestion module.

Pulls two complementary sentiment indices:
1. CNN Fear & Greed Index -- composite of 7 market indicators (VIX,
   momentum, breadth, put/call, junk bond demand, safe haven demand,
   market volatility). Published daily. Range: 0 (extreme fear) to
   100 (extreme greed).
2. Crypto Fear & Greed Index -- aggregated crypto market sentiment
   from volatility, volume, social media, surveys, dominance, and
   trends. Published daily. Range: 0-100.

Data sources:
- CNN: https://production.dataviz.cnn.io/index/fearandgreed/graphdata
- Crypto: https://api.alternative.me/fng/

Series stored:
- feargreed.cnn_value: CNN Fear & Greed Index current value (0-100)
- feargreed.cnn_previous_close: CNN Fear & Greed previous close value
- feargreed.crypto_value: Crypto Fear & Greed Index value (0-100)
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- URLs ----
_CNN_FG_URL: str = (
    "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
)
_CRYPTO_FG_URL: str = (
    "https://api.alternative.me/fng/?limit=30&format=json"
)

# Series ID prefix
_SERIES_PREFIX: str = "feargreed"

# Feature definitions
FEARGREED_FEATURES: dict[str, str] = {
    "cnn_value": "CNN Fear & Greed Index current value (0=extreme fear, 100=extreme greed)",
    "cnn_previous_close": "CNN Fear & Greed Index previous close value",
    "crypto_value": "Crypto Fear & Greed Index value (0=extreme fear, 100=extreme greed)",
}

# HTTP config
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.5


class FearGreedPuller(BasePuller):
    """Pulls CNN and Crypto Fear & Greed indices.

    The CNN Fear & Greed Index is a widely-followed composite sentiment
    indicator combining 7 market signals. Extreme readings (below 20 or
    above 80) are historically contrarian signals.

    The Crypto Fear & Greed Index tracks sentiment in cryptocurrency
    markets using volatility, volume, social media, and surveys.

    Features:
    - feargreed.cnn_value: CNN composite fear/greed (0-100)
    - feargreed.cnn_previous_close: CNN previous close value
    - feargreed.crypto_value: Crypto fear/greed (0-100)

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Fear_Greed.
    """

    SOURCE_NAME: str = "Fear_Greed"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the Fear & Greed puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "FearGreedPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    def _series_id(self, feature: str) -> str:
        """Build the full series_id for a feature.

        Parameters:
            feature: Feature suffix (e.g., 'cnn_value').

        Returns:
            Full series_id (e.g., 'feargreed.cnn_value').
        """
        return f"{_SERIES_PREFIX}.{feature}"

    # ------------------------------------------------------------------ #
    # CNN Fear & Greed
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
    def _fetch_cnn_data(self) -> dict[str, Any]:
        """Fetch the CNN Fear & Greed JSON endpoint.

        Returns:
            Parsed JSON response dict.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; GRID-DataPuller/1.0; "
                "+https://github.com/grid-trading)"
            ),
            "Accept": "application/json",
        }
        resp = requests.get(_CNN_FG_URL, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _parse_cnn_response(
        self, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Parse the CNN Fear & Greed JSON into row dicts.

        The CNN API returns a nested structure with current score,
        previous close, and historical data points under
        ``fear_and_greed_historical``.

        Parameters:
            data: Raw JSON response from CNN endpoint.

        Returns:
            List of dicts with keys: obs_date, cnn_value,
            cnn_previous_close, raw_payload.
        """
        rows: list[dict[str, Any]] = []

        # --- Current / previous close values ---
        fg = data.get("fear_and_greed") or {}
        current_score = fg.get("score")
        previous_close = fg.get("previous_close")
        timestamp_ms = fg.get("timestamp")

        if current_score is not None and timestamp_ms is not None:
            try:
                obs_dt = datetime.fromtimestamp(
                    timestamp_ms / 1000.0, tz=timezone.utc
                )
                obs_date = obs_dt.date()
                rows.append(
                    {
                        "obs_date": obs_date,
                        "cnn_value": float(current_score),
                        "cnn_previous_close": (
                            float(previous_close)
                            if previous_close is not None
                            else None
                        ),
                        "raw_payload": {
                            "rating": fg.get("rating"),
                            "timestamp_ms": timestamp_ms,
                            "source_url": _CNN_FG_URL,
                        },
                    }
                )
            except (ValueError, OverflowError, OSError) as exc:
                log.warning(
                    "CNN F&G: failed to parse timestamp {ts}: {e}",
                    ts=timestamp_ms,
                    e=str(exc),
                )

        # --- Historical data points ---
        historical = data.get("fear_and_greed_historical") or {}
        hist_data = historical.get("data") or []

        for point in hist_data:
            ts_ms = point.get("x")
            value = point.get("y")
            if ts_ms is None or value is None:
                continue
            try:
                obs_dt = datetime.fromtimestamp(
                    ts_ms / 1000.0, tz=timezone.utc
                )
                obs_date = obs_dt.date()
                rows.append(
                    {
                        "obs_date": obs_date,
                        "cnn_value": float(value),
                        "cnn_previous_close": None,
                        "raw_payload": {
                            "rating": point.get("rating"),
                            "timestamp_ms": ts_ms,
                            "source_url": _CNN_FG_URL,
                        },
                    }
                )
            except (ValueError, OverflowError, OSError) as exc:
                log.warning(
                    "CNN F&G historical: bad timestamp {ts}: {e}",
                    ts=ts_ms,
                    e=str(exc),
                )

        # Deduplicate by date (keep most recent entry per date)
        seen: dict[date, dict[str, Any]] = {}
        for row in rows:
            d = row["obs_date"]
            if d not in seen:
                seen[d] = row
            else:
                # Prefer the entry with previous_close populated
                if (
                    row.get("cnn_previous_close") is not None
                    and seen[d].get("cnn_previous_close") is None
                ):
                    seen[d] = row

        return list(seen.values())

    def pull_cnn(
        self,
        start_date: str | date = "2020-01-01",
        days_back: int | None = None,
    ) -> dict[str, Any]:
        """Pull CNN Fear & Greed data and store cnn_value + cnn_previous_close.

        Uses _get_existing_dates() for efficient batch deduplication.

        Parameters:
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.

        Returns:
            dict with status, rows_inserted, per_feature counts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if days_back is not None:
            cutoff = date.today() - timedelta(days=days_back)
            start_date = max(start_date, cutoff)

        try:
            raw_data = self._fetch_cnn_data()
        except Exception as exc:
            log.error("CNN Fear & Greed pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        parsed = self._parse_cnn_response(raw_data)
        if not parsed:
            log.warning("CNN Fear & Greed: no data parsed from response")
            return {"status": "SUCCESS", "rows_inserted": 0}

        # Filter by start_date
        parsed = [r for r in parsed if r["obs_date"] >= start_date]
        if not parsed:
            log.info(
                "CNN Fear & Greed: no new data after {d}", d=start_date
            )
            return {"status": "SUCCESS", "rows_inserted": 0}

        total_inserted = 0
        per_feature: dict[str, int] = {}

        with self.engine.begin() as conn:
            # --- cnn_value ---
            sid_value = self._series_id("cnn_value")
            existing_value = self._get_existing_dates(sid_value, conn)
            inserted_value = 0

            for row in parsed:
                if row["obs_date"] in existing_value:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid_value,
                    obs_date=row["obs_date"],
                    value=row["cnn_value"],
                    raw_payload=row["raw_payload"],
                )
                inserted_value += 1

            per_feature["cnn_value"] = inserted_value
            total_inserted += inserted_value
            log.info(
                "CNN {sid}: {n} rows inserted", sid=sid_value, n=inserted_value
            )

            # --- cnn_previous_close ---
            sid_prev = self._series_id("cnn_previous_close")
            existing_prev = self._get_existing_dates(sid_prev, conn)
            inserted_prev = 0

            for row in parsed:
                if row["cnn_previous_close"] is None:
                    continue
                if row["obs_date"] in existing_prev:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid_prev,
                    obs_date=row["obs_date"],
                    value=row["cnn_previous_close"],
                    raw_payload=row["raw_payload"],
                )
                inserted_prev += 1

            per_feature["cnn_previous_close"] = inserted_prev
            total_inserted += inserted_prev
            log.info(
                "CNN {sid}: {n} rows inserted", sid=sid_prev, n=inserted_prev
            )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "per_feature": per_feature,
        }

    # ------------------------------------------------------------------ #
    # Crypto Fear & Greed
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
    def _fetch_crypto_data(self) -> dict[str, Any]:
        """Fetch the Crypto Fear & Greed JSON endpoint.

        Returns:
            Parsed JSON response dict.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }
        resp = requests.get(
            _CRYPTO_FG_URL, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_crypto_response(
        self, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Parse the Crypto Fear & Greed API response.

        The API returns ``data`` as a list of objects with keys:
        ``value`` (str), ``value_classification`` (str),
        ``timestamp`` (unix seconds as str).

        Parameters:
            data: Raw JSON response from alternative.me.

        Returns:
            List of dicts with obs_date, crypto_value, raw_payload.
        """
        rows: list[dict[str, Any]] = []
        entries = data.get("data") or []

        for entry in entries:
            raw_value = entry.get("value")
            raw_ts = entry.get("timestamp")
            if raw_value is None or raw_ts is None:
                continue

            try:
                value = float(raw_value)
            except (ValueError, TypeError) as exc:
                log.warning(
                    "Crypto F&G: bad value {v}: {e}",
                    v=raw_value,
                    e=str(exc),
                )
                continue

            try:
                obs_dt = datetime.fromtimestamp(
                    int(raw_ts), tz=timezone.utc
                )
                obs_date = obs_dt.date()
            except (ValueError, OverflowError, OSError) as exc:
                log.warning(
                    "Crypto F&G: bad timestamp {ts}: {e}",
                    ts=raw_ts,
                    e=str(exc),
                )
                continue

            rows.append(
                {
                    "obs_date": obs_date,
                    "crypto_value": value,
                    "raw_payload": {
                        "value_classification": entry.get(
                            "value_classification"
                        ),
                        "timestamp_unix": raw_ts,
                        "source_url": _CRYPTO_FG_URL,
                    },
                }
            )

        return rows

    def pull_crypto(
        self,
        start_date: str | date = "2018-02-01",
        days_back: int | None = None,
    ) -> dict[str, Any]:
        """Pull Crypto Fear & Greed data and store crypto_value.

        Uses _get_existing_dates() for efficient batch deduplication.

        Parameters:
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.

        Returns:
            dict with status, rows_inserted, per_feature counts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if days_back is not None:
            cutoff = date.today() - timedelta(days=days_back)
            start_date = max(start_date, cutoff)

        try:
            raw_data = self._fetch_crypto_data()
        except Exception as exc:
            log.error("Crypto Fear & Greed pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        parsed = self._parse_crypto_response(raw_data)
        if not parsed:
            log.warning("Crypto Fear & Greed: no data parsed from response")
            return {"status": "SUCCESS", "rows_inserted": 0}

        # Filter by start_date
        parsed = [r for r in parsed if r["obs_date"] >= start_date]
        if not parsed:
            log.info(
                "Crypto Fear & Greed: no new data after {d}", d=start_date
            )
            return {"status": "SUCCESS", "rows_inserted": 0}

        inserted = 0
        sid = self._series_id("crypto_value")

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)

            for row in parsed:
                if row["obs_date"] in existing:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=row["obs_date"],
                    value=row["crypto_value"],
                    raw_payload=row["raw_payload"],
                )
                inserted += 1

        log.info(
            "Crypto {sid}: {n} rows inserted", sid=sid, n=inserted
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "per_feature": {"crypto_value": inserted},
        }

    # ------------------------------------------------------------------ #
    # Combined pull
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        start_date: str | date = "2018-02-01",
        days_back: int | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all Fear & Greed features (CNN + Crypto).

        Parameters:
            start_date: Earliest observation date.
            days_back: If set, only store recent data.

        Returns:
            List of result dicts (one per data source).
        """
        results: list[dict[str, Any]] = []

        cnn_result = self.pull_cnn(
            start_date=start_date, days_back=days_back
        )
        results.append(cnn_result)

        time.sleep(_RATE_LIMIT_DELAY)

        crypto_result = self.pull_crypto(
            start_date=start_date, days_back=days_back
        )
        results.append(crypto_result)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "Fear & Greed pull_all -- {ok}/{total} sources, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
