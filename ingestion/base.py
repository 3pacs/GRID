"""
Base puller class for GRID data ingestion.

Provides common database operations shared across all data pullers:
source ID resolution, row deduplication, and standardised insert.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

import time

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.sanity_ranges import get_range_for_series, MAX_PCT_CHANGE

# Retry configuration (used by pullers that opt in)
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF = 2.0  # seconds, multiplied by attempt number


def retry_on_failure(
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    backoff: float = DEFAULT_RETRY_BACKOFF,
    retryable_exceptions: tuple = (ConnectionError, TimeoutError, OSError),
):
    """Decorator for retrying API calls with exponential backoff and jitter.

    Parameters:
        max_attempts: Maximum number of attempts.
        backoff: Base backoff in seconds (multiplied by attempt number).
        retryable_exceptions: Tuple of exception types to retry on.
    """
    import functools
    import random

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        # Exponential backoff with jitter to avoid thundering herd
                        delay = backoff * attempt + random.uniform(0, backoff * 0.5)
                        log.warning(
                            "{f} attempt {a}/{m} failed: {e} — retrying in {d:.1f}s",
                            f=func.__name__,
                            a=attempt,
                            m=max_attempts,
                            e=str(exc),
                            d=delay,
                        )
                        time.sleep(delay)
                    else:
                        log.error(
                            "{f} failed after {m} attempts: {e}",
                            f=func.__name__,
                            m=max_attempts,
                            e=str(exc),
                        )
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


class BasePuller:
    """Base class for all GRID data ingestion pullers.

    Provides common methods for source catalog resolution, row
    deduplication, and raw_series insertion. Subclasses should set
    ``SOURCE_NAME`` and optionally ``SOURCE_CONFIG`` for auto-creation.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
    """

    SOURCE_NAME: str = ""
    SOURCE_CONFIG: dict[str, Any] | None = None  # For auto-creating source entries

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        if self.SOURCE_NAME:
            self.source_id = self._resolve_source_id()

    def _resolve_source_id(self) -> int:
        """Look up or create the source_catalog entry for this puller.

        Returns:
            int: The source_catalog.id.

        Raises:
            RuntimeError: If source not found and no SOURCE_CONFIG for auto-creation.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": self.SOURCE_NAME},
            ).fetchone()

        if row is not None:
            return row[0]

        if self.SOURCE_CONFIG is None:
            raise RuntimeError(
                f"{self.SOURCE_NAME} source not found in source_catalog. "
                "Run schema.sql first."
            )

        # Auto-create source entry
        log.info("Auto-creating source_catalog entry for {s}", s=self.SOURCE_NAME)
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    "INSERT INTO source_catalog "
                    "(name, base_url, cost_tier, latency_class, pit_available, "
                    "revision_behavior, trust_score, priority_rank, active) "
                    "VALUES (:name, :url, :cost, :latency, :pit, :rev, :trust, :rank, TRUE) "
                    "ON CONFLICT (name) DO NOTHING "
                    "RETURNING id"
                ),
                {
                    "name": self.SOURCE_NAME,
                    "url": self.SOURCE_CONFIG.get("base_url", ""),
                    "cost": self.SOURCE_CONFIG.get("cost_tier", "FREE"),
                    "latency": self.SOURCE_CONFIG.get("latency_class", "EOD"),
                    "pit": self.SOURCE_CONFIG.get("pit_available", False),
                    "rev": self.SOURCE_CONFIG.get("revision_behavior", "NEVER"),
                    "trust": self.SOURCE_CONFIG.get("trust_score", "MED"),
                    "rank": self.SOURCE_CONFIG.get("priority_rank", 50),
                },
            )
            new_row = result.fetchone()
            if new_row:
                return new_row[0]

        # If ON CONFLICT hit, re-fetch
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": self.SOURCE_NAME},
            ).fetchone()
        return row[0]

    def _row_exists(
        self,
        series_id: str,
        obs_date: date,
        conn: Any,
        dedup_hours: int = 1,
    ) -> bool:
        """Check if a raw_series row already exists within the dedup window.

        Parameters:
            series_id: The series identifier.
            obs_date: Observation date.
            conn: Active database connection.
            dedup_hours: Hours to look back for duplicates (default: 1).

        Returns:
            bool: True if a matching row exists.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=dedup_hours)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": cutoff},
        ).fetchone()
        return result is not None

    def _get_existing_dates(
        self,
        series_id: str,
        conn: Any,
    ) -> set[date]:
        """Fetch all obs_dates already stored for a series in one query.

        Much faster than per-row _row_exists() checks for bulk inserts.

        Parameters:
            series_id: The series identifier.
            conn: Active database connection.

        Returns:
            set[date]: All observation dates already in raw_series.
        """
        rows = conn.execute(
            text(
                "SELECT DISTINCT obs_date FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND pull_status = 'SUCCESS'"
            ),
            {"sid": series_id, "src": self.source_id},
        ).fetchall()
        return {r[0] for r in rows}

    def _get_latest_date(
        self,
        series_id: str,
    ) -> date | None:
        """Get the most recent obs_date for a series.

        Useful for incremental pulls — only fetch data after this date.

        Parameters:
            series_id: The series identifier.

        Returns:
            The latest obs_date, or None if no data exists.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT MAX(obs_date) FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src "
                    "AND pull_status = 'SUCCESS'"
                ),
                {"sid": series_id, "src": self.source_id},
            ).fetchone()
        if row and row[0]:
            return row[0]
        return None

    def validate_row(
        self,
        series_id: str,
        obs_date: date,
        value: float | None,
        family: str | None = None,
        previous_value: float | None = None,
    ) -> list[str]:
        """Validate a single data row before insertion.

        Checks:
          - value is not None, NaN, or Inf
          - value is within plausible range for the series/family
          - obs_date is not in the future
          - obs_date is not more than 5 years old
          - value has not changed >50% from previous observation

        Parameters:
            series_id: Raw series identifier.
            obs_date: Observation date.
            value: Numeric value to validate.
            family: Optional series family for range lookup.
            previous_value: Previous observation value for spike detection.

        Returns:
            List of warning strings.  Empty list means all checks passed.
            Warnings are logged but never block the pipeline.
        """
        warnings: list[str] = []
        today = date.today()

        # ── Value null / NaN / Inf check ──────────────────────────────────
        if value is None:
            warnings.append(
                f"SANITY [{series_id}] obs_date={obs_date}: value is None"
            )
            log.warning(warnings[-1])
            return warnings  # no further checks possible

        try:
            fval = float(value)
        except (TypeError, ValueError):
            warnings.append(
                f"SANITY [{series_id}] obs_date={obs_date}: "
                f"value not numeric ({value!r})"
            )
            log.warning(warnings[-1])
            return warnings

        if math.isnan(fval):
            warnings.append(
                f"SANITY [{series_id}] obs_date={obs_date}: value is NaN"
            )
            log.warning(warnings[-1])
            return warnings

        if math.isinf(fval):
            warnings.append(
                f"SANITY [{series_id}] obs_date={obs_date}: value is Inf"
            )
            log.warning(warnings[-1])
            return warnings

        # ── Range check ───────────────────────────────────────────────────
        bounds = get_range_for_series(series_id, family)
        if bounds is not None:
            lo, hi = bounds
            if fval < lo or fval > hi:
                warnings.append(
                    f"SANITY [{series_id}] obs_date={obs_date}: "
                    f"value={fval} outside plausible range [{lo}, {hi}]"
                )
                log.warning(warnings[-1])

        # ── Date: not in the future ───────────────────────────────────────
        if obs_date > today:
            warnings.append(
                f"SANITY [{series_id}]: obs_date={obs_date} is in the future"
            )
            log.warning(warnings[-1])

        # ── Date: not more than 5 years old ───────────────────────────────
        five_years_ago = today - timedelta(days=5 * 365)
        if obs_date < five_years_ago:
            warnings.append(
                f"SANITY [{series_id}]: obs_date={obs_date} is >5 years old "
                f"(stale source?)"
            )
            log.warning(warnings[-1])

        # ── Spike detection: >50% change from previous value ──────────────
        if previous_value is not None:
            try:
                prev = float(previous_value)
                if prev != 0:
                    pct_change = abs((fval - prev) / prev) * 100
                    if pct_change > MAX_PCT_CHANGE:
                        warnings.append(
                            f"SANITY [{series_id}] obs_date={obs_date}: "
                            f"value changed {pct_change:.1f}% from previous "
                            f"({prev} -> {fval})"
                        )
                        log.warning(warnings[-1])
            except (TypeError, ValueError):
                pass  # previous_value not usable, skip spike check

        return warnings

    def _insert_raw(
        self,
        conn: Any,
        series_id: str,
        obs_date: date,
        value: float,
        raw_payload: dict[str, Any] | None = None,
        pull_status: str = "SUCCESS",
    ) -> None:
        """Insert a row into raw_series.

        Runs sanity validation before inserting.  Warnings are logged
        but never block the insert.

        Parameters:
            conn: Active database connection (within a transaction).
            series_id: Series identifier.
            obs_date: Observation date.
            value: Numeric value.
            raw_payload: Optional JSON payload.
            pull_status: Pull status ('SUCCESS', 'PARTIAL', 'FAILED').
        """
        import json

        # Run sanity validation (log-only, never blocks)
        self.validate_row(series_id, obs_date, value)

        conn.execute(
            text(
                "INSERT INTO raw_series "
                "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :od, :val, :payload, :status)"
            ),
            {
                "sid": series_id,
                "src": self.source_id,
                "od": obs_date,
                "val": float(value) if value is not None else None,
                "payload": json.dumps(raw_payload) if raw_payload else None,
                "status": pull_status,
            },
        )
