"""Read-only raw-close accessor for the provisional signal evaluation.

Resolved features are deliberately excluded: a feature name and its resolved
row do not prove whether the value came from ``close`` or ``adj_close``.
Only the exact yfinance raw-series field can establish the requested basis.
No network/provider call occurs here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from math import isfinite
from typing import Optional
from zoneinfo import ZoneInfo

from evaluation.signal_outcomes import UnsupportedInstrumentError as _EvaluationUnsupported

class UnsupportedInstrumentError(_EvaluationUnsupported):
    """The raw-close source or instrument cannot be established."""


@dataclass(frozen=True)
class PricePoint:
    obs_date: date
    value: float
    source_ref: str
    basis: str = "raw_close"

    @property
    def price(self) -> float:
        return self.value

    @property
    def bar_date(self) -> date:
        return self.obs_date


def _bar_date(value) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("naive bar timestamp")
        return value.astimezone(ZoneInfo("America/New_York")).date()
    if isinstance(value, date):
        return value
    raise TypeError(f"unsupported bar date type: {type(value).__name__}")


class PITPriceAccessor:
    """Select only a successful, exactly named raw-close bar visible by cutoff.

The series convention is verified in ``ingestion/yfinance_pull.py``. A
source-catalog match and exact raw_series series_id are required. The query
uses no feature_registry/resolved_series join because those tables cannot
prove the close field after multiple raw fields merge into one feature.
"""

    def __init__(self, engine, *, verified_raw_close_since: Optional[datetime] = None) -> None:
        self._engine = engine
        self._verified_raw_close_since = verified_raw_close_since

    def __call__(self, instrument: str, as_of: date) -> Optional[PricePoint]:
        from sqlalchemy import text

        # Current ingestion code passes auto_adjust=False, but historical raw
        # rows have no per-row basis/version field. A code commit timestamp is
        # not proof of when that code began producing rows. The manual CLI
        # supplies no cutover, so it refuses ambiguous legacy price history.
        verified_since = self._verified_raw_close_since
        if verified_since is None or verified_since.tzinfo is None or verified_since.utcoffset() is None:
            raise UnsupportedInstrumentError("raw-close ingestion cutover is unverified")
        if not instrument or not instrument.isascii() or not all(
            c.isalnum() or c in ".-^" for c in instrument
        ):
            raise UnsupportedInstrumentError("invalid instrument identifier")
        if isinstance(as_of, datetime) or not isinstance(as_of, date):
            raise TypeError("as_of must be a DATE")
        series_id = f"YF:{instrument}:close"
        # End-of-day cutoff is an explicitly provisional availability rule.
        # pull_timestamp, not obs_date, establishes that a raw observation
        # was present by the evaluation date. It cannot establish a market
        # publication timestamp before the provider pull.
        cutoff = datetime.combine(as_of, time.max, ZoneInfo("America/New_York")).astimezone(timezone.utc)
        sql = text("""
            SELECT r.obs_date, r.value, r.series_id, r.pull_timestamp
              FROM raw_series r
              JOIN source_catalog s ON s.id = r.source_id
             WHERE LOWER(s.name) = 'yfinance'
               AND r.series_id = :series_id
               AND r.pull_status = 'SUCCESS'
               AND r.obs_date <= :as_of
               AND r.pull_timestamp >= :verified_since
               AND r.pull_timestamp <= :cutoff
             ORDER BY r.obs_date DESC, r.pull_timestamp DESC
             LIMIT 1
        """)
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"series_id": series_id, "as_of": as_of, "verified_since": verified_since, "cutoff": cutoff}).fetchone()
        if row is None:
            return None
        obs_date, value, actual_series, pulled_at = row
        if actual_series != series_id:
            raise UnsupportedInstrumentError("raw series identity mismatch")
        if not isinstance(pulled_at, datetime) or pulled_at.tzinfo is None or pulled_at.utcoffset() is None or pulled_at < verified_since or pulled_at > cutoff:
            raise UnsupportedInstrumentError("raw pull timestamp is not verifiable")
        value = float(value)
        if not isfinite(value) or value <= 0:
            raise UnsupportedInstrumentError("raw close is not a positive finite price")
        return PricePoint(_bar_date(obs_date), value, f"yfinance:{series_id}")
