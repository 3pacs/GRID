"""Read-only raw-close accessor for the provisional signal evaluation.

Resolved features are deliberately excluded: a feature name and its resolved
row do not prove whether the value came from ``close`` or ``adj_close``.
Only the exact yfinance raw-series field can establish the requested basis.
No network/provider call occurs here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from math import isfinite
from typing import Optional
from zoneinfo import ZoneInfo

from evaluation.signal_outcomes import AmbiguousPriceError as _EvaluationAmbiguous
from evaluation.signal_outcomes import UnsupportedInstrumentError as _EvaluationUnsupported
from evaluation.signal_outcomes import UnverifiedCutoverError as _EvaluationUnverifiedCutover

class UnsupportedInstrumentError(_EvaluationUnsupported):
    """The raw-close source or instrument cannot be established."""


class AmbiguousPriceError(_EvaluationAmbiguous, UnsupportedInstrumentError):
    """More than one distinct raw-close value exists for the selected date."""


class UnverifiedCutoverError(_EvaluationUnverifiedCutover, UnsupportedInstrumentError):
    """No verified raw-close ingestion cutover was supplied to this accessor."""


# fill_missing_features.py maps exactly these tickers to a 24/7 crypto series
# (BTC-USD, ETH-USD, SOL-USD, TAO-USD) via the same YF:{ticker}:{field} shape
# this accessor reads. Crypto trades continuously; this accessor's exact-date
# bar matching and after-16:00-America/New_York rollover are an equity/ETF
# NYSE-session policy, not the exchange-session policy a 24/7 instrument
# actually has. Refuse the instrument class outright rather than silently
# apply the wrong session policy to it. Case-insensitive: a lowercase ticker
# (e.g. "btc-usd") is the same instrument and must be refused identically.
_CRYPTO_TICKER_PATTERN = re.compile(r"^[A-Z0-9]{2,10}-USD$", re.IGNORECASE)


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
            raise UnverifiedCutoverError("raw-close ingestion cutover is unverified")
        if not instrument or not instrument.isascii() or not all(
            c.isalnum() or c in ".-^" for c in instrument
        ):
            raise UnsupportedInstrumentError("invalid instrument identifier")
        if _CRYPTO_TICKER_PATTERN.match(instrument):
            raise UnsupportedInstrumentError(
                "crypto/24-7 instrument refused: NYSE-session exact-date bar "
                "matching does not apply to this instrument class"
            )
        if isinstance(as_of, datetime) or not isinstance(as_of, date):
            raise TypeError("as_of must be a DATE")
        series_id = f"YF:{instrument}:close"
        # End-of-day cutoff is an explicitly provisional availability rule.
        # pull_timestamp, not obs_date, establishes that a raw observation
        # was present by the evaluation date. It cannot establish a market
        # publication timestamp before the provider pull.
        cutoff = datetime.combine(as_of, time.max, ZoneInfo("America/New_York")).astimezone(timezone.utc)
        # `raw_series`'s only uniqueness constraint is on (series_id,
        # source_id, obs_date, pull_timestamp) — pull_timestamp is part of
        # the key, so a second writer under the same series_id/source_id
        # (e.g. an adjusted-close puller sharing the "yfinance" source row)
        # can insert a second, differently-valued row for the same obs_date
        # at a later pull_timestamp. Historical YF:{ticker}:close rows are
        # confirmed contaminated this way (see PRICE_SERIES_CONTRACT.md). Do
        # not just take "latest pull wins" (ORDER BY ... LIMIT 1) — compute
        # the distinct-value count for the selected date in the same query
        # and refuse if it is ambiguous, rather than picking one silently.
        sql = text("""
            WITH candidate AS (
                SELECT r.obs_date
                  FROM raw_series r
                  JOIN source_catalog s ON s.id = r.source_id
                 WHERE LOWER(s.name) = 'yfinance'
                   AND r.series_id = :series_id
                   AND r.pull_status = 'SUCCESS'
                   AND r.obs_date <= :as_of
                   AND r.pull_timestamp >= :verified_since
                   AND r.pull_timestamp <= :cutoff
                 ORDER BY r.obs_date DESC
                 LIMIT 1
            ),
            window_rows AS (
                SELECT r.value, r.series_id, r.pull_timestamp
                  FROM raw_series r
                  JOIN source_catalog s ON s.id = r.source_id
                  JOIN candidate c ON r.obs_date = c.obs_date
                 WHERE LOWER(s.name) = 'yfinance'
                   AND r.series_id = :series_id
                   AND r.pull_status = 'SUCCESS'
                   AND r.pull_timestamp >= :verified_since
                   AND r.pull_timestamp <= :cutoff
            )
            SELECT c.obs_date,
                   (SELECT w.value FROM window_rows w ORDER BY w.pull_timestamp DESC LIMIT 1),
                   (SELECT w.series_id FROM window_rows w ORDER BY w.pull_timestamp DESC LIMIT 1),
                   (SELECT w.pull_timestamp FROM window_rows w ORDER BY w.pull_timestamp DESC LIMIT 1),
                   (SELECT COUNT(DISTINCT w.value) FROM window_rows w)
              FROM candidate c
        """)
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"series_id": series_id, "as_of": as_of, "verified_since": verified_since, "cutoff": cutoff}).fetchone()
        if row is None:
            return None
        obs_date, value, actual_series, pulled_at, distinct_value_count = row
        if actual_series != series_id:
            raise UnsupportedInstrumentError("raw series identity mismatch")
        if not isinstance(pulled_at, datetime) or pulled_at.tzinfo is None or pulled_at.utcoffset() is None or pulled_at < verified_since or pulled_at > cutoff:
            raise UnsupportedInstrumentError("raw pull timestamp is not verifiable")
        if distinct_value_count is None or distinct_value_count > 1:
            raise AmbiguousPriceError(
                f"multiple distinct raw-close values for {series_id} on {obs_date}; "
                "refusing rather than choosing one"
            )
        value = float(value)
        if not isfinite(value) or value <= 0:
            raise UnsupportedInstrumentError("raw close is not a positive finite price")
        return PricePoint(_bar_date(obs_date), value, f"yfinance:{series_id}")


# ── Completed-close accessor (replay + forward scoring) ─────────────────────
#
# PITPriceAccessor refuses every row until someone supplies a verified
# raw-close cutover, and its distinct-value check counts intraday pulls of
# the same label (12 different SPY values on 2026-04-15 alone), so it can
# never price history. This accessor reads through
# ``store.observations.read_completed_closes``: only captures pulled after
# the session closed count (yfinance and TIINGO raw closes, pooled), and a
# label whose completed vintages disagree -- across pulls or across the two
# sources, e.g. an adjusted-close writer sharing the series id -- is refused.

_US_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{0,5}(-[A-Z]{1,3})?$")  # trust_scorer._YF_SYMBOL_RE


# One hour after the latest regular NYSE close (16:00 ET; early closes are
# earlier). Measured on griddb 2026-11-06: single-stock closes exist only as
# TIINGO EOD pulls at ~17:30-19:30 ET on the session day (yfinance does not
# pull them), so the contract's next-NY-midnight boundary would refuse every
# single-stock close ever stored. A later vintage that disagrees still makes
# the label ambiguous.
US_CLOSE_COMPLETE_AFTER = time(17, 0)
COMPLETED_CLOSE_SOURCES: tuple[str, ...] = ("yfinance", "TIINGO")


def us_listed_close_boundary(obs_date: date) -> Optional[datetime]:
    """UTC instant after which a US-listed daily close for ``obs_date`` is complete.

    17:00 America/New_York on the session day (DST-aware), only for NYSE
    sessions; ``None`` otherwise.
    """
    from ingestion.market_calendar import is_market_open

    if not is_market_open(obs_date):
        return None
    return datetime.combine(obs_date, US_CLOSE_COMPLETE_AFTER,
                            ZoneInfo("America/New_York")).astimezone(timezone.utc)


class CompletedCloseAccessor:
    """``(instrument, as_of) -> PricePoint | None`` over completed yfinance closes.

    Each instrument's history is read once (one bounded series read over
    ``[start, evaluated_at]``, vintages known at ``evaluated_at``) and cached;
    call :meth:`evict` when an instrument is finished to bound memory.
    """

    def __init__(self, conn, *, start: date, evaluated_at: datetime) -> None:
        if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        self._conn = conn
        self._start = start
        self._evaluated_at = evaluated_at
        self._cache: dict[str, tuple[list[date], list[float], list[bool]]] = {}
        self.reads = 0

    def _load(self, instrument: str) -> tuple[list[date], list[float], list[bool]]:
        from store.observations import read_completed_closes

        closes = read_completed_closes(
            self._conn, f"YF:{instrument}:close",
            start=self._start, end=self._evaluated_at.astimezone(ZoneInfo("America/New_York")).date(),
            boundary=us_listed_close_boundary, source=COMPLETED_CLOSE_SOURCES,
            as_of_ts=self._evaluated_at,
        )
        self.reads += 1
        dates = sorted(closes)
        return dates, [closes[d].latest.value for d in dates], [closes[d].ambiguous for d in dates]

    def evict(self, instrument: str) -> None:
        self._cache.pop(instrument, None)

    def __call__(self, instrument: str, as_of: date) -> Optional[PricePoint]:
        from bisect import bisect_right

        if not instrument or _CRYPTO_TICKER_PATTERN.match(instrument) or not _US_TICKER_PATTERN.match(instrument):
            raise UnsupportedInstrumentError(f"not a US-listed ticker: {instrument!r}")
        if isinstance(as_of, datetime) or not isinstance(as_of, date):
            raise TypeError("as_of must be a DATE")
        if instrument not in self._cache:
            self._cache[instrument] = self._load(instrument)
        dates, values, ambiguous = self._cache[instrument]
        i = bisect_right(dates, as_of) - 1
        if i < 0:
            return None
        if ambiguous[i]:
            raise AmbiguousPriceError(f"completed YF:{instrument}:close vintages disagree on {dates[i]}")
        return PricePoint(dates[i], values[i], f"completed:YF:{instrument}:close")
