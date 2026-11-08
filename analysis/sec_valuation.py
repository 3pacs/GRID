"""Trailing P/E from filed SEC EPS and Tiingo's raw close, point in time.

Replaces Tiingo fundamentals' ``TIINGO_FUND:{T}:pe_ratio`` (retired
2026-11-08: the plan covered only the DOW 30). Inputs:

* ``sec_filed_fundamentals.{T}.eps_diluted`` rows written by
  ``ingestion.altdata.sec_edgar_company`` (source ``SEC_EDGAR_Fundamentals``).
  Each row is one reported period: 10-K rows are the fiscal year, 10-Q rows
  the fiscal year-to-date (the puller keeps the longest period per end
  date). ``raw_payload.filed`` is the filing date and is the known-at date.
* ``YF:{T}:close`` from source ``TIINGO`` (raw, unadjusted close).

Trailing-twelve-month diluted EPS at the latest period end E filed by
``as_of``: the fiscal-year value when E is a fiscal year end, otherwise
``FY(prior) + YTD(E) - YTD(E - 1 year)``. Diluted EPS is not strictly
additive across periods (share counts move), so this is the standard TTM
approximation, not a restatement.

Returns ``None`` when an input is missing, stale, or the trailing EPS is not
positive (a P/E on a loss is undefined, not a large number).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text

from store.observations import read_latest

SEC_SOURCE = "SEC_EDGAR_Fundamentals"
EPS_SERIES = "sec_filed_fundamentals.{ticker}.eps_diluted"
PRICE_SERIES = "YF:{ticker}:close"
PRICE_SOURCE = "TIINGO"

# A fiscal year reported in a 10-K spans 52-53 weeks.
_FY_DAYS = (350, 380)
# The same fiscal period one year earlier ends 52-53 weeks before.
_PRIOR_YEAR_DAYS = (357, 374)
# Year-to-date periods compared across years must be the same length.
_YTD_TOLERANCE_DAYS = 10
# Newest filed period older than this is not a current trailing EPS
# (a quarter plus the 10-K filing window, with slack).
MAX_PERIOD_AGE_DAYS = 200
# Oldest close accepted as the current price.
MAX_PRICE_AGE_DAYS = 7

_EPS_SQL = text(
    "SELECT rs.obs_date, rs.value, rs.raw_payload, rs.pull_timestamp "
    "FROM raw_series rs JOIN source_catalog sc ON sc.id = rs.source_id "
    "WHERE sc.name = :source AND rs.series_id = :sid AND rs.pull_status = 'SUCCESS' "
    "AND rs.obs_date <= :as_of "
    "ORDER BY rs.obs_date, rs.pull_timestamp"
)


@dataclass(frozen=True)
class TrailingPE:
    ticker: str
    pe: float
    eps_ttm: float
    price: float
    price_date: date
    period_end: date
    filed: date


@dataclass(frozen=True)
class _Period:
    end: date
    start: date
    value: float
    filed: date

    @property
    def days(self) -> int:
        return (self.end - self.start).days


def _periods(conn: Any, ticker: str, as_of: date) -> dict[date, _Period]:
    """Latest filing known by ``as_of`` for each reported period end."""
    out: dict[date, _Period] = {}
    rows = conn.execute(
        _EPS_SQL,
        {"source": SEC_SOURCE, "sid": EPS_SERIES.format(ticker=ticker), "as_of": as_of},
    ).fetchall()
    for obs_date, value, payload, _pulled in rows:
        try:
            if isinstance(payload, str):
                payload = json.loads(payload)
            filed = date.fromisoformat(payload["filed"])
            start = date.fromisoformat(payload["period_start"])
            number = float(value)
        except (TypeError, ValueError, KeyError, AttributeError):
            continue
        if filed > as_of or start >= obs_date or number != number:
            continue
        prev = out.get(obs_date)
        if prev is None or filed >= prev.filed:
            out[obs_date] = _Period(obs_date, start, number, filed)
    return out


def trailing_eps(conn: Any, ticker: str, as_of: date) -> tuple[float, _Period] | None:
    """Trailing-twelve-month diluted EPS and the period it ends on, or None."""
    periods = _periods(conn, ticker, as_of)
    if not periods:
        return None
    latest = periods[max(periods)]
    if (as_of - latest.end).days > MAX_PERIOD_AGE_DAYS:
        return None
    if _FY_DAYS[0] <= latest.days <= _FY_DAYS[1]:
        return latest.value, latest
    fiscal_years = [
        p for p in periods.values()
        if _FY_DAYS[0] <= p.days <= _FY_DAYS[1] and p.end < latest.end
        and (latest.end - p.end).days <= _FY_DAYS[1]
    ]
    prior_ytd = [
        p for p in periods.values()
        if _PRIOR_YEAR_DAYS[0] <= (latest.end - p.end).days <= _PRIOR_YEAR_DAYS[1]
        and abs(p.days - latest.days) <= _YTD_TOLERANCE_DAYS
    ]
    if not fiscal_years or not prior_ytd:
        return None
    fy = max(fiscal_years, key=lambda p: p.end)
    prior = max(prior_ytd, key=lambda p: p.end)
    return fy.value + latest.value - prior.value, latest


def trailing_pe(conn: Any, ticker: str, as_of: date) -> TrailingPE | None:
    """Price over trailing diluted EPS, both known by ``as_of``; None if undefined."""
    eps = trailing_eps(conn, ticker, as_of)
    if eps is None or eps[0] <= 0:
        return None
    price = read_latest(conn, PRICE_SERIES.format(ticker=ticker), source=PRICE_SOURCE, as_of=as_of)
    if price is None or price.value <= 0 or (as_of - price.obs_date) > timedelta(days=MAX_PRICE_AGE_DAYS):
        return None
    eps_ttm, period = eps
    return TrailingPE(
        ticker=ticker, pe=price.value / eps_ttm, eps_ttm=eps_ttm, price=price.value,
        price_date=price.obs_date, period_end=period.end, filed=period.filed,
    )
