"""yfinance market data for the GEX-levels paper log.

Every fetch captures its own ``fetched_at`` (UTC) alongside the data, per
the pre-registration ("with fetch time" appears on P0, VIX, and the
post-close OHLC/bars). All daily-bar fetches pass ``auto_adjust=False``
explicitly at the call site — GRID's established convention (see
``ingestion/yfinance_pull.py`` and
``tests/test_yfinance_auto_adjust_explicit.py``) for "I need the raw,
unadjusted print" call sites, which this is: "P0 ... yfinance daily bar,
unadjusted".

Every yfinance call in this module goes through :func:`_history`, the one
seam tests monkeypatch — no test in this package hits the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd

from paper_log.gex_levels.clock import now_utc
from paper_log.gex_levels.config import EASTERN
from paper_log.gex_levels.sessions import REGULAR_OPEN, expected_close_time


def _history(ticker: str, **kwargs) -> pd.DataFrame:
    """The only place this package calls into yfinance. ``auto_adjust`` is
    always passed explicitly by the caller below — never defaulted here."""
    import yfinance as yf

    return yf.Ticker(ticker).history(**kwargs)


@dataclass(frozen=True)
class PricePoint:
    price: float
    as_of_date: date
    fetched_at: datetime


@dataclass(frozen=True)
class SessionOHLC:
    open: float
    high: float
    low: float
    close: float
    fetched_at: datetime


@dataclass(frozen=True)
class Bar:
    time: datetime  # tz-aware, America/New_York
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class IntradayBars:
    bars: tuple[Bar, ...]
    fetched_at: datetime


def fetch_previous_close(ticker: str, before: date, *, now_fn=now_utc) -> PricePoint | None:
    """Latest unadjusted daily close strictly before ``before``.

    Used for both P0 (SPY previous regular-session close) and the VIX
    previous close — same rule, different ticker.
    """
    fetched_at = now_fn()
    df = _history(
        ticker,
        start=before - timedelta(days=12),
        end=before + timedelta(days=1),
        interval="1d",
        auto_adjust=False,
    )
    if df is None or df.empty:
        return None

    prior = df[df.index.date < before]
    if prior.empty:
        return None

    last = prior.iloc[-1]
    as_of = prior.index[-1].date()
    return PricePoint(price=float(last["Close"]), as_of_date=as_of, fetched_at=fetched_at)


def fetch_session_ohlc(ticker: str, session_date: date, *, now_fn=now_utc) -> SessionOHLC | None:
    """The session's own daily Open/High/Low/Close, unadjusted."""
    fetched_at = now_fn()
    df = _history(
        ticker,
        start=session_date,
        end=session_date + timedelta(days=1),
        interval="1d",
        auto_adjust=False,
    )
    if df is None or df.empty:
        return None

    row = df.iloc[0]
    return SessionOHLC(
        open=float(row["Open"]),
        high=float(row["High"]),
        low=float(row["Low"]),
        close=float(row["Close"]),
        fetched_at=fetched_at,
    )


def fetch_intraday_bars(ticker: str, session_date: date, *, now_fn=now_utc) -> IntradayBars:
    """5-minute regular-session bars for ``session_date``, ET, [09:30, close).

    "close" is the session's expected close (13:00 on a known early-close
    day, else 16:00) — see ``sessions.expected_close_time``. Bars outside
    that window (extended hours) are dropped even if yfinance returns them.
    """
    fetched_at = now_fn()
    df = _history(
        ticker,
        start=session_date,
        end=session_date + timedelta(days=1),
        interval="5m",
        auto_adjust=False,
    )

    bars: list[Bar] = []
    if df is not None and not df.empty:
        idx = df.index
        idx = idx.tz_localize(EASTERN) if idx.tz is None else idx.tz_convert(EASTERN)
        close_cutoff = expected_close_time(session_date)

        for ts, row in zip(idx, df.itertuples(index=False)):
            if ts.date() != session_date:
                continue
            t = ts.time()
            if not (REGULAR_OPEN <= t < close_cutoff):
                continue
            bars.append(
                Bar(
                    time=ts.to_pydatetime(),
                    open=float(row.Open),
                    high=float(row.High),
                    low=float(row.Low),
                    close=float(row.Close),
                )
            )

    bars.sort(key=lambda b: b.time)
    return IntradayBars(bars=tuple(bars), fetched_at=fetched_at)
