"""
Shares outstanding & market cap tracker.

Tracks the denominator changes that affect price:
  - Stock splits (handled by split_adjuster.py)
  - Share issuance (dilution from offerings, warrants, stock comp)
  - Buybacks (float reduction)

Uses Tiingo daily fundamentals for market cap, derives shares
outstanding from marketCap / price. Detects significant dilution
and buyback events by tracking shares_outstanding over time.

This data is critical for:
  - Market-cap-weighted signals (not price-weighted)
  - Detecting artificial price support from buybacks
  - Identifying dilution that masks underlying value destruction
  - Correcting drawdown calculations for heavily-diluted names (MSTR, RIVN, LCID)

Tiingo free/power tier: daily marketCap for DOW 30 only.
Tiingo commercial tier: all tickers.
Fallback: derive from yfinance sharesOutstanding field.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

_TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
_BASE_URL = "https://api.tiingo.com"


def fetch_daily_fundamentals(
    ticker: str,
    start_date: date | str = "2020-01-01",
    end_date: date | str | None = None,
) -> pd.DataFrame:
    """Fetch daily market cap from Tiingo."""
    if not _TIINGO_API_KEY:
        return pd.DataFrame()

    if end_date is None:
        end_date = date.today()

    url = f"{_BASE_URL}/tiingo/fundamentals/{ticker}/daily"
    params = {
        "token": _TIINGO_API_KEY,
        "startDate": str(start_date),
        "endDate": str(end_date),
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        if not r.ok:
            log.debug("Tiingo fundamentals failed for {t}: {s}", t=ticker, s=r.status_code)
            return pd.DataFrame()

        data = r.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        df = df.set_index("date").sort_index()
        return df

    except Exception as e:
        log.debug("Tiingo fundamentals error for {t}: {e}", t=ticker, e=str(e))
        return pd.DataFrame()


def compute_shares_outstanding(
    price_series: pd.Series,
    market_cap_series: pd.Series,
) -> pd.Series:
    """
    Derive shares outstanding from market_cap / price.

    Returns a Series of shares outstanding aligned to the price index.
    """
    common = price_series.index.intersection(market_cap_series.index)
    if len(common) < 10:
        return pd.Series(dtype=float, name="shares_outstanding")

    px = price_series.loc[common]
    mcap = market_cap_series.loc[common]

    shares = mcap / px
    shares.name = "shares_outstanding"
    return shares.dropna()


def detect_dilution_events(
    shares: pd.Series,
    threshold_pct: float = 5.0,
    window: int = 30,
) -> list[dict]:
    """
    Detect significant changes in shares outstanding.

    A dilution event is a >threshold_pct increase in shares over
    a rolling window. A buyback is a >threshold_pct decrease.
    """
    if len(shares) < window + 5:
        return []

    pct_change = shares.pct_change(window) * 100
    events = []

    for dt, chg in pct_change.items():
        if pd.isna(chg):
            continue
        if chg > threshold_pct:
            events.append({
                "date": dt,
                "type": "dilution",
                "change_pct": round(chg, 2),
                "shares_before": float(shares.shift(window).loc[dt]) if dt in shares.shift(window).index else None,
                "shares_after": float(shares.loc[dt]),
            })
        elif chg < -threshold_pct:
            events.append({
                "date": dt,
                "type": "buyback",
                "change_pct": round(chg, 2),
                "shares_before": float(shares.shift(window).loc[dt]) if dt in shares.shift(window).index else None,
                "shares_after": float(shares.loc[dt]),
            })

    return events


def market_cap_adjusted_return(
    price_series: pd.Series,
    market_cap_series: pd.Series,
    periods: int = 1,
) -> pd.Series:
    """
    Compute returns based on market cap changes, not price changes.

    This captures the true economic return including dilution/buyback effects.
    A company doing a secondary offering at a 10% discount shows as a ~10%
    price drop, but market cap may barely move — the return is near zero,
    not -10%.
    """
    common = price_series.index.intersection(market_cap_series.index)
    mcap = market_cap_series.loc[common]
    return mcap.pct_change(periods).dropna()


def get_dilution_adjusted_price(
    price_series: pd.Series,
    shares_series: pd.Series,
) -> pd.Series:
    """
    Adjust price for dilution/buyback by normalizing to a fixed share count.

    Uses the most recent shares outstanding as the base. All historical
    prices are adjusted as: adj_price = price * (shares_at_date / shares_current)

    This means:
      - If a company doubled its share count via offering, historical prices
        are scaled UP (the old price represented more ownership per share)
      - If a company bought back 20% of shares, historical prices are scaled
        DOWN (the old price represented less ownership per share)
    """
    common = price_series.index.intersection(shares_series.index)
    if len(common) < 10:
        return price_series.copy()

    current_shares = float(shares_series.iloc[-1])
    if current_shares <= 0:
        return price_series.copy()

    adjustment = shares_series / current_shares
    adjusted = price_series.copy()
    adjusted.loc[common] = price_series.loc[common] * adjustment.loc[common]

    return adjusted
