"""
Build PIT-correct ticker panel data from GRID's resolved_series.

GRID stores close prices as {ticker}_full features and volume as {ticker}_avg_volume.
This module builds the multi-ticker DataFrames that signals operate on.

All price panels are automatically split-adjusted via the universal
split adjuster. This is a top-level data concern — every downstream
consumer (signals, backtest, models, scanners) gets clean data.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alpha_research.data.split_adjuster import adjust_panel


def _feature_name_to_ticker(name: str) -> str:
    """spy_full -> SPY, qqq_avg_volume -> QQQ."""
    return name.replace("_full", "").replace("_avg_volume", "").upper()


def build_price_panel(
    engine: Engine,
    tickers: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    """
    Build a dates x tickers close-price panel from resolved_series.

    PIT constraint: release_date <= as_of_date (default: today).
    Returns DataFrame with DatetimeIndex and uppercase ticker columns.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    ticker_filter = ""
    params: dict = {
        "start": start_date,
        "end": end_date,
        "as_of": as_of_date,
    }

    if tickers:
        names = [f"{t.lower()}_full" for t in tickers]
        ticker_filter = "AND fr.name = ANY(:names)"
        params["names"] = names

    query = text(f"""
        SELECT fr.name, rs.obs_date, rs.value
        FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name LIKE '%%_full'
          AND rs.obs_date BETWEEN :start AND :end
          AND rs.release_date <= :as_of
          {ticker_filter}
        ORDER BY rs.obs_date, fr.name
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["feature_name", "obs_date", "value"])
    df["ticker"] = df["feature_name"].apply(_feature_name_to_ticker)
    df["obs_date"] = pd.to_datetime(df["obs_date"])

    # Drop duplicates keeping first (highest-priority source) per (date, ticker)
    df = df.drop_duplicates(subset=["obs_date", "ticker"], keep="first")

    panel = df.pivot(index="obs_date", columns="ticker", values="value")
    panel.sort_index(inplace=True)

    # Universal split adjustment — every downstream consumer gets clean data
    panel = adjust_panel(panel)

    return panel


def build_volume_panel(
    engine: Engine,
    tickers: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    """
    Build a dates x tickers volume panel from resolved_series.

    Uses {ticker}_avg_volume features.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    ticker_filter = ""
    params: dict = {
        "start": start_date,
        "end": end_date,
        "as_of": as_of_date,
    }

    if tickers:
        names = [f"{t.lower()}_avg_volume" for t in tickers]
        ticker_filter = "AND fr.name = ANY(:names)"
        params["names"] = names

    query = text(f"""
        SELECT fr.name, rs.obs_date, rs.value
        FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name LIKE '%%_avg_volume'
          AND rs.obs_date BETWEEN :start AND :end
          AND rs.release_date <= :as_of
          {ticker_filter}
        ORDER BY rs.obs_date, fr.name
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["feature_name", "obs_date", "value"])
    df["ticker"] = df["feature_name"].apply(_feature_name_to_ticker)
    df["obs_date"] = pd.to_datetime(df["obs_date"])

    panel = df.pivot(index="obs_date", columns="ticker", values="value")
    panel.sort_index(inplace=True)
    return panel


def build_returns_panel(price_panel: pd.DataFrame) -> pd.DataFrame:
    """Compute daily returns from close prices."""
    return price_panel.pct_change(fill_method=None)


def get_available_tickers(engine: Engine) -> list[str]:
    """Return all tickers that have _full price data."""
    query = text(
        "SELECT name FROM feature_registry WHERE name LIKE '%%_full' ORDER BY name"
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return [_feature_name_to_ticker(row[0]) for row in rows]


def get_vix_series(
    engine: Engine,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
) -> pd.Series:
    """
    Get VIX time series from FRED data in resolved_series.

    Looks for feature named 'vixcls' or 'vix_full' or similar.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if end_date is None:
        end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)

    query = text("""
        SELECT rs.obs_date, rs.value
        FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE (fr.name ILIKE '%%vix%%' AND fr.family = 'vol')
          AND rs.obs_date BETWEEN :start AND :end
          AND rs.release_date <= :as_of
        ORDER BY rs.obs_date
        LIMIT 5000
    """)

    params = {"start": start_date, "end": end_date, "as_of": as_of_date}
    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pd.Series(dtype=float, name="VIX")

    s = pd.Series(
        [r[1] for r in rows],
        index=pd.to_datetime([r[0] for r in rows]),
        name="VIX",
    )
    return s.sort_index()
