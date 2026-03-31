"""
VIX/MA Continuous Exposure Scalar.

From QuantConnect postmortem (Baldisserri):
  - HMMs fail for bear market detection — too slow
  - Single-threshold VIX (VIX > 20) didn't improve Sharpe
  - What worked: VIX above its moving average as a continuous exposure scalar

When VIX > MA → reduce exposure proportionally. Not a binary switch.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

VIX_FEATURE_ID = 105
MA_WINDOW = 20


def compute_vix_exposure_scalar(
    engine: Engine,
    as_of_date: date | None = None,
    ma_window: int = MA_WINDOW,
) -> dict:
    """
    Compute the VIX/MA exposure scalar.

    Returns dict with:
      - scalar: float in [0.0, 1.0] (1.0 = full exposure, 0.0 = flat)
      - vix: current VIX value
      - vix_ma: VIX moving average
      - ratio: VIX / VIX_MA
      - regime_hint: 'calm', 'elevated', 'stressed'
    """
    if as_of_date is None:
        as_of_date = date.today()

    lookback = as_of_date - timedelta(days=ma_window * 3)

    query = text("""
        SELECT obs_date, value
        FROM resolved_series
        WHERE feature_id = :fid
          AND obs_date BETWEEN :start AND :end
          AND release_date <= :as_of
        ORDER BY obs_date
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {"fid": VIX_FEATURE_ID, "start": lookback, "end": as_of_date, "as_of": as_of_date},
        ).fetchall()

    if len(rows) < ma_window:
        return {
            "scalar": 1.0,
            "vix": None,
            "vix_ma": None,
            "ratio": None,
            "regime_hint": "unknown",
            "error": f"insufficient data ({len(rows)} rows, need {ma_window})",
        }

    vix_series = pd.Series(
        [r[1] for r in rows], index=[r[0] for r in rows]
    ).sort_index()

    current_vix = float(vix_series.iloc[-1])
    vix_ma = float(vix_series.rolling(ma_window).mean().iloc[-1])

    if vix_ma <= 0:
        return {"scalar": 1.0, "vix": current_vix, "vix_ma": 0, "ratio": None, "regime_hint": "unknown"}

    ratio = current_vix / vix_ma

    # Scalar: 1.0 when VIX <= MA, decreasing linearly to 0.0 when VIX = 2x MA
    scalar = float(np.clip(1.0 - (ratio - 1.0), 0.0, 1.0))

    if ratio < 1.0:
        regime_hint = "calm"
    elif ratio < 1.3:
        regime_hint = "elevated"
    else:
        regime_hint = "stressed"

    return {
        "scalar": scalar,
        "vix": current_vix,
        "vix_ma": round(vix_ma, 2),
        "ratio": round(ratio, 4),
        "regime_hint": regime_hint,
    }


def compute_vix_exposure_series(
    engine: Engine,
    start_date: date | None = None,
    end_date: date | None = None,
    ma_window: int = MA_WINDOW,
) -> pd.DataFrame:
    """
    Compute VIX exposure scalar as a time series for backtesting.

    Returns DataFrame with columns: vix, vix_ma, ratio, scalar
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 3)

    lookback = start_date - timedelta(days=ma_window * 3)

    query = text("""
        SELECT obs_date, value
        FROM resolved_series
        WHERE feature_id = :fid
          AND obs_date BETWEEN :start AND :end
          AND release_date <= :as_of
        ORDER BY obs_date
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {"fid": VIX_FEATURE_ID, "start": lookback, "end": end_date, "as_of": end_date},
        ).fetchall()

    if not rows:
        return pd.DataFrame()

    vix = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows])).sort_index()
    vix_ma = vix.rolling(ma_window, min_periods=ma_window).mean()
    ratio = vix / vix_ma
    scalar = (1.0 - (ratio - 1.0)).clip(0.0, 1.0)

    df = pd.DataFrame({"vix": vix, "vix_ma": vix_ma, "ratio": ratio, "scalar": scalar})
    df = df.loc[str(start_date):str(end_date)]
    return df
