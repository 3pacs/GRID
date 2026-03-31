"""
Credit Cycle Detector.

From Factor Zoo (JPM 2024 Best Paper + ML Factor Zoo ScienceDirect 2024):
  - Only two alternating subsets of 3-4 characteristics dominate ML portfolio returns
  - Timing aligns with the US credit cycle:
    - Credit contraction → arbitrage constraint chars: Ivol, max/min effect
    - Credit expansion → financial constraint chars: profitability, external financing

GRID has HY spread (BAMLH0A0HYM2) and M2 money supply — both sufficient
to determine credit cycle state.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

HY_SPREAD_FEATURE_ID = 103
M2_FEATURE_ID = 117

# 6-month trend window (approx 126 trading days)
TREND_WINDOW_DAYS = 126


def compute_credit_cycle(
    engine: Engine,
    as_of_date: date | None = None,
) -> dict:
    """
    Determine current credit cycle state.

    Logic:
      - 6-month trend of HY spread: rising = contraction, falling = expansion
      - 6-month trend of M2 growth: falling = contraction, rising = expansion
      - Combined signal with confidence

    Returns dict with:
      - state: "contraction" or "expansion"
      - confidence: float [0, 1]
      - hy_spread_trend: float (positive = widening = stress)
      - m2_trend: float (positive = growing = easing)
      - signal_families: which factor families to trust
    """
    if as_of_date is None:
        as_of_date = date.today()

    lookback = as_of_date - timedelta(days=TREND_WINDOW_DAYS * 2)

    hy = _get_feature_series(engine, HY_SPREAD_FEATURE_ID, lookback, as_of_date)
    m2 = _get_feature_series(engine, M2_FEATURE_ID, lookback, as_of_date)

    signals = []

    # HY spread trend: rising spreads = contraction
    if len(hy) >= TREND_WINDOW_DAYS // 2:
        hy_pct = float(hy.iloc[-1] / hy.iloc[-min(TREND_WINDOW_DAYS, len(hy))] - 1)
        hy_signal = 1.0 if hy_pct > 0 else -1.0  # +1 = contraction
        hy_strength = min(abs(hy_pct) * 5, 1.0)  # scale to [0, 1]
        signals.append(("hy_spread", hy_signal, hy_strength))
    else:
        hy_pct = None

    # M2 trend: falling M2 growth = contraction
    if len(m2) >= TREND_WINDOW_DAYS // 2:
        m2_pct = float(m2.iloc[-1] / m2.iloc[-min(TREND_WINDOW_DAYS, len(m2))] - 1)
        m2_signal = 1.0 if m2_pct < 0 else -1.0  # +1 = contraction
        m2_strength = min(abs(m2_pct) * 10, 1.0)
        signals.append(("m2", m2_signal, m2_strength))
    else:
        m2_pct = None

    if not signals:
        return {
            "state": "expansion",
            "confidence": 0.0,
            "hy_spread_trend": None,
            "m2_trend": None,
            "signal_families": _expansion_families(),
            "error": "insufficient data",
        }

    # Weighted vote
    total_weight = sum(s[2] for s in signals)
    if total_weight == 0:
        composite = 0.0
    else:
        composite = sum(s[1] * s[2] for s in signals) / total_weight

    state: Literal["contraction", "expansion"] = "contraction" if composite > 0 else "expansion"
    confidence = min(abs(composite), 1.0)

    return {
        "state": state,
        "confidence": round(confidence, 4),
        "hy_spread_trend": round(hy_pct, 4) if hy_pct is not None else None,
        "m2_trend": round(m2_pct, 4) if m2_pct is not None else None,
        "signal_families": (
            _contraction_families() if state == "contraction" else _expansion_families()
        ),
    }


def _contraction_families() -> dict:
    """Signal families that work during credit contraction."""
    return {
        "prefer": ["vol", "alternative"],
        "reason": "Arbitrage constraints dominate: Ivol, max/min effect (Bali 2011)",
        "avoid": ["earnings", "flows"],
    }


def _expansion_families() -> dict:
    """Signal families that work during credit expansion."""
    return {
        "prefer": ["equity", "flows", "earnings"],
        "reason": "Financial constraints dominate: profitability (Novy-Marx), external financing (Bradshaw)",
        "avoid": ["vol"],
    }


def _get_feature_series(
    engine: Engine, feature_id: int, start: date, end: date
) -> pd.Series:
    """Fetch a single feature as a time series from resolved_series."""
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
            query, {"fid": feature_id, "start": start, "end": end, "as_of": end}
        ).fetchall()

    if not rows:
        return pd.Series(dtype=float)

    return pd.Series(
        [r[1] for r in rows],
        index=pd.to_datetime([r[0] for r in rows]),
    ).sort_index()
