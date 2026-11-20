"""Trailing baselines: every state variable read as "this far above or below its own normal".

Anik, 2026-11-13: "the state variables need to be standardized. it all needs a baseline to go up or down from."

One rule for every variable in the market-state record (``intelligence/regime/market_state.py``):

* **Baseline window** = the variable's own previous ``window`` observations, strictly before the observation being
  read. The current value never enters its own baseline, and nothing after it can.
* **Robust z** = (value - median) / (1.4826 * MAD) over that window, capped at +/- :data:`Z_CAP`. When the MAD is
  zero (a step series such as the fed funds target holding for months) the scale falls back to IQR / 1.349; when
  that is also zero the z is ``None`` and the percentile still reads.
* **Percentile** = mid-rank of the value inside the window, 0..1 (ties count half).
* **Label**: ``normal`` when |z| < 1, ``above`` / ``below`` for 1 <= |z| < 2, ``high`` / ``low`` for |z| >= 2.
  Without a z, the label comes from the percentile (< 0.1 low, < 0.25 below, > 0.75 above, > 0.9 high).
* Fewer than ``min_obs`` previous observations is ``None``: not enough history to say what normal is.

``intelligence.sleuth_intersections.robust_z`` is the same median/MAD idea tuned for return series (it drops zero
returns and caps at 50); it is left as it is because Sleuth's registered lead construction depends on it.
Pure functions, no I/O.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Sequence

import numpy as np

BASELINE_RULE = "trailing-robust/1"
MAD_SCALE = 1.4826       # MAD -> sigma for a normal distribution
IQR_SCALE = 1.349        # IQR -> sigma for a normal distribution
Z_CAP = 5.0

# Window classes (observations, not days): fast daily series look back one trading year, slow series five years.
WINDOWS: dict[str, tuple[int, int]] = {
    # name: (window, min_obs)
    "daily_fast": (252, 120),      # VIX, spreads, short volume, news: one trading year
    "daily_slow": (1260, 500),     # policy rate, curve: five trading years
    "weekly": (260, 104),          # claims, Fed balance sheet: five years of weeks
    "monthly": (60, 36),           # CPI, payrolls, industrial production: five years of months
}


@dataclass(frozen=True)
class Reading:
    """One standardized observation."""

    value: float
    median: float
    robust_z: float | None
    percentile: float
    label: str
    n: int
    window: int
    scale: str          # 'mad' | 'iqr' | 'none'

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for k in ("value", "median", "robust_z", "percentile"):
            if d[k] is not None:
                d[k] = round(float(d[k]), 6)
        return d


def _label(z: float | None, pct: float) -> str:
    if z is not None:
        a = abs(z)
        if a < 1.0:
            return "normal"
        if a < 2.0:
            return "above" if z > 0 else "below"
        return "high" if z > 0 else "low"
    if pct > 0.9:
        return "high"
    if pct > 0.75:
        return "above"
    if pct < 0.1:
        return "low"
    if pct < 0.25:
        return "below"
    return "normal"


def standardize(value: float, history: Sequence[float], *, window: int, min_obs: int) -> Reading | None:
    """Standardize ``value`` against the last ``window`` entries of ``history`` (all strictly before it).

    ``history`` must already exclude ``value``'s own observation and anything later; callers pass the series
    up to, not including, the observation being read. Non-finite history entries are dropped.
    """
    if value is None or not math.isfinite(float(value)):
        return None
    h = np.asarray([x for x in history if x is not None and math.isfinite(float(x))], dtype=np.float64)
    if window > 0:
        h = h[-window:]
    if len(h) < max(1, min_obs):
        return None
    v = float(value)
    med = float(np.median(h))
    mad = float(np.median(np.abs(h - med)))
    scale_kind = "mad"
    scale = MAD_SCALE * mad
    if scale <= 0:
        q75, q25 = np.percentile(h, [75, 25])
        scale = float(q75 - q25) / IQR_SCALE
        scale_kind = "iqr"
    z: float | None
    if scale > 0:
        z = max(-Z_CAP, min(Z_CAP, (v - med) / scale))
    else:
        z, scale_kind = None, "none"
    below = float(np.sum(h < v))
    equal = float(np.sum(h == v))
    pct = (below + 0.5 * equal) / len(h)
    return Reading(value=v, median=med, robust_z=z, percentile=pct, label=_label(z, pct), n=int(len(h)),
                   window=int(window), scale=scale_kind)


def standardize_last(series: Sequence[float], *, window: int, min_obs: int) -> Reading | None:
    """Standardize the last entry of ``series`` against the entries before it."""
    if len(series) == 0:
        return None
    return standardize(series[-1], list(series[:-1]), window=window, min_obs=min_obs)


def window_for(cadence: str) -> tuple[int, int]:
    """(window, min_obs) for a cadence class in :data:`WINDOWS`."""
    return WINDOWS[cadence]
