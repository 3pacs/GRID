"""CAT-35 — CFTC COT extremes + non-commercial z-scores.

The existing ``ingestion/altdata/cftc_cot.py`` pulls 17 major futures
contracts weekly. Each metric (net_speculative, commercial_long,
commercial_short, total_open_interest, etc.) is stored under
``cftc.<CONTRACT>.<metric>`` in raw_series.

This module reads those series and computes positioning EXTREMES:
  • rolling 3-year percentile rank (how extreme is current positioning?)
  • 52-week z-score vs own history
  • directional flags for contrarian signals

Why this matters (Tier A catalog #35): extreme positioning is a
well-documented contrarian indicator. When non-commercial (speculative)
longs are at 95th+ percentile in a commodity, the risk of a reversal
is elevated because there's no more marginal buying demand. This is
how firms like MRB Partners pick turning points in oil / gold / soy
decades old.

We emit a ``COTExtreme`` flag per contract per metric with:
  • current_value (raw)
  • percentile_rank (0..100 vs 3yr history)
  • z_score (vs 1yr history)
  • severity ('neutral' / 'elevated' / 'extreme')
  • direction ('long_crowd' / 'short_crowd' / 'neutral')

All functions are pure of DB semantics — compose_extremes takes the
history as a list and returns the classification. A thin reader wraps
the DB call.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Tuning constants ──────────────────────────────────────────────────────

# Rolling windows (in weekly observations)
_PERCENTILE_WINDOW = 156  # 3 years
_Z_SCORE_WINDOW = 52      # 1 year

# Severity thresholds on percentile rank
_PCTILE_ELEVATED = 85
_PCTILE_EXTREME = 95

# Minimum history required to emit a classification
_MIN_HISTORY = 40

# The CFTC contracts we care about most — reused from cftc_cot.py's canonical set
CORE_CONTRACTS: tuple[str, ...] = (
    "SP500", "NASDAQ", "RUSSELL", "DJIA",
    "USD_INDEX", "EUR", "JPY", "GBP",
    "TREASURY_10Y", "TREASURY_2Y", "EURODOLLAR",
    "GOLD", "SILVER", "COPPER",
    "CRUDE_OIL", "NATGAS",
    "CORN", "SOYBEAN", "WHEAT",
)

# Metrics we check for extremes
EXTREME_METRICS: tuple[str, ...] = (
    "net_speculative",
    "noncommercial_long",
    "noncommercial_short",
)


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class COTExtreme:
    """One contract/metric extreme classification."""

    contract: str
    metric: str
    as_of: date
    current_value: float
    percentile_rank: float      # 0..100
    z_score: float               # vs 1yr mean
    severity: str                # 'neutral' / 'elevated' / 'extreme'
    direction: str               # 'long_crowd' / 'short_crowd' / 'neutral'
    sample_size: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "metric": self.metric,
            "as_of": self.as_of.isoformat(),
            "current_value": self.current_value,
            "percentile_rank": round(self.percentile_rank, 2),
            "z_score": round(self.z_score, 4),
            "severity": self.severity,
            "direction": self.direction,
            "sample_size": self.sample_size,
        }


# ── Pure classification math ──────────────────────────────────────────────


def classify_extreme(
    *,
    contract: str,
    metric: str,
    history: Sequence[float],
    as_of: date | None = None,
) -> COTExtreme | None:
    """Classify the current value of ``history`` as extreme or not.

    Returns None when history is too short. The current value is assumed
    to be the LAST element of ``history``.
    """
    if len(history) < _MIN_HISTORY:
        return None

    arr = np.asarray(history, dtype=float)
    finite = arr[np.isfinite(arr)]
    if len(finite) < _MIN_HISTORY:
        return None

    current = float(finite[-1])

    # Percentile rank over the 3-year window (or full available history)
    pct_window = finite[-_PERCENTILE_WINDOW:] if len(finite) >= _PERCENTILE_WINDOW else finite
    below = (pct_window < current).sum()
    equal = (pct_window == current).sum()
    pct_rank = float(below + 0.5 * equal) / len(pct_window) * 100.0

    # Z-score vs 1-year window
    z_window = finite[-_Z_SCORE_WINDOW:] if len(finite) >= _Z_SCORE_WINDOW else finite
    mean = float(z_window.mean())
    std = float(z_window.std(ddof=1)) if len(z_window) > 1 else 0.0
    z = (current - mean) / std if std > 1e-9 else 0.0

    # Severity from percentile rank (either tail)
    severity = "neutral"
    if pct_rank >= _PCTILE_EXTREME or pct_rank <= (100 - _PCTILE_EXTREME):
        severity = "extreme"
    elif pct_rank >= _PCTILE_ELEVATED or pct_rank <= (100 - _PCTILE_ELEVATED):
        severity = "elevated"

    # Direction — high percentile = crowded long, low = crowded short
    if pct_rank >= _PCTILE_ELEVATED:
        direction = "long_crowd"
    elif pct_rank <= (100 - _PCTILE_ELEVATED):
        direction = "short_crowd"
    else:
        direction = "neutral"

    return COTExtreme(
        contract=contract,
        metric=metric,
        as_of=as_of or date.today(),
        current_value=current,
        percentile_rank=pct_rank,
        z_score=z,
        severity=severity,
        direction=direction,
        sample_size=len(finite),
    )


def rank_contrarian_signals(
    extremes: Sequence[COTExtreme],
) -> list[COTExtreme]:
    """Sort extremes by absolute z-score, descending.

    Extreme > elevated > neutral, then by absolute z. Used to feed the
    daily "top positioning extremes" briefing.
    """
    severity_rank = {"extreme": 0, "elevated": 1, "neutral": 2}
    return sorted(
        extremes,
        key=lambda e: (severity_rank.get(e.severity, 99), -abs(e.z_score)),
    )


# ── DB wrapper ────────────────────────────────────────────────────────────


def _read_series_history(
    engine: Engine, series_id: str, *, lookback_weeks: int = 200,
) -> list[tuple[date, float]]:
    """Read (obs_date, value) rows for a CFTC COT series."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT obs_date, value
                    FROM raw_series
                    WHERE series_id = :s
                      AND value IS NOT NULL
                      AND obs_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                    ORDER BY obs_date ASC
                    """
                ).bindparams(s=series_id, days=lookback_weeks * 7),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("cot_extremes read failed for {s}: {e}", s=series_id, e=str(exc))
        return []
    return [(r[0], float(r[1])) for r in rows]


def scan_all_extremes(
    engine: Engine,
    *,
    contracts: Sequence[str] = CORE_CONTRACTS,
    metrics: Sequence[str] = EXTREME_METRICS,
) -> list[COTExtreme]:
    """Scan every (contract, metric) pair and return classified extremes.

    Filters out None results (too-short histories). Returns the full
    list — call ``rank_contrarian_signals`` to sort.
    """
    out: list[COTExtreme] = []
    for contract in contracts:
        for metric in metrics:
            series_id = f"cftc.{contract}.{metric}"
            history = _read_series_history(engine, series_id)
            if not history:
                continue
            values = [v for _, v in history]
            as_of = history[-1][0]
            result = classify_extreme(
                contract=contract, metric=metric,
                history=values, as_of=as_of,
            )
            if result is not None:
                out.append(result)
    log.info("cot_extremes: scanned {n} pairs, found {e} extremes",
             n=len(contracts) * len(metrics),
             e=sum(1 for x in out if x.severity != "neutral"))
    return out
