"""CAT-124 — Financial Conditions Index (multi-factor FCI).

A single-number composite of the six most load-bearing financial-conditions
signals, rolled into a rolling z-score against a 2-year history:

  1. Fed net liquidity     (COMPUTED:fed_net_liquidity — from ALPHA-5)
  2. 10Y Treasury yield    (DGS10)
  3. 2s10s spread          (T10Y2Y) — yield curve slope
  4. HY credit spread      (BAMLH0A0HYM2) — BofA HY index OAS
  5. VIX term structure    (VIX + VIX3M if present)
  6. USD index             (DXY)

Each input is z-scored against its own 504-day rolling window, then combined
via domain-direction weights (tightening vs loosening):

    FCI = -( z(net_liquidity)
           - z(dgs10)
           + z(t10y2y)              # steeper = easier
           - z(hy_spread)
           - z(vix_spot)
           - z(dxy) ) / 6

Signs are chosen so POSITIVE FCI = easier conditions (growth friendly) and
NEGATIVE FCI = tighter conditions (risk-off). The final z-score is clamped
to [-3, +3] for display.

Why this matters (Tier A catalog #124): FCI is the CONDITION under which
every prediction runs. It doesn't replace the liquidity regime classifier
(ALPHA-5) — it complements it. The regime classifier gives a discrete state
(CRISIS/TIGHTENING/NEUTRAL/EXPANSION/EXPANSION_STRONG); FCI gives a
continuous slider that tracks intra-regime dynamics.

The oracle and recommender consume FCI as:
    - Feature engineering input (per-horizon importance will pick it up
      automatically via ALPHA-6)
    - Risk sizing multiplier alongside the ALPHA-5 regime dampening
    - Display axis in the daily briefing

All public functions are pure of DB semantics — they take an engine and
return structured dataclasses. Missing inputs fall through gracefully so
the FCI reports a partial score rather than crashing.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── FCI component configuration ───────────────────────────────────────────

# Each component: (series_id, sign, label).
# Sign: +1 means HIGHER value → TIGHTER conditions (z-score enters with -1
# so the contribution is EASING when the raw series is HIGH). -1 is the
# opposite. The confusing part is that the FCI formula in the docstring
# already flips signs — here we encode the SEMANTIC direction of the raw
# input, and the formula handles the inversion.
#
# Semantics: each entry says "does HIGHER value of this series mean
# easier or tighter conditions?"

_COMPONENTS: list[tuple[str, int, str]] = [
    # Higher liquidity → easier → POSITIVE FCI direction
    ("COMPUTED:fed_net_liquidity", +1, "fed_net_liquidity"),
    # Higher 10Y yield → tighter → NEGATIVE FCI direction
    ("DGS10", -1, "dgs10"),
    # Steeper 2s10s (more positive) → easier forward conditions
    ("T10Y2Y", +1, "t10y2y"),
    # Wider HY spread → tighter
    ("BAMLH0A0HYM2", -1, "hy_spread"),
    # Higher VIX → tighter
    ("VIX", -1, "vix"),
    # Stronger USD → tighter (especially for EM + commodities)
    ("DXY", -1, "dxy"),
]

# Rolling window for z-score baseline
_ROLLING_WINDOW_DAYS = 504  # ~2 trading years

# Clamp for the final FCI value (in units of std devs from baseline)
_FCI_CLAMP = 3.0

# Minimum non-missing components required to report a score at all
_MIN_VALID_COMPONENTS = 3

# Lookback for reads (need at least ROLLING_WINDOW + a buffer)
_READ_LOOKBACK_DAYS = 800


@dataclass(frozen=True)
class FCIComponent:
    """One line item in the FCI composite."""

    label: str
    series_id: str
    raw_value: float
    z_score: float
    weighted_contribution: float
    sign: int
    sample_size: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "series_id": self.series_id,
            "raw_value": self.raw_value,
            "z_score": round(self.z_score, 4),
            "weighted_contribution": round(self.weighted_contribution, 4),
            "sign": self.sign,
            "sample_size": self.sample_size,
        }


@dataclass(frozen=True)
class FCIResult:
    """Composite FCI snapshot."""

    as_of: date
    score: float                         # [-3, +3], + = easier
    regime: str                          # "VERY_TIGHT" / "TIGHT" / "NEUTRAL" / "EASY" / "VERY_EASY"
    components: list[FCIComponent]
    missing_components: list[str]
    sample_size: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "score": round(self.score, 4),
            "regime": self.regime,
            "components": [c.to_dict() for c in self.components],
            "missing_components": list(self.missing_components),
            "sample_size": self.sample_size,
        }


# ── Classification (score → regime label) ─────────────────────────────────


def _classify_fci(score: float) -> str:
    """Map a clamped FCI score to a human-readable regime label.

    Thresholds:
      score ≤ -2.0 → VERY_TIGHT
      -2.0 < score ≤ -0.75 → TIGHT
      -0.75 < score < +0.75 → NEUTRAL
      +0.75 ≤ score < +2.0 → EASY
      score ≥ +2.0 → VERY_EASY
    """
    if score <= -2.0:
        return "VERY_TIGHT"
    if score <= -0.75:
        return "TIGHT"
    if score < 0.75:
        return "NEUTRAL"
    if score < 2.0:
        return "EASY"
    return "VERY_EASY"


# ── Pure-function composite math ──────────────────────────────────────────


def compose_fci(
    component_inputs: Sequence[tuple[str, int, Sequence[float]]],
) -> tuple[float, list[FCIComponent], list[str]]:
    """Combine N component time-series into one FCI score.

    Each input tuple is ``(label, sign, history)`` where ``history`` is the
    rolling-window series with the CURRENT value as the last element.

    Returns ``(fci_score, component_rows, missing_labels)``.
    """
    components: list[FCIComponent] = []
    missing: list[str] = []

    for label, sign, history in component_inputs:
        if not history or len(history) < 30:
            missing.append(label)
            continue

        arr = np.asarray(history, dtype=float)
        finite = arr[np.isfinite(arr)]
        if len(finite) < 30:
            missing.append(label)
            continue

        current = float(finite[-1])
        mean = float(finite.mean())
        std = float(finite.std(ddof=1)) if len(finite) > 1 else 0.0
        if std <= 1e-9:
            missing.append(label)
            continue

        z = (current - mean) / std
        # sign convention:
        #   +1 = easing signal (higher raw → easier → positive FCI)
        #   -1 = tightening signal (higher raw → tighter → negative FCI)
        # So the per-component contribution is sign × z. A +1 signal
        # with +z means "easing is happening" → FCI climbs; a -1 signal
        # with +z means "tightening is happening" → FCI drops.
        contribution = sign * z
        components.append(FCIComponent(
            label=label,
            series_id="",           # filled in by the caller
            raw_value=current,
            z_score=z,
            weighted_contribution=contribution,
            sign=sign,
            sample_size=len(finite),
        ))

    if len(components) < _MIN_VALID_COMPONENTS:
        return 0.0, components, missing

    # Average of the weighted contributions
    total = sum(c.weighted_contribution for c in components)
    score = total / len(components)

    # Clamp
    if score > _FCI_CLAMP:
        score = _FCI_CLAMP
    elif score < -_FCI_CLAMP:
        score = -_FCI_CLAMP

    return score, components, missing


# ── DB I/O ────────────────────────────────────────────────────────────────


def _read_series_history(
    engine: Engine, series_id: str, *, lookback_days: int = _READ_LOOKBACK_DAYS,
) -> list[float]:
    """Return the raw series values ordered oldest→newest.

    Empty list when the series is missing. Caller handles the empty case
    via the FCI composer's missing_components path.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT value
                    FROM raw_series
                    WHERE series_id = :s
                      AND obs_date >= (CURRENT_DATE - :days * INTERVAL '1 day')
                      AND value IS NOT NULL
                    ORDER BY obs_date ASC
                    """
                ).bindparams(s=series_id, days=lookback_days),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("FCI: read failed for {s}: {e}", s=series_id, e=str(exc))
        return []
    return [float(r[0]) for r in rows]


def compute_fci(engine: Engine) -> FCIResult:
    """Read every FCI component from the database and return the composite.

    Gracefully degrades when components are missing — partial scores are
    still produced as long as at least _MIN_VALID_COMPONENTS (3) of the 6
    inputs are present. The response includes the missing_components list
    so operators can see which legs dropped.
    """
    component_inputs: list[tuple[str, int, Sequence[float]]] = []
    component_series_map: dict[str, str] = {}

    for series_id, sign, label in _COMPONENTS:
        history = _read_series_history(engine, series_id)
        component_inputs.append((label, sign, history))
        component_series_map[label] = series_id

    score, components, missing = compose_fci(component_inputs)

    # Backfill series_id on each component row
    resolved: list[FCIComponent] = []
    for c in components:
        resolved.append(FCIComponent(
            label=c.label,
            series_id=component_series_map.get(c.label, ""),
            raw_value=c.raw_value,
            z_score=c.z_score,
            weighted_contribution=c.weighted_contribution,
            sign=c.sign,
            sample_size=c.sample_size,
        ))

    regime = _classify_fci(score)
    total_samples = sum(c.sample_size for c in resolved)

    log.info(
        "FCI: score={s:.3f} regime={r} components={n} missing={m}",
        s=score, r=regime, n=len(resolved), m=len(missing),
    )

    return FCIResult(
        as_of=date.today(),
        score=score,
        regime=regime,
        components=resolved,
        missing_components=missing,
        sample_size=total_samples,
    )
