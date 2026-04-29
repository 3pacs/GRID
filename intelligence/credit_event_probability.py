"""CAT-162 — Credit event probability machine (per-name).

For every single name in the watchlist, compute the probability of a
credit event (default, downgrade to HY, restructuring) over two horizons
(90 days and 1 year) using the Merton-style structural approach
augmented with equity-vol signals. No ML — the math is closed-form and
defensible.

Merton distance-to-default (DTD):

    DTD = ln(V / D) + (μ - σ²/2) × T
          ─────────────────────────
                  σ × √T

where V = asset value, D = debt, μ = drift, σ = asset volatility, T = horizon.

P(default) = N(-DTD) where N is the standard normal CDF.

Inputs (graceful degradation — any missing leg marked as "partial"):
  - market_cap                 → V (equity value as asset proxy)
  - total_debt (from 10-K)     → D
  - equity_vol_30d             → σ_equity (leveraged into σ_asset)
  - credit_spread (bond)       → market's own P(default) for cross-check
  - rating_trajectory          → recent upgrades/downgrades

The output is a **range** of probabilities — Merton gives a floor,
credit spread gives a market comp, and rating trajectory adjusts for
momentum. The final P(event | 90d) + P(event | 1y) is the ensemble.

All functions are pure of DB semantics — they take structured inputs
and return dataclasses. Missing inputs don't crash; they just narrow
the set of inputs the composite averages over.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Domain constants ─────────────────────────────────────────────────────

# Equity vol → asset vol multiplier (leverage-adjusted).
# Rough rule of thumb: σ_asset ≈ σ_equity × E/(E+D).
# We use a simple 0.7 shrink when leverage is unknown.
_DEFAULT_LEVERAGE_SHRINK = 0.70

# Minimum distance-to-default below which we clamp (prevents infinite z).
_DTD_FLOOR = -10.0
_DTD_CEIL = 10.0

# Credit spread → implied default probability via CDS-style formula:
# PD(T) ≈ 1 - exp(-spread × T / (1 - recovery)), recovery assumed 40%.
_RECOVERY_RATE = 0.40

# Rating trajectory weights — how strongly recent moves pull the PD
_TRAJ_WEIGHT_DOWNGRADE = 0.15  # Additive to PD per recent downgrade
_TRAJ_WEIGHT_UPGRADE = -0.10   # Subtractive per recent upgrade

# Ensemble weights for the composite — DTD is the anchor because it has
# the strongest theoretical grounding. Spread is a market comp. Trajectory
# is a momentum adjustment.
_WEIGHTS: dict[str, float] = {
    "merton_dtd": 0.55,
    "credit_spread": 0.30,
    "rating_trajectory": 0.15,
}


@dataclass(frozen=True)
class CreditEventResult:
    """Per-name credit event probability snapshot."""

    ticker: str
    as_of: date
    p_default_90d: float          # [0, 1]
    p_default_1y: float
    dtd: float | None             # Distance-to-default (None if not computable)
    credit_spread: float | None   # bps
    rating_trajectory_score: float  # -1 .. +1 (up is bad)
    components_used: list[str]
    missing: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "as_of": self.as_of.isoformat(),
            "p_default_90d": round(self.p_default_90d, 4),
            "p_default_1y": round(self.p_default_1y, 4),
            "dtd": round(self.dtd, 4) if self.dtd is not None else None,
            "credit_spread": self.credit_spread,
            "rating_trajectory_score": round(self.rating_trajectory_score, 4),
            "components_used": list(self.components_used),
            "missing": list(self.missing),
        }


# ── Pure-function math ───────────────────────────────────────────────────


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def merton_distance_to_default(
    asset_value: float,
    debt: float,
    asset_vol: float,
    horizon_years: float,
    drift: float = 0.05,
) -> float | None:
    """Merton DTD: z-score of asset value vs debt.

    Returns None when inputs are degenerate (zero/negative asset value,
    zero vol, zero horizon).
    """
    if asset_value <= 0 or debt <= 0 or asset_vol <= 0 or horizon_years <= 0:
        return None
    log_ratio = math.log(asset_value / debt)
    drift_term = (drift - 0.5 * asset_vol * asset_vol) * horizon_years
    numerator = log_ratio + drift_term
    denominator = asset_vol * math.sqrt(horizon_years)
    dtd = numerator / denominator
    # Clamp to avoid runaway probabilities
    return max(_DTD_FLOOR, min(_DTD_CEIL, dtd))


def merton_default_probability(
    market_cap: float,
    total_debt: float,
    equity_vol_30d: float,
    horizon_years: float,
) -> tuple[float, float | None]:
    """Return (P_default, DTD) from Merton structural model.

    Uses equity as proxy for asset value (upper bound — real asset value
    is higher because debt adds cushion, but for distress measurement the
    equity-only view is a conservative signal).
    """
    asset_vol = equity_vol_30d * _DEFAULT_LEVERAGE_SHRINK
    asset_value = market_cap + total_debt * 0.6  # rough asset = equity + 60% of debt
    dtd = merton_distance_to_default(
        asset_value, total_debt, asset_vol, horizon_years,
    )
    if dtd is None:
        return 0.0, None
    return _norm_cdf(-dtd), dtd


def credit_spread_default_probability(
    spread_bps: float,
    horizon_years: float,
    recovery_rate: float = _RECOVERY_RATE,
) -> float:
    """CDS-style implied default probability from credit spread.

        PD(T) ≈ 1 - exp(-spread × T / (1 - recovery))

    spread_bps is in basis points; we convert to decimal.
    """
    if spread_bps <= 0 or horizon_years <= 0:
        return 0.0
    spread_decimal = spread_bps / 10000.0
    exponent = -spread_decimal * horizon_years / (1.0 - recovery_rate)
    return max(0.0, min(1.0, 1.0 - math.exp(exponent)))


def rating_trajectory_adjustment(
    downgrades_90d: int,
    upgrades_90d: int,
) -> float:
    """Convert recent rating changes into a [-1, +1] adjustment score.

    Positive score = bad news (more downgrades). Used additively by the
    composite to nudge PD up or down.
    """
    score = (
        downgrades_90d * _TRAJ_WEIGHT_DOWNGRADE
        + upgrades_90d * _TRAJ_WEIGHT_UPGRADE
    )
    return max(-1.0, min(1.0, score))


def compose_credit_event_probability(
    *,
    ticker: str,
    market_cap: float | None = None,
    total_debt: float | None = None,
    equity_vol_30d: float | None = None,
    credit_spread_bps: float | None = None,
    downgrades_90d: int = 0,
    upgrades_90d: int = 0,
    as_of: date | None = None,
) -> CreditEventResult:
    """Compose the per-name credit event probability.

    Every input is optional — the composite uses whatever's present and
    records the missing legs in the result. At least ONE of
    (Merton inputs, credit_spread) must be present to produce a meaningful
    score; otherwise returns PD=0 with no components used.
    """
    if as_of is None:
        as_of = date.today()

    p_90d_components: list[float] = []
    p_1y_components: list[float] = []
    p_weights: list[float] = []
    components_used: list[str] = []
    missing: list[str] = []

    # ── Merton leg ──
    dtd_val: float | None = None
    if (market_cap is not None and total_debt is not None
            and equity_vol_30d is not None and total_debt > 0):
        pd_90d, _ = merton_default_probability(
            market_cap=market_cap,
            total_debt=total_debt,
            equity_vol_30d=equity_vol_30d,
            horizon_years=90.0 / 365.0,
        )
        pd_1y, dtd_val = merton_default_probability(
            market_cap=market_cap,
            total_debt=total_debt,
            equity_vol_30d=equity_vol_30d,
            horizon_years=1.0,
        )
        p_90d_components.append(pd_90d)
        p_1y_components.append(pd_1y)
        p_weights.append(_WEIGHTS["merton_dtd"])
        components_used.append("merton_dtd")
    else:
        missing.append("merton_dtd")

    # ── Credit spread leg ──
    if credit_spread_bps is not None and credit_spread_bps > 0:
        pd_90d = credit_spread_default_probability(credit_spread_bps, 90.0 / 365.0)
        pd_1y = credit_spread_default_probability(credit_spread_bps, 1.0)
        p_90d_components.append(pd_90d)
        p_1y_components.append(pd_1y)
        p_weights.append(_WEIGHTS["credit_spread"])
        components_used.append("credit_spread")
    else:
        missing.append("credit_spread")

    # ── Rating trajectory leg ──
    trajectory_score = rating_trajectory_adjustment(downgrades_90d, upgrades_90d)
    has_trajectory = downgrades_90d > 0 or upgrades_90d > 0
    if has_trajectory:
        # Trajectory contributes via additive PD shift, not a full probability
        # Convert the [-1, +1] score into a bounded probability delta
        traj_shift = trajectory_score * 0.10  # max ±10% PD shift
        p_90d_components.append(max(0.0, min(1.0, 0.5 + traj_shift)))
        p_1y_components.append(max(0.0, min(1.0, 0.5 + traj_shift * 2.0)))
        p_weights.append(_WEIGHTS["rating_trajectory"])
        components_used.append("rating_trajectory")
    else:
        missing.append("rating_trajectory")

    # ── Compose ──
    if not p_90d_components:
        return CreditEventResult(
            ticker=ticker,
            as_of=as_of,
            p_default_90d=0.0,
            p_default_1y=0.0,
            dtd=dtd_val,
            credit_spread=credit_spread_bps,
            rating_trajectory_score=trajectory_score,
            components_used=[],
            missing=missing,
        )

    total_weight = sum(p_weights)
    p_90d = sum(p * w for p, w in zip(p_90d_components, p_weights)) / total_weight
    p_1y = sum(p * w for p, w in zip(p_1y_components, p_weights)) / total_weight

    return CreditEventResult(
        ticker=ticker,
        as_of=as_of,
        p_default_90d=p_90d,
        p_default_1y=p_1y,
        dtd=dtd_val,
        credit_spread=credit_spread_bps,
        rating_trajectory_score=trajectory_score,
        components_used=components_used,
        missing=missing,
    )


# ── DB I/O ───────────────────────────────────────────────────────────────


def _read_market_cap(engine: Engine, ticker: str) -> float | None:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT market_cap FROM ticker_metrics_daily "
                    "WHERE ticker = :t AND market_cap IS NOT NULL "
                    "ORDER BY as_of_date DESC LIMIT 1"
                ),
                {"t": ticker.upper()},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("market_cap read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    return float(row[0]) if row else None


def _read_total_debt(engine: Engine, ticker: str) -> float | None:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :s AND value IS NOT NULL "
                    "ORDER BY obs_date DESC LIMIT 1"
                ),
                {"s": f"sec_xbrl:{ticker.upper()}:total_debt"},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("total_debt read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    return float(row[0]) if row else None


def _read_equity_vol(engine: Engine, ticker: str) -> float | None:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT iv_atm FROM options_daily_signals "
                    "WHERE ticker = :t AND iv_atm IS NOT NULL "
                    "ORDER BY signal_date DESC LIMIT 1"
                ),
                {"t": ticker.upper()},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("equity_vol read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    return float(row[0]) if row else None


def compute_credit_event_probability(
    engine: Engine,
    ticker: str,
) -> CreditEventResult:
    """Read every available input and return the per-name credit event PD."""
    return compose_credit_event_probability(
        ticker=ticker,
        market_cap=_read_market_cap(engine, ticker),
        total_debt=_read_total_debt(engine, ticker),
        equity_vol_30d=_read_equity_vol(engine, ticker),
        credit_spread_bps=None,   # TODO: wire when TRACE/CDS puller lands (CAT-41)
        downgrades_90d=0,          # TODO: wire when rating_changes puller lands
        upgrades_90d=0,
    )
