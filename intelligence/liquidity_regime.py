"""ALPHA-5 / task #108 — Liquidity regime classifier.

Five-state classifier over the Fed net-liquidity stack. Reads the already-
computed ``COMPUTED:fed_net_liquidity`` series (produced by
``ingestion/altdata/fed_liquidity.py``) plus its 1-week and 1-month change
derivatives and classifies the current market into one of:

    CRISIS             net liquidity << historical, accelerating down
    TIGHTENING         net liquidity below median, direction down
    NEUTRAL            default / no strong signal
    EXPANSION          net liquidity above median, direction up
    EXPANSION_STRONG   net liquidity near historical high, accelerating up

The state is the **condition multiplier** the Tier A shortlist (#122) asks
for — every prediction should be dampened / amplified based on the prevailing
regime, and ALPHA-13 (per-regime sub-oracles) will eventually use the same
classifier to route between 5 sub-models.

Why percentile-based instead of HMM: HMMs need long histories to train
cleanly, and `COMPUTED:fed_net_liquidity` only has ~440 daily rows (from
2024 when the computation started). A defensible percentile + delta rule
beats a shaky HMM until we have 3+ years of coverage. The HMM upgrade is
queued as catalog entry #121 (CAT-121).

All public functions are pure of catalogue semantics — they read from the
DB but take a SQLAlchemy engine and return structured dataclasses. No
module-level state.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── State enum + thresholds ────────────────────────────────────────────────

STATE_CRISIS = "CRISIS"
STATE_TIGHTENING = "TIGHTENING"
STATE_NEUTRAL = "NEUTRAL"
STATE_EXPANSION = "EXPANSION"
STATE_EXPANSION_STRONG = "EXPANSION_STRONG"

ALL_STATES: tuple[str, ...] = (
    STATE_CRISIS,
    STATE_TIGHTENING,
    STATE_NEUTRAL,
    STATE_EXPANSION,
    STATE_EXPANSION_STRONG,
)

# State → oracle confidence multiplier. Tightening/crisis shrinks the Kelly
# fraction, expansion amplifies it (capped modestly so we never blow out the
# sizing). Neutral leaves predictions untouched.
STATE_CONFIDENCE_MULTIPLIER: dict[str, float] = {
    STATE_CRISIS: 0.60,
    STATE_TIGHTENING: 0.85,
    STATE_NEUTRAL: 1.00,
    STATE_EXPANSION: 1.10,
    STATE_EXPANSION_STRONG: 1.20,
}

# Percentile breakpoints for the LEVEL axis.
_PCTILE_CRISIS = 10   # below this percentile + down-momentum → CRISIS
_PCTILE_TIGHT = 40    # below this → TIGHTENING if direction is down
_PCTILE_EXP = 60      # above this → EXPANSION if direction is up
_PCTILE_STRONG = 85   # above this + up-momentum → EXPANSION_STRONG

# Minimum weekly change z-score for the accelerating labels.
_Z_STRONG = 1.0
# Minimum history before we trust the classifier.
_MIN_HISTORY_ROWS = 30


# ── Data classes ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LiquidityRegimeResult:
    """One-shot classification output."""

    state: str
    as_of: date
    net_liquidity: float
    level_percentile: float       # 0..100, where today sits in history
    weekly_change: float           # raw weekly delta
    weekly_change_z: float         # z-score vs history
    monthly_change: float          # raw monthly delta
    confidence_multiplier: float   # from STATE_CONFIDENCE_MULTIPLIER
    sample_size: int               # history rows used
    reason: str                    # one-line explanation

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "as_of": self.as_of.isoformat(),
            "net_liquidity": self.net_liquidity,
            "level_percentile": round(self.level_percentile, 2),
            "weekly_change": self.weekly_change,
            "weekly_change_z": round(self.weekly_change_z, 4),
            "monthly_change": self.monthly_change,
            "confidence_multiplier": self.confidence_multiplier,
            "sample_size": self.sample_size,
            "reason": self.reason,
        }


# ── Classification math (pure) ─────────────────────────────────────────────


def classify_from_series(
    *,
    history: Sequence[float],
    weekly_changes: Sequence[float],
    current_level: float,
    current_weekly: float,
    current_monthly: float,
    as_of: date,
) -> LiquidityRegimeResult:
    """Pure-function classifier — no DB I/O.

    Takes two parallel arrays (level history + weekly-change history) plus
    the current snapshot values. Returns the full result dataclass so
    callers can see both the state AND the inputs that led to it.
    """
    n = len(history)
    if n < _MIN_HISTORY_ROWS:
        return LiquidityRegimeResult(
            state=STATE_NEUTRAL,
            as_of=as_of,
            net_liquidity=current_level,
            level_percentile=50.0,
            weekly_change=current_weekly,
            weekly_change_z=0.0,
            monthly_change=current_monthly,
            confidence_multiplier=STATE_CONFIDENCE_MULTIPLIER[STATE_NEUTRAL],
            sample_size=n,
            reason=f"insufficient history ({n} rows < {_MIN_HISTORY_ROWS})",
        )

    hist_arr = np.asarray(history, dtype=float)
    week_arr = np.asarray(weekly_changes, dtype=float)

    # Level percentile — how today sits in history
    level_pct = float((hist_arr < current_level).mean() * 100.0)

    # Weekly-change z-score
    if len(week_arr) >= 2:
        w_mean = float(week_arr.mean())
        w_std = float(week_arr.std(ddof=1)) or 1e-9
        weekly_z = (current_weekly - w_mean) / w_std
    else:
        weekly_z = 0.0

    # Direction flags
    up_momentum = weekly_z >= _Z_STRONG or (current_monthly > 0 and current_weekly > 0)
    down_momentum = weekly_z <= -_Z_STRONG or (current_monthly < 0 and current_weekly < 0)

    # State assignment — check extremes first so STRONG/CRISIS beat the
    # generic EXPANSION/TIGHTENING buckets when they overlap.
    if level_pct <= _PCTILE_CRISIS and down_momentum:
        state = STATE_CRISIS
        reason = (
            f"level at {level_pct:.0f}th percentile AND weekly z={weekly_z:.2f}σ"
            " — crisis regime"
        )
    elif level_pct >= _PCTILE_STRONG and up_momentum:
        state = STATE_EXPANSION_STRONG
        reason = (
            f"level at {level_pct:.0f}th percentile AND weekly z={weekly_z:.2f}σ"
            " — strong expansion"
        )
    elif level_pct <= _PCTILE_TIGHT and current_monthly < 0:
        state = STATE_TIGHTENING
        reason = (
            f"level at {level_pct:.0f}th percentile with 1m change "
            f"{current_monthly:.2f} — tightening"
        )
    elif level_pct >= _PCTILE_EXP and current_monthly > 0:
        state = STATE_EXPANSION
        reason = (
            f"level at {level_pct:.0f}th percentile with 1m change "
            f"{current_monthly:.2f} — expansion"
        )
    else:
        state = STATE_NEUTRAL
        reason = (
            f"level at {level_pct:.0f}th percentile, no strong momentum — neutral"
        )

    return LiquidityRegimeResult(
        state=state,
        as_of=as_of,
        net_liquidity=current_level,
        level_percentile=level_pct,
        weekly_change=current_weekly,
        weekly_change_z=weekly_z,
        monthly_change=current_monthly,
        confidence_multiplier=STATE_CONFIDENCE_MULTIPLIER[state],
        sample_size=n,
        reason=reason,
    )


# ── DB I/O ─────────────────────────────────────────────────────────────────


def _read_series(
    engine: Engine, series_id: str, *, lookback_days: int = 730,
) -> list[tuple[date, float]]:
    """Read (obs_date, value) rows for ``series_id`` over the lookback window.

    Returns [] when the series is missing. Caller should handle empty lists
    gracefully — we log at debug level to avoid spam.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT obs_date, value
                    FROM raw_series
                    WHERE series_id = :s
                      AND obs_date >= (CURRENT_DATE - :days * INTERVAL '1 day')
                      AND value IS NOT NULL
                    ORDER BY obs_date ASC
                    """
                ).bindparams(s=series_id, days=lookback_days),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("_read_series failed for {s}: {e}", s=series_id, e=str(exc))
        return []
    return [(r[0], float(r[1])) for r in rows]


def classify_current_regime(
    engine: Engine, *, lookback_days: int = 730,
) -> LiquidityRegimeResult:
    """Read the latest Fed net-liquidity stack and classify the current regime.

    Uses:
      - ``COMPUTED:fed_net_liquidity``         (level)
      - ``COMPUTED:fed_net_liquidity_change_1w`` (weekly delta)
      - ``COMPUTED:fed_net_liquidity_change_1m`` (monthly delta)

    Falls back to STATE_NEUTRAL when any required series is missing.
    """
    level_rows = _read_series(
        engine, "COMPUTED:fed_net_liquidity", lookback_days=lookback_days,
    )
    weekly_rows = _read_series(
        engine, "COMPUTED:fed_net_liquidity_change_1w", lookback_days=lookback_days,
    )
    monthly_rows = _read_series(
        engine, "COMPUTED:fed_net_liquidity_change_1m", lookback_days=lookback_days,
    )

    if not level_rows:
        log.debug("classify_current_regime: no net-liquidity history")
        today = date.today()
        return LiquidityRegimeResult(
            state=STATE_NEUTRAL,
            as_of=today,
            net_liquidity=0.0,
            level_percentile=50.0,
            weekly_change=0.0,
            weekly_change_z=0.0,
            monthly_change=0.0,
            confidence_multiplier=STATE_CONFIDENCE_MULTIPLIER[STATE_NEUTRAL],
            sample_size=0,
            reason="no history",
        )

    as_of = level_rows[-1][0]
    level_values = [v for _, v in level_rows]
    weekly_values = [v for _, v in weekly_rows]

    current_level = level_values[-1]
    current_weekly = weekly_values[-1] if weekly_values else 0.0
    current_monthly = monthly_rows[-1][1] if monthly_rows else 0.0

    return classify_from_series(
        history=level_values,
        weekly_changes=weekly_values,
        current_level=current_level,
        current_weekly=current_weekly,
        current_monthly=current_monthly,
        as_of=as_of,
    )


def apply_to_confidence(confidence: float, state: str) -> float:
    """Apply the state's confidence multiplier with a [0, 1] clamp.

    Called by ``oracle/engine.py::EnsemblePredictor.predict`` after the
    catalyst + disagreement dampenings so the liquidity regime has the final
    say. Used as a multiplier, not an override — direction is untouched.
    """
    mult = STATE_CONFIDENCE_MULTIPLIER.get(state, 1.0)
    return max(0.0, min(1.0, float(confidence) * mult))
