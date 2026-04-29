"""
Exit policy definitions for Solana trading.

An :class:`ExitPolicy` is a fully-immutable recipe for how to manage an
open position from the moment it opens until the last atom is sold:

  * a take-profit *ladder* — a sequence of (trigger, fraction) rungs that
    scale out of the position as it runs
  * a hard stop-loss
  * an optional trailing stop (armed only after a configurable +%)
  * a max-hold timer so dead positions don't linger

The class is the unit of self-learning — the :class:`ExitLearner` picks
between variants and updates its posterior on each trade's outcome. Four
seed variants cover the memecoin archetype space: conservative, balanced,
aggressive, and scalper.

Sizing semantics:
  * ``close_fraction`` is always a fraction of the **original** position.
    So rungs (0.33, 0.33, 0.34) sell the full position across three hits;
    (0.25, 0.25, 0.25) leaves a 25% runner for a manual/trailing exit.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExitRung:
    """One step on a take-profit ladder."""

    trigger_pnl_pct: float  # 0.5 = trigger at +50% from entry
    close_fraction: float   # 0.33 = close 33% of original position

    def __post_init__(self) -> None:
        if self.trigger_pnl_pct <= 0:
            raise ValueError(
                f"trigger_pnl_pct must be > 0, got {self.trigger_pnl_pct}"
            )
        if not 0 < self.close_fraction <= 1:
            raise ValueError(
                f"close_fraction must be in (0, 1], got {self.close_fraction}"
            )


@dataclass(frozen=True)
class ExitPolicy:
    """A fully-specified exit strategy.

    Attributes:
        variant_id: stable identifier used by the learner.
        description: human-readable summary.
        take_profit_rungs: ladder of (trigger, fraction) pairs, in order.
        stop_loss_pct: hard stop, expressed as a negative fraction
            (-0.15 = exit at -15% from entry).
        trailing_stop_pct: optional trailing stop distance from the peak,
            as a negative fraction (-0.1 = 10% giveback from peak).
            Only applied once the position has reached
            ``activate_trailing_after_pct``.
        activate_trailing_after_pct: minimum pnl (as a positive fraction)
            at which the trailing stop arms.
        max_hold_seconds: if a position has been open longer than this,
            close the remainder regardless of pnl.
    """

    variant_id: str
    description: str
    take_profit_rungs: tuple[ExitRung, ...]
    stop_loss_pct: float
    trailing_stop_pct: float | None
    activate_trailing_after_pct: float
    max_hold_seconds: int

    def __post_init__(self) -> None:
        if not self.variant_id:
            raise ValueError("variant_id must be non-empty")
        if self.stop_loss_pct >= 0:
            raise ValueError(
                f"stop_loss_pct must be negative, got {self.stop_loss_pct}"
            )
        if self.trailing_stop_pct is not None and self.trailing_stop_pct >= 0:
            raise ValueError(
                f"trailing_stop_pct must be negative, got {self.trailing_stop_pct}"
            )
        if self.activate_trailing_after_pct < 0:
            raise ValueError(
                f"activate_trailing_after_pct must be ≥ 0, "
                f"got {self.activate_trailing_after_pct}"
            )
        if self.max_hold_seconds <= 0:
            raise ValueError(
                f"max_hold_seconds must be > 0, got {self.max_hold_seconds}"
            )
        # Rungs must be sorted ascending by trigger so the decision loop
        # can walk them in order.
        triggers = [r.trigger_pnl_pct for r in self.take_profit_rungs]
        if triggers != sorted(triggers):
            raise ValueError(
                f"take_profit_rungs must be sorted by trigger_pnl_pct, got {triggers}"
            )
        total_fraction = sum(r.close_fraction for r in self.take_profit_rungs)
        if total_fraction > 1.0 + 1e-9:
            raise ValueError(
                f"take_profit_rungs close more than 100% of position: {total_fraction}"
            )


# ----------------------------------------------------------------------
# Seed variants — the bandit's starting arms
# ----------------------------------------------------------------------
CONSERVATIVE = ExitPolicy(
    variant_id="conservative",
    description="fast scale-out, tight stop; aims for frequent small wins",
    take_profit_rungs=(
        ExitRung(0.30, 0.34),
        ExitRung(0.60, 0.33),
        ExitRung(1.20, 0.33),
    ),
    stop_loss_pct=-0.15,
    trailing_stop_pct=-0.10,
    activate_trailing_after_pct=0.25,
    max_hold_seconds=30 * 60,
)

BALANCED = ExitPolicy(
    variant_id="balanced",
    description="standard 33/33/34 ladder with a loose trailing runner",
    take_profit_rungs=(
        ExitRung(0.50, 0.33),
        ExitRung(1.00, 0.33),
        ExitRung(2.00, 0.34),
    ),
    stop_loss_pct=-0.20,
    trailing_stop_pct=-0.15,
    activate_trailing_after_pct=0.50,
    max_hold_seconds=60 * 60,
)

AGGRESSIVE = ExitPolicy(
    variant_id="aggressive",
    description="let winners breathe; wide stop; captures tail outcomes",
    take_profit_rungs=(
        ExitRung(0.80, 0.25),
        ExitRung(2.00, 0.25),
        ExitRung(5.00, 0.25),
    ),
    stop_loss_pct=-0.30,
    trailing_stop_pct=-0.20,
    activate_trailing_after_pct=0.80,
    max_hold_seconds=120 * 60,
)

SCALPER = ExitPolicy(
    variant_id="scalper",
    description="tiny targets, tight stop, short horizon",
    take_profit_rungs=(
        ExitRung(0.15, 0.50),
        ExitRung(0.30, 0.50),
    ),
    stop_loss_pct=-0.08,
    trailing_stop_pct=-0.05,
    activate_trailing_after_pct=0.10,
    max_hold_seconds=10 * 60,
)

SEED_VARIANTS: tuple[ExitPolicy, ...] = (
    CONSERVATIVE,
    BALANCED,
    AGGRESSIVE,
    SCALPER,
)


def policy_by_id(variant_id: str) -> ExitPolicy | None:
    """Return the seed variant with the given id, or None."""
    for p in SEED_VARIANTS:
        if p.variant_id == variant_id:
            return p
    return None
