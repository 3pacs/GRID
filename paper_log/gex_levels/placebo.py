"""Mirror-placebo levels.

Pre-registration: "Placebo levels: each real level L is mirrored around P0
(L' = 2*P0 - L). A placebo within 0.10% of any real level is dropped."
"""

from __future__ import annotations

from dataclasses import dataclass

from paper_log.gex_levels.config import LEVEL_NAMES, PLACEBO_COLLISION_THRESHOLD_PCT


@dataclass(frozen=True)
class PlaceboLevel:
    name: str
    value: float
    dropped: bool
    collided_with: str | None  # which real level triggered the drop, if any


def mirror(level: float, p0: float) -> float:
    """L' = 2*P0 - L."""
    return 2.0 * p0 - level


def build_placebos(real_levels: dict[str, float], p0: float) -> dict[str, PlaceboLevel]:
    """Mirror every real level around P0 and apply the 0.10% collision drop.

    ``real_levels`` maps level name -> value for whichever of
    {gamma_flip, put_wall, call_wall} are available this session (a level
    missing from the dict — e.g. engine_unavailable never reaches here, but
    a partially-available future engine result could — simply has no
    placebo).
    """
    placebos: dict[str, PlaceboLevel] = {}

    for name in LEVEL_NAMES:
        if name not in real_levels:
            continue
        mirrored = mirror(real_levels[name], p0)

        collided_with = _find_collision(mirrored, real_levels)
        placebos[name] = PlaceboLevel(
            name=name,
            value=mirrored,
            dropped=collided_with is not None,
            collided_with=collided_with,
        )

    return placebos


def _find_collision(mirrored: float, real_levels: dict[str, float]) -> str | None:
    """First real level (by LEVEL_NAMES order) within 0.10% of ``mirrored``."""
    for real_name in LEVEL_NAMES:
        real_value = real_levels.get(real_name)
        if real_value is None or real_value == 0:
            continue
        if abs(mirrored - real_value) / abs(real_value) <= PLACEBO_COLLISION_THRESHOLD_PCT:
            return real_name
    return None
