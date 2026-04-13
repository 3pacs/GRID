"""
GRID — physics/greeks package.

Shared, stateless, vectorizable Black-Scholes Greek primitives used by the
dealer-flow engine (crypto + equity). See `black_scholes.py` for the math
and unit conventions.

GEX-3 / Wave 1 of docs/planning/GEX-V2-BUILD-PLAN.md.
"""

from physics.greeks.black_scholes import (
    charm,
    color,
    d1,
    d2,
    delta,
    gamma,
    speed,
    vanna,
    vomma,
    zomma,
)

__all__ = [
    "d1",
    "d2",
    "delta",
    "gamma",
    "vanna",
    "charm",
    "vomma",
    "speed",
    "color",
    "zomma",
]
