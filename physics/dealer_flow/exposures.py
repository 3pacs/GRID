"""
GRID — Dealer-flow exposure aggregators (GEX V2 §6, §11).

Vectorized aggregation helpers for the three load-bearing exposures in
Wave 2: gamma (GEX), vanna (VEX), and charm (CEX). The other four
Greeks (vomma/color/zomma/speed) get their own functions in GEX-8.

Sign convention (documented ONCE, §6.3):

- dealers are assumed net-short options (market-making)
- GEX: calls contribute positive, puts contribute negative
- CEX: daily delta drift from charm, signed per dealer book
- VEX: delta sensitivity per 1 vol-point move (0.01 decimal IV)

This module imports the Black-Scholes primitives from
``physics.greeks.black_scholes`` (GEX-3). Because GEX-3 is a parallel
task, the import is wrapped in ``try/except ImportError`` so the
scaffold degrades gracefully until the shared module lands.
"""

from __future__ import annotations

from typing import Any

# Soft dependency on the shared Black-Scholes module (GEX-3).
try:  # pragma: no cover
    from physics.greeks import black_scholes as _bs  # type: ignore
    _BS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _bs = None  # type: ignore[assignment]
    _BS_AVAILABLE = False


def dealer_gex(
    contracts: list[Any],
    spot: float,
    *,
    sign_convention: str = "dealers_short",
) -> float | None:
    """Aggregate per-contract gamma into a dealer-GEX scalar.

    Formula (spec §6.2):

        gex = Σ gamma_i × oi_i × contract_size_i × spot^2 × 0.01 × sign_i

    Returns ``None`` when the scaffold cannot compute (e.g. BS module
    missing AND no exchange-supplied gamma).

    Full vectorized implementation lands in GEX-8.
    """
    # TODO: GEX-8 full impl — vectorized pandas / numpy version
    _ = (contracts, spot, sign_convention, _BS_AVAILABLE)
    return None


def dealer_vanna(
    contracts: list[Any],
    spot: float,
    *,
    vol_point_size: float = 0.01,
) -> float | None:
    """Aggregate per-contract vanna into a dealer-VEX scalar.

    Delta sensitivity to a 1 vol-point (default ``0.01`` decimal IV).
    """
    # TODO: GEX-8 full impl
    _ = (contracts, spot, vol_point_size, _BS_AVAILABLE)
    return None


def dealer_charm(
    contracts: list[Any],
    spot: float,
    *,
    day_fraction: float = 1.0 / 365.0,
) -> float | None:
    """Aggregate per-contract charm into a dealer-CEX scalar.

    Delta drift from one calendar day of time decay, annualized charm
    converted to a daily change explicitly per §6.2.
    """
    # TODO: GEX-8 full impl
    _ = (contracts, spot, day_fraction, _BS_AVAILABLE)
    return None
