"""
GRID — Orchestrator for the dealer_flow pipeline (GEX V2 §4).

Wires the five layers together for one snapshot:

    adapter.fetch_instruments + fetch_ticker
        ↓
    adapter.normalize → list[OptionContract]
        ↓
    validate (§9 hard/soft rules)                           [GEX-9]
        ↓
    complete missing Greeks via physics.greeks.black_scholes [GEX-3]
        ↓
    exposures.dealer_gex / dealer_vanna / dealer_charm
        ↓
    confidence.score_contract → row_confidence
        ↓
    OptionExposure payload

Skeleton only — :func:`run` returns a placeholder dict and logs a
NotImplementedError hint. Full orchestration lands in GEX-8.
"""

from __future__ import annotations

from typing import Any


def run(
    venue: str,
    instruments: list[str] | None = None,
    *,
    underlying: str = "BTC",
    max_dte_days: int = 7,
) -> dict[str, Any]:
    """Run the dealer-flow pipeline for one venue.

    Parameters
    ----------
    venue:
        Venue identifier, e.g. ``"deribit"``. Resolved to the concrete
        adapter via a registry (to be implemented in GEX-8).
    instruments:
        Optional explicit symbol list. When ``None``, the adapter is
        asked to discover all instruments for ``underlying``.
    underlying:
        Underlying asset, e.g. ``"BTC"``, ``"ETH"``.
    max_dte_days:
        Upper bound on days-to-expiry (spec §3).

    Returns
    -------
    dict
        Placeholder payload with the input echo and a ``status`` field.
        Real impl returns an :class:`OptionExposure` model dump.
    """
    # TODO: GEX-8 full impl
    #   1. resolve adapter from venue registry
    #   2. adapter.fetch_instruments(underlying) — filter by max_dte_days
    #   3. adapter.fetch_ticker per symbol (batched)
    #   4. adapter.normalize → list[OptionContract]
    #   5. validation.run(contracts) → clean, rejected, metrics
    #   6. greek_completion.run(clean) — uses physics.greeks.black_scholes
    #   7. exposures.dealer_gex + dealer_vanna + dealer_charm
    #   8. confidence.score_contract per row, then snapshot-level score
    #   9. persist to option_exposures table (migration 0037)
    #  10. return OptionExposure payload
    return {
        "status": "stub",
        "venue": venue,
        "underlying": underlying,
        "instruments": instruments or [],
        "max_dte_days": max_dte_days,
        "note": "physics.dealer_flow.pipeline.run is a scaffold — see GEX-8",
    }
