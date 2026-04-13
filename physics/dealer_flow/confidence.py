"""
GRID — Per-contract confidence scoring for dealer_flow (GEX V2 §12).

Every normalized contract gets a ``row_confidence`` in ``[0, 1]`` that
feeds into the snapshot-level confidence score. Scoring factors
(documented in spec §12.1):

- chain completeness (missing fields → penalty)
- quote freshness (``quote_age_ms``)
- spread quality (``spread_bps``)
- Greek provenance (``exchange`` > ``recomputed`` > ``missing``)
- IV sanity (inside ``(0, 5.0]`` bounds)

The scaffold returns a fixed midpoint (0.5) so downstream callers can
exercise the full pipeline without branching on ``None``. Real weighted
formula lands in GEX-8.
"""

from __future__ import annotations

from typing import Any


def score_contract(contract: Any) -> float:
    """Return a confidence score in ``[0.0, 1.0]`` for one contract.

    Scaffold implementation returns ``0.5`` — a neutral midpoint that
    lets downstream code treat every row as "unknown quality" until the
    real scorer lands in GEX-8.

    Parameters
    ----------
    contract:
        An ``OptionContract`` (or any object with the same field shape)
        to be evaluated. Accepts ``Any`` for scaffold flexibility.

    Returns
    -------
    float
        Confidence in the interval ``[0.0, 1.0]``, where 1.0 is a fully
        complete, fresh, tight-spread, exchange-greek row.
    """
    # TODO: GEX-8 full impl — weighted sum of completeness, freshness,
    # spread, greek provenance, IV sanity, cross-venue agreement.
    _ = contract
    return 0.5
