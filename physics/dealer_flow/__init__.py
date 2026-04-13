"""
GRID — physics.dealer_flow subpackage.

Crypto-first options dealer-flow alpha layer per docs/planning/GEX-V2-SPEC.md.
Skeleton only; full implementation is tracked as GEX-8 and downstream tasks.

This subpackage is the V2 home for normalized contract schemas, venue
adapters (Deribit first via CCXT), the validation + Greek-completion
pipeline, exposure aggregation (GEX/CEX/VEX/VOEX/COLEX/ZEX/SPEEDEX),
and per-snapshot confidence scoring.

Public API (re-exports):
    - schemas: OptionContract, OptionSnapshot, OptionExposure
    - adapters: VenueAdapter (abstract), DeribitAdapter (stub)
    - pipeline: run
    - exposures: dealer_gex, dealer_vanna, dealer_charm
    - confidence: score_contract
"""

from __future__ import annotations

from physics.dealer_flow.schemas import (
    OptionContract,
    OptionSnapshot,
    OptionExposure,
)
from physics.dealer_flow.adapters.base import VenueAdapter
from physics.dealer_flow.adapters.deribit import DeribitAdapter
from physics.dealer_flow.pipeline import run
from physics.dealer_flow.exposures import (
    dealer_gex,
    dealer_vanna,
    dealer_charm,
)
from physics.dealer_flow.confidence import score_contract

__all__ = [
    "OptionContract",
    "OptionSnapshot",
    "OptionExposure",
    "VenueAdapter",
    "DeribitAdapter",
    "run",
    "dealer_gex",
    "dealer_vanna",
    "dealer_charm",
    "score_contract",
]
