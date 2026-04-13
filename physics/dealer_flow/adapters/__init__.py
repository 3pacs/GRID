"""
GRID — physics.dealer_flow.adapters subpackage.

Venue adapters map raw exchange responses (Deribit, OKX, Bybit, ...) into
the normalized ``OptionContract`` schema defined in ``schemas.py``.

Every concrete adapter must subclass :class:`VenueAdapter` from ``base``
and implement ``fetch_instruments``, ``fetch_ticker``, and ``normalize``.

The Deribit adapter is the Wave-2 MVP; OKX/Bybit land in later waves.
"""

from __future__ import annotations

from physics.dealer_flow.adapters.base import VenueAdapter
from physics.dealer_flow.adapters.deribit import DeribitAdapter

__all__ = ["VenueAdapter", "DeribitAdapter"]
