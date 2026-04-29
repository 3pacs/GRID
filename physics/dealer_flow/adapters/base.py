"""
GRID — Abstract venue-adapter base class for dealer_flow (GEX V2 §8).

Defines the interface every concrete options venue adapter (Deribit, OKX,
Bybit, ...) must implement. The base class is ABC-backed so attempts to
instantiate :class:`VenueAdapter` directly raise ``TypeError``.

Adapter responsibilities (per spec §8.3):
    - fetch instrument metadata in bulk
    - fetch quotes / greeks / OI in the fewest possible calls
    - normalize field names, timestamps (UTC ms), IV (decimal), contract size
    - annotate missing fields instead of silently guessing
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from physics.dealer_flow.schemas import OptionContract


class VenueAdapter(ABC):
    """Abstract base class for options venue adapters.

    Subclasses must set ``venue`` (e.g. ``"deribit"``) and implement the
    three abstract methods below. Attempting to instantiate this class
    directly raises ``TypeError`` — per the ABC contract.
    """

    #: venue identifier, e.g. "deribit", "okx", "bybit"
    venue: str = ""

    @abstractmethod
    def fetch_instruments(self, underlying: str) -> list[dict[str, Any]]:
        """Return list of raw instrument metadata dicts for ``underlying``.

        Must return a list (never a generator) and must not perform any
        normalization — raw exchange shape only.
        """
        raise NotImplementedError(
            "VenueAdapter.fetch_instruments must be implemented by subclass"
        )

    @abstractmethod
    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        """Return raw ticker / quote dict for a single option ``symbol``.

        Should include mark, bid, ask, oi, iv, and any exchange-provided
        Greeks. Raise on network / auth failure — pipeline handles retry.
        """
        raise NotImplementedError(
            "VenueAdapter.fetch_ticker must be implemented by subclass"
        )

    @abstractmethod
    def normalize(
        self,
        raw_instruments: list[dict[str, Any]],
        raw_tickers: list[dict[str, Any]],
        underlying_price: float,
    ) -> list[OptionContract]:
        """Normalize raw venue payloads into :class:`OptionContract` rows.

        Implementations must:

        - set ``venue`` on every row
        - convert timestamps to UTC ms
        - convert IV into decimal form (0.65 not 65.0 not 6500)
        - multiply Greeks by ``contract_size`` where the venue reports
          per-underlying instead of per-contract (Deribit quirk)
        - append ``data_quality_flags`` for any coercion or missing field
        - never silently fabricate rows for missing instruments
        """
        raise NotImplementedError(
            "VenueAdapter.normalize must be implemented by subclass"
        )
