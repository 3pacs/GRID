"""
GRID — Deribit venue adapter for the dealer_flow subpackage (GEX V2 §8.2).

SKELETON ONLY. Real CCXT calls and normalization logic land in GEX-8.

This stub guarantees:

- the module imports cleanly even when ``ccxt`` is not installed (the
  CI box and developer laptops don't need the exchange client pinned in
  ``requirements.txt`` until Wave-2b goes live)
- :class:`DeribitAdapter` is instantiable with ``testnet`` and
  ``rate_limit_ms`` kwargs so downstream tests can construct it without
  touching the network
- every abstract method inherited from :class:`VenueAdapter` raises
  ``NotImplementedError`` with a pointer to the tracking task

Reference implementation: ``/Users/anikdang/grid_obsidian/Gex Grok MD.md``
(``fetch_crypto_option_chain``) — to be ported in GEX-8.
"""

from __future__ import annotations

from typing import Any

from physics.dealer_flow.adapters.base import VenueAdapter
from physics.dealer_flow.schemas import OptionContract

# Optional CCXT import. The live adapter requires it, but the scaffold
# must import cleanly in environments where ccxt is not installed.
try:  # pragma: no cover — exercised indirectly by test_dealer_flow_scaffold
    import ccxt  # type: ignore
    _CCXT_AVAILABLE = True
except ImportError:  # pragma: no cover
    ccxt = None  # type: ignore[assignment]
    _CCXT_AVAILABLE = False


class DeribitAdapter(VenueAdapter):
    """CCXT-backed Deribit adapter. STUB — see GEX-8 for full impl."""

    venue = "deribit"

    def __init__(
        self,
        testnet: bool = False,
        rate_limit_ms: int = 250,
        client: Any | None = None,
    ) -> None:
        """Store config; do NOT open a network connection here.

        Real impl (GEX-8) will lazily instantiate ``ccxt.deribit`` on first
        fetch so unit tests can pass a mock ``client`` and skip the import.
        """
        self.testnet = testnet
        self.rate_limit_ms = rate_limit_ms
        self._client = client  # injected mock for tests
        self._ccxt_available = _CCXT_AVAILABLE

    # ------------------------------------------------------------------
    # VenueAdapter contract
    # ------------------------------------------------------------------

    def fetch_instruments(self, underlying: str) -> list[dict[str, Any]]:
        # TODO: GEX-8 full impl — port fetch_crypto_option_chain from Grok MD
        raise NotImplementedError(
            "DeribitAdapter.fetch_instruments is a stub — see GEX-8"
        )

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        # TODO: GEX-8 full impl — batched ticker fetch via CCXT
        raise NotImplementedError(
            "DeribitAdapter.fetch_ticker is a stub — see GEX-8"
        )

    def normalize(
        self,
        raw_instruments: list[dict[str, Any]],
        raw_tickers: list[dict[str, Any]],
        underlying_price: float,
    ) -> list[OptionContract]:
        # TODO: GEX-8 full impl — Deribit delivers Greeks per-BTC, must
        # multiply by contract_size; IV already decimal; ts in UTC ms.
        raise NotImplementedError(
            "DeribitAdapter.normalize is a stub — see GEX-8"
        )
