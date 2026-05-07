"""
Jupiter API client for Solana token pricing and Ultra swaps.

Ported and adapted from AutoHedge (MIT, The-Swarm-Corporation/AutoHedge):
  * autohedge/tools/jupiter_price.py
  * autohedge/tools/ultra_tools.py

Changes from the upstream version:
  * Uses httpx.Client as an explicit dependency (no Swarms wrapping)
  * Single class ``JupiterClient`` instead of module-level functions, so that
    the key/RPC can be injected for tests and so that the client can be
    reused without reading os.environ on every call.
  * ``execute_swap`` keeps transaction signing optional — when the ``solders``
    SDK is not installed the client raises a clear ``WalletUnavailableError``
    from :mod:`trading.solana.wallet` instead of importing at module load.
  * All HTTP errors are wrapped in ``JupiterError`` so callers don't need to
    import httpx.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx
from loguru import logger as log

# Canonical mint addresses — exposed for convenience so callers don't
# hard-code them.
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

_PRICE_URL = "https://api.jup.ag/price/v3"
_ULTRA_ORDER_URL = "https://api.jup.ag/ultra/v1/order"
_ULTRA_EXECUTE_URL = "https://api.jup.ag/ultra/v1/execute"
_ULTRA_HOLDINGS_URL_TMPL = "https://api.jup.ag/ultra/v1/holdings/{address}"

_DEFAULT_TIMEOUT = 10.0


class JupiterError(RuntimeError):
    """Raised when the Jupiter API returns an error or is unreachable."""


@dataclass(frozen=True)
class SwapOrder:
    """Unsigned swap order returned by Jupiter Ultra.

    Attributes:
        request_id: opaque identifier that must be echoed back to ``/execute``
        transaction: base64-encoded unsigned Solana transaction
        input_mint: mint address of the token being sold
        output_mint: mint address of the token being bought
        in_amount: raw integer amount of the input token (lamports / atoms)
        out_amount: raw integer amount of the output token (lamports / atoms)
        raw: the full response JSON, in case the caller needs extra fields
    """

    request_id: str
    transaction: str
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    raw: dict[str, Any]


class JupiterClient:
    """Thin client over Jupiter's Price V3 and Ultra swap REST APIs.

    The client is safe to construct without an API key — Jupiter's public
    endpoints are usable anonymously, but paid keys unlock higher rate limits.
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
        client: httpx.Client | None = None,
    ) -> None:
        self._api_key = api_key or None
        self._timeout = timeout
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "JupiterClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Headers
    # ------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        if self._api_key:
            return {"x-api-key": self._api_key}
        return {}

    # ------------------------------------------------------------------
    # Price API (v3)
    # ------------------------------------------------------------------
    def get_token_price(
        self,
        ids: str | list[str],
    ) -> dict[str, dict[str, Any]]:
        """Return USD prices for one or more Solana mint addresses.

        Args:
            ids: a single mint address string or a list of addresses.

        Returns:
            Mapping of mint address to a dict with at least ``usdPrice``
            and ``decimals``. Returns ``{}`` for empty input.

        Raises:
            JupiterError: on any HTTP or parse failure.
        """
        if isinstance(ids, str):
            id_list = [ids]
        else:
            id_list = list(ids)

        id_list = [i for i in id_list if i]
        if not id_list:
            log.warning("JupiterClient.get_token_price called with empty ids")
            return {}

        params = {"ids": ",".join(id_list)}
        try:
            resp = self._client.get(
                _PRICE_URL, params=params, headers=self._headers()
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            log.error("Jupiter price API error: {e}", e=str(exc))
            raise JupiterError(f"price query failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise JupiterError(f"invalid JSON from price API: {exc}") from exc

    # ------------------------------------------------------------------
    # Ultra swap API
    # ------------------------------------------------------------------
    def get_order(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        taker: str,
    ) -> SwapOrder:
        """Request an unsigned swap transaction from Jupiter Ultra.

        Args:
            input_mint: mint address of the token being sold.
            output_mint: mint address of the token being bought.
            amount: raw integer amount of the input token (lamports / atoms).
            taker: base58 wallet address that will sign and receive.

        Returns:
            A :class:`SwapOrder` with the unsigned base64 transaction.

        Raises:
            ValueError: if any parameter is missing or non-positive.
            JupiterError: on any HTTP or parse failure.
        """
        if not input_mint or not output_mint:
            raise ValueError("input_mint and output_mint are required")
        if amount <= 0:
            raise ValueError("amount must be a positive integer")
        if not taker:
            raise ValueError("taker wallet address is required")

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "taker": taker,
        }
        try:
            resp = self._client.get(
                _ULTRA_ORDER_URL, params=params, headers=self._headers()
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            log.error("Jupiter ultra order API error: {e}", e=str(exc))
            raise JupiterError(f"get_order failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise JupiterError(f"invalid JSON from ultra order API: {exc}") from exc

        required = ("requestId", "transaction", "inputMint", "outputMint")
        missing = [k for k in required if k not in data]
        if missing:
            raise JupiterError(f"ultra order response missing keys: {missing}")

        return SwapOrder(
            request_id=data["requestId"],
            transaction=data["transaction"],
            input_mint=data["inputMint"],
            output_mint=data["outputMint"],
            in_amount=int(data.get("inAmount", amount)),
            out_amount=int(data.get("outAmount", 0)),
            raw=data,
        )

    def execute_swap(
        self,
        order: SwapOrder,
        wallet: "Any",
    ) -> dict[str, Any]:
        """Sign and submit an Ultra swap order.

        Args:
            order: the unsigned :class:`SwapOrder` from :meth:`get_order`.
            wallet: a :class:`trading.solana.wallet.SolanaWallet` instance.

        Returns:
            The execute-endpoint response JSON, which contains either a
            transaction signature on success or an error code.

        Raises:
            WalletUnavailableError: if the Solana SDK is not installed.
            JupiterError: on any HTTP failure.
        """
        # Imported lazily so importing this module never fails when the
        # heavy ``solders`` SDK is absent.
        from trading.solana.wallet import SolanaWallet

        if not isinstance(wallet, SolanaWallet):
            raise ValueError("wallet must be a SolanaWallet instance")

        signed_tx = wallet.sign_base64_transaction(order.transaction)

        payload = {
            "signedTransaction": signed_tx,
            "requestId": order.request_id,
        }
        try:
            resp = self._client.post(
                _ULTRA_EXECUTE_URL, json=payload, headers=self._headers()
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            log.error("Jupiter ultra execute error: {e}", e=str(exc))
            raise JupiterError(f"execute_swap failed: {exc}") from exc

    def get_holdings(self, address: str) -> dict[str, Any]:
        """Return SOL + token holdings for a wallet address.

        Args:
            address: base58 wallet address.

        Returns:
            The Jupiter holdings JSON: ``{"sol": {"amount": lamports}, "tokens": {...}}``

        Raises:
            JupiterError: on any HTTP failure.
        """
        if not address:
            raise ValueError("address is required")
        url = _ULTRA_HOLDINGS_URL_TMPL.format(address=address)
        try:
            resp = self._client.get(url, headers=self._headers())
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            log.error("Jupiter holdings API error: {e}", e=str(exc))
            raise JupiterError(f"get_holdings failed: {exc}") from exc
