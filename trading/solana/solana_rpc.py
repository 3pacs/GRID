"""
Minimal Solana JSON-RPC client for safety checks.

Deliberately avoids the ``solders`` Rust wheel so the whole safety module
can run in any Python environment (CI, paper mode, lightweight containers).

Only implements what the safety checker actually needs:

  * ``get_mint_info`` — fetches an SPL token mint account, parses its
    82-byte layout, and reports whether the mint / freeze authorities are
    renounced.
  * ``get_token_largest_accounts`` — returns the top-20 holders by balance
    so we can compute holder-concentration ratios.

SPL Token mint layout reference:
https://github.com/solana-labs/solana-program-library/blob/master/token/program/src/state.rs

  bytes 0-4   u32  mint_authority_option   (0 = None / renounced, 1 = Some)
  bytes 4-36  [u8] mint_authority pubkey   (only meaningful if option == 1)
  bytes 36-44 u64  supply
  byte  44    u8   decimals
  byte  45    u8   is_initialized
  bytes 46-50 u32  freeze_authority_option
  bytes 50-82 [u8] freeze_authority pubkey
"""

from __future__ import annotations

import base64
import struct
import time
from dataclasses import dataclass
from typing import Any, Callable

import httpx
from loguru import logger as log

MAINNET_URL = "https://api.mainnet-beta.solana.com"
_SPL_MINT_SIZE = 82
_DEFAULT_TIMEOUT = 10.0
# Public mainnet RPC answers bursts with 429. Retry a bounded number of
# times, honouring Retry-After when given, and never sleep longer than the cap.
_RATE_LIMIT_RETRIES = 2
_RATE_LIMIT_BACKOFF_S = 1.0
_RATE_LIMIT_MAX_SLEEP_S = 5.0


class SolanaRPCError(RuntimeError):
    """Raised when the Solana RPC returns an error or malformed response."""


@dataclass(frozen=True)
class MintInfo:
    """Parsed SPL token mint account state.

    Attributes:
        mint: the mint address that was queried.
        supply: total supply in raw atoms.
        decimals: token decimals.
        is_initialized: whether the mint account has been initialized.
        mint_authority_renounced: True when the mint authority option is
            ``None`` — i.e. no more tokens can ever be minted.
        freeze_authority_renounced: True when the freeze authority option
            is ``None`` — i.e. token accounts can never be frozen.
    """

    mint: str
    supply: int
    decimals: int
    is_initialized: bool
    mint_authority_renounced: bool
    freeze_authority_renounced: bool


@dataclass(frozen=True)
class TokenHolder:
    """One row from ``getTokenLargestAccounts``.

    Attributes:
        address: the token account address (NOT the owner wallet).
        amount: raw integer balance.
        ui_amount: human-readable float balance.
    """

    address: str
    amount: int
    ui_amount: float


class SolanaRPC:
    """Thin wrapper over a handful of Solana JSON-RPC methods."""

    def __init__(
        self,
        url: str = MAINNET_URL,
        timeout: float = _DEFAULT_TIMEOUT,
        client: httpx.Client | None = None,
        rate_limit_retries: int = _RATE_LIMIT_RETRIES,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._url = url
        self._timeout = timeout
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)
        self._rate_limit_retries = max(0, rate_limit_retries)
        self._sleep = sleep

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "SolanaRPC":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Low-level JSON-RPC
    # ------------------------------------------------------------------
    def _call(self, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        data = self._post_with_rate_limit_retry(method, payload)

        if "error" in data:
            raise SolanaRPCError(
                f"{method} returned error: {data['error']}"
            )
        if "result" not in data:
            raise SolanaRPCError(f"{method} missing 'result' field")
        return data["result"]

    def _post_with_rate_limit_retry(
        self, method: str, payload: dict[str, Any]
    ) -> Any:
        """POST ``payload``; retry HTTP 429 up to ``rate_limit_retries`` times.

        Log levels (errors.jsonl must stay signal-rich, see CLAUDE.md): a 429
        that survives the retries and transport failures are transient and
        log at warning; any other HTTP failure stays at error. Every failure
        raises :class:`SolanaRPCError`, exactly as before.
        """
        attempt = 0
        while True:
            try:
                resp = self._client.post(self._url, json=payload)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 429:
                    if attempt < self._rate_limit_retries:
                        delay = _rate_limit_delay(exc.response, attempt)
                        attempt += 1
                        log.debug(
                            "Solana RPC 429 for {m}; retry {a}/{n} in {d:.1f}s",
                            m=method, a=attempt, n=self._rate_limit_retries, d=delay,
                        )
                        self._sleep(delay)
                        continue
                    log.warning(
                        "Solana RPC rate limited (HTTP 429) for {m} after {n} retries",
                        m=method, n=attempt,
                    )
                else:
                    log.error(
                        "Solana RPC HTTP error for {m}: {e}", m=method, e=str(exc),
                    )
                raise SolanaRPCError(f"{method} failed: {exc}") from exc
            except httpx.TransportError as exc:
                log.warning(
                    "Solana RPC transport error for {m}: {e}", m=method, e=str(exc),
                )
                raise SolanaRPCError(f"{method} failed: {exc}") from exc
            except httpx.HTTPError as exc:
                log.error("Solana RPC HTTP error for {m}: {e}", m=method, e=str(exc))
                raise SolanaRPCError(f"{method} failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Mint info
    # ------------------------------------------------------------------
    def get_mint_info(self, mint: str) -> MintInfo:
        """Fetch and parse an SPL token mint account.

        Raises:
            ValueError: if ``mint`` is empty.
            SolanaRPCError: on RPC failure or malformed account data.
        """
        if not mint:
            raise ValueError("mint is required")

        result = self._call(
            "getAccountInfo",
            [mint, {"encoding": "base64", "commitment": "confirmed"}],
        )

        value = result.get("value") if isinstance(result, dict) else None
        if value is None:
            raise SolanaRPCError(f"mint account not found: {mint}")

        data = value.get("data")
        if not data or not isinstance(data, list) or len(data) < 2:
            raise SolanaRPCError(
                f"unexpected account data shape for {mint}: {data!r}"
            )

        b64_blob, encoding = data[0], data[1]
        if encoding != "base64":
            raise SolanaRPCError(f"unexpected encoding {encoding!r} for {mint}")

        try:
            raw = base64.b64decode(b64_blob)
        except (ValueError, TypeError) as exc:
            raise SolanaRPCError(f"base64 decode failed for {mint}: {exc}") from exc

        return parse_mint_account(mint, raw)

    # ------------------------------------------------------------------
    # Holder concentration
    # ------------------------------------------------------------------
    def get_token_largest_accounts(self, mint: str) -> list[TokenHolder]:
        """Return the top-20 token accounts holding ``mint``.

        Note: the returned ``address`` is the SPL token account, not the
        wallet owner. For concentration ratios this is usually fine —
        co-ordinated wallets still show up as separate large accounts.
        """
        if not mint:
            raise ValueError("mint is required")

        result = self._call(
            "getTokenLargestAccounts",
            [mint, {"commitment": "confirmed"}],
        )
        rows = result.get("value", []) if isinstance(result, dict) else []

        holders: list[TokenHolder] = []
        for row in rows:
            try:
                holders.append(
                    TokenHolder(
                        address=row["address"],
                        amount=int(row.get("amount", 0)),
                        ui_amount=float(row.get("uiAmount") or 0.0),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                log.warning("Malformed holder row {r}: {e}", r=row, e=str(exc))
        return holders


def _rate_limit_delay(resp: httpx.Response, attempt: int) -> float:
    """Seconds to wait before retry ``attempt`` (0-based) after a 429.

    Uses a numeric ``Retry-After`` header when present, otherwise
    exponential backoff from ``_RATE_LIMIT_BACKOFF_S``; capped at
    ``_RATE_LIMIT_MAX_SLEEP_S`` either way.
    """
    delay = _RATE_LIMIT_BACKOFF_S * (2 ** attempt)
    raw = resp.headers.get("Retry-After") if resp is not None else None
    if raw:
        try:
            delay = max(0.0, float(raw))
        except ValueError:
            pass
    return min(delay, _RATE_LIMIT_MAX_SLEEP_S)


# ----------------------------------------------------------------------
# Parsing — exposed at module level so tests can hit it without RPC
# ----------------------------------------------------------------------
def parse_mint_account(mint: str, raw: bytes) -> MintInfo:
    """Parse the 82-byte SPL token mint account layout.

    Raises:
        SolanaRPCError: if the blob is the wrong size or contains
            an impossible option discriminant.
    """
    if len(raw) < _SPL_MINT_SIZE:
        raise SolanaRPCError(
            f"SPL mint account must be ≥ {_SPL_MINT_SIZE} bytes, got {len(raw)}"
        )

    mint_auth_opt = struct.unpack("<I", raw[0:4])[0]
    supply = struct.unpack("<Q", raw[36:44])[0]
    decimals = raw[44]
    is_initialized = bool(raw[45])
    freeze_auth_opt = struct.unpack("<I", raw[46:50])[0]

    if mint_auth_opt not in (0, 1):
        raise SolanaRPCError(
            f"invalid mint_authority_option {mint_auth_opt} for {mint}"
        )
    if freeze_auth_opt not in (0, 1):
        raise SolanaRPCError(
            f"invalid freeze_authority_option {freeze_auth_opt} for {mint}"
        )

    return MintInfo(
        mint=mint,
        supply=supply,
        decimals=decimals,
        is_initialized=is_initialized,
        mint_authority_renounced=mint_auth_opt == 0,
        freeze_authority_renounced=freeze_auth_opt == 0,
    )
