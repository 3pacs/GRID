"""
Solana wallet helper with graceful degradation.

Signing a Solana transaction requires the ``solders`` SDK (which brings in a
Rust wheel). Most GRID deployments will run paper-first and never touch a
live wallet, so this module is designed to import cleanly even when
``solders`` is not installed — you only hit the import path the moment you
try to sign or derive an address.

This mirrors the GRID convention used by ``ingestion/`` modules: construct
freely, raise on first live use.
"""

from __future__ import annotations

import base64
from typing import Any


class WalletUnavailableError(RuntimeError):
    """Raised when Solana signing is requested but ``solders`` is missing."""


def _require_solders() -> Any:
    try:
        import solders.keypair as keypair  # noqa: F401
        import solders.message as message  # noqa: F401
        import solders.transaction as transaction  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised in CI w/o deps
        raise WalletUnavailableError(
            "The 'solders' package is required for live Solana trading. "
            "Install with: pip install solders"
        ) from exc
    import solders  # type: ignore

    return solders


class SolanaWallet:
    """Wraps a base58-encoded Solana private key.

    Deliberately minimal — this class only knows how to:
      1. return its public address
      2. sign an unsigned base64 Solana transaction (for Jupiter Ultra)

    Anything else should live in a higher-level ``trading/solana`` module so
    that signing logic stays in one place for audit.
    """

    def __init__(self, private_key_base58: str) -> None:
        if not private_key_base58:
            raise ValueError("private_key_base58 must be non-empty")
        self._private_key_base58 = private_key_base58
        self._keypair: Any | None = None

    # ------------------------------------------------------------------
    # Lazy keypair construction
    # ------------------------------------------------------------------
    def _get_keypair(self) -> Any:
        if self._keypair is not None:
            return self._keypair

        _require_solders()
        from solders.keypair import Keypair  # type: ignore

        self._keypair = Keypair.from_base58_string(self._private_key_base58)
        return self._keypair

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @property
    def address(self) -> str:
        """Base58 public address of this wallet."""
        return str(self._get_keypair().pubkey())

    def sign_base64_transaction(self, b64_tx: str) -> str:
        """Sign a Jupiter Ultra unsigned transaction and return b64.

        Args:
            b64_tx: base64-encoded unsigned versioned Solana transaction
                from Jupiter Ultra's ``/order`` endpoint.

        Returns:
            The same transaction, re-encoded as base64, with our signature
            applied in-place. Ready to POST to ``/execute``.
        """
        _require_solders()
        from solders.transaction import VersionedTransaction  # type: ignore

        if not b64_tx:
            raise ValueError("b64_tx must be non-empty")

        raw = base64.b64decode(b64_tx)
        keypair = self._get_keypair()
        unsigned = VersionedTransaction.from_bytes(raw)

        # VersionedTransaction's ctor signs the message with the given keypairs;
        # this mirrors the pattern used by Jupiter's own example code.
        signed = VersionedTransaction(unsigned.message, [keypair])
        return base64.b64encode(bytes(signed)).decode("ascii")
