"""
GRID x402 Agent Micropayments.

Implements the x402 payment protocol (Google AP2 + Coinbase) for
agent-native pay-per-request API access using stablecoins on Base L2.

How it works:
  1. Agent hits a paid GRID endpoint without payment header
  2. GRID returns HTTP 402 with x402 payment requirements
  3. Agent constructs a payment (USDC on Base L2)
  4. Agent retries with X-PAYMENT header containing the signed tx
  5. GRID verifies payment and serves the response

No accounts, no subscriptions — just micropayments per API call.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any

from loguru import logger as log


@dataclass(frozen=True)
class PricingTier:
    """Per-endpoint pricing configuration.

    Attributes:
        endpoint: API endpoint path pattern.
        price_usd: Price in USD (converted to USDC at 1:1).
        description: Human-readable description of what's being paid for.
    """

    endpoint: str
    price_usd: float
    description: str


# Default GRID API pricing
DEFAULT_PRICING: list[PricingTier] = [
    PricingTier("/api/v1/forecasts/generate", 0.01, "Single TimesFM forecast"),
    PricingTier("/api/v1/forecasts/batch", 0.05, "Batch TimesFM forecast (up to 100 series)"),
    PricingTier("/api/v1/oracle/predictions", 0.02, "Oracle prediction with signals"),
    PricingTier("/api/v1/regime/current", 0.005, "Current market regime"),
    PricingTier("/api/v1/signals", 0.01, "Signal analysis query"),
    PricingTier("/api/v1/intelligence/actors", 0.02, "Actor network query"),
    PricingTier("/api/v1/options/flow", 0.02, "Options flow analysis"),
]


@dataclass
class PaymentRecord:
    """Record of a verified x402 payment.

    Attributes:
        payment_id: Unique payment identifier (hash of tx).
        endpoint: API endpoint that was paid for.
        amount_usd: Amount paid in USD.
        payer_address: Blockchain address of the payer.
        tx_hash: Transaction hash on Base L2.
        verified_at: Unix timestamp of verification.
        metadata: Additional payment metadata.
    """

    payment_id: str
    endpoint: str
    amount_usd: float
    payer_address: str
    tx_hash: str
    verified_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class PaymentVerifier:
    """Verifies x402 payment headers against Base L2.

    In production, this would verify the on-chain transaction.
    Currently implements the protocol structure with signature
    verification placeholder for when Coinbase CDP SDK is integrated.

    Parameters:
        receiver_address: GRID's USDC receiving address on Base.
        network: Blockchain network ("base" or "base-sepolia").
        token: Payment token ("USDC").
    """

    def __init__(
        self,
        receiver_address: str,
        network: str = "base",
        token: str = "USDC",
    ) -> None:
        self.receiver_address = receiver_address
        self.network = network
        self.token = token
        self._ledger: list[PaymentRecord] = []

    def verify_payment(
        self,
        payment_header: str,
        expected_amount: float,
        endpoint: str,
    ) -> PaymentRecord | None:
        """Verify an x402 payment header.

        Parameters:
            payment_header: The X-PAYMENT header value (JSON-encoded).
            expected_amount: Expected payment amount in USD.
            endpoint: The API endpoint being accessed.

        Returns:
            PaymentRecord if valid, None if invalid.
        """
        try:
            payment_data = json.loads(payment_header)
        except (json.JSONDecodeError, TypeError):
            log.warning("x402: invalid payment header format")
            return None

        # Validate required fields
        required_fields = ["tx_hash", "payer", "amount", "token", "network"]
        for f in required_fields:
            if f not in payment_data:
                log.warning("x402: missing field '{f}' in payment", f=f)
                return None

        # Validate network and token
        if payment_data["network"] != self.network:
            log.warning(
                "x402: wrong network {got}, expected {exp}",
                got=payment_data["network"],
                exp=self.network,
            )
            return None

        if payment_data["token"] != self.token:
            log.warning(
                "x402: wrong token {got}, expected {exp}",
                got=payment_data["token"],
                exp=self.token,
            )
            return None

        # Validate amount
        paid_amount = float(payment_data["amount"])
        if paid_amount < expected_amount:
            log.warning(
                "x402: insufficient payment {paid} < {expected}",
                paid=paid_amount,
                expected=expected_amount,
            )
            return None

        # Check for replay (same tx_hash used before)
        tx_hash = payment_data["tx_hash"]
        if any(r.tx_hash == tx_hash for r in self._ledger):
            log.warning("x402: replay detected — tx_hash={tx}", tx=tx_hash[:16])
            return None

        # In production: verify tx on-chain via Coinbase CDP or Base RPC
        # For now, accept the payment if structure is valid
        payment_id = hashlib.sha256(
            f"{tx_hash}:{endpoint}:{time.time()}".encode()
        ).hexdigest()[:32]

        record = PaymentRecord(
            payment_id=payment_id,
            endpoint=endpoint,
            amount_usd=paid_amount,
            payer_address=payment_data["payer"],
            tx_hash=tx_hash,
            metadata={
                "network": self.network,
                "token": self.token,
                "receiver": self.receiver_address,
            },
        )

        self._ledger.append(record)

        log.info(
            "x402: payment verified — {amount} {token} from {payer} for {ep}",
            amount=paid_amount,
            token=self.token,
            payer=payment_data["payer"][:10] + "...",
            ep=endpoint,
        )

        return record

    def get_payment_requirements(
        self,
        endpoint: str,
        price_usd: float,
    ) -> dict[str, Any]:
        """Build the 402 response body with payment requirements.

        This is returned when an agent hits a paid endpoint without
        a valid X-PAYMENT header.

        Parameters:
            endpoint: The endpoint being accessed.
            price_usd: The price in USD.

        Returns:
            dict: x402 payment requirements for the agent.
        """
        return {
            "x402": {
                "version": "1",
                "network": self.network,
                "token": self.token,
                "receiver": self.receiver_address,
                "amount": str(price_usd),
                "description": f"Payment for {endpoint}",
                "endpoint": endpoint,
            }
        }

    @property
    def total_revenue(self) -> float:
        """Total revenue collected (USD)."""
        return sum(r.amount_usd for r in self._ledger)

    @property
    def payment_count(self) -> int:
        """Number of verified payments."""
        return len(self._ledger)

    def get_ledger(self, limit: int = 100) -> list[PaymentRecord]:
        """Return recent payment records.

        Parameters:
            limit: Maximum records to return.

        Returns:
            list[PaymentRecord]: Most recent payments first.
        """
        return sorted(
            self._ledger,
            key=lambda r: r.verified_at,
            reverse=True,
        )[:limit]


class X402Middleware:
    """FastAPI middleware for x402 payment gating.

    Checks incoming requests against the pricing table. If the endpoint
    requires payment and no valid X-PAYMENT header is present, returns
    HTTP 402 with payment requirements.

    Parameters:
        verifier: PaymentVerifier instance.
        pricing: List of PricingTier configurations.
        enabled: Whether x402 gating is active.
    """

    def __init__(
        self,
        verifier: PaymentVerifier,
        pricing: list[PricingTier] | None = None,
        enabled: bool = True,
    ) -> None:
        self.verifier = verifier
        self.enabled = enabled
        self._pricing: dict[str, PricingTier] = {}

        for tier in pricing or DEFAULT_PRICING:
            self._pricing[tier.endpoint] = tier

    def get_price(self, path: str) -> PricingTier | None:
        """Look up the price for an endpoint.

        Parameters:
            path: Request path.

        Returns:
            PricingTier or None if endpoint is free.
        """
        return self._pricing.get(path)

    def check_payment(
        self,
        path: str,
        payment_header: str | None,
    ) -> tuple[bool, dict[str, Any] | PaymentRecord | None]:
        """Check if a request has valid payment.

        Parameters:
            path: Request path.
            payment_header: Value of X-PAYMENT header (or None).

        Returns:
            tuple: (is_allowed, data) where data is either:
                - PaymentRecord if payment verified
                - dict with payment requirements if 402
                - None if endpoint is free
        """
        if not self.enabled:
            return True, None

        tier = self.get_price(path)
        if tier is None:
            return True, None  # Free endpoint

        if payment_header is None:
            requirements = self.verifier.get_payment_requirements(
                path, tier.price_usd
            )
            return False, requirements

        record = self.verifier.verify_payment(
            payment_header, tier.price_usd, path
        )
        if record is None:
            requirements = self.verifier.get_payment_requirements(
                path, tier.price_usd
            )
            return False, requirements

        return True, record

    def add_pricing(self, tier: PricingTier) -> None:
        """Add or update endpoint pricing.

        Parameters:
            tier: New pricing configuration.
        """
        self._pricing[tier.endpoint] = tier

    @property
    def priced_endpoints(self) -> list[PricingTier]:
        """Return all priced endpoints."""
        return list(self._pricing.values())
