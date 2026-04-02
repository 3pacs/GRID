"""
GRID Payments — AP2 + x402 Agent Micropayments.

Implements the Agent Payments Protocol with x402 extension for
agent-native micropayments using stablecoins on Base L2.

Architecture:
  - x402 middleware: FastAPI middleware that checks for 402 Payment Required
  - PaymentVerifier: Validates x402 payment headers (USDC on Base)
  - PricingTier: Configurable per-endpoint pricing
  - PaymentLedger: Tracks payments and usage

This enables GRID's API to accept pay-per-request micropayments
from AI agents without requiring accounts or subscriptions.
"""

from payments.x402 import X402Middleware, PaymentVerifier, PricingTier

__all__ = ["X402Middleware", "PaymentVerifier", "PricingTier"]
