"""
Tests for the x402 agent micropayment module.

Tests PaymentVerifier, X402Middleware, and PricingTier.
"""

from __future__ import annotations

import json

import pytest

from payments.x402 import (
    DEFAULT_PRICING,
    PaymentRecord,
    PaymentVerifier,
    PricingTier,
    X402Middleware,
)


# ---------------------------------------------------------------------------
# PricingTier
# ---------------------------------------------------------------------------


class TestPricingTier:
    def test_frozen_dataclass(self) -> None:
        tier = PricingTier("/api/v1/test", 0.01, "Test endpoint")
        assert tier.endpoint == "/api/v1/test"
        assert tier.price_usd == 0.01

        with pytest.raises(AttributeError):
            tier.price_usd = 0.02  # type: ignore[misc]

    def test_default_pricing_exists(self) -> None:
        assert len(DEFAULT_PRICING) > 0
        assert any(t.endpoint == "/api/v1/forecasts/generate" for t in DEFAULT_PRICING)


# ---------------------------------------------------------------------------
# PaymentVerifier
# ---------------------------------------------------------------------------


class TestPaymentVerifier:
    @pytest.fixture
    def verifier(self) -> PaymentVerifier:
        return PaymentVerifier(
            receiver_address="0xGRID_RECEIVER",
            network="base",
            token="USDC",
        )

    def test_valid_payment(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xabc123",
            "payer": "0xPAYER",
            "amount": "0.01",
            "token": "USDC",
            "network": "base",
        })

        record = verifier.verify_payment(header, 0.01, "/api/v1/test")

        assert record is not None
        assert record.amount_usd == 0.01
        assert record.payer_address == "0xPAYER"
        assert record.tx_hash == "0xabc123"
        assert record.endpoint == "/api/v1/test"

    def test_invalid_json(self, verifier: PaymentVerifier) -> None:
        record = verifier.verify_payment("not-json", 0.01, "/test")
        assert record is None

    def test_missing_fields(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({"tx_hash": "0xabc"})
        record = verifier.verify_payment(header, 0.01, "/test")
        assert record is None

    def test_wrong_network(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xabc",
            "payer": "0xPAYER",
            "amount": "0.01",
            "token": "USDC",
            "network": "ethereum",
        })
        record = verifier.verify_payment(header, 0.01, "/test")
        assert record is None

    def test_wrong_token(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xabc",
            "payer": "0xPAYER",
            "amount": "0.01",
            "token": "DAI",
            "network": "base",
        })
        record = verifier.verify_payment(header, 0.01, "/test")
        assert record is None

    def test_insufficient_amount(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xabc",
            "payer": "0xPAYER",
            "amount": "0.005",
            "token": "USDC",
            "network": "base",
        })
        record = verifier.verify_payment(header, 0.01, "/test")
        assert record is None

    def test_replay_detection(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xSAME_TX",
            "payer": "0xPAYER",
            "amount": "0.01",
            "token": "USDC",
            "network": "base",
        })

        # First payment succeeds
        r1 = verifier.verify_payment(header, 0.01, "/test")
        assert r1 is not None

        # Replay is rejected
        r2 = verifier.verify_payment(header, 0.01, "/test")
        assert r2 is None

    def test_overpayment_accepted(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xoverpay",
            "payer": "0xPAYER",
            "amount": "1.00",
            "token": "USDC",
            "network": "base",
        })
        record = verifier.verify_payment(header, 0.01, "/test")
        assert record is not None
        assert record.amount_usd == 1.00

    def test_payment_requirements(self, verifier: PaymentVerifier) -> None:
        reqs = verifier.get_payment_requirements("/api/v1/test", 0.01)

        assert "x402" in reqs
        assert reqs["x402"]["network"] == "base"
        assert reqs["x402"]["token"] == "USDC"
        assert reqs["x402"]["receiver"] == "0xGRID_RECEIVER"
        assert reqs["x402"]["amount"] == "0.01"

    def test_total_revenue(self, verifier: PaymentVerifier) -> None:
        assert verifier.total_revenue == 0.0

        header1 = json.dumps({
            "tx_hash": "0xtx1",
            "payer": "0xA",
            "amount": "0.01",
            "token": "USDC",
            "network": "base",
        })
        header2 = json.dumps({
            "tx_hash": "0xtx2",
            "payer": "0xB",
            "amount": "0.05",
            "token": "USDC",
            "network": "base",
        })

        verifier.verify_payment(header1, 0.01, "/test")
        verifier.verify_payment(header2, 0.05, "/test")

        assert verifier.total_revenue == pytest.approx(0.06)
        assert verifier.payment_count == 2

    def test_ledger(self, verifier: PaymentVerifier) -> None:
        header = json.dumps({
            "tx_hash": "0xledger",
            "payer": "0xA",
            "amount": "0.01",
            "token": "USDC",
            "network": "base",
        })
        verifier.verify_payment(header, 0.01, "/test")

        ledger = verifier.get_ledger()
        assert len(ledger) == 1
        assert ledger[0].tx_hash == "0xledger"


# ---------------------------------------------------------------------------
# X402Middleware
# ---------------------------------------------------------------------------


class TestX402Middleware:
    @pytest.fixture
    def middleware(self) -> X402Middleware:
        verifier = PaymentVerifier(
            receiver_address="0xGRID",
            network="base",
            token="USDC",
        )
        pricing = [
            PricingTier("/api/v1/paid", 0.01, "Paid endpoint"),
        ]
        return X402Middleware(verifier=verifier, pricing=pricing)

    def test_free_endpoint(self, middleware: X402Middleware) -> None:
        allowed, data = middleware.check_payment("/api/v1/free", None)
        assert allowed is True
        assert data is None

    def test_paid_endpoint_no_payment(self, middleware: X402Middleware) -> None:
        allowed, data = middleware.check_payment("/api/v1/paid", None)
        assert allowed is False
        assert data is not None
        assert "x402" in data

    def test_paid_endpoint_valid_payment(self, middleware: X402Middleware) -> None:
        header = json.dumps({
            "tx_hash": "0xvalid",
            "payer": "0xPAYER",
            "amount": "0.01",
            "token": "USDC",
            "network": "base",
        })
        allowed, data = middleware.check_payment("/api/v1/paid", header)
        assert allowed is True
        assert isinstance(data, PaymentRecord)

    def test_paid_endpoint_invalid_payment(self, middleware: X402Middleware) -> None:
        allowed, data = middleware.check_payment("/api/v1/paid", "bad-json")
        assert allowed is False

    def test_disabled_middleware(self) -> None:
        verifier = PaymentVerifier("0xGRID")
        middleware = X402Middleware(
            verifier=verifier,
            pricing=[PricingTier("/api/v1/paid", 0.01, "test")],
            enabled=False,
        )

        allowed, data = middleware.check_payment("/api/v1/paid", None)
        assert allowed is True

    def test_get_price(self, middleware: X402Middleware) -> None:
        tier = middleware.get_price("/api/v1/paid")
        assert tier is not None
        assert tier.price_usd == 0.01

        tier = middleware.get_price("/api/v1/free")
        assert tier is None

    def test_add_pricing(self, middleware: X402Middleware) -> None:
        middleware.add_pricing(PricingTier("/api/v1/new", 0.05, "New endpoint"))
        assert middleware.get_price("/api/v1/new") is not None

    def test_priced_endpoints(self, middleware: X402Middleware) -> None:
        endpoints = middleware.priced_endpoints
        assert len(endpoints) == 1
        assert endpoints[0].endpoint == "/api/v1/paid"
