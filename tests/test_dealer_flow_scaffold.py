"""
GRID — Scaffold tests for physics.dealer_flow (task GEX-4 / #80).

Covers the Wave-2 skeleton only:

1. every module in the subpackage imports cleanly
2. the abstract ``VenueAdapter`` cannot be instantiated directly
3. ``DeribitAdapter`` instantiates with stub kwargs
4. ``OptionContract`` accepts a 30+ field sample dict
5. ``score_contract`` returns a float in ``[0.0, 1.0]``

Real behavior tests (frozen Deribit snapshot, golden GEX values, wall
detection, flip interpolation, confidence weighting) land alongside
the full implementation in GEX-8.
"""

from __future__ import annotations

import importlib

import pytest


# ── 1. Import smoke test ────────────────────────────────────────────


def test_all_modules_import_cleanly() -> None:
    """Every scaffold module must import without error."""
    modules = [
        "physics.dealer_flow",
        "physics.dealer_flow.schemas",
        "physics.dealer_flow.adapters",
        "physics.dealer_flow.adapters.base",
        "physics.dealer_flow.adapters.deribit",
        "physics.dealer_flow.pipeline",
        "physics.dealer_flow.exposures",
        "physics.dealer_flow.confidence",
    ]
    for mod_name in modules:
        mod = importlib.import_module(mod_name)
        assert mod is not None, f"failed to import {mod_name}"


# ── 2. Abstract base class ──────────────────────────────────────────


def test_venue_adapter_is_abstract() -> None:
    """Directly instantiating VenueAdapter must raise TypeError."""
    from physics.dealer_flow.adapters.base import VenueAdapter

    with pytest.raises(TypeError):
        VenueAdapter()  # type: ignore[abstract]


# ── 3. Deribit stub instantiation ───────────────────────────────────


def test_deribit_adapter_instantiates() -> None:
    """DeribitAdapter should construct without network I/O."""
    from physics.dealer_flow.adapters.deribit import DeribitAdapter

    adapter = DeribitAdapter(testnet=True, rate_limit_ms=100)
    assert adapter.venue == "deribit"
    assert adapter.testnet is True
    assert adapter.rate_limit_ms == 100

    # Stub methods must raise NotImplementedError, not crash on attr lookup.
    with pytest.raises(NotImplementedError):
        adapter.fetch_instruments("BTC")
    with pytest.raises(NotImplementedError):
        adapter.fetch_ticker("BTC-27JUN25-80000-C")
    with pytest.raises(NotImplementedError):
        adapter.normalize([], [], 80000.0)


# ── 4. OptionContract accepts 30-field sample ───────────────────────


def test_option_contract_accepts_full_sample() -> None:
    """OptionContract should accept every spec §5.1 field at once."""
    from physics.dealer_flow.schemas import OptionContract

    sample: dict = {
        # identity
        "venue": "deribit",
        "symbol": "BTC-27JUN25-80000-C",
        "underlying": "BTC",
        # economics
        "expiry_ts_utc": 1_750_000_000_000,
        "strike": 80_000.0,
        "option_type": "call",
        "contract_size": 1.0,
        "settlement_currency": "BTC",
        "quote_currency": "USD",
        # quote state
        "mark_price": 0.0425,
        "bid": 0.0420,
        "ask": 0.0430,
        "mid": 0.0425,
        # OI / volume
        "oi_contracts": 1234.0,
        "oi_underlying_units": 1234.0,
        "volume_24h": 567.0,
        # underlying + vol
        "underlying_price": 79_500.0,
        "iv_decimal": 0.65,
        # exchange Greeks
        "delta": 0.52,
        "gamma": 0.000012,
        "vanna": 0.0034,
        "charm": -0.0001,
        "vomma": 0.0789,
        "color": -0.0002,
        "zomma": 0.0012,
        "speed": -0.00001,
        # timestamps
        "source_ts_utc": 1_712_000_000_000,
        "ingest_ts_utc": 1_712_000_001_000,
        # validation + derived
        "is_expired": False,
        "data_quality_flags": [],
        "time_to_expiry_years": 0.2,
        "dte_days": 73.0,
        "distance_from_spot_pct": 0.63,
        "spread_bps": 23.5,
        "quote_age_ms": 1000,
        "greek_source": "exchange",
        "row_confidence": 0.87,
    }
    # Sanity: we really do have 30+ fields in the sample.
    assert len(sample) >= 30

    contract = OptionContract(**sample)
    assert contract.venue == "deribit"
    assert contract.strike == 80_000.0
    assert contract.option_type == "call"
    assert contract.iv_decimal == 0.65
    assert contract.greek_source == "exchange"


# ── 5. Confidence score bounds ──────────────────────────────────────


def test_score_contract_returns_float_in_unit_interval() -> None:
    """score_contract must return a float in [0.0, 1.0]."""
    from physics.dealer_flow.confidence import score_contract

    # A minimally-valid object; scaffold doesn't introspect fields.
    dummy = object()
    score = score_contract(dummy)

    assert isinstance(score, float)
    assert 0.0 <= score <= 1.0
