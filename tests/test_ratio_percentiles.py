"""Tests for percentile primitives in features.lab.

Covers: ranking math, missing-ratio handling, polarity (higher-is-better
vs lower-is-better), cache idempotency, and the end-to-end wiring in the
capital_flow endpoint (`_percentiles` dict appears in the response).

Percentile primitives were relocated from intelligence/ratio_percentiles.py
to features/lab.py on 2026-04-11 (SYNTH-12 / Wave 3 dedupe). The alias
``rp = features.lab`` preserves the original test body unchanged.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from features import lab as rp


# ─── Shared fixtures ───────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clear_percentile_cache():
    """Isolate every test from the 1h snapshot cache."""
    rp.clear_cache()
    yield
    rp.clear_cache()


def _fake_amounts(**kwargs) -> dict[str, float]:
    """Build a minimal amounts dict for a ticker's latest period."""
    base = {"revenue": 100.0, "cogs": 50.0, "opex": 20.0, "capex": 10.0}
    base.update(kwargs)
    return base


def _install_stub_engine(monkeypatch, amounts_by_ticker: dict[str, dict[str, float]]) -> MagicMock:
    """Replace the DB-side loader with a canned dict so tests don't need
    Postgres. Returns a no-op engine mock in case the function uses it.
    """
    monkeypatch.setattr(
        rp,
        "_load_latest_amounts",
        lambda engine, period_type="annual": amounts_by_ticker,
    )
    return MagicMock()


def _install_sector_map(monkeypatch, mapping: dict[str, tuple[str, str]]) -> None:
    monkeypatch.setattr(rp, "_ticker_to_sector_subsector", lambda: mapping)


# ─── Unit tests ─────────────────────────────────────────────────────────


def test_compute_sector_percentiles_values_are_0_to_100(monkeypatch):
    """Every percentile emitted must land in [0, 100]."""
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
        "NVDA": ("Technology", "Semiconductors"),
    })
    engine = _install_stub_engine(monkeypatch, {
        "AAPL": _fake_amounts(revenue=100, cogs=60),  # gm = 0.4
        "MSFT": _fake_amounts(revenue=100, cogs=30),  # gm = 0.7
        "NVDA": _fake_amounts(revenue=100, cogs=25),  # gm = 0.75
    })

    out = rp.compute_sector_percentiles(engine, "gross_margin", "annual")

    assert "Technology" in out
    tech = out["Technology"]
    assert set(tech.keys()) == {"AAPL", "MSFT", "NVDA"}
    for pct in tech.values():
        assert 0.0 <= pct <= 100.0


def test_ranking_within_sector_best_ticker_is_100(monkeypatch):
    """Best gm → P100, worst → P0, middle somewhere between."""
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
        "NVDA": ("Technology", "Semiconductors"),
    })
    engine = _install_stub_engine(monkeypatch, {
        "AAPL": _fake_amounts(revenue=100, cogs=60),  # gm = 0.40  (worst)
        "MSFT": _fake_amounts(revenue=100, cogs=30),  # gm = 0.70  (mid)
        "NVDA": _fake_amounts(revenue=100, cogs=25),  # gm = 0.75  (best)
    })

    tech = rp.compute_sector_percentiles(engine, "gross_margin")["Technology"]

    assert tech["NVDA"] == 100.0
    assert tech["AAPL"] == 0.0
    assert 0.0 < tech["MSFT"] < 100.0


def test_lower_is_better_ratio_inverts_polarity(monkeypatch):
    """capex_intensity is lower-is-better: the smallest ratio gets P100."""
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
        "NVDA": ("Technology", "Semiconductors"),
    })
    engine = _install_stub_engine(monkeypatch, {
        "AAPL": _fake_amounts(revenue=100, capex=5),   # 0.05
        "MSFT": _fake_amounts(revenue=100, capex=10),  # 0.10
        "NVDA": _fake_amounts(revenue=100, capex=30),  # 0.30 (worst)
    })

    tech = rp.compute_sector_percentiles(engine, "capex_intensity")["Technology"]

    assert tech["AAPL"] == 100.0   # lowest capex intensity wins
    assert tech["NVDA"] == 0.0


def test_missing_ratios_excluded_from_ranking(monkeypatch):
    """Tickers whose latest period has None for this ratio drop out of
    the sector bucket and do not dilute the other tickers' percentile."""
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
        "BROKE": ("Technology", "Software"),
    })
    engine = _install_stub_engine(monkeypatch, {
        "AAPL": _fake_amounts(revenue=100, cogs=60),
        "MSFT": _fake_amounts(revenue=100, cogs=30),
        # BROKE has no revenue — gross_margin will be None.
        "BROKE": {"cogs": 20.0},
    })

    tech = rp.compute_sector_percentiles(engine, "gross_margin")["Technology"]
    assert "BROKE" not in tech
    assert set(tech.keys()) == {"AAPL", "MSFT"}


def test_cache_idempotency(monkeypatch):
    """compute_all_percentiles should reuse cached snapshots — the DB
    loader must only be called once per (period_type) cache window."""
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
    })

    call_count = {"n": 0}

    def fake_loader(engine, period_type="annual"):
        call_count["n"] += 1
        return {
            "AAPL": _fake_amounts(revenue=100, cogs=60),
            "MSFT": _fake_amounts(revenue=100, cogs=30),
        }

    monkeypatch.setattr(rp, "_load_latest_amounts", fake_loader)

    engine = MagicMock()
    first = rp.compute_all_percentiles(engine, "annual")
    second = rp.compute_all_percentiles(engine, "annual")

    assert first is second  # same object — served straight from cache
    assert call_count["n"] == 1, "loader should be hit exactly once"

    # After clear, the loader fires again.
    rp.clear_cache()
    rp.compute_all_percentiles(engine, "annual")
    assert call_count["n"] == 2


def test_ticker_without_sector_is_skipped(monkeypatch):
    """Tickers not in sector_map are silently dropped."""
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
    })
    engine = _install_stub_engine(monkeypatch, {
        "AAPL": _fake_amounts(revenue=100, cogs=60),
        "UNKNOWN": _fake_amounts(revenue=100, cogs=30),
    })
    out = rp.compute_sector_percentiles(engine, "gross_margin")
    assert "Technology" in out
    # UNKNOWN has no sector so it's excluded from every bucket.
    for sector_bucket in out.values():
        assert "UNKNOWN" not in sector_bucket


def test_unknown_ratio_name_returns_empty(monkeypatch):
    engine = MagicMock()
    out = rp.compute_sector_percentiles(engine, "totally_made_up_ratio")
    assert out == {}


def test_get_percentile_returns_sector_context(monkeypatch):
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
        "NVDA": ("Technology", "Semiconductors"),
    })
    engine = _install_stub_engine(monkeypatch, {
        "AAPL": _fake_amounts(revenue=100, cogs=60),  # gm 0.40
        "MSFT": _fake_amounts(revenue=100, cogs=30),  # gm 0.70
        "NVDA": _fake_amounts(revenue=100, cogs=25),  # gm 0.75
    })

    ctx = rp.get_percentile(engine, "msft", "gross_margin")
    assert ctx["sector"] == "Technology"
    assert ctx["subsector"] == "Software"
    assert ctx["percentile"] is not None
    assert ctx["sector_median"] is not None
    # Median of [0.40, 0.70, 0.75] = 0.70
    assert abs(ctx["sector_median"] - 0.70) < 1e-9


# ─── Integration: capital_flow endpoint wiring ─────────────────────────


def test_capital_flow_endpoint_exposes_percentiles(monkeypatch):
    """The ``_percentiles`` sub-dict must show up inside each period's
    ratios block when the endpoint runs end-to-end against stub data."""
    from api.routers import capital_flow as cf

    # Clear module caches so we're not reading stale results.
    cf._CACHE.clear()
    rp.clear_cache()

    # Stub _load_rows to return a single annual period for AAPL.
    def fake_load_rows(engine, actor_id, period_type, n):
        return [
            {
                "fiscal_period": __import__("datetime").date(2025, 12, 31),
                "flow_type": "revenue", "direction": "in",
                "amount_usd": 100.0, "currency": "USD",
                "counterparty_id": None, "source_filing": "10-K",
                "confidence": "confirmed",
            },
            {
                "fiscal_period": __import__("datetime").date(2025, 12, 31),
                "flow_type": "cogs", "direction": "out",
                "amount_usd": 60.0, "currency": "USD",
                "counterparty_id": None, "source_filing": "10-K",
                "confidence": "confirmed",
            },
        ]

    monkeypatch.setattr(cf, "_load_rows", fake_load_rows)
    monkeypatch.setattr(cf, "_fetch_market_cap", lambda *a, **k: None)
    monkeypatch.setattr(cf, "_lookup_actor", lambda actor_id: {
        "id": "AAPL", "label": "Apple", "type": "ticker",
        "sector": "Technology", "subsector": "Hardware",
    })
    monkeypatch.setattr(cf, "get_db_engine", lambda: MagicMock())

    # Stub percentile backend to avoid DB.
    _install_sector_map(monkeypatch, {
        "AAPL": ("Technology", "Hardware"),
        "MSFT": ("Technology", "Software"),
    })
    monkeypatch.setattr(rp, "_load_latest_amounts", lambda engine, period_type="annual": {
        "AAPL": _fake_amounts(revenue=100, cogs=60),
        "MSFT": _fake_amounts(revenue=100, cogs=30),
    })

    import asyncio
    result = asyncio.run(cf.get_capital_flow("aapl", 1, "annual", _token="test"))

    assert result["periods"], "endpoint returned no periods"
    ratios = result["periods"][0]["ratios"]
    assert "_percentiles" in ratios
    pct = ratios["_percentiles"]
    assert "gross_margin" in pct
    # AAPL has gm 0.40, MSFT has gm 0.70 — AAPL is the worst, P0.
    assert pct["gross_margin"] == 0.0
