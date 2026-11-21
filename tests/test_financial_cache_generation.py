"""Offline SEC-dependent TTL regressions: no DB/provider/network access."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from utils import financial_cache as fc
from utils.ttl_cache import TTLCache


@pytest.fixture
def source(monkeypatch):
    state = {"generation": "a" * 32, "failure": False}

    def current(_conn):
        if state["failure"]:
            raise RuntimeError("source identity read failed")
        return state["generation"]

    monkeypatch.setattr(fc, "get_financial_generation", current)
    return state


def test_stable_generation_reuses_value_and_checks_on_every_request(source):
    engine = MagicMock()
    cache = TTLCache()
    build = MagicMock(return_value={"revenue": 10})
    first = fc.get_cached_financial(cache, "annual", engine, build)
    assert fc.get_cached_financial(cache, "annual", engine, build) is first
    assert build.call_count == 1
    assert engine.connect.call_count == 4


@pytest.mark.parametrize("change", ["peer_head", "empty_head", "same_values_new_head", "ttm_ready"])
def test_generation_change_never_reuses_earlier_value(source, change):
    engine = MagicMock()
    cache = TTLCache()
    build = MagicMock(side_effect=[{"old": True}, {"current": change}])
    first = fc.get_cached_financial(cache, "annual", engine, build)
    source["generation"] = "b" * 32
    second = fc.get_cached_financial(cache, "annual", engine, build)
    assert second != first
    assert second == {"current": change}
    assert build.call_count == 2


def test_legacy_unbound_entry_is_not_admitted(source):
    cache = TTLCache()
    cache.set("annual", {"legacy": True})
    cache.set(f"{source['generation']}|annual", {"unbound": True})
    assert fc.get_cached_financial(cache, "annual", MagicMock(), lambda: {}) == {}


def test_same_generation_in_different_engines_cannot_share_result(source):
    cache = TTLCache()
    first_engine, second_engine = MagicMock(), MagicMock()
    assert fc.get_cached_financial(cache, "annual", first_engine, lambda: 1) == 1
    assert fc.get_cached_financial(cache, "annual", second_engine, lambda: 2) == 2
    assert fc.get_cached_financial(cache, "annual", first_engine, lambda: 3) == 3


@pytest.mark.parametrize("warm", [False, True])
def test_source_read_error_never_falls_back_or_builds(source, warm):
    cache = TTLCache()
    engine = MagicMock()
    if warm:
        fc.get_cached_financial(cache, "annual", engine, lambda: {"stale": True})
    source["failure"] = True
    build = MagicMock()
    with pytest.raises(fc.FinancialCacheUnavailable):
        fc.get_cached_financial(cache, "annual", engine, build)
    build.assert_not_called()


@pytest.mark.parametrize("invalid", [None, "", MagicMock()])
def test_missing_version_is_not_a_stable_default(monkeypatch, invalid):
    monkeypatch.setattr(fc, "get_financial_generation", lambda conn: invalid)
    with pytest.raises(fc.FinancialCacheUnavailable):
        fc.get_cached_financial(TTLCache(), "annual", MagicMock(), lambda: 42)


def test_rebuild_overlapping_change_is_discarded_and_retried(source):
    cache = TTLCache()
    built = []

    def build():
        value = source["generation"]
        built.append(value)
        source["generation"] = "b" * 32
        return value

    assert fc.get_cached_financial(cache, "annual", MagicMock(), build) == "b" * 32
    assert built == ["a" * 32, "b" * 32]
    assert cache.get(f"{'a' * 32}|annual") is None


def test_repeated_changes_fail_closed_and_do_not_cache(source):
    cache = TTLCache()
    build = MagicMock()

    def change():
        source["generation"] += "x"
        return {"inconsistent": True}

    build.side_effect = change
    with pytest.raises(fc.FinancialCacheUnavailable, match="changed"):
        fc.get_cached_financial(cache, "annual", MagicMock(), build)
    assert build.call_count == 2
    assert len(cache) == 0


def test_postbuild_version_read_failure_does_not_publish(source):
    cache = TTLCache()

    def build():
        source["failure"] = True
        return {"old": True}

    with pytest.raises(fc.FinancialCacheUnavailable):
        fc.get_cached_financial(cache, "annual", MagicMock(), build)
    assert len(cache) == 0


def test_cache_hit_is_rechecked_before_serving(source, monkeypatch):
    cache = TTLCache()
    engine = MagicMock()
    fc.get_cached_financial(cache, "annual", engine, lambda: {"old": True})
    generations = iter(["a" * 32, "b" * 32, "b" * 32, "b" * 32])
    monkeypatch.setattr(fc, "get_financial_generation", lambda conn: next(generations))
    assert fc.get_cached_financial(cache, "annual", engine, lambda: {"new": True}) == {"new": True}


def test_posthit_version_read_failure_does_not_serve_old(source, monkeypatch):
    cache = TTLCache()
    engine = MagicMock()
    fc.get_cached_financial(cache, "annual", engine, lambda: {"old": True})
    read = MagicMock(side_effect=["a" * 32, RuntimeError("unavailable")])
    monkeypatch.setattr(fc, "get_financial_generation", read)
    with pytest.raises(fc.FinancialCacheUnavailable):
        fc.get_cached_financial(cache, "annual", engine, lambda: {"unused": True})


def test_unavailable_results_need_not_be_cached(source):
    cache = TTLCache()
    out = fc.get_cached_financial(
        cache, "annual", MagicMock(), lambda: {"status": "unavailable"},
        cache_if=lambda value: value["status"] != "unavailable",
    )
    assert out["status"] == "unavailable"
    assert len(cache) == 0


@pytest.mark.parametrize("route", ["capital", "sector"])
def test_router_cache_invalidates_and_source_failure_returns_503(source, monkeypatch, route):
    if route == "capital":
        from api.routers import capital_flow as router
        builder = MagicMock(side_effect=[{"old": True}, {"new": True}])
        monkeypatch.setattr(router, "_build_capital_flow", builder)
        def call():
            return router.get_capital_flow("NVDA", 4, "annual", "token")
    else:
        from api.routers import sector_health as router
        builder = MagicMock(side_effect=[{"old": True}, {"new": True}])
        monkeypatch.setattr("intelligence.sector_health.compute_sector_health", builder)
        def call():
            return router.get_sector_health("Technology", "token")
    engine = MagicMock()
    monkeypatch.setattr(router, "get_db_engine", lambda: engine)
    router._CACHE.clear()
    try:
        assert call() == {"old": True}
        assert call() == {"old": True}
        source["generation"] = "b" * 32
        assert call() == {"new": True}
        source["failure"] = True
        with pytest.raises(HTTPException) as exc:
            call()
        assert exc.value.status_code == 503
        assert builder.call_count == 2
    finally:
        router._CACHE.clear()


@pytest.fixture
def percentiles(monkeypatch):
    from features import lab
    lab.clear_cache()
    amounts = {
        "AAPL": {"revenue": 100, "cogs": 50},
        "NVDA": {"revenue": 100, "cogs": 20},
    }
    monkeypatch.setattr(lab, "_load_latest_amounts", lambda engine, pt: amounts)
    monkeypatch.setattr(lab, "_ticker_to_sector_subsector", lambda: {
        "AAPL": ("Technology", "Hardware"), "NVDA": ("Technology", "Semiconductors"),
    })
    yield lab, amounts, MagicMock()
    lab.clear_cache()


def test_percentiles_remove_withdrawn_peer_before_ttl(source, percentiles):
    lab, amounts, engine = percentiles
    assert lab.compute_all_percentiles(engine)["gross_margin"]["Technology"]["AAPL"] == 0
    del amounts["NVDA"]
    source["generation"] = "b" * 32
    result = lab.compute_all_percentiles(engine)["gross_margin"]["Technology"]
    assert result == {"AAPL": 50.0}


def test_percentiles_recover_when_ttm_readiness_changes(source, percentiles):
    lab, amounts, engine = percentiles
    saved = dict(amounts)
    amounts.clear()
    assert lab.compute_all_percentiles(engine, "ttm") == {}
    amounts.update(saved)
    source["generation"] = "b" * 32
    assert lab.compute_all_percentiles(engine, "ttm")["gross_margin"]["Technology"]["NVDA"] == 100


def test_percentiles_source_failure_never_returns_warm_rank(source, percentiles):
    lab, _amounts, engine = percentiles
    assert lab.compute_all_percentiles(engine)
    source["failure"] = True
    assert lab.compute_all_percentiles(engine) == {}
    result = lab.get_percentile(engine, "NVDA", "gross_margin")
    assert result["percentile"] is None
    assert result["sector_median"] is None


def test_percentile_context_rejects_generation_change_between_rank_and_statistics(source, percentiles, monkeypatch):
    lab, _amounts, engine = percentiles
    real = lab.compute_all_percentiles

    def changing(engine, pt):
        out = real(engine, pt)
        source["generation"] = "b" * 32
        return out

    monkeypatch.setattr(lab, "compute_all_percentiles", changing)
    result = lab.get_percentile(engine, "NVDA", "gross_margin")
    assert result["percentile"] is None
    assert result["sector_median"] is None


def test_percentile_context_inner_version_failure_drops_all_statistics(source, percentiles, monkeypatch):
    lab, _amounts, engine = percentiles
    assert lab.compute_all_percentiles(engine)
    read = MagicMock(side_effect=["a" * 32, RuntimeError("transient read failure"), "a" * 32])
    monkeypatch.setattr(fc, "get_financial_generation", read)
    result = lab.get_percentile(engine, "NVDA", "gross_margin")
    assert result["percentile"] is None
    assert result["sector_median"] is None
    assert result["sector_top_decile"] is None


def test_capital_payload_withdrawal_drops_warm_values_without_waiting_for_ttl(source, monkeypatch):
    from datetime import date
    from api.routers import capital_flow as cf
    engine = MagicMock()
    rows = [{
        "fiscal_period": date(2025, 12, 31), "flow_type": "revenue", "direction": "in",
        "amount_usd": 100.0, "currency": "USD", "counterparty_id": None,
        "source_filing": "10-K", "confidence": "confirmed",
    }]
    monkeypatch.setattr(cf, "get_db_engine", lambda: engine)
    monkeypatch.setattr(cf, "_lookup_actor", lambda aid: {"id": aid, "label": aid, "sector": None})
    monkeypatch.setattr(cf, "_load_rows", lambda *args: list(rows))
    monkeypatch.setattr(cf, "_fetch_market_cap", lambda *args: None)
    monkeypatch.setattr(cf, "_load_deal_announcements", lambda *args: {})
    cf._CACHE.clear()
    try:
        first = cf.get_capital_flow("NVDA", 4, "annual", "token")
        assert first["summary"]["latest_revenue_usd"] == 100.0
        rows.clear()
        source["generation"] = "b" * 32
        current = cf.get_capital_flow("NVDA", 4, "annual", "token")
        assert current["summary"]["latest_revenue_usd"] is None
        assert current["provenance"]["source"] == "fallback"
        assert current["periods"][0]["inflows"] == []
    finally:
        cf._CACHE.clear()
