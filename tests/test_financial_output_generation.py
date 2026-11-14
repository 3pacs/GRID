"""Source-bound reports fail closed before persistence or cache reuse."""
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intelligence import fundamental_divergence as fd
from intelligence import sector_health as sh
from store.financial_generation import get_financial_generation


@pytest.mark.parametrize("value", [None, "", "a" * 31, "z" * 32, 12])
def test_generation_requires_verified_digest(value):
    conn = MagicMock()
    conn.execute.return_value.scalar_one.return_value = value
    with pytest.raises(RuntimeError):
        get_financial_generation(conn)


@pytest.mark.parametrize("end", ["b" * 32, RuntimeError("offline")])
def test_divergence_discards_mixed_source_result(monkeypatch, end):
    monkeypatch.setattr(fd, "_load_universe", lambda: [SimpleNamespace(ticker="ABC", sector="Technology")])
    gen = MagicMock(side_effect=["a" * 32, end])
    monkeypatch.setattr(fd, "get_financial_generation", gen)
    monkeypatch.setattr(fd, "_load_batch_fundamentals", lambda *a: {"ABC": {"revenue_cagr": 0.1, "shareholder_yield": 0.05}})
    monkeypatch.setattr(fd, "_load_batch_price_cagrs", lambda *a: {"ABC": 0.02})
    monkeypatch.setattr(fd, "_build_fundamental_score", lambda *a: 80.0)
    monkeypatch.setattr(fd, "_narrative", lambda *a: "synthetic")
    with pytest.raises(fd.FinancialGenerationRetry):
        fd.compute_divergence(MagicMock(), date(2026, 11, 8))
    assert gen.call_count == 2


def test_divergence_generation_missing_does_not_load_financials(monkeypatch):
    monkeypatch.setattr(fd, "_load_universe", lambda: [SimpleNamespace(ticker="ABC", sector="Technology")])
    monkeypatch.setattr(fd, "get_financial_generation", MagicMock(side_effect=RuntimeError("missing")))
    load = MagicMock()
    monkeypatch.setattr(fd, "_load_batch_fundamentals", load)
    with pytest.raises(fd.FinancialGenerationRetry):
        fd.compute_divergence(MagicMock(), date(2026, 11, 8))
    load.assert_not_called()


@pytest.mark.parametrize("end", ["b" * 32, RuntimeError("offline")])
def test_sector_discards_mixed_source_result(monkeypatch, end):
    monkeypatch.setattr(sh, "get_financial_generation", MagicMock(side_effect=["a" * 32, end]))
    monkeypatch.setattr(sh, "_SCORERS", {key: lambda *args: 0.6 for key in sh.WEIGHTS})
    monkeypatch.setattr(sh, "_trend_from_snapshots", lambda *a: None)
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.compute_sector_health(MagicMock(), "Technology")


def test_divergence_api_uses_current_views(monkeypatch):
    from api.routers import divergence as router
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    monkeypatch.setattr(router, "_table_exists", lambda *a: True)
    conn.execute.return_value.fetchall.return_value = []
    assert router._read_divergence_list("aligned", 20, engine)["rows"] == []
    sql = str(conn.execute.call_args.args[0])
    assert sql.count("FROM fundamental_divergence_current") == 2
    assert "current_current" not in sql


def test_divergence_cache_does_not_return_old_source_rows(monkeypatch):
    from api.routers import divergence as router
    from utils import financial_cache
    engine = MagicMock()
    monkeypatch.setattr(router, "get_db_engine", lambda: engine)
    source = ["a" * 32]
    monkeypatch.setattr(financial_cache, "get_financial_generation", lambda conn: source[0])
    build = MagicMock(side_effect=[{"rows": ["old"], "provenance": {"source": "db"}}, {"rows": [], "provenance": {"source": "db"}}])
    monkeypatch.setattr(router, "_read_divergence_list", build)
    router._CACHE.clear()
    assert router.list_divergence("aligned", 20)["rows"] == ["old"]
    source[0] = "b" * 32
    assert router.list_divergence("aligned", 20)["rows"] == []
    assert build.call_count == 2
