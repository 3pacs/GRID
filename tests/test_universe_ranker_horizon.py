"""``rank_universe`` threads ``horizon_days`` to every ``should_i_trade`` call.

The Sunday long-horizon sweep (intelligence/scheduler.py) ranks the
market-edge universe at 90 d; the weekday sweep keeps the legacy 7 d. The
horizon must reach the decision stack for each ticker, be recorded on the
report, and be persisted with the ranking row so the realized-alpha truth
gate can bucket by horizon.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from intelligence import universe_ranker as ur


def _response(ticker: str) -> MagicMock:
    prov = MagicMock()
    prov.aggregate_conviction = 1.1
    stress = MagicMock()
    stress.robustness_score = 0.9
    stress.robustness_label = "robust"
    resp = MagicMock()
    resp.ticker = ticker
    resp.unified_verdict = "high"
    resp.provenance_report = prov
    resp.stress_report = stress
    resp.trade_ticket = MagicMock()
    return resp


@pytest.fixture
def gateway_calls(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    def fake_should_i_trade(engine, ticker, **kwargs):
        calls.append({"ticker": ticker, **kwargs})
        return _response(ticker)

    mod = types.ModuleType("intelligence.decision_gateway")
    mod.should_i_trade = fake_should_i_trade  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "intelligence.decision_gateway", mod)
    monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: "tech")
    return calls


def test_default_horizon_is_seven_days() -> None:
    assert ur.DEFAULT_HORIZON_DAYS == 7
    assert ur.UniverseRankingReport(
        universe_name="custom", tickers_attempted=0, tickers_succeeded=0,
        top_k=[], all_rankings=[], sector_distributions=[],
        concentration_alerts=[], regime_signature="neutral", narrative="",
    ).horizon_days == 7


def test_run_one_ticker_passes_horizon(gateway_calls) -> None:
    ur._run_one_ticker(MagicMock(), "NVDA", account_size_usd=100_000.0, horizon_days=90)
    assert gateway_calls[0]["ticker"] == "NVDA"
    assert gateway_calls[0]["horizon_days"] == 90


@pytest.mark.parametrize("parallel", [False, True])
def test_rank_universe_threads_horizon_to_every_ticker(gateway_calls, parallel) -> None:
    report = ur.rank_universe(
        MagicMock(), ["A", "B", "C"], account_size_usd=100_000.0,
        top_k=5, parallel=parallel, horizon_days=90,
    )
    assert report.horizon_days == 90
    assert report.to_dict()["horizon_days"] == 90
    assert sorted(c["ticker"] for c in gateway_calls) == ["A", "B", "C"]
    assert {c["horizon_days"] for c in gateway_calls} == {90}


def test_rank_universe_default_horizon_is_legacy_seven(gateway_calls) -> None:
    report = ur.rank_universe(MagicMock(), ["A"], account_size_usd=100_000.0, top_k=5)
    assert report.horizon_days == 7
    assert gateway_calls[0]["horizon_days"] == 7


def test_persist_ranking_writes_horizon(gateway_calls) -> None:
    report = ur.rank_universe(
        MagicMock(), ["A", "B"], account_size_usd=100_000.0, top_k=5, horizon_days=180,
    )
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.begin.return_value = conn

    ur.persist_ranking(engine, report)

    inserts = [
        (str(call.args[0]), call.args[1] if len(call.args) > 1 else call.kwargs)
        for call in conn.execute.call_args_list
        if "INSERT INTO universe_ranking_history" in str(call.args[0])
    ]
    assert len(inserts) == 1, "expected exactly one ranking insert"
    sql, params = inserts[0]
    assert "horizon_days" in sql and ":horizon_days" in sql
    assert params["horizon_days"] == 180


def test_ensure_ranking_table_adds_horizon_column() -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.begin.return_value = conn

    ur.ensure_ranking_table(engine)

    executed = " ".join(str(c.args[0]) for c in conn.execute.call_args_list)
    assert "ADD COLUMN IF NOT EXISTS horizon_days INTEGER NOT NULL DEFAULT 7" in executed
