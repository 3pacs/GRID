"""Offline query-routing tests for the narrow provisional-journal read guard.

PostgreSQL marker semantics/preservation are tested separately in the disposable
PG suite. These mocks verify every scoped consumer includes the real predicate,
keeps genuine no-result behavior, and never retries against unfiltered history.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from store.journal_qualification import NON_PROVISIONAL_JOURNAL_PREDICATE as PREDICATE


NOW = datetime(2026, 11, 8, 12, tzinfo=timezone.utc)


def _engine(journal_rows=(), *, failure=False, production=None):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    queries = []

    def execute(statement, *_args, **_kwargs):
        sql = str(statement)
        queries.append(sql)
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        if "FROM model_registry" in sql:
            result.fetchone.return_value = production
        if "FROM decision_journal" in sql:
            if failure:
                raise RuntimeError("synthetic journal read failure")
            result.fetchone.return_value = journal_rows[0] if journal_rows else None
            result.fetchall.return_value = list(journal_rows)
        return result

    conn.execute.side_effect = execute
    return engine, queries


def _assert_guarded(engine, queries, *, count=1, limit=True):
    reads = [q for q in queries if "FROM decision_journal" in q]
    assert len(reads) == count
    for sql in reads:
        assert PREDICATE in sql
        assert sql.index(PREDICATE) < sql.index("ORDER BY")
        if limit:
            assert sql.index(PREDICATE) < sql.index("LIMIT")
        assert "WHERE" in sql[:sql.index(PREDICATE)]
    assert all(q.lstrip().upper().startswith("SELECT") for q in queries)
    engine.begin.assert_not_called()


def test_predicate_is_static_parameter_free_and_narrow():
    query = text("SELECT * FROM decision_journal WHERE " + PREDICATE)
    assert query.compile().params == {}
    assert "source_contract_id" not in PREDICATE
    assert "sec_generation" not in PREDICATE
    assert "LIKE" not in PREDICATE  # underscores must never act as wildcards
    assert "'intelligence.fundamental_divergence'" in PREDICATE
    assert "'fundamental_divergence:'" in PREDICATE


@pytest.mark.parametrize("consumer", ["feedback", "flow", "trace", "thesis", "sentiment"])
@pytest.mark.parametrize("mode", ["empty", "failure", "genuine"])
def test_internal_current_readers_guard_before_limit_and_keep_no_data(consumer, mode):
    from analysis import thesis_scorer
    from intelligence import sentiment_scorer
    from oracle.engine import OracleEngine
    from oracle.trace_evolver import TraceAnalyzer

    rows = []
    if mode == "genuine":
        rows = ([('HOLD', 'HELPED')] * 5 if consumer == "feedback"
                else [('GROWTH', 0.8, 'S=0.1')])
    engine, queries = _engine(rows, failure=mode == "failure")
    if consumer == "feedback":
        reader = object.__new__(OracleEngine)
        reader.engine = engine
        value = reader._get_journal_feedback("NVDA")
        assert value == ({"confidence_multiplier": 1.2} if mode == "genuine" else {})
    elif consumer == "flow":
        reader = object.__new__(OracleEngine)
        reader.engine = engine
        value = reader._get_flow_context("NVDA")
        assert value.get("regime") == ("GROWTH" if mode == "genuine" else None)
        if mode != "genuine":
            assert value == {}
    elif consumer == "trace":
        reader = object.__new__(TraceAnalyzer)
        reader.engine = engine
        assert reader._get_current_regime() == ("GROWTH" if mode == "genuine" else None)
    elif consumer == "thesis":
        value = thesis_scorer._get_regime_context(engine)
        assert value["regime"] == ("GROWTH" if mode == "genuine" else "UNKNOWN")
        if mode != "genuine":
            assert value["adjustments"] == {}
    else:
        value = sentiment_scorer._score_regime(engine)
        if mode == "genuine":
            assert "GROWTH" in value.detail
            assert value.raw_value == 0.8
        else:
            assert value.detail == "No regime data available"
            assert value.weight == 0
    _assert_guarded(engine, queries)


@pytest.mark.parametrize("has_production", [False, True])
@pytest.mark.parametrize("has_row", [False, True])
def test_regime_current_guards_production_and_fallback(monkeypatch, has_production, has_row):
    from api.routers import regime

    rows = [("GROWTH", 0.8, 0.1, {}, "HOLD", "HOLD", NOW)] if has_row else []
    engine, queries = _engine(rows, production=(7, "fixture", "1") if has_production else None)
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    value = regime.get_current(_token="fixture")
    assert value.state == ("GROWTH" if has_row else "UNCALIBRATED")
    if not has_row:
        assert value.model_version == "none"
        assert value.as_of_date == ""
        assert "No data" in value.baseline_comparison
    _assert_guarded(engine, queries, count=2 if has_production and not has_row else 1)


def test_regime_production_miss_uses_only_guarded_fallback(monkeypatch):
    from api.routers import regime

    engine, queries = _engine(production=(7, "fixture", "1"))
    conn = engine.connect.return_value.__enter__.return_value
    execute = conn.execute.side_effect

    def fallback(statement, *args, **kwargs):
        result = execute(statement, *args, **kwargs)
        sql = str(statement)
        if "FROM decision_journal" in sql and ":mid" not in sql:
            result.fetchone.return_value = ("GROWTH", 0.8, 0.1, {}, "HOLD", "HOLD", NOW)
        return result

    conn.execute.side_effect = fallback
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    assert regime.get_current(_token="fixture").state == "GROWTH"
    _assert_guarded(engine, queries, count=2)


def test_regime_query_failure_never_falls_back_to_raw_history(monkeypatch):
    from api.routers import regime

    engine, queries = _engine(failure=True, production=(7, "fixture", "1"))
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    with pytest.raises(RuntimeError, match="synthetic journal"):
        regime.get_current(_token="fixture")
    _assert_guarded(engine, queries)


@pytest.mark.parametrize("has_row", [False, True])
def test_current_weights_guard_and_empty_measurements(monkeypatch, has_row):
    from api.routers import regime

    path = MagicMock()
    path.exists.return_value = False
    monkeypatch.setitem(sys.modules, "scripts.auto_regime", SimpleNamespace(
        DEFAULT_FEATURE_WEIGHTS={}, FEATURE_WEIGHTS={}, WEIGHTS_OVERRIDE_PATH=path))
    engine, queries = _engine([("GROWTH", 0.8, "S=0.125")] if has_row else [])
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    value = regime.get_weights(_token="fixture")
    assert value["regime"] == ("GROWTH" if has_row else None)
    assert value["confidence"] == (0.8 if has_row else None)
    assert value["stress_index"] == (0.125 if has_row else None)
    _assert_guarded(engine, queries)


@pytest.mark.parametrize("mode", ["empty", "failure", "genuine"])
def test_dad_current_regime_guard_and_missing_context(mode):
    from api.routers import dad

    rows = [("GROWTH", 0.8, "HOLD", "HIGH", NOW)] if mode == "genuine" else []
    engine, queries = _engine(rows, failure=mode == "failure")
    value = dad._latest_signal_context(engine, "NVDA")
    if mode == "genuine":
        assert value["regime"]["state"] == "GROWTH"
    else:
        assert value["regime"] is None
    _assert_guarded(engine, queries)


@pytest.mark.parametrize("mode", ["empty", "failure", "genuine"])
def test_watchlist_current_regime_does_not_invent_missing_context(monkeypatch, mode):
    from api.routers import watchlist_overview as overview

    rows = [("GROWTH", 0.8, "HOLD")] if mode == "genuine" else []
    engine, queries = _engine(rows, failure=mode == "failure")
    monkeypatch.setattr(overview, "get_db_engine", lambda: engine)
    monkeypatch.setattr(overview, "_fetch_live_price", lambda *_: None)
    unavailable = SimpleNamespace(is_available=False)
    monkeypatch.setitem(sys.modules, "llm.router", SimpleNamespace(
        Tier=SimpleNamespace(LOCAL="fixture"), get_llm=lambda *_: unavailable))
    monkeypatch.setitem(sys.modules, "ollama.client", SimpleNamespace(get_client=lambda: unavailable))
    monkeypatch.setitem(sys.modules, "analysis.sector_map", SimpleNamespace(SECTOR_MAP={}))
    value = overview.get_ticker_overview("FIXTURE", _token="fixture")
    assert ("macro regime is currently GROWTH" in value["overview"]) == (mode == "genuine")
    if mode != "genuine":
        assert "macro regime" not in value["overview"].lower()
    _assert_guarded(engine, queries)


def test_historical_regime_apis_keep_provisional_evidence(monkeypatch):
    from api.routers import regime

    rows = [(date(2026, 11, 7), "GROWTH", 0.8),
            (date(2026, 11, 8), "PROVISIONAL_FUNDAMENTAL_DIVERGENCE", 0.9)]
    engine, queries = _engine(rows)
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    history = regime.get_history(days=90, _token="fixture")
    transitions = regime.get_transitions(_token="fixture")
    assert history.history[-1].state == "PROVISIONAL_FUNDAMENTAL_DIVERGENCE"
    assert transitions.transitions[-1].to_state == "PROVISIONAL_FUNDAMENTAL_DIVERGENCE"
    assert len(queries) == 2
    assert all(PREDICATE not in sql for sql in queries)
    engine.begin.assert_not_called()


@pytest.mark.parametrize("has_row", [False, True])
def test_all_active_filters_before_distinct_on_and_preserves_empty(monkeypatch, has_row):
    from api.routers import regime

    rows = [("GROWTH", 0.8, 0.1, {}, "HOLD", NOW)] if has_row else []
    engine, queries = _engine(rows)
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    monkeypatch.setitem(sys.modules, "discovery.clustering", SimpleNamespace(
        ClusterDiscovery=lambda *_: SimpleNamespace(identify_transition_leaders=lambda: {})))
    value = regime.get_all_active(_token="fixture")
    assert len(value["macro"]) == int(has_row)
    assert value["strategy"] == []
    assert value["total_journal_entries"] == int(has_row)
    _assert_guarded(engine, queries, limit=False)


@pytest.mark.parametrize("has_row", [False, True])
def test_synthesis_filters_and_does_not_invent_missing_regime(monkeypatch, has_row):
    from api.routers import regime
    from fastapi import HTTPException

    rows = [("GROWTH", 0.8, "HOLD")] if has_row else []
    engine, queries = _engine(rows)
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    client = MagicMock(is_available=True)
    client.chat.return_value = "Fixture synthesis"
    get_llm = MagicMock(return_value=client)
    monkeypatch.setitem(sys.modules, "llm.router", SimpleNamespace(
        Tier=SimpleNamespace(REASON="fixture"), get_llm=get_llm))
    if has_row:
        value = regime.get_synthesis(_token="fixture")
        assert value["regime_count"] == 1
        assert "GROWTH" in client.chat.call_args.args[0][1]["content"]
    else:
        with pytest.raises(HTTPException) as exc:
            regime.get_synthesis(_token="fixture")
        assert exc.value.status_code == 503
        assert "No non-provisional regime readings" in exc.value.detail
        get_llm.assert_not_called()
    _assert_guarded(engine, queries, limit=False)
