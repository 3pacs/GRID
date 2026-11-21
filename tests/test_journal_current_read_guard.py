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
from sqlalchemy import bindparam, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Select

from store.journal_qualification import (
    DECISION_JOURNAL,
    NON_PROVISIONAL_JOURNAL_PREDICATE as PREDICATE,
    non_provisional_journal_select,
)


PG_PREDICATE = str(text(PREDICATE).compile(dialect=postgresql.dialect()))
NOW = datetime(2026, 11, 8, 12, tzinfo=timezone.utc)
# Derive the reference from Python, rather than copying the SQL helper's list.
PYTHON_STRIP_WHITESPACE = "".join(chr(i) for i in range(0x110000) if chr(i).isspace())


def _engine(journal_rows=(), *, failure=False, production=None):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    queries = []

    def execute(statement, *_args, **_kwargs):
        sql = str(statement.compile(dialect=postgresql.dialect()))
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


def _assert_guarded(engine, queries, columns, *, count=1, limit=True,
                    limit_value=1, order="decision_timestamp", distinct=False,
                    feedback=False):
    reads = [q for q in queries if "FROM decision_journal" in q]
    assert len(reads) == count
    for sql in reads:
        assert PG_PREDICATE in sql
        assert sql.index(PG_PREDICATE) < sql.index("ORDER BY")
        if limit:
            assert sql.index(PG_PREDICATE) < sql.index("LIMIT")
        else:
            assert "LIMIT" not in sql
        assert "WHERE" in sql[:sql.index(PG_PREDICATE)]
        if distinct:
            assert "SELECT DISTINCT ON (decision_journal.inferred_state)" in sql
            assert "ORDER BY decision_journal.inferred_state, decision_journal.decision_timestamp DESC" in sql
        else:
            assert "DISTINCT" not in sql
            expected_order = ("ORDER BY decision_journal.id DESC" if order == "id"
                              else "ORDER BY decision_journal.decision_timestamp DESC")
            assert expected_order in sql
        if feedback:
            assert "decision_journal.decision_timestamp >= NOW() - INTERVAL '30 days'" in sql
            assert "decision_journal.verdict IS NOT NULL" in sql
        else:
            assert "INTERVAL" not in sql
    conn = engine.connect.return_value.__enter__.return_value
    statements = [call for call in conn.execute.call_args_list
                  if "FROM decision_journal" in str(call.args[0].compile(dialect=postgresql.dialect()))]
    for call in statements:
        statement = call.args[0]
        assert isinstance(statement, Select)
        assert tuple(statement.selected_columns.keys()) == tuple(columns)
        params = dict(statement.compile(dialect=postgresql.dialect()).params)
        assert params.pop("journal_marker_whitespace") == PYTHON_STRIP_WHITESPACE
        if "mid" in params:
            assert params.pop("mid") is None
            assert call.args[1] == {"mid": 7}
        assert list(params.values()) == ([limit_value] if limit else [])
    assert all(q.lstrip().upper().startswith("SELECT") for q in queries)
    engine.begin.assert_not_called()


def test_predicate_is_static_with_bound_normalization_and_narrow():
    query = non_provisional_journal_select("id")
    assert query.compile().params == {"journal_marker_whitespace": PYTHON_STRIP_WHITESPACE}
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
    _assert_guarded(engine, queries, {
        "feedback": ("grid_recommendation", "verdict"),
        "flow": ("inferred_state", "state_confidence", "transition_probability"),
        "trace": ("inferred_state",),
        "thesis": ("inferred_state", "state_confidence", "counterfactual"),
        "sentiment": ("inferred_state", "state_confidence"),
    }[consumer], limit_value=50 if consumer == "feedback" else 1,
        order="id" if consumer == "trace" else "decision_timestamp",
        feedback=consumer == "feedback")


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
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "transition_probability",
         "contradiction_flags", "grid_recommendation", "baseline_recommendation",
         "decision_timestamp"), count=2 if has_production and not has_row else 1)


def test_regime_production_miss_uses_only_guarded_fallback(monkeypatch):
    from api.routers import regime

    engine, queries = _engine(production=(7, "fixture", "1"))
    conn = engine.connect.return_value.__enter__.return_value
    execute = conn.execute.side_effect

    def fallback(statement, *args, **kwargs):
        result = execute(statement, *args, **kwargs)
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if "FROM decision_journal" in sql and "model_version_id =" not in sql:
            result.fetchone.return_value = ("GROWTH", 0.8, 0.1, {}, "HOLD", "HOLD", NOW)
        return result

    conn.execute.side_effect = fallback
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    assert regime.get_current(_token="fixture").state == "GROWTH"
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "transition_probability",
         "contradiction_flags", "grid_recommendation", "baseline_recommendation",
         "decision_timestamp"), count=2)


def test_regime_query_failure_never_falls_back_to_raw_history(monkeypatch):
    from api.routers import regime

    engine, queries = _engine(failure=True, production=(7, "fixture", "1"))
    monkeypatch.setattr(regime, "get_db_engine", lambda: engine)
    with pytest.raises(RuntimeError, match="synthetic journal"):
        regime.get_current(_token="fixture")
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "transition_probability",
         "contradiction_flags", "grid_recommendation", "baseline_recommendation",
         "decision_timestamp"))


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
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "counterfactual"))


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
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "grid_recommendation",
         "operator_confidence", "decision_timestamp"))


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
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "grid_recommendation"))


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
    assert all(PG_PREDICATE not in sql for sql in queries)
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
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "transition_probability",
         "contradiction_flags", "grid_recommendation", "decision_timestamp"), distinct=True, limit=False)


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
    _assert_guarded(engine, queries, ("inferred_state", "state_confidence", "grid_recommendation"), distinct=True, limit=False)


@pytest.mark.parametrize("name", ["*", "missing", "id, verdict", "id; DELETE FROM decision_journal"])
def test_builder_rejects_unknown_or_sql_like_column_names(name):
    with pytest.raises(KeyError):
        non_provisional_journal_select(name)


def test_builder_rejects_empty_projection():
    with pytest.raises(ValueError, match="at least one"):
        non_provisional_journal_select()


def test_builder_preserves_column_order_and_binds_values():
    model_id = "7 OR 1=1 --"
    statement = (
        non_provisional_journal_select("state_confidence", "id", "inferred_state")
        .where(DECISION_JOURNAL.c.model_version_id == bindparam("mid"))
        .order_by(DECISION_JOURNAL.c.decision_timestamp.desc())
        .limit(1)
        .params(mid=model_id)
    )
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert tuple(statement.selected_columns.keys()) == ("state_confidence", "id", "inferred_state")
    assert statement.get_final_froms() == [DECISION_JOURNAL]
    assert PG_PREDICATE in sql
    assert compiled.params == {
        "journal_marker_whitespace": PYTHON_STRIP_WHITESPACE, "mid": model_id, "param_1": 1,
    }
    assert model_id not in sql
    assert "model_version_id = %(mid)s" in sql
    assert "LIMIT %(param_1)s" in sql


def test_builder_distinct_on_is_postgresql_clause_not_plain_distinct():
    statement = (
        non_provisional_journal_select("inferred_state", "state_confidence")
        .distinct(DECISION_JOURNAL.c.inferred_state)
        .order_by(DECISION_JOURNAL.c.inferred_state, DECISION_JOURNAL.c.decision_timestamp.desc())
    )
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "SELECT DISTINCT ON (decision_journal.inferred_state)" in sql
    assert "ORDER BY decision_journal.inferred_state, decision_journal.decision_timestamp DESC" in sql
    assert PG_PREDICATE in sql
    assert compiled.params == {"journal_marker_whitespace": PYTHON_STRIP_WHITESPACE}


def test_builder_feedback_cutoff_and_verdict_remain_fixed_clauses():
    statement = (
        non_provisional_journal_select("grid_recommendation", "verdict")
        .where(
            DECISION_JOURNAL.c.decision_timestamp >= text("NOW() - INTERVAL '30 days'"),
            DECISION_JOURNAL.c.verdict.is_not(None),
        )
        .order_by(DECISION_JOURNAL.c.decision_timestamp.desc())
        .limit(50)
    )
    compiled = statement.compile(dialect=postgresql.dialect())
    assert "decision_journal.decision_timestamp >= NOW() - INTERVAL '30 days'" in str(compiled)
    assert "decision_journal.verdict IS NOT NULL" in str(compiled)
    assert compiled.params == {"journal_marker_whitespace": PYTHON_STRIP_WHITESPACE, "param_1": 50}


@pytest.mark.parametrize("normalized_marker", [
    """LOWER(BTRIM(COALESCE(contradiction_flags ->> 'signal_type', ''), :journal_marker_whitespace) COLLATE "C")""",
    """LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,signal_type}', ''), :journal_marker_whitespace) COLLATE "C")""",
    """LOWER(BTRIM(COALESCE(contradiction_flags ->> 'producer_module', ''), :journal_marker_whitespace) COLLATE "C")""",
    """LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,producer_module}', ''), :journal_marker_whitespace) COLLATE "C")""",
    """LOWER(BTRIM(COALESCE(contradiction_flags ->> 'source', ''), :journal_marker_whitespace) COLLATE "C")""",
    """LOWER(BTRIM(COALESCE(contradiction_flags #>> '{context,source}', ''), :journal_marker_whitespace) COLLATE "C")""",
])
def test_every_structured_financial_marker_normalizes_case_and_outer_whitespace(normalized_marker):
    assert normalized_marker in PREDICATE
    # Fixed normalization is applied before exact equality or prefix slicing.
    assert PREDICATE.count("LOWER(BTRIM(") == 8
    compiled = non_provisional_journal_select("id").compile(dialect=postgresql.dialect())
    assert compiled.params == {"journal_marker_whitespace": PYTHON_STRIP_WHITESPACE}
    assert PYTHON_STRIP_WHITESPACE not in str(compiled)


def test_normalization_preserves_exact_financial_family_boundaries():
    assert PREDICATE.count("= 'fundamental_divergence'") == 4
    assert PREDICATE.count("= 'intelligence.fundamental_divergence'") == 2
    assert PREDICATE.count(", 23) = 'fundamental_divergence:'") == 2
    assert "LIKE" not in PREDICATE
    assert "REGEXP" not in PREDICATE
    assert "REPLACE" not in PREDICATE  # Never strip whitespace from inside a marker.


def test_legacy_provisional_marker_rules_remain_unchanged():
    assert "LEFT(UPPER(COALESCE(inferred_state, '')), 12) = 'PROVISIONAL_'" in PREDICATE
    assert "UPPER(COALESCE(action_taken, '')) = 'PROVISIONAL'" in PREDICATE
    assert "LEFT(UPPER(COALESCE(action_taken, '')), 12) = 'PROVISIONAL '" in PREDICATE
    assert "LEFT(LOWER(COALESCE(counterfactual, '')), 26) = 'provisional-from-contract:'" in PREDICATE
