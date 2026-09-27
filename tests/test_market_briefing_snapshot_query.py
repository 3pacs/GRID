"""Regression tests for the resolved_series feature-value query in
``ollama/market_briefing.py::MarketBriefingEngine._gather_market_snapshot``.

Context (see the in-code comment at the call site): the query used to be a
single statement with a correlated ``obs_date = (SELECT MAX(obs_date) FROM
resolved_series WHERE feature_id = rs.feature_id)`` subquery. That subquery
re-scans every historical vintage of ``resolved_series`` once per outer row
and reliably tripped the hourly briefing's DB statement_timeout
(``psycopg2.errors.QueryCanceled``). It was replaced with a two-step,
parameterized query pair:

1. ``SELECT id FROM feature_registry WHERE model_eligible = TRUE`` — the
   bounded set of feature ids the snapshot actually needs.
2. A ``JOIN LATERAL`` "top-1-per-feature" query, bound via ``ANY(:fids)``
   (the same idiom as ``store/pit.py``'s ``PITStore.get_pit``), that reads
   the single latest ``(obs_date DESC, release_date DESC)`` row per feature
   from an index instead of sorting the whole table.

These tests check the SQL *shape* (no more correlated ``MAX()`` subquery,
parameterized ``ANY(:fids)``, ``release_date IS NOT NULL``, no string
interpolation of untrusted values) against a fake engine/connection, and the
*behaviour* (feature list built correctly; short-circuits cleanly when there
are no model-eligible features).

No SQLite behavioural run is included: the rewritten query relies on
Postgres-only syntax -- ``JOIN LATERAL`` combined with array-bind parameter
expansion via ``= ANY(:fids)`` -- neither of which the ``sqlite3`` DB-API
driver used by SQLAlchemy's sqlite dialect supports (array bind params
aren't a SQLite concept at all). This mirrors the existing, documented
constraint on ``store/pit.py``'s own ``DISTINCT ON`` queries in
``.claude/rules/data-integrity.md``: "this system will never work on SQLite
or MySQL." A fake engine is therefore the correct and sufficient tool here,
consistent with how the rest of this file's sibling tests
(``test_market_briefing_number_grounding.py``) already stand in for the DB.
"""
from __future__ import annotations

import os
from datetime import date

os.environ.setdefault("DB_PASSWORD", "test-password")

from ollama.market_briefing import MarketBriefingEngine


# ── Fakes ─────────────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, one=None, many=None):
        self._one = one
        self._many = many if many is not None else []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _FakeConnection:
    """Dispatches by SQL substring, matching each distinct statement
    ``_gather_market_snapshot`` issues on one connection.

    ``executed`` records every ``(sql, params)`` pair so tests can assert on
    the exact statement shape and bound parameters, not just the outcome.
    """

    def __init__(self, *, feature_ids=None, feature_rows=None):
        self.feature_ids = feature_ids if feature_ids is not None else []
        self.feature_rows = feature_rows if feature_rows is not None else []
        self.executed: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, params))

        if "SELECT id FROM feature_registry" in sql and "WHERE model_eligible = TRUE" in sql:
            return _FakeResult(many=[(fid,) for fid in self.feature_ids])
        if "JOIN LATERAL" in sql:
            return _FakeResult(many=self.feature_rows)
        if "FROM resolved_series" in sql and "fr.name = :name" in sql:
            return _FakeResult(one=None)  # spy put/call resolved_series lookup
        if "FROM options_daily_signals" in sql:
            return _FakeResult(one=None)
        if "FROM raw_series" in sql:
            return _FakeResult(one=None)
        if "FROM decision_journal" in sql:
            return _FakeResult(one=None)
        if "FROM signal_sources" in sql:
            return _FakeResult(many=[])
        return _FakeResult()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, conn: _FakeConnection):
        self._conn = conn

    def connect(self):
        return self._conn


class _FakeOllamaClient:
    is_available = False

    def chat(self, **kwargs):  # pragma: no cover - not exercised here
        raise AssertionError("LLM should not be called by these tests")


def _make_engine(conn: _FakeConnection, tmp_path) -> MarketBriefingEngine:
    engine = MarketBriefingEngine(ollama_client=_FakeOllamaClient(), db_engine=_FakeEngine(conn))
    engine.output_dir = tmp_path  # never touch the real outputs/market_briefings/
    return engine


def _feature_query_sql(conn: _FakeConnection) -> str:
    matches = [sql for sql, _ in conn.executed if "JOIN LATERAL" in sql]
    assert matches, "expected the LATERAL feature-value query to run"
    assert len(matches) == 1
    return matches[0]


# ── SQL shape ──────────────────────────────────────────────────────────────


def test_no_correlated_max_obs_date_subquery_remains(tmp_path):
    """The bug: a correlated MAX(obs_date) subquery scanned per outer row."""
    conn = _FakeConnection(feature_ids=[1, 2], feature_rows=[])
    engine = _make_engine(conn, tmp_path)

    engine._gather_market_snapshot()

    for sql, _ in conn.executed:
        assert "MAX(obs_date)" not in sql
        assert "MAX(OBS_DATE)" not in sql.upper()


def test_feature_ids_are_fetched_first_and_bounded_to_model_eligible(tmp_path):
    conn = _FakeConnection(feature_ids=[10, 20, 30], feature_rows=[])
    engine = _make_engine(conn, tmp_path)

    engine._gather_market_snapshot()

    id_query_calls = [
        (sql, params)
        for sql, params in conn.executed
        if "SELECT id FROM feature_registry" in sql
    ]
    assert len(id_query_calls) == 1
    sql, params = id_query_calls[0]
    assert "model_eligible = TRUE" in sql
    assert params == {}  # boolean literal, nothing user-supplied to bind


def test_feature_value_query_is_parameterized_via_any_fids(tmp_path):
    conn = _FakeConnection(feature_ids=[10, 20, 30], feature_rows=[])
    engine = _make_engine(conn, tmp_path)

    engine._gather_market_snapshot()

    sql = _feature_query_sql(conn)
    params_matches = [p for s, p in conn.executed if s is sql]
    assert params_matches
    params = params_matches[0]

    # Parameterized (bound), never interpolated into the SQL string.
    assert "ANY(:fids)" in sql
    assert params.get("fids") == [10, 20, 30]

    # Bounded to the features the snapshot needs, not every feature ever
    # registered.
    assert "fr.id = ANY(:fids)" in sql


def test_feature_value_query_orders_latest_obs_date_then_release_date(tmp_path):
    conn = _FakeConnection(feature_ids=[1], feature_rows=[])
    engine = _make_engine(conn, tmp_path)

    engine._gather_market_snapshot()

    sql = _feature_query_sql(conn)
    # store/pit.py + data-integrity.md vintage convention: latest obs_date
    # wins; release_date DESC deterministically tiebreaks same-obs_date
    # multi-vintage rows instead of returning an arbitrary one.
    assert "ORDER BY rs.obs_date DESC, rs.release_date DESC" in sql
    assert "release_date IS NOT NULL" in sql


def test_feature_value_query_uses_no_string_formatting(tmp_path):
    """Every dynamic value must travel as a bind param, never via
    f-string/.format()/concatenation into the SQL text -- per
    .claude/rules/security.md's SQL Safety rule."""
    conn = _FakeConnection(feature_ids=[1, 2], feature_rows=[])
    engine = _make_engine(conn, tmp_path)

    engine._gather_market_snapshot()

    sql = _feature_query_sql(conn)
    assert "1" not in sql.replace("LIMIT 1", "")  # no literal feature id spliced in
    assert "2" not in sql


# ── Behaviour ──────────────────────────────────────────────────────────────


def test_features_dict_built_from_query_rows(tmp_path):
    conn = _FakeConnection(
        feature_ids=[1, 2],
        feature_rows=[
            ("dgs10", 4.123456, date(2026, 9, 26)),
            ("vix_term_structure", -1.5, date(2026, 9, 25)),
        ],
    )
    engine = _make_engine(conn, tmp_path)

    snapshot = engine._gather_market_snapshot()

    assert snapshot["features"] == {
        "dgs10": {"value": 4.1235, "date": "2026-09-26"},
        "vix_term_structure": {"value": -1.5, "date": "2026-09-25"},
    }


def test_no_model_eligible_features_short_circuits_without_running_second_query(tmp_path):
    conn = _FakeConnection(feature_ids=[], feature_rows=[("should_not_appear", 1.0, date(2026, 9, 26))])
    engine = _make_engine(conn, tmp_path)

    snapshot = engine._gather_market_snapshot()

    assert snapshot["features"] == {}
    assert not any("JOIN LATERAL" in sql for sql, _ in conn.executed)


def test_gather_market_snapshot_never_raises_when_feature_query_blows_up(tmp_path):
    """The whole gather is wrapped in a broad try/except (graceful
    degradation per CLAUDE.md) -- a DB error must not crash the briefing."""

    class _Blows(_FakeConnection):
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "JOIN LATERAL" in sql:
                raise RuntimeError("statement timeout")
            return super().execute(stmt, params)

    conn = _Blows(feature_ids=[1], feature_rows=[])
    engine = _make_engine(conn, tmp_path)

    snapshot = engine._gather_market_snapshot()  # must not raise
    # The outer try/except in _gather_market_snapshot catches the failure
    # before "features" is ever assigned -- confirm no partial/stale value.
    assert snapshot.get("features") is None
