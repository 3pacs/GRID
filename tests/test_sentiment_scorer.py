"""Regression tests for intelligence/sentiment_scorer.py::_score_signals.

Production hit `unsupported operand type(s) for +=: 'float' and
'decimal.Decimal'` every hourly cycle: `signal_sources.trust_score` comes
back from psycopg2 as `decimal.Decimal` (NUMERIC column), but
`bull_score`/`bear_score` accumulators are seeded as Python floats.
`_score_signals` must convert each row's trust value to float at the
DB boundary before accumulating, matching the pattern already used by
`_score_flows` (`float(amount or 0)`).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from intelligence import sentiment_scorer
from intelligence.sentiment_scorer import (
    BEARISH_SIGNALS,
    BULLISH_SIGNALS,
    DEFAULT_WEIGHTS,
    _score_signals,
    score_past_predictions,
)


def _mock_engine(rows):
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    conn.execute.return_value.fetchall.return_value = rows
    return engine


class TestScoreSignalsDecimalBoundary:
    def test_decimal_trust_scores_do_not_raise(self):
        bull_type = next(iter(BULLISH_SIGNALS))
        bear_type = next(iter(BEARISH_SIGNALS))
        rows = [
            (bull_type, Decimal("0.80")),
            (bull_type, Decimal("0.65")),
            (bear_type, Decimal("0.40")),
        ]
        engine = _mock_engine(rows)

        component = _score_signals(engine)

        # No exception, and the raw_value reflects the Decimal inputs
        # converted to float (0.80 + 0.65 - 0.40 = 1.05).
        assert component.name == "signals"
        assert round(component.raw_value, 2) == 1.05
        assert isinstance(component.raw_value, float)
        assert component.detail.startswith("3 signals")

    def test_mixed_decimal_and_float_trust_scores(self):
        bull_type = next(iter(BULLISH_SIGNALS))
        rows = [
            (bull_type, Decimal("0.5")),
            (bull_type, 0.5),
        ]
        engine = _mock_engine(rows)

        component = _score_signals(engine)

        assert round(component.raw_value, 2) == 1.0

    def test_no_rows_returns_zero_component(self):
        engine = _mock_engine([])
        component = _score_signals(engine)
        assert component.score == 0.0
        assert component.detail == "No scored signals available"


# ---------------------------------------------------------------------------
# Outcome recording vs learned-weight promotion (PR #903 source review,
# 2026-10-23). These run the real ``score_past_predictions`` -- and, on the
# default path, the real ``_update_weights`` -- against a scripted
# connection that dispatches on SQL text and records every statement, so
# the coupling between scoring and ``sentiment_weights`` inserts is observed,
# not stubbed. Nothing here touches a database.
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class ScriptedScoringConnection:
    """Stand-in for a SQLAlchemy connection for the scoring path.

    ``pending``: rows of the PENDING query ``(id, prediction_date, score,
    label, components_json, weights_json)``. ``windows``: per
    ``prediction_date`` the raw_series rows read_window returns
    ``(obs_date, value, pull_timestamp, source_name)``. ``scored_history``:
    rows ``_update_weights`` reads ``(score, components, outcome,
    realized_return)``.
    """

    def __init__(self, pending, windows, scored_history=()):
        self.pending = list(pending)
        self.windows = dict(windows)
        self.scored_history = list(scored_history)
        self.executed: list[tuple[str, dict]] = []
        self.updates: list[dict] = []
        self.weight_inserts: list[dict] = []
        self.commits = 0

    def execute(self, stmt, params=None):
        sql = " ".join(str(getattr(stmt, "text", stmt)).split())
        params = dict(params or {})
        self.executed.append((sql, params))
        if sql.startswith("CREATE TABLE"):
            return _Result([])
        if "FROM sentiment_predictions" in sql and "outcome = 'PENDING'" in sql:
            return _Result(self.pending)
        if "FROM raw_series" in sql:
            rows = self.windows.get(params.get("start"), [])
            as_of = params.get("as_of")
            return _Result([r for r in rows if as_of is None or r[0] <= as_of])
        if sql.startswith("UPDATE sentiment_predictions"):
            self.updates.append(params)
            return _Result([])
        if "FROM sentiment_predictions" in sql and "WHERE outcome IN" in sql:
            return _Result(self.scored_history)
        if "FROM sentiment_weights" in sql:
            return _Result([])
        if sql.startswith("INSERT INTO sentiment_weights"):
            self.weight_inserts.append(params)
            return _Result([])
        raise AssertionError(f"unexpected SQL in scoring path: {sql[:90]}")

    def commit(self):
        self.commits += 1


class _Ctx:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


class ScriptedScoringEngine:
    def __init__(self, conn: ScriptedScoringConnection):
        self.conn = conn

    def connect(self):
        return _Ctx(self.conn)


_TS = datetime(2026, 10, 22, 22, 2, 46, tzinfo=timezone.utc)


def _components_json(**scores):
    return json.dumps(
        [{"name": n, "score": scores.get(n, 0.0), "weight": w} for n, w in DEFAULT_WEIGHTS.items()]
    )


def eligible_backlog():
    """Three predictions whose 5-day windows have elapsed.

    Prediction 196 is the traced live example (2026-10-14, BULLISH +0.232):
    its window closes 7718.94 -> 7663.43, realized -0.72%, so the real
    scorer must mark it WRONG. The other two cover CORRECT and
    NEUTRAL_CORRECT.
    """
    pending = [
        (196, date(2026, 10, 14), 0.23195651, "BULLISH", _components_json(momentum=0.22), "{}"),
        (195, date(2026, 10, 13), -0.30, "BEARISH", _components_json(momentum=-0.30), "{}"),
        (194, date(2026, 10, 12), 0.01, "NEUTRAL", _components_json(), "{}"),
    ]
    windows = {
        date(2026, 10, 14): [
            (date(2026, 10, 14), 7718.93994140625, _TS, "yfinance"),
            (date(2026, 10, 15), 7744.06005859375, _TS, "yfinance"),
            (date(2026, 10, 16), 7659.43994140625, _TS, "yfinance"),
            (date(2026, 10, 19), 7690.72998046875, _TS, "yfinance"),
            (date(2026, 10, 20), 7712.56982421875, _TS, "yfinance"),
            (date(2026, 10, 21), 7757.169921875, _TS, "yfinance"),
            (date(2026, 10, 22), 7663.43017578125, _TS, "yfinance"),
        ],
        date(2026, 10, 13): [
            (date(2026, 10, 13), 7800.0, _TS, "yfinance"),
            (date(2026, 10, 20), 7700.0, _TS, "yfinance"),
        ],
        date(2026, 10, 12): [
            (date(2026, 10, 12), 7700.0, _TS, "yfinance"),
            (date(2026, 10, 19), 7710.0, _TS, "yfinance"),
        ],
    }
    return pending, windows


def scored_history(n: int = 12):
    """Enough already-scored rows for ``_update_weights`` to insert a version."""
    return [
        (0.2, json.loads(_components_json(momentum=0.2)), "CORRECT", 0.01)
        for _ in range(n)
    ]


def make_scoring_engine(scored_rows=()):
    pending, windows = eligible_backlog()
    conn = ScriptedScoringConnection(pending, windows, scored_rows)
    return ScriptedScoringEngine(conn), conn


class TestOutcomeRecordingWithoutWeightPromotion:
    def test_outcome_only_path_scores_backlog_and_inserts_no_weights(self):
        engine, conn = make_scoring_engine(scored_history())

        summary = score_past_predictions(engine, update_weights=False)

        assert summary["scored"] == 3
        assert summary["correct"] == 2
        assert summary["weight_promotion"] == "disabled"
        by_id = {u["id"]: u for u in conn.updates}
        assert by_id[196]["outcome"] == "WRONG"
        assert round(by_id[196]["ret"] * 100, 4) == -0.7191
        assert by_id[195]["outcome"] == "CORRECT"
        assert by_id[194]["outcome"] == "NEUTRAL_CORRECT"
        assert conn.weight_inserts == []
        assert not any(sql.startswith("INSERT INTO sentiment_weights") for sql, _ in conn.executed)
        assert not any("WHERE outcome IN" in sql for sql, _ in conn.executed)
        assert conn.commits >= 1

    def test_default_path_still_promotes_weights_for_existing_callers(self):
        engine, conn = make_scoring_engine(scored_history())

        summary = score_past_predictions(engine)

        assert summary["scored"] == 3
        assert summary["weight_promotion"] == "attempted"
        assert len(conn.weight_inserts) == 1
        assert conn.weight_inserts[0]["version"] == 1

    def test_default_path_below_threshold_does_not_promote(self):
        pending, windows = eligible_backlog()
        conn = ScriptedScoringConnection(pending[:2], windows, scored_history())
        summary = score_past_predictions(ScriptedScoringEngine(conn))

        assert summary["scored"] == 2
        assert summary["weight_promotion"] == "not_due"
        assert conn.weight_inserts == []

    def test_no_pending_reports_promotion_state(self):
        conn = ScriptedScoringConnection([], {}, [])
        engine = ScriptedScoringEngine(conn)
        assert score_past_predictions(engine, update_weights=False)["weight_promotion"] == "disabled"
        assert score_past_predictions(engine)["weight_promotion"] == "not_due"

    def test_run_sentiment_cycle_keeps_default_promotion_contract(self, monkeypatch):
        calls: list[dict] = []

        def _score(engine, **kwargs):
            calls.append(kwargs)
            return {"scored": 0, "message": "none", "weight_promotion": "not_due"}

        class _Res:
            def to_dict(self):
                return {"score": 0.0, "label": "NEUTRAL"}

        monkeypatch.setattr(sentiment_scorer, "score_past_predictions", _score)
        monkeypatch.setattr(sentiment_scorer, "compute_sentiment", lambda engine: _Res())
        monkeypatch.setattr(sentiment_scorer, "log_prediction", lambda engine, result: 1)

        sentiment_scorer.run_sentiment_cycle(object())

        assert calls == [{}]
