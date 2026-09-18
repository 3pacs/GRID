"""Honesty guards for the earnings intelligence predictor (B-H8, B-M20).

With no options IV row the published expected move used to be 70% a literal 2.0,
`beat_rate` defaulted to the 0.5 midpoint inside the score, and a scorecard with
nothing scored reported 0.0% accuracy. All three are now explicit gaps.
"""

from __future__ import annotations

import inspect
from datetime import date
from unittest.mock import MagicMock

import pytest

import intelligence.earnings_intel as ei
from intelligence.earnings_intel import (
    get_prediction_scorecard,
    predict_earnings_reaction,
)


def test_source_has_no_magic_expected_move_default() -> None:
    source = inspect.getsource(ei)
    assert '"expected_move_options", 2.0' not in source
    assert '"historical_beat_rate", 0.5' not in source


class _FakeConn:
    """Minimal connection stub: routes each SQL statement to a canned result."""

    def __init__(self, *, history: list, iv_row, sector_row=None):
        self.history = history
        self.iv_row = iv_row
        self.sector_row = sector_row

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        result = MagicMock()
        if "earnings_date >= CURRENT_DATE" in sql and "eps_estimate" in sql:
            result.fetchone.return_value = (date(2026, 10, 1), 1.5, None)
        elif "reported = TRUE" in sql:
            result.fetchall.return_value = self.history
        else:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def engine(monkeypatch):
    eng = MagicMock()
    monkeypatch.setattr(ei, "_ensure_tables", lambda _engine: None)
    monkeypatch.setattr(ei, "_store_prediction", lambda _engine, _pred: None)
    monkeypatch.setattr(ei, "_get_sector_momentum", lambda _conn, _t: None)
    monkeypatch.setattr(ei, "_get_insider_signal", lambda _conn, _t: "none")
    monkeypatch.setattr(ei, "_get_congressional_signal", lambda _conn, _t: "none")
    return eng


def _run(engine, monkeypatch, *, history, iv_data):
    conn = _FakeConn(history=history, iv_row=None)
    engine.connect.return_value = conn
    monkeypatch.setattr(ei, "_get_iv_data", lambda _conn, _t: iv_data)
    return predict_earnings_reaction(engine, "AAPL")


def test_no_options_row_uses_history_only(engine, monkeypatch) -> None:
    pred = _run(
        engine,
        monkeypatch,
        history=[(6.0, "beat"), (4.0, "beat"), (-1.0, "miss"), (2.0, "beat")],
        iv_data=None,
    )
    assert pred["predicted_move_basis"] == "history_only"
    assert pred["expected_move_options"] is None
    # 2.75 is the mean |surprise|; the old code returned 0.3*2.75 + 0.7*2.0.
    assert abs(pred["predicted_move_pct"]) == pytest.approx(2.75, abs=0.01)


def test_options_row_present_blends_and_says_so(engine, monkeypatch) -> None:
    pred = _run(
        engine,
        monkeypatch,
        history=[(6.0, "beat"), (4.0, "beat"), (-1.0, "miss"), (2.0, "beat")],
        iv_data={"iv_rank": 55.0, "iv_atm": 0.40},
    )
    assert pred["predicted_move_basis"] == "history_and_options"
    assert pred["expected_move_options"] is not None
    assert pred["expected_move_options"] > 0


def test_no_history_drops_the_beat_rate_term(engine, monkeypatch) -> None:
    pred = _run(engine, monkeypatch, history=[], iv_data={"iv_rank": 50.0, "iv_atm": 0.30})
    assert pred["historical_beat_rate"] is None
    assert pred["historical_surprise_avg"] is None
    assert pred["predicted_move_basis"] == "options_only"
    # A dropped term must not become a bullish or bearish tilt of its own.
    assert pred["predicted_direction"] == "flat"


def test_no_history_and_no_options_publishes_no_move(engine, monkeypatch) -> None:
    pred = _run(engine, monkeypatch, history=[], iv_data=None)
    assert pred["predicted_move_basis"] == "unavailable"
    assert pred["predicted_move_pct"] is None
    assert pred["expected_move_options"] is None
    assert pred["historical_beat_rate"] is None


def test_history_without_a_beat_rate_does_not_tilt_the_call(engine, monkeypatch) -> None:
    """Partial data: surprises exist but every quarter is unclassified."""
    pred = _run(
        engine,
        monkeypatch,
        history=[(3.0, None), (1.0, None)],
        iv_data=None,
    )
    # beat_rate is 0/2 = 0.0, a genuine measurement, so it may tilt bearish.
    assert pred["historical_beat_rate"] == 0.0
    assert pred["predicted_move_basis"] == "history_only"


# ── scorecard ────────────────────────────────────────────────────────────────


class _ScorecardConn:
    def __init__(self, scored_rows):
        self.scored_rows = scored_rows

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        result = MagicMock()
        if "GROUP BY verdict" in sql:
            result.fetchall.return_value = self.scored_rows
        else:
            result.fetchall.return_value = []
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_scorecard_with_nothing_scored_is_null_not_zero(monkeypatch) -> None:
    eng = MagicMock()
    monkeypatch.setattr(ei, "_ensure_tables", lambda _engine: None)
    monkeypatch.setattr(ei, "_count_pending", lambda _engine: 7)
    eng.connect.return_value = _ScorecardConn([])

    card = get_prediction_scorecard(eng)
    assert card["overall"]["accuracy_pct"] is None
    assert card["overall"]["scored_n"] == 0
    assert card["overall"]["hits"] == 0
    assert card["overall"]["pending"] == 7


def test_scorecard_with_scored_rows_reports_the_rate(monkeypatch) -> None:
    eng = MagicMock()
    monkeypatch.setattr(ei, "_ensure_tables", lambda _engine: None)
    monkeypatch.setattr(ei, "_count_pending", lambda _engine: 0)
    eng.connect.return_value = _ScorecardConn([("hit", 3), ("miss", 1)])

    card = get_prediction_scorecard(eng)
    assert card["overall"]["accuracy_pct"] == 75.0
    assert card["overall"]["scored_n"] == 4
