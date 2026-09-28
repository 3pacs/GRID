"""Wave 3 triage report item #15: earnings_predictions / the scorecard are a
frozen, unmaintained artifact (run_earnings_cycle last scored/predicted
2026-04-06; ``confidence = clamp(net/3, 0.1, 0.9)`` is a tuned constant, not
a calibrated probability). Neither ``get_earnings_calendar`` nor
``get_prediction_scorecard`` may present that confidence as if it were live
or validated — both must carry an explicit frozen-since label so the PWA
never has to infer that from silence.

The genuinely fresh earnings_calendar fields (ticker/date/eps_estimate/...)
are untouched; this only pins the honesty label on the embedded prediction
and on the scorecard.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from intelligence import earnings_intel as ei

# earnings_intel.py calls .isoformat() on the DATE/TIMESTAMP columns it
# reads back, which assumes a real date/datetime object the way psycopg2
# hands them back on Postgres. SQLite's driver returns the raw stored
# string unless PARSE_DECLTYPES is on with a registered converter — wire
# that up so these fixtures behave like the real deployment.
sqlite3.register_converter("DATE", lambda b: date.fromisoformat(b.decode()))
sqlite3.register_converter(
    "TIMESTAMP", lambda b: datetime.fromisoformat(b.decode())
)


@pytest.fixture(autouse=True)
def _no_postgres_only_ddl():
    # _ensure_tables issues Postgres-only DDL (TIMESTAMPTZ, DEFAULT NOW())
    # that SQLite's parser rejects outright, even under CREATE TABLE IF NOT
    # EXISTS — these tests supply their own (already-present) SQLite-
    # compatible schema, so the real _ensure_tables call is a no-op here.
    with patch.object(ei, "_ensure_tables", lambda engine: None):
        yield


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES},
    )
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE earnings_calendar (
                ticker TEXT, earnings_date DATE, fiscal_quarter TEXT,
                eps_estimate REAL, revenue_estimate REAL, reported INTEGER
            )
        """))
        conn.execute(text("""
            CREATE TABLE earnings_predictions (
                id TEXT PRIMARY KEY, ticker TEXT, earnings_date DATE,
                predicted_direction TEXT, predicted_move_pct REAL,
                confidence REAL, iv_rank REAL, historical_surprise_avg REAL,
                historical_beat_rate REAL, sector_momentum REAL,
                insider_signal TEXT, congressional_signal TEXT,
                reasoning TEXT, actual_direction TEXT, actual_move_pct REAL,
                verdict TEXT DEFAULT 'pending', scored_at TIMESTAMP
            )
        """))
    return engine


class TestCalendarPredictionFrozenLabel:
    def test_entry_without_prediction_has_no_prediction_block(self):
        engine = _engine()
        d = date.today() + timedelta(days=5)
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO earnings_calendar (ticker, earnings_date, reported) "
                     "VALUES (:t, :d, 0)"),
                {"t": "AAPL", "d": d},
            )
        entries = ei.get_earnings_calendar(engine, days_ahead=30)
        assert entries[0]["prediction"] is None

    def test_entry_with_prediction_carries_frozen_label(self):
        engine = _engine()
        d = date.today() + timedelta(days=5)
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO earnings_calendar (ticker, earnings_date, reported) "
                     "VALUES (:t, :d, 0)"),
                {"t": "MSFT", "d": d},
            )
            conn.execute(
                text("INSERT INTO earnings_predictions "
                     "(id, ticker, earnings_date, predicted_direction, "
                     "predicted_move_pct, confidence, verdict) "
                     "VALUES (:id, :t, :d, 'up', 3.0, 0.72, 'pending')"),
                {"id": "p1", "t": "MSFT", "d": d},
            )
        entries = ei.get_earnings_calendar(engine, days_ahead=30)
        pred = entries[0]["prediction"]
        assert pred is not None
        assert pred["confidence"] == 0.72  # value preserved for history/debugging
        assert pred["frozen_since"] == ei.PREDICTION_SCORECARD_FROZEN_SINCE
        assert "unmaintained" in pred["note"] or "not scheduled" not in pred["note"]
        assert "2026-04-06" in pred["note"] or pred["frozen_since"] == "2026-04-06"


class TestPredictionScorecardFrozenLabel:
    def test_scorecard_carries_frozen_since_and_note(self):
        engine = _engine()
        out = ei.get_prediction_scorecard(engine)
        assert out["frozen_since"] == "2026-04-06"
        assert out["note"] == ei.PREDICTION_SCORECARD_FROZEN_NOTE
        assert "confidence" in out["note"]

    def test_scorecard_still_reports_overall_stats(self):
        engine = _engine()
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO earnings_predictions "
                     "(id, ticker, earnings_date, predicted_direction, "
                     "confidence, verdict, scored_at) "
                     "VALUES ('p1','AAPL','2026-04-01','up',0.7,'hit','2026-04-02')"),
            )
        out = ei.get_prediction_scorecard(engine)
        assert out["overall"]["total_scored"] == 1
        assert out["overall"]["hits"] == 1
