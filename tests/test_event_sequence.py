"""Tests for intelligence/event_sequence.py source pulls (D0 gaps G6/G7, #1129, #1142).

Runs the real SQL against an in-memory SQLite database with the columns the
pulls read. After the #1145 fix was integrated with Codex's D0 reader guards
(8befc05e), the contract is:

* ``_pull_signal_source_events`` reads rows by column name (the old ``r[8]``
  read on an 8-column row dropped every event). Events are emitted, but their
  timing is held: ``confidence="unverified"``, ``timestamp_basis=
  "source_event_date"`` and a ``timing_hold_reason``. Stored ``outcome`` text
  is not copied into descriptions, because outcomes need their own
  availability boundary.
* ``_pull_earnings_events`` withholds calendar rows and filing rollups without
  reading them. Neither ``period_end`` nor ``filing_date`` proves when the
  stored summary/sentiment payload became available, and the compat stubs
  (``earnings_date == period_end``) would otherwise read as releases.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from intelligence import event_sequence as es


NOW = datetime.now(timezone.utc)


def _d(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).date().isoformat()


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE signal_sources (
                id INTEGER PRIMARY KEY, source_type TEXT, source_id TEXT,
                ticker TEXT, signal_date DATE, signal_type TEXT,
                signal_value TEXT, outcome TEXT, trust_score NUMERIC
            )"""))
        conn.execute(text("""
            CREATE TABLE earnings_calendar (
                ticker TEXT, earnings_date DATE, fiscal_quarter TEXT,
                eps_estimate REAL, eps_actual REAL, eps_surprise_pct REAL,
                revenue_estimate REAL, revenue_actual REAL,
                revenue_surprise_pct REAL, classification TEXT, reported BOOLEAN
            )"""))
        conn.execute(text("""
            CREATE TABLE earnings_events (
                ticker TEXT, event_type TEXT, filing_date DATE, period_end DATE,
                fiscal_quarter TEXT, summary TEXT, sentiment TEXT,
                confidence REAL
            )"""))
    return eng


def _insert(engine, table: str, **cols) -> None:
    names = ", ".join(cols)
    binds = ", ".join(f":{k}" for k in cols)
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO {table} ({names}) VALUES ({binds})"), cols)  # noqa: S608 - test fixture


CUTOFF = NOW - timedelta(days=90)


class TestSignalSourceEvents:
    def test_rows_are_returned_not_dropped_and_timing_is_held(self, engine):
        _insert(engine, "signal_sources", source_type="congressional",
                source_id="Rep. Example", ticker="NVDA", signal_date=_d(10),
                signal_type="BUY",
                signal_value=json.dumps({"amount_usd": 50000, "description": "purchase"}),
                outcome="PENDING", trust_score=0.85)
        _insert(engine, "signal_sources", source_type="darkpool",
                source_id="scanner", ticker="NVDA", signal_date=_d(5),
                signal_type="SELL", signal_value=None, outcome="CORRECT",
                trust_score=0.3)

        events = es._pull_signal_source_events(engine, "NVDA", CUTOFF)

        assert len(events) == 2  # the G6 bug returned [] here
        by_type = {e.event_type: e for e in events}
        cong = by_type["congressional"]
        assert cong.amount_usd == 50000.0
        assert cong.direction == "bullish"
        assert cong.actor == "Rep. Example"
        assert "purchase" in cong.description
        assert cong.timestamp.startswith(_d(10))

        dark = by_type["dark_pool"]
        assert dark.direction == "bearish"
        assert dark.amount_usd is None
        for ev in events:
            assert ev.confidence == "unverified"  # not derived from trust_score
            assert ev.timestamp_basis == "source_event_date"
            assert ev.timing_hold_reason == es.EVENT_TIMING_HOLD_REASON
            assert ev.source_record_id is not None
            assert "[outcome:" not in ev.description  # no outcome leakage

    def test_respects_cutoff_and_ticker(self, engine):
        _insert(engine, "signal_sources", source_type="insider", source_id="CFO",
                ticker="NVDA", signal_date=_d(200), signal_type="BUY",
                signal_value="{}", outcome=None, trust_score=0.5)
        _insert(engine, "signal_sources", source_type="insider", source_id="CFO",
                ticker="AMD", signal_date=_d(3), signal_type="BUY",
                signal_value="{}", outcome=None, trust_score=0.5)

        assert es._pull_signal_source_events(engine, "NVDA", CUTOFF) == []

    def test_malformed_json_payload_keeps_event(self, engine):
        _insert(engine, "signal_sources", source_type="whale", source_id="0xabc",
                ticker="NVDA", signal_date=_d(2), signal_type="UNUSUAL_VOLUME",
                signal_value="not json", outcome=None, trust_score=None)

        events = es._pull_signal_source_events(engine, "NVDA", CUTOFF)

        assert len(events) == 1
        assert events[0].amount_usd is None
        assert events[0].direction == "neutral"


class TestEarningsWithheld:
    """Calendar rows and filing rollups are withheld until release/version receipts exist."""

    def _seed(self, engine):
        # A real filing (filing_date after period_end), a G7 compat stub
        # (earnings_date == period_end, no EPS actual), and a calendar row
        # that does carry an EPS actual.
        _insert(engine, "earnings_events", ticker="NVDA", event_type="10-Q",
                filing_date=_d(20), period_end=_d(50), fiscal_quarter="Q3",
                summary="quarterly report", sentiment="positive", confidence=0.9)
        _insert(engine, "earnings_calendar", ticker="NVDA", earnings_date=_d(50),
                fiscal_quarter="Q3", eps_actual=None, reported=True)
        _insert(engine, "earnings_calendar", ticker="NVDA", earnings_date=_d(15),
                fiscal_quarter="Q4", eps_estimate=1.0, eps_actual=1.2,
                eps_surprise_pct=20.0, reported=True)

    def test_nothing_is_emitted(self, engine):
        self._seed(engine)
        assert es._pull_earnings_events(engine, "NVDA", CUTOFF) == []

    def test_compat_stub_cannot_appear_as_a_release(self, engine):
        self._seed(engine)
        events = es._pull_earnings_events(engine, "NVDA", CUTOFF)
        assert not any(_d(50) in str(e.timestamp) for e in events)

    def test_withholding_does_no_io(self):
        class Boom:
            def connect(self):
                raise AssertionError("earnings pull must not read the database")

            begin = connect

        assert es._pull_earnings_events(Boom(), "NVDA", CUTOFF) == []
