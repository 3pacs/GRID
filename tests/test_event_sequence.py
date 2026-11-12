"""Tests for intelligence/event_sequence.py source pulls (D0 gap G6, #1129).

Runs the real SQL against an in-memory SQLite database with the columns the
pulls read. Covers the two G6 defects and the G7 compat-stub refusal:

* ``_pull_signal_source_events`` read ``r[8]`` from an 8-column row, so every
  row raised and every signal_sources event was dropped.
* ``earnings_events`` filings were stamped at ``period_end`` (the fiscal
  period covered, weeks before the filing existed) instead of ``filing_date``.
* ``earnings_calendar`` rows mirrored from ``earnings_events`` (earnings_date
  == SEC period_end, no EPS actual) are refused rather than read as releases.
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
    def test_rows_are_returned_not_dropped(self, engine):
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

        assert len(events) == 2
        by_type = {e.event_type: e for e in events}
        cong = by_type["congressional"]
        assert cong.amount_usd == 50000.0
        assert cong.direction == "bullish"
        assert cong.confidence == "confirmed"  # trust_score 0.85, not outcome text
        assert cong.actor == "Rep. Example"
        assert "purchase" in cong.description
        assert cong.timestamp.startswith(_d(10))

        dark = by_type["dark_pool"]
        assert dark.direction == "bearish"
        assert dark.amount_usd is None
        assert dark.confidence == "estimated"  # 0.3
        assert "[outcome: CORRECT]" in dark.description

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


class TestEarningsFilingTiming:
    def test_filing_is_stamped_at_filing_date_not_period_end(self, engine):
        _insert(engine, "earnings_events", ticker="NVDA", event_type="10-Q",
                filing_date=_d(20), period_end=_d(50), fiscal_quarter="Q3",
                summary="quarterly report", sentiment="positive", confidence=0.9)

        events = es._pull_earnings_events(engine, "NVDA", CUTOFF)

        assert len(events) == 1
        ev = events[0]
        assert ev.event_type == "earnings_filing"
        assert ev.timestamp.startswith(_d(20))
        assert f"period end {_d(50)}" in ev.description
        assert ev.direction == "bullish"

    def test_cutoff_applies_to_filing_date(self, engine):
        # Period ended before the window, but the filing landed inside it.
        _insert(engine, "earnings_events", ticker="NVDA", event_type="10-K",
                filing_date=_d(30), period_end=_d(120), fiscal_quarter="FY",
                summary=None, sentiment=None, confidence=None)
        # Filed before the window: excluded even though nothing else is.
        _insert(engine, "earnings_events", ticker="NVDA", event_type="8-K",
                filing_date=_d(100), period_end=_d(100), fiscal_quarter=None,
                summary=None, sentiment=None, confidence=None)

        events = es._pull_earnings_events(engine, "NVDA", CUTOFF)

        assert [e.timestamp[:10] for e in events] == [_d(30)]

    def test_compat_stub_is_refused(self, engine):
        # Stub mirrored by sync_earnings_events_to_calendar: earnings_date is
        # the SEC period_end and there is no EPS actual.
        _insert(engine, "earnings_events", ticker="NVDA", event_type="10-Q",
                filing_date=_d(20), period_end=_d(50), fiscal_quarter="Q3",
                summary=None, sentiment=None, confidence=None)
        _insert(engine, "earnings_calendar", ticker="NVDA", earnings_date=_d(50),
                fiscal_quarter="Q3", eps_estimate=None, eps_actual=None,
                eps_surprise_pct=None, revenue_estimate=None,
                revenue_actual=None, revenue_surprise_pct=None,
                classification=None, reported=True)

        events = es._pull_earnings_events(engine, "NVDA", CUTOFF)

        assert [e.source for e in events] == ["earnings_events"]
        assert all(not e.timestamp.startswith(_d(50)) for e in events)

    def test_real_calendar_release_is_kept_and_dedupes_same_day_filing(self, engine):
        _insert(engine, "earnings_calendar", ticker="NVDA", earnings_date=_d(20),
                fiscal_quarter="Q3", eps_estimate=1.0, eps_actual=1.2,
                eps_surprise_pct=20.0, revenue_estimate=3.0e10,
                revenue_actual=3.5e10, revenue_surprise_pct=16.0,
                classification="beat", reported=True)
        # 8-K filed the same day as the release: one event, not two.
        _insert(engine, "earnings_events", ticker="NVDA", event_type="8-K",
                filing_date=_d(20), period_end=_d(50), fiscal_quarter="Q3",
                summary=None, sentiment=None, confidence=None)

        events = es._pull_earnings_events(engine, "NVDA", CUTOFF)

        assert len(events) == 1
        assert events[0].source == "earnings_calendar"
        assert events[0].direction == "bullish"

    def test_calendar_row_with_actual_on_period_end_is_not_treated_as_stub(self, engine):
        # A populated EPS actual means a real puller wrote it; keep it.
        _insert(engine, "earnings_events", ticker="NVDA", event_type="10-Q",
                filing_date=_d(20), period_end=_d(50), fiscal_quarter="Q3",
                summary=None, sentiment=None, confidence=None)
        _insert(engine, "earnings_calendar", ticker="NVDA", earnings_date=_d(50),
                fiscal_quarter="Q3", eps_estimate=1.0, eps_actual=1.1,
                eps_surprise_pct=10.0, revenue_estimate=None,
                revenue_actual=None, revenue_surprise_pct=None,
                classification="beat", reported=True)

        events = es._pull_earnings_events(engine, "NVDA", CUTOFF)

        assert sorted(e.source for e in events) == ["earnings_calendar", "earnings_events"]
