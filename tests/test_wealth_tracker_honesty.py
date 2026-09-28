"""Tests for GD-FIX: wealth_tracker.py field-name honesty and wealth_flows
dedup-on-persist, plus the GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3
item 2 future-date guard.

Before the GD-FIX, ``track_wealth_migration`` read ``amount``/``market_value``
keys for 13F flows and ``amount``/``amount_low``/``amount_high`` keys for
congressional flows — none of which the actual writers
(ingestion/altdata/institutional_flows.py, ingestion/altdata/congressional.py)
ever set — so every 13F- and congressional-derived wealth flow silently
amounted to $0 while still being reported with a "confirmed"/"likely"
confidence.

Before the W3.3 fix, neither ``track_wealth_migration``'s signal_sources
reads nor ``persist_wealth_flows`` bounded ``signal_date``/``flow_date`` on
the upper end, so a future-dated row (16 found, max 2027-02-01) could reach
``wealth_flows``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from intelligence import wealth_tracker

sqlite3.register_adapter(date, lambda d: d.isoformat())


# ── _institutional_amount ────────────────────────────────────────────────

def test_institutional_amount_reads_value_usd():
    """value_usd is the key institutional_flows.py's NET_POSITION_DELTA
    signal actually writes."""
    assert wealth_tracker._institutional_amount({"value_usd": 12_500_000.0}) == 12_500_000.0


def test_institutional_amount_legacy_keys_still_work():
    assert wealth_tracker._institutional_amount({"amount": 500.0}) == 500.0
    assert wealth_tracker._institutional_amount({"market_value": 700.0}) == 700.0


def test_institutional_amount_missing_is_zero_not_fabricated():
    assert wealth_tracker._institutional_amount({}) == 0.0


# ── _congressional_amount ─────────────────────────────────────────────────

def test_congressional_amount_reads_amount_midpoint():
    """amount_midpoint is the key congressional.py's _emit_signal actually
    writes — there is no 'amount' key; Congress only discloses a range."""
    assert wealth_tracker._congressional_amount({"amount_midpoint": 8000.5}) == 8000.5


def test_congressional_amount_legacy_low_high_still_works():
    assert wealth_tracker._congressional_amount({"amount_low": 1_000, "amount_high": 15_000}) == 8_000.0


def test_congressional_amount_missing_is_zero_not_fabricated():
    assert wealth_tracker._congressional_amount({}) == 0.0


# ── persist_wealth_flows dedup (no unique constraint on wealth_flows) ───

class _FakeFlow:
    def __init__(self, from_actor, to_actor, amount, confidence, evidence, timestamp, implication):
        self.from_actor = from_actor
        self.to_actor = to_actor
        self.amount_estimate = amount
        self.confidence = confidence
        self.evidence = evidence
        self.timestamp = timestamp
        self.implication = implication


class _FakeConn:
    """Records INSERTs; the existence SELECT returns a hit for a
    pre-registered "already persisted" key, mimicking a duplicate re-run."""

    def __init__(self, existing_keys):
        self._existing_keys = existing_keys
        self.inserted: list[dict] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        result = MagicMock()
        if sql.strip().startswith("SELECT 1 FROM wealth_flows"):
            key = (params["from_actor"], params["to_entity"], params["amount"], params["impl"])
            result.fetchone.return_value = (1,) if key in self._existing_keys else None
        elif sql.strip().startswith("INSERT INTO wealth_flows"):
            self.inserted.append(dict(params))
            result.fetchone.return_value = None
        else:
            result.fetchone.return_value = None
        return result


def _engine_with(conn):
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def test_persist_wealth_flows_skips_exact_duplicate(monkeypatch):
    """GD-FIX: with no unique constraint on wealth_flows, a repeat run over
    the same lookback window must not re-insert the same disclosure."""
    monkeypatch.setattr(
        "intelligence.actor_network._ensure_tables", lambda engine: None
    )
    flow = _FakeFlow("ins_jane_doe", "corp_AAPL", 100_000.0, "confirmed", ["form4"], "2026-06-01", "Insider bought")
    existing_key = ("ins_jane_doe", "corp_AAPL", 100_000.0, "Insider bought")
    conn = _FakeConn(existing_keys={existing_key})
    engine = _engine_with(conn)

    count = wealth_tracker.persist_wealth_flows(engine, [flow])

    assert count == 0
    assert conn.inserted == []


def test_persist_wealth_flows_inserts_new_flow(monkeypatch):
    monkeypatch.setattr(
        "intelligence.actor_network._ensure_tables", lambda engine: None
    )
    flow = _FakeFlow("ins_jane_doe", "corp_AAPL", 100_000.0, "confirmed", ["form4"], "2026-06-01", "Insider bought")
    conn = _FakeConn(existing_keys=set())
    engine = _engine_with(conn)

    count = wealth_tracker.persist_wealth_flows(engine, [flow])

    assert count == 1
    assert len(conn.inserted) == 1
    assert conn.inserted[0]["from_actor"] == "ins_jane_doe"


# ── persist_wealth_flows future-date guard (GRID-WAVE3 §4.3 item 2) ────────

def test_persist_wealth_flows_rejects_future_flow_date(monkeypatch):
    """A flow whose timestamp resolves to a future flow_date must be
    skipped, never inserted — GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3
    item 2 found 16 wealth_flows rows dated as far out as 2027-02-01."""
    monkeypatch.setattr(
        "intelligence.actor_network._ensure_tables", lambda engine: None
    )
    future = (date.today() + timedelta(days=30)).isoformat()
    flow = _FakeFlow(
        "ins_jane_doe", "corp_AAPL", 100_000.0, "confirmed", ["form4"],
        future, "Insider bought",
    )
    conn = _FakeConn(existing_keys=set())
    engine = _engine_with(conn)

    count = wealth_tracker.persist_wealth_flows(engine, [flow])

    assert count == 0
    assert conn.inserted == []


def test_persist_wealth_flows_still_inserts_past_dated_flow(monkeypatch):
    """The future-date guard must not become a blanket break: a normal
    past-dated flow still persists."""
    monkeypatch.setattr(
        "intelligence.actor_network._ensure_tables", lambda engine: None
    )
    past = (date.today() - timedelta(days=1)).isoformat()
    flow = _FakeFlow(
        "ins_jane_doe", "corp_AAPL", 100_000.0, "confirmed", ["form4"],
        past, "Insider bought",
    )
    conn = _FakeConn(existing_keys=set())
    engine = _engine_with(conn)

    count = wealth_tracker.persist_wealth_flows(engine, [flow])

    assert count == 1
    assert len(conn.inserted) == 1


# ── track_wealth_migration future-date guard (real SQLite, not mocked) ─────

@pytest.fixture()
def signal_sources_engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE signal_sources (source_type TEXT, source_id TEXT, "
            "ticker TEXT, signal_date DATE, signal_type TEXT, "
            "signal_value TEXT, trust_score REAL)"
        ))
    return eng


def _insert_signal_source(engine, source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score=0.9):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                "VALUES (:st, :sid, :t, :d, :typ, :v, :ts)"
            ),
            {
                "st": source_type, "sid": source_id, "t": ticker, "d": signal_date,
                "typ": signal_type, "v": json.dumps(signal_value), "ts": trust_score,
            },
        )


def test_track_wealth_migration_excludes_future_signal_date(monkeypatch, signal_sources_engine):
    """GRID-WAVE3 §4.3 item 2: a future-dated congressional/institutional/
    insider/darkpool signal_source row must never surface as a wealth flow."""
    monkeypatch.setattr(
        "intelligence.actor_network._ensure_tables", lambda engine: None
    )
    past = date.today() - timedelta(days=1)
    future = date.today() + timedelta(days=30)

    _insert_signal_source(
        signal_sources_engine, "congressional", "rep-x", "GRDX", past, "BUY",
        {"amount_midpoint": 50_000},
    )
    _insert_signal_source(
        signal_sources_engine, "congressional", "rep-y", "GRDY", future, "BUY",
        {"amount_midpoint": 999_000},
    )

    flows = wealth_tracker.track_wealth_migration(signal_sources_engine, days=90)

    tickers = {f.to_actor for f in flows}
    assert "GRDX" in tickers, "past-dated row should still surface"
    assert "GRDY" not in tickers, "future-dated row must never surface"
