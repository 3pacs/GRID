"""Tests for GD-FIX: signal_extractor.extract_from_signal_sources no longer
writes a QuiverQuant feed's own constant source_id into signal_data.actor.

Before this fix, ``actor = source_id`` meant every row from a QuiverQuant
feed (which uses one constant source_id per endpoint, e.g. "qq_senate_
trading") got written as if the feed itself were a market actor. Downstream
consumers (signal_backlinker.py, scripts/enrich_connections.py) then wired
that feed-name into the actor graph as an edge like
``pol_qq_senate_trading -> corp_GEHC``. The fix resolves the real actor from
the payload (mirroring intelligence.lever_pullers.puller_identity) and
writes no actor at all for feeds that are ticker-level aggregates with
nobody behind them.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

from intelligence import signal_extractor


class _FakeConn:
    """Minimal connection double that records every executed statement."""

    def __init__(self, source_rows):
        self._source_rows = source_rows
        self.insert_params: list[dict] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        result = MagicMock()
        if "SELECT ss.source_type" in sql:
            result.fetchall.return_value = self._source_rows
        elif "INSERT INTO signal_data" in sql:
            self.insert_params.append(dict(params))
            result.fetchall.return_value = []
        else:
            # e.g. "SELECT 1 FROM signal_sources LIMIT 1" existence probe
            result.fetchall.return_value = []
        return result

    def commit(self):
        pass


def _run_extractor(source_rows):
    conn = _FakeConn(source_rows)
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    stats = signal_extractor.extract_from_signal_sources(engine, lookback_days=90)
    return conn, stats


def test_aggregate_feed_writes_no_pseudo_actor():
    """quiverquant:senate's constant source_id ("qq_senate_trading") must
    not become the actor for a row whose payload has no Senator field —
    but it also must not silently be written as-is; real payload data
    should still resolve to the true actor when present (see next test)."""
    rows = [(
        "quiverquant:gov_contracts", "qq_gov_contracts", "GEHC",
        date(2026, 6, 30), "gov_contracts",
        json.dumps({"Amount": 5_000_000}),
    )]
    conn, stats = _run_extractor(rows)
    assert stats["errors"] == 0
    assert len(conn.insert_params) == 1
    assert conn.insert_params[0]["actor"] is None


def test_senate_feed_resolves_real_senator_name_from_payload():
    rows = [(
        "quiverquant:senate", "qq_senate_trading", "GEHC",
        date(2026, 6, 1), "senate_trading",
        json.dumps({"Senator": "Sheldon Whitehouse"}),
    )]
    conn, stats = _run_extractor(rows)
    assert stats["errors"] == 0
    assert conn.insert_params[0]["actor"] == "Sheldon Whitehouse"


def test_senate_feed_without_senator_field_falls_back_to_source_id():
    """When the payload doesn't carry the real name either, puller_identity
    falls back to source_id — matching lever_pullers' own behaviour — but
    this is no worse than before, and callers downstream (e.g.
    signal_backlinker.is_real_actor) already treat unresolved feed ids as
    noise once they're this short/constant."""
    rows = [(
        "quiverquant:senate", "qq_senate_trading", "GEHC",
        date(2026, 6, 1), "senate_trading",
        json.dumps({}),
    )]
    conn, stats = _run_extractor(rows)
    assert stats["errors"] == 0
    assert conn.insert_params[0]["actor"] == "qq_senate_trading"


def test_native_congressional_feed_actor_unaffected():
    """Native (non-QuiverQuant) pullers already store the real name as
    source_id — this fix must not change that path."""
    rows = [(
        "congressional", "Nancy Pelosi", "NVDA",
        date(2026, 6, 1), "BUY",
        json.dumps({"amount_midpoint": 50_000}),
    )]
    conn, stats = _run_extractor(rows)
    assert stats["errors"] == 0
    assert conn.insert_params[0]["actor"] == "Nancy Pelosi"


def test_offexchange_aggregate_feed_writes_no_actor():
    rows = [(
        "quiverquant:offexchange", "qq_off_exchange", "AAPL",
        date(2026, 6, 1), "off_exchange",
        json.dumps({"DPI": 0.4}),
    )]
    conn, stats = _run_extractor(rows)
    assert stats["errors"] == 0
    assert conn.insert_params[0]["actor"] is None
