"""Tests for GD-FIX: wealth_tracker.py field-name honesty and wealth_flows
dedup-on-persist.

Before this fix, ``track_wealth_migration`` read ``amount``/``market_value``
keys for 13F flows and ``amount``/``amount_low``/``amount_high`` keys for
congressional flows — none of which the actual writers
(ingestion/altdata/institutional_flows.py, ingestion/altdata/congressional.py)
ever set — so every 13F- and congressional-derived wealth flow silently
amounted to $0 while still being reported with a "confirmed"/"likely"
confidence.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from intelligence import wealth_tracker


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
