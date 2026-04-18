"""Tests for the v2 trust scorer signal types.

Covers the ``TrustScorer`` class wrapper and the two new signal types:
``sec_filing`` (+0.15 absolute boost, 90d half-life) and
``chokepoint_crossing`` (-0.10 absolute penalty, 30d half-life).

These tests mock the database at the connection level so they never
hit a real Postgres instance. The contract under test is purely:

    1. TrustScorer._score_sec_filing emits a positive trust_delta.
    2. TrustScorer._score_chokepoint_crossing emits a negative trust_delta.
    3. Recency half-life cuts weight in half at the half-life mark.
    4. Missing data returns an empty list / None trust score — no errors.
    5. get_trust_score aggregates across signal types.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from intelligence.trust_scorer import (
    CHOKEPOINT_CROSSING_MIN_DELTA,
    EVALUATION_WINDOWS,
    SIGNAL_HALF_LIFE_DAYS,
    SIGNAL_TRUST_DELTA,
    SIGNAL_WINDOWS,
    TrustScorer,
)


# ── Fixture helpers ───────────────────────────────────────────────────────


def _make_engine_with_rows(
    sec_rows=None,
    choke_rows=None,
    classical_rows=None,
):
    """Build a MagicMock Engine whose ``execute()`` returns the correct
    row-set for each query based on the SQL text.

    The scorer passes a SQLAlchemy ``text()`` clause; we inspect its
    string form to route to the matching rows.
    """
    engine = MagicMock()

    sec_rows = sec_rows or []
    choke_rows = choke_rows or []
    classical_rows = classical_rows or []

    def execute(stmt, params=None):
        sql = str(stmt).lower()
        result = MagicMock()
        if "capital_flows" in sql:
            result.fetchall.return_value = sec_rows
        elif "supply_chain_edges" in sql:
            result.fetchall.return_value = choke_rows
        elif "signal_sources" in sql:
            result.fetchall.return_value = classical_rows
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    conn = MagicMock()
    conn.execute.side_effect = execute
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    engine.connect.return_value = cm
    return engine


def _sec_row(days_ago: int = 1, form: str = "10-K"):
    as_of = datetime.now(timezone.utc) - timedelta(days=days_ago)
    # columns: actor_id, counterparty_id, flow_type, direction, amount_usd,
    #          source_filing, confidence, as_of, fiscal_period, period_type
    return (
        "AAPL", None, "capex", "outflow", 1_000_000_000.0,
        form, "confirmed", as_of, date(2025, 12, 31), "annual",
    )


def _choke_row(days_ago: int = 1, score: float = 0.85):
    as_of = datetime.now(timezone.utc) - timedelta(days=days_ago)
    # columns: id, upstream_id, downstream_id, input_type, chokepoint_score,
    #          as_of, confidence, upstream_flag, downstream_flag
    return (
        42, "tsmc_taiwan", "AAPL", "cowos_packaging", score,
        as_of, "derived", True, False,
    )


def _classical_row(days_ago: int = 5, trust: float = 0.72):
    sig_date = datetime.now(timezone.utc) - timedelta(days=days_ago)
    # columns: source_type, source_id, signal_type, signal_date,
    #          trust_score, outcome
    return ("congressional", "pelosi", "BUY", sig_date, trust, "CORRECT")


# ── Tests ─────────────────────────────────────────────────────────────────


def test_signal_windows_has_new_types():
    """The two new signal types are registered with the correct windows."""
    assert EVALUATION_WINDOWS["sec_filing"] == 90
    assert EVALUATION_WINDOWS["chokepoint_crossing"] == 30
    assert SIGNAL_WINDOWS is EVALUATION_WINDOWS  # alias check
    assert SIGNAL_TRUST_DELTA["sec_filing"] == 0.15
    assert SIGNAL_TRUST_DELTA["chokepoint_crossing"] == -0.10
    assert SIGNAL_HALF_LIFE_DAYS["sec_filing"] == 90
    assert SIGNAL_HALF_LIFE_DAYS["chokepoint_crossing"] == 30
    assert CHOKEPOINT_CROSSING_MIN_DELTA == 0.15


def test_sec_filing_signal_boosts_trust():
    """A recent 10-K should produce a positive trust_delta."""
    engine = _make_engine_with_rows(sec_rows=[_sec_row(days_ago=1, form="10-K")])
    ts = TrustScorer(engine)

    signals = ts._score_sec_filing("AAPL")

    assert len(signals) == 1
    sig = signals[0]
    assert sig["signal_type"] == "sec_filing"
    assert sig["trust_delta"] > 0
    assert sig["trust_delta"] <= SIGNAL_TRUST_DELTA["sec_filing"]
    assert sig["confidence"] == "confirmed"
    assert sig["metadata"]["source_filing"] == "10-K"


def test_chokepoint_crossing_signal_decreases_trust():
    """A fresh chokepoint crossing produces a negative trust_delta."""
    engine = _make_engine_with_rows(
        choke_rows=[_choke_row(days_ago=2, score=0.85)],
    )
    ts = TrustScorer(engine)

    signals = ts._score_chokepoint_crossing("AAPL")

    assert len(signals) == 1
    sig = signals[0]
    assert sig["signal_type"] == "chokepoint_crossing"
    assert sig["trust_delta"] < 0
    assert sig["metadata"]["chokepoint_score"] == pytest.approx(0.85)
    assert sig["metadata"]["input_type"] == "cowos_packaging"
    assert sig["direction"] == "bearish"


def test_recency_half_life_cuts_weight():
    """At one half-life old, weight should be ~0.5 for both signal types."""
    # SEC filing 90 days old vs 0 days — 90 day half-life.
    engine_sec = _make_engine_with_rows(
        sec_rows=[_sec_row(days_ago=90, form="10-K")],
    )
    sec_signals = TrustScorer(engine_sec)._score_sec_filing("AAPL")
    assert sec_signals[0]["weight"] == pytest.approx(0.5, abs=0.02)

    # Chokepoint crossing 30 days old — 30 day half-life.
    engine_choke = _make_engine_with_rows(
        choke_rows=[_choke_row(days_ago=30, score=0.80)],
    )
    choke_signals = TrustScorer(engine_choke)._score_chokepoint_crossing("AAPL")
    assert choke_signals[0]["weight"] == pytest.approx(0.5, abs=0.02)

    # And the deltas scale with weight.
    fresh_engine = _make_engine_with_rows(
        sec_rows=[_sec_row(days_ago=0, form="10-K")],
    )
    fresh_sig = TrustScorer(fresh_engine)._score_sec_filing("AAPL")[0]
    assert fresh_sig["trust_delta"] > sec_signals[0]["trust_delta"]


def test_missing_data_returns_gracefully():
    """No rows and DB errors should never raise — return [] / None."""
    # Empty DB path.
    empty_engine = _make_engine_with_rows()
    ts_empty = TrustScorer(empty_engine)
    assert ts_empty._score_sec_filing("AAPL") == []
    assert ts_empty._score_chokepoint_crossing("AAPL") == []
    # Empty inputs are tolerated too.
    assert ts_empty._score_sec_filing("") == []
    assert ts_empty._score_chokepoint_crossing(None) == []
    assert ts_empty.get_trust_score("AAPL") is None
    assert ts_empty.get_recent_signals("") == []

    # DB errors fall through to [].
    crash_engine = MagicMock()
    crash_engine.connect.side_effect = RuntimeError("db down")
    ts_crash = TrustScorer(crash_engine)
    assert ts_crash._score_sec_filing("AAPL") == []
    assert ts_crash._score_chokepoint_crossing("AAPL") == []
    # get_trust_score with no data returns None, not an exception.
    assert ts_crash.get_trust_score("AAPL") is None


def test_aggregation_across_signal_types():
    """get_trust_score aggregates classical + sec + chokepoint correctly."""
    # Classical trust of 0.72 + fresh SEC (+~0.15) + fresh chokepoint (~-0.01
    # to -0.02 since delta * severity * weight is small). The result should
    # be strictly greater than 0.72 because the SEC boost dominates the
    # chokepoint penalty.
    engine = _make_engine_with_rows(
        classical_rows=[_classical_row(days_ago=0, trust=0.72)],
        sec_rows=[_sec_row(days_ago=0, form="10-K")],
        choke_rows=[_choke_row(days_ago=0, score=0.50)],
    )
    ts = TrustScorer(engine)
    score = ts.get_trust_score("AAPL")
    assert score is not None
    assert 0.0 <= score <= 1.0
    # SEC boost dominates small chokepoint penalty on a 0.72 baseline.
    assert score > 0.72

    # Unified recent signals list should contain all three types.
    engine2 = _make_engine_with_rows(
        classical_rows=[_classical_row()],
        sec_rows=[_sec_row()],
        choke_rows=[_choke_row()],
    )
    ts2 = TrustScorer(engine2)
    signals = ts2.get_recent_signals("AAPL")
    types = {s["signal_type"] for s in signals}
    assert "sec_filing" in types
    assert "chokepoint_crossing" in types
    assert "congressional" in types
