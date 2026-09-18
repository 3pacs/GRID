"""Honesty regression tests for trust scores and convergence — audit C-M22.

The 2026-09-17 fake-data audit found a fabricated ``0.5`` reaching users on
``GET /api/v1/watchlist/{ticker}/edge`` by three separate routes, all of which
originate above the router:

* ``schema.sql``'s ``signal_sources.trust_score NUMERIC DEFAULT 0.5`` — every
  ingested row started life at a midpoint nobody measured (dropped by
  ``migrations/versions/signal_sources_trust_nodefault.py``).
* ``intelligence/trust_scorer.py``'s ``float(r[N]) if r[N] else 0.5`` — which
  additionally rewrote a *measured* ``0.0`` into ``0.5``.
* ``detect_convergence``'s ``combined_confidence`` — the mean of those
  defaults, so a genuine 3-source convergence reported exactly ``0.5`` and
  reached a **high-severity** WebSocket alert.

Two mislabels on the same response object are covered here too: the router read
``best["direction"]`` while ``detect_convergence`` only ever emitted
``signal_type``, so ``convergence.direction`` was permanently ``"neutral"`` and
the edge summary printed "N independent sources neutral."

Vocabulary: docs/reference/CONFIDENCE_POLICY.md and
docs/reference/AVAILABILITY_CONTRACT.md — null plus a basis, never a midpoint,
and ``0.0`` is a measurement.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from api.auth import create_token
from api.dependencies import get_db_engine
from api.main import app
from intelligence.trust_scorer import (
    CONVERGENCE_HIGH_SEVERITY_TRUST,
    _resolve_convergence_direction,
    detect_convergence,
    get_insider_edge,
)

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


# ══════════════════════════════════════════════════════════════════════
#  Engine doubles
# ══════════════════════════════════════════════════════════════════════


def _engine(route):
    """Mock engine whose ``execute`` is dispatched by ``route(sql) -> rows``."""
    def _execute(statement, *args, **kwargs):
        result = MagicMock()
        result.fetchall.return_value = list(route(str(statement)) or [])
        result.fetchone.return_value = None
        result.scalar.return_value = None
        return result

    conn = MagicMock()
    conn.execute.side_effect = _execute
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _edge_row(source_id, signal_type, trust_score, metadata=None):
    """A ``signal_sources`` row shaped for ``get_insider_edge``.

    SELECT source_id, signal_type, signal_date, signal_value,
           trust_score, outcome, outcome_return, metadata
    """
    return (
        source_id, signal_type, "2026-09-15", None,
        trust_score, "PENDING", None, metadata or {},
    )


def _insider_edge_engine(congressional=(), insider=(), darkpool=()):
    def route(sql):
        if "source_type = 'congressional'" in sql:
            return congressional
        if "source_type = 'insider'" in sql:
            return insider
        if "source_type = 'darkpool'" in sql:
            return darkpool
        return []
    return _engine(route)


def _conv_row(source_type, source_id, signal_type, trust_score, signal_value=None):
    """A ``signal_sources`` row shaped for ``detect_convergence``.

    SELECT ticker, source_type, source_id, signal_type,
           signal_date, trust_score, signal_value
    """
    return ("NVDA", source_type, source_id, signal_type,
            "2026-09-15", trust_score, signal_value)


def _convergence_engine(rows):
    return _engine(lambda sql: rows if "FROM signal_sources" in sql else [])


_THREE_AGREEING = [
    ("congressional", "Rep. A"),
    ("insider", "Jane Doe"),
    ("darkpool", "venue-7"),
]


def _three_sources(trust_scores, signal_type="BUY"):
    return [
        _conv_row(st, sid, signal_type, trust)
        for (st, sid), trust in zip(_THREE_AGREEING, trust_scores)
    ]


# ══════════════════════════════════════════════════════════════════════
#  1. get_insider_edge — a measured 0.0 is a measurement
# ══════════════════════════════════════════════════════════════════════


class TestInsiderEdgeTrustScores:
    def test_measured_zero_survives_as_zero(self):
        """``float(x) if x else 0.5`` rewrote a scored 0.0 into a 0.5."""
        engine = _insider_edge_engine(
            congressional=[_edge_row("Rep. A", "BUY", 0.0)],
        )
        edge = get_insider_edge(engine, "NVDA")
        assert edge is not None
        assert edge["congressional"][0]["trust_score"] == 0.0

    def test_null_trust_score_is_none_not_a_midpoint(self):
        engine = _insider_edge_engine(
            congressional=[_edge_row("Rep. A", "BUY", None)],
            insider=[_edge_row("Jane Doe", "SELL", None)],
            darkpool=[_edge_row("venue-7", "BUY", None)],
        )
        edge = get_insider_edge(engine, "NVDA")
        assert edge is not None
        for category in ("congressional", "insider", "darkpool"):
            assert edge[category][0]["trust_score"] is None

    def test_scored_rows_pass_through(self):
        engine = _insider_edge_engine(
            congressional=[_edge_row("Rep. A", "BUY", 0.82)],
        )
        edge = get_insider_edge(engine, "NVDA")
        assert edge["congressional"][0]["trust_score"] == 0.82

    def test_unscored_rows_vote_unweighted_and_say_so(self):
        """No trust anywhere: a head count, labelled, not a 0.5-weighted one."""
        engine = _insider_edge_engine(
            congressional=[_edge_row("Rep. A", "BUY", None)],
            insider=[_edge_row("Jane Doe", "BUY", None)],
            darkpool=[_edge_row("venue-7", "SELL", None)],
        )
        edge = get_insider_edge(engine, "NVDA")
        assert edge["net_signal_type"] == "BUY"
        assert edge["signal_type_confidence_basis"] == "unweighted_vote_count"
        assert edge["scored_signal_count"] == 0
        assert edge["signal_count"] == 3

    def test_scored_rows_are_trust_weighted_and_say_so(self):
        engine = _insider_edge_engine(
            congressional=[_edge_row("Rep. A", "BUY", 0.9)],
            insider=[_edge_row("Jane Doe", "SELL", 0.1)],
        )
        edge = get_insider_edge(engine, "NVDA")
        assert edge["net_signal_type"] == "BUY"
        assert edge["signal_type_confidence"] == 0.9
        assert edge["signal_type_confidence_basis"] == "trust_weighted_over_scored_sources"
        assert edge["scored_signal_count"] == 2

    def test_a_tie_is_not_a_sell(self):
        """``"BUY" if buy > sell else "SELL"`` labelled every tie a SELL."""
        engine = _insider_edge_engine(
            congressional=[_edge_row("Rep. A", "BUY", 0.5)],
            insider=[_edge_row("Jane Doe", "SELL", 0.5)],
        )
        edge = get_insider_edge(engine, "NVDA")
        assert edge["net_signal_type"] is None
        assert edge["signal_type_confidence"] is None
        assert edge["signal_type_confidence_basis"].endswith("_tied")


# ══════════════════════════════════════════════════════════════════════
#  2. detect_convergence — confidence over scored sources only
# ══════════════════════════════════════════════════════════════════════


class TestConvergenceConfidence:
    def test_unscored_convergence_reports_null_not_half(self):
        """The headline case: 3 real sources, no trust scores, no number."""
        engine = _convergence_engine(_three_sources([None, None, None]))
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine, ticker="NVDA")
        assert len(events) == 1
        ev = events[0]
        assert ev["source_count"] == 3
        assert ev["scored_source_count"] == 0
        assert ev["combined_confidence"] is None
        assert ev["confidence_basis"] == "unscored"

    def test_measured_zero_trust_is_a_measurement(self):
        engine = _convergence_engine(_three_sources([0.0, 0.0, 0.0]))
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine, ticker="NVDA")
        ev = events[0]
        assert ev["combined_confidence"] == 0.0
        assert ev["scored_source_count"] == 3
        assert ev["confidence_basis"] == "mean_trust_of_scored_sources"

    def test_mixed_scoring_averages_the_scored_sources_only(self):
        """0.8 and 0.6 scored, one unscored -> 0.7, not (0.8+0.6+0.5)/3."""
        engine = _convergence_engine(_three_sources([0.8, 0.6, None]))
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine, ticker="NVDA")
        ev = events[0]
        assert ev["combined_confidence"] == pytest.approx(0.7)
        assert ev["source_count"] == 3
        assert ev["scored_source_count"] == 2
        assert ev["confidence_basis"] == "mean_trust_of_scored_sources"

    def test_per_source_trust_scores_are_nullable(self):
        engine = _convergence_engine(_three_sources([0.8, None, 0.0]))
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine, ticker="NVDA")
        trusts = sorted(
            (s["trust_score"] for s in events[0]["sources"]),
            key=lambda v: (v is None, v),
        )
        assert trusts == [0.0, 0.8, None]

    def test_unscored_events_sort_last(self):
        rows = _three_sources([None, None, None])
        rows += [
            ("AMD", st, sid, "BUY", "2026-09-15", 0.4, None)
            for st, sid in _THREE_AGREEING
        ]
        engine = _convergence_engine(rows)
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine)
        assert [e["ticker"] for e in events] == ["AMD", "NVDA"]
        assert events[-1]["combined_confidence"] is None


# ══════════════════════════════════════════════════════════════════════
#  3. Direction resolution
# ══════════════════════════════════════════════════════════════════════


class TestConvergenceDirection:
    def test_buy_convergence_is_bullish_not_neutral(self):
        engine = _convergence_engine(_three_sources([None, None, None], "BUY"))
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine, ticker="NVDA")
        ev = events[0]
        assert ev["direction"] == "bullish"
        assert ev["direction_basis"] == "inferred_from_signal_types"
        assert ev["signal_type"] == "BUY"

    def test_sell_convergence_is_bearish(self):
        engine = _convergence_engine(_three_sources([None, None, None], "SELL"))
        with patch("api.main.broadcast_event"):
            events = detect_convergence(engine, ticker="NVDA")
        assert events[0]["direction"] == "bearish"

    def test_disagreeing_sources_resolve_to_none_never_neutral(self):
        direction, basis = _resolve_convergence_direction([
            {"signal_type": "insider_buy", "signal_value": None},
            {"signal_type": "insider_sell", "signal_value": None},
            {"signal_type": "CLUSTER_BUY", "signal_value": None},
        ])
        assert direction is None
        assert basis == "sources_disagree"

    def test_sources_with_no_direction_resolve_to_none(self):
        direction, basis = _resolve_convergence_direction([
            {"signal_type": "UNUSUAL_VOLUME", "signal_value": None},
            {"signal_type": "HEAT_SPIKE", "signal_value": None},
        ])
        assert direction is None
        assert basis == "no_direction_on_sources"

    def test_partial_resolution_is_labelled(self):
        direction, basis = _resolve_convergence_direction([
            {"signal_type": "BUY", "signal_value": None},
            {"signal_type": "UNUSUAL_VOLUME", "signal_value": None},
        ])
        assert direction == "bullish"
        assert basis == "inferred_from_signal_types_partial"

    def test_direction_is_never_the_string_neutral(self):
        for signals in (
            [{"signal_type": "BUY"}],
            [{"signal_type": "SELL"}],
            [{"signal_type": "UNUSUAL_VOLUME"}],
            [{"signal_type": "BUY"}, {"signal_type": "SELL"}],
        ):
            direction, _ = _resolve_convergence_direction(signals)
            assert direction != "neutral"


# ══════════════════════════════════════════════════════════════════════
#  4. The WebSocket alert must not be driven by a fabricated confidence
# ══════════════════════════════════════════════════════════════════════


def _broadcast_payload(trust_scores):
    engine = _convergence_engine(_three_sources(trust_scores))
    with patch("api.main.broadcast_event") as bcast:
        detect_convergence(engine, ticker="NVDA")
    assert bcast.call_count == 1
    event_type, payload = bcast.call_args[0]
    assert event_type == "alert"
    return payload


class TestConvergenceAlert:
    def test_unscored_convergence_is_not_high_severity(self):
        """The audit's worst reach: a fabricated 0.5 behind ``severity: high``."""
        payload = _broadcast_payload([None, None, None])
        assert payload["combined_confidence"] is None
        assert payload["confidence_basis"] == "unscored"
        assert payload["scored_source_count"] == 0
        assert payload["source_count"] == 3
        assert payload["severity"] == "medium"
        assert payload["severity_basis"] == "source_count_only_unscored"

    def test_alert_carries_no_fabricated_number(self):
        payload = _broadcast_payload([None, None, None])
        assert 0.5 not in [v for v in payload.values() if isinstance(v, float)]

    def test_high_severity_needs_a_measured_confidence_above_threshold(self):
        high = CONVERGENCE_HIGH_SEVERITY_TRUST + 0.1
        payload = _broadcast_payload([high, high, high])
        assert payload["severity"] == "high"
        assert payload["combined_confidence"] == pytest.approx(high)
        assert payload["severity_basis"].startswith("mean_trust_of_scored_sources>=")

    def test_measured_but_low_confidence_downgrades_to_medium(self):
        payload = _broadcast_payload([0.2, 0.2, 0.2])
        assert payload["severity"] == "medium"
        assert payload["severity_basis"].startswith("mean_trust_of_scored_sources<")

    def test_measured_zero_is_not_treated_as_unscored(self):
        payload = _broadcast_payload([0.0, 0.0, 0.0])
        assert payload["combined_confidence"] == 0.0
        assert payload["confidence_basis"] == "mean_trust_of_scored_sources"
        assert payload["severity_basis"].startswith("mean_trust_of_scored_sources<")

    def test_alert_carries_the_resolved_direction(self):
        payload = _broadcast_payload([None, None, None])
        assert payload["direction"] == "bullish"
        assert payload["direction_basis"] == "inferred_from_signal_types"


# ══════════════════════════════════════════════════════════════════════
#  5. GET /api/v1/watchlist/{ticker}/edge
# ══════════════════════════════════════════════════════════════════════


def _router_engine(social_rows=()):
    def route(sql):
        if "source_type = 'social'" in sql:
            return social_rows
        return []
    return _engine(route)


def _get_edge(edge_data, conv_events, social_rows=(), conv_exc=None):
    engine = _router_engine(social_rows)
    app.dependency_overrides[get_db_engine] = lambda: engine
    conv = (
        MagicMock(side_effect=conv_exc) if conv_exc is not None
        else MagicMock(return_value=conv_events)
    )
    try:
        with patch("intelligence.trust_scorer.get_insider_edge", return_value=edge_data), \
                patch("intelligence.trust_scorer.detect_convergence", conv):
            response = client.get("/api/v1/watchlist/NVDA/edge", headers=_auth_header())
    finally:
        app.dependency_overrides.pop(get_db_engine, None)
    assert response.status_code == 200, response.text
    return response.json()


_NO_EDGE = {"congressional": [], "insider": [], "darkpool": []}


def _conv_event(**overrides):
    event = {
        "ticker": "NVDA",
        "signal_type": "BUY",
        "direction": "bullish",
        "direction_basis": "inferred_from_signal_types",
        "source_count": 3,
        "scored_source_count": 0,
        "sources": [],
        "combined_confidence": None,
        "confidence_basis": "unscored",
        "detected_at": "2026-09-17T00:00:00+00:00",
    }
    event.update(overrides)
    return event


class TestEdgeConvergenceObject:
    def test_no_convergence_event_is_not_a_half_confident_neutral(self):
        data = _get_edge(_NO_EDGE, [])
        conv = data["convergence"]
        assert conv["direction"] is None
        assert conv["source_count"] == 0
        assert conv["confidence"] is None
        assert conv["status"] == "none"

    def test_failed_detection_is_unavailable_with_a_reason(self):
        data = _get_edge(_NO_EDGE, None, conv_exc=RuntimeError("relation missing"))
        conv = data["convergence"]
        assert conv["status"] == "unavailable"
        assert conv["reason"]
        assert not any(ch.isdigit() for ch in conv["reason"])
        assert conv["confidence"] is None
        assert conv["direction"] is None

    def test_unscored_convergence_serves_null_confidence(self):
        data = _get_edge(_NO_EDGE, [_conv_event()])
        conv = data["convergence"]
        assert conv["source_count"] == 3
        assert conv["scored_source_count"] == 0
        assert conv["confidence"] is None
        assert conv["confidence_basis"] == "unscored"
        assert conv["status"] == "detected"

    def test_scored_convergence_rounds_the_real_number(self):
        data = _get_edge(_NO_EDGE, [_conv_event(
            combined_confidence=0.7133,
            scored_source_count=3,
            confidence_basis="mean_trust_of_scored_sources",
        )])
        assert data["convergence"]["confidence"] == 0.71

    def test_measured_zero_confidence_is_not_dropped(self):
        data = _get_edge(_NO_EDGE, [_conv_event(
            combined_confidence=0.0,
            scored_source_count=3,
            confidence_basis="mean_trust_of_scored_sources",
        )])
        assert data["convergence"]["confidence"] == 0.0

    def test_direction_comes_from_the_helper_not_a_default(self):
        data = _get_edge(_NO_EDGE, [_conv_event(direction="bearish")])
        assert data["convergence"]["direction"] == "bearish"


class TestEdgeSummaryWording:
    def test_unresolved_direction_is_not_printed_as_neutral(self):
        data = _get_edge(_NO_EDGE, [_conv_event(
            direction=None, direction_basis="sources_disagree",
        )])
        assert "3 independent sources, direction unresolved." in data["edge_summary"]
        assert "neutral" not in data["edge_summary"].lower()

    def test_resolved_direction_is_printed(self):
        data = _get_edge(_NO_EDGE, [_conv_event(direction="bullish")])
        assert "3 independent sources bullish." in data["edge_summary"]

    def test_no_convergence_says_limited_signals(self):
        data = _get_edge(_NO_EDGE, [])
        assert "Limited intelligence signals." in data["edge_summary"]
        assert "neutral" not in data["edge_summary"].lower()


class TestEdgeRowTrustScores:
    def test_congressional_trust_score_is_null_when_unscored(self):
        data = _get_edge({
            "congressional": [{
                "member": "Rep. A", "direction": "BUY", "date": "2026-09-15",
                "trust_score": None, "metadata": {},
            }],
            "insider": [], "darkpool": [],
        }, [])
        assert data["congressional"][0]["trust_score"] is None

    def test_congressional_measured_zero_survives(self):
        data = _get_edge({
            "congressional": [{
                "member": "Rep. A", "direction": "BUY", "date": "2026-09-15",
                "trust_score": 0.0, "metadata": {},
            }],
            "insider": [], "darkpool": [],
        }, [])
        assert data["congressional"][0]["trust_score"] == 0.0

    def test_smart_money_trust_score_is_null_when_unscored(self):
        # SELECT source_id, direction, signal_date, trust_score, metadata
        data = _get_edge(
            _NO_EDGE, [],
            social_rows=[("@whale", "BUY", "2026-09-15", None, {"platform": "x"})],
        )
        assert data["smart_money"][0]["trust_score"] is None

    def test_smart_money_measured_zero_survives(self):
        data = _get_edge(
            _NO_EDGE, [],
            social_rows=[("@whale", "BUY", "2026-09-15", 0.0, {"platform": "x"})],
        )
        assert data["smart_money"][0]["trust_score"] == 0.0

    def test_no_half_literal_anywhere_in_an_unscored_response(self):
        data = _get_edge(
            {
                "congressional": [{
                    "member": "Rep. A", "direction": "BUY",
                    "date": "2026-09-15", "trust_score": None, "metadata": {},
                }],
                "insider": [], "darkpool": [],
            },
            [_conv_event()],
            social_rows=[("@whale", "BUY", "2026-09-15", None, {"platform": "x"})],
        )
        import json
        assert "0.5" not in json.dumps(data)
