"""Tests for ``intelligence/trump_proximity.py`` — Phase 0 TPS v0.

Covers:
  * Happy path: every layer returns data → score in (0, 100].
  * NULL propagation: every layer returns None → aggregate score is None.
  * Mixed: one layer fires, four return None → score is computed from
    the one that fired (no silent 1.0 defaults).
  * Ticker with literally no evidence → score None and empty evidence.
  * Persistence: persist_snapshot calls the upsert SQL with JSONB casts.
  * Precision@10 hook: ``LAYER_WEIGHTS`` exposes all five layers so the
    precision-tuning harness can iterate without code changes.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pytest

from intelligence.trump_proximity import (
    LAYER_WEIGHTS,
    TPSResult,
    _bfs_to_admin,
    _is_admin_aligned_registrant,
    compute_tps_for_ticker,
    persist_snapshot,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _engine_with_results(by_query: list[list[tuple]]) -> MagicMock:
    """Build a mock engine whose successive ``conn.execute().fetchall()``
    calls return the supplied tuples (one list per SQL invocation).

    Also handles ``fetchone()`` for the actor-graph BFS layer.
    """
    engine = MagicMock()
    conn = MagicMock()

    call_index = {"i": 0}

    def _execute(sql, params=None):  # noqa: ARG001 — sql ignored
        idx = call_index["i"]
        rows = by_query[idx] if idx < len(by_query) else []
        call_index["i"] = idx + 1
        result = MagicMock()
        result.fetchall.return_value = rows
        # fetchone returns first row tuple or None
        result.fetchone.return_value = rows[0] if rows else None
        return result

    conn.execute.side_effect = _execute
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = False
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False
    return engine


# ── Happy path ─────────────────────────────────────────────────────────


def test_compute_tps_happy_path_all_layers_score():
    """All five layers return non-empty rows → score is between 0 and 100."""
    # Order of layer queries matches compute_tps_for_ticker:
    #   1) gov_contract signal_sources
    #   2) lobbying signal_sources
    #   3) congressional_trades
    #   4) FARA signal_sources
    #   5a) actor lookup (issuer resolve) — returns single row tuple
    #   5b) BFS frontier lookup
    queries = [
        # Direct contracts: $500M (half of $1B ref → 0.5)
        [(json.dumps({"amount": 500_000_000, "description": "DoD award"}), date(2026, 4, 1), "usaspending")],
        # Lobbying: $2.5M (half of $5M ref → 0.5)
        [(json.dumps({"amount": 2_500_000, "issue": "defense appropriations"}), date(2026, 5, 1), "senate_lda")],
        # Congressional: $300K net buy
        [("Rep Smith", "R", "BUY", 300_000.0, date(2026, 5, 10))],
        # FARA: 1 admin-aligned edge
        [
            (
                json.dumps({
                    "registrant_name": "Ballard Partners",
                    "country": "SAUDI ARABIA",
                    "activity_type": "LOBBYING",
                    "compensation": 500_000,
                }),
                date(2026, 4, 15),
                "fara_efile",
            )
        ],
        # Actor-graph issuer lookup
        [("issuer_lmt",)],
        # BFS frontier hop 1 — issuer has connection to admin-aligned actor
        [
            (
                "issuer_lmt",
                json.dumps([{"actor": "donor_admin"}]),
                None,
                None,
            ),
            (
                "donor_admin",
                json.dumps([]),
                None,
                json.dumps({"political_connections": "Trump-aligned"}),
            ),
        ],
    ]
    engine = _engine_with_results(queries)

    result = compute_tps_for_ticker(engine, "LMT", date(2026, 5, 16))

    assert result.score is not None
    assert 0 < result.score <= 100
    assert all(result.coverage.values())  # every layer fired
    assert result.layer_scores["direct_contracts"] == pytest.approx(0.5)
    assert result.layer_scores["lobbying_admin"] == pytest.approx(0.5)
    # Evidence list pulls from each layer
    layers_seen = {e.layer for e in result.evidence}
    assert {"direct_contracts", "lobbying_admin", "congressional_30d", "fara_admin"} <= layers_seen


# ── NULL propagation ───────────────────────────────────────────────────


def test_compute_tps_all_layers_missing_returns_null_score():
    """If every layer returns None the aggregate score MUST be None.

    Critical contract — see section 7 of the pivot doc.
    """
    queries = [[], [], [], [], [], []]
    engine = _engine_with_results(queries)

    result = compute_tps_for_ticker(engine, "NEVERHEARDOF", date(2026, 5, 16))

    assert result.score is None
    assert all(v is None for v in result.layer_scores.values())
    assert all(v is False for v in result.coverage.values())
    assert result.evidence == []


def test_compute_tps_one_layer_fires_others_null_no_silent_default():
    """Mixed coverage: only the contracts layer fires.

    The aggregate score must be computed from that one layer's normalised
    value — NOT diluted by silent ``1.0`` defaults on the missing four.
    """
    queries = [
        # contracts: $1B (caps at 1.0)
        [(json.dumps({"amount": 1_500_000_000}), date(2026, 5, 1), "usaspending")],
        [],  # lobbying empty
        [],  # congressional empty
        [],  # fara empty
        [],  # actor lookup empty
    ]
    engine = _engine_with_results(queries)

    result = compute_tps_for_ticker(engine, "BA", date(2026, 5, 16))

    assert result.score is not None
    # Only one layer with value 1.0, weight 1.0 → average is 1.0 → 100
    assert result.score == pytest.approx(100.0)
    assert result.coverage == {
        "direct_contracts": True,
        "lobbying_admin": False,
        "congressional_30d": False,
        "fara_admin": False,
        "actor_hops": False,
    }
    assert result.layer_scores["direct_contracts"] == pytest.approx(1.0)
    assert result.layer_scores["lobbying_admin"] is None
    assert result.layer_scores["actor_hops"] is None


# ── Edge: ticker with zero evidence ────────────────────────────────────


def test_compute_tps_ticker_with_zero_evidence_returns_none_score():
    queries = [[], [], [], [], []]
    engine = _engine_with_results(queries)

    result = compute_tps_for_ticker(engine, "FOO", date(2026, 5, 16))

    assert isinstance(result, TPSResult)
    assert result.ticker == "FOO"
    assert result.score is None


# ── Persistence ────────────────────────────────────────────────────────


def test_persist_snapshot_emits_upsert():
    """persist_snapshot upserts with JSONB-cast bind params."""
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    result = TPSResult(
        ticker="LMT",
        as_of="2026-05-16",
        score=72.5,
        layer_scores={"direct_contracts": 0.8, "lobbying_admin": None, "congressional_30d": 0.4, "fara_admin": None, "actor_hops": 0.7},
        coverage={"direct_contracts": True, "lobbying_admin": False, "congressional_30d": True, "fara_admin": False, "actor_hops": True},
        evidence=[],
    )

    persist_snapshot(engine, result)

    assert conn.execute.called
    args, kwargs = conn.execute.call_args
    params = args[1] if len(args) > 1 else kwargs.get("parameters") or {}
    assert params["ticker"] == "LMT"
    assert params["as_of"] == "2026-05-16"
    assert params["score"] == 72.5
    # JSON serialised — coverage round-trips
    assert json.loads(params["coverage"]) == result.coverage


# ── Admin-aligned helpers ──────────────────────────────────────────────


def test_admin_aligned_registrant_detects_ballard():
    assert _is_admin_aligned_registrant("Ballard Partners LLC") is True
    assert _is_admin_aligned_registrant("ballard partners") is True


def test_admin_aligned_registrant_rejects_unknown_firms():
    assert _is_admin_aligned_registrant("Some Random Lobbyist") is False
    assert _is_admin_aligned_registrant("") is False


# ── BFS termination & no-path ───────────────────────────────────────────


def test_bfs_returns_none_when_no_admin_in_budget():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("issuer_x", json.dumps([]), None, json.dumps({})),
    ]
    hops, admin = _bfs_to_admin(conn, "issuer_x", max_hops=2)
    assert hops is None
    assert admin is None


# ── Precision@10 placeholder hook ──────────────────────────────────────


def test_layer_weights_expose_all_five_layers_for_tuning():
    """The precision@10 forward-return tuning harness iterates over
    ``LAYER_WEIGHTS`` to grid-search Phase 1 weights. Surface the
    contract here so the harness doesn't have to import internals."""
    assert set(LAYER_WEIGHTS.keys()) == {
        "direct_contracts",
        "lobbying_admin",
        "congressional_30d",
        "fara_admin",
        "actor_hops",
    }
    # All weights start flat at 1.0 per section 6 of the pivot doc.
    assert all(w == 1.0 for w in LAYER_WEIGHTS.values())
