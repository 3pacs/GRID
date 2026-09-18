"""Honesty guards for the market-edge scanner (A-H14, cluster C4).

`expected_edge_pct` was a percentage *return* derived from a hand-tuned playbook
constant with no backtest anywhere in the path. These tests pin the replacement:
a dimensionless `heuristic_rank` with an explicit `basis`, a `heuristic_confidence`
whose every point award is published, and a `confidence` that stays `null` because
nothing here was ever scored against outcomes.
"""

from __future__ import annotations

import inspect
from datetime import date

import intelligence.market_edge_scanner as mes
from intelligence.market_edge_scanner import (
    PLAYBOOKS,
    TickerSignalProfile,
    _base_opportunity,
    _build_summary,
    build_market_edge_snapshot,
)


def _profile(ticker: str) -> TickerSignalProfile:
    return TickerSignalProfile(
        ticker=ticker,
        company=f"{ticker} Inc",
        sector="Industrials",
        subsector="Defense",
    )


def _opportunity() -> dict:
    playbook = PLAYBOOKS[0]
    profiles = [_profile(t) for t in playbook.target_pool[:2]]
    return _base_opportunity(playbook, profiles, date(2026, 9, 17))


def test_source_module_has_no_expected_edge_or_base_edge_symbol() -> None:
    source = inspect.getsource(mes)
    assert "expected_edge_pct" not in source
    assert "base_edge" not in source


def test_opportunity_has_no_expected_edge_pct_key() -> None:
    opportunity = _opportunity()
    assert "expected_edge_pct" not in opportunity
    assert not any("expected_edge" in key for key in opportunity)


def test_opportunity_publishes_heuristic_rank_with_basis() -> None:
    opportunity = _opportunity()
    assert isinstance(opportunity["heuristic_rank"], int)
    assert opportunity["basis"] == "playbook_prior"


def test_confidence_is_null_because_nothing_was_scored() -> None:
    opportunity = _opportunity()
    assert opportunity["confidence"] is None
    assert opportunity["confidence_basis"] == "no_scored_track_record"
    assert isinstance(opportunity["heuristic_confidence"], int)
    assert opportunity["heuristic_confidence_label"] in {"low", "medium", "high"}


def test_heuristic_confidence_inputs_account_for_the_published_score() -> None:
    opportunity = _opportunity()
    components = opportunity["heuristic_confidence_inputs"]
    assert components, "the score must ship the point awards that produced it"
    assert {entry["component"] for entry in components} >= {"playbook_prior"}
    for entry in components:
        assert isinstance(entry["points"], int)
        assert entry["detail"]
    assert sum(entry["points"] for entry in components) == opportunity["heuristic_confidence"]


def test_summary_reports_null_average_rank_when_nothing_is_ranked() -> None:
    summary = _build_summary([], [])
    # Nothing scored must not become 0 (or 0.0) under an averaging label.
    assert summary["avg_heuristic_rank"] is None
    assert "avg_expected_edge_pct" not in summary
    assert summary["heuristic_basis"] == "playbook_prior"
    assert summary["count"] == 0


def test_summary_averages_only_ranked_items() -> None:
    summary = _build_summary(
        [
            {"id": "a", "heuristic_rank": 18, "score": 80, "status": "active", "data_mode": "live", "evidence": []},
            {"id": "b", "heuristic_rank": 12, "score": 60, "status": "watch", "data_mode": "live", "evidence": []},
        ],
        [],
    )
    assert summary["avg_heuristic_rank"] == 15.0
    assert summary["high_heuristic_count"] == 1


def test_snapshot_without_engine_emits_no_ranked_numbers() -> None:
    snapshot = build_market_edge_snapshot(None, limit=5)
    assert snapshot["opportunities"] == []
    assert snapshot["summary"]["avg_heuristic_rank"] is None
    assert "avg_expected_edge_pct" not in snapshot["summary"]
