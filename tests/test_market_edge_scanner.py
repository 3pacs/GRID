from __future__ import annotations

from intelligence.market_edge_scanner import PLAYBOOKS, build_market_edge_snapshot


def test_market_edge_snapshot_without_engine_returns_no_synthetic_opportunities():
    snapshot = build_market_edge_snapshot(None, as_of=None, limit=10)

    assert snapshot["public_data_only"] is True
    assert "summary" in snapshot
    assert snapshot["opportunities"] == []
    assert snapshot["summary"]["count"] == 0
    assert snapshot["summary"]["coverage_gap_count"] == len(PLAYBOOKS)
    assert len(snapshot["coverage_gaps"]) == len(PLAYBOOKS)
    assert all(gap["missing_primary_sources"] for gap in snapshot["coverage_gaps"])
    assert all(gap["reason"] for gap in snapshot["coverage_gaps"])


def test_market_edge_snapshot_limit_is_honored():
    snapshot = build_market_edge_snapshot(None, limit=4)

    assert snapshot["opportunities"] == []
    assert snapshot["summary"]["count"] == 0
    assert len(snapshot["coverage_gaps"]) == len(PLAYBOOKS)
