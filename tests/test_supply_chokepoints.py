"""Tests for intelligence/supply_chokepoints.py.

Covers:
  - substitution_penalty: single vs many alternatives
  - buyer_concentration: explicit pct vs annual_usd fallback
  - geographic_concentration: HHI over alt supplier countries
  - historical_disruption: keyword match bumps
  - compute_chokepoint_score: weighted combination + clamping
  - preserve-existing: NULL-only UPDATE guard stays idempotent
  - flag_chokepoint_nodes: SQL builds and runs
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from intelligence.supply_chokepoints import (
    EdgeContext,
    HIGH_SCORE_THRESHOLD,
    buyer_concentration,
    compute_chokepoint_score,
    flag_chokepoint_nodes,
    geographic_concentration,
    historical_disruption,
    score_all_edges,
    substitution_penalty,
)


# ── Pure function tests ──────────────────────────────────────────────────────

def test_substitution_penalty_single_source_is_one():
    """Single upstream supplier → penalty 1.0 (max risk)."""
    assert substitution_penalty(0) == 1.0


def test_substitution_penalty_scales_inversely():
    """Two sources → 0.5, three → ~0.33, four → 0.25."""
    assert substitution_penalty(1) == pytest.approx(0.5)
    assert substitution_penalty(2) == pytest.approx(1 / 3)
    assert substitution_penalty(3) == pytest.approx(0.25)
    # Commoditized input with many sources → low penalty
    assert substitution_penalty(9) == pytest.approx(0.1)


def test_substitution_penalty_negative_clamped():
    assert substitution_penalty(-5) == 1.0


def test_buyer_concentration_prefers_explicit_pct():
    """pct_downstream_cogs is used directly when present."""
    assert buyer_concentration(0.35, 100.0, 1000.0) == pytest.approx(0.35)
    # 0-100 scale tolerance
    assert buyer_concentration(35.0, None, None) == pytest.approx(0.35)


def test_buyer_concentration_falls_back_to_annual_usd_share():
    """When pct missing, use annual_usd / downstream_total."""
    assert buyer_concentration(None, 250.0, 1000.0) == pytest.approx(0.25)
    assert buyer_concentration(None, None, None) == 0.0
    # Division by zero safe
    assert buyer_concentration(None, 10.0, 0.0) == 0.0


def test_buyer_concentration_clamps_to_unit_interval():
    # Value >1 is treated as a 0-100 percentage, then clamped
    assert buyer_concentration(150.0, None, None) == 1.0
    assert buyer_concentration(-0.2, None, None) == 0.0
    # Fallback ratio > 1 is clamped
    assert buyer_concentration(None, 2000.0, 1000.0) == 1.0


def test_geographic_concentration_single_country_is_one():
    """All suppliers in one country → HHI = 1.0."""
    assert geographic_concentration({"taiwan": 3}) == 1.0


def test_geographic_concentration_multi_country_hhi():
    """Two countries 50/50 → HHI = 0.5; four countries 25% each → 0.25."""
    assert geographic_concentration({"usa": 2, "china": 2}) == pytest.approx(0.5)
    assert geographic_concentration(
        {"usa": 1, "china": 1, "japan": 1, "korea": 1}
    ) == pytest.approx(0.25)


def test_geographic_concentration_empty_returns_zero():
    assert geographic_concentration({}) == 0.0


def test_historical_disruption_neon_ukraine():
    bump, reasons = historical_disruption("semiconductor-grade neon gas", "ukraine")
    assert bump >= 0.30
    assert any("Ukraine" in r for r in reasons)


def test_historical_disruption_no_match_zero():
    bump, reasons = historical_disruption("aluminum can sheet", None)
    assert bump == 0.0
    assert reasons == []


def test_historical_disruption_capped_at_one():
    # Stack multiple triggers
    bump, _ = historical_disruption(
        "rare_earths china cowos taiwan neon euv asml"
    )
    assert bump <= 1.0


# ── Weighted score tests ─────────────────────────────────────────────────────

def test_compute_score_single_source_high_risk():
    """Single-source edge with country concentration should score >= 0.6."""
    ctx = EdgeContext(
        alt_count=0,
        pct_downstream_cogs=0.40,
        country_hhi=1.0,
        historical_bump=0.0,
    )
    breakdown = compute_chokepoint_score({}, ctx)
    # 0.40*1.0 + 0.25*0.4 + 0.20*1.0 + 0.15*0 = 0.70
    assert breakdown.score >= 0.6
    assert breakdown.substitution_penalty == 1.0


def test_compute_score_commoditized_low_risk():
    """Many suppliers, small share, diverse geo → low score."""
    ctx = EdgeContext(
        alt_count=9,  # 10 suppliers
        pct_downstream_cogs=0.05,
        country_hhi=0.15,
        historical_bump=0.0,
    )
    breakdown = compute_chokepoint_score({}, ctx)
    # 0.40*0.1 + 0.25*0.05 + 0.20*0.15 + 0 = 0.0825
    assert breakdown.score <= 0.3


def test_compute_score_clamped_to_unit_interval():
    ctx = EdgeContext(
        alt_count=0,
        pct_downstream_cogs=1.0,
        country_hhi=1.0,
        historical_bump=1.0,
    )
    breakdown = compute_chokepoint_score({}, ctx)
    assert breakdown.score == 1.0


def test_compute_score_is_rounded_to_three_decimals():
    ctx = EdgeContext(alt_count=2, pct_downstream_cogs=0.123456)
    breakdown = compute_chokepoint_score({}, ctx)
    # The stored score is rounded to 3 decimals
    assert round(breakdown.score, 3) == breakdown.score


# ── DB path tests (mocked) ───────────────────────────────────────────────────

def _make_mock_engine(
    fetchall_rows: list = None, fetchone_row=None, scalar_value=None
):
    """Build a mock engine whose connect/begin yields a conn with scripted results."""
    from sqlalchemy.engine import Engine
    from unittest.mock import create_autospec

    engine = create_autospec(Engine, instance=True)
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = fetchall_rows or []
    result.fetchone.return_value = fetchone_row
    result.scalar.return_value = scalar_value
    conn.execute.return_value = result

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def test_score_all_edges_preserves_existing_values():
    """score_all_edges only queries rows where chokepoint_score IS NULL.

    When the unscored query returns zero rows, no UPDATE is issued — this is
    the guarantee that hand-curated values survive.
    """
    engine, conn = _make_mock_engine(fetchall_rows=[])
    stats = score_all_edges(engine)
    assert stats["scored"] == 0
    assert stats["scanned"] == 0
    # Only the SELECT fired; no UPDATE statements were executed.
    assert conn.execute.call_count == 1


def test_flag_chokepoint_nodes_runs_and_reports():
    """flag_chokepoint_nodes issues the UPDATE and returns the totals."""
    engine, conn = _make_mock_engine(
        fetchall_rows=[("hsy",), ("asml",)],  # RETURNING ids
        scalar_value=42,
    )
    stats = flag_chokepoint_nodes(engine, threshold=HIGH_SCORE_THRESHOLD)
    assert stats["newly_flagged"] == 2
    assert stats["total_flagged"] == 42
    assert stats["threshold"] == HIGH_SCORE_THRESHOLD
    # First call = UPDATE ... RETURNING; second = SELECT COUNT
    assert conn.execute.call_count == 2
