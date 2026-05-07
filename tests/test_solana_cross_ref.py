"""
Tests for trading/solana/cross_ref.py.

The cross-referencer is glue code over injectable sources, so every
test here uses a MagicMock for each source and verifies the composite
math plus the narrative-term injection.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trading.solana.cross_ref import (
    CrossRefWeights,
    CrossReferencer,
    LaunchEvent,
    NarrativeRegistry,
)
from trading.solana.deployer_registry import DeployerScoreResult, DeployerStats
from trading.solana.smart_money import SmartMoneyMatch, SmartMoneyMatchSet
from trading.solana.universe import UniverseRank


def _deployer_result(score: float = 0.8, n: int = 10) -> DeployerScoreResult:
    stats = DeployerStats(
        wallet="DEPLOYER1",
        n_launches=n,
        n_graduated=int(n * 0.7),
        median_peak_mc_usd=500_000.0,
        best_peak_mc_usd=2_000_000.0,
        avg_hold_seconds=1200.0,
        last_launch_at=None,
    )
    return DeployerScoreResult(
        wallet="DEPLOYER1",
        score=score,
        components={"graduation_rate": 0.7},
        stats=stats,
        reasons=(),
    )


def _smart_money_matches(n: int, trust: float = 0.8) -> SmartMoneyMatchSet:
    return SmartMoneyMatchSet(
        matches=tuple(
            SmartMoneyMatch(f"W{i}", f"label{i}", trust, "op") for i in range(n)
        )
    )


# ----------------------------------------------------------------------
# NarrativeRegistry
# ----------------------------------------------------------------------
def test_narrative_add_and_match_symbol():
    nr = NarrativeRegistry()
    nr.add("pepe", 1.0)
    nr.add("frog", 0.5)
    hits = nr.match(symbol="PEPEFROG", name="Pepe the frog coin")
    assert {h.term for h in hits} == {"pepe", "frog"}


def test_narrative_match_case_insensitive():
    nr = NarrativeRegistry()
    nr.add("AI16Z", 0.8)
    hits = nr.match(symbol="ai16z", name=None)
    assert len(hits) == 1
    assert hits[0].term == "ai16z"


def test_narrative_match_prefers_symbol_over_name():
    nr = NarrativeRegistry()
    nr.add("cat", 1.0)
    # 'cat' appears in both symbol and name; symbol wins (one hit only).
    hits = nr.match(symbol="FATCAT", name="the fat cat coin")
    assert len(hits) == 1
    assert hits[0].matched_field == "symbol"


def test_narrative_remove_and_clear():
    nr = NarrativeRegistry()
    nr.add("pepe", 1.0)
    nr.remove("PEPE")  # case insensitive
    assert nr.match(symbol="PEPE", name=None) == []
    nr.add("dog", 0.5)
    nr.clear()
    assert nr.list_terms() == []


def test_narrative_add_clamps_weight():
    nr = NarrativeRegistry()
    nr.add("x", 5.0)
    assert nr.list_terms() == [("x", 1.0)]


def test_narrative_ignores_empty_terms():
    nr = NarrativeRegistry()
    nr.add("   ", 1.0)
    assert nr.list_terms() == []


# ----------------------------------------------------------------------
# CrossReferencer — composite math
# ----------------------------------------------------------------------
def test_evaluate_all_sources_active_high_signal():
    deployer_registry = MagicMock()
    deployer_registry.get.return_value = _deployer_result(score=0.8)
    smart_money = MagicMock()
    smart_money.match_early_buyers.return_value = _smart_money_matches(3, 0.8)
    narratives = NarrativeRegistry()
    narratives.add("pepe", 1.0)
    convergence = MagicMock()
    convergence.detect.return_value = 0.6

    cr = CrossReferencer(
        deployer_registry=deployer_registry,
        smart_money=smart_money,
        narratives=narratives,
        convergence=convergence,
    )
    launch = LaunchEvent(
        mint="MINT1",
        deployer="DEPLOYER1",
        symbol="PEPEFROG",
        early_buyers=("W1", "W2", "W3"),
    )
    report = cr.evaluate(launch)

    assert report.deployer_score == 0.8
    assert report.smart_money_hits == 3
    assert report.smart_money_trust > 0.9
    assert report.narrative_weight == 1.0
    assert report.convergence_score == 0.6
    assert report.composite_score > 0.75
    assert report.actionable is True
    assert len(report.reasons) >= 3


def test_evaluate_degrades_when_only_deployer_active():
    deployer_registry = MagicMock()
    deployer_registry.get.return_value = _deployer_result(score=0.6)

    cr = CrossReferencer(deployer_registry=deployer_registry)
    launch = LaunchEvent(mint="MINT1", deployer="D1")
    report = cr.evaluate(launch)

    # Only deployer contributes; denom is deployer weight alone,
    # so composite equals deployer_score (0.6).
    assert report.composite_score == pytest.approx(0.6)
    assert report.smart_money_hits == 0
    assert report.narrative_weight == 0.0


def test_evaluate_unknown_deployer_skips_source():
    deployer_registry = MagicMock()
    deployer_registry.get.return_value = None

    cr = CrossReferencer(deployer_registry=deployer_registry)
    launch = LaunchEvent(mint="MINT1", deployer="UNKNOWN")
    report = cr.evaluate(launch)
    assert report.deployer_score == 0.0
    assert report.composite_score == 0.0
    assert not report.actionable


def test_evaluate_no_sources_returns_zero():
    cr = CrossReferencer()
    report = cr.evaluate(LaunchEvent(mint="M1"))
    assert report.composite_score == 0.0
    assert not report.actionable
    assert "no matching sources" in report.reasons[0]


def test_evaluate_isolates_source_exceptions():
    deployer_registry = MagicMock()
    deployer_registry.get.side_effect = RuntimeError("db down")
    smart_money = MagicMock()
    smart_money.match_early_buyers.return_value = _smart_money_matches(2, 0.8)

    cr = CrossReferencer(
        deployer_registry=deployer_registry,
        smart_money=smart_money,
    )
    report = cr.evaluate(
        LaunchEvent(mint="M1", deployer="D1", early_buyers=("W1", "W2"))
    )
    # Deployer crashed → deployer score 0; smart money still contributes.
    assert report.deployer_score == 0.0
    assert report.smart_money_hits == 2
    assert report.composite_score > 0.0


def test_evaluate_narrative_boosts_composite():
    narratives = NarrativeRegistry()
    narratives.add("pepe", 1.0)

    cr = CrossReferencer(narratives=narratives)
    with_match = cr.evaluate(LaunchEvent(mint="M1", symbol="PEPE420"))
    without_match = cr.evaluate(LaunchEvent(mint="M1", symbol="UNRELATED"))

    assert with_match.narrative_weight == 1.0
    assert with_match.composite_score == pytest.approx(1.0)
    # Without a match, narrative contributes 0 but the source is still
    # active, so the composite is 0.
    assert without_match.composite_score == 0.0


def test_evaluate_smart_money_with_empty_early_buyers():
    smart_money = MagicMock()
    smart_money.match_early_buyers.return_value = SmartMoneyMatchSet(matches=())

    cr = CrossReferencer(smart_money=smart_money)
    # Empty early_buyers → smart money isn't called at all.
    report = cr.evaluate(LaunchEvent(mint="M1"))
    assert report.smart_money_hits == 0
    assert report.composite_score == 0.0
    smart_money.match_early_buyers.assert_not_called()


def test_evaluate_clamps_composite_to_unit_interval():
    # Custom weights that sum > 1 to test clamping.
    weights = CrossRefWeights(
        deployer=1.0, smart_money=1.0, narrative=1.0, convergence=1.0, universe=1.0
    )
    deployer_registry = MagicMock()
    deployer_registry.get.return_value = _deployer_result(score=1.0)
    smart_money = MagicMock()
    smart_money.match_early_buyers.return_value = _smart_money_matches(5, 0.99)
    narratives = NarrativeRegistry()
    narratives.add("pepe", 1.0)
    convergence = MagicMock()
    convergence.detect.return_value = 1.0

    cr = CrossReferencer(
        deployer_registry=deployer_registry,
        smart_money=smart_money,
        narratives=narratives,
        convergence=convergence,
        weights=weights,
    )
    report = cr.evaluate(
        LaunchEvent(
            mint="M1", deployer="D1", symbol="PEPE", early_buyers=("W",)
        )
    )
    assert report.composite_score <= 1.0


# ----------------------------------------------------------------------
# Universe (top-N by volume) source
# ----------------------------------------------------------------------
from datetime import datetime, timezone  # noqa: E402
NOW = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)


def _universe_rank(mint: str = "MINT1", rank: int = 1) -> UniverseRank:
    return UniverseRank(
        mint=mint, rank=rank, volume_24h_usd=5_000_000.0, snapshot_at=NOW
    )


def test_universe_top_rank_boosts_composite():
    universe = MagicMock()
    universe.get_latest_rank.return_value = _universe_rank(rank=1)

    cr = CrossReferencer(universe=universe, universe_limit=250)
    report = cr.evaluate(LaunchEvent(mint="MINT1"))

    assert report.universe_score == 1.0
    # Universe is the only active source → composite equals the
    # source's own score.
    assert report.composite_score == pytest.approx(1.0)
    assert any("rank #1" in r for r in report.reasons)
    assert report.universe_rank is not None


def test_universe_tail_rank_gives_zero():
    universe = MagicMock()
    universe.get_latest_rank.return_value = _universe_rank(rank=250)

    cr = CrossReferencer(universe=universe, universe_limit=250)
    report = cr.evaluate(LaunchEvent(mint="MINT1"))

    assert report.universe_score == 0.0
    assert report.composite_score == 0.0


def test_universe_not_in_snapshot_is_active_but_zero():
    universe = MagicMock()
    universe.get_latest_rank.return_value = None

    cr = CrossReferencer(universe=universe, universe_limit=250)
    report = cr.evaluate(LaunchEvent(mint="MINT1"))

    # The source was consulted → it's active, contributing 0 to the
    # composite. Since it's the only source, composite is 0.
    assert report.universe_score == 0.0
    assert report.composite_score == 0.0
    assert report.universe_rank is None


def test_universe_combines_with_other_sources():
    universe = MagicMock()
    universe.get_latest_rank.return_value = _universe_rank(rank=10)  # ~0.58
    deployer_registry = MagicMock()
    deployer_registry.get.return_value = _deployer_result(score=0.8)

    cr = CrossReferencer(
        deployer_registry=deployer_registry,
        universe=universe,
    )
    report = cr.evaluate(LaunchEvent(mint="MINT1", deployer="D1"))

    # Both sources active → composite is a weighted blend bounded in
    # [0, 1]. Just verify it's between the two extremes and non-zero.
    assert 0 < report.composite_score < 1.0
    assert report.deployer_score == 0.8
    assert report.universe_score == pytest.approx(0.5834, abs=0.01)


def test_universe_error_is_isolated():
    universe = MagicMock()
    universe.get_latest_rank.side_effect = RuntimeError("db down")
    deployer_registry = MagicMock()
    deployer_registry.get.return_value = _deployer_result(score=0.6)

    cr = CrossReferencer(
        deployer_registry=deployer_registry,
        universe=universe,
    )
    report = cr.evaluate(LaunchEvent(mint="MINT1", deployer="D1"))
    # Universe crashed → inactive; deployer still contributes.
    assert report.universe_score == 0.0
    assert report.composite_score == pytest.approx(0.6)
