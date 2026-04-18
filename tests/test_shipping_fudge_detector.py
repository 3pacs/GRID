"""Tests for intelligence/shipping_fudge_detector.py.

All tests mock the engine and series-history helper entirely so they
never touch a real database. Goal: verify the pure-function delta and
z-score math, the pairing map coverage, and the graceful-degrade
behavior when the history is too short.
"""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from intelligence.cross_reference import (
    MAJOR_DIVERGENCE_THRESHOLD,
    MIN_OBSERVATIONS,
)
from intelligence.shipping_fudge_detector import (
    DELTA_LOOKBACK_DAYS,
    MIN_HISTORY_DAYS,
    PERSISTENCE_WINDOW_DAYS,
    SHIPPING_DIVERGENCE_THRESHOLD,
    _align_latest_pair,
    _build_implication,
    _build_narrative,
    _compute_delta_series,
    _zscore_of_latest,
    check_pairing,
    check_port_reported_vs_observed,
    pairings_for_port,
    run_shipping_fudge_detector,
)


# ── pairings_for_port ────────────────────────────────────────────────────


class TestPairingsForPort:
    def test_iron_ore_ports_get_iron_ore_pairings(self):
        pairings = pairings_for_port("qingdao")
        assert len(pairings) > 0
        # Qingdao should get iron ore + container pairings
        kinds = {p[0] for p in pairings}
        assert any("iron_ore" in k for k in kinds)
        assert any("freight" in k for k in kinds)

    def test_us_ports_get_container_pairings_only(self):
        pairings = pairings_for_port("la")
        kinds = {p[0] for p in pairings}
        assert not any("iron_ore" in k for k in kinds)
        assert any("freight" in k for k in kinds)

    def test_rotterdam_gets_container_and_lme(self):
        pairings = pairings_for_port("rotterdam")
        kinds = {p[0] for p in pairings}
        assert any("freight" in k for k in kinds)
        assert any("lme" in k for k in kinds)

    def test_unknown_port_returns_empty(self):
        assert pairings_for_port("nonexistent") == []

    def test_all_pairings_have_4_fields(self):
        for slug in ("qingdao", "la", "rotterdam", "kaohsiung"):
            for pairing in pairings_for_port(slug):
                assert len(pairing) == 4
                assert all(isinstance(x, str) for x in pairing)


# ── _compute_delta_series ────────────────────────────────────────────────


class TestComputeDeltaSeries:
    def test_empty_history_returns_empty(self):
        assert _compute_delta_series([]) == []

    def test_single_point_returns_empty(self):
        assert _compute_delta_series([(date(2026, 4, 13), 100.0)]) == []

    def test_rolling_week_delta(self):
        base = date(2026, 1, 1)
        history = [(base + timedelta(days=i), float(i)) for i in range(30)]
        deltas = _compute_delta_series(history, window_days=7)
        # Every delta past the 7th day should be +7 (linear ramp)
        assert len(deltas) > 0
        assert all(abs(d - 7.0) < 0.01 for _, d in deltas)

    def test_gap_tolerance_within_2_days(self):
        # Missing day-6 shouldn't break day-7 delta (falls back to day-5)
        base = date(2026, 1, 1)
        history = [
            (base + timedelta(days=i), float(i))
            for i in range(30)
            if i != 6
        ]
        deltas = _compute_delta_series(history, window_days=7)
        # Should still produce deltas (some will use day-5 as the prior)
        assert len(deltas) > 0


# ── _zscore_of_latest ────────────────────────────────────────────────────


class TestZScoreOfLatest:
    def test_insufficient_history(self):
        delta = [(date(2026, 4, 13), 1.0), (date(2026, 4, 12), 2.0)]
        z, d = _zscore_of_latest(delta)
        assert z is None and d is None

    def test_zero_variance_returns_zero(self):
        base = date(2026, 1, 1)
        delta = [(base + timedelta(days=i), 5.0) for i in range(MIN_OBSERVATIONS + 5)]
        z, d = _zscore_of_latest(delta)
        assert z == 0.0
        assert d == delta[-1][0]

    def test_positive_z_for_spike(self):
        base = date(2026, 1, 1)
        delta = [(base + timedelta(days=i), 1.0) for i in range(MIN_OBSERVATIONS)]
        delta.append((base + timedelta(days=MIN_OBSERVATIONS), 100.0))
        z, d = _zscore_of_latest(delta)
        assert z is not None
        assert z > 2.0


# ── _align_latest_pair ───────────────────────────────────────────────────


class TestAlignLatestPair:
    def test_empty_inputs(self):
        r, o = _align_latest_pair([], [])
        assert r is None and o is None

    def test_exact_date_match(self):
        d = date(2026, 4, 13)
        reported = [(d, 10.0)]
        observed = [(d, 20.0)]
        r, o = _align_latest_pair(reported, observed)
        assert r == (10.0, d)
        assert o == (20.0, d)

    def test_within_2_day_offset(self):
        d1 = date(2026, 4, 13)
        d2 = date(2026, 4, 12)
        reported = [(d1, 10.0)]
        observed = [(d2, 20.0)]
        r, o = _align_latest_pair(reported, observed)
        assert r == (10.0, d1)
        assert o == (20.0, d2)

    def test_no_overlap_returns_none(self):
        reported = [(date(2026, 1, 1), 10.0)]
        observed = [(date(2026, 4, 13), 20.0)]
        r, o = _align_latest_pair(reported, observed)
        assert r is None and o is None


# ── check_pairing — the integration point ───────────────────────────────


class TestCheckPairing:
    def _make_history(self, base: date, n: int, slope: float) -> list:
        return [(base + timedelta(days=i), i * slope) for i in range(n)]

    def test_returns_none_on_short_history(self):
        engine = MagicMock()
        with patch(
            "intelligence.shipping_fudge_detector._get_series_history",
            return_value=[(date(2026, 4, 13), 1.0)],
        ):
            result = check_pairing(
                engine, "qingdao",
                "iron_ore:port_stocks_mt:aggregate",
                "ais:ships_at_berth:qingdao",
                "positive_correlation",
                "test",
            )
        assert result is None

    def test_returns_none_when_both_sides_move_together(self):
        engine = MagicMock()
        base = date(2024, 1, 1)
        # Same slope on both sides → near-zero divergence
        same = self._make_history(base, 800, 1.0)
        with patch(
            "intelligence.shipping_fudge_detector._get_series_history",
            return_value=same,
        ):
            result = check_pairing(
                engine, "qingdao",
                "iron_ore:port_stocks_mt:aggregate",
                "ais:ships_at_berth:qingdao",
                "positive_correlation",
                "test",
            )
        # Same series on both sides → divergence ~0, below minor threshold
        assert result is None

    def test_fires_on_strong_divergence(self):
        engine = MagicMock()
        base = date(2024, 1, 1)
        # Reported: flat for 800 days, then sudden spike
        reported = [(base + timedelta(days=i), 100.0) for i in range(799)]
        reported.append((base + timedelta(days=799), 10000.0))
        # Observed: flat the whole time, no spike
        observed = [(base + timedelta(days=i), 50.0) for i in range(800)]

        call_args: list = []

        def fake_get_history(eng, series_id, since=None):
            call_args.append(series_id)
            if "iron_ore" in series_id:
                return reported
            return observed

        with patch(
            "intelligence.shipping_fudge_detector._get_series_history",
            side_effect=fake_get_history,
        ):
            result = check_pairing(
                engine, "qingdao",
                "iron_ore:port_stocks_mt:aggregate",
                "ais:ships_at_berth:qingdao",
                "positive_correlation",
                "reported stocks vs observed berth count",
            )
        assert result is not None
        assert result.category == "shipping"
        assert result.assessment in (
            "minor_divergence", "major_divergence", "contradiction",
        )
        assert abs(result.actual_divergence) >= 1.0


# ── check_port_reported_vs_observed ──────────────────────────────────────


class TestCheckPortReportedVsObserved:
    def test_unknown_port_returns_empty(self):
        engine = MagicMock()
        result = check_port_reported_vs_observed(engine, "nonexistent")
        assert result == []

    def test_port_with_no_history_returns_empty(self):
        engine = MagicMock()
        with patch(
            "intelligence.shipping_fudge_detector._get_series_history",
            return_value=[],
        ):
            result = check_port_reported_vs_observed(engine, "qingdao")
        assert result == []


# ── run_shipping_fudge_detector — full sweep ────────────────────────────


class TestRunShippingFudgeDetector:
    def test_empty_sweep_returns_clean_report(self):
        engine = MagicMock()
        # Mock the engine's begin/execute to swallow persistence writes
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        with patch(
            "intelligence.shipping_fudge_detector._get_series_history",
            return_value=[],
        ):
            report = run_shipping_fudge_detector(engine)
        assert report.checks == []
        assert report.red_flags == []
        assert "No shipping" in report.narrative
        assert report.summary["total_checks"] == 0
        assert report.summary["red_flag_count"] == 0

    def test_sweep_handles_port_failure_gracefully(self):
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        with patch(
            "intelligence.shipping_fudge_detector._get_series_history",
            side_effect=RuntimeError("db down"),
        ):
            report = run_shipping_fudge_detector(engine)
        # Errors don't crash the sweep — they just yield zero checks
        assert isinstance(report.checks, list)


# ── narrative builder ────────────────────────────────────────────────────


class TestBuildNarrative:
    def test_empty_returns_no_divergences_message(self):
        text = _build_narrative([], [])
        assert "No shipping" in text

    def test_minor_only_mentions_monitor(self):
        from intelligence.cross_reference import CrossRefCheck
        from datetime import datetime

        minor = CrossRefCheck(
            name="x", category="shipping", official_source="a",
            official_value=1.0, physical_source="b", physical_value=2.0,
            expected_relationship="positive_correlation",
            actual_divergence=1.2, assessment="minor_divergence",
            implication="i", confidence=0.5,
            checked_at=datetime.now().isoformat(),
        )
        text = _build_narrative([minor], [])
        assert "minor" in text.lower()
        assert "Monitor" in text or "monitor" in text

    def test_major_flags_include_top_3(self):
        from intelligence.cross_reference import CrossRefCheck
        from datetime import datetime

        flags = [
            CrossRefCheck(
                name=f"x{i}", category="shipping", official_source="a",
                official_value=1.0, physical_source="b", physical_value=2.0,
                expected_relationship="positive_correlation",
                actual_divergence=float(2 + i), assessment="major_divergence",
                implication=f"impl-{i}", confidence=0.5,
                checked_at=datetime.now().isoformat(),
            )
            for i in range(5)
        ]
        text = _build_narrative(flags, flags)
        # Top-3 only (not all 5)
        assert text.count("impl-") == 3
        # Sorted by severity — highest |z| first
        assert "impl-4" in text


# ── implication builder ────────────────────────────────────────────────


class TestBuildImplication:
    def test_positive_divergence_says_reported_higher(self):
        text = _build_implication(
            port_slug="qingdao",
            divergence_z=2.5,
            r_value=1.0,
            o_value=-1.0,
            assessment="major_divergence",
            description="test pairing",
        )
        assert "HIGHER" in text
        assert "qingdao" in text
        assert "MAJOR" in text

    def test_negative_divergence_says_reported_lower(self):
        text = _build_implication(
            port_slug="shanghai",
            divergence_z=-2.5,
            r_value=-1.0,
            o_value=1.0,
            assessment="contradiction",
            description="test pairing",
        )
        assert "LOWER" in text
        assert "CONTRADICTION" in text
